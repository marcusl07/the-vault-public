from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
from typing import Callable


@dataclass(frozen=True)
class SourceNote:
    path: Path
    filename: str
    frontmatter: dict[str, object]
    body: str

    @property
    def capture_id(self) -> str | None:
        value = self.frontmatter.get("capture_id")
        return value if isinstance(value, str) and value else None

    @property
    def ingest_attempts(self) -> int:
        value = self.frontmatter.get("ingest_attempts", 0)
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0


def utc_timestamp(value: float | None = None) -> str:
    if value is None:
        dt = datetime.now(UTC)
    else:
        dt = datetime.fromtimestamp(value, tz=UTC)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_repo_path(root: Path, path: str | Path) -> str:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = root / candidate

    root_abs = Path(os.path.abspath(root))
    candidate_abs = Path(os.path.abspath(candidate))
    if Path(os.path.commonpath([root_abs, candidate_abs])) != root_abs:
        raise ValueError(f"path must stay within repo root: {path}")
    relative = Path(os.path.relpath(candidate_abs, root_abs))
    return PurePosixPath(relative.as_posix()).as_posix()


def stable_source_id(source_kind: str, identity: str) -> str:
    return f"{source_kind}:{identity}"


def _parse_scalar(value: str) -> object:
    stripped = value.strip()
    if not stripped:
        return ""
    if stripped.startswith("'") and stripped.endswith("'"):
        return stripped[1:-1].replace("''", "'")
    if stripped.startswith('"') and stripped.endswith('"'):
        return stripped[1:-1].replace('\\"', '"')
    if re.fullmatch(r"-?\d+", stripped):
        return int(stripped)
    return stripped


def _render_scalar(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def split_frontmatter(text: str) -> tuple[dict[str, object], str, bool]:
    if not text.startswith("---\n"):
        return {}, text, False

    lines = text.splitlines(keepends=True)
    frontmatter_lines: list[str] = []
    for index in range(1, len(lines)):
        if lines[index].strip() == "---":
            frontmatter: dict[str, object] = {}
            for raw_line in frontmatter_lines:
                stripped = raw_line.strip()
                if not stripped:
                    continue
                if ":" not in raw_line:
                    raise ValueError("unparseable frontmatter line")
                key, raw_value = raw_line.split(":", 1)
                frontmatter[key.strip()] = _parse_scalar(raw_value)
            body = "".join(lines[index + 1 :])
            return frontmatter, body, True
        frontmatter_lines.append(lines[index])
    raise ValueError("unparseable frontmatter")


def render_note(frontmatter: dict[str, object], body: str, *, key_order: list[str] | None = None) -> str:
    if not frontmatter:
        return body
    ordered_keys = key_order or list(frontmatter.keys())
    seen: set[str] = set()
    lines = ["---"]
    for key in ordered_keys:
        if key in frontmatter:
            lines.append(f"{key}: {_render_scalar(frontmatter[key])}")
            seen.add(key)
    for key, value in frontmatter.items():
        if key not in seen:
            lines.append(f"{key}: {_render_scalar(value)}")
    lines.append("---")
    return "\n".join(lines) + "\n" + body


def read_source_note(path: Path) -> SourceNote:
    text = path.read_text(encoding="utf-8")
    frontmatter, body, _ = split_frontmatter(text)
    return SourceNote(path=path, filename=path.name, frontmatter=frontmatter, body=body)


def source_note_body_is_blank(note: SourceNote) -> bool:
    return note.body.strip() == ""


def write_source_note(
    path: Path,
    frontmatter: dict[str, object],
    body: str,
    *,
    writer: Callable[[Path, str], None],
) -> None:
    key_order = ["capture_id", "created_at", "ingest_attempts"]
    writer(path, render_note(frontmatter, body, key_order=key_order))


def append_jsonl_event(
    payload: dict[str, object],
    log_path: Path,
    *,
    timestamp: Callable[[], str] = utc_timestamp,
    fsync: Callable[[int], None] = os.fsync,
) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8", newline="") as handle:
        handle.write(json.dumps({"ts": timestamp(), **payload}, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()
        fsync(handle.fileno())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def raw_state_item(root: Path, raw_path: Path | object, frontmatter: dict[str, object] | None = None) -> dict[str, object]:
    artifact = raw_path if hasattr(raw_path, "path") and hasattr(raw_path, "frontmatter") else None
    if artifact is not None:
        raw_path = artifact.path
        frontmatter = artifact.frontmatter
    if not isinstance(raw_path, Path):
        raw_path = Path(raw_path)
    if frontmatter is None:
        frontmatter = {}
    source_id = frontmatter.get("source_id")
    if not isinstance(source_id, str) or not source_id:
        capture_id = frontmatter.get("capture_id")
        if isinstance(capture_id, str) and capture_id:
            source_id = stable_source_id("capture", capture_id)
    content_hash = sha256_file(raw_path)
    return {
        "item_id": source_id if source_id else f"sha256:{content_hash}",
        "source_id": source_id,
        "raw_path": normalize_repo_path(root, raw_path),
        "content_hash": content_hash,
    }


def latest_state_record(item_id: str, state_path: Path) -> dict[str, object] | None:
    if not state_path.exists():
        return None
    latest: dict[str, object] | None = None
    for line in state_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if payload.get("item_id") != item_id:
            continue
        if payload.get("event") in {"processed", "skipped", "updated", "failed", "deferred"}:
            latest = payload
    return latest


def state_item_seen(item_id: str, state_path: Path) -> bool:
    if not state_path.exists():
        return False
    for line in state_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if payload.get("item_id") == item_id:
            return True
    return False


def latest_ingest_event(*, capture_id: str, raw_path: str, log_path: Path) -> str | None:
    if not log_path.exists():
        return None
    latest_event: str | None = None
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if payload.get("capture_id") != capture_id or payload.get("raw_path") != raw_path:
            continue
        event = payload.get("event")
        if event in {"integrated", "integrate_failed", "integrate_deferred"}:
            latest_event = str(event)
    return latest_event


def has_logged_event(event: str, *, capture_id: str | None, filename: str | None, log_path: Path) -> bool:
    if not log_path.exists():
        return False
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if payload.get("event") != event:
            continue
        if capture_id is not None and payload.get("capture_id") != capture_id:
            continue
        if filename is not None and payload.get("filename") != filename:
            continue
        return True
    return False
