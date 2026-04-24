from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
import errno
import fcntl
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
import tempfile
from typing import Callable, Iterator, TextIO, TypedDict
import unicodedata
import uuid

try:
    from scripts import bootstrap_wiki as bw
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    import bootstrap_wiki as bw


ROOT = Path(__file__).resolve().parent.parent
RAW_ROOT = ROOT / "raw"
SOURCES_ROOT = ROOT / "sources"
CHAT_SOURCES_ROOT = SOURCES_ROOT / "chat"
WIKI_ROOT = ROOT / "wiki"
JSONL_LOG_PATH = ROOT / "log.jsonl"
DEFAULT_CAPTURE_ROOT = Path(
    "capture"
)

MARKER_PREFIX = "✓ "
_UNSAFE_SLUG_CHARS_RE = re.compile(r"[^a-z0-9\-]+")
_DASH_RUN_RE = re.compile(r"-{2,}")
_INDEX_ENTRY_RE = re.compile(r"^- \[\[(?P<slug>[^\]]+)\]\] — (?P<summary>.+)$")
_SOURCE_LINE_RE = re.compile(r"^- \[(?P<label>[^\]]+)\]\((?P<path>[^)]+)\)(?P<suffix>.*)$")
_CONNECTION_SLUG_RE = re.compile(r"\[\[(?P<slug>[^\]]+)\]\]")


def configure_workspace(root: Path, *, capture_root: Path | None = None) -> None:
    global ROOT, RAW_ROOT, SOURCES_ROOT, CHAT_SOURCES_ROOT, WIKI_ROOT, JSONL_LOG_PATH, DEFAULT_CAPTURE_ROOT

    ROOT = root
    RAW_ROOT = ROOT / "raw"
    SOURCES_ROOT = ROOT / "sources"
    CHAT_SOURCES_ROOT = SOURCES_ROOT / "chat"
    WIKI_ROOT = ROOT / "wiki"
    JSONL_LOG_PATH = ROOT / "log.jsonl"
    DEFAULT_CAPTURE_ROOT = capture_root or ROOT / "capture"
    bw.configure_workspace(root)


@contextmanager
def temporary_workspace(root: Path, *, capture_root: Path | None = None) -> Iterator[None]:
    original = (ROOT, RAW_ROOT, SOURCES_ROOT, CHAT_SOURCES_ROOT, WIKI_ROOT, JSONL_LOG_PATH, DEFAULT_CAPTURE_ROOT)
    configure_workspace(root, capture_root=capture_root)
    try:
        yield
    finally:
        (
            restored_root,
            restored_raw_root,
            restored_sources_root,
            restored_chat_sources_root,
            restored_wiki_root,
            restored_log_path,
            restored_capture_root,
        ) = original
        globals().update(
            {
                "ROOT": restored_root,
                "RAW_ROOT": restored_raw_root,
                "SOURCES_ROOT": restored_sources_root,
                "CHAT_SOURCES_ROOT": restored_chat_sources_root,
                "WIKI_ROOT": restored_wiki_root,
                "JSONL_LOG_PATH": restored_log_path,
                "DEFAULT_CAPTURE_ROOT": restored_capture_root,
            }
        )
        bw.configure_workspace(restored_root)


class ExportItem(TypedDict):
    capture_id: str
    raw_path: str


class ErrorItem(TypedDict):
    filename: str
    capture_id: str | None
    reason: str


class CaptureIngestResult(TypedDict):
    new_exports: list[ExportItem]
    errors: list[ErrorItem]


class IngestResult(TypedDict):
    integrated: list[ExportItem]
    skipped: list[ExportItem]
    failed: list[ExportItem]


class PipelineRunResult(TypedDict):
    capture_ingest: CaptureIngestResult
    wiki_ingest: IngestResult | None


@dataclass(frozen=True)
class RawCandidateSet:
    matching_valid: list[Path]
    matching_invalid: list[Path]
    unparseable_unknown: list[Path]


@dataclass(frozen=True)
class PipelineOptions:
    debug: bool = False
    dry_run: bool = False
    limit: int | None = None
    retry_failed: bool = False
    capture_root: Path = DEFAULT_CAPTURE_ROOT
    page_resynthesis_on_touch: bool = False


@dataclass(frozen=True)
class RouterDecision:
    action: str
    target_pages: list[str]
    new_page_signal: bool
    candidate_new_pages: list[str]
    contradiction_risk: str
    reorganization_risk: bool
    confidence: str
    reason: str


@dataclass
class MaintenanceOutcome:
    changed_slugs: list[str] = field(default_factory=list)
    router_decision: RouterDecision | None = None


@dataclass
class QueryWritebackResult:
    changed_slugs: list[str] = field(default_factory=list)
    source_path: Path | None = None
    router_decision: RouterDecision | None = None
    review_queued: bool = False
    superseded_source_paths: list[str] = field(default_factory=list)
    duplicate_of_source_path: str | None = None


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


def normalize_repo_path(path: str | Path) -> str:
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = ROOT / candidate

    root_abs = Path(os.path.abspath(ROOT))
    candidate_abs = Path(os.path.abspath(candidate))
    if Path(os.path.commonpath([root_abs, candidate_abs])) != root_abs:
        raise ValueError(f"path must stay within repo root: {path}")
    relative = Path(os.path.relpath(candidate_abs, root_abs))
    return PurePosixPath(relative.as_posix()).as_posix()


def clean_title_from_filename(filename: str) -> str:
    title = filename[:-3] if filename.endswith(".md") else filename
    if title.startswith(MARKER_PREFIX):
        title = title[len(MARKER_PREFIX):]
    return title


def is_placeholder_title(title: str) -> bool:
    return title.strip().lower() == "untitled"


def raw_file_slug(title: str) -> str:
    normalized = unicodedata.normalize("NFKD", title.strip().lower())
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    dashed = ascii_text.replace(" ", "-")
    sanitized = _UNSAFE_SLUG_CHARS_RE.sub("-", dashed)
    collapsed = _DASH_RUN_RE.sub("-", sanitized).strip("-")
    trimmed = collapsed[:80].rstrip("-")
    return trimmed or "untitled"


def stable_source_id(source_kind: str, identity: str) -> str:
    return f"{source_kind}:{identity}"


def debug_print(message: str, *, enabled: bool, stream: TextIO | None = None) -> None:
    if enabled:
        print(message, file=stream or sys.stderr)


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_prefix = f".{uuid.uuid5(uuid.NAMESPACE_URL, path.name).hex[:12]}."
    fd, tmp_name = tempfile.mkstemp(prefix=safe_prefix, suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


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


def write_source_note(path: Path, frontmatter: dict[str, object], body: str) -> None:
    key_order = ["capture_id", "created_at", "ingest_attempts"]
    atomic_write_text(path, render_note(frontmatter, body, key_order=key_order))


def append_jsonl_event(payload: dict[str, object], log_path: Path | None = None) -> None:
    effective_log_path = log_path or JSONL_LOG_PATH
    effective_log_path.parent.mkdir(parents=True, exist_ok=True)
    with effective_log_path.open("a", encoding="utf-8", newline="") as handle:
        handle.write(json.dumps({"ts": utc_timestamp(), **payload}, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _has_logged_event(event: str, *, capture_id: str | None, filename: str | None, log_path: Path) -> bool:
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


def discover_capture_candidates(capture_root: Path) -> list[Path]:
    candidates = [
        path
        for path in capture_root.iterdir()
        if path.is_file() and path.suffix == ".md" and not path.name.startswith(MARKER_PREFIX)
    ]
    return sorted(candidates, key=lambda path: path.stat().st_mtime)


def discover_processed_capture_candidates(capture_root: Path) -> list[Path]:
    candidates = [
        path
        for path in capture_root.iterdir()
        if path.is_file() and path.suffix == ".md" and path.name.startswith(MARKER_PREFIX)
    ]
    return sorted(candidates, key=lambda path: path.stat().st_mtime)


def discover_source_capture_id_counts(candidate_paths: list[Path]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for path in candidate_paths:
        try:
            note = read_source_note(path)
        except Exception:
            continue
        if note.capture_id is None:
            continue
        counts[note.capture_id] = counts.get(note.capture_id, 0) + 1
    return counts


def resolve_created_at(note: SourceNote) -> tuple[str, bool]:
    existing = note.frontmatter.get("created_at")
    if isinstance(existing, str) and existing:
        return existing, False
    stats = note.path.stat()
    birthtime = getattr(stats, "st_birthtime", 0)
    if birthtime:
        return utc_timestamp(float(birthtime)), False
    return utc_timestamp(stats.st_mtime), True


def render_raw_file(
    *,
    capture_id: str,
    title: str,
    created_at: str,
    source_file: str,
    body: str,
    integrated_at: str | None = None,
) -> str:
    external_url = bw.extract_first_url(body)
    frontmatter: dict[str, object] = {
        "capture_id": capture_id,
        "source_kind": "capture",
        "source_id": stable_source_id("capture", capture_id),
        "title": title,
        "created_at": created_at,
        "source_file": source_file,
    }
    if external_url:
        frontmatter["external_url"] = external_url
    if integrated_at:
        frontmatter["integrated_at"] = integrated_at
    return (
        render_note(
            frontmatter,
            f"# {title}\n\n{body}",
            key_order=[
                "capture_id",
                "source_kind",
                "source_id",
                "title",
                "created_at",
                "external_url",
                "source_file",
                "integrated_at",
            ],
        )
    )


def parse_raw_note(path: Path) -> tuple[dict[str, object], str]:
    text = path.read_text(encoding="utf-8")
    frontmatter, body, has_frontmatter = split_frontmatter(text)
    if not has_frontmatter:
        raise ValueError("raw file missing frontmatter")
    if not body.strip():
        raise ValueError("raw file body is empty")
    return frontmatter, body


def persist_chat_source_artifact(
    *,
    title: str,
    body: str,
    created_at: str,
    conversation_ref: str,
    external_url: str | None = None,
    extra_frontmatter: dict[str, object] | None = None,
) -> Path:
    CHAT_SOURCES_ROOT.mkdir(parents=True, exist_ok=True)
    identity = f"{created_at}:{title}:{body}"
    source_id = stable_source_id("chat", uuid.uuid5(uuid.NAMESPACE_URL, identity).hex)
    target = CHAT_SOURCES_ROOT / f"{raw_file_slug(title)}-{source_id.split(':', 1)[1]}.md"
    if target.exists():
        return target
    frontmatter: dict[str, object] = {
        "source_kind": "chat",
        "source_id": source_id,
        "title": title,
        "created_at": created_at,
        "provenance_pointer": conversation_ref,
    }
    if external_url:
        frontmatter["external_url"] = external_url
    if extra_frontmatter:
        frontmatter.update(extra_frontmatter)
    atomic_write_text(
        target,
        render_note(
            frontmatter,
            f"# {title}\n\n{body}",
            key_order=[
                "source_kind",
                "source_id",
                "title",
                "created_at",
                "external_url",
                "provenance_pointer",
                "target_page",
                "target_note",
                "fact_key",
                "replacement_intent",
            ],
        ),
    )
    return target


def _resolve_repo_relative_path(path: str) -> Path:
    normalized = path[3:] if path.startswith("../") else path
    resolved = (ROOT / normalized).resolve()
    root_resolved = ROOT.resolve()
    if Path(os.path.commonpath([root_resolved, resolved])) != root_resolved:
        raise ValueError(f"path must stay within repo root: {path}")
    return resolved


def _source_excerpt(source: bw.SourceRecord) -> str:
    excerpt = bw.compact_source_text(source, limit=220).strip()
    return re.sub(r"\s+", " ", excerpt)


def _remove_source_from_page(page: bw.Page, source_path: str) -> None:
    source = page.sources.pop(source_path, None)
    if source is None:
        return
    excerpt = _source_excerpt(source)
    if excerpt:
        page.notes = [note for note in page.notes if note != excerpt]


def _append_review_backlog_item(
    *,
    reason: str,
    affected_pages: list[str],
    source_paths: list[str],
    next_action: str,
    status: str = "open",
) -> None:
    review_path = WIKI_ROOT / "review.md"
    existing = review_path.read_text(encoding="utf-8") if review_path.exists() else "# Wiki Review Backlog\n"
    page_links = ", ".join(f"[[{slug}]]" for slug in bw.ordered_unique(affected_pages)) or "None"
    source_links = ", ".join(f"[{path}]({path})" for path in bw.ordered_unique(source_paths)) or "None"
    entry = "\n".join(
        [
            f"## [{_today_date()}] {status} | {reason}",
            f"- Affected pages: {page_links}",
            f"- Source artifacts: {source_links}",
            f"- Next action: {next_action}",
        ]
    )
    atomic_write_text(review_path, existing.rstrip() + "\n\n" + entry + "\n")


def _matching_chat_sources_for_fact(page: bw.Page, *, fact_key: str) -> list[tuple[str, dict[str, object]]]:
    matches: list[tuple[str, dict[str, object]]] = []
    for source_path, source in page.sources.items():
        if source.source_kind != "chat":
            continue
        try:
            artifact_frontmatter, _ = parse_raw_note(_resolve_repo_relative_path(source_path))
        except Exception:
            continue
        if artifact_frontmatter.get("fact_key") != fact_key:
            continue
        matches.append((source_path, artifact_frontmatter))
    return matches


def validate_adoptable_raw(path: Path, capture_id: str) -> None:
    classification = classify_raw_candidate(path, capture_id)
    if classification != "matching_valid":
        raise ValueError(f"raw file is not adoption-valid for capture_id {capture_id}: {path}")


def compute_raw_target(title: str, capture_id: str) -> Path:
    return RAW_ROOT / f"{raw_file_slug(title)}-{capture_id}.md"


def classify_raw_candidate(path: Path, capture_id: str) -> str | None:
    text = path.read_text(encoding="utf-8")
    try:
        frontmatter, body, has_frontmatter = split_frontmatter(text)
    except ValueError:
        return "unparseable_unknown"

    if not has_frontmatter:
        return "unparseable_unknown"

    candidate_capture_id = frontmatter.get("capture_id")
    if not isinstance(candidate_capture_id, str) or not candidate_capture_id:
        return "unparseable_unknown"
    if candidate_capture_id != capture_id:
        return None
    if not body.strip():
        return "matching_invalid"
    return "matching_valid"


def discover_raw_candidates(capture_id: str) -> RawCandidateSet:
    matching_valid: list[Path] = []
    matching_invalid: list[Path] = []
    unparseable_unknown: list[Path] = []

    if not RAW_ROOT.exists():
        return RawCandidateSet(matching_valid, matching_invalid, unparseable_unknown)

    for path in sorted(RAW_ROOT.rglob("*.md")):
        if path.is_symlink() or not path.is_file():
            continue
        classification = classify_raw_candidate(path, capture_id)
        if classification == "matching_valid":
            matching_valid.append(path)
        elif classification == "matching_invalid":
            matching_invalid.append(path)
        elif classification == "unparseable_unknown":
            unparseable_unknown.append(path)

    return RawCandidateSet(matching_valid, matching_invalid, unparseable_unknown)


def resolve_raw_path_for_capture(*, title: str, capture_id: str) -> tuple[Path, str, int]:
    candidates = discover_raw_candidates(capture_id)
    valid_count = len(candidates.matching_valid)
    invalid_count = len(candidates.matching_invalid)
    target_path = compute_raw_target(title, capture_id)

    if valid_count == 1 and invalid_count == 0:
        return candidates.matching_valid[0], "adopted", 1
    if valid_count > 1 or (valid_count == 1 and invalid_count > 0):
        raise ValueError("raw_identity_ambiguous")
    if valid_count == 0 and invalid_count > 0:
        raise ValueError("raw_invalid_existing")
    if target_path.exists():
        raise ValueError("raw_create_failed")
    return target_path, "created", 0


def increment_ingest_attempts(note: SourceNote) -> int:
    updated = dict(note.frontmatter)
    updated["ingest_attempts"] = note.ingest_attempts + 1
    write_source_note(note.path, updated, note.body)
    return int(updated["ingest_attempts"])


def _record_retry_gated_export_failure(
    *,
    note: SourceNote,
    capture_id: str,
    error: str,
    failure_class: str,
    log_path: Path | None,
) -> None:
    attempts = increment_ingest_attempts(note)
    append_jsonl_event(
        {
            "event": "export_failed",
            "capture_id": capture_id,
            "source_filename": note.filename,
            "error": error,
            "failure_class": failure_class,
            "counted_against_retry_gate": True,
            "ingest_attempts": attempts,
        },
        log_path=log_path,
    )


def inject_capture_id(note: SourceNote) -> SourceNote:
    updated = dict(note.frontmatter)
    updated["capture_id"] = str(uuid.uuid4())
    write_source_note(note.path, updated, note.body)
    return read_source_note(note.path)


def rename_processed(path: Path) -> Path:
    target = path.with_name(f"{MARKER_PREFIX}{path.name}")
    if target.exists():
        raise FileExistsError(f"processed target already exists: {target.name}")
    path.rename(target)
    return target


def capture_ingest(
    *,
    capture_root: Path = DEFAULT_CAPTURE_ROOT,
    debug: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
    retry_failed: bool = False,
    log_path: Path | None = None,
    debug_stream: TextIO | None = None,
) -> CaptureIngestResult:
    result: CaptureIngestResult = {"new_exports": [], "errors": []}
    processed = 0
    candidate_paths = discover_capture_candidates(capture_root)
    processed_candidate_paths = discover_processed_capture_candidates(capture_root)
    source_capture_id_counts = discover_source_capture_id_counts(candidate_paths)

    for path in candidate_paths:
        if limit is not None and processed >= limit:
            break

        try:
            note = read_source_note(path)
        except Exception as exc:
            result["errors"].append({"filename": path.name, "capture_id": None, "reason": f"read_failed: {exc}"})
            continue

        if note.ingest_attempts >= 3 and not retry_failed:
            capture_id = note.capture_id
            if capture_id and not _has_logged_event(
                "skipped_over_threshold", capture_id=capture_id, filename=note.filename, log_path=log_path or JSONL_LOG_PATH
            ):
                append_jsonl_event(
                    {
                        "event": "skipped_over_threshold",
                        "capture_id": capture_id,
                        "filename": note.filename,
                        "ingest_attempts": note.ingest_attempts,
                    },
                    log_path=log_path or JSONL_LOG_PATH,
                )
            continue

        title = clean_title_from_filename(note.filename)

        if source_note_body_is_blank(note) and is_placeholder_title(title):
            if dry_run:
                debug_print(f"Would delete empty note {note.filename}", enabled=debug, stream=debug_stream)
                processed += 1
                continue
            note.path.unlink()
            payload: dict[str, object] = {
                "event": "empty_deleted",
                "filename": note.filename,
                "reason": "empty_after_trim",
            }
            if note.capture_id:
                payload["capture_id"] = note.capture_id
            append_jsonl_event(payload, log_path=log_path)
            processed += 1
            continue

        if note.capture_id is None:
            if dry_run:
                debug_print(f"Would inject capture_id into {note.filename}", enabled=debug, stream=debug_stream)
                processed += 1
                continue
            try:
                note = inject_capture_id(note)
            except Exception as exc:
                append_jsonl_event(
                    {"event": "injection_failed", "filename": note.filename, "error": str(exc)},
                    log_path=log_path,
                )
                result["errors"].append(
                    {"filename": note.filename, "capture_id": None, "reason": f"injection_failed: {exc}"}
                )
                continue

        capture_id = note.capture_id
        assert capture_id is not None
        append_jsonl_event({"event": "discovered", "capture_id": capture_id, "filename": note.filename}, log_path=log_path)
        created_at, used_mtime_fallback = resolve_created_at(note)
        if used_mtime_fallback:
            append_jsonl_event(
                {"event": "created_at_mtime_fallback", "capture_id": capture_id, "filename": note.filename},
                log_path=log_path,
            )

        if source_capture_id_counts.get(capture_id, 0) > 1:
            try:
                _record_retry_gated_export_failure(
                    note=note,
                    capture_id=capture_id,
                    error="source identity ambiguous",
                    failure_class="source_identity_ambiguous",
                    log_path=log_path,
                )
            except Exception:
                pass
            result["errors"].append(
                {"filename": note.filename, "capture_id": capture_id, "reason": "source_identity_ambiguous"}
            )
            processed += 1
            continue

        try:
            raw_target, export_mode, candidate_count = resolve_raw_path_for_capture(title=title, capture_id=capture_id)
        except Exception as exc:
            try:
                _record_retry_gated_export_failure(
                    note=note,
                    capture_id=capture_id,
                    error=str(exc),
                    failure_class=str(exc),
                    log_path=log_path,
                )
            except Exception:
                pass
            result["errors"].append({"filename": note.filename, "capture_id": capture_id, "reason": str(exc)})
            processed += 1
            continue

        raw_repo_path = normalize_repo_path(raw_target)
        export_item: ExportItem = {"capture_id": capture_id, "raw_path": raw_repo_path}

        if dry_run:
            debug_print(f"Would export {note.filename} -> {raw_repo_path}", enabled=debug, stream=debug_stream)
            processed += 1
            continue

        try:
            if export_mode == "adopted":
                validate_adoptable_raw(raw_target, capture_id)
            else:
                if raw_target.exists():
                    raise ValueError("raw_create_failed")
                atomic_write_text(
                    raw_target,
                    render_raw_file(
                        capture_id=capture_id,
                        title=title,
                        created_at=created_at,
                        source_file=title + ".md",
                        body=note.body,
                    ),
                )
            append_jsonl_event(
                {
                    "event": "exported_to_raw",
                    "capture_id": capture_id,
                    "raw_path": raw_repo_path,
                    "mode": export_mode,
                    "candidate_count": candidate_count,
                },
                log_path=log_path,
            )
        except Exception as exc:
            failure_class = "raw_create_failed" if export_mode == "created" else str(exc)
            try:
                _record_retry_gated_export_failure(
                    note=note,
                    capture_id=capture_id,
                    error=str(exc),
                    failure_class=failure_class,
                    log_path=log_path,
                )
            except Exception:
                pass
            result["errors"].append({"filename": note.filename, "capture_id": capture_id, "reason": failure_class})
            processed += 1
            continue

        try:
            renamed = rename_processed(note.path)
            append_jsonl_event(
                {"event": "marked_processed", "capture_id": capture_id, "filename": renamed.name},
                log_path=log_path,
            )
            result["new_exports"].append(export_item)
        except Exception as exc:
            try:
                _record_retry_gated_export_failure(
                    note=note,
                    capture_id=capture_id,
                    error=str(exc),
                    failure_class="source_rename_failed",
                    log_path=log_path,
                )
            except Exception:
                pass
            result["errors"].append({"filename": note.filename, "capture_id": capture_id, "reason": "source_rename_failed"})
        processed += 1

    if not dry_run:
        for path in processed_candidate_paths:
            try:
                note = read_source_note(path)
            except Exception:
                continue
            capture_id = note.capture_id
            if capture_id is None:
                continue
            if discover_raw_candidates(capture_id).matching_valid:
                continue
            append_jsonl_event(
                {
                    "event": "export_failed",
                    "capture_id": capture_id,
                    "source_filename": note.filename,
                    "error": "processed note has no adoption-valid raw",
                    "failure_class": "processed_without_raw",
                    "counted_against_retry_gate": False,
                },
                log_path=log_path,
            )

    return result


def _normalize_ingest_item(item: ExportItem | dict[str, object]) -> ExportItem:
    if not isinstance(item, dict):
        raise ValueError("ingest item must be a mapping")
    capture_id = item.get("capture_id")
    raw_path = item.get("raw_path")
    if not isinstance(capture_id, str) or not capture_id:
        raise ValueError("ingest item capture_id must be a non-empty string")
    if not isinstance(raw_path, str) or not raw_path:
        raise ValueError("ingest item raw_path must be a non-empty string")
    return {"capture_id": capture_id, "raw_path": normalize_repo_path(raw_path)}


def validate_ingest_inputs(items: list[ExportItem | dict[str, object]]) -> list[ExportItem]:
    validated: list[ExportItem] = []
    for item in items:
        normalized = _normalize_ingest_item(item)
        raw_abspath = ROOT / normalized["raw_path"]
        if not raw_abspath.exists():
            raise ValueError(f"raw note does not exist for capture_id {normalized['capture_id']}: {normalized['raw_path']}")
        frontmatter, _ = parse_raw_note(raw_abspath)
        if frontmatter.get("capture_id") != normalized["capture_id"]:
            raise ValueError(
                f"raw note frontmatter capture_id mismatch for capture_id {normalized['capture_id']}: {normalized['raw_path']}"
            )
        if frontmatter.get("source_kind") != "capture":
            raise ValueError(f"raw note missing capture source_kind for capture_id {normalized['capture_id']}: {normalized['raw_path']}")
        if frontmatter.get("source_id") != stable_source_id("capture", normalized["capture_id"]):
            raise ValueError(f"raw note source_id mismatch for capture_id {normalized['capture_id']}: {normalized['raw_path']}")
        validated.append(normalized)
    return validated


def _today_date() -> str:
    return datetime.now().date().isoformat()


def _read_raw_note(path: Path) -> tuple[dict[str, object], str, str]:
    frontmatter, body = parse_raw_note(path)
    title = frontmatter.get("title")
    if not isinstance(title, str) or not title:
        raise ValueError(f"raw note missing title frontmatter: {path}")
    synthetic_heading = f"# {title}\n\n"
    content_body = body[len(synthetic_heading) :] if body.startswith(synthetic_heading) else body
    return frontmatter, title, content_body


def _rewrite_raw_integrated_at(path: Path, integrated_at: str) -> None:
    frontmatter, body = parse_raw_note(path)
    frontmatter["integrated_at"] = integrated_at
    atomic_write_text(
        path,
        render_note(
            frontmatter,
            body,
            key_order=[
                "capture_id",
                "source_kind",
                "source_id",
                "title",
                "created_at",
                "external_url",
                "source_file",
                "integrated_at",
            ],
        ),
    )


def _derive_path_topics(path: Path) -> list[str]:
    path_abs = Path(os.path.abspath(path))
    raw_root_abs = Path(os.path.abspath(RAW_ROOT))
    if Path(os.path.commonpath([raw_root_abs, path_abs])) == raw_root_abs:
        relative_parts = Path(os.path.relpath(path_abs, raw_root_abs)).parts[:-1]
    else:
        relative_parts = path.parts[:-1]
    topics: list[str] = []
    for component in reversed(relative_parts):
        topic = bw.clean_component(component)
        if topic and topic not in topics:
            topics.append(topic)
        if len(topics) >= 3:
            break
    return topics


def _build_default_page_assignments(title: str, body: str, raw_abspath: Path) -> list[tuple[str, str]]:
    url = bw.extract_first_url(body)
    path_topics = _derive_path_topics(raw_abspath)
    title_slug = bw.clean_component(title) or bw.slugify(title)
    assignments: list[tuple[str, str]] = []
    seen: set[str] = set()

    if bw.should_fold_note_into_parent(title, body, url) and path_topics:
        return [(path_topics[0], "folder")]

    if title_slug and title_slug not in {"new-note", "untitled"}:
        assignments.append((title_slug, "title"))
        seen.add(title_slug)

    for topic in path_topics[:2]:
        if topic not in seen:
            assignments.append((topic, "folder"))
            seen.add(topic)

    if not assignments:
        return [("uncategorized-captures", "folder")]
    return assignments


def _source_record_from_artifact(frontmatter: dict[str, object], title: str, body: str, source_path: Path) -> bw.SourceRecord:
    url = frontmatter.get("external_url")
    if not isinstance(url, str) or not url:
        url = bw.extract_first_url(body)
    fetched_summary = None
    source_status = "local_only"
    if url:
        if bw.is_google_search_url(url):
            source_status = "fetch_skipped"
        else:
            fetch_result = bw.fetch_url_summary(url)
            fetched_summary = fetch_result.summary
            source_status = fetch_result.status
    return bw.prepare_source_record(
        source_label=title,
        source_path="../" + normalize_repo_path(source_path),
        source_status=source_status,
        raw_content=body,
        fetched_summary=fetched_summary,
        detected_url=url,
        source_kind=str(frontmatter.get("source_kind", "capture")),
        source_id=str(frontmatter["source_id"]) if frontmatter.get("source_id") is not None else None,
        created_at=str(frontmatter["created_at"]) if frontmatter.get("created_at") is not None else None,
        title=str(frontmatter.get("title", title)),
        external_url=url,
        provenance_pointer=(
            str(frontmatter["provenance_pointer"]) if frontmatter.get("provenance_pointer") is not None else None
        ),
    )


def _resolve_synthesis_config() -> tuple[str | None, str]:
    return os.environ.get("GEMINI_API_KEY"), os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")


def _parse_existing_note_snippets(note_lines: list[str]) -> list[str]:
    return bw.parse_note_snippets(note_lines)


def _parse_source_line(line: str, retained_evidence: str) -> bw.SourceRecord | None:
    return bw.parse_source_line(line, retained_evidence)


def _parse_connection_slugs(connection_lines: list[str]) -> list[str]:
    return bw.extract_connection_slugs(connection_lines)


def _read_wiki_page(slug: str, *, original_title: str, seed_kind: str) -> bw.Page:
    page_path = WIKI_ROOT / f"{slug}.md"
    if page_path.exists():
        return bw.parsed_page_to_page(bw.parse_page_file(page_path))
    return bw.Page(
        slug=slug,
        title=bw.page_title(slug),
        page_type=bw.classify_page(slug, original_title, seed_kind),
        summary_hint=original_title,
    )


def _validate_router_decision(decision: RouterDecision) -> RouterDecision:
    allowed_actions = {"ignore", "light_update", "heavy_update", "queue_review"}
    allowed_risk = {"low", "medium", "high"}
    allowed_confidence = {"low", "medium", "high"}
    if decision.action not in allowed_actions:
        raise ValueError(f"invalid router action: {decision.action}")
    if decision.contradiction_risk not in allowed_risk:
        raise ValueError(f"invalid contradiction risk: {decision.contradiction_risk}")
    if decision.confidence not in allowed_confidence:
        raise ValueError(f"invalid router confidence: {decision.confidence}")
    if not decision.reason.strip():
        raise ValueError("router reason must be non-empty")
    if any(not slug.strip() for slug in decision.target_pages):
        raise ValueError("router target pages must be non-empty")
    return decision


def _route_source_update(
    *,
    title: str,
    body: str,
    page_assignments: list[tuple[str, str]],
) -> RouterDecision:
    target_pages = [slug for slug, _seed_kind in page_assignments]
    candidate_new_pages: list[str] = []
    reorganization_risk = False
    existing_atomic_targets = 0
    for slug, seed_kind in page_assignments:
        page = _read_wiki_page(slug, original_title=title, seed_kind=seed_kind)
        if (WIKI_ROOT / f"{slug}.md").exists():
            if bw.page_shape(page) == bw.PAGE_SHAPE_TOPIC:
                reorganization_risk = True
            else:
                existing_atomic_targets += 1
        else:
            candidate_new_pages.append(slug)
    new_page_signal = bool(candidate_new_pages)
    contradiction_risk = "low"
    if len(target_pages) == 1 and existing_atomic_targets == 1 and not new_page_signal and not reorganization_risk:
        action = "light_update"
        confidence = "high"
        reason = "Single existing atomic page can absorb a bounded update."
    elif not body.strip() and bw.slugify(title) in {"", "untitled", "new-note"}:
        action = "ignore"
        confidence = "medium"
        reason = "Source has no durable title or body signal after normalization."
    elif reorganization_risk or len(target_pages) > 2 or new_page_signal:
        action = "heavy_update"
        confidence = "medium"
        reason = "Source impacts new pages or pages that may need structural reorganization."
    else:
        action = "light_update"
        confidence = "medium"
        reason = "Source touches a small known page set and can be merged deterministically."
    return _validate_router_decision(
        RouterDecision(
            action=action,
            target_pages=target_pages,
            new_page_signal=new_page_signal,
            candidate_new_pages=candidate_new_pages,
            contradiction_risk=contradiction_risk,
            reorganization_risk=reorganization_risk,
            confidence=confidence,
            reason=reason,
        )
    )


def _build_bootstrap_page_for_touch(
    *,
    slug: str,
    seed_kind: str,
    original_title: str,
    parsed_page: dict[str, object],
    new_source_record: bw.SourceRecord,
    new_note_snippet: str,
    related_slugs: list[str],
) -> bw.Page:
    page = bw.Page(
        slug=slug,
        title=str(parsed_page["title"] or bw.page_title(slug)),
        page_type=bw.classify_page(slug, original_title, seed_kind),
        summary_hint=original_title,
    )
    page.seed_kinds.add(seed_kind)

    existing_note_snippets = _parse_existing_note_snippets(list(parsed_page["notes"]))
    for note in existing_note_snippets:
        if note not in page.notes:
            page.notes.append(note)
    normalized_new_note = bw.strip_markdown(new_note_snippet.lstrip("- ").strip()).strip()
    if normalized_new_note and normalized_new_note not in page.notes:
        page.notes.append(normalized_new_note)

    retained_evidence = "\n".join(f"- {note}" for note in page.notes)
    for line in list(parsed_page["sources"]):
        source_record = _parse_source_line(line, retained_evidence)
        if source_record is not None:
            page.sources[source_record.path] = source_record
    page.sources[new_source_record.path] = new_source_record

    merged_connection_slugs = _parse_connection_slugs(list(parsed_page["connections"]))
    for other_slug in related_slugs:
        if other_slug not in merged_connection_slugs:
            merged_connection_slugs.append(other_slug)
    for other_slug in merged_connection_slugs:
        if other_slug != slug:
            page.connections[other_slug] += 1

    return page


def _parse_existing_page(page_path: Path) -> dict[str, object]:
    if not page_path.exists():
        return {"title": None, "summary": "", "notes": [], "connections": [], "sources": []}

    parsed = bw.parse_page_file(page_path)
    return {
        "title": parsed.title,
        "summary": "\n".join(parsed.summary_lines).strip(),
        "notes": list(parsed.note_lines),
        "connections": [f"- [[{slug}]]" for slug in parsed.connection_slugs],
        "sources": bw.render_source_lines(bw.parsed_page_to_page(parsed)),
    }


def _render_merged_page(
    *,
    page_title: str,
    summary: str,
    note_lines: list[str],
    connection_lines: list[str],
    source_lines: list[str],
) -> str:
    return "\n".join(
        [
            f"# {page_title}",
            "",
            summary,
            "",
            "## Notes",
            "",
            "\n".join(note_lines) if note_lines else "- No notes yet.",
            "",
            "## Connections",
            "",
            "\n".join(connection_lines),
            "",
            "## Sources",
            "",
            "\n".join(source_lines),
            "",
        ]
    )


def _count_page_sources(page_path: Path) -> int:
    if not page_path.exists():
        return 0
    return len(bw.parse_page_file(page_path).sources)


def _rewrite_index(changed_pages: list[tuple[str, str]]) -> None:
    index_path = WIKI_ROOT / "index.md"
    page_types = bw.load_existing_page_types(index_path)
    pages: dict[str, bw.Page] = {}
    for page_path in sorted(WIKI_ROOT.glob("*.md")):
        if page_path.stem in {"index", "log", "review", Path(bw.CATALOG_PATH).stem}:
            continue
        parsed = bw.parse_page_file(page_path, page_type=page_types.get(page_path.stem))
        pages[page_path.stem] = bw.parsed_page_to_page(parsed)
    counts = bw.inbound_link_counts(pages)
    grouped: dict[str, list[bw.Page]] = {section: [] for section in bw.INDEX_SECTION_ORDER}
    forced_slugs = {slug for slug, _page_type in changed_pages}
    for page in pages.values():
        if page.slug in forced_slugs or bw.page_shape(page) == bw.PAGE_SHAPE_TOPIC or counts.get(page.slug, 0) >= bw.HIGH_SIGNAL_INBOUND_THRESHOLD:
            grouped.setdefault(page.page_type, []).append(page)

    lines = [
        "# Wiki Index",
        "",
        f"_Last updated: {_today_date()} — {len(pages)} pages_",
        "_Navigation only: topic pages plus high-signal atomic pages. Use [[catalog]] for exhaustive lookup._",
        "",
    ]
    for section in bw.INDEX_SECTION_ORDER:
        lines.append(f"## {section}")
        section_pages = sorted(grouped.get(section, []), key=lambda page: page.title.lower())
        if not section_pages:
            lines.append("- None yet.")
            lines.append("")
            continue
        for page in section_pages:
            lines.append(f"- [[{page.slug}]] — {bw.page_index_summary(page)}")
        lines.append("")
    atomic_write_text(index_path, "\n".join(lines))


def _append_wiki_ingest_log(title: str, *, router_decision: RouterDecision | None = None) -> None:
    log_path = WIKI_ROOT / "log.md"
    existing = log_path.read_text(encoding="utf-8") if log_path.exists() else "# Wiki Log\n"
    entry = f'## [{_today_date()}] ingest | Capture: "{title}"'
    if router_decision is not None:
        entry += f" | Router: {router_decision.action}"
    atomic_write_text(log_path, existing.rstrip() + "\n\n" + entry + "\n")


def query_writeback_chat_fact(
    *,
    page_title: str,
    note: str,
    related_pages: list[str],
    created_at: str,
    conversation_ref: str,
    fact_key: str,
    replacement_intent: bool = False,
    external_url: str | None = None,
) -> QueryWritebackResult:
    target_slug = bw.slugify(page_title)
    if not target_slug:
        raise ValueError("page_title must produce a valid slug")
    normalized_note = note.strip()
    if not normalized_note:
        raise ValueError("note must be non-empty")
    normalized_related_pages = bw.ordered_unique([bw.slugify(value) for value in related_pages if bw.slugify(value)])
    source_path = persist_chat_source_artifact(
        title=page_title,
        body=normalized_note,
        created_at=created_at,
        conversation_ref=conversation_ref,
        external_url=external_url,
        extra_frontmatter={
            "target_page": target_slug,
            "target_note": normalized_note,
            "fact_key": fact_key,
            "replacement_intent": replacement_intent,
        },
    )
    frontmatter, body = parse_raw_note(source_path)
    source_record = _source_record_from_artifact(frontmatter, page_title, body, source_path)
    router_decision = _route_source_update(
        title=page_title,
        body=normalized_note,
        page_assignments=[(target_slug, "title"), *[(slug, "query") for slug in normalized_related_pages]],
    )

    loaded_pages: dict[str, bw.Page] = {}

    def load_page(slug: str, seed_kind: str) -> bw.Page:
        if slug in loaded_pages:
            return loaded_pages[slug]
        page = _read_wiki_page(slug, original_title=page_title, seed_kind=seed_kind)
        loaded_pages[slug] = page
        return page

    target_page = load_page(target_slug, "title")
    target_page.shape = bw.PAGE_SHAPE_ATOMIC
    target_page.page_type = bw.classify_page(target_slug, page_title, "title")

    for related_slug in normalized_related_pages:
        related_page = load_page(related_slug, "query")
        if bw.page_shape(related_page) != bw.PAGE_SHAPE_TOPIC:
            related_page.shape = bw.PAGE_SHAPE_ATOMIC
        bw.connect_pages(loaded_pages, target_slug, related_slug)

    duplicate_of_source_path: str | None = None
    superseded_source_paths: list[str] = []
    review_queued = False
    matching_sources = _matching_chat_sources_for_fact(target_page, fact_key=fact_key)
    for existing_source_path, artifact_frontmatter in matching_sources:
        existing_note = str(artifact_frontmatter.get("target_note", "")).strip()
        if existing_note == normalized_note:
            duplicate_of_source_path = existing_source_path
            break

    if duplicate_of_source_path is None:
        if replacement_intent:
            for existing_source_path, artifact_frontmatter in matching_sources:
                existing_note = str(artifact_frontmatter.get("target_note", "")).strip()
                if existing_note == normalized_note:
                    continue
                _remove_source_from_page(target_page, existing_source_path)
                superseded_source_paths.append(existing_source_path)
        else:
            conflicting_notes = sorted(
                {
                    str(artifact_frontmatter.get("target_note", "")).strip()
                    for _existing_source_path, artifact_frontmatter in matching_sources
                    if str(artifact_frontmatter.get("target_note", "")).strip()
                    and str(artifact_frontmatter.get("target_note", "")).strip() != normalized_note
                }
            )
            if conflicting_notes:
                question = (
                    f"Conflicting chat-derived fact for {fact_key}: current notes include "
                    + "; ".join(f'"{value}"' for value in conflicting_notes + [normalized_note])
                    + "."
                )
                if question not in target_page.open_questions:
                    target_page.open_questions.append(question)
                _append_review_backlog_item(
                    reason=f"contradiction | {fact_key}",
                    affected_pages=[target_slug],
                    source_paths=[*superseded_source_paths, *[path for path, _ in matching_sources], "../" + normalize_repo_path(source_path)],
                    next_action="Confirm which chat-derived fact should remain current on the page.",
                )
                review_queued = True

        bw.add_source_to_page(target_page, source_record, seed_kind="query")

    changed_slugs: list[str] = []
    allow_missing_outbound = bool(normalized_related_pages) or bool(target_page.connections)
    issues = bw.validate_page(target_page, allow_missing_outbound=allow_missing_outbound)
    if issues:
        _append_review_backlog_item(
            reason="invalid query writeback",
            affected_pages=[target_slug],
            source_paths=["../" + normalize_repo_path(source_path)],
            next_action=f"Repair page shape before applying query writeback: {', '.join(issues)}",
        )
        raise ValueError(f"invalid page '{target_slug}': {', '.join(issues)}")

    pages_to_write = {target_slug: target_page}
    for related_slug in normalized_related_pages:
        pages_to_write[related_slug] = loaded_pages[related_slug]

    for slug, page in pages_to_write.items():
        page_issues = bw.validate_page(
            page,
            allow_missing_outbound=slug != target_slug or bool(page.connections),
        )
        if page_issues:
            raise ValueError(f"invalid page '{slug}': {', '.join(page_issues)}")
        atomic_write_text(WIKI_ROOT / f"{slug}.md", bw.render_page(page))
        changed_slugs.append(slug)

    if duplicate_of_source_path is None:
        _rewrite_index([(slug, pages_to_write[slug].page_type) for slug in changed_slugs])
        summary = f'writeback | "{page_title}" | Router: {router_decision.action}'
        if superseded_source_paths:
            summary += " | superseded prior chat fact"
        if review_queued:
            summary += " | review queued"
        bw.append_wiki_query_log(summary)

    return QueryWritebackResult(
        changed_slugs=changed_slugs,
        source_path=source_path,
        router_decision=router_decision,
        review_queued=review_queued,
        superseded_source_paths=superseded_source_paths,
        duplicate_of_source_path=duplicate_of_source_path,
    )


def _upsert_wiki_pages_for_note(
    *,
    frontmatter: dict[str, object],
    title: str,
    body: str,
    raw_path: Path,
    page_resynthesis_on_touch: bool = False,
) -> MaintenanceOutcome:
    _ = page_resynthesis_on_touch
    source_record = _source_record_from_artifact(frontmatter, title, body, raw_path)
    page_assignments = _build_default_page_assignments(title, body, raw_path)
    router_decision = _route_source_update(title=title, body=body, page_assignments=page_assignments)
    if router_decision.action == "ignore":
        return MaintenanceOutcome(router_decision=router_decision)
    loaded_pages: dict[str, bw.Page] = {}

    def load_page(slug: str, seed_kind: str) -> bw.Page:
        if slug in loaded_pages:
            return loaded_pages[slug]
        page = _read_wiki_page(slug, original_title=title, seed_kind=seed_kind)
        loaded_pages[slug] = page
        return page

    resolved_assignments: list[tuple[str, str, str | None]] = []
    for slug, seed_kind in page_assignments:
        page = load_page(slug, seed_kind)
        if bw.page_shape(page) == bw.PAGE_SHAPE_TOPIC:
            satellite_slug = bw.clean_component(title) or bw.slugify(title) or f"{slug}-note"
            if satellite_slug == slug:
                satellite_slug = f"{slug}-note"
            satellite_page = load_page(satellite_slug, seed_kind)
            satellite_page.topic_parent = slug
            resolved_assignments.append((satellite_slug, seed_kind, slug))
        else:
            resolved_assignments.append((slug, seed_kind, None))

    if not resolved_assignments:
        fallback_slug = bw.clean_component(title) or bw.slugify(title) or "uncategorized-captures"
        resolved_assignments.append((fallback_slug, "title", None))

    resolved_slugs = [slug for slug, _seed_kind, _parent_slug in resolved_assignments]
    for slug, seed_kind, parent_slug in resolved_assignments:
        page = load_page(slug, seed_kind)
        page.shape = bw.PAGE_SHAPE_ATOMIC
        page.page_type = bw.classify_page(slug, title, seed_kind)
        page.seed_kinds.add(seed_kind)
        bw.add_source_to_page(page, source_record, seed_kind)
        if parent_slug:
            parent_page = load_page(parent_slug, seed_kind)
            parent_page.shape = bw.PAGE_SHAPE_TOPIC
            bw.connect_pages(loaded_pages, parent_slug, slug)

    for slug in resolved_slugs:
        for other_slug in resolved_slugs:
            if other_slug != slug:
                bw.connect_pages(loaded_pages, slug, other_slug)

    api_key, model = _resolve_synthesis_config()
    if api_key:
        for slug in bw.ordered_unique(resolved_slugs):
            page = loaded_pages.get(slug)
            if page is None or bw.page_shape(page) == bw.PAGE_SHAPE_TOPIC or not page.sources:
                continue
            try:
                split_decision = bw.analyze_page_for_atomic_split(page, api_key, model)
            except Exception as exc:
                print(f"Split analysis skipped for '{slug}': {exc}", file=sys.stderr)
                continue
            bw.apply_split_decision(
                loaded_pages,
                slug,
                split_decision,
                seed_kind="ingest",
                allow_partial_source_coverage=True,
            )

    bw.prune_generic_media_links(loaded_pages)
    bw.ensure_meaningful_connections(loaded_pages)
    bw.finalize_page_shapes(loaded_pages)
    changed_slugs: list[str] = []
    for slug, page in loaded_pages.items():
        allow_missing_outbound = bool(page.topic_parent) or len(loaded_pages) == 1
        issues = bw.validate_page(page, allow_missing_outbound=allow_missing_outbound)
        if issues:
            raise ValueError(f"invalid page '{slug}': {', '.join(issues)}")
        atomic_write_text(WIKI_ROOT / f"{slug}.md", bw.render_page(page))
        changed_slugs.append(slug)

    _rewrite_index([(slug, loaded_pages[slug].page_type) for slug in changed_slugs])
    _append_wiki_ingest_log(title, router_decision=router_decision)
    return MaintenanceOutcome(changed_slugs=changed_slugs, router_decision=router_decision)


def _default_integration_handler(
    capture_id: str,
    raw_path: Path,
    *,
    page_resynthesis_on_touch: bool = False,
) -> None:
    _ = capture_id
    frontmatter, title, body = _read_raw_note(raw_path)
    _upsert_wiki_pages_for_note(
        frontmatter=frontmatter,
        title=title,
        body=body,
        raw_path=raw_path,
        page_resynthesis_on_touch=page_resynthesis_on_touch,
    )


def ingest_raw_notes(
    items: list[ExportItem | dict[str, object]],
    *,
    integration_handler: Callable[[str, Path], None] = _default_integration_handler,
    retry_failed: bool = False,
    page_resynthesis_on_touch: bool = False,
    debug: bool = False,
    debug_stream: TextIO | None = None,
    log_path: Path | None = None,
) -> IngestResult:
    validated = validate_ingest_inputs(items)
    result: IngestResult = {"integrated": [], "skipped": [], "failed": []}

    for item in validated:
        raw_abspath = ROOT / item["raw_path"]
        frontmatter, _, _ = _read_raw_note(raw_abspath)
        if frontmatter.get("integrated_at") and not retry_failed:
            result["skipped"].append(item)
            continue

        last_error: Exception | None = None
        for _ in range(3):
            try:
                if integration_handler is _default_integration_handler:
                    integration_handler(
                        item["capture_id"],
                        raw_abspath,
                        page_resynthesis_on_touch=page_resynthesis_on_touch,
                    )
                else:
                    integration_handler(item["capture_id"], raw_abspath)
                integrated_at = utc_timestamp()
                _rewrite_raw_integrated_at(raw_abspath, integrated_at)
                append_jsonl_event(
                    {"event": "integrated", "capture_id": item["capture_id"], "raw_path": item["raw_path"]},
                    log_path=log_path,
                )
                debug_print(f"Integrated {item['capture_id']}", enabled=debug, stream=debug_stream)
                result["integrated"].append(item)
                last_error = None
                break
            except Exception as exc:  # pragma: no cover - exercised by tests
                last_error = exc
        if last_error is not None:
            append_jsonl_event(
                {
                    "event": "integrate_failed",
                    "capture_id": item["capture_id"],
                    "raw_path": item["raw_path"],
                    "error": str(last_error),
                },
                log_path=log_path,
            )
            result["failed"].append(item)
            debug_print(f"Integration failed for {item['capture_id']}: {last_error}", enabled=debug, stream=debug_stream)

    return result


@contextmanager
def pipeline_lock(capture_root: Path) -> Iterator[None]:
    def parse_lock_timestamp(value: object) -> datetime:
        if not isinstance(value, str):
            raise ValueError("lock timestamp must be a string")
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)

    def read_lock_metadata(handle: TextIO) -> dict[str, object]:
        handle.seek(0)
        raw = handle.read().strip()
        if not raw:
            raise ValueError("lock metadata is empty")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("lock metadata must be an object")
        if payload.get("version") != 1:
            raise ValueError("unsupported lock metadata version")
        if not isinstance(payload.get("owner_token"), str) or not payload["owner_token"]:
            raise ValueError("lock metadata missing owner token")
        if not isinstance(payload.get("pid"), int):
            raise ValueError("lock metadata missing pid")
        if not isinstance(payload.get("capture_root"), str) or not payload["capture_root"]:
            raise ValueError("lock metadata missing capture root")
        parse_lock_timestamp(payload.get("acquired_at"))
        return payload

    def write_lock_metadata(handle: TextIO, metadata: dict[str, object]) -> None:
        handle.seek(0)
        handle.write(json.dumps(metadata, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.truncate()
        handle.flush()
        os.fsync(handle.fileno())

    def acquire_new_lock_handle(path: Path) -> TextIO:
        replacement_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        replacement_handle = replacement_path.open("w+", encoding="utf-8")
        try:
            fcntl.flock(replacement_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.replace(replacement_path, path)
            return replacement_handle
        except Exception:
            replacement_handle.close()
            replacement_path.unlink(missing_ok=True)
            raise

    capture_root.mkdir(parents=True, exist_ok=True)
    lock_path = capture_root / "pipeline.lock"
    stale_claim_path = capture_root / "pipeline.lock.stale-claim"
    metadata = {
        "version": 1,
        "owner_token": str(uuid.uuid4()),
        "pid": os.getpid(),
        "acquired_at": utc_timestamp(),
        "capture_root": str(capture_root),
    }
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN}:
                try:
                    existing_metadata = read_lock_metadata(handle)
                except (OSError, ValueError, json.JSONDecodeError):
                    raise RuntimeError(f"pipeline lock is already held: {lock_path}") from exc

                acquired_at = parse_lock_timestamp(existing_metadata["acquired_at"])
                if (datetime.now(UTC) - acquired_at).total_seconds() <= 30 * 60:
                    raise RuntimeError(f"pipeline lock is already held: {lock_path}") from exc

                try:
                    claim_fd = os.open(stale_claim_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                except FileExistsError as claim_exc:
                    raise RuntimeError(f"pipeline lock is already held: {lock_path}") from claim_exc

                os.close(claim_fd)
                handle.close()
                handle = acquire_new_lock_handle(lock_path)
            else:
                raise
        write_lock_metadata(handle, metadata)
        yield
    finally:
        try:
            if not handle.closed:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            if not handle.closed:
                handle.close()
            stale_claim_path.unlink(missing_ok=True)


def run_vault_pipeline(
    *,
    capture_root: Path = DEFAULT_CAPTURE_ROOT,
    debug: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
    retry_failed: bool = False,
    page_resynthesis_on_touch: bool = False,
    debug_stream: TextIO | None = None,
) -> PipelineRunResult:
    with pipeline_lock(capture_root):
        debug_print("Running capture ingest stage", enabled=debug, stream=debug_stream)
        try:
            capture_result = capture_ingest(
                capture_root=capture_root,
                debug=debug,
                dry_run=dry_run,
                limit=limit,
                retry_failed=retry_failed,
                debug_stream=debug_stream,
            )
        except Exception as exc:
            append_jsonl_event(
                {"event": "capture_stage_crashed", "error": str(exc), "capture_root": str(capture_root)},
                log_path=JSONL_LOG_PATH,
            )
            raise

        wiki_ingest: IngestResult | None = None
        if not dry_run:
            debug_print("Running wiki ingest stage", enabled=debug, stream=debug_stream)
            wiki_ingest = ingest_raw_notes(
                capture_result["new_exports"],
                retry_failed=retry_failed,
                page_resynthesis_on_touch=page_resynthesis_on_touch,
                debug=debug,
                debug_stream=debug_stream,
            )

        return {"capture_ingest": capture_result, "wiki_ingest": wiki_ingest}


def build_capture_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export Obsidian capture notes into raw markdown files.")
    parser.add_argument("--debug", action="store_true", help="Emit human-readable debug output to stderr.")
    parser.add_argument("--dry-run", action="store_true", help="Report actions without mutating state.")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N eligible notes in this run.")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Include notes whose source frontmatter has ingest_attempts >= 3.",
    )
    parser.add_argument(
        "--capture-root",
        type=Path,
        default=DEFAULT_CAPTURE_ROOT,
        help="Top-level Obsidian capture vault root to scan.",
    )
    return parser


def build_ingest_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest raw notes into the wiki.")
    parser.add_argument("--debug", action="store_true", help="Emit human-readable debug output to stderr.")
    parser.add_argument("--retry-failed", action="store_true", help="Re-run notes already marked integrated.")
    parser.add_argument(
        "--page-resynthesis-on-touch",
        action="store_true",
        help="Re-synthesize touched existing wiki pages from accumulated notes and sources during ingest.",
    )
    parser.add_argument(
        "--items-json",
        default="-",
        help="JSON array of {capture_id, raw_path}; use '-' to read from stdin.",
    )
    return parser


def build_run_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the full Vault capture and wiki ingest pipeline.")
    parser.add_argument("--debug", action="store_true", help="Emit human-readable debug output to stderr.")
    parser.add_argument("--dry-run", action="store_true", help="Report actions without mutating state.")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N eligible notes in this run.")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Include notes whose source frontmatter has ingest_attempts >= 3.",
    )
    parser.add_argument(
        "--page-resynthesis-on-touch",
        action="store_true",
        help="Re-synthesize touched existing wiki pages from accumulated notes and sources during ingest.",
    )
    parser.add_argument(
        "--capture-root",
        type=Path,
        default=DEFAULT_CAPTURE_ROOT,
        help="Top-level Obsidian capture vault root to scan.",
    )
    return parser


def capture_main(argv: list[str] | None = None) -> int:
    args = build_capture_parser().parse_args(argv)
    result = capture_ingest(
        capture_root=args.capture_root,
        debug=args.debug,
        dry_run=args.dry_run,
        limit=args.limit,
        retry_failed=args.retry_failed,
    )
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


def ingest_main(argv: list[str] | None = None) -> int:
    args = build_ingest_parser().parse_args(argv)
    items_json = sys.stdin.read() if args.items_json == "-" else args.items_json
    items = json.loads(items_json) if items_json.strip() else []
    result = ingest_raw_notes(
        items,
        retry_failed=args.retry_failed,
        page_resynthesis_on_touch=args.page_resynthesis_on_touch,
        debug=args.debug,
    )
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


def run_main(argv: list[str] | None = None) -> int:
    args = build_run_parser().parse_args(argv)
    result = run_vault_pipeline(
        capture_root=args.capture_root,
        debug=args.debug,
        dry_run=args.dry_run,
        limit=args.limit,
        retry_failed=args.retry_failed,
        page_resynthesis_on_touch=args.page_resynthesis_on_touch,
    )
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(run_main())
