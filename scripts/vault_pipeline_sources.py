from __future__ import annotations

from pathlib import Path
import re
from typing import TYPE_CHECKING
import unicodedata
import uuid

if TYPE_CHECKING:
    from types import ModuleType


_UNSAFE_SLUG_CHARS_RE = re.compile(r"[^a-z0-9\-]+")
_DASH_RUN_RE = re.compile(r"-{2,}")


def clean_title_from_filename(api: ModuleType, filename: str) -> str:
    title = filename[:-3] if filename.endswith(".md") else filename
    if title.startswith(api.MARKER_PREFIX):
        title = title[len(api.MARKER_PREFIX) :]
    return title


def is_placeholder_title(title: str) -> bool:
    return re.fullmatch(r"untitled(?:\s+\d+)?", title.strip(), flags=re.I) is not None


def raw_file_slug(title: str) -> str:
    normalized = unicodedata.normalize("NFKD", title.strip().lower())
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    dashed = ascii_text.replace(" ", "-")
    sanitized = _UNSAFE_SLUG_CHARS_RE.sub("-", dashed)
    collapsed = _DASH_RUN_RE.sub("-", sanitized).strip("-")
    trimmed = collapsed[:80].rstrip("-")
    return trimmed or "untitled"


def resolve_created_at(api: ModuleType, note: object) -> tuple[str, bool]:
    existing = note.frontmatter.get("created_at")
    if isinstance(existing, str) and existing:
        return existing, False
    stats = note.path.stat()
    birthtime = getattr(stats, "st_birthtime", 0)
    if birthtime:
        return api.utc_timestamp(float(birthtime)), False
    return api.utc_timestamp(stats.st_mtime), True


def render_raw_file(
    api: ModuleType,
    *,
    capture_id: str,
    title: str,
    created_at: str,
    source_file: str,
    body: str,
) -> str:
    external_url = api.bw.extract_first_url(body)
    frontmatter: dict[str, object] = {
        "capture_id": capture_id,
        "source_kind": "capture",
        "source_id": api.stable_source_id("capture", capture_id),
        "title": title,
        "created_at": created_at,
        "source_file": source_file,
    }
    if external_url:
        frontmatter["external_url"] = external_url
    return api.render_note(
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
        ],
    )


def parse_raw_note(api: ModuleType, path: Path) -> tuple[dict[str, object], str]:
    text = path.read_text(encoding="utf-8")
    frontmatter, body, has_frontmatter = api.split_frontmatter(text)
    if not has_frontmatter:
        raise ValueError("raw file missing frontmatter")
    if not body.strip():
        raise ValueError("raw file body is empty")
    return frontmatter, body


def persist_chat_source_artifact(
    api: ModuleType,
    *,
    title: str,
    body: str,
    created_at: str,
    conversation_ref: str,
    external_url: str | None = None,
    extra_frontmatter: dict[str, object] | None = None,
) -> Path:
    api.CHAT_SOURCES_ROOT.mkdir(parents=True, exist_ok=True)
    identity = f"{created_at}:{title}:{body}"
    source_id = api.stable_source_id("chat", uuid.uuid5(uuid.NAMESPACE_URL, identity).hex)
    target = api.CHAT_SOURCES_ROOT / f"{api.raw_file_slug(title)}-{source_id.split(':', 1)[1]}.md"
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
    api.atomic_write_text(
        target,
        api.render_note(
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
