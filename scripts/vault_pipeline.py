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
from typing import Callable, Iterator, TextIO, TypedDict
import unicodedata
import uuid

try:
    from scripts import bootstrap_wiki as bw
    from scripts import vault_pipeline_capture as capture_impl
    from scripts import vault_pipeline_cli as cli_impl
    from scripts import vault_pipeline_lint as lint_impl
    from scripts import vault_pipeline_query as query_impl
    from scripts import vault_pipeline_wiki as wiki_impl
    from scripts.workspace_fs import atomic_write_text as shared_atomic_write_text
    from scripts.workspace_fs import temporary_workspace as shared_temporary_workspace
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    import bootstrap_wiki as bw
    import vault_pipeline_capture as capture_impl
    import vault_pipeline_cli as cli_impl
    import vault_pipeline_lint as lint_impl
    import vault_pipeline_query as query_impl
    import vault_pipeline_wiki as wiki_impl
    from workspace_fs import atomic_write_text as shared_atomic_write_text
    from workspace_fs import temporary_workspace as shared_temporary_workspace


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
    with shared_temporary_workspace(
        _workspace_snapshot,
        configure_workspace,
        root,
        restore_kwargs=_restore_workspace_kwargs,
        capture_root=capture_root,
    ):
        yield


def _workspace_snapshot() -> tuple[Path, ...]:
    return (ROOT, RAW_ROOT, SOURCES_ROOT, CHAT_SOURCES_ROOT, WIKI_ROOT, JSONL_LOG_PATH, DEFAULT_CAPTURE_ROOT)


def _restore_workspace_kwargs(original: tuple[Path, ...]) -> dict[str, Path]:
    return {"capture_root": original[-1]}


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
    review_queued: bool = False
    deferred_items: list[str] = field(default_factory=list)


@dataclass
class QueryWritebackResult:
    changed_slugs: list[str] = field(default_factory=list)
    source_path: Path | None = None
    router_decision: RouterDecision | None = None
    review_queued: bool = False
    superseded_source_paths: list[str] = field(default_factory=list)
    duplicate_of_source_path: str | None = None


@dataclass(frozen=True)
class LintFinding:
    kind: str
    slug: str
    detail: str
    source_paths: tuple[str, ...] = ()


@dataclass
class LintReport:
    findings: list[LintFinding] = field(default_factory=list)
    review_updates: int = 0


@dataclass(frozen=True)
class MaintenanceBudget:
    max_candidate_pages: int = 3
    max_context_chars: int = 4_500
    max_pages_rewritten: int = 4
    max_heavy_updater_calls: int = 1


@dataclass
class HeavyPageDelta:
    notes_to_add: list[str] = field(default_factory=list)
    open_questions_to_add: list[str] = field(default_factory=list)


@dataclass
class HeavyNewPageProposal:
    title: str
    page_type: str
    notes: list[str] = field(default_factory=list)
    open_questions: list[str] = field(default_factory=list)


@dataclass
class HeavyUpdateProposal:
    page_updates: dict[str, HeavyPageDelta] = field(default_factory=dict)
    proposed_connections: dict[str, list[str]] = field(default_factory=dict)
    proposed_new_pages: dict[str, HeavyNewPageProposal] = field(default_factory=dict)
    contradiction_items: list[str] = field(default_factory=list)
    deferred_items: list[str] = field(default_factory=list)
    budget_exceeded: bool = False
    reason: str = ""


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
    return re.fullmatch(r"untitled(?:\s+\d+)?", title.strip(), flags=re.I) is not None


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
    shared_atomic_write_text(path, content)


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


def _latest_ingest_event(
    *,
    capture_id: str,
    raw_path: str,
    log_path: Path | None = None,
) -> str | None:
    effective_log_path = log_path or JSONL_LOG_PATH
    if not effective_log_path.exists():
        return None
    latest_event: str | None = None
    for line in effective_log_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if payload.get("capture_id") != capture_id or payload.get("raw_path") != raw_path:
            continue
        event = payload.get("event")
        if event in {"integrated", "integrate_failed"}:
            latest_event = str(event)
    return latest_event


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
    return query_impl._resolve_repo_relative_path(sys.modules[__name__], path)


def _source_excerpt(source: bw.SourceRecord) -> str:
    return query_impl._source_excerpt(sys.modules[__name__], source)


def _remove_source_from_page(page: bw.Page, source_path: str) -> None:
    query_impl._remove_source_from_page(sys.modules[__name__], page, source_path)


def _remove_open_questions_for_fact(page: bw.Page, *, fact_key: str) -> None:
    query_impl._remove_open_questions_for_fact(sys.modules[__name__], page, fact_key=fact_key)


def _append_review_backlog_item(
    *,
    reason: str,
    affected_pages: list[str],
    source_paths: list[str],
    next_action: str,
    status: str = "open",
) -> None:
    query_impl._append_review_backlog_item(
        sys.modules[__name__],
        reason=reason,
        affected_pages=affected_pages,
        source_paths=source_paths,
        next_action=next_action,
        status=status,
    )


def _resolve_review_backlog_entries(
    *,
    reason: str,
    affected_pages: list[str],
) -> int:
    return query_impl._resolve_review_backlog_entries(
        sys.modules[__name__],
        reason=reason,
        affected_pages=affected_pages,
    )


def _matching_chat_sources_for_fact(page: bw.Page, *, fact_key: str) -> list[tuple[str, dict[str, object]]]:
    return query_impl._matching_chat_sources_for_fact(sys.modules[__name__], page, fact_key=fact_key)


def _default_maintenance_budget(*, mode: str = "routine") -> MaintenanceBudget:
    if mode == "bootstrap":
        return MaintenanceBudget(max_candidate_pages=6, max_context_chars=9_000, max_pages_rewritten=8)
    return MaintenanceBudget()


def _note_token_set(note: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]+", note.lower()) if token}


def _notes_conflict(existing_note: str, new_note: str) -> bool:
    existing_tokens = _note_token_set(existing_note)
    new_tokens = _note_token_set(new_note)
    if not existing_tokens or not new_tokens or existing_note.strip() == new_note.strip():
        return False
    preference_tokens = {"prefer", "prefers", "preferred", "preference", "favorite", "favourite"}
    if not (existing_tokens | new_tokens) & preference_tokens:
        return False
    overlap = len(existing_tokens & new_tokens)
    if overlap < 2:
        return False
    return True


def _assemble_heavy_context(
    *,
    title: str,
    body: str,
    router_decision: RouterDecision,
    loaded_pages: dict[str, bw.Page],
    budget: MaintenanceBudget,
) -> tuple[list[str], list[str]]:
    context_chunks = [title.strip(), body.strip()]
    selected_targets: list[str] = []
    total_chars = sum(len(chunk) for chunk in context_chunks)
    deferred_items: list[str] = []

    for slug in router_decision.target_pages:
        if len(selected_targets) >= budget.max_candidate_pages:
            deferred_items.append(f"Deferred page '{slug}' because candidate page budget was exceeded.")
            continue
        page = loaded_pages.get(slug)
        page_context = ""
        if page is not None:
            page_context = bw.render_page(page)
        projected = total_chars + len(page_context)
        if selected_targets and projected > budget.max_context_chars:
            deferred_items.append(f"Deferred page '{slug}' because heavy-update context budget was exceeded.")
            continue
        selected_targets.append(slug)
        total_chars = projected
    if not selected_targets and router_decision.target_pages:
        selected_targets.append(router_decision.target_pages[0])
        deferred_items = [
            item
            for item in deferred_items
            if f"'{router_decision.target_pages[0]}'" not in item
        ]
    return selected_targets, deferred_items


def _build_heavy_update_proposal(
    *,
    title: str,
    source_record: bw.SourceRecord,
    loaded_pages: dict[str, bw.Page],
    resolved_assignments: list[tuple[str, str, str | None]],
    router_decision: RouterDecision,
    budget: MaintenanceBudget,
) -> HeavyUpdateProposal:
    selected_targets, deferred_items = _assemble_heavy_context(
        title=title,
        body=source_record.cleaned_text,
        router_decision=router_decision,
        loaded_pages=loaded_pages,
        budget=budget,
    )
    selected_set = set(selected_targets)
    proposal = HeavyUpdateProposal(
        deferred_items=deferred_items[:],
        budget_exceeded=bool(deferred_items),
        reason="Heavy update required for new-page creation, multi-page impact, or structural ambiguity.",
    )
    excerpt = _source_excerpt(source_record)

    for slug, seed_kind, _parent_slug in resolved_assignments:
        if slug not in selected_set:
            if slug not in router_decision.target_pages:
                proposal.deferred_items.append(f"Deferred derived page '{slug}' because heavy-update budget was exceeded.")
                proposal.budget_exceeded = True
            continue
        page = loaded_pages.get(slug)
        existing_page = page is not None and (WIKI_ROOT / f"{slug}.md").exists()
        if not existing_page:
            proposal.proposed_new_pages[slug] = HeavyNewPageProposal(
                title=bw.page_title(slug),
                page_type=bw.classify_page(slug, title, seed_kind),
                notes=[excerpt] if excerpt else [],
            )
            continue

        delta = proposal.page_updates.setdefault(slug, HeavyPageDelta())
        if excerpt and excerpt not in page.notes:
            delta.notes_to_add.append(excerpt)
        for existing_note in page.notes:
            if _notes_conflict(existing_note, excerpt):
                question = f'Potential contradiction after "{title}": "{existing_note}" vs "{excerpt}".'
                if question not in delta.open_questions_to_add:
                    delta.open_questions_to_add.append(question)
                if question not in proposal.contradiction_items:
                    proposal.contradiction_items.append(question)

    selected_assignment_slugs = [slug for slug, _seed_kind, _parent_slug in resolved_assignments if slug in selected_set]
    for slug, _seed_kind, parent_slug in resolved_assignments:
        if slug not in selected_set:
            continue
        targets = proposal.proposed_connections.setdefault(slug, [])
        for other_slug in selected_assignment_slugs:
            if other_slug != slug and other_slug not in targets:
                targets.append(other_slug)
        if parent_slug and parent_slug in loaded_pages:
            parent_targets = proposal.proposed_connections.setdefault(parent_slug, [])
            if slug not in parent_targets:
                parent_targets.append(slug)

    if proposal.contradiction_items:
        proposal.reason = "Heavy update recorded contradiction risk and open questions."
    return proposal


def _apply_heavy_update_proposal(
    *,
    title: str,
    source_record: bw.SourceRecord,
    loaded_pages: dict[str, bw.Page],
    resolved_assignments: list[tuple[str, str, str | None]],
    proposal: HeavyUpdateProposal,
    budget: MaintenanceBudget,
) -> tuple[dict[str, bw.Page], list[str], bool]:
    _ = title
    touched_pages: dict[str, bw.Page] = {}

    for slug, page_proposal in proposal.proposed_new_pages.items():
        page = loaded_pages.get(slug)
        if page is None:
            matching_assignment = next(
                ((assignment_slug, seed_kind, _parent_slug) for assignment_slug, seed_kind, _parent_slug in resolved_assignments if assignment_slug == slug),
                None,
            )
            seed_kind = matching_assignment[1] if matching_assignment is not None else "title"
            page = bw.Page(
                slug=slug,
                title=page_proposal.title,
                page_type=page_proposal.page_type,
                summary_hint=title,
            )
            page.seed_kinds.add(seed_kind)
            loaded_pages[slug] = page
        page.shape = bw.PAGE_SHAPE_ATOMIC
        page.page_type = page_proposal.page_type
        for note in page_proposal.notes:
            if note not in page.notes:
                page.notes.append(note)
        for question in page_proposal.open_questions:
            if question not in page.open_questions:
                page.open_questions.append(question)
        bw.add_source_to_page(page, source_record, seed_kind="ingest")
        touched_pages[slug] = page

    for slug, delta in proposal.page_updates.items():
        page = loaded_pages[slug]
        bw.add_source_to_page(page, source_record, seed_kind="ingest")
        for note in delta.notes_to_add:
            if note not in page.notes:
                page.notes.append(note)
        for question in delta.open_questions_to_add:
            if question not in page.open_questions:
                page.open_questions.append(question)
        touched_pages[slug] = page

    for slug, _seed_kind, parent_slug in resolved_assignments:
        if slug in touched_pages:
            if parent_slug:
                parent_page = loaded_pages[parent_slug]
                parent_page.shape = bw.PAGE_SHAPE_TOPIC
                touched_pages[parent_slug] = parent_page
            elif slug in loaded_pages:
                loaded_pages[slug].shape = bw.PAGE_SHAPE_ATOMIC

    for left, right_slugs in proposal.proposed_connections.items():
        if left not in loaded_pages:
            continue
        for right in right_slugs:
            if right not in loaded_pages:
                continue
            bw.connect_pages(loaded_pages, left, right)
            touched_pages[left] = loaded_pages[left]
            touched_pages[right] = loaded_pages[right]

    bw.prune_generic_media_links(loaded_pages)
    bw.ensure_meaningful_connections(loaded_pages)
    bw.finalize_page_shapes(loaded_pages)

    changed_slugs: list[str] = []
    ordered_touched = list(touched_pages)
    if len(ordered_touched) > budget.max_pages_rewritten:
        overflow = ordered_touched[budget.max_pages_rewritten :]
        proposal.deferred_items.extend(
            f"Deferred write for '{slug}' because rewritten page budget was exceeded." for slug in overflow
        )
        proposal.budget_exceeded = True
        ordered_touched = ordered_touched[: budget.max_pages_rewritten]

    for slug in ordered_touched:
        page = loaded_pages[slug]
        allow_missing_outbound = bool(page.topic_parent) or (slug not in proposal.proposed_new_pages and bool(page.connections))
        issues = bw.validate_page(page, allow_missing_outbound=allow_missing_outbound)
        if issues:
            if issues == ["atomic-pages-must-have-outbound-links"]:
                proposal.deferred_items.append(f"Deferred page '{slug}' until it has a meaningful outbound link.")
                proposal.budget_exceeded = True
                continue
            raise ValueError(f"invalid page '{slug}': {', '.join(issues)}")
        atomic_write_text(WIKI_ROOT / f"{slug}.md", bw.render_page(page))
        changed_slugs.append(slug)

    review_queued = bool(proposal.contradiction_items or proposal.deferred_items)
    if review_queued:
        reason = "heavy-update contradiction" if proposal.contradiction_items else "heavy-update deferred work"
        next_action = "Review contradiction items and confirm the correct current wiki state."
        if proposal.deferred_items and not proposal.contradiction_items:
            next_action = "Resume deferred heavy maintenance for the queued pages."
        _append_review_backlog_item(
            reason=reason,
            affected_pages=ordered_touched,
            source_paths=[source_record.path],
            next_action=next_action,
        )
    return touched_pages, changed_slugs, review_queued


def validate_adoptable_raw(path: Path, capture_id: str) -> None:
    capture_impl.validate_adoptable_raw(sys.modules[__name__], path, capture_id)


def compute_raw_target(title: str, capture_id: str) -> Path:
    return capture_impl.compute_raw_target(sys.modules[__name__], title, capture_id)


def classify_raw_candidate(path: Path, capture_id: str) -> str | None:
    return capture_impl.classify_raw_candidate(sys.modules[__name__], path, capture_id)


def discover_raw_candidates(capture_id: str) -> RawCandidateSet:
    return capture_impl.discover_raw_candidates(sys.modules[__name__], capture_id)


def resolve_raw_path_for_capture(*, title: str, capture_id: str) -> tuple[Path, str, int]:
    return capture_impl.resolve_raw_path_for_capture(sys.modules[__name__], title=title, capture_id=capture_id)


def increment_ingest_attempts(note: SourceNote) -> int:
    return capture_impl.increment_ingest_attempts(sys.modules[__name__], note)


def _record_retry_gated_export_failure(
    *,
    note: SourceNote,
    capture_id: str,
    error: str,
    failure_class: str,
    log_path: Path | None,
) -> None:
    capture_impl._record_retry_gated_export_failure(
        sys.modules[__name__],
        note=note,
        capture_id=capture_id,
        error=error,
        failure_class=failure_class,
        log_path=log_path,
    )


def inject_capture_id(note: SourceNote) -> SourceNote:
    return capture_impl.inject_capture_id(sys.modules[__name__], note)


def rename_processed(path: Path) -> Path:
    return capture_impl.rename_processed(sys.modules[__name__], path)


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
    return capture_impl.capture_ingest(
        sys.modules[__name__],
        capture_root=capture_root,
        debug=debug,
        dry_run=dry_run,
        limit=limit,
        retry_failed=retry_failed,
        log_path=log_path,
        debug_stream=debug_stream,
    )


def _normalize_ingest_item(item: ExportItem | dict[str, object]) -> ExportItem:
    return wiki_impl._normalize_ingest_item(sys.modules[__name__], item)


def validate_ingest_inputs(items: list[ExportItem | dict[str, object]]) -> list[ExportItem]:
    return wiki_impl.validate_ingest_inputs(sys.modules[__name__], items)


def _today_date() -> str:
    return wiki_impl._today_date(sys.modules[__name__])


def _read_raw_note(path: Path) -> tuple[dict[str, object], str, str]:
    return wiki_impl._read_raw_note(sys.modules[__name__], path)

def _derive_path_topics(path: Path) -> list[str]:
    return wiki_impl._derive_path_topics(sys.modules[__name__], path)


def _build_default_page_assignments(title: str, body: str, raw_abspath: Path) -> list[tuple[str, str]]:
    return wiki_impl._build_default_page_assignments(sys.modules[__name__], title, body, raw_abspath)


def _content_owner_slug(assignments: list[tuple[str, str, str | None]]) -> str | None:
    return wiki_impl._content_owner_slug(sys.modules[__name__], assignments)


def _source_record_from_artifact(frontmatter: dict[str, object], title: str, body: str, source_path: Path) -> bw.SourceRecord:
    return wiki_impl._source_record_from_artifact(sys.modules[__name__], frontmatter, title, body, source_path)


def _resolve_synthesis_config() -> tuple[str | None, str]:
    return wiki_impl._resolve_synthesis_config(sys.modules[__name__])


def _parse_existing_note_snippets(note_lines: list[str]) -> list[str]:
    return wiki_impl._parse_existing_note_snippets(sys.modules[__name__], note_lines)


def _parse_source_line(line: str, retained_evidence: str) -> bw.SourceRecord | None:
    return wiki_impl._parse_source_line(sys.modules[__name__], line, retained_evidence)


def _parse_connection_slugs(connection_lines: list[str]) -> list[str]:
    return wiki_impl._parse_connection_slugs(sys.modules[__name__], connection_lines)


def _read_wiki_page(slug: str, *, original_title: str, seed_kind: str) -> bw.Page:
    return wiki_impl._read_wiki_page(sys.modules[__name__], slug, original_title=original_title, seed_kind=seed_kind)


def _validate_router_decision(decision: RouterDecision) -> RouterDecision:
    return wiki_impl._validate_router_decision(sys.modules[__name__], decision)


def _route_source_update(
    *,
    title: str,
    body: str,
    page_assignments: list[tuple[str, str]],
) -> RouterDecision:
    return wiki_impl._route_source_update(sys.modules[__name__], title=title, body=body, page_assignments=page_assignments)


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
    return wiki_impl._build_bootstrap_page_for_touch(
        sys.modules[__name__],
        slug=slug,
        seed_kind=seed_kind,
        original_title=original_title,
        parsed_page=parsed_page,
        new_source_record=new_source_record,
        new_note_snippet=new_note_snippet,
        related_slugs=related_slugs,
    )


def _parse_existing_page(page_path: Path) -> dict[str, object]:
    return wiki_impl._parse_existing_page(sys.modules[__name__], page_path)


def _render_merged_page(
    *,
    page_title: str,
    summary: str,
    note_lines: list[str],
    connection_lines: list[str],
    source_lines: list[str],
) -> str:
    return wiki_impl._render_merged_page(
        sys.modules[__name__],
        page_title=page_title,
        summary=summary,
        note_lines=note_lines,
        connection_lines=connection_lines,
        source_lines=source_lines,
    )


def _count_page_sources(page_path: Path) -> int:
    return wiki_impl._count_page_sources(sys.modules[__name__], page_path)


def _rewrite_index(changed_pages: list[tuple[str, str]]) -> None:
    wiki_impl._rewrite_index(sys.modules[__name__], changed_pages)


def _append_wiki_ingest_log(
    title: str,
    *,
    router_decision: RouterDecision | None = None,
    deferred_items: list[str] | None = None,
) -> None:
    wiki_impl._append_wiki_ingest_log(
        sys.modules[__name__],
        title,
        router_decision=router_decision,
        deferred_items=deferred_items,
    )


def _append_bootstrap_pipeline_log(*, processed_sources: int, changed_pages: int, failed_sources: int) -> None:
    wiki_impl._append_bootstrap_pipeline_log(
        sys.modules[__name__],
        processed_sources=processed_sources,
        changed_pages=changed_pages,
        failed_sources=failed_sources,
    )


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
    return query_impl.query_writeback_chat_fact(
        sys.modules[__name__],
        page_title=page_title,
        note=note,
        related_pages=related_pages,
        created_at=created_at,
        conversation_ref=conversation_ref,
        fact_key=fact_key,
        replacement_intent=replacement_intent,
        external_url=external_url,
    )


def _upsert_wiki_pages_for_note(
    *,
    frontmatter: dict[str, object],
    title: str,
    body: str,
    raw_path: Path,
    page_resynthesis_on_touch: bool = False,
    budget: MaintenanceBudget | None = None,
) -> MaintenanceOutcome:
    return wiki_impl._upsert_wiki_pages_for_note(
        sys.modules[__name__],
        frontmatter=frontmatter,
        title=title,
        body=body,
        raw_path=raw_path,
        page_resynthesis_on_touch=page_resynthesis_on_touch,
        budget=budget,
    )


def _iter_source_artifact_paths(*, include_chat: bool) -> list[Path]:
    return wiki_impl._iter_source_artifact_paths(sys.modules[__name__], include_chat=include_chat)


def bootstrap_integrate_sources(
    *,
    source_paths: list[Path] | None = None,
    include_chat: bool = True,
    budget: MaintenanceBudget | None = None,
) -> dict[str, object]:
    return wiki_impl.bootstrap_integrate_sources(
        sys.modules[__name__],
        source_paths=source_paths,
        include_chat=include_chat,
        budget=budget,
    )


def trace_page_provenance(slug: str) -> dict[str, list[str]]:
    return lint_impl.trace_page_provenance(sys.modules[__name__], slug)


def lint_wiki(*, append_review: bool = False) -> LintReport:
    return lint_impl.lint_wiki(sys.modules[__name__], append_review=append_review)


def _default_integration_handler(
    capture_id: str,
    raw_path: Path,
    *,
    page_resynthesis_on_touch: bool = False,
) -> None:
    wiki_impl._default_integration_handler(
        sys.modules[__name__],
        capture_id,
        raw_path,
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
    return wiki_impl.ingest_raw_notes(
        sys.modules[__name__],
        items,
        integration_handler=integration_handler,
        retry_failed=retry_failed,
        page_resynthesis_on_touch=page_resynthesis_on_touch,
        debug=debug,
        debug_stream=debug_stream,
        log_path=log_path,
    )


@contextmanager
def pipeline_lock(capture_root: Path) -> Iterator[None]:
    with cli_impl.pipeline_lock(sys.modules[__name__], capture_root):
        yield


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
    return cli_impl.run_vault_pipeline(
        sys.modules[__name__],
        capture_root=capture_root,
        debug=debug,
        dry_run=dry_run,
        limit=limit,
        retry_failed=retry_failed,
        page_resynthesis_on_touch=page_resynthesis_on_touch,
        debug_stream=debug_stream,
    )


def build_capture_parser() -> argparse.ArgumentParser:
    return cli_impl.build_capture_parser(sys.modules[__name__])


def build_ingest_parser() -> argparse.ArgumentParser:
    return cli_impl.build_ingest_parser(sys.modules[__name__])


def build_run_parser() -> argparse.ArgumentParser:
    return cli_impl.build_run_parser(sys.modules[__name__])


def capture_main(argv: list[str] | None = None) -> int:
    return cli_impl.capture_main(sys.modules[__name__], argv)


def ingest_main(argv: list[str] | None = None) -> int:
    return cli_impl.ingest_main(sys.modules[__name__], argv)


def run_main(argv: list[str] | None = None) -> int:
    return cli_impl.run_main(sys.modules[__name__], argv)


if __name__ == "__main__":
    raise SystemExit(run_main())
