from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import dataclass, field
import os
from pathlib import Path
import re
import sys
from typing import Callable, Iterator, TextIO, TypedDict

try:
    from scripts import bootstrap_wiki as bw
    from scripts import vault_pipeline_capture as capture_impl
    from scripts import vault_pipeline_cli as cli_impl
    from scripts import vault_pipeline_lint as lint_impl
    from scripts import vault_pipeline_maintenance as maintenance_impl
    from scripts import vault_pipeline_notes as notes_impl
    from scripts import vault_pipeline_operations as operations_impl
    from scripts import vault_pipeline_query as query_impl
    from scripts import vault_pipeline_sources as sources_impl
    from scripts import wiki_search as search_impl
    from scripts import vault_pipeline_wiki as wiki_impl
    from scripts.workspace_fs import atomic_write_text as shared_atomic_write_text
    from scripts.workspace_fs import temporary_workspace as shared_temporary_workspace
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    import bootstrap_wiki as bw
    import vault_pipeline_capture as capture_impl
    import vault_pipeline_cli as cli_impl
    import vault_pipeline_lint as lint_impl
    import vault_pipeline_maintenance as maintenance_impl
    import vault_pipeline_notes as notes_impl
    import vault_pipeline_operations as operations_impl
    import vault_pipeline_query as query_impl
    import vault_pipeline_sources as sources_impl
    import wiki_search as search_impl
    import vault_pipeline_wiki as wiki_impl
    from workspace_fs import atomic_write_text as shared_atomic_write_text
    from workspace_fs import temporary_workspace as shared_temporary_workspace


ROOT = Path(__file__).resolve().parent.parent
RAW_ROOT = ROOT / "raw"
SOURCES_ROOT = ROOT / "sources"
CHAT_SOURCES_ROOT = SOURCES_ROOT / "chat"
WIKI_ROOT = ROOT / "wiki"
JSONL_LOG_PATH = ROOT / "log.jsonl"
STATE_EVENTS_PATH = ROOT / "state" / "events.jsonl"
DEFAULT_CAPTURE_ROOT = Path(
    os.environ.get("VAULT_CAPTURE_ROOT", ROOT / "capture")
)

MARKER_PREFIX = "✓ "
_INDEX_ENTRY_RE = re.compile(r"^- \[\[(?P<slug>[^\]]+)\]\] — (?P<summary>.+)$")
_SOURCE_LINE_RE = re.compile(r"^- \[(?P<label>[^\]]+)\]\((?P<path>[^)]+)\)(?P<suffix>.*)$")
_CONNECTION_SLUG_RE = re.compile(r"\[\[(?P<slug>[^\]]+)\]\]")


def configure_workspace(root: Path, *, capture_root: Path | None = None) -> None:
    global ROOT, RAW_ROOT, SOURCES_ROOT, CHAT_SOURCES_ROOT, WIKI_ROOT, JSONL_LOG_PATH, STATE_EVENTS_PATH, DEFAULT_CAPTURE_ROOT

    ROOT = root
    RAW_ROOT = ROOT / "raw"
    SOURCES_ROOT = ROOT / "sources"
    CHAT_SOURCES_ROOT = SOURCES_ROOT / "chat"
    WIKI_ROOT = ROOT / "wiki"
    JSONL_LOG_PATH = ROOT / "log.jsonl"
    STATE_EVENTS_PATH = ROOT / "state" / "events.jsonl"
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
    return (ROOT, RAW_ROOT, SOURCES_ROOT, CHAT_SOURCES_ROOT, WIKI_ROOT, JSONL_LOG_PATH, STATE_EVENTS_PATH, DEFAULT_CAPTURE_ROOT)


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
    source_path: str | None = None
    mode: str = "ingest"
    failed_reason: str | None = None
    effects: operations_impl.OperationalEffects = field(default_factory=operations_impl.OperationalEffects)


@dataclass
class QueryWritebackResult:
    changed_slugs: list[str] = field(default_factory=list)
    source_path: Path | None = None
    router_decision: RouterDecision | None = None
    review_queued: bool = False
    superseded_source_paths: list[str] = field(default_factory=list)
    duplicate_of_source_path: str | None = None
    effects: operations_impl.OperationalEffects = field(default_factory=operations_impl.OperationalEffects)


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
    effects: operations_impl.OperationalEffects = field(default_factory=operations_impl.OperationalEffects)


SourceNote = notes_impl.SourceNote
SourceArtifact = sources_impl.SourceArtifact
OperationalEffects = operations_impl.OperationalEffects
OperationalSink = operations_impl.OperationalSink
AppliedOperationalEffects = operations_impl.AppliedOperationalEffects
MaintenanceBudget = maintenance_impl.MaintenanceBudget
HeavyPageDelta = maintenance_impl.HeavyPageDelta
HeavyNewPageProposal = maintenance_impl.HeavyNewPageProposal
HeavyUpdateProposal = maintenance_impl.HeavyUpdateProposal


def utc_timestamp(value: float | None = None) -> str:
    return notes_impl.utc_timestamp(value)


def normalize_repo_path(path: str | Path) -> str:
    return notes_impl.normalize_repo_path(ROOT, path)


def clean_title_from_filename(filename: str) -> str:
    return sources_impl.clean_title_from_filename(sys.modules[__name__], filename)


def is_placeholder_title(title: str) -> bool:
    return sources_impl.is_placeholder_title(title)


def raw_file_slug(title: str) -> str:
    return sources_impl.raw_file_slug(title)


def stable_source_id(source_kind: str, identity: str) -> str:
    return notes_impl.stable_source_id(source_kind, identity)


def debug_print(message: str, *, enabled: bool, stream: TextIO | None = None) -> None:
    if enabled:
        print(message, file=stream or sys.stderr)


def atomic_write_text(path: Path, content: str) -> None:
    shared_atomic_write_text(path, content)


def _parse_scalar(value: str) -> object:
    return notes_impl._parse_scalar(value)


def _render_scalar(value: object) -> str:
    return notes_impl._render_scalar(value)


def split_frontmatter(text: str) -> tuple[dict[str, object], str, bool]:
    return notes_impl.split_frontmatter(text)


def render_note(frontmatter: dict[str, object], body: str, *, key_order: list[str] | None = None) -> str:
    return notes_impl.render_note(frontmatter, body, key_order=key_order)


def read_source_note(path: Path) -> SourceNote:
    return notes_impl.read_source_note(path)


def source_note_body_is_blank(note: SourceNote) -> bool:
    return notes_impl.source_note_body_is_blank(note)


def write_source_note(path: Path, frontmatter: dict[str, object], body: str) -> None:
    notes_impl.write_source_note(path, frontmatter, body, writer=atomic_write_text)


def append_jsonl_event(payload: dict[str, object], log_path: Path | None = None) -> None:
    operational_sink().apply(operations_impl.OperationalEffects.jsonl_event(payload), log_path=log_path)


def sha256_file(path: Path) -> str:
    return notes_impl.sha256_file(path)


def append_state_event(payload: dict[str, object], state_path: Path | None = None) -> None:
    operational_sink().apply(operations_impl.OperationalEffects.state_event(payload), state_path=state_path)


def operational_sink() -> operations_impl.OperationalSink:
    return operations_impl.OperationalSink(sys.modules[__name__])


def apply_operational_effects(
    effects: operations_impl.OperationalEffects,
    *,
    log_path: Path | None = None,
    state_path: Path | None = None,
) -> operations_impl.AppliedOperationalEffects:
    return operational_sink().apply(effects, log_path=log_path, state_path=state_path)


def raw_state_item(raw_path: Path | SourceArtifact, frontmatter: dict[str, object] | None = None) -> dict[str, object]:
    return notes_impl.raw_state_item(ROOT, raw_path, frontmatter)


def latest_state_record(item_id: str, state_path: Path | None = None) -> dict[str, object] | None:
    return notes_impl.latest_state_record(item_id, state_path or STATE_EVENTS_PATH)


def state_item_seen(item_id: str, state_path: Path | None = None) -> bool:
    return notes_impl.state_item_seen(item_id, state_path or STATE_EVENTS_PATH)


def _latest_ingest_event(
    *,
    capture_id: str,
    raw_path: str,
    log_path: Path | None = None,
) -> str | None:
    return notes_impl.latest_ingest_event(capture_id=capture_id, raw_path=raw_path, log_path=log_path or JSONL_LOG_PATH)


def _has_logged_event(event: str, *, capture_id: str | None, filename: str | None, log_path: Path) -> bool:
    return notes_impl.has_logged_event(event, capture_id=capture_id, filename=filename, log_path=log_path)


def discover_capture_candidates(capture_root: Path) -> list[Path]:
    return capture_impl.discover_capture_candidates(sys.modules[__name__], capture_root)


def discover_processed_capture_candidates(capture_root: Path) -> list[Path]:
    return capture_impl.discover_processed_capture_candidates(sys.modules[__name__], capture_root)


def discover_source_capture_id_counts(candidate_paths: list[Path]) -> dict[str, int]:
    return capture_impl.discover_source_capture_id_counts(sys.modules[__name__], candidate_paths)


def resolve_created_at(note: SourceNote) -> tuple[str, bool]:
    return sources_impl.resolve_created_at(sys.modules[__name__], note)


def render_raw_file(
    *,
    capture_id: str,
    title: str,
    created_at: str,
    source_file: str,
    body: str,
) -> str:
    return sources_impl.render_raw_file(
        sys.modules[__name__],
        capture_id=capture_id,
        title=title,
        created_at=created_at,
        source_file=source_file,
        body=body,
    )


def parse_raw_note(path: Path) -> tuple[dict[str, object], str]:
    return sources_impl.parse_raw_note(sys.modules[__name__], path)


def read_source_artifact(path: Path) -> SourceArtifact:
    return sources_impl.read_source_artifact(sys.modules[__name__], path)


def persist_chat_source_artifact(
    *,
    title: str,
    body: str,
    created_at: str,
    conversation_ref: str,
    external_url: str | None = None,
    extra_frontmatter: dict[str, object] | None = None,
) -> Path:
    return sources_impl.persist_chat_source_artifact(
        sys.modules[__name__],
        title=title,
        body=body,
        created_at=created_at,
        conversation_ref=conversation_ref,
        external_url=external_url,
        extra_frontmatter=extra_frontmatter,
    )


def _resolve_repo_relative_path(path: str) -> Path:
    return query_impl._resolve_repo_relative_path(sys.modules[__name__], path)


def _source_excerpt(source: bw.SourceRecord) -> str:
    return query_impl._source_excerpt(sys.modules[__name__], source)


def _default_maintenance_budget(*, mode: str = "routine") -> MaintenanceBudget:
    if mode == "bootstrap":
        return MaintenanceBudget(max_candidate_pages=6, max_context_chars=9_000, max_pages_rewritten=8)
    return MaintenanceBudget()


def _note_token_set(note: str) -> set[str]:
    return maintenance_impl._note_token_set(note)


def _notes_conflict(existing_note: str, new_note: str) -> bool:
    return maintenance_impl._notes_conflict(sys.modules[__name__], existing_note, new_note)


def _assemble_heavy_context(
    *,
    title: str,
    body: str,
    router_decision: RouterDecision,
    loaded_pages: dict[str, bw.Page],
    budget: MaintenanceBudget,
    target_pages: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    return maintenance_impl._assemble_heavy_context(
        sys.modules[__name__],
        title=title,
        body=body,
        router_decision=router_decision,
        loaded_pages=loaded_pages,
        budget=budget,
        target_pages=target_pages,
    )


def _build_heavy_update_proposal(
    *,
    title: str,
    source_record: bw.SourceRecord,
    loaded_pages: dict[str, bw.Page],
    resolved_assignments: list[tuple[str, str, str | None]],
    router_decision: RouterDecision,
    budget: MaintenanceBudget,
) -> HeavyUpdateProposal:
    return maintenance_impl._build_heavy_update_proposal(
        sys.modules[__name__],
        title=title,
        source_record=source_record,
        loaded_pages=loaded_pages,
        resolved_assignments=resolved_assignments,
        router_decision=router_decision,
        budget=budget,
    )


def _apply_heavy_update_proposal(
    *,
    title: str,
    source_record: bw.SourceRecord,
    loaded_pages: dict[str, bw.Page],
    resolved_assignments: list[tuple[str, str, str | None]],
    proposal: HeavyUpdateProposal,
    budget: MaintenanceBudget,
) -> tuple[dict[str, bw.Page], list[str], bool, OperationalEffects]:
    return maintenance_impl._apply_heavy_update_proposal(
        sys.modules[__name__],
        title=title,
        source_record=source_record,
        loaded_pages=loaded_pages,
        resolved_assignments=resolved_assignments,
        proposal=proposal,
        budget=budget,
    )


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


def source_artifact_to_evidence(artifact: SourceArtifact) -> bw.SourceEvidence:
    return wiki_impl.source_artifact_to_evidence(sys.modules[__name__], artifact)


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


def _rewrite_index(changed_pages: list[tuple[str, str]]) -> None:
    wiki_impl._rewrite_index(sys.modules[__name__], changed_pages)


def _append_wiki_ingest_log(
    title: str,
    *,
    router_decision: RouterDecision | None = None,
    deferred_items: list[str] | None = None,
) -> None:
    apply_operational_effects(
        operations_impl.OperationalEffects.ingest_log(
            title,
            date=_today_date(),
            router_decision=router_decision,
            deferred_items=deferred_items,
        )
    )


def _append_bootstrap_pipeline_log(*, processed_sources: int, changed_pages: int, failed_sources: int) -> None:
    apply_operational_effects(
        operations_impl.OperationalEffects.bootstrap_log(
            date=_today_date(),
            processed_sources=processed_sources,
            changed_pages=changed_pages,
            failed_sources=failed_sources,
        )
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
    title: str,
    body: str,
    raw_path: Path,
    budget: MaintenanceBudget | None = None,
    mode: str = "ingest",
    write_ingest_log: bool = True,
) -> MaintenanceOutcome:
    return wiki_impl._upsert_wiki_pages_for_note(
        sys.modules[__name__],
        title=title,
        body=body,
        raw_path=raw_path,
        budget=budget,
        mode=mode,
        write_ingest_log=write_ingest_log,
    )


def maintain_source_artifact(
    source_path: Path,
    *,
    mode: str,
    budget: MaintenanceBudget | None = None,
    options: dict[str, object] | None = None,
) -> MaintenanceOutcome:
    return wiki_impl.maintain_source_artifact(
        sys.modules[__name__],
        source_path,
        mode=mode,
        budget=budget,
        options=options,
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


def build_wiki_search_index(*, wiki_root: Path | None = None) -> search_impl.WikiSearchIndex:
    return search_impl.build_wiki_search_index(sys.modules[__name__], wiki_root=wiki_root)


def search_wiki(
    query: str,
    *,
    top_k: int = 5,
    index: search_impl.WikiSearchIndex | None = None,
    wiki_root: Path | None = None,
) -> list[search_impl.WikiSearchResult]:
    return search_impl.search_wiki(
        sys.modules[__name__],
        query,
        top_k=top_k,
        index=index,
        wiki_root=wiki_root,
    )


def search_main(argv: list[str] | None = None) -> int:
    return search_impl.search_main(sys.modules[__name__], argv)


def _default_integration_handler(
    capture_id: str,
    raw_path: Path,
) -> MaintenanceOutcome:
    return wiki_impl._default_integration_handler(
        sys.modules[__name__],
        capture_id,
        raw_path,
    )


def ingest_raw_notes(
    items: list[ExportItem | dict[str, object]],
    *,
    integration_handler: Callable[[str, Path], None] = _default_integration_handler,
    retry_failed: bool = False,
    debug: bool = False,
    debug_stream: TextIO | None = None,
    log_path: Path | None = None,
    state_path: Path | None = None,
    dry_run: bool = False,
) -> IngestResult:
    return wiki_impl.ingest_raw_notes(
        sys.modules[__name__],
        items,
        integration_handler=integration_handler,
        retry_failed=retry_failed,
        debug=debug,
        debug_stream=debug_stream,
        log_path=log_path,
        state_path=state_path,
        dry_run=dry_run,
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
    debug_stream: TextIO | None = None,
) -> PipelineRunResult:
    return cli_impl.run_vault_pipeline(
        sys.modules[__name__],
        capture_root=capture_root,
        debug=debug,
        dry_run=dry_run,
        limit=limit,
        retry_failed=retry_failed,
        debug_stream=debug_stream,
    )


def discover(argv: list[str] | None = None) -> cli_impl.PipelinePlan:
    return cli_impl.discover(sys.modules[__name__], argv)


def process(plan: cli_impl.PipelinePlan) -> PipelineRunResult:
    return cli_impl.process(sys.modules[__name__], plan)


def write_outputs(result: object) -> int:
    return cli_impl.write_outputs(sys.modules[__name__], result)


def pipeline_run_has_output(result: object) -> bool:
    return cli_impl.pipeline_run_has_output(result)


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
