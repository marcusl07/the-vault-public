from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from pathlib import Path
import sys

try:
    from scripts.workspace_fs import atomic_write_text as shared_atomic_write_text
    from scripts.workspace_fs import temporary_workspace as shared_temporary_workspace
    from scripts.source_model import content_body, source_artifact_to_evidence
    from scripts import bootstrap_wiki_cache as cache_impl
    from scripts import bootstrap_wiki_cli as cli_impl
    from scripts import bootstrap_wiki_engine as engine_impl
    from scripts import bootstrap_wiki_parsing as parsing_impl
    from scripts import bootstrap_wiki_remote as remote_impl
    from scripts import bootstrap_wiki_rendering as rendering_impl
    from scripts import bootstrap_wiki_sources as sources_impl
    from scripts import bootstrap_wiki_splitting as splitting_impl
    from scripts.bootstrap_wiki_model import (
        BOILERPLATE_PATTERNS,
        BOLD_HEADING_RE,
        CATALOG_PATH,
        COURSE_PAGE_SLUG_RE,
        GENERIC_BUCKET_SLUGS,
        HIGH_SIGNAL_INBOUND_THRESHOLD,
        INDEX_SECTION_ORDER,
        LECTURE_HEADING_RE,
        LECTURE_SOURCE_RE,
        PAGE_SHAPE_ATOMIC,
        PAGE_SHAPE_TOPIC,
        PAGE_SPLIT_MAX_BODY_CHARS,
        SOURCE_LINE_RE,
        TODAY,
        TOPIC_EXTRACTION_CONFIDENT_LEVELS,
        TOPIC_EXTRACTION_MAX_BODY_CHARS,
        BucketSignalResult,
        FetchResult,
        Page,
        PageSplitDecision,
        ParsedWikiPage,
        SourceArtifact,
        SourceCitation,
        SourceEvidence,
        SourceRecord,
        SplitCandidateEvaluation,
        SplitPhaseReport,
        classify_page,
        compact_source_text,
        ordered_unique,
        page_title,
        strip_markdown,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    from workspace_fs import atomic_write_text as shared_atomic_write_text
    from workspace_fs import temporary_workspace as shared_temporary_workspace
    from source_model import content_body, source_artifact_to_evidence
    import bootstrap_wiki_cache as cache_impl
    import bootstrap_wiki_cli as cli_impl
    import bootstrap_wiki_engine as engine_impl
    import bootstrap_wiki_parsing as parsing_impl
    import bootstrap_wiki_remote as remote_impl
    import bootstrap_wiki_rendering as rendering_impl
    import bootstrap_wiki_sources as sources_impl
    import bootstrap_wiki_splitting as splitting_impl
    from bootstrap_wiki_model import (
        BOILERPLATE_PATTERNS,
        BOLD_HEADING_RE,
        CATALOG_PATH,
        COURSE_PAGE_SLUG_RE,
        GENERIC_BUCKET_SLUGS,
        HIGH_SIGNAL_INBOUND_THRESHOLD,
        INDEX_SECTION_ORDER,
        LECTURE_HEADING_RE,
        LECTURE_SOURCE_RE,
        PAGE_SHAPE_ATOMIC,
        PAGE_SHAPE_TOPIC,
        PAGE_SPLIT_MAX_BODY_CHARS,
        SOURCE_LINE_RE,
        TODAY,
        TOPIC_EXTRACTION_CONFIDENT_LEVELS,
        TOPIC_EXTRACTION_MAX_BODY_CHARS,
        BucketSignalResult,
        FetchResult,
        Page,
        PageSplitDecision,
        ParsedWikiPage,
        SourceArtifact,
        SourceCitation,
        SourceEvidence,
        SourceRecord,
        SplitCandidateEvaluation,
        SplitPhaseReport,
        classify_page,
        compact_source_text,
        ordered_unique,
        page_title,
        strip_markdown,
    )


ROOT = Path(__file__).resolve().parent.parent
RAW_ROOT = ROOT / "raw"
APPLE_NOTES_ROOT = RAW_ROOT / "Apple Notes"
WIKI_ROOT = ROOT / "wiki"
CACHE_ROOT = ROOT / ".wiki-bootstrap-cache"
CACHE_NOTES_ROOT = CACHE_ROOT / "notes"
CACHE_PAGES_ROOT = CACHE_ROOT / "pages"
CACHE_MANIFEST_PATH = CACHE_ROOT / "manifest.json"
RENDER_STAGE_ROOT = ROOT / ".wiki-render-staging"
RENDER_BACKUP_ROOT = ROOT / ".wiki-render-backup"

GENERIC_COMPONENTS = sources_impl.GENERIC_COMPONENTS
COMPONENT_ALIASES = sources_impl.COMPONENT_ALIASES


def configure_workspace(root: Path) -> None:
    global ROOT, RAW_ROOT, APPLE_NOTES_ROOT, WIKI_ROOT, CACHE_ROOT, CACHE_NOTES_ROOT
    global CACHE_PAGES_ROOT, CACHE_MANIFEST_PATH, RENDER_STAGE_ROOT, RENDER_BACKUP_ROOT

    ROOT = root
    RAW_ROOT = ROOT / "raw"
    APPLE_NOTES_ROOT = RAW_ROOT / "Apple Notes"
    WIKI_ROOT = ROOT / "wiki"
    CACHE_ROOT = ROOT / ".wiki-bootstrap-cache"
    CACHE_NOTES_ROOT = CACHE_ROOT / "notes"
    CACHE_PAGES_ROOT = CACHE_ROOT / "pages"
    CACHE_MANIFEST_PATH = CACHE_ROOT / "manifest.json"
    RENDER_STAGE_ROOT = ROOT / ".wiki-render-staging"
    RENDER_BACKUP_ROOT = ROOT / ".wiki-render-backup"


@contextmanager
def temporary_workspace(root: Path):
    with shared_temporary_workspace(_workspace_snapshot, configure_workspace, root):
        yield


def _workspace_snapshot() -> tuple[Path, ...]:
    return (
        ROOT,
        RAW_ROOT,
        APPLE_NOTES_ROOT,
        WIKI_ROOT,
        CACHE_ROOT,
        CACHE_NOTES_ROOT,
        CACHE_PAGES_ROOT,
        CACHE_MANIFEST_PATH,
        RENDER_STAGE_ROOT,
        RENDER_BACKUP_ROOT,
    )


def _api():
    return sys.modules[__name__]


def atomic_write_text(path: Path, content: str) -> None:
    shared_atomic_write_text(path, content)


def slugify(text: str) -> str:
    return sources_impl.slugify(text)


def looks_like_archive(component: str) -> bool:
    return sources_impl.looks_like_archive(component)


def clean_component(component: str) -> str | None:
    return sources_impl.clean_component(component)


def derive_note_title(path: Path, content: str) -> str:
    return sources_impl.derive_note_title(_api(), path, content)


def note_has_url(content: str) -> bool:
    return sources_impl.note_has_url(content)


def unwrap_export_url(url: str) -> str:
    return sources_impl.unwrap_export_url(url)


def extract_urls(content: str) -> list[str]:
    return sources_impl.extract_urls(content)


def extract_first_url(content: str) -> str | None:
    return sources_impl.extract_first_url(content)


def is_bare_url_note(content: str) -> bool:
    return sources_impl.is_bare_url_note(content)


def is_google_search_url(url: str) -> bool:
    return sources_impl.is_google_search_url(url)


def is_google_search_title(title: str) -> bool:
    return sources_impl.is_google_search_title(title)


def is_youtube_url(url: str) -> bool:
    return sources_impl.is_youtube_url(url)


def note_embed_names(content: str) -> list[str]:
    return sources_impl.note_embed_names(content)


def first_meaningful_snippet(content: str, title: str) -> str:
    return sources_impl.first_meaningful_snippet(_api(), content, title)


def clean_source_text(content: str, title: str) -> str:
    return sources_impl.clean_source_text(_api(), content, title)


def detect_source_tags(title: str, cleaned_text: str, url: str | None, fetched_summary: str | None) -> set[str]:
    return sources_impl.detect_source_tags(_api(), title, cleaned_text, url, fetched_summary)


def should_exclude_from_body(tags: set[str]) -> bool:
    return sources_impl.should_exclude_from_body(tags)


def prepare_source_record(**kwargs: object) -> SourceEvidence:
    return sources_impl.prepare_source_record(_api(), **kwargs)


def should_fold_note_into_parent(title: str, content: str, url: str | None) -> bool:
    return sources_impl.should_fold_note_into_parent(title, content, url)


def derive_path_topics(path: Path) -> list[str]:
    return sources_impl.derive_path_topics(_api(), path)


def parse_note_snippets(note_lines: list[str]) -> list[str]:
    return parsing_impl.parse_note_snippets(note_lines)


def parse_source_line(line: str, retained_evidence: str = "") -> SourceRecord | None:
    return parsing_impl.parse_source_line(line, retained_evidence)


def extract_connection_slugs(connection_lines: list[str]) -> list[str]:
    return parsing_impl.extract_connection_slugs(connection_lines)


def parse_page_file(path: Path, page_type: str | None = None) -> ParsedWikiPage:
    return parsing_impl.parse_page_file(path, page_type)


def parsed_page_to_page(parsed: ParsedWikiPage) -> Page:
    return parsing_impl.parsed_page_to_page(parsed)


def load_existing_page_types(index_path: Path) -> dict[str, str]:
    return parsing_impl.load_existing_page_types(index_path)


def load_page_types() -> dict[str, str]:
    return parsing_impl.load_page_types(WIKI_ROOT)


def load_existing_wiki_pages() -> dict[str, ParsedWikiPage]:
    return parsing_impl.load_existing_wiki_pages(WIKI_ROOT)


def page_shape(page: Page) -> str:
    return rendering_impl.page_shape(page)


def sorted_connection_slugs(page: Page, *, limit: int | None = None) -> list[str]:
    return rendering_impl.sorted_connection_slugs(page, limit=limit)


def render_connection_lines(page: Page, *, limit: int | None = 12) -> list[str]:
    return rendering_impl.render_connection_lines(page, limit=limit)


def render_source_lines(page: Page) -> list[str]:
    return rendering_impl.render_source_lines(page)


def build_simple_notes_markdown(page: Page) -> str:
    return rendering_impl.build_simple_notes_markdown(page)


def page_index_summary(page: Page) -> str:
    return rendering_impl.page_index_summary(page)


def inbound_link_counts(pages: dict[str, Page]) -> Counter:
    return rendering_impl.inbound_link_counts(pages)


def render_catalog(pages: dict[str, Page]) -> str:
    return rendering_impl.render_catalog(pages, today=TODAY)


def normalize_page(page: Page) -> Page:
    return rendering_impl.normalize_page(page)


def validate_page(page: Page, *, allow_missing_outbound: bool = False) -> list[str]:
    return rendering_impl.validate_page(page, allow_missing_outbound=allow_missing_outbound)


def render_page(page: Page) -> str:
    return rendering_impl.render_page(page)


def render_index(pages: dict[str, Page]) -> str:
    return rendering_impl.render_index(pages, today=TODAY)


def merge_page_content(target: Page, source_page: Page) -> None:
    engine_impl.merge_page_content(_api(), target, source_page)


def merge_existing_pages(pages: dict[str, Page], existing_pages: dict[str, ParsedWikiPage]) -> None:
    engine_impl.merge_existing_pages(_api(), pages, existing_pages)


def migrate_pages_to_atomic_topics(
    pages: dict[str, Page],
    existing_pages: dict[str, ParsedWikiPage],
    api_key: str | None = None,
    model: str = "gemini-2.5-flash",
    target_slugs: set[str] | None = None,
) -> SplitPhaseReport:
    return engine_impl.migrate_pages_to_atomic_topics(
        _api(),
        pages,
        existing_pages,
        api_key=api_key,
        model=model,
        target_slugs=target_slugs,
    )


def ensure_connection_targets_exist(pages: dict[str, Page]) -> None:
    engine_impl.ensure_connection_targets_exist(_api(), pages)


def related_slug_candidates(page: Page) -> list[str]:
    return engine_impl.related_slug_candidates(_api(), page)


def find_best_related_slug(page: Page, pages: dict[str, Page]) -> str | None:
    return engine_impl.find_best_related_slug(_api(), page, pages)


def prune_generic_media_links(pages: dict[str, Page]) -> None:
    engine_impl.prune_generic_media_links(pages)


def ensure_meaningful_connections(pages: dict[str, Page]) -> None:
    engine_impl.ensure_meaningful_connections(_api(), pages)


def finalize_page_shapes(pages: dict[str, Page]) -> None:
    engine_impl.finalize_page_shapes(_api(), pages)


def add_page_note(
    pages: dict[str, Page],
    slug: str,
    title: str,
    page_type: str,
    summary_hint: str,
    note_text: str,
    source_label: str,
    source_path: str,
    source_status: str,
    seed_kind: str,
) -> None:
    engine_impl.add_page_note(
        _api(),
        pages,
        slug,
        title,
        page_type,
        summary_hint,
        note_text,
        source_label,
        source_path,
        source_status,
        seed_kind,
    )


def ensure_supporting_pages(pages: dict[str, Page], slug: str, original_title: str, seed_kind: str) -> None:
    engine_impl.ensure_supporting_pages(_api(), pages, slug, original_title, seed_kind)


def apply_query_time_split_fix(
    pages: dict[str, Page],
    parent_slug: str,
    split_decision: PageSplitDecision,
    *,
    seed_kind: str = "query",
) -> bool:
    return engine_impl.apply_query_time_split_fix(_api(), pages, parent_slug, split_decision, seed_kind=seed_kind)


def maybe_apply_query_time_split_fix(
    parent_slug: str,
    *,
    split_decision: PageSplitDecision | None = None,
    api_key: str | None = None,
    model: str = "gemini-2.5-flash",
    mutation_note: str = "query-time split fix",
) -> bool:
    return engine_impl.maybe_apply_query_time_split_fix(
        _api(),
        parent_slug,
        split_decision=split_decision,
        api_key=api_key,
        model=model,
        mutation_note=mutation_note,
    )


def split_report_manifest_payload(report: SplitPhaseReport) -> dict[str, object]:
    return engine_impl.split_report_manifest_payload(report)


def split_report_summary(report: SplitPhaseReport) -> str:
    return engine_impl.split_report_summary(report)


def slug_similarity(left: str, right: str) -> float:
    return splitting_impl.slug_similarity(left, right)


def derive_source_atomic_slug(page: Page, source: SourceRecord) -> str:
    return splitting_impl.derive_source_atomic_slug(_api(), page, source)


def infer_split_candidate_for_source(source: SourceRecord, candidate_slugs: list[str]) -> str | None:
    return splitting_impl.infer_split_candidate_for_source(_api(), source, candidate_slugs)


def source_looks_like_lecture_material(source: SourceRecord, page_slug: str) -> bool:
    return splitting_impl.source_looks_like_lecture_material(_api(), source, page_slug)


def page_looks_like_course_notes(page: Page) -> bool:
    return splitting_impl.page_looks_like_course_notes(_api(), page)


def score_bucket_signals(page: Page) -> BucketSignalResult:
    return splitting_impl.score_bucket_signals(_api(), page)


def add_source_to_page(page: Page, source: SourceRecord, seed_kind: str = "migration", *, append_excerpt: bool = True) -> None:
    splitting_impl.add_source_to_page(_api(), page, source, seed_kind, append_excerpt=append_excerpt)


def connect_pages(pages: dict[str, Page], left: str, right: str) -> None:
    splitting_impl.connect_pages(pages, left, right)


def gather_split_source_groups(page: Page, split_decision: PageSplitDecision) -> tuple[dict[str, list[SourceRecord]], set[str]]:
    return splitting_impl.gather_split_source_groups(_api(), page, split_decision)


def split_candidate_evaluation_map(split_decision: PageSplitDecision) -> dict[str, SplitCandidateEvaluation]:
    return splitting_impl.split_candidate_evaluation_map(split_decision)


def grounded_split_candidate_slugs(split_decision: PageSplitDecision) -> list[str]:
    return splitting_impl.grounded_split_candidate_slugs(_api(), split_decision)


def build_split_child_notes(split_decision: PageSplitDecision, child_slug: str) -> list[str]:
    return splitting_impl.build_split_child_notes(split_decision, child_slug)


def normalize_split_note_text(text: str) -> str:
    return splitting_impl.normalize_split_note_text(text)


def split_child_note_signature(split_decision: PageSplitDecision, child_slug: str) -> str:
    return splitting_impl.split_child_note_signature(split_decision, child_slug)


def validate_split_child_grounding(
    page: Page,
    split_decision: PageSplitDecision,
    source_groups: dict[str, list[SourceRecord]],
) -> PageSplitDecision:
    return splitting_impl.validate_split_child_grounding(_api(), page, split_decision, source_groups)


def page_title_is_source_shaped(page: Page) -> bool:
    return splitting_impl.page_title_is_source_shaped(_api(), page)


def resolve_parent_split_mode(page: Page, child_slugs: list[str]) -> str:
    return splitting_impl.resolve_parent_split_mode(_api(), page, child_slugs)


def apply_split_decision(
    pages: dict[str, Page],
    parent_slug: str,
    split_decision: PageSplitDecision,
    *,
    seed_kind: str = "migration",
    allow_partial_source_coverage: bool = False,
) -> bool:
    return splitting_impl.apply_split_decision(
        _api(),
        pages,
        parent_slug,
        split_decision,
        seed_kind=seed_kind,
        allow_partial_source_coverage=allow_partial_source_coverage,
    )


def stable_json_dumps(payload: object) -> str:
    return cache_impl.stable_json_dumps(_api(), payload)


def fingerprint_payload(payload: object) -> str:
    return cache_impl.fingerprint_payload(_api(), payload)


def cache_key(value: str) -> str:
    return cache_impl.cache_key(_api(), value)


def atomic_write_json(path: Path, payload: object) -> None:
    cache_impl.atomic_write_json(_api(), path, payload)


def read_json_file(path: Path) -> dict[str, object] | None:
    return cache_impl.read_json_file(_api(), path)


def note_cache_path(source_path: str) -> Path:
    return cache_impl.note_cache_path(_api(), source_path)


def page_cache_path(slug: str) -> Path:
    return cache_impl.page_cache_path(_api(), slug)


def update_manifest(**fields: object) -> None:
    cache_impl.update_manifest(_api(), **fields)


def source_record_to_cache_dict(source: SourceRecord) -> dict[str, object]:
    return cache_impl.source_record_to_cache_dict(_api(), source)


def source_record_from_cache_dict(payload: dict[str, object]) -> SourceRecord:
    return cache_impl.source_record_from_cache_dict(_api(), payload)


def build_note_fingerprint(path: Path, content: str, fetch_urls_enabled: bool) -> str:
    return cache_impl.build_note_fingerprint(_api(), path, content, fetch_urls_enabled)


def build_note_cache_entry(
    *,
    fingerprint: str,
    title: str,
    source_record: SourceRecord,
    note_text: str,
    page_assignments: list[tuple[str, str]],
    skipped: bool,
) -> dict[str, object]:
    return cache_impl.build_note_cache_entry(
        _api(),
        fingerprint=fingerprint,
        title=title,
        source_record=source_record,
        note_text=note_text,
        page_assignments=page_assignments,
        skipped=skipped,
    )


def build_page_fingerprint(page: Page) -> str:
    return cache_impl.build_page_fingerprint(_api(), page)


def apply_note_cache_entry_to_pages(*, pages: dict[str, Page], cache_entry: dict[str, object]) -> None:
    cache_impl.apply_note_cache_entry_to_pages(_api(), pages=pages, cache_entry=cache_entry)


def accumulate_url_stats(url_stats: Counter, source: SourceRecord, fetch_urls_enabled: bool) -> None:
    cache_impl.accumulate_url_stats(_api(), url_stats, source, fetch_urls_enabled)


def stage_rendered_wiki(*, pages: dict[str, Page], existing_log_text: str, bootstrap_entry: str) -> Path:
    return cache_impl.stage_rendered_wiki(_api(), pages=pages, existing_log_text=existing_log_text, bootstrap_entry=bootstrap_entry)


def append_wiki_query_log(summary: str) -> None:
    cache_impl.append_wiki_query_log(_api(), summary)


def swap_rendered_wiki(stage_dir: Path) -> None:
    cache_impl.swap_rendered_wiki(_api(), stage_dir)


def source_priority(source: SourceRecord) -> tuple[int, int, str]:
    return remote_impl.source_priority(_api(), source)


def select_sources_for_synthesis(page: Page, max_sources: int = 40, max_chars: int = 18_000) -> list[SourceRecord]:
    return remote_impl.select_sources_for_synthesis(_api(), page, max_sources, max_chars)


def serialize_sources_for_prompt(page: Page) -> str:
    return remote_impl.serialize_sources_for_prompt(_api(), page)


def build_synthesis_messages(page: Page) -> list[dict[str, str]]:
    return remote_impl.build_synthesis_messages(_api(), page)


def parse_synthesis_response(content: str) -> tuple[str, str]:
    return remote_impl.parse_synthesis_response(_api(), content)


def gemini_generate(
    *,
    messages: list[dict[str, str]],
    api_key: str,
    model: str,
    response_schema: dict[str, object],
    attempts: int = 4,
    timeout: float = 90,
) -> str:
    return remote_impl.gemini_generate(
        _api(),
        messages=messages,
        api_key=api_key,
        model=model,
        response_schema=response_schema,
        attempts=attempts,
        timeout=timeout,
    )


def synthesize_page(page: Page, api_key: str, model: str) -> tuple[str, str]:
    return remote_impl.synthesize_page(_api(), page, api_key, model)


def build_topic_extraction_messages(
    *,
    title: str,
    cleaned_text: str,
    fetched_summary: str | None,
    detected_url: str | None,
) -> list[dict[str, str]]:
    return remote_impl.build_topic_extraction_messages(
        _api(),
        title=title,
        cleaned_text=cleaned_text,
        fetched_summary=fetched_summary,
        detected_url=detected_url,
    )


def parse_topic_extraction_response(content: str) -> list[tuple[str, str]]:
    return remote_impl.parse_topic_extraction_response(_api(), content)


def extract_note_topics(*, title: str, source_record: SourceRecord, api_key: str, model: str) -> list[str]:
    return remote_impl.extract_note_topics(_api(), title=title, source_record=source_record, api_key=api_key, model=model)


def build_page_split_messages(page: Page) -> list[dict[str, str]]:
    return remote_impl.build_page_split_messages(_api(), page)


def parse_page_split_response(content: str, parent_slug: str, source_paths: set[str]) -> PageSplitDecision:
    return remote_impl.parse_page_split_response(_api(), content, parent_slug, source_paths)


def split_failure_mode() -> str:
    return remote_impl.split_failure_mode(_api())


def split_request_timeout() -> float:
    return remote_impl.split_request_timeout(_api())


def split_request_attempts() -> int:
    return remote_impl.split_request_attempts(_api())


def split_debug_enabled() -> bool:
    return remote_impl.split_debug_enabled(_api())


def format_split_decision_debug(page: Page, split_decision: PageSplitDecision) -> str:
    return remote_impl.format_split_decision_debug(_api(), page, split_decision)


def split_counts_toward_transport_abort(error: Exception) -> bool:
    return remote_impl.split_counts_toward_transport_abort(_api(), error)


def split_preflight_check(api_key: str | None, model: str, timeout: float = 5.0) -> tuple[bool, str | None]:
    return remote_impl.split_preflight_check(_api(), api_key, model, timeout)


def analyze_page_for_atomic_split(page: Page, api_key: str | None, model: str) -> PageSplitDecision:
    return remote_impl.analyze_page_for_atomic_split(_api(), page, api_key, model)


def summarize_remote_page(html_text: str) -> str | None:
    return remote_impl.summarize_remote_page(_api(), html_text)


def fetch_youtube_oembed_summary(url: str, timeout: float = 8.0) -> FetchResult:
    return remote_impl.fetch_youtube_oembed_summary(_api(), url, timeout)


def fetch_url_summary(url: str, timeout: float = 8.0) -> FetchResult:
    return remote_impl.fetch_url_summary(_api(), url, timeout)


def parse_requested_slugs(raw_value: str | None) -> set[str]:
    return cli_impl.parse_requested_slugs(_api(), raw_value)


def manifest_failed_split_slugs() -> set[str]:
    return cli_impl.manifest_failed_split_slugs(_api())


def run_split_only(
    *,
    api_key: str | None,
    model: str,
    target_slugs: set[str] | None,
    retry_failed_splits: bool,
) -> None:
    cli_impl.run_split_only(
        _api(),
        api_key=api_key,
        model=model,
        target_slugs=target_slugs,
        retry_failed_splits=retry_failed_splits,
    )


def main() -> None:
    cli_impl.main(_api())


if __name__ == "__main__":
    main()
