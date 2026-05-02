from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from types import ModuleType
from urllib.error import HTTPError, URLError
import json
import socket
import sys


def merge_page_content(api: ModuleType, target: object, source_page: object) -> None:
    if source_page.shape == api.PAGE_SHAPE_TOPIC:
        target.shape = api.PAGE_SHAPE_TOPIC
    for note in source_page.notes:
        if note not in target.notes:
            target.notes.append(note)
    for question in source_page.open_questions:
        if question not in target.open_questions:
            target.open_questions.append(question)
    if not target.rendered_notes_markdown and source_page.rendered_notes_markdown:
        target.rendered_notes_markdown = source_page.rendered_notes_markdown
    for source_path, source in source_page.sources.items():
        if source_path not in target.sources:
            target.sources[source_path] = source
    for slug, count in source_page.connections.items():
        if slug != target.slug:
            target.connections[slug] += count
    target.seed_kinds.update(source_page.seed_kinds)


def merge_existing_pages(api: ModuleType, pages: dict[str, object], existing_pages: dict[str, object]) -> None:
    for slug, parsed in existing_pages.items():
        existing_page = api.parsed_page_to_page(parsed)
        if slug not in pages:
            pages[slug] = existing_page
            continue
        merge_page_content(api, pages[slug], existing_page)


def migrate_pages_to_atomic_topics(
    api: ModuleType,
    pages: dict[str, object],
    existing_pages: dict[str, object],
    api_key: str | None = None,
    model: str = "gemini-2.5-flash",
    target_slugs: set[str] | None = None,
) -> object:
    report = api.SplitPhaseReport(failure_mode=api.split_failure_mode())
    for slug, parsed in existing_pages.items():
        if slug not in pages:
            continue
        page = pages[slug]
        if parsed.shape == api.PAGE_SHAPE_TOPIC:
            page.shape = api.PAGE_SHAPE_TOPIC
            page.notes = []
            page.rendered_notes_markdown = None
            page.sources = {}
            for child_slug in parsed.connection_slugs:
                if child_slug in pages:
                    api.connect_pages(pages, slug, child_slug)

    eligible_slugs = [
        slug
        for slug in sorted(pages)
        if pages[slug].shape != api.PAGE_SHAPE_TOPIC
        and len(pages[slug].sources) >= 2
        and (target_slugs is None or slug in target_slugs)
    ]
    report.eligible_pages = len(eligible_slugs)
    if not eligible_slugs:
        print("Split phase: no eligible pages.", file=sys.stderr)
        return report

    preflight_ok, preflight_reason = api.split_preflight_check(api_key, model)
    if not preflight_ok:
        report.mode = "no-split"
        report.status = "skipped_preflight"
        report.reason = preflight_reason
        print(
            f"Split phase: skipped {report.eligible_pages} eligible pages; preflight failed: {preflight_reason}",
            file=sys.stderr,
        )
        if report.failure_mode == "fail":
            raise RuntimeError(f"Split phase preflight failed: {preflight_reason}")
        return report

    print(f"Split phase: analyzing {report.eligible_pages} eligible pages.", file=sys.stderr)
    consecutive_failures = 0
    max_consecutive_failures = 3

    for slug in eligible_slugs:
        page = pages[slug]
        if api.page_looks_like_course_notes(page):
            report.atomic_pages += 1
            print(f"Split phase: '{slug}' -> atomic (course lecture guard).", file=sys.stderr)
            continue
        bucket_signals = api.score_bucket_signals(page)
        if bucket_signals.is_bucket_signaled:
            detail = f"{slug}: {', '.join(bucket_signals.reasons)}"
            report.bucket_signaled_details.append(detail)

        print(f"Split phase: analyzing '{slug}' ({len(page.sources)} sources).", file=sys.stderr)
        try:
            split_decision = api.analyze_page_for_atomic_split(page, api_key, model)
            report.analyzed_pages += 1
            consecutive_failures = 0
            if api.split_debug_enabled():
                print(api.format_split_decision_debug(page, split_decision), file=sys.stderr)
        except (HTTPError, URLError, TimeoutError, socket.timeout, json.JSONDecodeError, ValueError) as error:
            report.failed_pages += 1
            detail = f"{slug}: {type(error).__name__}: {error}"
            report.failure_details.append(detail)
            print(f"Split phase: failed '{slug}': {detail}", file=sys.stderr)
            if api.split_counts_toward_transport_abort(error):
                consecutive_failures += 1
            else:
                consecutive_failures = 0
            if consecutive_failures >= max_consecutive_failures:
                report.mode = "no-split"
                report.status = "aborted_transport_failure"
                report.aborted = True
                report.reason = f"{consecutive_failures} consecutive split failures"
                print(f"Split phase: aborting early after {consecutive_failures} consecutive failures.", file=sys.stderr)
                if report.failure_mode == "fail":
                    raise RuntimeError(f"Split phase aborted: {report.reason}")
                return report
            continue
        if split_decision.is_atomic:
            if bucket_signals.is_bucket_signaled:
                report.failed_pages += 1
                detail = f"{slug}: bucket-unsplit (model_kept_atomic; signals={', '.join(bucket_signals.reasons)})"
                report.bucket_unsplit_details.append(detail)
                report.failure_details.append(detail)
                print(f"Split phase: '{slug}' -> bucket-unsplit (model_kept_atomic).", file=sys.stderr)
                continue
            report.atomic_pages += 1
            print(f"Split phase: '{slug}' -> atomic.", file=sys.stderr)
            continue
        if len(split_decision.candidate_satellite_slugs) < 2:
            if bucket_signals.is_bucket_signaled:
                report.failed_pages += 1
                detail = f"{slug}: bucket-unsplit (insufficient satellites; signals={', '.join(bucket_signals.reasons)})"
                report.bucket_unsplit_details.append(detail)
                report.failure_details.append(detail)
                print(f"Split phase: '{slug}' -> bucket-unsplit (insufficient satellites).", file=sys.stderr)
                continue
            report.incomplete_pages += 1
            detail = f"{slug}: insufficient satellites"
            report.incomplete_details.append(detail)
            print(f"Split phase: '{slug}' -> incomplete (insufficient satellites).", file=sys.stderr)
            continue

        source_groups, assigned_source_paths = api.gather_split_source_groups(page, split_decision)
        if len(source_groups) < 2 or len(assigned_source_paths) != len(page.sources):
            reason = "incomplete assignments" if len(source_groups) < 2 else "partial coverage"
            if bucket_signals.is_bucket_signaled:
                report.failed_pages += 1
                detail = f"{slug}: bucket-unsplit ({reason}; signals={', '.join(bucket_signals.reasons)})"
                report.bucket_unsplit_details.append(detail)
                report.failure_details.append(detail)
                print(f"Split phase: '{slug}' -> bucket-unsplit ({reason}).", file=sys.stderr)
                continue
            report.incomplete_pages += 1
            detail = f"{slug}: {reason}"
            report.incomplete_details.append(detail)
            print(f"Split phase: '{slug}' -> incomplete ({reason}).", file=sys.stderr)
            continue

        page.shape = api.PAGE_SHAPE_TOPIC
        page.notes = []
        page.rendered_notes_markdown = None
        page.sources = {}
        page.connections = Counter()

        for child_slug, grouped_sources in sorted(source_groups.items()):
            child_page = pages.setdefault(
                child_slug,
                api.Page(
                    slug=child_slug,
                    title=api.page_title(child_slug),
                    page_type=api.classify_page(child_slug, grouped_sources[0].label, "migration"),
                    summary_hint=grouped_sources[0].label,
                ),
            )
            child_page.shape = api.PAGE_SHAPE_ATOMIC
            child_page.topic_parent = slug
            for source in grouped_sources:
                api.add_source_to_page(child_page, source)
            api.connect_pages(pages, slug, child_slug)
        report.split_pages += 1
        print(f"Split phase: '{slug}' -> split into {', '.join(sorted(source_groups))}.", file=sys.stderr)

    return report


def ensure_connection_targets_exist(api: ModuleType, pages: dict[str, object]) -> None:
    missing_targets: defaultdict[str, list[str]] = defaultdict(list)
    for page in pages.values():
        for slug in api.sorted_connection_slugs(page):
            if slug not in pages:
                missing_targets[slug].append(page.slug)
    for slug, referrers in missing_targets.items():
        stub = api.Page(
            slug=slug,
            title=api.page_title(slug),
            page_type=api.classify_page(slug, api.page_title(slug), "migration"),
            summary_hint=api.page_title(slug),
            shape=api.PAGE_SHAPE_TOPIC,
        )
        for referrer in api.ordered_unique(referrers):
            stub.connections[referrer] += 1
        pages[slug] = stub


def related_slug_candidates(api: ModuleType, page: object) -> list[str]:
    candidates: list[str] = []
    for source in page.sources.values():
        try:
            if source.path.startswith("../"):
                source_path = (api.ROOT / source.path[3:]).resolve()
            else:
                source_path = (api.ROOT / source.path).resolve()
        except Exception:
            continue
        for topic in api.derive_path_topics(source_path):
            if topic != page.slug and topic not in candidates:
                candidates.append(topic)
    return candidates


def find_best_related_slug(api: ModuleType, page: object, pages: dict[str, object]) -> str | None:
    best_slug: str | None = None
    best_score = 0.0
    for other_slug, other_page in pages.items():
        if other_slug == page.slug:
            continue
        score = api.slug_similarity(page.slug, other_slug)
        score += 0.2 * len(page.seed_kinds & other_page.seed_kinds)
        if score > best_score:
            best_score = score
            best_slug = other_slug
    if best_score > 0:
        return best_slug
    for topic in related_slug_candidates(api, page):
        if topic in pages and topic != page.slug:
            return topic
    return None


def prune_generic_media_links(pages: dict[str, object]) -> None:
    media_slug = "unclassified-media-captures"
    media_suffixes = {".png", ".jpg", ".jpeg", ".heic", ".pdf"}
    for slug, page in pages.items():
        if slug == media_slug:
            continue
        has_media_source = any(Path(source.path).suffix.lower() in media_suffixes for source in page.sources.values())
        if not has_media_source:
            page.connections.pop(media_slug, None)


def ensure_meaningful_connections(api: ModuleType, pages: dict[str, object]) -> None:
    ensure_connection_targets_exist(api, pages)
    for slug, page in pages.items():
        if slug in {"index", "log"}:
            continue
        if api.sorted_connection_slugs(page):
            continue
        related_slug = page.topic_parent if page.topic_parent in pages else find_best_related_slug(api, page, pages)
        if related_slug:
            api.connect_pages(pages, slug, related_slug)


def finalize_page_shapes(api: ModuleType, pages: dict[str, object]) -> None:
    for page in pages.values():
        if api.page_shape(page) == api.PAGE_SHAPE_TOPIC:
            page.shape = api.PAGE_SHAPE_TOPIC
            page.notes = []
            page.rendered_notes_markdown = None
            page.open_questions = []
            page.sources = {}
        else:
            page.shape = api.PAGE_SHAPE_ATOMIC
            page.notes = api.ordered_unique(page.notes)
            page.open_questions = api.ordered_unique(page.open_questions)
            page.rendered_notes_markdown = api.build_simple_notes_markdown(page) or None


def add_page_note(
    api: ModuleType,
    pages: dict[str, object],
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
    page = pages.setdefault(slug, api.Page(slug=slug, title=title, page_type=page_type, summary_hint=summary_hint))
    page.seed_kinds.add(seed_kind)
    if note_text not in page.notes:
        page.notes.append(note_text)
    existing_source = page.sources.get(source_path)
    if existing_source is None:
        page.sources[source_path] = api.SourceEvidence(
            label=source_label,
            path=source_path,
            status=source_status,
            cleaned_text=note_text,
            fetched_summary=None,
            detected_url=None,
        )
    else:
        existing_source.label = source_label
        existing_source.status = source_status


def ensure_supporting_pages(api: ModuleType, pages: dict[str, object], slug: str, original_title: str, seed_kind: str) -> None:
    pages.setdefault(
        slug,
        api.Page(
            slug=slug,
            title=api.page_title(slug),
            page_type=api.classify_page(slug, original_title, seed_kind),
            summary_hint=original_title,
        ),
    ).seed_kinds.add(seed_kind)


def apply_query_time_split_fix(
    api: ModuleType,
    pages: dict[str, object],
    parent_slug: str,
    split_decision: object,
    *,
    seed_kind: str = "query",
) -> bool:
    child_slugs = api.ordered_unique(split_decision.candidate_satellite_slugs)
    if len(child_slugs) < 2:
        return False
    evaluation_map = {evaluation.slug: evaluation for evaluation in split_decision.candidate_evaluations}
    for child_slug in child_slugs:
        evaluation = evaluation_map.get(child_slug)
        if evaluation is None or not evaluation.accepted or not evaluation.grounding:
            return False
        if not evaluation.passes_direct_link_test or not evaluation.passes_stable_page_test:
            return False
    return api.apply_split_decision(
        pages,
        parent_slug,
        split_decision,
        seed_kind=seed_kind,
        allow_partial_source_coverage=False,
    )


def maybe_apply_query_time_split_fix(
    api: ModuleType,
    parent_slug: str,
    *,
    split_decision: object | None = None,
    api_key: str | None = None,
    model: str = "gemini-2.5-flash",
    mutation_note: str = "query-time split fix",
) -> bool:
    parent_path = api.WIKI_ROOT / f"{parent_slug}.md"
    if not parent_path.exists():
        return False
    pages = {slug: api.parsed_page_to_page(parsed) for slug, parsed in api.load_existing_wiki_pages().items()}
    parent_page = pages.get(parent_slug)
    if parent_page is None:
        return False

    effective_split_decision = split_decision or api.analyze_page_for_atomic_split(parent_page, api_key, model)
    child_slugs = api.ordered_unique(effective_split_decision.candidate_satellite_slugs)
    touched_slugs = {parent_slug, *child_slugs}
    before_page_text = {
        slug: (api.WIKI_ROOT / f"{slug}.md").read_text(encoding="utf-8")
        if (api.WIKI_ROOT / f"{slug}.md").exists()
        else None
        for slug in touched_slugs
    }
    index_path = api.WIKI_ROOT / "index.md"
    before_index_text = index_path.read_text(encoding="utf-8") if index_path.exists() else None

    applied = apply_query_time_split_fix(api, pages, parent_slug, effective_split_decision, seed_kind="query")
    if not applied:
        return False

    changed_page_slugs: list[str] = []
    for slug in sorted(touched_slugs):
        page = pages.get(slug)
        if page is None:
            continue
        rendered = api.render_page(page)
        if before_page_text.get(slug) == rendered:
            continue
        api.atomic_write_text(api.WIKI_ROOT / f"{slug}.md", rendered)
        changed_page_slugs.append(slug)

    rendered_index = api.render_index(pages)
    rendered_catalog = api.render_catalog(pages)
    catalog_path = api.WIKI_ROOT / api.CATALOG_PATH
    before_catalog_text = catalog_path.read_text(encoding="utf-8") if catalog_path.exists() else None
    if not changed_page_slugs:
        return False
    if before_index_text != rendered_index:
        api.atomic_write_text(index_path, rendered_index)
    if before_catalog_text != rendered_catalog:
        api.atomic_write_text(catalog_path, rendered_catalog)
    api.append_wiki_query_log(f"{mutation_note} — {parent_slug} -> {', '.join(child_slugs[:8])}")
    return True


def split_report_manifest_payload(report: object) -> dict[str, object]:
    return {
        "mode": report.mode,
        "status": report.status,
        "failure_mode": report.failure_mode,
        "eligible_pages": report.eligible_pages,
        "analyzed_pages": report.analyzed_pages,
        "atomic_pages": report.atomic_pages,
        "incomplete_pages": report.incomplete_pages,
        "split_pages": report.split_pages,
        "failed_pages": report.failed_pages,
        "aborted": report.aborted,
        "reason": report.reason,
        "failure_details": report.failure_details,
        "incomplete_details": report.incomplete_details,
        "bucket_signaled_details": report.bucket_signaled_details,
        "bucket_unsplit_details": report.bucket_unsplit_details,
    }


def split_report_summary(report: object) -> str:
    summary = (
        f"split phase: {report.status}, eligible {report.eligible_pages}, "
        f"analyzed {report.analyzed_pages}, split {report.split_pages}, "
        f"atomic {report.atomic_pages}, incomplete {report.incomplete_pages}, failed {report.failed_pages}"
    )
    if report.mode != "performed":
        summary += f", mode {report.mode}"
    if report.reason:
        summary += f" ({report.reason})"
    return summary
