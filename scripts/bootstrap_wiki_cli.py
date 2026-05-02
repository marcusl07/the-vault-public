from __future__ import annotations

from collections import Counter
import argparse
import os
import sys
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


def parse_requested_slugs(api: ModuleType, raw_value: str | None) -> set[str]:
    if not raw_value:
        return set()
    slugs: set[str] = set()
    for raw_slug in raw_value.split(","):
        cleaned = api.clean_component(raw_slug.strip()) or api.slugify(raw_slug.strip())
        if cleaned:
            slugs.add(cleaned)
    return slugs


def manifest_failed_split_slugs(api: ModuleType) -> set[str]:
    manifest = api.read_json_file(api.CACHE_MANIFEST_PATH)
    if not isinstance(manifest, dict):
        return set()
    split_phase = manifest.get("split_phase")
    if not isinstance(split_phase, dict):
        return set()
    failed: set[str] = set()
    for key in ("failure_details", "incomplete_details", "bucket_signaled_details", "bucket_unsplit_details"):
        details = split_phase.get(key)
        if not isinstance(details, list):
            continue
        for detail in details:
            if not isinstance(detail, str):
                continue
            slug, _separator, _rest = detail.partition(":")
            slug = slug.strip()
            if slug:
                failed.add(slug)
    return failed


def run_split_only(api: ModuleType, *, api_key: str | None, model: str, target_slugs: set[str] | None, retry_failed_splits: bool) -> None:
    existing_pages = api.load_existing_wiki_pages()
    if not existing_pages:
        raise RuntimeError("Split-only mode requires an existing wiki to load.")

    selected_slugs = set(target_slugs or set())
    if retry_failed_splits:
        selected_slugs.update(api.manifest_failed_split_slugs())
    if not selected_slugs:
        raise RuntimeError("Split-only mode requires --pages or --retry-failed-splits.")

    pages = {slug: api.parsed_page_to_page(parsed) for slug, parsed in existing_pages.items()}
    api.update_manifest(
        phase="split_only",
        model=model,
        failure=None,
        split_targets=sorted(selected_slugs),
        split_phase={"status": "starting", "target_slugs": sorted(selected_slugs)},
    )

    split_report = api.migrate_pages_to_atomic_topics(
        pages,
        existing_pages,
        api_key=api_key,
        model=model,
        target_slugs=selected_slugs,
    )
    split_manifest = api.split_report_manifest_payload(split_report)
    split_manifest["target_slugs"] = sorted(selected_slugs)
    api.update_manifest(phase="split_only", split_phase=split_manifest, failure=split_report.reason)

    api.prune_generic_media_links(pages)
    api.ensure_meaningful_connections(pages)
    api.finalize_page_shapes(pages)

    existing_log = (api.WIKI_ROOT / "log.md").read_text(encoding="utf-8") if (api.WIKI_ROOT / "log.md").exists() else "# Wiki Log\n\n"
    bootstrap_entry = (
        f'## [{api.TODAY}] bootstrap | split-only — {len(selected_slugs)} requested pages; '
        f'{api.split_report_summary(split_report)}; targets: {", ".join(sorted(selected_slugs))}\n'
    )
    stage_dir = api.stage_rendered_wiki(pages=pages, existing_log_text=existing_log, bootstrap_entry=bootstrap_entry)
    api.swap_rendered_wiki(stage_dir)
    api.update_manifest(phase="completed", processed_pages=len(pages), split_phase=split_manifest, failure=None)


def main(api: ModuleType) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fetch-urls", action="store_true", help="Fetch remote title/meta for URL notes.")
    parser.add_argument("--only-youtube", action="store_true", help="Process only raw markdown notes containing YouTube URLs.")
    parser.add_argument("--split-only", action="store_true", help="Retry page splitting using the existing wiki only.")
    parser.add_argument("--pages", help="Comma-separated page slugs to target during split-only mode.")
    parser.add_argument(
        "--retry-failed-splits",
        action="store_true",
        help="In split-only mode, retry slugs recorded as failed in the manifest.",
    )
    args = parser.parse_args()

    api.WIKI_ROOT.mkdir(exist_ok=True)
    api.CACHE_NOTES_ROOT.mkdir(parents=True, exist_ok=True)
    api.CACHE_PAGES_ROOT.mkdir(parents=True, exist_ok=True)
    api_key = os.environ.get("GEMINI_API_KEY")
    model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
    target_slugs = api.parse_requested_slugs(args.pages)

    if args.split_only:
        api.run_split_only(api_key=api_key, model=model, target_slugs=target_slugs, retry_failed_splits=args.retry_failed_splits)
        return

    pages: dict[str, object] = {}
    existing_pages = api.load_existing_wiki_pages()
    markdown_files = sorted(path for path in api.RAW_ROOT.rglob("*.md") if path.is_file())
    if args.only_youtube:
        markdown_files = [
            path
            for path in markdown_files
            if any(api.is_youtube_url(url) for url in api.extract_urls(path.read_text(errors="ignore")))
        ]
        media_files = []
    else:
        media_files = sorted(
            path
            for path in api.RAW_ROOT.rglob("*")
            if path.is_file() and path.suffix.lower() in {".png", ".jpg", ".jpeg", ".heic", ".pdf"}
        )

    embedded_media = set()
    url_stats = Counter()
    api.update_manifest(
        phase="notes",
        total_notes=len(markdown_files),
        total_media=len(media_files),
        processed_notes=0,
        processed_pages=0,
        model=model,
        fetch_urls=args.fetch_urls,
        failure=None,
        last_note=None,
        last_page=None,
        split_phase=None,
    )

    for index, path in enumerate(markdown_files, start=1):
        content = path.read_text(errors="ignore")
        title = api.derive_note_title(path, content).strip() or path.stem
        embedded_media.update(api.note_embed_names(content))
        source_path = "../" + path.relative_to(api.ROOT).as_posix()
        note_fingerprint = api.build_note_fingerprint(path, content, args.fetch_urls)
        cache_path = api.note_cache_path(source_path)
        cached_entry = api.read_json_file(cache_path)
        cache_hit = isinstance(cached_entry, dict) and cached_entry.get("fingerprint") == note_fingerprint
        if cache_hit:
            try:
                api.apply_note_cache_entry_to_pages(pages=pages, cache_entry=cached_entry)
                source_payload = cached_entry.get("source_record")
                if isinstance(source_payload, dict):
                    api.accumulate_url_stats(url_stats, api.source_record_from_cache_dict(source_payload), args.fetch_urls)
            except Exception:
                cache_hit = False

        if not cache_hit:
            path_topics = api.derive_path_topics(path)
            url = api.extract_first_url(content)
            fold_into_parent = api.should_fold_note_into_parent(title, content, url)

            source_label = title
            source_status = "local_only"
            fetched_summary = None
            if args.fetch_urls:
                if url:
                    fetch_result = api.fetch_url_summary(url)
                    source_status = fetch_result.status
                    fetched_summary = fetch_result.summary
            else:
                if url:
                    source_status = "fetch_skipped"
            if args.fetch_urls and not url and api.note_has_url(content):
                source_status = "fetch_failed"

            source_record = api.prepare_source_record(
                source_label=source_label,
                source_path=source_path,
                source_status=source_status,
                raw_content=content,
                fetched_summary=fetched_summary,
                detected_url=url,
            )
            api.accumulate_url_stats(url_stats, source_record, args.fetch_urls)

            extracted_topics: list[str] = []
            if api_key:
                try:
                    extracted_topics = api.extract_note_topics(title=title, source_record=source_record, api_key=api_key, model=model)
                except Exception as error:
                    api.update_manifest(failure=f"topic extraction fallback for {source_path}: {error}", last_note=source_path, processed_notes=index - 1)
                    print(f"Topic extraction fallback for '{source_path}': {error}", file=sys.stderr)
                    extracted_topics = []

            page_slugs: list[tuple[str, str]] = [(slug, "model") for slug in extracted_topics]
            if not page_slugs:
                for topic_slug in path_topics[:2]:
                    page_slugs.append((topic_slug, "folder"))

            if fold_into_parent and page_slugs:
                page_slugs = [page_slugs[0]]

            skipped = False
            if not page_slugs:
                if fold_into_parent:
                    skipped = True
                else:
                    page_slugs.append(("uncategorized-captures", "folder"))

            note_text = api.first_meaningful_snippet(content, title)
            if fetched_summary:
                note_text = fetched_summary

            cache_entry = api.build_note_cache_entry(
                fingerprint=note_fingerprint,
                title=title,
                source_record=source_record,
                note_text=note_text,
                page_assignments=page_slugs,
                skipped=skipped,
            )
            api.atomic_write_json(cache_path, cache_entry)
            api.apply_note_cache_entry_to_pages(pages=pages, cache_entry=cache_entry)

        api.update_manifest(processed_notes=index, last_note=source_path, failure=None)
        if index % 50 == 0 or index == len(markdown_files):
            print(f"Processed notes: {index}/{len(markdown_files)}", file=sys.stderr)

    media_page_slug = "unclassified-media-captures"
    api.ensure_supporting_pages(pages, media_page_slug, "Unclassified media captures", "folder")
    api.update_manifest(phase="media")

    for media_path in media_files:
        if media_path.name in embedded_media:
            continue
        source_path = "../" + media_path.relative_to(api.ROOT).as_posix()
        note_text = f"Standalone raw media capture: {media_path.name}."
        api.add_page_note(
            pages=pages,
            slug=media_page_slug,
            title=api.page_title(media_page_slug),
            page_type="Concepts",
            summary_hint="Unclassified media captures",
            note_text=note_text,
            source_label=media_path.name,
            source_path=source_path,
            source_status="local_only",
            seed_kind="folder",
        )
        pages[media_page_slug].sources[source_path] = api.prepare_source_record(
            source_label=media_path.name,
            source_path=source_path,
            source_status="local_only",
            raw_content=note_text,
            fetched_summary=None,
            detected_url=None,
        )

    api.merge_existing_pages(pages, existing_pages)
    if args.only_youtube:
        split_report = api.SplitPhaseReport(
            mode="skipped",
            status="skipped",
            reason="only-youtube mode skips global split analysis",
        )
        split_manifest = api.split_report_manifest_payload(split_report)
        api.update_manifest(phase="split", split_phase=split_manifest, failure=None)
    else:
        api.update_manifest(phase="split", split_phase={"status": "starting"}, failure=None)
        split_report = api.migrate_pages_to_atomic_topics(pages, existing_pages, api_key=api_key, model=model)
        split_manifest = api.split_report_manifest_payload(split_report)
        api.update_manifest(phase="split", split_phase=split_manifest, failure=split_report.reason)
    api.prune_generic_media_links(pages)
    api.ensure_meaningful_connections(pages)
    api.finalize_page_shapes(pages)
    api.update_manifest(phase="synthesis", total_pages=len(pages), processed_pages=len(pages), failure=None)

    api.update_manifest(phase="render")
    existing_log = (api.WIKI_ROOT / "log.md").read_text(encoding="utf-8") if (api.WIKI_ROOT / "log.md").exists() else "# Wiki Log\n\n"
    bootstrap_entry = f'## [{api.TODAY}] bootstrap | completed — {len(markdown_files) + len(media_files)} raw files, {len(pages)} wiki pages migrated'
    bootstrap_entry += f"; {api.split_report_summary(split_report)}"
    if args.fetch_urls:
        bootstrap_entry += (
            f'; URL metadata: {url_stats["url_notes"]} URL notes, '
            f'{url_stats["fetched"]} fetched, '
            f'{url_stats["fetch_failed"]} failed, '
            f'{url_stats["http_dead"]} dead, '
            f'{url_stats["non_html"]} non-HTML'
        )
    bootstrap_entry += "\n"
    stage_dir = api.stage_rendered_wiki(pages=pages, existing_log_text=existing_log, bootstrap_entry=bootstrap_entry)
    api.swap_rendered_wiki(stage_dir)
    api.update_manifest(phase="completed", processed_notes=len(markdown_files), processed_pages=len(pages), failure=None)
