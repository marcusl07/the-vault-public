from __future__ import annotations

import os
import re
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import unquote

if TYPE_CHECKING:
    from types import ModuleType


def _resolve_repo_relative_path(api: ModuleType, path: str) -> Path:
    normalized = path[3:] if path.startswith("../") else path
    normalized = unquote(normalized)
    resolved = (api.ROOT / normalized).resolve()
    root_resolved = api.ROOT.resolve()
    if Path(os.path.commonpath([root_resolved, resolved])) != root_resolved:
        raise ValueError(f"path must stay within repo root: {path}")
    return resolved


def _source_excerpt(api: ModuleType, source: object) -> str:
    excerpt = api.bw.compact_source_text(source, limit=220).strip()
    return re.sub(r"\s+", " ", excerpt)


def _remove_source_from_page(api: ModuleType, page: object, source_path: str) -> None:
    source = page.sources.pop(source_path, None)
    if source is None:
        return
    excerpt = api._source_excerpt(source)
    if excerpt:
        page.notes = [note for note in page.notes if note != excerpt]


def _remove_open_questions_for_fact(api: ModuleType, page: object, *, fact_key: str) -> None:
    prefix = f"Conflicting chat-derived fact for {fact_key}:"
    page.open_questions = [question for question in page.open_questions if not question.startswith(prefix)]


def _append_review_backlog_item(
    api: ModuleType,
    *,
    reason: str,
    affected_pages: list[str],
    source_paths: list[str],
    next_action: str,
    status: str = "open",
) -> None:
    review_path = api.WIKI_ROOT / "review.md"
    existing = review_path.read_text(encoding="utf-8") if review_path.exists() else "# Wiki Review Backlog\n"
    page_links = ", ".join(f"[[{slug}]]" for slug in api.bw.ordered_unique(affected_pages)) or "None"
    source_links = ", ".join(f"[{path}]({path})" for path in api.bw.ordered_unique(source_paths)) or "None"
    entry = "\n".join(
        [
            f"## [{api._today_date()}] {status} | {reason}",
            f"- Affected pages: {page_links}",
            f"- Source artifacts: {source_links}",
            f"- Next action: {next_action}",
        ]
    )
    api.atomic_write_text(review_path, existing.rstrip() + "\n\n" + entry + "\n")


def _resolve_review_backlog_entries(
    api: ModuleType,
    *,
    reason: str,
    affected_pages: list[str],
) -> int:
    review_path = api.WIKI_ROOT / "review.md"
    if not review_path.exists():
        return 0
    lines = review_path.read_text(encoding="utf-8").splitlines()
    resolved = 0
    updated_lines: list[str] = []
    current_section: list[str] = []

    def flush_section(section: list[str]) -> None:
        nonlocal resolved
        if not section:
            return
        heading = section[0]
        if not heading.startswith("## ["):
            updated_lines.extend(section)
            return
        heading_parts = heading.split(" | ", 1)
        heading_reason = heading_parts[1] if len(heading_parts) == 2 else ""
        matches_reason = heading_reason == reason
        has_affected_pages = any(
            line.startswith("- Affected pages:") and all(f"[[{slug}]]" in line for slug in affected_pages)
            for line in section[1:]
        )
        if matches_reason and has_affected_pages and " open | " in heading:
            section = [heading.replace(" open | ", " resolved | ", 1), *section[1:]]
            resolved += 1
        updated_lines.extend(section)

    for line in lines:
        if line.startswith("## ["):
            flush_section(current_section)
            current_section = [line]
        elif current_section:
            current_section.append(line)
        else:
            updated_lines.append(line)
    flush_section(current_section)
    if resolved:
        api.atomic_write_text(review_path, "\n".join(updated_lines) + "\n")
    return resolved


def _matching_chat_sources_for_fact(api: ModuleType, page: object, *, fact_key: str) -> list[tuple[str, dict[str, object]]]:
    matches: list[tuple[str, dict[str, object]]] = []
    for source_path, source in page.sources.items():
        if source.source_kind != "chat":
            continue
        try:
            artifact_frontmatter, _ = api.parse_raw_note(api._resolve_repo_relative_path(source_path))
        except Exception:
            continue
        if artifact_frontmatter.get("fact_key") != fact_key:
            continue
        matches.append((source_path, artifact_frontmatter))
    return matches


def query_writeback_chat_fact(
    api: ModuleType,
    *,
    page_title: str,
    note: str,
    related_pages: list[str],
    created_at: str,
    conversation_ref: str,
    fact_key: str,
    replacement_intent: bool = False,
    external_url: str | None = None,
) -> object:
    target_slug = api.bw.slugify(page_title)
    if not target_slug:
        raise ValueError("page_title must produce a valid slug")
    normalized_note = note.strip()
    if not normalized_note:
        raise ValueError("note must be non-empty")
    normalized_related_pages = api.bw.ordered_unique(
        [api.bw.slugify(value) for value in related_pages if api.bw.slugify(value)]
    )
    source_path = api.persist_chat_source_artifact(
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
    frontmatter, body = api.parse_raw_note(source_path)
    source_record = api._source_record_from_artifact(frontmatter, page_title, body, source_path)
    router_decision = api._route_source_update(
        title=page_title,
        body=normalized_note,
        page_assignments=[(target_slug, "title"), *[(slug, "query") for slug in normalized_related_pages]],
    )

    loaded_pages: dict[str, object] = {}

    def load_page(slug: str, seed_kind: str) -> object:
        if slug in loaded_pages:
            return loaded_pages[slug]
        page = api._read_wiki_page(slug, original_title=page_title, seed_kind=seed_kind)
        loaded_pages[slug] = page
        return page

    target_page = load_page(target_slug, "title")
    target_page.shape = api.bw.PAGE_SHAPE_ATOMIC
    target_page.page_type = api.bw.classify_page(target_slug, page_title, "title")

    for related_slug in normalized_related_pages:
        related_page = load_page(related_slug, "query")
        if api.bw.page_shape(related_page) != api.bw.PAGE_SHAPE_TOPIC:
            related_page.shape = api.bw.PAGE_SHAPE_ATOMIC
        api.bw.connect_pages(loaded_pages, target_slug, related_slug)

    duplicate_of_source_path: str | None = None
    superseded_source_paths: list[str] = []
    review_queued = False
    matching_sources = api._matching_chat_sources_for_fact(target_page, fact_key=fact_key)
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
                api._remove_source_from_page(target_page, existing_source_path)
                superseded_source_paths.append(existing_source_path)
            api._remove_open_questions_for_fact(target_page, fact_key=fact_key)
            api._resolve_review_backlog_entries(reason=f"contradiction | {fact_key}", affected_pages=[target_slug])
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
                api._append_review_backlog_item(
                    reason=f"contradiction | {fact_key}",
                    affected_pages=[target_slug],
                    source_paths=[
                        *superseded_source_paths,
                        *[path for path, _ in matching_sources],
                        "../" + api.normalize_repo_path(source_path),
                    ],
                    next_action="Confirm which chat-derived fact should remain current on the page.",
                )
                review_queued = True

        api.bw.add_source_to_page(target_page, source_record, seed_kind="query")

    changed_slugs: list[str] = []
    allow_missing_outbound = bool(normalized_related_pages) or bool(target_page.connections)
    issues = api.bw.validate_page(target_page, allow_missing_outbound=allow_missing_outbound)
    if issues:
        api._append_review_backlog_item(
            reason="invalid query writeback",
            affected_pages=[target_slug],
            source_paths=["../" + api.normalize_repo_path(source_path)],
            next_action=f"Repair page shape before applying query writeback: {', '.join(issues)}",
        )
        raise ValueError(f"invalid page '{target_slug}': {', '.join(issues)}")

    pages_to_write = {target_slug: target_page}
    for related_slug in normalized_related_pages:
        pages_to_write[related_slug] = loaded_pages[related_slug]

    for slug, page in pages_to_write.items():
        page_issues = api.bw.validate_page(
            page,
            allow_missing_outbound=slug != target_slug or bool(page.connections),
        )
        if page_issues:
            raise ValueError(f"invalid page '{slug}': {', '.join(page_issues)}")
        api.atomic_write_text(api.WIKI_ROOT / f"{slug}.md", api.bw.render_page(page))
        changed_slugs.append(slug)

    if duplicate_of_source_path is None:
        api._rewrite_index([(slug, pages_to_write[slug].page_type) for slug in changed_slugs])
        summary = f'writeback | "{page_title}" | Router: {router_decision.action}'
        if superseded_source_paths:
            summary += " | superseded prior chat fact"
        if review_queued:
            summary += " | review queued"
        api.bw.append_wiki_query_log(summary)

    return api.QueryWritebackResult(
        changed_slugs=changed_slugs,
        source_path=source_path,
        router_decision=router_decision,
        review_queued=review_queued,
        superseded_source_paths=superseded_source_paths,
        duplicate_of_source_path=duplicate_of_source_path,
    )
