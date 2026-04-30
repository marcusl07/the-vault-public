from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
import re

try:
    from scripts.bootstrap_wiki_model import (
        BOILERPLATE_PATTERNS,
        CATALOG_PATH,
        HIGH_SIGNAL_INBOUND_THRESHOLD,
        INDEX_SECTION_ORDER,
        PAGE_SHAPE_ATOMIC,
        PAGE_SHAPE_TOPIC,
        TODAY,
        Page,
        SourceRecord,
        compact_source_text,
        ordered_unique,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    from bootstrap_wiki_model import (
        BOILERPLATE_PATTERNS,
        CATALOG_PATH,
        HIGH_SIGNAL_INBOUND_THRESHOLD,
        INDEX_SECTION_ORDER,
        PAGE_SHAPE_ATOMIC,
        PAGE_SHAPE_TOPIC,
        TODAY,
        Page,
        SourceRecord,
        compact_source_text,
        ordered_unique,
    )


def page_shape(page: Page) -> str:
    if page.shape == PAGE_SHAPE_TOPIC:
        return PAGE_SHAPE_TOPIC
    if not page.sources and not page.notes and page.connections:
        return PAGE_SHAPE_TOPIC
    return PAGE_SHAPE_ATOMIC


def sorted_connection_slugs(page: Page, *, limit: int | None = None) -> list[str]:
    ordered = [slug for slug, _count in page.connections.most_common() if slug and slug != page.slug]
    unique = ordered_unique(ordered)
    if limit is None:
        return unique
    return unique[:limit]


def render_connection_lines(page: Page, *, limit: int | None = 12) -> list[str]:
    return [f"- [[{slug}]]" for slug in sorted_connection_slugs(page, limit=limit)]


def render_source_lines(page: Page) -> list[str]:
    lines: list[str] = []
    suffix_map = {
        "fetch_failed": " — [⚠️ fetch failed]",
        "non_html": " — [⚠️ non-HTML resource]",
        "http_dead": " — [⚠️ dead link]",
    }
    for source_path in sorted(page.sources):
        source = page.sources[source_path]
        visible_label = source.external_url or source.detected_url or source.label
        lines.append(f"- [{visible_label}]({format_markdown_link_target(source_path)}){suffix_map.get(source.status, '')}")
    return lines


def format_markdown_link_target(target: str) -> str:
    if re.search(r"[\s()<>\"]", target):
        return f"<{target}>"
    return target


def build_simple_notes_markdown(page: Page) -> str:
    if page_shape(page) == PAGE_SHAPE_TOPIC:
        return ""

    if page.rendered_notes_markdown:
        stripped = page.rendered_notes_markdown.strip()
        if stripped and not any(pattern.fullmatch(stripped) for pattern in BOILERPLATE_PATTERNS):
            return stripped

    note_candidates = ordered_unique([note for note in page.notes if note.strip()])
    if len(page.sources) == 1:
        source = next(iter(page.sources.values()))
        excerpt = compact_source_text(source, limit=220).strip()
        normalized_excerpt = re.sub(r"\s+", " ", excerpt)
        synthesized_notes = [
            note
            for note in note_candidates
            if re.sub(r"\s+", " ", note.strip()) != normalized_excerpt
        ]
        if synthesized_notes:
            return "\n".join(f"- {note}" for note in note_candidates[:60])
        if note_candidates and normalized_excerpt:
            return f"- {normalized_excerpt}"

        body_parts = []
        if source.fetched_summary:
            body_parts.append(source.fetched_summary)
        if source.cleaned_text:
            body_parts.append(source.cleaned_text)
        body = "\n\n".join(part for part in body_parts if part).strip()
        if body:
            return body if ("\n" in body or re.search(r"^(?:#|-|\d+\.)", body, flags=re.M)) else f"- {body}"

    if note_candidates:
        return "\n".join(f"- {note}" for note in note_candidates[:60])

    multi_source_lines: list[str] = []
    for source in sorted(page.sources.values(), key=lambda item: item.label.lower()):
        excerpt = compact_source_text(source, limit=220).strip()
        excerpt = re.sub(r"\s+", " ", excerpt)
        if excerpt and excerpt not in multi_source_lines:
            multi_source_lines.append(excerpt)
    return "\n".join(f"- {line}" for line in multi_source_lines[:30])


def page_index_summary(page: Page) -> str:
    if page_shape(page) == PAGE_SHAPE_TOPIC:
        return "topic page"
    source_count = len(page.sources)
    return f"{source_count} source{'' if source_count == 1 else 's'}"


def inbound_link_counts(pages: dict[str, Page]) -> Counter:
    counts: Counter = Counter()
    known_slugs = set(pages)
    for page in pages.values():
        for slug in sorted_connection_slugs(page, limit=None):
            if slug in known_slugs:
                counts[slug] += 1
    return counts


def render_catalog(pages: dict[str, Page], *, today: str = TODAY) -> str:
    grouped: defaultdict[str, list[Page]] = defaultdict(list)
    for page in pages.values():
        if page.slug in {"index", "log", Path(CATALOG_PATH).stem}:
            continue
        grouped[page.page_type].append(page)

    lines = [
        "# Wiki Catalog",
        "",
        f"_Last updated: {today} — {len(pages)} pages_",
        "",
    ]

    for section in INDEX_SECTION_ORDER:
        lines.append(f"## {section}")
        section_pages = sorted(grouped.get(section, []), key=lambda page: page.title.lower())
        if not section_pages:
            lines.append("- None yet.")
            lines.append("")
            continue
        for page in section_pages:
            lines.append(f"- [[{page.slug}]] — {page_index_summary(page)}")
        lines.append("")

    return "\n".join(lines)


def normalize_page(page: Page) -> Page:
    page.connections = Counter({slug: count for slug, count in page.connections.items() if slug and slug != page.slug})
    if page_shape(page) == PAGE_SHAPE_TOPIC:
        page.shape = PAGE_SHAPE_TOPIC
        page.notes = []
        page.rendered_notes_markdown = None
        page.open_questions = []
        page.sources = {}
    else:
        page.shape = PAGE_SHAPE_ATOMIC
        page.notes = ordered_unique([note.strip() for note in page.notes if note.strip()])
        page.open_questions = ordered_unique([question.strip() for question in page.open_questions if question.strip()])
        page.rendered_notes_markdown = build_simple_notes_markdown(page) or None
    return page


def validate_page(page: Page, *, allow_missing_outbound: bool = False) -> list[str]:
    issues: list[str] = []
    if page_shape(page) == PAGE_SHAPE_TOPIC:
        if page.notes or page.rendered_notes_markdown:
            issues.append("topic-pages-cannot-have-notes")
        if page.open_questions:
            issues.append("topic-pages-cannot-have-open-questions")
        if page.sources:
            issues.append("topic-pages-cannot-have-sources")
    else:
        normalized = normalize_page(page)
        if not allow_missing_outbound and not sorted_connection_slugs(normalized, limit=None):
            issues.append("atomic-pages-must-have-outbound-links")
    return issues


def render_page(page: Page) -> str:
    page = normalize_page(page)
    lines = [f"# {page.title}", ""]

    connection_lines = render_connection_lines(page)
    if page_shape(page) == PAGE_SHAPE_TOPIC:
        if connection_lines:
            lines.extend(["## Connections", "", "\n".join(connection_lines), ""])
        return "\n".join(lines).rstrip() + "\n"

    note_lines = build_simple_notes_markdown(page)
    if note_lines.strip():
        lines.extend(["## Notes", "", note_lines, ""])
    if page.open_questions:
        lines.extend(["## Open Questions", "", "\n".join(f"- {question}" for question in page.open_questions), ""])
    if connection_lines:
        lines.extend(["## Connections", "", "\n".join(connection_lines), ""])
    source_lines = render_source_lines(page)
    if source_lines:
        lines.extend(["## Sources", "", "\n".join(source_lines), ""])
    return "\n".join(lines).rstrip() + "\n"


def render_index(pages: dict[str, Page], *, today: str = TODAY) -> str:
    counts = inbound_link_counts(pages)
    grouped: defaultdict[str, list[Page]] = defaultdict(list)
    for page in pages.values():
        if page.slug in {"index", "log", Path(CATALOG_PATH).stem}:
            continue
        if page_shape(page) == PAGE_SHAPE_TOPIC or counts.get(page.slug, 0) >= HIGH_SIGNAL_INBOUND_THRESHOLD:
            grouped[page.page_type].append(page)

    lines = [
        "# Wiki Index",
        "",
        f"_Last updated: {today} — {len(pages)} pages_",
        "_Navigation only: topic pages plus high-signal atomic pages. Use [[catalog]] for exhaustive lookup._",
        "",
    ]

    for section in INDEX_SECTION_ORDER:
        lines.append(f"## {section}")
        section_pages = sorted(grouped.get(section, []), key=lambda page: page.title.lower())
        if not section_pages:
            lines.append("- None yet.")
            lines.append("")
            continue
        for page in section_pages:
            lines.append(f"- [[{page.slug}]] — {page_index_summary(page)}")
        lines.append("")

    return "\n".join(lines)
