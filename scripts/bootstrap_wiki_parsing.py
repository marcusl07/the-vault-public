from __future__ import annotations

from pathlib import Path
import re

try:
    from scripts.bootstrap_wiki_model import (
        BOILERPLATE_PATTERNS,
        CATALOG_PATH,
        CONNECTION_SLUG_RE,
        INDEX_SECTION_ORDER,
        PAGE_SHAPE_ATOMIC,
        PAGE_SHAPE_TOPIC,
        Page,
        ParsedWikiPage,
        SourceRecord,
        classify_page,
        page_title,
        strip_markdown,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    from bootstrap_wiki_model import (
        BOILERPLATE_PATTERNS,
        CATALOG_PATH,
        CONNECTION_SLUG_RE,
        INDEX_SECTION_ORDER,
        PAGE_SHAPE_ATOMIC,
        PAGE_SHAPE_TOPIC,
        Page,
        ParsedWikiPage,
        SourceRecord,
        classify_page,
        page_title,
        strip_markdown,
    )


def parse_note_snippets(note_lines: list[str]) -> list[str]:
    snippets: list[str] = []
    for line in note_lines:
        stripped = line.strip()
        if not stripped:
            continue
        if any(pattern.fullmatch(stripped) for pattern in BOILERPLATE_PATTERNS):
            continue
        if stripped.startswith("### "):
            continue
        normalized = re.sub(r"^[-*+]\s+", "", stripped)
        normalized = re.sub(r"^\d+\.\s+", "", normalized)
        cleaned = strip_markdown(normalized).strip()
        if cleaned and cleaned not in snippets:
            snippets.append(cleaned)
    return snippets


def parse_markdown_source_link(line: str) -> tuple[str, str, str] | None:
    stripped = line.strip()
    match = re.match(r"^- \[(?P<label>[^\]]+)\]\((?P<rest>.*)$", stripped)
    if not match:
        return None
    label = match.group("label")
    rest = match.group("rest")
    if rest.startswith("<"):
        end_index = rest.find(">)")
        if end_index == -1:
            return None
        return label, rest[1:end_index], rest[end_index + 2 :]

    end_index = rest.rfind(")")
    if end_index == -1:
        return None
    return label, rest[:end_index], rest[end_index + 1 :]


def parse_source_line(line: str, retained_evidence: str = "") -> SourceRecord | None:
    parsed_link = parse_markdown_source_link(line)
    if parsed_link is None:
        return None
    label, path, suffix = parsed_link
    suffix = suffix.strip()
    status = "local_only"
    if suffix == "— [⚠️ fetch failed]":
        status = "fetch_failed"
    elif suffix == "— [⚠️ non-HTML resource]":
        status = "non_html"
    elif suffix == "— [⚠️ dead link]":
        status = "http_dead"
    return SourceRecord(
        label=label,
        path=path,
        status=status,
        raw_content="",
        cleaned_text=retained_evidence,
        fetched_summary=None,
        detected_url=label if label.startswith(("http://", "https://")) else None,
        source_kind="chat" if path.startswith("../sources/chat/") else "capture",
        title=label,
        external_url=label if label.startswith(("http://", "https://")) else None,
    )


def extract_connection_slugs(connection_lines: list[str]) -> list[str]:
    slugs: list[str] = []
    for line in connection_lines:
        match = CONNECTION_SLUG_RE.search(line)
        if not match:
            continue
        slug = match.group("slug")
        if slug not in slugs:
            slugs.append(slug)
    return slugs


def parse_page_file(path: Path, page_type: str | None = None) -> ParsedWikiPage:
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    lines = text.splitlines()
    slug = path.stem
    title = lines[0][2:].strip() if lines and lines[0].startswith("# ") else page_title(slug)
    sections: dict[str, list[str]] = {
        "summary": [],
        "notes": [],
        "connections": [],
        "sources": [],
        "open_questions": [],
    }
    current = "summary"
    for line in lines[1:]:
        if line == "## Notes":
            current = "notes"
            continue
        if line == "## Open Questions":
            current = "open_questions"
            continue
        if line == "## Connections":
            current = "connections"
            continue
        if line == "## Sources":
            current = "sources"
            continue
        sections[current].append(line)

    retained_evidence = "\n".join(parse_note_snippets([line for line in sections["notes"] if line.strip()]))
    sources: dict[str, SourceRecord] = {}
    for line in sections["sources"]:
        source = parse_source_line(line, retained_evidence)
        if source is not None:
            sources[source.path] = source

    connection_slugs = extract_connection_slugs([line for line in sections["connections"] if line.strip()])
    summary_lines = [line for line in sections["summary"] if line.strip()]
    note_lines = [line for line in sections["notes"] if line.strip()]
    open_question_lines = [line for line in sections["open_questions"] if line.strip()]
    inferred_shape = PAGE_SHAPE_TOPIC if (not note_lines and not sources and connection_slugs) else PAGE_SHAPE_ATOMIC

    return ParsedWikiPage(
        slug=slug,
        title=title,
        page_type=page_type or classify_page(slug, title, "title"),
        shape=inferred_shape,
        summary_lines=summary_lines,
        note_lines=note_lines,
        open_question_lines=open_question_lines,
        connection_slugs=connection_slugs,
        sources=sources,
    )


def parsed_page_to_page(parsed: ParsedWikiPage) -> Page:
    page = Page(
        slug=parsed.slug,
        title=parsed.title or page_title(parsed.slug),
        page_type=parsed.page_type,
        summary_hint=parsed.title,
        shape=parsed.shape,
    )
    page.notes = parse_note_snippets(parsed.note_lines)
    if parsed.note_lines:
        page.rendered_notes_markdown = "\n".join(parsed.note_lines).strip()
    page.open_questions = parse_note_snippets(parsed.open_question_lines)
    for source_path, source in parsed.sources.items():
        page.sources[source_path] = source
    for other in parsed.connection_slugs:
        page.connections[other] += 1
    return page


def load_existing_page_types(index_path: Path) -> dict[str, str]:
    if not index_path.exists():
        return {}
    page_types: dict[str, str] = {}
    current_section: str | None = None
    for line in index_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            section = line[3:].strip()
            current_section = section if section in INDEX_SECTION_ORDER else None
            continue
        if current_section is None:
            continue
        match = re.match(r"^- \[\[(?P<slug>[^\]]+)\]\] — ", line)
        if match:
            page_types[match.group("slug")] = current_section
    return page_types


def load_page_types(wiki_root: Path) -> dict[str, str]:
    catalog_path = wiki_root / CATALOG_PATH
    if catalog_path.exists():
        return load_existing_page_types(catalog_path)
    return load_existing_page_types(wiki_root / "index.md")


def load_existing_wiki_pages(wiki_root: Path) -> dict[str, ParsedWikiPage]:
    page_types = load_page_types(wiki_root)
    parsed_pages: dict[str, ParsedWikiPage] = {}
    if not wiki_root.exists():
        return parsed_pages
    for path in sorted(wiki_root.glob("*.md")):
        if path.stem in {"index", "log", Path(CATALOG_PATH).stem}:
            continue
        parsed = parse_page_file(path, page_type=page_types.get(path.stem))
        parsed_pages[parsed.slug] = parsed
    return parsed_pages
