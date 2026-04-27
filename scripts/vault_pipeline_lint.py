from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


def trace_page_provenance(api: ModuleType, slug: str) -> dict[str, list[str]]:
    page_path = api.WIKI_ROOT / f"{slug}.md"
    if not page_path.exists():
        raise FileNotFoundError(page_path)
    page = api.bw.parsed_page_to_page(api.bw.parse_page_file(page_path))
    sources = list(page.sources.values())
    if not sources:
        return {}

    trace: dict[str, list[str]] = {}
    for note in page.notes:
        matches: list[str] = []
        normalized_note = api.re.sub(r"\s+", " ", note.strip().lower())
        for source in sources:
            candidates = [
                source.cleaned_text,
                source.raw_content,
                source.fetched_summary or "",
                api._source_excerpt(source),
            ]
            if any(normalized_note and normalized_note in api.re.sub(r"\s+", " ", value.strip().lower()) for value in candidates if value):
                matches.append(source.path)
        trace[note] = api.bw.ordered_unique(matches)
    return trace


def lint_wiki(api: ModuleType, *, append_review: bool = False) -> object:
    findings: list[object] = []
    pages: dict[str, object] = {}
    for page_path in sorted(api.WIKI_ROOT.glob("*.md")):
        if page_path.stem in {"index", "log", "review", Path(api.bw.CATALOG_PATH).stem}:
            continue
        pages[page_path.stem] = api.bw.parsed_page_to_page(api.bw.parse_page_file(page_path))

    inbound_counts = api.bw.inbound_link_counts(pages)
    for slug, page in pages.items():
        issues = api.bw.validate_page(page)
        for issue in issues:
            findings.append(api.LintFinding(kind="invalid-page-shape", slug=slug, detail=issue))
            if issue == "atomic-pages-must-have-outbound-links":
                findings.append(api.LintFinding(kind="missing-outbound-link", slug=slug, detail=issue))

        if page.shape == api.bw.PAGE_SHAPE_ATOMIC and not page.connections and not any(
            finding.kind == "missing-outbound-link" and finding.slug == slug for finding in findings
        ):
            findings.append(api.LintFinding(kind="missing-outbound-link", slug=slug, detail="atomic page has no outbound links"))

        if page.shape == api.bw.PAGE_SHAPE_ATOMIC and inbound_counts.get(slug, 0) == 0:
            findings.append(api.LintFinding(kind="orphan", slug=slug, detail="page has no inbound links"))

        if page.open_questions:
            findings.append(
                api.LintFinding(
                    kind="contradiction-candidate",
                    slug=slug,
                    detail=f"{len(page.open_questions)} open question(s) need review",
                    source_paths=tuple(sorted(page.sources)),
                )
            )

        for source_path in sorted(page.sources):
            try:
                resolved_path = api._resolve_repo_relative_path(source_path)
            except ValueError:
                findings.append(api.LintFinding(kind="dead-citation", slug=slug, detail=f"source path escapes repo: {source_path}"))
                continue
            if not resolved_path.exists():
                findings.append(api.LintFinding(kind="dead-citation", slug=slug, detail=f"missing source artifact: {source_path}"))

        for connection_slug in sorted(page.connections):
            if connection_slug not in pages:
                findings.append(api.LintFinding(kind="dead-link", slug=slug, detail=f"missing linked page: [[{connection_slug}]]"))

    review_updates = 0
    if append_review:
        seen_review_keys: set[tuple[str, str, str]] = set()
        for finding in findings:
            review_key = (finding.kind, finding.slug, finding.detail)
            if review_key in seen_review_keys:
                continue
            seen_review_keys.add(review_key)
            api._append_review_backlog_item(
                reason=f"lint | {finding.kind}",
                affected_pages=[finding.slug],
                source_paths=list(finding.source_paths),
                next_action=f"Resolve lint finding: {finding.detail}",
            )
            review_updates += 1

    return api.LintReport(findings=findings, review_updates=review_updates)
