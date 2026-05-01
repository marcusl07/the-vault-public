from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlparse
import argparse
import html
import json
import os
import re
import socket
import sys
import unicodedata

try:
    from scripts.workspace_fs import atomic_write_text as shared_atomic_write_text
    from scripts.workspace_fs import temporary_workspace as shared_temporary_workspace
    from scripts.source_model import content_body, source_artifact_to_evidence
    from scripts import bootstrap_wiki_cache as cache_impl
    from scripts import bootstrap_wiki_cli as cli_impl
    from scripts.bootstrap_wiki_model import (
        BOILERPLATE_PATTERNS,
        CATALOG_PATH,
        HIGH_SIGNAL_INBOUND_THRESHOLD,
        INDEX_SECTION_ORDER,
        PAGE_SHAPE_ATOMIC,
        PAGE_SHAPE_TOPIC,
        TODAY,
        Page,
        ParsedWikiPage,
        SourceArtifact,
        SourceCitation,
        SourceEvidence,
        SourceRecord,
        classify_page,
        compact_source_text,
        ordered_unique,
        page_title,
        strip_markdown,
    )
    from scripts import bootstrap_wiki_parsing as parsing_impl
    from scripts import bootstrap_wiki_remote as remote_impl
    from scripts import bootstrap_wiki_rendering as rendering_impl
    from scripts import bootstrap_wiki_splitting as splitting_impl
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    from workspace_fs import atomic_write_text as shared_atomic_write_text
    from workspace_fs import temporary_workspace as shared_temporary_workspace
    from source_model import content_body, source_artifact_to_evidence
    import bootstrap_wiki_cache as cache_impl
    import bootstrap_wiki_cli as cli_impl
    from bootstrap_wiki_model import (
        BOILERPLATE_PATTERNS,
        CATALOG_PATH,
        HIGH_SIGNAL_INBOUND_THRESHOLD,
        INDEX_SECTION_ORDER,
        PAGE_SHAPE_ATOMIC,
        PAGE_SHAPE_TOPIC,
        TODAY,
        Page,
        ParsedWikiPage,
        SourceArtifact,
        SourceCitation,
        SourceEvidence,
        SourceRecord,
        classify_page,
        compact_source_text,
        ordered_unique,
        page_title,
        strip_markdown,
    )
    import bootstrap_wiki_parsing as parsing_impl
    import bootstrap_wiki_remote as remote_impl
    import bootstrap_wiki_rendering as rendering_impl
    import bootstrap_wiki_splitting as splitting_impl


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

GENERIC_COMPONENTS = {
    "apple notes",
    "marcus",
    "areas",
    "area",
    "resources",
    "resource",
    "archives",
    "archive",
    "projects",
    "project",
    "past",
    "bought",
    "rejected",
    "read",
    "test",
}

COMPONENT_ALIASES = {
    "日本語": "japanese",
    "hiking - camping": "camping-and-hiking",
    "apple notes": "apple-notes",
    "league stuff": "football",
    "fpl 23-24": "fantasy-premier-league",
    "fm24": "football-manager",
    "my info": "personal-admin",
    "five year plan": "five-year-plan",
    "shopping waitlist": "shopping-waitlist",
    "interesting youtube videos": "interesting-videos",
    "interesting articles": "interesting-articles",
    "art-design": "art-and-design",
    "dig min": "digital-minimalism",
    "digital minimalism": "digital-minimalism",
    "check it out- do it!": "things-to-try",
    "things to watch": "watchlist",
    "uci food": "uci-food",
    "course selection w23": "uci-course-selection",
    "credit card": "credit-cards",
    "buying a car": "car-buying",
    "drivers license": "drivers-license",
    "pol theory final": "political-theory",
    "music 49": "music-49",
    "self notes": "self-reflection",
}

TOPIC_EXTRACTION_MAX_BODY_CHARS = 4_000
TOPIC_EXTRACTION_CONFIDENT_LEVELS = {"high", "medium"}
PAGE_SPLIT_MAX_BODY_CHARS = 6_000
SOURCE_LINE_RE = re.compile(r"^- \[(?P<label>[^\]]+)\]\((?P<path>[^)]+)\)(?P<suffix>.*)$")
BOLD_HEADING_RE = re.compile(r"^[-*+]\s*\*\*(.+?)\*\*\s*$")
GENERIC_BUCKET_SLUGS = {
    "recipe",
    "recipes",
}
COURSE_PAGE_SLUG_RE = re.compile(r"^[a-z]{2,6}-\d{1,3}[a-z]?$")
LECTURE_SOURCE_RE = re.compile(
    r"\b(week\s*\d+|lecture|lec\b|discussion|disc\b|lab\b|midterm|final|quiz|exam|homework|hw\b|assignment|chapter)\b",
    flags=re.I,
)
LECTURE_HEADING_RE = re.compile(
    r"^(?:###\s+|[-*+]\s*\*\*)(week\s*\d+|lecture|lec\b|discussion|disc\b|lab\b|midterm|final|quiz|exam|homework|hw\b|assignment|chapter)",
    flags=re.I,
)


@dataclass
class FetchResult:
    summary: str | None
    status: str


@dataclass
class SplitCandidateEvaluation:
    slug: str
    accepted: bool = False
    grounding: list[str] = field(default_factory=list)
    why_distinct: str | None = None
    passes_direct_link_test: bool = False
    passes_stable_page_test: bool = False
    passes_search_test: bool = False
    rejection_reasons: list[str] = field(default_factory=list)


@dataclass
class PageSplitDecision:
    is_atomic: bool
    candidate_satellite_slugs: list[str] = field(default_factory=list)
    source_assignments: dict[str, str] = field(default_factory=dict)
    rationale: str | None = None
    rejection_reasons: list[str] = field(default_factory=list)
    candidate_evaluations: list[SplitCandidateEvaluation] = field(default_factory=list)


@dataclass
class BucketSignalResult:
    score: int = 0
    reasons: list[str] = field(default_factory=list)

    @property
    def is_bucket_signaled(self) -> bool:
        return self.score >= 2


@dataclass
class SplitPhaseReport:
    mode: str = "performed"
    eligible_pages: int = 0
    analyzed_pages: int = 0
    atomic_pages: int = 0
    incomplete_pages: int = 0
    split_pages: int = 0
    failed_pages: int = 0
    status: str = "completed"
    aborted: bool = False
    reason: str | None = None
    failure_mode: str = "no-split"
    failure_details: list[str] = field(default_factory=list)
    incomplete_details: list[str] = field(default_factory=list)
    bucket_signaled_details: list[str] = field(default_factory=list)
    bucket_unsplit_details: list[str] = field(default_factory=list)


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


def atomic_write_text(path: Path, content: str) -> None:
    shared_atomic_write_text(path, content)


def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = html.unescape(text)
    text = text.replace("&", " and ")
    text = text.replace("×", "x")
    text = re.sub(r"https?--", "", text, flags=re.I)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"[^A-Za-z0-9]+", "-", text.lower())
    collapsed = re.sub(r"-{2,}", "-", text).strip("-")
    return collapsed[:120].rstrip("-")


def looks_like_archive(component: str) -> bool:
    normalized = component.strip().lower()
    if normalized in GENERIC_COMPONENTS:
        return True
    if re.fullmatch(r"\d+", normalized):
        return True
    if "archive" in normalized:
        return True
    if re.fullmatch(
        r"(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|sept|september|oct|october|nov|november|dec|december)\s+\d{1,2}",
        normalized,
    ):
        return True
    if re.fullmatch(r"\d{1,2}-\d{1,2}(-\d{2,4})?", normalized):
        return True
    return False


def clean_component(component: str) -> str | None:
    stripped = component.strip()
    if not stripped or looks_like_archive(stripped):
        return None
    if stripped in COMPONENT_ALIASES:
        return COMPONENT_ALIASES[stripped]
    slug = slugify(stripped)
    if not slug or slug in GENERIC_COMPONENTS:
        return None
    return slug


def derive_note_title(path: Path, content: str) -> str:
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        heading = re.match(r"^#\s+(.+)$", stripped)
        if heading:
            return strip_markdown(heading.group(1)).strip() or path.stem
        link = re.match(r"^\[(.*?)\]\((https?://[^)]+)\)", stripped)
        if link:
            text = strip_markdown(link.group(1)).strip().strip("*")
            return text or path.stem
        anchor = re.search(r"<a [^>]*>(.*?)</a>", stripped, flags=re.I)
        if anchor:
            text = strip_markdown(anchor.group(1)).strip()
            return text or path.stem
        break
    return path.stem


def note_has_url(content: str) -> bool:
    return extract_first_url(content) is not None


def _strip_url_suffix(url: str) -> str:
    return url.rstrip('.,;:!?"\'*')


def unwrap_export_url(url: str) -> str:
    parsed = urlparse(url)
    if "urldefense.com" in parsed.netloc and "__https" in url:
        wrapped_match = re.search(r"__(https?://.+?)__", url)
        if wrapped_match:
            return wrapped_match.group(1)
    query = parse_qs(parsed.query)
    for key in ("url", "u", "q"):
        candidate = query.get(key, [None])[0]
        if candidate and candidate.startswith(("http://", "https://")):
            return candidate
    return url


def extract_urls(content: str) -> list[str]:
    urls: list[str] = []

    patterns = [
        r'href=["\'](https?://[^"\']+)["\']',
        r"\[[^\]]*\]\((https?://[^)\s]+)\)",
        r"https?://[^\s<>\])]+",
    ]
    for pattern in patterns:
        for match in re.findall(pattern, content, flags=re.I):
            url = unwrap_export_url(html.unescape(match).strip())
            url = _strip_url_suffix(url)
            if url and url not in urls:
                urls.append(url)
    return urls


def extract_first_url(content: str) -> str | None:
    urls = extract_urls(content)
    return urls[0] if urls else None


def is_bare_url_note(content: str) -> bool:
    stripped = content.strip()
    if not stripped:
        return False
    urls = extract_urls(stripped)
    return len(urls) == 1 and stripped == urls[0]


def is_google_search_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower().rstrip("/")
    return "google.com" in host and path == "/search"


def is_google_search_title(title: str) -> bool:
    return bool(re.search(r"\s-\sgoogle search$", title.strip(), flags=re.I))


def is_youtube_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path
    return (
        ("youtube.com" in host and path == "/watch")
        or host.endswith("youtu.be")
    )


def note_embed_names(content: str) -> list[str]:
    return re.findall(r"!\[\[([^\]]+)\]\]", content)


def first_meaningful_snippet(content: str, title: str) -> str:
    lines = []
    for raw_line in content.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            continue
        lines.append(strip_markdown(stripped))
    lines = [line for line in lines if line]
    if not lines:
        return f"Saved note: {title}."
    combined = " ".join(lines[:2]).strip()
    combined = re.sub(r"\s+", " ", combined)
    if len(combined) > 180:
        combined = combined[:177].rstrip() + "..."
    return combined


def clean_source_text(content: str, title: str) -> str:
    title_text = strip_markdown(title).strip().lower()
    cleaned_lines: list[str] = []

    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped:
            if cleaned_lines and cleaned_lines[-1] != "":
                cleaned_lines.append("")
            continue

        heading_match = re.match(r"^(#+)\s+(.*)$", stripped)
        if heading_match:
            heading_text = strip_markdown(heading_match.group(2)).strip()
            if heading_text and heading_text.lower() != title_text:
                cleaned_lines.append(f"{heading_match.group(1)} {heading_text}")
            continue

        bullet_match = re.match(r"^(\s*)[-*+]\s+(.*)$", line)
        if bullet_match:
            indent = "  " * (len(bullet_match.group(1).expandtabs(2)) // 2)
            bullet_text = strip_markdown(bullet_match.group(2)).strip()
            if bullet_text:
                cleaned_lines.append(f"{indent}- {bullet_text}")
            continue

        ordered_match = re.match(r"^(\s*)(\d+)\.\s+(.*)$", line)
        if ordered_match:
            indent = "  " * (len(ordered_match.group(1).expandtabs(2)) // 2)
            item_text = strip_markdown(ordered_match.group(3)).strip()
            if item_text:
                cleaned_lines.append(f"{indent}{ordered_match.group(2)}. {item_text}")
            continue

        cleaned = strip_markdown(stripped)
        if cleaned:
            cleaned_lines.append(cleaned)

    embeds = note_embed_names(content)
    if embeds:
        if cleaned_lines and cleaned_lines[-1] != "":
            cleaned_lines.append("")
        cleaned_lines.append("Embedded media:")
        cleaned_lines.extend(f"- {name}" for name in embeds)

    while cleaned_lines and cleaned_lines[0] == "":
        cleaned_lines.pop(0)
    while cleaned_lines and cleaned_lines[-1] == "":
        cleaned_lines.pop()
    return "\n".join(cleaned_lines)


def detect_source_tags(
    title: str,
    cleaned_text: str,
    url: str | None,
    fetched_summary: str | None,
) -> set[str]:
    combined = " ".join(part for part in [title, cleaned_text, fetched_summary or "", url or ""] if part)
    lowered = combined.lower()
    tags: set[str] = set()

    if re.search(
        r"\b(birthday|anniversary|date idea|date night|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|20\d{2}|\d{1,2}/\d{1,2})\b",
        lowered,
    ):
        tags.add("dates")
    if re.search(r"\b(address|unit\b|apt\b|suite\b|drive\b|street\b|avenue\b|boulevard\b|\d{5}(?:-\d{4})?)", lowered):
        tags.update({"address", "places"})
    if re.search(r"\b(gift|present|christmas idea|bouquet|flowers|perfume|plush|concert tickets?|cake)\b", lowered):
        tags.add("gift_ideas")
    if re.search(
        r"\b(activities?|trip|travel|visit|airport|flight|kayaking|archery|rock climbing|board game|festival|paint by numbers|ceramics|theme park|animal cafe)\b",
        lowered,
    ):
        tags.update({"activities", "travel_plans"})
    if re.search(
        r"\b(place|restaurant|cafe|bakery|mall|market|japantown|santa clara|sf|san fran|daly city|irvine|paris baguette|chipotle|valley fair|great america)\b",
        lowered,
    ):
        tags.add("places")
    if re.search(r"\b(food|drink|cake|rice|beans|chicken|salsa|corn|cheese|guac|tea|bakery|restaurant)\b", lowered):
        tags.add("food")
    if re.search(r"\b(favorite|favourite|i like|i dislike|preference|fave|favorite color|favorite restaurant)\b", lowered):
        tags.add("preferences")
    if fetched_summary:
        tags.add("fetched_summary")
    if url:
        tags.add("has_url")
    if is_google_search_url(url or ""):
        tags.add("search_result")
    if re.search(r"\b(pin|password|passcode|credential|login|verification code|routing number|account number|security question)\b", lowered):
        tags.add("sensitive")
    if re.search(r"pornhub|horny|hard fuck|creampie|sex\b|nude\b", lowered):
        tags.add("explicit")
    if re.search(r"\b(i love you|hehe|tehe)\b", lowered):
        tags.add("low_signal")
    if not cleaned_text.strip() and not fetched_summary:
        tags.add("low_signal")
    elif len(strip_markdown(cleaned_text).split()) <= 4 and not tags.intersection(
        {"dates", "address", "gift_ideas", "activities", "places", "food", "travel_plans", "preferences"}
    ):
        tags.add("low_signal")
    return tags


def should_exclude_from_body(tags: set[str]) -> bool:
    informative_tags = {"dates", "address", "gift_ideas", "activities", "places", "food", "travel_plans", "preferences"}
    if "sensitive" in tags or "explicit" in tags:
        return True
    if "low_signal" in tags and not tags.intersection(informative_tags):
        return True
    return False


def prepare_source_record(
    *,
    source_label: str,
    source_path: str,
    source_status: str,
    raw_content: str,
    fetched_summary: str | None,
    detected_url: str | None,
    source_kind: str = "capture",
    source_id: str | None = None,
    created_at: str | None = None,
    title: str | None = None,
    external_url: str | None = None,
    provenance_pointer: str | None = None,
) -> SourceEvidence:
    cleaned_text = clean_source_text(raw_content, source_label)
    tags = detect_source_tags(source_label, cleaned_text, detected_url, fetched_summary)
    return SourceEvidence(
        label=source_label,
        path=source_path,
        status=source_status,
        cleaned_text=cleaned_text,
        fetched_summary=fetched_summary,
        detected_url=detected_url,
        source_kind=source_kind,
        source_id=source_id,
        created_at=created_at,
        title=title or source_label,
        external_url=external_url or detected_url,
        provenance_pointer=provenance_pointer,
        tags=tags,
        excluded_from_body=should_exclude_from_body(tags),
    )


def should_fold_note_into_parent(title: str, content: str, url: str | None) -> bool:
    if is_bare_url_note(content):
        return True
    if not url:
        return False
    if is_google_search_url(url):
        return True
    return is_google_search_title(title)


def derive_path_topics(path: Path) -> list[str]:
    topics: list[str] = []
    path_abs = Path(os.path.abspath(path))
    apple_root_abs = Path(os.path.abspath(APPLE_NOTES_ROOT))
    raw_root_abs = Path(os.path.abspath(RAW_ROOT))
    if Path(os.path.commonpath([apple_root_abs, path_abs])) == apple_root_abs:
        relative_parts = Path(os.path.relpath(path_abs, apple_root_abs)).parts[:-1]
    elif Path(os.path.commonpath([raw_root_abs, path_abs])) == raw_root_abs:
        relative_parts = Path(os.path.relpath(path_abs, raw_root_abs)).parts[:-1]
    else:
        relative_parts = path.parts[:-1]
    for component in reversed(relative_parts):
        topic = clean_component(component)
        if topic and topic not in topics:
            topics.append(topic)
        if len(topics) >= 3:
            break
    return topics


def parse_note_snippets(note_lines: list[str]) -> list[str]:
    return parsing_impl.parse_note_snippets(note_lines)


def parse_source_line(line: str, retained_evidence: str = "") -> SourceRecord | None:
    return parsing_impl.parse_source_line(line, retained_evidence)


def extract_connection_slugs(connection_lines: list[str]) -> list[str]:
    return parsing_impl.extract_connection_slugs(connection_lines)


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


def merge_page_content(target: Page, source_page: Page) -> None:
    if source_page.shape == PAGE_SHAPE_TOPIC:
        target.shape = PAGE_SHAPE_TOPIC
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


def merge_existing_pages(
    pages: dict[str, Page],
    existing_pages: dict[str, ParsedWikiPage],
) -> None:
    for slug, parsed in existing_pages.items():
        existing_page = parsed_page_to_page(parsed)
        if slug not in pages:
            pages[slug] = existing_page
            continue
        merge_page_content(pages[slug], existing_page)


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


def add_source_to_page(
    page: Page,
    source: SourceRecord,
    seed_kind: str = "migration",
    *,
    append_excerpt: bool = True,
) -> None:
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


def apply_query_time_split_fix(
    pages: dict[str, Page],
    parent_slug: str,
    split_decision: PageSplitDecision,
    *,
    seed_kind: str = "query",
) -> bool:
    parent_page = pages.get(parent_slug)
    child_slugs = ordered_unique(split_decision.candidate_satellite_slugs)
    if parent_page is None or split_decision.is_atomic or len(child_slugs) < 2:
        return False

    evaluation_map = {evaluation.slug: evaluation for evaluation in split_decision.candidate_evaluations}
    for child_slug in child_slugs:
        evaluation = evaluation_map.get(child_slug)
        if evaluation is None or not evaluation.accepted:
            return False
        if not evaluation.grounding:
            return False
        if not evaluation.passes_direct_link_test or not evaluation.passes_stable_page_test:
            return False

    source_groups, assigned_source_paths = gather_split_source_groups(parent_page, split_decision)
    split_decision = validate_split_child_grounding(parent_page, split_decision, source_groups)
    child_slugs = ordered_unique(split_decision.candidate_satellite_slugs)
    if split_decision.is_atomic or len(child_slugs) < 2:
        return False

    source_groups, assigned_source_paths = gather_split_source_groups(parent_page, split_decision)
    if parent_page.sources and len(parent_page.sources) > 1:
        if len(assigned_source_paths) != len(parent_page.sources) or len(source_groups) < 2:
            return False

    parent_mode = resolve_parent_split_mode(parent_page, child_slugs)
    if parent_mode == PAGE_SHAPE_TOPIC:
        parent_page.shape = PAGE_SHAPE_TOPIC
        parent_page.notes = []
        parent_page.rendered_notes_markdown = None
        parent_page.sources = {}
        parent_page.connections = Counter()
    elif parent_mode == "deprecated":
        replacements = " and ".join(f"[[{slug}]]" for slug in child_slugs)
        parent_page.shape = PAGE_SHAPE_ATOMIC
        parent_page.notes = [f"Deprecated: superseded by {replacements}."]
        parent_page.rendered_notes_markdown = None
        parent_page.sources = {}
        parent_page.connections = Counter()

    for child_slug in child_slugs:
        child_page = pages.setdefault(
            child_slug,
            Page(
                slug=child_slug,
                title=page_title(child_slug),
                page_type=classify_page(child_slug, parent_page.title, seed_kind),
                summary_hint=parent_page.title,
            ),
        )
        child_page.shape = PAGE_SHAPE_ATOMIC
        child_page.page_type = classify_page(child_slug, parent_page.title, seed_kind)
        child_page.seed_kinds.add(seed_kind)
        child_page.topic_parent = parent_slug if parent_mode == PAGE_SHAPE_TOPIC else None

        split_notes = build_split_child_notes(split_decision, child_slug)
        for note in split_notes:
            if note not in child_page.notes:
                child_page.notes.append(note)
        for source in source_groups.get(child_slug, []):
            add_source_to_page(child_page, source, seed_kind, append_excerpt=not bool(split_notes))
        connect_pages(pages, parent_slug, child_slug)
    return True


def maybe_apply_query_time_split_fix(
    parent_slug: str,
    *,
    split_decision: PageSplitDecision | None = None,
    api_key: str | None = None,
    model: str = "gemini-2.5-flash",
    mutation_note: str = "query-time split fix",
) -> bool:
    parent_path = WIKI_ROOT / f"{parent_slug}.md"
    if not parent_path.exists():
        return False

    pages = {slug: parsed_page_to_page(parsed) for slug, parsed in load_existing_wiki_pages().items()}
    parent_page = pages.get(parent_slug)
    if parent_page is None:
        return False

    effective_split_decision = split_decision or analyze_page_for_atomic_split(parent_page, api_key, model)
    child_slugs = ordered_unique(effective_split_decision.candidate_satellite_slugs)
    touched_slugs = {parent_slug, *child_slugs}
    before_page_text = {
        slug: (WIKI_ROOT / f"{slug}.md").read_text(encoding="utf-8")
        if (WIKI_ROOT / f"{slug}.md").exists()
        else None
        for slug in touched_slugs
    }
    index_path = WIKI_ROOT / "index.md"
    before_index_text = index_path.read_text(encoding="utf-8") if index_path.exists() else None

    applied = apply_query_time_split_fix(
        pages,
        parent_slug,
        effective_split_decision,
        seed_kind="query",
    )
    if not applied:
        return False

    changed_page_slugs: list[str] = []
    for slug in sorted(touched_slugs):
        page = pages.get(slug)
        if page is None:
            continue
        rendered = render_page(page)
        if before_page_text.get(slug) == rendered:
            continue
        atomic_write_text(WIKI_ROOT / f"{slug}.md", rendered)
        changed_page_slugs.append(slug)

    rendered_index = render_index(pages)
    rendered_catalog = render_catalog(pages)
    catalog_path = WIKI_ROOT / CATALOG_PATH
    before_catalog_text = catalog_path.read_text(encoding="utf-8") if catalog_path.exists() else None
    index_changed = before_index_text != rendered_index
    catalog_changed = before_catalog_text != rendered_catalog
    if not changed_page_slugs:
        return False

    if index_changed:
        atomic_write_text(index_path, rendered_index)
    if catalog_changed:
        atomic_write_text(catalog_path, rendered_catalog)

    append_wiki_query_log(f"{mutation_note} — {parent_slug} -> {', '.join(child_slugs[:8])}")
    return True


def migrate_pages_to_atomic_topics(
    pages: dict[str, Page],
    existing_pages: dict[str, ParsedWikiPage],
    api_key: str | None = None,
    model: str = "gemini-2.5-flash",
    target_slugs: set[str] | None = None,
) -> SplitPhaseReport:
    report = SplitPhaseReport(failure_mode=split_failure_mode())
    for slug, parsed in existing_pages.items():
        if slug not in pages:
            continue

        page = pages[slug]
        if parsed.shape == PAGE_SHAPE_TOPIC:
            page.shape = PAGE_SHAPE_TOPIC
            page.notes = []
            page.rendered_notes_markdown = None
            page.sources = {}
            for child_slug in parsed.connection_slugs:
                if child_slug in pages:
                    connect_pages(pages, slug, child_slug)

    eligible_slugs = [
        slug
        for slug in sorted(pages)
        if pages[slug].shape != PAGE_SHAPE_TOPIC
        and len(pages[slug].sources) >= 2
        and (target_slugs is None or slug in target_slugs)
    ]
    report.eligible_pages = len(eligible_slugs)
    if not eligible_slugs:
        print("Split phase: no eligible pages.", file=sys.stderr)
        return report

    preflight_ok, preflight_reason = split_preflight_check(api_key, model)
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
        if page_looks_like_course_notes(page):
            report.atomic_pages += 1
            print(f"Split phase: '{slug}' -> atomic (course lecture guard).", file=sys.stderr)
            continue
        bucket_signals = score_bucket_signals(page)
        if bucket_signals.is_bucket_signaled:
            detail = f"{slug}: {', '.join(bucket_signals.reasons)}"
            report.bucket_signaled_details.append(detail)

        print(f"Split phase: analyzing '{slug}' ({len(page.sources)} sources).", file=sys.stderr)
        try:
            split_decision = analyze_page_for_atomic_split(page, api_key, model)
            report.analyzed_pages += 1
            consecutive_failures = 0
            if split_debug_enabled():
                print(format_split_decision_debug(page, split_decision), file=sys.stderr)
        except (HTTPError, URLError, TimeoutError, socket.timeout, json.JSONDecodeError, ValueError) as error:
            report.failed_pages += 1
            detail = f"{slug}: {type(error).__name__}: {error}"
            report.failure_details.append(detail)
            print(f"Split phase: failed '{slug}': {detail}", file=sys.stderr)
            if split_counts_toward_transport_abort(error):
                consecutive_failures += 1
            else:
                consecutive_failures = 0
            if consecutive_failures >= max_consecutive_failures:
                report.mode = "no-split"
                report.status = "aborted_transport_failure"
                report.aborted = True
                report.reason = f"{consecutive_failures} consecutive split failures"
                print(
                    f"Split phase: aborting early after {consecutive_failures} consecutive failures.",
                    file=sys.stderr,
                )
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

        source_groups, assigned_source_paths = gather_split_source_groups(page, split_decision)

        if len(source_groups) < 2:
            if bucket_signals.is_bucket_signaled:
                report.failed_pages += 1
                detail = f"{slug}: bucket-unsplit (incomplete assignments; signals={', '.join(bucket_signals.reasons)})"
                report.bucket_unsplit_details.append(detail)
                report.failure_details.append(detail)
                print(f"Split phase: '{slug}' -> bucket-unsplit (incomplete assignments).", file=sys.stderr)
                continue
            report.incomplete_pages += 1
            detail = f"{slug}: incomplete assignments"
            report.incomplete_details.append(detail)
            print(f"Split phase: '{slug}' -> incomplete (incomplete assignments).", file=sys.stderr)
            continue
        if len(assigned_source_paths) != len(page.sources):
            if bucket_signals.is_bucket_signaled:
                report.failed_pages += 1
                detail = f"{slug}: bucket-unsplit (partial coverage; signals={', '.join(bucket_signals.reasons)})"
                report.bucket_unsplit_details.append(detail)
                report.failure_details.append(detail)
                print(f"Split phase: '{slug}' -> bucket-unsplit (partial coverage).", file=sys.stderr)
                continue
            report.incomplete_pages += 1
            detail = f"{slug}: partial coverage"
            report.incomplete_details.append(detail)
            print(f"Split phase: '{slug}' -> incomplete (partial coverage).", file=sys.stderr)
            continue

        page.shape = PAGE_SHAPE_TOPIC
        page.notes = []
        page.rendered_notes_markdown = None
        page.sources = {}
        page.connections = Counter()

        for child_slug, grouped_sources in sorted(source_groups.items()):
            child_page = pages.setdefault(
                child_slug,
                Page(
                    slug=child_slug,
                    title=page_title(child_slug),
                    page_type=classify_page(child_slug, grouped_sources[0].label, "migration"),
                    summary_hint=grouped_sources[0].label,
                ),
            )
            child_page.shape = PAGE_SHAPE_ATOMIC
            child_page.topic_parent = slug
            for source in grouped_sources:
                add_source_to_page(child_page, source)
            connect_pages(pages, slug, child_slug)
        report.split_pages += 1
        print(
            f"Split phase: '{slug}' -> split into {', '.join(sorted(source_groups))}.",
            file=sys.stderr,
        )

    return report


def ensure_connection_targets_exist(pages: dict[str, Page]) -> None:
    missing_targets: defaultdict[str, list[str]] = defaultdict(list)
    for page in pages.values():
        for slug in sorted_connection_slugs(page):
            if slug not in pages:
                missing_targets[slug].append(page.slug)

    for slug, referrers in missing_targets.items():
        stub = Page(
            slug=slug,
            title=page_title(slug),
            page_type=classify_page(slug, page_title(slug), "migration"),
            summary_hint=page_title(slug),
            shape=PAGE_SHAPE_TOPIC,
        )
        for referrer in ordered_unique(referrers):
            stub.connections[referrer] += 1
        pages[slug] = stub


def related_slug_candidates(page: Page) -> list[str]:
    candidates: list[str] = []
    for source in page.sources.values():
        try:
            if source.path.startswith("../"):
                source_path = (ROOT / source.path[3:]).resolve()
            else:
                source_path = (ROOT / source.path).resolve()
        except Exception:
            continue
        for topic in derive_path_topics(source_path):
            if topic != page.slug and topic not in candidates:
                candidates.append(topic)
    return candidates


def find_best_related_slug(page: Page, pages: dict[str, Page]) -> str | None:
    best_slug: str | None = None
    best_score = 0.0
    for other_slug, other_page in pages.items():
        if other_slug == page.slug:
            continue
        score = slug_similarity(page.slug, other_slug)
        score += 0.2 * len(page.seed_kinds & other_page.seed_kinds)
        if score > best_score:
            best_score = score
            best_slug = other_slug
    if best_score > 0:
        return best_slug
    for topic in related_slug_candidates(page):
        if topic in pages and topic != page.slug:
            return topic
    return None


def prune_generic_media_links(pages: dict[str, Page]) -> None:
    media_slug = "unclassified-media-captures"
    media_suffixes = {".png", ".jpg", ".jpeg", ".heic", ".pdf"}
    for slug, page in pages.items():
        if slug == media_slug:
            continue
        has_media_source = any(Path(source.path).suffix.lower() in media_suffixes for source in page.sources.values())
        if not has_media_source:
            page.connections.pop(media_slug, None)


def ensure_meaningful_connections(pages: dict[str, Page]) -> None:
    ensure_connection_targets_exist(pages)
    for slug, page in pages.items():
        if slug in {"index", "log"}:
            continue
        if sorted_connection_slugs(page):
            continue
        related_slug = page.topic_parent if page.topic_parent in pages else find_best_related_slug(page, pages)
        if related_slug:
            connect_pages(pages, slug, related_slug)


def finalize_page_shapes(pages: dict[str, Page]) -> None:
    for page in pages.values():
        if page_shape(page) == PAGE_SHAPE_TOPIC:
            page.shape = PAGE_SHAPE_TOPIC
            page.notes = []
            page.rendered_notes_markdown = None
            page.open_questions = []
            page.sources = {}
        else:
            page.shape = PAGE_SHAPE_ATOMIC
            page.notes = ordered_unique(page.notes)
            page.open_questions = ordered_unique(page.open_questions)
            page.rendered_notes_markdown = build_simple_notes_markdown(page) or None


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
    page = pages.setdefault(slug, Page(slug=slug, title=title, page_type=page_type, summary_hint=summary_hint))
    page.seed_kinds.add(seed_kind)
    if note_text not in page.notes:
        page.notes.append(note_text)
    existing_source = page.sources.get(source_path)
    if existing_source is None:
        page.sources[source_path] = SourceEvidence(
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


def ensure_supporting_pages(pages: dict[str, Page], slug: str, original_title: str, seed_kind: str) -> None:
    pages.setdefault(
        slug,
        Page(
            slug=slug,
            title=page_title(slug),
            page_type=classify_page(slug, original_title, seed_kind),
            summary_hint=original_title,
        ),
    ).seed_kinds.add(seed_kind)


def normalize_page(page: Page) -> Page:
    return rendering_impl.normalize_page(page)


def validate_page(page: Page, *, allow_missing_outbound: bool = False) -> list[str]:
    return rendering_impl.validate_page(page, allow_missing_outbound=allow_missing_outbound)


def render_page(page: Page) -> str:
    return rendering_impl.render_page(page)


def render_index(pages: dict[str, Page]) -> str:
    return rendering_impl.render_index(pages, today=TODAY)


def split_report_manifest_payload(report: SplitPhaseReport) -> dict[str, object]:
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


def split_report_summary(report: SplitPhaseReport) -> str:
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


def _api():
    return sys.modules[__name__]


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


def apply_note_cache_entry_to_pages(
    *,
    pages: dict[str, Page],
    cache_entry: dict[str, object],
) -> None:
    cache_impl.apply_note_cache_entry_to_pages(_api(), pages=pages, cache_entry=cache_entry)


def accumulate_url_stats(url_stats: Counter, source: SourceRecord, fetch_urls_enabled: bool) -> None:
    cache_impl.accumulate_url_stats(_api(), url_stats, source, fetch_urls_enabled)


def stage_rendered_wiki(
    *,
    pages: dict[str, Page],
    existing_log_text: str,
    bootstrap_entry: str,
) -> Path:
    return cache_impl.stage_rendered_wiki(
        _api(),
        pages=pages,
        existing_log_text=existing_log_text,
        bootstrap_entry=bootstrap_entry,
    )


def append_wiki_query_log(summary: str) -> None:
    cache_impl.append_wiki_query_log(_api(), summary)


def swap_rendered_wiki(stage_dir: Path) -> None:
    cache_impl.swap_rendered_wiki(_api(), stage_dir)


def source_priority(source: SourceRecord) -> tuple[int, int, str]:
    return remote_impl.source_priority(_api(), source)


def select_sources_for_synthesis(
    page: Page,
    max_sources: int = 40,
    max_chars: int = 18_000,
) -> list[SourceRecord]:
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


def extract_note_topics(
    *,
    title: str,
    source_record: SourceRecord,
    api_key: str,
    model: str,
) -> list[str]:
    return remote_impl.extract_note_topics(
        _api(),
        title=title,
        source_record=source_record,
        api_key=api_key,
        model=model,
    )


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
