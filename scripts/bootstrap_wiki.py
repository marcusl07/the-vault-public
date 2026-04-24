from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
import hashlib
from pathlib import Path
import shutil
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen
import argparse
import html
import json
import os
import re
import socket
import sys
import tempfile
import time
import unicodedata


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

TODAY = date.today().isoformat()

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

TITLE_ALIASES = {
    "gtd": "GTD",
    "uci": "UCI",
    "ics-33": "ICS 33",
    "math-2a": "Math 2A",
    "math-2b": "Math 2B",
    "math-3a": "Math 3A",
    "stats-7": "Stats 7",
    "stats-67": "Stats 67",
    "writing-50": "Writing 50",
    "ai-photonics": "AI Photonics",
    "fpl-23-24": "FPL 23-24",
    "football-manager": "Football Manager",
    "fantasy-premier-league": "Fantasy Premier League",
    "apple-notes": "Apple Notes",
    "moka-pot": "Moka Pot",
    "sydney": "Sydney",
    "japanese": "Japanese",
    "uci-food": "UCI Food",
    "digital-minimalism": "Digital Minimalism",
}

ENTITY_SLUGS = {
    "sydney",
    "japan",
    "hong-kong",
    "bali",
    "arsenal",
    "manchester-united",
    "uci",
}

ASPIRATION_VERBS = {
    "learn",
    "visit",
    "go",
    "read",
    "try",
    "get",
    "buy",
    "run",
    "build",
    "make",
    "do",
    "write",
    "take",
    "start",
    "stop",
    "be",
    "live",
    "explore",
    "improve",
    "create",
    "find",
}

TOPIC_EXTRACTION_MAX_BODY_CHARS = 4_000
TOPIC_EXTRACTION_CONFIDENT_LEVELS = {"high", "medium"}
PAGE_SPLIT_MAX_BODY_CHARS = 6_000
PAGE_SHAPE_ATOMIC = "atomic"
PAGE_SHAPE_TOPIC = "topic"
INDEX_SECTION_ORDER = ("Concepts", "Entities", "Experiences", "Aspirations")
SOURCE_LINE_RE = re.compile(r"^- \[(?P<label>[^\]]+)\]\((?P<path>[^)]+)\)(?P<suffix>.*)$")
CONNECTION_SLUG_RE = re.compile(r"\[\[(?P<slug>[^\]]+)\]\]")
BOLD_HEADING_RE = re.compile(r"^[-*+]\s*\*\*(.+?)\*\*\s*$")
BOILERPLATE_PATTERNS = (
    re.compile(r"^This page collects Marcus's notes about .* across \d+ source(?:s)?\.$"),
    re.compile(r"^- No notes yet\.$"),
    re.compile(r"^- No sources linked yet\.$"),
)
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
class SourceRecord:
    label: str
    path: str
    status: str
    raw_content: str
    cleaned_text: str
    fetched_summary: str | None
    detected_url: str | None
    tags: set[str] = field(default_factory=set)
    excluded_from_body: bool = False


@dataclass
class Page:
    slug: str
    title: str
    page_type: str
    summary_hint: str
    shape: str = PAGE_SHAPE_ATOMIC
    notes: list[str] = field(default_factory=list)
    connections: Counter = field(default_factory=Counter)
    sources: dict[str, SourceRecord] = field(default_factory=dict)
    seed_kinds: set[str] = field(default_factory=set)
    rendered_summary: str | None = None
    rendered_notes_markdown: str | None = None
    topic_parent: str | None = None


@dataclass
class ParsedWikiPage:
    slug: str
    title: str
    page_type: str
    shape: str
    summary_lines: list[str] = field(default_factory=list)
    note_lines: list[str] = field(default_factory=list)
    connection_slugs: list[str] = field(default_factory=list)
    sources: dict[str, SourceRecord] = field(default_factory=dict)


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
    original = (
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
    configure_workspace(root)
    try:
        yield
    finally:
        (
            restored_root,
            restored_raw_root,
            restored_apple_notes_root,
            restored_wiki_root,
            restored_cache_root,
            restored_cache_notes_root,
            restored_cache_pages_root,
            restored_cache_manifest_path,
            restored_render_stage_root,
            restored_render_backup_root,
        ) = original
        globals().update(
            {
                "ROOT": restored_root,
                "RAW_ROOT": restored_raw_root,
                "APPLE_NOTES_ROOT": restored_apple_notes_root,
                "WIKI_ROOT": restored_wiki_root,
                "CACHE_ROOT": restored_cache_root,
                "CACHE_NOTES_ROOT": restored_cache_notes_root,
                "CACHE_PAGES_ROOT": restored_cache_pages_root,
                "CACHE_MANIFEST_PATH": restored_cache_manifest_path,
                "RENDER_STAGE_ROOT": restored_render_stage_root,
                "RENDER_BACKUP_ROOT": restored_render_backup_root,
            }
        )


def stable_json_dumps(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def fingerprint_payload(payload: object) -> str:
    return hashlib.sha256(stable_json_dumps(payload).encode("utf-8")).hexdigest()


def cache_key(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    safe_prefix = f".{hashlib.sha1(path.name.encode('utf-8')).hexdigest()[:12]}."
    fd, tmp_name = tempfile.mkstemp(prefix=safe_prefix, suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def atomic_write_json(path: Path, payload: object) -> None:
    atomic_write_text(path, stable_json_dumps(payload) + "\n")


def read_json_file(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def note_cache_path(source_path: str) -> Path:
    return CACHE_NOTES_ROOT / f"{cache_key(source_path)}.json"


def page_cache_path(slug: str) -> Path:
    return CACHE_PAGES_ROOT / f"{cache_key(slug)}.json"


def update_manifest(**fields: object) -> None:
    current = read_json_file(CACHE_MANIFEST_PATH) or {}
    current.update(fields)
    current["updated_at"] = TODAY
    atomic_write_json(CACHE_MANIFEST_PATH, current)


def source_record_to_cache_dict(source: SourceRecord) -> dict[str, object]:
    return {
        "label": source.label,
        "path": source.path,
        "status": source.status,
        "cleaned_text": source.cleaned_text,
        "fetched_summary": source.fetched_summary,
        "detected_url": source.detected_url,
        "tags": sorted(source.tags),
        "excluded_from_body": source.excluded_from_body,
    }


def source_record_from_cache_dict(payload: dict[str, object]) -> SourceRecord:
    return SourceRecord(
        label=str(payload["label"]),
        path=str(payload["path"]),
        status=str(payload["status"]),
        raw_content="",
        cleaned_text=str(payload.get("cleaned_text", "")),
        fetched_summary=str(payload["fetched_summary"]) if payload.get("fetched_summary") is not None else None,
        detected_url=str(payload["detected_url"]) if payload.get("detected_url") is not None else None,
        tags=set(str(tag) for tag in payload.get("tags", [])),
        excluded_from_body=bool(payload.get("excluded_from_body", False)),
    )


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


def page_title(slug: str) -> str:
    if slug in TITLE_ALIASES:
        return TITLE_ALIASES[slug]
    words = slug.split("-")
    titled = []
    for word in words:
        if word in {"and", "of", "to", "for", "in"}:
            titled.append(word)
        elif word.isdigit():
            titled.append(word)
        else:
            titled.append(word.capitalize())
    return " ".join(titled)


def classify_page(slug: str, original_title: str, seed_kind: str) -> str:
    lowered = original_title.strip().lower()
    if slug in ENTITY_SLUGS:
        return "Entities"
    first_word = slug.split("-", 1)[0]
    if first_word in ASPIRATION_VERBS:
        return "Aspirations"
    if seed_kind in {"title", "model"} and re.search(r"\b(trip|birthday|anniversary|camping|dinner|date)\b", lowered):
        return "Experiences"
    return "Concepts"


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


def strip_markdown(text: str) -> str:
    text = re.sub(r"!\[\[([^\]]+)\]\]", r"embedded media: \1", text)
    text = re.sub(r"\[\[(.*?)\]\]", r"\1", text)
    text = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("**", " ").replace("__", " ")
    text = text.replace("*", " ").replace("_", " ")
    text = re.sub(r"`+", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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
) -> SourceRecord:
    cleaned_text = clean_source_text(raw_content, source_label)
    tags = detect_source_tags(source_label, cleaned_text, detected_url, fetched_summary)
    return SourceRecord(
        label=source_label,
        path=source_path,
        status=source_status,
        raw_content=raw_content,
        cleaned_text=cleaned_text,
        fetched_summary=fetched_summary,
        detected_url=detected_url,
        tags=tags,
        excluded_from_body=should_exclude_from_body(tags),
    )


def build_note_fingerprint(path: Path, content: str, fetch_urls_enabled: bool) -> str:
    return fingerprint_payload(
        {
            "path": path.relative_to(ROOT).as_posix(),
            "content": content,
            "fetch_urls": fetch_urls_enabled,
        }
    )


def build_note_cache_entry(
    *,
    fingerprint: str,
    title: str,
    source_record: SourceRecord,
    note_text: str,
    page_assignments: list[tuple[str, str]],
    skipped: bool,
) -> dict[str, object]:
    return {
        "fingerprint": fingerprint,
        "title": title,
        "source_record": source_record_to_cache_dict(source_record),
        "note_text": note_text,
        "page_assignments": [
            {
                "slug": slug,
                "seed_kind": seed_kind,
            }
            for slug, seed_kind in page_assignments
        ],
        "skipped": skipped,
    }


def build_page_fingerprint(page: Page) -> str:
    serialized_sources = []
    for source_path in sorted(page.sources):
        source = page.sources[source_path]
        serialized_sources.append(
            {
                "path": source.path,
                "label": source.label,
                "status": source.status,
                "cleaned_text": source.cleaned_text,
                "fetched_summary": source.fetched_summary,
                "detected_url": source.detected_url,
                "tags": sorted(source.tags),
                "excluded_from_body": source.excluded_from_body,
            }
        )
    return fingerprint_payload(
        {
            "slug": page.slug,
            "title": page.title,
            "page_type": page.page_type,
            "sources": serialized_sources,
        }
    )


def apply_note_cache_entry_to_pages(
    *,
    pages: dict[str, Page],
    cache_entry: dict[str, object],
) -> None:
    if bool(cache_entry.get("skipped", False)):
        return

    source_payload = cache_entry.get("source_record")
    if not isinstance(source_payload, dict):
        raise ValueError("Cached note entry missing source_record.")

    source_record = source_record_from_cache_dict(source_payload)
    title = str(cache_entry.get("title", source_record.label))
    note_text = str(cache_entry.get("note_text", ""))
    page_assignments = cache_entry.get("page_assignments", [])
    seen: set[str] = set()
    unique_page_slugs: list[tuple[str, str]] = []
    for assignment in page_assignments:
        if not isinstance(assignment, dict):
            continue
        slug = str(assignment.get("slug", "")).strip()
        seed_kind = str(assignment.get("seed_kind", "folder")).strip() or "folder"
        if not slug or slug in seen:
            continue
        seen.add(slug)
        unique_page_slugs.append((slug, seed_kind))
        ensure_supporting_pages(pages, slug, title, seed_kind)
        add_page_note(
            pages=pages,
            slug=slug,
            title=page_title(slug),
            page_type=classify_page(slug, title, seed_kind),
            summary_hint=title,
            note_text=note_text,
            source_label=source_record.label,
            source_path=source_record.path,
            source_status=source_record.status,
            seed_kind=seed_kind,
        )
        pages[slug].sources[source_record.path] = source_record

    slugs_only = [slug for slug, _seed_kind in unique_page_slugs]
    for slug in slugs_only:
        for other in slugs_only:
            if other != slug:
                pages[slug].connections[other] += 1


def accumulate_url_stats(url_stats: Counter, source: SourceRecord, fetch_urls_enabled: bool) -> None:
    if not fetch_urls_enabled:
        return
    if source.detected_url:
        url_stats["url_notes"] += 1
        url_stats[source.status] += 1


def stage_rendered_wiki(
    *,
    pages: dict[str, Page],
    existing_log_text: str,
    bootstrap_entry: str,
) -> Path:
    if RENDER_STAGE_ROOT.exists():
        shutil.rmtree(RENDER_STAGE_ROOT)
    if RENDER_BACKUP_ROOT.exists():
        shutil.rmtree(RENDER_BACKUP_ROOT)

    RENDER_STAGE_ROOT.mkdir(parents=True, exist_ok=True)
    for page in pages.values():
        target = RENDER_STAGE_ROOT / f"{page.slug}.md"
        atomic_write_text(target, render_page(page))

    atomic_write_text(RENDER_STAGE_ROOT / "index.md", render_index(pages))

    existing_lines = [
        line
        for line in existing_log_text.splitlines()
        if not line.startswith(f"## [{TODAY}] bootstrap |")
    ]
    staged_log = "\n".join(existing_lines).rstrip() + "\n\n" + bootstrap_entry
    atomic_write_text(RENDER_STAGE_ROOT / "log.md", staged_log)
    return RENDER_STAGE_ROOT


def swap_rendered_wiki(stage_dir: Path) -> None:
    backup_dir = RENDER_BACKUP_ROOT
    if backup_dir.exists():
        shutil.rmtree(backup_dir)
    if WIKI_ROOT.exists():
        WIKI_ROOT.rename(backup_dir)
    try:
        stage_dir.rename(WIKI_ROOT)
    except Exception:
        if backup_dir.exists() and not WIKI_ROOT.exists():
            backup_dir.rename(WIKI_ROOT)
        raise
    if backup_dir.exists():
        shutil.rmtree(backup_dir)


def compact_source_text(source: SourceRecord, limit: int = 800) -> str:
    body_parts = []
    if source.fetched_summary:
        body_parts.append(f"Fetched summary: {source.fetched_summary}")
    if source.cleaned_text:
        body_parts.append(source.cleaned_text)
    combined = "\n\n".join(body_parts).strip()
    if len(combined) <= limit:
        return combined
    return combined[: limit - 3].rstrip() + "..."


def source_priority(source: SourceRecord) -> tuple[int, int, str]:
    informative_tags = {"dates", "address", "gift_ideas", "activities", "places", "food", "travel_plans", "preferences"}
    score = 0
    score += 5 * len(source.tags & informative_tags)
    if source.fetched_summary:
        score += 2
    if "low_signal" in source.tags:
        score -= 2
    if "uncategorized" in source.tags:
        score -= 1
    content_length = len(compact_source_text(source, limit=400))
    return (-score, content_length, source.label.lower())


def select_sources_for_synthesis(
    page: Page,
    max_sources: int = 40,
    max_chars: int = 18_000,
) -> list[SourceRecord]:
    selected: list[SourceRecord] = []
    total_chars = 0
    body_sources = sorted(
        (source for source in page.sources.values() if not source.excluded_from_body),
        key=source_priority,
    )
    for source in body_sources:
        excerpt = compact_source_text(source)
        estimated_chars = len(excerpt) + 200
        if selected and (len(selected) >= max_sources or total_chars + estimated_chars > max_chars):
            continue
        selected.append(source)
        total_chars += estimated_chars
        if len(selected) >= max_sources or total_chars >= max_chars:
            break
    return selected


def serialize_sources_for_prompt(page: Page) -> str:
    prompt_chunks: list[str] = []
    body_sources = select_sources_for_synthesis(page)
    for index, source in enumerate(body_sources, start=1):
        tags = ", ".join(sorted(source.tags - {"fetched_summary", "has_url"})) or "uncategorized"
        lines = [
            f"Source {index}: {source.label}",
            f"Path: {source.path}",
            f"Status: {source.status}",
            f"Tags: {tags}",
        ]
        if source.detected_url:
            lines.append(f"URL: {source.detected_url}")
        excerpt = compact_source_text(source)
        if excerpt:
            lines.append("Content:")
            lines.append(excerpt)
        prompt_chunks.append("\n".join(lines))
    omitted_count = sum(1 for source in page.sources.values() if not source.excluded_from_body) - len(body_sources)
    if omitted_count > 0:
        prompt_chunks.append(
            f"Omitted {omitted_count} lower-priority sources to keep synthesis within model limits. "
            "Prefer recurring, concrete facts from the included sources and avoid overgeneralizing."
        )
    return "\n\n".join(prompt_chunks)


def build_synthesis_messages(page: Page) -> list[dict[str, str]]:
    section_hint = (
        "For entity pages, prefer sections like `### Key Dates`, `### Gift Ideas`, `### Places`, "
        "`### Preferences`, and `### Notes`, but only include sections supported by the sources."
        if page.page_type == "Entities"
        else "For concept or resource pages, prefer a short synthesis paragraph followed by grouped subsections or list sections only when the material is inherently list-like."
    )
    system_prompt = (
        "You are writing a personal wiki page from source notes. "
        "Use only the provided notes. Do not invent facts. "
        "Return strict JSON with keys `summary` and `notes_markdown`. "
        "`summary` must be one short prose paragraph. "
        "`notes_markdown` must be valid Markdown for the body of the `## Notes` section only. "
        "Use thematic subsections when appropriate. Use bullets only for genuine lists. "
        "Do not include sensitive credentials, explicit content, or low-signal personal noise."
    )
    user_prompt = "\n\n".join(
        [
            f"Page title: {page.title}",
            f"Page type: {page.page_type}",
            section_hint,
            "Write a readable reference page grounded only in these source notes.",
            "Do not add a top-level heading or a `## Notes` heading.",
            "Sources excluded for sensitivity or noise are not included below and must not be surfaced.",
            "Source notes:",
            serialize_sources_for_prompt(page),
        ]
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def parse_synthesis_response(content: str) -> tuple[str, str]:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if not match:
            raise
        payload = json.loads(match.group(0))
    summary = str(payload.get("summary", "")).strip()
    notes_markdown = str(payload.get("notes_markdown", "")).strip()
    if not summary or not notes_markdown:
        raise ValueError("Synthesis response missing summary or notes_markdown.")
    return summary, notes_markdown


def gemini_generate(
    *,
    messages: list[dict[str, str]],
    api_key: str,
    model: str,
    response_schema: dict[str, object],
    attempts: int = 4,
    timeout: float = 90,
) -> str:
    request_body = {
        "generationConfig": {
            "temperature": 0.2,
            "responseMimeType": "application/json",
            "responseSchema": response_schema,
        },
        "contents": [
            {
                "role": "user",
                "parts": [{"text": message["content"]}],
            }
            for message in messages
        ],
    }
    request = Request(
        f"https://generativelanguage.googleapis.com/v1beta/models/{quote(model, safe='')}:generateContent?key={quote(api_key, safe='')}",
        data=json.dumps(request_body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
        },
        method="POST",
    )
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
            return payload["candidates"][0]["content"]["parts"][0]["text"]
        except HTTPError as error:
            last_error = error
            if error.code not in {429, 500, 502, 503, 504} or attempt == attempts:
                raise
        except URLError as error:
            last_error = error
            if attempt == attempts:
                raise
        except TimeoutError as error:
            last_error = error
            if attempt == attempts:
                raise
        except socket.timeout as error:
            last_error = error
            if attempt == attempts:
                raise
        time.sleep(min(2 ** (attempt - 1), 8))
    assert last_error is not None
    raise last_error


def synthesize_page(page: Page, api_key: str, model: str) -> tuple[str, str]:
    body_sources = select_sources_for_synthesis(page)
    if not body_sources:
        return (
            f"This page collects Marcus's notes about {page.title} across {len(page.sources)} source"
            f"{'' if len(page.sources) == 1 else 's'}.",
            build_simple_notes_markdown(page),
        )

    try:
        content = gemini_generate(
            messages=build_synthesis_messages(page),
            api_key=api_key,
            model=model,
            response_schema={
                "type": "OBJECT",
                "required": ["summary", "notes_markdown"],
                "properties": {
                    "summary": {
                        "type": "STRING",
                    },
                    "notes_markdown": {
                        "type": "STRING",
                    },
                },
            },
        )
    except (HTTPError, URLError, TimeoutError, socket.timeout):
        return (
            f"This page collects Marcus's notes about {page.title} across {len(page.sources)} source"
            f"{'' if len(page.sources) == 1 else 's'}.",
            build_simple_notes_markdown(page),
        )
    try:
        return parse_synthesis_response(content)
    except (json.JSONDecodeError, ValueError):
        return (
            f"This page collects Marcus's notes about {page.title} across {len(page.sources)} source"
            f"{'' if len(page.sources) == 1 else 's'}.",
            build_simple_notes_markdown(page),
        )


def build_topic_extraction_messages(
    *,
    title: str,
    cleaned_text: str,
    fetched_summary: str | None,
    detected_url: str | None,
) -> list[dict[str, str]]:
    excerpt = cleaned_text.strip()
    if len(excerpt) > TOPIC_EXTRACTION_MAX_BODY_CHARS:
        excerpt = excerpt[: TOPIC_EXTRACTION_MAX_BODY_CHARS - 3].rstrip() + "..."
    parts = [
        "Extract 1-3 canonical wiki topic slugs for a single note.",
        "Prioritize the note's actual concepts, entities, experiences, or aspirations.",
        "Ignore storage hierarchy, PARA folders, and generic buckets like resources, projects, or archive.",
        "Return only confident topics. If the note is weak or ambiguous, return an empty topics array.",
        "Each slug must be lowercase kebab-case, concise, and reusable across notes about the same topic.",
        f"Title: {title}",
    ]
    if detected_url:
        parts.append(f"URL: {detected_url}")
    if fetched_summary:
        parts.append(f"URL metadata: {fetched_summary}")
    parts.append("Cleaned note body:")
    parts.append(excerpt or "[empty]")
    return [
        {
            "role": "system",
            "content": (
                "You extract canonical topic slugs for a personal wiki. "
                "Use only the note title, body, and URL metadata provided. "
                "Return strict JSON."
            ),
        },
        {
            "role": "user",
            "content": "\n\n".join(parts),
        },
    ]


def parse_topic_extraction_response(content: str) -> list[tuple[str, str]]:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if not match:
            raise
        payload = json.loads(match.group(0))

    topics = payload.get("topics", [])
    if not isinstance(topics, list):
        raise ValueError("Topic extraction response missing topics list.")

    parsed: list[tuple[str, str]] = []
    seen: set[str] = set()
    for item in topics:
        if not isinstance(item, dict):
            continue
        raw_slug = str(item.get("slug", "")).strip()
        confidence = str(item.get("confidence", "")).strip().lower()
        slug = clean_component(raw_slug) or slugify(raw_slug)
        if not slug or confidence not in {"high", "medium", "low"} or slug in seen:
            continue
        seen.add(slug)
        parsed.append((slug, confidence))
    return parsed[:3]


def extract_note_topics(
    *,
    title: str,
    source_record: SourceRecord,
    api_key: str,
    model: str,
) -> list[str]:
    content = gemini_generate(
        messages=build_topic_extraction_messages(
            title=title,
            cleaned_text=source_record.cleaned_text,
            fetched_summary=source_record.fetched_summary,
            detected_url=source_record.detected_url,
        ),
        api_key=api_key,
        model=model,
        response_schema={
            "type": "OBJECT",
            "required": ["topics"],
            "properties": {
                "topics": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "required": ["slug", "confidence"],
                        "properties": {
                            "slug": {"type": "STRING"},
                            "confidence": {
                                "type": "STRING",
                                "enum": ["high", "medium", "low"],
                            },
                        },
                    },
                },
            },
        },
    )
    extracted = parse_topic_extraction_response(content)
    return [slug for slug, confidence in extracted if confidence in TOPIC_EXTRACTION_CONFIDENT_LEVELS]


def build_page_split_messages(page: Page) -> list[dict[str, str]]:
    note_content = page.rendered_notes_markdown or build_simple_notes_markdown(page)
    note_content = note_content.strip() or "[empty]"
    if len(note_content) > PAGE_SPLIT_MAX_BODY_CHARS:
        note_content = note_content[: PAGE_SPLIT_MAX_BODY_CHARS - 3].rstrip() + "..."

    source_lines = []
    for index, source in enumerate(sorted(page.sources.values(), key=lambda item: item.path), start=1):
        source_lines.append(f"{index}. {source.label} | {source.path}")

    return [
        {
            "role": "system",
            "content": (
                "You are auditing a personal wiki page for atomicity like a careful Zettelkasten editor. "
                "Decide whether the page should remain one reusable note or be split into multiple reusable satellite notes. "
                "Mark a page atomic when the material still reads as one note, even if it contains several closely related subtopics. "
                "Use only the page notes and source list provided. "
                "Return strict JSON."
            ),
        },
        {
            "role": "user",
            "content": "\n\n".join(
                [
                    f"Page title: {page.title}",
                    f"Page slug: {page.slug}",
                    "Task:",
                    "1. Decide whether this page should stay as a single reusable note.",
                    "2. If it should split, extract concise reusable kebab-case satellite slugs for the distinct notes.",
                    "3. For each proposed child, evaluate whether it is worth linking to directly and whether it would remain a stable page as more detail is added.",
                    "4. If it should split, include source assignments when they are clear and natural.",
                    "Rules:",
                    "- Do not require multiple source files. One raw source may support multiple child pages when it substantively grounds each child.",
                    "- Return is_atomic=true when this still feels like one note a real Zettelkasten user would keep together.",
                    "- Do not split just because one lecture note contains several related subtopics.",
                    "- Return is_atomic=false when the page is clearly a bucket containing multiple standalone notes.",
                    "- Do not return the parent slug.",
                    "- Do not return generic buckets like notes, ideas, resources, archive, misc, or overview.",
                    "- Prefer stable concept/entity/experience slugs that could stand as atomic pages.",
                    "- If a candidate child fails the reusable-idea test, explain the rejection reason instead of including it as an accepted child.",
                    "- Each accepted child must have enough grounded support to justify at least one meaningful note, not just a passing mention.",
                    "- If the page is atomic, return an empty candidate_satellite_slugs array and an empty source_assignments array.",
                    "- source_assignments may be partial when some source-to-note mapping is ambiguous.",
                    "- When source_assignments are ambiguous or the page has only one source, leave source_assignments empty rather than inventing ownership.",
                    "Return JSON fields:",
                    "- is_atomic: boolean",
                    "- rationale: short string",
                    "- rejection_reasons: array of short strings explaining why the page stays atomic or why a proposed split is rejected",
                    "- candidate_satellite_slugs: array of accepted kebab-case child slugs only",
                    "- candidate_evaluations: array of objects with slug, accepted, grounding, why_distinct, passes_direct_link_test, passes_stable_page_test, passes_search_test, rejection_reasons",
                    "- source_assignments: array of {source_path, satellite_slug}",
                    "Full page note content:",
                    note_content,
                    "Sources:",
                    "\n".join(source_lines) or "[none]",
                ]
            ),
        },
    ]


def parse_page_split_response(content: str, parent_slug: str, source_paths: set[str]) -> PageSplitDecision:
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.S)
        if not match:
            raise
        payload = json.loads(match.group(0))

    is_atomic = bool(payload.get("is_atomic", False))
    rationale_value = payload.get("rationale")
    rationale = str(rationale_value).strip() if isinstance(rationale_value, str) and rationale_value.strip() else None
    raw_rejection_reasons = payload.get("rejection_reasons", [])
    raw_candidates = payload.get("candidate_satellite_slugs", [])
    raw_candidate_evaluations = payload.get("candidate_evaluations", [])
    raw_assignments = payload.get("source_assignments", [])

    if (
        not isinstance(raw_candidates, list)
        or not isinstance(raw_candidate_evaluations, list)
        or not isinstance(raw_assignments, list)
        or not isinstance(raw_rejection_reasons, list)
    ):
        raise ValueError("Split response missing expected arrays.")

    candidate_slugs: list[str] = []
    seen_candidates: set[str] = set()
    for raw_slug in raw_candidates:
        slug = clean_component(str(raw_slug).strip()) or slugify(str(raw_slug).strip())
        if not slug or slug == parent_slug or slug in seen_candidates:
            continue
        seen_candidates.add(slug)
        candidate_slugs.append(slug)

    rejection_reasons = [str(item).strip() for item in raw_rejection_reasons if str(item).strip()]
    candidate_evaluations: list[SplitCandidateEvaluation] = []
    seen_evaluation_slugs: set[str] = set()
    for item in raw_candidate_evaluations:
        if not isinstance(item, dict):
            continue
        slug = clean_component(str(item.get("slug", "")).strip()) or slugify(str(item.get("slug", "")).strip())
        if not slug or slug == parent_slug or slug in seen_evaluation_slugs:
            continue
        seen_evaluation_slugs.add(slug)
        grounding = [str(entry).strip() for entry in item.get("grounding", []) if str(entry).strip()]
        why_distinct_value = item.get("why_distinct")
        why_distinct = (
            str(why_distinct_value).strip()
            if isinstance(why_distinct_value, str) and str(why_distinct_value).strip()
            else None
        )
        candidate_evaluations.append(
            SplitCandidateEvaluation(
                slug=slug,
                accepted=bool(item.get("accepted", False)),
                grounding=grounding,
                why_distinct=why_distinct,
                passes_direct_link_test=bool(item.get("passes_direct_link_test", False)),
                passes_stable_page_test=bool(item.get("passes_stable_page_test", False)),
                passes_search_test=bool(item.get("passes_search_test", False)),
                rejection_reasons=[
                    str(reason).strip() for reason in item.get("rejection_reasons", []) if str(reason).strip()
                ],
            )
        )

    if not candidate_slugs:
        for evaluation in candidate_evaluations:
            if evaluation.accepted and evaluation.slug not in seen_candidates:
                seen_candidates.add(evaluation.slug)
                candidate_slugs.append(evaluation.slug)

    source_assignments: dict[str, str] = {}
    for item in raw_assignments:
        if not isinstance(item, dict):
            continue
        source_path = str(item.get("source_path", "")).strip()
        satellite_slug = clean_component(str(item.get("satellite_slug", "")).strip()) or slugify(
            str(item.get("satellite_slug", "")).strip()
        )
        if (
            not source_path
            or source_path not in source_paths
            or not satellite_slug
            or satellite_slug == parent_slug
            or satellite_slug not in seen_candidates
        ):
            continue
        source_assignments[source_path] = satellite_slug

    if is_atomic:
        return PageSplitDecision(
            is_atomic=True,
            rationale=rationale,
            rejection_reasons=rejection_reasons,
            candidate_evaluations=candidate_evaluations,
        )
    return PageSplitDecision(
        is_atomic=False,
        candidate_satellite_slugs=candidate_slugs,
        source_assignments=source_assignments,
        rationale=rationale,
        rejection_reasons=rejection_reasons,
        candidate_evaluations=candidate_evaluations,
    )


def split_failure_mode() -> str:
    mode = os.environ.get("BOOTSTRAP_SPLIT_FAILURE_MODE", "no-split").strip().lower()
    return "fail" if mode == "fail" else "no-split"


def split_request_timeout() -> float:
    raw_value = os.environ.get("BOOTSTRAP_SPLIT_TIMEOUT_SECONDS", "30").strip()
    try:
        return max(5.0, float(raw_value))
    except ValueError:
        return 30.0


def split_request_attempts() -> int:
    raw_value = os.environ.get("BOOTSTRAP_SPLIT_ATTEMPTS", "2").strip()
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 2


def split_debug_enabled() -> bool:
    raw_value = os.environ.get("BOOTSTRAP_SPLIT_DEBUG", "").strip().lower()
    return raw_value in {"1", "true", "yes", "on"}


def format_split_decision_debug(page: Page, split_decision: PageSplitDecision) -> str:
    lines = [f"Split debug [{page.slug}]"]
    if split_decision.rationale:
        lines.append(f"  rationale: {split_decision.rationale}")
    if split_decision.rejection_reasons:
        lines.append(f"  rejection_reasons: {', '.join(split_decision.rejection_reasons)}")
    if split_decision.candidate_satellite_slugs:
        lines.append(f"  accepted_children: {', '.join(split_decision.candidate_satellite_slugs)}")
    if not split_decision.candidate_evaluations:
        lines.append("  candidate_evaluations: [none]")
        return "\n".join(lines)

    lines.append("  candidate_evaluations:")
    for evaluation in split_decision.candidate_evaluations:
        status = "accepted" if evaluation.accepted else "rejected"
        tests = ", ".join(
            [
                f"direct_link={'yes' if evaluation.passes_direct_link_test else 'no'}",
                f"stable_page={'yes' if evaluation.passes_stable_page_test else 'no'}",
                f"search={'yes' if evaluation.passes_search_test else 'no'}",
            ]
        )
        lines.append(f"    - {evaluation.slug}: {status}; {tests}")
        if evaluation.why_distinct:
            lines.append(f"      why_distinct: {evaluation.why_distinct}")
        if evaluation.grounding:
            lines.append(f"      grounding: {' | '.join(evaluation.grounding)}")
        if evaluation.rejection_reasons:
            lines.append(f"      rejection_reasons: {', '.join(evaluation.rejection_reasons)}")
    return "\n".join(lines)


def split_counts_toward_transport_abort(error: Exception) -> bool:
    if isinstance(error, HTTPError):
        return error.code in {429, 500, 502, 503, 504}
    if isinstance(error, URLError):
        reason = getattr(error, "reason", None)
        if isinstance(reason, socket.timeout):
            return False
        if isinstance(reason, TimeoutError):
            return False
        if isinstance(reason, str) and "timed out" in reason.lower():
            return False
        return True
    return False


def split_preflight_check(api_key: str | None, model: str, timeout: float = 5.0) -> tuple[bool, str | None]:
    if not api_key:
        return False, "missing GEMINI_API_KEY"

    request = Request(
        f"https://generativelanguage.googleapis.com/v1beta/models/{quote(model, safe='')}?key={quote(api_key, safe='')}",
        headers={"Accept": "application/json"},
        method="GET",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            if getattr(response, "status", 200) >= 400:
                return False, f"preflight returned HTTP {response.status}"
        return True, None
    except HTTPError as error:
        return False, f"HTTPError: {error}"
    except URLError as error:
        return False, f"URLError: {error}"
    except TimeoutError as error:
        return False, f"TimeoutError: {error}"
    except socket.timeout as error:
        return False, f"timeout: {error}"


def analyze_page_for_atomic_split(page: Page, api_key: str | None, model: str) -> PageSplitDecision:
    if not api_key:
        return PageSplitDecision(is_atomic=True)

    content = gemini_generate(
        messages=build_page_split_messages(page),
        api_key=api_key,
        model=model,
        response_schema={
            "type": "OBJECT",
            "required": [
                "is_atomic",
                "rationale",
                "rejection_reasons",
                "candidate_satellite_slugs",
                "candidate_evaluations",
                "source_assignments",
            ],
            "properties": {
                "is_atomic": {"type": "BOOLEAN"},
                "rationale": {"type": "STRING"},
                "rejection_reasons": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                },
                "candidate_satellite_slugs": {
                    "type": "ARRAY",
                    "items": {"type": "STRING"},
                },
                "candidate_evaluations": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "required": [
                            "slug",
                            "accepted",
                            "grounding",
                            "why_distinct",
                            "passes_direct_link_test",
                            "passes_stable_page_test",
                            "passes_search_test",
                            "rejection_reasons",
                        ],
                        "properties": {
                            "slug": {"type": "STRING"},
                            "accepted": {"type": "BOOLEAN"},
                            "grounding": {
                                "type": "ARRAY",
                                "items": {"type": "STRING"},
                            },
                            "why_distinct": {"type": "STRING"},
                            "passes_direct_link_test": {"type": "BOOLEAN"},
                            "passes_stable_page_test": {"type": "BOOLEAN"},
                            "passes_search_test": {"type": "BOOLEAN"},
                            "rejection_reasons": {
                                "type": "ARRAY",
                                "items": {"type": "STRING"},
                            },
                        },
                    },
                },
                "source_assignments": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "required": ["source_path", "satellite_slug"],
                        "properties": {
                            "source_path": {"type": "STRING"},
                            "satellite_slug": {"type": "STRING"},
                        },
                    },
                },
            },
        },
        attempts=split_request_attempts(),
        timeout=split_request_timeout(),
    )
    return parse_page_split_response(content, page.slug, set(page.sources))


def summarize_remote_page(html_text: str) -> str | None:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.I | re.S)
    meta_match = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
        html_text,
        flags=re.I | re.S,
    )
    if not meta_match:
        meta_match = re.search(
            r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']description["\']',
            html_text,
            flags=re.I | re.S,
        )

    parts = []
    if title_match:
        title = strip_markdown(html.unescape(title_match.group(1))).strip()
        if title:
            parts.append(title)
    if meta_match:
        description = strip_markdown(html.unescape(meta_match.group(1))).strip()
        if description:
            if len(description) > 180:
                description = description[:177].rstrip() + "..."
            parts.append(description)
    if not parts:
        h1_match = re.search(r"<h1[^>]*>(.*?)</h1>", html_text, flags=re.I | re.S)
        if h1_match:
            h1_title = strip_markdown(html.unescape(h1_match.group(1))).strip()
            if h1_title:
                parts.append(h1_title)
    if not parts:
        return None
    return " — ".join(parts[:2])


def fetch_youtube_oembed_summary(url: str, timeout: float = 8.0) -> FetchResult:
    oembed_url = f"https://www.youtube.com/oembed?url={quote(url, safe='')}&format=json"
    try:
        request = Request(
            oembed_url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; CodexWikiBootstrap/1.0)",
                "Accept": "application/json",
            },
        )
        with urlopen(request, timeout=timeout) as response:
            raw = response.read(50_000)
        payload = json.loads(raw.decode("utf-8", errors="ignore"))
        title = strip_markdown(str(payload.get("title", ""))).strip()
        author_name = strip_markdown(str(payload.get("author_name", ""))).strip()
        if title and author_name:
            return FetchResult(f"{title} — {author_name}", "fetched")
        if title:
            return FetchResult(title, "fetched")
        return FetchResult(None, "fetch_failed")
    except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError):
        return FetchResult(None, "fetch_failed")


def fetch_url_summary(url: str, timeout: float = 8.0) -> FetchResult:
    if is_youtube_url(url):
        return fetch_youtube_oembed_summary(url, timeout=timeout)
    try:
        request = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; CodexWikiBootstrap/1.0)",
                "Accept": "text/html,application/xhtml+xml",
            },
        )
        with urlopen(request, timeout=timeout) as response:
            content_type = response.headers.get("Content-Type", "")
            if "text/html" not in content_type.lower():
                host = urlparse(url).netloc or url
                return FetchResult(f"Referenced external resource at {host}.", "non_html")
            raw = response.read(200_000)
        text = raw.decode("utf-8", errors="ignore")
        summary = summarize_remote_page(text)
        if summary:
            return FetchResult(summary, "fetched")
        host = urlparse(url).netloc or url
        return FetchResult(f"Referenced article or page at {host}.", "fetched")
    except HTTPError as error:
        if error.code in {404, 410}:
            return FetchResult(None, "http_dead")
        return FetchResult(None, "fetch_failed")
    except (URLError, TimeoutError, ValueError):
        return FetchResult(None, "fetch_failed")


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
    if APPLE_NOTES_ROOT in path.parents:
        relative_parts = path.relative_to(APPLE_NOTES_ROOT).parts[:-1]
    else:
        relative_parts = path.relative_to(RAW_ROOT).parts[:-1]
    for component in reversed(relative_parts):
        topic = clean_component(component)
        if topic and topic not in topics:
            topics.append(topic)
        if len(topics) >= 3:
            break
    return topics


def ordered_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


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


def parse_source_line(line: str, retained_evidence: str = "") -> SourceRecord | None:
    match = SOURCE_LINE_RE.match(line.strip())
    if not match:
        return None
    label = match.group("label")
    suffix = match.group("suffix").strip()
    status = "local_only"
    if suffix == "— [⚠️ fetch failed]":
        status = "fetch_failed"
    elif suffix == "— [⚠️ non-HTML resource]":
        status = "non_html"
    elif suffix == "— [⚠️ dead link]":
        status = "http_dead"
    return SourceRecord(
        label=label,
        path=match.group("path"),
        status=status,
        raw_content="",
        cleaned_text=retained_evidence,
        fetched_summary=None,
        detected_url=label if label.startswith(("http://", "https://")) else None,
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
        visible_label = source.detected_url or source.label
        lines.append(f"- [{visible_label}]({source_path}){suffix_map.get(source.status, '')}")
    return lines


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


def parse_page_file(path: Path, page_type: str | None = None) -> ParsedWikiPage:
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    lines = text.splitlines()
    slug = path.stem
    title = lines[0][2:].strip() if lines and lines[0].startswith("# ") else page_title(slug)
    sections: dict[str, list[str]] = {"summary": [], "notes": [], "connections": [], "sources": []}
    current = "summary"
    for line in lines[1:]:
        if line == "## Notes":
            current = "notes"
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
    inferred_shape = PAGE_SHAPE_TOPIC if (not note_lines and not sources and connection_slugs) else PAGE_SHAPE_ATOMIC

    return ParsedWikiPage(
        slug=slug,
        title=title,
        page_type=page_type or classify_page(slug, title, "title"),
        shape=inferred_shape,
        summary_lines=summary_lines,
        note_lines=note_lines,
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


def load_existing_wiki_pages() -> dict[str, ParsedWikiPage]:
    page_types = load_existing_page_types(WIKI_ROOT / "index.md")
    parsed_pages: dict[str, ParsedWikiPage] = {}
    if not WIKI_ROOT.exists():
        return parsed_pages
    for path in sorted(WIKI_ROOT.glob("*.md")):
        if path.stem in {"index", "log"}:
            continue
        parsed = parse_page_file(path, page_type=page_types.get(path.stem))
        parsed_pages[parsed.slug] = parsed
    return parsed_pages


def merge_page_content(target: Page, source_page: Page) -> None:
    if source_page.shape == PAGE_SHAPE_TOPIC:
        target.shape = PAGE_SHAPE_TOPIC
    for note in source_page.notes:
        if note not in target.notes:
            target.notes.append(note)
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
    left_tokens = set(left.split("-"))
    right_tokens = set(right.split("-"))
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    return overlap / max(1, min(len(left_tokens), len(right_tokens)))


def derive_source_atomic_slug(page: Page, source: SourceRecord) -> str:
    candidate = clean_component(source.label) or slugify(source.label)
    if not candidate:
        return page.slug
    if candidate == page.slug:
        return page.slug
    if candidate in page.connections:
        return candidate
    if page.slug in candidate or candidate in page.slug:
        return page.slug
    if slug_similarity(candidate, page.slug) >= 0.5:
        return page.slug
    return candidate


def infer_split_candidate_for_source(source: SourceRecord, candidate_slugs: list[str]) -> str | None:
    if not candidate_slugs:
        return None

    raw_candidates = [source.label]
    path_name = Path(source.path).name
    if path_name:
        raw_candidates.append(Path(path_name).stem)

    normalized_candidates: list[str] = []
    for raw in raw_candidates:
        normalized = clean_component(raw) or slugify(raw)
        if normalized:
            normalized_candidates.append(normalized)

    for normalized in normalized_candidates:
        if normalized in candidate_slugs:
            return normalized

    best_slug: str | None = None
    best_score = 0.0
    for normalized in normalized_candidates:
        for candidate_slug in candidate_slugs:
            score = slug_similarity(normalized, candidate_slug)
            if score > best_score:
                best_score = score
                best_slug = candidate_slug

    if best_slug is not None and best_score >= 0.6:
        return best_slug
    return None


def source_looks_like_lecture_material(source: SourceRecord, page_slug: str) -> bool:
    normalized_label = clean_component(source.label) or slugify(source.label)
    if normalized_label and (normalized_label == page_slug or normalized_label.startswith(f"{page_slug}-")):
        return True

    page_slug_variants = {
        page_slug,
        page_slug.replace("-", " "),
        page_slug.replace("-", ""),
    }
    combined = " ".join(part for part in [source.label, source.path, source.cleaned_text] if part).lower()
    if any(variant and variant in combined for variant in page_slug_variants):
        return True
    return bool(LECTURE_SOURCE_RE.search(combined))


def page_looks_like_course_notes(page: Page) -> bool:
    if not COURSE_PAGE_SLUG_RE.fullmatch(page.slug):
        return False
    lecture_like_sources = sum(1 for source in page.sources.values() if source_looks_like_lecture_material(source, page.slug))
    return lecture_like_sources >= max(2, len(page.sources) - 1)


def score_bucket_signals(page: Page) -> BucketSignalResult:
    reasons: list[str] = []
    lecture_like_page = page_looks_like_course_notes(page)

    if page.slug in GENERIC_BUCKET_SLUGS:
        reasons.append("generic-parent-slug")

    structured_note_groups = 0
    note_lines = (page.rendered_notes_markdown or "").splitlines()
    for line in note_lines:
        stripped = line.strip()
        if stripped.startswith("### ") or BOLD_HEADING_RE.match(stripped):
            if lecture_like_page and LECTURE_HEADING_RE.match(stripped):
                continue
            structured_note_groups += 1
    if structured_note_groups >= 2 and not lecture_like_page:
        reasons.append("multi-cluster-notes")

    if not lecture_like_page:
        source_slugs = {
            derived
            for source in page.sources.values()
            for derived in [derive_source_atomic_slug(page, source)]
            if derived != page.slug
        }
        if len(source_slugs) >= 3:
            reasons.append("heterogeneous-sources")

    if not lecture_like_page:
        child_connection_candidates = {
            connection_slug
            for connection_slug in sorted_connection_slugs(page)
            if connection_slug != page.slug and slug_similarity(connection_slug, page.slug) < 0.5
        }
        if len(child_connection_candidates) >= 2:
            reasons.append("existing-satellites")

    return BucketSignalResult(score=len(reasons), reasons=reasons)


def add_source_to_page(page: Page, source: SourceRecord, seed_kind: str = "migration") -> None:
    page.seed_kinds.add(seed_kind)
    page.sources[source.path] = source
    page.rendered_notes_markdown = None
    excerpt = compact_source_text(source, limit=220).strip()
    excerpt = re.sub(r"\s+", " ", excerpt)
    if excerpt and excerpt not in page.notes:
        page.notes.append(excerpt)


def connect_pages(pages: dict[str, Page], left: str, right: str) -> None:
    if left == right:
        return
    if left not in pages or right not in pages:
        return
    pages[left].connections[right] += 1
    pages[right].connections[left] += 1


def gather_split_source_groups(page: Page, split_decision: PageSplitDecision) -> tuple[dict[str, list[SourceRecord]], set[str]]:
    source_groups: dict[str, list[SourceRecord]] = defaultdict(list)
    assigned_source_paths: set[str] = set()
    if len(page.sources) == 1:
        grounded_child_slugs = grounded_split_candidate_slugs(split_decision)
        if len(grounded_child_slugs) >= 2:
            source = next(iter(page.sources.values()))
            for child_slug in grounded_child_slugs:
                source_groups[child_slug].append(source)
            assigned_source_paths.add(source.path)
            return source_groups, assigned_source_paths

    for source_path, assigned_slug in split_decision.source_assignments.items():
        source = page.sources.get(source_path)
        if source is not None:
            source_groups[assigned_slug].append(source)
            assigned_source_paths.add(source_path)

    for source_path, source in sorted(page.sources.items()):
        if source_path in assigned_source_paths:
            continue
        inferred_slug = infer_split_candidate_for_source(source, split_decision.candidate_satellite_slugs)
        if inferred_slug is None:
            continue
        source_groups[inferred_slug].append(source)
        assigned_source_paths.add(source_path)
    return source_groups, assigned_source_paths


def split_candidate_evaluation_map(split_decision: PageSplitDecision) -> dict[str, SplitCandidateEvaluation]:
    return {evaluation.slug: evaluation for evaluation in split_decision.candidate_evaluations if evaluation.accepted}


def grounded_split_candidate_slugs(split_decision: PageSplitDecision) -> list[str]:
    evaluation_map = split_candidate_evaluation_map(split_decision)
    grounded_slugs: list[str] = []
    for child_slug in ordered_unique(split_decision.candidate_satellite_slugs):
        evaluation = evaluation_map.get(child_slug)
        if evaluation is None or not evaluation.grounding:
            continue
        grounded_slugs.append(child_slug)
    return grounded_slugs


def build_split_child_notes(split_decision: PageSplitDecision, child_slug: str) -> list[str]:
    evaluation = split_candidate_evaluation_map(split_decision).get(child_slug)
    if evaluation is None:
        return []

    notes: list[str] = []
    for grounding in evaluation.grounding:
        note = grounding.strip()
        if note and note not in notes:
            notes.append(note)
    if evaluation.why_distinct and evaluation.why_distinct not in notes:
        notes.append(evaluation.why_distinct)
    return notes


def page_title_is_source_shaped(page: Page) -> bool:
    normalized_title = page.title.strip().lower().replace(" ", "-")
    normalized_slug = page.slug.strip().lower()
    if normalized_slug in GENERIC_BUCKET_SLUGS:
        return True
    if looks_like_archive(page.title):
        return True
    if re.fullmatch(r"(?:lecture|lec|week|chapter|notes?|misc|overview|summary)(?:-\d+)?", normalized_title):
        return True
    if re.fullmatch(r"(?:lecture|lec|week|chapter|notes?|misc|overview|summary)(?:-\d+)?", normalized_slug):
        return True
    return False


def resolve_parent_split_mode(page: Page, child_slugs: list[str]) -> str:
    if any(child_slug == page.slug or slug_similarity(child_slug, page.slug) >= 0.8 for child_slug in child_slugs):
        return PAGE_SHAPE_ATOMIC
    if not page_title_is_source_shaped(page) and len(child_slugs) >= 2:
        return PAGE_SHAPE_TOPIC
    if page_title_is_source_shaped(page):
        return "deprecated"
    return PAGE_SHAPE_ATOMIC


def apply_split_decision(
    pages: dict[str, Page],
    parent_slug: str,
    split_decision: PageSplitDecision,
    *,
    seed_kind: str = "migration",
    allow_partial_source_coverage: bool = False,
) -> bool:
    parent_page = pages.get(parent_slug)
    child_slugs = ordered_unique(split_decision.candidate_satellite_slugs)
    if parent_page is None or split_decision.is_atomic or len(child_slugs) < 2:
        return False

    source_groups, assigned_source_paths = gather_split_source_groups(parent_page, split_decision)
    if parent_page.sources and not allow_partial_source_coverage:
        if len(parent_page.sources) > 1 and len(assigned_source_paths) != len(parent_page.sources):
            return False
        if len(parent_page.sources) > 1 and len(source_groups) < 2:
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

        for note in build_split_child_notes(split_decision, child_slug):
            if note not in child_page.notes:
                child_page.notes.append(note)
        for source in source_groups.get(child_slug, []):
            add_source_to_page(child_page, source, seed_kind)
        connect_pages(pages, parent_slug, child_slug)

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
            page.sources = {}
        else:
            page.shape = PAGE_SHAPE_ATOMIC
            page.notes = ordered_unique(page.notes)
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
        page.sources[source_path] = SourceRecord(
            label=source_label,
            path=source_path,
            status=source_status,
            raw_content="",
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


def render_page(page: Page) -> str:
    lines = [f"# {page.title}", ""]

    connection_lines = render_connection_lines(page)
    if page_shape(page) == PAGE_SHAPE_TOPIC:
        if connection_lines:
            lines.extend(["## Connections", "", "\n".join(connection_lines), ""])
        return "\n".join(lines).rstrip() + "\n"

    note_lines = build_simple_notes_markdown(page)
    if note_lines.strip():
        lines.extend(["## Notes", "", note_lines, ""])
    if connection_lines:
        lines.extend(["## Connections", "", "\n".join(connection_lines), ""])
    source_lines = render_source_lines(page)
    if source_lines:
        lines.extend(["## Sources", "", "\n".join(source_lines), ""])
    return "\n".join(lines).rstrip() + "\n"


def render_index(pages: dict[str, Page]) -> str:
    grouped: defaultdict[str, list[Page]] = defaultdict(list)
    for page in pages.values():
        if page.slug in {"index", "log"}:
            continue
        grouped[page.page_type].append(page)

    lines = [
        "# Wiki Index",
        "",
        f"_Last updated: {TODAY} — {len(pages)} pages_",
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


def parse_requested_slugs(raw_value: str | None) -> set[str]:
    if not raw_value:
        return set()
    slugs: set[str] = set()
    for raw_slug in raw_value.split(","):
        cleaned = clean_component(raw_slug.strip()) or slugify(raw_slug.strip())
        if cleaned:
            slugs.add(cleaned)
    return slugs


def manifest_failed_split_slugs() -> set[str]:
    manifest = read_json_file(CACHE_MANIFEST_PATH)
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


def run_split_only(
    *,
    api_key: str | None,
    model: str,
    target_slugs: set[str] | None,
    retry_failed_splits: bool,
) -> None:
    existing_pages = load_existing_wiki_pages()
    if not existing_pages:
        raise RuntimeError("Split-only mode requires an existing wiki to load.")

    selected_slugs = set(target_slugs or set())
    if retry_failed_splits:
        selected_slugs.update(manifest_failed_split_slugs())
    if not selected_slugs:
        raise RuntimeError("Split-only mode requires --pages or --retry-failed-splits.")

    pages = {slug: parsed_page_to_page(parsed) for slug, parsed in existing_pages.items()}
    update_manifest(
        phase="split_only",
        model=model,
        failure=None,
        split_targets=sorted(selected_slugs),
        split_phase={"status": "starting", "target_slugs": sorted(selected_slugs)},
    )

    split_report = migrate_pages_to_atomic_topics(
        pages,
        existing_pages,
        api_key=api_key,
        model=model,
        target_slugs=selected_slugs,
    )
    split_manifest = split_report_manifest_payload(split_report)
    split_manifest["target_slugs"] = sorted(selected_slugs)
    update_manifest(phase="split_only", split_phase=split_manifest, failure=split_report.reason)

    prune_generic_media_links(pages)
    ensure_meaningful_connections(pages)
    finalize_page_shapes(pages)

    existing_log = (WIKI_ROOT / "log.md").read_text(encoding="utf-8") if (WIKI_ROOT / "log.md").exists() else "# Wiki Log\n\n"
    bootstrap_entry = (
        f'## [{TODAY}] bootstrap | split-only — {len(selected_slugs)} requested pages; '
        f'{split_report_summary(split_report)}; targets: {", ".join(sorted(selected_slugs))}\n'
    )
    stage_dir = stage_rendered_wiki(
        pages=pages,
        existing_log_text=existing_log,
        bootstrap_entry=bootstrap_entry,
    )
    swap_rendered_wiki(stage_dir)
    update_manifest(phase="completed", processed_pages=len(pages), split_phase=split_manifest, failure=None)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fetch-urls", action="store_true", help="Fetch remote title/meta for URL notes.")
    parser.add_argument("--split-only", action="store_true", help="Retry page splitting using the existing wiki only.")
    parser.add_argument("--pages", help="Comma-separated page slugs to target during split-only mode.")
    parser.add_argument(
        "--retry-failed-splits",
        action="store_true",
        help="In split-only mode, retry slugs recorded as failed in the manifest.",
    )
    args = parser.parse_args()

    WIKI_ROOT.mkdir(exist_ok=True)
    CACHE_NOTES_ROOT.mkdir(parents=True, exist_ok=True)
    CACHE_PAGES_ROOT.mkdir(parents=True, exist_ok=True)
    api_key = os.environ.get("GEMINI_API_KEY")
    model = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")
    target_slugs = parse_requested_slugs(args.pages)

    if args.split_only:
        run_split_only(
            api_key=api_key,
            model=model,
            target_slugs=target_slugs,
            retry_failed_splits=args.retry_failed_splits,
        )
        return

    pages: dict[str, Page] = {}
    existing_pages = load_existing_wiki_pages()
    markdown_files = sorted(path for path in RAW_ROOT.rglob("*.md") if path.is_file())
    media_files = sorted(
        path
        for path in RAW_ROOT.rglob("*")
        if path.is_file() and path.suffix.lower() in {".png", ".jpg", ".jpeg", ".heic", ".pdf"}
    )

    embedded_media = set()
    url_stats: Counter = Counter()
    update_manifest(
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
        title = derive_note_title(path, content).strip() or path.stem
        embedded_media.update(note_embed_names(content))
        source_path = "../" + path.relative_to(ROOT).as_posix()
        note_fingerprint = build_note_fingerprint(path, content, args.fetch_urls)
        cache_path = note_cache_path(source_path)
        cached_entry = read_json_file(cache_path)
        cache_hit = isinstance(cached_entry, dict) and cached_entry.get("fingerprint") == note_fingerprint
        if cache_hit:
            try:
                apply_note_cache_entry_to_pages(pages=pages, cache_entry=cached_entry)
                source_payload = cached_entry.get("source_record")
                if isinstance(source_payload, dict):
                    accumulate_url_stats(url_stats, source_record_from_cache_dict(source_payload), args.fetch_urls)
            except Exception:
                cache_hit = False

        if not cache_hit:
            path_topics = derive_path_topics(path)
            url = extract_first_url(content)
            fold_into_parent = should_fold_note_into_parent(title, content, url)

            source_label = title
            source_status = "local_only"
            fetched_summary = None
            if args.fetch_urls:
                if url:
                    fetch_result = fetch_url_summary(url)
                    source_status = fetch_result.status
                    fetched_summary = fetch_result.summary
            else:
                if url:
                    source_status = "fetch_skipped"
            if args.fetch_urls and not url and note_has_url(content):
                source_status = "fetch_failed"

            source_record = prepare_source_record(
                source_label=source_label,
                source_path=source_path,
                source_status=source_status,
                raw_content=content,
                fetched_summary=fetched_summary,
                detected_url=url,
            )
            accumulate_url_stats(url_stats, source_record, args.fetch_urls)

            extracted_topics: list[str] = []
            if api_key:
                try:
                    extracted_topics = extract_note_topics(
                        title=title,
                        source_record=source_record,
                        api_key=api_key,
                        model=model,
                    )
                except Exception as error:
                    update_manifest(
                        failure=f"topic extraction fallback for {source_path}: {error}",
                        last_note=source_path,
                        processed_notes=index - 1,
                    )
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

            note_text = first_meaningful_snippet(content, title)
            if fetched_summary:
                note_text = fetched_summary

            cache_entry = build_note_cache_entry(
                fingerprint=note_fingerprint,
                title=title,
                source_record=source_record,
                note_text=note_text,
                page_assignments=page_slugs,
                skipped=skipped,
            )
            atomic_write_json(cache_path, cache_entry)
            apply_note_cache_entry_to_pages(pages=pages, cache_entry=cache_entry)

        update_manifest(processed_notes=index, last_note=source_path, failure=None)
        if index % 50 == 0 or index == len(markdown_files):
            print(f"Processed notes: {index}/{len(markdown_files)}", file=sys.stderr)

    media_page_slug = "unclassified-media-captures"
    ensure_supporting_pages(pages, media_page_slug, "Unclassified media captures", "folder")
    update_manifest(phase="media")

    for media_path in media_files:
        if media_path.name in embedded_media:
            continue
        source_path = "../" + media_path.relative_to(ROOT).as_posix()
        note_text = f"Standalone raw media capture: {media_path.name}."
        add_page_note(
            pages=pages,
            slug=media_page_slug,
            title=page_title(media_page_slug),
            page_type="Concepts",
            summary_hint="Unclassified media captures",
            note_text=note_text,
            source_label=media_path.name,
            source_path=source_path,
            source_status="local_only",
            seed_kind="folder",
        )
        pages[media_page_slug].sources[source_path] = prepare_source_record(
            source_label=media_path.name,
            source_path=source_path,
            source_status="local_only",
            raw_content=note_text,
            fetched_summary=None,
            detected_url=None,
        )

    merge_existing_pages(pages, existing_pages)
    update_manifest(phase="split", split_phase={"status": "starting"}, failure=None)
    split_report = migrate_pages_to_atomic_topics(pages, existing_pages, api_key=api_key, model=model)
    split_manifest = split_report_manifest_payload(split_report)
    update_manifest(phase="split", split_phase=split_manifest, failure=split_report.reason)
    prune_generic_media_links(pages)
    ensure_meaningful_connections(pages)
    finalize_page_shapes(pages)
    update_manifest(phase="synthesis", total_pages=len(pages), processed_pages=len(pages), failure=None)

    update_manifest(phase="render")
    existing_log = (WIKI_ROOT / "log.md").read_text(encoding="utf-8") if (WIKI_ROOT / "log.md").exists() else "# Wiki Log\n\n"
    bootstrap_entry = (
        f'## [{TODAY}] bootstrap | completed — {len(markdown_files) + len(media_files)} raw files, '
        f'{len(pages)} wiki pages migrated'
    )
    bootstrap_entry += f"; {split_report_summary(split_report)}"
    if args.fetch_urls:
        bootstrap_entry += (
            f'; URL metadata: {url_stats["url_notes"]} URL notes, '
            f'{url_stats["fetched"]} fetched, '
            f'{url_stats["fetch_failed"]} failed, '
            f'{url_stats["http_dead"]} dead, '
            f'{url_stats["non_html"]} non-HTML'
        )
    bootstrap_entry += "\n"
    stage_dir = stage_rendered_wiki(
        pages=pages,
        existing_log_text=existing_log,
        bootstrap_entry=bootstrap_entry,
    )
    swap_rendered_wiki(stage_dir)
    update_manifest(
        phase="completed",
        processed_notes=len(markdown_files),
        processed_pages=len(pages),
        failure=None,
    )


if __name__ == "__main__":
    main()
