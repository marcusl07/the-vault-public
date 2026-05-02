from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date
import re

try:
    from scripts.source_model import SourceArtifact, SourceCitation, SourceEvidence
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    from source_model import SourceArtifact, SourceCitation, SourceEvidence


PAGE_SHAPE_ATOMIC = "atomic"
PAGE_SHAPE_TOPIC = "topic"
INDEX_SECTION_ORDER = ("Concepts", "Entities", "Experiences", "Aspirations")
CATALOG_PATH = "catalog.md"
HIGH_SIGNAL_INBOUND_THRESHOLD = 3
CONNECTION_SLUG_RE = re.compile(r"\[\[(?P<slug>[^\]]+)\]\]")
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
BOILERPLATE_PATTERNS = (
    re.compile(r"^This page collects Marcus's notes about .* across \d+ source(?:s)?\.$"),
    re.compile(r"^- No notes yet\.$"),
    re.compile(r"^- No sources linked yet\.$"),
)
TODAY = date.today().isoformat()

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


SourceRecord = SourceEvidence


@dataclass
class Page:
    slug: str
    title: str
    page_type: str
    summary_hint: str
    shape: str = PAGE_SHAPE_ATOMIC
    notes: list[str] = field(default_factory=list)
    connections: Counter = field(default_factory=Counter)
    sources: dict[str, SourceEvidence] = field(default_factory=dict)
    seed_kinds: set[str] = field(default_factory=set)
    rendered_summary: str | None = None
    rendered_notes_markdown: str | None = None
    open_questions: list[str] = field(default_factory=list)
    topic_parent: str | None = None


@dataclass
class ParsedWikiPage:
    slug: str
    title: str
    page_type: str
    shape: str
    summary_lines: list[str] = field(default_factory=list)
    note_lines: list[str] = field(default_factory=list)
    open_question_lines: list[str] = field(default_factory=list)
    connection_slugs: list[str] = field(default_factory=list)
    sources: dict[str, SourceEvidence] = field(default_factory=dict)


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


def compact_source_text(source: SourceEvidence, limit: int = 800) -> str:
    body_parts = []
    if source.fetched_summary:
        body_parts.append(f"Fetched summary: {source.fetched_summary}")
    if source.cleaned_text:
        body_parts.append(source.cleaned_text)
    combined = "\n\n".join(body_parts).strip()
    if len(combined) <= limit:
        return combined
    return combined[: limit - 3].rstrip() + "..."


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
