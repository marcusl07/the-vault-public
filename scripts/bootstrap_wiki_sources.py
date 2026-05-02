from __future__ import annotations

from pathlib import Path
from types import ModuleType
from urllib.parse import parse_qs, urlparse
import html
import os
import re
import unicodedata


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


def derive_note_title(api: ModuleType, path: Path, content: str) -> str:
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        heading = re.match(r"^#\s+(.+)$", stripped)
        if heading:
            return api.strip_markdown(heading.group(1)).strip() or path.stem
        link = re.match(r"^\[(.*?)\]\((https?://[^)]+)\)", stripped)
        if link:
            text = api.strip_markdown(link.group(1)).strip().strip("*")
            return text or path.stem
        anchor = re.search(r"<a [^>]*>(.*?)</a>", stripped, flags=re.I)
        if anchor:
            text = api.strip_markdown(anchor.group(1)).strip()
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
    return ("youtube.com" in host and path == "/watch") or host.endswith("youtu.be")


def note_embed_names(content: str) -> list[str]:
    return re.findall(r"!\[\[([^\]]+)\]\]", content)


def first_meaningful_snippet(api: ModuleType, content: str, title: str) -> str:
    lines = []
    for raw_line in content.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            continue
        lines.append(api.strip_markdown(stripped))
    lines = [line for line in lines if line]
    if not lines:
        return f"Saved note: {title}."
    combined = " ".join(lines[:2]).strip()
    combined = re.sub(r"\s+", " ", combined)
    if len(combined) > 180:
        combined = combined[:177].rstrip() + "..."
    return combined


def clean_source_text(api: ModuleType, content: str, title: str) -> str:
    title_text = api.strip_markdown(title).strip().lower()
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
            heading_text = api.strip_markdown(heading_match.group(2)).strip()
            if heading_text and heading_text.lower() != title_text:
                cleaned_lines.append(f"{heading_match.group(1)} {heading_text}")
            continue
        bullet_match = re.match(r"^(\s*)[-*+]\s+(.*)$", line)
        if bullet_match:
            indent = "  " * (len(bullet_match.group(1).expandtabs(2)) // 2)
            bullet_text = api.strip_markdown(bullet_match.group(2)).strip()
            if bullet_text:
                cleaned_lines.append(f"{indent}- {bullet_text}")
            continue
        ordered_match = re.match(r"^(\s*)(\d+)\.\s+(.*)$", line)
        if ordered_match:
            indent = "  " * (len(ordered_match.group(1).expandtabs(2)) // 2)
            item_text = api.strip_markdown(ordered_match.group(3)).strip()
            if item_text:
                cleaned_lines.append(f"{indent}{ordered_match.group(2)}. {item_text}")
            continue
        cleaned = api.strip_markdown(stripped)
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


def detect_source_tags(api: ModuleType, title: str, cleaned_text: str, url: str | None, fetched_summary: str | None) -> set[str]:
    combined = " ".join(part for part in [title, cleaned_text, fetched_summary or "", url or ""] if part)
    lowered = combined.lower()
    tags: set[str] = set()
    if re.search(r"\b(birthday|anniversary|date idea|date night|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|20\d{2}|\d{1,2}/\d{1,2})\b", lowered):
        tags.add("dates")
    if re.search(r"\b(address|unit\b|apt\b|suite\b|drive\b|street\b|avenue\b|boulevard\b|\d{5}(?:-\d{4})?)", lowered):
        tags.update({"address", "places"})
    if re.search(r"\b(gift|present|christmas idea|bouquet|flowers|perfume|plush|concert tickets?|cake)\b", lowered):
        tags.add("gift_ideas")
    if re.search(r"\b(activities?|trip|travel|visit|airport|flight|kayaking|archery|rock climbing|board game|festival|paint by numbers|ceramics|theme park|animal cafe)\b", lowered):
        tags.update({"activities", "travel_plans"})
    if re.search(r"\b(place|restaurant|cafe|bakery|mall|market|japantown|santa clara|sf|san fran|daly city|irvine|paris baguette|chipotle|valley fair|great america)\b", lowered):
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
    elif len(api.strip_markdown(cleaned_text).split()) <= 4 and not tags.intersection(
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
    api: ModuleType,
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
) -> object:
    cleaned_text = clean_source_text(api, raw_content, source_label)
    tags = detect_source_tags(api, source_label, cleaned_text, detected_url, fetched_summary)
    return api.SourceEvidence(
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


def derive_path_topics(api: ModuleType, path: Path) -> list[str]:
    topics: list[str] = []
    path_abs = Path(os.path.abspath(path))
    apple_root_abs = Path(os.path.abspath(api.APPLE_NOTES_ROOT))
    raw_root_abs = Path(os.path.abspath(api.RAW_ROOT))
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
