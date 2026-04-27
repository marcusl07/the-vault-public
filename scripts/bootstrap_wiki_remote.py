from __future__ import annotations

import html
import json
import os
import re
import socket
import time
from typing import TYPE_CHECKING
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

if TYPE_CHECKING:
    from types import ModuleType


def source_priority(api: ModuleType, source: object) -> tuple[int, int, str]:
    informative_tags = {"dates", "address", "gift_ideas", "activities", "places", "food", "travel_plans", "preferences"}
    score = 0
    score += 5 * len(source.tags & informative_tags)
    if source.fetched_summary:
        score += 2
    if "low_signal" in source.tags:
        score -= 2
    if "uncategorized" in source.tags:
        score -= 1
    content_length = len(api.compact_source_text(source, limit=400))
    return (-score, content_length, source.label.lower())


def select_sources_for_synthesis(api: ModuleType, page: object, max_sources: int = 40, max_chars: int = 18_000) -> list[object]:
    selected: list[object] = []
    total_chars = 0
    body_sources = sorted(
        (source for source in page.sources.values() if not source.excluded_from_body),
        key=lambda source: source_priority(api, source),
    )
    for source in body_sources:
        excerpt = api.compact_source_text(source)
        estimated_chars = len(excerpt) + 200
        if selected and (len(selected) >= max_sources or total_chars + estimated_chars > max_chars):
            continue
        selected.append(source)
        total_chars += estimated_chars
        if len(selected) >= max_sources or total_chars >= max_chars:
            break
    return selected


def serialize_sources_for_prompt(api: ModuleType, page: object) -> str:
    prompt_chunks: list[str] = []
    body_sources = select_sources_for_synthesis(api, page)
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
        excerpt = api.compact_source_text(source)
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


def build_synthesis_messages(api: ModuleType, page: object) -> list[dict[str, str]]:
    section_hint = (
        "For entity pages, prefer sections like `### Key Dates`, `### Gift Ideas`, `### Places`, `### Preferences`, and `### Notes`, but only include sections supported by the sources."
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
            serialize_sources_for_prompt(api, page),
        ]
    )
    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]


def parse_synthesis_response(api: ModuleType, content: str) -> tuple[str, str]:
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


def gemini_generate(api: ModuleType, *, messages: list[dict[str, str]], api_key: str, model: str, response_schema: dict[str, object], attempts: int = 4, timeout: float = 90) -> str:
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
        headers={"Content-Type": "application/json"},
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


def synthesize_page(api: ModuleType, page: object, api_key: str, model: str) -> tuple[str, str]:
    body_sources = select_sources_for_synthesis(api, page)
    if not body_sources:
        return (
            f"This page collects Marcus's notes about {page.title} across {len(page.sources)} source{'' if len(page.sources) == 1 else 's'}.",
            api.build_simple_notes_markdown(page),
        )

    try:
        content = api.gemini_generate(
            messages=api.build_synthesis_messages(page),
            api_key=api_key,
            model=model,
            response_schema={
                "type": "OBJECT",
                "required": ["summary", "notes_markdown"],
                "properties": {
                    "summary": {"type": "STRING"},
                    "notes_markdown": {"type": "STRING"},
                },
            },
        )
    except (HTTPError, URLError, TimeoutError, socket.timeout):
        return (
            f"This page collects Marcus's notes about {page.title} across {len(page.sources)} source{'' if len(page.sources) == 1 else 's'}.",
            api.build_simple_notes_markdown(page),
        )
    try:
        return api.parse_synthesis_response(content)
    except (json.JSONDecodeError, ValueError):
        return (
            f"This page collects Marcus's notes about {page.title} across {len(page.sources)} source{'' if len(page.sources) == 1 else 's'}.",
            api.build_simple_notes_markdown(page),
        )


def build_topic_extraction_messages(api: ModuleType, *, title: str, cleaned_text: str, fetched_summary: str | None, detected_url: str | None) -> list[dict[str, str]]:
    excerpt = cleaned_text.strip()
    if len(excerpt) > api.TOPIC_EXTRACTION_MAX_BODY_CHARS:
        excerpt = excerpt[: api.TOPIC_EXTRACTION_MAX_BODY_CHARS - 3].rstrip() + "..."
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
        {"role": "user", "content": "\n\n".join(parts)},
    ]


def parse_topic_extraction_response(api: ModuleType, content: str) -> list[tuple[str, str]]:
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
        slug = api.clean_component(raw_slug) or api.slugify(raw_slug)
        if not slug or confidence not in {"high", "medium", "low"} or slug in seen:
            continue
        seen.add(slug)
        parsed.append((slug, confidence))
    return parsed[:3]


def extract_note_topics(api: ModuleType, *, title: str, source_record: object, api_key: str, model: str) -> list[str]:
    content = api.gemini_generate(
        messages=api.build_topic_extraction_messages(
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
                            "confidence": {"type": "STRING", "enum": ["high", "medium", "low"]},
                        },
                    },
                }
            },
        },
    )
    extracted = api.parse_topic_extraction_response(content)
    return [slug for slug, confidence in extracted if confidence in api.TOPIC_EXTRACTION_CONFIDENT_LEVELS]


def build_page_split_messages(api: ModuleType, page: object) -> list[dict[str, str]]:
    note_content = page.rendered_notes_markdown or api.build_simple_notes_markdown(page)
    note_content = note_content.strip() or "[empty]"
    if len(note_content) > api.PAGE_SPLIT_MAX_BODY_CHARS:
        note_content = note_content[: api.PAGE_SPLIT_MAX_BODY_CHARS - 3].rstrip() + "..."

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
                    "- Do not require multiple source files. One raw source may support multiple child pages only when it substantively grounds each child with child-specific facts.",
                    "- Return is_atomic=true when this still feels like one note a real Zettelkasten user would keep together.",
                    "- Do not split just because one lecture note contains several related subtopics.",
                    "- Return is_atomic=false when the page is clearly a bucket containing multiple standalone notes.",
                    "- Do not return the parent slug.",
                    "- Do not return generic buckets like notes, ideas, resources, archive, misc, or overview.",
                    "- Prefer stable concept/entity/experience slugs that could stand as atomic pages.",
                    "- If a candidate child fails the reusable-idea test, explain the rejection reason instead of including it as an accepted child.",
                    "- Each accepted child's grounding must be usable as that child page's ## Notes: child-specific facts, not a copied source title, list item, parent summary, or generic excerpt.",
                    "- Same-source sibling children are allowed only when that same source substantively grounds each child with distinct child-specific notes.",
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


def parse_page_split_response(api: ModuleType, content: str, parent_slug: str, source_paths: set[str]) -> object:
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
        slug = api.clean_component(str(raw_slug).strip()) or api.slugify(str(raw_slug).strip())
        if not slug or slug == parent_slug or slug in seen_candidates:
            continue
        seen_candidates.add(slug)
        candidate_slugs.append(slug)

    rejection_reasons = [str(item).strip() for item in raw_rejection_reasons if str(item).strip()]
    candidate_evaluations: list[object] = []
    seen_evaluation_slugs: set[str] = set()
    for item in raw_candidate_evaluations:
        if not isinstance(item, dict):
            continue
        slug = api.clean_component(str(item.get("slug", "")).strip()) or api.slugify(str(item.get("slug", "")).strip())
        if not slug or slug == parent_slug or slug in seen_evaluation_slugs:
            continue
        seen_evaluation_slugs.add(slug)
        grounding = [str(entry).strip() for entry in item.get("grounding", []) if str(entry).strip()]
        why_distinct_value = item.get("why_distinct")
        why_distinct = str(why_distinct_value).strip() if isinstance(why_distinct_value, str) and str(why_distinct_value).strip() else None
        candidate_evaluations.append(
            api.SplitCandidateEvaluation(
                slug=slug,
                accepted=bool(item.get("accepted", False)),
                grounding=grounding,
                why_distinct=why_distinct,
                passes_direct_link_test=bool(item.get("passes_direct_link_test", False)),
                passes_stable_page_test=bool(item.get("passes_stable_page_test", False)),
                passes_search_test=bool(item.get("passes_search_test", False)),
                rejection_reasons=[str(reason).strip() for reason in item.get("rejection_reasons", []) if str(reason).strip()],
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
        satellite_slug = api.clean_component(str(item.get("satellite_slug", "")).strip()) or api.slugify(str(item.get("satellite_slug", "")).strip())
        if (
            not source_path
            or source_path not in source_paths
            or not satellite_slug
            or satellite_slug == parent_slug
            or satellite_slug not in seen_candidates
        ):
            continue
        source_assignments[source_path] = satellite_slug

    def slugs_overlap_too_much(left: str, right: str) -> bool:
        left_base = left.rstrip("s")
        right_base = right.rstrip("s")
        if left == right:
            return True
        if left in right or right in left:
            return True
        if left_base and right_base and (left_base in right_base or right_base in left_base):
            return True
        if api.slug_similarity(left, right) >= 0.8:
            return True
        return False

    def collapse_overlapping_candidates(slugs: list[str]) -> list[str]:
        merged: list[str] = []
        for slug in slugs:
            if any(slugs_overlap_too_much(slug, existing) for existing in merged):
                continue
            merged.append(slug)
        return merged

    candidate_slugs = collapse_overlapping_candidates(candidate_slugs)

    if is_atomic:
        return api.PageSplitDecision(is_atomic=True, rationale=rationale, rejection_reasons=rejection_reasons, candidate_evaluations=candidate_evaluations)
    if len(candidate_slugs) < 2:
        return api.PageSplitDecision(
            is_atomic=True,
            rationale=rationale,
            rejection_reasons=[*rejection_reasons, "overlapping child set collapsed below split threshold"],
            candidate_evaluations=candidate_evaluations,
        )
    return api.PageSplitDecision(
        is_atomic=False,
        candidate_satellite_slugs=candidate_slugs,
        source_assignments=source_assignments,
        rationale=rationale,
        rejection_reasons=rejection_reasons,
        candidate_evaluations=candidate_evaluations,
    )


def split_failure_mode(api: ModuleType) -> str:
    mode = os.environ.get("BOOTSTRAP_SPLIT_FAILURE_MODE", "no-split").strip().lower()
    return "fail" if mode == "fail" else "no-split"


def split_request_timeout(api: ModuleType) -> float:
    raw_value = os.environ.get("BOOTSTRAP_SPLIT_TIMEOUT_SECONDS", "30").strip()
    try:
        return max(5.0, float(raw_value))
    except ValueError:
        return 30.0


def split_request_attempts(api: ModuleType) -> int:
    raw_value = os.environ.get("BOOTSTRAP_SPLIT_ATTEMPTS", "2").strip()
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 2


def split_debug_enabled(api: ModuleType) -> bool:
    raw_value = os.environ.get("BOOTSTRAP_SPLIT_DEBUG", "").strip().lower()
    return raw_value in {"1", "true", "yes", "on"}


def format_split_decision_debug(api: ModuleType, page: object, split_decision: object) -> str:
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
        tests = ", ".join([
            f"direct_link={'yes' if evaluation.passes_direct_link_test else 'no'}",
            f"stable_page={'yes' if evaluation.passes_stable_page_test else 'no'}",
            f"search={'yes' if evaluation.passes_search_test else 'no'}",
        ])
        lines.append(f"    - {evaluation.slug}: {status}; {tests}")
        if evaluation.why_distinct:
            lines.append(f"      why_distinct: {evaluation.why_distinct}")
        if evaluation.grounding:
            lines.append(f"      grounding: {' | '.join(evaluation.grounding)}")
        if evaluation.rejection_reasons:
            lines.append(f"      rejection_reasons: {', '.join(evaluation.rejection_reasons)}")
    return "\n".join(lines)


def split_counts_toward_transport_abort(api: ModuleType, error: Exception) -> bool:
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


def split_preflight_check(api: ModuleType, api_key: str | None, model: str, timeout: float = 5.0) -> tuple[bool, str | None]:
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


def analyze_page_for_atomic_split(api: ModuleType, page: object, api_key: str | None, model: str) -> object:
    if not api_key:
        return api.PageSplitDecision(is_atomic=True)

    content = api.gemini_generate(
        messages=api.build_page_split_messages(page),
        api_key=api_key,
        model=model,
        response_schema={
            "type": "OBJECT",
            "required": ["is_atomic", "rationale", "rejection_reasons", "candidate_satellite_slugs", "candidate_evaluations", "source_assignments"],
            "properties": {
                "is_atomic": {"type": "BOOLEAN"},
                "rationale": {"type": "STRING"},
                "rejection_reasons": {"type": "ARRAY", "items": {"type": "STRING"}},
                "candidate_satellite_slugs": {"type": "ARRAY", "items": {"type": "STRING"}},
                "candidate_evaluations": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "required": ["slug", "accepted", "grounding", "why_distinct", "passes_direct_link_test", "passes_stable_page_test", "passes_search_test", "rejection_reasons"],
                        "properties": {
                            "slug": {"type": "STRING"},
                            "accepted": {"type": "BOOLEAN"},
                            "grounding": {"type": "ARRAY", "items": {"type": "STRING"}},
                            "why_distinct": {"type": "STRING"},
                            "passes_direct_link_test": {"type": "BOOLEAN"},
                            "passes_stable_page_test": {"type": "BOOLEAN"},
                            "passes_search_test": {"type": "BOOLEAN"},
                            "rejection_reasons": {"type": "ARRAY", "items": {"type": "STRING"}},
                        },
                    },
                },
                "source_assignments": {
                    "type": "ARRAY",
                    "items": {
                        "type": "OBJECT",
                        "required": ["source_path", "satellite_slug"],
                        "properties": {"source_path": {"type": "STRING"}, "satellite_slug": {"type": "STRING"}},
                    },
                },
            },
        },
        attempts=api.split_request_attempts(),
        timeout=api.split_request_timeout(),
    )
    return api.parse_page_split_response(content, page.slug, set(page.sources))


def summarize_remote_page(api: ModuleType, html_text: str) -> str | None:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.I | re.S)
    meta_match = re.search(r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']', html_text, flags=re.I | re.S)
    if not meta_match:
        meta_match = re.search(r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']description["\']', html_text, flags=re.I | re.S)

    parts = []
    if title_match:
        title = api.strip_markdown(html.unescape(title_match.group(1))).strip()
        if title:
            parts.append(title)
    if meta_match:
        description = api.strip_markdown(html.unescape(meta_match.group(1))).strip()
        if description:
            if len(description) > 180:
                description = description[:177].rstrip() + "..."
            parts.append(description)
    if not parts:
        h1_match = re.search(r"<h1[^>]*>(.*?)</h1>", html_text, flags=re.I | re.S)
        if h1_match:
            h1_title = api.strip_markdown(html.unescape(h1_match.group(1))).strip()
            if h1_title:
                parts.append(h1_title)
    if not parts:
        return None
    return " — ".join(parts[:2])


def fetch_youtube_oembed_summary(api: ModuleType, url: str, timeout: float = 8.0) -> object:
    oembed_url = f"https://www.youtube.com/oembed?url={quote(url, safe='')}&format=json"
    try:
        request = Request(
            oembed_url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CodexWikiBootstrap/1.0)", "Accept": "application/json"},
        )
        with urlopen(request, timeout=timeout) as response:
            raw = response.read(50_000)
        payload = json.loads(raw.decode("utf-8", errors="ignore"))
        title = api.strip_markdown(str(payload.get("title", ""))).strip()
        author_name = api.strip_markdown(str(payload.get("author_name", ""))).strip()
        if title and author_name:
            return api.FetchResult(f"{title} — {author_name}", "fetched")
        if title:
            return api.FetchResult(title, "fetched")
        return api.FetchResult(None, "fetch_failed")
    except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError):
        return api.FetchResult(None, "fetch_failed")


def fetch_url_summary(api: ModuleType, url: str, timeout: float = 8.0) -> object:
    if api.is_youtube_url(url):
        return api.fetch_youtube_oembed_summary(url, timeout=timeout)
    try:
        request = Request(
            url,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CodexWikiBootstrap/1.0)", "Accept": "text/html,application/xhtml+xml"},
        )
        with urlopen(request, timeout=timeout) as response:
            content_type = response.headers.get("Content-Type", "")
            if "text/html" not in content_type.lower():
                host = urlparse(url).netloc or url
                return api.FetchResult(f"Referenced external resource at {host}.", "non_html")
            raw = response.read(200_000)
        text = raw.decode("utf-8", errors="ignore")
        summary = api.summarize_remote_page(text)
        if summary:
            return api.FetchResult(summary, "fetched")
        host = urlparse(url).netloc or url
        return api.FetchResult(f"Referenced article or page at {host}.", "fetched")
    except HTTPError as error:
        if error.code in {404, 410}:
            return api.FetchResult(None, "http_dead")
        return api.FetchResult(None, "fetch_failed")
    except (URLError, TimeoutError, ValueError):
        return api.FetchResult(None, "fetch_failed")
