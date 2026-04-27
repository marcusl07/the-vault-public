from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from types import ModuleType
import re


def slug_similarity(left: str, right: str) -> float:
    left_tokens = set(left.split("-"))
    right_tokens = set(right.split("-"))
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    return overlap / max(1, min(len(left_tokens), len(right_tokens)))


def derive_source_atomic_slug(api: ModuleType, page: object, source: object) -> str:
    candidate = api.clean_component(source.label) or api.slugify(source.label)
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


def infer_split_candidate_for_source(api: ModuleType, source: object, candidate_slugs: list[str]) -> str | None:
    if not candidate_slugs:
        return None

    raw_candidates = [source.label]
    path_name = Path(source.path).name
    if path_name:
        raw_candidates.append(Path(path_name).stem)

    normalized_candidates: list[str] = []
    for raw in raw_candidates:
        normalized = api.clean_component(raw) or api.slugify(raw)
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


def source_looks_like_lecture_material(api: ModuleType, source: object, page_slug: str) -> bool:
    normalized_label = api.clean_component(source.label) or api.slugify(source.label)
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
    return bool(api.LECTURE_SOURCE_RE.search(combined))


def page_looks_like_course_notes(api: ModuleType, page: object) -> bool:
    if not api.COURSE_PAGE_SLUG_RE.fullmatch(page.slug):
        return False
    lecture_like_sources = sum(1 for source in page.sources.values() if source_looks_like_lecture_material(api, source, page.slug))
    return lecture_like_sources >= max(2, len(page.sources) - 1)


def score_bucket_signals(api: ModuleType, page: object) -> object:
    reasons: list[str] = []
    lecture_like_page = page_looks_like_course_notes(api, page)

    if page.slug in api.GENERIC_BUCKET_SLUGS:
        reasons.append("generic-parent-slug")

    structured_note_groups = 0
    note_lines = (page.rendered_notes_markdown or "").splitlines()
    for line in note_lines:
        stripped = line.strip()
        if stripped.startswith("### ") or api.BOLD_HEADING_RE.match(stripped):
            if lecture_like_page and api.LECTURE_HEADING_RE.match(stripped):
                continue
            structured_note_groups += 1
    if structured_note_groups >= 2 and not lecture_like_page:
        reasons.append("multi-cluster-notes")

    if not lecture_like_page:
        source_slugs = {
            derived
            for source in page.sources.values()
            for derived in [derive_source_atomic_slug(api, page, source)]
            if derived != page.slug
        }
        if len(source_slugs) >= 3:
            reasons.append("heterogeneous-sources")

    if not lecture_like_page:
        child_connection_candidates = {
            connection_slug
            for connection_slug in api.sorted_connection_slugs(page)
            if connection_slug != page.slug and slug_similarity(connection_slug, page.slug) < 0.5
        }
        if len(child_connection_candidates) >= 2:
            reasons.append("existing-satellites")

    return api.BucketSignalResult(score=len(reasons), reasons=reasons)


def add_source_to_page(
    api: ModuleType,
    page: object,
    source: object,
    seed_kind: str = "migration",
    *,
    append_excerpt: bool = True,
) -> None:
    page.seed_kinds.add(seed_kind)
    page.sources[source.path] = source
    page.rendered_notes_markdown = None
    if not append_excerpt:
        return
    excerpt = api.compact_source_text(source, limit=220).strip()
    excerpt = re.sub(r"\s+", " ", excerpt)
    if excerpt and excerpt not in page.notes:
        page.notes.append(excerpt)


def connect_pages(pages: dict[str, object], left: str, right: str) -> None:
    if left == right:
        return
    if left not in pages or right not in pages:
        return
    pages[left].connections[right] += 1
    pages[right].connections[left] += 1


def split_candidate_evaluation_map(split_decision: object) -> dict[str, object]:
    return {evaluation.slug: evaluation for evaluation in split_decision.candidate_evaluations if evaluation.accepted}


def grounded_split_candidate_slugs(api: ModuleType, split_decision: object) -> list[str]:
    evaluation_map = split_candidate_evaluation_map(split_decision)
    grounded_slugs: list[str] = []
    for child_slug in api.ordered_unique(split_decision.candidate_satellite_slugs):
        evaluation = evaluation_map.get(child_slug)
        if evaluation is None or not evaluation.grounding:
            continue
        grounded_slugs.append(child_slug)
    return grounded_slugs


def gather_split_source_groups(api: ModuleType, page: object, split_decision: object) -> tuple[dict[str, list[object]], set[str]]:
    source_groups: dict[str, list[object]] = defaultdict(list)
    assigned_source_paths: set[str] = set()
    if len(page.sources) == 1:
        grounded_child_slugs = grounded_split_candidate_slugs(api, split_decision)
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
        inferred_slug = infer_split_candidate_for_source(api, source, split_decision.candidate_satellite_slugs)
        if inferred_slug is None:
            continue
        source_groups[inferred_slug].append(source)
        assigned_source_paths.add(source_path)
    return source_groups, assigned_source_paths


def build_split_child_notes(split_decision: object, child_slug: str) -> list[str]:
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


def normalize_split_note_text(text: str) -> str:
    cleaned = text.strip().lower()
    cleaned = re.sub(r"\[\[([^\]|]+)(?:\|[^\]]+)?\]\]", r"\1", cleaned)
    cleaned = re.sub(r"\[[^\]]+\]\([^)]+\)", " ", cleaned)
    cleaned = re.sub(r"[`*_>#]", " ", cleaned)
    cleaned = re.sub(r"^\s*[-+*]\s+", "", cleaned)
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def split_child_note_signature(split_decision: object, child_slug: str) -> str:
    notes = build_split_child_notes(split_decision, child_slug)
    return normalize_split_note_text(" ".join(notes))


def _split_child_rejection_reason(
    api: ModuleType,
    page: object,
    split_decision: object,
    source_groups: dict[str, list[object]],
    child_slug: str,
    signature: str,
) -> str | None:
    if not signature:
        return "empty child grounding"

    title_only_values = {
        normalize_split_note_text(child_slug),
        normalize_split_note_text(child_slug.replace("-", " ")),
        normalize_split_note_text(api.page_title(child_slug)),
    }
    if signature in title_only_values:
        return "title-only child grounding"

    source_label_values: set[str] = set()
    for source in page.sources.values():
        source_label_values.add(normalize_split_note_text(source.label))
        source_label_values.add(normalize_split_note_text(Path(source.path).stem))
    for source in source_groups.get(child_slug, []):
        source_label_values.add(normalize_split_note_text(source.label))
        source_label_values.add(normalize_split_note_text(Path(source.path).stem))
    if signature in {value for value in source_label_values if value}:
        return "source-label-only child grounding"

    parent_note_values = {
        normalize_split_note_text(note)
        for note in page.notes
        if normalize_split_note_text(note)
    }
    if page.rendered_notes_markdown:
        for raw_line in page.rendered_notes_markdown.splitlines():
            stripped = raw_line.strip()
            if stripped.startswith("- "):
                parent_note_values.add(normalize_split_note_text(stripped[2:]))
    if signature in parent_note_values:
        return "parent-note-identical child grounding"

    return None


def validate_split_child_grounding(
    api: ModuleType,
    page: object,
    split_decision: object,
    source_groups: dict[str, list[object]],
) -> object:
    accepted_slugs = api.ordered_unique(split_decision.candidate_satellite_slugs)
    if split_decision.is_atomic or not accepted_slugs:
        return split_decision

    signatures = {slug: split_child_note_signature(split_decision, slug) for slug in accepted_slugs}
    source_sets = {
        slug: tuple(sorted(source.path for source in source_groups.get(slug, [])))
        for slug in accepted_slugs
    }
    rejected: dict[str, str] = {}

    for child_slug, signature in signatures.items():
        reason = _split_child_rejection_reason(api, page, split_decision, source_groups, child_slug, signature)
        if reason:
            rejected[child_slug] = reason

    duplicate_groups: defaultdict[tuple[str, tuple[str, ...]], list[str]] = defaultdict(list)
    for child_slug, signature in signatures.items():
        if child_slug in rejected or not signature:
            continue
        duplicate_groups[(signature, source_sets[child_slug])].append(child_slug)
    for duplicate_slugs in duplicate_groups.values():
        if len(duplicate_slugs) < 2:
            continue
        for child_slug in duplicate_slugs:
            rejected[child_slug] = "duplicate sibling child grounding"

    if not rejected:
        return split_decision

    candidate_slugs = [slug for slug in accepted_slugs if slug not in rejected]
    candidate_evaluations: list[object] = []
    for evaluation in split_decision.candidate_evaluations:
        if evaluation.slug not in rejected:
            candidate_evaluations.append(evaluation)
            continue
        candidate_evaluations.append(
            api.SplitCandidateEvaluation(
                slug=evaluation.slug,
                accepted=False,
                grounding=list(evaluation.grounding),
                why_distinct=evaluation.why_distinct,
                passes_direct_link_test=evaluation.passes_direct_link_test,
                passes_stable_page_test=evaluation.passes_stable_page_test,
                passes_search_test=evaluation.passes_search_test,
                rejection_reasons=api.ordered_unique([*evaluation.rejection_reasons, rejected[evaluation.slug]]),
            )
        )

    rejection_reasons = api.ordered_unique([*split_decision.rejection_reasons, *rejected.values()])
    source_assignments = {
        source_path: satellite_slug
        for source_path, satellite_slug in split_decision.source_assignments.items()
        if satellite_slug not in rejected
    }
    if len(candidate_slugs) < 2:
        return api.PageSplitDecision(
            is_atomic=True,
            rationale=split_decision.rationale,
            rejection_reasons=api.ordered_unique([*rejection_reasons, "child grounding collapsed below split threshold"]),
            candidate_evaluations=candidate_evaluations,
        )

    return api.PageSplitDecision(
        is_atomic=False,
        candidate_satellite_slugs=candidate_slugs,
        source_assignments=source_assignments,
        rationale=split_decision.rationale,
        rejection_reasons=rejection_reasons,
        candidate_evaluations=candidate_evaluations,
    )


def page_title_is_source_shaped(api: ModuleType, page: object) -> bool:
    normalized_title = page.title.strip().lower().replace(" ", "-")
    normalized_slug = page.slug.strip().lower()
    if normalized_slug in api.GENERIC_BUCKET_SLUGS:
        return True
    if api.looks_like_archive(page.title):
        return True
    if re.fullmatch(r"(?:lecture|lec|week|chapter|notes?|misc|overview|summary)(?:-\d+)?", normalized_title):
        return True
    if re.fullmatch(r"(?:lecture|lec|week|chapter|notes?|misc|overview|summary)(?:-\d+)?", normalized_slug):
        return True
    return False


def resolve_parent_split_mode(api: ModuleType, page: object, child_slugs: list[str]) -> str:
    if any(child_slug == page.slug or slug_similarity(child_slug, page.slug) >= 0.8 for child_slug in child_slugs):
        return api.PAGE_SHAPE_ATOMIC
    if not page_title_is_source_shaped(api, page) and len(child_slugs) >= 2:
        return api.PAGE_SHAPE_TOPIC
    if page_title_is_source_shaped(api, page):
        return "deprecated"
    return api.PAGE_SHAPE_ATOMIC


def apply_split_decision(
    api: ModuleType,
    pages: dict[str, object],
    parent_slug: str,
    split_decision: object,
    *,
    seed_kind: str = "migration",
    allow_partial_source_coverage: bool = False,
) -> bool:
    parent_page = pages.get(parent_slug)
    child_slugs = api.ordered_unique(split_decision.candidate_satellite_slugs)
    if parent_page is None or split_decision.is_atomic or len(child_slugs) < 2:
        return False

    source_groups, assigned_source_paths = gather_split_source_groups(api, parent_page, split_decision)
    split_decision = validate_split_child_grounding(api, parent_page, split_decision, source_groups)
    child_slugs = api.ordered_unique(split_decision.candidate_satellite_slugs)
    if split_decision.is_atomic or len(child_slugs) < 2:
        return False

    source_groups, assigned_source_paths = gather_split_source_groups(api, parent_page, split_decision)
    if parent_page.sources and not allow_partial_source_coverage:
        if len(parent_page.sources) > 1 and len(assigned_source_paths) != len(parent_page.sources):
            return False
        if len(parent_page.sources) > 1 and len(source_groups) < 2:
            return False

    parent_mode = resolve_parent_split_mode(api, parent_page, child_slugs)
    if parent_mode == api.PAGE_SHAPE_TOPIC:
        parent_page.shape = api.PAGE_SHAPE_TOPIC
        parent_page.notes = []
        parent_page.rendered_notes_markdown = None
        parent_page.sources = {}
        parent_page.connections = Counter()
    elif parent_mode == "deprecated":
        replacements = " and ".join(f"[[{slug}]]" for slug in child_slugs)
        parent_page.shape = api.PAGE_SHAPE_ATOMIC
        parent_page.notes = [f"Deprecated: superseded by {replacements}."]
        parent_page.rendered_notes_markdown = None
        parent_page.sources = {}
        parent_page.connections = Counter()

    for child_slug in child_slugs:
        child_page = pages.setdefault(
            child_slug,
            api.Page(
                slug=child_slug,
                title=api.page_title(child_slug),
                page_type=api.classify_page(child_slug, parent_page.title, seed_kind),
                summary_hint=parent_page.title,
            ),
        )
        child_page.shape = api.PAGE_SHAPE_ATOMIC
        child_page.page_type = api.classify_page(child_slug, parent_page.title, seed_kind)
        child_page.seed_kinds.add(seed_kind)
        child_page.topic_parent = parent_slug if parent_mode == api.PAGE_SHAPE_TOPIC else None

        split_notes = build_split_child_notes(split_decision, child_slug)
        for note in split_notes:
            if note not in child_page.notes:
                child_page.notes.append(note)
        for source in source_groups.get(child_slug, []):
            add_source_to_page(api, child_page, source, seed_kind, append_excerpt=not bool(split_notes))
        connect_pages(pages, parent_slug, child_slug)

    return True
