from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


@dataclass(frozen=True)
class MaintenanceBudget:
    max_candidate_pages: int = 3
    max_context_chars: int = 4_500
    max_pages_rewritten: int = 4
    max_heavy_updater_calls: int = 1


@dataclass
class HeavyPageDelta:
    notes_to_add: list[str] = field(default_factory=list)
    open_questions_to_add: list[str] = field(default_factory=list)


@dataclass
class HeavyNewPageProposal:
    title: str
    page_type: str
    notes: list[str] = field(default_factory=list)
    open_questions: list[str] = field(default_factory=list)


@dataclass
class HeavyUpdateProposal:
    page_updates: dict[str, HeavyPageDelta] = field(default_factory=dict)
    proposed_connections: dict[str, list[str]] = field(default_factory=dict)
    proposed_new_pages: dict[str, HeavyNewPageProposal] = field(default_factory=dict)
    contradiction_items: list[str] = field(default_factory=list)
    deferred_items: list[str] = field(default_factory=list)
    budget_exceeded: bool = False
    reason: str = ""


def _note_token_set(note: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9]+", note.lower()) if token}


def _notes_conflict(api: ModuleType, existing_note: str, new_note: str) -> bool:
    existing_tokens = api._note_token_set(existing_note)
    new_tokens = api._note_token_set(new_note)
    if not existing_tokens or not new_tokens or existing_note.strip() == new_note.strip():
        return False
    preference_tokens = {"prefer", "prefers", "preferred", "preference", "favorite", "favourite"}
    if not (existing_tokens | new_tokens) & preference_tokens:
        return False
    overlap = len(existing_tokens & new_tokens)
    if overlap < 2:
        return False
    return True


def _assemble_heavy_context(
    api: ModuleType,
    *,
    title: str,
    body: str,
    router_decision: object,
    loaded_pages: dict[str, object],
    budget: MaintenanceBudget,
) -> tuple[list[str], list[str]]:
    context_chunks = [title.strip(), body.strip()]
    selected_targets: list[str] = []
    total_chars = sum(len(chunk) for chunk in context_chunks)
    deferred_items: list[str] = []

    for slug in router_decision.target_pages:
        if len(selected_targets) >= budget.max_candidate_pages:
            deferred_items.append(f"Deferred page '{slug}' because candidate page budget was exceeded.")
            continue
        page = loaded_pages.get(slug)
        page_context = ""
        if page is not None:
            page_context = api.bw.render_page(page)
        projected = total_chars + len(page_context)
        if selected_targets and projected > budget.max_context_chars:
            deferred_items.append(f"Deferred page '{slug}' because heavy-update context budget was exceeded.")
            continue
        selected_targets.append(slug)
        total_chars = projected
    if not selected_targets and router_decision.target_pages:
        selected_targets.append(router_decision.target_pages[0])
        deferred_items = [
            item
            for item in deferred_items
            if f"'{router_decision.target_pages[0]}'" not in item
        ]
    return selected_targets, deferred_items


def _build_heavy_update_proposal(
    api: ModuleType,
    *,
    title: str,
    source_record: object,
    loaded_pages: dict[str, object],
    resolved_assignments: list[tuple[str, str, str | None]],
    router_decision: object,
    budget: MaintenanceBudget,
) -> HeavyUpdateProposal:
    selected_targets, deferred_items = api._assemble_heavy_context(
        title=title,
        body=source_record.cleaned_text,
        router_decision=router_decision,
        loaded_pages=loaded_pages,
        budget=budget,
    )
    selected_set = set(selected_targets)
    proposal = HeavyUpdateProposal(
        deferred_items=deferred_items[:],
        budget_exceeded=bool(deferred_items),
        reason="Heavy update required for new-page creation, multi-page impact, or structural ambiguity.",
    )
    excerpt = api._source_excerpt(source_record)

    for slug, seed_kind, _parent_slug in resolved_assignments:
        if slug not in selected_set:
            if slug not in router_decision.target_pages:
                proposal.deferred_items.append(f"Deferred derived page '{slug}' because heavy-update budget was exceeded.")
                proposal.budget_exceeded = True
            continue
        page = loaded_pages.get(slug)
        existing_page = page is not None and (api.WIKI_ROOT / f"{slug}.md").exists()
        if not existing_page:
            proposal.proposed_new_pages[slug] = HeavyNewPageProposal(
                title=api.bw.page_title(slug),
                page_type=api.bw.classify_page(slug, title, seed_kind),
                notes=[excerpt] if excerpt else [],
            )
            continue

        delta = proposal.page_updates.setdefault(slug, HeavyPageDelta())
        if excerpt and excerpt not in page.notes:
            delta.notes_to_add.append(excerpt)
        for existing_note in page.notes:
            if api._notes_conflict(existing_note, excerpt):
                question = f'Potential contradiction after "{title}": "{existing_note}" vs "{excerpt}".'
                if question not in delta.open_questions_to_add:
                    delta.open_questions_to_add.append(question)
                if question not in proposal.contradiction_items:
                    proposal.contradiction_items.append(question)

    selected_assignment_slugs = [slug for slug, _seed_kind, _parent_slug in resolved_assignments if slug in selected_set]
    for slug, _seed_kind, parent_slug in resolved_assignments:
        if slug not in selected_set:
            continue
        targets = proposal.proposed_connections.setdefault(slug, [])
        for other_slug in selected_assignment_slugs:
            if other_slug != slug and other_slug not in targets:
                targets.append(other_slug)
        if parent_slug and parent_slug in loaded_pages:
            parent_targets = proposal.proposed_connections.setdefault(parent_slug, [])
            if slug not in parent_targets:
                parent_targets.append(slug)

    if proposal.contradiction_items:
        proposal.reason = "Heavy update recorded contradiction risk and open questions."
    return proposal


def _apply_heavy_update_proposal(
    api: ModuleType,
    *,
    title: str,
    source_record: object,
    loaded_pages: dict[str, object],
    resolved_assignments: list[tuple[str, str, str | None]],
    proposal: HeavyUpdateProposal,
    budget: MaintenanceBudget,
) -> tuple[dict[str, object], list[str], bool]:
    touched_pages: dict[str, object] = {}

    for slug, page_proposal in proposal.proposed_new_pages.items():
        page = loaded_pages.get(slug)
        if page is None:
            matching_assignment = next(
                (
                    (assignment_slug, seed_kind, _parent_slug)
                    for assignment_slug, seed_kind, _parent_slug in resolved_assignments
                    if assignment_slug == slug
                ),
                None,
            )
            seed_kind = matching_assignment[1] if matching_assignment is not None else "title"
            page = api.bw.Page(
                slug=slug,
                title=page_proposal.title,
                page_type=page_proposal.page_type,
                summary_hint=title,
            )
            page.seed_kinds.add(seed_kind)
            loaded_pages[slug] = page
        page.shape = api.bw.PAGE_SHAPE_ATOMIC
        page.page_type = page_proposal.page_type
        for note in page_proposal.notes:
            if note not in page.notes:
                page.notes.append(note)
        for question in page_proposal.open_questions:
            if question not in page.open_questions:
                page.open_questions.append(question)
        api.bw.add_source_to_page(page, source_record, seed_kind="ingest")
        touched_pages[slug] = page

    for slug, delta in proposal.page_updates.items():
        page = loaded_pages[slug]
        api.bw.add_source_to_page(page, source_record, seed_kind="ingest")
        for note in delta.notes_to_add:
            if note not in page.notes:
                page.notes.append(note)
        for question in delta.open_questions_to_add:
            if question not in page.open_questions:
                page.open_questions.append(question)
        touched_pages[slug] = page

    for slug, _seed_kind, parent_slug in resolved_assignments:
        if slug in touched_pages:
            if parent_slug:
                parent_page = loaded_pages[parent_slug]
                parent_page.shape = api.bw.PAGE_SHAPE_TOPIC
                touched_pages[parent_slug] = parent_page
            elif slug in loaded_pages:
                loaded_pages[slug].shape = api.bw.PAGE_SHAPE_ATOMIC

    for left, right_slugs in proposal.proposed_connections.items():
        if left not in loaded_pages:
            continue
        for right in right_slugs:
            if right not in loaded_pages:
                continue
            api.bw.connect_pages(loaded_pages, left, right)
            touched_pages[left] = loaded_pages[left]
            touched_pages[right] = loaded_pages[right]

    api.bw.prune_generic_media_links(loaded_pages)
    api.bw.ensure_meaningful_connections(loaded_pages)
    api.bw.finalize_page_shapes(loaded_pages)

    changed_slugs: list[str] = []
    ordered_touched = list(touched_pages)
    if len(ordered_touched) > budget.max_pages_rewritten:
        overflow = ordered_touched[budget.max_pages_rewritten :]
        proposal.deferred_items.extend(
            f"Deferred write for '{slug}' because rewritten page budget was exceeded." for slug in overflow
        )
        proposal.budget_exceeded = True
        ordered_touched = ordered_touched[: budget.max_pages_rewritten]

    for slug in ordered_touched:
        page = loaded_pages[slug]
        allow_missing_outbound = bool(page.topic_parent) or (slug not in proposal.proposed_new_pages and bool(page.connections))
        issues = api.bw.validate_page(page, allow_missing_outbound=allow_missing_outbound)
        if issues:
            if issues == ["atomic-pages-must-have-outbound-links"]:
                proposal.deferred_items.append(f"Deferred page '{slug}' until it has a meaningful outbound link.")
                proposal.budget_exceeded = True
                continue
            raise ValueError(f"invalid page '{slug}': {', '.join(issues)}")
        api.atomic_write_text(api.WIKI_ROOT / f"{slug}.md", api.bw.render_page(page))
        changed_slugs.append(slug)

    review_queued = bool(proposal.contradiction_items or proposal.deferred_items)
    if review_queued:
        reason = "heavy-update contradiction" if proposal.contradiction_items else "heavy-update deferred work"
        next_action = "Review contradiction items and confirm the correct current wiki state."
        if proposal.deferred_items and not proposal.contradiction_items:
            next_action = "Resume deferred heavy maintenance for the queued pages."
        api._append_review_backlog_item(
            reason=reason,
            affected_pages=ordered_touched,
            source_paths=[source_record.path],
            next_action=next_action,
        )
    return touched_pages, changed_slugs, review_queued
