from __future__ import annotations

from datetime import datetime
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Callable, TextIO

if TYPE_CHECKING:
    from types import ModuleType


def _normalize_ingest_item(api: ModuleType, item: dict[str, object]) -> dict[str, str]:
    if not isinstance(item, dict):
        raise ValueError("ingest item must be a mapping")
    capture_id = item.get("capture_id")
    raw_path = item.get("raw_path")
    if not isinstance(capture_id, str) or not capture_id:
        raise ValueError("ingest item capture_id must be a non-empty string")
    if not isinstance(raw_path, str) or not raw_path:
        raise ValueError("ingest item raw_path must be a non-empty string")
    return {"capture_id": capture_id, "raw_path": api.normalize_repo_path(raw_path)}


def validate_ingest_inputs(api: ModuleType, items: list[dict[str, object]]) -> list[dict[str, str]]:
    validated: list[dict[str, str]] = []
    for item in items:
        normalized = api._normalize_ingest_item(item)
        raw_abspath = api.ROOT / normalized["raw_path"]
        if not raw_abspath.exists():
            raise ValueError(f"raw note does not exist for capture_id {normalized['capture_id']}: {normalized['raw_path']}")
        artifact = api.read_source_artifact(raw_abspath)
        if artifact.frontmatter.get("capture_id") != normalized["capture_id"]:
            raise ValueError(
                f"raw note frontmatter capture_id mismatch for capture_id {normalized['capture_id']}: {normalized['raw_path']}"
            )
        if artifact.source_kind != "capture":
            raise ValueError(f"raw note missing capture source_kind for capture_id {normalized['capture_id']}: {normalized['raw_path']}")
        if artifact.source_id != api.stable_source_id("capture", normalized["capture_id"]):
            raise ValueError(f"raw note source_id mismatch for capture_id {normalized['capture_id']}: {normalized['raw_path']}")
        validated.append(normalized)
    return validated


def _today_date(api: ModuleType) -> str:
    return datetime.now().date().isoformat()


def _read_raw_note(api: ModuleType, path: Path) -> tuple[dict[str, object], str, str]:
    artifact = api.read_source_artifact(path)
    if not artifact.title:
        raise ValueError(f"raw note missing title frontmatter: {path}")
    return artifact.frontmatter, artifact.title, api.bw.content_body(artifact)


def _derive_path_topics(api: ModuleType, path: Path) -> list[str]:
    path_abs = Path(os.path.abspath(path))
    raw_root_abs = Path(os.path.abspath(api.RAW_ROOT))
    if Path(os.path.commonpath([raw_root_abs, path_abs])) == raw_root_abs:
        relative_parts = Path(os.path.relpath(path_abs, raw_root_abs)).parts[:-1]
    else:
        relative_parts = path.parts[:-1]
    topics: list[str] = []
    for component in reversed(relative_parts):
        topic = api.bw.clean_component(component)
        if topic and topic not in topics:
            topics.append(topic)
        if len(topics) >= 3:
            break
    return topics


def _build_default_page_assignments(api: ModuleType, title: str, body: str, raw_abspath: Path) -> list[tuple[str, str]]:
    url = api.bw.extract_first_url(body)
    path_topics = api._derive_path_topics(raw_abspath)
    title_slug = api.bw.clean_component(title) or api.bw.slugify(title)
    assignments: list[tuple[str, str]] = []
    seen: set[str] = set()

    if api.bw.should_fold_note_into_parent(title, body, url) and path_topics:
        return [(path_topics[0], "folder")]

    if title_slug and title_slug != "new-note" and not api.is_placeholder_title(title):
        assignments.append((title_slug, "title"))
        seen.add(title_slug)

    for topic in path_topics[:2]:
        if topic not in seen:
            assignments.append((topic, "folder"))
            seen.add(topic)

    if not assignments:
        return [("uncategorized-captures", "folder")]
    return assignments


def _content_owner_slug(api: ModuleType, assignments: list[tuple[str, str, str | None]]) -> str | None:
    if not assignments:
        return None
    for preferred_seed_kind in ("title", "model", "query"):
        for slug, seed_kind, _parent_slug in assignments:
            if seed_kind == preferred_seed_kind:
                return slug
    return assignments[0][0]


def source_artifact_to_evidence(api: ModuleType, artifact: object) -> object:
    return api.bw.source_artifact_to_evidence(
        artifact,
        fetcher=api.bw.fetch_url_summary,
        extract_first_url=api.bw.extract_first_url,
        is_google_search_url=api.bw.is_google_search_url,
        clean_source_text=api.bw.clean_source_text,
        detect_source_tags=api.bw.detect_source_tags,
        should_exclude_from_body=api.bw.should_exclude_from_body,
    )


def _resolve_synthesis_config(api: ModuleType) -> tuple[str | None, str]:
    return os.environ.get("GEMINI_API_KEY"), os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")


def _parse_existing_note_snippets(api: ModuleType, note_lines: list[str]) -> list[str]:
    return api.bw.parse_note_snippets(note_lines)


def _parse_source_line(api: ModuleType, line: str, retained_evidence: str) -> object | None:
    return api.bw.parse_source_line(line, retained_evidence)


def _parse_connection_slugs(api: ModuleType, connection_lines: list[str]) -> list[str]:
    return api.bw.extract_connection_slugs(connection_lines)


def _read_wiki_page(api: ModuleType, slug: str, *, original_title: str, seed_kind: str) -> object:
    page_path = api.WIKI_ROOT / f"{slug}.md"
    if page_path.exists():
        return api.bw.parsed_page_to_page(api.bw.parse_page_file(page_path))
    return api.bw.Page(
        slug=slug,
        title=api.bw.page_title(slug),
        page_type=api.bw.classify_page(slug, original_title, seed_kind),
        summary_hint=original_title,
    )


def _validate_router_decision(api: ModuleType, decision: object) -> object:
    allowed_actions = {"ignore", "light_update", "heavy_update", "queue_review"}
    allowed_risk = {"low", "medium", "high"}
    allowed_confidence = {"low", "medium", "high"}
    if decision.action not in allowed_actions:
        raise ValueError(f"invalid router action: {decision.action}")
    if decision.contradiction_risk not in allowed_risk:
        raise ValueError(f"invalid contradiction risk: {decision.contradiction_risk}")
    if decision.confidence not in allowed_confidence:
        raise ValueError(f"invalid router confidence: {decision.confidence}")
    if not decision.reason.strip():
        raise ValueError("router reason must be non-empty")
    if any(not slug.strip() for slug in decision.target_pages):
        raise ValueError("router target pages must be non-empty")
    return decision


def _route_source_update(
    api: ModuleType,
    *,
    title: str,
    body: str,
    page_assignments: list[tuple[str, str]],
) -> object:
    target_pages = [slug for slug, _seed_kind in page_assignments]
    candidate_new_pages: list[str] = []
    reorganization_risk = False
    existing_atomic_targets = 0
    contradiction_risk = "low"
    router_excerpt = api.re.sub(r"\s+", " ", body.strip())
    for slug, seed_kind in page_assignments:
        page = api._read_wiki_page(slug, original_title=title, seed_kind=seed_kind)
        if (api.WIKI_ROOT / f"{slug}.md").exists():
            if api.bw.page_shape(page) == api.bw.PAGE_SHAPE_TOPIC:
                reorganization_risk = True
            else:
                existing_atomic_targets += 1
                if router_excerpt and any(api._notes_conflict(existing_note, router_excerpt) for existing_note in page.notes):
                    contradiction_risk = "high"
        else:
            candidate_new_pages.append(slug)
    new_page_signal = bool(candidate_new_pages)
    if len(target_pages) == 1 and existing_atomic_targets == 1 and not new_page_signal and not reorganization_risk:
        if contradiction_risk != "low":
            action = "heavy_update"
            confidence = "medium"
            reason = "Single existing atomic page shows contradiction risk and needs review-aware maintenance."
        else:
            action = "light_update"
            confidence = "high"
            reason = "Single existing atomic page can absorb a bounded update."
    elif not body.strip() and (api.bw.slugify(title) in {"", "new-note"} or api.is_placeholder_title(title)):
        action = "ignore"
        confidence = "medium"
        reason = "Source has no durable title or body signal after normalization."
    elif reorganization_risk or len(target_pages) > 2 or new_page_signal:
        action = "heavy_update"
        confidence = "medium"
        reason = "Source impacts new pages or pages that may need structural reorganization."
    else:
        action = "light_update"
        confidence = "medium"
        reason = "Source touches a small known page set and can be merged deterministically."
    return api._validate_router_decision(
        api.RouterDecision(
            action=action,
            target_pages=target_pages,
            new_page_signal=new_page_signal,
            candidate_new_pages=candidate_new_pages,
            contradiction_risk=contradiction_risk,
            reorganization_risk=reorganization_risk,
            confidence=confidence,
            reason=reason,
        )
    )


def _rewrite_index(api: ModuleType, changed_pages: list[tuple[str, str]]) -> None:
    index_path = api.WIKI_ROOT / "index.md"
    page_types = api.bw.load_existing_page_types(index_path)
    pages: dict[str, object] = {}
    for page_path in sorted(api.WIKI_ROOT.glob("*.md")):
        if page_path.stem in {"index", "log", "review", Path(api.bw.CATALOG_PATH).stem}:
            continue
        parsed = api.bw.parse_page_file(page_path, page_type=page_types.get(page_path.stem))
        pages[page_path.stem] = api.bw.parsed_page_to_page(parsed)
    counts = api.bw.inbound_link_counts(pages)
    grouped: dict[str, list[object]] = {section: [] for section in api.bw.INDEX_SECTION_ORDER}
    forced_slugs = {slug for slug, _page_type in changed_pages}
    for page in pages.values():
        if (
            page.slug in forced_slugs
            or api.bw.page_shape(page) == api.bw.PAGE_SHAPE_TOPIC
            or counts.get(page.slug, 0) >= api.bw.HIGH_SIGNAL_INBOUND_THRESHOLD
        ):
            grouped.setdefault(page.page_type, []).append(page)

    lines = [
        "# Wiki Index",
        "",
        f"_Last updated: {api._today_date()} — {len(pages)} pages_",
        "_Navigation only: topic pages plus high-signal atomic pages. Use [[catalog]] for exhaustive lookup._",
        "",
    ]
    for section in api.bw.INDEX_SECTION_ORDER:
        lines.append(f"## {section}")
        section_pages = sorted(grouped.get(section, []), key=lambda page: page.title.lower())
        if not section_pages:
            lines.append("- None yet.")
            lines.append("")
            continue
        for page in section_pages:
            lines.append(f"- [[{page.slug}]] — {api.bw.page_index_summary(page)}")
        lines.append("")
    api.atomic_write_text(index_path, "\n".join(lines))


def _append_wiki_ingest_log(
    api: ModuleType,
    title: str,
    *,
    router_decision: object | None = None,
    deferred_items: list[str] | None = None,
) -> None:
    api.apply_operational_effects(
        api.OperationalEffects.ingest_log(
            title,
            date=api._today_date(),
            router_decision=router_decision,
            deferred_items=deferred_items,
        )
    )


def _append_bootstrap_pipeline_log(api: ModuleType, *, processed_sources: int, changed_pages: int, failed_sources: int) -> None:
    api.apply_operational_effects(
        api.OperationalEffects.bootstrap_log(
            date=api._today_date(),
            processed_sources=processed_sources,
            changed_pages=changed_pages,
            failed_sources=failed_sources,
        )
    )


def _upsert_wiki_pages_for_note(
    api: ModuleType,
    *,
    title: str,
    body: str,
    raw_path: Path,
    budget: object | None = None,
    mode: str = "ingest",
    write_ingest_log: bool = True,
) -> object:
    effective_budget = budget or api._default_maintenance_budget(mode=mode if mode == "bootstrap" else "routine")
    source_record = api.source_artifact_to_evidence(api.read_source_artifact(raw_path))
    page_assignments = api._build_default_page_assignments(title, body, raw_path)
    router_decision = api._route_source_update(title=title, body=body, page_assignments=page_assignments)
    effects = api.OperationalEffects()
    if router_decision.action == "ignore":
        return api.MaintenanceOutcome(
            router_decision=router_decision,
            effects=effects,
            source_path=api.normalize_repo_path(raw_path),
            mode=mode,
        )
    loaded_pages: dict[str, object] = {}

    def load_page(slug: str, seed_kind: str) -> object:
        if slug in loaded_pages:
            return loaded_pages[slug]
        page = api._read_wiki_page(slug, original_title=title, seed_kind=seed_kind)
        loaded_pages[slug] = page
        return page

    resolved_assignments: list[tuple[str, str, str | None]] = []
    for slug, seed_kind in page_assignments:
        page = load_page(slug, seed_kind)
        if api.bw.page_shape(page) == api.bw.PAGE_SHAPE_TOPIC:
            satellite_slug = api.bw.clean_component(title) or api.bw.slugify(title) or f"{slug}-note"
            if satellite_slug == slug:
                satellite_slug = f"{slug}-note"
            satellite_page = load_page(satellite_slug, seed_kind)
            satellite_page.topic_parent = slug
            resolved_assignments.append((satellite_slug, seed_kind, slug))
        else:
            resolved_assignments.append((slug, seed_kind, None))

    if not resolved_assignments:
        fallback_slug = api.bw.clean_component(title) or api.bw.slugify(title) or "uncategorized-captures"
        resolved_assignments.append((fallback_slug, "title", None))

    resolved_slugs = [slug for slug, _seed_kind, _parent_slug in resolved_assignments]
    simple_new_page_flow = (
        router_decision.action == "heavy_update"
        and len(router_decision.target_pages) == 1
        and router_decision.new_page_signal
        and not router_decision.reorganization_risk
    )
    if router_decision.action == "heavy_update" and not simple_new_page_flow:
        proposal = api._build_heavy_update_proposal(
            title=title,
            source_record=source_record,
            loaded_pages=loaded_pages,
            resolved_assignments=resolved_assignments,
            router_decision=router_decision,
            budget=effective_budget,
        )
        _touched_pages, changed_slugs, review_queued, review_effects = api._apply_heavy_update_proposal(
            title=title,
            source_record=source_record,
            loaded_pages=loaded_pages,
            resolved_assignments=resolved_assignments,
            proposal=proposal,
            budget=effective_budget,
        )
        effects.extend(review_effects)
        api._rewrite_index([(slug, loaded_pages[slug].page_type) for slug in changed_slugs])
        if write_ingest_log:
            log_effects = api.OperationalEffects.ingest_log(
                title,
                date=api._today_date(),
                router_decision=router_decision,
                deferred_items=proposal.deferred_items,
            )
            effects.extend(log_effects)
            api.apply_operational_effects(log_effects)
        return api.MaintenanceOutcome(
            changed_slugs=changed_slugs,
            router_decision=router_decision,
            review_queued=review_queued,
            deferred_items=proposal.deferred_items,
            effects=effects,
            source_path=api.normalize_repo_path(raw_path),
            mode=mode,
        )

    content_owner_slug = api._content_owner_slug(resolved_assignments)
    for slug, seed_kind, parent_slug in resolved_assignments:
        page = load_page(slug, seed_kind)
        page.shape = api.bw.PAGE_SHAPE_ATOMIC
        page.page_type = api.bw.classify_page(slug, title, seed_kind)
        page.seed_kinds.add(seed_kind)
        if slug == content_owner_slug:
            api.bw.add_source_to_page(page, source_record, seed_kind)
        if parent_slug:
            parent_page = load_page(parent_slug, seed_kind)
            parent_page.shape = api.bw.PAGE_SHAPE_TOPIC
            api.bw.connect_pages(loaded_pages, parent_slug, slug)

    for slug in resolved_slugs:
        for other_slug in resolved_slugs:
            if other_slug != slug:
                api.bw.connect_pages(loaded_pages, slug, other_slug)

    api_key, model = api._resolve_synthesis_config()
    if api_key:
        for slug in api.bw.ordered_unique(resolved_slugs):
            page = loaded_pages.get(slug)
            if page is None or api.bw.page_shape(page) == api.bw.PAGE_SHAPE_TOPIC or not page.sources:
                continue
            try:
                split_decision = api.bw.analyze_page_for_atomic_split(page, api_key, model)
            except Exception as exc:
                print(f"Split analysis skipped for '{slug}': {exc}", file=sys.stderr)
                continue
            api.bw.apply_split_decision(
                loaded_pages,
                slug,
                split_decision,
                seed_kind="ingest",
                allow_partial_source_coverage=True,
            )

    api.bw.prune_generic_media_links(loaded_pages)
    api.bw.ensure_meaningful_connections(loaded_pages)
    api.bw.finalize_page_shapes(loaded_pages)
    changed_slugs: list[str] = []
    deferred_items: list[str] = []
    review_queued = False
    for slug, page in loaded_pages.items():
        allow_missing_outbound = bool(page.topic_parent)
        issues = api.bw.validate_page(page, allow_missing_outbound=allow_missing_outbound)
        if issues:
            if issues == ["atomic-pages-must-have-outbound-links"]:
                deferred_items.append(f"Deferred page '{slug}' until it has a meaningful outbound link.")
                review_queued = True
                continue
            raise ValueError(f"invalid page '{slug}': {', '.join(issues)}")
        api.atomic_write_text(api.WIKI_ROOT / f"{slug}.md", api.bw.render_page(page))
        changed_slugs.append(slug)

    if deferred_items:
        review_effects = api.OperationalEffects.review_item(
            reason="light-update deferred work",
            affected_pages=[slug for slug, _seed_kind, _parent_slug in resolved_assignments],
            source_paths=[source_record.path],
            next_action="Add a meaningful outbound link or revise the page split before applying this source.",
        )
        effects.extend(review_effects)
        api.apply_operational_effects(review_effects)
    api._rewrite_index([(slug, loaded_pages[slug].page_type) for slug in changed_slugs])
    if write_ingest_log:
        log_effects = api.OperationalEffects.ingest_log(
            title,
            date=api._today_date(),
            router_decision=router_decision,
            deferred_items=deferred_items,
        )
        effects.extend(log_effects)
        api.apply_operational_effects(log_effects)
    return api.MaintenanceOutcome(
        changed_slugs=changed_slugs,
        router_decision=router_decision,
        review_queued=review_queued,
        deferred_items=deferred_items,
        effects=effects,
        source_path=api.normalize_repo_path(raw_path),
        mode=mode,
    )


def maintain_source_artifact(
    api: ModuleType,
    source_path: Path,
    *,
    mode: str,
    budget: object | None = None,
    options: dict[str, object] | None = None,
) -> object:
    if mode not in {"ingest", "bootstrap"}:
        raise ValueError(f"unsupported maintenance mode: {mode}")
    options = options or {}
    artifact = api.read_source_artifact(source_path)
    if not artifact.title:
        raise ValueError(f"source artifact missing title: {source_path}")
    body = api.bw.content_body(artifact)
    effective_budget = budget or api._default_maintenance_budget(mode=mode if mode == "bootstrap" else "routine")
    write_ingest_log = bool(options.get("write_ingest_log", mode == "ingest"))
    return api._upsert_wiki_pages_for_note(
        title=artifact.title,
        body=body,
        raw_path=source_path,
        budget=effective_budget,
        mode=mode,
        write_ingest_log=write_ingest_log,
    )


def _iter_source_artifact_paths(api: ModuleType, *, include_chat: bool) -> list[Path]:
    paths = sorted(path for path in api.RAW_ROOT.rglob("*.md") if path.is_file())
    if include_chat and api.CHAT_SOURCES_ROOT.exists():
        paths.extend(sorted(path for path in api.CHAT_SOURCES_ROOT.rglob("*.md") if path.is_file()))
    return paths


def bootstrap_integrate_sources(
    api: ModuleType,
    *,
    source_paths: list[Path] | None = None,
    include_chat: bool = True,
    budget: object | None = None,
) -> dict[str, object]:
    effective_budget = budget or api._default_maintenance_budget(mode="bootstrap")
    selected_paths = source_paths or api._iter_source_artifact_paths(include_chat=include_chat)
    changed_slugs: set[str] = set()
    failed_sources: list[str] = []
    processed_sources = 0
    deferred_count = 0
    review_count = 0

    for source_path in selected_paths:
        try:
            outcome = api.maintain_source_artifact(
                source_path,
                mode="bootstrap",
                budget=effective_budget,
                options={"write_ingest_log": False},
            )
        except Exception:
            failed_sources.append(api.normalize_repo_path(source_path))
            continue
        processed_sources += 1
        changed_slugs.update(outcome.changed_slugs)
        deferred_count += len(outcome.deferred_items)
        if outcome.review_queued:
            review_count += 1

    if selected_paths:
        api.apply_operational_effects(api.OperationalEffects.bootstrap_log(
            date=api._today_date(),
            processed_sources=processed_sources,
            changed_pages=len(changed_slugs),
            failed_sources=len(failed_sources),
        ))

    return {
        "processed_sources": processed_sources,
        "changed_slugs": sorted(changed_slugs),
        "failed_sources": failed_sources,
        "deferred_count": deferred_count,
        "review_count": review_count,
    }


def _default_integration_handler(
    api: ModuleType,
    capture_id: str,
    raw_path: Path,
) -> object:
    _ = capture_id
    return api.maintain_source_artifact(
        raw_path,
        mode="ingest",
    )


def ingest_raw_notes(
    api: ModuleType,
    items: list[dict[str, object]],
    *,
    integration_handler: Callable[[str, Path], None] | None = None,
    retry_failed: bool = False,
    debug: bool = False,
    debug_stream: TextIO | None = None,
    log_path: Path | None = None,
    state_path: Path | None = None,
    dry_run: bool = False,
) -> object:
    active_handler = integration_handler or api._default_integration_handler
    if dry_run:
        effects = api.OperationalEffects()
        result = {"integrated": [], "skipped": [], "failed": [], "discovered": [], "updated": [], "processed": [], "failed_risk": []}
        for raw_item in items:
            try:
                item = api._normalize_ingest_item(raw_item)
                raw_abspath = api.ROOT / item["raw_path"]
                if not raw_abspath.exists():
                    raise ValueError(f"raw note does not exist for capture_id {item['capture_id']}: {item['raw_path']}")
                artifact = api.read_source_artifact(raw_abspath)
                if artifact.frontmatter.get("capture_id") != item["capture_id"]:
                    raise ValueError(
                        f"raw note frontmatter capture_id mismatch for capture_id {item['capture_id']}: {item['raw_path']}"
                    )
                if artifact.source_kind != "capture":
                    raise ValueError(f"raw note missing capture source_kind for capture_id {item['capture_id']}: {item['raw_path']}")
                if artifact.source_id != api.stable_source_id("capture", item["capture_id"]):
                    raise ValueError(f"raw note source_id mismatch for capture_id {item['capture_id']}: {item['raw_path']}")

                state_item = api.raw_state_item(artifact)
                state_payload = {
                    "item_id": state_item["item_id"],
                    "source_id": state_item["source_id"],
                    "raw_path": state_item["raw_path"],
                    "content_hash": state_item["content_hash"],
                }
                prior_state = api.latest_state_record(str(state_item["item_id"]), state_path=state_path)
                if not api.state_item_seen(str(state_item["item_id"]), state_path=state_path):
                    discovered = {"event": "discovered", **state_payload}
                    effects.extend(api.OperationalEffects.state_event(discovered))
                    result["discovered"].append(discovered)
                    api.debug_print(f"Would discover {item['capture_id']}", enabled=debug, stream=debug_stream)

                legacy_integrated = (
                    api._latest_ingest_event(capture_id=item["capture_id"], raw_path=item["raw_path"], log_path=log_path)
                    == "integrated"
                )
                unchanged_in_state = (
                    prior_state is not None
                    and prior_state.get("content_hash") == state_item["content_hash"]
                    and prior_state.get("event") in {"processed", "skipped", "updated"}
                )
                unchanged_legacy_item = prior_state is None and legacy_integrated
                if (unchanged_in_state or unchanged_legacy_item) and not retry_failed:
                    skipped = {"event": "skipped", **state_payload, "reason": "unchanged"}
                    effects.extend(api.OperationalEffects.state_event(skipped))
                    result["skipped"].append(item)
                    api.debug_print(f"Would skip {item['capture_id']}: unchanged", enabled=debug, stream=debug_stream)
                    continue

                if prior_state is not None and prior_state.get("content_hash") != state_item["content_hash"]:
                    updated = {"event": "updated", **state_payload}
                    effects.extend(api.OperationalEffects.state_event(updated))
                    result["updated"].append(updated)
                    api.debug_print(f"Would update {item['capture_id']}", enabled=debug, stream=debug_stream)

                effects.extend(api.OperationalEffects.state_event({"event": "processed", **state_payload}))
                result["processed"].append(item)
                api.debug_print(f"Would process {item['capture_id']}", enabled=debug, stream=debug_stream)
            except Exception as exc:
                fallback_item = raw_item if isinstance(raw_item, dict) else {"item": repr(raw_item)}
                result["failed_risk"].append({"item": fallback_item, "error": str(exc)})
                api.debug_print(f"Failed risk for ingest item: {exc}", enabled=debug, stream=debug_stream)
        result["effects"] = effects.to_dict()
        return result

    validated = api.validate_ingest_inputs(items)
    effects = api.OperationalEffects()
    result = {"integrated": [], "skipped": [], "failed": []}

    for item in validated:
        raw_abspath = api.ROOT / item["raw_path"]
        artifact = api.read_source_artifact(raw_abspath)
        state_item = api.raw_state_item(artifact)
        state_payload = {
            "item_id": state_item["item_id"],
            "source_id": state_item["source_id"],
            "raw_path": state_item["raw_path"],
            "content_hash": state_item["content_hash"],
        }
        prior_state = api.latest_state_record(str(state_item["item_id"]), state_path=state_path)
        if not api.state_item_seen(str(state_item["item_id"]), state_path=state_path):
            state_effects = api.OperationalEffects.state_event({"event": "discovered", **state_payload})
            effects.extend(state_effects)
            api.apply_operational_effects(state_effects, state_path=state_path)
        legacy_integrated = (
            api._latest_ingest_event(capture_id=item["capture_id"], raw_path=item["raw_path"], log_path=log_path)
            == "integrated"
        )
        unchanged_in_state = (
            prior_state is not None
            and prior_state.get("content_hash") == state_item["content_hash"]
            and prior_state.get("event") in {"processed", "skipped", "updated"}
        )
        unchanged_legacy_item = prior_state is None and legacy_integrated
        if (unchanged_in_state or unchanged_legacy_item) and not retry_failed:
            state_effects = api.OperationalEffects.state_event({"event": "skipped", **state_payload, "reason": "unchanged"})
            effects.extend(state_effects)
            api.apply_operational_effects(state_effects, state_path=state_path)
            result["skipped"].append(item)
            continue

        last_error: Exception | None = None
        for _ in range(3):
            try:
                if active_handler is api._default_integration_handler:
                    handler_result = active_handler(
                        item["capture_id"],
                        raw_abspath,
                    )
                else:
                    handler_result = active_handler(item["capture_id"], raw_abspath)
                handler_effects = getattr(handler_result, "effects", None)
                if handler_effects is not None:
                    effects.extend(handler_effects)
                jsonl_effects = api.OperationalEffects.jsonl_event(
                    {"event": "integrated", "capture_id": item["capture_id"], "raw_path": item["raw_path"]}
                )
                effects.extend(jsonl_effects)
                api.apply_operational_effects(jsonl_effects, log_path=log_path)
                if prior_state is not None and prior_state.get("content_hash") != state_item["content_hash"]:
                    state_effects = api.OperationalEffects.state_event({"event": "updated", **state_payload})
                    effects.extend(state_effects)
                    api.apply_operational_effects(state_effects, state_path=state_path)
                state_effects = api.OperationalEffects.state_event({"event": "processed", **state_payload})
                effects.extend(state_effects)
                api.apply_operational_effects(state_effects, state_path=state_path)
                api.debug_print(f"Integrated {item['capture_id']}", enabled=debug, stream=debug_stream)
                result["integrated"].append(item)
                last_error = None
                break
            except Exception as exc:
                last_error = exc
        if last_error is not None:
            error_summary = str(last_error)
            jsonl_effects = api.OperationalEffects.jsonl_event(
                {
                    "event": "integrate_failed",
                    "capture_id": item["capture_id"],
                    "raw_path": item["raw_path"],
                    "error": error_summary,
                }
            )
            effects.extend(jsonl_effects)
            api.apply_operational_effects(jsonl_effects, log_path=log_path)
            state_effects = api.OperationalEffects.state_event(
                {
                    "event": "failed",
                    **state_payload,
                    "stage": "wiki_ingest",
                    "error_summary": error_summary,
                }
            )
            effects.extend(state_effects)
            api.apply_operational_effects(state_effects, state_path=state_path)
            result["failed"].append(item)
            api.debug_print(f"Integration failed for {item['capture_id']}: {last_error}", enabled=debug, stream=debug_stream)

    result["effects"] = effects.to_dict()
    return result
