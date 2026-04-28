from __future__ import annotations

from collections import Counter
from pathlib import Path
import json
import shutil
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from types import ModuleType


def stable_json_dumps(api: ModuleType, payload: object) -> str:
    return json.dumps(payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"))


def fingerprint_payload(api: ModuleType, payload: object) -> str:
    return api.hashlib.sha256(stable_json_dumps(api, payload).encode("utf-8")).hexdigest()


def cache_key(api: ModuleType, value: str) -> str:
    return api.hashlib.sha1(value.encode("utf-8")).hexdigest()


def atomic_write_json(api: ModuleType, path: Path, payload: object) -> None:
    api.atomic_write_text(path, stable_json_dumps(api, payload) + "\n")


def read_json_file(api: ModuleType, path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def note_cache_path(api: ModuleType, source_path: str) -> Path:
    return api.CACHE_NOTES_ROOT / f"{cache_key(api, source_path)}.json"


def page_cache_path(api: ModuleType, slug: str) -> Path:
    return api.CACHE_PAGES_ROOT / f"{cache_key(api, slug)}.json"


def update_manifest(api: ModuleType, **fields: object) -> None:
    current = read_json_file(api, api.CACHE_MANIFEST_PATH) or {}
    current.update(fields)
    current["updated_at"] = api.TODAY
    atomic_write_json(api, api.CACHE_MANIFEST_PATH, current)


def source_record_to_cache_dict(api: ModuleType, source: object) -> dict[str, object]:
    return {
        "label": source.label,
        "path": source.path,
        "status": source.status,
        "cleaned_text": source.cleaned_text,
        "fetched_summary": source.fetched_summary,
        "detected_url": source.detected_url,
        "source_kind": source.source_kind,
        "source_id": source.source_id,
        "created_at": source.created_at,
        "title": source.title,
        "external_url": source.external_url,
        "provenance_pointer": source.provenance_pointer,
        "tags": sorted(source.tags),
        "excluded_from_body": source.excluded_from_body,
    }


def source_record_from_cache_dict(api: ModuleType, payload: dict[str, object]) -> object:
    return api.SourceRecord(
        label=str(payload["label"]),
        path=str(payload["path"]),
        status=str(payload["status"]),
        raw_content="",
        cleaned_text=str(payload.get("cleaned_text", "")),
        fetched_summary=str(payload["fetched_summary"]) if payload.get("fetched_summary") is not None else None,
        detected_url=str(payload["detected_url"]) if payload.get("detected_url") is not None else None,
        source_kind=str(payload.get("source_kind", "capture")),
        source_id=str(payload["source_id"]) if payload.get("source_id") is not None else None,
        created_at=str(payload["created_at"]) if payload.get("created_at") is not None else None,
        title=str(payload["title"]) if payload.get("title") is not None else None,
        external_url=str(payload["external_url"]) if payload.get("external_url") is not None else None,
        provenance_pointer=(
            str(payload["provenance_pointer"]) if payload.get("provenance_pointer") is not None else None
        ),
        tags=set(str(tag) for tag in payload.get("tags", [])),
        excluded_from_body=bool(payload.get("excluded_from_body", False)),
    )


def build_note_fingerprint(api: ModuleType, path: Path, content: str, fetch_urls_enabled: bool) -> str:
    return fingerprint_payload(
        api,
        {
            "path": path.relative_to(api.ROOT).as_posix(),
            "content": content,
            "fetch_urls": fetch_urls_enabled,
        },
    )


def build_note_cache_entry(
    api: ModuleType,
    *,
    fingerprint: str,
    title: str,
    source_record: object,
    note_text: str,
    page_assignments: list[tuple[str, str]],
    skipped: bool,
) -> dict[str, object]:
    return {
        "fingerprint": fingerprint,
        "title": title,
        "source_record": source_record_to_cache_dict(api, source_record),
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


def build_page_fingerprint(api: ModuleType, page: object) -> str:
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
        api,
        {
            "slug": page.slug,
            "title": page.title,
            "page_type": page.page_type,
            "sources": serialized_sources,
        },
    )


def apply_note_cache_entry_to_pages(
    api: ModuleType,
    *,
    pages: dict[str, object],
    cache_entry: dict[str, object],
) -> None:
    if bool(cache_entry.get("skipped", False)):
        return

    source_payload = cache_entry.get("source_record")
    if not isinstance(source_payload, dict):
        raise ValueError("Cached note entry missing source_record.")

    source_record = source_record_from_cache_dict(api, source_payload)
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
        api.ensure_supporting_pages(pages, slug, title, seed_kind)

    owner_slug: str | None = None
    for preferred_seed_kind in ("title", "model", "query"):
        for slug, seed_kind in unique_page_slugs:
            if seed_kind == preferred_seed_kind:
                owner_slug = slug
                break
        if owner_slug is not None:
            break
    if owner_slug is None and unique_page_slugs:
        owner_slug = unique_page_slugs[0][0]
    if owner_slug is not None:
        owner_seed_kind = next(seed_kind for slug, seed_kind in unique_page_slugs if slug == owner_slug)
        api.add_page_note(
            pages=pages,
            slug=owner_slug,
            title=api.page_title(owner_slug),
            page_type=api.classify_page(owner_slug, title, owner_seed_kind),
            summary_hint=title,
            note_text=note_text,
            source_label=source_record.label,
            source_path=source_record.path,
            source_status=source_record.status,
            seed_kind=owner_seed_kind,
        )
        pages[owner_slug].sources[source_record.path] = source_record

    slugs_only = [slug for slug, _seed_kind in unique_page_slugs]
    for slug in slugs_only:
        for other in slugs_only:
            if other != slug:
                pages[slug].connections[other] += 1


def accumulate_url_stats(api: ModuleType, url_stats: Counter, source: object, fetch_urls_enabled: bool) -> None:
    if not fetch_urls_enabled:
        return
    if source.detected_url:
        url_stats["url_notes"] += 1
        url_stats[source.status] += 1


def stage_rendered_wiki(
    api: ModuleType,
    *,
    pages: dict[str, object],
    existing_log_text: str,
    bootstrap_entry: str,
) -> Path:
    if api.RENDER_STAGE_ROOT.exists():
        shutil.rmtree(api.RENDER_STAGE_ROOT)
    if api.RENDER_BACKUP_ROOT.exists():
        shutil.rmtree(api.RENDER_BACKUP_ROOT)

    api.RENDER_STAGE_ROOT.mkdir(parents=True, exist_ok=True)
    for page in pages.values():
        target = api.RENDER_STAGE_ROOT / f"{page.slug}.md"
        api.atomic_write_text(target, api.render_page(page))

    api.atomic_write_text(api.RENDER_STAGE_ROOT / "index.md", api.render_index(pages))
    api.atomic_write_text(api.RENDER_STAGE_ROOT / api.CATALOG_PATH, api.render_catalog(pages))

    existing_lines = [
        line for line in existing_log_text.splitlines() if not line.startswith(f"## [{api.TODAY}] bootstrap |")
    ]
    staged_log = "\n".join(existing_lines).rstrip() + "\n\n" + bootstrap_entry
    api.atomic_write_text(api.RENDER_STAGE_ROOT / "log.md", staged_log)
    return api.RENDER_STAGE_ROOT


def append_wiki_query_log(api: ModuleType, summary: str) -> None:
    log_path = api.WIKI_ROOT / "log.md"
    existing = log_path.read_text(encoding="utf-8") if log_path.exists() else "# Wiki Log\n"
    entry = f"## [{api.TODAY}] query | {summary}"
    api.atomic_write_text(log_path, existing.rstrip() + "\n\n" + entry + "\n")


def swap_rendered_wiki(api: ModuleType, stage_dir: Path) -> None:
    backup_dir = api.RENDER_BACKUP_ROOT
    if backup_dir.exists():
        shutil.rmtree(backup_dir)
    if api.WIKI_ROOT.exists():
        api.WIKI_ROOT.rename(backup_dir)
    try:
        stage_dir.rename(api.WIKI_ROOT)
    except Exception:
        if backup_dir.exists() and not api.WIKI_ROOT.exists():
            backup_dir.rename(api.WIKI_ROOT)
        raise
    if backup_dir.exists():
        shutil.rmtree(backup_dir)
