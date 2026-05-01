from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, TextIO
import uuid

if TYPE_CHECKING:
    from types import ModuleType


def validate_adoptable_raw(api: ModuleType, path: Path, capture_id: str) -> None:
    classification = api.classify_raw_candidate(path, capture_id)
    if classification != "matching_valid":
        raise ValueError(f"raw file is not adoption-valid for capture_id {capture_id}: {path}")


def compute_raw_target(api: ModuleType, title: str, capture_id: str) -> Path:
    return api.RAW_ROOT / f"{api.raw_file_slug(title)}-{capture_id}.md"


def classify_raw_candidate(api: ModuleType, path: Path, capture_id: str) -> str | None:
    text = path.read_text(encoding="utf-8")
    try:
        frontmatter, body, has_frontmatter = api.split_frontmatter(text)
    except ValueError:
        return "unparseable_unknown"

    if not has_frontmatter:
        return "unparseable_unknown"

    candidate_capture_id = frontmatter.get("capture_id")
    if not isinstance(candidate_capture_id, str) or not candidate_capture_id:
        return "unparseable_unknown"
    if candidate_capture_id != capture_id:
        return None
    if not body.strip():
        return "matching_invalid"
    return "matching_valid"


def discover_raw_candidates(api: ModuleType, capture_id: str) -> object:
    matching_valid: list[Path] = []
    matching_invalid: list[Path] = []
    unparseable_unknown: list[Path] = []

    if not api.RAW_ROOT.exists():
        return api.RawCandidateSet(matching_valid, matching_invalid, unparseable_unknown)

    for path in sorted(api.RAW_ROOT.rglob("*.md")):
        if path.is_symlink() or not path.is_file():
            continue
        classification = api.classify_raw_candidate(path, capture_id)
        if classification == "matching_valid":
            matching_valid.append(path)
        elif classification == "matching_invalid":
            matching_invalid.append(path)
        elif classification == "unparseable_unknown":
            unparseable_unknown.append(path)

    return api.RawCandidateSet(matching_valid, matching_invalid, unparseable_unknown)


def resolve_raw_path_for_capture(api: ModuleType, *, title: str, capture_id: str) -> tuple[Path, str, int]:
    candidates = api.discover_raw_candidates(capture_id)
    valid_count = len(candidates.matching_valid)
    invalid_count = len(candidates.matching_invalid)
    target_path = api.compute_raw_target(title, capture_id)

    if valid_count == 1 and invalid_count == 0:
        return candidates.matching_valid[0], "adopted", 1
    if valid_count > 1 or (valid_count == 1 and invalid_count > 0):
        raise ValueError("raw_identity_ambiguous")
    if valid_count == 0 and invalid_count > 0:
        raise ValueError("raw_invalid_existing")
    if target_path.exists():
        raise ValueError("raw_create_failed")
    return target_path, "created", 0


def increment_ingest_attempts(api: ModuleType, note: object) -> int:
    updated = dict(note.frontmatter)
    updated["ingest_attempts"] = note.ingest_attempts + 1
    api.write_source_note(note.path, updated, note.body)
    return int(updated["ingest_attempts"])


def _record_retry_gated_export_failure(
    api: ModuleType,
    *,
    note: object,
    capture_id: str,
    error: str,
    failure_class: str,
    log_path: Path | None,
) -> None:
    attempts = api.increment_ingest_attempts(note)
    api.append_jsonl_event(
        {
            "event": "export_failed",
            "capture_id": capture_id,
            "source_filename": note.filename,
            "error": error,
            "failure_class": failure_class,
            "counted_against_retry_gate": True,
            "ingest_attempts": attempts,
        },
        log_path=log_path,
    )


def inject_capture_id(api: ModuleType, note: object) -> object:
    updated = dict(note.frontmatter)
    updated["capture_id"] = str(uuid.uuid4())
    api.write_source_note(note.path, updated, note.body)
    return api.read_source_note(note.path)


def rename_processed(api: ModuleType, path: Path) -> Path:
    target = path.with_name(f"{api.MARKER_PREFIX}{path.name}")
    if target.exists():
        raise FileExistsError(f"processed target already exists: {target.name}")
    path.rename(target)
    return target


def discover_capture_candidates(api: ModuleType, capture_root: Path) -> list[Path]:
    candidates = [
        path
        for path in capture_root.iterdir()
        if path.is_file() and path.suffix == ".md" and not path.name.startswith(api.MARKER_PREFIX)
    ]
    return sorted(candidates, key=lambda path: path.stat().st_mtime)


def discover_processed_capture_candidates(api: ModuleType, capture_root: Path) -> list[Path]:
    candidates = [
        path
        for path in capture_root.iterdir()
        if path.is_file() and path.suffix == ".md" and path.name.startswith(api.MARKER_PREFIX)
    ]
    return sorted(candidates, key=lambda path: path.stat().st_mtime)


def discover_source_capture_id_counts(api: ModuleType, candidate_paths: list[Path]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for path in candidate_paths:
        try:
            note = api.read_source_note(path)
        except Exception:
            continue
        if note.capture_id is None:
            continue
        counts[note.capture_id] = counts.get(note.capture_id, 0) + 1
    return counts


def capture_ingest(
    api: ModuleType,
    *,
    capture_root: Path,
    debug: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
    retry_failed: bool = False,
    log_path: Path | None = None,
    debug_stream: TextIO | None = None,
) -> object:
    result = {"new_exports": [], "errors": []}
    if dry_run:
        result["planned_exports"] = []
        result["audit"] = []
        result["failed_risk"] = []
    processed = 0
    candidate_paths = api.discover_capture_candidates(capture_root)
    processed_candidate_paths = api.discover_processed_capture_candidates(capture_root)
    source_capture_id_counts = api.discover_source_capture_id_counts(candidate_paths)

    for path in candidate_paths:
        if limit is not None and processed >= limit:
            break

        try:
            note = api.read_source_note(path)
        except Exception as exc:
            result["errors"].append({"filename": path.name, "capture_id": None, "reason": f"read_failed: {exc}"})
            continue

        if note.ingest_attempts >= 3 and not retry_failed:
            capture_id = note.capture_id
            if dry_run:
                result["audit"].append(
                    {
                        "event": "skipped",
                        "capture_id": capture_id,
                        "filename": note.filename,
                        "reason": "ingest_attempts_over_threshold",
                    }
                )
                api.debug_print(f"Would skip {note.filename}: ingest attempts over threshold", enabled=debug, stream=debug_stream)
                continue
            if capture_id and not api._has_logged_event(
                "skipped_over_threshold",
                capture_id=capture_id,
                filename=note.filename,
                log_path=log_path or api.JSONL_LOG_PATH,
            ):
                api.append_jsonl_event(
                    {
                        "event": "skipped_over_threshold",
                        "capture_id": capture_id,
                        "filename": note.filename,
                        "ingest_attempts": note.ingest_attempts,
                    },
                    log_path=log_path or api.JSONL_LOG_PATH,
                )
            continue

        title = api.clean_title_from_filename(note.filename)

        if api.source_note_body_is_blank(note) and api.is_placeholder_title(title):
            if dry_run:
                result["audit"].append({"event": "processed", "filename": note.filename, "action": "would_delete_empty"})
                api.debug_print(f"Would delete empty note {note.filename}", enabled=debug, stream=debug_stream)
                processed += 1
                continue
            note.path.unlink()
            payload: dict[str, object] = {
                "event": "empty_deleted",
                "filename": note.filename,
                "reason": "empty_after_trim",
            }
            if note.capture_id:
                payload["capture_id"] = note.capture_id
            api.append_jsonl_event(payload, log_path=log_path)
            processed += 1
            continue

        if note.capture_id is None:
            if dry_run:
                result["audit"].append({"event": "processed", "filename": note.filename, "action": "would_inject_capture_id"})
                api.debug_print(f"Would inject capture_id into {note.filename}", enabled=debug, stream=debug_stream)
                processed += 1
                continue
            try:
                note = api.inject_capture_id(note)
            except Exception as exc:
                api.append_jsonl_event(
                    {"event": "injection_failed", "filename": note.filename, "error": str(exc)},
                    log_path=log_path,
                )
                result["errors"].append(
                    {"filename": note.filename, "capture_id": None, "reason": f"injection_failed: {exc}"}
                )
                continue

        capture_id = note.capture_id
        assert capture_id is not None
        if dry_run:
            result["audit"].append({"event": "discovered", "capture_id": capture_id, "filename": note.filename})
        else:
            api.append_jsonl_event({"event": "discovered", "capture_id": capture_id, "filename": note.filename}, log_path=log_path)
        created_at, used_mtime_fallback = api.resolve_created_at(note)
        if used_mtime_fallback:
            if dry_run:
                result["audit"].append({"event": "updated", "capture_id": capture_id, "filename": note.filename, "action": "would_use_mtime_created_at"})
            else:
                api.append_jsonl_event(
                    {"event": "created_at_mtime_fallback", "capture_id": capture_id, "filename": note.filename},
                    log_path=log_path,
                )

        if source_capture_id_counts.get(capture_id, 0) > 1:
            if dry_run:
                result["failed_risk"].append(
                    {"filename": note.filename, "capture_id": capture_id, "reason": "source_identity_ambiguous"}
                )
                processed += 1
                continue
            try:
                api._record_retry_gated_export_failure(
                    note=note,
                    capture_id=capture_id,
                    error="source identity ambiguous",
                    failure_class="source_identity_ambiguous",
                    log_path=log_path,
                )
            except Exception:
                pass
            result["errors"].append(
                {"filename": note.filename, "capture_id": capture_id, "reason": "source_identity_ambiguous"}
            )
            processed += 1
            continue

        try:
            raw_target, export_mode, candidate_count = api.resolve_raw_path_for_capture(title=title, capture_id=capture_id)
        except Exception as exc:
            if dry_run:
                result["failed_risk"].append({"filename": note.filename, "capture_id": capture_id, "reason": str(exc)})
                processed += 1
                continue
            try:
                api._record_retry_gated_export_failure(
                    note=note,
                    capture_id=capture_id,
                    error=str(exc),
                    failure_class=str(exc),
                    log_path=log_path,
                )
            except Exception:
                pass
            result["errors"].append({"filename": note.filename, "capture_id": capture_id, "reason": str(exc)})
            processed += 1
            continue

        raw_repo_path = api.normalize_repo_path(raw_target)
        export_item = {"capture_id": capture_id, "raw_path": raw_repo_path}

        if dry_run:
            result["planned_exports"].append(export_item)
            result["audit"].append(
                {
                    "event": "processed",
                    "capture_id": capture_id,
                    "filename": note.filename,
                    "raw_path": raw_repo_path,
                    "action": "would_export",
                    "mode": export_mode,
                    "candidate_count": candidate_count,
                }
            )
            api.debug_print(f"Would export {note.filename} -> {raw_repo_path}", enabled=debug, stream=debug_stream)
            processed += 1
            continue

        try:
            if export_mode == "adopted":
                api.validate_adoptable_raw(raw_target, capture_id)
            else:
                if raw_target.exists():
                    raise ValueError("raw_create_failed")
                api.atomic_write_text(
                    raw_target,
                    api.render_raw_file(
                        capture_id=capture_id,
                        title=title,
                        created_at=created_at,
                        source_file=title + ".md",
                        body=note.body,
                    ),
                )
            api.append_jsonl_event(
                {
                    "event": "exported_to_raw",
                    "capture_id": capture_id,
                    "raw_path": raw_repo_path,
                    "mode": export_mode,
                    "candidate_count": candidate_count,
                },
                log_path=log_path,
            )
        except Exception as exc:
            failure_class = "raw_create_failed" if export_mode == "created" else str(exc)
            try:
                api._record_retry_gated_export_failure(
                    note=note,
                    capture_id=capture_id,
                    error=str(exc),
                    failure_class=failure_class,
                    log_path=log_path,
                )
            except Exception:
                pass
            result["errors"].append({"filename": note.filename, "capture_id": capture_id, "reason": failure_class})
            processed += 1
            continue

        try:
            renamed = api.rename_processed(note.path)
            api.append_jsonl_event(
                {"event": "marked_processed", "capture_id": capture_id, "filename": renamed.name},
                log_path=log_path,
            )
            result["new_exports"].append(export_item)
        except Exception as exc:
            try:
                api._record_retry_gated_export_failure(
                    note=note,
                    capture_id=capture_id,
                    error=str(exc),
                    failure_class="source_rename_failed",
                    log_path=log_path,
                )
            except Exception:
                pass
            result["errors"].append({"filename": note.filename, "capture_id": capture_id, "reason": "source_rename_failed"})
        processed += 1

    if not dry_run:
        for path in processed_candidate_paths:
            try:
                note = api.read_source_note(path)
            except Exception:
                continue
            capture_id = note.capture_id
            if capture_id is None:
                continue
            if api.discover_raw_candidates(capture_id).matching_valid:
                continue
            api.append_jsonl_event(
                {
                    "event": "export_failed",
                    "capture_id": capture_id,
                    "source_filename": note.filename,
                    "error": "processed note has no adoption-valid raw",
                    "failure_class": "processed_without_raw",
                    "counted_against_retry_gate": False,
                },
                log_path=log_path,
            )

    return result
