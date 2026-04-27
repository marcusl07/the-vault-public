from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import UTC, datetime
import errno
import fcntl
import json
import os
from pathlib import Path
import sys
from typing import TYPE_CHECKING, Iterator, TextIO
import uuid

if TYPE_CHECKING:
    from types import ModuleType


@contextmanager
def pipeline_lock(api: ModuleType, capture_root: Path) -> Iterator[None]:
    def parse_lock_timestamp(value: object) -> datetime:
        if not isinstance(value, str):
            raise ValueError("lock timestamp must be a string")
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)

    def read_lock_metadata(handle: TextIO) -> dict[str, object]:
        handle.seek(0)
        raw = handle.read().strip()
        if not raw:
            raise ValueError("lock metadata is empty")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("lock metadata must be an object")
        if payload.get("version") != 1:
            raise ValueError("unsupported lock metadata version")
        if not isinstance(payload.get("owner_token"), str) or not payload["owner_token"]:
            raise ValueError("lock metadata missing owner token")
        if not isinstance(payload.get("pid"), int):
            raise ValueError("lock metadata missing pid")
        if not isinstance(payload.get("capture_root"), str) or not payload["capture_root"]:
            raise ValueError("lock metadata missing capture root")
        parse_lock_timestamp(payload.get("acquired_at"))
        return payload

    def write_lock_metadata(handle: TextIO, metadata: dict[str, object]) -> None:
        handle.seek(0)
        handle.write(json.dumps(metadata, ensure_ascii=False, separators=(",", ":")) + "\n")
        handle.truncate()
        handle.flush()
        os.fsync(handle.fileno())

    def acquire_new_lock_handle(path: Path) -> TextIO:
        replacement_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        replacement_handle = replacement_path.open("w+", encoding="utf-8")
        try:
            fcntl.flock(replacement_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.replace(replacement_path, path)
            return replacement_handle
        except Exception:
            replacement_handle.close()
            replacement_path.unlink(missing_ok=True)
            raise

    capture_root.mkdir(parents=True, exist_ok=True)
    lock_path = capture_root / "pipeline.lock"
    stale_claim_path = capture_root / "pipeline.lock.stale-claim"
    metadata = {
        "version": 1,
        "owner_token": str(uuid.uuid4()),
        "pid": os.getpid(),
        "acquired_at": api.utc_timestamp(),
        "capture_root": str(capture_root),
    }
    handle = lock_path.open("a+", encoding="utf-8")
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in {errno.EACCES, errno.EAGAIN}:
                try:
                    existing_metadata = read_lock_metadata(handle)
                except (OSError, ValueError, json.JSONDecodeError):
                    raise RuntimeError(f"pipeline lock is already held: {lock_path}") from exc

                acquired_at = parse_lock_timestamp(existing_metadata["acquired_at"])
                if (datetime.now(UTC) - acquired_at).total_seconds() <= 30 * 60:
                    raise RuntimeError(f"pipeline lock is already held: {lock_path}") from exc

                try:
                    claim_fd = os.open(stale_claim_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
                except FileExistsError as claim_exc:
                    raise RuntimeError(f"pipeline lock is already held: {lock_path}") from claim_exc

                os.close(claim_fd)
                handle.close()
                handle = acquire_new_lock_handle(lock_path)
            else:
                raise
        write_lock_metadata(handle, metadata)
        yield
    finally:
        try:
            if not handle.closed:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            if not handle.closed:
                handle.close()
            stale_claim_path.unlink(missing_ok=True)


def run_vault_pipeline(
    api: ModuleType,
    *,
    capture_root: Path,
    debug: bool = False,
    dry_run: bool = False,
    limit: int | None = None,
    retry_failed: bool = False,
    page_resynthesis_on_touch: bool = False,
    debug_stream: TextIO | None = None,
) -> object:
    with api.pipeline_lock(capture_root):
        api.debug_print("Running capture ingest stage", enabled=debug, stream=debug_stream)
        try:
            capture_result = api.capture_ingest(
                capture_root=capture_root,
                debug=debug,
                dry_run=dry_run,
                limit=limit,
                retry_failed=retry_failed,
                debug_stream=debug_stream,
            )
        except Exception as exc:
            api.append_jsonl_event(
                {"event": "capture_stage_crashed", "error": str(exc), "capture_root": str(capture_root)},
                log_path=api.JSONL_LOG_PATH,
            )
            raise

        wiki_ingest = None
        if not dry_run:
            api.debug_print("Running wiki ingest stage", enabled=debug, stream=debug_stream)
            wiki_ingest = api.ingest_raw_notes(
                capture_result["new_exports"],
                retry_failed=retry_failed,
                page_resynthesis_on_touch=page_resynthesis_on_touch,
                debug=debug,
                debug_stream=debug_stream,
            )

        return {"capture_ingest": capture_result, "wiki_ingest": wiki_ingest}


def build_capture_parser(api: ModuleType) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export Obsidian capture notes into raw markdown files.")
    parser.add_argument("--debug", action="store_true", help="Emit human-readable debug output to stderr.")
    parser.add_argument("--dry-run", action="store_true", help="Report actions without mutating state.")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N eligible notes in this run.")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Include notes whose source frontmatter has ingest_attempts >= 3.",
    )
    parser.add_argument(
        "--capture-root",
        type=Path,
        default=api.DEFAULT_CAPTURE_ROOT,
        help="Top-level Obsidian capture vault root to scan.",
    )
    return parser


def build_ingest_parser(api: ModuleType) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ingest raw notes into the wiki.")
    parser.add_argument("--debug", action="store_true", help="Emit human-readable debug output to stderr.")
    parser.add_argument("--retry-failed", action="store_true", help="Re-run notes already marked integrated.")
    parser.add_argument(
        "--page-resynthesis-on-touch",
        action="store_true",
        help="Re-synthesize touched existing wiki pages from accumulated notes and sources during ingest.",
    )
    parser.add_argument(
        "--items-json",
        default="-",
        help="JSON array of {capture_id, raw_path}; use '-' to read from stdin.",
    )
    return parser


def build_run_parser(api: ModuleType) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the full Vault capture and wiki ingest pipeline.")
    parser.add_argument("--debug", action="store_true", help="Emit human-readable debug output to stderr.")
    parser.add_argument("--dry-run", action="store_true", help="Report actions without mutating state.")
    parser.add_argument("--limit", type=int, default=None, help="Process at most N eligible notes in this run.")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Include notes whose source frontmatter has ingest_attempts >= 3.",
    )
    parser.add_argument(
        "--page-resynthesis-on-touch",
        action="store_true",
        help="Re-synthesize touched existing wiki pages from accumulated notes and sources during ingest.",
    )
    parser.add_argument(
        "--capture-root",
        type=Path,
        default=api.DEFAULT_CAPTURE_ROOT,
        help="Top-level Obsidian capture vault root to scan.",
    )
    return parser


def capture_main(api: ModuleType, argv: list[str] | None = None) -> int:
    args = api.build_capture_parser().parse_args(argv)
    result = api.capture_ingest(
        capture_root=args.capture_root,
        debug=args.debug,
        dry_run=args.dry_run,
        limit=args.limit,
        retry_failed=args.retry_failed,
    )
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


def ingest_main(api: ModuleType, argv: list[str] | None = None) -> int:
    args = api.build_ingest_parser().parse_args(argv)
    items_json = sys.stdin.read() if args.items_json == "-" else args.items_json
    items = json.loads(items_json) if items_json.strip() else []
    result = api.ingest_raw_notes(
        items,
        retry_failed=args.retry_failed,
        page_resynthesis_on_touch=args.page_resynthesis_on_touch,
        debug=args.debug,
    )
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


def run_main(api: ModuleType, argv: list[str] | None = None) -> int:
    args = api.build_run_parser().parse_args(argv)
    result = api.run_vault_pipeline(
        capture_root=args.capture_root,
        debug=args.debug,
        dry_run=args.dry_run,
        limit=args.limit,
        retry_failed=args.retry_failed,
        page_resynthesis_on_touch=args.page_resynthesis_on_touch,
    )
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0
