#!/usr/bin/env python3
"""Sync a public-safe mirror of this vault into a separate repo directory.

The private repo remains canonical. This script copies only publishable files
and excludes private notes, raw captures, local caches, and git metadata.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


DEFAULT_EXCLUDES = {
    ".git",
    ".gitignore",
    ".DS_Store",
    "__pycache__",
    ".pytest_cache",
    ".wiki-bootstrap-cache",
    "raw",
    "wiki",
    "ingest.log",
    "log.jsonl",
    "state",
    "AGENTS.md",
}


def should_skip(path: Path) -> bool:
    parts = set(path.parts)
    return any(part in DEFAULT_EXCLUDES for part in parts)


def sync_tree(source_root: Path, dest_root: Path) -> None:
    dest_root.mkdir(parents=True, exist_ok=True)

    expected_paths: set[Path] = set()

    for path in source_root.rglob("*"):
        if should_skip(path.relative_to(source_root)):
            continue
        if path.is_dir():
            continue

        relative = path.relative_to(source_root)
        expected_paths.add(relative)
        target = dest_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)

    for path in sorted(dest_root.rglob("*"), key=lambda item: len(item.parts), reverse=True):
        relative = path.relative_to(dest_root)
        if relative.parts and relative.parts[0] == ".git":
            continue
        if path.is_file() and relative not in expected_paths:
            path.unlink()
        elif path.is_dir() and not any(path.iterdir()):
            path.rmdir()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        default=Path(__file__).resolve().parent.parent,
        type=Path,
        help="Private canonical repo root.",
    )
    parser.add_argument(
        "--dest",
        required=True,
        type=Path,
        help="Public repo root to receive the mirrored files.",
    )
    args = parser.parse_args()

    sync_tree(args.source.resolve(), args.dest.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
