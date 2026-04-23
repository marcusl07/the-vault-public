#!/usr/bin/env python3
"""Publish the private canonical repo and the filtered public mirror.

Workflow:
1. Sync publishable files into the public repo.
2. Commit and push the private repo.
3. Commit and push the public repo.

The private repo remains the source of truth. The public repo is regenerated
from it and should never receive raw/ or private wiki content.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.sync_public_mirror import sync_tree

DEFAULT_PUBLIC_REPO = Path(__file__).resolve().parents[2] / "The Vault Public"


def run_git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        text=True,
        capture_output=True,
    )


def git_has_changes(repo: Path) -> bool:
    result = subprocess.run(
        ["git", "-C", str(repo), "status", "--porcelain"],
        check=True,
        text=True,
        capture_output=True,
    )
    return bool(result.stdout.strip())


def commit_and_push(repo: Path, message: str) -> None:
    if not git_has_changes(repo):
        return
    run_git(repo, "add", "-A")
    run_git(repo, "commit", "-m", message)
    run_git(repo, "push", "-u", "origin", "HEAD")


def ensure_git_repo(repo: Path) -> None:
    repo.mkdir(parents=True, exist_ok=True)
    git_dir = repo / ".git"
    if not git_dir.exists():
        subprocess.run(["git", "init", "-b", "main", str(repo)], check=True, text=True)


def set_remote(repo: Path, remote_url: str) -> None:
    existing = subprocess.run(
        ["git", "-C", str(repo), "remote", "get-url", "origin"],
        text=True,
        capture_output=True,
    )
    if existing.returncode == 0:
        run_git(repo, "remote", "set-url", "origin", remote_url)
    else:
        run_git(repo, "remote", "add", "origin", remote_url)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--public-repo",
        default=DEFAULT_PUBLIC_REPO,
        type=Path,
        help="Path to the separate public repo checkout.",
    )
    parser.add_argument(
        "--public-remote",
        help="Optional git remote URL for the public repo. If set, origin is created or updated.",
    )
    parser.add_argument(
        "--message",
        default="Publish private/public sync",
        help="Commit message to use in both repos.",
    )
    args = parser.parse_args()

    private_repo = Path(__file__).resolve().parent.parent
    public_repo = args.public_repo.resolve()

    ensure_git_repo(public_repo)
    if args.public_remote:
        set_remote(public_repo, args.public_remote)

    sync_tree(private_repo, public_repo)
    commit_and_push(private_repo, args.message)
    commit_and_push(public_repo, args.message)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
