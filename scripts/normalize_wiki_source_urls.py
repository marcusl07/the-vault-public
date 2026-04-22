from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import sys
from typing import Iterable

try:
    from scripts import vault_pipeline as vp
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    import vault_pipeline as vp


ROOT = Path(__file__).resolve().parent.parent
WIKI_ROOT = ROOT / "wiki"

SOURCE_LINE_RE = re.compile(r"^(?P<indent>\s*-\s)\[(?P<label>[^\]]+)\]\((?P<target>[^)]+)\)(?P<suffix>.*)$")
MARKDOWN_URL_RE = re.compile(r"\[[^\]]*?\]\((https?://[^)\s>]+)\)")
HTML_HREF_RE = re.compile(r"""href=["'](https?://[^"'<>]+)["']""")
BARE_URL_RE = re.compile(r"(?<![\w@])https?://[^\s<>)\]]+")


@dataclass(frozen=True)
class Rewrite:
    wiki_path: Path
    line_index: int
    before: str
    after: str
    raw_target: str
    chosen_url: str


@dataclass(frozen=True)
class Skip:
    wiki_path: Path
    line_index: int
    reason: str
    raw_target: str


def iter_wiki_files() -> Iterable[Path]:
    yield from sorted(WIKI_ROOT.glob("*.md"))


def extract_urls(raw_text: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    def add(candidate: str) -> None:
        normalized = candidate.strip().rstrip(".,;:")
        if normalized and normalized not in seen:
            seen.add(normalized)
            urls.append(normalized)

    for pattern in (MARKDOWN_URL_RE, HTML_HREF_RE, BARE_URL_RE):
        for match in pattern.findall(raw_text):
            add(match)

    return urls


def resolve_raw_target(wiki_path: Path, target: str) -> Path | None:
    candidate = (wiki_path.parent / target).resolve()
    try:
        candidate.relative_to(ROOT)
    except ValueError:
        return None
    if not candidate.is_file():
        return None
    return candidate


def normalize_visible_label(url: str) -> str:
    return url


def plan_rewrites() -> tuple[list[Rewrite], list[Skip]]:
    rewrites: list[Rewrite] = []
    skips: list[Skip] = []

    for wiki_path in iter_wiki_files():
        lines = wiki_path.read_text(encoding="utf-8").splitlines()
        in_sources = False

        for index, line in enumerate(lines):
            if line.startswith("## "):
                in_sources = line.strip() == "## Sources"
                continue
            if not in_sources:
                continue
            if line.startswith("## "):
                in_sources = False
                continue
            if not line.strip():
                continue

            match = SOURCE_LINE_RE.match(line)
            if not match:
                continue

            target = match.group("target")
            if "/raw/" not in target and not target.startswith("raw/") and not target.startswith("../raw/"):
                continue

            raw_path = resolve_raw_target(wiki_path, target)
            if raw_path is None:
                skips.append(Skip(wiki_path, index + 1, "missing raw target", target))
                continue

            raw_text = raw_path.read_text(encoding="utf-8", errors="ignore")
            urls = extract_urls(raw_text)
            if not urls:
                skips.append(Skip(wiki_path, index + 1, "no URL in raw note", target))
                continue
            if len(urls) > 1:
                skips.append(Skip(wiki_path, index + 1, "multiple URLs in raw note", target))
                continue

            chosen_url = urls[0]
            desired_label = normalize_visible_label(chosen_url)
            current_label = match.group("label")
            if current_label == desired_label:
                continue

            after = f"{match.group('indent')}[{desired_label}]({target}){match.group('suffix')}"
            rewrites.append(
                Rewrite(
                    wiki_path=wiki_path,
                    line_index=index,
                    before=line,
                    after=after,
                    raw_target=target,
                    chosen_url=chosen_url,
                )
            )

    return rewrites, skips


def apply_rewrites(rewrites: list[Rewrite]) -> None:
    updates: dict[Path, list[Rewrite]] = {}
    for rewrite in rewrites:
        updates.setdefault(rewrite.wiki_path, []).append(rewrite)

    for wiki_path, path_rewrites in updates.items():
        lines = wiki_path.read_text(encoding="utf-8").splitlines()
        for rewrite in sorted(path_rewrites, key=lambda item: item.line_index):
            lines[rewrite.line_index] = rewrite.after
        vp.atomic_write_text(wiki_path, "\n".join(lines) + "\n")


def print_summary(rewrites: list[Rewrite], skips: list[Skip], *, stream: object = sys.stdout) -> None:
    rewritten_files = len({rewrite.wiki_path for rewrite in rewrites})
    print(
        f"Planned rewrites: {len(rewrites)} lines across {rewritten_files} files",
        file=stream,
    )
    print(f"Skipped lines: {len(skips)}", file=stream)

    if skips:
        reason_counts: dict[str, int] = {}
        for skip in skips:
            reason_counts[skip.reason] = reason_counts.get(skip.reason, 0) + 1
        for reason in sorted(reason_counts):
            print(f"- {reason}: {reason_counts[reason]}", file=stream)


def print_examples(rewrites: list[Rewrite], skips: list[Skip], *, limit: int, stream: object = sys.stdout) -> None:
    if rewrites:
        print("\nRewrite examples:", file=stream)
        for rewrite in rewrites[:limit]:
            rel = rewrite.wiki_path.relative_to(ROOT)
            print(f"- {rel}:{rewrite.line_index + 1}", file=stream)
            print(f"  before: {rewrite.before}", file=stream)
            print(f"  after:  {rewrite.after}", file=stream)
    if skips:
        print("\nSkip examples:", file=stream)
        for skip in skips[:limit]:
            rel = skip.wiki_path.relative_to(ROOT)
            print(f"- {rel}:{skip.line_index} — {skip.reason} ({skip.raw_target})", file=stream)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize wiki source labels to literal raw URLs when the linked raw note has exactly one URL."
    )
    parser.add_argument("--apply", action="store_true", help="Write the rewrites to wiki files.")
    parser.add_argument("--examples", type=int, default=10, help="How many rewrite/skip examples to print.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rewrites, skips = plan_rewrites()
    print_summary(rewrites, skips)
    print_examples(rewrites, skips, limit=max(args.examples, 0))

    if args.apply and rewrites:
        apply_rewrites(rewrites)
        print("\nApplied rewrites.", file=sys.stdout)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
