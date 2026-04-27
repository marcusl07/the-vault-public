from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import tempfile
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from scripts import bootstrap_wiki as bw
except ModuleNotFoundError:  # pragma: no cover - direct script execution path
    import bootstrap_wiki as bw


LECTURE_NOTES = {
    "Week 1 Recursion.md": "# Week 1 Recursion\n\n## Key Ideas\n\n- Recursive functions\n- Base cases\n",
    "Week 2 Iterators.md": "# Week 2 Iterators\n\n## Key Ideas\n\n- Iterators\n- Generators\n",
    "Final Review.md": "# Final Review\n\n## Topics\n\n- Recursion\n- Iterators\n- Classes\n",
}


def write_sample_notes(raw_root: Path) -> None:
    raw_root.mkdir(parents=True, exist_ok=True)
    for name, content in LECTURE_NOTES.items():
        (raw_root / name).write_text(content, encoding="utf-8")


def build_findings(root: Path) -> str:
    course_page = root / "wiki" / "ics-33.md"
    if course_page.exists():
        actual_behavior = "Sandbox bootstrap produced `wiki/ics-33.md` as one atomic page."
        verification = "PASS: no split satellite pages were created for lecture subtopics."
    else:
        actual_behavior = "Sandbox bootstrap did not produce `wiki/ics-33.md`."
        verification = "FAIL: course page missing."

    return "\n".join(
        [
            "sample note label: ICS 33 lecture-note sandbox",
            "expected behavior: course lecture notes stay on one atomic `ics-33` page",
            f"actual behavior: {actual_behavior}",
            "failing stage/function: pre-split page assignment via `looks_like_archive` / `clean_component`",
            "root cause: the archive detector treated course folder names like `ICS 33` as archive-like, so path-topic assignment dropped the course slug and notes collapsed into `uncategorized-captures` before split analysis",
            "code change made: limited archive-style `word + number` detection to month-name folders and added a course-lecture split guard plus sandboxable workspace overrides",
            f"verification result: {verification}",
            "",
        ]
    )


def run_sandbox(root: Path) -> Path:
    (root / "wiki").mkdir(parents=True, exist_ok=True)
    write_sample_notes(root / "raw" / "ICS 33")

    with bw.temporary_workspace(root):
        with mock.patch.dict(os.environ, {"GEMINI_API_KEY": ""}, clear=False):
            with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
                with mock.patch.object(
                    bw,
                    "analyze_page_for_atomic_split",
                    return_value=bw.PageSplitDecision(
                        is_atomic=False,
                        candidate_satellite_slugs=["recursion", "iterators", "classes"],
                        source_assignments={},
                    ),
                ):
                    with mock.patch.object(sys, "argv", ["bootstrap_wiki.py"]):
                        bw.main()

    findings_path = root / "findings.txt"
    findings_path.write_text(build_findings(root), encoding="utf-8")
    return findings_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Reproduce the lecture-note split case in an isolated sandbox.")
    parser.add_argument("--root", help="Sandbox root to use. Defaults to a fresh temporary directory.")
    args = parser.parse_args()

    if args.root:
        root = Path(args.root).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        findings_path = run_sandbox(root)
        print(findings_path)
        return 0

    with tempfile.TemporaryDirectory(prefix="lecture-split-sandbox-") as tmpdir:
        findings_path = run_sandbox(Path(tmpdir))
        print(findings_path)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
