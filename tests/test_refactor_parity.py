from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys
from unittest import mock

import capture_ingest
import ingest_raw_notes
import run_vault_pipeline
from scripts import bootstrap_wiki as bw
from scripts import normalize_wiki_source_urls as nwsu
from scripts import publish_private_and_public as ppp
from scripts import sync_public_mirror as spm
from scripts import vault_pipeline as vp
from scripts import workspace_fs


class WrapperDispatchTests(unittest.TestCase):
    def test_capture_wrapper_delegates_to_vault_pipeline(self) -> None:
        with mock.patch.object(capture_ingest.vp, "capture_main", return_value=17) as capture_main:
            result = capture_ingest.main(["--debug"])

        capture_main.assert_called_once_with(["--debug"])
        self.assertEqual(result, 17)

    def test_ingest_wrapper_delegates_to_vault_pipeline(self) -> None:
        with mock.patch.object(ingest_raw_notes.vp, "ingest_main", return_value=23) as ingest_main:
            result = ingest_raw_notes.main(["--limit", "1"])

        ingest_main.assert_called_once_with(["--limit", "1"])
        self.assertEqual(result, 23)

    def test_run_wrapper_delegates_to_vault_pipeline(self) -> None:
        plan = object()
        result_payload = {"capture_ingest": {"new_exports": [], "errors": []}, "wiki_ingest": None}
        with mock.patch.object(run_vault_pipeline.vp, "discover", return_value=plan) as discover, mock.patch.object(
            run_vault_pipeline.vp, "process", return_value=result_payload
        ) as process, mock.patch.object(run_vault_pipeline.vp, "write_outputs", return_value=31) as write_outputs:
            result = run_vault_pipeline.main(["--dry-run"])

        discover.assert_called_once_with(["--dry-run"])
        process.assert_called_once_with(plan)
        write_outputs.assert_called_once_with(result_payload)
        self.assertEqual(result, 31)


class NormalizeWikiSourceUrlsTests(unittest.TestCase):
    def test_plan_and_apply_rewrites_single_url_source_labels(self) -> None:
        original_root = nwsu.ROOT
        original_wiki_root = nwsu.WIKI_ROOT

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir).resolve()
            wiki_root = root / "wiki"
            raw_root = root / "raw"
            wiki_root.mkdir()
            raw_root.mkdir()

            raw_path = raw_root / "coffee.md"
            raw_path.write_text(
                "# Coffee\n\nSource: https://example.com/coffee\n",
                encoding="utf-8",
            )
            wiki_path = wiki_root / "coffee.md"
            wiki_path.write_text(
                "\n".join(
                    [
                        "# Coffee",
                        "",
                        "## Sources",
                        "",
                        "- [Local note](../raw/coffee.md)",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            nwsu.ROOT = root
            nwsu.WIKI_ROOT = wiki_root
            try:
                rewrites, skips = nwsu.plan_rewrites()
                self.assertEqual(len(rewrites), 1)
                self.assertEqual(skips, [])
                self.assertEqual(rewrites[0].chosen_url, "https://example.com/coffee")

                nwsu.apply_rewrites(rewrites)
                updated = wiki_path.read_text(encoding="utf-8")
                self.assertIn("- [https://example.com/coffee](../raw/coffee.md)", updated)
            finally:
                nwsu.ROOT = original_root
                nwsu.WIKI_ROOT = original_wiki_root

    def test_plan_rewrites_skips_ambiguous_raw_sources(self) -> None:
        original_root = nwsu.ROOT
        original_wiki_root = nwsu.WIKI_ROOT

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir).resolve()
            wiki_root = root / "wiki"
            raw_root = root / "raw"
            wiki_root.mkdir()
            raw_root.mkdir()

            (raw_root / "links.md").write_text(
                "https://example.com/one\nhttps://example.com/two\n",
                encoding="utf-8",
            )
            (wiki_root / "topic.md").write_text(
                "\n".join(
                    [
                        "# Topic",
                        "",
                        "## Sources",
                        "",
                        "- [Links](../raw/links.md)",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            nwsu.ROOT = root
            nwsu.WIKI_ROOT = wiki_root
            try:
                rewrites, skips = nwsu.plan_rewrites()
                self.assertEqual(rewrites, [])
                self.assertEqual(len(skips), 1)
                self.assertEqual(skips[0].reason, "multiple URLs in raw note")
            finally:
                nwsu.ROOT = original_root
                nwsu.WIKI_ROOT = original_wiki_root

    def test_plan_rewrites_preserves_source_line_suffix(self) -> None:
        original_root = nwsu.ROOT
        original_wiki_root = nwsu.WIKI_ROOT

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir).resolve()
            wiki_root = root / "wiki"
            raw_root = root / "raw"
            wiki_root.mkdir()
            raw_root.mkdir()

            (raw_root / "article.md").write_text("https://example.com/article\n", encoding="utf-8")
            wiki_path = wiki_root / "article.md"
            wiki_path.write_text(
                "\n".join(
                    [
                        "# Article",
                        "",
                        "## Sources",
                        "",
                        "- [Saved article](../raw/article.md) — [⚠️ fetch failed]",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            nwsu.ROOT = root
            nwsu.WIKI_ROOT = wiki_root
            try:
                rewrites, skips = nwsu.plan_rewrites()
                self.assertEqual(skips, [])
                self.assertEqual(len(rewrites), 1)
                self.assertEqual(
                    rewrites[0].after,
                    "- [https://example.com/article](../raw/article.md) — [⚠️ fetch failed]",
                )
            finally:
                nwsu.ROOT = original_root
                nwsu.WIKI_ROOT = original_wiki_root


class SyncPublicMirrorTests(unittest.TestCase):
    def test_sync_tree_copies_publishable_files_and_removes_stale_dest_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_root = Path(tmp_dir)
            source_root = tmp_root / "source"
            dest_root = tmp_root / "dest"
            source_root.mkdir()
            dest_root.mkdir()

            (source_root / "docs").mkdir()
            (source_root / "docs" / "note.md").write_text("publish me\n", encoding="utf-8")
            (source_root / "raw").mkdir()
            (source_root / "raw" / "secret.md").write_text("private\n", encoding="utf-8")
            (source_root / "wiki").mkdir()
            (source_root / "wiki" / "index.md").write_text("private wiki\n", encoding="utf-8")
            (source_root / "log.jsonl").write_text("{}\n", encoding="utf-8")
            (source_root / "state").mkdir()
            (source_root / "state" / "events.jsonl").write_text("{}\n", encoding="utf-8")

            (dest_root / "stale.txt").write_text("remove me\n", encoding="utf-8")
            (dest_root / "old").mkdir()
            (dest_root / "old" / "stale.txt").write_text("remove me too\n", encoding="utf-8")
            (dest_root / ".git").mkdir()
            (dest_root / ".git" / "config").write_text("[core]\n", encoding="utf-8")

            spm.sync_tree(source_root, dest_root)

            self.assertTrue((dest_root / "docs" / "note.md").is_file())
            self.assertFalse((dest_root / "raw" / "secret.md").exists())
            self.assertFalse((dest_root / "wiki" / "index.md").exists())
            self.assertFalse((dest_root / "log.jsonl").exists())
            self.assertFalse((dest_root / "state" / "events.jsonl").exists())
            self.assertFalse((dest_root / "stale.txt").exists())
            self.assertFalse((dest_root / "old").exists())
            self.assertTrue((dest_root / ".git" / "config").is_file())


class PublishPrivateAndPublicTests(unittest.TestCase):
    def test_main_orchestrates_sync_and_commits_for_both_repos(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            public_repo = Path(tmp_dir) / "public"
            public_repo.mkdir()

            with mock.patch.object(ppp, "ensure_git_repo") as ensure_git_repo, mock.patch.object(
                ppp, "set_remote"
            ) as set_remote, mock.patch.object(ppp, "sync_tree") as sync_tree, mock.patch.object(
                ppp, "commit_and_push"
            ) as commit_and_push, mock.patch.object(
                sys,
                "argv",
                [
                    "publish_private_and_public.py",
                    "--public-repo",
                    str(public_repo),
                    "--public-remote",
                    "git@github.com:example/public.git",
                    "--message",
                    "Sync both repos",
                ],
            ):
                result = ppp.main()

        self.assertEqual(result, 0)
        ensure_git_repo.assert_called_once_with(public_repo.resolve())
        set_remote.assert_called_once_with(public_repo.resolve(), "git@github.com:example/public.git")
        sync_tree.assert_called_once()
        self.assertEqual(commit_and_push.call_args_list[0].args[1], "Sync both repos")
        self.assertEqual(commit_and_push.call_args_list[1].args[1], "Sync both repos")
        self.assertEqual(len(commit_and_push.call_args_list), 2)

    def test_commit_and_push_skips_git_commands_when_repo_has_no_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            repo = Path(tmp_dir)
            with mock.patch.object(ppp, "git_has_changes", return_value=False) as git_has_changes, mock.patch.object(
                ppp, "run_git"
            ) as run_git:
                ppp.commit_and_push(repo, "No changes")

        git_has_changes.assert_called_once_with(repo)
        run_git.assert_not_called()


class SharedWorkspaceFilesystemTests(unittest.TestCase):
    def test_atomic_write_text_replaces_existing_contents(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "nested" / "note.txt"
            workspace_fs.atomic_write_text(path, "first\n")
            workspace_fs.atomic_write_text(path, "second\n")

            self.assertEqual(path.read_text(encoding="utf-8"), "second\n")

    def test_bootstrap_temporary_workspace_restores_globals(self) -> None:
        original_root = bw.ROOT
        original_raw_root = bw.RAW_ROOT
        original_wiki_root = bw.WIKI_ROOT

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            with bw.temporary_workspace(root):
                self.assertEqual(bw.ROOT, root)
                self.assertEqual(bw.RAW_ROOT, root / "raw")
                self.assertEqual(bw.WIKI_ROOT, root / "wiki")

        self.assertEqual(bw.ROOT, original_root)
        self.assertEqual(bw.RAW_ROOT, original_raw_root)
        self.assertEqual(bw.WIKI_ROOT, original_wiki_root)

    def test_vault_temporary_workspace_restores_globals_and_bootstrap_workspace(self) -> None:
        original_root = vp.ROOT
        original_raw_root = vp.RAW_ROOT
        original_capture_root = vp.DEFAULT_CAPTURE_ROOT
        original_bootstrap_root = vp.bw.ROOT

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            capture_root = root / "captures"
            with vp.temporary_workspace(root, capture_root=capture_root):
                self.assertEqual(vp.ROOT, root)
                self.assertEqual(vp.RAW_ROOT, root / "raw")
                self.assertEqual(vp.DEFAULT_CAPTURE_ROOT, capture_root)
                self.assertEqual(vp.bw.ROOT, root)

        self.assertEqual(vp.ROOT, original_root)
        self.assertEqual(vp.RAW_ROOT, original_raw_root)
        self.assertEqual(vp.DEFAULT_CAPTURE_ROOT, original_capture_root)
        self.assertEqual(vp.bw.ROOT, original_bootstrap_root)
