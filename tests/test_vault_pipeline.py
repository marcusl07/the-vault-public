from __future__ import annotations

import fcntl
import io
import json
import os
import tempfile
import unittest
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

from scripts import vault_pipeline as vp


@contextmanager
def isolated_env() -> tuple[Path, Path, Path, Path]:
    original_root = vp.ROOT
    original_raw_root = vp.RAW_ROOT
    original_wiki_root = vp.WIKI_ROOT
    original_log_path = vp.JSONL_LOG_PATH
    original_capture_root = vp.DEFAULT_CAPTURE_ROOT

    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_root = root / "raw"
        wiki_root = root / "wiki"
        capture_root = root / "capture"
        raw_root.mkdir()
        wiki_root.mkdir()
        capture_root.mkdir()

        vp.ROOT = root
        vp.RAW_ROOT = raw_root
        vp.WIKI_ROOT = wiki_root
        vp.JSONL_LOG_PATH = root / "log.jsonl"
        vp.DEFAULT_CAPTURE_ROOT = capture_root
        try:
            yield root, raw_root, wiki_root, capture_root
        finally:
            vp.ROOT = original_root
            vp.RAW_ROOT = original_raw_root
            vp.WIKI_ROOT = original_wiki_root
            vp.JSONL_LOG_PATH = original_log_path
            vp.DEFAULT_CAPTURE_ROOT = original_capture_root


def write_note(path: Path, body: str, frontmatter: dict[str, object] | None = None) -> None:
    path.write_text(vp.render_note(frontmatter or {}, body), encoding="utf-8")


class VaultPipelineTests(unittest.TestCase):
    def test_helpers_render_expected_shapes(self) -> None:
        self.assertEqual(vp.clean_title_from_filename("✓ My note.md"), "My note")
        self.assertTrue(vp.is_placeholder_title("Untitled"))
        self.assertTrue(vp.is_placeholder_title(" untitled "))
        self.assertFalse(vp.is_placeholder_title("New Note"))
        self.assertEqual(vp.raw_file_slug("A/B\\C:*?<>|"), "a-b-c")
        self.assertEqual(vp.raw_file_slug("---"), "untitled")
        self.assertRegex(vp.utc_timestamp(), r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

        rendered = vp.render_raw_file(
            capture_id="123",
            title="My note",
            created_at="2026-04-20T17:32:00Z",
            source_file="My note.md",
            body="Line 1\n\n# Existing heading",
        )
        self.assertIn("capture_id: '123'", rendered)
        self.assertIn("source_file: 'My note.md'", rendered)
        self.assertIn("# My note\n\nLine 1", rendered)

    def test_capture_ingest_injects_exports_and_marks_processed(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            note_path = capture_root / "Gym Plan.md"
            write_note(note_path, "Line 1\n")

            result = vp.capture_ingest(capture_root=capture_root)

            self.assertEqual(len(result["new_exports"]), 1)
            export_item = result["new_exports"][0]
            exported_path = raw_root / Path(export_item["raw_path"]).name
            self.assertTrue(exported_path.exists())
            self.assertFalse(note_path.exists())
            self.assertTrue((capture_root / "✓ Gym Plan.md").exists())

            source_note = vp.read_source_note(capture_root / "✓ Gym Plan.md")
            self.assertIsNotNone(source_note.capture_id)
            raw_frontmatter, raw_body = vp.parse_raw_note(exported_path)
            self.assertEqual(raw_frontmatter["capture_id"], source_note.capture_id)
            self.assertEqual(raw_frontmatter["title"], "Gym Plan")
            self.assertEqual(raw_frontmatter["source_file"], "Gym Plan.md")
            self.assertIn("# Gym Plan", raw_body)

            log_events = [json.loads(line) for line in (vp.JSONL_LOG_PATH).read_text(encoding="utf-8").splitlines()]
            self.assertEqual([event["event"] for event in log_events], ["discovered", "exported_to_raw", "marked_processed"])

    def test_placeholder_empty_note_is_deleted_but_dry_run_preserves_it(self) -> None:
        with isolated_env() as (_, _, _, capture_root):
            dry_note = capture_root / "Untitled.md"
            write_note(dry_note, "   \n")
            vp.capture_ingest(capture_root=capture_root, dry_run=True)
            self.assertTrue(dry_note.exists())
            self.assertFalse(vp.JSONL_LOG_PATH.exists())

            real_note = capture_root / "Untitled.md"
            write_note(real_note, "\n\t \n")
            result = vp.capture_ingest(capture_root=capture_root)
            self.assertEqual(result["new_exports"], [])
            self.assertFalse(real_note.exists())
            payload = json.loads(vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(payload["event"], "empty_deleted")
            self.assertEqual(payload["filename"], "Untitled.md")

    def test_blank_body_with_meaningful_title_is_exported(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            note_path = capture_root / "i like eating grapes.md"
            write_note(note_path, " \n\t", {"created_at": "2024-01-02T03:04:05Z"})

            result = vp.capture_ingest(capture_root=capture_root)

            self.assertEqual(len(result["new_exports"]), 1)
            export_item = result["new_exports"][0]
            exported_path = raw_root / Path(export_item["raw_path"]).name
            self.assertTrue(exported_path.exists())
            self.assertFalse(note_path.exists())
            self.assertTrue((capture_root / "✓ i like eating grapes.md").exists())

            raw_frontmatter, raw_body = vp.parse_raw_note(exported_path)
            self.assertEqual(raw_frontmatter["title"], "i like eating grapes")
            self.assertEqual(raw_frontmatter["created_at"], "2024-01-02T03:04:05Z")
            self.assertEqual(raw_body, "# i like eating grapes\n\n \n\t")

            log_events = [json.loads(line) for line in vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()]
            self.assertEqual([event["event"] for event in log_events], ["discovered", "exported_to_raw", "marked_processed"])
            self.assertFalse(any(event["event"] == "empty_deleted" for event in log_events))

    def test_limit_excludes_threshold_skips_and_injection_failures(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            write_note(capture_root / "Skip.md", "Body", {"capture_id": "skip-id", "ingest_attempts": 3})
            write_note(capture_root / "Fail.md", "Body")
            write_note(capture_root / "Good.md", "Body")

            original_inject = vp.inject_capture_id

            def selective_inject(note: vp.SourceNote) -> vp.SourceNote:
                if note.filename == "Fail.md":
                    raise PermissionError("permission denied")
                return original_inject(note)

            with mock.patch.object(vp, "inject_capture_id", side_effect=selective_inject):
                result = vp.capture_ingest(capture_root=capture_root, limit=1)

            self.assertEqual(len(result["new_exports"]), 1)
            self.assertEqual(result["new_exports"][0]["raw_path"], "raw/good-" + result["new_exports"][0]["capture_id"] + ".md")
            self.assertTrue((capture_root / "✓ Good.md").exists())
            self.assertTrue((capture_root / "Skip.md").exists())
            self.assertTrue((capture_root / "Fail.md").exists())
            self.assertTrue((raw_root / Path(result["new_exports"][0]["raw_path"]).name).exists())
            reasons = [error["reason"] for error in result["errors"]]
            self.assertIn("injection_failed: permission denied", reasons)

    def test_existing_raw_file_is_adopted_and_target_path_collision_increments_attempts(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            adopt_id = "adopt-123"
            adopt_note = capture_root / "Adopt Me.md"
            write_note(adopt_note, "Body", {"capture_id": adopt_id})
            adopt_raw = raw_root / "adopt-me-adopt-123.md"
            adopt_raw.write_text(
                vp.render_raw_file(
                    capture_id=adopt_id,
                    title="Adopt Me",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Adopt Me.md",
                    body="Body",
                ),
                encoding="utf-8",
            )

            bad_note = capture_root / "Broken.md"
            write_note(bad_note, "Body", {"capture_id": "broken-123"})
            (raw_root / "broken-broken-123.md").write_text("---\ncapture_id: 'wrong'\n---\n", encoding="utf-8")

            result = vp.capture_ingest(capture_root=capture_root)

            self.assertTrue((capture_root / "✓ Adopt Me.md").exists())
            self.assertFalse(adopt_note.exists())
            exported = [json.loads(line) for line in vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()]
            adopt_event = next(
                event for event in exported if event["event"] == "exported_to_raw" and event["capture_id"] == adopt_id
            )
            self.assertEqual(adopt_event["mode"], "adopted")
            self.assertEqual(adopt_event["candidate_count"], 1)
            updated_bad_note = vp.read_source_note(bad_note)
            self.assertEqual(updated_bad_note.ingest_attempts, 1)
            self.assertEqual(len(result["errors"]), 1)
            self.assertEqual(result["errors"][0]["filename"], "Broken.md")
            self.assertEqual(result["errors"][0]["reason"], "raw_create_failed")

    def test_raw_discovery_scans_recursively_and_ignores_non_markdown_and_symlinks(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            capture_id = "nested-123"
            note_path = capture_root / "Nested Note.md"
            write_note(note_path, "Body", {"capture_id": capture_id})

            nested_dir = raw_root / "nested" / "deeper"
            nested_dir.mkdir(parents=True)
            valid_raw = nested_dir / "different-title.md"
            valid_raw.write_text(
                vp.render_raw_file(
                    capture_id=capture_id,
                    title="Completely Different Title",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Nested Note.md",
                    body="Body",
                ),
                encoding="utf-8",
            )
            (raw_root / "nested" / "ignore.txt").write_text("capture_id: nested-123", encoding="utf-8")
            (raw_root / "nested" / "linked.md").symlink_to(valid_raw)

            result = vp.capture_ingest(capture_root=capture_root)

            self.assertEqual(result["errors"], [])
            self.assertEqual(result["new_exports"], [{"capture_id": capture_id, "raw_path": "raw/nested/deeper/different-title.md"}])
            self.assertTrue((capture_root / "✓ Nested Note.md").exists())

    def test_multiple_valid_raw_matches_are_ambiguous(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            capture_id = "dupe-123"
            note_path = capture_root / "Duplicate.md"
            write_note(note_path, "Body", {"capture_id": capture_id})
            for relative in ("first.md", "nested/second.md"):
                raw_path = raw_root / relative
                raw_path.parent.mkdir(parents=True, exist_ok=True)
                raw_path.write_text(
                    vp.render_raw_file(
                        capture_id=capture_id,
                        title="Duplicate",
                        created_at="2026-04-20T17:32:00Z",
                        source_file="Duplicate.md",
                        body="Body",
                    ),
                    encoding="utf-8",
                )

            result = vp.capture_ingest(capture_root=capture_root)

            self.assertEqual(result["new_exports"], [])
            self.assertEqual(result["errors"][0]["reason"], "raw_identity_ambiguous")
            self.assertEqual(vp.read_source_note(note_path).ingest_attempts, 1)

    def test_valid_and_invalid_raw_match_is_ambiguous(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            capture_id = "mix-123"
            note_path = capture_root / "Mixed.md"
            write_note(note_path, "Body", {"capture_id": capture_id})
            (raw_root / "valid.md").write_text(
                vp.render_raw_file(
                    capture_id=capture_id,
                    title="Mixed",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Mixed.md",
                    body="Body",
                ),
                encoding="utf-8",
            )
            (raw_root / "invalid.md").write_text(
                vp.render_note({"capture_id": capture_id}, "   \n"),
                encoding="utf-8",
            )

            result = vp.capture_ingest(capture_root=capture_root)

            self.assertEqual(result["new_exports"], [])
            self.assertEqual(result["errors"][0]["reason"], "raw_identity_ambiguous")
            self.assertEqual(vp.read_source_note(note_path).ingest_attempts, 1)

    def test_invalid_matching_raw_blocks_create(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            capture_id = "invalid-only-123"
            note_path = capture_root / "Invalid Only.md"
            write_note(note_path, "Body", {"capture_id": capture_id})
            (raw_root / "nested" / "invalid.md").parent.mkdir(parents=True, exist_ok=True)
            (raw_root / "nested" / "invalid.md").write_text(
                vp.render_note({"capture_id": capture_id}, "   \n"),
                encoding="utf-8",
            )

            result = vp.capture_ingest(capture_root=capture_root)

            self.assertEqual(result["new_exports"], [])
            self.assertEqual(result["errors"][0]["reason"], "raw_invalid_existing")
            self.assertEqual(vp.read_source_note(note_path).ingest_attempts, 1)

    def test_zero_valid_zero_invalid_creates_new_raw_even_with_unparseable_markdown_elsewhere(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            capture_id = "create-123"
            note_path = capture_root / "Create Me.md"
            write_note(note_path, "Body", {"capture_id": capture_id})
            (raw_root / "garbage.md").write_text("---\ncapture_id: 'oops'\n", encoding="utf-8")

            result = vp.capture_ingest(capture_root=capture_root)

            self.assertEqual(len(result["new_exports"]), 1)
            exported_path = raw_root / Path(result["new_exports"][0]["raw_path"]).name
            self.assertTrue(exported_path.exists())
            events = [json.loads(line) for line in vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()]
            export_event = next(event for event in events if event["event"] == "exported_to_raw")
            self.assertEqual(export_event["mode"], "created")
            self.assertEqual(export_event["candidate_count"], 0)

    def test_duplicate_unprocessed_source_notes_with_same_capture_id_fail_as_source_identity_ambiguous(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            capture_id = "shared-123"
            write_note(capture_root / "First.md", "Body 1", {"capture_id": capture_id})
            write_note(capture_root / "Second.md", "Body 2", {"capture_id": capture_id})

            result = vp.capture_ingest(capture_root=capture_root)

            self.assertEqual(result["new_exports"], [])
            self.assertEqual(
                sorted(error["reason"] for error in result["errors"]),
                ["source_identity_ambiguous", "source_identity_ambiguous"],
            )
            self.assertFalse(any(raw_root.iterdir()))
            for name in ("First.md", "Second.md"):
                self.assertEqual(vp.read_source_note(capture_root / name).ingest_attempts, 1)

            events = [json.loads(line) for line in vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()]
            export_failures = [event for event in events if event["event"] == "export_failed"]
            self.assertEqual(len(export_failures), 2)
            self.assertTrue(all(event["failure_class"] == "source_identity_ambiguous" for event in export_failures))
            self.assertEqual({event["source_filename"] for event in export_failures}, {"First.md", "Second.md"})
            self.assertTrue(all(event["counted_against_retry_gate"] is True for event in export_failures))

    def test_processed_without_raw_logs_each_run_and_does_not_increment_attempts(self) -> None:
        with isolated_env() as (_, _, _, capture_root):
            processed_path = capture_root / "✓ Orphaned.md"
            write_note(processed_path, "Body", {"capture_id": "orphan-123", "ingest_attempts": 2})

            first = vp.capture_ingest(capture_root=capture_root)
            second = vp.capture_ingest(capture_root=capture_root)

            self.assertEqual(first["new_exports"], [])
            self.assertEqual(second["new_exports"], [])
            self.assertEqual(vp.read_source_note(processed_path).ingest_attempts, 2)

            events = [json.loads(line) for line in vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()]
            export_failures = [event for event in events if event["event"] == "export_failed"]
            self.assertEqual(len(export_failures), 2)
            self.assertTrue(all(event["failure_class"] == "processed_without_raw" for event in export_failures))
            self.assertTrue(all(event["counted_against_retry_gate"] is False for event in export_failures))
            self.assertEqual({event["source_filename"] for event in export_failures}, {"✓ Orphaned.md"})

    def test_retry_gated_export_failure_is_not_logged_when_attempt_increment_fails(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            note_path = capture_root / "Broken.md"
            write_note(note_path, "Body", {"capture_id": "broken-123"})
            raw_root.joinpath("nested").mkdir(parents=True, exist_ok=True)
            (raw_root / "nested" / "invalid.md").write_text(
                vp.render_note({"capture_id": "broken-123"}, "   \n"),
                encoding="utf-8",
            )

            with mock.patch.object(vp, "increment_ingest_attempts", side_effect=OSError("disk full")):
                result = vp.capture_ingest(capture_root=capture_root)

            self.assertEqual(result["errors"], [{"filename": "Broken.md", "capture_id": "broken-123", "reason": "raw_invalid_existing"}])
            self.assertEqual(vp.read_source_note(note_path).ingest_attempts, 0)
            if vp.JSONL_LOG_PATH.exists():
                events = [json.loads(line) for line in vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()]
            else:
                events = []
            self.assertFalse(any(event["event"] == "export_failed" for event in events))

    def test_created_at_uses_frontmatter_then_birthtime_or_mtime_fallback(self) -> None:
        with isolated_env() as (_, raw_root, _, capture_root):
            explicit = capture_root / "Explicit.md"
            write_note(explicit, "Body", {"created_at": "2024-01-02T03:04:05Z"})
            explicit_result = vp.capture_ingest(capture_root=capture_root)
            explicit_raw = raw_root / Path(explicit_result["new_exports"][0]["raw_path"]).name
            frontmatter, _ = vp.parse_raw_note(explicit_raw)
            self.assertEqual(frontmatter["created_at"], "2024-01-02T03:04:05Z")

            fallback = capture_root / "Fallback.md"
            write_note(fallback, "Body")
            original_resolve = vp.resolve_created_at

            def fake_resolve(note: vp.SourceNote) -> tuple[str, bool]:
                if note.filename == "Fallback.md":
                    return "2024-04-20T12:20:00Z", True
                return original_resolve(note)

            with mock.patch.object(vp, "resolve_created_at", side_effect=fake_resolve):
                vp.capture_ingest(capture_root=capture_root)

            events = [json.loads(line) for line in vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()]
            self.assertTrue(any(event["event"] == "created_at_mtime_fallback" for event in events))

    def test_ingest_raw_notes_updates_wiki_and_marks_integrated(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            raw_path = raw_root / "note-123.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Note 1",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Note 1.md",
                    body="This note has enough detail to stay in the page body.",
                ),
                encoding="utf-8",
            )

            result = vp.ingest_raw_notes([{"capture_id": "123", "raw_path": "raw/note-123.md"}])

            self.assertEqual(result["integrated"], [{"capture_id": "123", "raw_path": "raw/note-123.md"}])
            frontmatter, _ = vp.parse_raw_note(raw_path)
            self.assertRegex(str(frontmatter["integrated_at"]), r"^\d{4}-\d{2}-\d{2}T")
            self.assertTrue((wiki_root / "note-1.md").exists())
            self.assertIn("[[note-1]]", (wiki_root / "index.md").read_text(encoding="utf-8"))
            self.assertIn('ingest | Capture: "Note 1"', (wiki_root / "log.md").read_text(encoding="utf-8"))
            event = json.loads(vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(event["event"], "integrated")

    def test_page_resynthesis_on_touch_rewrites_existing_page_with_shared_atomic_shape(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            existing_page = wiki_root / "topic-a.md"
            existing_page.write_text(
                "\n".join(
                    [
                        "# Topic A",
                        "",
                        "## Notes",
                        "",
                        "- Old note about the topic.",
                        "",
                        "## Connections",
                        "",
                        "- [[existing-link]] — already connected",
                        "",
                        "## Sources",
                        "",
                        "- [Old Source](../raw/old-source.md)",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            raw_path = raw_root / "new-note-123.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Topic A",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Topic A.md",
                    body="Fresh note body with new evidence for topic A.",
                ),
                encoding="utf-8",
            )

            result = vp.ingest_raw_notes(
                [{"capture_id": "123", "raw_path": "raw/new-note-123.md"}],
                page_resynthesis_on_touch=True,
            )

            self.assertEqual(result["integrated"], [{"capture_id": "123", "raw_path": "raw/new-note-123.md"}])
            page_text = existing_page.read_text(encoding="utf-8")
            self.assertNotIn("Old hand-written summary that should be replaced.", page_text)
            self.assertNotIn("This page collects Marcus's notes", page_text)
            self.assertIn("- [Old Source](../raw/old-source.md)", page_text)
            self.assertIn("- [Topic A](../raw/new-note-123.md)", page_text)
            self.assertIn("- [[existing-link]]", page_text)

    def test_page_resynthesis_on_touch_preserves_atomic_notes_without_summary_boilerplate(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            existing_page = wiki_root / "topic-a.md"
            existing_page.write_text(
                "\n".join(
                    [
                        "# Topic A",
                        "",
                        "## Notes",
                        "",
                        "- Existing retained note.",
                        "",
                        "## Connections",
                        "",
                        "- [[existing-link]] — already connected",
                        "",
                        "## Sources",
                        "",
                        "- [Old Source](../raw/old-source.md)",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            raw_path = raw_root / "new-note-123.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Topic A",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Topic A.md",
                    body="Fresh note body with new evidence for topic A.",
                ),
                encoding="utf-8",
            )

            result = vp.ingest_raw_notes(
                [{"capture_id": "123", "raw_path": "raw/new-note-123.md"}],
                page_resynthesis_on_touch=True,
            )

            self.assertEqual(result["integrated"], [{"capture_id": "123", "raw_path": "raw/new-note-123.md"}])
            page_text = existing_page.read_text(encoding="utf-8")
            self.assertIn("- Existing retained note.", page_text)
            self.assertIn("Fresh note body with new evidence for topic A.", page_text)
            self.assertIn("- [Old Source](../raw/old-source.md)", page_text)
            self.assertIn("- [Topic A](../raw/new-note-123.md)", page_text)
            self.assertNotIn("This page collects Marcus's notes", page_text)
            self.assertNotIn("- No notes yet.", page_text)

    def test_page_resynthesis_on_touch_preserves_connections_and_updates_index_log_and_integrated_at(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            existing_page = wiki_root / "topic-a.md"
            existing_page.write_text(
                "\n".join(
                    [
                        "# Topic A",
                        "",
                        "## Notes",
                        "",
                        "- Existing retained note.",
                        "",
                        "## Connections",
                        "",
                        "- [[existing-link]] — already connected",
                        "",
                        "## Sources",
                        "",
                        "- [Old Source](../raw/old-source.md)",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            raw_path = raw_root / "topic-a-123.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Topic A",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Topic A.md",
                    body="Fresh note body with new evidence for topic A.",
                ),
                encoding="utf-8",
            )

            result = vp.ingest_raw_notes(
                [{"capture_id": "123", "raw_path": "raw/topic-a-123.md"}],
                page_resynthesis_on_touch=True,
            )

            self.assertEqual(result["integrated"], [{"capture_id": "123", "raw_path": "raw/topic-a-123.md"}])
            page_text = existing_page.read_text(encoding="utf-8")
            self.assertIn("[[existing-link]]", page_text)
            self.assertNotIn("[[unclassified-media-captures]]", page_text)
            self.assertIn("[[topic-a]]", (wiki_root / "index.md").read_text(encoding="utf-8"))
            self.assertIn('ingest | Capture: "Topic A"', (wiki_root / "log.md").read_text(encoding="utf-8"))
            frontmatter, _ = vp.parse_raw_note(raw_path)
            self.assertIn("integrated_at", frontmatter)

    def test_run_vault_pipeline_integrates_blank_body_meaningful_title_note(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, capture_root):
            note_path = capture_root / "i like eating grapes.md"
            write_note(note_path, "\n \t\n")

            result = vp.run_vault_pipeline(capture_root=capture_root)

            self.assertEqual(len(result["capture_ingest"]["new_exports"]), 1)
            self.assertEqual(result["wiki_ingest"]["integrated"], result["capture_ingest"]["new_exports"])
            self.assertFalse(note_path.exists())
            self.assertTrue((capture_root / "✓ i like eating grapes.md").exists())

            raw_path = raw_root / Path(result["capture_ingest"]["new_exports"][0]["raw_path"]).name
            raw_frontmatter, raw_body = vp.parse_raw_note(raw_path)
            self.assertEqual(raw_frontmatter["title"], "i like eating grapes")
            self.assertIn("integrated_at", raw_frontmatter)
            self.assertEqual(raw_body, "# i like eating grapes\n\n\n \t\n")

            wiki_page = wiki_root / "i-like-eating-grapes.md"
            self.assertTrue(wiki_page.exists())
            page_text = wiki_page.read_text(encoding="utf-8")
            self.assertIn("[i like eating grapes](../raw/", page_text)
            self.assertNotIn("This page collects Marcus's notes", page_text)
            self.assertNotIn("- No notes yet.", page_text)

            events = [json.loads(line) for line in vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()]
            self.assertEqual(
                [event["event"] for event in events],
                ["discovered", "exported_to_raw", "marked_processed", "integrated"],
            )
            self.assertFalse(any(event["event"] == "empty_deleted" for event in events))

    def test_run_vault_pipeline_holds_lock_and_skips_wiki_ingest_on_dry_run(self) -> None:
        with isolated_env() as (_, _, _, capture_root):
            write_note(capture_root / "Dry.md", "Body")
            debug_stream = io.StringIO()

            result = vp.run_vault_pipeline(capture_root=capture_root, dry_run=True, debug=True, debug_stream=debug_stream)

            self.assertIsNone(result["wiki_ingest"])
            self.assertTrue((capture_root / "Dry.md").exists())
            self.assertIn("Running capture ingest stage", debug_stream.getvalue())

            with mock.patch.object(vp, "pipeline_lock", side_effect=RuntimeError("pipeline lock is already held")):
                with self.assertRaises(RuntimeError):
                    vp.run_vault_pipeline(capture_root=capture_root)

    def test_append_jsonl_event_flushes_and_fsyncs(self) -> None:
        with isolated_env() as (root, _, _, _):
            log_path = root / "events.jsonl"

            with mock.patch.object(vp.os, "fsync", wraps=vp.os.fsync) as fsync_mock:
                vp.append_jsonl_event({"event": "marked_processed", "capture_id": "abc"}, log_path=log_path)

            self.assertTrue(log_path.exists())
            self.assertEqual(fsync_mock.call_count, 1)
            payload = json.loads(log_path.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(payload["event"], "marked_processed")
            self.assertEqual(payload["capture_id"], "abc")

    def test_pipeline_lock_writes_metadata(self) -> None:
        with isolated_env() as (_, _, _, capture_root):
            with vp.pipeline_lock(capture_root):
                metadata = json.loads((capture_root / "pipeline.lock").read_text(encoding="utf-8"))
                self.assertEqual(metadata["version"], 1)
                self.assertEqual(metadata["pid"], os.getpid())
                self.assertEqual(metadata["capture_root"], str(capture_root))
                self.assertIsInstance(metadata["owner_token"], str)
                self.assertTrue(metadata["owner_token"])
                self.assertEqual(
                    datetime.strptime(metadata["acquired_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC).tzinfo,
                    UTC,
                )

    def test_pipeline_lock_rejects_fresh_contention_and_recovers_stale_lock(self) -> None:
        with isolated_env() as (_, _, _, capture_root):
            lock_path = capture_root / "pipeline.lock"

            with vp.pipeline_lock(capture_root):
                with self.assertRaisesRegex(RuntimeError, "pipeline lock is already held"):
                    with vp.pipeline_lock(capture_root):
                        pass

            lock_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "owner_token": "stale-owner",
                        "pid": 99999,
                        "acquired_at": (datetime.now(UTC) - timedelta(minutes=31)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "capture_root": str(capture_root),
                    },
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )
            stale_handle = lock_path.open("a+", encoding="utf-8")
            self.addCleanup(stale_handle.close)
            fcntl.flock(stale_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            try:
                with vp.pipeline_lock(capture_root):
                    metadata = json.loads(lock_path.read_text(encoding="utf-8"))
                    self.assertNotEqual(metadata["owner_token"], "stale-owner")
                self.assertFalse((capture_root / "pipeline.lock.stale-claim").exists())
            finally:
                fcntl.flock(stale_handle.fileno(), fcntl.LOCK_UN)
                stale_handle.close()

    def test_pipeline_lock_stale_claim_blocks_second_recovery_attempt(self) -> None:
        with isolated_env() as (_, _, _, capture_root):
            lock_path = capture_root / "pipeline.lock"
            lock_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "owner_token": "stale-owner",
                        "pid": 99999,
                        "acquired_at": (datetime.now(UTC) - timedelta(minutes=31)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "capture_root": str(capture_root),
                    },
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )
            (capture_root / "pipeline.lock.stale-claim").write_text("", encoding="utf-8")
            stale_handle = lock_path.open("a+", encoding="utf-8")
            self.addCleanup(stale_handle.close)
            fcntl.flock(stale_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

            try:
                with self.assertRaisesRegex(RuntimeError, "pipeline lock is already held"):
                    with vp.pipeline_lock(capture_root):
                        pass
            finally:
                fcntl.flock(stale_handle.fileno(), fcntl.LOCK_UN)
                stale_handle.close()

    def test_helper_induced_repo_writes_use_atomic_write_text(self) -> None:
        with isolated_env() as (_, raw_root, _, _):
            raw_path = raw_root / "note-123.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Note 1",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Note 1.md",
                    body="This note has enough detail to stay in the page body.",
                ),
                encoding="utf-8",
            )

            with mock.patch.object(vp, "atomic_write_text", wraps=vp.atomic_write_text) as atomic_write_mock:
                vp.ingest_raw_notes([{"capture_id": "123", "raw_path": "raw/note-123.md"}])

            written_paths = {Path(call.args[0]).name for call in atomic_write_mock.call_args_list}
            self.assertIn("note-1.md", written_paths)
            self.assertIn("index.md", written_paths)
            self.assertIn("log.md", written_paths)


if __name__ == "__main__":
    unittest.main()
