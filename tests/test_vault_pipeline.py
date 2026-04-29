from __future__ import annotations

import fcntl
import io
import json
import os
import tempfile
import unittest
from contextlib import contextmanager, redirect_stdout
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

from scripts import vault_pipeline as vp


@contextmanager
def isolated_env() -> tuple[Path, Path, Path, Path]:
    original_root = vp.ROOT
    original_raw_root = vp.RAW_ROOT
    original_sources_root = vp.SOURCES_ROOT
    original_chat_sources_root = vp.CHAT_SOURCES_ROOT
    original_wiki_root = vp.WIKI_ROOT
    original_log_path = vp.JSONL_LOG_PATH
    original_capture_root = vp.DEFAULT_CAPTURE_ROOT

    with tempfile.TemporaryDirectory() as tmp_dir:
        root = Path(tmp_dir)
        raw_root = root / "raw"
        sources_root = root / "sources"
        wiki_root = root / "wiki"
        capture_root = root / "capture"
        raw_root.mkdir()
        sources_root.mkdir()
        wiki_root.mkdir()
        capture_root.mkdir()

        vp.ROOT = root
        vp.RAW_ROOT = raw_root
        vp.SOURCES_ROOT = sources_root
        vp.CHAT_SOURCES_ROOT = sources_root / "chat"
        vp.WIKI_ROOT = wiki_root
        vp.JSONL_LOG_PATH = root / "log.jsonl"
        vp.DEFAULT_CAPTURE_ROOT = capture_root
        vp.bw.configure_workspace(root)
        try:
            yield root, raw_root, wiki_root, capture_root
        finally:
            vp.ROOT = original_root
            vp.RAW_ROOT = original_raw_root
            vp.SOURCES_ROOT = original_sources_root
            vp.CHAT_SOURCES_ROOT = original_chat_sources_root
            vp.WIKI_ROOT = original_wiki_root
            vp.JSONL_LOG_PATH = original_log_path
            vp.DEFAULT_CAPTURE_ROOT = original_capture_root
            vp.bw.configure_workspace(original_root)


def write_note(path: Path, body: str, frontmatter: dict[str, object] | None = None) -> None:
    path.write_text(vp.render_note(frontmatter or {}, body), encoding="utf-8")


class VaultPipelineTests(unittest.TestCase):
    def test_helpers_render_expected_shapes(self) -> None:
        self.assertEqual(vp.clean_title_from_filename("✓ My note.md"), "My note")
        self.assertTrue(vp.is_placeholder_title("Untitled"))
        self.assertTrue(vp.is_placeholder_title(" untitled "))
        self.assertTrue(vp.is_placeholder_title("Untitled 1"))
        self.assertTrue(vp.is_placeholder_title("untitled 25"))
        self.assertFalse(vp.is_placeholder_title("New Note"))
        self.assertFalse(vp.is_placeholder_title("Untitled note"))
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
        self.assertIn("source_kind: 'capture'", rendered)
        self.assertIn("source_id: 'capture:123'", rendered)
        self.assertIn("source_file: 'My note.md'", rendered)
        self.assertIn("# My note\n\nLine 1", rendered)

    def test_persist_chat_source_artifact_uses_chat_root_and_metadata(self) -> None:
        with isolated_env() as (root, _, _, _):
            created_at = "2026-04-23T10:00:00Z"
            path = vp.persist_chat_source_artifact(
                title="Favorite Coffee",
                body="Marcus prefers pourover over espresso at home.",
                created_at=created_at,
                conversation_ref="chat:2026-04-23T10:00:00Z",
            )

            self.assertTrue(path.is_file())
            self.assertTrue(path.is_relative_to(root / "sources" / "chat"))
            frontmatter, body = vp.parse_raw_note(path)
            self.assertEqual(frontmatter["source_kind"], "chat")
            self.assertTrue(str(frontmatter["source_id"]).startswith("chat:"))
            self.assertEqual(frontmatter["created_at"], created_at)
            self.assertEqual(frontmatter["provenance_pointer"], "chat:2026-04-23T10:00:00Z")
            self.assertIn("# Favorite Coffee", body)

    def test_router_marks_single_existing_atomic_page_as_light_update(self) -> None:
        with isolated_env() as (_, _, wiki_root, _):
            (wiki_root / "topic-a.md").write_text(
                "\n".join(
                    [
                        "# Topic A",
                        "",
                        "## Notes",
                        "",
                        "- Existing note.",
                        "",
                        "## Connections",
                        "",
                        "- [[existing-link]]",
                        "",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            decision = vp._route_source_update(
                title="Topic A",
                body="Fresh bounded update.",
                page_assignments=[("topic-a", "title")],
            )

            self.assertEqual(decision.action, "light_update")
            self.assertEqual(decision.target_pages, ["topic-a"])
            self.assertFalse(decision.new_page_signal)

    def test_router_escalates_single_existing_atomic_page_with_conflicting_note(self) -> None:
        with isolated_env() as (_, _, wiki_root, _):
            (wiki_root / "coffee-preferences.md").write_text(
                "\n".join(
                    [
                        "# Coffee Preferences",
                        "",
                        "## Notes",
                        "",
                        "- Marcus prefers pourover over espresso at home.",
                        "",
                        "## Connections",
                        "",
                        "- [[coffee]]",
                        "",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            decision = vp._route_source_update(
                title="Coffee Preferences",
                body="Marcus prefers espresso over pourover at home.",
                page_assignments=[("coffee-preferences", "title")],
            )

            self.assertEqual(decision.action, "heavy_update")
            self.assertEqual(decision.contradiction_risk, "high")

    def test_router_marks_new_page_as_heavy_update(self) -> None:
        with isolated_env():
            decision = vp._route_source_update(
                title="Brand New Topic",
                body="New source for a new page.",
                page_assignments=[("brand-new-topic", "title")],
            )

            self.assertEqual(decision.action, "heavy_update")
            self.assertEqual(decision.candidate_new_pages, ["brand-new-topic"])

    def test_heavy_ingest_adds_open_question_and_review_for_contradiction_risk(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            (wiki_root / "coffee-preferences.md").write_text(
                "\n".join(
                    [
                        "# Coffee Preferences",
                        "",
                        "## Notes",
                        "",
                        "- Marcus prefers pourover over espresso at home.",
                        "",
                        "## Connections",
                        "",
                        "- [[coffee]]",
                        "",
                        "## Sources",
                        "",
                        "- [Local note](../raw/coffee-source.md)",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            (wiki_root / "coffee.md").write_text(
                "\n".join(
                    [
                        "# Coffee",
                        "",
                        "## Connections",
                        "",
                        "- [[coffee-preferences]]",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            raw_path = raw_root / "life" / "coffee" / "home" / "coffee-preferences-123.md"
            raw_path.parent.mkdir(parents=True)
            raw_path.write_text(
                vp.render_note(
                    {
                        "capture_id": "123",
                        "source_kind": "capture",
                        "source_id": "capture:123",
                        "title": "Coffee Preferences",
                        "created_at": "2026-04-24T10:00:00Z",
                        "source_file": "Coffee Preferences.md",
                    },
                    "# Coffee Preferences\n\nMarcus prefers espresso over pourover at home.",
                ),
                encoding="utf-8",
            )

            outcome = vp._upsert_wiki_pages_for_note(
                frontmatter=vp.parse_raw_note(raw_path)[0],
                title="Coffee Preferences",
                body="Marcus prefers espresso over pourover at home.",
                raw_path=raw_path,
            )

            self.assertEqual(outcome.router_decision.action, "heavy_update")
            self.assertTrue(outcome.review_queued)
            page_text = (wiki_root / "coffee-preferences.md").read_text(encoding="utf-8")
            self.assertIn("## Open Questions", page_text)
            self.assertIn("Potential contradiction after", page_text)
            review_text = (wiki_root / "review.md").read_text(encoding="utf-8")
            self.assertIn("heavy-update contradiction", review_text)
            log_text = (wiki_root / "log.md").read_text(encoding="utf-8")
            self.assertIn('ingest | Capture: "Coffee Preferences" | Router: heavy_update', log_text)

    def test_heavy_ingest_budget_defers_extra_pages_and_logs_deferral(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            raw_path = raw_root / "a" / "b" / "c" / "d" / "e" / "budgeted-note-123.md"
            raw_path.parent.mkdir(parents=True)
            raw_path.write_text(
                vp.render_note(
                    {
                        "capture_id": "123",
                        "source_kind": "capture",
                        "source_id": "capture:123",
                        "title": "Budgeted Note",
                        "created_at": "2026-04-24T10:00:00Z",
                        "source_file": "Budgeted Note.md",
                    },
                    "# Budgeted Note\n\nMarcus prefers a smaller, bounded maintenance budget.",
                ),
                encoding="utf-8",
            )

            outcome = vp._upsert_wiki_pages_for_note(
                frontmatter=vp.parse_raw_note(raw_path)[0],
                title="Budgeted Note",
                body="Marcus prefers a smaller, bounded maintenance budget.",
                raw_path=raw_path,
                budget=vp.MaintenanceBudget(max_candidate_pages=2, max_context_chars=1_200, max_pages_rewritten=2),
            )

            self.assertEqual(outcome.router_decision.action, "heavy_update")
            self.assertTrue(outcome.deferred_items)
            self.assertTrue(outcome.review_queued)
            review_text = (wiki_root / "review.md").read_text(encoding="utf-8")
            self.assertIn("heavy-update deferred work", review_text)
            log_text = (wiki_root / "log.md").read_text(encoding="utf-8")
            self.assertIn('ingest | Capture: "Budgeted Note" | Router: heavy_update | Deferred:', log_text)

    def test_query_writeback_chat_fact_persists_artifact_updates_page_and_logs_query(self) -> None:
        with isolated_env() as (root, _, wiki_root, _):
            result = vp.query_writeback_chat_fact(
                page_title="Coffee Preferences",
                note="Marcus prefers pourover over espresso at home.",
                related_pages=["coffee"],
                created_at="2026-04-23T10:00:00Z",
                conversation_ref="chat:2026-04-23T10:00:00Z",
                fact_key="home-brew-method",
            )

            self.assertEqual(sorted(result.changed_slugs), ["coffee", "coffee-preferences"])
            self.assertIsNotNone(result.source_path)
            assert result.source_path is not None
            self.assertTrue(result.source_path.is_relative_to(root / "sources" / "chat"))

            target_page = (wiki_root / "coffee-preferences.md").read_text(encoding="utf-8")
            self.assertIn("Marcus prefers pourover over espresso at home.", target_page)
            self.assertIn("- [[coffee]]", target_page)
            self.assertIn("../sources/chat/", target_page)

            query_log = (wiki_root / "log.md").read_text(encoding="utf-8")
            self.assertIn('query | writeback | "Coffee Preferences" | Router: heavy_update', query_log)

            index_text = (wiki_root / "index.md").read_text(encoding="utf-8")
            self.assertNotIn("[[review]]", index_text)

    def test_query_writeback_chat_fact_supersedes_prior_chat_fact_without_open_question(self) -> None:
        with isolated_env() as (_, _, wiki_root, _):
            first = vp.query_writeback_chat_fact(
                page_title="Coffee Preferences",
                note="Marcus prefers pourover over espresso at home.",
                related_pages=["coffee"],
                created_at="2026-04-23T10:00:00Z",
                conversation_ref="chat:2026-04-23T10:00:00Z",
                fact_key="home-brew-method",
            )
            second = vp.query_writeback_chat_fact(
                page_title="Coffee Preferences",
                note="Marcus now prefers espresso over pourover at home.",
                related_pages=["coffee"],
                created_at="2026-04-23T11:00:00Z",
                conversation_ref="chat:2026-04-23T11:00:00Z",
                fact_key="home-brew-method",
                replacement_intent=True,
            )

            self.assertIsNotNone(first.source_path)
            self.assertIsNotNone(second.source_path)
            page_text = (wiki_root / "coffee-preferences.md").read_text(encoding="utf-8")
            self.assertIn("Marcus now prefers espresso over pourover at home.", page_text)
            self.assertNotIn("Marcus prefers pourover over espresso at home.", page_text)
            self.assertNotIn("## Open Questions", page_text)
            self.assertEqual(len(list((vp.CHAT_SOURCES_ROOT).glob("*.md"))), 2)
            self.assertTrue(second.superseded_source_paths)
            review_path = wiki_root / "review.md"
            self.assertFalse(review_path.exists())

    def test_query_writeback_chat_fact_queues_review_for_conflicting_fact(self) -> None:
        with isolated_env() as (_, _, wiki_root, _):
            vp.query_writeback_chat_fact(
                page_title="Coffee Preferences",
                note="Marcus prefers pourover over espresso at home.",
                related_pages=["coffee"],
                created_at="2026-04-23T10:00:00Z",
                conversation_ref="chat:2026-04-23T10:00:00Z",
                fact_key="home-brew-method",
            )
            result = vp.query_writeback_chat_fact(
                page_title="Coffee Preferences",
                note="Marcus prefers espresso over pourover at home.",
                related_pages=["coffee"],
                created_at="2026-04-23T11:00:00Z",
                conversation_ref="chat:2026-04-23T11:00:00Z",
                fact_key="home-brew-method",
            )

            self.assertTrue(result.review_queued)
            page_text = (wiki_root / "coffee-preferences.md").read_text(encoding="utf-8")
            self.assertIn("## Open Questions", page_text)
            self.assertIn("Conflicting chat-derived fact for home-brew-method", page_text)
            self.assertIn("Marcus prefers pourover over espresso at home.", page_text)
            self.assertIn("Marcus prefers espresso over pourover at home.", page_text)

            review_text = (wiki_root / "review.md").read_text(encoding="utf-8")
            self.assertIn("contradiction | home-brew-method", review_text)
            self.assertIn("[../sources/chat/", review_text)

    def test_query_writeback_chat_fact_supersession_resolves_existing_review_entry(self) -> None:
        with isolated_env() as (_, _, wiki_root, _):
            vp.query_writeback_chat_fact(
                page_title="Coffee Preferences",
                note="Marcus prefers pourover over espresso at home.",
                related_pages=["coffee"],
                created_at="2026-04-23T10:00:00Z",
                conversation_ref="chat:2026-04-23T10:00:00Z",
                fact_key="home-brew-method",
            )
            vp.query_writeback_chat_fact(
                page_title="Coffee Preferences",
                note="Marcus prefers espresso over pourover at home.",
                related_pages=["coffee"],
                created_at="2026-04-23T11:00:00Z",
                conversation_ref="chat:2026-04-23T11:00:00Z",
                fact_key="home-brew-method",
            )

            result = vp.query_writeback_chat_fact(
                page_title="Coffee Preferences",
                note="Marcus now prefers espresso over pourover at home.",
                related_pages=["coffee"],
                created_at="2026-04-23T12:00:00Z",
                conversation_ref="chat:2026-04-23T12:00:00Z",
                fact_key="home-brew-method",
                replacement_intent=True,
            )

            self.assertTrue(result.superseded_source_paths)
            page_text = (wiki_root / "coffee-preferences.md").read_text(encoding="utf-8")
            self.assertNotIn("## Open Questions", page_text)
            review_text = (wiki_root / "review.md").read_text(encoding="utf-8")
            self.assertIn("resolved | contradiction | home-brew-method", review_text)
            self.assertNotIn("open | contradiction | home-brew-method", review_text)

    def test_bootstrap_integration_reuses_pipeline_with_bootstrap_budget(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            budgeted_path = raw_root / "nested" / "budgeted-note-123.md"
            budgeted_path.parent.mkdir(parents=True)
            budgeted_path.write_text(
                vp.render_note(
                    {
                        "capture_id": "123",
                        "source_kind": "capture",
                        "source_id": "capture:123",
                        "title": "Budgeted Note",
                        "created_at": "2026-04-24T10:00:00Z",
                        "source_file": "Budgeted Note.md",
                    },
                    "# Budgeted Note\n\nMarcus prefers a broader bootstrap budget for first-pass integration.",
                ),
                encoding="utf-8",
            )

            result = vp.bootstrap_integrate_sources()

            self.assertEqual(result["processed_sources"], 1)
            self.assertIn("budgeted-note", result["changed_slugs"])
            page_text = (wiki_root / "budgeted-note.md").read_text(encoding="utf-8")
            self.assertIn("Marcus prefers a broader bootstrap budget for first-pass integration.", page_text)
            log_text = (wiki_root / "log.md").read_text(encoding="utf-8")
            self.assertIn("bootstrap | pipeline", log_text)
            self.assertIn('ingest | Capture: "Budgeted Note" | Router: heavy_update', log_text)

    def test_light_update_defers_new_atomic_page_without_outbound_links(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            raw_path = raw_root / "isolated-note-123.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Isolated Note",
                    created_at="2026-04-24T10:00:00Z",
                    source_file="Isolated Note.md",
                    body="A standalone note with no related pages yet.",
                ),
                encoding="utf-8",
            )

            outcome = vp._upsert_wiki_pages_for_note(
                frontmatter=vp.parse_raw_note(raw_path)[0],
                title="Isolated Note",
                body="A standalone note with no related pages yet.",
                raw_path=raw_path,
                budget=vp.MaintenanceBudget(max_candidate_pages=1, max_context_chars=10, max_pages_rewritten=4),
            )

            self.assertTrue(outcome.review_queued)
            self.assertIn("Deferred page 'isolated-note' until it has a meaningful outbound link.", outcome.deferred_items)
            self.assertFalse((wiki_root / "isolated-note.md").exists())
            review_text = (wiki_root / "review.md").read_text(encoding="utf-8")
            self.assertIn("light-update deferred work", review_text)

    def test_ingest_does_not_clone_source_body_to_folder_topic_assignment(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            (wiki_root / "coffee-grinder.md").write_text(
                "\n".join(
                    [
                        "# Coffee Grinder",
                        "",
                        "## Notes",
                        "",
                        "- Existing grinder note.",
                        "",
                        "## Connections",
                        "",
                        "- [[coffee]]",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            (wiki_root / "coffee.md").write_text(
                "\n".join(
                    [
                        "# Coffee",
                        "",
                        "## Connections",
                        "",
                        "- [[coffee-grinder]]",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            raw_path = raw_root / "coffee" / "coffee-grinder-123.md"
            raw_path.parent.mkdir(parents=True)
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Coffee Grinder",
                    created_at="2026-04-24T10:00:00Z",
                    source_file="Coffee Grinder.md",
                    body="Coffee tastes better when the grinder is dialed in.",
                ),
                encoding="utf-8",
            )

            outcome = vp._upsert_wiki_pages_for_note(
                frontmatter=vp.parse_raw_note(raw_path)[0],
                title="Coffee Grinder",
                body="Coffee tastes better when the grinder is dialed in.",
                raw_path=raw_path,
            )

            self.assertEqual(outcome.router_decision.action, "heavy_update")
            owner_text = (wiki_root / "coffee-grinder.md").read_text(encoding="utf-8")
            folder_text = (wiki_root / "coffee.md").read_text(encoding="utf-8")
            self.assertIn("Coffee tastes better when the grinder is dialed in.", owner_text)
            self.assertIn("../raw/coffee/coffee-grinder-123.md", owner_text)
            self.assertNotIn("Coffee tastes better when the grinder is dialed in.", folder_text)
            self.assertNotIn("../raw/coffee/coffee-grinder-123.md", folder_text)

    def test_lint_reports_invalid_shape_dead_citation_dead_link_orphan_and_open_question(self) -> None:
        with isolated_env() as (_, _, wiki_root, _):
            (wiki_root / "broken-topic.md").write_text(
                "\n".join(
                    [
                        "# Broken Topic",
                        "",
                        "## Notes",
                        "",
                        "- Should not be here.",
                        "",
                        "## Connections",
                        "",
                        "- [[missing-page]]",
                        "",
                        "## Sources",
                        "",
                        "- [Missing](../raw/missing.md)",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            (wiki_root / "isolated-note.md").write_text(
                "\n".join(
                    [
                        "# Isolated Note",
                        "",
                        "## Notes",
                        "",
                        "- A standalone note.",
                        "",
                        "## Open Questions",
                        "",
                        "- Is this still current?",
                        "",
                    ]
                ),
                encoding="utf-8",
            )

            report = vp.lint_wiki(append_review=True)

            findings = {(finding.kind, finding.slug) for finding in report.findings}
            self.assertIn(("invalid-page-shape", "isolated-note"), findings)
            self.assertIn(("dead-citation", "broken-topic"), findings)
            self.assertIn(("dead-link", "broken-topic"), findings)
            self.assertIn(("orphan", "broken-topic"), findings)
            self.assertIn(("missing-outbound-link", "isolated-note"), findings)
            self.assertIn(("contradiction-candidate", "isolated-note"), findings)
            self.assertGreater(report.review_updates, 0)

            review_text = (wiki_root / "review.md").read_text(encoding="utf-8")
            self.assertIn("lint | dead-citation", review_text)
            self.assertIn("lint | contradiction-candidate", review_text)
            self.assertNotIn("lint | orphan", review_text)

    def test_lint_resolves_percent_encoded_source_paths(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            raw_path = raw_root / "Apple Notes" / "Programming Note.md"
            raw_path.parent.mkdir(parents=True)
            raw_path.write_text("# Programming Note\n", encoding="utf-8")
            (wiki_root / "programming-note.md").write_text(
                "\n".join(
                    [
                        "# Programming Note",
                        "",
                        "## Notes",
                        "",
                        "- A source-backed note.",
                        "",
                        "## Connections",
                        "",
                        "- [[programming]]",
                        "",
                        "## Sources",
                        "",
                        "- [Programming Note](../raw/Apple%20Notes/Programming%20Note.md)",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            (wiki_root / "programming.md").write_text("# Programming\n\n## Connections\n\n- [[programming-note]]\n", encoding="utf-8")

            report = vp.lint_wiki(append_review=False)

            findings = {(finding.kind, finding.slug) for finding in report.findings}
            self.assertNotIn(("dead-citation", "programming-note"), findings)

    def test_trace_page_provenance_maps_note_to_persisted_source_artifact(self) -> None:
        with isolated_env() as (_, _, wiki_root, _):
            result = vp.query_writeback_chat_fact(
                page_title="Coffee Preferences",
                note="Marcus prefers pourover over espresso at home.",
                related_pages=["coffee"],
                created_at="2026-04-23T10:00:00Z",
                conversation_ref="chat:2026-04-23T10:00:00Z",
                fact_key="home-brew-method",
            )

            trace = vp.trace_page_provenance("coffee-preferences")

            self.assertIn("Marcus prefers pourover over espresso at home.", trace)
            assert result.source_path is not None
            expected_source = "../" + result.source_path.relative_to(vp.ROOT).as_posix()
            self.assertEqual(trace["Marcus prefers pourover over espresso at home."], [expected_source])
            self.assertTrue((wiki_root / "coffee-preferences.md").exists())

    def test_simple_ingest_acceptance_flow_updates_page_without_review(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            (wiki_root / "coffee.md").write_text(
                "\n".join(
                    [
                        "# Coffee",
                        "",
                        "## Notes",
                        "",
                        "- Coffee is a durable interest.",
                        "",
                        "## Connections",
                        "",
                        "- [[brewing]]",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            (wiki_root / "brewing.md").write_text(
                "\n".join(
                    [
                        "# Brewing",
                        "",
                        "## Connections",
                        "",
                        "- [[coffee]]",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            raw_path = raw_root / "coffee-123.md"
            raw_path.write_text(
                vp.render_note(
                    {
                        "capture_id": "123",
                        "source_kind": "capture",
                        "source_id": "capture:123",
                        "title": "Coffee",
                        "created_at": "2026-04-24T10:00:00Z",
                        "source_file": "Coffee.md",
                    },
                    "# Coffee\n\nCoffee tastes better when the grinder is dialed in.",
                ),
                encoding="utf-8",
            )

            outcome = vp._upsert_wiki_pages_for_note(
                frontmatter=vp.parse_raw_note(raw_path)[0],
                title="Coffee",
                body="Coffee tastes better when the grinder is dialed in.",
                raw_path=raw_path,
            )

            self.assertEqual(outcome.router_decision.action, "light_update")
            self.assertFalse(outcome.review_queued)
            page_text = (wiki_root / "coffee.md").read_text(encoding="utf-8")
            self.assertIn("Coffee tastes better when the grinder is dialed in.", page_text)
            self.assertIn("../raw/coffee-123.md", page_text)
            self.assertFalse((wiki_root / "review.md").exists())

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

    def test_numbered_placeholder_empty_note_is_deleted(self) -> None:
        with isolated_env() as (_, _, _, capture_root):
            note_path = capture_root / "Untitled 2.md"
            write_note(note_path, " \n\t")

            result = vp.capture_ingest(capture_root=capture_root)

            self.assertEqual(result["new_exports"], [])
            self.assertFalse(note_path.exists())
            payload = json.loads(vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(payload["event"], "empty_deleted")
            self.assertEqual(payload["filename"], "Untitled 2.md")

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
            self.assertNotIn("integrated_at", frontmatter)
            self.assertFalse((wiki_root / "note-1.md").exists())
            self.assertIn('ingest | Capture: "Note 1" | Router: heavy_update | Deferred: 1', (wiki_root / "log.md").read_text(encoding="utf-8"))
            self.assertIn("light-update deferred work", (wiki_root / "review.md").read_text(encoding="utf-8"))
            event = json.loads(vp.JSONL_LOG_PATH.read_text(encoding="utf-8").splitlines()[0])
            self.assertEqual(event["event"], "integrated")

    def test_ingest_raw_notes_skips_when_latest_event_is_integrated(self) -> None:
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
            vp.append_jsonl_event({"event": "integrated", "capture_id": "123", "raw_path": "raw/note-123.md"})

            result = vp.ingest_raw_notes([{"capture_id": "123", "raw_path": "raw/note-123.md"}])

            self.assertEqual(result["integrated"], [])
            self.assertEqual(result["skipped"], [{"capture_id": "123", "raw_path": "raw/note-123.md"}])

    def test_ingest_reuses_single_raw_source_across_grounded_split_children(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            raw_path = raw_root / "ics33-week1.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Lazy Sequences",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Lazy Sequences.md",
                    body="Iterators define traversal. Generators use yield to produce values lazily.",
                ),
                encoding="utf-8",
            )

            decision = vp.bw.PageSplitDecision(
                is_atomic=False,
                candidate_satellite_slugs=["iterators", "generators"],
                candidate_evaluations=[
                    vp.bw.SplitCandidateEvaluation(
                        slug="iterators",
                        accepted=True,
                        grounding=["Iterator protocol and traversal state."],
                        why_distinct="Consumer-facing traversal interface.",
                        passes_direct_link_test=True,
                        passes_stable_page_test=True,
                    ),
                    vp.bw.SplitCandidateEvaluation(
                        slug="generators",
                        accepted=True,
                        grounding=["Yield-based lazy value production."],
                        why_distinct="Producer-side lazy sequence construction.",
                        passes_direct_link_test=True,
                        passes_stable_page_test=True,
                    ),
                ],
            )

            with mock.patch.object(vp, "_resolve_synthesis_config", return_value=("token", "gemini-test")):
                with mock.patch.object(vp.bw, "analyze_page_for_atomic_split", return_value=decision):
                    result = vp.ingest_raw_notes([{"capture_id": "123", "raw_path": "raw/ics33-week1.md"}])

            self.assertEqual(result["integrated"], [{"capture_id": "123", "raw_path": "raw/ics33-week1.md"}])
            iterators_page = (wiki_root / "iterators.md").read_text(encoding="utf-8")
            generators_page = (wiki_root / "generators.md").read_text(encoding="utf-8")
            self.assertIn("(../raw/ics33-week1.md)", iterators_page)
            self.assertIn("(../raw/ics33-week1.md)", generators_page)

    def test_ingest_rejects_single_source_split_with_identical_child_notes(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            raw_path = raw_root / "recipe-list.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Recipe List",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Recipe List.md",
                    body="Soba noodles are served chilled. Ponzu sauce adds citrus and soy.",
                ),
                encoding="utf-8",
            )

            decision = vp.bw.PageSplitDecision(
                is_atomic=False,
                candidate_satellite_slugs=["soba-noodles", "ponzu-sauce"],
                candidate_evaluations=[
                    vp.bw.SplitCandidateEvaluation(
                        slug="soba-noodles",
                        accepted=True,
                        grounding=["Recipe List"],
                        passes_direct_link_test=True,
                        passes_stable_page_test=True,
                    ),
                    vp.bw.SplitCandidateEvaluation(
                        slug="ponzu-sauce",
                        accepted=True,
                        grounding=["Recipe List"],
                        passes_direct_link_test=True,
                        passes_stable_page_test=True,
                    ),
                ],
            )

            with mock.patch.object(vp, "_resolve_synthesis_config", return_value=("token", "gemini-test")):
                with mock.patch.object(vp.bw, "analyze_page_for_atomic_split", return_value=decision):
                    result = vp.ingest_raw_notes([{"capture_id": "123", "raw_path": "raw/recipe-list.md"}])

            self.assertEqual(result["integrated"], [{"capture_id": "123", "raw_path": "raw/recipe-list.md"}])
            self.assertFalse((wiki_root / "soba-noodles.md").exists())
            self.assertFalse((wiki_root / "ponzu-sauce.md").exists())

    def test_ingest_fetches_remote_summary_for_url_note_and_renders_literal_url_source(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            raw_path = raw_root / "chocolate-cake-123.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Chocolate Cake",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Chocolate Cake.md",
                    body="https://example.com/cake\n\nmy short note",
                ),
                encoding="utf-8",
            )

            with mock.patch.object(
                vp.bw,
                "fetch_url_summary",
                return_value=vp.bw.FetchResult("Chocolate Cake Recipe — Rich and easy.", "fetched"),
            ) as fetch_mock:
                result = vp.ingest_raw_notes([{"capture_id": "123", "raw_path": "raw/chocolate-cake-123.md"}])

            self.assertEqual(result["integrated"], [{"capture_id": "123", "raw_path": "raw/chocolate-cake-123.md"}])
            fetch_mock.assert_called_once_with("https://example.com/cake")
            self.assertFalse((wiki_root / "chocolate-cake.md").exists())
            review_text = (wiki_root / "review.md").read_text(encoding="utf-8")
            self.assertIn("light-update deferred work", review_text)
            self.assertIn("[../raw/chocolate-cake-123.md]", review_text)

    def test_ingest_skips_google_search_fetch_but_preserves_literal_url_source(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            raw_path = raw_root / "search-123.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Cake Search",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Cake Search.md",
                    body="https://www.google.com/search?q=chocolate+cake\n\nlook at later",
                ),
                encoding="utf-8",
            )

            with mock.patch.object(vp.bw, "fetch_url_summary") as fetch_mock:
                result = vp.ingest_raw_notes([{"capture_id": "123", "raw_path": "raw/search-123.md"}])

            self.assertEqual(result["integrated"], [{"capture_id": "123", "raw_path": "raw/search-123.md"}])
            fetch_mock.assert_not_called()
            self.assertFalse((wiki_root / "cake-search.md").exists())
            review_text = (wiki_root / "review.md").read_text(encoding="utf-8")
            self.assertIn("light-update deferred work", review_text)
            self.assertIn("[../raw/search-123.md]", review_text)

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

    def test_page_resynthesis_on_touch_preserves_connections_and_updates_index_log_without_mutating_raw(self) -> None:
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
            self.assertNotIn("integrated_at", frontmatter)

    def test_ingest_realizes_split_by_converting_clean_parent_to_topic(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            raw_path = raw_root / "fruit-desserts-123.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Fruit Desserts",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Fruit Desserts.md",
                    body="One note about apple pie technique and berry tart structure.",
                ),
                encoding="utf-8",
            )
            decision = vp.bw.PageSplitDecision(
                is_atomic=False,
                candidate_satellite_slugs=["apple-pie", "berry-tart"],
                rationale="This note contains two reusable dessert notes.",
                candidate_evaluations=[
                    vp.bw.SplitCandidateEvaluation(
                        slug="apple-pie",
                        accepted=True,
                        grounding=["Apple pie filling and crust technique are described directly."],
                        why_distinct="It is a standalone dessert with separate preparation concerns.",
                        passes_direct_link_test=True,
                        passes_stable_page_test=True,
                        passes_search_test=True,
                    ),
                    vp.bw.SplitCandidateEvaluation(
                        slug="berry-tart",
                        accepted=True,
                        grounding=["Berry tart assembly and texture cues are described directly."],
                        why_distinct="It remains a distinct dessert page as more baking notes are added.",
                        passes_direct_link_test=True,
                        passes_stable_page_test=True,
                        passes_search_test=True,
                    ),
                ],
            )

            with mock.patch.object(vp, "_resolve_synthesis_config", return_value=("token", "gemini-test")):
                with mock.patch.object(vp.bw, "analyze_page_for_atomic_split", return_value=decision):
                    result = vp.ingest_raw_notes([{"capture_id": "123", "raw_path": "raw/fruit-desserts-123.md"}])

            self.assertEqual(result["integrated"], [{"capture_id": "123", "raw_path": "raw/fruit-desserts-123.md"}])
            parent_text = (wiki_root / "fruit-desserts.md").read_text(encoding="utf-8")
            self.assertIn("## Connections", parent_text)
            self.assertIn("[[apple-pie]]", parent_text)
            self.assertIn("[[berry-tart]]", parent_text)
            self.assertNotIn("## Notes", parent_text)
            self.assertNotIn("## Sources", parent_text)

            apple_text = (wiki_root / "apple-pie.md").read_text(encoding="utf-8")
            berry_text = (wiki_root / "berry-tart.md").read_text(encoding="utf-8")
            self.assertIn("Apple pie filling and crust technique are described directly.", apple_text)
            self.assertIn("Berry tart assembly and texture cues are described directly.", berry_text)
            self.assertIn("[[fruit-desserts]]", apple_text)
            self.assertIn("[[fruit-desserts]]", berry_text)

    def test_ingest_realizes_split_by_deprecating_generic_parent_label(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            raw_path = raw_root / "recipe-123.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Recipe",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Recipe.md",
                    body="One note that really contains a pasta salad recipe and a roast chicken recipe.",
                ),
                encoding="utf-8",
            )
            decision = vp.bw.PageSplitDecision(
                is_atomic=False,
                candidate_satellite_slugs=["pasta-salad", "roast-chicken"],
                rationale="This generic umbrella should split into concrete dishes.",
                candidate_evaluations=[
                    vp.bw.SplitCandidateEvaluation(
                        slug="pasta-salad",
                        accepted=True,
                        grounding=["The source includes a distinct pasta salad ingredient list and method."],
                        why_distinct="It can be reused independently from the roast chicken material.",
                        passes_direct_link_test=True,
                        passes_stable_page_test=True,
                        passes_search_test=True,
                    ),
                    vp.bw.SplitCandidateEvaluation(
                        slug="roast-chicken",
                        accepted=True,
                        grounding=["The source includes a separate roast chicken method and timing notes."],
                        why_distinct="It remains useful as its own page as more cooking notes arrive.",
                        passes_direct_link_test=True,
                        passes_stable_page_test=True,
                        passes_search_test=True,
                    ),
                ],
            )

            with mock.patch.object(vp, "_resolve_synthesis_config", return_value=("token", "gemini-test")):
                with mock.patch.object(vp.bw, "analyze_page_for_atomic_split", return_value=decision):
                    vp.ingest_raw_notes([{"capture_id": "123", "raw_path": "raw/recipe-123.md"}])

            parent_text = (wiki_root / "recipe.md").read_text(encoding="utf-8")
            self.assertIn("Deprecated: superseded by [[pasta-salad]] and [[roast-chicken]].", parent_text)
            self.assertIn("[[pasta-salad]]", parent_text)
            self.assertIn("[[roast-chicken]]", parent_text)

    def test_ingest_realizes_split_while_preserving_atomic_parent_when_title_still_tracks_core_concept(self) -> None:
        with isolated_env() as (_, raw_root, wiki_root, _):
            raw_path = raw_root / "running-123.md"
            raw_path.write_text(
                vp.render_raw_file(
                    capture_id="123",
                    title="Running",
                    created_at="2026-04-20T17:32:00Z",
                    source_file="Running.md",
                    body="A note covering running form and a running training plan.",
                ),
                encoding="utf-8",
            )
            decision = vp.bw.PageSplitDecision(
                is_atomic=False,
                candidate_satellite_slugs=["running-form", "running-plan"],
                rationale="The parent can remain useful while linked child pages carry specific sub-areas.",
                candidate_evaluations=[
                    vp.bw.SplitCandidateEvaluation(
                        slug="running-form",
                        accepted=True,
                        grounding=["The source has concrete cues about posture, cadence, and foot strike."],
                        why_distinct="Form advice is reusable independently of planning advice.",
                        passes_direct_link_test=True,
                        passes_stable_page_test=True,
                        passes_search_test=True,
                    ),
                    vp.bw.SplitCandidateEvaluation(
                        slug="running-plan",
                        accepted=True,
                        grounding=["The source has a separate structure for weekly mileage and progression."],
                        why_distinct="Training plans remain stable as separate notes over time.",
                        passes_direct_link_test=True,
                        passes_stable_page_test=True,
                        passes_search_test=True,
                    ),
                ],
            )

            with mock.patch.object(vp, "_resolve_synthesis_config", return_value=("token", "gemini-test")):
                with mock.patch.object(vp.bw, "analyze_page_for_atomic_split", return_value=decision):
                    vp.ingest_raw_notes([{"capture_id": "123", "raw_path": "raw/running-123.md"}])

            parent_text = (wiki_root / "running.md").read_text(encoding="utf-8")
            self.assertIn("## Notes", parent_text)
            self.assertIn("[Running](../raw/running-123.md)", parent_text)
            self.assertIn("[[running-form]]", parent_text)
            self.assertIn("[[running-plan]]", parent_text)

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
            self.assertNotIn("integrated_at", raw_frontmatter)
            self.assertEqual(raw_body, "# i like eating grapes\n\n\n \t\n")

            wiki_page = wiki_root / "i-like-eating-grapes.md"
            self.assertFalse(wiki_page.exists())
            review_text = (wiki_root / "review.md").read_text(encoding="utf-8")
            self.assertIn("light-update deferred work", review_text)
            self.assertIn("[../raw/i-like-eating-grapes-", review_text)

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

    def test_run_main_suppresses_empty_pipeline_result(self) -> None:
        empty_result = {
            "capture_ingest": {"new_exports": [], "errors": []},
            "wiki_ingest": {"integrated": [], "skipped": [], "failed": []},
        }

        with isolated_env() as (_, _, _, capture_root):
            stdout = io.StringIO()
            with mock.patch.object(vp, "run_vault_pipeline", return_value=empty_result):
                with redirect_stdout(stdout):
                    result = vp.run_main(["--capture-root", str(capture_root)])

        self.assertEqual(result, 0)
        self.assertEqual(stdout.getvalue(), "")

    def test_run_main_prints_nonempty_pipeline_result(self) -> None:
        nonempty_result = {
            "capture_ingest": {
                "new_exports": [{"capture_id": "123", "raw_path": "raw/note-123.md"}],
                "errors": [],
            },
            "wiki_ingest": {
                "integrated": [{"capture_id": "123", "raw_path": "raw/note-123.md"}],
                "skipped": [],
                "failed": [],
            },
        }

        with isolated_env() as (_, _, _, capture_root):
            stdout = io.StringIO()
            with mock.patch.object(vp, "run_vault_pipeline", return_value=nonempty_result):
                with redirect_stdout(stdout):
                    result = vp.run_main(["--capture-root", str(capture_root)])

        self.assertEqual(result, 0)
        self.assertEqual(json.loads(stdout.getvalue()), nonempty_result)

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
            self.assertIn("index.md", written_paths)
            self.assertIn("log.md", written_paths)
            self.assertIn("review.md", written_paths)


if __name__ == "__main__":
    unittest.main()
