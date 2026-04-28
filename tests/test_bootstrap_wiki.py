import unittest
from contextlib import redirect_stderr
from io import StringIO
import sys
import tempfile
from unittest import mock
from pathlib import Path
from urllib.error import URLError

from scripts import bootstrap_wiki as bw


ROOT = Path(__file__).resolve().parents[1]
SYDNEY_ROOT = ROOT / "raw" / "Apple Notes" / "Marcus" / "Areas" / "Sydney"


def load_source(name: str) -> bw.SourceRecord:
    path = SYDNEY_ROOT / name
    content = path.read_text(encoding="utf-8", errors="ignore")
    return bw.prepare_source_record(
        source_label=bw.derive_note_title(path, content),
        source_path="../" + path.relative_to(ROOT).as_posix(),
        source_status="local_only",
        raw_content=content,
        fetched_summary=None,
        detected_url=bw.extract_first_url(content),
    )


class BootstrapWikiTests(unittest.TestCase):
    def test_validate_page_rejects_topic_with_atomic_sections(self) -> None:
        page = bw.Page(
            slug="desserts",
            title="Desserts",
            page_type="Concepts",
            summary_hint="Desserts",
            shape=bw.PAGE_SHAPE_TOPIC,
        )
        page.notes = ["Should not be here."]
        page.sources["../raw/desserts.md"] = bw.SourceRecord(
            label="Desserts",
            path="../raw/desserts.md",
            status="local_only",
            raw_content="",
            cleaned_text="",
            fetched_summary=None,
            detected_url=None,
        )

        issues = bw.validate_page(page)

        self.assertEqual(issues, ["topic-pages-cannot-have-notes", "topic-pages-cannot-have-sources"])

    def test_render_page_includes_open_questions_for_atomic_pages(self) -> None:
        page = bw.Page(
            slug="coffee",
            title="Coffee",
            page_type="Concepts",
            summary_hint="Coffee",
        )
        page.notes = ["Marcus prefers pourover."]
        page.open_questions = ["Is this still true when traveling?"]
        page.connections["pourover"] += 1

        rendered = bw.render_page(page)

        self.assertIn("## Open Questions", rendered)
        self.assertIn("- Is this still true when traveling?", rendered)

    def test_render_index_stays_compact_and_catalog_remains_exhaustive(self) -> None:
        topic = bw.Page(
            slug="coffee",
            title="Coffee",
            page_type="Concepts",
            summary_hint="Coffee",
            shape=bw.PAGE_SHAPE_TOPIC,
        )
        topic.connections["espresso"] += 1
        topic.connections["pourover"] += 1

        espresso = bw.Page(
            slug="espresso",
            title="Espresso",
            page_type="Concepts",
            summary_hint="Espresso",
        )
        espresso.notes = ["Concentrated coffee shot."]
        espresso.connections["coffee"] += 1

        obscure = bw.Page(
            slug="obscure-brewing-note",
            title="Obscure Brewing Note",
            page_type="Concepts",
            summary_hint="Obscure Brewing Note",
        )
        obscure.notes = ["Brew ratio note worth keeping."]
        obscure.connections["coffee"] += 1

        aeropress = bw.Page(
            slug="aeropress",
            title="Aeropress",
            page_type="Concepts",
            summary_hint="Aeropress",
        )
        aeropress.notes = ["Portable brew method."]
        aeropress.connections["espresso"] += 1
        grinder = bw.Page(
            slug="grinder",
            title="Grinder",
            page_type="Concepts",
            summary_hint="Grinder",
        )
        grinder.notes = ["Grind consistency matters."]
        grinder.connections["espresso"] += 1
        beans = bw.Page(
            slug="beans",
            title="Beans",
            page_type="Concepts",
            summary_hint="Beans",
        )
        beans.notes = ["Origin affects flavor."]
        beans.connections["espresso"] += 1

        pages = {
            "coffee": topic,
            "espresso": espresso,
            "obscure-brewing-note": obscure,
            "aeropress": aeropress,
            "grinder": grinder,
            "beans": beans,
        }

        index_text = bw.render_index(pages)
        catalog_text = bw.render_catalog(pages)

        self.assertIn("[[coffee]]", index_text)
        self.assertIn("[[espresso]]", index_text)
        self.assertNotIn("[[obscure-brewing-note]]", index_text)
        self.assertIn("[[obscure-brewing-note]]", catalog_text)
        self.assertIn("[[catalog]]", index_text)

    def test_load_existing_wiki_pages_uses_catalog_for_page_types(self) -> None:
        page = bw.Page(
            slug="obscure-brewing-note",
            title="Obscure Brewing Note",
            page_type="Experiences",
            summary_hint="Obscure Brewing Note",
        )
        page.notes = ["One-off cafe observation."]
        page.connections["coffee"] += 1

        coffee = bw.Page(
            slug="coffee",
            title="Coffee",
            page_type="Concepts",
            summary_hint="Coffee",
            shape=bw.PAGE_SHAPE_TOPIC,
        )
        coffee.connections["obscure-brewing-note"] += 1

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            wiki_root = root / "wiki"
            wiki_root.mkdir()

            with bw.temporary_workspace(root):
                bw.atomic_write_text(wiki_root / "obscure-brewing-note.md", bw.render_page(page))
                bw.atomic_write_text(wiki_root / "coffee.md", bw.render_page(coffee))
                bw.atomic_write_text(wiki_root / "index.md", bw.render_index({"obscure-brewing-note": page, "coffee": coffee}))
                bw.atomic_write_text(wiki_root / bw.CATALOG_PATH, bw.render_catalog({"obscure-brewing-note": page, "coffee": coffee}))
                bw.atomic_write_text(wiki_root / "log.md", "# Wiki Log\n")

                parsed_pages = bw.load_existing_wiki_pages()

        self.assertEqual(parsed_pages["obscure-brewing-note"].page_type, "Experiences")

    def test_bucket_scoring_recipe_like_page_crosses_threshold(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Pesto Eggs",
                path="../raw/pesto-eggs.md",
                status="local_only",
                raw_content="",
                cleaned_text="Recipe note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Vodka Pasta",
                path="../raw/vodka-pasta.md",
                status="local_only",
                raw_content="",
                cleaned_text="Recipe note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Miso Salmon",
                path="../raw/miso-salmon.md",
                status="local_only",
                raw_content="",
                cleaned_text="Recipe note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="recipe",
            title="Recipe",
            page_type="Concepts",
            summary_hint="Recipe",
            sources={source.path: source for source in sources},
        )
        page.rendered_notes_markdown = "\n".join(
            [
                "### Breakfast",
                "- Pesto eggs",
                "### Dinner",
                "- Vodka pasta",
            ]
        )

        result = bw.score_bucket_signals(page)

        self.assertTrue(result.is_bucket_signaled)
        self.assertIn("generic-parent-slug", result.reasons)
        self.assertIn("multi-cluster-notes", result.reasons)
        self.assertIn("heterogeneous-sources", result.reasons)

    def test_bucket_scoring_uci_like_page_crosses_threshold(self) -> None:
        sources = [
            bw.SourceRecord(
                label="ICS 33",
                path="../raw/ics-33.md",
                status="local_only",
                raw_content="",
                cleaned_text="Course note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="UCI Food",
                path="../raw/uci-food.md",
                status="local_only",
                raw_content="",
                cleaned_text="Food note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="ARC Gym",
                path="../raw/arc-gym.md",
                status="local_only",
                raw_content="",
                cleaned_text="Gym note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="uci",
            title="UCI",
            page_type="Entities",
            summary_hint="UCI",
            sources={source.path: source for source in sources},
        )
        page.connections["ics-33"] += 1
        page.connections["uci-food"] += 1
        page.connections["arc-gym"] += 1

        result = bw.score_bucket_signals(page)

        self.assertTrue(result.is_bucket_signaled)
        self.assertIn("heterogeneous-sources", result.reasons)
        self.assertIn("existing-satellites", result.reasons)

    def test_bucket_scoring_atomic_page_with_subpoints_stays_below_threshold(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Moka Pot Tips",
                path="../raw/moka-pot-a.md",
                status="local_only",
                raw_content="",
                cleaned_text="Brew ratio note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Moka Pot Cleaning",
                path="../raw/moka-pot-b.md",
                status="local_only",
                raw_content="",
                cleaned_text="Cleaning note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="moka-pot",
            title="Moka Pot",
            page_type="Concepts",
            summary_hint="Moka Pot",
            sources={source.path: source for source in sources},
        )
        page.rendered_notes_markdown = "\n".join(
            [
                "### Brewing",
                "- Lower heat helps.",
                "### Cleaning",
                "- Rinse only.",
            ]
        )

        result = bw.score_bucket_signals(page)

        self.assertFalse(result.is_bucket_signaled)
        self.assertEqual(result.reasons, ["multi-cluster-notes"])

    def test_bucket_scoring_lecture_page_remains_atomic(self) -> None:
        sources = [
            bw.SourceRecord(
                label="ICS 33 Week 1",
                path="../raw/ics33-week1.md",
                status="local_only",
                raw_content="",
                cleaned_text="Functions and recursion.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="ICS 33 Week 2",
                path="../raw/ics33-week2.md",
                status="local_only",
                raw_content="",
                cleaned_text="Classes and iterators.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="ics-33",
            title="ICS 33",
            page_type="Concepts",
            summary_hint="ICS 33",
            sources={source.path: source for source in sources},
        )
        page.notes = ["Lecture notes on Python abstractions.", "Covered recursion and iterators."]

        result = bw.score_bucket_signals(page)

        self.assertFalse(result.is_bucket_signaled)
        self.assertEqual(result.reasons, [])

    def test_course_folder_name_is_not_treated_as_archive(self) -> None:
        self.assertEqual(bw.clean_component("ICS 33"), "ics-33")

    def test_bucket_scoring_structured_course_page_ignores_lecture_shape(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Week 1 Recursion",
                path="../raw/Apple Notes/Marcus/Archives/ICS 33/week-1-recursion.md",
                status="local_only",
                raw_content="",
                cleaned_text="Lecture 1 covered recursion and tracing recursive calls.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Week 2 Iterators",
                path="../raw/Apple Notes/Marcus/Archives/ICS 33/week-2-iterators.md",
                status="local_only",
                raw_content="",
                cleaned_text="Lecture 2 covered generators and iterators.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Midterm Review",
                path="../raw/Apple Notes/Marcus/Archives/ICS 33/midterm-review.md",
                status="local_only",
                raw_content="",
                cleaned_text="Midterm review topics and practice questions.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="ics-33",
            title="ICS 33",
            page_type="Concepts",
            summary_hint="ICS 33",
            sources={source.path: source for source in sources},
        )
        page.rendered_notes_markdown = "\n".join(
            [
                "### Week 1",
                "- Recursion and tracing calls.",
                "### Week 2",
                "- Iterators and generators.",
                "### Midterm Review",
                "- Practice prompts.",
            ]
        )

        result = bw.score_bucket_signals(page)

        self.assertFalse(result.is_bucket_signaled)
        self.assertEqual(result.reasons, [])

    def test_sydney_source_tagging_and_sensitive_filtering(self) -> None:
        pin_source = load_source("Ebt pin is 9926.md")
        gift_source = load_source("if u need a christmas idea i like this fruity floral pink prada triangle….md")
        trip_source = load_source("YIPPEE TRIP TG.md")

        self.assertIn("sensitive", pin_source.tags)
        self.assertTrue(pin_source.excluded_from_body)

        self.assertIn("gift_ideas", gift_source.tags)
        self.assertFalse(gift_source.excluded_from_body)

        self.assertTrue({"activities", "travel_plans", "places"} <= trip_source.tags)
        self.assertFalse(trip_source.excluded_from_body)

    def test_entity_prompt_excludes_sensitive_sources_and_biases_sections(self) -> None:
        page = bw.Page(
            slug="sydney",
            title="Sydney",
            page_type="Entities",
            summary_hint="Sydney",
            sources={},
        )
        safe_source = load_source("Sydney birthday July 20.md")
        sensitive_source = load_source("Ebt pin is 9926.md")
        page.sources[safe_source.path] = safe_source
        page.sources[sensitive_source.path] = sensitive_source

        prompt_blob = bw.serialize_sources_for_prompt(page)
        messages = bw.build_synthesis_messages(page)
        rendered = bw.render_page(page)

        self.assertIn("Sydney birthday July 20", prompt_blob)
        self.assertNotIn("Ebt pin is 9926", prompt_blob)
        self.assertIn("### Key Dates", messages[1]["content"])
        self.assertIn("must not be surfaced", messages[1]["content"])
        self.assertIn("[Ebt pin is 9926]", rendered)

    def test_single_source_structured_notes_preserve_lists(self) -> None:
        source = bw.SourceRecord(
            label="Things I Like",
            path="../raw/example.md",
            status="local_only",
            raw_content="",
            cleaned_text="## Gift Ideas\n\n- Rug making\n- Asian bakery",
            fetched_summary=None,
            detected_url=None,
            tags={"gift_ideas"},
            excluded_from_body=False,
        )
        page = bw.Page(
            slug="example",
            title="Example",
            page_type="Concepts",
            summary_hint="Example",
            sources={source.path: source},
        )

        notes = bw.build_simple_notes_markdown(page)

        self.assertIn("## Gift Ideas", notes)
        self.assertIn("- Rug making", notes)
        self.assertIn("- Asian bakery", notes)

    def test_single_source_page_with_auto_excerpt_does_not_expand_to_full_source_body(self) -> None:
        full_body = " ".join(f"detail-{index}" for index in range(80))
        source = bw.SourceRecord(
            label="Long Source",
            path="../raw/long-source.md",
            status="local_only",
            raw_content="",
            cleaned_text=full_body,
            fetched_summary=None,
            detected_url=None,
        )
        page = bw.Page(
            slug="long-source",
            title="Long Source",
            page_type="Concepts",
            summary_hint="Long Source",
            sources={source.path: source},
        )
        page.connections["related-page"] += 1
        page.notes.append(bw.compact_source_text(source, limit=220).strip())

        rendered = bw.render_page(page)

        self.assertIn("## Notes", rendered)
        self.assertIn("detail-0", rendered)
        self.assertNotIn("detail-79", rendered)
        self.assertIn("## Sources", rendered)

    def test_cached_bootstrap_note_uses_single_content_owner_for_source_body(self) -> None:
        source = bw.prepare_source_record(
            source_label="Coffee Grinder",
            source_path="../raw/coffee/Coffee Grinder.md",
            source_status="local_only",
            raw_content="Coffee tastes better when the grinder is dialed in.",
            fetched_summary=None,
            detected_url=None,
        )
        cache_entry = bw.build_note_cache_entry(
            fingerprint="abc",
            title="Coffee Grinder",
            source_record=source,
            note_text="Coffee tastes better when the grinder is dialed in.",
            page_assignments=[("coffee-grinder", "title"), ("coffee", "folder")],
            skipped=False,
        )
        pages: dict[str, bw.Page] = {}

        bw.apply_note_cache_entry_to_pages(pages=pages, cache_entry=cache_entry)

        self.assertIn(source.path, pages["coffee-grinder"].sources)
        self.assertIn("Coffee tastes better when the grinder is dialed in.", pages["coffee-grinder"].notes)
        self.assertNotIn(source.path, pages["coffee"].sources)
        self.assertNotIn("Coffee tastes better when the grinder is dialed in.", pages["coffee"].notes)
        self.assertIn("coffee", pages["coffee-grinder"].connections)

    def test_atomic_page_renderer_omits_boilerplate_and_keeps_real_sections(self) -> None:
        source = bw.SourceRecord(
            label="Example Source",
            path="../raw/example.md",
            status="local_only",
            raw_content="",
            cleaned_text="One concrete fact.",
            fetched_summary=None,
            detected_url=None,
        )
        page = bw.Page(
            slug="example",
            title="Example",
            page_type="Concepts",
            summary_hint="Example",
            sources={source.path: source},
        )
        page.connections["related-page"] += 1

        rendered = bw.render_page(page)

        self.assertIn("# Example", rendered)
        self.assertIn("## Notes", rendered)
        self.assertIn("## Connections", rendered)
        self.assertIn("## Sources", rendered)
        self.assertNotIn("This page collects Marcus's notes", rendered)
        self.assertNotIn("No notes yet", rendered)
        self.assertNotIn("No sources linked yet", rendered)
        self.assertNotIn("unclassified-media-captures", rendered)

    def test_render_source_lines_prefers_literal_detected_url(self) -> None:
        source = bw.SourceRecord(
            label="Chocolate Cake",
            path="../raw/chocolate-cake.md",
            status="local_only",
            raw_content="",
            cleaned_text="Cake recipe.",
            fetched_summary=None,
            detected_url="https://example.com/cake",
        )
        page = bw.Page(
            slug="chocolate-cake",
            title="Chocolate Cake",
            page_type="Concepts",
            summary_hint="Chocolate Cake",
            sources={source.path: source},
        )

        self.assertEqual(
            bw.render_source_lines(page),
            ["- [https://example.com/cake](../raw/chocolate-cake.md)"],
        )

    def test_parse_source_line_restores_detected_url_from_literal_label(self) -> None:
        parsed = bw.parse_source_line("- [https://example.com/cake](../raw/chocolate-cake.md)")

        assert parsed is not None
        self.assertEqual(parsed.detected_url, "https://example.com/cake")

    def test_parse_source_line_handles_parentheses_in_target(self) -> None:
        parsed = bw.parse_source_line("- [Chanko Nabe](../raw/Food/Chanko Nabe (Sumo Stew).md)")

        assert parsed is not None
        self.assertEqual(parsed.path, "../raw/Food/Chanko Nabe (Sumo Stew).md")

    def test_render_source_lines_wraps_unsafe_target_in_angle_brackets(self) -> None:
        source = bw.SourceRecord(
            label="Chanko Nabe",
            path="../raw/Food/Chanko Nabe (Sumo Stew).md",
            status="local_only",
            raw_content="",
            cleaned_text="Recipe.",
            fetched_summary=None,
            detected_url=None,
        )
        page = bw.Page(
            slug="chanko-nabe",
            title="Chanko Nabe",
            page_type="Resources",
            summary_hint="Chanko Nabe",
            sources={source.path: source},
        )

        self.assertEqual(
            bw.render_source_lines(page),
            ["- [Chanko Nabe](<../raw/Food/Chanko Nabe (Sumo Stew).md>)"],
        )

    def test_atomic_page_with_only_notes_and_link_omits_sources(self) -> None:
        page = bw.Page(
            slug="example",
            title="Example",
            page_type="Concepts",
            summary_hint="Example",
            notes=["One atomic note."],
        )
        page.connections["related-page"] += 1

        rendered = bw.render_page(page)

        self.assertIn("## Notes", rendered)
        self.assertIn("## Connections", rendered)
        self.assertNotIn("## Sources", rendered)

    def test_topic_page_renders_only_title_and_connections(self) -> None:
        page = bw.Page(
            slug="travel-bags",
            title="Travel Bags",
            page_type="Concepts",
            summary_hint="Travel Bags",
            shape=bw.PAGE_SHAPE_TOPIC,
        )
        page.connections["backpack"] += 1
        page.connections["duffel-bag"] += 1

        rendered = bw.render_page(page)

        self.assertEqual(
            rendered,
            "# Travel Bags\n\n## Connections\n\n- [[backpack]]\n- [[duffel-bag]]\n",
        )

    def test_single_source_page_stays_atomic_after_migration(self) -> None:
        source = bw.SourceRecord(
            label="Pocket Knife Review",
            path="../raw/pocket-knife.md",
            status="local_only",
            raw_content="",
            cleaned_text="Compact everyday carry knife.",
            fetched_summary=None,
            detected_url=None,
        )
        pages = {
            "pocket-knife": bw.Page(
                slug="pocket-knife",
                title="Pocket Knife",
                page_type="Concepts",
                summary_hint="Pocket Knife",
                sources={source.path: source},
            )
        }

        bw.migrate_pages_to_atomic_topics(pages, {})
        bw.ensure_meaningful_connections(pages)
        bw.finalize_page_shapes(pages)

        self.assertEqual(pages["pocket-knife"].shape, bw.PAGE_SHAPE_ATOMIC)

    def test_three_satellites_convert_existing_slug_to_topic(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Backpack",
                path="../raw/backpack.md",
                status="local_only",
                raw_content="",
                cleaned_text="Backpack note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Duffel Bag",
                path="../raw/duffel.md",
                status="local_only",
                raw_content="",
                cleaned_text="Duffel note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Sling Bag",
                path="../raw/sling.md",
                status="local_only",
                raw_content="",
                cleaned_text="Sling note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="travel-bags",
            title="Travel Bags",
            page_type="Concepts",
            summary_hint="Travel Bags",
            sources={source.path: source for source in sources},
        )
        existing = {
            "travel-bags": bw.ParsedWikiPage(
                slug="travel-bags",
                title="Travel Bags",
                page_type="Concepts",
                shape=bw.PAGE_SHAPE_ATOMIC,
            )
        }

        pages = {"travel-bags": page}
        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(
                    is_atomic=False,
                    candidate_satellite_slugs=["backpack", "duffel-bag", "sling-bag"],
                    source_assignments={
                        "../raw/backpack.md": "backpack",
                        "../raw/duffel.md": "duffel-bag",
                        "../raw/sling.md": "sling-bag",
                    },
                ),
            ):
                bw.migrate_pages_to_atomic_topics(pages, existing, api_key="token")
            bw.ensure_meaningful_connections(pages)
            bw.finalize_page_shapes(pages)

        self.assertEqual(pages["travel-bags"].shape, bw.PAGE_SHAPE_TOPIC)
        self.assertIn("backpack", pages)
        self.assertIn("duffel-bag", pages)
        self.assertIn("sling-bag", pages)
        self.assertEqual(pages["backpack"].topic_parent, "travel-bags")
        self.assertIn("backpack", bw.sorted_connection_slugs(pages["travel-bags"]))

    def test_parse_page_split_response_keeps_single_source_candidate_debug(self) -> None:
        content = """
        {
          "is_atomic": false,
          "rationale": "One lecture note substantively covers two reusable concepts.",
          "rejection_reasons": [],
          "candidate_satellite_slugs": ["iterators", "generators"],
          "candidate_evaluations": [
            {
              "slug": "iterators",
              "accepted": true,
              "grounding": ["Defines iterator protocol and traversal behavior."],
              "why_distinct": "Focuses on the consumer-side iteration interface.",
              "passes_direct_link_test": true,
              "passes_stable_page_test": true,
              "passes_search_test": true,
              "rejection_reasons": []
            },
            {
              "slug": "generators",
              "accepted": true,
              "grounding": ["Explains yield-based lazy sequence construction."],
              "why_distinct": "Focuses on producing iterator values lazily.",
              "passes_direct_link_test": true,
              "passes_stable_page_test": true,
              "passes_search_test": true,
              "rejection_reasons": []
            },
            {
              "slug": "week-1",
              "accepted": false,
              "grounding": ["Appears only as a lecture heading."],
              "why_distinct": "It is just a source-shaped section label.",
              "passes_direct_link_test": false,
              "passes_stable_page_test": false,
              "passes_search_test": false,
              "rejection_reasons": ["source-shaped bucket"]
            }
          ],
          "source_assignments": []
        }
        """

        decision = bw.parse_page_split_response(content, "ics-33", {"../raw/ics33-week1.md"})

        self.assertFalse(decision.is_atomic)
        self.assertEqual(decision.candidate_satellite_slugs, ["iterators", "generators"])
        self.assertEqual(decision.rationale, "One lecture note substantively covers two reusable concepts.")
        self.assertEqual(len(decision.candidate_evaluations), 3)
        self.assertEqual(decision.candidate_evaluations[0].grounding, ["Defines iterator protocol and traversal behavior."])
        self.assertEqual(decision.candidate_evaluations[2].rejection_reasons, ["source-shaped bucket"])
        self.assertEqual(decision.source_assignments, {})

    def test_analyze_page_for_atomic_split_requests_debuggable_single_source_schema(self) -> None:
        source = bw.SourceRecord(
            label="ICS 33 Iterators and Generators",
            path="../raw/ics33-week1.md",
            status="local_only",
            raw_content="",
            cleaned_text="Iterators define traversal. Generators use yield to produce values lazily.",
            fetched_summary=None,
            detected_url=None,
        )
        page = bw.Page(
            slug="ics-33",
            title="ICS 33",
            page_type="Concepts",
            summary_hint="ICS 33",
            sources={source.path: source},
        )
        page.rendered_notes_markdown = "### Iterators\n- Traversal protocol.\n### Generators\n- Yield-based lazy production."

        llm_payload = """
        {
          "is_atomic": false,
          "rationale": "This single source contains two independently reusable ideas.",
          "rejection_reasons": [],
          "candidate_satellite_slugs": ["iterators", "generators"],
          "candidate_evaluations": [
            {
              "slug": "iterators",
              "accepted": true,
              "grounding": ["Traversal protocol and StopIteration behavior."],
              "why_distinct": "Consumers of iterable state.",
              "passes_direct_link_test": true,
              "passes_stable_page_test": true,
              "passes_search_test": true,
              "rejection_reasons": []
            },
            {
              "slug": "generators",
              "accepted": true,
              "grounding": ["Yield-based lazy production and generator objects."],
              "why_distinct": "Producer-side abstraction for lazy sequences.",
              "passes_direct_link_test": true,
              "passes_stable_page_test": true,
              "passes_search_test": true,
              "rejection_reasons": []
            }
          ],
          "source_assignments": []
        }
        """

        with mock.patch.object(bw, "gemini_generate", return_value=llm_payload) as generate_mock:
            decision = bw.analyze_page_for_atomic_split(page, api_key="token", model="gemini-test")

        kwargs = generate_mock.call_args.kwargs
        self.assertIn("Do not require multiple source files.", kwargs["messages"][1]["content"])
        self.assertIn("grounding must be usable as that child page's ## Notes", kwargs["messages"][1]["content"])
        self.assertIn("Same-source sibling children are allowed only when", kwargs["messages"][1]["content"])
        self.assertIn("candidate_evaluations", kwargs["response_schema"]["required"])
        self.assertFalse(decision.is_atomic)
        self.assertEqual(decision.candidate_satellite_slugs, ["iterators", "generators"])
        self.assertEqual(decision.candidate_evaluations[0].slug, "iterators")

    def test_apply_split_decision_reuses_single_source_across_grounded_children(self) -> None:
        source = bw.SourceRecord(
            label="ICS 33 Iterators and Generators",
            path="../raw/ics33-week1.md",
            status="local_only",
            raw_content="",
            cleaned_text="Iterators define traversal. Generators use yield to produce values lazily.",
            fetched_summary=None,
            detected_url=None,
        )
        pages = {
            "lazy-sequences": bw.Page(
                slug="lazy-sequences",
                title="Lazy Sequences",
                page_type="Concepts",
                summary_hint="Lazy Sequences",
                sources={source.path: source},
            )
        }
        decision = bw.PageSplitDecision(
            is_atomic=False,
            candidate_satellite_slugs=["iterators", "generators"],
            candidate_evaluations=[
                bw.SplitCandidateEvaluation(
                    slug="iterators",
                    accepted=True,
                    grounding=["Iterator protocol and traversal state."],
                    why_distinct="Consumer-facing traversal interface.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
                bw.SplitCandidateEvaluation(
                    slug="generators",
                    accepted=True,
                    grounding=["Yield-based lazy value production."],
                    why_distinct="Producer-side lazy sequence construction.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
            ],
        )

        applied = bw.apply_split_decision(pages, "lazy-sequences", decision, seed_kind="ingest", allow_partial_source_coverage=True)

        self.assertTrue(applied)
        self.assertIn(source.path, pages["iterators"].sources)
        self.assertIn(source.path, pages["generators"].sources)
        self.assertNotIn(source.cleaned_text, pages["iterators"].notes)
        self.assertNotIn(source.cleaned_text, pages["generators"].notes)

    def test_apply_split_decision_rejects_same_source_cloned_child_notes(self) -> None:
        source = bw.SourceRecord(
            label="Soba Noodles With Ponzu Sauce",
            path="../raw/soba-ponzu.md",
            status="local_only",
            raw_content="",
            cleaned_text="Soba noodles are served chilled. Ponzu sauce adds citrus and soy.",
            fetched_summary=None,
            detected_url=None,
        )
        pages = {
            "recipe": bw.Page(
                slug="recipe",
                title="Recipe",
                page_type="Concepts",
                summary_hint="Recipe",
                sources={source.path: source},
            )
        }
        decision = bw.PageSplitDecision(
            is_atomic=False,
            candidate_satellite_slugs=["soba-noodles", "ponzu-sauce"],
            candidate_evaluations=[
                bw.SplitCandidateEvaluation(
                    slug="soba-noodles",
                    accepted=True,
                    grounding=["Soba Noodles With Ponzu Sauce"],
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
                bw.SplitCandidateEvaluation(
                    slug="ponzu-sauce",
                    accepted=True,
                    grounding=["Soba Noodles With Ponzu Sauce"],
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
            ],
        )

        applied = bw.apply_split_decision(pages, "recipe", decision, seed_kind="ingest", allow_partial_source_coverage=True)

        self.assertFalse(applied)
        self.assertNotIn("soba-noodles", pages)
        self.assertNotIn("ponzu-sauce", pages)

    def test_validate_split_child_grounding_rejects_source_label_or_parent_note_only(self) -> None:
        source = bw.SourceRecord(
            label="Soba Noodles With Ponzu Sauce",
            path="../raw/soba-ponzu.md",
            status="local_only",
            raw_content="",
            cleaned_text="Soba noodles are served chilled. Ponzu sauce adds citrus and soy.",
            fetched_summary=None,
            detected_url=None,
        )
        parent = bw.Page(
            slug="recipe",
            title="Recipe",
            page_type="Concepts",
            summary_hint="Recipe",
            notes=["Ponzu sauce adds citrus and soy."],
            sources={source.path: source},
        )
        decision = bw.PageSplitDecision(
            is_atomic=False,
            candidate_satellite_slugs=["soba-noodles", "ponzu-sauce", "dipping-sauce"],
            candidate_evaluations=[
                bw.SplitCandidateEvaluation(
                    slug="soba-noodles",
                    accepted=True,
                    grounding=["Soba noodles are served chilled."],
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
                bw.SplitCandidateEvaluation(
                    slug="ponzu-sauce",
                    accepted=True,
                    grounding=["Soba Noodles With Ponzu Sauce"],
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
                bw.SplitCandidateEvaluation(
                    slug="dipping-sauce",
                    accepted=True,
                    grounding=["Ponzu sauce adds citrus and soy."],
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
            ],
        )
        source_groups = {
            "soba-noodles": [source],
            "ponzu-sauce": [source],
            "dipping-sauce": [source],
        }

        validated = bw.validate_split_child_grounding(parent, decision, source_groups)

        self.assertTrue(validated.is_atomic)
        self.assertIn("source-label-only child grounding", validated.rejection_reasons)
        self.assertIn("parent-note-identical child grounding", validated.rejection_reasons)
        self.assertIn("child grounding collapsed below split threshold", validated.rejection_reasons)

    def test_apply_split_decision_skips_incidental_single_source_reuse(self) -> None:
        source = bw.SourceRecord(
            label="ICS 33 Iterators and Generators",
            path="../raw/ics33-week1.md",
            status="local_only",
            raw_content="",
            cleaned_text="Iterators define traversal. Generators use yield to produce values lazily.",
            fetched_summary=None,
            detected_url=None,
        )
        pages = {
            "lazy-sequences": bw.Page(
                slug="lazy-sequences",
                title="Lazy Sequences",
                page_type="Concepts",
                summary_hint="Lazy Sequences",
                sources={source.path: source},
            )
        }
        decision = bw.PageSplitDecision(
            is_atomic=False,
            candidate_satellite_slugs=["iterators", "searching"],
            candidate_evaluations=[
                bw.SplitCandidateEvaluation(
                    slug="iterators",
                    accepted=True,
                    grounding=["Iterator protocol and traversal state."],
                    why_distinct="Consumer-facing traversal interface.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
                bw.SplitCandidateEvaluation(
                    slug="searching",
                    accepted=True,
                    grounding=[],
                    why_distinct="Only a passing mention in the lecture note.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
            ],
        )

        applied = bw.apply_split_decision(pages, "lazy-sequences", decision, seed_kind="ingest", allow_partial_source_coverage=True)

        self.assertTrue(applied)
        self.assertIn(source.path, pages["iterators"].sources)
        self.assertNotIn(source.path, pages["searching"].sources)

    def test_apply_split_decision_is_stable_when_re_run_on_unchanged_source(self) -> None:
        source = bw.SourceRecord(
            label="ICS 33 Iterators and Generators",
            path="../raw/ics33-week1.md",
            status="local_only",
            raw_content="",
            cleaned_text="Iterators define traversal. Generators use yield to produce values lazily.",
            fetched_summary=None,
            detected_url=None,
        )
        pages = {
            "lazy-sequences": bw.Page(
                slug="lazy-sequences",
                title="Lazy Sequences",
                page_type="Concepts",
                summary_hint="Lazy Sequences",
                sources={source.path: source},
            )
        }
        decision = bw.PageSplitDecision(
            is_atomic=False,
            candidate_satellite_slugs=["iterators", "generators"],
            candidate_evaluations=[
                bw.SplitCandidateEvaluation(
                    slug="iterators",
                    accepted=True,
                    grounding=["Iterator protocol and traversal state."],
                    why_distinct="Consumer-facing traversal interface.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
                bw.SplitCandidateEvaluation(
                    slug="generators",
                    accepted=True,
                    grounding=["Yield-based lazy value production."],
                    why_distinct="Producer-side lazy sequence construction.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
            ],
        )

        first = bw.apply_split_decision(pages, "lazy-sequences", decision, seed_kind="ingest", allow_partial_source_coverage=True)
        snapshot = {
            slug: (
                page.shape,
                tuple(page.notes),
                tuple(sorted(page.sources)),
                page.topic_parent,
            )
            for slug, page in sorted(pages.items())
        }
        second = bw.apply_split_decision(pages, "lazy-sequences", decision, seed_kind="ingest", allow_partial_source_coverage=True)
        rerun_snapshot = {
            slug: (
                page.shape,
                tuple(page.notes),
                tuple(sorted(page.sources)),
                page.topic_parent,
            )
            for slug, page in sorted(pages.items())
        }

        self.assertTrue(first)
        self.assertTrue(second)
        self.assertEqual(snapshot, rerun_snapshot)

    def test_maybe_apply_query_time_split_fix_rewrites_only_affected_pages_and_logs(self) -> None:
        source = bw.SourceRecord(
            label="ICS 33 Iterators and Generators",
            path="../raw/ics33-week1.md",
            status="local_only",
            raw_content="",
            cleaned_text="Iterators define traversal. Generators use yield to produce values lazily.",
            fetched_summary=None,
            detected_url=None,
        )
        pages = {
            "lazy-sequences": bw.Page(
                slug="lazy-sequences",
                title="Lazy Sequences",
                page_type="Concepts",
                summary_hint="Lazy Sequences",
                sources={source.path: source},
            )
        }
        decision = bw.PageSplitDecision(
            is_atomic=False,
            candidate_satellite_slugs=["iterators", "generators"],
            candidate_evaluations=[
                bw.SplitCandidateEvaluation(
                    slug="iterators",
                    accepted=True,
                    grounding=["Iterator protocol and traversal state."],
                    why_distinct="Consumer-facing traversal interface.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
                bw.SplitCandidateEvaluation(
                    slug="generators",
                    accepted=True,
                    grounding=["Yield-based lazy value production."],
                    why_distinct="Producer-side lazy sequence construction.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            wiki_root = root / "wiki"
            wiki_root.mkdir()
            unrelated_page = bw.Page(
                slug="coffee",
                title="Coffee",
                page_type="Concepts",
                summary_hint="Coffee",
            )
            unrelated_page.notes = ["Lower heat makes moka pot coffee less bitter."]

            with bw.temporary_workspace(root):
                bw.atomic_write_text(wiki_root / "lazy-sequences.md", bw.render_page(pages["lazy-sequences"]))
                bw.atomic_write_text(wiki_root / "coffee.md", bw.render_page(unrelated_page))
                bw.atomic_write_text(wiki_root / "index.md", bw.render_index({**pages, "coffee": unrelated_page}))
                bw.atomic_write_text(wiki_root / bw.CATALOG_PATH, bw.render_catalog({**pages, "coffee": unrelated_page}))
                bw.atomic_write_text(wiki_root / "log.md", "# Wiki Log\n")
                unrelated_before = (wiki_root / "coffee.md").read_text(encoding="utf-8")

                with mock.patch.object(bw, "atomic_write_text", wraps=bw.atomic_write_text) as write_mock:
                    applied = bw.maybe_apply_query_time_split_fix(
                        "lazy-sequences",
                        split_decision=decision,
                        mutation_note="split-adjacent query writeback",
                    )

                self.assertTrue(applied)
                self.assertEqual(
                    {Path(call.args[0]).name for call in write_mock.call_args_list},
                    {"lazy-sequences.md", "iterators.md", "generators.md", "index.md", "catalog.md", "log.md"},
                )
                self.assertEqual(unrelated_before, (wiki_root / "coffee.md").read_text(encoding="utf-8"))
                log_text = (wiki_root / "log.md").read_text(encoding="utf-8")
                index_text = (wiki_root / "index.md").read_text(encoding="utf-8")
                catalog_text = (wiki_root / bw.CATALOG_PATH).read_text(encoding="utf-8")
                self.assertIn("query | split-adjacent query writeback", log_text)
                self.assertIn("lazy-sequences -> iterators, generators", log_text)
                self.assertIn("[[iterators]]", catalog_text)
                self.assertIn("[[generators]]", catalog_text)
                self.assertNotIn(source.cleaned_text, (wiki_root / "iterators.md").read_text(encoding="utf-8"))
                self.assertNotIn(source.cleaned_text, (wiki_root / "generators.md").read_text(encoding="utf-8"))

    def test_maybe_apply_query_time_split_fix_rejects_unstable_decisions(self) -> None:
        source = bw.SourceRecord(
            label="ICS 33 Iterators and Generators",
            path="../raw/ics33-week1.md",
            status="local_only",
            raw_content="",
            cleaned_text="Iterators define traversal. Generators use yield to produce values lazily.",
            fetched_summary=None,
            detected_url=None,
        )
        page = bw.Page(
            slug="lazy-sequences",
            title="Lazy Sequences",
            page_type="Concepts",
            summary_hint="Lazy Sequences",
            sources={source.path: source},
        )
        decision = bw.PageSplitDecision(
            is_atomic=False,
            candidate_satellite_slugs=["iterators", "generators"],
            candidate_evaluations=[
                bw.SplitCandidateEvaluation(
                    slug="iterators",
                    accepted=True,
                    grounding=["Iterator protocol and traversal state."],
                    why_distinct="Consumer-facing traversal interface.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
                bw.SplitCandidateEvaluation(
                    slug="generators",
                    accepted=True,
                    grounding=["Yield-based lazy value production."],
                    why_distinct="Producer-side lazy sequence construction.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=False,
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            wiki_root = root / "wiki"
            wiki_root.mkdir()

            with bw.temporary_workspace(root):
                bw.atomic_write_text(wiki_root / "lazy-sequences.md", bw.render_page(page))
                bw.atomic_write_text(wiki_root / "index.md", bw.render_index({"lazy-sequences": page}))
                bw.atomic_write_text(wiki_root / bw.CATALOG_PATH, bw.render_catalog({"lazy-sequences": page}))
                bw.atomic_write_text(wiki_root / "log.md", "# Wiki Log\n")
                parent_before = (wiki_root / "lazy-sequences.md").read_text(encoding="utf-8")
                log_before = (wiki_root / "log.md").read_text(encoding="utf-8")

                with mock.patch.object(bw, "atomic_write_text", wraps=bw.atomic_write_text) as write_mock:
                    applied = bw.maybe_apply_query_time_split_fix("lazy-sequences", split_decision=decision)

                self.assertFalse(applied)
                self.assertEqual(write_mock.call_args_list, [])
                self.assertEqual(parent_before, (wiki_root / "lazy-sequences.md").read_text(encoding="utf-8"))
                self.assertEqual(log_before, (wiki_root / "log.md").read_text(encoding="utf-8"))
                self.assertFalse((wiki_root / "iterators.md").exists())
                self.assertFalse((wiki_root / "generators.md").exists())

    def test_maybe_apply_query_time_split_fix_is_idempotent(self) -> None:
        source = bw.SourceRecord(
            label="ICS 33 Iterators and Generators",
            path="../raw/ics33-week1.md",
            status="local_only",
            raw_content="",
            cleaned_text="Iterators define traversal. Generators use yield to produce values lazily.",
            fetched_summary=None,
            detected_url=None,
        )
        page = bw.Page(
            slug="lazy-sequences",
            title="Lazy Sequences",
            page_type="Concepts",
            summary_hint="Lazy Sequences",
            sources={source.path: source},
        )
        decision = bw.PageSplitDecision(
            is_atomic=False,
            candidate_satellite_slugs=["iterators", "generators"],
            candidate_evaluations=[
                bw.SplitCandidateEvaluation(
                    slug="iterators",
                    accepted=True,
                    grounding=["Iterator protocol and traversal state."],
                    why_distinct="Consumer-facing traversal interface.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
                bw.SplitCandidateEvaluation(
                    slug="generators",
                    accepted=True,
                    grounding=["Yield-based lazy value production."],
                    why_distinct="Producer-side lazy sequence construction.",
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            wiki_root = root / "wiki"
            wiki_root.mkdir()

            with bw.temporary_workspace(root):
                bw.atomic_write_text(wiki_root / "lazy-sequences.md", bw.render_page(page))
                bw.atomic_write_text(wiki_root / "index.md", bw.render_index({"lazy-sequences": page}))
                bw.atomic_write_text(wiki_root / bw.CATALOG_PATH, bw.render_catalog({"lazy-sequences": page}))
                bw.atomic_write_text(wiki_root / "log.md", "# Wiki Log\n")

                first = bw.maybe_apply_query_time_split_fix(
                    "lazy-sequences",
                    split_decision=decision,
                    mutation_note="split-adjacent query writeback",
                )
                log_after_first = (wiki_root / "log.md").read_text(encoding="utf-8")

                with mock.patch.object(bw, "atomic_write_text", wraps=bw.atomic_write_text) as write_mock:
                    second = bw.maybe_apply_query_time_split_fix(
                        "lazy-sequences",
                        split_decision=decision,
                        mutation_note="split-adjacent query writeback",
                    )

                self.assertTrue(first)
                self.assertFalse(second)
                self.assertEqual(write_mock.call_args_list, [])
                self.assertEqual(log_after_first, (wiki_root / "log.md").read_text(encoding="utf-8"))
                self.assertEqual(log_after_first.count("query | split-adjacent query writeback"), 1)

    def test_maybe_apply_query_time_split_fix_rejects_cloned_child_notes(self) -> None:
        source = bw.SourceRecord(
            label="Soba Noodles With Ponzu Sauce",
            path="../raw/soba-ponzu.md",
            status="local_only",
            raw_content="",
            cleaned_text="Soba noodles are served chilled. Ponzu sauce adds citrus and soy.",
            fetched_summary=None,
            detected_url=None,
        )
        page = bw.Page(
            slug="recipe",
            title="Recipe",
            page_type="Resources",
            summary_hint="Recipe",
            sources={source.path: source},
        )
        decision = bw.PageSplitDecision(
            is_atomic=False,
            candidate_satellite_slugs=["soba-noodles", "ponzu-sauce"],
            candidate_evaluations=[
                bw.SplitCandidateEvaluation(
                    slug="soba-noodles",
                    accepted=True,
                    grounding=["Soba Noodles With Ponzu Sauce"],
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
                bw.SplitCandidateEvaluation(
                    slug="ponzu-sauce",
                    accepted=True,
                    grounding=["Soba Noodles With Ponzu Sauce"],
                    passes_direct_link_test=True,
                    passes_stable_page_test=True,
                ),
            ],
        )

        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            wiki_root = root / "wiki"
            wiki_root.mkdir()

            with bw.temporary_workspace(root):
                bw.atomic_write_text(wiki_root / "recipe.md", bw.render_page(page))
                bw.atomic_write_text(wiki_root / "index.md", bw.render_index({"recipe": page}))
                bw.atomic_write_text(wiki_root / bw.CATALOG_PATH, bw.render_catalog({"recipe": page}))
                bw.atomic_write_text(wiki_root / "log.md", "# Wiki Log\n")

                with mock.patch.object(bw, "atomic_write_text", wraps=bw.atomic_write_text) as write_mock:
                    applied = bw.maybe_apply_query_time_split_fix("recipe", split_decision=decision)

                self.assertFalse(applied)
                self.assertEqual(write_mock.call_args_list, [])
                self.assertFalse((wiki_root / "soba-noodles.md").exists())
                self.assertFalse((wiki_root / "ponzu-sauce.md").exists())

    def test_split_debug_output_includes_grounding_and_rejection_reasons(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Pesto Eggs",
                path="../raw/pesto-eggs.md",
                status="local_only",
                raw_content="",
                cleaned_text="Recipe note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Vodka Pasta",
                path="../raw/vodka-pasta.md",
                status="local_only",
                raw_content="",
                cleaned_text="Recipe note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="recipe",
            title="Recipe",
            page_type="Concepts",
            summary_hint="Recipe",
            sources={source.path: source for source in sources},
        )
        pages = {"recipe": page}
        decision = bw.PageSplitDecision(
            is_atomic=True,
            rationale="The page still behaves like one recipe collection note.",
            rejection_reasons=["children are storage labels rather than durable ideas"],
            candidate_evaluations=[
                bw.SplitCandidateEvaluation(
                    slug="breakfast",
                    accepted=False,
                    grounding=["Only appears as a section heading."],
                    why_distinct="Meal time is not the durable concept here.",
                    rejection_reasons=["generic bucket"],
                )
            ],
        )

        with mock.patch.dict("os.environ", {"BOOTSTRAP_SPLIT_DEBUG": "1"}, clear=False):
            with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
                with mock.patch.object(bw, "analyze_page_for_atomic_split", return_value=decision):
                    stderr = StringIO()
                    with redirect_stderr(stderr):
                        bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        output = stderr.getvalue()
        self.assertIn("Split debug [recipe]", output)
        self.assertIn("grounding: Only appears as a section heading.", output)
        self.assertIn("rejection_reasons: children are storage labels rather than durable ideas", output)
        self.assertIn("breakfast: rejected;", output)

    def test_parse_page_split_response_collapses_overlapping_children(self) -> None:
        content = """
        {
            "is_atomic": false,
            "candidate_satellite_slugs": ["lazy-sequences", "lazy-sequence-notes", "generators"],
            "candidate_evaluations": [
                {
                    "slug": "lazy-sequences",
                    "accepted": true,
                    "grounding": ["Reusable idea one."],
                    "why_distinct": "Primary cluster.",
                    "passes_direct_link_test": true,
                    "passes_stable_page_test": true
                },
                {
                    "slug": "lazy-sequence-notes",
                    "accepted": true,
                    "grounding": ["Near-duplicate cluster."],
                    "why_distinct": "Too close to the first child.",
                    "passes_direct_link_test": true,
                    "passes_stable_page_test": true
                },
                {
                    "slug": "generators",
                    "accepted": true,
                    "grounding": ["Reusable idea two."],
                    "why_distinct": "Secondary cluster.",
                    "passes_direct_link_test": true,
                    "passes_stable_page_test": true
                }
            ]
        }
        """

        decision = bw.parse_page_split_response(content, "lazy-sequences", {"../raw/ics33-week1.md"})

        self.assertFalse(decision.is_atomic)
        self.assertEqual(len(decision.candidate_satellite_slugs), 2)
        self.assertIn("generators", decision.candidate_satellite_slugs)
        self.assertTrue(
            {"lazy-sequences", "lazy-sequence-notes"}.intersection(decision.candidate_satellite_slugs)
        )

    def test_parse_page_split_response_rejects_overlapping_child_set_that_collapses_to_one(self) -> None:
        content = """
        {
            "is_atomic": false,
            "candidate_satellite_slugs": ["iterators", "iterator-notes"],
            "candidate_evaluations": [
                {
                    "slug": "iterators",
                    "accepted": true,
                    "grounding": ["Reusable idea one."],
                    "why_distinct": "Primary cluster.",
                    "passes_direct_link_test": true,
                    "passes_stable_page_test": true
                },
                {
                    "slug": "iterator-notes",
                    "accepted": true,
                    "grounding": ["Near-duplicate cluster."],
                    "why_distinct": "Too close to the first child.",
                    "passes_direct_link_test": true,
                    "passes_stable_page_test": true
                }
            ]
        }
        """

        decision = bw.parse_page_split_response(content, "lazy-sequences", {"../raw/ics33-week1.md"})

        self.assertTrue(decision.is_atomic)
        self.assertIn("overlapping child set collapsed below split threshold", decision.rejection_reasons)

    def test_mixed_umbrella_and_satellite_sources_convert_existing_slug_to_topic(self) -> None:
        sources = [
            bw.SourceRecord(
                label="How to travel cheaply",
                path="../raw/travel-cheaply.md",
                status="local_only",
                raw_content="",
                cleaned_text="Budget travel advice.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="My packing list (Japan)",
                path="../raw/japan-packing.md",
                status="local_only",
                raw_content="",
                cleaned_text="Packing list.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="YIPPEE TRIP TG",
                path="../raw/sf-trip.md",
                status="local_only",
                raw_content="",
                cleaned_text="Trip ideas for Santa Clara and San Francisco.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        pages = {
            "travel": bw.Page(
                slug="travel",
                title="Travel",
                page_type="Concepts",
                summary_hint="Travel",
                sources={source.path: source for source in sources},
            )
        }

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(
                    is_atomic=False,
                    candidate_satellite_slugs=["budget-travel", "packing-list", "santa-clara"],
                    source_assignments={
                        "../raw/travel-cheaply.md": "budget-travel",
                        "../raw/japan-packing.md": "packing-list",
                        "../raw/sf-trip.md": "santa-clara",
                    },
                ),
            ):
                bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")
            bw.ensure_meaningful_connections(pages)
            bw.finalize_page_shapes(pages)

        self.assertEqual(pages["travel"].shape, bw.PAGE_SHAPE_TOPIC)
        self.assertIn("budget-travel", pages)
        self.assertIn("packing-list", pages)
        self.assertIn("santa-clara", pages)
        self.assertEqual(pages["budget-travel"].topic_parent, "travel")

    def test_two_source_page_stays_atomic_when_llm_confirms_single_idea(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Backpack",
                path="../raw/backpack.md",
                status="local_only",
                raw_content="",
                cleaned_text="Backpack note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Duffel Bag",
                path="../raw/duffel.md",
                status="local_only",
                raw_content="",
                cleaned_text="Duffel note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        pages = {
            "travel-bags": bw.Page(
                slug="travel-bags",
                title="Travel Bags",
                page_type="Concepts",
                summary_hint="Travel Bags",
                sources={source.path: source for source in sources},
            )
        }

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(is_atomic=True),
            ):
                bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")
            bw.ensure_meaningful_connections(pages)
            bw.finalize_page_shapes(pages)

        self.assertEqual(pages["travel-bags"].shape, bw.PAGE_SHAPE_ATOMIC)
        self.assertNotIn("backpack", pages)

    def test_split_preflight_failure_skips_immediately_and_reports_no_split(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Backpack",
                path="../raw/backpack.md",
                status="local_only",
                raw_content="",
                cleaned_text="Backpack note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Duffel Bag",
                path="../raw/duffel.md",
                status="local_only",
                raw_content="",
                cleaned_text="Duffel note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        pages = {
            "travel-bags": bw.Page(
                slug="travel-bags",
                title="Travel Bags",
                page_type="Concepts",
                summary_hint="Travel Bags",
                sources={source.path: source for source in sources},
            )
        }

        with mock.patch.object(bw, "split_preflight_check", return_value=(False, "URLError: dns failed")):
            with mock.patch.object(bw, "analyze_page_for_atomic_split") as analyze_mock:
                stderr = StringIO()
                with redirect_stderr(stderr):
                    report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        analyze_mock.assert_not_called()
        self.assertEqual(report.mode, "no-split")
        self.assertEqual(report.status, "skipped_preflight")
        self.assertEqual(report.reason, "URLError: dns failed")
        self.assertEqual(report.eligible_pages, 1)
        self.assertIn("preflight failed", stderr.getvalue())

    def test_split_exception_is_counted_and_surfaced(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Backpack",
                path="../raw/backpack.md",
                status="local_only",
                raw_content="",
                cleaned_text="Backpack note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Duffel Bag",
                path="../raw/duffel.md",
                status="local_only",
                raw_content="",
                cleaned_text="Duffel note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        pages = {
            "travel-bags": bw.Page(
                slug="travel-bags",
                title="Travel Bags",
                page_type="Concepts",
                summary_hint="Travel Bags",
                sources={source.path: source for source in sources},
            )
        }

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                side_effect=ValueError("bad JSON"),
            ):
                stderr = StringIO()
                with redirect_stderr(stderr):
                    report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        self.assertEqual(report.status, "completed")
        self.assertEqual(report.failed_pages, 1)
        self.assertEqual(report.analyzed_pages, 0)
        self.assertIn("ValueError: bad JSON", report.failure_details[0])
        self.assertIn("failed 'travel-bags'", stderr.getvalue())

    def test_non_atomic_page_with_insufficient_satellites_is_marked_incomplete(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Aphelios",
                path="../raw/aphelios.md",
                status="local_only",
                raw_content="",
                cleaned_text="Champion note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Account note",
                path="../raw/account-note.md",
                status="local_only",
                raw_content="",
                cleaned_text="Account note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        pages = {
            "league-stuff": bw.Page(
                slug="league-stuff",
                title="League Stuff",
                page_type="Concepts",
                summary_hint="League Stuff",
                sources={source.path: source for source in sources},
            )
        }

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(
                    is_atomic=False,
                    candidate_satellite_slugs=["aphelios"],
                    source_assignments={"../raw/aphelios.md": "aphelios"},
                ),
            ):
                stderr = StringIO()
                with redirect_stderr(stderr):
                    report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        self.assertEqual(report.atomic_pages, 0)
        self.assertEqual(report.incomplete_pages, 1)
        self.assertEqual(report.incomplete_details, ["league-stuff: insufficient satellites"])
        self.assertIn("incomplete (insufficient satellites)", stderr.getvalue())
        self.assertEqual(pages["league-stuff"].shape, bw.PAGE_SHAPE_ATOMIC)

    def test_non_atomic_page_with_incomplete_assignments_is_marked_incomplete(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Aphelios",
                path="../raw/aphelios.md",
                status="local_only",
                raw_content="",
                cleaned_text="Champion note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Velkoz",
                path="../raw/velkoz.md",
                status="local_only",
                raw_content="",
                cleaned_text="Champion note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Username",
                path="../raw/username.md",
                status="local_only",
                raw_content="",
                cleaned_text="Account note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        pages = {
            "league-stuff": bw.Page(
                slug="league-stuff",
                title="League Stuff",
                page_type="Concepts",
                summary_hint="League Stuff",
                sources={source.path: source for source in sources},
            )
        }

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(
                    is_atomic=False,
                    candidate_satellite_slugs=["aphelios", "velkoz", "account-preferences"],
                    source_assignments={
                        "../raw/aphelios.md": "aphelios",
                        "../raw/velkoz.md": "velkoz",
                    },
                ),
            ):
                stderr = StringIO()
                with redirect_stderr(stderr):
                    report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        self.assertEqual(report.atomic_pages, 0)
        self.assertEqual(report.incomplete_pages, 1)
        self.assertEqual(report.incomplete_details, ["league-stuff: partial coverage"])
        self.assertIn("incomplete (partial coverage)", stderr.getvalue())

    def test_unused_extra_satellite_slug_does_not_block_split(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Aphelios",
                path="../raw/aphelios.md",
                status="local_only",
                raw_content="",
                cleaned_text="Champion note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Velkoz",
                path="../raw/velkoz.md",
                status="local_only",
                raw_content="",
                cleaned_text="Champion note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        pages = {
            "league-stuff": bw.Page(
                slug="league-stuff",
                title="League Stuff",
                page_type="Concepts",
                summary_hint="League Stuff",
                sources={source.path: source for source in sources},
            )
        }

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(
                    is_atomic=False,
                    candidate_satellite_slugs=["aphelios", "velkoz", "jhin"],
                    source_assignments={
                        "../raw/aphelios.md": "aphelios",
                        "../raw/velkoz.md": "velkoz",
                    },
                ),
            ):
                report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        self.assertEqual(report.split_pages, 1)
        self.assertEqual(report.incomplete_pages, 0)
        self.assertEqual(pages["league-stuff"].shape, bw.PAGE_SHAPE_TOPIC)
        self.assertIn("aphelios", pages)
        self.assertIn("velkoz", pages)

    def test_missing_assignments_can_be_inferred_from_source_titles(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Aphelios",
                path="../raw/aphelios.md",
                status="local_only",
                raw_content="",
                cleaned_text="Champion note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Velkoz",
                path="../raw/velkoz.md",
                status="local_only",
                raw_content="",
                cleaned_text="Champion note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        pages = {
            "league-stuff": bw.Page(
                slug="league-stuff",
                title="League Stuff",
                page_type="Concepts",
                summary_hint="League Stuff",
                sources={source.path: source for source in sources},
            )
        }

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(
                    is_atomic=False,
                    candidate_satellite_slugs=["aphelios", "velkoz"],
                    source_assignments={},
                ),
            ):
                report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        self.assertEqual(report.split_pages, 1)
        self.assertEqual(report.incomplete_pages, 0)
        self.assertEqual(pages["league-stuff"].shape, bw.PAGE_SHAPE_TOPIC)
        self.assertEqual(pages["aphelios"].topic_parent, "league-stuff")
        self.assertEqual(pages["velkoz"].topic_parent, "league-stuff")

    def test_repeated_split_timeouts_do_not_abort_early(self) -> None:
        pages = {}
        for slug in ("travel", "camping", "league-of-legends"):
            sources = [
                bw.SourceRecord(
                    label=f"{slug} note a",
                    path=f"../raw/{slug}-a.md",
                    status="local_only",
                    raw_content="",
                    cleaned_text="Note a.",
                    fetched_summary=None,
                    detected_url=None,
                ),
                bw.SourceRecord(
                    label=f"{slug} note b",
                    path=f"../raw/{slug}-b.md",
                    status="local_only",
                    raw_content="",
                    cleaned_text="Note b.",
                    fetched_summary=None,
                    detected_url=None,
                ),
            ]
            pages[slug] = bw.Page(
                slug=slug,
                title=bw.page_title(slug),
                page_type="Concepts",
                summary_hint=bw.page_title(slug),
                sources={source.path: source for source in sources},
            )

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                side_effect=TimeoutError("The read operation timed out"),
            ) as analyze_mock:
                stderr = StringIO()
                with redirect_stderr(stderr):
                    report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        self.assertFalse(report.aborted)
        self.assertEqual(report.mode, "performed")
        self.assertEqual(report.status, "completed")
        self.assertEqual(report.failed_pages, 3)
        self.assertEqual(analyze_mock.call_count, 3)
        self.assertNotIn("aborting early", stderr.getvalue())

    def test_repeated_split_transport_failures_abort_early(self) -> None:
        pages = {}
        for slug in ("travel", "camping", "league-of-legends"):
            sources = [
                bw.SourceRecord(
                    label=f"{slug} note a",
                    path=f"../raw/{slug}-a.md",
                    status="local_only",
                    raw_content="",
                    cleaned_text="Note a.",
                    fetched_summary=None,
                    detected_url=None,
                ),
                bw.SourceRecord(
                    label=f"{slug} note b",
                    path=f"../raw/{slug}-b.md",
                    status="local_only",
                    raw_content="",
                    cleaned_text="Note b.",
                    fetched_summary=None,
                    detected_url=None,
                ),
            ]
            pages[slug] = bw.Page(
                slug=slug,
                title=bw.page_title(slug),
                page_type="Concepts",
                summary_hint=bw.page_title(slug),
                sources={source.path: source for source in sources},
            )

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                side_effect=URLError("dns failed"),
            ) as analyze_mock:
                stderr = StringIO()
                with redirect_stderr(stderr):
                    report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        self.assertTrue(report.aborted)
        self.assertEqual(report.mode, "no-split")
        self.assertEqual(report.status, "aborted_transport_failure")
        self.assertEqual(report.failed_pages, 3)
        self.assertEqual(analyze_mock.call_count, 3)
        self.assertIn("aborting early", stderr.getvalue())

    def test_target_slugs_limit_split_analysis(self) -> None:
        pages = {}
        for slug in ("travel", "camping"):
            sources = [
                bw.SourceRecord(
                    label=f"{slug} note a",
                    path=f"../raw/{slug}-a.md",
                    status="local_only",
                    raw_content="",
                    cleaned_text="Note a.",
                    fetched_summary=None,
                    detected_url=None,
                ),
                bw.SourceRecord(
                    label=f"{slug} note b",
                    path=f"../raw/{slug}-b.md",
                    status="local_only",
                    raw_content="",
                    cleaned_text="Note b.",
                    fetched_summary=None,
                    detected_url=None,
                ),
            ]
            pages[slug] = bw.Page(
                slug=slug,
                title=bw.page_title(slug),
                page_type="Concepts",
                summary_hint=bw.page_title(slug),
                sources={source.path: source for source in sources},
            )

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(is_atomic=True),
            ) as analyze_mock:
                report = bw.migrate_pages_to_atomic_topics(
                    pages,
                    {},
                    api_key="token",
                    target_slugs={"travel"},
                )

        self.assertEqual(report.eligible_pages, 1)
        self.assertEqual(analyze_mock.call_count, 1)
        analyzed_page = analyze_mock.call_args.args[0]
        self.assertEqual(analyzed_page.slug, "travel")

    def test_bucket_signaled_page_with_good_split_becomes_topic(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Pesto Eggs",
                path="../raw/pesto-eggs.md",
                status="local_only",
                raw_content="",
                cleaned_text="Recipe note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Vodka Pasta",
                path="../raw/vodka-pasta.md",
                status="local_only",
                raw_content="",
                cleaned_text="Recipe note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Miso Salmon",
                path="../raw/miso-salmon.md",
                status="local_only",
                raw_content="",
                cleaned_text="Recipe note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="recipe",
            title="Recipe",
            page_type="Concepts",
            summary_hint="Recipe",
            sources={source.path: source for source in sources},
        )
        page.rendered_notes_markdown = "### Breakfast\n- Pesto eggs\n### Dinner\n- Vodka pasta"
        pages = {"recipe": page}

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(
                    is_atomic=False,
                    candidate_satellite_slugs=["pesto-eggs", "vodka-pasta", "miso-salmon"],
                    source_assignments={
                        "../raw/pesto-eggs.md": "pesto-eggs",
                        "../raw/vodka-pasta.md": "vodka-pasta",
                        "../raw/miso-salmon.md": "miso-salmon",
                    },
                ),
            ):
                report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        self.assertEqual(report.split_pages, 1)
        self.assertEqual(pages["recipe"].shape, bw.PAGE_SHAPE_TOPIC)
        self.assertTrue(any(detail.startswith("recipe:") for detail in report.bucket_signaled_details))

    def test_bucket_signaled_page_kept_atomic_is_reported_bucket_unsplit(self) -> None:
        sources = [
            bw.SourceRecord(
                label="ICS 33",
                path="../raw/ics-33.md",
                status="local_only",
                raw_content="",
                cleaned_text="Course note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="UCI Food",
                path="../raw/uci-food.md",
                status="local_only",
                raw_content="",
                cleaned_text="Food note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="ARC Gym",
                path="../raw/arc-gym.md",
                status="local_only",
                raw_content="",
                cleaned_text="Gym note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="uci",
            title="UCI",
            page_type="Entities",
            summary_hint="UCI",
            sources={source.path: source for source in sources},
        )
        page.connections["ics-33"] += 1
        page.connections["uci-food"] += 1
        page.connections["arc-gym"] += 1
        pages = {"uci": page}

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(is_atomic=True),
            ):
                report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        self.assertEqual(report.atomic_pages, 0)
        self.assertEqual(report.failed_pages, 1)
        self.assertEqual(report.bucket_unsplit_details, ["uci: bucket-unsplit (model_kept_atomic; signals=heterogeneous-sources, existing-satellites)"])
        self.assertEqual(pages["uci"].shape, bw.PAGE_SHAPE_ATOMIC)

    def test_bucket_signaled_page_with_unusable_output_is_reported_bucket_unsplit(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Pesto Eggs",
                path="../raw/pesto-eggs.md",
                status="local_only",
                raw_content="",
                cleaned_text="Recipe note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Vodka Pasta",
                path="../raw/vodka-pasta.md",
                status="local_only",
                raw_content="",
                cleaned_text="Recipe note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Miso Salmon",
                path="../raw/miso-salmon.md",
                status="local_only",
                raw_content="",
                cleaned_text="Recipe note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="recipe",
            title="Recipe",
            page_type="Concepts",
            summary_hint="Recipe",
            sources={source.path: source for source in sources},
        )
        page.rendered_notes_markdown = "### Breakfast\n- Pesto eggs\n### Dinner\n- Vodka pasta"
        pages = {"recipe": page}

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(
                    is_atomic=False,
                    candidate_satellite_slugs=["pesto-eggs", "vodka-pasta"],
                    source_assignments={"../raw/pesto-eggs.md": "pesto-eggs"},
                ),
            ):
                report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        self.assertEqual(report.failed_pages, 1)
        self.assertEqual(report.incomplete_pages, 0)
        self.assertEqual(
            report.bucket_unsplit_details,
            ["recipe: bucket-unsplit (partial coverage; signals=generic-parent-slug, multi-cluster-notes, heterogeneous-sources)"],
        )

    def test_non_bucket_page_preserves_existing_gemini_atomic_behavior(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Moka Pot Tips",
                path="../raw/moka-pot-a.md",
                status="local_only",
                raw_content="",
                cleaned_text="Brew ratio note.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Moka Pot Cleaning",
                path="../raw/moka-pot-b.md",
                status="local_only",
                raw_content="",
                cleaned_text="Cleaning note.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="moka-pot",
            title="Moka Pot",
            page_type="Concepts",
            summary_hint="Moka Pot",
            sources={source.path: source for source in sources},
        )
        page.rendered_notes_markdown = "### Brewing\n- Lower heat helps.\n### Cleaning\n- Rinse only."
        pages = {"moka-pot": page}

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(is_atomic=True),
            ):
                report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        self.assertEqual(report.atomic_pages, 1)
        self.assertEqual(report.failed_pages, 0)
        self.assertEqual(report.bucket_unsplit_details, [])

    def test_lecture_like_page_skips_split_analysis_and_stays_atomic(self) -> None:
        sources = [
            bw.SourceRecord(
                label="Week 1 Recursion",
                path="../raw/Apple Notes/Marcus/Archives/ICS 33/week-1-recursion.md",
                status="local_only",
                raw_content="",
                cleaned_text="Lecture 1 covered recursion.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Week 2 Iterators",
                path="../raw/Apple Notes/Marcus/Archives/ICS 33/week-2-iterators.md",
                status="local_only",
                raw_content="",
                cleaned_text="Lecture 2 covered iterators.",
                fetched_summary=None,
                detected_url=None,
            ),
            bw.SourceRecord(
                label="Final Review",
                path="../raw/Apple Notes/Marcus/Archives/ICS 33/final-review.md",
                status="local_only",
                raw_content="",
                cleaned_text="Final review topics.",
                fetched_summary=None,
                detected_url=None,
            ),
        ]
        page = bw.Page(
            slug="ics-33",
            title="ICS 33",
            page_type="Concepts",
            summary_hint="ICS 33",
            sources={source.path: source for source in sources},
        )
        page.rendered_notes_markdown = "### Week 1\n- Recursion.\n### Week 2\n- Iterators."
        pages = {"ics-33": page}

        with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
            with mock.patch.object(
                bw,
                "analyze_page_for_atomic_split",
                return_value=bw.PageSplitDecision(
                    is_atomic=False,
                    candidate_satellite_slugs=["recursion", "iterators", "final-review"],
                    source_assignments={
                        "../raw/Apple Notes/Marcus/Archives/ICS 33/week-1-recursion.md": "recursion",
                        "../raw/Apple Notes/Marcus/Archives/ICS 33/week-2-iterators.md": "iterators",
                        "../raw/Apple Notes/Marcus/Archives/ICS 33/final-review.md": "final-review",
                    },
                ),
            ) as analyze_mock:
                stderr = StringIO()
                with redirect_stderr(stderr):
                    report = bw.migrate_pages_to_atomic_topics(pages, {}, api_key="token")

        analyze_mock.assert_not_called()
        self.assertEqual(report.atomic_pages, 1)
        self.assertEqual(report.analyzed_pages, 0)
        self.assertEqual(report.split_pages, 0)
        self.assertEqual(pages["ics-33"].shape, bw.PAGE_SHAPE_ATOMIC)
        self.assertIn("course lecture guard", stderr.getvalue())

    def test_bootstrap_main_sandbox_keeps_lecture_page_atomic(self) -> None:
        lecture_notes = {
            "Week 1 Recursion.md": "# Week 1 Recursion\n\n## Key Ideas\n\n- Recursive functions\n- Base cases\n",
            "Week 2 Iterators.md": "# Week 2 Iterators\n\n## Key Ideas\n\n- Iterators\n- Generators\n",
            "Final Review.md": "# Final Review\n\n## Topics\n\n- Recursion\n- Iterators\n- Classes\n",
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            raw_root = root / "raw" / "ICS 33"
            raw_root.mkdir(parents=True)
            (root / "wiki").mkdir()
            for name, content in lecture_notes.items():
                (raw_root / name).write_text(content, encoding="utf-8")

            with bw.temporary_workspace(root):
                with mock.patch.dict("os.environ", {"GEMINI_API_KEY": ""}, clear=False):
                    with mock.patch.object(bw, "split_preflight_check", return_value=(True, None)):
                        with mock.patch.object(
                            bw,
                            "analyze_page_for_atomic_split",
                            return_value=bw.PageSplitDecision(
                                is_atomic=False,
                                candidate_satellite_slugs=["recursion", "iterators", "classes"],
                                source_assignments={},
                            ),
                        ) as analyze_mock:
                            with mock.patch.object(sys, "argv", ["bootstrap_wiki.py"]):
                                bw.main()

                analyze_mock.assert_not_called()
                course_page = (bw.WIKI_ROOT / "ics-33.md").read_text(encoding="utf-8")
                self.assertIn("# ICS 33", course_page)
                self.assertIn("## Notes", course_page)
                self.assertIn("## Sources", course_page)
                self.assertNotIn("## Connections\n\n- [[recursion]]", course_page)
                self.assertFalse((bw.WIKI_ROOT / "recursion.md").exists())
                self.assertIn('bootstrap | completed', (bw.WIKI_ROOT / "log.md").read_text(encoding="utf-8"))

    def test_manifest_failed_split_slugs_reads_failure_details(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = Path(tmpdir) / "manifest.json"
            payload = {
                "split_phase": {
                    "failure_details": [
                        "league-stuff: TimeoutError: The read operation timed out",
                        "travel: ValueError: bad JSON",
                    ],
                    "incomplete_details": [
                        "limits: partial coverage",
                    ],
                    "bucket_signaled_details": [
                        "recipe: generic-parent-slug, multi-cluster-notes",
                    ],
                    "bucket_unsplit_details": [
                        "uci: bucket-unsplit (model_kept_atomic; signals=heterogeneous-sources, existing-satellites)",
                    ],
                }
            }
            manifest_path.write_text(bw.stable_json_dumps(payload), encoding="utf-8")
            with mock.patch.object(bw, "CACHE_MANIFEST_PATH", manifest_path):
                self.assertEqual(bw.manifest_failed_split_slugs(), {"league-stuff", "travel", "limits", "recipe", "uci"})


if __name__ == "__main__":
    unittest.main()
