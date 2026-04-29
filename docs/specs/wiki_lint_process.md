# Wiki Lint Process

This document defines wiki lint as an LLM-maintainer process run through Codex. It is not a CI-style blocking linter.

Codex owns routine wiki hygiene. Marcus remains curator for semantic decisions.

## Defaults

- Lint is advisory only. It never blocks ingest, commits, or pushes.
- Marcus triggers lint manually by asking Codex to lint the wiki.
- Use `lint_wiki(append_review=True)` as the Level 1 mechanical engine unless Marcus asks for a chat-only pass.
- `wiki/review.md` is the only persistent lint queue for v1.
- Mechanical findings and open-question/contradiction candidates are queued in `wiki/review.md` by default.
- Graph and synthesis findings stay in chat unless Marcus explicitly asks to queue them.
- Orphans are valid Zettelkasten notes. Treat them as review candidates, not failures.
- Before re-raising Level 2 or Level 3 candidates, check `wiki/review.md` for `accepted | lint | ...` entries that record curator-approved exceptions.
- Do not auto-merge, delete, rewrite, or otherwise fix semantic findings without explicit approval.

## Level 1: Mechanical Integrity

Run the deterministic lint engine:

```python
from scripts import vault_pipeline as vp

report = vp.lint_wiki(append_review=True)
```

Check for:

- invalid page shape
- dead wikilinks
- dead source citations
- missing required outbound links
- open questions or contradiction candidates

When `append_review=True`, durable review items are queued for:

- `invalid-page-shape`
- `missing-outbound-link`
- `dead-citation`
- `dead-link`
- `contradiction-candidate`

Report the top findings in chat. If there are no mechanical issues, say that clearly before listing any graph or synthesis candidates.

## Level 2: Graph Hygiene

Use the Level 1 report, wiki index, catalog when present, and direct wiki search to review structural health.

Look for:

- orphan pages
- weakly linked pages
- heavily linked clusters that may deserve topic pages
- repeated titles or slugs
- large bucket pages
- topic pages with weak support

These are ranked review candidates, not failures. Do not append them to `wiki/review.md` unless Marcus asks.

Topic page candidates should be specific and supported by at least three atomic notes that compress cleanly into the label. Generic buckets should stay out of the wiki.

If Marcus accepts a graph-hygiene exception, record it as an `accepted | lint | ...` entry in `wiki/review.md` and do not keep reporting it in future lint summaries unless new evidence changes the judgment.

## Level 3: Synthesis Review

Make a bounded judgment pass for synthesis quality.

Look for:

- duplicate concepts split across pages
- pages that are source-shaped rather than idea-shaped
- mega-pages covering multiple concepts
- under-linked atomic notes
- places where multiple captures should be synthesized into existing notes

Treat source-shaped pages as soft candidates only when multiple signals combine:

- raw-looking title
- weak links
- one source
- low reuse

Never auto-merge, delete, or rewrite these findings without explicit approval. Present the options and recommendation in chat.

If Marcus accepts a synthesis exception, record it as an `accepted | lint | ...` entry in `wiki/review.md` and treat it as settled unless the page materially changes.

## Reporting

Default chat report:

- ranked top 20 findings
- counts by finding type
- clear distinction between mechanical issues and semantic review candidates
- suggested fixes grouped by repair type

Suggested fix categories:

- safe mechanical repair
- likely semantic improvement
- curator decision needed

If `wiki/review.md` was updated, report the number of queued durable items. If the durable queue was not updated, say so.

Do not create timestamped lint report files in v1.

## Operator Checklist

1. Read `AGENTS.md` and this spec.
2. Run `lint_wiki(append_review=True)` through Codex or a short Python invocation.
3. Inspect `report.findings` and group counts by `finding.kind`.
4. Review graph-hygiene candidates separately from durable Level 1 findings.
5. Make a bounded synthesis pass over the highest-risk pages only.
6. Report the ranked top 20 findings and queue count in chat.
7. Ask before applying semantic fixes.

## Non-Goals

- No standalone CLI in v1.
- No blocking lint gate.
- No timestamped report files.
- No automatic semantic cleanup.
- No treatment of orphan pages as errors.
