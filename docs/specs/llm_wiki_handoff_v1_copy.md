# LLM Wiki Handoff v1

This document is an implementation-oriented companion to [LLM Wiki Architecture](./llm_wiki_architecture.md).

Use the architecture doc as the canonical source for high-level system principles. Use this handoff doc for concrete implementation choices, data contracts, and operational boundaries.

## Scope And Precedence

- `docs/specs/llm_wiki_architecture.md` remains the canonical architecture reference and should be treated as immutable upstream source material in this repo.
- `AGENTS.md` remains the active operating contract for page shape, ingest behavior, query behavior, and wiki tone.
- This handoff doc refines implementation details that are too specific for the architecture doc.
- This handoff doc is the local extension layer for repo-specific implementation choices that should not modify the upstream architecture document.
- If this handoff introduces a repo-shape extension that is not yet reflected in `AGENTS.md`, treat that extension as intentional implementation direction and update `AGENTS.md` when the implementation lands.

## Summary

Build the wiki as a two-layer knowledge system:

- immutable source artifacts remain the ground truth
- `wiki/` remains the maintained synthesis layer

Use a two-tier LLM maintenance pipeline:

- every new source artifact gets a cheap router pass
- only sources that warrant deeper work trigger the heavy updater

Support writeback from both ingest and query flows, but keep provenance strict: every material wiki claim must trace back to a persisted immutable source artifact.

## Source Model

### Source roots

- Keep `raw/` reserved for imported capture material from Obsidian/export flows.
- Add `sources/chat/` as a second immutable source root for chat-derived source artifacts.
- Treat both source roots as immutable after creation.
- Require explicit source metadata on every persisted artifact rather than inferring meaning from path alone.
- Treat `sources/chat/` as private source material, the same as `raw/`, and never publish it to the public mirror.

### Required source metadata

Every persisted source artifact should carry enough frontmatter or adjacent metadata to support deterministic downstream behavior:

- `source_kind`: `capture` or `chat`
- `created_at`
- stable source id
- user-visible title or label
- optional external URL
- provenance pointer for chat sources, such as query id or conversation timestamp

### Source artifact conventions

- Capture artifacts remain exactly as exported into `raw/`.
- Chat artifacts in `sources/chat/` should be append-only snapshots of the user-provided fact or preference that justified writeback.
- Never edit an existing artifact to reflect a newer claim. Persist a new artifact and let the wiki point at the current one.

### Source citations

- Keep wiki citation rendering consistent across both source roots.
- When a source has an external URL, the visible citation text should be the literal URL and the markdown target should point to the local source artifact.
- When a source has no external URL, use a descriptive label linked to the local source artifact.
- If a newer chat-derived fact supersedes an older one, preserve the older source artifact for auditability rather than mutating or deleting it.

## Operational Invariants

These invariants are code-enforced and should be validated after every wiki mutation:

- Topic pages contain only a title and `## Connections`.
- Atomic pages may include `## Notes`, `## Connections`, `## Sources`, and `## Open Questions`, but empty sections must be stripped.
- Every new or materially updated atomic page must have at least one meaningful outbound link unless the change is deferred into `wiki/review.md`.
- `## Sources` entries are code-rendered, not model-rendered.
- `## Connections` entries are normalized by code even if the model proposes them.
- `wiki/index.md` stays compact and navigation-first.
- `wiki/log.md` stays append-only and chronological.
- `wiki/review.md` is a backlog, not an audit log.

## Maintenance Pipeline

### Deterministic ownership

Code remains authoritative for:

- source persistence and immutability
- source ids and source metadata
- source link rendering
- page-shape validation
- connection normalization
- index refresh
- append-only log updates
- maintenance budgets
- fallback behavior when model output is invalid, incomplete, or over budget

The model remains responsible for semantic synthesis: what the source means, which pages it likely touches, what new notes belong on those pages, and whether a contradiction or new atomic page is present.

### Pipeline stages

Normal ingest and query-time writeback follow the same staged pipeline:

1. Persist the source artifact if it does not already exist.
2. Assemble bounded context for the source and likely target pages.
3. Run the router.
4. If the router requests it, run the heavy updater.
5. Validate and apply the resulting page deltas.
6. Refresh `wiki/index.md`.
7. Append to `wiki/log.md`.
8. Append to `wiki/review.md` when contradictions or deferred work remain unresolved.

### Router contract

Run a cheap router pass for every new source artifact.

The router output should be small, structured, and easy for code to validate:

- `action`: `ignore`, `light_update`, `heavy_update`, or `queue_review`
- `target_pages`: canonical wiki page titles
- `new_page_signal`: boolean
- `candidate_new_pages`: proposed new atomic page titles, if any
- `contradiction_risk`: `low`, `medium`, or `high`
- `reorganization_risk`: boolean
- `confidence`: `low`, `medium`, or `high`
- `reason`: short human-readable explanation

The router should escalate on semantic grounds, not on a long deterministic heuristic tree:

- ambiguity
- contradiction risk
- likely new atomic page
- multi-page impact
- reorganization need

### Light update path

When the router returns `light_update`, code may apply a bounded update without calling the heavy updater if all of the following are true:

- the source touches a small known set of existing atomic pages
- no contradiction risk is above `low`
- no new page creation is required
- page-shape invariants can be preserved with local section edits

The light path should favor small note additions, source additions, and simple connection updates.

### Heavy updater contract

Invoke the heavy updater only when the router requests `heavy_update`.

The heavy updater receives bounded source context plus the current contents of the touched pages. It should return a structured proposal rather than free-form file contents:

- `page_updates`: per-page semantic deltas for `## Notes` and `## Open Questions`
- `proposed_connections`: proposed outbound links by page
- `proposed_new_pages`: title plus initial semantic content for any truly new atomic page
- `contradiction_items`: unresolved claim conflicts, if any
- `deferred_items`: work that should be queued because it exceeds budget or certainty
- `budget_exceeded`: boolean
- `reason`: short explanation of what changed

The heavy updater may rewrite `## Notes` and `## Open Questions` on touched atomic pages. It may propose connection changes and new pages, but final rendering remains code-owned.

### Merge contract

Code applies model output through a deterministic merge layer:

- validate that every proposed page still obeys atomic-page or topic-page shape rules
- reject any topic-page mutation that adds notes, sources, or open questions
- render `## Sources` from persisted artifacts
- normalize `## Connections` formatting and deduplicate entries
- strip empty sections
- reject unsupported claims that do not map back to the source artifact set
- if model output is invalid, retry once with a narrower request or fall back to `queue_review`

## Conflict And Review Model

### Contradictions

- When a new source conflicts with existing wiki knowledge, do not silently overwrite the older claim.
- Record the current competing claims on the affected page in `## Open Questions` unless the conflict is a normal chat-fact supersession case defined below.
- Add an entry to `wiki/review.md` for unresolved contradictions.

### Supersession vs contradiction

Treat a newer chat-derived statement as superseding an older one, not as a contradiction, only when all of the following are true:

- both claims are chat-derived
- both claims concern a stable personal fact or preference
- the newer statement is clearly intended to replace the earlier one
- there is no mixed-source disagreement with capture material or external sources

In that case:

- persist the newer chat artifact
- update the wiki to reflect the newer current fact
- preserve the older source artifact for auditability
- do not create an `## Open Questions` entry unless the replacement intent is ambiguous

### Review backlog

Add `wiki/review.md` as a first-class backlog file for unresolved contradictions and deferred work.

Each backlog entry should include:

- created date
- status
- reason
- affected wiki pages
- source artifact links
- next required action

Resolution behavior:

- resolved items may be edited in place to mark them resolved
- resolved items should not be deleted from the backlog unless a later cleanup rule is added
- `wiki/log.md` remains the historical audit trail; `wiki/review.md` remains the actionable queue

## Query-Time Writeback

Allow queries to write back only when the interaction exposes a concrete wiki gap worth preserving.

Eligible chat-derived writeback should be narrow by default:

- stable personal facts
- stable personal preferences
- durable resource references explicitly supplied by the user

Do not persist ephemeral chat content such as:

- one-off brainstorming
- temporary plans
- speculative statements
- ambiguous preferences

Writeback behavior:

- persist the chat artifact in `sources/chat/` before mutating wiki pages
- deduplicate against existing current chat-derived facts when the new artifact is materially the same
- use light update when a single existing page can absorb the fact safely
- use heavy update when the source touches multiple pages, creates a new page, or changes open questions
- when a query mutates the wiki, explicitly report the mutation in the answer

## Cost And Budgeting

Use the two-tier pipeline as the default cost-control mechanism.

Bound heavy maintenance on a per-source basis:

- max candidate pages considered
- max assembled context size
- max number of pages rewritten from one source
- max number of heavy updater calls per source

If heavy maintenance would exceed budget:

- apply the highest-confidence bounded update that still preserves invariants
- queue the remaining work in `wiki/review.md`
- log that deferral in `wiki/log.md`

One source must never trigger an unbounded multi-page rewrite.

## Bootstrap And Lint

### Bootstrap

- Bootstrap should reuse the same router and heavy updater contracts as normal ingest.
- Bootstrap may use larger budgets than routine ingest, but the same page-shape and provenance rules still apply.
- Bootstrap should prefer coverage over perfect restructuring in one pass; unresolved items may be queued into `wiki/review.md`.

### Lint

- Lint remains primarily deterministic.
- Lint should detect orphans, missing concept pages, contradictions, invalid page shapes, dead citations, and missing outbound links on atomic pages.
- Lint may append repair candidates into `wiki/review.md`, but it should not auto-rewrite wiki content unless a later implementation mode explicitly allows that.

## Public Interfaces And Files

The implementation and surrounding docs should define:

- two immutable source roots: `raw/` and `sources/chat/`
- source metadata including `source_kind`
- the router and heavy-updater response contracts
- the merge layer that renders sources and normalizes connections
- query-time writeback admissibility rules
- contradiction and supersession handling
- budget fallback behavior
- `wiki/review.md` as a first-class backlog artifact

Keep `wiki/log.md` as the chronological audit log, not the backlog queue.

## Acceptance Criteria

The design is good enough if the following scenarios are implementable and testable:

- Simple ingest:
  one new source updates one existing atomic page, preserves page shape, refreshes `wiki/index.md`, and appends one ingest entry to `wiki/log.md` without calling the heavy updater.
- Ambiguous ingest:
  one contradiction-prone source triggers heavy maintenance, updates `## Open Questions` on the affected page, and adds a backlog item to `wiki/review.md`.
- New atomic idea:
  one source creates exactly one new atomic page with at least one meaningful outbound link and at least one source citation tied to a persisted artifact.
- Topic-page protection:
  an attempted topic-page rewrite that adds notes or sources is rejected by code-owned validation.
- Query writeback:
  one stable chat-derived fact is persisted into `sources/chat/`, integrated into the correct wiki page, reported in the query answer, and logged.
- Chat correction:
  a later chat correction updates the current fact, preserves the older chat artifact, and avoids `## Open Questions` when the replacement intent is clear.
- Over-budget maintenance:
  the system applies only the highest-confidence bounded update, records the deferred work in `wiki/review.md`, and notes the deferral in `wiki/log.md`.
- Lint:
  lint can detect invalid page shape or missing outbound links and record repair work without directly mutating the wiki.
- Provenance:
  every material wiki claim in the touched pages can be traced to a persisted source artifact.

## Implementation Tasks

Implement the system as a staged sequence of small, independent tasks:

1. Source roots and metadata
   Add support for immutable source artifacts in `raw/` and `sources/chat/`, with explicit source metadata such as `source_kind`, `created_at`, stable id, title, and optional external URL.
2. Source persistence and citation rendering
   Implement code-owned source persistence, source lookup, and deterministic `## Sources` rendering for both source roots.
3. Page-shape validation
   Implement validators for atomic-page and topic-page invariants, including empty-section stripping and outbound-link checks.
4. Router contract
   Implement the cheap router call plus schema validation for `action`, `target_pages`, `candidate_new_pages`, contradiction risk, confidence, and reason.
5. Light update path
   Implement the bounded light-update path for small updates to existing atomic pages without calling the heavy updater.
6. Merge layer
   Implement code-owned merge logic for semantic deltas, including connection normalization, source rendering, deduplication, and invalid-output fallback.
7. Index and log refresh
   Implement deterministic `wiki/index.md` refresh and append-only `wiki/log.md` updates after every successful mutation.
8. Query-time chat writeback
   Implement persistence of eligible stable chat-derived facts or preferences into `sources/chat/`, followed by normal router and merge flow.
9. Supersession handling
   Implement historical progression behavior for superseded chat-derived facts or preferences so the wiki can reflect change over time without treating clear replacements as contradictions.
10. Review backlog
    Add `wiki/review.md` handling for unresolved contradictions, deferred work, and invalid or over-budget maintenance outcomes.
11. Heavy updater path
    Implement the heavy updater contract and bounded application flow for ambiguity, contradiction risk, multi-page impact, or new-page creation.
12. Budget enforcement
    Enforce per-source maintenance budgets and ensure deferred work is logged and queued when heavy maintenance exceeds limits.
13. Bootstrap integration
    Reuse the same router, merge, and validation pipeline for bootstrap with larger budgets and coverage-first defaults.
14. Lint integration
    Implement deterministic lint checks for orphans, invalid page shapes, dead citations, missing outbound links, and contradiction/reporting candidates.
15. Acceptance tests
    Add scenario-style tests for simple ingest, ambiguous ingest, new atomic page creation, topic-page protection, query writeback, chat correction, over-budget maintenance, lint reporting, and provenance traceability.

Recommended delivery order:

- Phase 1: tasks 1 through 7
- Phase 2: tasks 8 through 10
- Phase 3: tasks 11 through 12
- Phase 4: tasks 13 through 15

## Non-Goals

- turning the wiki into a generic RAG system
- auto-generating pages with no real source grounding
- creating massive summary pages that flatten distinct ideas
- forcing every source into a new page
- replacing deterministic operational ownership with a large heuristic rules framework

## Defaults

- Shared synthesis components are reused across bootstrap, ingest, and query maintenance, with mode-specific wrappers.
- Routine ingests auto-apply; human review is reserved for contradictions and deferred expensive work.
- Provenance is strict: synthesized prose is allowed, unsupported facts are not.
- Query writeback is enabled by default only when the interaction reveals a concrete durable gap worth preserving.
- Simplicity is preferred over a large deterministic rules framework; semantic routing is model-driven, with minimal code-owned guardrails around structure, provenance, and budget.
