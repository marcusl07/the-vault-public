# LLM Wiki Handoff v1

This document is an implementation-oriented companion to [LLM Wiki Architecture](./llm_wiki_architecture.md).

Use the architecture doc as the canonical source for high-level system principles. Use this handoff doc for repo-specific implementation direction.

## Summary

Build the wiki as a two-layer system:

- immutable source artifacts remain the ground truth
- `wiki/` remains the maintained synthesis layer

Use a two-tier LLM pipeline for normal operation:

- every new source gets a cheap routing pass
- only sources that warrant deeper work trigger the heavier maintenance updater

The system should support writeback from both ingest and query flows, but keep provenance strict: every material claim in the wiki must trace back to a persisted source artifact.

## Source Model

### Source roots

- Keep `raw/` reserved for imported capture material from Obsidian/export flows.
- Add a second immutable source root for chat-derived source artifacts.
- Require source-type metadata on all persisted sources so downstream code can distinguish capture sources from chat sources without relying on path conventions alone.
- Never mutate source artifacts after creation.

### Source citations

- Keep wiki citation format the same for both source roots.
- Use normal `## Sources` entries with visible literal URLs when applicable and file links to the source artifact.
- If a later fact supersedes an earlier chat-derived fact, preserve the older source in audit/history rather than mutating it.

## Maintenance Pipeline

### Deterministic ownership

Code should remain authoritative for:

- capture export into `raw/`
- source identity and immutability
- source link rendering
- index refresh
- append-only log updates
- bounded per-source execution and fallback behavior

### Router pass

- Run a cheap router pass for every new source artifact.
- Keep the router output minimal: action type, candidate target pages, confidence, contradiction risk, new-page signal, and heavy-update-required flag.
- Let the router escalate on semantic grounds rather than a long heuristic tree:
  - ambiguity
  - contradiction risk
  - likely new atomic page
  - multi-page impact
  - reorganization need

### Heavy updater

- Invoke the heavy updater only when the router requests it.
- Let the heavy updater rewrite `## Notes` and `## Open Questions` on touched atomic pages.
- Preserve `## Sources` and `## Connections` deterministically during maintenance.
- The model may propose relationship changes, but final source formatting and connection rendering stay code-owned.
- Create a new page only when the source introduces a distinct reusable idea that meets the atomic-note rules from `AGENTS.md`.
- Keep topic pages compressed and pure: title plus `## Connections` only.

## Conflict And Review Model

### Conflicts

- When a new source conflicts with existing wiki knowledge, record both claims and flag the page rather than silently overwriting.
- Represent unresolved contradictions in a lightweight `## Open Questions` section on affected pages.

### Review backlog

- Add a separate markdown backlog file, e.g. `wiki/review.md`, for:
  - unresolved contradictions
  - deferred over-budget maintenance work
- Do not use `index.md` as the review queue.
- Keep `wiki/log.md` as the chronological audit log, not the backlog.
- Query answers should surface relevant pending review items when applicable.

## Query-Time Writeback

- Allow queries to write back when the query exposes a concrete wiki gap worth preserving.
- Allow chat-provided facts to become source-backed wiki knowledge only for stable personal facts and preferences.
- Persist those chat facts as immutable chat-source artifacts in the separate source root before updating wiki pages.
- If a later chat statement supersedes an earlier chat-derived fact, update the wiki to the newer fact and preserve the older source in audit/history rather than treating it as an unresolved contradiction by default.
- When query-time writeback creates or updates pages, explicitly report the mutations in the answer.

## Cost And Budgeting

- Use the two-tier pipeline as the default cost-control mechanism.
- Bound the heavy updater with a per-source maintenance budget:
  - max candidate pages considered
  - max assembled update context
  - max number of pages rewritten from one source
  - max number of heavy update calls per source
- If the heavy updater would exceed budget, apply the highest-confidence bounded update and queue the rest in `wiki/review.md`.
- Do not let one source trigger an unbounded multi-page rewrite.

## Public Interfaces And Files

- Update the implementation/docs to define:
  - two source roots
  - two-tier router/updater flow
  - query-time writeback rules
  - contradiction handling
  - maintenance budget fallback
- Extend source frontmatter/types to include a source kind such as capture vs chat.
- Add a review backlog file in `wiki/` as a first-class artifact.
- Keep `wiki/log.md` as the chronological audit log, not the backlog queue.

## Repo Workflow And Artifact Policy

### Bootstrap

- Bootstrap is a coverage-first initialization pass over existing `raw/` content.
- Batch processing is acceptable during bootstrap; precision can improve in later maintenance passes.
- Treat source paths as weak signals only. Do not reproduce input folder structure in the wiki.
- If a source includes an external URL, preserve the literal URL in wiki citations when the page is integrated.

### Navigation artifacts

- `wiki/index.md` is the primary navigation surface and should stay compact enough to fit comfortably in model context.
- `wiki/catalog.md`, when present, is the exhaustive lookup artifact and should be regenerated automatically rather than hand-maintained.
- Query flow should use `wiki/index.md` first, then `wiki/catalog.md` if available, then direct wiki search as a last resort.
- `wiki/log.md` is append-only historical trace.
- `wiki/review.md` is the actionable backlog for contradictions and deferred work.

### Logging shape

- Keep log headings grep-friendly and stable, for example:
  - `## [YYYY-MM-DD] ingest | Capture: "note title"`
  - `## [YYYY-MM-DD] query | "question text"`
  - `## [YYYY-MM-DD] lint | pass — 3 orphans found`
  - `## [YYYY-MM-DD] bootstrap | completed — ...`

### Private/public publishing workflow

- This repository is the private canonical source of truth.
- `raw/`, `sources/chat/`, and `wiki/` stay private here and must never be pushed to the public repo.
- Public publication should happen from a filtered mirror, not from direct pushes of this repo.
- Use `scripts/sync_public_mirror.py --dest <public-repo-root>` to copy publishable files into the public repository.
- Use `scripts/publish_private_and_public.py --public-repo <public-repo-root>` when a task requires committing and pushing both repos in one pass.
- Treat the public repo as disposable output regenerated from the private repo.
- After changes in this repo, commit and push the private repo unless a higher-priority instruction or user request says otherwise.
- After code-related changes, also sync, commit, and push the public repo unless a higher-priority instruction or user request says otherwise.

## Acceptance Criteria

The design is good enough if:

- a simple ingest can update one existing atomic page without a heavy rewrite
- an ambiguous or contradiction-prone source can trigger heavy maintenance and update `## Open Questions`
- a source that introduces a true new atomic idea creates one new atomic page with at least one meaningful outbound link
- topic pages remain topic-shaped and are not polluted with notes or sources
- query-time writeback can persist a chat-derived stable fact as a source artifact, update the relevant page, and report the mutation in the answer
- a later chat correction can supersede an earlier chat-derived fact while preserving auditability
- over-budget maintenance applies only the bounded highest-confidence update and records deferred work in `wiki/review.md`
- `index.md` remains compact and navigation-focused after ingest/query mutations
- `wiki/log.md` records ingest/query events without becoming the review queue
- material wiki claims remain traceable to persisted source artifacts

## Non-Goals

- turning the wiki into a generic RAG system
- auto-generating pages that have no real source grounding
- creating massive summary pages that flatten distinct ideas
- forcing every source into a new page
- replacing deterministic operational ownership with a large heuristic rules framework

## Defaults

- Shared core synthesis engine is reused across bootstrap, ingest, and query maintenance, with mode-specific wrappers.
- Routine ingests auto-apply; human review is reserved for contradictions and deferred expensive work.
- Provenance is strict: synthesized prose is allowed, unsupported facts are not.
- Query writeback is enabled by default, but only when the query reveals a concrete gap worth preserving.
- Simplicity is preferred over a large deterministic rule framework; semantic routing is model-driven, with only minimal code-owned operational guardrails.
