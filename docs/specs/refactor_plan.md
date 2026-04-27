# Refactor Plan

This document defines the structural refactor plan for the vault codebase.

Use it as the implementation contract for modernization work that must preserve behavior. It complements, but does not replace, [LLM Wiki Architecture](./llm_wiki_architecture.md), [LLM Wiki Handoff v1](./llm_wiki_handoff_v1.md), and the operational rules in `AGENTS.md`.

## Goal

Improve maintainability and change velocity without changing intended behavior.

The refactor should:

- preserve public entrypoints and current runtime behavior unless a later task explicitly changes behavior
- break work into small reviewable passes
- reduce duplicated infrastructure and oversized modules
- make deterministic code easier to test in isolation
- keep wiki policy and operational semantics aligned with the existing specs

## Current Structural Problems

The current codebase is functional, but it is carrying several forms of structural drag:

- `scripts/bootstrap_wiki.py` is oversized and mixes parsing, rendering, URL fetching, synthesis prompting, split analysis, page mutation, caching, and CLI execution
- `scripts/vault_pipeline.py` is oversized and mixes capture ingest, source persistence, wiki integration, query writeback, linting, logging, locking, workspace management, and CLI execution
- workspace and filesystem helpers are duplicated across the two main modules
- several utilities have weaker test coverage than the core ingest and bootstrap paths
- top-level wrapper scripts are valid compatibility shims, but they should remain thin and should not accumulate logic
- stale or duplicate artifacts may remain in the repo and should be removed only after reference checks

## Invariants

These rules apply to every pass:

- preserve behavior unless a task explicitly calls for a functional change
- keep public CLI entrypoints stable:
  - `capture_ingest.py`
  - `ingest_raw_notes.py`
  - `run_vault_pipeline.py`
- keep source artifact immutability rules unchanged
- keep wiki rendering rules unchanged unless a behavior change is explicitly approved
- keep provenance, logging, and review-queue semantics aligned with existing specs
- prefer compatibility wrappers during refactor over broad call-site rewrites in one pass
- do not combine a structural refactor with a provider migration, dependency migration, or workflow redesign

## Public Surface To Preserve

The following interfaces should remain stable during refactor unless a later approved migration says otherwise:

- top-level script invocation contracts and exit behavior
- `scripts.vault_pipeline` CLI entry functions:
  - `capture_main`
  - `ingest_main`
  - `run_main`
- current wiki file shapes and section rules
- log and review artifact semantics
- publish and mirror script behavior

Internal function names may change if compatibility shims are kept long enough to avoid forcing a large rename wave in one pass.

## Required Parity Checks

Before and during refactor, behavior stability should be proven by repeatable checks.

Minimum required checks:

- run the existing test suite
- add characterization tests before moving logic that lacks strong coverage
- preserve byte-stable or semantically stable output for representative rendered wiki pages
- preserve routing decisions for representative ingest cases
- preserve CLI dispatch behavior for the thin top-level wrappers
- preserve mirror/publish inclusion and exclusion behavior

Recommended parity artifacts to add before deeper extraction work:

- golden fixtures for rendered wiki pages
- golden fixtures for log output
- fixture-driven tests for utility scripts
- a small API inventory of compatibility-sensitive functions and scripts

## Pass Structure

Refactor work should proceed in the following passes.

### Pass 0: Parity Harness

Current behavior:

- the repo already has a solid passing test baseline for core flows
- some utilities and boundary behaviors are covered only indirectly

Structural improvement:

- add characterization coverage for behavior that must remain fixed during refactor
- define representative fixture cases for bootstrap, ingest, query writeback, rendering, and utility scripts

Validation check:

- existing tests still pass
- new characterization tests pass against the pre-refactor code

### Pass 1: Dead Code And Stale Artifact Cleanup

Current behavior:

- stale or duplicate docs and repro-only files increase search and review noise

Structural improvement:

- remove clearly unused duplicate artifacts after verifying they are unreferenced
- move intentionally retained repro scripts into a clearer dev-only location if needed

Validation check:

- grep/reference checks show no live references were broken
- tests still pass
- remaining scripts still respond to `--help` where applicable

### Pass 2: Shared Workspace And Filesystem Layer

Current behavior:

- workspace configuration, temporary workspace handling, atomic file writes, and some parsing helpers are duplicated

Structural improvement:

- extract shared workspace and filesystem primitives into a dedicated module
- keep existing call sites stable through compatibility wrappers during the transition

Validation check:

- temp-workspace tests still pass
- file outputs remain stable
- no CLI behavior changes

### Pass 3: Extract Pure Domain Logic

Current behavior:

- parsing, rendering, and page/source transformation logic is mixed with orchestration and side effects

Structural improvement:

- move pure domain logic into focused modules such as page parsing, source parsing, rendering, routing helpers, and split helpers
- leave orchestration code in the pipeline layer

Validation check:

- representative render outputs remain stable
- routing and parsing tests remain stable
- no change to external scripts

### Pass 4: `vault_pipeline` Decomposition

Current behavior:

- `scripts/vault_pipeline.py` owns many unrelated responsibilities in one module

Structural improvement:

- split the module into narrower units for:
  - capture ingest
  - wiki integration
  - query writeback
  - lint/provenance
  - CLI plumbing

Validation check:

- end-to-end pipeline tests still pass
- wrapper scripts still dispatch identically
- log and review behaviors remain stable

### Pass 5: `bootstrap_wiki` Decomposition

Current behavior:

- `scripts/bootstrap_wiki.py` combines deterministic wiki logic with fetch, synthesis, split-analysis, caching, and CLI concerns

Structural improvement:

- separate deterministic transforms from side-effect adapters
- isolate fetch and synthesis clients from local page mutation rules

Validation check:

- bootstrap tests still pass
- mocked synthesis and split tests still produce the same outcomes
- bootstrap output for representative fixtures stays stable

### Pass 6: Internal API Normalization

Current behavior:

- direct-script execution fallbacks and historical import shapes make internal boundaries less clear than they should be

Structural improvement:

- normalize internal module boundaries and import conventions
- keep top-level wrappers and compatibility imports intact until the refactor is complete

Validation check:

- current top-level scripts still work
- package imports still resolve in tests and direct execution modes

### Pass 7: Utility Hardening

Current behavior:

- utility scripts are smaller, but some have limited direct coverage

Structural improvement:

- add focused tests and minor cleanup to:
  - `scripts/normalize_wiki_source_urls.py`
  - `scripts/sync_public_mirror.py`
  - `scripts/publish_private_and_public.py`

Validation check:

- fixture-based utility tests pass
- file inclusion/exclusion semantics stay stable
- rewrite planning output stays stable

## Recommended Sequencing

Use this order unless a specific local dependency changes it:

1. Pass 0
2. Pass 1
3. Pass 2
4. Pass 4
5. Pass 3
6. Pass 5
7. Pass 7
8. Pass 6

Rationale:

- parity coverage should land before structural movement
- cleanup should remove noise before deeper extraction
- shared helpers should exist before module decomposition
- `vault_pipeline` should usually be simplified before broad pure-logic extraction because it owns more orchestration pressure
- import normalization should come late, after boundaries are already real

## When To Start A Fresh Agent

If work is executed across multiple `/new` agent contexts, clear context at major boundary shifts rather than after every pass.

Recommended context boundaries:

1. Pass 0 and Pass 1
2. Pass 2
3. `vault_pipeline` decomposition work
4. `bootstrap_wiki` decomposition work
5. final verification and cleanup

Reason:

- each boundary changes the mental model enough that a fresh read is useful
- clearing context too often will slow down local continuity
- clearing too rarely increases the chance of acting on stale structural assumptions

## Migrations That Must Be Separate Tasks

The following should not be bundled into this refactor plan:

- LLM provider migration
- prompt contract redesign
- dependency upgrades that change runtime semantics
- CLI installation model changes such as moving from script files to packaged console scripts
- architecture redesigns that replace current configuration flow in one step
- publish workflow semantic changes
- wiki behavior changes such as different routing, rendering, contradiction handling, or source policies

If one of these becomes necessary, create a separate migration spec and land it independently.

## Deliverables

A refactor implementation is complete when it leaves behind:

- smaller, more focused modules
- preserved public entrypoints
- reduced helper duplication
- improved direct test coverage for previously under-owned areas
- a stable parity story proving behavior did not drift

## Acceptance Criteria

The refactor plan is successful if:

- all refactor passes can be reviewed independently
- public behavior remains stable across the refactor
- the two oversized modules are meaningfully decomposed
- shared infrastructure is no longer duplicated
- future changes can target smaller modules with less context loading
- migration-scale work has been explicitly excluded from the refactor track
