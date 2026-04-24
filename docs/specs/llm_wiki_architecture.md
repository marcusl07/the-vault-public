# LLM Wiki Architecture

## Purpose

This document is the canonical architecture reference for the LLM wiki system.

It exists to govern repo-level design decisions without putting a long-form architectural essay into `AGENTS.md` on every run. Use this document when making decisions about system shape, workflow boundaries, ingestion strategy, query behavior, maintenance patterns, or supporting tooling.

## Core Idea

Most LLM document workflows are retrieval systems. They search raw material at query time and re-synthesize an answer from scratch each time. That works, but it does not accumulate understanding.

The LLM wiki pattern is different. The system maintains a persistent, interlinked wiki that sits between the user and the raw source material. When a new source arrives, the system does not merely index it for later retrieval. It reads it, extracts the useful knowledge, integrates that knowledge into existing pages, updates the graph, and preserves the synthesis as durable state.

The wiki is the compiled knowledge layer. Queries should read from that maintained layer first rather than repeatedly rediscovering the same facts from raw notes.

## Architectural Model

The system has three layers:

### 1. Raw sources

Immutable source material such as captured notes, articles, papers, transcripts, or images. This is the source of truth. The system may read from this layer but must not modify it.

### 2. The wiki

A directory of LLM-maintained markdown pages. This is the synthesis layer. The system creates pages, updates existing ones, maintains cross-links, records contradictions, and keeps the knowledge base coherent over time.

### 3. The schema

The instruction layer that teaches the LLM how to operate on the wiki. In this repo, `AGENTS.md` is the active operational schema. Additional documents in `docs/specs/` can define architectural doctrine and implementation guidance that should shape future evolution without being loaded every run.

## Design Principles

- Prefer persistent synthesis over repeated raw retrieval.
- Treat the wiki as a compounding artifact that improves with each ingest and query.
- Keep raw sources immutable.
- Update existing pages when new material sharpens an existing concept; create new pages only when there is a genuinely distinct idea.
- Favor tight, meaningful cross-links over dense but weak graph structure.
- Flag contradictions instead of silently overwriting earlier claims.
- Keep the index usable as the primary navigation layer at moderate scale.
- Prefer simple local workflows over prematurely adding embedding or RAG infrastructure.

## Operating Loops

### Ingest

When a new source arrives:

1. Read the source from `raw/`.
2. Identify the existing wiki pages it relates to.
3. Update those pages or create new ones as needed.
4. Add meaningful wikilinks where the relationship is real.
5. Update `wiki/index.md`.
6. Append a chronological entry to `wiki/log.md`.

One source may update many wiki pages. The goal is integration, not just storage.

### Query

When answering a question:

1. Read `wiki/index.md` first to identify relevant pages.
2. Read the most relevant wiki pages.
3. Follow one or two tight wikilink hops when they add real context.
4. Answer from the maintained wiki, not from general memory and not by rediscovering everything from raw sources.

Useful query outputs may themselves become durable wiki artifacts when they add lasting value.

### Lint

Periodically inspect the wiki for structural health:

- orphan pages
- stale claims superseded by newer sources
- contradictions between pages
- concepts mentioned repeatedly without their own page
- missing cross-links
- clusters that deserve a topic page
- gaps that justify finding more sources

## Human / LLM Split

The human curates sources, directs inquiry, evaluates output quality, and decides what matters.

The LLM performs the bookkeeping work humans rarely maintain consistently at scale: summarization, integration, cross-linking, index maintenance, contradiction surfacing, and routine wiki hygiene.

The intended relationship is:

- Obsidian or the markdown repo is the interface for browsing and editing artifacts
- the LLM is the maintainer
- the wiki is the codebase

## Tooling Stance

This architecture does not require a full RAG stack.

At small to moderate scale, `wiki/index.md` can act as the primary navigation surface. Search tooling may be added later when scale justifies it, but it should support the wiki-first architecture rather than replace it with raw-document retrieval. Optional tools such as markdown search, local indexing, slide generation, charts, or structured metadata are extensions, not prerequisites.

## Implications For This Repo

- `raw/` remains immutable source material.
- `wiki/` remains the maintained synthesis layer.
- `AGENTS.md` remains the always-on operational contract.
- `docs/specs/` holds lower-frequency architectural and implementation guidance.
- New specs should align with this document unless they explicitly supersede part of it.

## Non-Goals

- Turning the system into a generic chunk-retrieval workflow.
- Treating the wiki as a thin cache over raw sources.
- Forcing every source into a brand-new page.
- Reproducing input folder structure in the wiki.
- Adding complex retrieval infrastructure before the index and wiki graph stop being sufficient.

