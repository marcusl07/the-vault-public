# The Vault

The Vault is my take on Andrej Karpathy's LLM wiki idea, blended with a Zettelkasten.

![The Vault graph](assets/vault-graph.png)

The basic bet is simple: notes should not just pile up. They should compound. Instead of treating an LLM like a search box over raw files, this system uses it as a wiki maintainer that reads new material, integrates it into existing ideas, and keeps the knowledge graph moving.

## What This Is

This is a personal wiki workflow built around atomic markdown notes, durable synthesis, and explicit links between ideas.

The Karpathy-inspired part is the maintained wiki layer: the LLM does not just retrieve notes at question time. It writes and updates a persistent layer of understanding as new sources come in.

The Zettelkasten part is the shape of the wiki itself: small notes, one idea at a time, connected through meaningful links instead of folders or rigid categories.

## How It Works

- I write and collect notes in Obsidian.
- New captures are ingested into the wiki workflow.
- The system updates existing wiki pages when a note sharpens an idea.
- It creates new atomic pages when a source introduces a genuinely reusable concept.
- It maintains links, navigation pages, and a chronological log as the wiki evolves.
- I query the wiki through Codex, which reads the maintained pages first instead of searching the web or re-summarizing everything from scratch.

## Why

Most note systems make it easy to save things and hard to make them useful later. Most LLM workflows answer the same questions from scratch every time.

The Vault is meant to sit between those two extremes. It keeps the original note-taking flow lightweight, then uses an LLM to do the maintenance work that makes a wiki valuable over time: synthesis, connection, contradiction tracking, and cleanup.

The goal is not a perfect archive. The goal is a living knowledge base that gets more coherent each time it is used.
