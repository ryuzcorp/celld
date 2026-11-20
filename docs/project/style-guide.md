---
title: Docs style guide
description: "How to write Cello docs: voice, structure, terms, and page types."
---

Write for a tired operator at 2 a.m. Lead with the answer, then explain. Cut every sentence that does not change what the reader does next.

## Voice

- Write in the second person. Say "you", not "one" or "the user".
- Use the active voice. Say "Cello answers a write", not "a write is answered".
- Keep sentences under 25 words. Split long ones.
- Keep paragraphs to 2–4 sentences. Start a new paragraph for a new idea.
- Use intent headings in sentence case: "Run a fleet", not "Fleet Management".
- Define each term where you first use it. Link to the concept page for the full story.
- Use no filler and no editorializing: no "simply", "just", "easily", "powerful", or "robust".

## Terms

Use one term per concept, exactly as below.

| Term | Meaning |
| --- | --- |
| cell | A Durable Object: a named server with its own SQLite database. Exactly one node serves a cell at a time. |
| node | One `cello` process on one machine. |
| fleet | The nodes that share one bucket. A fleet runs one application. |
| bucket | The object store that holds the fleet's long-term state (S3-compatible, Google Cloud Storage, or Azure Blob Storage). |
| binding | A named capability a Worker declares in its Wrangler configuration (KV namespace, D1 database, queue, R2 bucket). |
| deployment | An uploaded version of the application. Nodes adopt the serving deployment without a restart. |
| epoch | A fencing number on a cell's ownership record. Each activation advances it, so a node that lost the cell writes only into a superseded prefix. |
| resident | A cell held in a node's memory: **active** while it works, **idle** while it waits. |
| hibernated | A resident cell whose WebSocket clients stay connected while it waits. |
| inactive | A cell no node holds: only objects in the bucket, costing almost nothing. |
| fleet posture | The default durability mode: each write waits for one or two peer nodes to hold it on disk. |
| bucket posture | The durability mode that waits for the bucket on every write. |
| ensemble | The owner node plus the followers that replicate a cell's writes in fleet posture. |

## Page types

Put each page in the group that matches its type.

- **Get started** (root): the overview, install, and quickstart. Tutorials teach by doing; every command runs as written.
- **Guides**: how-to tasks under a named goal ("Run a fleet"). Give steps in order, with the expected output.
- **Concepts**: explanations of how Cello works ("Durability"). No steps; build understanding.
- **Reference**: complete boundaries: configuration, compatibility matrices, limits. Every option, flag, and variable appears once.
- **Services**: one page per service with a runnable example and its differences from Cloudflare.
- **Project**: what Cello adds over upstream, the changelog, contributing, and this guide.

## Frontmatter and links

- Every page sets `title` and `description`. The title names the page's promise.
- Link to pages by relative path. After moving a page, fix every link that points at it and add a redirect in `blume.config.ts` when the URL changes.
- Start every page with the frontmatter block on line 1. Put the `Modified by Ryuz Corp` marker comment on the line right after the closing `---`, never above it. A marker above the frontmatter makes the builder ignore the frontmatter and render the comment as the title. `npm run validate` checks this.
- Keep the sidebar order in each folder's `meta.ts`: install before quickstart, guides before reference.
