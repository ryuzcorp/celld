<!-- Modified by Ryuz Corp (Cello): CELLO_MEMORY_SCOPE; deploy --sourcemap writes the bundle's external map; operator token for the internal listener; r2 list --delimiter, the --page shape, and --cursor continuation; Workers Cache API page; deployment cache scope; deploy --sourcemap collects the no_bundle sibling map; staged invoke docs; deploy confines esbuild reads to the project; deploy --read-root; CELLO_SQLITE_CACHE_KB; CELLO_C_TRIM_MB; bucket-posture group commit docs: bundle switch default-on, linger, guarantees (0070); bundle switch defaults off (0075); CELLO_BACKGROUND_BUDGET_S (upstream #275); CELLO_TMP_MAX_BYTES (upstream #280); CELLO_SQL_STATEMENT_MS (upstream #274); CELLO_MAX_CELLS_PER_ISOLATE (upstream #278). -->
---
title: Cello
description: Self-hosted, distributed Durable Objects. Run your Cloudflare Workers application on your own machines.
---

Cello runs your Cloudflare Workers application on your own machines: Workers, Durable Objects, KV, Queues, D1, R2, Workflows, Cron Triggers, and static assets. You deploy the Wrangler project you already have. Cello stores the long-term state in a bucket you own: S3-compatible, Google Cloud Storage, or Azure Blob Storage.

You run Cello as one process on each machine. That process is a **node**, and the nodes that share one bucket are a **fleet**. Any node serves any cell, so you add capacity by starting another node against the same bucket. There is no control plane, no membership list, no leader election, and no consensus service.

## Cells hold your state

In Cloudflare terms, a **cell** is a Durable Object: a named server with its own SQLite database. You make one cell for each user, document, chat room, or AI agent. A cell serves HTTP, holds WebSocket connections, sets alarms, and makes outbound connections.

Each cell runs on one thread and owns its database alone. Exactly one node serves a cell at a time: a node claims a cell with a conditional write to the bucket, so the bucket decides who wins. When a node stops, another node takes its cells over and recovers the writes the stopped node had not uploaded yet.

An idle cell leaves memory after `CELLO_IDLE_EVICT_S` seconds, or earlier under memory pressure. A cell that no node holds is **inactive**: it is only objects in the bucket and costs almost nothing. One 8 GB node holds about 1,000 resident cells.

## Writes survive failure

Cello answers a write only after the data survives a failure (RPO=0). `CELLO_DURABILITY` selects how Cello proves that a write is durable. In `fleet` mode (the default), the node sends each write to one or two peer nodes and answers when they hold it on disk. In `bucket` mode, it answers after the write reaches the bucket.

A one-node fleet has no peer, so it waits for the bucket on every write. Run two or more nodes if write latency matters. Read [what Cello guarantees](concepts/durability.md) for the full contract.

## Start here

- [Install](install.md): the one-line installer, Docker images, and requirements.
- [Quickstart](quickstart.md): run a Worker with a Durable Object locally, then share it across a two-node fleet. Takes about 15 minutes.
- [Run a fleet](guides/run-fleet.md): configure storage, deploy, develop locally, and operate nodes.
- [Concepts](concepts/durability.md): the durability contract and how Cello is tested.
- [Services](services/workers.md): one reference page per service, each with a runnable example.
- [Project](project/what-cello-adds/index.md): what Cello adds over upstream, the changelog, and how to contribute.
