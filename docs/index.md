<!-- Modified by Ryuz Corp (Cello): CELLO_MEMORY_SCOPE; deploy --sourcemap writes the bundle's external map; operator token for the internal listener; r2 list --delimiter, the --page shape, and --cursor continuation; Workers Cache API page; deployment cache scope; deploy --sourcemap collects the no_bundle sibling map; staged invoke docs; deploy confines esbuild reads to the project; deploy --read-root; CELLO_SQLITE_CACHE_KB; CELLO_C_TRIM_MB; bucket-posture group commit docs: bundle switch default-on, linger, guarantees (0070); bundle switch defaults off (0075); CELLO_BACKGROUND_BUDGET_S (upstream #275); CELLO_TMP_MAX_BYTES (upstream #280); CELLO_SQL_STATEMENT_MS (upstream #274); CELLO_MAX_CELLS_PER_ISOLATE (upstream #278). -->
# Cello

Cello is a stateful distributed system. It runs server-side JavaScript on
your machines, and it stores the long-term state in a bucket that you own:
S3-compatible, Google Cloud Storage, or Azure Blob Storage. The JavaScript
API and the configuration follow Cloudflare Workers: Workers, Durable
Objects, KV, Queues, D1, R2, Workflows, Cron Triggers, and static assets.

## Cells and nodes

In Cloudflare terms, a cell is a Durable Object: a small server with a
name and a private SQLite database. You make one cell for each user,
document, chat room, or AI agent. A cell serves HTTP, holds WebSocket
connections, sets alarms, and makes outbound connections. Cells share no
database. Each cell runs on one thread: a second request interleaves only
while the first awaits, and storage operations are synchronous.

You run Cello as one process on each machine. That process is a **node**,
and the nodes that share one bucket are a **fleet**. Any node can serve
any cell, so you add capacity by starting another node against the same
bucket.

## Cell lifecycle

A cell has the same states as a Durable Object. A **resident** cell is in
memory: it is **active** while it does work, and **idle** when it waits.
Cello removes an idle cell from memory after `CELLO_IDLE_EVICT_S` seconds
without work, and earlier under memory pressure or at the residency cap.
Without `CELLO_IDLE_EVICT_S`, only pressure or the cap removes an idle
cell. A cell that keeps its hibernatable WebSocket clients on its node is
**hibernated**. A cell that no node holds is **inactive**: it is only an
object in the bucket and costs almost zero. Every cell starts inactive.

The constructor runs again on each activation, because a cell keeps no
memory across these transitions. A hibernated cell wakes like a cold start,
but its WebSocket clients stay connected and it stays on its node.

Each activation can add a new epoch prefix to the bucket. Cello deletes
the older prefixes only when `CELLO_LTX_RETENTION_SECS` enables epoch GC,
so without it the bytes of a cell increase with each activation. See
[epoch GC](guarantees.md#epoch-gc).

One 8 GB node holds 1,000 resident cells, so one resident cell costs
approximately $0.05 each month.

## Ownership and durability

Exactly one node serves a cell at a time. A node claims a cell with a
conditional write of a small record to the bucket, so the bucket decides
who wins. There is no leader election and no membership list. The claim
expires unless the node renews it, so a failed machine releases its cells.

Cello does not answer a write until the data survives a failure (RPO=0).
`CELLO_DURABILITY` selects how Cello proves that a write is durable:

- `bucket`: the node answers after the write reaches the bucket. Each write
  costs one object store round trip.
- `fleet` (the default): the node sends each write to one or two other
  nodes, and it answers when they hold it on disk or when the bucket upload
  finishes, whichever comes first.

A single node has no peer, so a one-node fleet in `fleet` mode waits for the
bucket on every write, as `bucket` mode does. Two or more
nodes are faster, because a follower fsync is much faster than an object
store write. Run two or more nodes if write latency
matters. A fleet that loses all but one node falls back to the bucket in the
same way, and it stays correct. Set `bucket` to wait for the bucket on every
write, even when peers are available.

In bucket posture, writes are grouped only when the operator opts in: with
`CELLO_BUCKET_BUNDLES=1`, once per flush round the node writes one shared
bundle object carrying every dirty cell's new rows, and that PUT is the
durability proof for every row it carries — instead of one upload per write.
Grouping needs every live lease to read format 3 (see
[guarantees](guarantees.md#bucket-posture-bundle-proofs)); while an older
lease is live the node uploads per-cell as before. The default (`0`) uploads
per-cell always. `CELLO_BUCKET_BUNDLE_LINGER_MS` holds a round open so
concurrent writes share one PUT, but only while more than one cell is dirty.

Grouping trades write latency for request cost. Measured at `DELAY_MS=100`,
release builds, 1 node: the 200-agent workload drops from 1934 to 1140 Class A
requests with grouping on, but write p50 rises from 210 ms to 445 ms; a hot
cell with 16 writers rises from 235 ms to 432 ms p50 with ~45% fewer turns.
Enable `CELLO_BUCKET_BUNDLES=1` for request-cost-sensitive workloads that
tolerate about one extra round trip per durable write.

When a node stops, another node takes its cells over, and first collects
the data that the stopped node had not uploaded. See
[what Cello guarantees](guarantees.md) for the mechanism and the bucket
requirements.

## Alarms

When an event sets an alarm before its response boundary, Cello does not
send a successful response until a durable wake entry covers the alarm. A
later `waitUntil` alarm does not delay the response of another event.

A hibernated cell fires its alarm on the node that owns it. One node holds
the waker role, and it wakes only the cells whose owner node has stopped.

## What do you build with cells

- **Real-time applications.** A multiplayer game, a chat room, or a
  collaborative document is one cell. The room needs no lock and no
  external message bus.
- **Agents.** Each AI agent is one cell with its memory, schedule, and
  inbox in its own SQLite database. An inactive agent costs almost nothing.
- **Sharded web applications.** One cell for each user, tenant, or device
  shards the application from the start, so no shared database exists.

## Start here

- [Install](install.md): the one-line installer, Docker images, and requirements.
- [Operations](operations.md): configure storage, deploy, develop locally, run and scale nodes.
- **Services** — one page per service in the sidebar, each with a runnable example and the known differences from the Cloudflare service.
- [What Cello guarantees](guarantees.md): the durability and failover contract.
- [What Cello adds over celld](what-cello-adds/index.md): every improvement over upstream, grouped by area.
- [Compatibility with celld](compatibility.md): frozen identifiers and upgrading from a celld deployment.
- [Changelog](changelog.md): what shipped in each release.
