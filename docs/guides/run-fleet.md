---
title: Run a fleet
description: Start nodes, add capacity, feed an autoscaler, roll out gracefully, diagnose, and list cells.
---

<!-- Modified by Ryuz Corp (Cello): CELLO_MEMORY_SCOPE; deploy --sourcemap writes the bundle's external map; operator token for the internal listener; r2 list --delimiter, the --page shape, and --cursor continuation; Workers Cache API page; deployment cache scope; deploy --sourcemap collects the no_bundle sibling map; staged invoke docs; deploy confines esbuild reads to the project; deploy --read-root; CELLO_SQLITE_CACHE_KB; CELLO_C_TRIM_MB; bucket-posture group commit docs: bundle switch default-on, linger, guarantees (0070); bundle switch defaults off (0075); CELLO_BACKGROUND_BUDGET_S (upstream #275); CELLO_TMP_MAX_BYTES (upstream #280); CELLO_SQL_STATEMENT_MS (upstream #274); CELLO_MAX_CELLS_PER_ISOLATE (upstream #278). -->

You run one `cello` process per machine, all pointed at the same bucket. For a new machine, start at [Install](../install.md). Storage setup lives on the [storage](storage.md) page, deploys on the [deploy](deploy.md) page, and every environment variable on the [configuration](../reference/configuration.md) page.

## Configure object storage

Set `CELLO_BUCKET` and the matching credentials before you start a node. See [configure storage](storage.md) for every provider.

## Deploy an application

Deploy with `cello deploy`, then watch nodes adopt the release in place. See [deploy an application](deploy.md).

## Develop an application locally

Use `cello dev` for local development with persistent state. See [develop locally](local-dev.md).

## Operate D1, KV, and R2

Use `cello d1`, `cello kv`, and `cello r2` against the fleet bucket. See the [CLI reference](../reference/cli.md) for every subcommand and flag.

## Invoke a staged version

Send one request to a staged version with `cello invoke`. See [stage versions](staged-versions.md).

## Start a node

For local development, the default listener is sufficient:

```sh
cello \
  --bucket "$CELLO_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

For a fleet node, bind the public and internal listeners separately:

```sh
cello \
  --bucket "$CELLO_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION" \
  --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 \
  --advertise node-a.internal:8081
```

`--advertise` requires an explicit `--internal-listen`, and so does a non-loopback `--listen`. You must route the advertised address to the internal listener, not to the public listener; Cello cannot verify this.

## Add nodes

Start each node with the same bucket settings and its own reachable `--advertise` address. The nodes find each other through leases in the bucket; there is no join command.

Peer traffic has no TLS and cell fetch and RPC traffic is not signed, and without `CELLO_OPERATOR_TOKEN` the internal listener has an unauthenticated operator API. Put the advertised addresses on a trusted private network or an encrypted overlay such as WireGuard or Tailscale, and never expose the internal listener to the internet. See [security](security.md).

The node that receives a request activates a new cell, so a load balancer must include a new node in its rotation. A node at its residency cap or under memory pressure hands the activation to the least-loaded peer. A pressure eviction releases the ownership record (`CELLO_PRESSURE_OWNERSHIP=release`, the default), so the cell can move. An idle eviction keeps the record, so the cell wakes on its own node. An idle eviction must stop the runtime within `CELLO_OPERATION_DEADLINE_MS`, or the cell stays resident until the next idle period.

A new node also receives idle cells. Every node reads a shared fleet sample, `fleet/capacity-v1.json`, every `CELLO_REBALANCE_INTERVAL_MS` (default 5000; `0` disables balancing). Each node gets a share of the owned cells in proportion to `CELLO_PLACEMENT_WEIGHT` (default: the CPU count). The most loaded node hands at most 32 idle cells at a time to the node furthest below its target, and a receiver fills to 2% below its target. Only a hibernated cell moves; its hibernatable WebSockets close with code 1012 so that the clients reconnect. A fleet without `CELLO_IDLE_EVICT_S` therefore balances only the cells that hibernate on their own. A draining node and a node with a cold-activation backlog (`restoring`) receive no cells. Nothing moves while a node reports a lease without a weight, so a rolling upgrade completes first. `POST /rebalance/pause` on any node pauses balancing for the fleet, and `POST /rebalance/resume` on the same node resumes it.

## Feed an autoscaler

Cello does not scale itself. An external system starts and stops the nodes, and Cello hands the cells off.

Each node writes its lease to `nodes/<node>.json` in the bucket. The lease lives `CELLO_TTL_MS` (default 10000) and renews at one third of that. Its load block, dated by `sampled_ms`, contains `owned_cells`, `placement_weight`, `resident_cells`, `host_websockets`, `rss_bytes`, `in_use_bytes`, `cpu_percent_x100`, `open_fds`, `pressured`, `memory_headroom`, `shed_cells`, and `restoring`. `owned_cells` includes dormant cells; a restarted node omits it until it has read its ownership records. `resident_cells` counts only cells in memory. The memory and CPU values are sampled every second.

`GET /state` on the internal listener reports the live counters. Its `node_load` object is the lease load block.

| Field | Meaning |
| --- | --- |
| `capacity_waiting` | Activations queued behind the residency cap. Positive means add a node |
| `activation_waiting`, `restoring` | Cold activations that wait for or hold a permit |
| `owned_cells`, `occupied`, `shedding` | Ownership, residency, and pressure shedding |
| `handed_off`, `rebalanced`, `rebalance_failed` | Cells given to peers, the part balancing moved, and the balancing moves no peer took |
| `remote_route_refreshes` | Cached routes that expired and started a new lookup. Normal lease renewal increases it |
| `allocator` | Rust allocator bytes: `allocated_bytes`, `resident_bytes`, `mapped_bytes`, `retained_bytes`. V8 heaps are not included |
| `libc_malloc` | Linux only: `in_use_bytes` and `free_bytes` of the C allocator that SQLite and V8 use |
| `deployment.isolates`, `deployment.draining` | Per Worker script (`cells`, `stateless`, `services`): `live`, `live_empty`, `retiring`, `freed`, `heap_bytes`, `external_bytes` |

A `live_empty` count that persists for more than 30 seconds means that isolate maintenance does not run. A persistent `retiring` count means that a turn or a request still holds the heap.

A positive `capacity_waiting` or a `pressured` lease is the signal to add a node. Scale down only when every remaining node reports `memory_headroom` and a small `restoring` backlog. A drain into a full fleet leaves the cells dormant on the survivors, and each later activation must shed a resident cell.

`/.well-known/cello/health` on the public listener is a boolean. It reports 503 during a drain and before a joining node settles.

## Shut down and roll out a node

Cello shuts down gracefully on SIGTERM or SIGINT. The health path reports 503, new public requests receive 503 with the connection closed, and accepted requests finish. The node hands its cells off in batches: it cancels running alarms (the successor retries them), proves the data durable, publishes a final snapshot, and releases each ownership record to a peer. The peer keeps the cell dormant until a request arrives. A database larger than the durability deadline can upload (80 MiB at the default 10 seconds) hands off without the snapshot. Busy cells move first. `POST /shutdown` on the internal listener starts the same handoff.

Concurrent demand-driven restores and startup work are bounded by `CELLO_ACTIVATIONS`, and concurrent complete handoffs by `CELLO_RELEASES` (default 128) (see [configuration](../reference/configuration.md)).

Concurrent stops do not flood the survivors: a draining node claims a fleet drain token, so nodes hand off one at a time. A node that cannot claim the token within its wait proceeds without it.

A new process does not report healthy until the fleet settles: no node drains, every node is below its memory low watermark, the restore backlogs are small, and ownership is balanced. At `CELLO_READY_FLEET_GATE_MS` (default 120000; `0` disables the gate), Cello logs `ready_gate_expired` once, but readiness stays closed, so set an orchestrator rollout deadline. After the first healthy response, fleet state does not remove readiness.

A cut handoff can leave the log of the stopped process for its replacement to recover. Another process can replace a recovery that does not respond for 30 seconds.

A remote call to a cell that is changing owner waits for the new owner, for at most `CELLO_OPERATION_DEADLINE_MS` (default 15000). Cello retries only an attempt that provably did not start. An application must keep one stable operation ID when it retries an ambiguous fetch, RPC, D1, or service operation. A WebSocket client must reconnect.

To roll out a new version, use the rolling update of your orchestrator: stop each node with SIGTERM, wait for its replacement to report healthy, and then move to the next node. Cello paces the handoffs and readiness itself.

`POST /shutdown?handoff=preserve` prepares a same-node restart that keeps the ownership records. It cancels cold activations and uploads each stopped database first. If an upload fails, the next process uses normal recovery. The internal operator API is alpha and can change in any release, so keep operator tooling and Cello releases together.

## Upgrade notes

Each release names its own upgrade path: stop-all or rolling, renames, and downgrade limits. See [upgrade](upgrade.md).

## Diagnose a fleet

`cello diagnose` reads the node leases and probes each live peer. It does not take a lease or change ownership. Use `--peer NODE_ID`, repeatable, to probe only some nodes.

```sh
cello diagnose \
  --bucket "$CELLO_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

The report shows expired leases, bad advertised addresses, unreachable peers, authentication failures, and protocol mismatches. Each node line shows `restoring`, the cold activations in progress. During a rolling update, wait for `restoring=0` on every node before you restart the next.

## List Durable Objects

```sh
cello cell list \
  --bucket "$CELLO_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

Each line is a `Class:ID` scope. Give a class name to list only that class, and pass `--json` for one JSON object per line. An instance appears after its first event; an ID that is only derived does not appear. D1 databases, KV namespaces, and Workflows are cells in reserved classes whose names start with `__`. The `--json` output marks them `"reserved": true`:

```sh
cello cell list --all --json --bucket "$CELLO_BUCKET" |
  jq -r 'select(.reserved | not) | .scope'
```

The command stops at 1000 instances and prints the continuation on stderr:

```
1000 cells shown; more exist. Continue with --after Room:d99d9174b25e46310694dd931b47fbde70a7460bb7b210b546060651ea2ff6e0
```

Pass that `--after SCOPE` for the next page, `--limit N` for a different page size, or `--all` for everything (one request per 1000, with progress on stderr). The listing is not a snapshot, but a sequence of `--after` commands lists each existing instance once.

`cello cell gc --dry-run` takes the same options and prints, for each cell, the epoch prefixes that epoch GC can delete, their bytes, and the restore base. It writes nothing, and it exits with an error at the end if it could not read a cell.

```sh
cello cell gc --dry-run --bucket "$CELLO_BUCKET" --grace-secs 3600
```

The real deletion can come later or not at all. Only a node with a positive `CELLO_LTX_RETENTION_SECS` deletes, with that grace instead of `--grace-secs`. A cell deletes only while it is active, after its activation has a write and, if paged, after its local file is complete. With `CELLO_LTX_COMPACTION=0`, a cell on a fleet node waits for a handoff snapshot. One pass deletes at most 64 epochs of a cell.

## Handle a hot cell

Cello admits at most 64 concurrent fetch events for one Durable Object (`CELLO_MAX_CELL_REQUESTS`). A Queue broker admits 256 concurrent producer calls, commits at most 64 in one transaction, and overlaps up to four transactions.

Over the limit, Cello returns `503` with `Retry-After: 1` and `x-cello-overload: cell`, and does not start the event. The Queue response body is `{"error":"cell admission refused"}`, and a caught Queue producer error contains `cell overload: admission refused`. A load generator must count these responses as rejected work. Cello logs `cell_overload_refused` with the cell scope, node, region, in-flight count, and limit.

## Environment variables

Every `CELLO_*` variable, default, and removed setting lives on the [configuration](../reference/configuration.md) page. An unset variable selects its default, and a Boolean variable accepts only `0` or `1`. Cello exits at startup on an invalid value.
