---
title: Configuration reference
description: Every CELLO_* environment variable, its default, and what it changes. Boolean variables accept only 0 or 1.
---

You configure a node and every operator subcommand through the environment. An unset variable selects its default. A Boolean variable accepts only `0` or `1`. Cello exits at startup on an invalid value, including a removed setting listed at the end.

Telemetry variables (`CELLO_OTEL*`, `OTEL_*`) live on the [telemetry](../guides/telemetry.md) page.

## Storage

| Variable | Default | Effect |
| --- | --- | --- |
| `CELLO_BUCKET` | unset (required) | The fleet bucket, with an optional key prefix. Same as `--bucket`. See [storage](../guides/storage.md). |
| `S3_ENDPOINT` | unset | The S3-compatible endpoint. Same as `--endpoint`. |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | `us-east-1` | The storage region. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | unset | Explicit AWS credentials. The standard AWS credential chain is also available. |
| `CELLO_DEPLOY_AWS_ACCESS_KEY_ID`, `CELLO_DEPLOY_AWS_SECRET_ACCESS_KEY`, `CELLO_DEPLOY_AWS_SESSION_TOKEN` | unset | A second, read-only credential over the deployment pointer and bundles: a node reads `deploy/` and `deploy-blobs/` with it and everything else with `AWS_*`. S3 only; the access key and secret are both required. |
| `GOOGLE_APPLICATION_CREDENTIALS`, `GOOGLE_SERVICE_ACCOUNT_KEY` | unset | Google credentials for a `gs://` bucket. Application Default Credentials are also available. |
| `AZURE_STORAGE_ACCOUNT_NAME` | unset | The storage account for an `az://` bucket. |
| `AZURE_STORAGE_ACCOUNT_KEY` | unset | The storage account key. Do not combine it with an identity selector. |
| `AZURE_AUTHORITY_HOST`, `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERAL_TOKEN_FILE` | unset | The AKS workload identity environment. The authority must be the public Azure host. |
| `AZURE_STORAGE_USE_EMULATOR` | unset | Set to `true` to develop against Azurite. |

## Listeners and identity

| Variable | Default | Effect |
| --- | --- | --- |
| `CELLO_ADDR` | `127.0.0.1:8080` | The public Worker listener. Same as `--listen`. |
| `CELLO_INTERNAL_ADDR` | `127.0.0.1:0` | The peer and operator listener. Same as `--internal-listen`. |
| `CELLO_ADVERTISE` | unset | The internal address that peers can reach. Same as `--advertise`. |
| `CELLO_UNSAFE_PUBLIC_ADVERTISE` | `0` | Set to `1` to permit a literal public IP in `CELLO_ADVERTISE`. It does not resolve a DNS name or restrict the internal listener. |
| `CELLO_OPERATOR_TOKEN` | unset | A bearer token the whole fleet shares. When set, every internal-listener route requires `Authorization: Bearer <token>` and answers 401 without it. Set but empty: the node refuses to start. |
| `CELLO_TRUST_FORWARDED_HEADERS` | `0` | Set to `1` to trust `X-Forwarded-Host` and `X-Forwarded-Proto`. Same as `--trust-forwarded-headers`. |
| `CELLO_NODE` | generated, pinned to `node-id` in the data dir | An explicit node-session ID: 1 to 128 ASCII letters, numbers, dots, dashes, or underscores, but not `.` or `..`. |
| `CELLO_WATCH` | platform temp dir | The local work directory for SQLite and replication. Set it to a persistent directory to keep the node ID across restarts. |
| `CELLO_ESBUILD` | `esbuild` on `PATH` | The path of the esbuild executable: the self-contained release binary, since on Linux it runs with reads confined to the project and its read roots. |
| `CELLO_DEV_STATE` | `PROJECT/.cello/dev` | The `cello dev` state directory. Same as `--state`. |
| `RUST_LOG` | `info` | The runtime log filter. |

## Deployments

| Variable | Default | Effect |
| --- | --- | --- |
| `CELLO_DEPLOY_POLL_S` | `30` | The deployment pointer poll interval in seconds. |
| `CELLO_DEPLOY_MAX_AGE_S` | `60` | Seconds before Cello forces a resident Durable Object onto a new deployment. `0` forces at once. |

## Requests and isolates

| Variable | Default | Effect |
| --- | --- | --- |
| `CELLO_OPERATION_DEADLINE_MS` | `15000` | The deadline for a non-restore operation. |
| `CELLO_MAX_CELL_REQUESTS` | `64` | Concurrent fetch events for one Durable Object. Past it, Cello answers `503` with `Retry-After: 1`. |
| `CELLO_MAX_CELLS_PER_ISOLATE` | `32` | Cells packed into one isolate. Lower it for heavyweight cells such as agents. |
| `CELLO_MAX_REQUEST_BODY_BYTES` | `1 GiB` | The body limit for a public Worker request or a direct Durable Object request. |
| `CELLO_TMP_MAX_BYTES` | `67108864` (64 MiB) | The per-request `/tmp` size limit in bytes. A write past it fails with `ENOSPC`. |
| `CELLO_HANDLER_BUDGET_S` | `300` | The JavaScript handler budget in seconds. |
| `CELLO_BACKGROUND_BUDGET_S` | `30` | Post-response background budget in seconds. Past it the driver ends leftover timers and `waitUntil` work. |
| `CELLO_FETCH_TIMEOUT_S` | `120` | Outbound fetch timeout in seconds. |
| `CELLO_SQL_STATEMENT_MS` | unset (unbounded) | The per-statement SQL time bound in milliseconds. A timed-out statement fails with a catchable timeout error and rolls back. |
| `CELLO_V8_HEAP_LIMIT_MB` | unset | Per-isolate V8 heap limit in MiB. |
| `CELLO_CACHE_SCOPE` | `script` | Which cache entries a Worker sees: `script` isolates each script's entries, `deployment` shares one `caches.default` and one set of `caches.open(name)` namespaces across every script of the serving deployment. See [security](../guides/security.md). |
| `CELLO_EGRESS` | `open` | `public` refuses Worker dials to non-public addresses. See [security](../guides/security.md). |
| `CELLO_EGRESS_ALLOW` | unset | Exceptions as comma-separated CIDRs or `host:port` pairs. |

## Placement and memory

| Variable | Default | Effect |
| --- | --- | --- |
| `CELLO_ACTIVATIONS` | 8 per CPU, at least 16, at most 128 | Concurrent cold-cell activations. |
| `CELLO_MAX_RESIDENT_CELLS` | unset | The hard limit for resident cells, enforced at admission. |
| `CELLO_IDLE_EVICT_S` | unset | Seconds after which an idle cell leaves memory and hibernates. Unset: only pressure or the residency cap removes an idle cell. |
| `CELLO_PLACEMENT_WEIGHT` | the CPU count | The ownership share of this node, relative to the other nodes. |
| `CELLO_REBALANCE_INTERVAL_MS` | `5000` | The fleet sample interval and maximum age. `0` disables balancing. |
| `CELLO_MAX_RSS_MB` | 80% of the available memory | The memory threshold for pressure shedding, on the greater of the allocator-adjusted RSS and the active cgroup working set. `0` disables the threshold and the absolute cap. |
| `CELLO_MEMORY_SCOPE` | `container` | Which cgroup the pressure sample and limit read: `container` measures the cgroupfs root, `self` the process's own cgroup v2 leaf. With `self` and an unset `CELLO_MAX_RSS_MB`, the threshold defaults to 80% of the leaf's `memory.max`. |
| `CELLO_C_TRIM_MB` | `8` | Freed C-heap growth, in MiB, that earns another `malloc_trim` on the sampler tick. |
| `CELLO_SQLITE_CACHE_KB` | `2000` | The SQLite page-cache budget per resident cell connection, in KiB. |
| `CELLO_ASSET_CACHE_DIR` | unset | Downloaded static-asset cache directory. |
| `CELLO_ASSET_CACHE_BYTES` | compiled-in default | Asset cache limit in bytes. |
| `CELLO_LOCAL_CACHE_MAX_BYTES` | `2 GiB` | Hibernated SQLite cache limit. `0` disables it. |
| `CELLO_PRESSURE_OWNERSHIP` | `release` | `release` hands a pressure-evicted cell to a peer; `sticky` keeps the ownership record so the cell wakes locally. |
| `CELLO_ALARM_RESIDENT_MS` | `3600000` | Near-alarm residency window in milliseconds. |
| `CELLO_WAKER_TICK_MS` | `60000` | Orphan-alarm scan interval in milliseconds. |
| `CELLO_TOKIO_THREADS` | the CPU count | Host Tokio worker threads. |

## Durability

| Variable | Default | Effect |
| --- | --- | --- |
| `CELLO_DURABILITY` | `fleet` | `fleet`: each write waits for one or two peer nodes to hold it on disk (a single node waits for the bucket instead). `bucket`: always wait for the bucket. |
| `CELLO_BUCKET_BUNDLES` | `0` | In bucket posture, `1` groups each flush round's dirty rows into shared bundle proofs. Any other value stops startup. |
| `CELLO_BUCKET_BUNDLE_LINGER_MS` | `0` | Hold a bundle round open this many milliseconds after the first dirty wake so concurrent writes share one PUT. Values above `1000` clamp to `1000`. |
| `CELLO_LOG_PIPELINE` | `4` | Fleet log rounds in flight. |
| `CELLO_LOG_HEDGE_MS` | adaptive (4x the slowest recent append, at least 250 ms) | The wait before a second copy of a slow log append. `0` disables the copy. |
| `CELLO_LTX_TRUNCATE_PAGES` | `128` (512 KiB) | The WAL size, in pages, at which Cello truncates the WAL at the next checkpoint. A database larger than 4 MiB waits until the WAL is larger than the database. `0` disables it. |
| `CELLO_LTX_RETENTION_SECS` | unset (no deletion) | A positive value enables epoch GC: the owner deletes prefixes below the restore base, keeping its own epoch, the one before it, and each epoch younger than this many seconds. |
| `CELLO_LTX_COMPACTION` | `1` | Creates additive L1 objects so a takeover reads tens of objects instead of thousands. Set `0` on every node of a mixed fleet until all nodes read v0.5.2 block objects. |
| `CELLO_LTX_COMPACTION_MIN_TXIDS` | `256` | The TXID distance that queues an L1 compaction. |
| `CELLO_LTX_COMPACTION_MIN_MB` | `32` (at most 64) | The L0 MiB that queue an L1 compaction. |
| `CELLO_LTX_COMPACTIONS` | `2` | Concurrent L1 compactions per node. |
| `CELLO_LTX_PAGED` | `1` | `1`: a takeover of a chain of at least `CELLO_LTX_PAGED_MIN_MB` reads pages from the bucket on first use. Set `0` to download every chain, and on every node of a mixed fleet with a node before v0.4.1. |
| `CELLO_LTX_PAGED_MIN_MB` | `256` | The chain size, in MiB, from which a restore pages. `0` pages every chain. |
| `CELLO_LTX_HYDRATE_MBPS` | `16` | The background fill rate of a paged cell, in MiB per second. `0` keeps the cell sparse. |
| `CELLO_LTX_DURABILITY_TIMEOUT_SECS` | `10` | The budget for one durability proof, and for the final snapshot of a handoff. A queued write waits at most six budgets. |
| `CELLO_TTL_MS` | `10000` | Node lease lifetime in milliseconds; the lease renews at one third of it. |
| `CELLO_RECOVERY_RETRY_MS` | `1000` | Dead-owner recovery retry backoff in milliseconds. |
| `CELLO_RECOVERY_RETRIES` | `240` | Recovery retries before a request fails. |
| `CELLO_SHUTDOWN_TOTAL_MS` | `40000` | The complete stop bound. The drain-token wait is 3/4 of it and the handoff no-progress interval is 5/8. |
| `CELLO_READY_FLEET_GATE_MS` | `120000` | The first-readiness gate deadline; `0` disables the gate. |
| `CELLO_MAX_ASSET_FILE_BYTES` | `25 MiB` | Per-file static asset limit. |
| `CELLO_MAX_DYNAMIC_WORKER_CODE_BYTES` | `64 MiB` | Loaded dynamic module total. |

An L1 compaction merges at most 256 source objects within a 64 MiB buffer. A larger source object spills to temporary files in the cell's local LTX directory, so the node needs free disk for the source and the output.

## Removed settings

Remove these from the environment, including an empty value or the former default. Cello rejects `CELLO_SHUTDOWN_DRAIN_MS` and `CELLO_DRAIN_TOKEN_WAIT_MS` at startup; the rest are ignored or replaced:

| Removed setting | Current behavior |
| --- | --- |
| `CELLO_OUTPUT_GATE` | Cello always waits for the durability proof before it acknowledges a write. |
| `CELLO_SHUTDOWN_DRAIN_MS`, `CELLO_DRAIN_TOKEN_WAIT_MS` | Rejected at startup. Use `CELLO_SHUTDOWN_TOTAL_MS`. |
| `CELLO_OTEL_SINK` | Set `CELLO_OTEL=1` for the fleet bucket, or `CELLO_OTEL` to the collector base URL. |
| `CELLO_AI_BINDING`, `CELLO_AI_URL` | The AI adapter is removed. Call the provider from application code. |
| `CELLO_CLOUD_RESTART_ON_DEPLOY` | A managed deployment adopts the new code in place. |
| `CELLO_STORAGE_PROBE` | A node always checks the storage contract. |
| `CELLO_EVICTIONS` | A node runs at most four concurrent evictions. |
| `CELLO_LOG_CAPTURE_WORKERS` | A node uses at most eight log capture workers. |
| `CELLO_REBALANCE_BATCH_CELLS` | A balancing batch moves at most 32 idle cells. |
| `CELLO_PRESENCE_SHADOW` | Use `cello diagnose --read-only` to check leases. |
| `CELLO_LOG_BUNDLE` | Fleet durability always bundles uploads. |
| `CELLO_QUEUE_PRODUCER_GROUP_MS` | A Queue owner groups producer calls with a 4 ms timer. |
| `CELLO_LOG_GROUP_COMMIT_MS` | A node waits 1 ms before a fleet log capture when Queue writes are pending. |
| `CELLO_PACED_HANDOFF` | A node always attempts the handoff within `CELLO_SHUTDOWN_TOTAL_MS`. |
