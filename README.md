# Cello

Self-hosted, distributed **Durable Objects**.

Cello runs a Cloudflare Workers application on your own machines:
Workers, Durable Objects, KV, Queues, D1, R2, Workflows, Cron Triggers,
and static assets, deployed from the `wrangler.json` you already have.
Each object is a cell: a named server with its own SQLite database.
Cello stores the long-term state in your S3-compatible, Google Cloud
Storage, or Azure Blob Storage bucket. It needs no serving control
plane or consensus service, and an inactive cell costs almost nothing.

📚 Docs: **https://cello.ryuz.eu** — [Install](https://cello.ryuz.eu/install) ·
[Operations](https://cello.ryuz.eu/operations) ·
[What Cello adds over upstream](https://cello.ryuz.eu/what-cello-adds) ·
[Changelog](https://cello.ryuz.eu/changelog)

## Quickstart

### 1. Install

```sh
curl -fsSL https://cello.ryuz.eu/install.sh | sh
```

Worker projects deployed with `cello deploy` need
[esbuild](https://esbuild.github.io) on `PATH`; asset-only projects do
not. Or run the image (Linux amd64/arm64):

```sh
docker run --rm ghcr.io/ryuzcorp/cello:latest --version
```

### 2. Run it locally

```sh
cello dev
```

One node with a local object store — no Docker, no cloud bucket. The
Worker answers at `http://127.0.0.1:9876`, state lives in `.cello/dev`,
and the project rebuilds on every change without losing durable data.

### 3. Deploy it

```sh
cello deploy . --bucket s3://my-cells-bucket

cello \
  --bucket s3://my-cells-bucket \
  --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 \
  --advertise 10.0.0.12:8081
```

Start a second node against the same bucket and each write finishes as
soon as the second node holds it on disk — much faster than the bucket
round trip. No extra configuration: a node finds the other nodes
through the bucket.

## Why Cello

- **Your Workers, unchanged.** Deploy the Wrangler project you already
  have. The runtime, and every binding Cloudflare builds on Workers
  and Durable Objects, runs on your fleet.
- **No control plane.** A node is one `cello` process per machine; the
  nodes that share one bucket are a fleet. A conditional bucket write
  gives a node ownership of a cell — no membership list, no leader
  election, no consensus service. Signed peer HTTP carries routing and
  replicated-log transport.
- **Durable by default.** Cello does not answer a write until the data
  survives a failure (RPO=0): through the bucket, or through one or two
  peer nodes first when the fleet has them.
- **Cheap at rest.** An inactive cell is only objects in your bucket —
  one 8 GB node holds 1,000 resident cells, about $0.05 each per month.
- **Hardened for other people's code.** Operator token, tenant egress
  policy, project-confined builds, split deployment credentials, and
  non-dumpable serving processes. See
  [Security](https://cello.ryuz.eu/security).
- **Observable.** Traces and logs to your collector or to Parquet in
  the bucket, DuckDB-queryable. See
  [Telemetry](https://cello.ryuz.eu/telemetry).

## What runs on Cello

| service | example |
| --- | --- |
| Workers: fetch handlers, service bindings, JS RPC, Node.js compat | [`hello`](examples/hello) |
| Durable Objects: SQLite storage, alarms, hibernating WebSockets | [`counter`](examples/counter) |
| KV: list, metadata, expiration, bulk import | [`kv`](examples/kv) |
| Queues: producers, batching consumers, retries, dead letters | [`queues`](examples/queues) |
| D1: SQL databases, batches, migrations | [`d1`](examples/d1) |
| R2: reads, writes, lists, multipart uploads | [`r2`](examples/r2) |
| Workflows: durable steps, sleeps, events, pause and restart | [`workflow`](examples/workflow) |
| Cron Triggers: one run for each occurrence across the fleet | [`cron`](examples/cron) |
| Static assets: asset-only or with a Worker, `_headers`, `_redirects` | [`static-assets`](examples/static-assets) |
| Dynamic Workers: runtime-loaded code and Tail Worker observability | [`dynamic-worker-tails`](examples/dynamic-worker-tails) |
| Containers (experimental): a Durable Object that supervises a container | [`container`](examples/container) |
| Sandboxes: the Cloudflare Sandbox SDK on a container per sandbox | [`sandbox`](examples/sandbox) |

The [Cloudflare compatibility](https://cello.ryuz.eu/cloudflare-compat)
page lists each gap in the services above.

## Operate a fleet

```sh
cello diagnose --bucket s3://my-cells-bucket
cello cell list --bucket s3://my-cells-bucket
cello d1 migrations apply ledger --bucket s3://my-cells-bucket
```

`diagnose` probes every node lease; `cell list` prints one `Class:ID`
cell scope per line; `d1`, `kv`, and `r2` run SQL and read and write
namespaces and objects through the fleet. The [Operations
guide](https://cello.ryuz.eu/operations) covers storage setup,
autoscaling, shutdown and rollout, upgrade notes, and every environment
variable.

A fleet runs one application, and every node loads its latest
deployment from `deploy/current.json`. Upgrades keep the bucket and the
node data directories: stop all old nodes, start the new binaries with
the same configuration. Mixed versions cannot share a serving fleet.

## Based on celld

Cello is a renamed continuation of [celld](https://github.com/denoland/celld)
by Deno Land Inc. — the [What Cello adds over
upstream](https://cello.ryuz.eu/what-cello-adds) page documents every
improvement. To upgrade, swap the binary or image on the same bucket:
stop all celld nodes first (a mixed fleet is unsupported), rename
`CELLD_*` variables to `CELLO_*`, and start Cello. Cello reads
celld-written data and writes new data with `cello` names; see
[Compatibility](https://cello.ryuz.eu/compatibility) for the details.

## License

[Apache-2.0](LICENSE); see [NOTICE](NOTICE).
