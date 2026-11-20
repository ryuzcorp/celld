---
title: CLI reference
description: Every cello subcommand and flag, generated from the 0.2.1 release binary help tree.
---

You run one binary for everything: `cello` starts a node with no subcommand, and every other task is a subcommand. Each listing below comes from the 0.2.1 `--help` tree, and each `--help` screen was run against the release binary. Fleet subcommands take `--bucket` (or `CELLO_BUCKET`), `--endpoint` (or `S3_ENDPOINT`), and `--region` (or `AWS_REGION`, default `us-east-1`). Every command writes data to stdout and messages to stderr.

## Start a node

```sh
cello --bucket s3://YOUR-BUCKET [OPTIONS]
```

| Flag | Effect |
| --- | --- |
| `--bucket s3://\|gs://\|az://NAME[/PREFIX]` | Fleet bucket and prefix. `gs://` and `az://` reject `--endpoint` and ignore `--region`. |
| `--endpoint URL` | S3-compatible endpoint. |
| `--region REGION` | Storage region. |
| `--listen IP:PORT` | Public Worker listener (default `127.0.0.1:8080`; a non-loopback address requires `--internal-listen`). |
| `--internal-listen IP:PORT` | Peer and operator listener (default `127.0.0.1:0`). |
| `--advertise ADDR:PORT` | Peer-reachable `IP:PORT` or `HOST:PORT` (requires `--internal-listen`). |
| `--peer NODE_ID` | Diagnose one node with a signed direct probe. Repeatable. |
| `--json` | Print one JSON object per diagnostic check. |
| `--read-only` | Skip the bucket write probe. |
| `--unsafe-public-advertise` | Permit a literal public IP in `--advertise`. |
| `--trust-forwarded-headers` | Let `X-Forwarded-Host` and `X-Forwarded-Proto` set the scheme and host in `request.url`. |
| `-h, --help` | Show the help. |
| `-V, --version` | Show the version. |

`cello diagnose --bucket s3://YOUR-BUCKET [--peer NODE_ID]... [--json] [--read-only]` reads the node leases and probes each live peer. It takes no lease and changes no ownership. Without `--read-only` it also tests the conditional write, which writes and deletes a small object under the `probe/` prefix.

## Deploy and release

```sh
cello deploy [PROJECT] --bucket NAME [--dry-run] [--no-activate] [--sourcemap PATH] [--read-root DIR]... [--config PATH] [--json]
cello versions [SCRIPT] --bucket NAME [--json]
cello activate SCRIPT=VERSION... --bucket NAME [--primary SCRIPT] [--json]
cello discard SCRIPT=VERSION... --bucket NAME [--json]
cello invoke SCRIPT=VERSION... --path PATH --bucket NAME [--target SCRIPT] [--method METHOD] [--header N:V]... [--header-env N=VAR]... [--body TEXT | --body-file PATH] [--json] [--unsafe-public-advertise]
```

`PROJECT` is a directory or a Wrangler config, defaulting to the working directory. `--dry-run` bundles and prints the version without writing. `--no-activate` uploads and validates without moving any pointer. `--sourcemap PATH` writes the Worker's source map to a file; the deployed bytes are identical with or without it. `--read-root DIR` (repeatable) names a directory beyond the project that build inputs may come from; only the operator sets it, on the command line.

`versions` lists the immutable deployments in the bucket, marking the serving generation `current`; an optional `SCRIPT` narrows the rows. `activate` replaces the fleet-wide pointer in one write, so nodes adopt the whole generation at once. Never run `activate` concurrently with another `activate` or `deploy`. `discard` deletes staged versions no pointer names; shared content-addressed objects stay.

`invoke` sends one HTTP request to a staged version on one node, served by that version's code with its declared bindings against the real data. `--body-file -` reads the body from stdin. `--header-env NAME=VAR` sends header `NAME` with the value of environment variable `VAR`, so secrets stay out of `/proc/<pid>/cmdline`. Stdout carries the staged answer body, or the result envelope with `--json`; a non-2xx answer still prints its body, then exits nonzero. See [stage versions](../guides/staged-versions.md).

## Develop locally

```sh
cello dev [PROJECT] [--host IP] [--port PORT] [--logs] [--no-watch] [--watch-ignore GLOB]... [--clean] [--state DIR]
```

`--port` defaults to `9876` (`0` assigns an ephemeral port); `--host` defaults to `127.0.0.1`. `--logs` shows node warning and information logs. `--no-watch` disables rebuilds; `--watch-ignore GLOB` (repeatable) ignores one more project-relative glob. `--clean` deletes the local state before the start. `--state DIR` (or `CELLO_DEV_STATE`) keeps local state in `DIR` instead of `PROJECT/.cello/dev`. See [develop locally](../guides/local-dev.md).

## Cells

```sh
cello cell list [CLASS] --bucket NAME [--limit N] [--after CURSOR] [--all] [--json]
cello cell gc --dry-run [CLASS] --bucket NAME [--grace-secs N] [--limit N] [--after CURSOR] [--all] [--json]
```

`cell list` prints one `Class:ID` scope per line (`--json` prints one JSON object per line, marking reserved classes `"reserved": true`). An instance appears after its first event. The listing stops at 1000 instances (`--limit N` changes the page size, `--all` reads everything) and prints the continuation on stderr: pass `--after SCOPE` for the next page.

`cell gc --dry-run` reports, for each cell, the epoch prefixes that epoch GC can delete, their bytes, and the restore base. It writes nothing and exits nonzero at the end if it could not read a cell. `--grace-secs` defaults to `3600`.

## D1

```sh
cello d1 execute DATABASE (--command SQL | --file PATH | --batch) [PROJECT] --bucket NAME [--json]
cello d1 export DATABASE [--output PATH] [--table NAME]... [--no-schema] [--no-data] [PROJECT] --bucket NAME
cello d1 migrations apply DATABASE [PROJECT] --bucket NAME
cello d1 migrations list DATABASE [PROJECT] --bucket NAME
```

`DATABASE` is a `database_name` the project declares in `d1_databases`. `execute` prints a table; `--json` prints one JSON object per row. `--batch` reads a JSON array of `{"sql", "params"}` from stdin and runs it as one transactional batch (requires `--json`). `export` writes the database as SQL read under one transaction; `--table` (repeatable) exports only those tables with their indexes and triggers. Append `--local` to target the `cello dev` node instead of the fleet; `--local` takes no `--bucket`, and takes `--state DIR` when the dev node runs with an explicit state directory.

## KV

```sh
cello kv get <namespace-id> <key> [fleet options]
cello kv put <namespace-id> <key> <value> [--path FILE] [--ttl SECONDS] [--expiration SECONDS] [--metadata JSON] [fleet options]
cello kv delete <namespace-id> <key>... [fleet options]
cello kv list <namespace-id> [--prefix P] [--limit N] [--after CURSOR] [--all] [--json]
cello kv info <namespace-id> [fleet options]
cello kv bulk get <namespace-id> [FILE] [fleet options]
cello kv bulk put <namespace-id> FILE [fleet options]
cello kv bulk delete <namespace-id> FILE [fleet options]
```

The namespace id is the `id` from the project's `kv_namespaces` entry, verbatim. `get` writes the value to stdout as bytes. `put --ttl` expires seconds from now (at least 60); `--expiration` expires at an absolute unix time; `--metadata` stores at most 1024 bytes. `list` defaults to 1000 rows with `--limit`, `--after`, `--all`, and `--json` (one JSON object per line). `info` reports live keys, their bytes, and stored rows. Bulk files use Wrangler's format, so an export here imports there too.

## Queues

```sh
cello queue info QUEUE [fleet options]
cello queue peek QUEUE [--limit N] [fleet options]
cello queue purge QUEUE --force [fleet options]
cello queue pause QUEUE [fleet options]
cello queue resume QUEUE [fleet options]
cello queue redrive QUEUE [--limit N] [fleet options]
```

`peek` reads up to 10 head messages without consuming them; `redrive` returns up to 100 dead-lettered messages to their recorded sources. Both accept a limit from 1 through 100. `purge` deletes visible and delayed messages and marks live leases.

## R2

```sh
cello r2 get <bucket-name> <key> [--json] [fleet options]
cello r2 head <bucket-name> <key> [--json] [fleet options]
cello r2 put <bucket-name> <key> --path FILE | --pipe [--metadata JSON] [--content-type VALUE] [--content-language VALUE] [--content-disposition VALUE] [--content-encoding VALUE] [--cache-control VALUE] [--cache-expiry MS] [fleet options]
cello r2 delete <bucket-name> <key>... [fleet options]
cello r2 list <bucket-name> [--prefix P] [--delimiter D] [--cursor C] [--page] [--limit N] [--after CURSOR] [--all] [--json]
```

The bucket name is the `bucket_name` from the project's `r2_buckets` entry, not the binding name. `r2` reads the fleet bucket directly and needs no running node. `get` writes the object to stdout as bytes. A `get` or `head` of an absent key fails with `no object ...`; with `--json` it also prints `{"error":"not_found",...}` first. `delete` of an absent key succeeds. With `--page`, `list` prints one binding-shaped page object; continue a truncated listing with `--cursor`.
