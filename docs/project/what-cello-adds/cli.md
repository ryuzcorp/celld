---
title: CLI
description: New cello commands and flags — deploy, invoke, d1, r2, versions, dev state — over upstream denoland/celld.
---

These patches change what the `cello` CLI can do. For the full surface, run `cello --help` and `cello <cmd> --help`, or read the [CLI reference](../../reference/cli.md). Each entry names its fork patch number.

## Ship code and provenance

Deploy the way the [deploy guide](../../guides/deploy.md) shows, with these additions:

- `0012` — `cello deploy --sourcemap PATH` writes the bundled Worker's external source map to a local file. The esbuild run adds `--sourcemap=external`, so the deployed module bytes are byte-identical with or without the flag, and the map's `sources` are rebased to project-root-relative paths. Only the bundled JS entry is mapped; asset-only and Python projects warn and write nothing.
- `0047` — `--sourcemap` also collects the `no_bundle` entry's sibling `<entry>.map`: the entry deploys as written, and the sibling (a regular non-symlink file under 64 MiB of v3 JSON inside the project tree) never fails the deploy — anything else warns and writes nothing, with `sources` rebased lexically so the map cannot probe which host files exist.
- `0046` — `cello --version` reports the full build version (`0.2.0`, or `<version>-ryuzcorp.<n>` on the old line): `build.rs` bakes `CELLO_VERSION` into the binary from the Docker build stage, and a plain `cargo build` without it reports the crate version.
- `0049` — `cello invoke --header-env NAME=VAR` sends header NAME with the value of environment variable VAR, so a secret header never sits on argv where every local user reads `/proc/<pid>/cmdline`. An unset variable is refused without echoing anything.
- `0120` — Container image tars upload multipart — 8 MiB parts, each on its own request — instead of one PUT under the 30 s single-try timeout, so GB-scale images stop failing deterministically ([upstream #307](https://github.com/denoland/celld/issues/307)).
- `0123` — `cello versions [SCRIPT]` lists deployed versions from one LIST of `deploy/<script>/<version>/manifest.json`, marking the serving generation current from the fleet-wide pointer — the top-level version, staged components, and, under a pointer without a components map, every named-pointer service version — with an optional `SCRIPT` narrowing the rows ([upstream #181](https://github.com/denoland/celld/issues/181)). See [staged versions](../../guides/staged-versions.md).

## Inspect D1, KV, and R2

- `0017` — `cello r2 list` takes `--delimiter` and, with `--page`, prints one page — decoded keys with `httpMetadata` and `customMetadata`, plus `delimitedPrefixes`, `truncated`, and `cursor` — through the same listing the R2 binding calls. A truncated listing continues with `--cursor` (`--after` can omit keys on non-UTF-8-safe orderings). `--json` keeps the one-record-per-line output. See [R2](../../services/r2.md).
- `0022` — `cello d1 export` writes a consistent SQL dump: the owner node reads schema beside data under one transaction, streams schema and rows against a 32 MiB budget, skips generated columns, carries NUL and invalid-UTF-8 TEXT byte-exact, and rebuilds fts5/virtual tables without their shadows. `--table` (repeatable), `--no-schema`, and `--no-data` narrow it. Export is operator-only: stub fetch to a D1 scope answers only the query/batch/exec branches behind a host-set operator-ingress marker. See [D1](../../services/d1.md).
- `0023` — `cello d1 execute --batch --json` reads a JSON array of statements from stdin and runs it as one transactional batch — the same implementation the Worker's `batch()` uses — printing one JSON array shaped as the Worker's `batch()` shapes it.
- `0024` — `cello d1 export` scopes each `sqlite_sequence` reset to its own counter, so a filtered or data-only dump replayed into an existing database no longer clears the AUTOINCREMENT counters of tables the dump does not name.
- `0044` — `cello r2 head` and `cello r2 get` with `--json` print `{"error":"not_found","bucket":…,"key":…}` on stdout for an absent key, so you match the `error` field instead of substring-matching the English message. Human message on stderr and exit code unchanged.
- `0121` — `cello d1 execute`, `export`, and migrations take `--local` to run against the running `cello dev` instead of the fleet ([upstream #272](https://github.com/denoland/celld/issues/272)). See [local development](../../guides/local-dev.md).

## Keep dev state out of your project

- `0020` — `cello dev --port 0` assigns an ephemeral port: the node binds it and the supervisor learns the address from the node's own announce line, so parallel boots never race on a probed-free port.
- `0124` — `cello dev --state DIR` (or `CELLO_DEV_STATE`, default `PROJECT/.cello/dev`) keeps local state outside the project, where backups and search tools pick it up ([upstream #302](https://github.com/denoland/celld/issues/302)). `--clean` over an explicit directory deletes only when the marker proves Cello created it, and `--state` refuses to adopt a non-empty foreign dir. See [local development](../../guides/local-dev.md).
