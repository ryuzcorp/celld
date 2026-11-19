---
title: CLI
description: New cello commands and flags — deploy, invoke, d1, r2, versions, dev state — over upstream denoland/celld.
---

New commands, flags, and output shapes in the `cello` CLI. Each entry
names its fork patch number. Run `cello --help` and `cello <cmd> --help`
for the complete surface.

## Deploy and release

- `0012` — `cello deploy --sourcemap PATH` writes the bundled Worker's
  external source map to a local file: the esbuild run adds
  `--sourcemap=external`, so the deployed module bytes are byte-identical
  with or without the flag, and the map's `sources` are rebased to
  project-root-relative paths. Only the bundled JS entry is mapped;
  asset-only and Python projects warn and write nothing.
- `0047` — `--sourcemap` collects the `no_bundle` entry's sibling
  `<entry>.map`: the entry deploys as written, and the sibling (which
  must be a regular non-symlink file under 64 MiB of v3 JSON inside the
  project tree) never fails the deploy — anything else warns and writes
  nothing, with `sources` rebased lexically so the map cannot probe
  which host files exist.
- `0046` — `cello --version` reports the full build version
  (`0.1.0`, `<version>-ryuzcorp.<n>` on the old line): `build.rs` bakes
  `CELLO_VERSION` into the binary from the Docker build stage, and a
  plain `cargo build` without it reports the crate version.
- `0049` — `cello invoke --header-env NAME=VAR` sends header NAME with
  the value of environment variable VAR, so a secret header never sits
  on argv where every local user reads `/proc/<pid>/cmdline`. An unset
  variable is refused without echoing anything.
- `0120` — Container image tars upload multipart — 8 MiB parts, each on
  its own request — instead of one PUT under the 30 s single-try
  timeout, so GB-scale images stop failing deterministically
  ([upstream #307](https://github.com/denoland/celld/issues/307)).
- `0123` — `cello versions [SCRIPT]` lists deployed versions from one
  LIST of `deploy/<script>/<version>/manifest.json`, marking the
  serving generation current from the fleet-wide pointer — the
  top-level version, staged components, and, under a pointer without a
  components map, every named-pointer service version — with an
  optional `SCRIPT` narrowing the rows
  ([upstream #181](https://github.com/denoland/celld/issues/181)).

## D1, KV, and R2

- `0017` — `cello r2 list` takes `--delimiter` and, with `--page`,
  prints one page — decoded keys with `httpMetadata` and
  `customMetadata`, plus `delimitedPrefixes`, `truncated`, and
  `cursor` — through the same listing the R2 binding calls. A truncated
  listing continues with `--cursor` (`--after` can omit keys on
  non-UTF-8-safe orderings). `--json` keeps the one-record-per-line
  output.
- `0022` — `cello d1 export` writes a consistent SQL dump: the owner
  node reads schema beside data under one transaction, streams schema
  and rows against a 32 MiB budget, skips generated columns, carries
  NUL and invalid-UTF-8 TEXT byte-exact, and rebuilds fts5/virtual
  tables without their shadows. `--table` (repeatable), `--no-schema`,
  and `--no-data` narrow it. Export is operator-only: stub fetch to a
  D1 scope answers only the query/batch/exec branches behind a
  host-set operator-ingress marker.
- `0023` — `cello d1 execute --batch --json` reads a JSON array of
  statements from stdin and runs it as one transactional batch — the
  same implementation the Worker's `batch()` uses — printing one JSON
  array shaped as the Worker's `batch()` shapes it.
- `0024` — `cello d1 export` scopes each `sqlite_sequence` reset to its
  own counter, so a filtered or data-only dump replayed into an
  existing database no longer clears the AUTOINCREMENT counters of
  tables the dump does not name.
- `0044` — `cello r2 head` and `cello r2 get` with `--json` print
  `{"error":"not_found","bucket":…,"key":…}` on stdout for an absent
  key, so callers match the `error` field instead of substring-matching
  the English message. Human message on stderr and exit code unchanged.
- `0121` — `cello d1 execute`, `export`, and migrations take `--local`
  to run against the running `cello dev` instead of the fleet: the dev
  node's local store carries its leases and secret like a fleet bucket.
  `--local` resolves the project through the canonical config and opens
  `PROJECT/.cello/dev` (following the `--state` / `CELLO_DEV_STATE`
  override). Combining it with `--bucket` / `--endpoint` (or
  `CELLO_BUCKET`) is refused, so migrations can never run against the
  wrong database by a silent bucket choice. `d1` also takes
  `--state DIR`; `--state` without `--local` is refused
  ([upstream #272](https://github.com/denoland/celld/issues/272)).

## Local development

- `0020` — `cello dev --port 0` assigns an ephemeral port: the node
  binds it and the supervisor learns the address from the node's own
  announce line, so parallel boots never race on a probed-free port.
- `0124` — `cello dev --state DIR` (or `CELLO_DEV_STATE`, default
  `PROJECT/.cello/dev`) keeps local state outside the project, where
  backups and search tools pick it up. `--clean` over an explicit
  directory deletes only when the marker proves Cello created it, and
  `--state` refuses to adopt a non-empty foreign dir
  ([upstream #302](https://github.com/denoland/celld/issues/302)).
