---
title: Develop locally
description: Run your application on cello dev with persistent local state, then operate its D1, KV, and R2 data.
---

You develop with `cello dev`. It starts a local object store, deploys the application, and runs one node. It needs no Docker and no cloud bucket. The Worker listener is `http://127.0.0.1:9876` by default.

```sh
cello dev [PROJECT_DIR_OR_CONFIG]
```

| Flag | Effect |
| --- | --- |
| `--port PORT` | The Worker listener port |
| `--host ADDR` | The Worker listener interface. The operator listener stays on loopback |
| `--logs` | Show the node warning and information logs |
| `--clean` | Delete `.cello/dev` before the start |
| `--state DIR` | Keep local state in DIR instead of `PROJECT/.cello/dev` (`CELLO_DEV_STATE` sets the same) |
| `--watch-ignore GLOB` | Ignore one more project-relative glob. Repeatable. Not valid with `--no-watch` |
| `--no-watch` | Disable automatic builds and restarts |

Set `NO_COLOR` to disable color, or `FORCE_COLOR` to enable it when the output is not a terminal. `NO_COLOR` takes priority.

## Supply Worker variables

`cello dev` reads `.dev.vars` beside the Wrangler configuration, as `wrangler dev` does. Without `.dev.vars`, it reads `.env` and then `.env.local`, which overrides `.env`. Each `NAME=value` entry becomes a Worker variable and overrides `vars`. A line can start with `export`, and a quoted value can span lines, for example a PEM key. Comments after a value and variable references are not supported. In `.env` files, an entry that is not a valid binding name or that collides with another binding is skipped with a warning; in `.dev.vars` such an entry stops the build. `cello deploy` does not read these files. Add `.dev.vars`, `.env`, `.env.local`, and `.cello/` to `.gitignore`.

## Keep or reset local state

The command keeps local state in `PROJECT/.cello/dev` (`--state DIR` or `CELLO_DEV_STATE` moves it elsewhere), across restarts and configuration changes. Cello does not migrate stored state, so a stored value from an earlier configuration can make the application fail with an error that looks unrelated. Use `--clean` to start from an empty state. `--clean` deletes nothing else under `.cello`, and over an explicit `--state` directory it deletes only what Cello created: a missing directory is already clean, and a foreign one is refused with its contents intact.

The command watches the project directory and rebuilds on a source, configuration, or dotenv change. The current application serves during the build, and a failed build does not replace it. The watcher ignores `.cello`, `.git`, `.wrangler`, `node_modules`, and `target` at each depth, and it does not watch files outside the project.

The local object store is not available to a fleet node or an operator subcommand; they need a supported cloud bucket.

## Operate D1, KV, and R2

These commands use the fleet bucket. `cello d1` and `cello kv` reach the database cell through a node.

```sh
cello d1 migrations apply ledger --bucket "$CELLO_BUCKET"
cello kv bulk put sessions wrangler-export.json --bucket "$CELLO_BUCKET"
```

The `migrations_dir` value must be a relative path inside the project, without a `..` component. The KV bulk commands use the Wrangler file format. `cello kv bulk get` streams; a named output file changes only after the export completes, and a failed stdout export leaves an incomplete JSON array.

`cello r2` reads and writes the objects of an `r2_buckets` binding under the reserved `r2/<bucket_name>/` prefix. It needs no running node, so a release pipeline can publish an artifact before it deploys.

```sh
cello r2 put assets app.zip --path dist/app.zip \
  --content-type application/zip \
  --metadata '{"release":"1.2.3"}' \
  --bucket "$CELLO_BUCKET"
```

The first argument is the `bucket_name`, not the binding name. `--metadata` becomes `customMetadata`, and the content flags become `httpMetadata`. An object that another tool writes into the prefix is also readable, without `cacheExpiry` or checksums. `cello r2 get` streams the body to stdout, and `cello r2 head` prints the stored record; with `--json`, the `http` and `custom` fields hold `httpMetadata` and `customMetadata`.

`cello kv list` and `cello r2 list` print at most 1000 keys and, on stderr, the flag that continues the listing: `--after KEY` for kv, `--cursor CURSOR` for r2. Pass `--all` for every key. `cello kv list --json` prints one JSON object per key. `cello r2 list` takes `--prefix` and `--delimiter`; with `--page` it prints one page object — objects with `httpMetadata` and `customMetadata`, plus `delimitedPrefixes`, `truncated` and `cursor` — a Worker's `list()` page without `version`, `checksums` and `storageClass`.

Append `--local` to a `cello d1` command to target the `cello dev` node instead of the fleet. It resolves the same state directory, which carries that node's leases and secret, so pass the same `--state DIR` (or `CELLO_DEV_STATE`) when the node runs with an explicit state directory. See [CLI reference](../reference/cli.md) for every subcommand and flag.

Every Cello command writes data to stdout and messages to stderr. A closed stdout pipe is a successful stop.
