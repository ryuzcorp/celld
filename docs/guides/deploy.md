---
title: Deploy an application
description: Deploy a Wrangler project to the fleet bucket, adopt releases, and invoke staged versions safely.
---

You deploy from a Wrangler project directory. Install `esbuild` on `PATH` first if the project contains Worker code; asset-only projects do not need it.

## Deploy the project

```sh
cello deploy . \
  --bucket "$CELLO_BUCKET" \
  --endpoint "$S3_ENDPOINT" \
  --region "$AWS_REGION"
```

`cello deploy` accepts module Workers, Durable Object bindings, service bindings, variables, cron triggers, D1 databases, KV namespaces, queues, R2 buckets, workflows, WebAssembly modules, and static assets. An unknown Wrangler configuration key stops the deploy. See [Cloudflare compatibility](../reference/cloudflare-compat.md) for the full boundary. A node verifies the SHA-256 digest of each module before it builds a deployment.

On Linux, esbuild runs under Landlock with reads confined to the project directory and its read roots. An import, input source map, or `tsconfig.json` `extends` outside them fails like a missing file. It must be the self-contained release binary, not a wrapper script. Pass `--read-root DIR` (repeatable) to name further directories that esbuild inputs and `containers[].image` Dockerfiles with their build contexts may come from, such as a monorepo root whose `node_modules` sit above the config. Only the operator sets it, on the command line. A Dockerfile builds with its own directory as the context, so one placed directly at a read root sends that whole root to the builder. A kernel without Landlock runs esbuild unconfined, with a warning. Bundled files are still checked against the project and its read roots; only an outside input source map, tsconfig, or failing file can reach the source map or the error output.

Pass `--sourcemap PATH` to write the Worker's source map to a local file for stack-trace symbolication: the esbuild bundle's external map, or the `no_bundle` entry's sibling `<entry>.map`. The deployed bytes are identical with or without it. Asset-only and Python projects, and `no_bundle` entries without a sibling map, warn and write nothing. A sibling map that is a symlink, not a regular file, larger than 64 MiB, outside the project root, or not a v3 source map is skipped with a warning.

## Adopt the release

A running node does not restart for a new deployment. Each node reads `deploy/current.json` every `CELLO_DEPLOY_POLL_S` seconds (default 30) and adopts a new deployment in place. `POST /reload` on the internal listener adopts it now, and also rebuilds an unchanged deployment. A request that started on the previous deployment finishes on it. A deployment that does not build leaves the current one serving, and the node reports the error in its log and in the `/reload` response.

An inactive Durable Object runs the new deployment at its next activation. A resident one moves when no request, alarm handler, or regular WebSocket is active, and it keeps its storage and its hibernatable WebSockets. A request that arrives during the move waits for the new code. An outbound call from a request that already runs in the object does not wait, so that request can finish on the old code. After `CELLO_DEPLOY_MAX_AGE_S` seconds (default 60; 0 forces at once), Cello forces the move: it cancels the running work and closes regular WebSockets with code 1012, as Cloudflare does. During the move, a request on one deployment can call a Durable Object on the other, so two adjacent versions must accept each other's calls. `/state` reports the deployment of each resident object.

## Stage and activate versions

`cello deploy --no-activate` uploads a version that serves no traffic until `cello activate` names it. Use it to run a migration endpoint before the activation that would serve it:

```sh
cello deploy . --bucket "$CELLO_BUCKET" --no-activate  # prints VERSION
cello invoke app=VERSION --path /migrate --method POST --bucket "$CELLO_BUCKET"
cello activate app=VERSION --bucket "$CELLO_BUCKET"
```

`cello versions [SCRIPT]` lists the deployed versions, marking the serving generation `current`. `cello discard SCRIPT=VERSION` deletes a staged version no pointer names. See [stage versions](staged-versions.md) for the invoke contract and [CLI reference](../reference/cli.md) for every flag.
