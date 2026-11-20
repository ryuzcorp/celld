---
title: Stage versions
description: Upload a version without serving it, invoke it once against real data, then activate it.
---

<!-- Modified by Ryuz Corp (Cello): staged invoke page; invoke --header-env. -->

A version uploaded with `cello deploy --no-activate` serves no traffic until `cello activate` names it. That leaves a gap for schema migrations: the migration endpoint exists only in the staged code, so nothing can call it before the activation — and calling it after means the new code serves the old schema in between. `cello invoke` closes the gap: it sends one HTTP request to a staged version on one node of the fleet, served by that version's code with its declared bindings against the real data. Migrate first, then activate.

```sh
cello deploy . --bucket "$CELLO_BUCKET" --no-activate  # prints VERSION
cello invoke app=VERSION --path /migrate --method POST --bucket "$CELLO_BUCKET"
cello activate app=VERSION --bucket "$CELLO_BUCKET"
```

The invoke moves no pointer and changes nothing serving: the staged code answers this request and this request only. The public listener keeps serving the live version throughout.

## Name the staged generation

`SCRIPT=VERSION` pins name the staged generation: the receiver plus every cohosted script the request must see staged. Scripts the pins omit resolve from the live generation, exactly as an activation of the same pins would carry them — from the activated component map when there is one, from the named pointers otherwise. With one pin it receives the request; with more, `--target` names the receiver, and it must be a pinned script.

Each pin must be staged in the fleet bucket. The node reads the version's manifest, checks it names the pinned script and version, and verifies the module digests — the same checks activation runs. A well-formed but unknown version is a 404; a malformed name or version, or more than 64 pins, is a 400 before any bucket read. Pins no loaded script depends on are ignored with a warning in the node log, after the node has checked they exist: they cannot affect execution, because a service call resolves only a binding the calling script declares.

If the fleet pointer moves while the invoke loads its graph — a concurrent activation — the invoke runs nothing and answers 409, so a retry loads one consistent generation.

## Know what the staged code sees

The staged scripts run in fresh isolate pools built for the invoke and retired after it. They never share an isolate, turn, promise, socket, or budget with live traffic. The staged generation is never adopted, never becomes current, drives no queues, arms no crons, and installs no container specs.

Bindings reach the real data. Cells always serve from the live generation, so D1, KV, Queue, and Durable Object bindings read and write the same cells the live version uses. Service bindings resolve from the staged generation's components: a staged app that calls its helper reaches the staged helper when pinned, the carried live helper otherwise. `request.url` carries the invoked path on the target script as its host. The Workers Cache API is the one exception: under `CELLO_CACHE_SCOPE=deployment` each invoke's staged generation has a scope of its own, keyed by its reserved generation id, so staged code never reads or writes the live deployment's cached entries — even when the invoke names the live versions. The scope drops when the staged generation retires.

The handler budget bounds the response, and the invoke answers only after `waitUntil` settles, so work the handler defers — including service calls it makes after answering — still resolves against the staged graph. The settle wait is bounded by a further handler budget past the reply: background work that never settles fails the invoke instead of pinning the staged generation, so make migration endpoints answer only done work. Upgrades are refused and streamed answers are drained up to 64 MiB. The request body is capped by `CELLO_MAX_REQUEST_BODY_BYTES`. At most four invokes run on one node at once; past that the route sheds with 503 and the CLI tries the next node.

The invoke runs to completion on the node even if you stop waiting: a client disconnect or CLI timeout cannot retire the staged generation under a running driver. A timeout therefore means the answer is unknown, never that the request failed; only node shutdown — or the settle deadline above — cancels it. `cello invoke` waits up to ten minutes. Delivery is at-least-once across retries, so make migration endpoints idempotent.

## Authenticate the invoke

Only the operator can invoke. `POST /invoke` on the internal listener requires the per-request fleet signature — the same HMAC the `/runtime/` operator routes require, bound to method, path, body, timestamp, nonce, and target node — and, when `CELLO_OPERATOR_TOKEN` is set, the operator token. A tenant Worker has neither, and the public listener has no such route. Peer tunnels carry cell calls, never this path, so there is nothing to smuggle a version through: a non-activated version is reachable only through this gate. See [security](security.md).

## Reference

```sh
cello invoke SCRIPT=VERSION [SCRIPT=VERSION...] --path PATH --bucket NAME
  [--target SCRIPT] [--method METHOD] [--header NAME:VALUE]...
  [--header-env NAME=VAR]...
  [--body TEXT | --body-file PATH] [--json] [--unsafe-public-advertise]
```

Stdout carries the staged version's answer body, or the result envelope with `--json`; anything else goes to stderr. A non-2xx answer still prints its body, then exits nonzero. `--body-file -` reads the request body from stdin. `--header-env NAME=VAR` sends header NAME with the value of environment variable VAR: pass secret headers that way, since argv is readable by every local user through `/proc/<pid>/cmdline` and the environment only by the same user.

The route answers `{"result": {"status", "headers", "body"}}`, the body base64, or `{"error": ...}` with 400 (malformed), 404 (unknown version), 409 (the pointer moved), 413 (body over the ingress limit), or 500 (the load, build, dispatch, or answer failed). A draining node answers 503 and the CLI moves to the next live node.
