<!-- Modified by ryuzcorp (ryuzcorp/celld): staged invoke page. -->
# Invoke a staged version

A version uploaded with `celld deploy --no-activate` serves no traffic
until `celld activate` names it. That leaves a gap for schema migrations:
the migration endpoint exists only in the staged code, so nothing can
call it before the activation — and calling it after means the new code
serves the old schema in between. `celld invoke` closes the gap: it sends
one HTTP request to a staged version on one node of the fleet, served by
that version's code with its declared bindings against the real data.
Migrate first, then activate.

```sh
celld deploy . --bucket "$CELLD_BUCKET" --no-activate  # prints VERSION
celld invoke app=VERSION --path /migrate --method POST --bucket "$CELLD_BUCKET"
celld activate app=VERSION --bucket "$CELLD_BUCKET"
```

The invoke moves no pointer and changes nothing serving: the staged code
answers this request and this request only. The public listener keeps
serving the live version throughout.

## The staged generation

`SCRIPT=VERSION` pins name the staged generation: the receiver plus every
cohosted script the request must see staged. Scripts the pins omit resolve
from the live generation, exactly as an activation of the same pins would
carry them — from the activated component map when there is one, from the
named pointers otherwise. With one pin it receives the request; with more,
`--target` names the receiver, and it must be a pinned script.

Each pin must be staged in the fleet bucket: the node reads the version's
manifest, checks it names the pinned script and version, and verifies the
module digests, the same checks activation runs. A well-formed but unknown
version is a 404; a malformed name or version, or more than 64 pins, is
a 400 before any bucket read. Pins no loaded script depends on are ignored with a warning in the
node log, after the node has checked they exist: they cannot affect
execution, because a service call resolves only a binding the calling
script declares.

If the fleet pointer moves while the invoke loads its graph — a
concurrent activation — the invoke runs nothing and answers 409, so a
retry loads one consistent generation.

## What the staged code sees

The staged scripts run in fresh isolate pools built for the invoke and
retired after it. They never share an isolate, turn, promise, socket, or
budget with live traffic: pool sharing stops at the pool boundary, so
separate pools are separate by construction. The staged generation is
never adopted, never becomes current, drives no queues, arms no crons,
and installs no container specs.

Bindings reach the real data. Cells always serve from the live
generation, so D1, KV, Queue, and Durable Object bindings read and write
the same cells the live version uses: a D1 write from the staged version
is visible to the live one, and vice versa. Service bindings resolve
from the staged generation's components, not the live pointer: a staged
app that calls its helper reaches the staged helper when pinned, the
carried live helper otherwise. `request.url` carries the invoked path on
the target script as its host. The Workers Cache API is the one exception to
real data: under `CELLD_CACHE_SCOPE=deployment` each invoke's staged
generation has a scope of its own, keyed by its reserved generation id,
so staged code never reads or writes the live deployment's cached
entries (even when the invoke names the live versions), and the scope
is dropped when the staged generation retires.

The request behaves like a normal request. The handler budget bounds the
response, and the invoke answers only after `waitUntil` settles, so work
the handler defers — including service calls it makes after answering —
still resolves against the staged graph. The settle wait is bounded by a
further handler budget past the reply: background work that never
settles fails the invoke instead of pinning the staged generation, so
make migration endpoints answer only done work. Upgrades are refused and
streamed answers are drained up to 64 MiB: only a buffered request and
answer cross. The request body is capped by the node's ingress limit,
`CELLD_MAX_REQUEST_BODY_BYTES`. At most four invokes run on one node at
once; past that the route sheds with 503 and the CLI tries the next node.

The invoke runs to completion on the node even if the caller stops
waiting: a client disconnect or CLI timeout cannot retire the staged
generation under a running driver, so it cannot leave a half-applied
migration behind. A timeout therefore means the answer is unknown, never
that the request failed; only node shutdown — or the settle deadline
above — cancels it. `celld invoke` waits up to ten minutes. Delivery is
at-least-once across retries, so migration endpoints must be
idempotent.

## Authentication

Only the operator can invoke. `POST /invoke` on the internal listener
requires the per-request fleet signature — the same HMAC the
`/runtime/` operator routes require, bound to method, path, body,
timestamp, nonce, and target node — and, when `CELLD_OPERATOR_TOKEN` is
set, the operator token. A tenant Worker has neither: the token never
enters an isolate, and the fleet secret needs bucket credentials. The
public listener has no such route, and peer tunnels carry cell calls,
never this path, so there is nothing to smuggle a version through: a
non-activated version is reachable only through this gate. See
[security](security.md).

## Reference

```sh
celld invoke SCRIPT=VERSION [SCRIPT=VERSION...] --path PATH --bucket NAME
  [--target SCRIPT] [--method METHOD] [--header NAME:VALUE]...
  [--body TEXT | --body-file PATH] [--json] [--unsafe-public-advertise]
```

Stdout carries the staged version's answer body, or the result envelope
with `--json`; anything else goes to stderr. A non-2xx answer still
prints its body, then exits nonzero. `--body-file -` reads the request
body from stdin.

The route answers `{"result": {"status", "headers", "body"}}`, the body
base64, or `{"error": ...}` with 400 (malformed), 404 (unknown version),
409 (the pointer moved), 413 (body over the ingress limit), or 500 (the
load, build, dispatch, or answer failed). A draining node answers 503 and
the CLI moves to the next live node, as for the other operator commands.
