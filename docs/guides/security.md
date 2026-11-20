---
title: Harden a node
description: Separate the listeners, guard the operator API with a token, and lock down egress and the bucket.
---

<!-- Modified by Ryuz Corp (Cello): operator token for the internal listener; tenant egress policy; runtime cell isolation; deployment cache scope trade-off; staged invoke. -->

Cello v0.6.3 is a beta release. It is not safe for hostile multi-tenant use. Only the latest release receives security fixes. One fleet runs one application — see [what Cello isolates](../concepts/security-boundary.md) before you host code from mutually distrusting tenants.

## Separate the listeners

Cello opens two HTTP listeners and terminates TLS on neither.

| Listener | Serves | Required protection |
| --- | --- | --- |
| `--listen` (public) | The deployed Worker | Terminate TLS and authenticate users in a proxy or the application. |
| `--internal-listen` (default `127.0.0.1:0`, a free loopback port) | The peer protocol and the operator API | Restrict access to trusted operators and fleet nodes. Never expose it to the public internet. Use an encrypted overlay such as WireGuard or Tailscale when the network does not provide confidentiality. |

`--advertise` gives peers the internal address. Cello rejects an explicit `--advertise` or a non-loopback `--listen` without an explicit `--internal-listen`. Cello cannot verify a hostname or a translated port, so you must route the advertised address to the internal listener.

The public listener reserves only `/.well-known/cello/health`: 200 with `{"ok":true}` when healthy, 503 otherwise. The Worker owns every other public path, including `/health`. The internal listener returns 404 for an unknown path, so an operator request cannot become an application request.

The internal listener has three request groups, and all three require the trusted private network:

- Most operator routes have no request authentication, unless `CELLO_OPERATOR_TOKEN` is set (see below).
- `/peer/tunnel` opens a tunnel for cell fetch, RPC, and WebSocket calls. The establishment request carries the fleet HMAC, a clock limit, and replay protection. The calls inside the tunnel are plain, unsigned HTTP.
- The peer-control and reserved-cell routes sign each request with the fleet HMAC, a clock limit, and replay protection.

The HMAC does not authenticate tunnel bytes after establishment and does not encrypt traffic, so it does not replace the private network.

## Guard the internal listener with a token

Set `CELLO_OPERATOR_TOKEN` to the same value on every node of the fleet, and every internal-listener route requires `Authorization: Bearer <token>`, compared in constant time, and answers 401 without it. There are no exempt routes: unknown paths answer 401 too, and liveness and readiness probes use the public listener's `/.well-known/cello/health`, which the token does not guard. A node with the variable set but empty refuses to start; an unset variable is the open listener above.

Every in-tree client of the internal listener reads the variable from its own environment and sends the token: node-to-node peer forwarding, handoff, abort, the log tier, and the signed probe, as well as `cello d1`, `cello kv`, `cello queue`, `cello diagnose`, `cello invoke`, and the `cello dev` supervisor's shutdown. A caller outside the fleet — a deployment script that posts `/reload`, a monitor that reads `/state` — must send the header itself. Roll the token out only after every node runs a Cello that knows the variable: a node that never sends the header is refused by the nodes that require it, so a mixed fleet would split its peer traffic.

A node holds one token and tunnels between nodes do not re-authenticate, so rotating the token means restarting the fleet with the new value: either every node at once, or node by node while accepting that nodes on different tokens refuse each other's peer traffic until the last one restarts.

The token authenticates the caller; it does not encrypt the traffic. Without TLS on the internal listener the token travels in cleartext, so a network observer still sees it. Keep the trusted private network or the encrypted overlay: the token shrinks what that network must prove, it does not replace it.

## Use the internal operator API

The operator API is an alpha interface. A release can change its paths or response formats. Without the token, these routes do not authenticate the caller:

- `/state` reports node state.
- `/cell/<SCOPE>` resolves or activates a cell.
- `/evict/<SCOPE>` tries to evict a resident cell and reports the result.
- `/do/<ID>` sends a direct request to an ordinary Durable Object.
- `POST /shutdown` starts a graceful ownership handoff. `handoff=preserve` prepares a same-node reload.

`/do/<ID>` refuses every reserved runtime class, such as D1, Workflows, KV, and Queues, because their protocols can access application data. Use the HMAC-authenticated `/runtime/<SCOPE>` route for these classes.

`POST /invoke` serves one request from staged, not yet activated, script versions (`cello invoke`), so a migration endpoint runs before the activation that would serve it. Like `/runtime/`, it requires the per-request fleet HMAC plus the operator token when one is set; without the token it still requires the HMAC, which a tenant cannot mint. No public route and no peer tunnel reaches a non-activated version: it is reachable only through this gate. The invoke writes nothing to the bucket, moves no pointer, and serves staged code in fresh isolate pools that share nothing with live traffic and are retired after the request; cells resolve from the live generation, so bindings reach the real data. It runs to completion server-side even if the caller disconnects, and it answers only after `waitUntil` settles. See [stage versions](staged-versions.md).

`/peer/probe` returns a signed diagnostic response. Do not call the other reserved peer paths directly.

### Read an eviction result

An accepted eviction waits for the operation to finish, and a refused one returns at once. Concurrent callers can join one eviction, so the success count does not equal the stop count.

The Rust method `AppHandle::evict` returns `Result<EvictSuccess, EvictError>`. `Evicted` confirms a completed runtime stop, and `AlreadyAbsent` confirms settled local absence. A caller that needs a completed eviction must check for `Evicted`. Over HTTP, both return 200 with `{"ok":true}`. A later request can reactivate the cell before the response arrives.

A missing cell or a settled `Inactive`, `Dormant`, or `Remote` cell is locally absent. A pending activation, stop, or ownership transfer prevents that result. The request does not evict a remote runtime. A dormant cell keeps its ownership and its hibernated host sockets. The node refuses during preservation, reload, or local inventory confirmation, or when it lacks authority.

An error body has this form:

```json
{"ok":false,"error":{"kind":"refused","reason":"cell_active"}}
```

| Status | Kind | Reason | Cause |
| --- | --- | --- | --- |
| 409 | `refused` | `cell_active` | The cell has active work or a socket that requires a runtime. |
| 409 | `refused` | `cell_transitioning` | The cell has another lifecycle transition. |
| 409 | `refused` | `alarm_imminent` | The alarm residency policy retains the cell. |
| 409 | `refused` | `alarm_uncovered` | The alarm coverage is unconfirmed, or a firing alarm blocks eviction during pressure shedding. |
| 503 | `refused` | `node_unavailable` | The node cannot accept the eviction. |
| 503 | `refused` | `eviction_limit` | The node has reached its eviction concurrency limit. |
| 409 | `cancelled` | `new_activity` | A new request cancels an accepted eviction. |
| 409 | `cancelled` | `alarm_activity` | An alarm observation or firing cancels an accepted eviction. |
| 409 | `cancelled` | `node_fenced` | The node loses its authority during the eviction. |
| 503 | `failed` | `actor_unavailable` | The request cannot reach the Actor. |
| 500 | `failed` | `reply_lost` | A delivered request loses its reply, so its outcome is unknown. |
| 500 | `failed` | `durability_failed` | The durability verification fails. |
| 500 | `failed` | `durability_timeout` | The durability verification exceeds its operation deadline. |
| 500 | `failed` | `runtime_stop_failed` | The runtime stop reports a failure. |

A malformed scope returns 400. An error does not prove that the runtime is still resident. The runtime stop has no overall timeout, so a stop that never returns keeps the request pending.

## Set the forwarded-header policy

Cello ignores `X-Forwarded-Host` and `X-Forwarded-Proto` by default. Set `--trust-forwarded-headers` or `CELLO_TRUST_FORWARDED_HEADERS=1` only when a trusted proxy replaces both headers. Cello uses the last value in each header, so an earlier client value cannot override the proxy value.

Cello takes the path and query from the request target and ignores the scheme and authority of an absolute-form target. Without a trusted proxy, the `Host` header sets the hostname in `request.url`. Cello accepts a hostname, an IPv4 address, or a bracketed IPv6 address, with an optional port. It rejects malformed and noncanonical values and falls back to `cello.local`.

The hostname is still client-controlled. Do not use an unchecked hostname for an authorization decision. Use a trusted proxy, or check the hostname against a list in the Worker.

## Limit request bodies

The public listener and `/do/<ID>` limit a request body to 1 GiB by default. Set `CELLO_MAX_REQUEST_BODY_BYTES` to a smaller positive value to lower it. Cello returns 413 for a declared oversized body and when a Worker reads past the limit.

For a method other than `GET` or `HEAD`, `/do/<ID>` streams a body of unknown length or of at least 1 MiB. It collects a smaller body before dispatch.

## Restrict Worker egress

By default (`CELLO_EGRESS=open`) Worker code may dial any address with `fetch()`, `connect()` sockets, and outbound WebSockets. `CELLO_EGRESS=public` refuses dials to every non-public address: loopback, private (RFC 1918), link-local, carrier-grade NAT (RFC 6598), IPv6 unique-local, unspecified, multicast, reserved (`240.0.0.0/4`), the IPv4-mapped and transition-embedded (NAT64, 6to4, Teredo, IPv4-compatible) forms of those, and the cloud metadata address (`169.254.169.254`).

`CELLO_EGRESS_ALLOW` lists exceptions as comma-separated CIDRs (any port) or `host:port` pairs (that host on that port only), for example an object store endpoint on a private address:

```sh
CELLO_EGRESS=public CELLO_EGRESS_ALLOW=10.89.0.7:9000,10.89.0.0/24
```

A hostname entry resolves on every check, so a moved endpoint is re-allowed without a restart. A `host:port` entry is port-scoped on every hop: the initial request and each redirect hop match the entry only on its port, against the hop's full URL. A redirect to a bare IP literal matches only IP, CIDR, and IP:port entries — never a hostname entry — so allowlist redirect targets in one of those forms. The node refuses to boot when either variable is invalid.

Enforcement happens on the resolved address at connect time: hostnames resolve and filter before the dial, every address is judged, and redirects re-check on every hop, so DNS rebinding and redirects cannot bypass the policy. In `public` mode tenant clients never use a proxy (`HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` are ignored): through a proxy the target would resolve with the proxy's DNS and redirect hops would expose only the proxy's address, so a proxied fetch cannot be policed. Cello's own traffic — the object store, peers, control, telemetry, and container-engine clients — never goes through these checks and keeps its proxy handling. Dials to container endpoints this node published via `getTcpPort()` are exempt, and that exemption is node-global: any Worker on the node may dial a published container endpoint by its address and port. A refused `fetch()` rejects with a `TypeError` whose message starts `fetch failed: egress policy refused ...`; a refused `connect()` rejects the socket's `opened`/`closed` promises, and a refused WebSocket fires `error` and closes with 1006, each naming the refused address and reason. Every refusal also logs an `egress_refused` warning with the target's scheme, host, and port, the refused address, and the reason — never the full URL, whose userinfo or query may carry secrets.

## Protect the fleet bucket

The fleet bucket is the root of authority. It stores the deployments, the cell state, the ownership and node leases, and the shared peer-authentication secret. A holder of the bucket credentials controls the fleet. Give each credential access to one fleet bucket only, and rotate it after a suspected disclosure.
## Security boundary

Moved to [what Cello isolates](../concepts/security-boundary.md).

## Runtime cell isolation

Moved to [what Cello isolates](../concepts/security-boundary.md#isolate-runtime-cells).

## Cache scope and co-hosted scripts

Moved to [what Cello isolates](../concepts/security-boundary.md#scope-the-cache).

## Cell ownership

Moved to [what Cello isolates](../concepts/security-boundary.md#fence-cells-without-isolating-tenants).
