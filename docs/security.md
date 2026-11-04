<!-- Modified by ryuzcorp (ryuzcorp/celld): operator token for the internal listener; tenant egress policy; runtime cell isolation; deployment cache scope trade-off; staged invoke. -->
# Security

celld v0.6.3 is a beta release. It is not safe for hostile multi-tenant use.
Only the latest release receives security fixes.

## Security boundary

One fleet runs one application. celld trusts the application code, the fleet
nodes, and the operators. Application code can use its configured bindings and
can consume shared node resources. Do not run code from mutually distrusting
tenants in one fleet.

Application code cannot reach the engine's host functions. An internal script
receives them as function parameters, and `globalThis` carries no
`__`-prefixed property. A Worker Loader worker with `globalOutbound: null`
therefore reaches the host only through the capabilities in its `env`.

celld depends on two external boundaries: a trusted private network protects
the internal listener, and object storage credentials control the fleet.

## Runtime cell isolation

Cells of the shared runtime classes — D1 databases, KV namespaces, and Queue
brokers — serve every script in the fleet, so they run in a dedicated runtime
isolate pool whose isolates evaluate no tenant bundle: the harness plus a
fixed inert entry module. No script's top level shares a realm with them, and
the pool holds no tenant bindings, vars, services, loaders, or schedule —
only the deployment-wide queue catalog, the node's routing identity, the
generation tag, and the union of every declaring script's compatibility
switches. Workflow
cells and the cron cell stay in their own script's pool: they execute that
script's `run()` and `scheduled()` handlers with its bindings, so isolating
them from their own script would isolate them from the code they run.
Per-cell placement and ownership are unchanged.

## Cache scope and co-hosted scripts

The Workers Cache API is per script by default: no script can read,
overwrite, or purge another script's entries. `CELLD_CACHE_SCOPE=deployment`
lifts that boundary for scripts of one deployment, like one Cloudflare
zone: every script of the serving deployment shares one `caches.default`
and one set of `caches.open(name)` namespaces, so a co-hosted script can
read entries holding another script's session or personalized bodies,
overwrite them, and purge them. Turn it on only when every script of the
deployment is mutually trusted. The scope is an operator setting read
from the node's process environment at startup; no binding, Worker var,
or JS API can change it, and an unknown value refuses the boot rather
than silently sharing. Sharing stays bounded: a different deployment or
fleet still sees nothing, the runtime pool and dynamically loaded
workers keep their own entries, and each scope carries its own size cap
and recency (see [cache](services/cache.md#cache-scope)).

## Separate the listeners

celld opens two HTTP listeners and terminates TLS on neither.

| Listener | Serves | Required protection |
| --- | --- | --- |
| `--listen` (public) | The deployed Worker | Terminate TLS and authenticate users in a proxy or the application. |
| `--internal-listen` (default `127.0.0.1:0`, a free loopback port) | The peer protocol and the operator API | Restrict access to trusted operators and fleet nodes. Never expose it to the public internet. Use an encrypted overlay such as WireGuard or Tailscale when the network does not provide confidentiality. |

`--advertise` gives peers the internal address. celld rejects an explicit
`--advertise` or a non-loopback `--listen` without an explicit
`--internal-listen`. celld cannot verify a hostname or a translated port, so
you must route the advertised address to the internal listener.

The public listener reserves only `/.well-known/celld/health`: 200 with
`{"ok":true}` when healthy, 503 otherwise. The Worker owns every other public
path, including `/health`. The internal listener returns 404 for an unknown
path, so an operator request cannot become an application request.

The internal listener has three request groups, and all three require the
trusted private network:

- Most operator routes have no request authentication, unless
  `CELLD_OPERATOR_TOKEN` is set (see below).
- `/peer/tunnel` opens a tunnel for cell fetch, RPC, and WebSocket calls. The
  establishment request carries the fleet HMAC, a clock limit, and replay
  protection. The calls inside the tunnel are plain, unsigned HTTP.
- The peer-control and reserved-cell routes sign each request with the fleet
  HMAC, a clock limit, and replay protection.

Every path to a runtime class therefore demands the fleet secret. The HMAC
does not authenticate tunnel bytes after establishment and does not encrypt
traffic, so it does not replace the private network.

## Guard the internal listener with a token

Set `CELLD_OPERATOR_TOKEN` to the same value on every node of the fleet,
and every internal-listener route requires
`Authorization: Bearer <token>`, compared in constant time, and answers
401 without it. There are no exempt routes: unknown paths answer 401 too,
and liveness and readiness probes use the public listener's
`/.well-known/celld/health`, which the token does not guard. A node with
the variable set but empty refuses to start; an unset variable is the
open listener above.

Every in-tree client of the internal listener reads the variable from its
own environment and sends the token: node-to-node peer forwarding,
handoff, abort, the log tier, and the signed probe, as well as
`celld d1`, `celld kv`, `celld queue`, `celld diagnose`, `celld invoke`,
and the `celld dev` supervisor's shutdown. A caller outside the fleet — a deployment
script that posts `/reload`, a monitor that reads `/state` — must send
the header itself. Roll the token out only after every node runs a celld
that knows the variable: a node that never sends the header is refused
by the nodes that require it, so a mixed fleet would split its peer
traffic.

A node holds one token and tunnels between nodes do not re-authenticate,
so rotating the token means restarting the fleet with the new value:
either every node at once, or node by node while accepting that nodes
on different tokens refuse each other's peer traffic until the last one
restarts.

The token authenticates the caller; it does not encrypt the traffic.
Without TLS on the internal listener the token travels in cleartext, so
a network observer still sees it. Keep the trusted private network or
the encrypted overlay: the token shrinks what that network must prove,
it does not replace it.

## Use the internal operator API

The operator API is an alpha interface. A release can change its paths or
response formats. Without the token, these routes do not authenticate the caller:

- `/state` reports node state.
- `/cell/<SCOPE>` resolves or activates a cell.
- `/evict/<SCOPE>` tries to evict a resident cell and reports the result.
- `/do/<ID>` sends a direct request to an ordinary Durable Object.
- `POST /shutdown` starts a graceful ownership handoff. `handoff=preserve`
  prepares a same-node reload.

`/do/<ID>` refuses every reserved runtime class, such as D1, Workflows, KV,
and Queues, because their protocols can access application data. Use the
HMAC-authenticated `/runtime/<SCOPE>` route for these classes.

`POST /invoke` serves one request from staged, not yet activated, script
versions (`celld invoke`), so a migration endpoint runs before the
activation that would serve it. Like `/runtime/`, it requires the
per-request fleet HMAC plus the operator token when one is set; without
the token it still requires the HMAC, which a tenant cannot mint. No
public route and no peer tunnel reaches a non-activated version: the
public listener has no such path, and tunnels carry cell calls, never
this route. The invoke writes nothing to the bucket, moves no pointer,
and serves staged code in fresh isolate pools that share nothing with
live traffic and are retired after the request; cells resolve from the
live generation, so bindings reach the real data. It runs to completion
server-side even if the caller disconnects, so stopping the wait cannot
leave a half-applied migration, and it answers only after `waitUntil`
settles. See [Invoke a staged version](staged-invoke.md).

`/peer/probe` returns a signed diagnostic response. Do not call the other
reserved peer paths directly.

### Read an eviction result

An accepted eviction waits for the operation to finish, and a refused one
returns at once. Concurrent callers can join one eviction, so the success
count does not equal the stop count.

The Rust method `AppHandle::evict` returns `Result<EvictSuccess, EvictError>`.
`Evicted` confirms a completed runtime stop, and `AlreadyAbsent` confirms
settled local absence. A caller that needs a completed eviction must check for
`Evicted`. Over HTTP, both return 200 with `{"ok":true}`. `kind()` and
`reason()` on the error match the HTTP error body. A later request can
reactivate the cell before the response arrives.

A missing cell or a settled `Inactive`, `Dormant`, or `Remote` cell is locally
absent. A pending activation, stop, or ownership transfer prevents that
result. The request does not evict a remote runtime. A dormant cell keeps its
ownership and its hibernated host sockets. The node refuses during
preservation, reload, or local inventory confirmation, or when it lacks
authority.

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

A malformed scope returns 400. An error does not prove that the runtime is
still resident. The runtime stop has no overall timeout, so a stop that never
returns keeps the request pending.

## Set the forwarded-header policy

celld ignores `X-Forwarded-Host` and `X-Forwarded-Proto` by default. Set
`--trust-forwarded-headers` or `CELLD_TRUST_FORWARDED_HEADERS=1` only when a
trusted proxy replaces both headers. celld uses the last value in each header,
so an earlier client value cannot override the proxy value.

celld takes the path and query from the request target and ignores the scheme
and authority of an absolute-form target. Without a trusted proxy, the `Host`
header sets the hostname in `request.url`. celld accepts a hostname, an IPv4
address, or a bracketed IPv6 address, with an optional port. It rejects
malformed and noncanonical values and falls back to `celld.local`.

The hostname is still client-controlled. Do not use an unchecked hostname for
an authorization decision. Use a trusted proxy, or check the hostname against a
list in the Worker.

## Limit request bodies

The public listener and `/do/<ID>` limit a request body to 1 GiB by default.
Set `CELLD_MAX_REQUEST_BODY_BYTES` to a smaller positive value to lower it.
celld returns 413 for a declared oversized body and when a Worker reads past
the limit.

For a method other than `GET` or `HEAD`, `/do/<ID>` streams a body of unknown
length or of at least 1 MiB. It collects a smaller body before dispatch.

## Restrict Worker egress

By default (`CELLD_EGRESS=open`) Worker code may dial any address with
`fetch()`, `connect()` sockets, and outbound WebSockets.
`CELLD_EGRESS=public` refuses dials to every non-public address:
loopback, private (RFC 1918), link-local, carrier-grade NAT (RFC 6598),
IPv6 unique-local, unspecified, multicast, reserved (`240.0.0.0/4`),
the IPv4-mapped and transition-embedded (NAT64, 6to4, Teredo,
IPv4-compatible) forms of those, and the cloud metadata address
(`169.254.169.254`).

`CELLD_EGRESS_ALLOW` lists exceptions as comma-separated CIDRs (any
port) or `host:port` pairs (that host on that port only), for example
an object store endpoint on a private address:

```sh
CELLD_EGRESS=public CELLD_EGRESS_ALLOW=10.89.0.7:9000,10.89.0.0/24
```

A hostname entry resolves on every check, so a moved endpoint is
re-allowed without a restart. A `host:port` entry is port-scoped on
every hop: the initial request and each redirect hop match the entry
only on its port, against the hop's full URL. A redirect to a bare IP
literal matches only IP, CIDR, and IP:port entries — never a hostname
entry, which cannot be resolved in that path — so allowlist redirect
targets in one of those forms. The node refuses to boot when either
variable is invalid.

Enforcement happens on the resolved address at connect time: hostnames
are resolved and filtered before the dial, every address is judged, and
redirects re-check on every hop, so DNS rebinding and redirects cannot
bypass the policy. In `public` mode tenant clients never use a proxy
(`HTTP_PROXY`/`HTTPS_PROXY`/`ALL_PROXY` are ignored): through a proxy
the target would resolve with the proxy's DNS and redirect hops would
expose only the proxy's address, so a proxied fetch cannot be policed.
celld's own traffic — the object store, peers, control, telemetry, and
container-engine clients — never goes through these checks and keeps
its proxy handling. Dials to container endpoints this node published
via `getTcpPort()` are exempt, and that exemption is node-global: any
Worker on the node may dial a published container endpoint by its
address and port, the same reachability the nft gateway granted. A
refused `fetch()` rejects with a `TypeError` whose message starts
`fetch failed: egress policy refused ...`; a refused `connect()`
rejects the socket's `opened`/`closed` promises, and a refused
WebSocket fires `error` and closes with 1006, each naming the refused
address and reason. Every refusal also logs an `egress_refused`
warning with the target's scheme, host, and port, the refused address,
and the reason — never the full URL, whose userinfo or query may carry
secrets.

## Protect the fleet bucket

The fleet bucket is the root of authority. It stores the deployments, the cell
state, the ownership and node leases, and the shared peer-authentication
secret. A holder of the bucket credentials controls the fleet. Give each
credential access to one fleet bucket only, and rotate it after a suspected
disclosure.

## Cell ownership

Each cell is a SQLite database with one writer. An ownership epoch fences each
cell, so a node that loses its lease cannot modify the current state. This
fencing protects storage consistency. It does not isolate hostile
applications. See [what celld guarantees](guarantees.md) and
[limitations](limitations.md).
