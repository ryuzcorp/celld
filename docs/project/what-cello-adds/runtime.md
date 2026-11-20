---
title: Runtime
description: JavaScript runtime, RPC, container exec, isolate concurrency, and cancellation fixes over upstream denoland/celld.
---

These patches change what your code can do at runtime and how concurrent requests share the process. For the APIs themselves, read [Workers](../../services/workers.md), [Durable Objects](../../services/durable-objects.md), and [Containers](../../services/containers.md); for option defaults, [configuration](../../reference/configuration.md). Each entry names its fork patch number.

## Pass stubs and streams across calls

- `0003` — `node:crypto` gains `scrypt` and `scryptSync` with Node's parameters, defaults, and error codes.
- `0004` — `Response.clone()` tees a streaming body like `Request.clone()` does, and both `clone()`s refuse a body already read through its reader, as Fetch does.
- `0005` — Service-binding RPC carries Durable Object stubs both ways: a `WorkerEntrypoint` method can return or accept a stub of a namespace it holds. RpcTargets, functions, and streams still fail with `DataCloneError`.
- `0007` — Durable Object RPC exports cross isolates on the same node: stubs mint under host-routable ids validated on every op, and `ReadableStream` args cross by caller-side push into a bounded, backpressured host channel. Same node only. Mint caps (4096 per scope, 65536 per process) fail the call with a clear error.
- `0043` — A stream received over RPC stays usable while the receiver holds it: an active reader or writer keeps the stream past the receiving call, the way a `ReadableStream` sent over RPC stays open until its writer closes it. Reading to the end, closing, cancelling, or an explicit dispose releases the hold at once. This ends the spurious aborts of signal-carrying container execs.
- `0115` — RPC results that are plain objects or arrays carry `Symbol.dispose`, so `using` declarations clean them up like other RPC handles ([upstream #260](https://github.com/denoland/celld/issues/260)).

## Drive container exec without orphans

See [Containers](../../services/containers.md) for the API. These patches close the failure modes around it:

- `0006` — Exec takes `pty` (`true` or `{ cols, rows }`, default 80×24), `isPty`, and `resize(cols, rows)`; a failed first resize kills the exec instead of orphaning it.
- `0008` — `exitCode` survives the exec event: a retained wait re-subscribes in each later event instead of hanging on the first event's dropped op.
- `0010` — Exec honors `options.signal` (AbortSignal): an already-aborted signal refuses with its reason before spawning, aborting later sends SIGKILL, and the listener is removed when the process exits.
- `0015` — Exec kill reaches the process: the kill op runs under the start's lifetime, and the signal is delivered by an in-container scan for the exec's marker env instead of a host-pid `kill`. A killed exec's `exitCode` settles with its signal exit.
- `0016` — Exec reads survive their subscribing event, handing bytes exactly once across re-subscription.
- `0038` — Exec stdin close and signals run under the start's lifetime, so an exec with the default stdin reaches EOF instead of hanging. Single-shot engine control calls fail after 10 s (60 s for create/start/remove); a failed post-start exec inspect kills the started process so no untracked orphan remains.

## Share one isolate across concurrent requests

Concurrent requests share one isolate without stalling or borrowing each other's request. The pinning patches below are white-box and black-box conformance pins with no runtime change unless noted:

- `0019` — Context end aborts every upload id the context created, not only pumps still registered, so finished uploads stop lingering sender-alive until isolate retirement.
- `0036` — Stateless entries carry continuation tokens; request-owned sockets, WebSockets, HTMLRewriters, and stream tees are accounted to the owning request, and a retired owner throws with nothing registered.
- `0079` — Cancelled requests reject shared work instead of hanging co-awaiters: `reject_pending_ops` rejects the refed operations the event still waits on, so every request awaiting the same promise settles ([upstream #317](https://github.com/denoland/celld/issues/317)).
- `0080` — Shared-promise op attribution is pinned for waiter-owned next ops: a request that awaits another's fetch, then arms its own timer or fetch, attributes the next op to its own driver ([upstream #316](https://github.com/denoland/celld/issues/316)).
- `0081` — Answered drivers poll while waitUntil background is outstanding, keeping the registering event alive to lend its context and take the handoff, instead of retiring early and failing later work ([upstream #300](https://github.com/denoland/celld/issues/300)).
- `0082` — Cancelled-request lazy-init settlement is pinned for Durable Objects: a mid-flight cancel rejects the shared timer op and the retry answers ([upstream #294](https://github.com/denoland/celld/issues/294)).
- `0083` — Drivers retire with the mailbox drained: each drive loop takes the mailbox once more before leaving, so a late handoff either lands or is refused at the source instead of dropping its resolver ([upstream #309](https://github.com/denoland/celld/issues/309)).
- `0084` — Refused-handoff settlement is pinned for retired owners: refused handoffs reject instead of dropping the resolver ([upstream #207](https://github.com/denoland/celld/issues/207)).
- `0085` — Answered waitUntil retirement with no native ops is pinned: the wake path polls while background work is outstanding, so the entry retires once the settlement arrives instead of spinning to the budget ([upstream #289](https://github.com/denoland/celld/issues/289)).
- `0086` — Constructor task loops resumed by a later event are pinned: the continuation-token line roots the woken work ([upstream #277](https://github.com/denoland/celld/issues/277)).
- `0087` — Woken-continuation outbound settlement is pinned: the dead origin's ops refuse at creation and the waiter settles loudly ([upstream #258](https://github.com/denoland/celld/issues/258)).
- `0118` — Cancelling a loader call publishes the abort synchronously instead of a turn late, so abandoned work stops at once; a loader fetch dropped before dispatch releases its cancellation state ([upstream #281](https://github.com/denoland/celld/issues/281)).

## Bound background work and recover cleanly

- `0101` — Post-response background work ends past the new knob `CELLO_BACKGROUND_BUDGET_S` (default 30 seconds, Cloudflare's waitUntil limit) without failing the answered request, releasing the isolate so retiring generations drain ([upstream #275](https://github.com/denoland/celld/issues/275)). An event that still holds the cell's input gate stays loud about it. A streaming response body is response work, not background work: it holds its own lifetime outside the budget, so an SSE stream keeps delivering while the client stays connected and the body produces. The budget clock for leftover `waitUntil` work starts when the response is fully released past the output gate, so expiry can end that work while the body still streams; timers it already started stop one budget window after the body ends.
- `0104` — The cell restarts on a fresh isolate when a finished activity leaves its scope poisoned: a write failing with `SQLITE_FULL`, `IOERR`, `NOMEM`, or `INTERRUPT` destroys its transaction, and only a reopen clears it ([upstream #291](https://github.com/denoland/celld/issues/291)).
- `0106` — `AbortSignal.timeout()` is unref'd so a created signal frees its cell slot past its usefulness ([upstream #306](https://github.com/denoland/celld/issues/306)).
- `0107` — The 101 webSocket target crosses the loader fetch, with per-deployment handoff quotas (64 per quota, 1024 globally, 60 s expiry), a staged 16 MiB pipe budget that fails loudly instead of parking silently, and request-owner read authorization with cell-shutdown reclamation by scope ([upstream #305](https://github.com/denoland/celld/issues/305)).
- `0108` — External stubs build from the union of main and sibling imports, not the main module's alone ([upstream #304](https://github.com/denoland/celld/issues/304)).
- `0117` — A Durable Object opening a WebSocket after its response was sent dials with its object's cell attached, routing the connection ([upstream #254](https://github.com/denoland/celld/issues/254)).
- `0125` — A workflow retry arriving after the old drive was budget-orphaned drops the orphaned drive, marked by an in-memory redrive bit so a racing nudge cannot hide it ([upstream #299](https://github.com/denoland/celld/issues/299)).
- `0127` — A restored alarm whose start failed disarms if covered, and re-arms at scan pace (`FAILED_START_ALARM_REARM_MS = 60 s`) if uncovered, instead of refiring into the same failure ([upstream #276](https://github.com/denoland/celld/issues/276)).
