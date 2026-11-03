// Modified by ryuzcorp (ryuzcorp/celld): cross-isolate RPC
// stubs and streams. Apache-2.0.
//
// Conformance fixture for cross-isolate Durable Object RPC stubs and byte
// streams (Worker<->DO and DO<->DO, same node). Each Worker route drives one
// scenario and answers its transcript as JSON, so a runner can assert
// behavior through plain HTTP. The DO side keeps its transcripts in memory;
// a route reads them back through a later DO method on the same instance.
import { DurableObject, RpcTarget } from "cloudflare:workers";

const utf8 = new TextEncoder();
const utf8d = new TextDecoder();

class Capability extends RpcTarget {
  constructor(state) {
    super();
    this.state = state;
  }
  // The release path runs the target's Symbol.dispose: count it, so a
  // caller can prove an explicit dispose reached the owner.
  [Symbol.dispose]() {
    this.state.disposed++;
  }
  // Plain-data method call, in the shape of a process capability.
  async invoke(operation, args) {
    if (operation === "add") return args[0] + args[1];
    if (operation === "echo") return args;
    if (operation === "slowAdd") {
      // A slow stub op: flags arrival owner-side (so a test can dispose
      // mid-drive deterministically), sleeps, then answers. A release
      // racing the drive must not fail it: validated ops pin.
      this.state.slowArrived = true;
      await new Promise((resolve) => setTimeout(resolve, args[2] ?? 0));
      return args[0] + args[1];
    }
    if (operation === "makeChild") {
      // A fresh child capability: the reply lift exports it, so a
      // nested caller revives a live handle for it.
      return new Capability(this.state);
    }
    if (operation === "slowNestedEcho") {
      // Call out to a sibling capability and hold the nested child:
      // the nested reply revive registers in this drive's context
      // (not the params, which the op disposes), and the returned
      // child's reply lift must move it out even when a concurrent
      // drive against the same entry ends first.
      const child = await args[0].invoke("makeChild", []);
      await new Promise((resolve) => setTimeout(resolve, args[1] ?? 0));
      return child;
    }
    if (operation === "exitCode") {
      this.state.exitCodeCalls++;
      return 0;
    }
    throw new TypeError(`unknown operation ${operation}`);
  }
  get label() {
    return "capability";
  }
}

// A capability minted by an own-script facet: the facet runs in its
// root's isolate, so a cross-isolate export must mint under the root's
// scope, not the facet's unpublished one.
class FacetThing extends RpcTarget {
  async invoke(operation, args) {
    if (operation === "add") return args[0] + args[1];
    throw new TypeError(`unknown operation ${operation}`);
  }
}

export class Cap extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.writes = [];
    this.closed = false;
    this.aborted = false;
    this.cancelled = false;
    this.disposed = 0;
    this.exitCodeCalls = 0;
    this.slowArrived = false;
    this.carrierLast = null;
    this.lastSignal = null;
  }

  // A capability box after a delay, so a waitUntil-floated call's reply
  // provably lands after response end.
  async slowBox(delayMs) {
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    return this.makeCapability();
  }

  // One RpcTarget with plain-data methods, plus a readable and a writable
  // byte stream owned by this cell.
  async makeCapability() {
    const box = {};
    box.capability = new Capability(this);
    box.stdout = new ReadableStream({
      start(controller) {
        controller.enqueue(utf8.encode("out-1\n"));
        controller.enqueue(utf8.encode("out-2\n"));
        controller.close();
      },
    });
    const state = this;
    box.stdin = new WritableStream({
      write(chunk) {
        state.writes.push(utf8d.decode(chunk));
      },
      close() {
        state.closed = true;
      },
      abort() {
        state.aborted = true;
      },
    });
    return box;
  }

  // The sandboxed `exec` return shape.
  async exec() {
    const wire = await this.makeCapability();
    return {
      pid: 4242,
      isPty: false,
      stdin: wire.stdin,
      stdout: wire.stdout,
      stderr: new ReadableStream({
        start(controller) {
          controller.enqueue(utf8.encode("err-1\n"));
          controller.close();
        },
      }),
      capability: wire.capability,
    };
  }

  // A stream that fails after two chunks. The error arrives on the
  // third pull: erroring inside start() would discard the queued chunks
  // per the stream spec, before any bridge is involved.
  async failingStream() {
    let pulls = 0;
    return {
      stream: new ReadableStream({
        pull(controller) {
          pulls++;
          if (pulls <= 2) {
            controller.enqueue(utf8.encode(`chunk-${pulls}\n`));
          } else {
            controller.error(new Error("boom-mid-stream"));
          }
        },
      }),
    };
  }

  // A stream that never produces, so the caller can cancel it mid-flight.
  // Cancelling must reach this side.
  async hangingStream() {
    const state = this;
    return {
      stream: new ReadableStream({
        start() {},
        cancel() {
          state.cancelled = true;
        },
      }),
    };
  }

  // Lend a local capability to a sibling cell and await its verdict:
  // the sibling drives the lent stub concurrently while this invocation
  // (the entry's context) is still live, so both drives share one live
  // entry context owner-side.
  async lendAndAwait(peerName) {
    const peer = this.env.CAP.getByName(peerName);
    return await peer.raceLent(new Capability(this), this.ctx.id.name);
  }

  // Drive a borrowed stub concurrently: a slow op plus a nested-echo op
  // whose reply carries a nested child. The first drive's end must not
  // dispose the sibling's nested revives.
  async raceLent(borrowed, ownerName) {
    const owner = this.env.CAP.getByName(ownerName);
    const mine = await this.makeCapability();
    const pendingA = borrowed.invoke("slowAdd", [40, 2, 800]);
    let arrived = false;
    for (let i = 0; i < 300 && !arrived; i++) {
      const t = await owner.transcript();
      arrived = t.slowArrived === true;
      if (!arrived) await new Promise((resolve) => setTimeout(resolve, 10));
    }
    const pendingB = borrowed.invoke("slowNestedEcho", [mine.capability, 2000]);
    const sumA = await pendingA;
    let sumB = null;
    let errorB = null;
    try {
      const echoed = await pendingB;
      sumB = await echoed.invoke("add", [40, 2]);
    } catch (error) {
      errorB = String((error && error.message) || error);
    }
    return { arrived, sumA, sumB, errorB };
  }

  // One DO calling another DO's capability, through the same stub path.
  async useSibling() {
    const sibling = this.env.CAP.getByName("sibling");
    const wire = await sibling.makeCapability();
    const sum = await wire.capability.invoke("add", [20, 22]);
    const label = await wire.capability.label;
    const stdout = await new Response(wire.stdout).text();
    const writer = wire.stdin.getWriter();
    await writer.write(utf8.encode("sib-stdin\n"));
    await writer.close();
    const transcript = await sibling.transcript();
    return { sum, label, stdout, transcript };
  }

  // Reads a stream passed as an argument back to the caller. A
  // received host stream forwards its id and reads through the loopback;
  // a Worker-owned readable arrives over an upload channel and reads
  // through it.
  async sink(stream) {
    try {
      return { text: await new Response(stream).text() };
    } catch (error) {
      return { error: String(error && error.message || error) };
    }
  }

  // Read one chunk, then cancel: the sender's pump must observe the
  // cancel and tear its origin down.
  async sinkFirst(stream) {
    const reader = stream.getReader();
    try {
      const first = await reader.read();
      return { first: first.done ? null : utf8d.decode(first.value) };
    } finally {
      await reader.cancel().catch(() => {});
    }
  }

  async sinkMany(streams) {
    const texts = [];
    for (const stream of streams) {
      texts.push(await new Response(stream).text());
    }
    return { count: texts.length, texts };
  }

  async sinkWrite(stream) {
    try {
      const writer = stream.getWriter();
      await writer.write(utf8.encode("hello\n"));
      await writer.close();
      return { error: null };
    } catch (error) {
      return { error: String(error && error.message || error) };
    }
  }

  // Stringify a received upload-backed stream: upload ids must never
  // surface to script, like stub ids.
  async uploadLeakProbe(stream) {
    const parts = [];
    parts.push(String(JSON.stringify(stream)));
    try {
      parts.push(String(stream));
    } catch (error) {
      parts.push(`threw:${String((error && error.message) || error)}`);
    }
    return { parts };
  }

  async echoBody(request) {
    if (!(request instanceof Request)) {
      return new Response("not-a-request", { status: 400 });
    }
    return new Response(request.body);
  }

  async signalProbe(signal) {
    this.lastSignal = signal;
    return {
      isSignal: signal instanceof AbortSignal,
      aborted: signal instanceof AbortSignal ? signal.aborted : null,
    };
  }

  async signalState() {
    return {
      aborted:
        this.lastSignal instanceof AbortSignal ? this.lastSignal.aborted : null,
    };
  }

  async invokeForeign(marker, witness) {
    // Unreachable with a forged marker: the reserved-key refusal throws
    // at lift before dispatch. Kept as the attempted target so the
    // /malformed route proves the call never dispatches.
    let witnessText;
    try {
      witnessText = await new Response(witness).text();
    } catch (error) {
      witnessText = `error:${String(error && error.message || error)}`;
    }
    try {
      await marker.someMethod();
      return { error: null, witness: witnessText };
    } catch (error) {
      return {
        error: String(error && error.message || error),
        witness: witnessText,
      };
    }
  }

  // An abort-carrier exec: options carry an abort ReadableStream (the
  // carrier a sandbox client sends), stdout stays quiet until the
  // abort lands, and a capability answers exitCode/pid. One pending
  // read on the carrier; finishing cancels it.
  async carrierExec(argv, options) {
    const state = {
      abortObserved: false,
      finishCalled: false,
      argv: Array.isArray(argv) ? argv : [],
    };
    this.carrierLast = state;
    const { abort } = options ?? {};
    const controller = new AbortController();
    let outController = null;
    let finish = () => {};
    if (abort !== undefined) {
      if (!(abort instanceof ReadableStream)) {
        throw new TypeError("Invalid Sandbox cancellation stream");
      }
      const reader = abort.getReader();
      let finished = false;
      finish = () => {
        if (finished) return;
        finished = true;
        state.finishCalled = true;
        reader.cancel().catch(() => {});
      };
      // The single pending read settles once, on close or error; either
      // way the carrier fired and the operation aborts.
      const onCarrier = () => {
        state.abortObserved = true;
        controller.abort();
      };
      reader.read().then(onCarrier, onCarrier);
    }
    const stdout = new ReadableStream({
      start(c) {
        c.enqueue(utf8.encode("chunk-1\n"));
        outController = c;
      },
    });
    let resolveExit;
    const exitCode = new Promise((resolve) => {
      resolveExit = resolve;
    });
    controller.signal.addEventListener(
      "abort",
      () => {
        try {
          outController.enqueue(utf8.encode("aborted\n"));
        } catch {}
        try {
          outController.close();
        } catch {}
        finish();
        resolveExit(137);
      },
      { once: true },
    );
    const wireExit = exitCode;
    class CarrierExecCapability extends RpcTarget {
      async invoke(operation, args) {
        void args;
        if (operation === "exitCode") return wireExit;
        if (operation === "pid") return 4242;
        throw new TypeError(`unknown operation ${operation}`);
      }
    }
    return { capability: new CarrierExecCapability(), pid: 4242, stdout, isPty: false };
  }

  async carrierTranscript() {
    return this.carrierLast;
  }

  // A stub minted by an own-script facet, handed to the caller: the
  // facet runs in this cell's isolate, so the export must mint under
  // this root's scope and epoch for the handle to validate.
  async facetCapability() {
    const FacetClass = this.ctx.exports.FacetCap({ props: {} });
    const facet = this.ctx.facets.get("stub-facet", () => ({
      class: FacetClass, id: "stub-facet-id",
    }));
    return facet.makeThing();
  }

  async transcript() {
    return {
      writes: this.writes,
      closed: this.closed,
      aborted: this.aborted,
      cancelled: this.cancelled,
      disposed: this.disposed,
      exitCodeCalls: this.exitCodeCalls,
      slowArrived: this.slowArrived,
    };
  }

  async stats() {
    const hook = globalThis.__celldStubStatsForTestOnly;
    if (typeof hook !== "function") return { unavailable: true };
    return hook();
  }
}

// An own-script facet class for the facet-stub flow: same script, so
// the facet runs in its root's isolate (no worker loader involved).
export class FacetCap extends DurableObject {
  async makeThing() {
    return new FacetThing();
  }
}

const json = (value) => Response.json(value);

async function readText(stream) {
  return await new Response(stream).text();
}

export default {
  // A loopback entrypoint op (via ctx.exports.default): with
  // mode "abort" it aborts its own invocation context, so the op
  // rejects past the reply and must still end its context.
  async entryOpProbe(arg, env, ctx) {
    void env;
    if (arg?.mode === "abort") {
      ctx.abort(new Error("op-boom"));
      return { ok: true };
    }
    return { ok: true, arg };
  },
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const stub = (name) => env.CAP.getByName(name);

    // Stub method call with plain args/results, plus a property GET.
    if (url.pathname === "/stub-call") {
      const wire = await stub("one").makeCapability();
      const sum = await wire.capability.invoke("add", [40, 2]);
      const echo = await wire.capability.invoke("echo", [{ a: [1, 2] }, "b"]);
      const label = await wire.capability.label;
      let noSuch = null;
      try {
        await wire.capability.invoke("missing", []);
      } catch (error) {
        noSuch = String(error && error.message || error);
      }
      return json({ sum, echo, label, noSuch });
    }

    // The sandboxed exec shape: capability + pid/isPty + three streams.
    if (url.pathname === "/exec-shape") {
      const wire = await stub("two").exec();
      const writer = wire.stdin.getWriter();
      await writer.write(utf8.encode("hello-stdin\n"));
      await writer.close();
      const stdout = await readText(wire.stdout);
      const stderr = await readText(wire.stderr);
      const exitCode = await wire.capability.invoke("exitCode", []);
      const transcript = await stub("two").transcript();
      return json({
        pid: wire.pid,
        isPty: wire.isPty,
        stdout,
        stderr,
        exitCode,
        transcript,
      });
    }

    // Mid-stream error and caller cancel, DO-owned streams.
    if (url.pathname === "/stream-errors") {
      const failing = await stub("three").failingStream();
      const reader = failing.stream.getReader();
      const chunks = [];
      let error = null;
      for (;;) {
        try {
          const next = await reader.read();
          if (next.done) break;
          chunks.push(utf8d.decode(next.value));
        } catch (caught) {
          error = String(caught && caught.message || caught);
          break;
        }
      }
      const hanging = await stub("three").hangingStream();
      const hangingReader = hanging.stream.getReader();
      const pending = hangingReader.read();
      await hangingReader.cancel();
      // Cancelling resolves the pending pull as done, and the cancel
      // reaches the DO-side producer.
      let pendingDone = null;
      let pendingError = null;
      try {
        pendingDone = (await pending).done;
      } catch (caught) {
        pendingError = String(caught && caught.message || caught);
      }
      const transcript = await stub("three").transcript();
      return json({ chunks, error, pendingDone, pendingError, transcript });
    }

    // Worker->DO bytes: write, close, and abort, verified DO-side.
    if (url.pathname === "/stdin-abort") {
      const wire = await stub("four").makeCapability();
      const writer = wire.stdin.getWriter();
      await writer.write(utf8.encode("part-1\n"));
      await writer.abort(new Error("worker-abort"));
      const transcript = await stub("four").transcript();
      return json({ transcript });
    }

    // DO->DO through the same stub path.
    if (url.pathname === "/do-to-do") {
      return json(await stub("five").useSibling());
    }

    // Explicit dispose frees the owner entry; later use fails. The
    // release round trip is asynchronous, so the runner polls
    // /transcript until the owner's dispose count settles.
    if (url.pathname === "/dispose") {
      const wire = await stub("six").makeCapability();
      const hanging = await stub("six").hangingStream();
      const hangingReader = hanging.stream.getReader();
      // The hanging origin never produces, so this read is still in
      // flight when the trees below are disposed. Disposal settles it:
      // the owner teardown usually cancels the origin first (done),
      // while an op that validates after the take rejects instead.
      // Either way it must settle, never hang.
      const pending = hangingReader.read();
      const sum = await wire.capability.invoke("add", [1, 1]);
      await wire[Symbol.dispose]();
      await hanging[Symbol.dispose]();
      let pendingDone = null;
      let pendingError = null;
      try {
        pendingDone = (await pending).done;
      } catch (error) {
        pendingError = String(error && error.message || error);
      }
      let after = null;
      try {
        await wire.capability.invoke("add", [1, 1]);
      } catch (error) {
        after = String(error && error.message || error);
      }
      // The streams went with the tree: reading must fail too.
      let streamAfter = null;
      try {
        await readText(wire.stdout);
      } catch (error) {
        streamAfter = String(error && error.message || error);
      }
      return json({ sum, after, streamAfter, pendingDone, pendingError });
    }

    // Dup/dispose churn against a tiny scope cap: each iteration dups
    // a host handle and disposes both copies plus the rest of the tree.
    // A per-handle release frees every entry, so the final fresh export
    // succeeds; a leaked hold per iteration fills the cap and the final
    // export throws the cap error instead. (Polling the table count
    // cannot prove this: the heap-retirement backstop frees leaked holds
    // late, so a slow drain still converges.)
    if (url.pathname === "/dup-churn") {
      const iters = Number(url.searchParams.get("iters") || "8");
      let sum = null;
      let twinSum = null;
      for (let i = 0; i < iters; i++) {
        const wire = await stub("dupdo").makeCapability();
        const twin = wire.capability.dup();
        sum = await wire.capability.invoke("add", [1, 1]);
        twinSum = await twin.invoke("add", [2, 3]);
        await wire[Symbol.dispose]();
        await twin[Symbol.dispose]();
      }
      let finalSum = null;
      let finalError = null;
      try {
        const fresh = await stub("dupdo").makeCapability();
        finalSum = await fresh.capability.invoke("add", [4, 5]);
        await fresh[Symbol.dispose]();
      } catch (error) {
        finalError = String((error && error.message) || error);
      }
      return json({ iters, sum, twinSum, finalSum, finalError });
    }

    // An own-script facet's stub crosses isolates under its root's
    // scope and epoch: the facet scope itself is never published, so
    // minting under it could never validate.
    if (url.pathname === "/facet-stub") {
      const thing = await stub("facetdo").facetCapability();
      const sum = await thing.invoke("add", [40, 2]);
      await thing[Symbol.dispose]();
      return json({ sum });
    }

    if (url.pathname === "/transcript") {
      const name = url.searchParams.get("name") || "one";
      return json(await stub(name).transcript());
    }

    // Owner eviction of an idle cell: every later op on the old
    // handles rejects, and streams surface disconnect errors, while the
    // cell itself reactivates and serves fresh handles. The runner
    // passes the node's internal base URL, so this request can evict
    // its own owner halfway through. (celld refuses to evict a cell
    // with work in flight, so an op pending across the eviction
    // instant is not reachable; the reload probe below covers a
    // pending op across a lifecycle change instead.)
    if (url.pathname === "/evict-flow") {
      const internal = url.searchParams.get("internal");
      const name = url.searchParams.get("name") || "seven";
      const scope = "Cap:" + stub(name).id.toString();
      const wire = await stub(name).makeCapability();
      const sum = await wire.capability.invoke("add", [1, 1]);
      const evicted = await (await fetch(`${internal}/evict/${scope}`)).text();
      let laterError = null;
      try {
        await wire.capability.invoke("add", [1, 1]);
      } catch (error) {
        laterError = String(error && error.message || error);
      }
      let streamError = null;
      try {
        await readText(wire.stdout);
      } catch (error) {
        streamError = String(error && error.message || error);
      }
      let writeError = null;
      try {
        const writer = wire.stdin.getWriter();
        await writer.write(utf8.encode("late\n"));
      } catch (error) {
        writeError = String(error && error.message || error);
      }
      // The cell reactivated under a new epoch: plain calls work again,
      // while the old handles stay broken.
      const revived = await stub(name).transcript();
      const fresh = await stub(name).makeCapability();
      const freshSum = await fresh.capability.invoke("add", [2, 3]);
      return json({
        evicted,
        sum,
        laterError,
        streamError,
        writeError,
        revived,
        freshSum,
      });
    }

    // A generation reload revokes every stub the old generation minted:
    // later ops on old handles reject, fresh calls work.
    if (url.pathname === "/reload-flow") {
      const internal = url.searchParams.get("internal");
      const name = url.searchParams.get("name") || "nine";
      const wire = await stub(name).makeCapability();
      const sum = await wire.capability.invoke("add", [1, 1]);
      const reloaded =
        await (await fetch(`${internal}/reload`, { method: "POST" })).text();
      let laterError = null;
      try {
        await wire.capability.invoke("add", [1, 1]);
      } catch (error) {
        laterError = String(error && error.message || error);
      }
      const fresh = await stub(name).makeCapability();
      const freshSum = await fresh.capability.invoke("add", [2, 3]);
      return json({ reloaded, sum, laterError, freshSum });
    }

    // Forward a received host stream back to its owner: the reference
    // moves and the owner reads through the loopback.
    if (url.pathname === "/forward-loopback") {
      const name = url.searchParams.get("name") || "twelve";
      const wire = await stub(name).makeCapability();
      return json(await stub(name).sink(wire.stdout));
    }

    // A Worker-owned readable passed as an argument crosses to the DO
    // through an upload channel now: the Worker pumps, the DO pulls.
    if (url.pathname === "/worker-owned-arg") {
      const name = url.searchParams.get("name") || "thirteen";
      const owned = new ReadableStream({
        start(controller) {
          controller.enqueue(utf8.encode("owned\n"));
          controller.close();
        },
      });
      return json(await stub(name).sink(owned));
    }

    // No reverse path exists for a caller-owned writable: it keeps a
    // same-isolate marker and fails on use across isolates.
    if (url.pathname === "/worker-owned-writable") {
      const name = url.searchParams.get("name") || "thirteen-w";
      const owned = new WritableStream();
      return json(await stub(name).sinkWrite(owned));
    }

    if (url.pathname === "/caller-stats") {
      const hook = globalThis.__celldStubStatsForTestOnly;
      if (typeof hook !== "function") return json({ unavailable: true });
      return json(hook());
    }

    if (url.pathname === "/do-stats") {
      const name = url.searchParams.get("name") || "stats";
      return json(await stub(name).stats());
    }

    if (url.pathname === "/upload-roundtrip") {
      const origin = new ReadableStream({
        start(c) {
          c.enqueue(utf8.encode("up-1\n"));
          c.enqueue(utf8.encode("up-2\n"));
          c.close();
        },
      });
      const echo = await stub("up").sink(origin);
      // The DO cancels after one chunk; producing one more chunk lets
      // the pump's push reject, which cancels the origin.
      let originController = null;
      let firstCancelled = false;
      const firstOrigin = new ReadableStream({
        start(c) {
          originController = c;
          c.enqueue(utf8.encode("first\n"));
        },
        cancel() {
          firstCancelled = true;
        },
      });
      const first = await stub("up").sinkFirst(firstOrigin);
      originController.enqueue(utf8.encode("second\n"));
      for (let i = 0; i < 300 && !firstCancelled; i++) {
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      return json({ echo, first, firstCancelled });
    }

    if (url.pathname === "/fetch-body-roundtrip") {
      const body = new ReadableStream({
        start(c) {
          c.enqueue(utf8.encode("post-body\n"));
          c.close();
        },
      });
      const req = new Request("http://sandbox.internal/run", {
        method: "POST",
        body,
        duplex: "half",
      });
      const res = await stub("fb").echoBody(req);
      const text = await res.text();
      return json({ text });
    }

    // The abort-carrier shape: pass an abort carrier in the options, read
    // the first chunk of a quiet stream, abort mid-exec, then drain the
    // signalled tail and the exit code.
    if (url.pathname === "/carrier-exec") {
      const appSignal = new AbortController();
      let finishCarrier = () => {};
      const abort = new ReadableStream({
        start(c) {
          const onAbort = () => {
            try {
              c.close();
            } catch {}
          };
          appSignal.signal.addEventListener("abort", onAbort, { once: true });
          finishCarrier = () => {
            appSignal.signal.removeEventListener("abort", onAbort);
            try {
              c.close();
            } catch {}
          };
        },
        cancel() {},
      });
      const wire = await stub("carrier").carrierExec(["sleep", "120"], { abort });
      const reader = wire.stdout.getReader();
      const first = await reader.read();
      appSignal.abort();
      const rest = [];
      for (;;) {
        const next = await reader.read();
        if (next.done) break;
        rest.push(utf8d.decode(next.value));
      }
      reader.releaseLock();
      const exitCode = await wire.capability.invoke("exitCode", []);
      const transcript = await stub("carrier").carrierTranscript();
      finishCarrier();
      return json({
        first: first.done ? null : utf8d.decode(first.value),
        rest,
        exitCode,
        transcript,
      });
    }

    if (url.pathname === "/signal") {
      const controller = new AbortController();
      const probe = await stub("sig").signalProbe(controller.signal);
      controller.abort(new Error("test-abort"));
      let state = null;
      for (let i = 0; i < 300; i++) {
        state = await stub("sig").signalState();
        if (state.aborted === true) break;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      return json({ probe, state });
    }

    // Dispose mid-drive: the op provably validates+pins owner-side (the
    // arrival flag) before the release routes, so it still completes.
    if (url.pathname === "/pin-race") {
      const wire = await stub("pin").makeCapability();
      const pending = wire.capability.invoke("slowAdd", [40, 2, 1500]);
      let arrived = false;
      for (let i = 0; i < 300 && !arrived; i++) {
        const t = await stub("pin").transcript();
        arrived = t.slowArrived === true;
        if (!arrived) await new Promise((resolve) => setTimeout(resolve, 10));
      }
      await wire[Symbol.dispose]();
      const sum = await pending;
      return json({ arrived, sum });
    }

    // Two concurrent stub ops against one dead-entry stub: the first
    // drive's end must not dispose the still-running sibling's nested
    // revives. A is arrival-flagged and finishes first; B calls out to
    // a sibling and returns the nested child, whose reply lift must
    // still move it out.
    if (url.pathname === "/concurrent-drives") {
      const wire = await stub("r1").makeCapability();
      const pendingA = wire.capability.invoke("slowAdd", [40, 2, 800]);
      let arrived = false;
      for (let i = 0; i < 300 && !arrived; i++) {
        const t = await stub("r1").transcript();
        arrived = t.slowArrived === true;
        if (!arrived) await new Promise((resolve) => setTimeout(resolve, 10));
      }
      const wire2 = await stub("r1").makeCapability();
      const pendingB =
        wire.capability.invoke("slowNestedEcho", [wire2.capability, 2000]);
      const sumA = await pendingA;
      let sumB = null;
      let errorB = null;
      try {
        const echoed = await pendingB;
        sumB = await echoed.invoke("add", [40, 2]);
      } catch (error) {
        errorB = String((error && error.message) || error);
      }
      return json({ arrived, sumA, sumB, errorB });
    }

    // Re-entrant concurrent stub ops sharing one live entry context:
    // the owner lends a capability to a sibling mid-invocation and the
    // sibling drives it concurrently (see raceLent).
    if (url.pathname === "/reentrant-drives") {
      return json(await stub("s1owner").lendAndAwait("s1peer"));
    }

    // A quiet handle survives across unrelated activity: no timer, no
    // use-count, no expiry.
    if (url.pathname === "/quiet-across-activity") {
      const quiet = await stub("quiet").makeCapability();
      for (let i = 0; i < 20; i++) {
        const busy = await stub("quiet").makeCapability();
        await busy.capability.invoke("add", [i, 1]);
        await busy[Symbol.dispose]();
      }
      const sum = await quiet.capability.invoke("add", [40, 2]);
      return json({ sum });
    }

    if (url.pathname === "/caller-abort") {
      const wire = await stub("cabort").makeCapability();
      const sum = await wire.capability.invoke("add", [1, 1]);
      const slow = wire.capability.invoke("slowAdd", [1, 2, 5000]);
      ctx.abort(new Error("caller-boom"));
      let slowError = null;
      try {
        await slow;
      } catch (error) {
        slowError = String((error && error.message) || error);
      }
      let after = null;
      try {
        await wire.capability.invoke("add", [1, 1]);
      } catch (error) {
        after = String((error && error.message) || error);
      }
      return json({ sum, slowError, after });
    }

    // Forged markers fail closed with clear errors: never a hang, never
    // a crash, never another handle's data. Each forged marker travels
    // with a real stream witness, so the tree takes the lift path
    // instead of crossing as plain data -- and the reserved-key
    // refusal (0005, every lift path including host lifts) rejects the
    // call with DataCloneError before anything revives or dispatches.
    if (url.pathname === "/malformed") {
      const badStub = {
        __celld$stub: "!!!not-hex!!!",
        t: "forged",
        h: 1,
        scope: "Cap:forged",
        epoch: 1,
        gen: 1,
      };
      const witness = new ReadableStream({
        start(c) {
          c.enqueue(utf8.encode("witness\n"));
          c.close();
        },
      });
      let bad = null;
      try {
        await stub("mal").invokeForeign(badStub, witness);
        bad = { threw: false };
      } catch (error) {
        bad = {
          threw: true,
          name: error.name,
          error: String((error && error.message) || error),
        };
      }
      const badUp = { __celld$rs: "!!!not-hex!!!", t: "forged", h: 2 };
      const realUp = new ReadableStream({
        start(c) {
          c.enqueue(utf8.encode("real\n"));
          c.close();
        },
      });
      let badStream = null;
      try {
        await stub("mal").sinkMany([badUp, realUp]);
        badStream = { threw: false };
      } catch (error) {
        badStream = {
          threw: true,
          name: error.name,
          error: String((error && error.message) || error),
        };
      }
      const wire = await stub("mal").makeCapability();
      let node = wire.capability;
      for (let i = 0; i < 40; i++) node = node[`p${i}`];
      let deepError = null;
      try {
        await node;
      } catch (error) {
        deepError = String((error && error.message) || error);
      }
      await wire[Symbol.dispose]();
      return json({ bad, badStream, deepError });
    }

    // 0005+v2: a forged `__celld$` key in DO RPC args is refused at
    // lift even when live host values travel beside it -- a
    // Worker-owned readable (upload channel) and a received host stub
    // being forwarded. The refusal names the reserved key.
    if (url.pathname === "/forged-upload") {
      const owned = new ReadableStream({
        start(c) {
          c.enqueue(utf8.encode("owned\n"));
          c.close();
        },
      });
      const forgedRs = { __celld$rs: "0".repeat(32), t: "forged", h: 2 };
      let besideUpload = null;
      try {
        await stub("forge").sinkMany([owned, forgedRs]);
        besideUpload = { threw: false };
      } catch (error) {
        besideUpload = {
          threw: true,
          name: error.name,
          error: String((error && error.message) || error),
        };
      }
      const wire = await stub("forge").makeCapability();
      const forgedStub = {
        __celld$stub: "0".repeat(32),
        t: "forged",
        h: 1,
        scope: "Cap:forged",
        epoch: 1,
        gen: 1,
      };
      let besideHost = null;
      try {
        await stub("forge").sinkMany([wire.stdout, forgedStub]);
        besideHost = { threw: false };
      } catch (error) {
        besideHost = {
          threw: true,
          name: error.name,
          error: String((error && error.message) || error),
        };
      }
      await wire[Symbol.dispose]();
      return json({ besideUpload, besideHost });
    }

    // Stub ids never serialize observably: no 32-hex anywhere a script
    // can read (stringified handles, error texts), over stub or upload
    // surfaces.
    if (url.pathname === "/no-id-leak") {
      const wire = await stub("leak").makeCapability();
      const parts = [];
      parts.push(String(JSON.stringify(wire.capability)));
      parts.push(String(JSON.stringify(wire.stdout)));
      const owned = new ReadableStream({
        start(c) {
          c.enqueue(utf8.encode("owned\n"));
          c.close();
        },
      });
      const uploadProbe = await stub("leak").uploadLeakProbe(owned);
      parts.push(...uploadProbe.parts);
      try {
        parts.push(String(wire.capability));
      } catch (error) {
        parts.push(`threw:${String((error && error.message) || error)}`);
      }
      try {
        await wire.capability.invoke("missing", [wire.stdout]);
      } catch (error) {
        parts.push(String((error && error.message) || error));
      }
      const leaked = parts.some((part) => /[0-9a-f]{32}/.test(part));
      await wire[Symbol.dispose]();
      return json({ leaked, parts });
    }

    // Churn capabilities without disposing them. With `dispose=1` every
    // iteration releases its tree and the host table stays flat;
    // otherwise the request end disposes every held handle and the
    // releases drain the table right after the response.
    if (url.pathname === "/churn") {
      const count = Number(url.searchParams.get("n") || "10");
      const dispose = url.searchParams.get("dispose") === "1";
      for (let i = 0; i < count; i++) {
        const wire = await stub("eight").makeCapability();
        await readText(wire.stdout);
        if (dispose) await wire[Symbol.dispose]();
      }
      return json({ count, dispose });
    }

    // Settle handle-bearing calls after response end: waitUntil-float
    // them (a plain float is cancelled with the request) and delay the
    // reply past response end, so the revives land after the fetch
    // context ended. Post-end revives must not register into the dead
    // context: the host table, the owner maps, and the caller handles
    // all still drain. A chained stream argument lifts after response
    // end too: no live sender context owns its pump, so the lift keeps
    // a same-isolate marker and the callee observes the foreign-stream
    // error instead of a channel.
    if (url.pathname === "/float-past-end") {
      const count = Number(url.searchParams.get("n") || "5");
      const slow = Number(url.searchParams.get("slow") || "500");
      const late = new ReadableStream({
        start(c) {
          c.enqueue(utf8.encode("late\n"));
          c.close();
        },
      });
      for (let i = 0; i < count; i++) {
        const reply = stub("float").slowBox(slow);
        // Handles revived after response end arrive disposed: the first
        // float's use must fail locally with no owner drive. Awaited, so
        // the waitUntil covers the use either way.
        const settled = reply.then(
          async (wire) => {
            if (i === 0) {
              await wire.capability
                .invoke("exitCode", [])
                .then(
                  () => {},
                  () => {},
                );
            }
          },
          () => {},
        );
        ctx.waitUntil(settled);
        // The first reply chains a stream argument, so its lift also
        // lands after response end.
        if (i === 0) {
          const chained = reply
            .then(() => stub("float").sink(late))
            .then(
              () => {},
              () => {},
            );
          ctx.waitUntil(chained);
        }
      }
      return json({ floated: count });
    }

    // Fill the scope mint cap (tiny on the caps node) and hold: the
    // next export fails the call with a clear error, strands nothing,
    // and the held wires still drain at request end.
    if (url.pathname === "/caps-stub") {
      const held = [];
      let error = null;
      try {
        for (let i = 0; i < 100; i++) {
          held.push(await stub("cap").makeCapability());
        }
      } catch (e) {
        error = String((e && e.message) || e);
      }
      return json({ held: held.length, error });
    }

    if (url.pathname === "/caps-upload") {
      const streams = [];
      for (let i = 0; i < 8; i++) {
        streams.push(
          new ReadableStream({
            start(c) {
              c.enqueue(utf8.encode(`s${i}\n`));
              c.close();
            },
          }),
        );
      }
      let error = null;
      try {
        await stub("capup").sinkMany(streams);
      } catch (e) {
        error = String((e && e.message) || e);
      }
      return json({ error });
    }

    // A self-fetch target that calls stub ops and uses the revived
    // handles inline: under a dead scope they arrive disposed and every
    // use fails; under a live one they work.
    if (url.pathname === "/self-fetch-inner") {
      const wire = await stub("self").makeCapability();
      const sum = await wire.capability.invoke("add", [40, 2]);
      const text = await readText(wire.stdout);
      return json({ sum, text });
    }

    // Fetch the worker's own handler through a same-script service
    // binding: stubs and streams the inner handler returns through
    // use must be usable, not disposed-on-arrival.
    if (url.pathname === "/self-fetch-caps") {
      try {
        const res = await env.SELF.fetch("http://internal/self-fetch-inner");
        const body = await res.json();
        return json({ status: res.status, body });
      } catch (error) {
        return json({
          threw: true,
          error: String((error && error.message) || error),
        });
      }
    }

    // Drive a throwing loopback entrypoint op: it aborts its own
    // context, so the op rejects and must leave no live context.
    if (url.pathname === "/entrypoint-op-abort") {
      let error = null;
      try {
        await ctx.exports.default.entryOpProbe({ mode: "abort" });
      } catch (caught) {
        error = String((caught && caught.message) || caught);
      }
      return json({ error });
    }

    if (url.pathname === "/scope") {
      const name = url.searchParams.get("name") || "one";
      return json({ scope: "Cap:" + stub(name).id.toString() });
    }

    return new Response("not found", { status: 404 });
  },
};
