// Modified by ryuzcorp (ryuzcorp/celld): abort/drain conformance fixture. Apache-2.0.
import { DurableObject, RpcStub, WorkerEntrypoint } from 'cloudflare:workers';

const J = (o, st = 200) => Response.json(o, { status: st });
const te = new TextEncoder();

// Published through the isolate's module state: the reset cell shares a
// bound abort, and the caller cell wraps a target closing over it in a
// loopback stub owned by its own invocation. The invocation runs in the
// caller's turn, so the abort below runs under a same-isolate stub caller.
let publishedAbort = null;

export class Reset extends DurableObject {
  async publish() {
    const ctx = this.ctx;
    publishedAbort = (reason) => ctx.abort(new Error(reason));
    return 'published';
  }
  async ping() {
    return 'pong';
  }
}

export class Caller extends DurableObject {
  async run() {
    await this.env.RESET.getByName('r').publish();
    const abort = publishedAbort;
    const state = this.ctx;
    // One stub-mediated op whose walk queues this cell's put and aborts
    // the reset cell synchronously: the put is still queued (its flush
    // runs in a later microtask) when the abort drops queues.
    const stub = new RpcStub({
      go() {
        state.storage.put('ck', 'cv');
        abort('reset-by-test');
      },
    });
    let abortError = null;
    try {
      await stub.go();
    } catch (error) {
      abortError = `${error.name}: ${error.message}`;
    }
    const kept = await this.ctx.storage.get('ck');
    let resetState;
    try {
      resetState = await this.env.RESET.getByName('r').ping();
    } catch (error) {
      resetState = `${error.name}: ${error.message}`;
    }
    return { abortError, kept, resetState };
  }
}

export class Svc extends WorkerEntrypoint {
  // A capability-bearing reply (stream plus stub) whose stream is fed by
  // waitUntil work that settles on first pull: the reply must not wait
  // for that work, which cannot finish until the caller pulls the stream.
  async streamAndStub() {
    let release;
    const pulled = new Promise((resolve) => {
      release = resolve;
    });
    const stream = new ReadableStream({
      start(c) {
        c.enqueue(te.encode('chunk'));
      },
      pull(c) {
        release();
        c.close();
      },
    });
    this.ctx.waitUntil(pulled);
    return { stream, stub: this.env.RESET.getByName('s') };
  }
  // Control: the same shape without the stub (a bare capability reply).
  async streamOnly() {
    let release;
    const pulled = new Promise((resolve) => {
      release = resolve;
    });
    const stream = new ReadableStream({
      start(c) {
        c.enqueue(te.encode('chunk'));
      },
      pull(c) {
        release();
        c.close();
      },
    });
    this.ctx.waitUntil(pulled);
    return { stream };
  }
}

const readAll = async (stream) => {
  const reader = stream.getReader();
  const chunks = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  const total = chunks.reduce((n, c) => n + c.byteLength, 0);
  const out = new Uint8Array(total);
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.byteLength;
  }
  return new TextDecoder().decode(out);
};

export default {
  async fetch(req, env, ctx) {
    const t = new URL(req.url).pathname.slice(1);
    const run = async () => {
      if (t === 'health') return { ok: true };
      if (t === 'reset-caller-put') return env.CALLER.getByName('c').run();
      if (t === 'stream-and-stub') {
        const { stream, stub } = await ctx.exports.Svc.streamAndStub();
        const text = await readAll(stream);
        return { text, ping: await stub.ping() };
      }
      if (t === 'stream-only') {
        const { stream } = await ctx.exports.Svc.streamOnly();
        return { text: await readAll(stream) };
      }
      return { error: `unknown ${t}` };
    };
    try {
      return J({ test: t, ok: true, result: await run() });
    } catch (e) {
      return J({ test: t, ok: false, error: `${e.name}: ${e.message}` });
    }
  },
};
