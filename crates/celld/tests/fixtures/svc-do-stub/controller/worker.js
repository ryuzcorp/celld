// Modified by ryuzcorp (ryuzcorp/celld): R7 service-RPC DO-stub fixture. Apache-2.0.
import { DurableObject, RpcTarget, WorkerEntrypoint } from 'cloudflare:workers';

class ControllerCap extends RpcTarget {
  async hello() { return 'controller-hello'; }
}

// A process-capability shape: exitCode/kill/resize over invoke().
class ExecCapability extends RpcTarget {
  constructor(state) {
    super();
    this.state = state;
  }
  async invoke(operation, args) {
    void args;
    if (operation === 'exitCode') {
      this.state.exitCodeCalls++;
      return this.state.exitCode;
    }
    if (operation === 'kill') {
      this.state.killed = true;
      this.state.exitCode = 137;
      return true;
    }
    if (operation === 'resize') {
      this.state.resized.push(args);
      return true;
    }
    throw new TypeError(`unknown operation ${operation}`);
  }
}

const utf8 = new TextEncoder();
const utf8d = new TextDecoder();

export class ControllerDO extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.writes = [];
    this.stdinClosed = false;
    this.carrierText = null;
    this.signalAborted = null;
    this.exitCode = 0;
    this.exitCodeCalls = 0;
    this.killed = false;
    this.resized = [];
  }
  async ping(x) { return `pong:${x}`; }
  // The managed-sandbox hop: the app fetched this stub over the
  // service binding (0005) and now invokes exec over DO RPC (v2). A
  // PTY-shaped return: process stub + byte streams, with the caller's
  // abort carrier arriving as an upload arg.
  async exec(argv, options) {
    const { abort, signal } = options ?? {};
    if (abort !== undefined) {
      if (!(abort instanceof ReadableStream)) {
        throw new TypeError('Invalid Sandbox cancellation stream');
      }
      this.carrierText = await new Response(abort).text();
    }
    if (signal instanceof AbortSignal) {
      this.signalAborted = signal.aborted;
      if (!signal.aborted) {
        const state = this;
        signal.addEventListener('abort', () => {
          state.signalAborted = true;
        }, { once: true });
      }
    }
    const state = this;
    void argv;
    return {
      pid: 4242,
      isPty: true,
      stdin: new WritableStream({
        write(chunk) { state.writes.push(utf8d.decode(chunk)); },
        close() { state.stdinClosed = true; },
      }),
      stdout: new ReadableStream({
        start(c) {
          c.enqueue(utf8.encode('pty-1\n'));
          c.enqueue(utf8.encode('pty-2\n'));
          c.close();
        },
      }),
      stderr: new ReadableStream({
        start(c) {
          c.enqueue(utf8.encode('pty-err\n'));
          c.close();
        },
      }),
      capability: new ExecCapability(this),
    };
  }
  async transcript() {
    return {
      writes: this.writes,
      stdinClosed: this.stdinClosed,
      carrierText: this.carrierText,
      signalAborted: this.signalAborted,
      exitCodeCalls: this.exitCodeCalls,
      killed: this.killed,
      resized: this.resized,
    };
  }
}

export default class ControllerService extends WorkerEntrypoint {
  async getControllerStub(id) {
    return this.env.SANDBOX.getByName(id);
  }
  async pingBack(stub) {
    return { pong: await stub.ping('via-controller') };
  }
  async getCap() {
    return new ControllerCap();
  }
  async takeCap(cap) {
    return { hello: await cap.hello() };
  }
  async getStream() {
    return new ReadableStream({
      start(c) { c.enqueue(new TextEncoder().encode('x')); c.close(); },
    });
  }
  async takeStream(s) {
    const r = s.getReader();
    const out = await r.read();
    return { done: !!out.done };
  }
  async takeTwo(a, b) {
    return 'reached';
  }
  async inspectArg(x) {
    return { hasPing: typeof x?.ping };
  }
  // D3: the receiver reads the revived stub's name, which comes from the
  // lift marker's `n`. A sender-side `stub.name =` must not change it.
  async echoName(stub) {
    return { name: stub.name ?? null };
  }
}
