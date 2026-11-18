// Modified by ryuzcorp (ryuzcorp/celld): exec-carrier conformance fixture. Apache-2.0.
import { DurableObject } from 'cloudflare:workers';

const J = (o, st = 200) => Response.json(o, { status: st });

// The carrier a caller sends where an AbortSignal cannot cross RPC: an
// open ReadableStream the sender closes to abort. The receiver decodes it
// back into a signal and wires that signal to its exec.
const decodeCarrier = (abort) => {
  const controller = new AbortController();
  if (abort === undefined) return { signal: undefined, finish() {} };
  if (!(abort instanceof ReadableStream)) {
    throw new TypeError('Invalid cancellation stream.');
  }
  const reader = abort.getReader();
  let finished = false;
  const onCarrier = () => {
    if (!finished) controller.abort();
  };
  reader.read().then(onCarrier, onCarrier);
  return {
    signal: controller.signal,
    finish() {
      finished = true;
      reader.cancel().catch(() => {});
    },
  };
};

// A cell-side exec host: starts a fake exec under the decoded carrier
// signal and holds the process across calls, so the carrier must stay
// open past the call that delivered it.
export class CarrierDO extends DurableObject {
  async startExec(scope, options) {
    const test = globalThis.__celldTestExecForTestOnly;
    test.attach(scope);
    const decoded = decodeCarrier(options?.abort);
    const proc = await test.exec(
      scope,
      ['sleep', '30'],
      decoded.signal === undefined ? undefined : { signal: decoded.signal },
    );
    const ids = test.ids(scope);
    this._run = { proc, id: ids[ids.length - 1], finish: decoded.finish };
    return { started: true };
  }

  async statusExec() {
    const test = globalThis.__celldTestExecForTestOnly;
    return { kills: test.kills(this._run.id) };
  }

  async finishExec(code) {
    const test = globalThis.__celldTestExecForTestOnly;
    test.exit(this._run.id, code);
    const exitCode = await this._run.proc.exitCode;
    this._run.finish();
    return { exitCode, kills: test.kills(this._run.id) };
  }
}

// The abort phase runs in one request: the sender's invocation end also
// terminates the channel, so a split start/close would let request
// cleanup abort the exec and the test would no longer prove that
// `close()` is what kills it.

const openCarrier = () => {
  let close = () => {};
  const abort = new ReadableStream({
    start(c) {
      close = () => {
        try {
          c.close();
        } catch {}
      };
    },
    cancel() {},
  });
  return { abort, close: () => close() };
};

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const t = url.pathname.slice(1);
    const scope = `exec-carrier:${url.searchParams.get('scope') ?? t}`;
    const run = async () => {
      if (t === 'health') return { ok: true };
      const test = globalThis.__celldTestExecForTestOnly;
      if (test === undefined) {
        throw new Error('test exec hooks missing (boot without CELLD_TEST_WAIT=1?)');
      }
      const stub = env.CARRIER_DO.get(env.CARRIER_DO.idFromName(scope));
      switch (t) {
        // A signal-carrying exec the sender never aborts: the carrier
        // stays open across the start/status/finish calls, so the
        // process sleeps undisturbed and exits normally with no kill.
        case 'no-abort': {
          const { abort } = openCarrier();
          await stub.startExec(scope, { abort });
          const mid = await stub.statusExec();
          const done = await stub.finishExec(0);
          return { midKills: mid.kills, ...done };
        }
        // The same exec, aborted for real, all within one sender request:
        // no kill before the close, SIGKILL after it.
        case 'abort': {
          const carrier = openCarrier();
          await stub.startExec(scope, { abort: carrier.abort });
          const before = await stub.statusExec();
          carrier.close();
          const deadline = Date.now() + 20_000;
          let after = await stub.statusExec();
          while (after.kills.length === 0 && Date.now() < deadline) {
            after = await stub.statusExec();
          }
          const done = await stub.finishExec(137);
          return { beforeKills: before.kills, afterKills: after.kills, ...done };
        }
        default:
          return { error: `unknown ${t}` };
      }
    };
    try {
      return J({ test: t, ok: true, result: await run() });
    } catch (e) {
      return J({ test: t, ok: false, error: `${e.name}: ${e.message}` });
    }
  },
};
