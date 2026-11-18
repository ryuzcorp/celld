// Modified by ryuzcorp (ryuzcorp/celld): exec-signal conformance fixture; exec kill delivery fixture; close/signal delivery fixture; failed-event signal fixture. Apache-2.0.
import { DurableObject } from 'cloudflare:workers';

const J = (o, st = 200) => Response.json(o, { status: st });

let nextScope = 0;

// A cell fetch event that signals and then fails: the signal op is
// already enqueued when the throw rejects the event, so only a
// lifetime that survives failed cell events delivers it. (An RPC
// method will not do: the RPC layer answers throws as error
// envelopes, so the event succeeds.)
export class SignalDO extends DurableObject {
  async fetch() {
    const test = globalThis.__celldTestExecForTestOnly;
    const fixed = 'signal-fail';
    test.attach(fixed);
    test.signal(fixed, 15);
    throw new Error('signal-fail boom');
  }
}

export default {
  async fetch(req, env) {
    const t = new URL(req.url).pathname.slice(1);
    const run = async () => {
      if (t === 'health') return { ok: true };
      const test = globalThis.__celldTestExecForTestOnly;
      if (test === undefined)
        throw new Error('test exec hooks missing (boot without CELLD_TEST_WAIT=1?)');
      // A fresh scope per request, so fake processes never leak across cases.
      const scope = `exec-signal:${t}:${nextScope++}`;
      test.attach(scope);
      switch (t) {
        // Control: an exec without a signal through the same fake path
        // settles, proving the fake engine itself is sound.
        case 'control': {
          const proc = await test.exec(scope, ['echo', 'hi']);
          const [id] = test.ids(scope);
          test.exit(id, 3);
          return {
            exitCode: await proc.exitCode,
            kills: test.kills(id),
            params: test.params(id),
          };
        }
        // An already-aborted signal refuses with its reason before
        // spawning anything: an AbortError for abort(), the custom
        // reason for abort(reason).
        case 'already-aborted': {
          const before = test.ids(scope).length;
          const plain = new AbortController();
          plain.abort();
          let err1;
          try {
            await test.exec(scope, ['sh'], { signal: plain.signal });
          } catch (e) { err1 = e; }
          const reason = new Error('stop-it');
          const custom = new AbortController();
          custom.abort(reason);
          let err2;
          try {
            await test.exec(scope, ['sh'], { signal: custom.signal });
          } catch (e) { err2 = e; }
          return {
            defaultName: err1?.name,
            customSame: err2 === reason,
            customMessage: err2?.message,
            spawned: test.ids(scope).length - before,
          };
        }
        // A non-AbortSignal signal is rejected; null is absent.
        case 'signal-type': {
          let invalid;
          try {
            await test.exec(scope, ['sh'], { signal: 123 });
          } catch (e) { invalid = `${e.name}: ${e.message}`; }
          const proc = await test.exec(scope, ['sh'], { signal: null });
          const [id] = test.ids(scope);
          test.exit(id, 0);
          return { invalid, exitCode: await proc.exitCode };
        }
        // Aborting after the process exists kills it with SIGKILL, and
        // the exit the engine then reports settles exitCode.
        case 'abort-after-start': {
          const controller = new AbortController();
          const proc = await test.exec(scope, ['sleep', '30'], { signal: controller.signal });
          const [id] = test.ids(scope);
          const killsBefore = test.kills(id);
          controller.abort();
          const killsAfter = test.kills(id);
          test.exit(id, 137);
          return {
            killsBefore,
            killsAfter,
            params: test.params(id),
            exitCode: await proc.exitCode,
          };
        }
        // An abort landing while the exec op is in flight kills the
        // fresh process at once instead of registering a listener.
        case 'abort-during-flight': {
          const controller = new AbortController();
          const pending = test.exec(scope, ['sleep', '30'], { signal: controller.signal });
          controller.abort();
          const proc = await pending;
          const [id] = test.ids(scope);
          test.exit(id, 137);
          return { kills: test.kills(id), exitCode: await proc.exitCode };
        }
        // The process exits first: the abort listener is gone, so a
        // later abort sends no kill to the dead process.
        case 'exit-before-abort': {
          const controller = new AbortController();
          const proc = await test.exec(scope, ['true'], { signal: controller.signal });
          const [id] = test.ids(scope);
          test.exit(id, 0);
          const exitCode = await proc.exitCode;
          controller.abort();
          return { exitCode, kills: test.kills(id) };
        }
        // Fire-and-forget kill delivery: kill() returns void, so the
        // handler answers in the same turn without awaiting anything.
        // A fixed scope carries the exec across the two requests.
        case 'kill-delivery': {
          const fixed = 'kill-delivery';
          test.attach(fixed);
          const proc = await test.exec(fixed, ['sleep', '30']);
          const [id] = test.ids(fixed);
          // Fire and forget: the handler answers in this same turn,
          // awaiting nothing after the kill.
          proc.kill(9);
          return { id };
        }
        case 'kill-delivery-status': {
          const fixed = 'kill-delivery';
          const [id] = test.ids(fixed);
          return { completed: test.killsCompleted(id) };
        }
        // Stdin-close delivery: the default stdin:none path fires
        // __container_exec_close without awaiting it (there is no
        // handle to await), and the handler answers in the same turn.
        // A fixed scope carries the exec across the two requests.
        case 'close-delivery': {
          const fixed = 'close-delivery';
          test.attach(fixed);
          await test.exec(fixed, ['cat']);
          const [id] = test.ids(fixed);
          return { id, dispatched: test.closes(id) };
        }
        case 'close-delivery-status': {
          const fixed = 'close-delivery';
          const [id] = test.ids(fixed);
          return {
            dispatched: test.closes(id),
            completed: test.closesCompleted(id),
          };
        }
        // Container-signal delivery: signal() returns void, so the
        // handler answers in the same turn without awaiting anything.
        // A fixed scope carries the signals across the two requests.
        case 'signal-delivery': {
          const fixed = 'signal-delivery';
          test.attach(fixed);
          test.signal(fixed, 15);
          return { dispatched: test.signals(fixed) };
        }
        case 'signal-delivery-status': {
          const fixed = 'signal-delivery';
          return {
            dispatched: test.signals(fixed),
            completed: test.signalsCompleted(fixed),
          };
        }
        // Failed-event signal delivery: the DO fetch signals and then
        // throws, so its cell event fails with the signal op pending.
        // A fixed scope carries the signals across the two requests.
        case 'signal-fail': {
          const stub = env.SIGNAL_DO.get(env.SIGNAL_DO.idFromName('signal-fail'));
          let threw = false;
          try {
            await stub.fetch('https://signal-fail.invalid/');
          } catch { threw = true; }
          return { threw, dispatched: test.signals('signal-fail') };
        }
        case 'signal-fail-status': {
          return {
            dispatched: test.signals('signal-fail'),
            completed: test.signalsCompleted('signal-fail'),
          };
        }
        default: return { error: `unknown ${t}` };
      }
    };
    try {
      return J({ test: t, ok: true, result: await run() });
    } catch (e) {
      return J({ test: t, ok: false, error: `${e.name}: ${e.message}` });
    }
  },
};
