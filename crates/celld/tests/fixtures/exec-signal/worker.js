// Modified by ryuzcorp (ryuzcorp/celld): exec-signal conformance fixture; exec kill delivery fixture. Apache-2.0.
const J = (o, st = 200) => Response.json(o, { status: st });

let nextScope = 0;

export default {
  async fetch(req) {
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
