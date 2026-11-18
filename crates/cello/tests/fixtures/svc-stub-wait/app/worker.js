// Modified by ryuzcorp (ryuzcorp/celld): cross-event retained-wait fixture. Apache-2.0.
const J = (o, st = 200) => Response.json(o, { status: st });

export default {
  async fetch(req, env) {
    const t = new URL(req.url).pathname.slice(1);
    const run = async () => {
      if (t === 'health') return { ok: true };
      const stub = await env.SB.getWaitStub(`wait-${t}`);
      switch (t) {
        // The wait fires before the capability method runs: the
        // awaiting event subscribes after the value resolved.
        case 'wait-after-fire': {
          const { cap, id } = await stub.makeWaiter('w-after');
          await stub.fireWait(id, 'three');
          return { value: await cap.invoke('wait') };
        }
        // The capability method runs before the wait fires: the
        // promise resolves after the call.
        case 'wait-before-fire': {
          const { cap, id } = await stub.makeWaiter('w-before');
          const pending = cap.invoke('wait');
          await stub.fireWait(id, 'three');
          return { value: await pending };
        }
        // Control: an already-resolved promise through the same shape.
        case 'wait-control': {
          const { cap } = await stub.makeWaiter('w-control');
          return { value: await cap.invoke('control') };
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
