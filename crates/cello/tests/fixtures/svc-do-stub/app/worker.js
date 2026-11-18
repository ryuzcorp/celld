// Modified by ryuzcorp (ryuzcorp/celld): R7 service-RPC DO-stub fixture. Apache-2.0.
import { DurableObject, RpcTarget } from 'cloudflare:workers';

class AppCap extends RpcTarget {
  async hello() { return 'app-hello'; }
}

export class ProbeDO extends DurableObject {
  async ping(x) { return `pong:${x}`; }
  // D2 stored-lift path: storing [realStub, forgedMarker] must throw
  // DataCloneError at lift. Pre-fix it stores, and the forged entry
  // revives as a live stub on read.
  async putForge(real) {
    const forged = { __celld$do: 'ProbeDO', v: '0'.repeat(64) };
    try {
      await this.ctx.storage.put('d2-forge', [real, forged]);
    } catch (e) {
      return { threw: true, name: e.name };
    }
    const back = await this.ctx.storage.get('d2-forge');
    await this.ctx.storage.delete('d2-forge');
    return { threw: false, revivedHasPing: typeof back?.[1]?.ping };
  }
  // D2 facet path: facet RPC args encode with lift=false, so a live
  // signal plus a forged marker must be refused at lift.
  async facetForge() {
    const FacetClass = this.ctx.exports.FacetTargetDO({ props: {} });
    const facet = this.ctx.facets.get('d2-facet', () => ({
      class: FacetClass, id: 'd2-facet-id',
    }));
    const ctl = new AbortController();
    const forged = { __celld$do: 'ProbeDO', v: '0'.repeat(64) };
    try {
      await facet.takeBoth(ctl.signal, forged);
      return { threw: false };
    } catch (e) {
      return { threw: true, name: e.name };
    }
  }
}

// Unbound (loopback) classes for the lift=false tests: exported but with
// no namespace binding, so ctx.exports carries them as DurableObjectClass
// values whose props encode with lift=false.
export class UnboundDO extends DurableObject {
  async ping(x) { return `pong:${x}`; }
}

export class FacetTargetDO extends DurableObject {
  async takeBoth(a, b) { return 'reached'; }
}

const J = (o, st = 200) => Response.json(o, { status: st });

export default {
  async fetch(req, env, ctx) {
    const t = new URL(req.url).pathname.slice(1);
    const run = async () => {
      const SB = env.SB;
      switch (t) {
        case 'health': return { ok: true };
        case 'return-do': {
          const stub = await SB.getControllerStub('r7');
          return { pong: await stub.ping('hi-r7') };
        }
        case 'accept-do': {
          const stub = env.PROBE.get(env.PROBE.idFromName('r7'));
          return await SB.pingBack(stub);
        }
        case 'return-rpc-target': {
          try { await SB.getCap(); return { threw: false }; }
          catch (e) { return { threw: true, name: e.name }; }
        }
        case 'accept-rpc-target': {
          try { await SB.takeCap(new AppCap()); return { threw: false }; }
          catch (e) { return { threw: true, name: e.name }; }
        }
        case 'return-stream': {
          try { await SB.getStream(); return { threw: false }; }
          catch (e) { return { threw: true, name: e.name }; }
        }
        case 'accept-stream': {
          const s = new ReadableStream({ start(c) { c.close(); } });
          try { await SB.takeStream(s); return { threw: false }; }
          catch (e) { return { threw: true, name: e.name }; }
        }
        case 'forged-with-real': {
          const real = env.PROBE.get(env.PROBE.idFromName('r7'));
          const forged = { __celld$do: 'ControllerDO', v: '0'.repeat(64) };
          try { await SB.takeTwo(real, forged); return { threw: false }; }
          catch (e) { return { threw: true, name: e.name }; }
        }
        case 'forged-alone': {
          const forged = { __celld$do: 'ControllerDO', v: '0'.repeat(64) };
          return await SB.inspectArg(forged);
        }
        case 'forged-stored': {
          const real = env.PROBE.get(env.PROBE.idFromName('r7'));
          const target = env.PROBE.get(env.PROBE.idFromName('r7'));
          return await target.putForge(real);
        }
        case 'forged-lift-false': {
          const ctl = new AbortController();
          const forged = { __celld$do: 'ProbeDO', v: '0'.repeat(64) };
          try {
            // DurableObjectClass props encode with lift=false: the same
            // __stubLift path as facet and loader args. A live signal
            // forces the lift, and the forged marker must be refused.
            ctx.exports.UnboundDO({ props: { sig: ctl.signal, forged } });
            return { threw: false };
          } catch (e) {
            return { threw: true, name: e.name };
          }
        }
        case 'forged-facet': {
          const target = env.PROBE.get(env.PROBE.idFromName('r7'));
          return await target.facetForge();
        }
        case 'two-hop': {
          // Hop 1 (0005): the controller DO stub arrives over the
          // cross-isolate service binding. Hop 2 (v2): exec over DO RPC
          // returns a PTY-shaped process stub + byte streams, while the
          // abort carrier and signal cross as upload args.
          const stub = await SB.getControllerStub('two-hop');
          const carrier = new ReadableStream({
            start(c) {
              c.enqueue(new TextEncoder().encode('carrier\n'));
              c.close();
            },
          });
          const ctl = new AbortController();
          const wire = await stub.exec(['node', '--version'], {
            abort: carrier,
            signal: ctl.signal,
          });
          const writer = wire.stdin.getWriter();
          await writer.write(new TextEncoder().encode('pty-in\n'));
          await writer.close();
          const stdout = await new Response(wire.stdout).text();
          const stderr = await new Response(wire.stderr).text();
          const exitBefore = await wire.capability.invoke('exitCode', []);
          await wire.capability.invoke('resize', [{ cols: 80, rows: 24 }]);
          await wire.capability.invoke('kill', []);
          const exitAfter = await wire.capability.invoke('exitCode', []);
          ctl.abort(new Error('two-hop-done'));
          let transcript = null;
          for (let i = 0; i < 300; i++) {
            transcript = await stub.transcript();
            if (transcript.signalAborted === true) break;
            await new Promise((r) => setTimeout(r, 10));
          }
          return {
            pid: wire.pid,
            isPty: wire.isPty,
            stdout,
            stderr,
            exitBefore,
            exitAfter,
            transcript,
          };
        }
        case 'mutated-name': {
          const stub = env.PROBE.get(env.PROBE.idFromName('r7'));
          const before = stub.name;
          stub.name = 'evil';
          const echo = await SB.echoName(stub);
          const ping = await SB.pingBack(stub);
          return {
            before,
            afterSend: stub.name,
            echoName: echo.name,
            pong: ping.pong,
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
