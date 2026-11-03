// Modified by ryuzcorp (ryuzcorp/celld): binding-forgery conformance fixture. Apache-2.0.
import { DurableObject, WorkflowEntrypoint } from 'cloudflare:workers';

export class VictimDO extends DurableObject {
  // Store a received stub and use the read-back: the receiver's cell
  // round-trips a stub for a class its own script never declared.
  async storeRoundTrip(stub) {
    await this.ctx.storage.put('xstore', stub);
    const back = await this.ctx.storage.get('xstore');
    const r = await (await back.fetch('https://x/')).json();
    return { secret: r.secret };
  }
  async fetch(req) {
    const url = new URL(req.url);
    if (url.searchParams.has('set')) {
      await this.ctx.storage.put('secret', url.searchParams.get('set'));
      return Response.json({ ok: true });
    }
    const secret = await this.ctx.storage.get('secret');
    return Response.json({ secret: secret ?? 'unset' });
  }
}

export class VictimWF extends WorkflowEntrypoint {
  async run() {
    return 'victim-wf-output';
  }
}

const J = (o, st = 200) => Response.json(o, { status: st });

export default {
  async scheduled() {},
  // Object-entrypoint RPC methods (single-argument form): use a stub
  // the caller handed over the service binding, directly or via this
  // script's own cell storage round trip.
  async useStub(stub, env) {
    const r = await (await stub.fetch('https://x/')).json();
    return { secret: r.secret };
  },
  async storeViaCell(stub, env) {
    return await env.NS.getByName('xstore-cell').storeRoundTrip(stub);
  },
  async fetch(req, env) {
    const t = new URL(req.url).pathname.slice(1);
    if (t === 'ping') return J({ pong: true });
    if (t === 'seed') {
      await env.DB.exec(
        'CREATE TABLE IF NOT EXISTS t (v TEXT)',
      );
      await env.DB.exec("DELETE FROM t");
      await env.DB.exec("INSERT INTO t (v) VALUES ('victim-row')");
      await env.SHARED.exec(
        'CREATE TABLE IF NOT EXISTS t (v TEXT)',
      );
      await env.SHARED.exec("DELETE FROM t");
      await env.SHARED.exec("INSERT INTO t (v) VALUES ('shared-row')");
      await env.KV.put('seedkey', 'victim-kv-value');
      await env.NS.getByName('seeded-name').fetch(
        'https://x/?set=victim-do-secret',
      );
      // The boot probe calls this route before the test does; creating
      // the instance twice must not fail the seed.
      try {
        await env.WF.create({ id: 'seed0' });
      } catch {
        /* already seeded */
      }
      await env.R2.put('seedkey', 'victim-r2-value');
      return J({ ok: true });
    }
    return J({ error: 'no route' }, 404);
  },
};
