// Modified by ryuzcorp (ryuzcorp/celld): abort-cleared timer regression fixture (upstream #309). Apache-2.0.
import { DurableObject } from 'cloudflare:workers';

const sleep = (ms, signal) => new Promise((resolve) => {
  if (signal.aborted) return resolve();
  const t = setTimeout(done, ms);
  signal.addEventListener('abort', done, { once: true });
  function done() { clearTimeout(t); signal.removeEventListener('abort', done); resolve(); }
});

export class Cell extends DurableObject {
  state = 'idle';
  async fetch(req) {
    const u = new URL(req.url);
    if (u.pathname === '/start') {
      const c = new AbortController();
      this.c = c;
      this.state = 'running';
      // Background loop with no waitUntil: each iteration's timer is new
      // work the loop's continuation creates after the starter answered.
      const loop = (async () => {
        while (!c.signal.aborted) await sleep(200, c.signal);
        this.state = 'aborted';
        await new Promise((r) => setTimeout(r, 10));
        this.state = 'done';
      })();
      if (u.searchParams.get('wu') === '1') this.ctx.waitUntil(loop);
      return new Response('started');
    }
    if (u.pathname === '/abort') { this.c.abort(); return new Response('aborted'); }
    if (u.pathname === '/status') return new Response(this.state);
    return new Response('ok');
  }
}

export default {
  fetch(req, env) {
    const u = new URL(req.url);
    if (u.pathname === '/health') return new Response('ok');
    return env.CELL.get(env.CELL.idFromName(u.searchParams.get('cell') ?? 'one')).fetch(req);
  },
};
