// Modified by ryuzcorp (ryuzcorp/celld): woken-continuation outbound regression fixture (upstream #258). Apache-2.0.
import { DurableObject } from 'cloudflare:workers';

// Async work started by one request, woken by another: the continuation
// resumes with its origin event retired. Whatever the outbound call does,
// the waiter must settle instead of hanging to its budget.
export class Cell extends DurableObject {
  async op(how, origin) {
    if (how === 'fetch') return (await fetch(`${origin}/plain`)).text();
    await this.ctx.storage.put('k', 1);
    return 'stored';
  }
  async fetch(request) {
    const url = new URL(request.url);
    const how = url.searchParams.get('how');
    const settle = (p) => p.then(
      (v) => { this.result = `ok ${v}`; },
      (e) => { this.result = `threw ${e.message}`; },
    );
    if (url.pathname === '/self') {
      // Control: woken by its own timer, no other event involved.
      this.result = 'pending';
      settle(new Promise((r) => setTimeout(r, 200)).then(() => this.op(how, url.origin)));
      return new Response('started\n');
    }
    if (url.pathname === '/start') {
      this.result = 'pending';
      settle(new Promise((r) => { this.notify = r; }).then(() => this.op(how, url.origin)));
      return new Response('started\n');
    }
    if (url.pathname === '/notify') {
      this.notify();
      return new Response('notified\n');
    }
    if (url.pathname === '/result') return new Response(`${this.result}\n`);
    return new Response('not found', { status: 404 });
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health') return new Response('ok');
    if (url.pathname === '/plain') return new Response('plain ok');
    return env.CELL.get(env.CELL.idFromName(url.searchParams.get('name') ?? 'one')).fetch(request);
  },
};
