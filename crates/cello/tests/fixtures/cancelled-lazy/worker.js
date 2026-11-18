// Modified by Ryuz Corp (Cello): cancelled-request lazy-init regression fixture (upstream #294). Apache-2.0.
import { DurableObject } from 'cloudflare:workers';

// Lazily started work shared by every later request to the object: the
// usual `this.ready ??= init()` shape. A cancelled request must not leave
// it pending forever.
export class Lazy extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/started') {
      // Handshake for the abort: reports whether the lazy init the
      // cancelled request must be inside of has actually started.
      return new Response(this.ready ? 'yes' : 'no');
    }
    if (url.pathname === '/timer') {
      // Past the retry deadline on purpose: the retry may only answer
      // because cancellation settled this init early, never because the
      // timer ran its natural course.
      this.ready ??= new Promise((resolve) => setTimeout(() => resolve('timer fired'), 60_000));
    }
    return new Response(await this.ready);
  }
}

export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health') return new Response('ok');
    return env.LAZY.get(env.LAZY.idFromName('one')).fetch(request);
  },
};
