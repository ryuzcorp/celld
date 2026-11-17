// Modified by ryuzcorp (ryuzcorp/celld): cancelled-request lazy-init regression fixture (upstream #294). Apache-2.0.
import { DurableObject } from 'cloudflare:workers';

// Lazily started work shared by every later request to the object: the
// usual `this.ready ??= init()` shape. A cancelled request must not leave
// it pending forever.
export class Lazy extends DurableObject {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === '/timer') {
      this.ready ??= new Promise((resolve) => setTimeout(() => resolve('timer fired'), 500));
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
