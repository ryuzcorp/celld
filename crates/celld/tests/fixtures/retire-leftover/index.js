import { DurableObject } from 'cloudflare:workers';

// A cell whose background work never ends on its own: the constructor
// starts a timer loop, and every fetch parks a waitUntil promise that
// never settles. After the response goes out, both keep the drive — and
// its isolate affiliation — alive, so an evicted cell's isolate stays
// retiring until the background budget ends it (upstream #275).
export class Cell extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    const tick = () => {
      this.timer = setTimeout(tick, 50);
    };
    tick();
  }
  async fetch() {
    this.ctx.waitUntil(new Promise(() => {}));
    return new Response('ok');
  }
}

export default {
  fetch(req, env) {
    return env.CELL.get(env.CELL.idFromName(new URL(req.url).pathname)).fetch(req);
  },
};
