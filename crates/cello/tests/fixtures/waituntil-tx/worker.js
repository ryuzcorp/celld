// Modified by ryuzcorp (ryuzcorp/celld): waitUntil background storage transaction regression fixture (upstream #300). Apache-2.0.
import { DurableObject } from 'cloudflare:workers';

export class Repro extends DurableObject {
  resolve = null;

  async run(socket) {
    await new Promise((resolve) => {
      this.resolve = resolve;
      socket.send('waiting');
    });

    await this.ctx.storage.transaction(async (tx) => {
      await tx.put('first', 1);
    });
    await this.ctx.storage.transaction(async (tx) => {
      await tx.put('second', 2);
    });

    socket.send(JSON.stringify({
      first: await this.ctx.storage.get('first'),
      second: await this.ctx.storage.get('second'),
    }));
  }

  fetch() {
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    server.accept();
    server.addEventListener('message', (event) => {
      if (event.data === 'start') this.ctx.waitUntil(this.run(server));
      else if (event.data === 'complete') this.resolve();
    });

    return new Response(null, { status: 101, webSocket: client });
  }
}

export default {
  fetch: (request, env) => {
    const url = new URL(request.url);
    if (url.pathname === '/health') return new Response('ok');
    return env.REPRO.get(env.REPRO.idFromName('one')).fetch(request);
  },
};
