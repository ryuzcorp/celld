import { DurableObject, waitUntil } from 'cloudflare:workers';

// Every tick is one write the response pump must deliver. A 2 s background
// budget is several ticks away from the end of the window the test reads, so
// a budget that seals the pump shows up as ticks that stop arriving.
const TICK_MS = 100;

// An SSE body that keeps producing for as long as the client stays connected.
// `onTick` records progress; the producer stops when the pump cancels it on a
// client disconnect (the forwarded signal) or when its write fails because the
// consumer is gone.
const openStream = (request, onTick, padBytes = 0) => {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();
  // A comment line of padding: big frames fill the socket buffers fast
  // when the client stops reading.
  const pad = padBytes > 0 ? `: ${'x'.repeat(padBytes)}\n` : '';
  let tick = 0;
  const pump = async () => {
    while (!request.signal.aborted) {
      await new Promise((resolve) => setTimeout(resolve, TICK_MS));
      if (request.signal.aborted) return;
      tick += 1;
      await writer.write(encoder.encode(`${pad}data: tick ${tick}\n\n`));
      onTick?.(tick);
    }
  };
  pump().catch(() => {});
  return new Response(readable, {
    headers: {
      'content-type': 'text/event-stream',
      'cache-control': 'no-store',
    },
  });
};

// A cell whose fetch streams: the pump belongs to the cell event, and the
// forwarded request signal proves the producer is cancelled on disconnect.
export class Streamer extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ticks = 0;
    this.aborted = false;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname.endsWith('/state')) {
      return Response.json({ ticks: this.ticks, aborted: this.aborted });
    }
    if (url.pathname.endsWith('/wait-forever')) {
      // Leftover background work: the budget still ends it.
      waitUntil(new Promise(() => {}));
      return new Response('ok');
    }
    if (url.pathname.endsWith('/stream-with-wait-forever')) {
      // The same entry streams a body and parks a waitUntil that never
      // settles: the budget must end the background claim without touching
      // the body.
      waitUntil(new Promise(() => {}));
      return openStream(request);
    }
    request.signal.addEventListener('abort', () => {
      this.aborted = true;
    });
    // `/big-stream` writes 256 KiB frames, so a client that stops reading
    // leaves the next write blocked on backpressure.
    const padBytes = url.pathname.endsWith('/big-stream') ? 256 * 1024 : 0;
    return openStream(
      request,
      (tick) => {
        this.ticks = tick;
      },
      padBytes,
    );
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health') {
      // The test's readiness probe: a complete response, not a stream.
      return new Response('ok');
    }
    if (url.pathname.startsWith('/cell/')) {
      const id = env.STREAMER.idFromName('one');
      return env.STREAMER.get(id).fetch(
        new Request(`https://cell${url.pathname.slice('/cell'.length)}`),
      );
    }
    if (url.pathname === '/wait-forever') {
      // A `waitUntil` that never settles: bounded by the budget, and bounded
      // whether or not another response is streaming.
      waitUntil(new Promise(() => {}));
      return new Response('ok');
    }
    if (url.pathname === '/idle') {
      // A body that never produces and never closes: the idle deadline, not
      // the client, has to end it.
      return new Response(new ReadableStream({}), {
        headers: { 'content-type': 'text/event-stream' },
      });
    }
    return openStream(request);
  },
};
