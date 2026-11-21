// Modified by Ryuz Corp (Cello): release smoke fixture.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The Worker `scripts/release-smoke.sh` serves from a release image. Every
// route is one of the checks: a long-lived SSE body out of a
// `TransformStream`, a body that never produces, a `tee()` drain, a
// WebSocket echo that has to carry a message at the 32 MiB cap intact and
// refuse the byte past it, and a producer whose completed writes must
// plateau when the client stops reading.
//
// `no_bundle` keeps the image's missing esbuild out of the way: the release
// image ships the binary only, so the entry file is served as written.
import { DurableObject } from 'cloudflare:workers';

// One SSE frame every 5 s. The gate reads frames and measures the span
// between the first and the last, so a body cut at 30 s shows up as a short
// span instead of a socket that looks healthy.
const FRAME_MS = 5000;

// The tee check: an 8 MiB body of deterministic bytes, small enough to stay
// well inside the 32 MiB skew budget when branch A is drained first.
const TEE_BYTES = 8 * 1024 * 1024;
const TEE_CHUNK_BYTES = 1024 * 1024;

// A producer that writes 128 KiB chunks up to 512 of them (64 MiB). A
// stalled client may absorb a few dozen through the socket and host
// buffers; 192 chunks (24 MiB) is far above that and far below the cap, so
// a producer that never blocks (all 512) fails loudly while a healthy
// plateau has room to vary with the kernel's TCP buffer sizes.
const CHUNK_BYTES = 128 * 1024;
const MAX_WRITES = 512;

const json = (body, status = 200) => Response.json(body, { status });

const hex = (bytes) =>
  Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('');

const sha256 = async (bytes) =>
  hex(new Uint8Array(await crypto.subtle.digest('SHA-256', bytes)));

// An SSE body out of a `TransformStream`: every write is only completed once
// the response pump reads it, which is what keeps a slow client from
// buffering the body in the isolate.
const openSse = (request) => {
  const { readable, writable } = new TransformStream();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();
  let tick = 0;
  const pump = async () => {
    try {
      while (!request.signal.aborted) {
        await new Promise((resolve) => setTimeout(resolve, FRAME_MS));
        if (request.signal.aborted) return;
        tick += 1;
        await writer.write(encoder.encode(`event: tick\ndata: ${tick}\n\n`));
      }
    } catch {
      // The response pump cancels the producer on a client disconnect.
    }
  };
  pump().catch(() => {});
  request.signal.addEventListener('abort', () => {
    writer.abort(request.signal.reason).catch(() => {});
  });
  return new Response(readable, {
    headers: { 'content-type': 'text/event-stream', 'cache-control': 'no-store' },
  });
};

// A deterministic 8 MiB source. Byte at offset i is `i & 0xff`, so the gate
// can rebuild the expected body and compare hashes.
const teeSource = () => {
  let produced = 0;
  return new ReadableStream({
    pull(controller) {
      if (produced >= TEE_BYTES) {
        controller.close();
        return;
      }
      const chunk = new Uint8Array(TEE_CHUNK_BYTES);
      for (let i = 0; i < chunk.length; i++) chunk[i] = (produced + i) & 0xff;
      produced += chunk.length;
      controller.enqueue(chunk);
    },
  });
};

const readAll = async (stream) => new Uint8Array(await new Response(stream).arrayBuffer());

export class Smoke extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.completed = 0;
    this.bytes = 0;
    this.phase = 'idle';
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname.endsWith('/state')) {
      return json({ completed: this.completed, bytes: this.bytes, phase: this.phase });
    }
    if (url.pathname.endsWith('/tee')) {
      const [a, b] = teeSource().tee();
      const aBytes = await readAll(a);
      const bBytes = await readAll(b);
      return json({
        aBytes: aBytes.byteLength,
        bBytes: bBytes.byteLength,
        aSha: await sha256(aBytes),
        bSha: await sha256(bBytes),
      });
    }
    if (!url.pathname.endsWith('/tight')) return new Response('not found', { status: 404 });
    if (this.phase !== 'idle') return new Response('busy', { status: 409 });
    this.completed = 0;
    this.bytes = 0;
    this.phase = 'producing';
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    // One reused buffer: a write is only observable to the host once it is
    // read, so nothing here mutates while it is in flight.
    const chunk = new Uint8Array(CHUNK_BYTES);
    chunk[0] = 0x78;
    const pump = async () => {
      try {
        for (let i = 0; i < MAX_WRITES; i++) {
          await writer.write(chunk);
          this.completed = i + 1;
          this.bytes = this.completed * CHUNK_BYTES;
        }
        await writer.close();
        this.phase = 'done';
      } catch (e) {
        this.phase = 'failed: ' + String(e);
      }
    };
    pump();
    return new Response(readable, {
      headers: { 'content-type': 'application/octet-stream', 'cache-control': 'no-store' },
    });
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/health') return new Response('ok');
    if (url.pathname === '/sse') return openSse(request);
    if (url.pathname === '/idle') {
      // A body that never produces and never closes: the 60 s stream
      // deadline, not the client, has to end it.
      return new Response(new ReadableStream({}), {
        headers: { 'content-type': 'text/event-stream', 'cache-control': 'no-store' },
      });
    }
    if (url.pathname.startsWith('/cell/')) {
      const id = env.SMOKE.idFromName('one');
      return env.SMOKE.get(id).fetch(new Request(`https://cell${url.pathname.slice('/cell'.length)}`));
    }
    if (url.pathname === '/ws') {
      // One message in, the same bytes out: at the cap the message must pass
      // whole, one byte past it the edge must end the socket with 1009.
      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      server.addEventListener('message', (event) => server.send(event.data));
      return new Response(null, { status: 101, webSocket: client });
    }
    return new Response('not found', { status: 404 });
  },
};
