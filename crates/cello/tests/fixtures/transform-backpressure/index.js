import { DurableObject } from 'cloudflare:workers';

// A TransformStream write must wait for the readable side to be read, which
// is what keeps a streamed response whose client stopped reading from
// buffering the body in the isolate's heap. CHUNK_BYTES x MAX_WRITES bounds
// what a producer can queue on origin/main; the fix stalls it at whatever
// the socket and host buffers absorb.
const CHUNK_BYTES = 128 * 1024;
const MAX_WRITES = 512;

const json = (o, status = 200) => Response.json(o, { status });

// Settle a promise for a few macrotask turns and report whether it has.
// A settled write/ready crosses at most a couple of promise hops, so three
// timer turns are enough to tell "parked" from "resolved".
const outcome = async (promise) => {
  let result = { state: 'pending', value: undefined };
  promise.then(
    (value) => { result = { state: 'fulfilled', value }; },
    (value) => { result = { state: 'rejected', value }; },
  );
  for (let i = 0; i < 3; i++) {
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  return result;
};

const settled = async (promise) => {
  const { state, value } = await outcome(promise);
  if (state !== 'rejected') return state;
  return 'rejected:' + String(value);
};

// The conformance checks run inside the isolate and report failures, so the
// test reads spec values (`desiredSize`, pending vs settled promises) rather
// than the fixture's own conclusions.
const conformance = async () => {
  const failures = [];
  const check = (name, ok, detail) => {
    if (!ok) failures.push(`${name}: ${detail}`);
  };

  // 1. With no consumer, write() and ready stay pending; a read releases
  //    them and yields the chunk.
  {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const write = writer.write('first');
    write.catch(() => {});
    const state = await settled(write);
    check('stalled write', state === 'pending', `settled as ${state}`);
    check(
      'stalled ready',
      (await settled(writer.ready)) === 'pending',
      'writer.ready settled with no reader',
    );
    check(
      'stalled desiredSize',
      writer.desiredSize === 0,
      `writer.desiredSize is ${writer.desiredSize}, expected 0`,
    );
    const reader = readable.getReader();
    const read = await reader.read();
    check('read value', read.value === 'first', `got ${String(read.value)}`);
    check(
      'released write',
      (await settled(write)) === 'fulfilled',
      'write stayed pending after a read',
    );
    check(
      'released ready',
      (await settled(writer.ready)) === 'fulfilled',
      'ready stayed pending after a read',
    );
    check(
      'idle desiredSize',
      writer.desiredSize === 1,
      `writer.desiredSize is ${writer.desiredSize}, expected 1`,
    );

    // 2. Backpressure arms again for the next write.
    const second = writer.write('second');
    second.catch(() => {});
    check(
      'write re-arms',
      (await settled(second)) === 'pending',
      'the second write ran without a consumer',
    );
    const read2 = await reader.read();
    check('second read', read2.value === 'second', `got ${String(read2.value)}`);
    check(
      'second release',
      (await settled(second)) === 'fulfilled',
      'the second write stayed pending after a read',
    );
    await writer.close();
    const done = await reader.read();
    check('close', done.done === true, 'close did not end the readable');
  }

  // 3. A readable highWaterMark of 2 lets two writes run ahead of the
  //    consumer, and no more; desiredSize inside the transform reports it.
  {
    const sizes = [];
    const { readable, writable } = new TransformStream(
      {
        transform(chunk, controller) {
          sizes.push(controller.desiredSize);
          controller.enqueue(chunk);
        },
      },
      undefined,
      { highWaterMark: 2 },
    );
    const writer = writable.getWriter();
    const a = writer.write(1);
    const b = writer.write(2);
    const c = writer.write(3);
    for (const write of [a, b, c]) write.catch(() => {});
    check('hwm read-ahead a', (await settled(a)) === 'fulfilled', 'write 1 parked');
    check('hwm read-ahead b', (await settled(b)) === 'fulfilled', 'write 2 parked');
    check('hwm stall c', (await settled(c)) === 'pending', 'write 3 ran with two chunks queued');
    check(
      'hwm desiredSize',
      sizes[0] === 2,
      `controller.desiredSize inside the first transform is ${sizes[0]}, expected 2`,
    );
    const reader = readable.getReader();
    const values = [];
    values.push((await reader.read()).value);
    values.push((await reader.read()).value);
    check('hwm release', (await settled(c)) === 'fulfilled', 'write 3 stayed parked after a read');
    values.push((await reader.read()).value);
    check('hwm order', values.join(',') === '1,2,3', `read ${values.join(',')}`);
  }

  // 4. The default (hwm 0) readable reports desiredSize 0 to the transform.
  {
    let seen;
    const { readable, writable } = new TransformStream({
      transform(chunk, controller) {
        seen = controller.desiredSize;
        controller.enqueue(chunk);
      },
    });
    const writer = writable.getWriter();
    const reader = readable.getReader();
    const read = reader.read();
    await writer.write('x');
    check(
      'transform desiredSize',
      seen === 0,
      `controller.desiredSize inside transform is ${seen}, expected 0`,
    );
    check('transform read', (await read).value === 'x', 'read did not yield the chunk');
  }

  // 5. IdentityTransformStream already resolves a write only once it is
  //    read; the fix must not regress it.
  {
    const { readable, writable } = new IdentityTransformStream();
    const writer = writable.getWriter();
    const write = writer.write(new Uint8Array([1, 2, 3]));
    write.catch(() => {});
    check(
      'identity write stalls',
      (await settled(write)) === 'pending',
      'the identity write ran without a read',
    );
    const reader = readable.getReader();
    const read = await reader.read();
    check(
      'identity read',
      Array.from(read.value).join(',') === '1,2,3',
      'the identity read did not yield the bytes',
    );
    check(
      'identity release',
      (await settled(write)) === 'fulfilled',
      'the identity write stayed pending after a read',
    );
  }

  // 6. Cancelling the readable fails a write parked on the handshake with
  //    the cancel reason, instead of leaving it pending forever.
  {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const write = writer.write('cancelled');
    write.catch(() => {});
    await settled(write);
    const reason = new Error('cancel reason');
    await readable.cancel(reason);
    let seen = null;
    await write.catch((e) => (seen = e));
    check('cancel unblocks', seen === reason, `the parked write rejected with ${String(seen)}`);
  }

  // 7. The same for abort().
  {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const write = writer.write('aborted');
    write.catch(() => {});
    await settled(write);
    const reason = new Error('abort reason');
    await writer.abort(reason);
    let seen = null;
    await write.catch((e) => (seen = e));
    check('abort unblocks', seen === reason, `the parked write rejected with ${String(seen)}`);
    check(
      'abort closes readable',
      (await settled(readable.getReader().closed)).startsWith('rejected'),
      'the readable survived the abort',
    );
  }

  // 8. A transform that throws still errors both sides.
  {
    const { readable, writable } = new TransformStream({
      transform() {
        throw new Error('transform failed');
      },
    });
    const writer = writable.getWriter();
    const reader = readable.getReader();
    const write = writer.write('x');
    write.catch(() => {});
    const read = reader.read();
    read.catch(() => {});
    check(
      'transform error write',
      (await settled(write)).startsWith('rejected:Error: transform failed'),
      'the write did not reject with the transform error',
    );
    check(
      'transform error read',
      (await settled(read)).startsWith('rejected:Error: transform failed'),
      'the read did not reject with the transform error',
    );
  }

  // 9. controller.error() from a later tick errors both sides and releases
  //    a write parked on the backpressure handshake; ready rejects with the
  //    same error.
  {
    let ctrl;
    const { readable, writable } = new TransformStream({
      start(c) { ctrl = c; },
    });
    const writer = writable.getWriter();
    const write = writer.write('parked');
    write.catch(() => {});
    check('error parked write', (await settled(write)) === 'pending', 'the write did not park');
    const failure = new Error('controller error');
    ctrl.error(failure);
    const released = await outcome(write);
    check(
      'error unblocks write',
      released.state === 'rejected' && released.value === failure,
      `the parked write settled as ${released.state}:${String(released.value)}`,
    );
    check(
      'error rejects ready',
      (await settled(writer.ready)) === 'rejected:Error: controller error',
      'writer.ready did not reject with the controller error',
    );
    check(
      'error errors readable',
      (await settled(readable.getReader().read())) === 'rejected:Error: controller error',
      'the readable did not error',
    );
  }

  // 10. controller.error() inside start() errors the writable side too: a
  //     write that arrives afterwards must reject instead of parking on a
  //     readable nothing will pull again.
  {
    const failure = new Error('start error');
    const { readable, writable } = new TransformStream({
      start(controller) { controller.error(failure); },
    });
    const writer = writable.getWriter();
    const state = await settled(writer.write('late'));
    check(
      'start error write',
      state === 'rejected:Error: start error',
      `the write settled as ${state}`,
    );
    check(
      'start error readable',
      (await settled(readable.getReader().read())) === 'rejected:Error: start error',
      'the readable did not error',
    );
  }

  // 11. controller.terminate() inside start() closes the readable and errors
  //     the writable: a later write rejects with a TypeError.
  {
    const { readable, writable } = new TransformStream({
      start(controller) { controller.terminate(); },
    });
    const writer = writable.getWriter();
    const state = await settled(writer.write('late'));
    check(
      'start terminate write',
      state.startsWith('rejected:TypeError'),
      `the write settled as ${state}`,
    );
    check(
      'start terminate readable',
      (await settled(readable.getReader().closed)) === 'fulfilled',
      'the readable was not closed',
    );
  }

  // 12. controller.terminate() from a later tick releases a parked write.
  {
    let ctrl;
    const { readable, writable } = new TransformStream({
      start(c) { ctrl = c; },
    });
    const writer = writable.getWriter();
    const write = writer.write('parked');
    write.catch(() => {});
    await settled(write);
    ctrl.terminate();
    const released = await outcome(write);
    check(
      'terminate unblocks write',
      released.state === 'rejected' && released.value instanceof TypeError,
      `the parked write settled as ${released.state}:${String(released.value)}`,
    );
    check(
      'terminate closes readable',
      (await settled(readable.getReader().closed)) === 'fulfilled',
      'the readable was not closed',
    );
  }

  // 13. The hook that releases a parked write is internal: a tenant
  //     shadowing the old public field must not make abort or cancel throw
  //     before the transformer's cancel runs.
  {
    let cancelled = 0;
    const { readable, writable } = new TransformStream({
      cancel() { cancelled += 1; },
    });
    writable._unblockWrite = () => { throw new Error('tenant hook'); };
    const writer = writable.getWriter();
    const write = writer.write('parked');
    write.catch(() => {});
    await settled(write);
    const reason = new Error('abort after overwrite');
    let abortError = null;
    try { await writer.abort(reason); } catch (e) { abortError = e; }
    const released = await outcome(write);
    check(
      'overwritten hook abort settles',
      abortError === null,
      `abort threw ${String(abortError)}`,
    );
    check(
      'overwritten hook abort',
      released.state === 'rejected' && released.value === reason,
      `the parked write settled as ${released.state}:${String(released.value)}`,
    );
    check('overwritten hook abort cancel', cancelled === 1, `transformer cancel ran ${cancelled} times`);

    const second = new TransformStream({ cancel() { cancelled += 1; } });
    second.writable._unblockWrite = () => { throw new Error('tenant hook'); };
    const secondWriter = second.writable.getWriter();
    const secondWrite = secondWriter.write('parked');
    secondWrite.catch(() => {});
    await settled(secondWrite);
    const cancelReason = new Error('cancel after overwrite');
    let cancelError = null;
    try { await second.readable.cancel(cancelReason); } catch (e) { cancelError = e; }
    const secondReleased = await outcome(secondWrite);
    check(
      'overwritten hook readable cancel settles',
      cancelError === null,
      `readable.cancel threw ${String(cancelError)}`,
    );
    check(
      'overwritten hook readable cancel',
      secondReleased.state === 'rejected' && secondReleased.value === cancelReason,
      `the parked write settled as ${secondReleased.state}:${String(secondReleased.value)}`,
    );
    check(
      'overwritten hook readable cancel ran',
      cancelled === 2,
      `transformer cancel ran ${cancelled} times`,
    );
  }

  return { failures };
};

// A cell whose producer writes into a TransformStream as fast as the
// backpressure handshake allows, with the count of completed writes
// observable from another request.
export class Blocked extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.completed = 0;
    this.bytes = 0;
    this.phase = 'idle';
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname.endsWith('/state')) {
      return json({
        completed: this.completed,
        bytes: this.bytes,
        phase: this.phase,
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
        // End the body: the HTTP response only gains its terminal chunk when
        // the readable closes, and the test asserts the decoded framing.
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
    if (url.pathname === '/conformance') return json(await conformance());
    if (url.pathname === '/cell/tight') {
      const id = env.BLOCKED.idFromName('one');
      return env.BLOCKED.get(id).fetch(new Request('https://cell/tight'));
    }
    if (url.pathname === '/cell/state') {
      const id = env.BLOCKED.idFromName('one');
      return env.BLOCKED.get(id).fetch(new Request('https://cell/state'));
    }
    return new Response('not found', { status: 404 });
  },
};
