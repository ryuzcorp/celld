// Modified by Ryuz Corp (Cello): release smoke client.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Host-side driver for `scripts/release-smoke.sh`. It talks to the Worker
// the release image serves with plain Node builtins (no npm packages): the
// checks are the consumer-facing shapes that have broken in released images
// before — a long-lived SSE body, a body that never produces, WebSocket
// messages at and past the 32 MiB cap, an 8 MiB `tee()`, and a
// backpressured producer.
//
// Usage: node client.mjs http://127.0.0.1:PORT
//
//   SMOKE_SSE_SECONDS  span the SSE check must survive (default 300)
//   SMOKE_QUICK=1      shortens the SSE span to 90 s for local iteration
import { createHash, randomBytes } from 'node:crypto';
import { connect } from 'node:net';

const BASE = process.argv[2];
if (!BASE) {
  console.error('usage: node client.mjs http://127.0.0.1:PORT');
  process.exit(2);
}
const { hostname: HOST, port: PORT } = new URL(BASE);

const QUICK = process.env.SMOKE_QUICK === '1';
const SSE_SECONDS =
  Number(process.env.SMOKE_SSE_SECONDS ?? '') || (QUICK ? 90 : 300);
/// One fixture frame lands every 5 s; a gap this wide is a dead stream, not
/// scheduler jitter.
const SSE_GAP_MS = 15000;
/// The edge's idle response-body deadline is 60 s.
const IDLE_MIN_MS = 55000;
const IDLE_MAX_MS = 75000;

const WS_MAX_BYTES = 32 * 1024 * 1024;
const WS_ROUNDTRIP_BYTES = 20 * 1024 * 1024;
const TEE_BYTES = 8 * 1024 * 1024;

/// The stalled producer's ceiling, matching
/// `crates/cello/tests/transform_backpressure_conformance.rs`: 192 chunks
/// of 128 KiB is far above what socket and host buffers absorb and far
/// below the fixture's 512-chunk cap.
const STALLED_MAX_WRITES = 192;
const STALL_SAMPLE_MS = 200;
const STALL_DEADLINE_MS = 30000;

const results = [];
const clock = (start) => `${((Date.now() - start) / 1000).toFixed(1)} s`;

const withDeadline = (promise, ms, what) => {
  let timer;
  const guard = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} within ${ms / 1000} s`)), ms);
  });
  return Promise.race([promise, guard]).finally(() => clearTimeout(timer));
};

const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');

const pattern = (bytes) => {
  const buf = Buffer.allocUnsafe(bytes);
  for (let i = 0; i < bytes; i++) buf[i] = i & 0xff;
  return buf;
};

// `limitMs` is the check's whole-run ceiling: every network wait inside a
// check has its own deadline, and this one guarantees a hung image still
// reaches the summary instead of stalling the gate.
async function runCheck(id, title, fn, limitMs) {
  const started = Date.now();
  try {
    const note = await withDeadline(
      Promise.resolve().then(fn),
      limitMs,
      `check ${id} did not finish`,
    );
    const result = { id, title, ok: true, note, elapsed: clock(started) };
    results.push(result);
    return result;
  } catch (error) {
    const result = {
      id,
      title,
      ok: false,
      note: error?.message ?? String(error),
      elapsed: clock(started),
    };
    results.push(result);
    return result;
  }
}

function report(result) {
  const verdict = result.ok ? 'PASS' : 'FAIL';
  console.log(
    `${verdict} check ${result.id}  ${result.title}  (${result.elapsed})  ${result.note}`,
  );
}

// ---------------------------------------------------------------------------
// Check 1: SSE out of a TransformStream must stay alive for SSE_SECONDS.
async function checkSse() {
  const controller = new AbortController();
  const response = await withDeadline(
    fetch(`${BASE}/sse`, {
      signal: controller.signal,
      headers: { accept: 'text/event-stream', 'cache-control': 'no-store' },
    }),
    30000,
    'the SSE response head did not arrive',
  );
  if (response.status !== 200) {
    throw new Error(`the SSE route answered ${response.status}`);
  }
  const reader = response.body.getReader();
  // The release bug this catches cut the body a fixed time after the
  // response head, so the failure names that time, not only the span.
  const opened = Date.now();
  const stopped = (error) => {
    const alive = ((Date.now() - opened) / 1000).toFixed(1);
    const lastAt = last ? ((last - opened) / 1000).toFixed(1) : '0.0';
    const span = first ? ((last - first) / 1000).toFixed(1) : '0.0';
    return (
      `the SSE stream died ${alive} s after the head (last frame at ${lastAt} s, ` +
      `span ${span} s over ${frames} frames): ${error.message}`
    );
  };
  const decoder = new TextDecoder();
  let buffer = '';
  let frames = 0;
  let first = 0;
  let last = 0;
  let ended = false;
  while (true) {
    let chunk;
    try {
      chunk = await withDeadline(reader.read(), SSE_GAP_MS, 'no SSE frame arrived');
    } catch (error) {
      throw new Error(stopped(error));
    }
    if (chunk.done) {
      ended = true;
      break;
    }
    buffer += decoder.decode(chunk.value, { stream: true });
    let newline;
    while ((newline = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, newline).replace(/\r$/, '');
      buffer = buffer.slice(newline + 1);
      if (!line.startsWith('data:')) continue;
      const now = Date.now();
      if (!first) first = now;
      if (last && now - last > SSE_GAP_MS) {
        throw new Error(`a ${(now - last) / 1000} s gap between SSE frames`);
      }
      last = now;
      frames += 1;
      if ((last - first) / 1000 >= SSE_SECONDS) {
        controller.abort();
        return `span ${((last - first) / 1000).toFixed(1)} s over ${frames} frames`;
      }
    }
  }
  if (ended) {
    throw new Error(stopped(new Error('the body ended')));
  }
  throw new Error('unreachable');
}

// ---------------------------------------------------------------------------
// Check 2: a body that never produces is ended by the 60 s idle deadline.
async function checkIdleBody() {
  const started = Date.now();
  const response = await withDeadline(
    fetch(`${BASE}/idle`, { headers: { accept: 'text/event-stream' } }),
    30000,
    'the idle response head did not arrive',
  );
  if (response.status !== 200) {
    throw new Error(`the idle route answered ${response.status}`);
  }
  const reader = response.body.getReader();
  const deadline = started + 120000;
  let produced = 0;
  let ended = 0;
  while (!ended) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) throw new Error('the idle body did not end within 120 s');
    let timer;
    const timeout = new Promise((resolve) => {
      timer = setTimeout(() => resolve({ timeout: true }), remaining);
    });
    // The edge ends an idle body by closing the socket under the framing, so
    // a read rejection is the end too: what the check asserts is when.
    const step = await Promise.race([
      reader.read().then((value) => ({ value }), (error) => ({ error })),
      timeout,
    ]);
    clearTimeout(timer);
    if (step.timeout) throw new Error('the idle body did not end within 120 s');
    if (step.error || step.value.done) {
      ended = Date.now();
    } else {
      produced += step.value.value.byteLength;
    }
  }
  const elapsed = ended - started;
  if (produced !== 0) {
    throw new Error(`the idle body produced ${produced} bytes`);
  }
  if (elapsed < IDLE_MIN_MS || elapsed > IDLE_MAX_MS) {
    throw new Error(`the idle body ended after ${(elapsed / 1000).toFixed(1)} s, not 55-75 s`);
  }
  return `ended after ${(elapsed / 1000).toFixed(1)} s`;
}

// ---------------------------------------------------------------------------
// A minimal WebSocket client over `node:net`: enough framing to send one
// binary message, hash the echo as it arrives, and read the close code. The
// built-in `WebSocket` would decide its own message-size limits, and the
// whole point of checks 3 and 4 is the server's 32 MiB cap.
function connectWebSocket(path) {
  return new Promise((resolve, reject) => {
    const socket = connect({ host: HOST, port: Number(PORT) });
    // The caller's deadline is a backstop; this one closes the socket, so a
    // hung edge leaves no descriptor behind.
    const opened = setTimeout(() => {
      socket.destroy();
      reject(new Error('the WebSocket did not open within 30 s'));
    }, 30000);
    const key = randomBytes(16).toString('base64');
    const expectedAccept = createHash('sha1')
      .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
      .digest('base64');
    let chunks = [];
    let head = 0;
    let buffered = 0;
    let closed = null;
    let wake = null;
    const ping = () => {
      const resume = wake;
      wake = null;
      resume?.();
    };
    const fail = (error) => {
      closed = error;
      ping();
    };
    socket.on('data', (chunk) => {
      chunks.push(chunk);
      buffered += chunk.length;
      ping();
    });
    socket.on('end', () => fail(new Error('the socket ended')));
    socket.on('error', (error) => fail(error));
    socket.setNoDelay(true);

    const read = async (n) => {
      while (buffered < n) {
        if (closed) throw closed;
        await new Promise((resume) => {
          wake = resume;
        });
      }
      const out = Buffer.allocUnsafe(n);
      let filled = 0;
      while (filled < n) {
        const first = chunks[0];
        const available = first.length - head;
        const take = Math.min(available, n - filled);
        first.copy(out, filled, head, head + take);
        filled += take;
        head += take;
        buffered -= take;
        if (head === first.length) {
          chunks.shift();
          head = 0;
        }
      }
      return out;
    };

    // `sendDeclared` writes a frame whose header claims `declared` bytes but
    // whose body is only `body`. The oversize check needs that: the edge
    // refuses the length as soon as it reads the header, and pushing the
    // remaining 32 MiB into a socket it is closing earns an RST that
    // discards the close frame we are here to read.
    const writeFrame = (opcode, declared, body) => {
      let header;
      if (declared < 126) {
        header = Buffer.from([0x80 | opcode, 0x80 | declared]);
      } else if (declared < 65536) {
        header = Buffer.alloc(4);
        header[0] = 0x80 | opcode;
        header[1] = 0x80 | 126;
        header.writeUInt16BE(declared, 2);
      } else {
        header = Buffer.alloc(10);
        header[0] = 0x80 | opcode;
        header[1] = 0x80 | 127;
        header.writeBigUInt64BE(BigInt(declared), 2);
      }
      const mask = randomBytes(4);
      const masked = Buffer.allocUnsafe(body.length);
      for (let i = 0; i < body.length; i++) masked[i] = body[i] ^ mask[i & 3];
      socket.write(header);
      socket.write(mask);
      socket.write(masked);
    };
    const sendFrame = (opcode, payload) => writeFrame(opcode, payload.length, payload);

    const readHead = async () => {
      let raw = Buffer.alloc(0);
      while (!raw.includes('\r\n\r\n')) {
        raw = Buffer.concat([raw, await read(1)]);
        if (raw.length > 16384) throw new Error('the handshake head is too large');
      }
      return raw.toString('latin1');
    };

    // One message, hashed as its frames arrive; control frames are handled
    // here so the caller only sees data and close.
    const readMessage = async () => {
      const hash = createHash('sha256');
      let length = 0;
      let messageOpcode = null;
      for (;;) {
        const header = await read(2);
        const fin = (header[0] & 0x80) !== 0;
        const opcode = header[0] & 0x0f;
        const masked = (header[1] & 0x80) !== 0;
        let size = header[1] & 0x7f;
        if (size === 126) size = (await read(2)).readUInt16BE(0);
        else if (size === 127) size = Number((await read(8)).readBigUInt64BE(0));
        if (masked) await read(4);
        if (opcode === 0x8) {
          const body = await read(size);
          return { close: size >= 2 ? body.readUInt16BE(0) : 1005, length, hash };
        }
        if (opcode === 0x9) {
          sendFrame(0x0a, await read(size));
          continue;
        }
        if (opcode === 0x0a) {
          await read(size);
          continue;
        }
        if (opcode === 0x1 || opcode === 0x2) messageOpcode = opcode;
        let left = size;
        while (left > 0) {
          const slice = await read(Math.min(left, 1024 * 1024));
          hash.update(slice);
          left -= slice.length;
        }
        length += size;
        if (fin) return { opcode: messageOpcode, length, hash };
      }
    };

    // The handshake goes out before the head is read: the edge answers the
    // upgrade only once it has the request.
    socket.write(
      `GET ${path} HTTP/1.1\r\nHost: ${HOST}:${PORT}\r\nUpgrade: websocket\r\n` +
        `Connection: Upgrade\r\nSec-WebSocket-Key: ${key}\r\n` +
        'Sec-WebSocket-Version: 13\r\n\r\n',
    );

    readHead()
      .then((raw) => {
        const status = raw.split('\r\n')[0] ?? '';
        if (!/\s101\s/.test(status)) {
          throw new Error(`the upgrade answered ${status}`);
        }
        const accept = /sec-websocket-accept:\s*(\S+)/i.exec(raw)?.[1];
        if (accept !== expectedAccept) {
          throw new Error('the upgrade accept header does not match the key');
        }
        clearTimeout(opened);
        resolve({
          send: sendFrame,
          sendDeclared: writeFrame,
          readMessage,
          destroy: () => socket.destroy(),
        });
      })
      .catch((error) => {
        clearTimeout(opened);
        socket.destroy();
        reject(error);
      });
  });
}

// Check 3: a 20 MiB message round-trips intact.
async function checkWsRoundtrip() {
  const payload = pattern(WS_ROUNDTRIP_BYTES);
  const expected = sha256(payload);
  const client = await withDeadline(connectWebSocket('/ws'), 30000, 'the WebSocket did not open');
  try {
    client.send(0x2, payload);
    const echo = await withDeadline(
      client.readMessage(),
      180000,
      'the 20 MiB echo did not arrive',
    );
    if (echo.close !== undefined) {
      throw new Error(`the edge closed with ${echo.close} instead of echoing`);
    }
    if (echo.length !== WS_ROUNDTRIP_BYTES) {
      throw new Error(`the echo carried ${echo.length} bytes, not ${WS_ROUNDTRIP_BYTES}`);
    }
    const got = echo.hash.digest('hex');
    if (got !== expected) {
      throw new Error(`the echo hash is ${got}, not ${expected}`);
    }
    return `20 MiB round-tripped, sha256 ${got.slice(0, 16)}…`;
  } finally {
    client.destroy();
  }
}

// Check 4: exactly the 32 MiB cap passes; one byte more closes with 1009.
async function checkWsCap() {
  const payload = pattern(WS_MAX_BYTES);
  const expected = sha256(payload);
  let echoNote;
  {
    const client = await withDeadline(
      connectWebSocket('/ws'),
      30000,
      'the WebSocket did not open',
    );
    try {
      client.send(0x2, payload);
      const echo = await withDeadline(
        client.readMessage(),
        240000,
        'the 32 MiB echo did not arrive',
      );
      if (echo.close !== undefined) {
        throw new Error(`exactly 32 MiB closed with ${echo.close} instead of echoing`);
      }
      if (echo.length !== WS_MAX_BYTES) {
        throw new Error(`the echo carried ${echo.length} bytes, not ${WS_MAX_BYTES}`);
      }
      const got = echo.hash.digest('hex');
      if (got !== expected) {
        throw new Error(`the echo hash is ${got}, not ${expected}`);
      }
      echoNote = `32 MiB intact (sha256 ${got.slice(0, 16)}…)`;
    } finally {
      client.destroy();
    }
  }
  {
    const client = await withDeadline(
      connectWebSocket('/ws'),
      30000,
      'the WebSocket did not open',
    );
    try {
      // Declare one byte past the cap and send only a prefix: the edge
      // refuses the length, and the close frame is what the check reads.
      client.sendDeclared(
        0x2,
        WS_MAX_BYTES + 1,
        payload.subarray(0, 1024 * 1024),
      );
      const result = await withDeadline(
        client.readMessage(),
        120000,
        'no close frame for the oversize message',
      );
      if (result.close !== 1009) {
        throw new Error(
          result.close === undefined
            ? 'the edge echoed the oversize message instead of closing'
            : `the edge closed with ${result.close}, not 1009`,
        );
      }
      return `${echoNote}; 32 MiB + 1 closed with 1009`;
    } finally {
      client.destroy();
    }
  }
}

// Check 5: an 8 MiB body tee()d, branch A drained before B, both intact.
async function checkTee() {
  const expected = sha256(pattern(TEE_BYTES));
  const response = await withDeadline(
    fetch(`${BASE}/cell/tee`),
    120000,
    'the tee route did not answer',
  );
  if (response.status !== 200) {
    throw new Error(`the tee route answered ${response.status}`);
  }
  const report = await response.json();
  if (report.aBytes !== TEE_BYTES || report.bBytes !== TEE_BYTES) {
    throw new Error(
      `the tee branches carried ${report.aBytes} and ${report.bBytes} bytes, not ${TEE_BYTES}`,
    );
  }
  if (report.aSha !== expected || report.bSha !== expected) {
    throw new Error(
      `the tee hashes are ${report.aSha} and ${report.bSha}, not ${expected}`,
    );
  }
  return `A then B each ${TEE_BYTES / 1024 / 1024} MiB, sha256 ${expected.slice(0, 16)}…`;
}

// Check 6: a client that stops reading stalls the producer.
async function checkBackpressure() {
  const socket = connect({ host: HOST, port: Number(PORT) });
  let state = { completed: 0, phase: 'idle' };
  try {
    await withDeadline(
      new Promise((resolve, reject) => {
        socket.once('error', reject);
        socket.once('connect', resolve);
      }),
      30000,
      'the tight route did not accept a connection',
    );
    socket.setNoDelay(true);
    socket.write(
      `GET /cell/tight HTTP/1.1\r\nHost: ${HOST}:${PORT}\r\n` +
        'Accept: application/octet-stream\r\n\r\n',
    );
    // Take the response head and then stop reading: from here the only
    // bound on the producer is TransformStream backpressure.
    let seen = Buffer.alloc(0);
    await withDeadline(
      new Promise((resolve, reject) => {
        const onData = (chunk) => {
          seen = Buffer.concat([seen, chunk]);
          if (seen.includes('\r\n\r\n')) {
            socket.off('data', onData);
            socket.pause();
            resolve();
          }
          if (seen.length > 1024 * 1024) {
            socket.off('data', onData);
            reject(new Error('no response head on the tight route'));
          }
        };
        socket.on('data', onData);
        socket.once('error', reject);
        socket.once('end', () => reject(new Error('the tight route ended before a head')));
      }),
      30000,
      'the tight route sent no response head',
    );
    const head = seen.toString('latin1').split('\r\n\r\n')[0];
    const status = Number(head.split(' ')[1]);
    if (status !== 200) throw new Error(`the tight route answered ${status}`);

    const deadline = Date.now() + STALL_DEADLINE_MS;
    let previous = -1;
    for (;;) {
      if (Date.now() > deadline) {
        throw new Error(`the producer never plateaued (at ${state.completed} writes)`);
      }
      await new Promise((resolve) => setTimeout(resolve, STALL_SAMPLE_MS));
      const response = await withDeadline(
        fetch(`${BASE}/cell/state`),
        10000,
        'the producer state did not answer',
      );
      state = await response.json();
      if (typeof state.phase === 'string' && state.phase.startsWith('failed')) {
        throw new Error(`the producer failed instead of parking: ${state.phase}`);
      }
      if (state.completed >= 1 && state.completed === previous) break;
      previous = state.completed;
    }
    const bytes = state.completed * 128 * 1024;
    if (state.completed > STALLED_MAX_WRITES) {
      throw new Error(
        `the producer wrote ${state.completed} chunks (${bytes / 1024 / 1024} MiB) ` +
          'with no reader: backpressure did not hold it',
      );
    }
    return `plateau at ${state.completed} completed writes (${(bytes / 1024 / 1024).toFixed(1)} MiB)`;
  } finally {
    socket.destroy();
  }
}

// ---------------------------------------------------------------------------
console.log(`release-smoke client: ${BASE}`);
console.log(
  `Node ${process.version}; SSE span ${SSE_SECONDS} s${QUICK ? ' (SMOKE_QUICK=1)' : ''}`,
);

// The two long checks run together: the SSE span dominates the gate, and
// the idle-body deadline is independent of it. Their lines print in check
// order once both finish.
for (const result of await Promise.all([
  runCheck(
    1,
    `SSE TransformStream stays alive for ${SSE_SECONDS} s`,
    checkSse,
    (SSE_SECONDS + 90) * 1000,
  ),
  runCheck(2, 'idle response body ends at ~60 s', checkIdleBody, 150000),
])) {
  report(result);
}
report(await runCheck(3, '20 MiB WebSocket message round-trips', checkWsRoundtrip, 240000));
report(
  await runCheck(4, '32 MiB WebSocket cap: 32 MiB passes, +1 closes 1009', checkWsCap, 420000),
);
report(await runCheck(5, '8 MiB tee(): A then B, both intact', checkTee, 180000));
report(
  await runCheck(
    6,
    'stalled reader plateaus the TransformStream producer',
    checkBackpressure,
    120000,
  ),
);

const ordered = results.sort((a, b) => a.id - b.id);
const passed = ordered.filter((result) => result.ok).length;
console.log('');
console.log(`release-smoke summary: ${passed}/${ordered.length} checks passed`);
for (const result of ordered) {
  console.log(`  check ${result.id}  ${result.ok ? 'PASS' : 'FAIL'}  ${result.title}`);
}
process.exit(passed === ordered.length ? 0 : 1);
