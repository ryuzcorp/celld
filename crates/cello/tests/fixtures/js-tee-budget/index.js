// A tee of a Worker-constructed ReadableStream must bound the skew between
// its branches, the way the host tee of a platform body does. The checks
// below run inside the isolate and report what they observed, so the test
// asserts behavior rather than the fixture's conclusions.
//
// `tee()` used to enqueue every chunk into both branches whatever the other
// branch did, so a branch nobody read buffered the whole source: one tenant
// could grow the shared fleet process's heap, and `near_heap_limit` extends
// the heap rather than failing.

// The budget and the error the JS tee shares with the host tee
// (HTTP_TEE_MAX_SKEW_BYTES / HTTP_TEE_SKEW_MESSAGE in http_streams.rs).
const BUDGET = 32 * 1024 * 1024;
const MESSAGE =
  'tee branch fell more than 32 MiB behind its sibling; read both branches concurrently';

// 1 MiB chunks: the lagging branch fills the 32 MiB budget after 33 of
// them, so a 36-chunk source is past the budget and still small enough to
// hold in an isolate.
const CHUNK_BYTES = 1024 * 1024;
const BYTE_CHUNKS = 36;

// The per-chunk floor `_teeChunkBytes` applies to every chunk, so empty
// chunks cannot be free.
const FLOOR = 64;

// A single chunk larger than the whole budget. Both copies must still get
// it when neither branch lags; only a branch that is actually behind may
// fail.
const OVERSIZE_BYTES = 40 * 1024 * 1024;

const json = (body, status = 200) => Response.json(body, { status });

const settle = async (promise) => {
  try {
    return { value: await promise, error: null };
  } catch (error) {
    return { value: null, error };
  }
};

/// A source of `count` byte chunks that records how much it handed out and
/// whether anything cancelled it.
const byteSource = (count, chunkBytes = CHUNK_BYTES, type = null) => {
  const state = { produced: 0, bytes: 0, canceled: false };
  const source = {
    pull(controller) {
      if (state.produced >= count) {
        controller.close();
        return;
      }
      const chunk = new Uint8Array(chunkBytes);
      chunk.fill(state.produced & 0xff);
      state.produced += 1;
      state.bytes += chunk.byteLength;
      controller.enqueue(chunk);
    },
    cancel() {
      state.canceled = true;
    },
  };
  if (type !== null) source.type = type;
  return { stream: new ReadableStream(source), state };
};

/// A source of one `chunk`, then end, that records whether anything
/// cancelled it.
const singleChunkSource = (chunk) => {
  const state = { sent: false, canceled: false };
  const stream = new ReadableStream({
    pull(controller) {
      if (state.sent) {
        controller.close();
        return;
      }
      state.sent = true;
      controller.enqueue(chunk);
    },
    cancel() {
      state.canceled = true;
    },
  });
  return { stream, state };
};

/// Read a branch to its end, reporting the bytes it yielded.
const drain = async (reader, size = (chunk) => chunk.byteLength) => {
  let bytes = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return bytes;
    bytes += size(value);
  }
};

/// (a) Read one branch to the end and never touch the other: the read
/// fails on the branch nobody drained, the drained branch completes every
/// byte, and the source is not cancelled while it is live.
const checkUnreadBranch = async (check) => {
  const { stream, state } = byteSource(BYTE_CHUNKS);
  const [first, second] = stream.tee();
  const received = await drain(first.getReader());
  check(
    'unread branch: the drained branch completes',
    received === state.bytes,
    `read ${received} of ${state.bytes} bytes`,
  );
  check(
    'unread branch: the source produced past the budget',
    state.bytes > BUDGET,
    `produced ${state.bytes} bytes`,
  );
  const { error } = await settle(second.getReader().read());
  check(
    'unread branch: the unread branch read rejects',
    error !== null && error.message === MESSAGE,
    `rejected with ${error}`,
  );
  check(
    'unread branch: the rejection is the host tee\'s error',
    error !== null && error.name === 'Error' && error instanceof Error,
    `name ${error === null ? 'none' : error.name}`,
  );
  check(
    'unread branch: the source is not cancelled',
    state.canceled === false,
    'the source cancel ran while the drained branch was still reading',
  );
};

/// (b) Both branches read in lockstep pass well past the budget: with no
/// skew there is nothing to charge.
const checkLockstep = async (check) => {
  const { stream, state } = byteSource(80);
  const [first, second] = stream.tee();
  const a = first.getReader();
  const b = second.getReader();
  let total = 0;
  let endedApart = false;
  for (;;) {
    const [x, y] = await Promise.all([a.read(), b.read()]);
    if (x.done !== y.done) endedApart = true;
    if (x.done) break;
    total += x.value.byteLength + y.value.byteLength;
  }
  check(
    'lockstep: both branches end together',
    endedApart === false,
    'one branch ended before the other',
  );
  check(
    'lockstep: both copies see every byte',
    total === 2 * state.bytes,
    `${total} of ${2 * state.bytes} bytes`,
  );
  check(
    'lockstep: the source passed twice the budget',
    state.bytes > 2 * BUDGET,
    `produced ${state.bytes} bytes`,
  );
};

/// (c) Non-byte chunks are bounded too: a string is charged 2 bytes per
/// UTF-16 code unit, so a stream of strings cannot buffer without limit
/// either.
const checkStringChunks = async (check) => {
  const chunkLength = 8 * 1024;
  // 2048 chunks of 8 KiB code units fill the budget exactly at 2 bytes
  // each; the next chunk fails the lagging branch.
  const count = 2100;
  const state = { produced: 0, bytes: 0, canceled: false };
  const stream = new ReadableStream({
    pull(controller) {
      if (state.produced >= count) {
        controller.close();
        return;
      }
      const chunk = 'x'.repeat(chunkLength);
      state.produced += 1;
      state.bytes += chunk.length * 2;
      controller.enqueue(chunk);
    },
    cancel() {
      state.canceled = true;
    },
  });
  const [first, second] = stream.tee();
  const received = await drain(
    first.getReader(), (chunk) => chunk.length * 2);
  check(
    'strings: the drained branch completes',
    received === state.bytes,
    `read ${received} of ${state.bytes} bytes`,
  );
  check(
    'strings: the source produced past the budget',
    state.bytes > BUDGET,
    `produced ${state.bytes} bytes`,
  );
  const { error } = await settle(second.getReader().read());
  check(
    'strings: the unread branch read rejects',
    error !== null && error.message === MESSAGE,
    `rejected with ${error}`,
  );
  check(
    'strings: the source is not cancelled',
    state.canceled === false,
    'the source cancel ran while the drained branch was still reading',
  );
};

/// (d) `Response.clone()` of a streaming body tees it, so a clone nobody
/// reads is the same unbounded branch.
const checkClonedResponse = async (check) => {
  const { stream, state } = byteSource(BYTE_CHUNKS);
  const response = new Response(stream);
  const copy = response.clone();
  const received = await drain(response.body.getReader());
  check(
    'clone: the retained copy completes',
    received === state.bytes,
    `read ${received} of ${state.bytes} bytes`,
  );
  const { error } = await settle(copy.body.getReader().read());
  check(
    'clone: the unread copy rejects',
    error !== null && error.message === MESSAGE,
    `rejected with ${error}`,
  );
  check(
    'clone: the source is not cancelled',
    state.canceled === false,
    'the source cancel ran while the retained copy was still reading',
  );
};

/// (e) Byte-stream branches are accounted in bytes as well, including the
/// BYOB reader that reads the lagging branch.
const checkByteStreamBranches = async (check) => {
  const { stream, state } = byteSource(BYTE_CHUNKS, CHUNK_BYTES, 'bytes');
  const [first, second] = stream.tee();
  const received = await drain(first.getReader());
  check(
    'byob: the drained branch completes',
    received === state.bytes,
    `read ${received} of ${state.bytes} bytes`,
  );
  const byob = second.getReader({ mode: 'byob' });
  const { error } = await settle(byob.read(new Uint8Array(1024)));
  check(
    'byob: the lagging branch read rejects',
    error !== null && error.message === MESSAGE,
    `rejected with ${error}`,
  );
  check(
    'byob: the source is not cancelled',
    state.canceled === false,
    'the source cancel ran while the drained branch was still reading',
  );
};

/// (f) A single chunk larger than the budget is not lag: both branches
/// read it in lockstep and neither is failed, so neither copy is lost and
/// the source is never cancelled.
const checkOversizeLockstep = async (check) => {
  const chunk = new Uint8Array(OVERSIZE_BYTES);
  const { stream, state } = singleChunkSource(chunk);
  const [first, second] = stream.tee();
  const a = first.getReader();
  const b = second.getReader();
  const both = await settle(Promise.all([a.read(), b.read()]));
  check(
    'oversize lockstep: both branches receive the chunk',
    both.error === null && both.value !== null &&
    both.value[0].done === false && both.value[1].done === false &&
    both.value[0].value.byteLength === OVERSIZE_BYTES &&
    both.value[1].value.byteLength === OVERSIZE_BYTES,
    both.error !== null ? `rejected with ${both.error}` :
      `first ${both.value[0].done ? 'done' : 'chunk'}, ` +
      `second ${both.value[1].done ? 'done' : 'chunk'}`,
  );
  const ends = await settle(Promise.all([a.read(), b.read()]));
  check(
    'oversize lockstep: neither branch failed',
    ends.error === null && ends.value !== null &&
    ends.value[0].done === true && ends.value[1].done === true,
    ends.error !== null ? `rejected with ${ends.error}` :
      `first ${ends.value[0].done}, second ${ends.value[1].done}`,
  );
  check(
    'oversize lockstep: the source is not cancelled',
    state.canceled === false,
    'the source cancel ran although no branch lagged',
  );
};

/// (g) The same chunk with one branch unread: the read branch still
/// receives all of it, and only the unread branch fails, once it holds
/// more than the budget the read branch has already taken.
const checkOversizeUnread = async (check) => {
  const chunk = new Uint8Array(OVERSIZE_BYTES);
  const { stream, state } = singleChunkSource(chunk);
  const [first, second] = stream.tee();
  const reader = first.getReader();
  const read = await settle(reader.read());
  check(
    'oversize unread: the read branch receives the whole chunk',
    read.error === null && read.value !== null &&
    read.value.done === false &&
    read.value.value.byteLength === OVERSIZE_BYTES,
    `read ${read.error !== null ? read.error :
      read.value.value && read.value.value.byteLength} bytes`,
  );
  const end = await settle(reader.read());
  check(
    'oversize unread: the read branch ends',
    end.error === null && end.value !== null && end.value.done === true,
    `settled with ${end.error !== null ? end.error : 'a chunk'}`,
  );
  const unread = await settle(second.getReader().read());
  check(
    'oversize unread: only the unread branch fails',
    unread.error !== null && unread.error.message === MESSAGE,
    `rejected with ${unread.error}`,
  );
  check(
    'oversize unread: the source is not cancelled',
    state.canceled === false,
    'the source cancel ran while the read branch was still live',
  );
};

/// (h) The accounting probe must be total and trap-free: a fake
/// ArrayBuffer (whose native byteLength getter throws) and a Proxy whose
/// traps would throw are ordinary chunks charged the floor, so both
/// branches get them and neither is stranded.
const checkHostileChunks = async (check) => {
  const state = { sent: 0, traps: 0, canceled: false };
  const fake = Object.create(ArrayBuffer.prototype);
  Object.defineProperty(fake, 'byteLength', {
    get() {
      state.traps += 1;
      throw new TypeError('the tee read a fake ArrayBuffer');
    },
  });
  const proxy = new Proxy({ payload: 'hidden' }, {
    get(target, key, receiver) {
      state.traps += 1;
      return Reflect.get(target, key, receiver);
    },
    getPrototypeOf() {
      state.traps += 1;
      return ArrayBuffer.prototype;
    },
    has() {
      state.traps += 1;
      return false;
    },
    ownKeys() {
      state.traps += 1;
      return [];
    },
  });
  const stream = new ReadableStream({
    pull(controller) {
      if (state.sent >= 2) {
        controller.close();
        return;
      }
      controller.enqueue(state.sent === 0 ? fake : proxy);
      state.sent += 1;
    },
    cancel() {
      state.canceled = true;
    },
  });
  const readAll = async (reader) => {
    const values = [];
    for (;;) {
      const { value, done } = await reader.read();
      if (done) return values;
      values.push(value);
    }
  };
  const [first, second] = stream.tee();
  const settled = await settle(Promise.all([
    readAll(first.getReader()), readAll(second.getReader())]));
  check(
    'hostile chunks: both branches settle',
    settled.error === null,
    `read rejected with ${settled.error}`,
  );
  const [left, right] = settled.value || [[], []];
  check(
    'hostile chunks: both branches receive both chunks',
    left.length === 2 && right.length === 2 &&
    left[0] === fake && left[1] === proxy &&
    right[0] === fake && right[1] === proxy,
    `first ${left.length}, second ${right.length} chunks`,
  );
  check(
    'hostile chunks: the probe ran no chunk code',
    state.traps === 0,
    `ran ${state.traps} chunk getters/traps`,
  );
  check(
    'hostile chunks: the source is not cancelled',
    state.canceled === false,
    'the source cancel ran for a chunk that is merely unusual',
  );
};

/// (i) No throw may strand a branch: if an enqueue throws after the first
/// branch already has the chunk, the sibling still settles and the source
/// reader is released (its cancel runs).
const checkEnqueueThrow = async (check) => {
  const { stream, state } = byteSource(1);
  const [first, second] = stream.tee();
  const reader1 = first.getReader();
  const reader2 = second.getReader();
  // Close branch 2's controller underneath the tee: its next enqueue
  // throws, after branch 1 already got the chunk.
  second._controller.close();
  const left = await settle(reader1.read());
  const right = await settle(reader2.read());
  check(
    'enqueue throw: both branches settle',
    left.error === null && left.value !== null &&
    left.value.done === false && right.error !== null,
    `first ${left.error !== null ? left.error : 'a chunk'}, ` +
    `second ${right.error !== null ? right.error : 'a chunk'}`,
  );
  check(
    'enqueue throw: the sibling gets the same error',
    right.error !== null &&
    right.error.message === 'Cannot enqueue a closed/errored stream',
    `rejected with ${right.error}`,
  );
  const after = await settle(reader1.read());
  check(
    'enqueue throw: the teed stream is closed, not left readable',
    after.error !== null && after.error === right.error,
    `read settled with ${after.error !== null ? after.error :
      (after.value && after.value.done) ? 'done' : 'a chunk'}`,
  );
  check(
    'enqueue throw: the source cancel ran',
    state.canceled === true,
    'the source reader was left holding the body',
  );
};

/// (j) Zero-length chunks are not free: each still costs the per-chunk
/// floor, so an unread branch fed only empty strings reaches the budget.
const checkEmptyChunks = async (check) => {
  // 33554432 / 64 = 524288 chunks fill the budget exactly; the next one
  // puts the unread branch past it.
  const count = BUDGET / FLOOR + 1;
  const state = { produced: 0, canceled: false };
  const stream = new ReadableStream({
    pull(controller) {
      if (state.produced >= count) {
        controller.close();
        return;
      }
      state.produced += 1;
      controller.enqueue('');
    },
    cancel() {
      state.canceled = true;
    },
  });
  const [first, second] = stream.tee();
  const reader = first.getReader();
  let received = 0;
  let nonEmpty = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    received += 1;
    if (value !== '') nonEmpty += 1;
  }
  check(
    'empty chunks: the drained branch completes',
    received === count && nonEmpty === 0,
    `read ${received} of ${count} chunks (${nonEmpty} non-empty)`,
  );
  const { error } = await settle(second.getReader().read());
  check(
    'empty chunks: the unread branch fails at the floor',
    error !== null && error.message === MESSAGE,
    `rejected with ${error}`,
  );
  check(
    'empty chunks: the source is not cancelled',
    state.canceled === false,
    'the source cancel ran while the drained branch was still reading',
  );
};

export default {
  async fetch(request) {
    const { pathname } = new URL(request.url);
    if (pathname === '/health') return new Response('ok');
    if (pathname !== '/conformance')
      return json({ error: 'not found' }, 404);
    const failures = [];
    const check = (name, ok, detail) => {
      if (!ok) failures.push(`${name}: ${detail}`);
    };
    await checkUnreadBranch(check);
    await checkLockstep(check);
    await checkStringChunks(check);
    await checkClonedResponse(check);
    await checkByteStreamBranches(check);
    await checkOversizeLockstep(check);
    await checkOversizeUnread(check);
    await checkEnqueueThrow(check);
    await checkEmptyChunks(check);
    // Last: on a tee whose probe runs chunk code this check strands the
    // sibling branch, so it is the one that can hang the whole route.
    await checkHostileChunks(check);
    return json({ failures });
  },
};
