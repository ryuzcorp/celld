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
    return json({ failures });
  },
};
