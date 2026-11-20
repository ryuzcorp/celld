// CompressionStream / DecompressionStream -- WHATWG Compression Streams,
// each a TransformStream over one stateful native zlib stream
// (__zlib_stream_new / push / end / drop). Output streams chunk by chunk
// as input is written; the terminal block arrives from flush() when the
// writable side closes. The native state is released on that flush, on
// cancel or abort, on a push error, and -- for a stream abandoned
// mid-flight -- by a FinalizationRegistry once the transformer, which
// both sides keep alive, is collected. The isolate's table also goes
// when the isolate does, and caps how many streams can be live at once.
//
// Compiled on first access (see LAZY_GLOBALS); a bundle that never names
// either global never pays for this file.
return (() => {

// Per spec the chunk must be a BufferSource; anything else is a
// TypeError, which errors both sides of the transform.
function bytes(chunk) {
  if (ArrayBuffer.isView(chunk))
    return new Uint8Array(chunk.buffer, chunk.byteOffset,
                          chunk.byteLength);
  if (chunk instanceof ArrayBuffer) return new Uint8Array(chunk);
  throw new TypeError(
    'The provided value is not of type (ArrayBuffer or ' +
    'ArrayBufferView).');
}

// Releases the native stream of a collected transformer. Ids are never
// reused, so a release after flush or cancel already freed it is a
// no-op. Created with the first stream, not when this file compiles.
let registry;

function make(format, decompress) {
  format = String(format);
  if (format !== 'gzip' && format !== 'deflate' &&
      format !== 'deflate-raw')
    throw new TypeError(
      `Unsupported compression format: '${format}'`);
  let id;
  // The TransformStream's readable and writable sides both close over
  // this object, so it is collectable exactly when neither side is
  // reachable any more.
  const transformer = {
    transform(chunk, controller) {
      let out;
      try {
        out = __zlib_stream_push(id, bytes(chunk));
        if (out.byteLength) controller.enqueue(out);
      } catch (e) {
        __zlib_stream_drop(id);
        throw e;
      }
    },
    flush(controller) {
      const out = __zlib_stream_end(id);
      if (out.byteLength) controller.enqueue(out);
    },
    cancel() { __zlib_stream_drop(id); },
  };
  const ts = new TransformStream(transformer);
  // Workerd's compression streams are internal streams: a
  // pending read() rejects with the cancel reason instead
  // of resolving {done: true}.
  ts.readable._cancelRejectsReads = true;
  // Last, so nothing after it can throw and strand the native stream.
  // Throws a RangeError when the isolate holds too many.
  id = __zlib_stream_new(format, decompress);
  try {
    registry ??= new FinalizationRegistry(__zlib_stream_drop);
    registry.register(transformer, id);
  } catch (e) {
    __zlib_stream_drop(id);
    throw e;
  }
  return ts;
}

class CompressionStream {
  constructor(format) {
    const ts = make(format, false);
    this.readable = ts.readable;
    this.writable = ts.writable;
  }
}

class DecompressionStream {
  constructor(format) {
    const ts = make(format, true);
    this.readable = ts.readable;
    this.writable = ts.writable;
  }
}

for (const [cls, name] of [
  [CompressionStream, 'CompressionStream'],
  [DecompressionStream, 'DecompressionStream'],
])
  Object.defineProperty(cls.prototype, Symbol.toStringTag,
                        { value: name, configurable: true });

return { CompressionStream, DecompressionStream };
})()
