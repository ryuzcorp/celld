// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! zlib: the one-shot `__zlib` op and the streaming compression backend.
//!
//! `CompressionStream` and `DecompressionStream` need output chunk by chunk,
//! so a stream keeps a live flate2 writer whose `Vec` sink is drained after
//! every push. The one-shot op has no such state and runs to completion.
//!
//! A live writer is native memory V8 neither sees nor bounds: a gzip
//! encoder holds roughly 300 KiB, a decoder roughly 75 KiB. Three rules
//! bound it. The table is an isolate slot, so retiring an isolate frees
//! every stream it still holds. JS releases a stream on its last flush,
//! on cancel or abort, and through a `FinalizationRegistry` when a stream
//! is abandoned mid-flight. And an isolate holds at most
//! [`MAX_LIVE_ZLIB_STREAMS`] at once, so a tenant that keeps streams
//! reachable, or outruns the collector, is refused instead of growing the
//! process.
use super::*;

pub(super) fn op_zlib(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    use std::io::{Read, Write};
    let mode = args.get(0).to_rust_string_lossy(scope);
    let bytes = view_bytes(args.get(1)).unwrap_or_default();
    let result = (|| -> Result<Vec<u8>> {
        match mode.as_str() {
            "gzip" => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&bytes)?;
                Ok(encoder.finish()?)
            }
            "gunzip" => {
                let mut decoder = flate2::read::GzDecoder::new(bytes.as_slice());
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            "deflate" => {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&bytes)?;
                Ok(encoder.finish()?)
            }
            "inflate" => {
                let mut decoder = flate2::read::ZlibDecoder::new(bytes.as_slice());
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            "deflateRaw" => {
                let mut encoder =
                    flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(&bytes)?;
                Ok(encoder.finish()?)
            }
            "inflateRaw" => {
                let mut decoder = flate2::read::DeflateDecoder::new(bytes.as_slice());
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            _ => Err(anyhow!("unsupported zlib operation")),
        }
    })();
    match result {
        Ok(bytes) => webcrypto_return_bytes(scope, rv, &bytes),
        Err(error) => {
            let message = v8::String::new(scope, &format!("zlib: {error}")).unwrap();
            let exception = v8::Exception::error(scope, message);
            scope.throw_exception(exception);
        }
    }
}

/// One live `CompressionStream`/`DecompressionStream`: a stateful flate2
/// writer whose `Vec` sink is drained after every push, so output streams
/// chunk by chunk instead of arriving once at close.
enum ZlibStream {
    GzipEncode(flate2::write::GzEncoder<Vec<u8>>),
    GzipDecode(flate2::write::GzDecoder<Vec<u8>>),
    ZlibEncode(flate2::write::ZlibEncoder<Vec<u8>>),
    ZlibDecode(flate2::write::ZlibDecoder<Vec<u8>>),
    RawEncode(flate2::write::DeflateEncoder<Vec<u8>>),
    RawDecode(flate2::write::DeflateDecoder<Vec<u8>>),
}

/// The six writer types share `write`/`flush`/`get_mut`/`finish` but no
/// trait, so dispatch once here.
macro_rules! zlib_each {
    ($stream:expr, $w:ident => $body:expr) => {
        match $stream {
            ZlibStream::GzipEncode($w) => $body,
            ZlibStream::GzipDecode($w) => $body,
            ZlibStream::ZlibEncode($w) => $body,
            ZlibStream::ZlibDecode($w) => $body,
            ZlibStream::RawEncode($w) => $body,
            ZlibStream::RawDecode($w) => $body,
        }
    };
}

impl ZlibStream {
    fn new(format: &str, decompress: bool) -> Option<ZlibStream> {
        use flate2::write as z;
        let level = flate2::Compression::default();
        let sink = Vec::new;
        Some(match (format, decompress) {
            ("gzip", false) => Self::GzipEncode(z::GzEncoder::new(sink(), level)),
            ("gzip", true) => Self::GzipDecode(z::GzDecoder::new(sink())),
            ("deflate", false) => Self::ZlibEncode(z::ZlibEncoder::new(sink(), level)),
            ("deflate", true) => Self::ZlibDecode(z::ZlibDecoder::new(sink())),
            ("deflate-raw", false) => Self::RawEncode(z::DeflateEncoder::new(sink(), level)),
            ("deflate-raw", true) => Self::RawDecode(z::DeflateDecoder::new(sink())),
            _ => return None,
        })
    }

    fn decompress(&self) -> bool {
        matches!(
            self,
            Self::GzipDecode(_) | Self::ZlibDecode(_) | Self::RawDecode(_)
        )
    }

    /// Workerd's canonical message, matched by its conformance suite.
    fn failure(&self) -> &'static str {
        if self.decompress() {
            "Decompression failed."
        } else {
            "Compression failed."
        }
    }

    /// Write one chunk and return whatever output it produced. Decoded
    /// output flows per chunk; encoders hold theirs until a full buffer
    /// accumulates (matching Workerd, whose suite reads a small message
    /// back as one chunk), with the rest arriving from `finish`. A
    /// decoder that stops consuming input has hit the end of the
    /// compressed stream, so leftover bytes are trailing garbage —
    /// Workerd's strict_compression_checks.
    fn push(&mut self, mut input: &[u8]) -> Result<Vec<u8>> {
        use std::io::Write;
        const ENCODER_CHUNK: usize = 32 * 1024;
        let failure = self.failure();
        let held = if self.decompress() { 0 } else { ENCODER_CHUNK };
        zlib_each!(self, w => {
            while !input.is_empty() {
                let n = w.write(input).map_err(|_| anyhow!(failure))?;
                if n == 0 { return Err(anyhow!(failure)); }
                input = &input[n..];
            }
            if w.get_ref().len() < held { return Ok(Vec::new()); }
            Ok(std::mem::take(w.get_mut()))
        })
    }

    /// Terminate the stream and return the final output. For gzip this
    /// also validates the trailer, so a truncated stream errors here.
    fn finish(self) -> Result<Vec<u8>> {
        let failure = self.failure();
        zlib_each!(self, w => w.finish().map_err(|_| anyhow!(failure)))
    }
}

/// The most native compression streams one isolate may hold at once.
///
/// Sized against the default 128 MiB V8 heap: 128 gzip encoders hold about
/// 38 MiB, decoders about a quarter of that. A stateless isolate serves a
/// couple of requests before the pool grows another, and a cell isolate
/// hosts up to 32 cells, so a live stream per request or per cell is far
/// below it. Reaching it means streams were left open faster than the
/// collector frees them; the constructor then throws a `RangeError`.
const MAX_LIVE_ZLIB_STREAMS: usize = 128;

/// Live compression streams for one isolate, keyed by an id JS holds across
/// awaits.
///
/// An isolate slot, for the reason `TextDecoders` in `js.rs` is one: a
/// `CompressionStream` is a JS object, so it never leaves the isolate that
/// made it, and every op that touches it runs under that isolate's
/// `v8::Locker` — whichever tokio worker drives the turn. It used to be a
/// process-wide table, which nothing but JS ever emptied: a stream left
/// open stayed for the life of the process, after the isolate that made it
/// was gone. Dropping the isolate now drops the table.
///
/// Ids count up per isolate and are never reused, so a finalizer's late
/// release of a stream that already ended is a no-op.
#[derive(Default)]
struct ZlibStreams {
    live: HashMap<u64, ZlibStream>,
    next: u64,
    /// Dropped with the table, so a test can see the isolate's teardown
    /// release it. Never read: its drop is the observation.
    #[cfg(test)]
    #[allow(dead_code)]
    teardown_probe: Option<Arc<()>>,
}

/// The calling isolate's stream table, created on first use.
fn zlib_streams<'a>(scope: &'a mut v8::PinScope) -> &'a mut ZlibStreams {
    if scope.get_slot::<ZlibStreams>().is_none() {
        scope.set_slot(ZlibStreams::default());
    }
    scope.get_slot_mut::<ZlibStreams>().expect("just set")
}

fn zlib_stream_return(
    scope: &mut v8::PinScope,
    rv: v8::ReturnValue<v8::Value>,
    result: Result<Vec<u8>>,
) {
    match result {
        Ok(bytes) => webcrypto_return_bytes(scope, rv, &bytes),
        Err(error) => {
            let message = v8::String::new(scope, &error.to_string()).unwrap();
            let exception = v8::Exception::type_error(scope, message);
            scope.throw_exception(exception);
        }
    }
}

/// `__zlib_stream_new(format, decompress)` -> id. JS validates the format
/// and throws the spec TypeError, so an unknown one here is a bug. An
/// isolate already holding [`MAX_LIVE_ZLIB_STREAMS`] gets a `RangeError`
/// and no native state.
pub(super) fn op_zlib_stream_new(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let format = args.get(0).to_rust_string_lossy(scope);
    let decompress = args.get(1).boolean_value(scope);
    let streams = zlib_streams(scope);
    if streams.live.len() >= MAX_LIVE_ZLIB_STREAMS {
        let message = v8::String::new(
            scope,
            &format!(
                "Too many open compression streams in this isolate (limit \
                 {MAX_LIVE_ZLIB_STREAMS}); close, cancel or abort the ones \
                 no longer in use."
            ),
        )
        .unwrap();
        let exception = v8::Exception::range_error(scope, message);
        scope.throw_exception(exception);
        return;
    }
    let stream = ZlibStream::new(&format, decompress).expect("JS validated the compression format");
    streams.next += 1;
    let id = streams.next;
    streams.live.insert(id, stream);
    rv.set(v8::Number::new(scope, id as f64).into());
}

/// `__zlib_stream_push(id, view)` -> Uint8Array (possibly empty). An
/// error poisons the stream: its native state is dropped and the
/// TypeError errors both sides of the transform.
pub(super) fn op_zlib_stream_push(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let bytes = view_bytes(args.get(1)).unwrap_or_default();
    let streams = &mut zlib_streams(scope).live;
    let result = match streams.get_mut(&id) {
        Some(stream) => {
            let out = stream.push(&bytes);
            if out.is_err() {
                streams.remove(&id);
            }
            out
        }
        None => Err(anyhow!("zlib stream already closed")),
    };
    zlib_stream_return(scope, rv, result);
}

/// `__zlib_stream_end(id)` -> the final Uint8Array. Consumes the stream.
pub(super) fn op_zlib_stream_end(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    let result = zlib_streams(scope)
        .live
        .remove(&id)
        .ok_or_else(|| anyhow!("zlib stream already closed"));
    zlib_stream_return(scope, rv, result.and_then(ZlibStream::finish));
}

/// `__zlib_stream_drop(id)` — cancelled/aborted transform cleanup, and the
/// `FinalizationRegistry` release of an abandoned stream. An id that
/// already ended is a no-op.
pub(super) fn op_zlib_stream_drop(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = args.get(0).integer_value(scope).unwrap_or(0) as u64;
    zlib_streams(scope).live.remove(&id);
}

/// Native stream lifetimes, observed in the isolate's own table: every way
/// a stream ends releases its entry, the cap refuses the stream past it,
/// collection releases an abandoned stream, and dropping the isolate drops
/// the table. Every stream here is a small deflate-raw decoder or a short
/// round trip, so a full table stays near 10 MiB.
#[cfg(test)]
mod tests {
    use super::{ZlibStreams, MAX_LIVE_ZLIB_STREAMS};
    use crate::js::{Compat, RequestBody, Worker, WorkerConfig, WorkerConfigOptions};
    use std::sync::Arc;
    use std::time::Instant;

    const SCRIPT: &str = r#"
const held = [];
async function drain(readable) {
  const reader = readable.getReader();
  const parts = [];
  let length = 0;
  for (;;) {
    const { value, done } = await reader.read();
    if (done) break;
    parts.push(value);
    length += value.byteLength;
  }
  const out = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) { out.set(part, offset); offset += part.byteLength; }
  return out;
}
async function through(stream, input) {
  const writer = stream.writable.getWriter();
  const output = drain(stream.readable);
  await writer.write(input);
  await writer.close();
  return output;
}
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const n = Number(url.searchParams.get("n") ?? 0);
    switch (url.pathname) {
      case "/roundtrip": {
        const input = new TextEncoder().encode("cello ".repeat(4096));
        for (const format of ["gzip", "deflate", "deflate-raw"]) {
          const packed = await through(new CompressionStream(format), input);
          const back = await through(new DecompressionStream(format), packed);
          if (packed.byteLength >= input.byteLength ||
              back.byteLength !== input.byteLength ||
              !back.every((byte, i) => byte === input[i]))
            return Response.json({ ok: false, format });
        }
        return Response.json({ ok: true });
      }
      case "/release": {
        // Held, so only the stream's own ending can release its entry.
        const cancelled = new CompressionStream("gzip");
        held.push(cancelled);
        await cancelled.readable.cancel("done");
        const aborted = new DecompressionStream("deflate");
        held.push(aborted);
        await aborted.writable.abort("done");
        const poisoned = new DecompressionStream("gzip");
        held.push(poisoned);
        const writer = poisoned.writable.getWriter();
        const read = drain(poisoned.readable).then(() => "resolved", (e) => e.name);
        const wrote = await writer.write(new Uint8Array(16).fill(7))
          .then(() => "resolved", (e) => e.name);
        // A non-BufferSource chunk fails before any push, so the transform
        // must still release its native entry.
        const invalid = new CompressionStream("gzip");
        held.push(invalid);
        const iwriter = invalid.writable.getWriter();
        const iread = drain(invalid.readable).then(() => "resolved", (e) => e.name);
        const iwrote = await iwriter.write(123)
          .then(() => "resolved", (e) => e.name);
        return Response.json({ wrote, read: await read, iwrote, iread: await iread });
      }
      case "/fill":
        for (let i = 0; i < n; i++) held.push(new DecompressionStream("deflate-raw"));
        return Response.json({ ok: true });
      case "/one":
        try {
          held.push(new DecompressionStream("deflate-raw"));
          return Response.json({ ok: true });
        } catch (e) {
          return Response.json({ ok: false, name: e.name, message: e.message });
        }
      case "/cancel-one":
        await held.pop().readable.cancel("done");
        return Response.json({ ok: true });
      case "/abandon":
        for (let i = 0; i < n; i++) new DecompressionStream("deflate-raw");
        return Response.json({ ok: true });
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

    /// Runs `test` on a thread created after V8 initialised: the PKRU
    /// access an isolate needs is inherited at thread creation. The test-only
    /// constructor in `runtime` has already initialised V8 on the process's
    /// initial thread (see `init_v8`), so no libtest thread can be the one
    /// that wins the one-time grant; this call only states the dependency.
    fn on_engine_thread(test: impl FnOnce() + Send + 'static) {
        crate::runtime::init_v8();
        crate::asyncrt::shared_test_runtime();
        std::thread::spawn(test).join().expect("engine thread");
    }

    fn load() -> Worker {
        let config = WorkerConfig::new(WorkerConfigOptions {
            src: SCRIPT.to_string(),
            script_name: "compression-lifecycle".to_string(),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            r2_bindings: Vec::new(),
            d1_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: String::new(),
            modules: Vec::new(),
            compat: Compat::default(),
        });
        Worker::load_config(Arc::new(config)).expect("load worker")
    }

    /// One request whose handler settles on promises alone, so its first
    /// turn is the whole request. Answers the JSON body.
    fn fetch(worker: &mut Worker, path: &str) -> serde_json::Value {
        let (reply, mut receive) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Fetch {
            queued_at: Instant::now(),
            entrypoint: None,
            invocation_limits: None,
            url: format!("http://test{path}"),
            method: "GET".to_string(),
            body: RequestBody::Bytes(bytes::Bytes::new()),
            headers: Vec::new(),
            request_id: None,
            tail_report: None,
            reply,
        };
        let (entry, started) = worker.turn_begin(job, None);
        drop(started);
        let mut entry = entry.expect("request starts");
        entry.abandon();
        drop(entry);
        let response = receive
            .try_recv()
            .expect("answered in one turn")
            .expect("ok answer");
        assert_eq!(response.status, 200, "{path} answered {}", response.status);
        serde_json::from_slice(&response.body).expect("json body")
    }

    fn live(worker: &Worker) -> usize {
        let inner = worker.inner.as_ref().expect("live worker");
        let (locker, _cells) = inner.lock();
        locker
            .get_slot::<ZlibStreams>()
            .map_or(0, |streams| streams.live.len())
    }

    /// A full collection, then the foreground tasks it posted — V8 runs
    /// `FinalizationRegistry` callbacks from one — until none is runnable.
    fn collect(worker: &mut Worker) {
        worker.settle_heap();
        let inner = worker.inner.as_ref().expect("live worker");
        let (locker, _cells) = inner.lock();
        let platform = v8::V8::get_current_platform();
        for _ in 0..1024 {
            if !v8::Platform::pump_message_loop(&platform, &locker, false) {
                return;
            }
        }
        panic!("foreground tasks kept coming after 1024 runs");
    }

    /// Every format round-trips chunk by chunk, and a stream that flushes
    /// leaves nothing behind.
    #[test]
    fn round_trip_releases_on_flush() {
        on_engine_thread(|| {
            let mut worker = load();
            assert_eq!(fetch(&mut worker, "/roundtrip")["ok"], true);
            assert_eq!(live(&worker), 0);
        });
    }

    /// Cancel, abort, a poisoning push and an invalid chunk each release
    /// the native stream while JS still holds the stream object.
    #[test]
    fn cancel_abort_and_push_error_release_while_held() {
        on_engine_thread(|| {
            let mut worker = load();
            let report = fetch(&mut worker, "/release");
            assert_eq!(report["wrote"], "TypeError", "{report}");
            assert_eq!(report["read"], "TypeError", "{report}");
            assert_eq!(report["iwrote"], "TypeError", "{report}");
            assert_eq!(report["iread"], "TypeError", "{report}");
            assert_eq!(live(&worker), 0);
        });
    }

    /// The stream past the cap is refused with a `RangeError` and no
    /// native state; releasing one admits the next.
    #[test]
    fn cap_refuses_the_stream_past_it() {
        on_engine_thread(|| {
            let mut worker = load();
            fetch(&mut worker, &format!("/fill?n={MAX_LIVE_ZLIB_STREAMS}"));
            assert_eq!(live(&worker), MAX_LIVE_ZLIB_STREAMS);
            let refused = fetch(&mut worker, "/one");
            assert_eq!(refused["ok"], false, "{refused}");
            assert_eq!(refused["name"], "RangeError", "{refused}");
            assert_eq!(live(&worker), MAX_LIVE_ZLIB_STREAMS);
            fetch(&mut worker, "/cancel-one");
            assert_eq!(live(&worker), MAX_LIVE_ZLIB_STREAMS - 1);
            assert_eq!(fetch(&mut worker, "/one")["ok"], true);
            assert_eq!(live(&worker), MAX_LIVE_ZLIB_STREAMS);
        });
    }

    /// Streams nobody closed and nobody references are released once the
    /// collector finds them, which frees the cap for new streams.
    #[test]
    fn abandoned_streams_release_after_collection() {
        on_engine_thread(|| {
            let mut worker = load();
            fetch(&mut worker, &format!("/abandon?n={MAX_LIVE_ZLIB_STREAMS}"));
            assert_eq!(live(&worker), MAX_LIVE_ZLIB_STREAMS);
            collect(&mut worker);
            assert_eq!(live(&worker), 0);
            assert_eq!(fetch(&mut worker, "/one")["ok"], true);
        });
    }

    /// The cap and the table belong to one isolate: a full isolate does not
    /// refuse another's stream, and dropping it drops every stream it still
    /// held, reachable from JS or not.
    #[test]
    fn isolate_teardown_drops_its_streams() {
        on_engine_thread(|| {
            let mut full = load();
            let mut other = load();
            fetch(&mut full, &format!("/fill?n={MAX_LIVE_ZLIB_STREAMS}"));
            assert_eq!(fetch(&mut other, "/one")["ok"], true);
            let probe = Arc::new(());
            let released = Arc::downgrade(&probe);
            {
                let inner = full.inner.as_ref().expect("live worker");
                let (mut locker, _cells) = inner.lock();
                let streams = locker
                    .get_slot_mut::<ZlibStreams>()
                    .expect("the isolate made streams");
                assert_eq!(streams.live.len(), MAX_LIVE_ZLIB_STREAMS);
                streams.teardown_probe = Some(probe);
            }
            drop(full);
            assert!(
                released.upgrade().is_none(),
                "the retired isolate's stream table outlived it"
            );
            assert_eq!(live(&other), 1);
        });
    }
}
