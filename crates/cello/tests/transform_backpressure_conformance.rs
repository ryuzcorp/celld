// Modified by Ryuz Corp (Cello): TransformStream backpressure conformance.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A black-box runner, not node code: it boots the binary, copies the
// fixture, and reads TCP with wall-clock deadlines, so the execution
// boundary's injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A `TransformStream` applies backpressure (WHATWG Streams). Its writable
//! side used to resolve `write()` as soon as the transform (or the default
//! enqueue) returned, whether or not anybody read the readable side. A
//! Worker streaming a response out of a `TransformStream` — the shape
//! `const {readable, writable} = new TransformStream(); return new
//! Response(readable)` plus `await writer.write(...)` — therefore buffered
//! the whole body in the isolate's heap while the HTTP client did not read,
//! because `near_heap_limit` grows the heap instead of failing, one
//! non-reading client could run the fleet process out of memory.
//!
//! The fix runs the spec handshake: `[[backpressure]]` starts true (the
//! readable defaults to a highWaterMark of 0), the readable's `pull` clears
//! it, an `enqueue` re-arms it once the readable no longer wants data, and
//! the default sink write waits on `[[backpressureChangePromise]]` before
//! running the transform. `writer.write()` and `writer.ready` stay pending
//! until the consumer pulls, end to end through the response-body pump.
//!
//! Two tests: spec values asserted inside the isolate (`/conformance`), and
//! a real stalled HTTP client against a tight producer, whose completed
//! writes must stay at the socket and host buffers instead of reaching the
//! fixture's cap.

mod common;

use common::{announced, cello_command, forward_announce};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The fixture writes 128 KiB chunks, up to 512 of them (64 MiB). A stalled
/// client may absorb a few dozen through the socket and host buffers; 192
/// chunks (24 MiB) is far above that and far below the cap, so a producer
/// that never blocks (all 512) fails loudly while a healthy plateau has
/// room to vary with the kernel's TCP buffer sizes.
const STALLED_MAX_WRITES: u64 = 192;
const FIXTURE_CHUNK_BYTES: u64 = 128 * 1024;
const FIXTURE_MAX_WRITES: u64 = 512;
/// The stall observation: a plateau is two consecutive samples this far
/// apart, and the whole wait for one is bounded by a single failure deadline.
const SAMPLE_INTERVAL: Duration = Duration::from_millis(200);
const STALL_DEADLINE: Duration = Duration::from_secs(30);

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/transform-backpressure")
        .canonicalize()
        .expect("fixture exists")
}

fn fixture_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    match builder.tempdir_in("/dev/shm") {
        Ok(dir) => dir,
        Err(error) => {
            eprintln!("fixture tempdir fell back to disk (/dev/shm unusable: {error})");
            builder.tempdir().expect("fixture tempdir")
        }
    }
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("copy fixture dir");
    for entry in std::fs::read_dir(src).expect("read fixture dir") {
        let entry = entry.expect("fixture entry");
        let to = dst.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).expect("copy fixture file");
        }
    }
}

/// A booted `cello dev` and its addresses. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(project: &Path) -> Self {
        // Port zero: the OS assigns the worker port, so parallel boots never
        // claim the same one; discovery below learns it.
        let mut child = cello_command(project, [("NO_COLOR".to_string(), "1".to_string())])
            .arg("dev")
            .arg(project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--clean")
            .arg("--logs")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cello dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        let announced_lines: Arc<Mutex<Vec<String>>> = Arc::default();
        forward_announce(stderr, Arc::clone(&announced_lines));
        let mut node = Self {
            child,
            public: String::new(),
        };
        let public = poll_until("boot", Duration::from_secs(180), || {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!(
                    "cello dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            let public = announced(&announced_lines, "cello listening on ")?;
            probe(&public, "/health").then_some(public)
        });
        node.public = public;
        node
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One complete HTTP/1.1 exchange, for the fixture's JSON routes.
fn request(
    addr: &str,
    method: &str,
    path: &str,
    timeout: Duration,
) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n"
    );
    stream
        .write_all(wire.as_bytes())
        .map_err(|error| format!("write: {error}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|error| format!("read: {error}"))?;
    let raw = String::from_utf8(raw).map_err(|error| format!("utf8: {error}"))?;
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("no header/body split in {raw:?}"))?;
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| format!("no status in {head:?}"))?;
    Ok((status, body.to_string()))
}

fn get(addr: &str, path: &str) -> Result<(u16, String), String> {
    request(addr, "GET", path, Duration::from_secs(30))
}

fn probe(addr: &str, path: &str) -> bool {
    matches!(
        request(addr, "GET", path, Duration::from_secs(2)),
        Ok((200, _))
    )
}

fn poll_until(
    name: &str,
    deadline: Duration,
    mut condition: impl FnMut() -> Option<String>,
) -> String {
    let start = Instant::now();
    loop {
        if let Some(done) = condition() {
            return done;
        }
        if start.elapsed() > deadline {
            panic!("{name}: condition unmet within the deadline");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The blocked cell's counter, as another request sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Producer {
    completed: u64,
    phase: Phase,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    Producing,
    Done,
    Failed,
}

fn producer_state(addr: &str) -> Result<Producer, String> {
    let (status, body) = get(addr, "/cell/state")?;
    if status != 200 {
        return Err(format!("state answered {status}: {body}"));
    }
    let state: Value =
        serde_json::from_str(&body).map_err(|error| format!("state json: {error}"))?;
    let completed = state["completed"]
        .as_u64()
        .ok_or_else(|| format!("no completed count in {body}"))?;
    let phase = match state["phase"].as_str().unwrap_or_default() {
        "idle" => Phase::Idle,
        "producing" => Phase::Producing,
        "done" => Phase::Done,
        "failed" => Phase::Failed,
        other => return Err(format!("unknown phase {other} in {body}")),
    };
    Ok(Producer { completed, phase })
}

fn boot_node() -> (tempfile::TempDir, TestNode) {
    let dir = fixture_tempdir("cello-transform-backpressure-");
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir(), &app_project);
    let node = TestNode::boot(&app_project);
    (dir, node)
}

/// The spec values a stalled `TransformStream` reports: `write()` and
/// `ready` pending with no consumer, a read releasing them, `desiredSize`
/// following the highWaterMark, cancellation and abort releasing a parked
/// write, and a transform error reaching both sides.
#[test]
fn transform_stream_applies_backpressure() {
    let (_dir, node) = boot_node();
    let (status, body) = get(&node.public, "/conformance").expect("conformance answers");
    assert_eq!(status, 200, "conformance failed: {body}");
    let report: Value = serde_json::from_str(&body).expect("conformance json");
    let failures: Vec<String> = report["failures"]
        .as_array()
        .expect("failures array")
        .iter()
        .map(|failure| failure.as_str().unwrap_or_default().to_string())
        .collect();
    assert!(
        failures.is_empty(),
        "TransformStream backpressure conformance failed:\n  {}",
        failures.join("\n  ")
    );
}

/// A client that takes the response head and then stops reading must stall
/// the producer's `write()`, not grow the isolate's heap: the completed-write
/// count reaches a plateau at the socket and host buffers and stays there.
/// Reading the body again resumes the producer and delivers every byte, so
/// the drain decodes the HTTP framing and compares the payload -- not the
/// wire bytes -- with what the fixture wrote.
#[test]
fn stalled_client_bounds_the_transform_stream_producer() {
    let (_dir, node) = boot_node();
    let mut stream = TcpStream::connect(&node.public).expect("connect stalled producer");
    // Short reads keep the deadline loop responsive; after the response head
    // nothing is read until the drain below, which is the whole point.
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("stalled read timeout");
    let wire = format!(
        "GET /cell/tight HTTP/1.1\r\nHost: {}\r\nAccept: application/octet-stream\r\n\r\n",
        node.public
    );
    stream
        .write_all(wire.as_bytes())
        .expect("write stalled request");

    // The response head alone says the handler answered; from here the client
    // never reads, so the only thing that bounds the producer is the
    // TransformStream's backpressure. The head is also where the body framing
    // (chunked, normally) is learned, so the drain can count payload bytes
    // rather than HTTP bytes.
    let (head, pending) = read_response_head(&mut stream, Duration::from_secs(30));
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status in response head {head:?}"));
    assert_eq!(status, 200, "the stalled request answered {status}: {head}");
    let framing = BodySink::framing(&head);

    // Wait until the producer has completed at least one write, then sample
    // until two consecutive samples (one interval apart) are equal: a real
    // plateau, not a fixed warm-up a slow-but-correct implementation could
    // fail. Every sample enforces the cap, so an implementation without
    // backpressure still fails fast instead of running to the fixture's cap.
    let stall_deadline = Instant::now() + STALL_DEADLINE;
    let check_sample = |state: Producer| {
        assert_ne!(
            state.phase,
            Phase::Failed,
            "the stalled producer failed instead of parking: {state:?}"
        );
        assert!(
            state.completed <= STALLED_MAX_WRITES,
            "a client that never reads must stall the producer at the socket and host \
             buffers, but {} writes of {FIXTURE_CHUNK_BYTES} bytes completed (cap \
             {STALLED_MAX_WRITES} of {FIXTURE_MAX_WRITES}); the TransformStream buffered \
             the body in the isolate's heap",
            state.completed
        );
    };
    poll_until(
        "producer started",
        stall_deadline.saturating_duration_since(Instant::now()),
        || {
            let state = producer_state(&node.public).ok()?;
            // Started once it has completed a write, or already failed (the
            // sample check below reports that). A producer that finished all
            // writes before the first sample also reports here, and the cap
            // check then fails it immediately.
            (state.completed > 0 || state.phase == Phase::Failed).then_some("started".to_string())
        },
    );
    let mut previous = producer_state(&node.public).expect("state after start");
    check_sample(previous);
    let plateau = loop {
        std::thread::sleep(SAMPLE_INTERVAL);
        let current = producer_state(&node.public).expect("state mid-stall");
        check_sample(current);
        let equal = current.completed == previous.completed;
        previous = current;
        if equal {
            break current.completed;
        }
        assert!(
            Instant::now() < stall_deadline,
            "the stalled producer never reached a plateau within {STALL_DEADLINE:?} \
             (last sample {} of {FIXTURE_MAX_WRITES} writes); the client is not reading",
            previous.completed
        );
    };
    eprintln!(
        "stalled producer plateaued at {plateau} of {FIXTURE_MAX_WRITES} writes before the \
         client read"
    );

    // Reading resumes it: the framed body arrives and the producer finishes.
    // Payload bytes are counted after stripping the head and the chunk
    // framing, so a header or CRLF cannot stand in for a missing write, and
    // a short final chunk is a framing error rather than a byte-count miss.
    let total = FIXTURE_MAX_WRITES * FIXTURE_CHUNK_BYTES;
    let mut body = framing;
    body.feed(&pending)
        .unwrap_or_else(|error| panic!("the response body is malformed: {error}"));
    let mut buf = vec![0u8; 64 * 1024];
    let mut quiet = 0u32;
    let drain_deadline = Instant::now() + Duration::from_secs(120);
    while !body.done() {
        assert!(
            Instant::now() < drain_deadline,
            "the stalled body never resumed after the client read {} payload bytes of {total}",
            body.payload_so_far()
        );
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(read) => {
                body.feed(&buf[..read])
                    .unwrap_or_else(|error| panic!("the response body is malformed: {error}"));
                quiet = 0;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                quiet += 1;
                // Several seconds of silence after the resume means the body
                // stopped for good; `finish` then reports the truncation.
                if quiet >= 25 {
                    break;
                }
            }
            Err(error) => panic!("drain read failed: {error}"),
        }
    }
    let payload = body
        .finish()
        .unwrap_or_else(|error| panic!("the drained body is truncated: {error}"));
    assert_eq!(
        payload, total,
        "the decoded body payload is {payload} bytes, not the {total} bytes the fixture wrote"
    );
    poll_until("producer finished", Duration::from_secs(30), || {
        let state = producer_state(&node.public).ok()?;
        (state.phase == Phase::Done && state.completed == FIXTURE_MAX_WRITES)
            .then(|| format!("{state:?}"))
    });
}

/// Read one HTTP/1.1 response head (through the blank line), returning it and
/// any body bytes that arrived with the same read.
fn read_response_head(stream: &mut TcpStream, deadline: Duration) -> (String, Vec<u8>) {
    let start = Instant::now();
    let mut raw = Vec::new();
    loop {
        if let Some(position) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            let head = String::from_utf8(raw[..position].to_vec())
                .unwrap_or_else(|error| panic!("response head is not utf8: {error}"));
            return (head, raw[position + 4..].to_vec());
        }
        assert!(
            raw.len() < 64 * 1024,
            "oversize response head ({} bytes)",
            raw.len()
        );
        assert!(
            start.elapsed() < deadline,
            "no response head within {deadline:?}"
        );
        let mut buf = [0u8; 4096];
        match stream.read(&mut buf) {
            Ok(0) => panic!(
                "the connection closed before the response head: {:?}",
                String::from_utf8_lossy(&raw)
            ),
            Ok(read) => raw.extend_from_slice(&buf[..read]),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) => {}
            Err(error) => panic!("response head read failed: {error}"),
        }
    }
}

fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().skip(1).find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim().eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

/// How the response body is framed on the wire.
enum BodySink {
    Chunked(ChunkedDecoder),
    Length { expected: u64, seen: u64 },
    UntilEof { seen: u64 },
}

impl BodySink {
    fn framing(head: &str) -> Self {
        let chunked = header_value(head, "transfer-encoding")
            .is_some_and(|value| value.to_ascii_lowercase().contains("chunked"));
        if chunked {
            return Self::Chunked(ChunkedDecoder::default());
        }
        match header_value(head, "content-length").and_then(|value| value.parse().ok()) {
            Some(expected) => Self::Length { expected, seen: 0 },
            None => Self::UntilEof { seen: 0 },
        }
    }

    fn feed(&mut self, input: &[u8]) -> Result<(), String> {
        match self {
            Self::Chunked(decoder) => decoder.feed(input),
            Self::Length { expected, seen } => {
                *seen += input.len() as u64;
                if *seen > *expected {
                    return Err(format!(
                        "body is longer than content-length {expected} ({seen} bytes)"
                    ));
                }
                Ok(())
            }
            Self::UntilEof { seen } => {
                *seen += input.len() as u64;
                Ok(())
            }
        }
    }

    fn done(&self) -> bool {
        match self {
            Self::Chunked(decoder) => decoder.done(),
            Self::Length { expected, seen } => seen == expected,
            Self::UntilEof { .. } => false,
        }
    }

    fn payload_so_far(&self) -> u64 {
        match self {
            Self::Chunked(decoder) => decoder.payload,
            Self::Length { seen, .. } | Self::UntilEof { seen } => *seen,
        }
    }

    fn finish(self) -> Result<u64, String> {
        match self {
            Self::Chunked(decoder) => {
                if decoder.done() {
                    Ok(decoder.payload)
                } else {
                    Err(format!(
                        "the chunked body ended in {:?} after {} payload bytes",
                        decoder.state, decoder.payload
                    ))
                }
            }
            Self::Length { expected, seen } => {
                if seen == expected {
                    Ok(seen)
                } else {
                    Err(format!("body ended after {seen} of {expected} bytes"))
                }
            }
            Self::UntilEof { seen } => Ok(seen),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ChunkState {
    #[default]
    Size,
    Data,
    DataCrlf,
    Terminal,
    Done,
}

/// Strict incremental `Transfer-Encoding: chunked` decoder. Counts decoded
/// payload bytes, validates every size line and CRLF, accepts trailers, and
/// refuses to finish before the terminal chunk, so truncated framing cannot
/// pass as a short-but-valid body.
#[derive(Default)]
struct ChunkedDecoder {
    state: ChunkState,
    remaining: u64,
    crlf_seen: u8,
    line: Vec<u8>,
    payload: u64,
}

impl ChunkedDecoder {
    fn done(&self) -> bool {
        self.state == ChunkState::Done
    }

    fn feed(&mut self, mut input: &[u8]) -> Result<(), String> {
        loop {
            match self.state {
                ChunkState::Done => return Ok(()),
                ChunkState::Size | ChunkState::Terminal => {
                    if input.is_empty() {
                        return Ok(());
                    }
                    if let Some(position) = input.windows(2).position(|window| window == b"\r\n") {
                        let terminal_line_empty = self.state == ChunkState::Terminal
                            && self.line.is_empty()
                            && position == 0;
                        self.line.extend_from_slice(&input[..position]);
                        input = &input[position + 2..];
                        let line = String::from_utf8(std::mem::take(&mut self.line))
                            .map_err(|_| "HTTP chunk line is not utf8".to_string())?;
                        if self.state == ChunkState::Terminal {
                            // A trailer line, or the empty line ending them.
                            if terminal_line_empty {
                                self.state = ChunkState::Done;
                            }
                            continue;
                        }
                        let hex = line.split(';').next().unwrap_or("").trim();
                        let size = u64::from_str_radix(hex, 16)
                            .map_err(|_| format!("invalid chunk size {line:?}"))?;
                        if size == 0 {
                            self.state = ChunkState::Terminal;
                        } else {
                            self.remaining = size;
                            self.state = ChunkState::Data;
                        }
                    } else {
                        self.line.extend_from_slice(input);
                        if self.line.len() > 8192 {
                            return Err("HTTP chunk line is too long".to_string());
                        }
                        return Ok(());
                    }
                }
                ChunkState::Data => {
                    if input.is_empty() {
                        return Ok(());
                    }
                    let take = (input.len() as u64).min(self.remaining) as usize;
                    self.payload += take as u64;
                    self.remaining -= take as u64;
                    input = &input[take..];
                    if self.remaining == 0 {
                        self.state = ChunkState::DataCrlf;
                    }
                }
                ChunkState::DataCrlf => {
                    while self.crlf_seen < 2 {
                        let Some((&byte, rest)) = input.split_first() else {
                            return Ok(());
                        };
                        let want = if self.crlf_seen == 0 { b'\r' } else { b'\n' };
                        if byte != want {
                            return Err(format!(
                                "chunk data is not CRLF-terminated (byte {byte:#04x})"
                            ));
                        }
                        input = rest;
                        self.crlf_seen += 1;
                    }
                    self.crlf_seen = 0;
                    self.state = ChunkState::Size;
                }
            }
        }
    }
}
