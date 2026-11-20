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
/// count stays at the socket and host buffers. Reading the body again resumes
/// the producer and delivers every byte.
#[test]
fn stalled_client_bounds_the_transform_stream_producer() {
    let (_dir, node) = boot_node();
    let mut stream = TcpStream::connect(&node.public).expect("connect stalled producer");
    // Short reads keep the deadline loop responsive; nothing is read until the
    // drain below, which is the whole point.
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
    // TransformStream's backpressure. The first second lets the socket and
    // host buffers fill; after that the producer must not advance at all, and
    // it must never approach the fixture's cap.
    std::thread::sleep(Duration::from_secs(1));
    let filled = producer_state(&node.public).expect("state mid-stall");
    let mut highest = filled.completed;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        let state = producer_state(&node.public).expect("state mid-stall");
        assert!(
            state.phase != Phase::Failed,
            "the stalled producer failed instead of parking: {state:?}"
        );
        highest = highest.max(state.completed);
    }
    assert!(
        highest <= STALLED_MAX_WRITES,
        "a client that never reads must stall the producer at the socket and host buffers, \
         but {highest} writes of {FIXTURE_CHUNK_BYTES} bytes completed (cap {FIXTURE_MAX_WRITES}); \
         the TransformStream buffered the body in the isolate's heap"
    );
    eprintln!(
        "stalled producer completed {highest} of {FIXTURE_MAX_WRITES} writes before the client read"
    );
    assert_eq!(
        highest, filled.completed,
        "the producer kept completing writes with no reader: {} -> {highest}",
        filled.completed
    );

    // Reading resumes it: every byte arrives and the producer finishes. The
    // body may stay open past the last write, so end the drain at the byte
    // count plus a quiet window instead of at EOF.
    let total = FIXTURE_MAX_WRITES * FIXTURE_CHUNK_BYTES;
    let mut buf = vec![0u8; 64 * 1024];
    let mut bytes = 0u64;
    let mut quiet = 0u32;
    let drain_deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(read) => {
                bytes += read as u64;
                quiet = 0;
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                quiet += 1;
            }
            Err(error) => panic!("drain read failed: {error}"),
        }
        assert!(
            Instant::now() < drain_deadline,
            "the stalled body never resumed after the client read {bytes} of {total} bytes"
        );
        if bytes >= total && quiet >= 3 {
            break;
        }
    }
    let finished = producer_state(&node.public).expect("state after drain");
    assert_eq!(
        finished.phase,
        Phase::Done,
        "the producer must finish once the client reads ({finished:?})"
    );
    assert_eq!(
        finished.completed, FIXTURE_MAX_WRITES,
        "every write must complete once the client reads"
    );
    assert!(
        bytes >= total,
        "the drained body is {bytes} bytes, short of the {total} bytes written"
    );
}
