// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): handler-visible trace context (upstream #313).

// A black-box runner, not node code: it boots the real `cello dev` binary
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A fetch handler reads the W3C traceparent of the span it runs in through
//! `getCurrentTraceparent()` from `cloudflare:workers` (denoland/celld#313),
//! so it can return the trace id to the caller or record it with the data it
//! writes. The test boots `dev` with OTLP pointed at a capture server, reads
//! the handler's answer, and checks it names a sampled trace the export
//! carries a span for.

mod common;

use common::{announced, forward_announce};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn cello_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cello"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/trace-context")
        .canonicalize()
        .expect("fixture exists")
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// A minimal OTLP/HTTP capture server: it answers every export with 200 and
/// keeps the raw request bodies, so the test can prove the handler's span
/// was exported without decoding protobuf.
struct Capture {
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    addr: String,
}

impl Capture {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("capture listener");
        listener
            .set_nonblocking(true)
            .expect("nonblocking capture listener");
        let addr = listener.local_addr().expect("capture addr").to_string();
        let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
        let kept = Arc::clone(&bodies);
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(120);
            while Instant::now() < deadline {
                let Ok((mut stream, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let mut raw = Vec::new();
                let mut buf = [0u8; 8192];
                // Read the request line and headers, then exactly the
                // announced body: keep-alive would otherwise stall the read
                // waiting for the next request.
                let mut head = Vec::new();
                let content_length = loop {
                    match stream.read(&mut buf) {
                        Ok(0) => break None,
                        Ok(read) => {
                            head.extend_from_slice(&buf[..read]);
                            if let Some(end) = find_head_end(&head) {
                                let length = content_length(&head[..end]);
                                raw.extend_from_slice(&head[end..]);
                                break Some(length);
                            }
                        }
                        Err(_) => break None,
                    }
                };
                if let Some(length) = content_length {
                    while raw.len() < length {
                        match stream.read(&mut buf) {
                            Ok(0) => break,
                            Ok(read) => raw.extend_from_slice(&buf[..read]),
                            Err(_) => break,
                        }
                    }
                    raw.truncate(length);
                    kept.lock().expect("capture lock").push(raw);
                }
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
            }
        });
        Self { bodies, addr }
    }
}

fn find_head_end(head: &[u8]) -> Option<usize> {
    head.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn content_length(head: &[u8]) -> usize {
    let text = String::from_utf8_lossy(head).to_ascii_lowercase();
    text.lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}

struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(project: &Path, otel: &str) -> Self {
        let mut command = Command::new(cello_binary());
        command
            .arg("dev")
            .arg(project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--logs")
            .env("CELLO_OTEL", otel)
            .env("CELLO_OTEL_FLUSH_MS", "100")
            .env("OTEL_SERVICE_NAME", "trace-probe")
            // An ambient `CELLO_DEV_STATE` would pull the child's state out
            // of the temporary project and leave `objects.sqlite3` behind.
            // Drop it so the child keeps the normal project-local state.
            .env_remove("CELLO_DEV_STATE")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn cello dev");
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
        node.public = poll_until("boot", Duration::from_secs(120), || {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!(
                    "cello dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            let public = announced(&announced_lines, "cello listening on ")?;
            if !probe(&public, "/") {
                return None;
            }
            Some(public)
        });
        node
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn get(addr: &str, path: &str) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
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

fn probe(addr: &str, path: &str) -> bool {
    matches!(get(addr, path), Ok((200, _)))
}

fn poll_until<T>(name: &str, deadline: Duration, mut condition: impl FnMut() -> Option<T>) -> T {
    let start = Instant::now();
    loop {
        if let Some(done) = condition() {
            return done;
        }
        if start.elapsed() > deadline {
            panic!("{name}: condition unmet within the deadline");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn is_traceparent(value: &str) -> bool {
    let parts: Vec<&str> = value.split('-').collect();
    let [version, trace_id, span_id, flags] = parts.as_slice() else {
        return false;
    };
    *version == "00"
        && trace_id.len() == 32
        && trace_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        && span_id.len() == 16
        && span_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        && *flags == "01"
        && trace_id.bytes().any(|byte| byte != b'0')
        && span_id.bytes().any(|byte| byte != b'0')
}

/// The handler's answer names the sampled trace its own span exports in.
#[test]
fn handler_reads_its_spans_traceparent() {
    let capture = Capture::start();
    let project = tempfile::tempdir().expect("project dir");
    copy_dir(&fixture_dir(), project.path());
    let node = TestNode::boot(project.path(), &format!("http://{}", capture.addr));
    let (status, body) = get(&node.public, "/").expect("public GET /");
    assert_eq!(status, 200, "GET /: {body}");
    let reply: Value = serde_json::from_str(&body).expect("JSON body");
    let traceparent = reply["traceparent"]
        .as_str()
        .expect("the handler answers its traceparent");
    assert!(
        is_traceparent(traceparent),
        "a sampled W3C traceparent: {traceparent}"
    );
    // The export carries a span for the request the handler just answered:
    // the trace id the handler returned is one the collector actually sees.
    // Matching the raw trace id bytes (not just any `cello.fetch` span)
    // keeps the boot probe's span for a different trace from passing.
    let trace_id = trace_id_bytes(traceparent);
    let exported = poll_until("export", Duration::from_secs(60), || {
        let bodies = capture.bodies.lock().expect("capture lock");
        bodies
            .iter()
            .any(|body| {
                body.windows(b"cello.fetch".len())
                    .any(|window| window == b"cello.fetch")
                    && body
                        .windows(trace_id.len())
                        .any(|window| window == trace_id)
            })
            .then(|| bodies.len())
    });
    assert!(exported > 0, "the handler's span reached the collector");
}

/// The raw 16 trace id bytes a `traceparent` names. The test runs after the
/// format assertion, so this only handles well-formed values.
fn trace_id_bytes(traceparent: &str) -> [u8; 16] {
    let hex = traceparent.split('-').nth(1).expect("trace id field");
    let (pairs, rest) = hex.as_bytes().as_chunks::<2>();
    assert!(rest.is_empty(), "a 32-digit trace id: {traceparent}");
    let mut out = [0u8; 16];
    for (slot, pair) in out.iter_mut().zip(pairs) {
        let hi = (pair[0] as char).to_digit(16).expect("hex trace id") as u8;
        let lo = (pair[1] as char).to_digit(16).expect("hex trace id") as u8;
        *slot = hi << 4 | lo;
    }
    out
}
