// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): abort/drain conformance. Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for two reset/drain boundaries, driven through the
//! `tests/fixtures/abort-drain` fixture:
//!
//! * A cell abort under a same-isolate stub caller drops the aborting
//!   cell's queued puts, not the caller's: the caller transacts a put,
//!   invokes the reset cell's loopback stub (which aborts inline in the
//!   caller's turn), and commits. The put must survive; the reset cell
//!   must stay broken afterwards, proving the abort ran.
//! * A same-script entrypoint reply carrying a `ReadableStream` plus a
//!   stub answers without waiting for the `waitUntil` work that feeds
//!   the stream, which cannot finish until the caller pulls it.
//!
//! One `celld dev` boots the single fixture project per test. There are
//! no fixed sleeps: every wait polls an observable condition with a
//! deadline, and the server guard kills the child on every exit path.

use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn celld_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_celld"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/abort-drain")
        .canonicalize()
        .expect("fixture exists")
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    listener.local_addr().expect("probe addr").port()
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// A booted `celld dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(project: &Path) -> Self {
        let port = free_port();
        let mut child = Command::new(celld_binary())
            .arg("dev")
            .arg(project)
            .arg("--port")
            .arg(port.to_string())
            .arg("--no-watch")
            .arg("--logs")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn celld dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        // Drain both pipes so a chatty child can never block on a full one.
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stderr.read_to_end(&mut sink);
        });
        let public = format!("127.0.0.1:{port}");
        // The kill-on-drop guard exists before the readiness poll, so a
        // boot timeout or an early child exit cannot leak the server.
        let mut node = Self {
            child,
            public: public.clone(),
        };
        // Readiness is the public port answering with the app loaded.
        poll_until("boot", Duration::from_secs(120), || {
            if !probe(&public, "/health") {
                return None;
            }
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("celld dev exited during boot: {status}");
            }
            Some(public.clone())
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

fn request_timeout(
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
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
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
    request_timeout(addr, "GET", path, Duration::from_secs(60))
}

/// Readiness probe with a short timeout: a bound-but-not-ready node holds
/// connections open, so a long read would eat the boot deadline.
fn probe(addr: &str, path: &str) -> bool {
    matches!(
        request_timeout(addr, "GET", path, Duration::from_secs(2)),
        Ok((200, _))
    )
}

fn get_json(node: &TestNode, path: &str) -> Value {
    let (status, body) = get(&node.public, path).expect("public GET");
    assert_eq!(status, 200, "GET {path}: {body}");
    serde_json::from_str(&body).expect("JSON body")
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
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn boot_fixture(name: &str) -> (tempfile::TempDir, TestNode) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("celld-abort-drain-{name}-"))
        .tempdir()
        .expect("tempdir");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot(&project);
    (dir, node)
}

#[test]
fn abort_during_stub_call_keeps_caller_puts() {
    let (_dir, node) = boot_fixture("caller-puts");
    let outcome = get_json(&node, "/reset-caller-put");
    assert_eq!(outcome["ok"], true, "{outcome}");
    let result = &outcome["result"];
    // The reset cell stays broken afterwards, proving the abort ran: a
    // vacuous pass (no abort) would answer 'pong' here.
    assert!(
        result["resetState"]
            .as_str()
            .is_some_and(|state| state.contains("reset-by-test")),
        "{result}"
    );
    assert_eq!(result["kept"], "cv", "{result}");
}

#[test]
fn entrypoint_stream_stub_reply_skips_drain() {
    let (_dir, node) = boot_fixture("stream");
    // Control first: the same shape without the stub answers, proving
    // the stream path itself is sound.
    let control = get_json(&node, "/stream-only");
    assert_eq!(control["ok"], true, "{control}");
    assert_eq!(control["result"]["text"], "chunk", "{control}");

    let outcome = get_json(&node, "/stream-and-stub");
    assert_eq!(outcome["ok"], true, "{outcome}");
    assert_eq!(outcome["result"]["text"], "chunk", "{outcome}");
    assert_eq!(outcome["result"]["ping"], "pong", "{outcome}");
}
