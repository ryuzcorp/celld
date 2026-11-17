// Modified by ryuzcorp (ryuzcorp/celld): cancelled-request lazy-init conformance (upstream #294). Apache-2.0.

// A black-box runner, not node code: it boots the real `celld dev` binary,
// cancels one Durable Object request mid-flight by closing its TCP
// connection, and asserts the next request to the same object answers, so
// the execution boundary's injected clock/filesystem/async rules do not
// apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for cancelled Durable Object requests (denoland/celld#294):
//! the object lazily starts a timer shared through `this.ready`, the first
//! request is cancelled once its init has provably started (well before
//! the 60 s timer), and the retry must answer instead of hanging until
//! its budget.
//!
//! Before the fix the cancelled entry's `abandon()` purged the timer op's
//! resolver, so the shared promise never settled and every later request
//! hung with nothing logged. Now the dropped op rejects, the harness fires
//! the armed timer's callback early, and the retry answers with the value
//! the callback computes. The fetch half of the same path is pinned
//! white-box by `retired_event_attribution` (upstream #317), which shares
//! the cancel path.
//!
//! One `celld dev` boots the fixture per test. No fixed sleeps: the abort
//! waits on the `/started` handshake, and the server guard kills the child
//! on every exit path.

mod common;

use common::{announced, forward_announce};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn celld_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_celld"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/cancelled-lazy")
        .canonicalize()
        .expect("fixture exists")
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
        let mut child = Command::new(celld_binary())
            .arg("dev")
            .arg(project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--logs")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn celld dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        // Drain stdout so a chatty child can never block on a full pipe.
        // Stderr is collected line by line: it carries the node's announce
        // lines, the only channel to its bound addresses now that the node
        // is non-dumpable (patch 0053).
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        let announced_lines: Arc<Mutex<Vec<String>>> = Arc::default();
        forward_announce(stderr, Arc::clone(&announced_lines));
        // The kill-on-drop guard exists before the readiness poll, so a
        // boot timeout or an early child exit cannot leak the server.
        let mut node = Self {
            child,
            public: String::new(),
        };
        // Readiness is the announced public address answering with the app
        // loaded. The port is ephemeral (`--port 0`): the announce line is
        // the node's own report of where it bound, so no probe-then-bind
        // race can hand us another test's node.
        node.public = poll_boot(&mut node, &announced_lines);
        node
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn poll_boot(node: &mut TestNode, announced_lines: &Arc<Mutex<Vec<String>>>) -> String {
    let start = std::time::Instant::now();
    loop {
        if let Some(status) = node.child.try_wait().expect("poll child") {
            panic!(
                "celld dev exited during boot: {status}: {}",
                announced_lines.lock().expect("announce lock").join("\n")
            );
        }
        if let Some(public) = announced(announced_lines, "celld listening on ") {
            if probe(&public) {
                return public;
            }
        }
        if start.elapsed() > Duration::from_secs(120) {
            panic!("boot: condition unmet within the deadline");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Readiness probe with a short timeout: a bound-but-not-ready node holds
/// connections open, so a long read would eat the boot deadline.
fn probe(addr: &str) -> bool {
    matches!(
        http_get(addr, "/health", Duration::from_secs(2)),
        Ok((200, _))
    )
}

fn http_get(addr: &str, path: &str, timeout: Duration) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire =
        format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    stream
        .write_all(wire.as_bytes())
        .map_err(|error| format!("write: {error}"))?;
    read_response(&mut stream)
}

fn read_response(stream: &mut TcpStream) -> Result<(u16, String), String> {
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

/// Open a `/timer` request and hold it until the object reports its lazy
/// init started, then close it like a client giving up mid-flight. The
/// handshake — not a sleep — decides when the abort lands, so it cannot
/// race init. The server observes the hangup on its own; the retry below
/// proves what it did with the orphaned work.
fn abort_request_after_start(addr: &str) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .write_all(b"GET /timer HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .expect("write");
    let start = std::time::Instant::now();
    loop {
        if let Ok((status, body)) = http_get(addr, "/started", Duration::from_secs(5)) {
            if status == 200 && body.trim() == "yes" {
                break;
            }
        }
        if start.elapsed() > Duration::from_secs(15) {
            panic!("lazy init never started");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    drop(stream);
}

#[test]
fn cancelled_request_leaves_no_pending_lazy_init() {
    let dir = tempfile::Builder::new()
        .prefix("celld-cancelled-lazy-")
        .tempdir()
        .expect("tempdir");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot(&project);

    // Start the lazy timer, then give up once init has provably started,
    // well before the 60 s timer fires.
    abort_request_after_start(&node.public);

    // The retry must answer. Pre-fix the shared promise never settled and
    // this read died on its deadline; now the dropped timer fires early and
    // the object answers with the value its callback computes. The retry
    // deadline (15 s) is well inside the timer's natural 60 s expiry, so
    // answering proves cancellation settled init early.
    let (status, body) = http_get(&node.public, "/timer", Duration::from_secs(15))
        .expect("retry answers instead of hanging");
    assert_eq!(status, 200, "{body}");
    assert_eq!(body, "timer fired", "{body}");
}
