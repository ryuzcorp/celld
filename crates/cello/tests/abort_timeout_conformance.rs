// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): AbortSignal.timeout admission regression test (upstream #306).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Upstream denoland/celld#306: `AbortSignal.timeout(ms)` inside a Durable
//! Object held one of the object's `CELLD_MAX_CELL_REQUESTS` slots until the
//! timer fired, so 64 sequential requests each minting an unused timeout
//! signal made the 65th fail with `503 cell request limit reached`.
//!
//! The timeout timer is unreferenced now (matching Node, Bun, and workerd,
//! which let it die with the request), so sequential requests never exhaust
//! the admission window; the timeout itself still aborts.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn cello_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cello"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/abort-timeout")
        .canonicalize()
        .expect("fixture exists")
}

mod common;
use common::{announced, forward_announce};

/// Tempdir for one fixture copy: `/dev/shm` when usable, else the
/// default tempdir. Tmpfs has no fsync/writeback latency, so the store
/// stops sharing the box's disk queue.
fn fixture_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    match builder.tempdir_in("/dev/shm") {
        Ok(dir) => dir,
        Err(error) => {
            eprintln!("fixture tempdir fell back to disk (/dev/shm unusable: {error}");
            builder.tempdir().expect("fixture tempdir")
        }
    }
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
    fn boot(app_project: &Path) -> Self {
        let mut child = Command::new(cello_binary())
            .arg("dev")
            .env("NO_COLOR", "1")
            .arg(app_project)
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
                panic!("celld dev exited during boot: {status}");
            }
            let addr = announced(&announced_lines, "celld listening on ")?;
            probe(&addr, "/health").then_some(addr)
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

fn probe(addr: &str, path: &str) -> bool {
    matches!(
        request_timeout(addr, "GET", path, Duration::from_secs(2)),
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
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn unused_timeout_signal_frees_the_admission_slot() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-abort-timeout-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // The timeout still fires when awaited: TimeoutError, not a hang.
    let (status, body) = get(&node.public, "/fires").expect("GET /fires");
    assert_eq!(status, 200, "GET /fires: {body}");
    assert!(
        body.contains("TimeoutError"),
        "expected a TimeoutError abort, got: {body}"
    );

    // 70 sequential requests, each minting an unused 60 s timeout signal.
    // The timer outlives the loop, so a referenced timer would hold its
    // admission slot for the whole run and refuse from the 65th with 503.
    // Never above concurrency 1: every response fully precedes the next.
    // The count exceeds `DEFAULT_MAX_CELL_REQUESTS` (64), the admission
    // limit this test boots with by default: one past the limit would
    // already fail, and 70 leaves headroom without changing the verdict.
    const {
        assert!(
            70 > cello::runtime::DEFAULT_MAX_CELL_REQUESTS,
            "the request count must exceed the admission limit",
        )
    };
    let loop_start = Instant::now();
    for i in 0..70 {
        let (status, body) = get(&node.public, "/timeout-signal")
            .unwrap_or_else(|error| panic!("request {i}: {error}"));
        assert_eq!(status, 200, "request {i} refused: {body}");
        assert_eq!(body, "ok", "request {i} body: {body}");
    }
    // The timer must outlive the loop: on a slow runner the first timers
    // could otherwise fire mid-loop, freeing slots and letting all 70 pass
    // even with the bug. Fail loudly instead of passing silently.
    assert!(
        loop_start.elapsed() < Duration::from_secs(60),
        "the loop outran the 60 s timers, so a pass proves nothing",
    );
}
