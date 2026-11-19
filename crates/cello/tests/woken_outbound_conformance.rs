// Modified by Ryuz Corp (Cello): woken-continuation outbound conformance (upstream #258). Apache-2.0.

// A black-box runner, not node code: it boots the real `cello dev` binary
// and drives the issue's self-vs-woken outbound dance, so the execution
// boundary's injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for outbound calls made from a continuation woken by another
//! event (denoland/celld#258): request A starts async work and returns; a
//! later request resolves the promise it waits on; the continuation then
//! makes an outbound `fetch()` (or a storage write). The waiter must settle
//! instead of hanging to its budget.
//!
//! Before the retirement-hardening line the woken outbound call never
//! settled — no result, no error, nothing logged. Now the call fails loudly
//! (its origin event is retired) while the self-woken control still
//! succeeds, so every leg of the dance answers. The full workerd parity —
//! running the call under the live waker — needs the live-ambient adoption
//! design and stays open; this test pins the no-hang half: any leg still
//! `pending` at the deadline fails.
//!
//! One `cello dev` boots the fixture per test. No fixed sleeps on the pass
//! path: each leg polls its result with a deadline, and the server guard
//! kills the child on every exit path.

mod common;

use common::{NO_EXTRA_ENV, announced, cello_command, forward_announce};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/woken-outbound")
        .canonicalize()
        .expect("fixture exists")
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// A booted `cello dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(project: &Path) -> Self {
        let mut child = cello_command(project, NO_EXTRA_ENV)
            .arg("dev")
            .arg(project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--logs")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cello dev");
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
                "cello dev exited during boot: {status}: {}",
                announced_lines.lock().expect("announce lock").join("\n")
            );
        }
        if let Some(public) = announced(announced_lines, "cello listening on ") {
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

/// Drive one leg of the dance and return its settled result, or fail when
/// the waiter is still `pending` at the deadline: that is the hang this
/// test pins against.
fn run_leg(node: &TestNode, begin: &str, woken: bool, name: &str) -> String {
    let (status, _) = http_get(&node.public, begin, Duration::from_secs(15)).expect("begin");
    assert_eq!(status, 200, "{begin}");
    if woken {
        let (status, _) = http_get(
            &node.public,
            &format!("/notify?name={name}"),
            Duration::from_secs(15),
        )
        .expect("notify");
        assert_eq!(status, 200);
    } else {
        // The self wake is a 200 ms timer; give it room without asserting
        // on timing.
        std::thread::sleep(Duration::from_millis(500));
    }
    let start = std::time::Instant::now();
    loop {
        let (status, body) = http_get(
            &node.public,
            &format!("/result?name={name}"),
            Duration::from_secs(15),
        )
        .expect("result answers");
        assert_eq!(status, 200, "{body}");
        let result = body.trim().to_string();
        if result != "pending" {
            return result;
        }
        if start.elapsed() > Duration::from_secs(15) {
            panic!("woken waiter never settled (still pending)");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn woken_continuation_outbound_work_settles() {
    let dir = tempfile::Builder::new()
        .prefix("cello-woken-outbound-")
        .tempdir()
        .expect("tempdir");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot(&project);

    // Control first: woken by its own timer, everything succeeds.
    let self_fetch = run_leg(&node, "/self?how=fetch&name=sfetch", false, "sfetch");
    assert_eq!(self_fetch, "ok plain ok", "{self_fetch}");
    let self_store = run_leg(&node, "/self?how=storage&name=sstore", false, "sstore");
    assert_eq!(self_store, "ok stored", "{self_store}");

    // Woken by another request: must settle (loudly), never hang.
    let woken_fetch = run_leg(&node, "/start?how=fetch&name=wfetch", true, "wfetch");
    assert_ne!(woken_fetch, "pending", "woken fetch hung");
    let woken_store = run_leg(&node, "/start?how=storage&name=wstore", true, "wstore");
    assert_ne!(woken_store, "pending", "woken storage hung");
}
