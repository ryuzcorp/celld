// Modified by ryuzcorp (ryuzcorp/celld): retired-continuation resume conformance (upstream #277). Apache-2.0.

// A black-box runner, not node code: it boots the real `celld dev` binary
// and drives the issue's constructor-started task loop, so the execution
// boundary's injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for async work resumed from a retired event's continuation
//! (denoland/celld#277): the object starts a task loop in its constructor
//! without keeping that event alive; a later request resolves the gate the
//! loop waits on; the continuation must then complete an outbound fetch and
//! an input-gated storage write, reporting
//! `["awaited", "fetched", "stored"]`.
//!
//! The issue's reproduction answers exactly that on this tree — the
//! continuation-token and cross-entry-gate line already roots the woken
//! work — so there is no runtime change here. This test pins the shape so
//! a future attribution or retirement regression turns the silent hang or
//! the input-gate refusal back into a loud failure instead of a wedge: any
//! entry other than the full triple fails the assertion, and a hang fails
//! the deadline.
//!
//! One `celld dev` boots the fixture per test. No fixed sleeps: the log is
//! polled with a deadline, and the server guard kills the child on every
//! exit path.

mod common;

use common::{announced, forward_announce};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn cello_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cello"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/continuation-resume")
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
        let mut child = Command::new(cello_binary())
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

#[test]
fn constructor_task_loop_resumed_by_later_event_completes() {
    let dir = tempfile::Builder::new()
        .prefix("celld-continuation-resume-")
        .tempdir()
        .expect("tempdir");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot(&project);

    // Resolve the gate from a later event, passing the node's own address
    // so the loop's fetch needs no external network.
    let origin = format!("http://{}", node.public);
    // The origin contains `://`, which is legal in a query value.
    let (status, body) = http_get(
        &node.public,
        &format!("/resolve?origin={origin}"),
        Duration::from_secs(15),
    )
    .expect("resolve answers");
    assert_eq!(status, 200, "{body}");

    // The loop must run to completion: a hang stalls here until the
    // deadline, and a refusal lands in the log instead of `stored`.
    let start = std::time::Instant::now();
    loop {
        let (status, body) =
            http_get(&node.public, "/log", Duration::from_secs(15)).expect("log answers");
        assert_eq!(status, 200, "{body}");
        let results: serde_json::Value = serde_json::from_str(&body).expect("JSON results");
        if results == serde_json::json!(["awaited", "fetched", "stored"]) {
            return;
        }
        if start.elapsed() > Duration::from_secs(30) {
            panic!("task loop never completed: {results}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}
