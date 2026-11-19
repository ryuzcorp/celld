// Modified by Ryuz Corp (Cello): abort-cleared timer conformance (upstream #309). Apache-2.0.

// A black-box runner, not node code: it boots the real `cello dev` binary,
// aborts a background loop's sleep from another request, and asserts the
// loop's next timer still fires, so the execution boundary's injected
// clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for timers armed by background work after another request
//! clears the starter's last timer (denoland/celld#309): request A starts a
//! loop and answers; request B aborts it, which clears the loop's pending
//! sleep; the loop's continuation then arms a fresh 10 ms timer that must
//! fire so the loop reaches `done`.
//!
//! Before the fix the starter's driver retired with the fresh timer still
//! queued in its mailbox — the handoff landed after the driver's last take
//! but before its exit check — so the timer never fired and the loop wedged
//! at `aborted`. Now the driver takes the mailbox once more before leaving,
//! the timer rejoins the drive, and the loop completes.
//!
//! One `cello dev` boots the fixture per test. No fixed sleeps on the pass
//! path past the abort itself: the status is polled with a deadline, and
//! the server guard kills the child on every exit path.

mod common;

use common::{announced, cello_command, forward_announce, NO_EXTRA_ENV};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/abort-timer")
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
fn timer_armed_after_abort_clears_starter_still_fires() {
    let dir = tempfile::Builder::new()
        .prefix("cello-abort-timer-")
        .tempdir()
        .expect("tempdir");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot(&project);

    let (status, _) =
        http_get(&node.public, "/start?cell=t1", Duration::from_secs(15)).expect("start answers");
    assert_eq!(status, 200);
    // Land mid-flight: the loop is parked in its 200 ms sleep, well before
    // any budget, and the handler starts in milliseconds locally.
    std::thread::sleep(Duration::from_millis(300));
    let (status, _) =
        http_get(&node.public, "/abort?cell=t1", Duration::from_secs(15)).expect("abort answers");
    assert_eq!(status, 200);

    // The loop arms a fresh 10 ms timer after the abort clears its sleep.
    // Pre-fix the starter retired with that timer still queued in its
    // mailbox, so the state wedged at `aborted` until this deadline.
    let start = std::time::Instant::now();
    loop {
        let (status, body) = http_get(&node.public, "/status?cell=t1", Duration::from_secs(15))
            .expect("status answers");
        assert_eq!(status, 200, "{body}");
        if body == "done" {
            return;
        }
        assert_eq!(body, "aborted", "unexpected state: {body}");
        if start.elapsed() > Duration::from_secs(15) {
            panic!("loop never reached done: {body}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}
