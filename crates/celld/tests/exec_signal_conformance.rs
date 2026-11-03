// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): container exec AbortSignal
// conformance; container exec kill delivery conformance. Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for the AbortSignal `exec()` takes: an already-aborted
//! signal refuses with its reason before spawning, aborting later sends
//! SIGKILL to the process, an abort in flight kills the fresh process at
//! once, and a process that exits first unwires its listener so a later
//! abort sends nothing. The node boots with `CELLD_TEST_WAIT=1`, which
//! defines the test-only fake exec engine the fixture drives: the test
//! environment has no container engine (image resolution needs Docker,
//! so a fixture cannot even declare `containers[]`), and the fake stands
//! in for it beneath the real `Container.prototype.exec` path.
//!
//! One `celld dev` boots the fixture; there are no fixed sleeps: every
//! wait polls an observable condition with a deadline, and the server
//! guard kills the child on every exit path.

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
        .join("tests/fixtures/exec-signal")
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
    fn boot(app_project: &Path) -> Self {
        let port = free_port();
        let mut child = Command::new(celld_binary())
            .arg("dev")
            .arg(app_project)
            .arg("--port")
            .arg(port.to_string())
            .arg("--no-watch")
            .arg("--logs")
            .env("CELLD_TEST_WAIT", "1")
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

#[test]
fn exec_signal_conformance() {
    let dir = tempfile::Builder::new()
        .prefix("celld-exec-signal-")
        .tempdir()
        .expect("tempdir");
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir(), &app_project);
    let node = TestNode::boot(&app_project);

    // Control first: an exec without a signal through the same fake
    // path settles, proving the fake engine itself is sound.
    let control = get_json(&node, "/control");
    assert_eq!(control["ok"], true, "{control}");
    assert_eq!(control["result"]["exitCode"], 3, "{control}");
    assert_eq!(
        control["result"]["kills"],
        serde_json::json!([]),
        "{control}"
    );

    // An already-aborted signal refuses with its reason -- an
    // AbortError for abort(), the custom reason for abort(reason) --
    // before spawning anything.
    let refused = get_json(&node, "/already-aborted");
    assert_eq!(refused["ok"], true, "{refused}");
    assert_eq!(refused["result"]["defaultName"], "AbortError", "{refused}");
    assert_eq!(refused["result"]["customSame"], true, "{refused}");
    assert_eq!(refused["result"]["customMessage"], "stop-it", "{refused}");
    assert_eq!(refused["result"]["spawned"], 0, "{refused}");

    // A non-AbortSignal signal is rejected; null counts as absent.
    let typed = get_json(&node, "/signal-type");
    assert_eq!(typed["ok"], true, "{typed}");
    assert_eq!(
        typed["result"]["invalid"], "TypeError: signal must be an AbortSignal.",
        "{typed}"
    );
    assert_eq!(typed["result"]["exitCode"], 0, "{typed}");

    // Aborting after the process exists sends SIGKILL, and the exit
    // the engine then reports settles exitCode. The params the native
    // op received carry no signal key: the JS object never crosses.
    let aborted = get_json(&node, "/abort-after-start");
    assert_eq!(aborted["ok"], true, "{aborted}");
    assert_eq!(
        aborted["result"]["killsBefore"],
        serde_json::json!([]),
        "{aborted}"
    );
    assert_eq!(
        aborted["result"]["killsAfter"],
        serde_json::json!([9]),
        "{aborted}"
    );
    assert_eq!(aborted["result"]["exitCode"], 137, "{aborted}");
    let params = &aborted["result"]["params"];
    assert_eq!(
        params["cmd"],
        serde_json::json!(["sleep", "30"]),
        "{aborted}"
    );
    assert!(params.get("signal").is_none(), "{aborted}");

    // An abort landing while the exec op is in flight kills the fresh
    // process at once instead of registering a listener.
    let flight = get_json(&node, "/abort-during-flight");
    assert_eq!(flight["ok"], true, "{flight}");
    assert_eq!(
        flight["result"]["kills"],
        serde_json::json!([9]),
        "{flight}"
    );
    assert_eq!(flight["result"]["exitCode"], 137, "{flight}");

    // The process exits first: the abort listener is gone, so a later
    // abort sends no kill to the dead process.
    let exited = get_json(&node, "/exit-before-abort");
    assert_eq!(exited["ok"], true, "{exited}");
    assert_eq!(exited["result"]["exitCode"], 0, "{exited}");
    assert_eq!(exited["result"]["kills"], serde_json::json!([]), "{exited}");
}

/// A fire-and-forget `kill()` (sync, returns void) still runs its op:
/// the fixture answers in the same turn without awaiting anything, and
/// the kill completion must land anyway. An op the event drops never
/// completes, so the suite observes delivery, not dispatch.
#[test]
fn exec_kill_delivery_conformance() {
    let dir = tempfile::Builder::new()
        .prefix("celld-exec-kill-delivery-")
        .tempdir()
        .expect("tempdir");
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir(), &app_project);
    let node = TestNode::boot(&app_project);

    let fired = get_json(&node, "/kill-delivery");
    assert_eq!(fired["ok"], true, "{fired}");

    // The completion lands once the driver polls the op; poll the
    // observable condition with a deadline, like the boot poll.
    let done = poll_until("kill completion", Duration::from_secs(30), || {
        let status = get_json(&node, "/kill-delivery-status");
        (status["ok"] == true && status["result"]["completed"] == serde_json::json!([9]))
            .then(|| status.to_string())
    });
    assert!(done.contains("[9]"), "{done}");
}
