// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): workflow budget-expiry continuation (upstream #299).

// A black-box runner, not node code: it boots the real `celld dev` binary
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A Workflow whose steps sum past `CELLD_HANDLER_BUDGET_S` keeps running
//! to completion across alarm retries instead of stalling `running` forever
//! (denoland/celld#299). Each drive runs what fits in the budget; when the
//! handler expires, the engine retries the alarm, and the redrive replays
//! the ledger and continues. The redrive used to park behind the killed
//! drive's unsettled promise and fail the same way on every retry.

mod common;

use common::{announced, forward_announce};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn celld_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_celld"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/workflow-budget")
        .canonicalize()
        .expect("fixture exists")
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "index.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(project: &Path) -> Self {
        let mut command = Command::new(celld_binary());
        command
            .arg("dev")
            .arg(project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--logs")
            // Twelve one-second steps under a five-second handler budget:
            // no single drive can finish, so completion proves every redrive
            // replays and continues instead of stalling.
            .env("CELLD_HANDLER_BUDGET_S", "5")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn celld dev");
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
                    "celld dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            let public = announced(&announced_lines, "celld listening on ")?;
            if !probe(&public, "/start?id=bootprobe") {
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

fn poll_until<T>(
    name: &str,
    deadline: Duration,
    mut condition: impl FnMut() -> Option<T>,
) -> T {
    let start = Instant::now();
    loop {
        if let Some(done) = condition() {
            return done;
        }
        if start.elapsed() > deadline {
            panic!("{name}: condition unmet within the deadline");
        }
        std::thread::sleep(Duration::from_secs(2));
    }
}

/// Twelve one-second steps under a five-second budget complete: every drive
/// does what fits, the budget kills the rest, and each redrive continues.
#[test]
fn workflow_steps_past_the_handler_budget_complete() {
    let project = tempfile::tempdir().expect("project dir");
    copy_dir(&fixture_dir(), project.path());
    let node = TestNode::boot(project.path());
    let (status, body) = get(&node.public, "/start?id=budget").expect("public GET /start");
    assert_eq!(status, 200, "start: {body}");
    let reply: Value = poll_until("complete", Duration::from_secs(150), || {
        let (status, body) = get(&node.public, "/status?id=budget").ok()?;
        if status != 200 {
            return None;
        }
        let reply: Value = serde_json::from_str(&body).ok()?;
        (reply["status"] == "complete").then_some(reply)
    });
    assert_eq!(
        reply["output"], serde_json::json!({"done": 12}),
        "all twelve steps ran exactly once: {reply}"
    );
}
