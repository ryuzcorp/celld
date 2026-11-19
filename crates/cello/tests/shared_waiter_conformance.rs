// Modified by Ryuz Corp (Cello): shared-fetch waiter conformance (upstream #316). Apache-2.0.

// A black-box runner, not node code: it boots the real `cello dev` binary,
// awaits one request's shared subrequest from a second request that then
// arms its own timer/fetch, and asserts the waiter answers end to end, so
// the execution boundary's injected clock/filesystem/async rules do not
// apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for shared-promise waiter attribution (denoland/celld#316):
//! B awaits A's shared fetch, then arms its own timer (or fetch). The
//! waiter's next op must belong to the waiter's driver — the 0036
//! continuation-token line attributes it to B and hands it over instead of
//! adopting it into A. Attributed to the wrong driver, the op dies with the
//! starter's answer and B hangs to its budget.
//!
//! The shared fetch targets a slow endpoint served by this test (500 ms),
//! so it is still in flight when B joins; B then arms a 20 ms timer (or a
//! second subrequest) and answers. Before the attribution fix the waiter
//! hung and the read below died on its deadline; now both legs answer in
//! about a second.
//!
//! One `cello dev` boots the fixture per test. There are no fixed sleeps on
//! the pass path beyond the 200 ms head start that lets A suspend first
//! (B's in-worker wait loop covers a slow starter); the read deadline only
//! fires when the waiter regresses to hanging. The server guard kills the
//! child on every exit path.

mod common;

use common::{announced, cello_command, forward_announce, NO_EXTRA_ENV};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/shared-waiter")
        .canonicalize()
        .expect("fixture exists")
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// The slow endpoint the shared fetch targets: `/slow` answers after
/// 500 ms, `/plain` at once. Plain blocking I/O on its own thread; the
/// worker's subrequest is what suspends, never this server.
fn slow_server() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind slow server");
    let addr = listener.local_addr().expect("slow server addr").to_string();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(stream) => stream,
                Err(_) => continue,
            };
            std::thread::spawn(move || {
                let mut raw = Vec::new();
                let mut head = [0u8; 1];
                // Read through the request head; the body (if any) is ignored.
                while raw.len() < 8192 {
                    match stream.read(&mut head) {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            raw.push(head[0]);
                            if raw.ends_with(b"\r\n\r\n") {
                                break;
                            }
                        }
                    }
                }
                let slow = raw.starts_with(b"GET /slow");
                if slow {
                    std::thread::sleep(Duration::from_millis(500));
                }
                let body = if slow { "slow-ok" } else { "plain-ok" };
                let _ = stream.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                );
            });
        }
    });
    format!("http://{addr}")
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

/// One leg: A starts the shared fetch in the background, B awaits it and
/// arms its own `then` op, and both must answer. The 200 ms head start lets
/// A suspend first, like the issue's repro; B's in-worker wait loop covers
/// a slow starter, so nothing here races.
fn leg(node: &TestNode, slow: &str, key: &str, then: &str) {
    let public = node.public.clone();
    let start_path = format!("/start?key={key}&slow={slow}/slow");
    let starter =
        std::thread::spawn(move || http_get(&public, &start_path, Duration::from_secs(15)));
    std::thread::sleep(Duration::from_millis(200));

    let join_path = format!("/join?key={key}&then={then}&slow={slow}/slow");
    let (status, body) = http_get(&node.public, &join_path, Duration::from_secs(15))
        .expect("B answers with its own work instead of hanging");
    assert_eq!(status, 200, "{body}");
    let answered: serde_json::Value = serde_json::from_str(&body).expect("JSON body");
    assert_eq!(answered["value"], "slow-ok", "{body}");
    assert_eq!(answered["then"], then, "{body}");

    let (status, body) = starter.join().expect("starter thread").expect("A answers");
    assert_eq!(status, 200, "{body}");
}

#[test]
fn shared_fetch_waiter_answers_with_its_own_work() {
    let dir = tempfile::Builder::new()
        .prefix("cello-shared-waiter-")
        .tempdir()
        .expect("tempdir");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot(&project);
    let slow = slow_server();

    // B awaits A's in-flight fetch, then arms its own timer.
    leg(&node, &slow, "timer", "timer");
    // Same shape with a second subrequest instead of a timer.
    leg(&node, &slow, "fetch", "fetch");
}
