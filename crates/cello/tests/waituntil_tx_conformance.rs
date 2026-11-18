// Modified by ryuzcorp (ryuzcorp/celld): waitUntil background storage transaction conformance (upstream #300). Apache-2.0.

// A black-box runner, not node code: it boots the real `celld dev` binary,
// drives a WebSocket pair like the issue's reproduction, and asserts the
// background storage transactions complete, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for background storage transactions (denoland/celld#300): a
//! `start` message registers `run()` with `ctx.waitUntil()`, and a later
//! `complete` message from another event resolves the promise `run()` waits
//! on. The two sequential storage transactions must then commit and read
//! back, even though the event that registered the work has retired and the
//! resuming continuation carries its retired token.
//!
//! Before the fix the first message's driver concluded "waiting on nothing"
//! the moment it answered with no native op of its own, abandoned the
//! `waitUntil` aggregate, and dropped the registering event's context. The
//! resumed transaction then failed with "the cell event ended before it
//! could acquire an input gate", and the error report over the same socket
//! failed with "the event that issued this operation already ended", so the
//! client saw nothing at all. Now the answered driver polls instead of
//! idling while background work is outstanding, the registering event stays
//! alive to lend its context, and the handoff lands on a live mailbox.
//!
//! One `celld dev` boots the fixture per test. There are no fixed sleeps:
//! every wait polls an observable condition with a deadline, and the server
//! guard kills the child on every exit path.

mod common;

use common::{announced, forward_announce};
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
        .join("tests/fixtures/waituntil-tx")
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
        node.public = poll_until("boot", Duration::from_secs(120), || {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!(
                    "celld dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            let public = announced(&announced_lines, "celld listening on ")?;
            if !probe(&public, "/health") {
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

/// Readiness probe with a short timeout: a bound-but-not-ready node holds
/// connections open, so a long read would eat the boot deadline.
fn probe(addr: &str, path: &str) -> bool {
    matches!(http_get(addr, path, Duration::from_secs(2)), Ok((200, _)))
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

/// One client side of a WebSocket, hand-rolled over TCP so the test needs
/// no extra dependencies. Server frames arrive unmasked; client frames go
/// out masked with a fixed mask. Only single-frame text messages occur in
/// this dance; anything else is a failure.
struct WsClient {
    stream: TcpStream,
}

impl WsClient {
    fn connect(addr: &str) -> Result<Self, String> {
        let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .map_err(|error| format!("timeout: {error}"))?;
        let wire = format!(
            "GET / HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        );
        stream
            .write_all(wire.as_bytes())
            .map_err(|error| format!("write: {error}"))?;
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            stream
                .read_exact(&mut byte)
                .map_err(|error| format!("read: {error}"))?;
            head.push(byte[0]);
            if head.ends_with(b"\r\n\r\n") {
                break;
            }
            if head.len() > 8192 {
                return Err(format!("oversize handshake: {head:?}"));
            }
        }
        let head = String::from_utf8(head).map_err(|error| format!("utf8: {error}"))?;
        if !head.starts_with("HTTP/1.1 101") {
            return Err(format!("no 101 upgrade: {head:?}"));
        }
        Ok(Self { stream })
    }

    fn send(&mut self, text: &str) -> Result<(), String> {
        let payload = text.as_bytes();
        if payload.len() > 125 {
            return Err(format!("test message too long: {}", payload.len()));
        }
        let mask = [0x11u8, 0x22, 0x33, 0x44];
        let mut frame = vec![0x81, 0x80 | payload.len() as u8];
        frame.extend_from_slice(&mask);
        frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
        self.stream
            .write_all(&frame)
            .map_err(|error| format!("write: {error}"))
    }

    fn recv(&mut self) -> Result<String, String> {
        loop {
            let mut hdr = [0u8; 2];
            self.stream
                .read_exact(&mut hdr)
                .map_err(|error| format!("read: {error}"))?;
            let opcode = hdr[0] & 0x0f;
            let masked = hdr[1] & 0x80 != 0;
            if masked {
                return Err("server sent a masked frame".to_string());
            }
            let mut len = (hdr[1] & 0x7f) as u64;
            if len == 126 {
                let mut ext = [0u8; 2];
                self.stream
                    .read_exact(&mut ext)
                    .map_err(|error| format!("read: {error}"))?;
                len = u16::from_be_bytes(ext) as u64;
            } else if len == 127 {
                let mut ext = [0u8; 8];
                self.stream
                    .read_exact(&mut ext)
                    .map_err(|error| format!("read: {error}"))?;
                len = u64::from_be_bytes(ext);
            }
            if len > 1 << 20 {
                return Err(format!("oversize frame: {len}"));
            }
            let mut payload = vec![0u8; len as usize];
            self.stream
                .read_exact(&mut payload)
                .map_err(|error| format!("read: {error}"))?;
            match opcode {
                0x1 => {
                    return String::from_utf8(payload).map_err(|error| format!("utf8: {error}"));
                }
                0x8 => return Err("server closed the socket".to_string()),
                0x9 => {
                    // Answer pings so a keepalive cannot wedge the dance.
                    let mut pong = vec![0x8a, 0x80 | payload.len() as u8];
                    let mask = [0x11u8, 0x22, 0x33, 0x44];
                    pong.extend_from_slice(&mask);
                    pong.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
                    self.stream
                        .write_all(&pong)
                        .map_err(|error| format!("write: {error}"))?;
                }
                _ => return Err(format!("unexpected opcode: {opcode:#x}")),
            }
        }
    }
}

#[test]
fn waituntil_background_storage_transactions_complete() {
    let dir = tempfile::Builder::new()
        .prefix("celld-waituntil-tx-")
        .tempdir()
        .expect("tempdir");
    let project = dir.path().join("app");
    copy_dir(&fixture_dir(), &project);
    let node = TestNode::boot(&project);

    let mut ws = WsClient::connect(&node.public).expect("websocket upgrade");
    ws.send("start").expect("send start");
    let waiting = ws.recv().expect("run() to wait on its gate promise");
    assert_eq!(waiting, "waiting", "{waiting}");
    ws.send("complete").expect("send complete");
    // Pre-fix this read timed out: the background transactions never ran
    // and the error report never arrived, so only the deadline spoke.
    let done = poll_until("background transactions", Duration::from_secs(60), || {
        ws.recv().ok()
    });
    let result: serde_json::Value = serde_json::from_str(&done).expect("JSON result");
    assert_eq!(result["first"], 1, "{result}");
    assert_eq!(result["second"], 2, "{result}");
}
