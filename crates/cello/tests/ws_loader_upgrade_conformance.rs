// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): loader 101 upgrade regression test (upstream #305).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and speaks raw WebSocket framing over TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! Upstream denoland/celld#305: a 101 WebSocket response from a loaded
//! (dynamic) Worker's `fetch()` arrived at the caller with `webSocket: null`,
//! so returning it failed the client's handshake. workerd delivers the
//! socket. The loader fetch now carries the upgrade target (joining the
//! loaded worker's socket isolate-to-isolate), and the caller binds it like
//! any other upgrade.

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
        .join("tests/fixtures/ws-loader")
        .canonicalize()
        .expect("fixture exists")
}

mod common;
use common::{announced, forward_announce};

/// Tempdir for one fixture copy: `/dev/shm` when usable, else the
/// default tempdir.
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

fn plain_get(addr: &str, path: &str) -> Result<(u16, String), String> {
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
    matches!(plain_get(addr, path), Ok((200, _)))
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

fn read_exact(stream: &mut TcpStream, mut len: usize, what: &str, path: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut chunk = [0u8; 1024];
    while len > 0 {
        let take = chunk.len().min(len);
        let read = stream
            .read(&mut chunk[..take])
            .unwrap_or_else(|error| panic!("{path} {what} read: {error}"));
        assert!(read > 0, "{path} {what}: connection closed");
        out.extend_from_slice(&chunk[..read]);
        len -= read;
    }
    out
}

fn read_head(stream: &mut TcpStream, path: &str) -> String {
    let mut head = Vec::new();
    loop {
        let byte = read_exact(stream, 1, "handshake", path);
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            break;
        }
        assert!(head.len() < 8192, "handshake head too long");
    }
    String::from_utf8(head).expect("handshake head utf8")
}

/// Handshake, send "hi", and read one echo frame on `path`.
fn websocket_roundtrip(addr: &str, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .expect("read timeout");
    let wire = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
    );
    stream.write_all(wire.as_bytes()).expect("write handshake");
    let head = read_head(&mut stream, path);
    let status = head.lines().next().expect("status line").to_string();
    assert!(
        status.contains("101"),
        "{path}: expected 101 upgrade, got: {status} ({head})"
    );
    // One masked "hi" frame.
    let mask = [0x11u8, 0x22, 0x33, 0x44];
    let payload = b"hi";
    let mut frame = vec![0x81u8, 0x80 | payload.len() as u8];
    frame.extend_from_slice(&mask);
    frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    stream.write_all(&frame).expect("write frame");
    // One unmasked server frame back.
    let hdr = read_exact(&mut stream, 2, "frame header", path);
    assert_eq!(
        hdr[0], 0x81,
        "{path}: expected a text frame, got {:02x}",
        hdr[0]
    );
    assert_eq!(hdr[1] & 0x80, 0, "{path}: server frame must not be masked");
    let body = read_exact(&mut stream, (hdr[1] & 0x7f) as usize, "frame body", path);
    String::from_utf8(body).expect("echo utf8")
}

#[test]
fn loader_101_response_carries_its_websocket() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-ws-loader-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    let direct = websocket_roundtrip(&node.public, "/direct");
    assert_eq!(direct, "echo:hi", "control socket: {direct}");

    // Pre-fix these handshakes failed: the caller got status 101 with
    // `webSocket: null` and answered without an upgrade instead.
    for path in ["/loaded", "/loaded-do"] {
        let echo = websocket_roundtrip(&node.public, path);
        assert_eq!(echo, "echo:hi", "{path} socket: {echo}");
    }
}

#[test]
fn caller_initiated_close_reaches_the_caller() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-ws-loader-close-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // The caller accepts the loaded socket and closes it: its own close
    // event carries the code, reason, and clean flag it sent. Before the
    // fix the caller's pull queue was unregistered with no terminal, so
    // its pump reported an abnormal 1006 instead.
    let (status, body) = plain_get(&node.public, "/caller-close").expect("GET /caller-close");
    assert_eq!(status, 200, "GET /caller-close: {body}");
    let info: serde_json::Value = serde_json::from_str(&body).expect("close JSON");
    let field = |name: &str| {
        info.get(name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
            .clone()
    };
    assert_eq!(field("code").as_u64(), Some(1000), "close code: {body}");
    assert_eq!(
        field("reason").as_str(),
        Some("done"),
        "close reason: {body}"
    );
    assert_eq!(field("wasClean").as_bool(), Some(true), "wasClean: {body}");
}

#[test]
fn flooded_pipe_still_delivers_caller_close() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-ws-loader-flood-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // The loaded worker accepts but stops reading on the first message (a
    // throwing listener ends its pump), so the caller's flood fills the
    // loaded queue and the close queues behind the flooded frames. Before the
    // fix the pipe parked in a queue send while frames were pending and never
    // observed the close: both sockets stayed open and the caller's close
    // event never arrived (the handler times out with a 500).
    let (status, body) = plain_get(&node.public, "/flood-close").expect("GET /flood-close");
    assert_eq!(status, 200, "GET /flood-close: {body}");
    let info: serde_json::Value = serde_json::from_str(&body).expect("close JSON");
    let field = |name: &str| {
        info.get(name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
            .clone()
    };
    assert_eq!(field("code").as_u64(), Some(1000), "close code: {body}");
    assert_eq!(
        field("reason").as_str(),
        Some("done"),
        "close reason: {body}"
    );
    assert_eq!(field("wasClean").as_bool(), Some(true), "wasClean: {body}");
}

#[test]
fn overloaded_pipe_closes_with_1011() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-ws-loader-overload-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // The caller floods past the pipe's staged-bytes cap into a worker that
    // stopped reading, and never closes. The pipe must tear itself down
    // with 1011 instead of staging the flood without bound; the cap makes
    // the flood fail closed instead of hanging the socket (the handler
    // times out with a 500 when no close arrives).
    let (status, body) = plain_get(&node.public, "/flood-overload").expect("GET /flood-overload");
    assert_eq!(status, 200, "GET /flood-overload: {body}");
    let info: serde_json::Value = serde_json::from_str(&body).expect("close JSON");
    let field = |name: &str| {
        info.get(name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
            .clone()
    };
    assert_eq!(field("code").as_u64(), Some(1011), "close code: {body}");
    assert_eq!(field("wasClean").as_bool(), Some(false), "wasClean: {body}");
}

#[test]
fn closing_pipe_flushes_queued_frames_first() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-ws-loader-order-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // The caller floods a slow reader, then sends one text and closes. The
    // close queued behind the flood must deliver every frame first, in
    // order, and then arrive as the caller's own close event. Dropping the
    // queued frames with the close loses the trailing text.
    let (status, body) = plain_get(&node.public, "/order-close").expect("GET /order-close");
    assert_eq!(status, 200, "GET /order-close: {body}");
    let info: serde_json::Value = serde_json::from_str(&body).expect("close JSON");
    let field = |name: &str| {
        info.get(name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
            .clone()
    };
    assert_eq!(field("code").as_u64(), Some(1000), "close code: {body}");
    assert_eq!(
        field("reason").as_str(),
        Some("done"),
        "close reason: {body}"
    );
    assert_eq!(field("wasClean").as_bool(), Some(true), "wasClean: {body}");
    let (status, body) = plain_get(&node.public, "/room-messages").expect("GET /room-messages");
    assert_eq!(status, 200, "GET /room-messages: {body}");
    let recorded: serde_json::Value = serde_json::from_str(&body).expect("messages JSON");
    let messages = recorded
        .get("messages")
        .and_then(|value| value.as_array())
        .unwrap_or_else(|| panic!("messages missing: {body}"));
    assert_eq!(
        messages.len(),
        34,
        "every flooded frame must flush first: {body}"
    );
    assert_eq!(
        messages.last().and_then(|value| value.as_str()),
        Some("last"),
        "the trailing text must arrive before the close: {body}"
    );
}

#[test]
fn queued_close_reaches_the_caller() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-ws-loader-queued-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // The caller receives a text, then blocks its event loop while the
    // loaded side closes: the close queued behind the text must still
    // arrive as its own close event. Unregistering the pull endpoint at
    // teardown makes the next `__ws_next` report a synthetic 1006 instead.
    let (status, body) = plain_get(&node.public, "/queued-close").expect("GET /queued-close");
    assert_eq!(status, 200, "GET /queued-close: {body}");
    let info: serde_json::Value = serde_json::from_str(&body).expect("close JSON");
    let field = |name: &str| {
        info.get(name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
            .clone()
    };
    assert_eq!(field("code").as_u64(), Some(1000), "close code: {body}");
    assert_eq!(
        field("reason").as_str(),
        Some("bye"),
        "close reason: {body}"
    );
    assert_eq!(field("wasClean").as_bool(), Some(true), "wasClean: {body}");
}

#[test]
fn queued_close_sync_reaches_the_caller() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-ws-loader-queued-sync-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // The loaded side echoes and closes synchronously, so both frames reach
    // the pipe before the caller's post-message burn ends: the next
    // `__ws_next` necessarily runs after teardown. A teardown that
    // unregisters the pull endpoint reports a synthetic 1006 here; keeping
    // the endpoint until its terminal is consumed delivers the queued close.
    // No timers: the burn outlasts native pipe teardown by orders of
    // magnitude, so this fails before the fix without racing.
    let (status, body) =
        plain_get(&node.public, "/queued-close-sync").expect("GET /queued-close-sync");
    assert_eq!(status, 200, "GET /queued-close-sync: {body}");
    let info: serde_json::Value = serde_json::from_str(&body).expect("close JSON");
    let field = |name: &str| {
        info.get(name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
            .clone()
    };
    assert_eq!(field("code").as_u64(), Some(1000), "close code: {body}");
    assert_eq!(
        field("reason").as_str(),
        Some("bye"),
        "close reason: {body}"
    );
    assert_eq!(field("wasClean").as_bool(), Some(true), "wasClean: {body}");
}

#[test]
fn loaded_queued_close_reaches_the_loaded() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-ws-loader-loaded-queued-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // The caller floods a slow reader and closes at once: the loaded
    // socket's close event must carry the caller's code even though text
    // was queued ahead of the terminal when the pipe tore down.
    let (status, body) =
        plain_get(&node.public, "/loaded-queued-close").expect("GET /loaded-queued-close");
    assert_eq!(status, 200, "GET /loaded-queued-close: {body}");
    let (status, body) = plain_get(&node.public, "/room-messages").expect("GET /room-messages");
    assert_eq!(status, 200, "GET /room-messages: {body}");
    let recorded: serde_json::Value = serde_json::from_str(&body).expect("messages JSON");
    let messages = recorded
        .get("messages")
        .and_then(|value| value.as_array())
        .unwrap_or_else(|| panic!("messages missing: {body}"));
    let closes = recorded
        .get("closes")
        .and_then(|value| value.as_array())
        .unwrap_or_else(|| panic!("closes missing: {body}"));
    let texts: Vec<&str> = messages.iter().filter_map(|value| value.as_str()).collect();
    assert_eq!(
        texts,
        vec!["a", "b"],
        "queued text must flush before the close: {body}"
    );
    assert_eq!(
        closes.len(),
        1,
        "the loaded socket must see exactly one close: {body}"
    );
    assert_eq!(
        closes[0].get("code").and_then(|code| code.as_u64()),
        Some(1000),
        "loaded close code: {body}"
    );
}

#[test]
fn cell_queued_close_reaches_the_caller() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-ws-do-queued-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // Same queued-close shape through a cell target: the caller accepts the
    // Durable Object's socket, receives a text, blocks, and the cell side
    // closes. Its own close event must carry the cell's code.
    let (status, body) = plain_get(&node.public, "/do-queued-close").expect("GET /do-queued-close");
    assert_eq!(status, 200, "GET /do-queued-close: {body}");
    let info: serde_json::Value = serde_json::from_str(&body).expect("close JSON");
    let field = |name: &str| {
        info.get(name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
            .clone()
    };
    assert_eq!(field("code").as_u64(), Some(1000), "close code: {body}");
    assert_eq!(
        field("reason").as_str(),
        Some("bye"),
        "close reason: {body}"
    );
    assert_eq!(field("wasClean").as_bool(), Some(true), "wasClean: {body}");
}

#[test]
fn cell_target_caller_initiated_close_reaches_the_caller() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-ws-do-close-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // Same shape through a cell target: the caller accepts the Durable
    // Object's socket and closes it, and its own close event carries the
    // code, reason, and clean flag it sent. Before the fix the caller's
    // pull queue was unregistered with no terminal, so its pump reported
    // an abnormal 1006 instead.
    let (status, body) = plain_get(&node.public, "/do-caller-close").expect("GET /do-caller-close");
    assert_eq!(status, 200, "GET /do-caller-close: {body}");
    let info: serde_json::Value = serde_json::from_str(&body).expect("close JSON");
    let field = |name: &str| {
        info.get(name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
            .clone()
    };
    assert_eq!(field("code").as_u64(), Some(1000), "close code: {body}");
    assert_eq!(
        field("reason").as_str(),
        Some("done"),
        "close reason: {body}"
    );
    assert_eq!(field("wasClean").as_bool(), Some(true), "wasClean: {body}");
}
