// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): server-initiated close regression test (upstream #279).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and speaks raw WebSocket framing over TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! Upstream denoland/celld#279: a Worker that accepted the server end of a
//! `WebSocketPair` and later called `server.close(code, reason)` never
//! closed the client's socket. The close-code validation rejected every
//! code outside 1000/3000-4999 — including the 1008 policy-violation close
//! the Worker used — so the listener threw and no close frame was ever
//! sent. The validation now matches workerd's sendable range.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/ws-close")
        .canonicalize()
        .expect("fixture exists")
}

mod common;
use common::{announced, cello_command, forward_announce};

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

/// A booted `cello dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path) -> Self {
        let mut child = cello_command(app_project, [("NO_COLOR", "1")])
            .arg("dev")
            .arg(app_project)
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
                panic!("cello dev exited during boot: {status}");
            }
            let addr = announced(&announced_lines, "cello listening on ")?;
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
        assert!(head.len() < 8192, "{path}: handshake head too long");
    }
    String::from_utf8(head).expect("handshake head utf8")
}

/// Handshake, send "hi", and read frames until the close frame, returning
/// its code and reason.
fn await_server_close(addr: &str, path: &str) -> (u16, String) {
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
    let mask = [0x11u8, 0x22, 0x33, 0x44];
    let payload = b"hi";
    let mut frame = vec![0x81u8, 0x80 | payload.len() as u8];
    frame.extend_from_slice(&mask);
    frame.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i % 4]));
    stream.write_all(&frame).expect("write frame");
    loop {
        let hdr = read_exact(&mut stream, 2, "frame header", path);
        let opcode = hdr[0] & 0x0f;
        let mut len = (hdr[1] & 0x7f) as usize;
        if len == 126 {
            let ext = read_exact(&mut stream, 2, "frame length", path);
            len = u16::from_be_bytes([ext[0], ext[1]]) as usize;
        } else {
            assert!(len < 126, "{path}: unexpected 64-bit frame length");
        }
        assert_eq!(hdr[1] & 0x80, 0, "{path}: server frame must not be masked");
        let body = read_exact(&mut stream, len, "frame body", path);
        if opcode == 0x8 {
            assert!(body.len() >= 2, "{path}: short close frame");
            let code = u16::from_be_bytes([body[0], body[1]]);
            let reason = String::from_utf8(body[2..].to_vec()).expect("close reason utf8");
            return (code, reason);
        }
        assert!(
            opcode == 0x1 || opcode == 0x2,
            "{path}: expected data or close, got opcode {opcode}"
        );
    }
}

#[test]
fn server_close_reaches_the_client() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("cello-ws-close-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // The sendable range accepts the policy-violation close and the
    // application range, and still refuses reserved and out-of-range codes.
    for (code, allowed) in [(1008, true), (3000, true), (1005, false), (999, false)] {
        let (status, body) = get(&node.public, &format!("/validate?code={code}"))
            .unwrap_or_else(|error| panic!("GET /validate?code={code}: {error}"));
        assert_eq!(status, 200, "GET /validate?code={code}: {body}");
        let threw_null = body.contains("\"threw\":null");
        assert_eq!(
            threw_null, allowed,
            "close({code}): expected allowed={allowed}, got {body}"
        );
    }

    // Pre-fix the client hung here: the message listener threw on the
    // 1008 close, so no close frame ever arrived.
    let (code, reason) = await_server_close(&node.public, "/");
    assert_eq!(code, 1008, "close code");
    assert_eq!(reason, "refused", "close reason");
}

/// One 20 MiB binary message through the tenant pump echoes intact instead of
/// ending the socket loudly with 1011: the otherwise-empty read stage and the
/// output channel each admit one message up to the message cap. Reverting
/// the head exemption ends this with 1011, so it fails before the fix.
#[test]
fn large_message_passes_an_idle_pump() {
    const PATH: &str = "/big-echo";
    const BIG: usize = 20 * 1024 * 1024;
    let fixture = fixture_dir();
    let project = fixture_tempdir("cello-ws-big-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    let mut stream = TcpStream::connect(node.public.as_str()).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(180)))
        .expect("read timeout");
    let wire = format!(
        "GET {PATH} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n",
        addr = node.public,
    );
    stream.write_all(wire.as_bytes()).expect("write handshake");
    let head = read_head(&mut stream, PATH);
    assert!(
        head.lines().next().is_some_and(|line| line.contains("101")),
        "{PATH}: expected 101 upgrade, got: {head}"
    );
    // One masked binary frame with a 64-bit length and a checkable pattern.
    let mask = [0x11u8, 0x22, 0x33, 0x44];
    let mut frame = vec![0x82u8, 0xFF];
    frame.extend_from_slice(&(BIG as u64).to_be_bytes());
    frame.extend_from_slice(&mask);
    frame.reserve(BIG);
    for i in 0..BIG {
        frame.push((i % 251) as u8 ^ mask[i % 4]);
    }
    stream.write_all(&frame).expect("write frame");
    drop(frame);
    // The echo arrives whole; a close frame first means the pump ended the
    // socket loudly instead of staging the message.
    loop {
        let hdr = read_exact(&mut stream, 2, "frame header", PATH);
        let opcode = hdr[0] & 0x0f;
        let mut len = (hdr[1] & 0x7f) as u64;
        if len == 126 {
            let ext = read_exact(&mut stream, 2, "frame length", PATH);
            len = u16::from_be_bytes([ext[0], ext[1]]) as u64;
        } else if len == 127 {
            let ext = read_exact(&mut stream, 8, "frame length", PATH);
            len = u64::from_be_bytes([
                ext[0], ext[1], ext[2], ext[3], ext[4], ext[5], ext[6], ext[7],
            ]);
        }
        assert_eq!(hdr[1] & 0x80, 0, "{PATH}: server frame must not be masked");
        let body = read_exact(&mut stream, len as usize, "frame body", PATH);
        match opcode {
            0x2 => {
                assert_eq!(body.len(), BIG, "{PATH}: echo length");
                assert!(
                    body.iter().enumerate().all(|(i, b)| *b == (i % 251) as u8),
                    "{PATH}: echo payload is intact"
                );
                return;
            }
            0x8 => {
                let code = u16::from_be_bytes([body[0], body[1]]);
                panic!("{PATH}: pump closed the socket with {code} instead of echoing");
            }
            _ => continue,
        }
    }
}
