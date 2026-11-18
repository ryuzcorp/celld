// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): gzip decode regression test (upstream #238).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// serves a deterministic gzip fixture over loopback, and polls TCP with
// wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! Upstream denoland/celld#238: `fetch()` returned the raw gzip-compressed
//! response body instead of transparently decompressing it. workerd decodes
//! a lone gzip/deflate coding when the worker leaves `accept-encoding` to
//! the runtime, while keeping `content-encoding`. An explicit worker coding
//! still takes the body raw.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// gzip.compress(b'{"gzipped":true,"message":"hello"}', mtime=0): the 34-byte
// JSON as 54 deterministic wire bytes (1f 8b magic). The deflate form is the
// 42-byte zlib stream for the same JSON.
const GZIP_BODY: &[u8] = &[
    31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 171, 86, 74, 175, 202, 44, 40, 72, 77, 81, 178, 42, 41, 42,
    77, 213, 81, 202, 77, 45, 46, 78, 76, 79, 85, 178, 82, 202, 72, 205, 201, 201, 87, 170, 5, 0,
    230, 41, 169, 169, 34, 0, 0, 0,
];
const DEFLATE_BODY: &[u8] = &[
    120, 156, 171, 86, 74, 175, 202, 44, 40, 72, 77, 81, 178, 42, 41, 42, 77, 213, 81, 202, 77, 45,
    46, 78, 76, 79, 85, 178, 82, 202, 72, 205, 201, 201, 87, 170, 5, 0, 214, 214, 12, 17,
];
const RAW_JSON: &str = r#"{"gzipped":true,"message":"hello"}"#;

fn cello_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cello"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/gzip-decode")
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

fn copy_dir(src: &Path, dst: &Path, gzip_port: u16) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    std::fs::copy(src.join("wrangler.json"), dst.join("wrangler.json"))
        .expect("copy wrangler.json");
    let worker = std::fs::read_to_string(src.join("worker.js")).expect("read worker.js");
    std::fs::write(
        dst.join("worker.js"),
        worker.replace("__GZIP_PORT__", &gzip_port.to_string()),
    )
    .expect("write worker.js");
}

/// A booted `cello dev` and its address. Kills the child on drop, so a
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

fn read_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("read request head");
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            return head;
        }
        assert!(head.len() < 8192, "request head too long");
    }
}

/// The deterministic gzip origin: every GET answers the canned body with a
/// `gzip` (or `deflate`, by path) coding and an exact `Content-Length`,
/// like the issue's fixture.
fn serve_gzip(listener: TcpListener, max_requests: usize) {
    let mut served = 0;
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(stream) => stream,
            Err(_) => continue,
        };
        let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
        let head = read_head(&mut stream);
        let deflate = head
            .windows(b"/deflate".len())
            .any(|window| window == b"/deflate");
        let (coding, body) = if deflate {
            ("deflate", DEFLATE_BODY)
        } else {
            ("gzip", GZIP_BODY)
        };
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: {coding}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(body);
        served += 1;
        if served >= max_requests {
            return;
        }
    }
}

#[test]
fn fetch_decompresses_gzip_transparently() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind gzip origin");
    let gzip_port = listener.local_addr().expect("origin addr").port();
    std::thread::spawn(move || serve_gzip(listener, 32));

    let fixture = fixture_dir();
    let project = fixture_tempdir("cello-gzip-decode-");
    copy_dir(&fixture, project.path(), gzip_port);
    let node = TestNode::boot(project.path());

    // The runtime-owned coding decodes: status and content-encoding stay,
    // but the body is the 34 decoded bytes.
    let (status, body) = get(&node.public, "/").expect("GET /");
    assert_eq!(status, 200, "GET /: {body}");
    let out: serde_json::Value = serde_json::from_str(&body).expect("result JSON");
    let field = |name: &str| {
        out.get(name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
            .clone()
    };
    assert_eq!(field("status").as_u64(), Some(200));
    assert_eq!(field("contentEncoding").as_str(), Some("gzip"));
    assert_eq!(field("bytes").as_u64(), Some(RAW_JSON.len() as u64));
    assert_eq!(field("first").as_u64(), Some(0x7b22));
    assert_eq!(
        field("json")
            .as_object()
            .and_then(|o| o.get("gzipped"))
            .and_then(|v| v.as_bool()),
        Some(true),
        "json parses: {body}"
    );
    assert_eq!(field("text").as_str(), Some(RAW_JSON));

    // An explicit worker coding takes the 54 wire bytes raw.
    let (status, body) = get(&node.public, "/explicit").expect("GET /explicit");
    assert_eq!(status, 200, "GET /explicit: {body}");
    let out: serde_json::Value = serde_json::from_str(&body).expect("result JSON");
    let field = |name: &str| {
        out.get(name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
            .clone()
    };
    assert_eq!(field("bytes").as_u64(), Some(GZIP_BODY.len() as u64));
    assert_eq!(field("first").as_u64(), Some(0x1f8b));
    assert_eq!(field("jsonError").as_str(), Some("SyntaxError"));

    // The deflate coding decodes the same way.
    let (status, body) = get(&node.public, "/deflate").expect("GET /deflate");
    assert_eq!(status, 200, "GET /deflate: {body}");
    let out: serde_json::Value = serde_json::from_str(&body).expect("result JSON");
    let field = |name: &str| {
        out.get(name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
            .clone()
    };
    assert_eq!(field("contentEncoding").as_str(), Some("deflate"));
    assert_eq!(field("bytes").as_u64(), Some(RAW_JSON.len() as u64));
    assert_eq!(field("text").as_str(), Some(RAW_JSON));
}
