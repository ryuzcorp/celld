// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): incoming spans carry HTTP method/status (upstream #285).

// A black-box runner, not node code: it boots the real `celld dev` binary
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Incoming Worker spans carry the HTTP method and the response status, and
//! a handler that answers 5xx without throwing still marks its span an error
//! (denoland/celld#285). The test boots `dev` with OTLP pointed at a capture
//! server, serves one 200 and one 503, and asserts the exported bytes carry
//! `http.request.method` and `http.response.status_code` plus the 5xx error.

mod common;

use common::{announced, forward_announce};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn celld_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_celld"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/telemetry-http")
        .canonicalize()
        .expect("fixture exists")
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// A minimal OTLP/HTTP capture server: it answers every export with 200 and
/// keeps the raw request bodies, so the test can assert on the span
/// attributes without decoding protobuf.
struct Capture {
    bodies: Arc<Mutex<Vec<Vec<u8>>>>,
    addr: String,
}

impl Capture {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("capture listener");
        listener
            .set_nonblocking(true)
            .expect("nonblocking capture listener");
        let addr = listener.local_addr().expect("capture addr").to_string();
        let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
        let kept = Arc::clone(&bodies);
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(120);
            while Instant::now() < deadline {
                let Ok((mut stream, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let mut raw = Vec::new();
                let mut buf = [0u8; 8192];
                let mut head = Vec::new();
                let content_length = loop {
                    match stream.read(&mut buf) {
                        Ok(0) => break None,
                        Ok(read) => {
                            head.extend_from_slice(&buf[..read]);
                            if let Some(end) = find_head_end(&head) {
                                let length = content_length(&head[..end]);
                                raw.extend_from_slice(&head[end..]);
                                break Some(length);
                            }
                        }
                        Err(_) => break None,
                    }
                };
                if let Some(length) = content_length {
                    while raw.len() < length {
                        match stream.read(&mut buf) {
                            Ok(0) => break,
                            Ok(read) => raw.extend_from_slice(&buf[..read]),
                            Err(_) => break,
                        }
                    }
                    raw.truncate(length);
                    kept.lock().expect("capture lock").push(raw);
                }
                let _ = stream.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
            }
        });
        Self { bodies, addr }
    }
}

fn find_head_end(head: &[u8]) -> Option<usize> {
    head.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|position| position + 4)
}

fn content_length(head: &[u8]) -> usize {
    let text = String::from_utf8_lossy(head).to_ascii_lowercase();
    text.lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}

struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(project: &Path, otel: &str) -> Self {
        let mut command = Command::new(celld_binary());
        command
            .arg("dev")
            .arg(project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--logs")
            .env("CELLD_OTEL", otel)
            .env("CELLD_OTEL_FLUSH_MS", "100")
            .env("OTEL_SERVICE_NAME", "telemetry-http-probe")
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
            if !probe(&public, "/ok") {
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

fn poll_until<T>(name: &str, deadline: Duration, mut condition: impl FnMut() -> Option<T>) -> T {
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

fn contains(bodies: &[Vec<u8>], needle: &str) -> bool {
    bodies.iter().any(|body| {
        body.windows(needle.len())
            .any(|window| window == needle.as_bytes())
    })
}

/// The exported spans name the HTTP method and status, and the 503 without
#[test]
fn incoming_spans_carry_method_and_status() {
    let capture = Capture::start();
    let project = tempfile::tempdir().expect("project dir");
    copy_dir(&fixture_dir(), project.path());
    let node = TestNode::boot(project.path(), &format!("http://{}", capture.addr));
    let (status, _) = get(&node.public, "/ok").expect("public GET /ok");
    assert_eq!(status, 200);
    let (status, _) = get(&node.public, "/fail").expect("public GET /fail");
    assert_eq!(status, 503);
    // The Durable Object calls go out with rewritten methods and, for the
    // failure, a masked status: the worker spans record GET/200 while the
    // DO's cell fetch spans must record POST-or-PUT and the 500, so the
    // export tells the two apart (denoland/celld#285).
    let (status, body) = get(&node.public, "/do-ok").expect("public GET /do-ok");
    assert_eq!(status, 200, "GET /do-ok: {body}");
    let fetched: serde_json::Value = serde_json::from_str(&body).expect("do-ok JSON");
    assert_eq!(
        fetched.get("body").and_then(|v| v.as_str()),
        Some("do-ok POST"),
        "the DO saw the rewritten method: {body}"
    );
    let (status, body) = get(&node.public, "/do-fail").expect("public GET /do-fail");
    assert_eq!(status, 200, "GET /do-fail: {body}");
    let fetched: serde_json::Value = serde_json::from_str(&body).expect("do-fail JSON");
    assert_eq!(
        fetched.get("doStatus").and_then(|v| v.as_u64()),
        Some(500),
        "the DO failed without throwing: {body}"
    );
    let bodies = poll_until("export", Duration::from_secs(60), || {
        let bodies = capture.bodies.lock().expect("capture lock");
        (contains(&bodies, "http.request.method")
            && contains(&bodies, "http.response.status_code")
            && contains(&bodies, "handler returned HTTP 503")
            && contains(&bodies, "POST")
            && contains(&bodies, "PUT")
            && contains(&bodies, "handler returned HTTP 500"))
        .then(|| bodies.clone())
    });
    assert!(contains(&bodies, "GET"), "the method value is exported");
    assert!(
        contains(&bodies, "celld.cell_fetch"),
        "the cell fetch spans are exported"
    );
    assert!(
        !contains(&bodies, "handler returned HTTP 200"),
        "a 200 without an exception stays successful"
    );
    // POST and PUT never ran against the worker — every worker request was
    // GET — so their presence proves the DO's cell fetch spans carry
    // `http.request.method`. The worker masked the DO's 500 as 200, so the
    // 500 error text can only come from the DO's span carrying the
    // status-driven error.
}
