// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): loader abort propagation regression test (upstream #281).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// serves a hanging provider over loopback, and polls TCP with wall-clock
// deadlines.
#![allow(clippy::disallowed_methods)]

//! Upstream denoland/celld#281: aborting a fetch through Worker Loader left
//! the loaded Worker's pending binding request open — and the provider
//! connection with it. The abort reached the caller's promise but never the
//! child: publishing the abort waited on the loader op's select, which the
//! retired caller entry never polled again. The abort now publishes
//! synchronously from the cancel path, so the child's `request.signal`
//! fires and its outbound fetch cancels.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn cello_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cello"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/loader-abort")
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

fn copy_dir(src: &Path, dst: &Path, provider_port: u16) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    std::fs::copy(src.join("wrangler.json"), dst.join("wrangler.json"))
        .expect("copy wrangler.json");
    let worker = std::fs::read_to_string(src.join("worker.js")).expect("read worker.js");
    std::fs::write(
        dst.join("worker.js"),
        worker.replace("__PROVIDER_PORT__", &provider_port.to_string()),
    )
    .expect("write worker.js");
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

fn body_text(addr: &str, path: &str) -> String {
    let (status, body) = get(addr, path).unwrap_or_else(|error| panic!("GET {path}: {error}"));
    assert_eq!(status, 200, "GET {path}: {body}");
    body
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
        stream.read_exact(&mut byte).expect("read provider head");
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            return head;
        }
        assert!(head.len() < 8192, "provider head too long");
    }
}

/// The hanging provider: every connection is read to its head, then held
/// open until the peer hangs up. Each observed hangup bumps `closed`.
fn serve_hanging_provider(listener: TcpListener, closed: Arc<AtomicUsize>) {
    for stream in listener.incoming().take(8) {
        let closed = Arc::clone(&closed);
        std::thread::spawn(move || {
            let mut stream = match stream {
                Ok(stream) => stream,
                Err(_) => return,
            };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
            let _ = read_head(&mut stream);
            // Held open: the request never answers. A close (or a read
            // timeout long after the test ends) ends this watch.
            let mut byte = [0u8; 1];
            match stream.read(&mut byte) {
                Ok(0) | Err(_) => {}
                Ok(_) => {}
            }
            closed.fetch_add(1, Ordering::SeqCst);
        });
    }
}

#[test]
fn loader_abort_cancels_the_child_request() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind provider");
    let provider_port = listener.local_addr().expect("provider addr").port();
    let closed = Arc::new(AtomicUsize::new(0));
    std::thread::spawn({
        let closed = Arc::clone(&closed);
        move || serve_hanging_provider(listener, closed)
    });

    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-loader-abort-");
    copy_dir(&fixture, project.path(), provider_port);
    let node = TestNode::boot(project.path());

    // Control: aborting a direct fetch closes the provider connection.
    let body = body_text(&node.public, "/direct");
    assert_eq!(body, "direct-saw-abort:AbortError", "direct: {body}");
    poll_until("direct provider close", Duration::from_secs(10), || {
        (closed.load(Ordering::SeqCst) >= 1).then(|| "closed".to_string())
    });

    // The parent's own abort rejects its promise, as before.
    let body = body_text(&node.public, "/loaded");
    assert_eq!(body, "loaded-saw-abort:AbortError", "loaded: {body}");

    // Pre-fix the provider connection stayed open: the abort never reached
    // the loaded worker, so its outbound fetch never cancelled.
    poll_until("loaded provider close", Duration::from_secs(10), || {
        (closed.load(Ordering::SeqCst) >= 2).then(|| "closed".to_string())
    });
}
