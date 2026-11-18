// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): post-response outbound dial regression test (upstream #254).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! Upstream denoland/celld#254: in a Durable Object, an outbound WebSocket
//! opened from the continuation of a promise created by an earlier request,
//! after that request's response was sent, failed with `WebSocket upgrade
//! failed: isolate stopped reading WebSocket`. The continuation had lost
//! its actor scope when the dispatch ended, so the dial left as a Worker
//! isolate socket of an already-retired event, and the turn-end cleanup
//! closed it before the connector opened it. The dial now recovers its
//! cell from the request's egress frame, routing like a dial made inside
//! the request — a Durable Object's I/O belongs to the object.

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
        .join("tests/fixtures/do-later-ws")
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

fn body_json(addr: &str, path: &str) -> serde_json::Value {
    let (status, body) = get(addr, path).unwrap_or_else(|error| panic!("GET {path}: {error}"));
    assert_eq!(status, 200, "GET {path}: {body}");
    serde_json::from_str(&body).expect("JSON body")
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

/// The dial log, polled until it holds every expected row.
fn dial_log(node: &TestNode, name: &str) -> Vec<String> {
    poll_until("dial log", Duration::from_secs(30), || {
        let out = body_json(&node.public, &format!("/?name={name}"));
        let rows = out.as_array().expect("log rows");
        if rows.len() < 3 {
            return None;
        }
        Some(
            rows.iter()
                .map(|row| row.as_str().expect("log row").to_string())
                .collect::<Vec<_>>()
                .join("\n"),
        )
    })
    .lines()
    .map(str::to_string)
    .collect()
}

fn row_message(row: &str) -> &str {
    row.split_once("ms ")
        .map(|(_, rest)| rest)
        .unwrap_or_else(|| panic!("log row shape: {row}"))
}

#[test]
fn outbound_dial_after_the_response_lands() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("cello-do-later-ws-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // The control: a dial made inside the request opens and echoes twice.
    let (status, _) = get(&node.public, "/now?name=a").expect("GET /now");
    assert_eq!(status, 200);
    let rows = dial_log(&node, "a");
    assert_eq!(row_message(&rows[0]), "open");
    assert!(
        row_message(&rows[1]).starts_with("message echo 1"),
        "{rows:?}"
    );
    assert!(
        row_message(&rows[2]).starts_with("message echo 2"),
        "{rows:?}"
    );

    // Pre-fix this threw `WebSocket upgrade failed: isolate stopped
    // reading WebSocket`: the post-response dial lost its cell scope.
    let (status, body) = get(&node.public, "/later?name=b").expect("GET /later");
    assert_eq!((status, body.as_str()), (200, "will dial\n"));
    let rows = dial_log(&node, "b");
    assert_eq!(row_message(&rows[0]), "open", "{rows:?}");
    assert!(
        row_message(&rows[1]).starts_with("message echo 1"),
        "{rows:?}"
    );
    assert!(
        row_message(&rows[2]).starts_with("message echo 2"),
        "{rows:?}"
    );
}
