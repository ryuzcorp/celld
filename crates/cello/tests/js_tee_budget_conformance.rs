// Modified by Ryuz Corp (Cello): a JS tee's branches share a skew budget.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A black-box runner, not node code: it boots the binary, copies the
// fixture, and reads TCP with wall-clock deadlines, so the execution
// boundary's injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A `ReadableStream.prototype.tee()` bounds the skew between its branches.
//!
//! `tee()` used to enqueue every chunk the faster branch pulled into both
//! branch controllers, so a branch nobody read buffered the whole source:
//! `const [a, b] = response.body.tee(); drain(a)` grew the isolate's heap by
//! the entire body. That is a fleet-OOM class rather than one Worker's
//! failure, because `near_heap_limit` extends the shared process's heap
//! instead of failing the tenant. `Response.clone()` / `Request.clone()` of
//! a Worker-constructed streaming body tee it the same way, so the unread
//! copy has the same problem.
//!
//! The fix fails a branch only for actual lag: a branch whose queued bytes
//! exceed a live sibling's by more than the host tee's 32 MiB
//! (`HTTP_TEE_MAX_SKEW_BYTES`). The drained branch keeps flowing to the
//! end, the source is not cancelled while a branch is live, and the failing
//! read rejects with the host tee's error and message. A single chunk
//! larger than the budget is not lag, so it still reaches both copies; the
//! accounting probe is total (no chunk code runs), so an unusual chunk
//! cannot strand a branch; and every chunk costs at least the 64-byte floor,
//! so empty chunks are not free.
//!
//! The fixture asserts all of that inside the isolate -- byte chunks, string
//! chunks, byte-stream (BYOB) branches, `Response.clone()`, an oversize
//! chunk read in lockstep and with one branch unread, a fake `ArrayBuffer`
//! and a Proxy chunk, a forced enqueue throw, and a source of only empty
//! chunks -- and the test only checks that it reported no failures. The byte
//! paths produce more than the budget on purpose (36 MiB for one branch,
//! 80 MiB for lockstep, 40 MiB in one chunk), so the bound has to be real
//! rather than incidental.

mod common;

use common::{announced, cello_command, forward_announce};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/js-tee-budget")
        .canonicalize()
        .expect("fixture exists")
}

fn fixture_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    match builder.tempdir_in("/dev/shm") {
        Ok(dir) => dir,
        Err(error) => {
            eprintln!("fixture tempdir fell back to disk (/dev/shm unusable: {error})");
            builder.tempdir().expect("fixture tempdir")
        }
    }
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("copy fixture dir");
    for entry in std::fs::read_dir(src).expect("read fixture dir") {
        let entry = entry.expect("fixture entry");
        let to = dst.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).expect("copy fixture file");
        }
    }
}

/// A booted `cello dev` and its addresses. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(project: &Path) -> Self {
        // Port zero: the OS assigns the worker port, so parallel boots never
        // claim the same one; discovery below learns it.
        let mut child = cello_command(project, [("NO_COLOR".to_string(), "1".to_string())])
            .arg("dev")
            .arg(project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--clean")
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
        // The announce line names the public address; `/health` answers once
        // the node is serving. Both waits share one deadline, and either
        // failure reports the supervisor's lines so a boot that never got
        // that far is still diagnosable.
        let deadline = Instant::now() + Duration::from_secs(180);
        let public = loop {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!(
                    "cello dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            if let Some(public) = announced(&announced_lines, "cello listening on ") {
                if probe(&public, "/health") {
                    break public;
                }
            }
            if Instant::now() > deadline {
                panic!(
                    "boot: no healthy node within the deadline: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        node.public = public;
        node
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One complete HTTP/1.1 exchange, for the fixture's JSON routes. The
/// conformance run allocates a few hundred MiB in total, so the deadline is
/// generous; nothing waits on a fixed sleep.
fn request(addr: &str, path: &str, timeout: Duration) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire =
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: */*\r\n\r\n");
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
    request(addr, path, Duration::from_secs(120))
}

fn probe(addr: &str, path: &str) -> bool {
    matches!(request(addr, path, Duration::from_secs(2)), Ok((200, _)))
}

fn boot_node() -> (tempfile::TempDir, TestNode) {
    let dir = fixture_tempdir("cello-js-tee-budget-");
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir(), &app_project);
    let node = TestNode::boot(&app_project);
    (dir, node)
}

/// The in-isolate checks: an unread byte branch and an unread clone fail
/// with the budget error while the drained copy completes, lockstep copies
/// pass well beyond the budget, and a string-chunk branch and a BYOB
/// branch are bounded too.
#[test]
fn js_tee_bounds_the_skew_between_its_branches() {
    let (_dir, node) = boot_node();
    let (status, body) = get(&node.public, "/conformance").expect("conformance answers");
    assert_eq!(status, 200, "conformance failed: {body}");
    let report: Value = serde_json::from_str(&body).expect("conformance json");
    let failures: Vec<String> = report["failures"]
        .as_array()
        .expect("failures array")
        .iter()
        .map(|failure| failure.as_str().unwrap_or_default().to_string())
        .collect();
    assert!(
        failures.is_empty(),
        "JS tee skew-budget conformance failed:\n  {}",
        failures.join("\n  ")
    );
}
