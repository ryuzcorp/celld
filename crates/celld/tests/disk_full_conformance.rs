// Modified by ryuzcorp (ryuzcorp/celld): full-disk recovery test (upstream #291).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A black-box runner, not node code: it boots the binary and drives HTTP
// plus a page-count cap through the OS, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A full disk must not poison a cell past recovery (upstream #291): a
//! transactional write that fails with SQLITE_FULL destroys its
//! transaction, and every later request — reads included — fails until
//! something reopens the database. Clamping the node's file-size limit
//! deterministically produces the same destroyed-transaction poison —
//! `poisons_actor` classifies FULL, IOERR, NOMEM, and INTERRUPT together —
//! and lifting the limit simulates space coming back. (A page-count cap
//! would be the exact SQLITE_FULL trigger, but `max_page_count` is
//! per-connection: an external PRAGMA never reaches the cell's own
//! connection, and the in-process test hook is not compiled into the dev
//! binary this runner boots.)
//!
//! Without the fix, reads keep failing after the write fails, and only an
//! eviction recovers the cell. With it, the failed write restarts the cell
//! on a fresh isolate, reads work again once the restart lands, and writes
//! work again once the limit is lifted. No eviction happens in this test.

mod common;

use common::{announced, forward_announce};
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
        .join("tests/fixtures/disk-full")
        .canonicalize()
        .expect("fixture exists")
}

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

/// A booted `celld dev` and its public address. Kills the child on drop.
struct TestNode {
    child: Child,
    public: String,
    lines: Arc<Mutex<Vec<String>>>,
}

impl TestNode {
    fn boot(project: &Path) -> Self {
        // Bash ignores SIGXFSZ before exec so a clamped file-size limit
        // fails writes with EFBIG instead of killing the node; the ignore
        // disposition survives exec into celld.
        let mut child = Command::new("bash")
            .arg("-c")
            .arg("trap \"\" XFSZ; exec \"$0\" \"$@\"")
            .arg(celld_binary())
            .arg("dev")
            .env("NO_COLOR", "1")
            .arg(project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--clean")
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
            lines: Arc::clone(&announced_lines),
        };
        node.public = poll_until("boot", Duration::from_secs(180), || {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!(
                    "celld dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
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

fn request(addr: &str, path: &str, timeout: Duration) -> Result<(u16, String), String> {
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
    Ok((status, body.trim().to_string()))
}

fn get(addr: &str, path: &str) -> Result<(u16, String), String> {
    request(addr, path, Duration::from_secs(60))
}

fn probe(addr: &str, path: &str) -> bool {
    matches!(request(addr, path, Duration::from_secs(5)), Ok((200, _)))
}

fn poll_until(name: &str, deadline: Duration, mut condition: impl FnMut() -> Option<String>) -> String {
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

/// Every process in the node's tree. Isolates run in forked children, and a
/// file-size limit only binds the processes it is set on.
fn process_tree(root: u32) -> Vec<u32> {
    let mut found = vec![root];
    let mut index = 0;
    while index < found.len() {
        let pid = found[index];
        index += 1;
        let tasks = std::fs::read_dir(format!("/proc/{pid}/task")).expect("list tasks");
        for task in tasks.filter_map(|task| task.ok()) {
            let children = task.path().join("children");
            let list = std::fs::read_to_string(&children).unwrap_or_default();
            for child in list.split_whitespace() {
                if let Ok(child) = child.parse::<u32>() {
                    if !found.contains(&child) {
                        found.push(child);
                    }
                }
            }
        }
    }
    found.sort();
    found
}

/// Clamp (or release) the whole node's file-size limit. A clamped limit
/// turns the next file growth into EFBIG, which SQLite reports as a
/// storage I/O error — the same poison path as the SQLITE_FULL of a full
/// disk (`poisons_actor` classifies FULL/IOERR/NOMEM/INTERRUPT together).
/// Only the soft limit moves: lowering the hard limit is irreversible
/// without privilege, and the soft limit alone is enough to fail growth.
fn clamp_files(node: &TestNode, soft: &str) {
    // A restart spawns a fresh isolate that inherits the clamp, so one
    // pass covers the tree as it stands; callers re-enumerate per step.
    for pid in process_tree(node.child.id()) {
        let output = Command::new("prlimit")
            .args(["--pid", &pid.to_string(), &format!("--fsize={soft}:unlimited")])
            .output()
            .expect("run prlimit");
        assert!(
            output.status.success(),
            "prlimit {soft} on {pid} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn full_disk_keeps_reads_serving() {
    let dir = fixture_tempdir("celld-disk-full-");
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir(), &app_project);
    let node = TestNode::boot(&app_project);
    let (status, _body) = get(&node.public, "/write?n=20").expect("first write");
    assert_eq!(status, 200);
    let (status, body) = get(&node.public, "/read").expect("first read");
    assert_eq!((status, body.as_str()), (200, "rows=20"));

    // The database file now holds ~5MB; a 1MB file-size limit lets nothing
    // grow, so the next multi-row write fails with a storage I/O error.
    clamp_files(&node, "1048576");

    // The transactional write fails and poisons the actor.
    let (status, _) = get(&node.public, "/write?n=40").expect("clamped write responds");
    assert_eq!(status, 500, "a full disk fails the write");
    // Reads must recover without an eviction: the poisoned cell restarts on
    // a fresh isolate and its database reopens clean.
    poll_until("reads recover after the disk filled", Duration::from_secs(90), || {
        match get(&node.public, "/read") {
            Ok((200, body)) if body == "rows=20" => Some(body),
            _ => None,
        }
    });
    // Space comes back: writes work again on the same generation.
    clamp_files(&node, "unlimited");
    let (status, _body) = get(&node.public, "/write?n=1").expect("write after space");
    assert_eq!(status, 200);
    let (status, body) = get(&node.public, "/read").expect("read after space");
    assert_eq!((status, body.as_str()), (200, "rows=21"));
    let logs = node.lines.lock().expect("announce lock").join("\n");
    assert!(
        !logs.contains("evict"),
        "no eviction was needed for the recovery"
    );
}
