// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): directory-index regression test (upstream #318).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! Upstream denoland/celld#318: with static assets, `sub/index.html` was
//! not served at `sub/`. Requests for `/sub` and `/sub/index.html`
//! redirected to `/sub/`, but `/sub/` itself 404d: the request-path decoder
//! rejected every trailing-slash path, so the directory index the redirect
//! points at could never be served.

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
        .join("tests/fixtures/static-dir-index")
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

fn copy_tree(src: &Path, dst: &Path) {
    for entry in walkdir_files(src) {
        let relative = entry.strip_prefix(src).expect("relative fixture path");
        let target = dst.join(relative);
        if entry.is_dir() {
            std::fs::create_dir_all(&target).expect("create fixture dir");
        } else {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent).expect("create fixture dir");
            }
            std::fs::copy(&entry, &target).expect("copy fixture");
        }
    }
}

fn walkdir_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let entries = std::fs::read_dir(&current).expect("read fixture dir");
        for entry in entries {
            let path = entry.expect("fixture entry").path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

/// A booted `celld dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path) -> Self {
        let mut child = Command::new(celld_binary())
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
            probe(&addr, "/").then_some(addr)
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

type RawResponse = (u16, Vec<(String, String)>, String);

fn raw_get(addr: &str, path: &str) -> Result<RawResponse, String> {
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
    let mut lines = head.lines();
    let status: u16 = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| format!("no status in {head:?}"))?;
    let mut headers = Vec::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
        }
    }
    Ok((status, headers, body.to_string()))
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_str())
}

fn probe(addr: &str, path: &str) -> bool {
    matches!(raw_get(addr, path), Ok((200, _, _)))
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

#[test]
fn dir_index_served_at_dir_url() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-static-dir-");
    copy_tree(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // The top-level index still serves at `/`.
    let (status, _, body) = raw_get(&node.public, "/").expect("GET /");
    assert_eq!(status, 200, "GET /: {body}");
    assert!(body.contains("home"), "GET / body: {body}");

    // `/sub` redirects to the canonical `/sub/`, and `/sub/index.html`
    // redirects there too.
    for path in ["/sub", "/sub/index.html"] {
        let (status, headers, body) =
            raw_get(&node.public, path).unwrap_or_else(|error| panic!("GET {path}: {error}"));
        assert_eq!(status, 307, "GET {path}: {body}");
        assert_eq!(header(&headers, "location"), Some("/sub/"), "GET {path}");
    }

    // Pre-fix this 404d: the trailing-slash path never decoded, so the
    // redirect above pointed at a URL that could not be served.
    let (status, _, body) = raw_get(&node.public, "/sub/").expect("GET /sub/");
    assert_eq!(status, 200, "GET /sub/: {body}");
    assert!(body.contains("sub"), "GET /sub/ body: {body}");

    // A genuinely missing directory still 404s.
    let (status, _, _) = raw_get(&node.public, "/missing/").expect("GET /missing/");
    assert_eq!(status, 404, "GET /missing/");
}
