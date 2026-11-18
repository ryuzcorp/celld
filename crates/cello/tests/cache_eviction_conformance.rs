// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): per-node Workers Cache
// API conformance; deploy --read-root option; ephemeral-port boot (0077). Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Eviction conformance for the per-node Workers Cache API: under a small
//! `CELLO_CACHE_MAX_BYTES` the store evicts least-recently-used entries.
//!
//! A separate binary from `cache_conformance` because the harness installs
//! its async runtime handle exactly once per process. See that file for the
//! fixture and boot pattern.

use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn cello_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cello"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/cache")
        .canonicalize()
        .expect("fixture exists")
}

/// One address of a `cello dev` node, from the announce line the supervisor
/// forwards to its stderr. The node is non-dumpable (patch 0053), so
/// `/proc/<pid>/fd` no longer names its listeners, and this is the node's
/// own report of where it bound.
fn announced(lines: &Mutex<Vec<String>>, prefix: &str) -> Option<String> {
    lines
        .lock()
        .expect("announce lock")
        .iter()
        .find_map(|line| {
            let address = line.strip_prefix(prefix)?.split_once(' ')?.0;
            Some(address.to_string())
        })
}

/// Collect the supervisor's stderr lines, which carry the node's announce
/// lines (`--logs` forwards them).
fn forward_announce(stderr: std::process::ChildStderr, lines: Arc<Mutex<Vec<String>>>) {
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        while reader.read_line(&mut line).is_ok_and(|read| read > 0) {
            lines
                .lock()
                .expect("announce lock")
                .push(std::mem::take(&mut line));
        }
    });
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// Percent-encode a query value: cache directives and HTTP dates carry
/// commas, spaces, quotes, and slashes no raw query string can hold.
fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Deploy the probe into the app project's dev bucket before `dev`
/// boots, so the node loads both scripts. `dev` deploys the app itself and
/// becomes primary (last write wins `deploy/current.json`); the
/// probe's named pointer stays for the service binding to resolve.
fn preseed_probe(app_project: &Path, probe_src: &Path) {
    let state = app_project.join(".cello/dev");
    std::fs::create_dir_all(state.join("runtime")).expect("state dir");
    let database = state.join("objects.sqlite3");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    cello::asyncrt::set_host_handle(rt.handle().clone());
    rt.block_on(async {
        let bucket = cello::dev::open_local_bucket(&database).expect("open dev bucket");
        cello::fleet::validate_bucket(&bucket)
            .await
            .expect("validate bucket");
        bucket.probe_cas().await.expect("probe cas");
        let built = cello::deploy::build(&cello::deploy::Options {
            config: Some(probe_src.to_path_buf()),
            bucket: None,
            endpoint: None,
            region: None,
            dry_run: false,
            json: false,
            vars: cello::deploy::VarOverrides::None,
            local_images: true,
            sourcemap: None,
            no_activate: false,
            read_roots: Vec::new(),
        })
        .expect("build probe");
        cello::deploy::write(&bucket, &built)
            .await
            .expect("write probe");
        cello::wake_format::ensure_ready(&bucket)
            .await
            .expect("ensure ready");
    });
}

/// A booted `cello dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path, envs: &[(&str, &str)]) -> Self {
        let mut command = Command::new(cello_binary());
        command
            .arg("dev")
            .arg(app_project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--logs");
        for (name, value) in envs {
            command.env(name, value);
        }
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cello dev");
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
                    "cello dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            let public = announced(&announced_lines, "cello listening on ")?;
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

fn request_timeout(
    addr: &str,
    method: &str,
    path: &str,
    timeout: Duration,
) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
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

fn get(addr: &str, path: &str) -> Result<(u16, String), String> {
    request_timeout(addr, "GET", path, Duration::from_secs(60))
}

/// Readiness probe with a short timeout: a bound-but-not-ready node holds
/// connections open, so a long read would eat the boot deadline.
fn probe(addr: &str, path: &str) -> bool {
    matches!(
        request_timeout(addr, "GET", path, Duration::from_secs(2)),
        Ok((200, _))
    )
}

fn get_json(node: &TestNode, path: &str) -> Value {
    let (status, body) = get(&node.public, path).expect("public GET");
    assert_eq!(status, 200, "GET {path}: {body}");
    serde_json::from_str(&body).expect("JSON body")
}

fn poll_until(
    name: &str,
    deadline: Duration,
    mut condition: impl FnMut() -> Option<String>,
) -> String {
    let start = std::time::Instant::now();
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

fn query(endpoint: &str, params: &[(&str, &str)]) -> String {
    let query = params
        .iter()
        .map(|(name, value)| format!("{name}={}", encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!("/{endpoint}?{query}")
}

#[test]
fn cache_eviction_conformance() {
    // A 6 KiB cache holds two ~2.3 KiB entries but never three, so the
    // third put evicts exactly the least-recently-used entry.
    let dir = tempfile::Builder::new()
        .prefix("cello-cache-")
        .tempdir()
        .expect("tempdir");
    let app_project = dir.path().join("app");
    let probe_src = dir.path().join("probe");
    copy_dir(&fixture_dir().join("app"), &app_project);
    copy_dir(&fixture_dir().join("probe"), &probe_src);
    preseed_probe(&app_project, &probe_src);
    let node = TestNode::boot(&app_project, &[("CELLO_CACHE_MAX_BYTES", "6000")]);

    let evicted = get_json(
        &node,
        &query("evict", &[("keys", "e1,e2,e3"), ("size", "1400")]),
    );
    assert_eq!(evicted["ok"], true, "{evicted}");
    assert_eq!(evicted["result"]["hits"]["e1"]["hit"], false, "{evicted}");
    assert_eq!(evicted["result"]["hits"]["e2"]["hit"], true, "{evicted}");
    assert_eq!(evicted["result"]["hits"]["e2"]["ccs"], "HIT", "{evicted}");
    assert_eq!(evicted["result"]["hits"]["e2"]["length"], 1400, "{evicted}");
    assert_eq!(evicted["result"]["hits"]["e3"]["hit"], true, "{evicted}");

    // A match refreshes recency: the untouched middle key goes, not the
    // re-read first one.
    let lru = get_json(
        &node,
        &query("evict-lru", &[("keys", "r1,r2,r3"), ("size", "1400")]),
    );
    assert_eq!(lru["ok"], true, "{lru}");
    assert_eq!(lru["result"]["hits"]["r1"]["hit"], true, "{lru}");
    assert_eq!(lru["result"]["hits"]["r2"]["hit"], false, "{lru}");
    assert_eq!(lru["result"]["hits"]["r3"]["hit"], true, "{lru}");
}
