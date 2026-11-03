// Modified by ryuzcorp (ryuzcorp/celld): operator token for the internal listener. Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for `CELLD_OPERATOR_TOKEN`: when the variable is set, every
//! route on the internal listener requires `Authorization: Bearer <token>`
//! and answers 401 without it; when it is set but empty, the node refuses
//! to start.
//!
//! Each test boots its own `celld dev` on a private copy of the fixture,
//! the way `rpc_stub_conformance.rs` does. The client is plain TCP with
//! `Connection: close`, so the runner cannot mask a protocol fault. There
//! are no fixed sleeps: every wait polls an observable condition with a
//! deadline, and the server guard kills the child on every exit path.
//!
//! There is deliberately no assertion that an authenticated `/shutdown`
//! stops the node: the 401 without the token proves the gate, and draining
//! the test node would only prove the shutdown path other suites own.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TOKEN: &str = "fleet-test-token";
const AUTH: &str = "Bearer fleet-test-token";
const WRONG_AUTH: &str = "Bearer wrong-token";

fn celld_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_celld"))
}

fn fixture_source() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/operator-token")
        .canonicalize()
        .expect("fixture exists")
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    listener.local_addr().expect("probe addr").port()
}

fn copy_fixture(name: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(&format!("celld-operator-token-{name}-"))
        .tempdir()
        .expect("tempdir");
    for file in ["wrangler.json", "index.js"] {
        std::fs::copy(fixture_source().join(file), dir.path().join(file)).expect("copy fixture");
    }
    dir
}

/// A booted `celld dev` and its addresses. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its ports.
struct TestNode {
    child: Child,
    public: String,
    internal: String,
    stderr: Arc<Mutex<Vec<u8>>>,
}

/// Direct children of a process, via `/proc/<pid>/task/*/children`.
fn child_pids(pid: u32) -> Vec<u32> {
    let mut children = Vec::new();
    let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
        return children;
    };
    for task in tasks.map_while(Result::ok) {
        let Ok(list) = std::fs::read_to_string(task.path().join("children")) else {
            continue;
        };
        children.extend(
            list.split_whitespace()
                .filter_map(|id| id.parse::<u32>().ok()),
        );
    }
    children.sort_unstable();
    children.dedup();
    children
}

/// Loopback TCP ports one process listens on, matching its socket inodes
/// against `/proc/net/tcp` LISTEN entries.
fn listening_loopback_ports(pid: u32) -> Vec<u16> {
    let mut inodes = std::collections::HashSet::new();
    if let Ok(fds) = std::fs::read_dir(format!("/proc/{pid}/fd")) {
        for fd in fds.map_while(Result::ok) {
            let Ok(link) = std::fs::read_link(fd.path()) else {
                continue;
            };
            if let Some(name) = link.to_str() {
                if let Some(inode) = name
                    .strip_prefix("socket:[")
                    .and_then(|inode| inode.strip_suffix(']'))
                {
                    inodes.insert(inode.to_string());
                }
            }
        }
    }
    let Ok(tcp) = std::fs::read_to_string("/proc/net/tcp") else {
        return Vec::new();
    };
    let mut ports = Vec::new();
    for line in tcp.lines().skip(1) {
        let mut fields = line.split_whitespace();
        let (Some(local), Some(state)) = (fields.nth(1), fields.nth(1)) else {
            continue;
        };
        if state != "0A" {
            continue;
        }
        // Fields past `st`: tx_queue, tr, retransmit, uid, timeout, then
        // the inode.
        let Some(inode) = fields.nth(5) else { continue };
        if !inodes.contains(inode) {
            continue;
        }
        let mut addr = local.split(':');
        if addr.next() != Some("0100007F") {
            continue;
        }
        if let Some(port) = addr
            .next()
            .and_then(|port| u16::from_str_radix(port, 16).ok())
        {
            ports.push(port);
        }
    }
    ports.sort_unstable();
    ports.dedup();
    ports
}

impl TestNode {
    fn boot_with_env(fixture: &Path, name: &str, env: &[(&str, &str)]) -> Self {
        let port = free_port();
        let mut child = Command::new(celld_binary())
            .arg("dev")
            .arg(fixture)
            .arg("--port")
            .arg(port.to_string())
            .arg("--no-watch")
            .arg("--clean")
            .arg("--logs")
            .envs(env.iter().copied())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn celld dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        // Drain stdout so a chatty child can never block on a full pipe.
        // Stderr is kept: the token must never appear in it.
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
        let captured_sink = Arc::clone(&captured);
        std::thread::spawn(move || {
            // Streamed, not read-to-end: the node is still running when the
            // test reads this, so bytes must arrive while the pipe is open.
            let mut chunk = [0u8; 4096];
            loop {
                match stderr.read(&mut chunk) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => captured_sink
                        .lock()
                        .expect("stderr lock")
                        .extend_from_slice(&chunk[..n]),
                }
            }
        });
        let public = format!("127.0.0.1:{port}");
        // The kill-on-drop guard exists before the readiness poll, so a
        // boot timeout or an early child exit cannot leak the server.
        let mut node = Self {
            child,
            public: public.clone(),
            internal: String::new(),
            stderr: captured,
        };
        // Readiness is the public health endpoint answering 200; the
        // internal port is the node child's other loopback listener.
        node.internal = poll_until(&format!("{name} boot"), Duration::from_secs(120), || {
            if !probe(&public, "/.well-known/celld/health") {
                return None;
            }
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("{name}: celld dev exited during boot: {status}");
            }
            let mut internal = None;
            for pid in child_pids(node.child.id()) {
                for found in listening_loopback_ports(pid) {
                    if found != port {
                        internal = Some(found);
                    }
                }
            }
            internal.map(|port| format!("127.0.0.1:{port}"))
        });
        node
    }

    /// The supervisor's stderr so far. The node child inherits the
    /// supervisor's stderr, so this also carries the node's startup logs.
    fn stderr_text(&self) -> String {
        String::from_utf8_lossy(&self.stderr.lock().expect("stderr lock")).into_owned()
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn request_with_headers(
    addr: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    timeout: Duration,
) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    let mut wire = format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    for (name, value) in headers {
        wire.push_str(&format!("{name}: {value}\r\n"));
    }
    wire.push_str("Content-Length: 0\r\n\r\n");
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

fn get(addr: &str, path: &str, auth: Option<&str>) -> Result<(u16, String), String> {
    let headers: Vec<(&str, &str)> = auth
        .map(|value| vec![("Authorization", value)])
        .unwrap_or_default();
    request_with_headers(addr, "GET", path, &headers, Duration::from_secs(60))
}

fn post(addr: &str, path: &str, auth: Option<&str>) -> Result<(u16, String), String> {
    let headers: Vec<(&str, &str)> = auth
        .map(|value| vec![("Authorization", value)])
        .unwrap_or_default();
    request_with_headers(addr, "POST", path, &headers, Duration::from_secs(60))
}

/// Readiness probe with a short timeout: a bound-but-not-ready node holds
/// connections open, so a long read would eat the boot deadline.
fn probe(addr: &str, path: &str) -> bool {
    matches!(
        request_with_headers(addr, "GET", path, &[], Duration::from_secs(2)),
        Ok((200, _))
    )
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
fn operator_token_gates_every_internal_route_class() {
    let fixture = copy_fixture("gated");
    let node = TestNode::boot_with_env(fixture.path(), "gated", &[("CELLD_OPERATOR_TOKEN", TOKEN)]);

    // Operator state: unauthenticated and wrong-token callers get 401, the
    // right token gets the node state.
    let (status, _) = get(&node.internal, "/state", None).expect("internal /state");
    assert_eq!(status, 401, "GET /state without a token must be 401");
    let (status, _) = get(&node.internal, "/state", Some(WRONG_AUTH)).expect("internal /state");
    assert_eq!(status, 401, "GET /state with a wrong token must be 401");
    let (status, body) = get(&node.internal, "/state", Some(AUTH)).expect("internal /state");
    assert_eq!(status, 200, "GET /state with the token: {body}");
    serde_json::from_str::<serde_json::Value>(&body).expect("state JSON");

    // Reload: the gate answers before the pointer watcher is nudged.
    let (status, _) = post(&node.internal, "/reload", None).expect("internal /reload");
    assert_eq!(status, 401, "POST /reload without a token must be 401");
    let (status, body) = post(&node.internal, "/reload", Some(AUTH)).expect("internal /reload");
    assert_eq!(status, 200, "POST /reload with the token: {body}");

    // Rebalance switch, paused and resumed so the node is left as found.
    let (status, _) = post(&node.internal, "/rebalance/pause", None).expect("pause");
    assert_eq!(
        status, 401,
        "POST /rebalance/pause without a token must be 401"
    );
    let (status, _) = post(&node.internal, "/rebalance/resume", None).expect("resume");
    assert_eq!(
        status, 401,
        "POST /rebalance/resume without a token must be 401"
    );
    let (status, body) = post(&node.internal, "/rebalance/pause", Some(AUTH)).expect("pause");
    assert_eq!(status, 200, "POST /rebalance/pause with the token: {body}");
    assert!(
        body.contains("\"rebalance_paused\":true"),
        "pause body: {body}"
    );
    let (status, body) = post(&node.internal, "/rebalance/resume", Some(AUTH)).expect("resume");
    assert_eq!(status, 200, "POST /rebalance/resume with the token: {body}");

    // Shutdown: the 401 proves the gate; the test never sends an
    // authenticated shutdown, which would drain the node under test.
    let (status, _) = post(&node.internal, "/shutdown?handoff=preserve", None).expect("shutdown");
    assert_eq!(status, 401, "POST /shutdown without a token must be 401");

    // Cell routing, eviction, and direct DO dispatch: malformed scopes
    // answer 400 once past the gate, which proves the request reached the
    // handler instead of the gate.
    for path in ["/cell/!!!", "/evict/!!!", "/do/!!!"] {
        let (status, _) = get(&node.internal, path, None).expect("gated route");
        assert_eq!(status, 401, "GET {path} without a token must be 401");
        let (status, body) = get(&node.internal, path, Some(AUTH)).expect("gated route");
        assert_eq!(status, 400, "GET {path} with the token: {body}");
    }

    // The fleet-HMAC routes sit behind the same gate: no bearer is a 401
    // from the gate, while a bearer without the HMAC reaches the peer
    // authentication and fails there.
    let (status, _) = get(&node.internal, "/peer/probe", None).expect("probe");
    assert_eq!(status, 401, "GET /peer/probe without a token must be 401");
    let (status, body) = get(&node.internal, "/peer/probe", Some(AUTH)).expect("probe");
    assert_eq!(status, 401, "GET /peer/probe with only the token: {body}");
    assert!(
        body.contains("peer authentication failed"),
        "the bearer passed the gate and the HMAC refused: {body}"
    );
    let (status, _) = get(&node.internal, "/runtime/!!!", None).expect("runtime");
    assert_eq!(status, 401, "GET /runtime/!!! without a token must be 401");
    let (status, body) = get(&node.internal, "/runtime/!!!", Some(AUTH)).expect("runtime");
    assert_eq!(status, 400, "GET /runtime/!!! with the token: {body}");

    // Unknown paths are gated too, so the 404 oracle stays behind the token.
    let (status, _) = get(&node.internal, "/no-such-route", None).expect("unknown");
    assert_eq!(status, 401, "unknown path without a token must be 401");
    let (status, _) = get(&node.internal, "/no-such-route", Some(AUTH)).expect("unknown");
    assert_eq!(status, 404, "unknown path with the token must be 404");

    // Liveness and readiness probes stay open: they use the public
    // listener's health endpoint, which the token does not guard.
    let (status, body) = get(&node.public, "/.well-known/celld/health", None).expect("health");
    assert_eq!(status, 200, "public health without a token: {body}");

    // The token is a credential: it must never appear in the logs.
    assert!(
        !node.stderr_text().contains(TOKEN),
        "the supervisor stderr leaks the token"
    );
}

#[test]
fn empty_operator_token_refuses_to_start() {
    let fixture = copy_fixture("empty");
    let port = free_port();
    let mut child = Command::new(celld_binary())
        .arg("dev")
        .arg(fixture.path())
        .arg("--port")
        .arg(port.to_string())
        .arg("--no-watch")
        .arg("--clean")
        .env("CELLD_OPERATOR_TOKEN", "")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn celld dev");
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");
    // Drain both pipes on threads so a failing child can never block on a
    // full one; stderr is kept to prove the refusal names the variable.
    std::thread::spawn(move || {
        let mut sink = Vec::new();
        let _ = stdout.read_to_end(&mut sink);
    });
    let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_sink = Arc::clone(&captured);
    std::thread::spawn(move || {
        let mut chunk = [0u8; 4096];
        loop {
            match stderr.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => captured_sink
                    .lock()
                    .expect("stderr lock")
                    .extend_from_slice(&chunk[..n]),
            }
        }
    });
    // The child node refuses before its listeners bind, and the supervisor
    // exits when the child does. Poll the exit; a node that served anyway
    // would hold this deadline and fail.
    let start = Instant::now();
    let deadline = Duration::from_secs(90);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll child") {
            break status;
        }
        if start.elapsed() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("celld dev with an empty token still runs after {deadline:?}");
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert!(
        !status.success(),
        "celld dev with an empty token must exit non-zero"
    );
    // The exit status can win the race with the last stderr bytes through
    // the pipe; poll for the refusal line itself.
    let start = Instant::now();
    let output = loop {
        let text = String::from_utf8_lossy(&captured.lock().expect("stderr lock")).into_owned();
        if text.contains("CELLD_OPERATOR_TOKEN") {
            break text;
        }
        if start.elapsed() > Duration::from_secs(10) {
            break text;
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(
        output.contains("CELLD_OPERATOR_TOKEN"),
        "the refusal must name the variable: {output:?}"
    );
}
