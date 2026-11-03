// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): D1 concurrent-call conformance; cross-request attribution conformance. Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for concurrent D1 calls from a Worker, driven through the
//! `tests/fixtures/d1-concurrency` fixture the way `docs/testing.md` runs
//! examples: boot the real binary, replay HTTP.
//!
//! The `attribution_*` tests share the fixture: cross-request checks that a
//! continuation resumed in another entry's checkpoint still sees its own
//! request's resources — its event frames, its `/tmp`, its body streams —
//! and that a first turn's settling work carries its own token.
//!
//! Each test boots its own `celld dev` on a private copy of the fixture
//! (dev state lives beside the project, so sharing one copy would collide),
//! waits for the public port, then asserts the fixture's transcripts. The
//! client is dependency-free on purpose: plain TCP with `Connection: close`
//! and `serde_json` for bodies, so the runner cannot mask a protocol fault.
//! There are no fixed sleeps: every wait polls an observable condition with
//! a deadline, and the server guard kills the child on every exit path.
//! A stalled D1 call surfaces as a read timeout on its HTTP request, never
//! as a sleep in the harness.
//!
//! The R1 ownership tests additionally bind a loopback listener the worker
//! dials, so the harness observes a socket's whole life — handshake, close —
//! directly. The R2 tee test reuses the F5 streamed-body shape.

use serde_json::Value;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn celld_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_celld"))
}

fn fixture_source() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/d1-concurrency")
        .canonicalize()
        .expect("fixture exists")
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    listener.local_addr().expect("probe addr").port()
}

fn copy_fixture(name: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(&format!("celld-d1-concurrency-{name}-"))
        .tempdir()
        .expect("tempdir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(fixture_source().join(file), dir.path().join(file)).expect("copy fixture");
    }
    dir
}

/// A booted `celld dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its ports.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(fixture: &Path, name: &str) -> Self {
        let port = free_port();
        let mut child = Command::new(celld_binary())
            .arg("dev")
            .arg(fixture)
            .arg("--port")
            .arg(port.to_string())
            .arg("--no-watch")
            .arg("--clean")
            .arg("--logs")
            // One stateless isolate, so concurrent requests share the
            // fixture's module state (its isolate-once shared setup) the
            // way suspended requests pile onto one isolate in production.
            .env("CELLD_MAX_STATELESS_ISOLATES", "1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn celld dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        // Drain both pipes so a chatty child can never block on a full
        // one. The drains only discard: readiness is the public port
        // answering below.
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stderr.read_to_end(&mut sink);
        });
        let public = format!("127.0.0.1:{port}");
        // The kill-on-drop guard exists before the readiness poll, so a
        // boot timeout or an early child exit cannot leak the server.
        let mut node = Self {
            child,
            public: public.clone(),
        };
        poll_until(&format!("{name} boot"), Duration::from_secs(120), || {
            if !probe(&public, "/") {
                return None;
            }
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("{name}: celld dev exited during boot: {status}");
            }
            Some("ready".to_string())
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
    read_response(&mut stream)
}

/// POST `body` with an explicit length, for routes that must see a streamed
/// request body rather than held bytes.
fn post_bytes(
    addr: &str,
    path: &str,
    body: &[u8],
    timeout: Duration,
) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream
        .write_all(wire.as_bytes())
        .map_err(|error| format!("write head: {error}"))?;
    stream
        .write_all(body)
        .map_err(|error| format!("write body: {error}"))?;
    read_response(&mut stream)
}

fn read_response(stream: &mut TcpStream) -> Result<(u16, String), String> {
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

/// GET one path from each of `n` threads at once, returning every decoded
/// body. A stalled D1 call fails its thread's read (no sleep), which fails
/// the join below with the hung path and the timeout error.
fn get_concurrent(node_public: &str, path: &str, n: usize) -> Vec<Value> {
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let addr = node_public.to_string();
        let path = path.to_string();
        handles.push(std::thread::spawn(move || {
            request_timeout(&addr, "GET", &path, Duration::from_secs(60))
        }));
    }
    let mut out = Vec::with_capacity(n);
    for (index, handle) in handles.into_iter().enumerate() {
        let (status, body) = handle
            .join()
            .expect("worker thread")
            .unwrap_or_else(|error| panic!("concurrent GET {path} [{index}]: {error}"));
        assert_eq!(status, 200, "concurrent GET {path} [{index}]: {body}");
        out.push(serde_json::from_str(&body).expect("JSON body"));
    }
    out
}

#[test]
fn d1_sequential_calls_answer() {
    let fixture = copy_fixture("sequential");
    let node = TestNode::boot(fixture.path(), "sequential");
    let setup = get_json(&node, "/setup");
    assert_eq!(setup["ok"], true, "{setup}");

    // Several sequential D1 calls in one request: prepare/first, all,
    // batch, exec, and raw. Every call answers.
    let seq = get_json(&node, "/sequential?n=5");
    assert_eq!(seq["firsts"], serde_json::json!([5, 5, 5, 5, 5]), "{seq}");
    assert_eq!(seq["all"], 5, "{seq}");
    assert_eq!(seq["batch"], 2, "{seq}");
    assert_eq!(seq["raw"], serde_json::json!([["v0"]]), "{seq}");
}

#[test]
fn d1_concurrent_calls_within_one_request_answer() {
    let fixture = copy_fixture("parallel");
    let node = TestNode::boot(fixture.path(), "parallel");
    let setup = get_json(&node, "/setup");
    assert_eq!(setup["ok"], true, "{setup}");

    // Concurrent calls within one request (Promise.all): same method.
    let par = get_json(&node, "/parallel?n=8");
    assert_eq!(
        par["par"],
        serde_json::json!([5, 5, 5, 5, 5, 5, 5, 5]),
        "{par}"
    );

    // Concurrent calls of different methods in one request.
    let mixed = get_json(&node, "/mixed-parallel");
    assert_eq!(mixed["a"], 5, "{mixed}");
    assert_eq!(mixed["b"], 5, "{mixed}");
    assert_eq!(mixed["c"], 2, "{mixed}");
    assert_eq!(mixed["d"]["v"], "v0", "{mixed}");

    // The cached-prepare shape parameterized-query drivers use: one base
    // prepare, then concurrent bind/all from it.
    let cached = get_json(&node, "/cached-prepare?n=8");
    assert_eq!(
        cached["cached"],
        serde_json::json!([1, 1, 1, 1, 1, 1, 1, 1]),
        "{cached}"
    );
}

#[test]
fn d1_concurrent_requests_answer() {
    let fixture = copy_fixture("requests");
    let node = TestNode::boot(fixture.path(), "requests");
    let setup = get_json(&node, "/setup");
    assert_eq!(setup["ok"], true, "{setup}");

    // N concurrent requests, each doing several D1 calls. Every request
    // answers with every call's result.
    let bodies = get_concurrent(&node.public, "/multi", 8);
    assert_eq!(bodies.len(), 8);
    for body in &bodies {
        assert_eq!(body["s1"], true, "{body}");
        assert_eq!(body["s2"], 5, "{body}");
        assert_eq!(body["all"], 5, "{body}");
        assert_eq!(body["batch"], 2, "{body}");
    }

    // N concurrent requests each doing concurrent calls within the request.
    let bodies = get_concurrent(&node.public, "/parallel?n=5", 8);
    assert_eq!(bodies.len(), 8);
    for body in &bodies {
        assert_eq!(body["par"], serde_json::json!([5, 5, 5, 5, 5]), "{body}");
    }
}

/// Start one request in a background thread: the caller polls an observable
/// condition, releases the request, then joins for its decoded JSON body.
fn spawn_get(addr: &str, path: &str) -> std::thread::JoinHandle<Result<(u16, String), String>> {
    let addr = addr.to_string();
    let path = path.to_string();
    std::thread::spawn(move || request_timeout(&addr, "GET", &path, Duration::from_secs(60)))
}

fn join_json(handle: std::thread::JoinHandle<Result<(u16, String), String>>, what: &str) -> Value {
    let (status, body) = handle
        .join()
        .expect("worker thread")
        .unwrap_or_else(|error| panic!("{what}: {error}"));
    assert_eq!(status, 200, "{what}: {body}");
    serde_json::from_str(&body).expect("JSON body")
}

/// Poll the gated setup `name` until the starter began it. The starter is
/// then suspended on the gate, so a waiter fired next provably waits.
fn await_attr_started(node: &TestNode, name: &str) {
    poll_until(
        &format!("{name} setup start"),
        Duration::from_secs(30),
        || {
            let status = get_json(node, &format!("/attr-status?name={name}"));
            (status["started"] == true).then(|| "started".to_string())
        },
    );
}

/// Poll until `waiters` waiters attached to the setup `name`, then open its
/// gate. Attachment precedes the release causally — no timing — so every
/// waiter provably resumes in the starter's checkpoint.
fn release_attr_setup(node: &TestNode, name: &str, waiters: u64) {
    poll_until(
        &format!("{name} waiter attach"),
        Duration::from_secs(30),
        || {
            let status = get_json(node, &format!("/attr-status?name={name}"));
            (status["waiters"] == waiters).then(|| "attached".to_string())
        },
    );
    let release = get_json(node, &format!("/attr-release?name={name}"));
    assert_eq!(release["ok"], true, "{release}");
}

/// Bind a loopback listener for one R1 test. The worker dials the returned
/// port, so the harness observes the socket's whole life directly.
fn bind_loopback() -> (TcpListener, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind harness listener");
    let port = listener.local_addr().expect("harness addr").port();
    (listener, port)
}

/// Accept one connection, polling with the suite's 200 ms interval. Every
/// caller accepts only after its peer provably dialed, so the deadline is
/// failure-path only.
fn accept_one(listener: &TcpListener, name: &str, deadline: Duration) -> TcpStream {
    let start = Instant::now();
    loop {
        match listener.accept() {
            Ok((stream, _)) => return stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if start.elapsed() > deadline {
                    panic!("{name}: no connection within the deadline");
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            Err(error) => panic!("{name}: accept: {error}"),
        }
    }
}

/// RFC 6455's proof the server read the key — the same computation the
/// worker's connector verifies, mirrored here for the harness server.
fn ws_accept_key(key: &str) -> String {
    use base64::Engine as _;
    use sha1::Digest as _;
    let mut digest = sha1::Sha1::new();
    digest.update(key.as_bytes());
    digest.update(b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
    base64::engine::general_purpose::STANDARD.encode(digest.finalize())
}

/// Answer one RFC 6455 handshake: read the upgrade request, prove the key,
/// send the 101. Minimal on purpose — this server exists to hold the socket
/// the worker opened, not to serve frames. Leaves the stream nonblocking
/// for the close poll that follows.
fn serve_ws_handshake(stream: &mut TcpStream) {
    stream.set_nonblocking(false).expect("blocking handshake");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("handshake timeout");
    let mut raw = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).expect("handshake read");
        raw.push(byte[0]);
        if raw.ends_with(b"\r\n\r\n") {
            break;
        }
        assert!(raw.len() < 65536, "oversize websocket handshake");
    }
    let head = String::from_utf8(raw).expect("handshake utf8");
    let key = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("sec-websocket-key")
                .then(|| value.trim().to_string())
        })
        .expect("handshake key");
    let wire = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\n\r\n",
        ws_accept_key(&key)
    );
    stream.write_all(wire.as_bytes()).expect("handshake write");
    stream
        .set_nonblocking(true)
        .expect("nonblocking after handshake");
}

#[test]
fn attribution_first_turn_streaming_body_completes() {
    let fixture = copy_fixture("attr-f1");
    let node = TestNode::boot(fixture.path(), "attr-f1");

    // A response that settles in its first turn starts its body pump while
    // settling; the producer suspends on the module gate. Release only once
    // the producer attached, so its continuation provably resumes in the
    // release request's turn — and the pump's write with it. Without the
    // first turn's token the write adopts into the release request, which
    // retires first and drops it, and the body never completes.
    let addr = node.public.clone();
    let stream = std::thread::spawn(move || {
        request_timeout(&addr, "GET", "/f1-stream", Duration::from_secs(20))
    });
    poll_until("f1 producer attach", Duration::from_secs(30), || {
        let status = get_json(&node, "/f1-status");
        (status["pulled"] == true).then(|| "attached".to_string())
    });
    let release = get_json(&node, "/f1-release");
    assert_eq!(release["ok"], true, "{release}");
    let (status, body) = stream
        .join()
        .expect("stream thread")
        .expect("streaming body");
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        decode_chunked(&body),
        "f1-chunk",
        "streaming body completes"
    );
}

/// Decode a chunked transfer body. The streaming test answers with one data
/// chunk and the terminal chunk; anything else fails the parse below.
fn decode_chunked(body: &str) -> String {
    let mut out = String::new();
    let mut rest = body;
    loop {
        let (size_line, tail) = rest.split_once("\r\n").expect("chunk size line");
        let size = usize::from_str_radix(size_line.trim(), 16).expect("chunk size hex");
        if size == 0 {
            break;
        }
        out.push_str(&tail[..size]);
        rest = &tail[size + 2..];
    }
    out
}

#[test]
fn attribution_event_frames_follow_the_running_entry() {
    let fixture = copy_fixture("attr-f2");
    let node = TestNode::boot(fixture.path(), "attr-f2");

    // The starter begins the shared setup; the waiter suspends inside a
    // scheduled() event frame on the same pass. The frame end then runs in
    // the starter's checkpoint: it must pop the waiter's frame, never the
    // starter's. Popping the starter's leaks the waiter's frame, so the
    // request end drains the leak instead of the host frame and the early
    // waitUntil below never runs.
    let start = spawn_get(&node.public, "/f2-start");
    await_attr_started(&node, "f2");
    let wait = spawn_get(&node.public, "/f2-wait");
    release_attr_setup(&node, "f2", 1);
    let started = join_json(start, "f2 start");
    assert_eq!(started["ok"], true, "{started}");
    let outcome = join_json(wait, "f2 wait");
    assert_eq!(outcome["outcome"], "ok", "{outcome}");
    // Both requests answered. Open the second gate: the waiter's early
    // waitUntil now enqueues its write, handed to the waiter. The write
    // lands only when the waiter is still alive for it — when its host
    // frame drained into the background instead of leaking.
    let opened = get_json(&node, "/f2-open2");
    assert_eq!(opened["ok"], true, "{opened}");
    poll_until("f2 waitUntil write", Duration::from_secs(30), || {
        let flag = get_json(&node, "/f2-flag");
        (flag["present"] == true).then(|| "written".to_string())
    });
}

#[test]
fn attribution_request_tmp_follows_the_running_entry() {
    let fixture = copy_fixture("attr-f3");
    let node = TestNode::boot(fixture.path(), "attr-f3");

    // The waiter creates its marker in `/tmp`, then suspends on the shared
    // setup; its continuation resumes in the starter's checkpoint and must
    // still see its own request's tree, never the starter's.
    let start = spawn_get(&node.public, "/f3-start");
    await_attr_started(&node, "f3");
    let wait = spawn_get(&node.public, "/f3-wait");
    release_attr_setup(&node, "f3", 1);
    let started = join_json(start, "f3 start");
    assert_eq!(started["ok"], true, "{started}");
    let waiter = join_json(wait, "f3 wait");
    assert_eq!(waiter["seen"], true, "{waiter}");
}

#[test]
fn attribution_body_stream_forwarding_follows_the_running_entry() {
    let fixture = copy_fixture("attr-f5");
    let node = TestNode::boot(fixture.path(), "attr-f5");

    // 1 MiB clears the ingress stream threshold, so the waiter's body stays
    // a host stream instead of held bytes. The waiter suspends on the shared
    // setup, then forwards its body; the transfer must resolve the waiter's
    // streams even though its continuation resumes in the starter's
    // checkpoint, not refuse a stream the waiter owns.
    let start = spawn_get(&node.public, "/f5-start");
    await_attr_started(&node, "f5");
    let payload = vec![b'x'; 1 << 20];
    let addr = node.public.clone();
    let forward = std::thread::spawn(move || {
        post_bytes(&addr, "/f5-forward", &payload, Duration::from_secs(60))
    });
    release_attr_setup(&node, "f5", 1);
    let started = join_json(start, "f5 start");
    assert_eq!(started["ok"], true, "{started}");
    let forwarded = join_json(forward, "f5 forward");
    assert_eq!(forwarded["ok"], true, "{forwarded}");
    assert_eq!(forwarded["size"], 1 << 20, "{forwarded}");
}

#[test]
fn d1_concurrent_cold_requests_share_setup_and_answer() {
    let fixture = copy_fixture("cold");
    let node = TestNode::boot(fixture.path(), "cold");

    // Reset the isolate-once shared setup, then fire N concurrent
    // requests that all await the same cold setup pass before doing
    // their own several D1 calls. The starter answers at once; every
    // waiter must answer too, not stall on a lost D1 response.
    let reset = get_json(&node, "/reset");
    assert_eq!(reset["ok"], true, "{reset}");
    let bodies = get_concurrent(&node.public, "/multi", 8);
    assert_eq!(bodies.len(), 8);
    for body in &bodies {
        assert_eq!(body["s1"], true, "{body}");
        assert_eq!(body["s2"], 5, "{body}");
        assert_eq!(body["all"], 5, "{body}");
        assert_eq!(body["batch"], 2, "{body}");
    }
}

#[test]
fn attribution_request_tcp_socket_owned_by_running_entry() {
    let fixture = copy_fixture("attr-r1tcp");
    let node = TestNode::boot(fixture.path(), "attr-r1tcp");
    let (listener, port) = bind_loopback();
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");

    // The starter shares the setup, then holds the second gate: it provably
    // stays alive while the waiter connects in its checkpoint and retires.
    // The waiter's retirement must close its socket — EOF below — which only
    // happens when the socket is owned by the waiter rather than the starter.
    let start = spawn_get(&node.public, "/r1-start?name=r1tcp");
    await_attr_started(&node, "r1tcp");
    let wait = spawn_get(
        &node.public,
        &format!("/r1-tcp-wait?name=r1tcp&port={port}"),
    );
    release_attr_setup(&node, "r1tcp", 1);
    let waiter = join_json(wait, "r1tcp wait");
    assert_eq!(waiter["ok"], true, "{waiter}");
    let mut stream = accept_one(&listener, "r1tcp accept", Duration::from_secs(30));
    stream.set_nonblocking(true).expect("nonblocking stream");
    poll_until("r1tcp retirement close", Duration::from_secs(30), || {
        let mut byte = [0u8; 1];
        match stream.read(&mut byte) {
            // EOF: the retired waiter closed its socket. The worker sends
            // nothing, so only EOF ends the poll; anything else keeps
            // waiting for the close.
            Ok(0) => Some("closed".to_string()),
            Ok(_) => None,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(error) => panic!("r1tcp read: {error}"),
        }
    });
    // Only now may the starter go: the close above is the waiter's iff the
    // starter was still alive for it.
    let release = get_json(&node, "/r1-release2?name=r1tcp");
    assert_eq!(release["ok"], true, "{release}");
    let started = join_json(start, "r1tcp start");
    assert_eq!(started["ok"], true, "{started}");
}

#[test]
fn attribution_request_ws_socket_owned_by_running_entry() {
    let fixture = copy_fixture("attr-r1ws");
    let node = TestNode::boot(fixture.path(), "attr-r1ws");
    let (listener, port) = bind_loopback();
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");

    // Same shape as the TCP test, over an outbound WebSocket: the waiter
    // dials in the starter's checkpoint, and its retirement must close the
    // socket while the starter still lives.
    let start = spawn_get(&node.public, "/r1-start?name=r1ws");
    await_attr_started(&node, "r1ws");
    let wait = spawn_get(&node.public, &format!("/r1-ws-wait?name=r1ws&port={port}"));
    release_attr_setup(&node, "r1ws", 1);
    // The handshake must complete before the waiter can answer.
    let mut stream = accept_one(&listener, "r1ws accept", Duration::from_secs(30));
    serve_ws_handshake(&mut stream);
    let waiter = join_json(wait, "r1ws wait");
    assert_eq!(waiter["ok"], true, "{waiter}");
    poll_until("r1ws retirement close", Duration::from_secs(30), || {
        let mut byte = [0u8; 1];
        match stream.read(&mut byte) {
            // A close frame (FIN + opcode 8) or EOF: the retired waiter
            // closed its socket. Before the fix the starter owns it and
            // holds it, so this poll fails at the deadline.
            Ok(0) => Some("closed".to_string()),
            Ok(_) if byte[0] == 0x88 => Some("close frame".to_string()),
            Ok(_) => None,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(error) => panic!("r1ws read: {error}"),
        }
    });
    let release = get_json(&node, "/r1-release2?name=r1ws");
    assert_eq!(release["ok"], true, "{release}");
    let started = join_json(start, "r1ws start");
    assert_eq!(started["ok"], true, "{started}");
}

#[test]
fn attribution_request_rewriter_owned_by_running_entry() {
    let fixture = copy_fixture("attr-r1hr");
    let node = TestNode::boot(fixture.path(), "attr-r1hr");
    let (listener, port) = bind_loopback();
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");

    // The starter connects first, so EOF on its connection proves it retired.
    // Retirement frees every request-owned registry entry in one pass, so
    // that EOF also proves a mis-tracked rewriter is already freed.
    let start = spawn_get(&node.public, &format!("/r1hr-start?port={port}"));
    await_attr_started(&node, "r1hr");
    let mut stream = accept_one(&listener, "r1hr accept", Duration::from_secs(30));
    // The waiter transforms in the starter's checkpoint, then reads only
    // after the second gate — which opens after the starter's EOF below.
    // Its own 20 s read timeout bounds a freed rewriter's hung stream.
    let addr = node.public.clone();
    let wait = std::thread::spawn(move || {
        request_timeout(&addr, "GET", "/r1hr-wait", Duration::from_secs(20))
    });
    release_attr_setup(&node, "r1hr", 1);
    let started = join_json(start, "r1hr start");
    assert_eq!(started["ok"], true, "{started}");
    stream.set_nonblocking(true).expect("nonblocking stream");
    poll_until("r1hr starter retirement", Duration::from_secs(30), || {
        let mut byte = [0u8; 1];
        match stream.read(&mut byte) {
            Ok(0) => Some("retired".to_string()),
            Ok(_) => None,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => None,
            Err(error) => panic!("r1hr read: {error}"),
        }
    });
    let release = get_json(&node, "/r1-release2?name=r1hr");
    assert_eq!(release["ok"], true, "{release}");
    let waiter = join_json(wait, "r1hr wait");
    assert_eq!(waiter["ok"], true, "{waiter}");
    let text = waiter["text"].as_str().unwrap_or_default();
    assert!(text.contains("bye") && !text.contains("hello"), "{waiter}");
}

#[test]
fn attribution_request_tee_owned_by_running_entry() {
    let fixture = copy_fixture("attr-r2tee");
    let node = TestNode::boot(fixture.path(), "attr-r2tee");

    // 1 MiB clears the ingress stream threshold (as in F5), so the waiter's
    // body stays a host stream. The waiter tees it in the starter's
    // checkpoint: the tee must replace the stream in the waiter's set, so
    // the forwarded branch transfers as owned instead of failing "not owned".
    let start = spawn_get(&node.public, "/r2-tee-start");
    await_attr_started(&node, "r2tee");
    let payload = vec![b'x'; 1 << 20];
    let addr = node.public.clone();
    let forward = std::thread::spawn(move || {
        post_bytes(&addr, "/r2-tee-forward", &payload, Duration::from_secs(60))
    });
    release_attr_setup(&node, "r2tee", 1);
    let started = join_json(start, "r2tee start");
    assert_eq!(started["ok"], true, "{started}");
    let out = join_json(forward, "r2tee forward");
    assert_eq!(out["ok"], true, "{out}");
    assert_eq!(out["forwarded"], 1 << 20, "{out}");
    assert_eq!(out["local"], 1 << 20, "{out}");
}
