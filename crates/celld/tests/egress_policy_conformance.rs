// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): tenant egress policy conformance.

// A black-box runner, not node code: it boots the binary, serves a local
// echo fixture, and polls TCP with wall-clock deadlines, so the execution
// boundary's injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for the tenant egress policy (`CELLD_EGRESS=public`): one
//! `celld dev` boots with the loopback echo port allowlisted, proxy
//! variables pointing at a blackhole listener, and the fixture worker
//! driving every outbound path — `fetch()` to each denied range (by
//! literal and by hostname), an allowlisted fetch, a wrong-port fetch, a
//! redirect to a refused address, an unreachable public address,
//! `connect()` refused and allowlisted, and WebSockets refused and
//! allowlisted. Refusals must name the policy; the unreachable public
//! address must fail without naming it, proving the policy passed it
//! through to TCP; and the allowlisted fetch must answer through the
//! proxy variables, proving tenant clients ignore them.
//!
//! There are no fixed sleeps: every wait polls an observable condition
//! with a deadline, and the server guard kills the child on every exit
//! path. The unreachable-public case is bounded by the node's own short
//! fetch timeout, not by the test.

use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn celld_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_celld"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/egress")
        .canonicalize()
        .expect("fixture exists")
}

fn copy_dir(src: &Path, dst: &Path, echo_port: u16) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    std::fs::copy(src.join("worker.js"), dst.join("worker.js")).expect("copy worker");
    let manifest = std::fs::read_to_string(src.join("wrangler.json")).expect("read manifest");
    let manifest = manifest.replace("@ECHO_PORT@", &echo_port.to_string());
    assert!(!manifest.contains("@ECHO_PORT@"), "port templated");
    std::fs::write(dst.join("wrangler.json"), manifest).expect("write manifest");
}

/// A booted `celld dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path, echo_port: u16, proxy: &str) -> Self {
        let port = free_port();
        let mut child = Command::new(celld_binary())
            .arg("dev")
            .arg(app_project)
            .arg("--port")
            .arg(port.to_string())
            .arg("--no-watch")
            .arg("--logs")
            .env("CELLD_EGRESS", "public")
            .env("CELLD_EGRESS_ALLOW", format!("127.0.0.1:{echo_port}"))
            .env("CELLD_FETCH_TIMEOUT_S", "3")
            .env("HTTP_PROXY", proxy)
            .env("HTTPS_PROXY", proxy)
            .env("ALL_PROXY", proxy)
            .env_remove("NO_PROXY")
            .env_remove("no_proxy")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn celld dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        // Drain both pipes so a chatty child can never block on a full one.
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
        // Readiness is the public port answering with the app loaded.
        poll_until("boot", Duration::from_secs(120), || {
            if !probe(&public, "/health") {
                return None;
            }
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("celld dev exited during boot: {status}");
            }
            Some(public.clone())
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

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    listener.local_addr().expect("probe addr").port()
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

/// The loopback echo fixture: `/echo` answers 200, `/redir` redirects to
/// a non-allowlisted loopback port, `/ws` echoes WebSocket frames, and
/// anything that is not HTTP is echoed back raw for `connect()`.
struct EchoServer {
    port: u16,
}

impl EchoServer {
    fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("echo bind");
        let port = listener.local_addr().expect("echo addr").port();
        assert!(port < 65534, "room for the redirect target beside the port");
        let other = port + 1;
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                serve_conn(stream, other);
            }
        });
        Self { port }
    }
}

fn serve_conn(stream: TcpStream, other: u16) {
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(10))).ok();
    let mut stream = stream;
    let mut buf = vec![0u8; 8192];
    let Ok(n) = stream.read(&mut buf) else { return };
    buf.truncate(n);
    if buf.starts_with(b"GET ") || buf.starts_with(b"POST ") {
        serve_http(&mut stream, &buf, other);
    } else if !buf.is_empty() {
        let _ = stream.write_all(&buf);
    }
}

fn serve_http(stream: &mut TcpStream, buf: &[u8], other: u16) {
    let text = String::from_utf8_lossy(buf);
    let path = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");
    if path == "/echo" {
        let _ = stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 7\r\nConnection: close\r\n\r\necho-ok");
    } else if path == "/redir" {
        let _ = stream.write_all(
            format!(
                "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{other}/\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        );
    } else if path == "/ws" && text.to_ascii_lowercase().contains("upgrade: websocket") {
        serve_ws(stream, &text);
    } else {
        let _ = stream
            .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    }
}

/// A minimal WebSocket acceptor: one 101, then echo text frames until a
/// close frame or the peer goes away.
fn serve_ws(stream: &mut TcpStream, request: &str) {
    use base64::Engine;
    use sha1::{Digest, Sha1};
    let key = request
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.trim()
                .eq_ignore_ascii_case("sec-websocket-key")
                .then(|| value.trim())
        })
        .unwrap_or("");
    let mut digest = Sha1::new();
    digest.update(format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes());
    let accept = base64::engine::general_purpose::STANDARD.encode(digest.finalize());
    if stream
        .write_all(
            format!(
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
            )
            .as_bytes(),
        )
        .is_err()
    {
        return;
    }
    loop {
        let mut head = [0u8; 2];
        if stream.read_exact(&mut head).is_err() {
            return;
        }
        let opcode = head[0] & 0x0f;
        let mut len = (head[1] & 0x7f) as u64;
        if len == 126 {
            let mut ext = [0u8; 2];
            if stream.read_exact(&mut ext).is_err() {
                return;
            }
            len = u16::from_be_bytes(ext) as u64;
        } else if len == 127 {
            let mut ext = [0u8; 8];
            if stream.read_exact(&mut ext).is_err() {
                return;
            }
            len = u64::from_be_bytes(ext);
        }
        if len > 64 * 1024 {
            return;
        }
        let mut mask = [0u8; 4];
        if head[1] & 0x80 != 0 && stream.read_exact(&mut mask).is_err() {
            return;
        }
        let mut payload = vec![0u8; len as usize];
        if stream.read_exact(&mut payload).is_err() {
            return;
        }
        if head[1] & 0x80 != 0 {
            for (i, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[i % 4];
            }
        }
        if opcode == 8 {
            let _ = stream.write_all(&[0x88, 0x00]);
            return;
        }
        if opcode == 9 {
            let _ = stream.write_all(&[0x8a, 0x00]);
            continue;
        }
        let mut frame = vec![0x81];
        if payload.len() < 126 {
            frame.push(payload.len() as u8);
        } else {
            frame.push(126);
            frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        }
        frame.extend_from_slice(&payload);
        if stream.write_all(&frame).is_err() {
            return;
        }
    }
}

/// A proxy that answers nothing: it accepts and drops every
/// connection, so any fetch routed through it fails instead of
/// returning. Tenant fetches must never arrive here.
struct BlackholeProxy {
    url: String,
}

impl BlackholeProxy {
    fn start() -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("proxy bind");
        let url = format!("http://{}", listener.local_addr().expect("proxy addr"));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                // Drop without answering: a proxied fetch fails on the
                // reset instead of reaching its target.
                drop(stream);
            }
        });
        Self { url }
    }
}

/// A refused fetch rejects with `TypeError` and names the policy and the
/// refused address.
fn assert_fetch_refused(node: &TestNode, path: &str, address: &str) {
    let result = get_json(node, path);
    assert_eq!(result["ok"], true, "{result}");
    let outcome = &result["result"];
    assert_eq!(outcome["refused"], true, "{result}");
    assert_eq!(outcome["name"], "TypeError", "{result}");
    let message = outcome["message"].as_str().unwrap_or("");
    assert!(message.contains("egress policy refused"), "{result}");
    assert!(message.contains(address), "{result}");
}

#[test]
fn egress_policy_conformance() {
    let dir = tempfile::Builder::new()
        .prefix("celld-egress-")
        .tempdir()
        .expect("tempdir");
    let echo = EchoServer::start();
    let proxy = BlackholeProxy::start();
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir().join("app"), &app_project, echo.port);
    let node = TestNode::boot(&app_project, echo.port, &proxy.url);

    // Control first: the allowlisted loopback port answers, proving the
    // node serves under `public`, the allowlist passes dials through,
    // and tenant clients ignore the proxy variables (anything routed to
    // the blackhole would fail instead of answering).
    let allowed = get_json(&node, "/fetch-allowlisted");
    assert_eq!(allowed["ok"], true, "{allowed}");
    assert_eq!(allowed["result"]["status"], 200, "{allowed}");
    assert_eq!(allowed["result"]["body"], "echo-ok", "{allowed}");

    // Every denied range refuses, by literal and (for loopback) by
    // hostname, and each refusal names the refused address.
    assert_fetch_refused(&node, "/fetch-loopback", "127.0.0.1");
    assert_fetch_refused(&node, "/fetch-private", "10.255.255.1");
    assert_fetch_refused(&node, "/fetch-metadata", "169.254.169.254");
    assert_fetch_refused(&node, "/fetch-cgnat", "100.64.0.1");
    assert_fetch_refused(&node, "/fetch-v6-loopback", "::1");
    assert_fetch_refused(&node, "/fetch-mapped", "::ffff:127.0.0.1");
    let localhost = get_json(&node, "/fetch-localhost");
    assert_eq!(localhost["ok"], true, "{localhost}");
    assert_eq!(localhost["result"]["refused"], true, "{localhost}");
    assert_eq!(localhost["result"]["name"], "TypeError", "{localhost}");
    let message = localhost["result"]["message"].as_str().unwrap_or("");
    assert!(message.contains("egress policy refused"), "{localhost}");
    assert!(message.contains("localhost"), "{localhost}");

    // The allowlist is port-scoped: the next loopback port stays refused.
    assert_fetch_refused(&node, "/fetch-allowlisted-wrongport", "127.0.0.1");

    // A redirect to a refused address refuses on the hop.
    assert_fetch_refused(&node, "/fetch-redirect-private", "127.0.0.1");

    // An unreachable public address fails TCP, not policy: the error
    // must not name the egress policy.
    let public = get_json(&node, "/fetch-public-unreachable");
    assert_eq!(public["ok"], true, "{public}");
    assert_eq!(public["result"]["refused"], true, "{public}");
    assert_eq!(public["result"]["name"], "Error", "{public}");
    assert_eq!(public["result"]["policed"], false, "{public}");

    // `connect()` refuses a private address and dials the allowlisted one.
    let refused = get_json(&node, "/connect-private");
    assert_eq!(refused["ok"], true, "{refused}");
    assert_eq!(refused["result"]["refused"], true, "{refused}");
    let message = refused["result"]["message"].as_str().unwrap_or("");
    assert!(message.contains("egress policy refused"), "{refused}");
    assert!(message.contains("10.255.255.1"), "{refused}");
    let connected = get_json(&node, "/connect-allowlisted");
    assert_eq!(connected["ok"], true, "{connected}");
    assert!(
        connected["result"]["remote"]
            .as_str()
            .unwrap_or("")
            .contains("127.0.0.1"),
        "{connected}"
    );
    assert_eq!(connected["result"]["echo"], "ping", "{connected}");

    // Outbound WebSockets refuse a private address and reach the
    // allowlisted one.
    let ws_refused = get_json(&node, "/ws-private");
    assert_eq!(ws_refused["ok"], true, "{ws_refused}");
    assert_eq!(ws_refused["result"]["refused"], true, "{ws_refused}");
    let message = ws_refused["result"]["message"].as_str().unwrap_or("");
    assert!(message.contains("egress policy refused"), "{ws_refused}");
    assert!(message.contains("10.255.255.1"), "{ws_refused}");
    let ws = get_json(&node, "/ws-allowlisted");
    assert_eq!(ws["ok"], true, "{ws}");
    assert_eq!(ws["result"]["echo"], "hello-ws", "{ws}");
}
