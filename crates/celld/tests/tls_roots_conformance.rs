// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): shared tenant TLS roots regression test (upstream #230).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! Tenant TLS clients share one root store: Mozilla's roots plus the
//! platform store and `SSL_CERT_FILE` (denoland/celld#230). `fetch()` to a
//! private-CA host succeeded while `cloudflare:sockets` `startTls()` and
//! outbound WebSockets verified against Mozilla only. The test serves HTTPS
//! from a loopback server whose self-signed anchor is trusted only through
//! `SSL_CERT_FILE`, then fetches it through the real tenant `fetch()`
//! client: with the variable set the fetch succeeds, without it the fetch
//! fails, so the test goes red if the clients revert to Mozilla-only roots.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn celld_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_celld"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/tls-roots")
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

/// A loopback HTTPS server presenting the fixture's self-signed certificate:
/// its anchor reaches the client only through `SSL_CERT_FILE`, never through
/// Mozilla's roots. Runs on its own thread with its own runtime, like the
/// capture servers in the other conformance tests, and exits on a generous
/// deadline so a failed assertion cannot leak it.
struct TlsServer {
    port: u16,
}

impl TlsServer {
    fn start(cert_path: &Path, key_path: &Path) -> Self {
        let certs = {
            let mut reader =
                std::io::BufReader::new(std::fs::File::open(cert_path).expect("open cert"));
            rustls_pemfile::certs(&mut reader)
                .map(|result| result.expect("parse cert"))
                .collect::<Vec<_>>()
        };
        let key = {
            let mut reader =
                std::io::BufReader::new(std::fs::File::open(key_path).expect("open key"));
            rustls_pemfile::private_key(&mut reader)
                .expect("read key")
                .expect("fixture holds a private key")
        };
        let config = std::sync::Arc::new(
            rustls::ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .expect("fixture cert and key agree"),
        );
        let acceptor = tokio_rustls::TlsAcceptor::from(config);
        let (port_tx, port_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("server runtime");
            runtime.block_on(async move {
                // A bounded serve: the test is over long before this fires.
                let _ = tokio::time::timeout(Duration::from_secs(120), async {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind loopback");
                    port_tx
                        .send(listener.local_addr().expect("server addr").port())
                        .expect("hand the port back");
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            continue;
                        };
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            let Ok(mut tls) = acceptor.accept(stream).await else {
                                return;
                            };
                            // One GET: read the head, ignore the rest.
                            let mut head = Vec::new();
                            let mut chunk = vec![0u8; 8192];
                            loop {
                                match tls.read(&mut chunk).await {
                                    Ok(0) => return,
                                    Ok(read) => {
                                        head.extend_from_slice(&chunk[..read]);
                                        if head.windows(4).any(|window| window == b"\r\n\r\n") {
                                            break;
                                        }
                                    }
                                    Err(_) => return,
                                }
                            }
                            const RESPONSE: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 6\r\nconnection: close\r\n\r\ntls-ok";
                            let _ = tls.write_all(RESPONSE).await;
                            let _ = tls.shutdown().await;
                        });
                    }
                })
                .await;
            });
        });
        let port = port_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("server port");
        Self { port }
    }
}

/// A booted `celld dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path, ssl_cert_file: Option<&Path>) -> Self {
        let mut command = Command::new(celld_binary());
        command
            .arg("dev")
            .env("NO_COLOR", "1")
            .arg(app_project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--logs")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // `Command` inherits the parent environment: without this, a parent
        // `SSL_CERT_FILE` pointing at the fixture CA would leak into the
        // negative control and turn the expected 500 into a 200.
        command.env_remove("SSL_CERT_FILE");
        if let Some(ca) = ssl_cert_file {
            command.env("SSL_CERT_FILE", ca);
        }
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

/// The tenant TLS clients trust the `SSL_CERT_FILE` anchor: the private-CA
/// loopback fetch succeeds through the real `fetch()` client and a
/// `startTls()` handshake succeeds through the socket client, with the
/// variable set.
#[test]
fn tenant_fetch_trusts_ssl_cert_file_roots() {
    let fixture = fixture_dir();
    let server = TlsServer::start(
        &fixture.join("server-cert.pem"),
        &fixture.join("server-key.pem"),
    );
    let project = fixture_tempdir("celld-tls-roots-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path(), Some(&fixture.join("ca-cert.pem")));
    let (status, body) =
        get(&node.public, &format!("/fetch-tls?port={}", server.port)).expect("GET /fetch-tls");
    assert_eq!(status, 200, "GET /fetch-tls: {body}");
    let fetched: serde_json::Value = serde_json::from_str(&body).expect("fetch JSON");
    assert_eq!(
        fetched.get("status").and_then(|v| v.as_u64()),
        Some(200),
        "the upstream answered: {body}"
    );
    assert_eq!(
        fetched.get("body").and_then(|v| v.as_str()),
        Some("tls-ok"),
        "the upstream body survived: {body}"
    );
    // The same anchor carries a `startTls()` handshake: the socket client
    // verifies against the shared roots too.
    let (status, body) =
        get(&node.public, &format!("/starttls?port={}", server.port)).expect("GET /starttls");
    assert_eq!(status, 200, "GET /starttls: {body}");
    let upgraded: serde_json::Value = serde_json::from_str(&body).expect("starttls JSON");
    assert!(
        upgraded
            .get("body")
            .and_then(|v| v.as_str())
            .is_some_and(|text| text.contains("tls-ok")),
        "the upgraded socket read the upstream: {body}"
    );
}

/// Control: without `SSL_CERT_FILE` the same fetches fail, so the positive
/// case above is the anchor doing the work and not some ambient trust.
#[test]
fn tenant_tls_rejects_unknown_private_ca() {
    let fixture = fixture_dir();
    let server = TlsServer::start(
        &fixture.join("server-cert.pem"),
        &fixture.join("server-key.pem"),
    );
    let project = fixture_tempdir("celld-tls-roots-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path(), None);

    for route in ["fetch-tls", "starttls"] {
        let (status, body) =
            get(&node.public, &format!("/{route}?port={}", server.port)).expect("GET {route}");
        assert_eq!(status, 500, "GET /{route}: {body}");
        assert!(
            body.contains("error"),
            "the worker reports the TLS failure: {body}"
        );
    }
}
