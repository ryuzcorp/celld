// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): node identity on env (upstream #219).

// A black-box runner, not node code: it boots the real `cello dev` binary
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A bundle learns the name of the node it runs on through `env.CELLO_NODE`
//! (denoland/celld#219): the host injects its own identity after every
//! deployment binding, and deployments cannot bind the name. The node boots
//! with an explicit `CELLO_NODE`, serves the fixture Worker, and the Worker
//! echoes the value back.

mod common;

use common::{announced, forward_announce};
use serde_json::Value;
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
        .join("tests/fixtures/node-identity")
        .canonicalize()
        .expect("fixture exists")
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

struct TestNode {
    child: Child,
    public: String,
    /// The serving node's own lease identity, from its `node lease attempt`
    /// log line (`node=<name>`): the identity the Worker must echo.
    node_name: String,
}

impl TestNode {
    fn boot(project: &Path) -> Self {
        let mut command = Command::new(cello_binary());
        command
            .arg("dev")
            .arg(project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--logs")
            // `--logs` forwards through the inherited filter, so an ambient
            // `RUST_LOG=error` would suppress the lease records boot waits
            // for. Pin the filter the default path already uses.
            .env("RUST_LOG", "info")
            // An ambient `CELLO_DEV_STATE` would pull the child's state out
            // of the temporary project and leave `objects.sqlite3` behind.
            // Drop it so the child keeps the normal project-local state.
            .env_remove("CELLO_DEV_STATE")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn cello dev");
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
            node_name: String::new(),
        };
        let (public, node_name) = poll_until("boot", Duration::from_secs(120), || {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!(
                    "cello dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            let public = announced(&announced_lines, "cello listening on ")?;
            // The serving child logs its lease attempts with its own node
            // name (`node=<name>`); the supervisor names the child
            // `dev-<project hash>` through `CELLO_NODE`, so the two agree.
            let node_name = announced_lines
                .lock()
                .expect("announce lock")
                .iter()
                .find_map(|line| {
                    if !line.contains("node_lease_attempt") {
                        return None;
                    }
                    let (_, rest) = line.split_once("node=")?;
                    Some(
                        rest.split([',', ' ', '"'])
                            .next()
                            .unwrap_or_default()
                            .to_string(),
                    )
                })
                .filter(|name| !name.is_empty())?;
            if !probe(&public, "/") {
                return None;
            }
            Some((public, node_name))
        });
        node.public = public;
        node.node_name = node_name;
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

fn poll_until<T>(name: &str, deadline: Duration, mut condition: impl FnMut() -> Option<T>) -> T {
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

/// The Worker reads its own node's name from `env`, with no per-node
/// configuration: the host injects the identity it already routes by.
#[test]
fn worker_reads_its_node_name_from_env() {
    let project = tempfile::tempdir().expect("project dir");
    copy_dir(&fixture_dir(), project.path());
    let node = TestNode::boot(project.path());
    let (status, body) = get(&node.public, "/").expect("public GET /");
    assert_eq!(status, 200, "GET /: {body}");
    let reply: Value = serde_json::from_str(&body).expect("JSON body");
    assert_eq!(
        reply["node"],
        node.node_name.as_str(),
        "env.CELLO_NODE must name the serving node: {reply}"
    );
}
