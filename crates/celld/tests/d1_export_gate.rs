// Modified by ryuzcorp (ryuzcorp/celld): Queue/KV operator fetch gate; Queue/KV stub-RPC allowlists; Queue DLQ RPC refusal; Queue send cell-side validation; KV prototype-safe operator lookup. Apache-2.0.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): D1 export is operator-only.
// Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for the D1 operator gate: a Worker that holds a D1 binding can
//! neither dump the database over stub RPC (`__d1Export`) nor over a crafted
//! `stub.fetch()` export, while the binding's own query/batch/exec and the
//! fetch query branch keep working. Every probe uses the Worker's own bound
//! database, so the suite passes whether or not a forged binding for another
//! database is possible.
//!
//! One `celld dev` boots the app fixture. There are no fixed sleeps: every
//! wait polls an observable condition with a deadline, and the server guard
//! kills the child on every exit path.

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
        .join("tests/fixtures/d1-export-gate")
        .canonicalize()
        .expect("fixture exists")
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    listener.local_addr().expect("probe addr").port()
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// A booted `celld dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path) -> Self {
        let port = free_port();
        let mut child = Command::new(celld_binary())
            .arg("dev")
            .arg(app_project)
            .arg("--port")
            .arg(port.to_string())
            .arg("--no-watch")
            .arg("--logs")
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

#[test]
fn d1_export_is_operator_only() {
    let dir = tempfile::Builder::new()
        .prefix("celld-d1-export-gate-")
        .tempdir()
        .expect("tempdir");
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir().join("app"), &app_project);
    let node = TestNode::boot(&app_project);

    // Seed rows so a refusal cannot be mistaken for an empty database.
    let seed = get_json(&node, "/seed");
    assert_eq!(seed["ok"], true, "{seed}");

    // The binding's own surface keeps working: the RPC allowlist permits
    // exactly what the binding uses, and the fetch gate leaves the query
    // branch alone.
    let binding = get_json(&node, "/binding-ok");
    assert_eq!(binding["ok"], true, "{binding}");
    assert_eq!(binding["result"]["first"], "one", "{binding}");
    assert_eq!(
        binding["result"]["batchCounts"],
        serde_json::json!([1, 1]),
        "{binding}"
    );

    let query = get_json(&node, "/fetch-query");
    assert_eq!(query["ok"], true, "{query}");
    assert_eq!(query["result"]["status"], 200, "{query}");
    assert_eq!(
        query["result"]["rows"],
        serde_json::json!([["one"]]),
        "{query}"
    );

    // Stub RPC export is refused: the method is not on the D1 allowlist.
    let rpc = get_json(&node, "/rpc-export");
    assert_eq!(rpc["ok"], true, "{rpc}");
    assert_eq!(rpc["result"]["threw"], true, "{rpc}");
    assert!(
        rpc["result"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("is not a function"),
        "{rpc}"
    );

    // A crafted stub.fetch() export is refused: the request carries no
    // operator-ingress marker.
    let fetched = get_json(&node, "/fetch-export");
    assert_eq!(fetched["ok"], true, "{fetched}");
    assert_eq!(fetched["result"]["status"], 403, "{fetched}");
    assert!(
        fetched["result"]["body"]
            .as_str()
            .unwrap_or_default()
            .contains("operator"),
        "{fetched}"
    );

    // A forged operator init key gains nothing: the Request constructor
    // honors no such key, and the dispatch builds a fresh server-side
    // Request that carries no membership.
    let forged = get_json(&node, "/fetch-forged-operator");
    assert_eq!(forged["ok"], true, "{forged}");
    assert_eq!(forged["result"]["status"], 403, "{forged}");
    assert!(
        forged["result"]["body"]
            .as_str()
            .unwrap_or_default()
            .contains("operator"),
        "{forged}"
    );

    // Hardening: migration over stub RPC and stub.fetch refuses the same way.
    let migrate_rpc = get_json(&node, "/rpc-migrate");
    assert_eq!(migrate_rpc["ok"], true, "{migrate_rpc}");
    assert_eq!(migrate_rpc["result"]["threw"], true, "{migrate_rpc}");

    let migrate_fetch = get_json(&node, "/fetch-migrate");
    assert_eq!(migrate_fetch["ok"], true, "{migrate_fetch}");
    assert_eq!(migrate_fetch["result"]["status"], 403, "{migrate_fetch}");

    // Hardening: exec with rows over stub.fetch refuses; the Worker binding's
    // exec() never asks for rows.
    let exec_rows = get_json(&node, "/fetch-exec-rows");
    assert_eq!(exec_rows["ok"], true, "{exec_rows}");
    assert_eq!(exec_rows["result"]["status"], 403, "{exec_rows}");

    // The KV binding keeps working over RPC while every fetch op refuses:
    // the fetch surface is the operator's alone.
    let kv_ok = get_json(&node, "/kv-binding-ok");
    assert_eq!(kv_ok["ok"], true, "{kv_ok}");
    assert_eq!(kv_ok["result"]["value"], "gate-value", "{kv_ok}");
    assert_eq!(kv_ok["result"]["metaValue"], "gate-value", "{kv_ok}");
    assert!(kv_ok["result"]["gone"].is_null(), "{kv_ok}");
    assert_eq!(
        kv_ok["result"]["names"],
        serde_json::json!(["gate-key"]),
        "{kv_ok}"
    );
    assert_eq!(kv_ok["result"]["complete"], true, "{kv_ok}");

    let kv = get_json(&node, "/kv-gate");
    assert_eq!(kv["ok"], true, "{kv}");
    for op in ["put", "get", "info", "delete", "list", "put-base64"] {
        assert_eq!(kv["result"][op]["status"], 403, "{kv} {op}");
        assert!(
            kv["result"][op]["body"]
                .as_str()
                .unwrap_or_default()
                .contains("operator"),
            "{kv} {op}"
        );
    }

    // Inherited op names must not resolve through the operator lookup:
    // they fall through to "unknown operation" instead of 403ing with a
    // leaked native function in the message. A JSON null body parses
    // without throwing and answers 400, not 500.
    let proto = get_json(&node, "/kv-gate-proto");
    assert_eq!(proto["ok"], true, "{proto}");
    for op in ["constructorOp", "toStringOp", "protoOp"] {
        assert_eq!(proto["result"][op]["status"], 400, "{proto} {op}");
        let body = proto["result"][op]["body"].as_str().unwrap_or_default();
        assert!(body.contains("unknown operation"), "{proto} {op}");
        assert!(
            !body.contains("native code") && !body.contains("function Object"),
            "{proto} {op}"
        );
    }
    assert_eq!(proto["result"]["nullBody"]["status"], 400, "{proto}");
    assert!(
        proto["result"]["nullBody"]["body"]
            .as_str()
            .unwrap_or_default()
            .contains("invalid request body"),
        "{proto}"
    );

    // The Queue fetch has no lookup table, but a JSON null body still
    // reaches past the parse: it answers 400 from inside the handler.
    let queue_null = get_json(&node, "/queue-gate-null");
    assert_eq!(queue_null["ok"], true, "{queue_null}");
    assert_eq!(queue_null["result"]["status"], 400, "{queue_null}");

    // Stub RPC on the KV scope refuses the internal cell methods the D1
    // allowlist shape refuses: sweep and metrics run as direct calls
    // inside the cell, never over RPC.
    let kv_rpc = get_json(&node, "/kv-rpc-gate");
    assert_eq!(kv_rpc["ok"], true, "{kv_rpc}");
    for method in ["sweep", "metrics"] {
        assert_eq!(kv_rpc["result"][method]["threw"], true, "{kv_rpc} {method}");
        assert!(
            kv_rpc["result"][method]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("is not a function"),
            "{kv_rpc} {method}"
        );
    }

    // The Queue binding sends while every operator fetch op refuses.
    let queue_ok = get_json(&node, "/queue-binding-ok");
    assert_eq!(queue_ok["ok"], true, "{queue_ok}");
    assert_eq!(queue_ok["result"]["sent"], true, "{queue_ok}");
    assert_eq!(queue_ok["result"]["backlog"], 3, "{queue_ok}");

    let queue = get_json(&node, "/queue-gate");
    assert_eq!(queue["ok"], true, "{queue}");
    for op in ["info", "purge", "pause", "resume", "peek", "redrive"] {
        assert_eq!(queue["result"][op]["status"], 403, "{queue} {op}");
        assert!(
            queue["result"][op]["body"]
                .as_str()
                .unwrap_or_default()
                .contains("operator"),
            "{queue} {op}"
        );
    }

    // Stub RPC on the Queue scope refuses the operator cell methods and
    // the DLQ accept alike: the accept rides the `__queue_dlq_forward`
    // host op, never stub RPC, so even the reviewer's forged batch
    // (forged id, forged dead-letter source, lying size) refuses before
    // validation. Direct `__queueSend` validates every entry cell-side.
    let queue_rpc = get_json(&node, "/queue-rpc-gate");
    assert_eq!(queue_rpc["ok"], true, "{queue_rpc}");
    for method in [
        "purge",
        "peek",
        "setPaused",
        "redrive",
        "dlqAccept",
        "dlqForge",
    ] {
        assert_eq!(
            queue_rpc["result"][method]["threw"], true,
            "{queue_rpc} {method}"
        );
        assert!(
            queue_rpc["result"][method]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("is not a function"),
            "{queue_rpc} {method}"
        );
    }
    for (method, fragment) in [
        ("sendSizeLie", "byte length"),
        ("sendBadType", "contentType"),
        ("sendOversize", "at most"),
        ("sendBadDelay", "delaySeconds"),
    ] {
        assert_eq!(
            queue_rpc["result"][method]["threw"], true,
            "{queue_rpc} {method}"
        );
        assert!(
            queue_rpc["result"][method]["message"]
                .as_str()
                .unwrap_or_default()
                .contains(fragment),
            "{queue_rpc} {method}"
        );
    }
    // A well-formed direct send still lands: the cell validates entries,
    // it does not block the method.
    assert_eq!(
        queue_rpc["result"]["sendDirectOk"]["threw"], false,
        "{queue_rpc}"
    );

    // Host-driven settlement stays open: a body with no lease 409s instead
    // of 403ing.
    let settle = get_json(&node, "/queue-settle-open");
    assert_eq!(settle["ok"], true, "{settle}");
    assert_eq!(settle["result"]["status"], 409, "{settle}");
}
