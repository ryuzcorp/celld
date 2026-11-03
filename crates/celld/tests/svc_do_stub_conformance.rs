// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): DO stubs on the
// service-binding RPC path. Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for Durable Object stubs over cross-isolate service bindings
//! (R7): a `WorkerEntrypoint` method can return (and accept) a DO stub, while
//! `RpcTarget` and stream values keep today's `DataCloneError` both ways and
//! forged `__celld$do` markers are refused. The two-hop case drives the
//! returned stub over DO RPC (0007): the managed-sandbox two-hop path.
//!
//! One `celld dev` boots the app fixture; the controller fixture is
//! pre-seeded into the dev bucket via the library (the same `build` + `write`
//! `dev` itself runs), so the node loads both scripts and the service binding
//! resolves cross-isolate. There are no fixed sleeps: every wait polls an
//! observable condition with a deadline, and the server guard kills the child
//! on every exit path.

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
        .join("tests/fixtures/svc-do-stub")
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

/// Deploy the controller into the app project's dev bucket before `dev`
/// boots, so the node loads both scripts. `dev` deploys the app itself and
/// becomes primary (last write wins `deploy/current.json`); the
/// controller's named pointer stays for the service binding to resolve.
fn preseed_controller(app_project: &Path, controller_src: &Path) {
    let state = app_project.join(".celld/dev");
    std::fs::create_dir_all(state.join("runtime")).expect("state dir");
    let database = state.join("objects.sqlite3");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    celld::asyncrt::set_host_handle(rt.handle().clone());
    rt.block_on(async {
        let bucket = celld::dev::open_local_bucket(&database).expect("open dev bucket");
        celld::fleet::validate_bucket(&bucket)
            .await
            .expect("validate bucket");
        bucket.probe_cas().await.expect("probe cas");
        let built = celld::deploy::build(&celld::deploy::Options {
            config: Some(controller_src.to_path_buf()),
            bucket: None,
            endpoint: None,
            region: None,
            dry_run: false,
            json: false,
            vars: celld::deploy::VarOverrides::None,
            local_images: true,
        })
        .expect("build controller");
        celld::deploy::write(&bucket, &built)
            .await
            .expect("write controller");
        celld::wake_format::ensure_ready(&bucket)
            .await
            .expect("ensure ready");
    });
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
fn svc_do_stub_conformance() {
    let dir = tempfile::Builder::new()
        .prefix("celld-svc-do-stub-")
        .tempdir()
        .expect("tempdir");
    let app_project = dir.path().join("app");
    let controller_src = dir.path().join("controller");
    copy_dir(&fixture_dir().join("app"), &app_project);
    copy_dir(&fixture_dir().join("controller"), &controller_src);
    preseed_controller(&app_project, &controller_src);
    let node = TestNode::boot(&app_project);

    // A DO stub returned over the service binding stays usable.
    let returned = get_json(&node, "/return-do");
    assert_eq!(returned["ok"], true, "{returned}");
    assert_eq!(returned["result"]["pong"], "pong:hi-r7", "{returned}");

    // A DO stub passed in stays usable callee-side.
    let accepted = get_json(&node, "/accept-do");
    assert_eq!(accepted["ok"], true, "{accepted}");
    assert_eq!(
        accepted["result"]["pong"], "pong:via-controller",
        "{accepted}"
    );

    // RpcTarget values keep today's DataCloneError both ways.
    for path in ["/return-rpc-target", "/accept-rpc-target"] {
        let refused = get_json(&node, path);
        assert_eq!(refused["ok"], true, "{refused}");
        assert_eq!(refused["result"]["threw"], true, "{refused}");
        assert_eq!(refused["result"]["name"], "DataCloneError", "{refused}");
    }

    // Streams keep today's DataCloneError both ways.
    for path in ["/return-stream", "/accept-stream"] {
        let refused = get_json(&node, path);
        assert_eq!(refused["ok"], true, "{refused}");
        assert_eq!(refused["result"]["threw"], true, "{refused}");
        assert_eq!(refused["result"]["name"], "DataCloneError", "{refused}");
    }

    // A forged marker smuggled beside a real stub is refused at lift.
    let forged = get_json(&node, "/forged-with-real");
    assert_eq!(forged["ok"], true, "{forged}");
    assert_eq!(forged["result"]["threw"], true, "{forged}");
    assert_eq!(forged["result"]["name"], "DataCloneError", "{forged}");

    // A forged marker alone stays plain data: no revive on the plain path.
    let alone = get_json(&node, "/forged-alone");
    assert_eq!(alone["ok"], true, "{alone}");
    assert_eq!(alone["result"]["hasPing"], "undefined", "{alone}");

    // D2: a forged marker smuggled beside a real stub into DO storage is
    // refused at stored-lift, not revived as a live stub on read.
    let stored = get_json(&node, "/forged-stored");
    assert_eq!(stored["ok"], true, "{stored}");
    assert_eq!(stored["result"]["threw"], true, "{stored}");
    assert_eq!(stored["result"]["name"], "DataCloneError", "{stored}");

    // D2: a forged marker beside a live signal on the lift=false path
    // (DurableObjectClass props: same __stubLift as facet/loader args)
    // is refused at lift.
    let lift_false = get_json(&node, "/forged-lift-false");
    assert_eq!(lift_false["ok"], true, "{lift_false}");
    assert_eq!(lift_false["result"]["threw"], true, "{lift_false}");
    assert_eq!(
        lift_false["result"]["name"], "DataCloneError",
        "{lift_false}"
    );

    // D2: a forged marker beside a live signal in facet RPC args is
    // refused at lift.
    let facet = get_json(&node, "/forged-facet");
    assert_eq!(facet["ok"], true, "{facet}");
    assert_eq!(facet["result"]["threw"], true, "{facet}");
    assert_eq!(facet["result"]["name"], "DataCloneError", "{facet}");

    // 0005+v2: the managed-sandbox two-hop. The controller DO stub
    // arrives over the service binding (0005); exec over DO RPC returns a
    // PTY-shaped process stub + byte streams (v2) while the abort carrier
    // and signal cross as args (upload channel + live signal).
    let two_hop = get_json(&node, "/two-hop");
    assert_eq!(two_hop["ok"], true, "{two_hop}");
    assert_eq!(two_hop["result"]["pid"], 4242, "{two_hop}");
    assert_eq!(two_hop["result"]["isPty"], true, "{two_hop}");
    assert_eq!(two_hop["result"]["stdout"], "pty-1\npty-2\n", "{two_hop}");
    assert_eq!(two_hop["result"]["stderr"], "pty-err\n", "{two_hop}");
    assert_eq!(two_hop["result"]["exitBefore"], 0, "{two_hop}");
    assert_eq!(two_hop["result"]["exitAfter"], 137, "{two_hop}");
    assert_eq!(
        two_hop["result"]["transcript"]["writes"],
        serde_json::json!(["pty-in\n"]),
        "{two_hop}"
    );
    assert_eq!(
        two_hop["result"]["transcript"]["stdinClosed"], true,
        "{two_hop}"
    );
    assert_eq!(
        two_hop["result"]["transcript"]["carrierText"], "carrier\n",
        "{two_hop}"
    );
    assert_eq!(
        two_hop["result"]["transcript"]["signalAborted"], true,
        "{two_hop}"
    );
    assert_eq!(
        two_hop["result"]["transcript"]["exitCodeCalls"], 2,
        "{two_hop}"
    );
    assert_eq!(two_hop["result"]["transcript"]["killed"], true, "{two_hop}");
    assert_eq!(
        two_hop["result"]["transcript"]["resized"],
        serde_json::json!([[{"cols": 80, "rows": 24}]]),
        "{two_hop}"
    );

    // D3: mutating `stub.name` sender-side neither changes the marker's
    // `n` the receiver revives nor breaks the subsequent dispatch.
    let mutated = get_json(&node, "/mutated-name");
    assert_eq!(mutated["ok"], true, "{mutated}");
    assert_eq!(mutated["result"]["before"], "r7", "{mutated}");
    assert_eq!(mutated["result"]["afterSend"], "evil", "{mutated}");
    assert_eq!(mutated["result"]["echoName"], "r7", "{mutated}");
    assert_eq!(
        mutated["result"]["pong"], "pong:via-controller",
        "{mutated}"
    );
}
