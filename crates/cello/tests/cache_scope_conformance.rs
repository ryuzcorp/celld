// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): deployment cache scope
// conformance; deploy --read-root option; announce-line discovery.
// Apache-2.0.

// A black-box runner, not node code: it boots the binary, deploys
// fixtures through the library, and polls TCP with wall-clock deadlines,
// so the execution boundary's injected clock/filesystem/async rules do
// not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for `CELLO_CACHE_SCOPE`: which entries a cache op
//! addresses — one script's, or the serving deployment's.
//!
//! - `cache_deployment_scope_shares`: with the deployment scope, the app
//!   puts and the probe (a second script of the same deployment) matches
//!   and deletes the entry, in `caches.default` and in a named namespace,
//!   and a cross-script overwrite lands in the one shared entry.
//! - `cache_script_scope_isolates_by_default`: with no scope configured,
//!   the probe sees none of the app's entries and deletes none of them.
//! - `cache_deployment_scope_isolates_other_deployments`: a different
//!   fleet (a second node) and a redeployed generation (a new deployment
//!   identity on the same node) never see the first deployment's entries.
//! - `cache_scope_rejects_garbage`: a scope that names neither `script`
//!   nor `deployment` refuses the boot.
//!
//! Every test boots its own `cello dev` nodes on hermetic tempdirs, so
//! tests share nothing and run in any order. Boots use `--port 0` with
//! worker-listener discovery from the node's own announce line, which the
//! supervisor forwards to stderr (`--logs`), so parallel boots never share
//! a port and no probe can answer from a neighboring test's node. The node
//! is non-dumpable (patch 0053), so `/proc/<pid>/fd` cannot name its
//! listeners. Library deploys (the probe preseed, the mid-test redeploy)
//! run on one shared runtime, because `asyncrt::set_host_handle` runs
//! exactly once per process. There are no fixed sleeps: every wait polls an
//! observable condition with a deadline, and the server guard kills the
//! child on every exit path.

use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One runtime for the whole binary: `asyncrt::set_host_handle` runs
/// exactly once per process. Blocking is serialized so no two tests
/// drive the runtime at once.
static SHARED_RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
static BLOCK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn rt() -> &'static tokio::runtime::Runtime {
    SHARED_RT.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        cello::asyncrt::set_host_handle(rt.handle().clone());
        rt
    })
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let _guard = BLOCK_LOCK.lock().expect("block lock");
    rt().block_on(future)
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/cache-scope")
        .canonicalize()
        .expect("fixture exists")
}

mod common;

use common::{announced, cello_command, forward_announce};

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// Percent-encode a query value: cache names carry characters no raw
/// query string can hold.
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

/// Open the dev bucket of a project that `cello dev` has not booted yet.
fn open_bucket(app_project: &Path) -> cello::bucket::Bucket {
    let state = app_project.join(".cello/dev");
    std::fs::create_dir_all(state.join("runtime")).expect("state dir");
    let database = state.join("objects.sqlite3");
    block_on(async {
        let bucket = cello::dev::open_local_bucket(&database).expect("open dev bucket");
        cello::fleet::validate_bucket(&bucket)
            .await
            .expect("validate bucket");
        bucket.probe_cas().await.expect("probe cas");
        cello::wake_format::ensure_ready(&bucket)
            .await
            .expect("ensure ready");
        bucket
    })
}

/// Build the project at `src` and write it to `bucket`, moving the fleet
/// pointer: the node's watcher adopts it as a new deployment.
fn deploy_src(bucket: &cello::bucket::Bucket, src: &Path) {
    block_on(async {
        let built = cello::deploy::build(&cello::deploy::Options {
            config: Some(src.to_path_buf()),
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
        .expect("build fixture");
        cello::deploy::write(bucket, &built)
            .await
            .expect("write fixture");
    })
}

/// Deploy the probe into the app project's dev bucket before `dev`
/// boots, so the node loads both scripts. `dev` deploys the app itself
/// and becomes primary (last write wins `deploy/current.json`); the
/// probe's named pointer stays for the service binding to resolve.
fn preseed_probe(app_project: &Path, probe_src: &Path) {
    let bucket = open_bucket(app_project);
    deploy_src(&bucket, probe_src);
}

/// A booted `cello dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path, envs: &[(&str, &str)]) -> Self {
        // Port zero: the OS assigns the worker port, so parallel boots
        // can never claim the same one; discovery below learns it.
        let mut command = cello_command(app_project, [("NO_COLOR", "1")]);
        command.envs(envs.iter().copied());
        let mut child = command
            .arg("dev")
            .arg(app_project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--logs")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cello dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        // Drain stdout so a chatty child can never block on a full pipe.
        // The supervisor's stderr carries the node's announce lines.
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        let announced_lines: Arc<Mutex<Vec<String>>> = Arc::default();
        forward_announce(stderr, Arc::clone(&announced_lines));
        // The kill-on-drop guard exists before discovery, so a boot
        // timeout or an early child exit cannot leak the server; the
        // address is a placeholder until discovery fills it.
        let mut node = Self {
            child,
            public: String::new(),
        };
        // Discovery doubles as readiness: the node announces its worker
        // listener before it serves, and /health answers once the app
        // loads. Only this child's announce is ever read, so a
        // neighboring test's node cannot answer.
        let public = poll_until("boot", Duration::from_secs(120), || {
            // Liveness first: a dead child must fail fast, not burn
            // the deadline on refused probes.
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("cello dev exited during boot: {status}");
            }
            let addr = announced(&announced_lines, "cello listening on ")?;
            probe(&addr, "/health").then_some(addr)
        });
        node.public = public;
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

fn query(endpoint: &str, params: &[(&str, &str)]) -> String {
    let query = params
        .iter()
        .map(|(name, value)| format!("{name}={}", encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!("/{endpoint}?{query}")
}

/// `put` then `match` one key; the match is a separate request, so a hit
/// proves the entry survived outside the putting isolate.
fn put(node: &TestNode, params: &[(&str, &str)]) -> Value {
    let stored = get_json(node, &query("put", params));
    assert_eq!(stored["ok"], true, "{stored}");
    assert_eq!(stored["result"]["bodyUsed"], true, "{stored}");
    stored
}

fn matched(node: &TestNode, params: &[(&str, &str)]) -> Value {
    let found = get_json(node, &query("match", params));
    assert_eq!(found["ok"], true, "{found}");
    assert_eq!(found["result"]["hit"], true, "{found}");
    found["result"].clone()
}

fn missed(node: &TestNode, params: &[(&str, &str)]) {
    let found = get_json(node, &query("match", params));
    assert_eq!(found["ok"], true, "{found}");
    assert_eq!(found["result"]["hit"], false, "{found}");
}

fn boot_node(envs: &[(&str, &str)]) -> (tempfile::TempDir, TestNode) {
    let dir = tempfile::Builder::new()
        .prefix("cello-cache-scope-")
        .tempdir()
        .expect("tempdir");
    let app_project = dir.path().join("app");
    let probe_src = dir.path().join("probe");
    copy_dir(&fixture_dir().join("app"), &app_project);
    copy_dir(&fixture_dir().join("probe"), &probe_src);
    preseed_probe(&app_project, &probe_src);
    let node = TestNode::boot(&app_project, envs);
    (dir, node)
}

#[test]
fn cache_deployment_scope_shares() {
    let (_dir, node) = boot_node(&[("CELLO_CACHE_SCOPE", "deployment")]);

    // The app puts, the probe matches: one entry, two scripts.
    put(&node, &[("key", "s1"), ("body", "shared-app")]);
    let peer = get_json(&node, &query("peer-get", &[("key", "s1")]));
    assert_eq!(peer["result"]["hit"], true, "{peer}");
    assert_eq!(peer["result"]["body"], "shared-app", "{peer}");

    // The probe puts, the app matches.
    get_json(
        &node,
        &query("peer-put", &[("key", "s2"), ("body", "shared-probe")]),
    );
    let back = matched(&node, &[("key", "s2")]);
    assert_eq!(back["body"], "shared-probe", "{back}");

    // The probe deletes the app's entry: the purge lands in the shared
    // scope, so the app misses afterwards.
    let deleted = get_json(&node, &query("peer-delete", &[("key", "s1")]));
    assert_eq!(deleted["result"]["peerDeleted"], true, "{deleted}");
    missed(&node, &[("key", "s1")]);

    // A named namespace is shared the same way.
    put(
        &node,
        &[("key", "n1"), ("body", "named-app"), ("cache", "pages")],
    );
    let named = get_json(
        &node,
        &query("peer-get", &[("key", "n1"), ("cache", "pages")]),
    );
    assert_eq!(named["result"]["hit"], true, "{named}");
    assert_eq!(named["result"]["body"], "named-app", "{named}");
    get_json(
        &node,
        &query(
            "peer-put",
            &[("key", "n2"), ("body", "named-probe"), ("cache", "pages")],
        ),
    );
    let named_back = matched(&node, &[("key", "n2"), ("cache", "pages")]);
    assert_eq!(named_back["body"], "named-probe", "{named_back}");
    // The default namespace holds no copy of a named entry.
    missed(&node, &[("key", "n1")]);

    // A cross-script overwrite lands in the one shared entry: the last
    // write wins for both scripts.
    put(&node, &[("key", "w1"), ("body", "APP")]);
    get_json(
        &node,
        &query("peer-put", &[("key", "w1"), ("body", "PROBE")]),
    );
    let won = matched(&node, &[("key", "w1")]);
    assert_eq!(won["body"], "PROBE", "{won}");
    let won_peer = get_json(&node, &query("peer-get", &[("key", "w1")]));
    assert_eq!(won_peer["result"]["body"], "PROBE", "{won_peer}");
}

#[test]
fn cache_script_scope_isolates_by_default() {
    // No CELLO_CACHE_SCOPE: today's per-script entries, unchanged.
    let (_dir, node) = boot_node(&[]);

    put(&node, &[("key", "iso1"), ("body", "secret-app")]);
    matched(&node, &[("key", "iso1")]);
    let peer_miss = get_json(&node, &query("peer-check", &[("key", "iso1")]));
    assert_eq!(peer_miss["result"]["peerHit"], false, "{peer_miss}");

    get_json(
        &node,
        &query("peer-put", &[("key", "iso2"), ("body", "secret-probe")]),
    );
    missed(&node, &[("key", "iso2")]);

    // The probe's delete reaches only the probe's entries: it reports
    // nothing deleted, and the app's entry still hits.
    let deleted = get_json(&node, &query("peer-delete", &[("key", "iso1")]));
    assert_eq!(deleted["result"]["peerDeleted"], false, "{deleted}");
    matched(&node, &[("key", "iso1")]);

    // Named namespaces are isolated the same way.
    put(
        &node,
        &[("key", "iso3"), ("body", "named-app"), ("cache", "pages")],
    );
    let named_miss = get_json(
        &node,
        &query("peer-check", &[("key", "iso3"), ("cache", "pages")]),
    );
    assert_eq!(named_miss["result"]["peerHit"], false, "{named_miss}");
}

#[test]
fn cache_deployment_scope_isolates_other_deployments() {
    // A different fleet — a second node on its own bucket — never sees
    // the first deployment's entries, even in deployment scope.
    let (_dir_a, node_a) = boot_node(&[("CELLO_CACHE_SCOPE", "deployment")]);
    let (_dir_b, node_b) = boot_node(&[("CELLO_CACHE_SCOPE", "deployment")]);
    put(&node_a, &[("key", "fleet"), ("body", "fleet-a")]);
    matched(&node_a, &[("key", "fleet")]);
    missed(&node_b, &[("key", "fleet")]);
    put(&node_b, &[("key", "fleet"), ("body", "fleet-b")]);
    let kept = matched(&node_a, &[("key", "fleet")]);
    assert_eq!(kept["body"], "fleet-a", "{kept}");

    // A redeployed generation on the same node is a new deployment
    // identity with an empty scope: the old entries stay behind.
    let dir = tempfile::Builder::new()
        .prefix("cello-cache-scope-redeploy-")
        .tempdir()
        .expect("tempdir");
    let app_project = dir.path().join("app");
    let probe_src = dir.path().join("probe");
    copy_dir(&fixture_dir().join("app"), &app_project);
    copy_dir(&fixture_dir().join("probe"), &probe_src);
    preseed_probe(&app_project, &probe_src);
    let node = TestNode::boot(
        &app_project,
        &[
            ("CELLO_CACHE_SCOPE", "deployment"),
            ("CELLO_DEPLOY_POLL_S", "1"),
        ],
    );
    let marker = get_json(&node, "/marker");
    assert_eq!(marker["result"]["marker"], "v1", "{marker}");
    put(&node, &[("key", "gen"), ("body", "gen-one")]);
    matched(&node, &[("key", "gen")]);

    // Deploy the same app stamped v2; the pointer watcher adopts it as a
    // new deployment, and the marker proves the new generation serves.
    let app_v2 = dir.path().join("app-v2");
    copy_dir(&app_project, &app_v2);
    let worker = app_v2.join("worker.js");
    let src = std::fs::read_to_string(&worker).expect("read worker.js");
    let stamped = src
        .replace("const MARKER = 'v1';", "const MARKER = 'v2';")
        .to_owned();
    assert_ne!(src, stamped, "the v2 stamp must change worker.js");
    std::fs::write(&worker, stamped).expect("write worker.js");
    let bucket = open_bucket(&app_project);
    deploy_src(&bucket, &app_v2);
    poll_until("adopt v2", Duration::from_secs(180), || {
        let marker = get_json(&node, "/marker");
        assert_eq!(marker["ok"], true, "{marker}");
        (marker["result"]["marker"] == "v2").then(|| "v2".to_string())
    });

    // The new deployment starts with an empty scope.
    missed(&node, &[("key", "gen")]);
    // The cache itself works in the new generation, shared as before.
    put(&node, &[("key", "gen2"), ("body", "gen-two")]);
    matched(&node, &[("key", "gen2")]);
    let peer = get_json(&node, &query("peer-get", &[("key", "gen2")]));
    assert_eq!(peer["result"]["body"], "gen-two", "{peer}");
}

#[test]
fn cache_scope_rejects_garbage() {
    let dir = tempfile::Builder::new()
        .prefix("cello-cache-scope-garbage-")
        .tempdir()
        .expect("tempdir");
    let app_project = dir.path().join("app");
    let probe_src = dir.path().join("probe");
    copy_dir(&fixture_dir().join("app"), &app_project);
    copy_dir(&fixture_dir().join("probe"), &probe_src);
    preseed_probe(&app_project, &probe_src);

    let mut child = cello_command(
        &app_project,
        [("NO_COLOR", "1"), ("CELLO_CACHE_SCOPE", "zone")],
    )
    .arg("dev")
    .arg(&app_project)
    .arg("--port")
    .arg("0")
    .arg("--no-watch")
    .arg("--logs")
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .expect("spawn cello dev");
    let mut stdout = child.stdout.take().expect("piped stdout");
    std::thread::spawn(move || {
        let mut sink = Vec::new();
        let _ = stdout.read_to_end(&mut sink);
    });
    let mut stderr = child.stderr.take().expect("piped stderr");
    let errors = std::thread::spawn(move || {
        let mut sink = String::new();
        let _ = stderr.read_to_string(&mut sink);
        sink
    });
    // The node must exit without ever serving: it refuses the scope before
    // it binds, so no announce line ever reaches the supervisor's stderr.
    poll_until("refuse garbage scope", Duration::from_secs(120), || {
        child
            .try_wait()
            .expect("poll child")
            .map(|status| format!("{status}"))
    });
    let status = child.wait().expect("reap child");
    assert!(!status.success(), "garbage scope exited {status}");
    let errors = errors.join().expect("read stderr");
    assert!(
        !errors.contains("cello listening on "),
        "the node announced a listener before refusing the scope: {errors}"
    );
    assert!(
        errors.contains("CELLO_CACHE_SCOPE must be script or deployment"),
        "stderr names the bad scope: {errors}"
    );
}
