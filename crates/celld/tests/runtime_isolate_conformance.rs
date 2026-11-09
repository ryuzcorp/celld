// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): shared runtime cells run in a dedicated runtime isolate pool; Queue/KV operator fetch gate refusals; deploy --read-root option; announce-line discovery.
// Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for runtime-cell isolation: the first script's module top
//! level overrides the shared primordials the runtime cells' gates and data
//! paths use (WeakSet/Set/Map/Array/Function.prototype methods, JSON,
//! String.prototype), and the second script's D1/KV/Queue operations and the
//! operator gates must behave exactly as without that pollution. The shared
//! cells run in a runtime-only pool whose isolates evaluate no tenant
//! bundle, so the hosting script's top level cannot reach them.
//!
//! One `celld dev` boots the host fixture as primary; the victim fixture is
//! pre-seeded into the dev bucket via the library (the same `build` + `write`
//! `dev` itself runs), so the node loads both scripts and the service binding
//! resolves cross-isolate. The host declares the same D1/KV/Queue bindings,
//! so it registers the shared classes first — the placement this suite pins
//! away from any tenant pool. There are no fixed sleeps: every wait polls an
//! observable condition with a deadline, and the server guard kills the child
//! on every exit path.

use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
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
        .join("tests/fixtures/runtime-isolate")
        .canonicalize()
        .expect("fixture exists")
}

/// One address of a `celld dev` node, from the announce line the supervisor
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

/// Tempdir for one fixture copy: `/dev/shm` when usable, else the
/// default tempdir.
///
/// Each booted `celld dev` keeps its SQLite store beside its fixture
/// copy, and the node self-fences when a lease commit stalls past the
/// TTL bound. Parallel suites on a loaded disk stall commits for
/// seconds, fencing healthy nodes and failing the run with
/// refused/reset/empty reads. Tmpfs has no fsync/writeback latency, so
/// the stores stop sharing the box's disk queue; the suites test RPC
/// behavior, not store durability (the fence path has its own tests), so
/// coverage is unchanged. Where `/dev/shm` is missing or unwritable the
/// default tempdir applies, as before.
fn fixture_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    match builder.tempdir_in("/dev/shm") {
        Ok(dir) => dir,
        Err(error) => {
            eprintln!("fixture tempdir fell back to disk (/dev/shm unusable: {error})");
            builder.tempdir().expect("fixture tempdir")
        }
    }
}

/// Deploy the victim into the host project's dev bucket before `dev`
/// boots, so the node loads both scripts. `dev` deploys the host itself
/// and becomes primary (last write wins `deploy/current.json`); the
/// victim's named pointer stays for the service binding to resolve.
fn preseed_victim(app_project: &Path, victim_src: &Path) {
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
            config: Some(victim_src.to_path_buf()),
            bucket: None,
            endpoint: None,
            region: None,
            dry_run: false,
            json: false,
            vars: celld::deploy::VarOverrides::None,
            local_images: true,
            sourcemap: None,
            no_activate: false,
            read_roots: Vec::new(),
        })
        .expect("build victim");
        celld::deploy::write(&bucket, &built)
            .await
            .expect("write victim");
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
        // Port zero: the OS assigns the worker port, so parallel boots
        // can never claim the same one; discovery below learns it.
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
        // Drain both pipes so a chatty child can never block on a full one.
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
        // loads. The node is non-dumpable (patch 0053), so the supervisor's
        // stderr is the discovery channel.
        node.public = poll_until("boot", Duration::from_secs(120), || {
            // Liveness first: a dead child must fail fast, not burn
            // the deadline on refused probes.
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
fn runtime_isolate_conformance() {
    let dir = fixture_tempdir("celld-runtime-isolate-");
    let app_project = dir.path().join("app");
    let victim_src = dir.path().join("victim");
    copy_dir(&fixture_dir().join("app"), &app_project);
    copy_dir(&fixture_dir().join("victim"), &victim_src);
    preseed_victim(&app_project, &victim_src);
    let node = TestNode::boot(&app_project);

    // The host's own fetch path survives its own pollution: without the
    // forwarding below there is no observation at all.
    let health = get_json(&node, "/health");
    assert_eq!(health["ok"], true, "{health}");

    // Seed rows so a refusal cannot be mistaken for an empty database.
    let seed = get_json(&node, "/v/seed");
    assert_eq!(seed["ok"], true, "{seed}");

    // The binding's own surface keeps working under the host's pollution.
    let binding = get_json(&node, "/v/d1-binding-ok");
    assert_eq!(binding["ok"], true, "{binding}");
    assert_eq!(binding["result"]["first"], "one", "{binding}");
    assert_eq!(
        binding["result"]["batchCounts"],
        serde_json::json!([1, 1]),
        "{binding}"
    );

    // A storage capability only the victim declares still applies to
    // the shared D1 cell it uses: the runtime pool opens the cell with
    // the union of every declaring script's switches, not the primary's.
    let vec_query = get_json(&node, "/v/d1-vec-query");
    assert_eq!(vec_query["ok"], true, "{vec_query}");
    assert!(
        vec_query["result"]["version"]
            .as_str()
            .unwrap_or_default()
            .starts_with('v'),
        "{vec_query}"
    );
    assert_eq!(vec_query["result"]["top"], 1, "{vec_query}");

    // The open fetch query branch keeps working, with and without a
    // top-level marker the polluted JSON.parse would throw on.
    let query = get_json(&node, "/v/d1-fetch-query");
    assert_eq!(query["ok"], true, "{query}");
    assert_eq!(query["result"]["status"], 200, "{query}");
    assert_eq!(
        query["result"]["rows"],
        serde_json::json!([["one"]]),
        "{query}"
    );
    let marked = get_json(&node, "/v/d1-fetch-query-top-marker");
    assert_eq!(marked["ok"], true, "{marked}");
    assert_eq!(marked["result"]["status"], 200, "{marked}");
    assert_eq!(
        marked["result"]["rows"],
        serde_json::json!([["one"]]),
        "{marked}"
    );

    // A marked statement still runs: the polluted Array.prototype.map
    // would have dropped it to an empty batch.
    let nested = get_json(&node, "/v/d1-fetch-query-nested-marker");
    assert_eq!(nested["ok"], true, "{nested}");
    assert_eq!(nested["result"]["status"], 200, "{nested}");
    assert_eq!(nested["result"]["count"], 1, "{nested}");

    // The D1 operator gates refuse: export, migrate, and exec-with-rows
    // over stub.fetch stay 403, and the RPC allowlist still refuses the
    // operator methods as not-a-function.
    let fetched = get_json(&node, "/v/d1-fetch-export");
    assert_eq!(fetched["ok"], true, "{fetched}");
    assert_eq!(fetched["result"]["status"], 403, "{fetched}");
    assert!(
        fetched["result"]["body"]
            .as_str()
            .unwrap_or_default()
            .contains("operator"),
        "{fetched}"
    );
    let migrate_fetch = get_json(&node, "/v/d1-fetch-migrate");
    assert_eq!(migrate_fetch["ok"], true, "{migrate_fetch}");
    assert_eq!(migrate_fetch["result"]["status"], 403, "{migrate_fetch}");
    let exec_rows = get_json(&node, "/v/d1-fetch-exec-rows");
    assert_eq!(exec_rows["ok"], true, "{exec_rows}");
    assert_eq!(exec_rows["result"]["status"], 403, "{exec_rows}");
    let rpc = get_json(&node, "/v/d1-rpc-export");
    assert_eq!(rpc["ok"], true, "{rpc}");
    assert_eq!(rpc["result"]["threw"], true, "{rpc}");
    assert!(
        rpc["result"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("is not a function"),
        "{rpc}"
    );
    let migrate_rpc = get_json(&node, "/v/d1-rpc-migrate");
    assert_eq!(migrate_rpc["ok"], true, "{migrate_rpc}");
    assert_eq!(migrate_rpc["result"]["threw"], true, "{migrate_rpc}");

    // A marked RPC call is not redirected: the polluted
    // Function.prototype.apply would have answered an export dump.
    let redirected = get_json(&node, "/v/d1-rpc-apply");
    assert_eq!(redirected["ok"], true, "{redirected}");
    assert_eq!(redirected["result"]["threw"], true, "{redirected}");

    // The KV binding keeps working, but the fetch surface is the
    // operator's: stub.fetch get/put refuse without the host-set marker,
    // with and without a top-level marker. The host's WeakSet override
    // reads every Request as operator, so these 403s pin the runtime
    // pool as well as the gate.
    let kv = get_json(&node, "/v/kv-binding-ok");
    assert_eq!(kv["ok"], true, "{kv}");
    assert_eq!(kv["result"]["got"], "v1", "{kv}");
    assert_eq!(kv["result"]["gone"], Value::Null, "{kv}");
    let kv_get = get_json(&node, "/v/kv-fetch-get");
    assert_eq!(kv_get["ok"], true, "{kv_get}");
    assert_eq!(kv_get["result"]["status"], 403, "{kv_get}");
    assert!(
        kv_get["result"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("operator"),
        "{kv_get}"
    );
    let kv_marked = get_json(&node, "/v/kv-fetch-get-marker");
    assert_eq!(kv_marked["ok"], true, "{kv_marked}");
    assert_eq!(kv_marked["result"]["status"], 403, "{kv_marked}");
    assert!(
        kv_marked["result"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("operator"),
        "{kv_marked}"
    );
    let kv_put = get_json(&node, "/v/kv-fetch-put-marker");
    assert_eq!(kv_put["ok"], true, "{kv_put}");
    assert_eq!(kv_put["result"]["status"], 403, "{kv_put}");
    assert!(
        kv_put["result"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("operator"),
        "{kv_put}"
    );
    // The refused put wrote nothing.
    let kv_k2 = get_json(&node, "/v/kv-get-k2");
    assert_eq!(kv_k2["ok"], true, "{kv_k2}");
    assert_eq!(kv_k2["result"]["value"], Value::Null, "{kv_k2}");

    // Queue producers send, but the info surface is the operator's:
    // stub.fetch info refuses without the marker, with and without a
    // top-level marker.
    let sent = get_json(&node, "/v/qsend");
    assert_eq!(sent["ok"], true, "{sent}");
    let info = get_json(&node, "/v/qinfo");
    assert_eq!(info["ok"], true, "{info}");
    assert_eq!(info["result"]["status"], 403, "{info}");
    assert!(
        info["result"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("operator"),
        "{info}"
    );
    let info_marked = get_json(&node, "/v/qinfo-marker");
    assert_eq!(info_marked["ok"], true, "{info_marked}");
    assert_eq!(info_marked["result"]["status"], 403, "{info_marked}");
    assert!(
        info_marked["result"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("operator"),
        "{info_marked}"
    );

    // Every sent message is delivered exactly once by id: the polluted
    // JSON.stringify would have failed the dispatch serialization, so a
    // shared queue cell beside the host's bundle never delivers.
    poll_until("queue delivery", Duration::from_secs(90), || {
        let consumed: Value =
            get(&node.public, "/v/qconsumed")
                .ok()
                .and_then(|(status, body)| {
                    (status == 200).then(|| serde_json::from_str(&body).ok())?
                })?;
        (consumed["ok"] == true && consumed["result"]["count"] == 3).then(|| consumed.to_string())
    });

    // The node is still up after the whole battery.
    let health = get_json(&node, "/health");
    assert_eq!(health["ok"], true, "{health}");
}
