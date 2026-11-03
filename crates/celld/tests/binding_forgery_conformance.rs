// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): binding-forgery conformance. Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for binding forgery refusal: a Worker that reaches past its
//! declared bindings -- `new (env.DB.constructor)("other-db")` and the same
//! shape for every other binding type -- is refused at the host boundary,
//! while its own declared bindings keep working. The victim fixture owns one
//! resource of each type; the attacker fixture owns one of its own and tries
//! to forge the victim's. See FORK.md patch 0011.
//!
//! One `celld dev` boots the attacker fixture; the victim fixture is
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
        .join("tests/fixtures/binding-forgery")
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

/// Deploy the victim into the attacker project's dev bucket before `dev`
/// boots, so the node loads both scripts. `dev` deploys the attacker itself
/// and becomes primary (last write wins `deploy/current.json`); the
/// victim's named pointer stays for the service binding to resolve.
fn preseed_victim(attacker_project: &Path, victim_src: &Path) {
    let state = attacker_project.join(".celld/dev");
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
    fn boot(attacker_project: &Path) -> Self {
        Self::boot_with_env(attacker_project, true)
    }

    /// `with_test_hooks` boots with `CELLD_TEST_WAIT=1` (test-only globals
    /// like the plant hook defined) or without it (shipping shape, the
    /// plant hook absent).
    fn boot_with_env(attacker_project: &Path, with_test_hooks: bool) -> Self {
        let port = free_port();
        let mut command = Command::new(celld_binary());
        command
            .arg("dev")
            .arg(attacker_project)
            .arg("--port")
            .arg(port.to_string())
            .arg("--no-watch")
            .arg("--logs")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if with_test_hooks {
            command.env("CELLD_TEST_WAIT", "1");
        }
        let mut child = command.spawn().expect("spawn celld dev");
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
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("celld dev exited during boot: {status}");
            }
            if !probe(&public, "/seed-victim") {
                return None;
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
    let (status, body) =
        get(&node.public, path).unwrap_or_else(|error| panic!("public GET {path}: {error}"));
    assert_eq!(status, 200, "GET {path}: {body}");
    serde_json::from_str(&body).unwrap_or_else(|error| panic!("JSON body {path}: {error}"))
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

/// A forged binding attempt must throw, and the refusal must name the
/// policy (an undeclared resource), not merely fail to route.
fn assert_refused(reply: &Value, path: &str) {
    assert_eq!(reply["ok"], true, "{path}: {reply}");
    assert_denied(reply, path);
}

/// The same refusal inside a compound reply (no `ok` wrapper).
fn assert_denied(reply: &Value, path: &str) {
    assert_eq!(reply["threw"], true, "{path}: {reply}");
    let error = reply["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("not declared") || error.contains("not bound"),
        "{path}: refusal names the policy: {reply}"
    );
}

#[test]
fn binding_forgery_conformance() {
    let dir = tempfile::Builder::new()
        .prefix("celld-binding-forgery-")
        .tempdir()
        .expect("tempdir");
    let attacker_project = dir.path().join("attacker");
    let victim_src = dir.path().join("victim");
    copy_dir(&fixture_dir().join("attacker"), &attacker_project);
    copy_dir(&fixture_dir().join("victim"), &victim_src);
    preseed_victim(&attacker_project, &victim_src);
    let node = TestNode::boot(&attacker_project);

    // The victim seeds one row/key/secret/instance/object of each type.
    // Boot already probed this route, so a second call must stay
    // idempotent (CREATE TABLE IF, DELETE before INSERT, put-overwrite).
    let seed = get_json(&node, "/seed-victim");
    assert_eq!(seed["ok"], true, "{seed}");

    // D1: the forged database name is refused; the own database works.
    let own = get_json(&node, "/d1-own");
    assert_eq!(own["ok"], true, "{own}");
    assert_eq!(own["value"], "own-row", "{own}");
    let forged = get_json(&node, "/d1-forge");
    assert_refused(&forged, "/d1-forge");
    assert!(
        !forged.to_string().contains("victim-row"),
        "/d1-forge leaks nothing: {forged}"
    );

    // D1 RPC allowlist: the query method answers, migrate does not exist.
    let rpc = get_json(&node, "/d1-rpc-deny");
    assert_eq!(rpc["ok"], true, "{rpc}");
    assert_eq!(rpc["queryOk"], true, "{rpc}");
    assert_eq!(rpc["denied"]["threw"], true, "{rpc}");
    assert!(
        rpc["denied"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("is not a function"),
        "{rpc}"
    );

    // KV: the forged namespace id is refused; the own namespace works.
    let own = get_json(&node, "/kv-own");
    assert_eq!(own["ok"], true, "{own}");
    assert_eq!(own["value"], "own-kv-value", "{own}");
    let forged = get_json(&node, "/kv-forge");
    assert_refused(&forged, "/kv-forge");
    assert!(
        !forged.to_string().contains("victim-kv-value"),
        "/kv-forge leaks nothing: {forged}"
    );

    // KV RPC allowlist: the sweep method does not exist over RPC.
    let rpc = get_json(&node, "/kv-rpc-deny");
    assert_eq!(rpc["ok"], true, "{rpc}");
    assert_eq!(rpc["denied"]["threw"], true, "{rpc}");
    assert!(
        rpc["denied"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("is not a function"),
        "{rpc}"
    );

    // Queues: the forged queue name is refused; the own queue works.
    let own = get_json(&node, "/queue-own");
    assert_eq!(own["ok"], true, "{own}");
    assert_eq!(own["sent"], true, "{own}");
    let forged = get_json(&node, "/queue-forge");
    assert_refused(&forged, "/queue-forge");

    // Durable Objects: the forged namespace is refused through the id
    // op path and through the pure-JS id path; the own namespace works.
    let own = get_json(&node, "/do-own");
    assert_eq!(own["ok"], true, "{own}");
    assert_eq!(own["secret"], "own-do-secret", "{own}");
    let forged = get_json(&node, "/do-forge");
    assert_refused(&forged, "/do-forge");
    assert!(
        !forged.to_string().contains("victim-do-secret"),
        "/do-forge leaks nothing: {forged}"
    );
    let direct = get_json(&node, "/do-forge-direct");
    assert_refused(&direct, "/do-forge-direct");
    assert!(
        !direct.to_string().contains("victim-do-secret"),
        "/do-forge-direct leaks nothing: {direct}"
    );

    // Workflows: an undeclared workflow name and another script's
    // workflow class are refused; the own workflow works.
    let own = get_json(&node, "/wf-own");
    assert_eq!(own["ok"], true, "{own}");
    assert_eq!(own["id"], "own0", "{own}");
    let forged = get_json(&node, "/wf-forge");
    assert_refused(&forged, "/wf-forge");
    let xforge = get_json(&node, "/wf-xforge");
    assert_refused(&xforge, "/wf-xforge");

    // The reserved scheduler cell is not stub-addressable from a
    // Worker, even the deployment's own.
    let cron = get_json(&node, "/cron-forge");
    assert_refused(&cron, "/cron-forge");

    // Grant laundering: a forged stub gains nothing by crossing RPC or
    // storage. The lift refuses what the sender could not dispatch to,
    // with the serialization error naming the policy. Every shape here
    // leaked or reached the victim cell before the lift gate.
    for (path, leak) in [
        ("/launder-rpc-do", Some("victim-do-secret")),
        ("/launder-rpc-d1", Some("victim-row")),
        ("/launder-rpc-wf", None),
        ("/launder-rpc-cron", None),
        ("/launder-store-d1", Some("victim-row")),
        ("/launder-store-wf", None),
        ("/launder-store-do", Some("victim-do-secret")),
    ] {
        let reply = get_json(&node, path);
        assert_refused(&reply, path);
        assert_eq!(reply["name"], "DataCloneError", "{path}: {reply}");
        if let Some(leak) = leak {
            assert!(
                !reply.to_string().contains(leak),
                "{path} leaks nothing: {reply}"
            );
        }
    }

    // Cell storage: a Durable Object reaches its own storage but not a
    // forged SQL scope (the victim D1) or a forged KV scope (a put/get
    // round trip through the victim D1 cell, and the victim object).
    let own = get_json(&node, "/storage-own");
    assert_eq!(own["ok"], true, "{own}");
    assert_eq!(own["value"], "own-value", "{own}");
    let sql = get_json(&node, "/storage-forge-sql");
    assert_refused(&sql, "/storage-forge-sql");
    assert!(
        !sql.to_string().contains("victim-row"),
        "/storage-forge-sql leaks nothing: {sql}"
    );
    let kv = get_json(&node, "/storage-forge-do");
    assert_eq!(kv["ok"], true, "{kv}");
    assert_denied(&kv["d1kv"], "/storage-forge-do d1kv");
    assert!(
        !kv["d1kv"].to_string().contains("cross-cell-kv-write"),
        "/storage-forge-do writes nothing: {kv}"
    );
    assert_denied(&kv["victimDo"], "/storage-forge-do victimDo");
    assert!(
        !kv.to_string().contains("victim-do-secret"),
        "/storage-forge-do leaks nothing: {kv}"
    );

    // Legitimate transfers keep working: a stub the sender could
    // dispatch to crosses a service binding, cell RPC, and storage, and
    // the receiver uses it -- including for a class its own script never
    // declared, including after a store round trip on the receiver side.
    let pass = get_json(&node, "/stub-pass-svc");
    assert_eq!(pass["ok"], true, "{pass}");
    assert_eq!(pass["secret"], "stub-pass-secret", "{pass}");
    let pass = get_json(&node, "/stub-pass-rpc");
    assert_eq!(pass["ok"], true, "{pass}");
    assert_eq!(pass["secret"], "rpc-pass-secret", "{pass}");
    let pass = get_json(&node, "/stub-pass-stored");
    assert_eq!(pass["ok"], true, "{pass}");
    assert_eq!(pass["secret"], "stored-secret", "{pass}");
    let pass = get_json(&node, "/stub-pass-xstore");
    assert_eq!(pass["ok"], true, "{pass}");
    assert_eq!(pass["secret"], "xstore-secret", "{pass}");

    // Two scripts declaring the same database name share it by design.
    let shared = get_json(&node, "/d1-shared");
    assert_eq!(shared["ok"], true, "{shared}");
    assert_eq!(shared["value"], "shared-row", "{shared}");

    // R2 and service bindings expose no constructible binding class, so
    // there is no name to forge; the declared bindings keep working.
    let own = get_json(&node, "/r2-own");
    assert_eq!(own["ok"], true, "{own}");
    assert_eq!(own["value"], "own-r2-value", "{own}");
    let noforge = get_json(&node, "/r2-noforge");
    assert_eq!(noforge["ok"], true, "{noforge}");
    assert_eq!(noforge["ctor"], "Object", "{noforge}");
    let own = get_json(&node, "/svc-own");
    assert_eq!(own["ok"], true, "{own}");
    assert_eq!(own["pong"], true, "{own}");
    let noforge = get_json(&node, "/svc-noforge");
    assert_eq!(noforge["ok"], true, "{noforge}");
    assert_eq!(noforge["ctor"], "ServiceStub", "{noforge}");

    // A forged marker smuggled through a class instance beside a
    // legitimate stub grants nothing: the side-table-listed stub revives
    // and the smuggled shape stays inert data. Before the structural fix
    // this leaked the victim secret.
    let smuggle = get_json(&node, "/smuggle-store-instance");
    assert_eq!(smuggle["ok"], true, "{smuggle}");
    assert_eq!(smuggle["ownOk"], true, "{smuggle}");
    assert_ne!(smuggle["evilFetch"], "function", "{smuggle}");
    assert!(
        !smuggle.to_string().contains("victim-do-secret"),
        "/smuggle-store-instance leaks nothing: {smuggle}"
    );

    // A legitimate class instance beside a stub round-trips as data while
    // the stub stays usable.
    let ok = get_json(&node, "/store-instance-ok");
    assert_eq!(ok["ok"], true, "{ok}");
    assert_eq!(ok["secret"], "instance-ok-secret", "{ok}");
    assert_eq!(ok["a"], 1, "{ok}");
    assert_eq!(ok["b"], "x", "{ok}");

    // A hand-planted pre-gate envelope decodes as plain data and grants
    // nothing.
    let planted = get_json(&node, "/plant-v1-inert");
    assert_eq!(planted["ok"], true, "{planted}");
    assert_ne!(planted["evilFetch"], "function", "{planted}");
    assert!(
        !planted.to_string().contains("victim-do-secret"),
        "/plant-v1-inert leaks nothing: {planted}"
    );
    assert!(
        planted["keys"]
            .as_array()
            .is_some_and(|keys| keys.iter().any(|key| key == "__celld$do")),
        "/plant-v1-inert keeps marker keys as data: {planted}"
    );

    // The remaining stored-marker shapes: a lone plain marker stays data,
    // a plain marker beside a stub is refused at lift, and an Error
    // carrying marker properties arrives as an inert Error.
    let shapes = get_json(&node, "/store-shapes");
    assert_eq!(shapes["ok"], true, "{shapes}");
    assert!(
        shapes["naked"]["keys"]
            .as_array()
            .is_some_and(|keys| keys.iter().any(|key| key == "__celld$do")),
        "{shapes}"
    );
    assert_ne!(shapes["naked"]["hasFetch"], "function", "{shapes}");
    assert_eq!(shapes["plainco"]["threw"], true, "{shapes}");
    assert_eq!(shapes["plainco"]["name"], "DataCloneError", "{shapes}");
    assert_eq!(shapes["error"]["isError"], true, "{shapes}");
    assert_ne!(shapes["error"]["hasFetch"], "function", "{shapes}");
    assert_eq!(shapes["error"]["stubOk"], "function", "{shapes}");

    // The RPC-marker shapes: a class instance is refused by projection,
    // and Error/plain marker payloads arrive as inert data.
    let rpc = get_json(&node, "/rpc-shapes");
    assert_eq!(rpc["ok"], true, "{rpc}");
    assert_eq!(rpc["instance"]["threw"], true, "{rpc}");
    assert_eq!(rpc["instance"]["name"], "DataCloneError", "{rpc}");
    assert_eq!(rpc["error"]["isError"], true, "{rpc}");
    assert_ne!(rpc["error"]["hasFetch"], "function", "{rpc}");
    assert!(
        rpc["plain"]["keys"]
            .as_array()
            .is_some_and(|keys| keys.iter().any(|key| key == "__celld$do")),
        "{rpc}"
    );
    assert_ne!(rpc["plain"]["hasFetch"], "function", "{rpc}");

    // Raw SQL cannot reach the storage tables.
    let cfkv = get_json(&node, "/sql-cfkv");
    assert_eq!(cfkv["ok"], true, "{cfkv}");
    assert_eq!(cfkv["insert"]["threw"], true, "{cfkv}");
    assert_eq!(cfkv["select"]["threw"], true, "{cfkv}");

    // H1: push injection is neutralized (host-side table): the forged
    // entry never records, so `evil` stays inert data (no fetch, no
    // victim secret) while the direct dispatch below stays refused.
    let push = get_json(&node, "/poc-push-inject");
    assert_eq!(push["ok"], true, "{push}");
    assert_ne!(push["evilFetch"], "function", "{push}");
    assert_ne!(push["leaked"], "victim-do-secret", "{push}");
    assert_ne!(push["localLeaked"], "victim-do-secret", "{push}");

    // H2: WeakMap.get split is neutralized (host reads one brand value
    // for check and record): `m` revives as the own stub, never the
    // victim, so no victim secret leaks.
    let split = get_json(&node, "/poc-weakmap-split");
    assert_eq!(split["ok"], true, "{split}");
    assert_ne!(split["leaked"], "victim-do-secret", "{split}");

    // H3 storage: the plant hook's 0x01 tree flips to 0x02, but pre-
    // host-table rows are inert: no stub installs, no victim secret.
    let plant = get_json(&node, "/poc-plant-flip");
    assert_eq!(plant["ok"], true, "{plant}");
    assert_ne!(plant["sFetch"], "function", "{plant}");
    assert_ne!(plant["leaked"], "victim-do-secret", "{plant}");

    // H3 RPC: tags are written host-side, so the 1->3 flip never lands:
    // the callee receives bare data (no table), no victim stub installs.
    let flip = get_json(&node, "/poc-rpc-tagflip");
    assert_eq!(flip["ok"], true, "{flip}");
    assert_ne!(flip["result"]["sFetch"], "function", "{flip}");
    assert_ne!(flip["result"]["leaked"], "victim-do-secret", "{flip}");

    // H6: both scopes are non-writable (assignment throws) and the own
    // queue still drops on abort/reset paths (put works).
    let scope = get_json(&node, "/poc-scope-mutate");
    assert_eq!(scope["ok"], true, "{scope}");
    assert_eq!(scope["storageAssignThrew"], true, "{scope}");
    assert_eq!(scope["ctxAssignThrew"], true, "{scope}");
    assert_eq!(scope["putOk"], true, "{scope}");

    // Full pollution prelude: legit stored + RPC stub passing still
    // works, forged stored (H1-style) grants nothing, forged RPC still
    // throws the policy refusal.
    let pollution = get_json(&node, "/pollution-roundtrip");
    assert_eq!(pollution["ok"], true, "{pollution}");
    assert_eq!(
        pollution["stored"]["storedLegit"]["threw"], false,
        "{pollution}"
    );
    assert_eq!(
        pollution["stored"]["storedLegit"]["secret"], "pollution-secret",
        "{pollution}"
    );
    assert_eq!(pollution["stored"]["storedLegit"]["n"], 1, "{pollution}");
    assert_ne!(
        pollution["stored"]["storedForged"]["evilFetch"], "function",
        "{pollution}"
    );
    assert_ne!(
        pollution["stored"]["storedForged"]["leaked"], "victim-do-secret",
        "{pollution}"
    );
    assert_eq!(pollution["rpc"]["threw"], false, "{pollution}");
    assert_eq!(
        pollution["rpc"]["secret"], "pollution-rpc-secret",
        "{pollution}"
    );
    assert_eq!(pollution["forged"]["threw"], true, "{pollution}");
    let forged_name = pollution["forged"]["name"].as_str().unwrap_or_default();
    let forged_error = pollution["forged"]["error"].as_str().unwrap_or_default();
    assert_eq!(forged_name, "DataCloneError", "{pollution}");
    assert!(
        forged_error.contains("not bound"),
        "forged RPC keeps the policy refusal: {pollution}"
    );

    // I1: a `__proto__` table path refuses at lift and leaves the
    // shared prototype clean, while a legit nested stub round-trips.
    let proto = get_json(&node, "/proto-after");
    assert_eq!(proto["ok"], true, "{proto}");
    assert_eq!(proto["nestedSecret"], "proto-nested-secret", "{proto}");
    assert_eq!(proto["nestedN"], 7, "{proto}");
    assert!(
        proto["putThrew"].as_str().is_some(),
        "/proto-after refuses the put: {proto}"
    );
    assert_eq!(proto["putName"], "DataCloneError", "{proto}");
    assert_eq!(
        proto["protoKeys"].as_array().map(Vec::len),
        Some(0),
        "{proto}"
    );
    assert_eq!(proto["protoHasX"], false, "{proto}");
    assert_eq!(proto["freshProtoOk"], true, "{proto}");
    assert!(
        proto["ctorThrew"].as_str().is_some(),
        "/proto-after refuses constructor paths: {proto}"
    );
    assert_eq!(proto["ctorName"], "DataCloneError", "{proto}");
    assert_eq!(proto["ctorProtoOk"], true, "{proto}");

    // I2: the plant hook's 1->3 tag flip stays inert: no stub installs
    // and no grant mints for the locally-built stub either.
    let flip03 = get_json(&node, "/plant-flip03");
    assert_eq!(flip03["ok"], true, "{flip03}");
    assert_ne!(flip03["sFetch"], "function", "{flip03}");
    assert_ne!(flip03["leaked"], "victim-do-secret", "{flip03}");
    assert_ne!(flip03["localLeaked"], "victim-do-secret", "{flip03}");

    // I3: plain stored data revives as data under a smuggled shape
    // marker on the shared prototype, beside a working stub.
    let svc = get_json(&node, "/svc-proto");
    assert_eq!(svc["ok"], true, "{svc}");
    assert_eq!(svc["threw"], Value::Null, "{svc}");
    assert_eq!(svc["rootType"], "object", "{svc}");
    assert_eq!(svc["a"], 1, "{svc}");
    assert_eq!(svc["b"], "x", "{svc}");
    assert_eq!(svc["stubOk"], "function", "{svc}");

    // I4: the callee error message survives a polluted subarray on the
    // RPC error path.
    let err = get_json(&node, "/err-subarray");
    assert_eq!(err["ok"], true, "{err}");
    assert_eq!(err["threw"], true, "{err}");
    assert!(
        err["error"]
            .as_str()
            .unwrap_or_default()
            .contains("callee-boom"),
        "{err}"
    );

    // H4: the plant hook exists on this test-hooks node (the plant tests
    // above used it); a shipping node never defines it.
    let hook_here = get_json(&node, "/plant-hook-absent");
    assert_eq!(hook_here["ok"], true, "{hook_here}");
    assert_eq!(hook_here["plantType"], "function", "{hook_here}");
    drop(node);
    let shipping = TestNode::boot_with_env(&attacker_project, false);
    let absent = get_json(&shipping, "/plant-hook-absent");
    assert_eq!(absent["ok"], true, "{absent}");
    assert_eq!(absent["plantType"], "undefined", "{absent}");
}
