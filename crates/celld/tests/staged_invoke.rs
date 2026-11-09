// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): staged invoke; deploy --read-root
// option; announce-line discovery. Apache-2.0.

// A black-box runner, not node code: it boots the binary, stages versions
// into the node's own store, and polls TCP with wall-clock deadlines, so
// the execution boundary's injected clock/filesystem/async rules do not
// apply to it.
#![allow(clippy::disallowed_methods)]

//! Staged invoke: `POST /invoke` serves one operator request from staged
//! versions, while the public listener keeps serving the live ones.
//!
//! Each test owns a hermetic fleet: a temp project serving app-v1 plus a
//! helper-v1 deployed before boot (so the live graph resolves), a booted
//! `celld dev` on it, and staged v2 versions uploaded in-process through
//! the same `deploy::build`/`write_staged` the CLI calls. The operator
//! requests are real HTTP against the real binary, signed in-process with
//! the fleet secret from the node's own store — the same bytes
//! `celld invoke` would send. There are no fixed sleeps: every wait polls
//! an observable condition with a deadline, and the server guard kills
//! the child on every exit path.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One runtime for the whole binary: `asyncrt::set_host_handle` runs
/// exactly once per process, and the dev store's background tasks spawn
/// onto that handle. Blocking is serialized so no two tests drive the
/// runtime at once.
static SHARED_RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
static BLOCK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn rt() -> &'static tokio::runtime::Runtime {
    SHARED_RT.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        celld::asyncrt::set_host_handle(rt.handle().clone());
        rt
    })
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let _guard = BLOCK_LOCK.lock().expect("block lock");
    rt().block_on(future)
}

const TOKEN: &str = "fleet-test-token";
const WRONG_TOKEN: &str = "wrong-token";
/// The HTTP auth scheme. Kept apart from the token so the two never sit
/// beside each other in one literal.
const SCHEME: &str = "Bearer";

/// One `Authorization` header value, built at runtime from the scheme
/// and the token.
fn bearer(token: &str) -> String {
    format!("{SCHEME} {token}")
}
const APP: &str = "staged-invoke-app";
const HELPER: &str = "staged-invoke-helper";

fn celld_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_celld"))
}

fn fixture_src(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/staged-invoke")
        .join(name)
        .canonicalize()
        .expect("fixture exists")
}

fn copy_project(src: &str, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create project dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(fixture_src(src).join(file), dst.join(file)).expect("copy fixture");
    }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    listener.local_addr().expect("probe addr").port()
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

/// A booted `celld dev` and its addresses. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its ports.
struct TestNode {
    child: Child,
    public: String,
    internal: String,
}

impl TestNode {
    fn boot(project: &Path, name: &str, token: Option<&str>) -> Self {
        Self::boot_with_env(project, name, token, &[])
    }

    fn boot_with_env(
        project: &Path,
        name: &str,
        token: Option<&str>,
        env: &[(&str, &str)],
    ) -> Self {
        let port = free_port();
        // No `--clean`: the store below already holds helper-v1, deployed
        // before boot so the live graph resolves, and cleaning would wipe
        // it. The temp project is fresh per test, so nothing stale lurks.
        let mut command = Command::new(celld_binary());
        command
            .arg("dev")
            .env("NO_COLOR", "1")
            .arg(project)
            .arg("--port")
            .arg(port.to_string())
            .arg("--no-watch")
            .arg("--logs");
        if let Some(token) = token {
            command.env("CELLD_OPERATOR_TOKEN", token);
        }
        for (key, value) in env {
            command.env(key, value);
        }
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn celld dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        // The supervisor's stderr carries the node's announce lines, the
        // only channel to the internal listener now that the node is
        // non-dumpable (patch 0053).
        let announced_lines: Arc<Mutex<Vec<String>>> = Arc::default();
        forward_announce(stderr, Arc::clone(&announced_lines));
        let public = format!("127.0.0.1:{port}");
        // The kill-on-drop guard exists before the readiness poll, so a
        // boot timeout or an early child exit cannot leak the server.
        let mut node = Self {
            child,
            public: public.clone(),
            internal: String::new(),
        };
        // Readiness is the live app answering its version: the deploy is
        // adopted exactly then. The internal port comes from the node's own
        // announce line.
        node.internal = poll_until(&format!("{name} boot"), Duration::from_secs(180), || {
            if get_body(&public, "/version") != Some("app-v1".to_string()) {
                return None;
            }
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("{name}: celld dev exited during boot: {status}");
            }
            announced(&announced_lines, "celld internal listening on ")
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

/// The node's own fleet store, opened in-process. The tests stage versions
/// and read pointers, leases, and the peer secret through the same
/// library calls the CLIs use; serving assertions always go over HTTP.
struct FleetStore {
    bucket: celld::bucket::Bucket,
}

impl FleetStore {
    /// Prepare the store `celld dev` will open, with helper-v1 deployed:
    /// the live app resolves its service binding through the helper's
    /// named pointer from the first adoption.
    fn prepare(project: &Path, helper_src: &Path) -> (Self, String) {
        let state = project.join(".celld/dev");
        std::fs::create_dir_all(&state).expect("state dir");
        let database = state.join("objects.sqlite3");
        let bucket = celld::dev::open_local_bucket(&database).expect("open dev bucket");
        block_on(async {
            celld::fleet::validate_bucket(&bucket)
                .await
                .expect("validate bucket");
            bucket.probe_cas().await.expect("probe cas");
            celld::wake_format::ensure_ready(&bucket)
                .await
                .expect("ensure ready");
        });
        let store = Self { bucket };
        let built = Self::build(helper_src);
        let version = built.version.clone();
        block_on(async {
            celld::deploy::write(&store.bucket, &built)
                .await
                .expect("deploy helper v1");
        });
        (store, version)
    }

    fn build(project: &Path) -> celld::deploy::Built {
        celld::deploy::build(&celld::deploy::Options {
            config: Some(project.to_path_buf()),
            bucket: None,
            endpoint: None,
            region: None,
            dry_run: false,
            json: false,
            vars: celld::deploy::VarOverrides::None,
            local_images: true,
            sourcemap: None,
            no_activate: true,
            read_roots: Vec::new(),
        })
        .expect("build")
    }

    /// Upload one more staged version, like `celld deploy --no-activate`.
    fn stage(&self, project: &Path) -> String {
        let built = Self::build(project);
        let version = built.version.clone();
        block_on(async {
            celld::deploy::write_staged(&self.bucket, &built)
                .await
                .expect("stage version");
        });
        version
    }

    fn activate(&self, scripts: &[(&str, &str)], primary: &str) {
        let refs = scripts
            .iter()
            .map(|(script, version)| celld::deploy::VersionRef {
                script: script.to_string(),
                version: version.to_string(),
            })
            .collect::<Vec<_>>();
        block_on(async {
            celld::deploy::activate(&self.bucket, &refs, Some(primary))
                .await
                .expect("activate");
        });
    }

    fn live_version(&self, script: &str) -> String {
        block_on(async {
            let pointer = celld::fleet::read_current_pointer(&self.bucket)
                .await
                .expect("read pointer");
            if pointer
                .script_name
                .as_deref()
                .is_some_and(|primary| primary == script)
            {
                return pointer.version;
            }
            let key = format!("deploy/{script}/current.json");
            let (body, _) = self
                .bucket
                .get(&key)
                .await
                .expect("read named pointer")
                .expect("named pointer exists");
            let named: serde_json::Value =
                serde_json::from_slice(&body).expect("named pointer JSON");
            named
                .get("version")
                .and_then(serde_json::Value::as_str)
                .expect("named version")
                .to_string()
        })
    }

    /// The pointer bytes the poll watcher and `POST /reload` read.
    fn pointer_snapshot(&self) -> Vec<(String, Vec<u8>)> {
        block_on(async {
            let mut snapshot = Vec::new();
            for key in [
                "deploy/current.json".to_string(),
                format!("deploy/{APP}/current.json"),
                format!("deploy/{HELPER}/current.json"),
            ] {
                let body = self
                    .bucket
                    .get(&key)
                    .await
                    .expect("read pointer")
                    .map(|(body, _)| body.to_vec());
                snapshot.push((key, body.unwrap_or_default()));
            }
            snapshot
        })
    }

    fn delete_key(&self, key: &str) {
        block_on(async {
            self.bucket.delete(key).await.expect("delete key");
        });
    }

    fn node_ids(&self) -> Vec<String> {
        block_on(async {
            let mut nodes = Vec::new();
            for object in self.bucket.list("nodes/").await.expect("list leases") {
                let location: &str = object.location.as_ref();
                if let Some(node) = location
                    .strip_prefix("nodes/")
                    .and_then(|key| key.strip_suffix(".json"))
                {
                    nodes.push(node.to_string());
                }
            }
            nodes
        })
    }

    /// The fleet secret plus the node to address: the test signs exactly
    /// what `celld invoke` signs. Both appear once the node boots.
    fn signer(&self, name: &str) -> Signer {
        poll_until(&format!("{name} lease"), Duration::from_secs(120), || {
            let nodes = self.node_ids();
            let node = nodes.first()?.clone();
            let key = block_on(celld::peer_auth::load_existing(&self.bucket)).ok()?;
            let auth = celld::peer_auth::PeerAuth::new(key, "staged-invoke-test").ok()?;
            Some(Signer { node, auth })
        })
    }
}

struct Signer {
    node: String,
    auth: celld::peer_auth::PeerAuth,
}

impl Signer {
    /// The fleet-HMAC headers for one `POST /invoke`, exactly as the CLI
    /// signs them. Called once per request: every signature carries a
    /// fresh nonce.
    fn headers(&self, body: &[u8]) -> Vec<(String, String)> {
        self.headers_for("POST", "/invoke", body, &self.node)
    }

    fn headers_for(
        &self,
        method: &str,
        path: &str,
        body: &[u8],
        target: &str,
    ) -> Vec<(String, String)> {
        let signed = self
            .auth
            .signed_headers(method, path, body, target)
            .expect("sign");
        signed
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_string(),
                    value.to_str().expect("ASCII header").to_string(),
                )
            })
            .collect()
    }
}

fn b64(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn unb64(text: &str) -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD
        .decode(text)
        .expect("base64 body")
}

/// One `/invoke` envelope: the staged generation plus the inner request.
fn envelope(
    script: &str,
    version: &str,
    pins: &[(&str, &str)],
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> String {
    let pins: std::collections::BTreeMap<&str, &str> = pins.iter().copied().collect();
    serde_json::json!({
        "target": { "script": script, "version": version },
        "pins": pins,
        "request": {
            "method": method,
            "path": path,
            "headers": headers,
            "body": b64(body),
        },
    })
    .to_string()
}

/// A raw HTTP exchange over plain TCP with `Connection: close`, so the
/// runner cannot mask a protocol fault.
fn exchange(
    addr: &str,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
    timeout: Duration,
) -> (u16, String) {
    try_exchange(addr, method, path, headers, body, timeout)
        .unwrap_or_else(|error| panic!("{method} {path}: {error}"))
}

fn try_exchange(
    addr: &str,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    body: &[u8],
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
    wire.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    stream
        .write_all(wire.as_bytes())
        .map_err(|error| format!("write head: {error}"))?;
    stream
        .write_all(body)
        .map_err(|error| format!("write body: {error}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|error| format!("read reply: {error}"))?;
    let raw = String::from_utf8(raw).map_err(|error| format!("UTF-8 reply: {error}"))?;
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

fn get(addr: &str, path: &str) -> (u16, String) {
    exchange(addr, "GET", path, &[], &[], Duration::from_secs(60))
}

fn get_body(addr: &str, path: &str) -> Option<String> {
    // Readiness probe with a short timeout: a bound-but-not-ready node
    // holds connections open, so any failure here means "not yet".
    match try_exchange(addr, "GET", path, &[], &[], Duration::from_secs(2)) {
        Ok((200, body)) => Some(body),
        _ => None,
    }
}

fn post_json(addr: &str, path: &str, body: &str) -> (u16, String) {
    exchange(
        addr,
        "POST",
        path,
        &[("Content-Type".to_string(), "application/json".to_string())],
        body.as_bytes(),
        Duration::from_secs(60),
    )
}

/// One signed `POST /invoke`: the fleet HMAC plus, when the node runs
/// with a token, the operator bearer header. Fresh signature per call.
fn invoke(node: &TestNode, signer: &Signer, body: &str, auth: Option<String>) -> (u16, String) {
    let mut headers = signer.headers(body.as_bytes());
    if let Some(auth) = auth {
        headers.push(("Authorization".to_string(), auth));
    }
    exchange(
        &node.internal,
        "POST",
        "/invoke",
        &headers,
        body.as_bytes(),
        Duration::from_secs(120),
    )
}

/// The staged answer inside a 200 `/invoke` reply: status plus raw bytes.
fn invoked(body: &str) -> (u64, Vec<u8>) {
    let reply: serde_json::Value = serde_json::from_str(body).expect("invoke reply JSON");
    let result = reply.get("result").expect("invoke result");
    let status = result
        .get("status")
        .and_then(serde_json::Value::as_u64)
        .expect("result status");
    let body = result
        .get("body")
        .and_then(serde_json::Value::as_str)
        .expect("result body");
    (status, unb64(body))
}

fn error_of(body: &str) -> String {
    let reply: serde_json::Value = serde_json::from_str(body).expect("error reply JSON");
    reply
        .get("error")
        .and_then(serde_json::Value::as_str)
        .expect("error field")
        .to_string()
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

/// One hermetic fleet: the temp dir, the app-v1 project dev serves, the
/// staged sources, and the store with helper-v1 deployed.
struct Fleet {
    _temp: tempfile::TempDir,
    project: PathBuf,
    app_v2: PathBuf,
    helper_v2: PathBuf,
    store: FleetStore,
}

impl Fleet {
    fn new(name: &str) -> Self {
        let temp = tempfile::Builder::new()
            .prefix(&format!("celld-staged-invoke-{name}-"))
            .tempdir()
            .expect("tempdir");
        let project = temp.path().join("app");
        copy_project("app-v1", &project);
        let helper_src = temp.path().join("helper-v1");
        copy_project("helper-v1", &helper_src);
        let app_v2 = temp.path().join("app-v2");
        copy_project("app-v2", &app_v2);
        let helper_v2 = temp.path().join("helper-v2");
        copy_project("helper-v2", &helper_v2);
        let (store, _) = FleetStore::prepare(&project, &helper_src);
        Self {
            _temp: temp,
            project,
            app_v2,
            helper_v2,
            store,
        }
    }
}

#[test]
fn staged_version_answers_operator_path_live_serves_public() {
    let fleet = Fleet::new("serve");
    let node = TestNode::boot(&fleet.project, "serve", Some(TOKEN));
    let signer = fleet.store.signer("serve");
    let app_v2 = fleet.store.stage(&fleet.app_v2);
    // After boot and staging: dev's own deploy moved the fleet pointer,
    // and staging never does. From here only invokes run.
    let before = fleet.store.pointer_snapshot();

    let (status, body) = get(&node.public, "/version");
    assert_eq!((status, body.as_str()), (200, "app-v1"));

    let request = envelope(APP, &app_v2, &[], "GET", "/version", &[], &[]);
    let (status, body) = invoke(&node, &signer, &request, Some(bearer(TOKEN)));
    assert_eq!(status, 200, "invoke must answer 200, got {body}");
    let (inner, bytes) = invoked(&body);
    assert_eq!(inner, 200);
    assert_eq!(bytes, b"app-v2");

    let (status, body) = get(&node.public, "/version");
    assert_eq!((status, body.as_str()), (200, "app-v1"));

    assert_eq!(
        fleet.store.pointer_snapshot(),
        before,
        "an invoke must not move any pointer"
    );
}

#[test]
fn staged_and_live_share_real_bindings() {
    let fleet = Fleet::new("bindings");
    let node = TestNode::boot(&fleet.project, "bindings", Some(TOKEN));
    let signer = fleet.store.signer("bindings");
    let app_v2 = fleet.store.stage(&fleet.app_v2);

    // The staged version migrates; the live one reads the migrated rows.
    let request = envelope(APP, &app_v2, &[], "POST", "/migrate", &[], &[]);
    let (status, body) = invoke(&node, &signer, &request, Some(bearer(TOKEN)));
    assert_eq!(status, 200, "invoke must answer 200, got {body}");
    let (inner, bytes) = invoked(&body);
    assert_eq!(inner, 200);
    let migrated: serde_json::Value = serde_json::from_slice(&bytes).expect("migrate JSON");
    assert_eq!(
        migrated.get("migrated").and_then(serde_json::Value::as_str),
        Some("app-v2")
    );
    let (status, body) = get(&node.public, "/read");
    assert_eq!(status, 200);
    let read: serde_json::Value = serde_json::from_str(&body).expect("read JSON");
    assert_eq!(
        read.get("reader").and_then(serde_json::Value::as_str),
        Some("app-v1"),
        "the live version still serves publicly"
    );
    let rows = read
        .get("rows")
        .and_then(serde_json::Value::as_array)
        .expect("rows");
    assert!(
        rows.iter()
            .any(
                |row| row.get("tag").and_then(serde_json::Value::as_str) == Some("migrated")
                    && row.get("writer").and_then(serde_json::Value::as_str) == Some("app-v2")
            ),
        "live read must show the staged migration: {rows:?}"
    );

    // And the staged version reads what the live one wrote.
    let (status, _) = post_json(
        &node.public,
        "/note",
        r#"{"tag":"live-note","writer":"live"}"#,
    );
    assert_eq!(status, 200);
    let request = envelope(APP, &app_v2, &[], "GET", "/read", &[], &[]);
    let (status, body) = invoke(&node, &signer, &request, Some(bearer(TOKEN)));
    assert_eq!(status, 200, "invoke must answer 200, got {body}");
    let (inner, bytes) = invoked(&body);
    assert_eq!(inner, 200);
    let read: serde_json::Value = serde_json::from_slice(&bytes).expect("staged read JSON");
    let rows = read
        .get("rows")
        .and_then(serde_json::Value::as_array)
        .expect("rows");
    assert!(
        rows.iter()
            .any(|row| row.get("tag").and_then(serde_json::Value::as_str) == Some("live-note")),
        "staged read must show the live write: {rows:?}"
    );
}

#[test]
fn service_bindings_resolve_staged_generation() {
    let fleet = Fleet::new("services");
    let node = TestNode::boot(&fleet.project, "services", Some(TOKEN));
    let signer = fleet.store.signer("services");
    let app_v2 = fleet.store.stage(&fleet.app_v2);
    let helper_v2 = fleet.store.stage(&fleet.helper_v2);

    // Both pins staged: the staged app reaches the staged helper.
    let request = envelope(
        APP,
        &app_v2,
        &[(HELPER, helper_v2.as_str())],
        "GET",
        "/via-helper",
        &[],
        &[],
    );
    let (status, body) = invoke(&node, &signer, &request, Some(bearer(TOKEN)));
    assert_eq!(status, 200, "invoke must answer 200, got {body}");
    let (inner, bytes) = invoked(&body);
    assert_eq!(
        (inner, bytes.as_slice()),
        (200, b"via-helper-v2".as_slice())
    );

    // The helper unpinned: carried from the live generation, as an
    // activation of the same pins would carry it.
    let request = envelope(APP, &app_v2, &[], "GET", "/via-helper", &[], &[]);
    let (status, body) = invoke(&node, &signer, &request, Some(bearer(TOKEN)));
    assert_eq!(status, 200, "invoke must answer 200, got {body}");
    let (inner, bytes) = invoked(&body);
    assert_eq!(
        (inner, bytes.as_slice()),
        (200, b"via-helper-v1".as_slice())
    );

    // Under an activated map the carried script resolves from the map,
    // not the named pointer: delete the named pointer and the invoke
    // still answers from the map entry.
    let app_v1 = fleet.store.live_version(APP);
    let helper_v1 = fleet.store.live_version(HELPER);
    fleet
        .store
        .activate(&[(APP, &app_v1), (HELPER, &helper_v1)], APP);
    let (status, _) = exchange(
        &node.internal,
        "POST",
        "/reload",
        &[("Authorization".to_string(), bearer(TOKEN))],
        &[],
        Duration::from_secs(120),
    );
    assert_eq!(status, 200, "reload must adopt the map");
    let (status, body) = get(&node.public, "/via-helper");
    assert_eq!((status, body.as_str()), (200, "via-helper-v1"));
    fleet
        .store
        .delete_key(&format!("deploy/{HELPER}/current.json"));
    let request = envelope(APP, &app_v2, &[], "GET", "/via-helper", &[], &[]);
    let (status, body) = invoke(&node, &signer, &request, Some(bearer(TOKEN)));
    assert_eq!(
        status, 200,
        "map-carried invoke must answer 200, got {body}"
    );
    let (inner, bytes) = invoked(&body);
    assert_eq!(
        (inner, bytes.as_slice()),
        (200, b"via-helper-v1".as_slice())
    );
}

#[test]
fn waituntil_runs_staged_past_reply() {
    let fleet = Fleet::new("waituntil");
    let node = TestNode::boot(&fleet.project, "waituntil", Some(TOKEN));
    let signer = fleet.store.signer("waituntil");
    let app_v2 = fleet.store.stage(&fleet.app_v2);
    let helper_v2 = fleet.store.stage(&fleet.helper_v2);

    // The handler answers at once and the helper write lands in
    // `waitUntil`: the invoke returns only after it settles, still on
    // the staged graph — a reply-time retire would resolve the live
    // helper instead.
    let request = envelope(
        APP,
        &app_v2,
        &[(HELPER, helper_v2.as_str())],
        "POST",
        "/migrate-late",
        &[],
        &[],
    );
    let (status, body) = invoke(&node, &signer, &request, Some(bearer(TOKEN)));
    assert_eq!(status, 200, "invoke must answer 200, got {body}");
    let (inner, bytes) = invoked(&body);
    assert_eq!(inner, 200);
    assert_eq!(bytes, b"late-accepted-app-v2");

    let (status, body) = get(&node.public, "/read");
    assert_eq!(status, 200);
    let read: serde_json::Value = serde_json::from_str(&body).expect("read JSON");
    let rows = read
        .get("rows")
        .and_then(serde_json::Value::as_array)
        .expect("rows");
    assert!(
        rows.iter().any(
            |row| row.get("tag").and_then(serde_json::Value::as_str) == Some("late")
                && row.get("writer").and_then(serde_json::Value::as_str) == Some("helper-v2")
        ),
        "the staged waitUntil write must be visible when the invoke returns: {rows:?}"
    );
}

#[test]
fn invoke_refuses_unauthenticated_and_forged() {
    let fleet = Fleet::new("auth");
    let node = TestNode::boot(&fleet.project, "auth", Some(TOKEN));
    let signer = fleet.store.signer("auth");
    let body = "not an envelope";

    let (status, _) = exchange(
        &node.internal,
        "POST",
        "/invoke",
        &[],
        body.as_bytes(),
        Duration::from_secs(60),
    );
    assert_eq!(status, 401, "no token must be 401");
    let (status, _) = exchange(
        &node.internal,
        "POST",
        "/invoke",
        &[("Authorization".to_string(), bearer(WRONG_TOKEN))],
        body.as_bytes(),
        Duration::from_secs(60),
    );
    assert_eq!(status, 401, "a wrong token must be 401");

    // The token alone is not enough: every call carries the fleet HMAC.
    let (status, reply) = exchange(
        &node.internal,
        "POST",
        "/invoke",
        &[("Authorization".to_string(), bearer(TOKEN))],
        body.as_bytes(),
        Duration::from_secs(60),
    );
    assert_eq!(status, 401, "an unsigned call must be 401, got {reply}");

    // A signature for another node is not replayable here.
    let mut headers = signer.headers_for("POST", "/invoke", body.as_bytes(), "some-other-node");
    headers.push(("Authorization".to_string(), bearer(TOKEN)));
    let (status, _) = exchange(
        &node.internal,
        "POST",
        "/invoke",
        &headers,
        body.as_bytes(),
        Duration::from_secs(60),
    );
    assert_eq!(status, 409, "a wrong-target signature must be 409");

    // A signature is single-use: the identical bytes sent twice replay.
    let mut headers = signer.headers(body.as_bytes());
    headers.push(("Authorization".to_string(), bearer(TOKEN)));
    let (first, _) = exchange(
        &node.internal,
        "POST",
        "/invoke",
        &headers,
        body.as_bytes(),
        Duration::from_secs(60),
    );
    assert_eq!(first, 400, "the garbage envelope must fail validation");
    let (status, _) = exchange(
        &node.internal,
        "POST",
        "/invoke",
        &headers,
        body.as_bytes(),
        Duration::from_secs(60),
    );
    assert_eq!(status, 409, "a replayed signature must be 409");

    // Only POST invokes.
    let mut headers = signer.headers_for("GET", "/invoke", &[], &signer.node);
    headers.push(("Authorization".to_string(), bearer(TOKEN)));
    let (status, _) = exchange(
        &node.internal,
        "GET",
        "/invoke",
        &headers,
        &[],
        Duration::from_secs(60),
    );
    assert_eq!(status, 405, "GET /invoke must be 405");
}

#[test]
fn invoke_refuses_unknown_and_malformed() {
    let fleet = Fleet::new("shapes");
    let node = TestNode::boot(&fleet.project, "shapes", Some(TOKEN));
    let signer = fleet.store.signer("shapes");

    // Well-formed but never staged: the bucket has no such version.
    let request = envelope(APP, "0123456789abcdef", &[], "GET", "/version", &[], &[]);
    let (status, body) = invoke(&node, &signer, &request, Some(bearer(TOKEN)));
    assert_eq!(status, 404, "an unknown version must be 404, got {body}");
    assert!(
        error_of(&body).contains("0123456789abcdef"),
        "the refusal names the version: {body}"
    );
    let app_v2 = fleet.store.stage(&fleet.app_v2);
    let request = envelope(
        APP,
        &app_v2,
        &[("staged-invoke-helper", "0123456789abcdef")],
        "GET",
        "/version",
        &[],
        &[],
    );
    let (status, body) = invoke(&node, &signer, &request, Some(bearer(TOKEN)));
    assert_eq!(status, 404, "an unknown pin must be 404, got {body}");

    // Malformed shapes never reach the bucket.
    let staged = envelope(APP, &app_v2, &[], "GET", "/version", &[], &[]);
    let cases: Vec<(&str, String)> = vec![
        ("not JSON", "{".to_string()),
        (
            "missing target",
            serde_json::json!({
                "request": { "method": "GET", "path": "/" },
            })
            .to_string(),
        ),
        (
            "unknown field",
            serde_json::json!({
                "target": { "script": APP, "version": app_v2 },
                "request": { "method": "GET", "path": "/" },
                "debug": true,
            })
            .to_string(),
        ),
        (
            "uppercase script",
            envelope("App", &app_v2, &[], "GET", "/version", &[], &[]),
        ),
        (
            "empty script",
            envelope("", &app_v2, &[], "GET", "/version", &[], &[]),
        ),
        (
            "short version",
            envelope(APP, "0123456789abcde", &[], "GET", "/version", &[], &[]),
        ),
        (
            "long version",
            envelope(APP, "0123456789abcdef0", &[], "GET", "/version", &[], &[]),
        ),
        (
            "uppercase version",
            envelope(APP, "0123456789ABCDEF", &[], "GET", "/version", &[], &[]),
        ),
        (
            "non-hex version",
            envelope(APP, "0123456789abcdeg", &[], "GET", "/version", &[], &[]),
        ),
        (
            "pin naming the target",
            envelope(
                APP,
                &app_v2,
                &[(APP, "0123456789abcdef")],
                "GET",
                "/version",
                &[],
                &[],
            ),
        ),
        (
            "bad pin version",
            envelope(
                APP,
                &app_v2,
                &[(HELPER, "xyz")],
                "GET",
                "/version",
                &[],
                &[],
            ),
        ),
        (
            "bad method",
            envelope(APP, &app_v2, &[], "GE T", "/version", &[], &[]),
        ),
        (
            "absolute-form path",
            envelope(APP, &app_v2, &[], "GET", "http://x/", &[], &[]),
        ),
        (
            "path with a space",
            envelope(APP, &app_v2, &[], "GET", "/ver sion", &[], &[]),
        ),
        (
            "bad header name",
            envelope(APP, &app_v2, &[], "GET", "/version", &[("a b", "c")], &[]),
        ),
        (
            "bad header value",
            envelope(APP, &app_v2, &[], "GET", "/version", &[("a", "b\nc")], &[]),
        ),
        (
            "control-plane header",
            envelope(
                APP,
                &app_v2,
                &[],
                "GET",
                "/version",
                &[("x-cells-operator", "x")],
                &[],
            ),
        ),
        (
            "bad base64 body",
            serde_json::json!({
                "target": { "script": APP, "version": app_v2 },
                "request": { "method": "POST", "path": "/", "body": "!!!" },
            })
            .to_string(),
        ),
    ];
    for (name, body) in cases {
        let (status, reply) = invoke(&node, &signer, &body, Some(bearer(TOKEN)));
        assert_eq!(status, 400, "{name} must be 400, got {reply}");
    }
    // The staged version still answers after the refusals.
    let (status, body) = invoke(&node, &signer, &staged, Some(bearer(TOKEN)));
    assert_eq!(status, 200, "invoke must answer 200, got {body}");
}

#[test]
fn public_requests_never_reach_staged() {
    let fleet = Fleet::new("public");
    let node = TestNode::boot(&fleet.project, "public", Some(TOKEN));
    let signer = fleet.store.signer("public");
    let app_v2 = fleet.store.stage(&fleet.app_v2);

    // The public listener has no operator route: `/invoke` is a Worker
    // path like any other, and the live worker answers its own 404.
    for method in ["GET", "POST"] {
        let (status, body) = exchange(
            &node.public,
            method,
            "/invoke",
            &[],
            &[],
            Duration::from_secs(60),
        );
        assert_eq!(status, 404, "{method} /invoke must hit the live worker");
        assert_eq!(body, "no such route");
    }
    let (status, body) = exchange(
        &node.public,
        "GET",
        "/version",
        &[("X-Staged-Version".to_string(), app_v2.clone())],
        &[],
        Duration::from_secs(60),
    );
    assert_eq!((status, body.as_str()), (200, "app-v1"));

    // Sanity: the staged version is really there, on the operator path.
    let request = envelope(APP, &app_v2, &[], "GET", "/version", &[], &[]);
    let (status, body) = invoke(&node, &signer, &request, Some(bearer(TOKEN)));
    assert_eq!(status, 200, "invoke must answer 200, got {body}");
    let (_, bytes) = invoked(&body);
    assert_eq!(bytes, b"app-v2");
}

#[test]
fn hung_background_work_fails_the_invoke() {
    let fleet = Fleet::new("settle");
    // A five-second handler budget makes the settle deadline one too:
    // the invoke must fail promptly, not pin the shadow forever.
    let node = TestNode::boot_with_env(
        &fleet.project,
        "settle",
        Some(TOKEN),
        &[("CELLD_HANDLER_BUDGET_S", "5")],
    );
    let signer = fleet.store.signer("settle");
    let app_v2 = fleet.store.stage(&fleet.app_v2);

    let request = envelope(APP, &app_v2, &[], "GET", "/hang", &[], &[]);
    let (status, body) = invoke(&node, &signer, &request, Some(bearer(TOKEN)));
    assert_eq!(status, 500, "a hung waitUntil must fail, got {body}");
    assert!(
        error_of(&body).contains("settle"),
        "the failure names the settle deadline: {body}"
    );

    // The node is healthy after: the hung work was cancelled and the
    // shadow retired, so live traffic never noticed.
    let (status, body) = get(&node.public, "/version");
    assert_eq!((status, body.as_str()), (200, "app-v1"));
}

#[test]
fn invoke_without_token_still_requires_fleet_signature() {
    let fleet = Fleet::new("notoken");
    let node = TestNode::boot(&fleet.project, "notoken", None);
    let signer = fleet.store.signer("notoken");
    let app_v2 = fleet.store.stage(&fleet.app_v2);

    // No token configured: the listener is open, but the route still
    // demands the fleet signature a tenant cannot mint.
    let request = envelope(APP, &app_v2, &[], "GET", "/version", &[], &[]);
    let (status, _) = exchange(
        &node.internal,
        "POST",
        "/invoke",
        &[],
        request.as_bytes(),
        Duration::from_secs(60),
    );
    assert_eq!(status, 401, "an unsigned call must be 401 without a token");
    let (status, body) = invoke(&node, &signer, &request, None);
    assert_eq!(
        status, 200,
        "a signed call must work without a token, got {body}"
    );
    let (_, bytes) = invoked(&body);
    assert_eq!(bytes, b"app-v2");
}
