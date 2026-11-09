// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): cross-isolate RPC
// stubs and streams; deploy --sourcemap option; staged activation; deploy
// --read-root option; announce-line discovery. Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for stub methods returning promises minted by an earlier
//! native op: two scripts in one fleet, the app fetches the controller's
//! Durable Object stub over a service binding, a DO method returns an
//! object holding a nested capability, and the app awaits a method that
//! returns the retained promise. The minting event retires with its
//! reply; the promise must still deliver in the later event that awaits
//! it, whether the wait resolved before the call or after it.
//!
//! One `celld dev` boots the app fixture; the controller fixture is
//! pre-seeded into the dev bucket via the library (the same `build` + `write`
//! `dev` itself runs), so the node loads both scripts and the service binding
//! resolves cross-isolate. The node boots with `CELLD_TEST_WAIT=1`, which
//! defines the test-only retained-wait hook the fixture drives. There are
//! no fixed sleeps: every wait polls an observable condition with a
//! deadline, and the server guard kills the child on every exit path.

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
        .join("tests/fixtures/svc-stub-wait")
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
/// seconds (observed: a 700-byte commit taking 4.4 s), fencing healthy
/// nodes and failing the run with refused/reset/empty reads. Tmpfs has
/// no fsync/writeback latency, so the stores stop sharing the box's
/// disk queue; the suites test RPC behavior, not store durability (the
/// fence path has its own tests), so coverage is unchanged. Where
/// `/dev/shm` is missing or unwritable the default tempdir applies, as
/// before.
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
            sourcemap: None,
            no_activate: false,
            read_roots: Vec::new(),
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
            .env("CELLD_TEST_WAIT", "1")
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
fn fixture_tempdir_prefers_shm() {
    // Pins the flake fix: when /dev/shm takes a tempdir, fixtures must
    // live there, not on the box's disk queue.
    let probe = tempfile::Builder::new()
        .prefix("celld-shm-probe-")
        .tempdir_in("/dev/shm");
    if probe.is_ok() {
        assert!(
            fixture_tempdir("celld-shm-pin-")
                .path()
                .starts_with("/dev/shm"),
            "fixtures must prefer /dev/shm when usable"
        );
    }
}

#[test]
fn svc_stub_wait_conformance() {
    let dir = fixture_tempdir("celld-svc-stub-wait-");
    let app_project = dir.path().join("app");
    let controller_src = dir.path().join("controller");
    copy_dir(&fixture_dir().join("app"), &app_project);
    copy_dir(&fixture_dir().join("controller"), &controller_src);
    preseed_controller(&app_project, &controller_src);
    let node = TestNode::boot(&app_project);

    // Control first: an already-resolved promise through the same nested
    // capability shape settles, proving the stub path itself is sound.
    let control = get_json(&node, "/wait-control");
    assert_eq!(control["ok"], true, "{control}");
    assert_eq!(control["result"]["value"], "control-7", "{control}");

    // The wait fires before the capability method runs: the awaiting
    // event subscribes after the value resolved and must still get it.
    let after = get_json(&node, "/wait-after-fire");
    assert_eq!(after["ok"], true, "{after}");
    assert_eq!(after["result"]["value"], "three", "{after}");

    // The capability method runs before the wait fires: the promise
    // resolves after the call and the awaiting call must still get it.
    let before = get_json(&node, "/wait-before-fire");
    assert_eq!(before["ok"], true, "{before}");
    assert_eq!(before["result"]["value"], "three", "{before}");
}
