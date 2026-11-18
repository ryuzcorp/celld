// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): process.env population regression test (upstream #296).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! Upstream denoland/celld#296: worker variables reached the handler's `env`
//! argument but not `globalThis.process.env` under `nodejs_compat`, and
//! `node:process` exposed truthy function stubs for every name instead of the
//! real process object. Vars now populate `process.env`, and `node:process`
//! is the global process object.

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
        .join("tests/fixtures/env-probe")
        .canonicalize()
        .expect("fixture exists")
}

fn optout_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/env-probe-no-populate")
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
    for file in ["wrangler.json", "worker.js", ".dev.vars"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// A booted `cello dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path) -> Self {
        let mut child = Command::new(cello_binary())
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
            .expect("spawn cello dev");
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
                panic!("cello dev exited during boot: {status}");
            }
            let addr = announced(&announced_lines, "cello listening on ")?;
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

#[test]
fn worker_vars_populate_process_env() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("cello-env-probe-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    let (status, body) = get(&node.public, "/").expect("GET /");
    assert_eq!(status, 200, "GET /: {body}");
    let probe: serde_json::Value = serde_json::from_str(&body).expect("probe JSON");
    let get = |name: &str| {
        probe
            .get(name)
            .unwrap_or_else(|| panic!("probe lacks {name}: {body}"))
            .clone()
    };
    let value = |name: &str| {
        get(name)
            .get("value")
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };

    // The handler env still carries both sources.
    assert_eq!(value("vars"), Some("set-in-vars".to_string()));
    assert_eq!(value("devVars"), Some("set-in-dev-vars".to_string()));
    // So does the global process.env.
    assert_eq!(value("globalVars"), Some("set-in-vars".to_string()));
    assert_eq!(value("globalDevVars"), Some("set-in-dev-vars".to_string()));
    // And the node:process import, which is the global object itself.
    assert_eq!(value("importedVars"), Some("set-in-vars".to_string()));
    assert_eq!(get("sameProcess").as_bool(), Some(true));
    assert_eq!(get("namedEnvSame").as_bool(), Some(true));
    // An unset name is undefined, never a truthy stub.
    let unset = get("importedUnset");
    assert_eq!(
        unset.get("type").and_then(|v| v.as_str()),
        Some("undefined")
    );
    assert_eq!(unset.get("truthy").and_then(|v| v.as_bool()), Some(false));
    // String() of a set var is the value, not the empty string.
    assert_eq!(
        get("importedString").as_str(),
        Some("set-in-vars"),
        "String(env.FROM_VARS)"
    );
}

/// A dynamically loaded Worker's loader-supplied strings reach the handler's
/// `env` and — under `nodejs_compat` — `process.env` alike, while the
/// host-owned `CELLO_NODE` name never crosses into `process.env`
/// (denoland/celld#296).
#[test]
fn loaded_worker_env_populates_process_env() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("cello-env-probe-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    let (status, body) = get(&node.public, "/loader").expect("GET /loader");
    assert_eq!(status, 200, "GET /loader: {body}");
    let probe: serde_json::Value = serde_json::from_str(&body).expect("probe JSON");
    let value = |name: &str| {
        probe
            .get(name)
            .unwrap_or_else(|| panic!("probe lacks {name}: {body}"))
            .get("value")
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };
    // The loader env reaches the handler, as before.
    assert_eq!(value("handlerEnv"), Some("1".to_string()));
    // And the same string reaches process.env.
    assert_eq!(value("procEnv"), Some("1".to_string()));
    // Module-scope reads agree: the strings were installed before the
    // child's module evaluated, not only by the post-evaluation install
    // the handler-time copy above observes.
    assert_eq!(value("moduleScopeFeature"), Some("1".to_string()));
    // The smuggled node name stays out of process.env.
    let proc_node = probe
        .get("procNode")
        .unwrap_or_else(|| panic!("probe lacks procNode: {body}"));
    assert_eq!(
        proc_node.get("type").and_then(|v| v.as_str()),
        Some("undefined"),
        "CELLO_NODE must not cross into process.env: {body}"
    );
    // At module scope too: the reserved name never crosses, at either time.
    let module_node = probe
        .get("moduleScopeNode")
        .unwrap_or_else(|| panic!("probe lacks moduleScopeNode: {body}"));
    assert_eq!(
        module_node.get("type").and_then(|v| v.as_str()),
        Some("undefined"),
        "CELLO_NODE must not cross into process.env at module scope: {body}"
    );
    // Dangerous keys never cross either: `constructor` would shadow the
    // inherited member on `process.env`.
    assert_eq!(
        probe.get("ctorIsFunction").and_then(|v| v.as_bool()),
        Some(true),
        "the constructor key must not shadow process.env: {body}"
    );
    // The `__proto__` smuggle is dropped by the loader encoder, so this is
    // a pin: `process.env` must keep its prototype either way.
    assert_eq!(
        probe.get("protoIntact").and_then(|v| v.as_bool()),
        Some(true),
        "process.env must keep its prototype: {body}"
    );
}

/// The `nodejs_compat_do_not_populate_process_env` flag keeps worker vars
/// out of `process.env` even with `nodejs_compat` set, while the handler's
/// `env` argument still carries them (denoland/celld#296).
#[test]
fn process_env_opt_out_keeps_vars_out() {
    let project = fixture_tempdir("cello-env-probe-optout-");
    copy_dir(&optout_fixture_dir(), project.path());
    let node = TestNode::boot(project.path());

    let (status, body) = get(&node.public, "/").expect("GET /");
    assert_eq!(status, 200, "GET /: {body}");
    let probe: serde_json::Value = serde_json::from_str(&body).expect("probe JSON");
    let entry = |name: &str| {
        probe
            .get(name)
            .unwrap_or_else(|| panic!("probe lacks {name}: {body}"))
            .clone()
    };
    // The handler env still carries the vars ...
    assert_eq!(
        entry("vars").get("value").and_then(|v| v.as_str()),
        Some("set-in-vars"),
    );
    // ... while process.env stays without them, however it is reached.
    for name in ["globalVars", "globalDevVars", "importedVars"] {
        assert_eq!(
            entry(name).get("type").and_then(|v| v.as_str()),
            Some("undefined"),
            "{name} must stay unpopulated under the opt-out: {body}",
        );
    }
}
