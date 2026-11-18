// Modified by ryuzcorp (ryuzcorp/celld): retiring-generation regression test (upstream #275).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A black-box runner, not node code: it boots the binary, stages a second
// version in-process, and polls TCP plus the node's logs with wall-clock
// deadlines, so the execution boundary's injected clock/filesystem/async
// rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A retiring generation's background work pins its isolate (upstream
//! #275): a cell event that answered but left a timer loop and a
//! never-settling `waitUntil` behind keeps its drive — and its isolate
//! affiliation — alive, so `may_free` never passes and the heap is never
//! reclaimed. Past the background budget the driver ends the background
//! work and the generation drains.
//!
//! The node runs `celld dev`; a staged v2 adopted through `POST /reload`
//! moves the cell to a new generation, stranding the first request's
//! leftover work on the old isolate. The test waits for the old generation
//! to be freed. Without the bound it never is. There are no fixed sleeps:
//! every wait polls an observable condition with a deadline, and the
//! server guard kills the child on every exit path.

mod common;

use common::{announced, forward_announce};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const TOKEN: &str = "retire-fleet-token";
const SCHEME: &str = "Bearer";

fn bearer() -> String {
    format!("{SCHEME} {TOKEN}")
}

fn cello_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cello"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/retire-leftover")
        .canonicalize()
        .expect("fixture exists")
}

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

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("copy fixture dir");
    for entry in std::fs::read_dir(src).expect("read fixture dir") {
        let entry = entry.expect("fixture entry");
        let to = dst.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).expect("copy fixture file");
        }
    }
}

/// A booted `celld dev` and its addresses. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
    internal: String,
    lines: Arc<Mutex<Vec<String>>>,
}

impl TestNode {
    fn boot(project: &Path) -> Self {
        // Port zero: the OS assigns the worker port, so parallel boots
        // never claim the same one; discovery below learns it.
        let mut child = Command::new(cello_binary())
            .arg("dev")
            .env("NO_COLOR", "1")
            // Seconds, not the 30 s default: the test watches one budget
            // pass, not a production ceiling.
            .env("CELLD_BACKGROUND_BUDGET_S", "5")
            .env("CELLD_OPERATOR_TOKEN", TOKEN)
            .arg(project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--clean")
            .arg("--logs")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn celld dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        // Drain stdout so a chatty child can never block on a full pipe;
        // stderr carries the announce lines and the pool lifecycle log.
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        let announced_lines: Arc<Mutex<Vec<String>>> = Arc::default();
        forward_announce(stderr, Arc::clone(&announced_lines));
        // The kill-on-drop guard exists before discovery, so a boot
        // timeout or an early child exit cannot leak the server.
        let mut node = Self {
            child,
            public: String::new(),
            internal: String::new(),
            lines: Arc::clone(&announced_lines),
        };
        // Discovery doubles as readiness: the announce lines name the
        // ports, and /health answers once the app loads.
        let addrs = poll_until("boot", Duration::from_secs(180), || {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!(
                    "celld dev exited during boot: {status}: {}",
                    announced_lines.lock().expect("announce lock").join("\n")
                );
            }
            let public = announced(&announced_lines, "celld listening on ")?;
            if !probe(&public, "/health") {
                return None;
            }
            let internal = announced(&announced_lines, "celld internal listening on ")?;
            Some(format!("{public}\n{internal}"))
        });
        let mut addrs = addrs.splitn(2, '\n');
        node.public = addrs.next().expect("public addr").to_string();
        node.internal = addrs.next().expect("internal addr").to_string();
        node
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn request(
    addr: &str,
    method: &str,
    path: &str,
    headers: &[(String, String)],
    timeout: Duration,
) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    let mut wire = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: 0\r\n"
    );
    for (name, value) in headers {
        wire.push_str(&format!("{name}: {value}\r\n"));
    }
    wire.push_str("\r\n");
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
    request(addr, "GET", path, &[], Duration::from_secs(60))
}

fn probe(addr: &str, path: &str) -> bool {
    matches!(
        request(addr, "GET", path, &[], Duration::from_secs(2)),
        Ok((200, _))
    )
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

/// lines containing `needle`, for log-driven assertions.
fn log_lines(node: &TestNode, needle: &str) -> Vec<String> {
    node.lines
        .lock()
        .expect("announce lock")
        .iter()
        .filter(|line| line.contains(needle))
        .cloned()
        .collect()
}

#[test]
fn retiring_generation_frees_its_isolates() {
    let dir = fixture_tempdir("celld-retire-leftover-");
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir(), &app_project);
    let node = TestNode::boot(&app_project);
    // Cell x runs on the first generation: the constructor starts the
    // timer loop, and the fetch parks a waitUntil that never settles.
    let (status, body) = get(&node.public, "/x").expect("cell answers");
    assert_eq!((status, body.as_str()), (200, "ok"));
    // Stage a second version of the same app and adopt it: the cell moves
    // to a new generation, stranding the first request's background work
    // on the old isolate.
    {
        let index = app_project.join("index.js");
        let mut src = std::fs::read_to_string(&index).expect("read worker");
        src.push_str("\n// v2: identical behavior, new version.\n");
        std::fs::write(&index, src).expect("write v2");
        let built = cello::deploy::build(&cello::deploy::Options {
            config: Some(app_project.clone()),
            bucket: None,
            endpoint: None,
            region: None,
            dry_run: false,
            json: false,
            vars: cello::deploy::VarOverrides::None,
            local_images: false,
            sourcemap: None,
            no_activate: true,
            read_roots: Vec::new(),
        })
        .expect("build v2");
        let version = built.version.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        cello::asyncrt::set_host_handle(rt.handle().clone());
        rt.block_on(async {
            let database = app_project.join(".celld/dev/objects.sqlite3");
            let bucket = cello::dev::open_local_bucket(&database).expect("open dev bucket");
            cello::fleet::validate_bucket(&bucket)
                .await
                .expect("validate bucket");
            bucket.probe_cas().await.expect("probe cas");
            cello::deploy::write_staged(&bucket, &built)
                .await
                .expect("stage v2");
            cello::deploy::activate(
                &bucket,
                &[cello::deploy::VersionRef {
                    script: "retire-leftover".to_string(),
                    version: version.clone(),
                }],
                Some("retire-leftover"),
            )
            .await
            .expect("activate v2");
        });
    };
    let adopted_before = log_lines(&node, "deployment_generation_adopted").len();
    let (status, body) = request(
        &node.internal,
        "POST",
        "/reload",
        &[("Authorization".to_string(), bearer())],
        Duration::from_secs(120),
    )
    .expect("reload adopts v2");
    assert_eq!(status, 200, "reload must adopt the staged map: {body}");
    assert!(
        body.contains("\"adopted\""),
        "reload must adopt v2, not stay unchanged: {body}"
    );
    // The old generation retires once the new one serves; without the
    // background bound its isolate never drains, and the freed line below
    // never arrives.
    let retired_at = poll_until("old generation retires", Duration::from_secs(120), || {
        (log_lines(&node, "deployment_generation_adopted").len() > adopted_before)
            .then(|| "adopted".to_string())
    });
    assert_eq!(retired_at, "adopted");
    poll_until(
        "retired generation is freed",
        Duration::from_secs(150),
        || {
            log_lines(&node, "deployment_generation_freed")
                .first()
                .cloned()
        },
    );
    // The node itself is unaffected: the new generation keeps serving.
    let (status, body) = get(&node.public, "/x").expect("cell still answers");
    assert_eq!((status, body.as_str()), (200, "ok"));
}
