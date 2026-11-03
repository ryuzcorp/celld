// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): cross-isolate RPC
// stubs and streams; response bodies across events. Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for cross-isolate Durable Object RPC stubs and byte streams
//! (same node), driven through the `tests/fixtures/rpc-stub` fixture the way
//! `docs/testing.md` runs examples: boot the real binary, replay HTTP.
//!
//! Each test boots its own `celld dev` on a private copy of the fixture
//! (dev state lives beside the project, so sharing one copy would collide),
//! waits for the public port, then asserts the fixture's transcripts. The
//! client is dependency-free on purpose: plain TCP with `Connection: close`
//! and `serde_json` for bodies, so the runner cannot mask a protocol fault.
//! There are no fixed sleeps: every wait polls an observable condition with
//! a deadline, and the server guard kills the child on every exit path.
//!
//! Lifetime under test: a stub (or stream) received by a caller is owned by
//! the caller's request. Request end, context abort, and isolate retirement
//! dispose what the invocation still holds and route every release to the
//! owner; there is no idle sweep, so a quiet-but-live handle stays usable
//! however long nothing flows. The drain phases below assert the host
//! table, the owner JS maps, and the caller JS handles together: a table
//! count alone would miss either side leaking.

use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const RELEASED_STUB_ERROR: &str =
    "The Durable Object that returned this RPC stub no longer runs on this node.";
const STREAM_DISCONNECT_ERROR: &str = "ReadableStream received over RPC disconnected prematurely.";
const STUB_DISPOSED_ERROR: &str = "RPC stub used after being disposed.";
const FOREIGN_STREAM_ERROR: &str = "RPC streams cannot cross isolate boundaries yet.";

fn celld_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_celld"))
}

fn fixture_source() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/rpc-stub")
        .canonicalize()
        .expect("fixture exists")
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    listener.local_addr().expect("probe addr").port()
}

fn copy_fixture(name: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix(&format!("celld-rpc-stub-{name}-"))
        .tempdir()
        .expect("tempdir");
    for file in ["wrangler.json", "index.js"] {
        std::fs::copy(fixture_source().join(file), dir.path().join(file)).expect("copy fixture");
    }
    dir
}

/// A booted `celld dev` and its addresses. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its ports.
struct TestNode {
    child: Child,
    public: String,
    internal: String,
}

/// Direct children of a process, via `/proc/<pid>/task/*/children`.
fn child_pids(pid: u32) -> Vec<u32> {
    let mut children = Vec::new();
    let Ok(tasks) = std::fs::read_dir(format!("/proc/{pid}/task")) else {
        return children;
    };
    for task in tasks.map_while(Result::ok) {
        let Ok(list) = std::fs::read_to_string(task.path().join("children")) else {
            continue;
        };
        children.extend(
            list.split_whitespace()
                .filter_map(|id| id.parse::<u32>().ok()),
        );
    }
    children.sort_unstable();
    children.dedup();
    children
}

/// Loopback TCP ports one process listens on, matching its socket inodes
/// against `/proc/net/tcp` LISTEN entries.
fn listening_loopback_ports(pid: u32) -> Vec<u16> {
    let mut inodes = std::collections::HashSet::new();
    if let Ok(fds) = std::fs::read_dir(format!("/proc/{pid}/fd")) {
        for fd in fds.map_while(Result::ok) {
            let Ok(link) = std::fs::read_link(fd.path()) else {
                continue;
            };
            if let Some(name) = link.to_str() {
                if let Some(inode) = name
                    .strip_prefix("socket:[")
                    .and_then(|inode| inode.strip_suffix(']'))
                {
                    inodes.insert(inode.to_string());
                }
            }
        }
    }
    let Ok(tcp) = std::fs::read_to_string("/proc/net/tcp") else {
        return Vec::new();
    };
    let mut ports = Vec::new();
    for line in tcp.lines().skip(1) {
        let mut fields = line.split_whitespace();
        let (Some(local), Some(state)) = (fields.nth(1), fields.nth(1)) else {
            continue;
        };
        if state != "0A" {
            continue;
        }
        // Fields past `st`: tx_queue, tr, retransmit, uid, timeout, then
        // the inode.
        let Some(inode) = fields.nth(5) else { continue };
        if !inodes.contains(inode) {
            continue;
        }
        let mut addr = local.split(':');
        if addr.next() != Some("0100007F") {
            continue;
        }
        if let Some(port) = addr
            .next()
            .and_then(|port| u16::from_str_radix(port, 16).ok())
        {
            ports.push(port);
        }
    }
    ports.sort_unstable();
    ports.dedup();
    ports
}

impl TestNode {
    fn boot(fixture: &Path, name: &str) -> Self {
        Self::boot_with_env(fixture, name, &[])
    }

    fn boot_with_env(fixture: &Path, name: &str, env: &[(&str, &str)]) -> Self {
        let port = free_port();
        let mut child = Command::new(celld_binary())
            .arg("dev")
            .arg(fixture)
            .arg("--port")
            .arg(port.to_string())
            .arg("--no-watch")
            .arg("--clean")
            .arg("--logs")
            .envs(env.iter().copied())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn celld dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        // Drain both pipes so a chatty child can never block on a full
        // one. The drains only discard: the supervisor block-buffers
        // into pipes, so its log lines are not a discovery channel and
        // the internal port comes from /proc below instead.
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
            internal: String::new(),
        };
        // Readiness is the public port answering with the app loaded; the
        // internal port is the node child's other loopback listener.
        node.internal = poll_until(&format!("{name} boot"), Duration::from_secs(120), || {
            if !probe(&public, "/scope?name=probe") {
                return None;
            }
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("{name}: celld dev exited during boot: {status}");
            }
            let mut internal = None;
            for pid in child_pids(node.child.id()) {
                for found in listening_loopback_ports(pid) {
                    if found != port {
                        internal = Some(found);
                    }
                }
            }
            internal.map(|port| format!("127.0.0.1:{port}"))
        });
        node
    }

    fn internal_base(&self) -> String {
        format!("http://{}", self.internal)
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

fn state_json(node: &TestNode) -> Value {
    let (status, body) = get(&node.internal, "/state").expect("internal /state");
    assert_eq!(status, 200, "/state: {body}");
    serde_json::from_str(&body).expect("state JSON")
}

fn host_stub_count(node: &TestNode) -> usize {
    state_json(node)
        .get("hostStubCount")
        .and_then(Value::as_u64)
        .expect("hostStubCount") as usize
}

fn upload_count(node: &TestNode) -> usize {
    state_json(node)
        .get("uploadCount")
        .and_then(Value::as_u64)
        .expect("uploadCount") as usize
}

/// Owner-isolate JS stats for one cell: its stub entries, host mappings,
/// and live caller handles, via the fixture's test-only hook.
fn do_stats(node: &TestNode, name: &str) -> Value {
    get_json(node, &format!("/do-stats?name={name}"))
}

/// Caller-isolate (Worker) JS stats, via the fixture's test-only hook.
fn caller_stats(node: &TestNode) -> Value {
    get_json(node, "/caller-stats")
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
fn rpc_stub_conformance() {
    let fixture = copy_fixture("flow");
    // The JS-side drain asserts below read the test-only stats hook, so
    // the node boots with it enabled.
    let node = TestNode::boot_with_env(fixture.path(), "flow", &[("CELLD_TEST_STUB_STATS", "1")]);

    // Stub method call with plain args/results, plus a property GET and a
    // callee-side error crossing back.
    let call = get_json(&node, "/stub-call");
    assert_eq!(call["sum"], 42, "{call}");
    assert_eq!(
        call["echo"],
        serde_json::json!([{"a": [1, 2]}, "b"]),
        "{call}"
    );
    assert_eq!(call["label"], "capability", "{call}");
    assert_eq!(call["noSuch"], "unknown operation missing", "{call}");

    // The sandboxed exec shape: pid/isPty plus stdin/stdout/stderr/capability.
    let exec = get_json(&node, "/exec-shape");
    assert_eq!(exec["pid"], 4242, "{exec}");
    assert_eq!(exec["isPty"], false, "{exec}");
    assert_eq!(exec["stdout"], "out-1\nout-2\n", "{exec}");
    assert_eq!(exec["stderr"], "err-1\n", "{exec}");
    assert_eq!(exec["exitCode"], 0, "{exec}");
    assert_eq!(
        exec["transcript"]["writes"],
        serde_json::json!(["hello-stdin\n"]),
        "{exec}"
    );
    assert_eq!(exec["transcript"]["closed"], true, "{exec}");
    assert_eq!(exec["transcript"]["exitCodeCalls"], 1, "{exec}");

    // Mid-stream error, caller cancel of a pending read, cancel observed
    // DO-side. Read errors surface as the disconnect error, as they do
    // same-isolate; only EOF ends cleanly.
    let errors = get_json(&node, "/stream-errors");
    assert_eq!(
        errors["chunks"],
        serde_json::json!(["chunk-1\n", "chunk-2\n"]),
        "{errors}"
    );
    assert_eq!(errors["error"], STREAM_DISCONNECT_ERROR, "{errors}");
    assert_eq!(errors["pendingDone"], true, "{errors}");
    assert_eq!(errors["pendingError"], Value::Null, "{errors}");
    assert_eq!(errors["transcript"]["cancelled"], true, "{errors}");

    // Worker->DO bytes: write then abort, verified DO-side.
    let abort = get_json(&node, "/stdin-abort");
    assert_eq!(
        abort["transcript"]["writes"],
        serde_json::json!(["part-1\n"]),
        "{abort}"
    );
    assert_eq!(abort["transcript"]["aborted"], true, "{abort}");
    assert_eq!(abort["transcript"]["closed"], false, "{abort}");

    // DO->DO through the same stub path, including streams both ways.
    let dodo = get_json(&node, "/do-to-do");
    assert_eq!(dodo["sum"], 42, "{dodo}");
    assert_eq!(dodo["label"], "capability", "{dodo}");
    assert_eq!(dodo["stdout"], "out-1\nout-2\n", "{dodo}");
    assert_eq!(
        dodo["transcript"]["writes"],
        serde_json::json!(["sib-stdin\n"]),
        "{dodo}"
    );
    assert_eq!(dodo["transcript"]["closed"], true, "{dodo}");

    // Forwarding a received host stream back to its owner moves the
    // reference and reads through the loopback.
    let forward = get_json(&node, "/forward-loopback");
    assert_eq!(forward["text"], "out-1\nout-2\n", "{forward}");

    // A Worker-owned readable in arguments crosses to the DO through an
    // upload channel: the Worker pumps, the DO pulls.
    let owned = get_json(&node, "/worker-owned-arg");
    assert_eq!(owned["text"], "owned\n", "{owned}");

    // No reverse path exists for a caller-owned writable: same-isolate
    // marker, foreign on use.
    let owned_writable = get_json(&node, "/worker-owned-writable");
    assert_eq!(
        owned_writable["error"], FOREIGN_STREAM_ERROR,
        "{owned_writable}"
    );

    // Explicit dispose frees the owner entries: the capability's disposer
    // and the hanging stream's cancel both run DO-side. The release round
    // trip is asynchronous, so poll the transcript until it settles.
    let dispose = get_json(&node, "/dispose");
    assert_eq!(dispose["sum"], 2, "{dispose}");
    assert_eq!(dispose["after"], STUB_DISPOSED_ERROR, "{dispose}");
    assert_eq!(dispose["streamAfter"], STREAM_DISCONNECT_ERROR, "{dispose}");
    let settled = dispose["pendingDone"] == true || dispose["pendingError"].is_string();
    assert!(settled, "in-flight read settles on dispose: {dispose}");
    poll_until("dispose", Duration::from_secs(30), || {
        let transcript = get_json(&node, "/transcript?name=six");
        (transcript["disposed"] == 1 && transcript["cancelled"] == true)
            .then(|| transcript.to_string())
    });

    // Caller-pushed bytes round trip, and a receiver cancel propagates to
    // the sender's origin. The channels drain with the request: the host
    // upload count and the caller pumps both return to nothing.
    let upload = get_json(&node, "/upload-roundtrip");
    assert_eq!(upload["echo"]["text"], "up-1\nup-2\n", "{upload}");
    assert_eq!(upload["first"]["first"], "first\n", "{upload}");
    assert_eq!(upload["firstCancelled"], true, "{upload}");
    poll_until("upload drain", Duration::from_secs(30), || {
        (upload_count(&node) == 0).then(|| "drained".to_string())
    });
    poll_until("upload pumps", Duration::from_secs(30), || {
        (caller_stats(&node)["ctxUploads"] == 0).then(|| "drained".to_string())
    });

    // A Request with a live caller-owned body crosses by value and echoes
    // back through the same channel.
    let fetch_body = get_json(&node, "/fetch-body-roundtrip");
    assert_eq!(fetch_body["text"], "post-body\n", "{fetch_body}");

    // The abort-carrier scenario: options carry an abort stream, stdout
    // stays quiet until the abort lands mid-exec, then the signalled
    // tail drains and the exit code resolves.
    let carrier_exec = get_json(&node, "/carrier-exec");
    assert_eq!(carrier_exec["first"], "chunk-1\n", "{carrier_exec}");
    assert_eq!(
        carrier_exec["rest"],
        serde_json::json!(["aborted\n"]),
        "{carrier_exec}"
    );
    assert_eq!(carrier_exec["exitCode"], 137, "{carrier_exec}");
    assert_eq!(
        carrier_exec["transcript"]["abortObserved"], true,
        "{carrier_exec}"
    );
    assert_eq!(
        carrier_exec["transcript"]["finishCalled"], true,
        "{carrier_exec}"
    );
    assert_eq!(
        carrier_exec["transcript"]["argv"],
        serde_json::json!(["sleep", "120"]),
        "{carrier_exec}"
    );

    // An AbortSignal in arguments crosses live: the DO observes the
    // abort the Worker fires after the call.
    let signal = get_json(&node, "/signal");
    assert_eq!(signal["probe"]["isSignal"], true, "{signal}");
    assert_eq!(signal["probe"]["aborted"], false, "{signal}");
    assert_eq!(signal["state"]["aborted"], true, "{signal}");

    // A release racing a validated op does not fail it: the op pins, the
    // dispose lands mid-drive, the answer still completes.
    let pin = get_json(&node, "/pin-race");
    assert_eq!(pin["arrived"], true, "{pin}");
    assert_eq!(pin["sum"], 42, "{pin}");

    // A quiet handle survives across unrelated activity: no timer, no
    // use-count, no expiry.
    let quiet = get_json(&node, "/quiet-across-activity");
    assert_eq!(quiet["sum"], 42, "{quiet}");

    // Caller context abort disposes the invocation's handles: the
    // in-flight op rejects with the reason, later use fails, and the
    // owner teardown runs.
    let caller_abort = get_json(&node, "/caller-abort");
    assert_eq!(caller_abort["sum"], 2, "{caller_abort}");
    assert_eq!(caller_abort["slowError"], "caller-boom", "{caller_abort}");
    assert_eq!(caller_abort["after"], STUB_DISPOSED_ERROR, "{caller_abort}");
    poll_until("caller abort dispose", Duration::from_secs(30), || {
        let transcript = get_json(&node, "/transcript?name=cabort");
        (transcript["disposed"] == 1).then(|| transcript.to_string())
    });

    // Forged markers fail closed with clear errors: a forged stub id and
    // a forged upload id beside a real stream are refused at lift with
    // DataCloneError (0005's reserved-key refusal runs on host lifts
    // too), never reviving or dispatching; a path past the walk budget
    // still fails at the walk. Malformed ids that reach the host tables
    // stay covered by the stub_table/upload_table unit tests.
    let malformed = get_json(&node, "/malformed");
    assert_eq!(malformed["bad"]["threw"], true, "{malformed}");
    assert_eq!(malformed["bad"]["name"], "DataCloneError", "{malformed}");
    assert!(
        malformed["bad"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("reserved key"),
        "{malformed}"
    );
    assert_eq!(malformed["badStream"]["threw"], true, "{malformed}");
    assert_eq!(
        malformed["badStream"]["name"], "DataCloneError",
        "{malformed}"
    );
    assert!(
        malformed["badStream"]["error"]
            .as_str()
            .unwrap_or_default()
            .contains("reserved key"),
        "{malformed}"
    );
    assert!(
        malformed["deepError"]
            .as_str()
            .unwrap_or_default()
            .contains("walk budget"),
        "{malformed}"
    );

    // 0005+v2: a forged `__celld$` key in DO RPC args is refused at
    // lift beside a Worker-owned readable (upload channel) and beside
    // a received host stream being forwarded.
    let forged_upload = get_json(&node, "/forged-upload");
    for side in ["besideUpload", "besideHost"] {
        assert_eq!(forged_upload[side]["threw"], true, "{forged_upload}");
        assert_eq!(
            forged_upload[side]["name"], "DataCloneError",
            "{forged_upload}"
        );
        assert!(
            forged_upload[side]["error"]
                .as_str()
                .unwrap_or_default()
                .contains("reserved key"),
            "{forged_upload}"
        );
    }

    // Stub ids never serialize observably: no 32-hex anywhere a script
    // can read.
    let leak = get_json(&node, "/no-id-leak");
    assert_eq!(leak["leaked"], false, "{leak}");

    // Request end disposes held stubs and releases owner entries: churn
    // without disposing, then the host table, the owner maps, and the
    // caller handles all return to baseline right after the response.
    // Caller handles drain synchronously at request end, so they assert
    // directly; the releases route asynchronously, so the table, the
    // owner maps, and the owner teardowns poll.
    let owner_before = do_stats(&node, "eight");
    assert_eq!(caller_stats(&node)["ctxHandles"], 0, "caller starts quiet");
    let baseline = host_stub_count(&node);
    let churn = get_json(&node, "/churn?n=10");
    assert_eq!(churn["count"], 10, "{churn}");
    assert_eq!(
        caller_stats(&node)["ctxHandles"],
        0,
        "request end disposes caller handles"
    );
    poll_until("request-end table", Duration::from_secs(30), || {
        (host_stub_count(&node) == baseline).then(|| baseline.to_string())
    });
    poll_until("request-end owner", Duration::from_secs(30), || {
        let stats = do_stats(&node, "eight");
        (stats["ownerEntries"] == owner_before["ownerEntries"]
            && stats["hostIds"] == owner_before["hostIds"])
            .then(|| stats.to_string())
    });
    poll_until("request-end teardowns", Duration::from_secs(30), || {
        let transcript = get_json(&node, "/transcript?name=eight");
        (transcript["disposed"] == 10).then(|| transcript.to_string())
    });

    // Churn with explicit dispose: the host table returns to baseline.
    let baseline = host_stub_count(&node);
    let churn = get_json(&node, "/churn?n=20&dispose=1");
    assert_eq!(churn["count"], 20, "{churn}");
    poll_until("churn", Duration::from_secs(30), || {
        (host_stub_count(&node) == baseline).then(|| baseline.to_string())
    });

    // waitUntil-floated handle-bearing calls settle after response end
    // (a plain float is cancelled with the request): their revives must
    // not register into the dead context. The host table, the owner
    // maps, and the caller handles all still drain; the owner teardowns
    // run (the handoff cycle completes); and the post-end stream lift
    // leaves no upload channel behind.
    let owner_before = do_stats(&node, "float");
    let baseline = host_stub_count(&node);
    let float = get_json(&node, "/float-past-end?n=5");
    assert_eq!(float["floated"], 5, "{float}");
    assert_eq!(
        caller_stats(&node)["ctxHandles"],
        0,
        "post-end revives register nowhere"
    );
    poll_until("float table", Duration::from_secs(30), || {
        (host_stub_count(&node) == baseline).then(|| baseline.to_string())
    });
    // The replies landed by now (their releases drained the table), so a
    // post-end registration would show here.
    assert_eq!(
        caller_stats(&node)["ctxHandles"],
        0,
        "post-end revives register nowhere"
    );
    poll_until("float owner", Duration::from_secs(30), || {
        let stats = do_stats(&node, "float");
        (stats["ownerEntries"] == owner_before["ownerEntries"]
            && stats["hostIds"] == owner_before["hostIds"])
            .then(|| stats.to_string())
    });
    poll_until("float teardowns", Duration::from_secs(30), || {
        let transcript = get_json(&node, "/transcript?name=float");
        (transcript["disposed"] == 5).then(|| transcript.to_string())
    });
    // The first float used its post-end handle: disposed on arrival, so
    // the use failed locally and drove nothing owner-side.
    assert_eq!(
        get_json(&node, "/transcript?name=float")["exitCodeCalls"],
        0,
        "post-end revives arrive disposed"
    );
    poll_until("float uploads", Duration::from_secs(30), || {
        (upload_count(&node) == 0).then(|| "drained".to_string())
    });

    // Owner eviction of the idle cell: later stub ops reject with the
    // released-stub error, reads disconnect, writes propagate the
    // released error, and the cell reactivates for fresh handles.
    let evict = get_json(
        &node,
        &format!("/evict-flow?internal={}&name=seven", node.internal_base()),
    );
    assert_eq!(evict["evicted"], "{\"ok\":true}", "{evict}");
    assert_eq!(evict["sum"], 2, "{evict}");
    assert_eq!(evict["laterError"], RELEASED_STUB_ERROR, "{evict}");
    assert_eq!(evict["streamError"], STREAM_DISCONNECT_ERROR, "{evict}");
    assert_eq!(evict["writeError"], RELEASED_STUB_ERROR, "{evict}");
    assert_eq!(evict["revived"]["disposed"], 0, "{evict}");
    assert_eq!(evict["freshSum"], 5, "{evict}");

    // A generation reload revokes the old generation's stubs; a draining
    // caller still reaches the residency that minted its fresh ids, the
    // way its plain RPCs do.
    let reload = get_json(
        &node,
        &format!("/reload-flow?internal={}&name=nine", node.internal_base()),
    );
    assert_eq!(reload["sum"], 2, "{reload}");
    assert!(
        reload["reloaded"]
            .as_str()
            .unwrap_or_default()
            .contains("\"ok\":true"),
        "{reload}"
    );
    assert_eq!(reload["laterError"], RELEASED_STUB_ERROR, "{reload}");
    assert_eq!(reload["freshSum"], 5, "{reload}");
}

#[test]
fn rpc_stub_caps_fail_closed() {
    // Tiny caps, so the fail-closed errors are reachable without churning
    // thousands of entries.
    let fixture = copy_fixture("caps");
    let node = TestNode::boot_with_env(
        fixture.path(),
        "caps",
        &[
            ("CELLD_STUB_SCOPE_CAP", "8"),
            ("CELLD_UPLOAD_CAP", "4"),
            ("CELLD_TEST_STUB_STATS", "1"),
        ],
    );
    let baseline = host_stub_count(&node);

    // The scope cap fails the minting call with a clear error. Two wires
    // (six entries) fit under a cap of eight; the third call's ninth
    // mint throws, the failed lift rolls its partial mints back, and the
    // held wires still drain at request end.
    let cap = get_json(&node, "/caps-stub");
    assert_eq!(cap["held"], 2, "{cap}");
    let error = cap["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("too many live cross-isolate RPC stubs") && error.contains("(limit 8)"),
        "{cap}"
    );
    poll_until("caps table", Duration::from_secs(30), || {
        (host_stub_count(&node) == baseline).then(|| baseline.to_string())
    });
    poll_until("caps owner", Duration::from_secs(30), || {
        let stats = do_stats(&node, "cap");
        (stats["ownerEntries"] == 0 && stats["hostIds"] == 0).then(|| stats.to_string())
    });

    // The upload cap fails the same way: eight streams under a cap of
    // four, and the call errors instead of minting dead channels.
    let upload_cap = get_json(&node, "/caps-upload");
    let error = upload_cap["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("too many live caller-pushed RPC streams") && error.contains("(limit 4)"),
        "{upload_cap}"
    );
    poll_until("caps uploads", Duration::from_secs(30), || {
        (upload_count(&node) == 0).then(|| "drained".to_string())
    });
}

#[test]
fn rpc_stub_concurrent_drives_keep_sibling_revives() {
    // R1: two concurrent stub ops against one dead-entry stub share no
    // disposal scope. A finishes first; B's reply lift must still move
    // its echoed capability out, and the echo must stay usable.
    let fixture = copy_fixture("r1");
    let node = TestNode::boot(fixture.path(), "r1");
    let drives = get_json(&node, "/concurrent-drives");
    assert_eq!(drives["arrived"], true, "{drives}");
    assert_eq!(drives["sumA"], 42, "{drives}");
    assert_eq!(drives["errorB"], Value::Null, "{drives}");
    assert_eq!(drives["sumB"], 42, "{drives}");
}

#[test]
fn rpc_stub_reentrant_drives_keep_sibling_revives() {
    // S1: re-entrant concurrent stub ops sharing one still-live entry
    // ctx. The owner lends a capability to a sibling mid-invocation;
    // the sibling's first drive must not dispose the second drive's
    // nested revives when it ends.
    let fixture = copy_fixture("s1");
    let node = TestNode::boot(fixture.path(), "s1");
    let drives = get_json(&node, "/reentrant-drives");
    assert_eq!(drives["arrived"], true, "{drives}");
    assert_eq!(drives["sumA"], 42, "{drives}");
    assert_eq!(drives["errorB"], Value::Null, "{drives}");
    assert_eq!(drives["sumB"], 42, "{drives}");
}

#[test]
fn rpc_stub_self_fetch_keeps_caps_usable() {
    // R3: a self-fetch handler that calls stub ops and uses the revived
    // handles inline must get live capabilities, not disposed-on-arrival
    // ones.
    let fixture = copy_fixture("r3");
    let node = TestNode::boot(fixture.path(), "r3");
    let fetched = get_json(&node, "/self-fetch-caps");
    assert_eq!(fetched["threw"], Value::Null, "{fetched}");
    assert_eq!(fetched["status"], 200, "{fetched}");
    assert_eq!(fetched["body"]["sum"], 42, "{fetched}");
    assert_eq!(fetched["body"]["text"], "out-1\nout-2\n", "{fetched}");
}

#[test]
fn rpc_stub_throwing_entrypoint_op_ends_its_ctx() {
    // R4: an entrypoint op that aborts its own context rejects past its
    // reply, and must leave no live context behind. The stats read runs
    // as its own request, so exactly one context (itself) is live.
    let fixture = copy_fixture("r4");
    let node = TestNode::boot_with_env(fixture.path(), "r4", &[("CELLD_TEST_STUB_STATS", "1")]);
    let aborted = get_json(&node, "/entrypoint-op-abort");
    assert_eq!(aborted["error"], "op-boom", "{aborted}");
    assert_eq!(caller_stats(&node)["liveCtxs"], 1, "no live ctx leaks");
}

#[test]
fn rpc_stub_dup_dispose_frees_the_entry() {
    // A dup'd host handle owns its own host hold: churning dup+dispose
    // against a tiny scope cap must not fill it, the final fresh export
    // must succeed, and every iteration must route its owner dispose.
    let fixture = copy_fixture("dup");
    let node = TestNode::boot_with_env(
        fixture.path(),
        "dup",
        &[
            ("CELLD_STUB_SCOPE_CAP", "8"),
            ("CELLD_TEST_STUB_STATS", "1"),
        ],
    );
    let baseline = host_stub_count(&node);
    let flow = get_json(&node, "/dup-churn?iters=8");
    assert_eq!(flow["sum"], 2, "{flow}");
    assert_eq!(flow["twinSum"], 5, "{flow}");
    assert_eq!(flow["finalError"], Value::Null, "{flow}");
    assert_eq!(flow["finalSum"], 9, "{flow}");
    poll_until("dup table", Duration::from_secs(30), || {
        (host_stub_count(&node) == baseline).then(|| baseline.to_string())
    });
    poll_until("dup owner", Duration::from_secs(30), || {
        let stats = do_stats(&node, "dupdo");
        (stats["ownerEntries"] == 0 && stats["hostIds"] == 0).then(|| stats.to_string())
    });
    poll_until("dup disposed", Duration::from_secs(30), || {
        let transcript = get_json(&node, "/transcript?name=dupdo");
        (transcript["disposed"] == 9).then(|| transcript.to_string())
    });
}

#[test]
fn rpc_stub_facet_export_validates_under_its_root() {
    // An own-script facet runs in its root's isolate: a stub it mints
    // must export under the root's scope and epoch (the facet scope is
    // never published, so minting under it could never validate), and
    // dispatch must find the entry in the root isolate.
    let fixture = copy_fixture("facet");
    let node = TestNode::boot(fixture.path(), "facet");
    let flow = get_json(&node, "/facet-stub");
    assert_eq!(flow["sum"], 42, "{flow}");
}

#[test]
fn rpc_stub_response_body_across_events() {
    // A stub method returning a Response with a streamed body settles
    // when the body's reads span native events: the read-ahead went
    // out in the call's event and the bytes arrive after it (the file
    // read shape). The buffered shape is the control: it crosses by
    // value and never touches the read path. Both shapes also run
    // through a nested stub op.
    let fixture = copy_fixture("resp");
    let node = TestNode::boot_with_env(fixture.path(), "resp", &[("CELLD_TEST_WAIT", "1")]);
    let buffered = get_json(&node, "/response-buffered");
    assert_eq!(
        buffered["data"],
        serde_json::json!({"ok": true, "n": 3}),
        "{buffered}"
    );
    let streamed = get_json(&node, "/response-read-ahead");
    assert_eq!(
        streamed["data"],
        serde_json::json!({"ok": true, "n": 3}),
        "{streamed}"
    );
    let nested = get_json(&node, "/response-nested");
    assert_eq!(
        nested["data"],
        serde_json::json!({"ok": true, "n": 3}),
        "{nested}"
    );
    assert_eq!(
        nested["bufData"],
        serde_json::json!({"ok": true, "n": 3}),
        "{nested}"
    );
}

#[test]
fn rpc_stub_stats_gated_without_env() {
    // Without CELLD_TEST_STUB_STATS the stats hook is absent from every
    // isolate: production tenants cannot probe neighbor-invocation
    // liveness on a shared heap. The fixture reports its absence
    // instead of counts, on both sides.
    let fixture = copy_fixture("gated");
    let node = TestNode::boot(fixture.path(), "gated");
    assert_eq!(
        caller_stats(&node),
        serde_json::json!({"unavailable": true})
    );
    assert_eq!(
        do_stats(&node, "gated"),
        serde_json::json!({"unavailable": true})
    );
}
