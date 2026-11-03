// Modified by ryuzcorp (ryuzcorp/celld): Queue DLQ forward/redrive conformance. Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// reads the cells' SQLite files, and polls TCP with wall-clock deadlines,
// so the execution boundary's injected clock/filesystem/async rules do
// not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for the Queue dead-letter path: a message that spends its
//! retries on `dlq-src` lands in `dlq-dst` with the recorded source, the
//! true body size, and a host-minted id, and the redrive-direction
//! forward returns it with a cleared source and another minted id.
//!
//! The forward rides the `__queue_dlq_forward` host op, never stub RPC,
//! so the tenant cannot forge the id, the source, or the size. Metrics
//! prove the stored sizes (the stats triggers sum the `size` column);
//! the SQLite reads prove the source and the id minting, which neither
//! the binding nor the consumer delivery exposes.
//!
//! Redrive itself is operator-triggered over the signed `/runtime/`
//! route, which no test can sign: the dev node mints a random peer key.
//! The test therefore writes the same durable mark `_redrive` writes
//! (`dlq_target`, `dlq_transfer_id`, `transfer_kind = 'redrive'`) and a
//! fresh send arms the alarm that drives it, which covers the forward
//! and accept in the redrive direction — the marking query is untouched
//! by this change. One `celld dev` boots the app fixture with
//! `CELLD_TEST_DATA_DIR` pinned, so the cell databases are addressable.
//! There are no fixed sleeps: every wait polls an observable condition
//! with a deadline, and the server guard kills the child on every exit
//! path.
//!
//! A second test reuses the runtime-isolate fleet fixtures: the hosting
//! script pollutes the shared primordials at module top level (a
//! `Set.prototype.has` that answers true for the operator methods and
//! the unknown content type, among others), and the second script's
//! queue still refuses a stub-RPC redrive, a forged DLQ batch, and a
//! direct send with a bad content type — because the shared queue cell
//! runs in the runtime pool, whose isolates evaluate no tenant bundle.

use rusqlite::Connection;
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
        .join("tests/fixtures/queue-dlq")
        .canonicalize()
        .expect("fixture exists")
}

fn runtime_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/runtime-isolate")
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
    fn boot(app_project: &Path, data_dir: &Path) -> Self {
        let port = free_port();
        let mut child = Command::new(celld_binary())
            .arg("dev")
            .arg(app_project)
            .arg("--port")
            .arg(port.to_string())
            .arg("--no-watch")
            .arg("--logs")
            .env("CELLD_TEST_DATA_DIR", data_dir)
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

fn metrics(node: &TestNode) -> Value {
    let response = get_json(node, "/metrics");
    assert_eq!(response["ok"], true, "{response}");
    response["result"].clone()
}

/// One stored queue row: the id, the raw body, the stored size, and the
/// recorded dead-letter source.
struct StoredRow {
    id: String,
    body: Vec<u8>,
    size: i64,
    source: Option<String>,
}

fn cell_db(data_dir: &Path, queue: &str) -> PathBuf {
    data_dir
        .join(celld::engine_api::queue_cell_scope(queue))
        .join("ltx/e1/db.sqlite")
}

fn open_db(path: &Path) -> Connection {
    let db = Connection::open(path).expect("open cell db");
    db.busy_timeout(Duration::from_secs(10))
        .expect("set busy timeout");
    db
}

fn stored_rows(db: &Connection) -> Vec<StoredRow> {
    let mut select = db
        .prepare(
            "SELECT id, body, size, dead_letter_source
               FROM __queue_messages
              ORDER BY seq",
        )
        .expect("prepare rows");
    select
        .query_map([], |row| {
            Ok(StoredRow {
                id: row.get(0)?,
                body: row.get(1)?,
                size: row.get(2)?,
                source: row.get(3)?,
            })
        })
        .expect("query rows")
        .map(|row| row.expect("read row"))
        .collect()
}

fn receipt_count(db: &Connection) -> i64 {
    db.query_row(
        "SELECT COUNT(*) FROM __queue_transfer_receipts",
        [],
        |row| row.get(0),
    )
    .expect("count receipts")
}

/// A UUIDv7 id: 36 chars, dashes in place, the version nibble `7`.
fn assert_uuidv7(id: &str, context: &str) {
    let bytes = id.as_bytes();
    assert_eq!(bytes.len(), 36, "{context}: {id}");
    for position in [8, 13, 18, 23] {
        assert_eq!(bytes[position], b'-', "{context}: {id}");
    }
    assert_eq!(bytes[14], b'7', "{context}: {id}");
}

#[test]
fn queue_dlq_move_and_redrive() {
    let dir = tempfile::Builder::new()
        .prefix("celld-queue-dlq-")
        .tempdir()
        .expect("tempdir");
    let app_project = dir.path().join("app");
    copy_dir(&fixture_dir().join("app"), &app_project);
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let node = TestNode::boot(&app_project, &data_dir);

    // Three text messages on the source queue; the consumer fails each
    // twice (max_retries 1) before the broker dead-letters them.
    let sent = get_json(&node, "/send?n=3&prefix=dlq-payload");
    assert_eq!(sent["ok"], true, "{sent}");
    let bodies: Vec<String> =
        serde_json::from_value(sent["result"]["bodies"].clone()).expect("sent bodies");
    assert_eq!(bodies.len(), 3);
    let body_bytes: i64 = bodies.iter().map(|body| body.len() as i64).sum();

    poll_until("dead-letter", Duration::from_secs(120), || {
        let current = metrics(&node);
        (current["src"]["backlog"] == 0 && current["dlq"]["backlog"] == 3)
            .then(|| "moved".to_string())
    });

    // Every message failed twice on the source before moving.
    let consumed = get_json(&node, "/consumed");
    let deliveries = consumed["result"]["deliveries"]
        .as_array()
        .expect("deliveries")
        .clone();
    assert_eq!(deliveries.len(), 6, "{consumed}");
    let mut src_ids: Vec<String> = Vec::new();
    for delivery in &deliveries {
        assert_eq!(delivery["queue"], "dlq-src", "{delivery}");
        let id = delivery["id"].as_str().expect("id").to_string();
        if !src_ids.contains(&id) {
            src_ids.push(id);
        }
    }
    assert_eq!(src_ids.len(), 3, "{consumed}");
    for id in &src_ids {
        let mut attempts: Vec<u64> = deliveries
            .iter()
            .filter(|delivery| delivery["id"] == *id)
            .map(|delivery| delivery["attempts"].as_u64().expect("attempts"))
            .collect();
        attempts.sort_unstable();
        assert_eq!(attempts, vec![1, 2], "{consumed} {id}");
    }

    // The DLQ metrics sum the stored sizes, so the byte total proves the
    // move carried the true body lengths.
    let current = metrics(&node);
    assert_eq!(current["dlq"]["bytes"], body_bytes, "{current}");

    // The stored DLQ rows record the source queue, the true sizes, and
    // host-minted ids: none of them is the id the source delivered.
    let dlq = open_db(&cell_db(&data_dir, "dlq-dst"));
    let rows = stored_rows(&dlq);
    assert_eq!(rows.len(), 3);
    let mut dlq_ids: Vec<String> = Vec::new();
    for row in &rows {
        assert_eq!(row.source.as_deref(), Some("dlq-src"), "{}", row.id);
        assert_eq!(row.size, row.body.len() as i64, "{}", row.id);
        assert_uuidv7(&row.id, "dlq id");
        assert!(
            !src_ids.contains(&row.id),
            "dlq id {} reuses a delivered id",
            row.id
        );
        dlq_ids.push(row.id.clone());
    }
    let mut stored: Vec<String> = rows
        .iter()
        .map(|row| String::from_utf8(row.body.clone()).expect("text body"))
        .collect();
    stored.sort_unstable();
    let mut expected = bodies.clone();
    expected.sort_unstable();
    assert_eq!(stored, expected);
    assert_eq!(receipt_count(&dlq), 3);

    // Redrive direction: the test writes the durable mark `_redrive`
    // writes, and a fresh send arms the alarm that drives it. The parked
    // handler holds the redriven batch leased, so the rows are readable
    // before the release acks them.
    get_json(&node, "/mode?mode=hold");
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let marked = dlq
        .execute(
            "UPDATE __queue_messages
                SET dlq_target = 'dlq-src',
                    dlq_transfer_id = 'redrive-' || ? || '-' || seq,
                    transfer_kind = 'redrive'
              WHERE dead_letter_source IS NOT NULL
                AND dlq_target IS NULL
                AND lease_id IS NULL",
            [nanos.to_string()],
        )
        .expect("mark redrive rows");
    assert_eq!(marked, 3);
    drop(dlq);

    let driver = get_json(&node, "/send?queue=dlq&n=1&prefix=dlq-driver");
    assert_eq!(driver["ok"], true, "{driver}");

    poll_until("redrive", Duration::from_secs(120), || {
        let current = metrics(&node);
        (current["dlq"]["backlog"] == 1).then(|| "redriven".to_string())
    });
    poll_until("redrive-delivered", Duration::from_secs(120), || {
        let consumed = get_json(&node, "/consumed");
        let deliveries = consumed["result"]["deliveries"]
            .as_array()
            .expect("deliveries");
        (deliveries.len() == 9).then(|| "delivered".to_string())
    });

    // The redriven rows are ordinary queue state again: the source marker
    // is cleared, the sizes are true, and the ids are minted anew.
    let src = open_db(&cell_db(&data_dir, "dlq-src"));
    let rows = stored_rows(&src);
    assert_eq!(rows.len(), 3);
    for row in &rows {
        assert_eq!(row.source, None, "{}", row.id);
        assert_eq!(row.size, row.body.len() as i64, "{}", row.id);
        assert_uuidv7(&row.id, "redriven id");
        assert!(
            !dlq_ids.contains(&row.id),
            "redriven id {} reuses a dlq id",
            row.id
        );
    }
    let mut stored: Vec<String> = rows
        .iter()
        .map(|row| String::from_utf8(row.body.clone()).expect("text body"))
        .collect();
    stored.sort_unstable();
    assert_eq!(stored, expected);
    assert_eq!(receipt_count(&src), 3);
    drop(src);

    // Fresh queue state redelivers at attempt one.
    let consumed = get_json(&node, "/consumed");
    let deliveries = consumed["result"]["deliveries"]
        .as_array()
        .expect("deliveries");
    for delivery in &deliveries[6..] {
        assert_eq!(delivery["queue"], "dlq-src", "{delivery}");
        assert_eq!(delivery["attempts"], 1, "{delivery}");
    }

    // The release acks the parked batch; the source drains and only the
    // driver message stays backlogged on the DLQ.
    get_json(&node, "/release");
    poll_until("settle", Duration::from_secs(120), || {
        let current = metrics(&node);
        (current["src"]["backlog"] == 0 && current["dlq"]["backlog"] == 1)
            .then(|| "settled".to_string())
    });
}

// The operator gate and the cell-side validation hold when the fleet's
// first script pollutes the shared primordials at module top level. The
// host fixture overrides `Set.prototype.has` so the queue's RPC
// allowlist and content-type check would pass what they must refuse,
// and the second script's queue still refuses a stub-RPC redrive, a
// forged DLQ batch, and a direct send with an unknown content type —
// while a well-formed direct send lands and the binding send keeps
// working. The shared queue cell runs in the runtime pool, whose
// isolates evaluate no tenant bundle, so the hosting script's top level
// cannot reach the allowlist or the validation.
#[test]
fn queue_polluted_host_gate() {
    let dir = tempfile::Builder::new()
        .prefix("celld-queue-pollution-")
        .tempdir()
        .expect("tempdir");
    let app_project = dir.path().join("app");
    let victim_src = dir.path().join("victim");
    copy_dir(&runtime_fixture_dir().join("app"), &app_project);
    copy_dir(&runtime_fixture_dir().join("victim"), &victim_src);
    preseed_victim(&app_project, &victim_src);
    let data_dir = dir.path().join("data");
    std::fs::create_dir_all(&data_dir).expect("create data dir");
    let node = TestNode::boot(&app_project, &data_dir);

    // The host's own fetch path survives its own pollution: without the
    // forwarding below there is no observation at all.
    let health = get_json(&node, "/health");
    assert_eq!(health["ok"], true, "{health}");

    // Seed rows so the consumer's delivery log has its table.
    let seed = get_json(&node, "/v/seed");
    assert_eq!(seed["ok"], true, "{seed}");

    // The binding send keeps working under the host's pollution.
    let sent = get_json(&node, "/v/qsend");
    assert_eq!(sent["ok"], true, "{sent}");

    // The redrive and the forged DLQ batch refuse as not-a-function,
    // and the unknown content type refuses in validation — while a
    // well-formed direct send lands.
    let gate = get_json(&node, "/v/q-pollution-gate");
    assert_eq!(gate["ok"], true, "{gate}");
    let redrive = &gate["result"]["redrive"];
    assert_eq!(redrive["threw"], true, "{gate}");
    assert!(
        redrive["message"]
            .as_str()
            .unwrap_or_default()
            .contains("is not a function"),
        "{gate}"
    );
    let forge = &gate["result"]["dlqForge"];
    assert_eq!(forge["threw"], true, "{gate}");
    assert!(
        forge["message"]
            .as_str()
            .unwrap_or_default()
            .contains("is not a function"),
        "{gate}"
    );
    let bad_type = &gate["result"]["sendBadType"];
    assert_eq!(bad_type["threw"], true, "{gate}");
    assert!(
        bad_type["message"]
            .as_str()
            .unwrap_or_default()
            .contains("contentType"),
        "{gate}"
    );
    assert_eq!(gate["result"]["sendDirectOk"]["threw"], false, "{gate}");

    // The node is still up after the battery.
    let health = get_json(&node, "/health");
    assert_eq!(health["ok"], true, "{health}");
}
