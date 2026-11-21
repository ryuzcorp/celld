// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A white-box driver test, not node code: it builds a Worker in-process and
// runs its turns directly, so the execution boundary's injected
// clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A disconnecting request with a pending unrefed retained wait must drop
//! the subscription, not reject it (denoland/celld#317): the record
//! outlives each subscription, and a later event re-subscribes what its
//! event dropped — the same rule `__retainedExecRead` follows. Rejecting
//! the unrefed op settles the record fatally (`__settleRetainedWait`
//! skips settled records in `__refreshRetainedWaits`), so every later
//! request keeps the rejection. The test-only wait stands in for the
//! container exec wait, which needs an engine this environment has not
//! got.

use cello::js::{Compat, RequestBody, Worker, WorkerConfig, WorkerConfigOptions};
use std::sync::OnceLock;
use std::time::Instant;

// V8's protection-key grant must land on the process's initial thread, before
// libtest creates a test thread; see `cello::init_v8_before_threads`.
cello::init_v8_before_threads!();

/// Install the asyncrt host handle once per test binary, on a leaked
/// current-thread runtime. V8 is already initialised by the process constructor
/// (see `cello::init_v8_before_threads`); the `init_v8` call stays as an
/// idempotent statement of the dependency. The handle asserts exactly-once, so
/// the tests share it; both drive turns on their own threads and never poll an
/// op.
fn ensure_engine() {
    static INIT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    INIT.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        cello::runtime::init_v8();
        cello::asyncrt::set_host_handle(runtime.handle().clone());
        runtime
    });
}

/// Load one module worker from `src` for a direct turn-driving test.
fn load_worker(src: &str, script_name: &str) -> Worker {
    let config = WorkerConfig::new(WorkerConfigOptions {
        src: src.to_string(),
        script_name: script_name.to_string(),
        do_classes: Vec::new(),
        bindings: Vec::new(),
        r2_bindings: Vec::new(),
        d1_bindings: Vec::new(),
        kv_bindings: Vec::new(),
        queue_bindings: Vec::new(),
        queue_consumers: Vec::new(),
        workflow_bindings: Vec::new(),
        vars: Vec::new(),
        node: String::new(),
        modules: Vec::new(),
        compat: Compat::default(),
    });
    Worker::load_config(std::sync::Arc::new(config)).expect("load worker")
}

/// Run one request's first turn to its sync answer and retire it, returning
/// the status and body. Every fixture route used this way answers without
/// suspending, so one turn is the whole request.
fn turn_once(worker: &mut Worker, url: &str) -> (u16, Vec<u8>) {
    let (reply, mut receive) = tokio::sync::oneshot::channel();
    let job = cello::WorkerJob::Fetch {
        queued_at: Instant::now(),
        entrypoint: None,
        invocation_limits: None,
        url: url.to_string(),
        method: "GET".to_string(),
        body: RequestBody::Bytes(bytes::Bytes::new()),
        headers: Vec::new(),
        request_id: None,
        tail_report: None,
        reply,
    };
    let (entry, started) = worker.turn_begin(job, None);
    drop(started);
    let mut entry = entry.expect("request starts");
    entry.abandon();
    drop(entry);
    let response = receive.try_recv().expect("sync answer").expect("ok answer");
    (response.status, response.body)
}

/// Start one request that suspends, keeping its entry and reply channel open
/// so the test can cancel or poll it like the driver would.
fn begin_suspended(
    worker: &mut Worker,
    url: &str,
    request_id: u128,
) -> (
    cello::js::InFlight,
    tokio::sync::oneshot::Receiver<Result<cello::js::HttpResponse, anyhow::Error>>,
    Vec<cello::js::Op>,
) {
    let (reply, receive) = tokio::sync::oneshot::channel();
    let job = cello::WorkerJob::Fetch {
        queued_at: Instant::now(),
        entrypoint: None,
        invocation_limits: None,
        url: url.to_string(),
        method: "GET".to_string(),
        body: RequestBody::Bytes(bytes::Bytes::new()),
        headers: Vec::new(),
        request_id: Some(request_id),
        tail_report: None,
        reply,
    };
    let (entry, started) = worker.turn_begin(job, None);
    (entry.expect("request starts"), receive, started)
}

const RETAINED_DISCONNECT: &str = r#"
let shared = null;
let outcomeA = "unset";
let outcomeB = "unset";
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const hooks = globalThis.__celloTestWaitForTestOnly;
    if (!hooks) return new Response("test waits disabled", { status: 500 });
    if (url.pathname === "/start") {
      // Retained-wait share over an unrefed backing wait, mirroring
      // `__retainedExecRead`: the record outlives each subscription, and
      // a later event re-subscribes what its event dropped.
      shared = hooks.makeWait("retained-disconnect").promise;
      try {
        const value = await shared;
        outcomeA = "resolved:" + value;
      } catch (error) {
        outcomeA = "rejected:" + String((error && error.message) || error);
      }
      return Response.json({ outcomeA });
    }
    if (url.pathname === "/join") {
      const record = shared;
      if (!record) return Response.json({ error: "no-shared" });
      try {
        const value = await record;
        outcomeB = "resolved:" + value;
      } catch (error) {
        outcomeB = "rejected:" + String((error && error.message) || error);
      }
      return Response.json({ outcomeB });
    }
    if (url.pathname === "/fire") {
      hooks.fire("retained-disconnect", url.searchParams.get("v") ?? "late-value");
      return Response.json({ fired: true });
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

/// Serializes this binary's tests around the retained-wait gate: the
/// globals exist only with `CELLO_TEST_WAIT=1`, read once per process at
/// first worker load, so every load in this binary happens under this
/// lock after the var is set.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A disconnect with a pending retained read must drop the unrefed op, so
/// a later request re-subscribes and reads the same output instead of
/// keeping a fatal rejection (denoland/celld#317). Rejecting the unrefed
/// subscription settles the record with "The client has disconnected";
/// the refresh then skips the settled record and the joiner answers with
/// the rejection instead of suspending.
#[test]
fn disconnect_drops_unrefed_retained_wait_so_later_request_rereads() {
    let _serial = SERIAL.lock().unwrap();
    std::env::set_var("CELLO_TEST_WAIT", "1");
    ensure_engine();
    let mut worker = load_worker(RETAINED_DISCONNECT, "retained-disconnect");

    // A subscribes to the retained wait and suspends on it: one unrefed
    // op, proving the subscription is live before the disconnect.
    let (mut entry_a, mut reply_a, started_a) =
        begin_suspended(&mut worker, "http://test/start", 31711);
    assert_eq!(started_a.len(), 1, "A holds its unrefed subscription");
    drop(started_a);
    assert!(reply_a.try_recv().is_err(), "A is still suspended");

    // A's client disconnects with the retained read pending. The entry
    // itself fails, but the shared record must stay unsettled.
    cello::js::abort_request(31711);
    let started = worker.turn_cancel(&mut entry_a);
    drop(started);
    drop(entry_a);

    // B joins the same record from a later event: the begin refresh
    // re-subscribes what the disconnect dropped, so B suspends on a live
    // record instead of observing a rejection. Pre-fix the record settled
    // at the cancel and no refresh could heal it, so B answers at once
    // with nothing outstanding.
    let (mut entry_b, mut reply_b, started_b) =
        begin_suspended(&mut worker, "http://test/join", 31712);
    assert_eq!(started_b.len(), 1, "B re-subscribed the retained wait");
    assert!(reply_b.try_recv().is_err(), "B is still suspended");

    // The output arrives late; it lands on B's subscription.
    let (status, _) = turn_once(&mut worker, "http://test/fire?v=late-value");
    assert_eq!(status, 200);
    // Deliver what the driver would have delivered for the fired wait,
    // then poll B's resumed continuation to its answer.
    let mut delivered = false;
    for (id, _) in &started_b {
        let started_poll = worker.turn_deliver(
            &mut entry_b,
            *id,
            Ok(cello::asyncrt::OpOut::Str("late-value".to_string())),
        );
        drop(started_poll);
        delivered = true;
    }
    assert!(delivered, "B holds its subscription op");
    drop(started_b);
    let started = worker.turn_poll(&mut entry_b);
    drop(started);
    let response = reply_b
        .try_recv()
        .expect("B answered after the fire")
        .unwrap_or_else(|error| panic!("B must answer, not fail: {error:#}"));
    assert_eq!(response.status, 200);
    let body: serde_json::Value = serde_json::from_slice(&response.body).expect("JSON body");
    assert_eq!(body["outcomeB"], "resolved:late-value", "{body}");
    drop(entry_b);

    // A itself still failed with the disconnect; only the shared record
    // survived it, so its own catch observed the same early firing the
    // fetch half pins in `retired_event_attribution`.
    match reply_a
        .try_recv()
        .expect("A answered after its client left")
    {
        Ok(_) => panic!("cancelled A must fail"),
        Err(error) => assert!(format!("{error:#}").contains("disconnected"), "{error:#}"),
    }
}
