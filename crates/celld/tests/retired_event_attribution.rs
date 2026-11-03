// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): retired-event attribution test. Apache-2.0.

// A white-box driver test, not node code: it builds a Worker in-process and
// runs its turns directly, so the execution boundary's injected
// clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A continuation that outlives its event must not borrow the ambient
//! entry's request: subrequest work from a retired event fails instead of
//! charging whoever runs the turn. This drives one isolate's turns directly,
//! so the first entry's retirement is exact — dropped, not raced — and the
//! stale continuation provably runs with a retired token. Everything runs on
//! the test thread; no op is ever polled, so the test needs no network.

use celld::js::{Compat, RequestBody, Worker, WorkerConfig, WorkerConfigOptions};
use std::sync::OnceLock;
use std::time::Instant;

/// Init V8 and the asyncrt host handle once per test binary, on a leaked
/// current-thread runtime. The handle asserts exactly-once, so the tests
/// share it; both drive turns on their own threads and never poll an op.
fn ensure_engine() {
    static INIT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    INIT.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        celld::runtime::init_v8();
        celld::asyncrt::set_host_handle(runtime.handle().clone());
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

const WORKER: &str = r#"
let releaseGate;
const gate = new Promise((resolve) => {
  releaseGate = resolve;
});
let outcome = "unset";

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/arm") {
      // Attached, never awaited: runs after this event retires.
      gate.then(async () => {
        try {
          await fetch("http://127.0.0.1:9/unreachable");
          outcome = "proceeded";
        } catch (error) {
          outcome = String((error && error.message) || error);
        }
      });
      return Response.json({ ok: true });
    }
    if (url.pathname === "/release") {
      releaseGate();
      return Response.json({ ok: true });
    }
    if (url.pathname === "/outcome") {
      return Response.json({ outcome });
    }
    return Response.json({ ok: true });
  },
};
"#;

/// Run one request's first turn to its sync answer and retire it, returning
/// the status and body. Every fixture route answers without suspending, so
/// one turn is the whole request.
fn turn_once(worker: &mut Worker, url: &str) -> (u16, Vec<u8>) {
    let (reply, mut receive) = tokio::sync::oneshot::channel();
    let job = celld::WorkerJob::Fetch {
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

#[test]
fn retired_event_subrequest_work_fails_instead_of_charging_ambient() {
    ensure_engine();
    let mut worker = load_worker(WORKER, "retired-event-attribution");

    // Arm, then retire: dropping the entry retires its token exactly.
    let (status, _) = turn_once(&mut worker, "http://test/arm");
    assert_eq!(status, 200);
    // Release: the stale continuation runs in this turn with a retired
    // token. Its fetch must fail, not charge this request and hang on a
    // resolver the retired owner can never drive.
    let (status, _) = turn_once(&mut worker, "http://test/release");
    assert_eq!(status, 200);
    let (status, body) = turn_once(&mut worker, "http://test/outcome");
    assert_eq!(status, 200);
    let outcome: serde_json::Value = serde_json::from_slice(&body).expect("JSON body");
    assert_eq!(
        outcome["outcome"], "the event that issued this operation already ended",
        "{outcome}"
    );
}

const WORKER_RESOURCES: &str = r#"
import { connect } from "cloudflare:sockets";
let releaseGate;
const gate = new Promise((resolve) => {
  releaseGate = resolve;
});
let tcp = "unset";
let ws = "unset";
let hr = "unset";

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/arm") {
      // Attached, never awaited: runs after this event retires.
      gate.then(() => {
        try {
          connect("127.0.0.1:9");
          tcp = "proceeded";
        } catch (error) {
          tcp = String((error && error.message) || error);
        }
        try {
          new WebSocket("ws://127.0.0.1:9/ws");
          ws = "proceeded";
        } catch (error) {
          ws = String((error && error.message) || error);
        }
        try {
          new HTMLRewriter()
            .on("p", {})
            .transform(new Response("<p>x</p>"));
          hr = "proceeded";
        } catch (error) {
          hr = String((error && error.message) || error);
        }
      });
      return Response.json({ ok: true });
    }
    if (url.pathname === "/release") {
      releaseGate();
      return Response.json({ ok: true });
    }
    if (url.pathname === "/outcome") {
      return Response.json({ tcp, ws, hr });
    }
    return Response.json({ ok: true });
  },
};
"#;

#[test]
fn retired_event_resource_accounting_throws_with_nothing_owned() {
    ensure_engine();
    let mut worker = load_worker(WORKER_RESOURCES, "retired-resource-attribution");

    // Arm, then retire: dropping the entry retires its token exactly.
    let (status, _) = turn_once(&mut worker, "http://test/arm");
    assert_eq!(status, 200);
    // Release: the stale continuation runs in this turn with a retired
    // token. Owning work must throw with nothing pushed — a socket,
    // WebSocket, or rewriter opened here would otherwise be accounted to
    // this live request, which is not the request that opened it.
    let (status, _) = turn_once(&mut worker, "http://test/release");
    assert_eq!(status, 200);
    let (status, body) = turn_once(&mut worker, "http://test/outcome");
    assert_eq!(status, 200);
    let outcome: serde_json::Value = serde_json::from_slice(&body).expect("JSON body");
    assert_eq!(
        outcome["tcp"], "the event that issued this operation already ended",
        "{outcome}"
    );
    assert_eq!(
        outcome["ws"], "the event that issued this operation already ended",
        "{outcome}"
    );
    assert_eq!(
        outcome["hr"], "the event that issued this operation already ended",
        "{outcome}"
    );
}
