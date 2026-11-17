// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): retired-event attribution test; cancelled-request shared-work regression tests (upstream #317); shared-promise attribution regression test (upstream #316); trailing-handoff rejection test (upstream #309); refused-handoff settlement test (upstream #207). Apache-2.0.

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

/// Start one request that suspends, keeping its entry and reply channel open
/// so the test can cancel or poll it like the driver would.
fn begin_suspended(
    worker: &mut Worker,
    url: &str,
    request_id: u128,
) -> (
    celld::js::InFlight,
    tokio::sync::oneshot::Receiver<Result<celld::js::HttpResponse, anyhow::Error>>,
    Vec<celld::js::Op>,
) {
    let (reply, receive) = tokio::sync::oneshot::channel();
    let job = celld::WorkerJob::Fetch {
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

const SHARED_FLIGHT: &str = r#"
const flights = new Map();
let outcomeA = "unset";
let outcomeB = "unset";

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/start") {
      // Shared work: a timer this white-box driver never polls, so it stays
      // pending like an outbound fetch whose response never arrives.
      const flight = new Promise((resolve) => setTimeout(resolve, 60000));
      flights.set("k", flight);
      try {
        await flight;
        outcomeA = "resolved";
      } catch (error) {
        outcomeA = "rejected:" + String((error && error.message) || error);
      }
      return Response.json({ outcomeA });
    }
    if (url.pathname === "/join") {
      const flight = flights.get("k");
      if (!flight) return Response.json({ error: "no-flight" });
      try {
        await flight;
        outcomeB = "resolved";
      } catch (error) {
        outcomeB = "rejected:" + String((error && error.message) || error);
      }
      return Response.json({ outcomeB });
    }
    if (url.pathname === "/outcome") {
      return Response.json({ outcomeA, outcomeB });
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

/// A request whose client disconnects must settle the work it started, so a
/// request awaiting the same promise answers instead of hanging to its
/// budget (denoland/celld#317). Dropping the op's resolver left every
/// co-awaiter's promise pending forever with nothing logged: the dropped
/// timer op now fires its callback early, so the shared promise settles
/// with the value the callback computes.
#[test]
fn cancelled_request_settles_shared_work_so_co_awaiters_answer() {
    ensure_engine();
    let mut worker = load_worker(SHARED_FLIGHT, "cancelled-shared-flight");

    // A starts shared work and suspends on it; B awaits the same promise and
    // holds no op of its own.
    let (mut entry_a, mut reply_a, started_a) =
        begin_suspended(&mut worker, "http://test/start", 31701);
    assert_eq!(started_a.len(), 1, "A waits on its timer op");
    drop(started_a);
    let (mut entry_b, mut reply_b, started_b) =
        begin_suspended(&mut worker, "http://test/join", 31702);
    assert!(
        started_b.is_empty(),
        "B waits on A's promise, no op of its own"
    );
    drop(started_b);
    assert!(reply_b.try_recv().is_err(), "B is still suspended");

    // A's client disconnects before the work finishes.
    celld::js::abort_request(31701);
    let started = worker.turn_cancel(&mut entry_a);
    drop(started);
    drop(entry_a);

    // B's handler must settle now instead of hanging: the dropped timer
    // fired its callback early, so the shared promise resolved.
    let started = worker.turn_poll(&mut entry_b);
    drop(started);
    let response = reply_b
        .try_recv()
        .expect("B answered after A was cancelled")
        .unwrap_or_else(|error| panic!("B must answer, not fail: {error:#}"));
    assert_eq!(response.status, 200);
    let body: serde_json::Value = serde_json::from_slice(&response.body).expect("JSON body");
    assert_eq!(body["outcomeB"], "resolved", "{body}");
    drop(entry_b);

    // A itself still failed with the disconnect; only the shared work it
    // started settled, so its own catch observed the early firing too.
    match reply_a
        .try_recv()
        .expect("A answered after its client left")
    {
        Ok(_) => panic!("cancelled A must fail"),
        Err(error) => assert!(format!("{error:#}").contains("disconnected"), "{error:#}"),
    }
    let (status, body) = turn_once(&mut worker, "http://test/outcome");
    assert_eq!(status, 200);
    let outcome: serde_json::Value = serde_json::from_slice(&body).expect("JSON body");
    assert_eq!(outcome["outcomeA"], "resolved", "{outcome}");
}

const SHARED_FETCH: &str = r#"
const flights = new Map();
let outcomeA = "unset";
let outcomeB = "unset";

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/start") {
      // Unroutable: the subrequest stays in flight until the client goes
      // away, and the future is never polled in this driver. The op is
      // created without I/O, so this needs no network.
      const flight = fetch("http://10.255.255.1/slow")
        .then((r) => r.text())
        .catch((e) => `error:${e && e.name ? e.name : e}`);
      flights.set("k", flight);
      outcomeA = await flight;
      return Response.json({ outcomeA });
    }
    if (url.pathname === "/join") {
      const flight = flights.get("k");
      if (!flight) return Response.json({ error: "no-flight" });
      outcomeB = await flight;
      return Response.json({ outcomeB });
    }
    if (url.pathname === "/outcome") {
      return Response.json({ outcomeA, outcomeB });
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

/// The fetch half of the same fix: a dropped subrequest rejects, and the
/// fetch wrapper carries that rejection to every co-awaiter's `.catch`
/// (denoland/celld#317). A subrequest has no value to produce early, so
/// unlike a timer it cannot fire — it must fail loudly.
#[test]
fn cancelled_request_rejects_shared_fetch_so_co_awaiters_fail_loudly() {
    ensure_engine();
    let mut worker = load_worker(SHARED_FETCH, "cancelled-shared-fetch");

    let (mut entry_a, mut reply_a, started_a) =
        begin_suspended(&mut worker, "http://test/start", 31711);
    assert!(!started_a.is_empty(), "A waits on its subrequest op");
    drop(started_a);
    let (mut entry_b, mut reply_b, started_b) =
        begin_suspended(&mut worker, "http://test/join", 31712);
    assert!(
        started_b.is_empty(),
        "B waits on A's promise, no op of its own"
    );
    drop(started_b);

    celld::js::abort_request(31711);
    let started = worker.turn_cancel(&mut entry_a);
    drop(started);
    drop(entry_a);

    // The fetch wrapper turned the dropped op into a rejection, so both
    // requests observed the disconnect instead of hanging.
    let started = worker.turn_poll(&mut entry_b);
    drop(started);
    let response = reply_b
        .try_recv()
        .expect("B answered after A was cancelled")
        .unwrap_or_else(|error| panic!("B must answer, not fail: {error:#}"));
    assert_eq!(response.status, 200);
    let body: serde_json::Value = serde_json::from_slice(&response.body).expect("JSON body");
    assert_eq!(body["outcomeB"], "error:Error", "{body}");
    drop(entry_b);
    let (status, body) = turn_once(&mut worker, "http://test/outcome");
    assert_eq!(status, 200);
    let outcome: serde_json::Value = serde_json::from_slice(&body).expect("JSON body");
    assert_eq!(outcome["outcomeA"], "error:Error", "{outcome}");
    match reply_a
        .try_recv()
        .expect("A answered after its client left")
    {
        Ok(_) => panic!("cancelled A must fail"),
        Err(error) => assert!(format!("{error:#}").contains("disconnected"), "{error:#}"),
    }
}

const SHARED_THEN_TIMER: &str = r#"
const flights = new Map();
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const key = url.searchParams.get("key");
    const then = url.searchParams.get("then");
    if (url.pathname === "/start") {
      const flight = new Promise((resolve) => setTimeout(() => resolve("v"), 20));
      flights.set(key, flight);
      const value = await flight;
      return Response.json({ value });
    }
    if (url.pathname === "/join") {
      const flight = flights.get(key);
      if (!flight) return Response.json({ error: "no-flight" });
      const value = await flight;
      if (then === "timer") await new Promise((resolve) => setTimeout(resolve, 20));
      return Response.json({ value });
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

/// After awaiting a promise another request settled, a request's next timer
/// must belong to that request's driver — not to the entry whose checkpoint
/// resumed it (denoland/celld#316). Adopted by the wrong driver, the timer
/// died with that entry's answer and the waiter hung to its budget.
#[test]
fn shared_promise_waiter_owns_its_next_op() {
    ensure_engine();
    let mut worker = load_worker(SHARED_THEN_TIMER, "shared-then-timer");

    // A starts shared work; B awaits the same promise and holds no op.
    let (mut entry_a, mut reply_a, started_a) =
        begin_suspended(&mut worker, "http://test/start?key=x", 31601);
    assert_eq!(started_a.len(), 1, "A waits on its timer op");
    let (entry_b, reply_b, started_b) =
        begin_suspended(&mut worker, "http://test/join?key=x&then=timer", 31602);
    assert!(started_b.is_empty(), "B waits on A's promise");
    drop(started_b);
    drop(reply_b);

    // Fire the shared timer by driving its future, then deliver to A. One
    // checkpoint resumes A (which answers) and B (which arms its own timer).
    let (id_a, fut_a) = started_a.into_iter().next().unwrap();
    let settle = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("settle runtime")
        .block_on(fut_a);
    let started = worker.turn_deliver(&mut entry_a, id_a, settle);
    assert!(
        started.is_empty(),
        "B's timer must not be adopted by A's driver"
    );
    drop(entry_a);
    drop(entry_b);

    // A answered normally once the shared work finished.
    let response = reply_a
        .try_recv()
        .expect("A answered")
        .unwrap_or_else(|error| panic!("A must answer: {error:#}"));
    assert_eq!(response.status, 200);
}

const STALE_HANDOFF: &str = r#"
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
      gate.then(() => {
        // A timer op, entangled with nothing else. Its owner is this
        // retired event, so the releasing turn cannot hand it over.
        new Promise((resolve) => setTimeout(resolve, 60000)).then(
          () => { outcome = "fired"; },
          (error) => { outcome = "rejected:" + String((error && error.message) || error); },
        );
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

/// An op handed to an event that is already gone must settle instead of
/// vanishing: dropping its resolver left the waiter pending forever with
/// nothing logged (denoland/celld#207). A timer, which the harness fires
/// early when its op rejects, settles its dependents so background loops
/// retired with their starter keep running (denoland/celld#309).
#[test]
fn refused_handoff_settles_instead_of_vanishing() {
    ensure_engine();
    let mut worker = load_worker(STALE_HANDOFF, "stale-handoff");

    // Arm, then retire: dropping the entry retires its token exactly.
    let (status, _) = turn_once(&mut worker, "http://test/arm");
    assert_eq!(status, 200);
    // Release: the stale continuation runs in this turn with a retired
    // token. Its timer op names the dead owner, so the handoff is refused
    // and the promise must settle here rather than hang on a dropped
    // resolver.
    let (status, _) = turn_once(&mut worker, "http://test/release");
    assert_eq!(status, 200);
    let (status, body) = turn_once(&mut worker, "http://test/outcome");
    assert_eq!(status, 200);
    let outcome: serde_json::Value = serde_json::from_slice(&body).expect("JSON body");
    assert_eq!(outcome["outcome"], "fired", "{outcome}");
}

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

const TRAILING_HANDOFF: &str = r#"
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
      gate.then(() => {
        // A timer op, entangled with nothing else. Its owner is this
        // retired event, so a releasing turn hands it to the owner's
        // mailbox — still open here, unlike the refused-handoff pin.
        new Promise((resolve) => setTimeout(resolve, 60000)).then(
          () => { outcome = "fired"; },
          (error) => { outcome = "rejected:" + String((error && error.message) || error); },
        );
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
    return new Response("not found", { status: 404 });
  },
};
"#;

/// A handoff that lands after a finished, non-keeping event's last take
/// must reject instead of dropping silently (denoland/celld#207). The
/// trailing take in both drive loops found work for an answered event with
/// no `waitUntil`, gate, or `io_context` op, and the old branch dropped the
/// futures while `abandon` purged their resolvers unresolved — byte-for-byte
/// the #207 hang signature, reachable by any stale continuation landing
/// during the final wake. The fixed branch rejects each one with the
/// refused-handoff message, so the harness fires the timer early and its
/// dependents settle. This drives the same step the driver runs —
/// `turn_reject_trailing_handed_ops` — through the public API only.
#[test]
fn finished_non_keeping_event_rejects_trailing_handoff() {
    ensure_engine();
    let mut worker = load_worker(TRAILING_HANDOFF, "trailing-handoff");

    // A answers at once and stays un-abandoned, so its mailbox stays open:
    // answered, no background, no gate — finished, keeping nothing. That
    // is the trailing branch's `else` precondition.
    let (mut entry_a, mut reply_a, started_a) =
        begin_suspended(&mut worker, "http://test/arm", 30901);
    assert!(started_a.is_empty(), "A answers without suspending");
    drop(started_a);
    let response = reply_a
        .try_recv()
        .expect("A answered at once")
        .unwrap_or_else(|error| panic!("A must answer: {error:#}"));
    assert_eq!(response.status, 200);
    assert!(entry_a.finished(), "A is finished with nothing outstanding");

    // B's turn runs the stale continuation, whose timer op names the
    // retired owner A. A's mailbox is still open, so the handoff is
    // delivered — landing after the driver's last take, as in the final
    // wake.
    let (mut entry_b, mut reply_b, started_b) =
        begin_suspended(&mut worker, "http://test/release", 30902);
    assert!(started_b.is_empty(), "B answers without suspending");
    drop(started_b);
    let response = reply_b
        .try_recv()
        .expect("B answered at once")
        .unwrap_or_else(|error| panic!("B must answer: {error:#}"));
    assert_eq!(response.status, 200);

    // The driver's trailing step for an event that keeps nothing: take
    // once more inside the turn and reject the handoff instead of dropping
    // it. Without the reject the resolvers are purged unresolved and the
    // waiter's promise pends forever (denoland/celld#207).
    assert!(
        worker.turn_reject_trailing_handed_ops(&mut entry_a),
        "the handoff landed after A's last take"
    );
    entry_a.abandon();
    drop(entry_a);

    // The rejection settles the waiter instead of vanishing: the harness
    // fires the dropped timer early, so its dependents run on the next
    // checkpoint.
    let started = worker.turn_poll(&mut entry_b);
    drop(started);
    drop(entry_b);
    drop(reply_a);
    drop(reply_b);

    let (mut entry_c, mut reply_c, started_c) =
        begin_suspended(&mut worker, "http://test/outcome", 30903);
    assert!(started_c.is_empty(), "outcome answers without suspending");
    drop(started_c);
    let response = reply_c
        .try_recv()
        .expect("outcome answered")
        .unwrap_or_else(|error| panic!("outcome must answer: {error:#}"));
    assert_eq!(response.status, 200);
    let body: serde_json::Value = serde_json::from_slice(&response.body).expect("JSON body");
    assert_eq!(body["outcome"], "fired", "{body}");
    entry_c.abandon();
}

/// A handoff racing the trailing take must be refused at the source, not
/// land after it. The trailing step closes the mailbox inside the turn, so
/// a refed op handed after the take rejects on the handing path — and its
/// waiter settles — instead of sitting in the mailbox until `abandon`
/// purges it unresolved, byte-for-byte the #207 hang signature again.
/// This drives the same step the driver runs, but with the releasing turn
/// after the trailing take rather than before it.
#[test]
fn trailing_take_closes_mailbox_so_late_handoff_refuses() {
    ensure_engine();
    let mut worker = load_worker(TRAILING_HANDOFF, "trailing-handoff-closed");

    // A answers at once and stays un-abandoned, so its mailbox is open:
    // answered, no background, no gate — finished, keeping nothing. That
    // is the trailing branch's precondition.
    let (mut entry_a, mut reply_a, started_a) =
        begin_suspended(&mut worker, "http://test/arm", 30911);
    assert!(started_a.is_empty(), "A answers without suspending");
    drop(started_a);
    let response = reply_a
        .try_recv()
        .expect("A answered at once")
        .unwrap_or_else(|error| panic!("A must answer: {error:#}"));
    assert_eq!(response.status, 200);
    assert!(entry_a.finished(), "A is finished with nothing outstanding");

    // The trailing step with an empty mailbox: nothing to settle, but the
    // mailbox must close here.
    assert!(
        !worker.turn_reject_trailing_handed_ops(&mut entry_a),
        "nothing handed yet"
    );

    // B's turn runs the stale continuation, whose timer op names the
    // retired owner A. The mailbox is closed, so the handoff is refused
    // and the promise rejects here instead of hanging on a purged
    // resolver.
    let (mut entry_b, mut reply_b, started_b) =
        begin_suspended(&mut worker, "http://test/release", 30912);
    assert!(started_b.is_empty(), "B answers without suspending");
    drop(started_b);
    let response = reply_b
        .try_recv()
        .expect("B answered at once")
        .unwrap_or_else(|error| panic!("B must answer: {error:#}"));
    assert_eq!(response.status, 200);

    entry_a.abandon();
    drop(entry_a);

    // The refusal settles the waiter instead of vanishing: the harness
    // fires the rejected timer early, so its dependents run on the next
    // checkpoint.
    let started = worker.turn_poll(&mut entry_b);
    drop(started);
    drop(entry_b);
    drop(reply_a);
    drop(reply_b);

    let (status, body) = turn_once(&mut worker, "http://test/outcome");
    assert_eq!(status, 200);
    let body: serde_json::Value = serde_json::from_slice(&body).expect("JSON body");
    assert_eq!(body["outcome"], "fired", "{body}");
}
