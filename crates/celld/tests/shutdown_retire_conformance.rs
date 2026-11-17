// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// A white-box driver test, not node code: it builds a Worker in-process and
// drives it through the real pool driver, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! A lifecycle cancellation must retire an answered entry with pending
//! `waitUntil` and no native op of its own (denoland/celld#300), instead
//! of spinning on `Wake::Poll` past it until the background budget gives
//! out. The empty-ops branch samples shutdown cancellation before
//! concluding the entry only needs a look-in; a client disconnect no
//! longer matters once answered, so only shutdown interrupts the poll.

use celld::js::{Compat, RequestBody, Worker, WorkerConfig, WorkerConfigOptions};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

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

const SHUTDOWN_RETIRE: &str = r#"
export default {
  async fetch(request, env, ctx) {
    // Answered at once, leaving only `waitUntil` work behind: no native
    // op of its own, background outstanding.
    ctx.waitUntil(new Promise(() => {}));
    return Response.json({ ok: true });
  },
};
"#;

/// A lifecycle cancellation must retire an answered entry with pending
/// `waitUntil` and no native op of its own (denoland/celld#300), instead
/// of spinning on `Wake::Poll` past it until the background budget gives
/// out. Pre-fix this drive ran to the 30 s background budget, so the 20 s
/// deadline below fires; with the fix the abort retires it at once.
#[test]
fn shutdown_cancellation_retires_answered_waituntil_without_ops() {
    ensure_engine();
    // One pool-built isolate serves the entry, so the drive below is the
    // real one — not a hand-driven turn.
    let config = std::sync::Arc::new(WorkerConfig::new(WorkerConfigOptions {
        src: SHUTDOWN_RETIRE.to_string(),
        script_name: "shutdown-retire".to_string(),
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
    }));
    let pool = celld::pool::Pool::new(
        celld::runtime::pool_limits(),
        Duration::from_secs(5),
        Box::new({
            let config = config.clone();
            move || Worker::load_config(config.clone())
        }),
    );

    let (reply, receive) = tokio::sync::oneshot::channel();
    let job = celld::WorkerJob::Fetch {
        queued_at: Instant::now(),
        entrypoint: None,
        invocation_limits: None,
        url: "http://test/work".to_string(),
        method: "GET".to_string(),
        body: RequestBody::Bytes(bytes::Bytes::new()),
        headers: Vec::new(),
        request_id: Some(30001),
        tail_report: None,
        reply,
    };

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("drive runtime")
        .block_on(async {
            let affiliation = pool
                .admit_or_wait(false)
                .await
                .expect("admit the test slot");
            let slot = affiliation.slot().clone();
            let drive = celld::runtime::drive(slot.clone(), job, None);
            tokio::pin!(drive);
            // The answer proves the entry is parked on background alone:
            // settled handler, no native op of its own. Poll the drive
            // while waiting for it.
            celld::asyncrt::select_biased! {
                "a drive that ends before answering wins a tie with the answer";
                _ = &mut drive => panic!("drive ended before answering"),
                response = receive => {
                    let response = response
                        .expect("reply arrives")
                        .unwrap_or_else(|error| panic!("entry must answer: {error:#}"));
                    assert_eq!(response.status, 200);
                }
            }
            // Shut the runtime down under it: the answered entry must
            // retire at once, not at the background budget.
            celld::js::abort_request_for_shutdown(30001);
            tokio::time::timeout(Duration::from_secs(20), drive)
                .await
                .expect("shutdown retires the answered entry promptly");
            drop(affiliation);
        });
}
