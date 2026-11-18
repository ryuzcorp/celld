// Modified by ryuzcorp (ryuzcorp/celld): exec() waits for start(); exec pty/resize; test-only fake exec engine; fake kill completion; exec reads survive their subscribing event; close/signal IoContext lifetime with fake completion; binding-forgery enforcement.
//! Host ops behind `ctx.container`.
//!
//! JavaScript owns the surface and every validation message, copied from
//! workerd's `api/container.c++`; these ops perform the effects through
//! `crate::container`. Each takes the cell scope as its first argument,
//! the way the storage ops do, and an exec process is addressed by the id
//! `__container_exec` returned.

use super::*;
use crate::container::{self, CellContainer, ContainerEngine, ExecParams, StartGuard, StartParams};
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

async fn cell_for(scope: &str) -> Result<(Arc<ContainerEngine>, Arc<CellContainer>), String> {
    let engine = container::engine()
        .await
        .map_err(|error| format!("{error:#}"))?;
    let cell = engine
        .cell(scope)
        .ok_or_else(|| "this Durable Object has no container".to_string())?;
    Ok((engine, cell))
}

fn string_arg(
    scope: &mut v8::PinScope,
    args: &v8::FunctionCallbackArguments,
    index: i32,
) -> String {
    args.get(index).to_rust_string_lossy(scope)
}

fn number_arg(scope: &mut v8::PinScope, args: &v8::FunctionCallbackArguments, index: i32) -> u64 {
    args.get(index).integer_value(scope).unwrap_or(0).max(0) as u64
}

/// The scope this container op may address: the executing cell's own, or
/// a throw. A `Container` forged with another cell's scope reaches this
/// boundary and no further. Scopes the test fake serves
/// (`CELLD_TEST_WAIT=1` only) bypass the check, because the conformance
/// suite drives them from stateless events with no executing cell.
fn own_cell_container(
    scope: &mut v8::PinScope,
    operation: &str,
    cell: String,
) -> Result<String, ()> {
    if test_wait_enabled() && test_execs().lock().unwrap().scopes.contains(&cell) {
        return Ok(cell);
    }
    if let Err(error) = super::binding_policy::require_executing_cell(scope, &cell) {
        loader_throw(scope, &format!("{operation}: {error}"));
        return Err(());
    }
    Ok(cell)
}

pub(super) fn op_container_running(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let Ok(cell) = own_cell_container(scope, "running", cell) else {
        return;
    };
    if test_wait_enabled() && test_execs().lock().unwrap().scopes.contains(&cell) {
        rv.set(v8::Boolean::new(scope, true).into());
        return;
    }
    let running = container::engine_if_ready()
        .and_then(|engine| engine.cell(&cell))
        .is_some_and(|cell| cell.running());
    rv.set(v8::Boolean::new(scope, running).into());
}

/// `host:port` the node dials for `getTcpPort(port)`, or a throw.
pub(super) fn op_container_address(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let Ok(cell) = own_cell_container(scope, "getTcpPort", cell) else {
        return;
    };
    let port = number_arg(scope, &args, 1) as u16;
    let address = container::engine_if_ready()
        .and_then(|engine| engine.cell(&cell))
        .ok_or_else(|| "this Durable Object has no container".to_string())
        .and_then(|cell| cell.address(port));
    match address {
        Ok(address) => rv.set(v8::String::new(scope, &address).unwrap().into()),
        Err(message) => loader_throw(scope, &message),
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartArgs {
    entrypoint: Option<Vec<String>>,
    #[serde(default)]
    env: Vec<(String, String)>,
    #[serde(default)]
    enable_internet: bool,
    #[serde(default)]
    labels: Vec<(String, String)>,
}

pub(super) fn op_container_start(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let Ok(cell) = own_cell_container(scope, "start()", cell) else {
        return;
    };
    let raw = string_arg(scope, &args, 1);
    let request: StartArgs = match serde_json::from_str(&raw) {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &format!("start(): {error}")),
    };
    // The run opens here, on the calling thread, so the monitor() that
    // follows this call in the same turn waits for this start's exit.
    let opened = container::engine_if_ready()
        .and_then(|engine| engine.cell(&cell))
        // Guarded from the moment it opens: whatever ends the task below
        // (done, failed, or cancelled and dropped, even before it first
        // runs), the run counts as finished, or an exec() waiting on it
        // would wait forever.
        .map(|cell| StartGuard::new(cell.clone(), cell.begin_run()));
    let async_id = asyncrt::enqueue_io_context(async move {
        let (engine, cell) = cell_for(&cell).await?;
        // The run belongs to the cell it was opened on. If that cell was
        // evicted and the scope came back as a new one, release the old
        // run and open one here, or the new cell's exec() would see a
        // foreign run number as started.
        let guard = match opened {
            Some(opened) if Arc::ptr_eq(opened.cell(), &cell) => opened,
            // Dropping `released` releases the run opened on a cell that
            // was evicted since; this cell gets a run of its own.
            released => {
                drop(released);
                StartGuard::new(cell.clone(), cell.begin_run())
            }
        };
        let run = guard.run();
        engine
            .start(
                &cell,
                run,
                StartParams {
                    entrypoint: request.entrypoint,
                    env: request.env,
                    enable_internet: request.enable_internet,
                    labels: request.labels,
                },
            )
            .await
            .map_err(|error| format!("{error:#}"))?;
        drop(guard);
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

/// Resolves with the root process's exit code as text once the current
/// run ends. Unrefed: a handler can await it, but once the handler has
/// answered it cannot keep the event, and so the cell, alive. A drain
/// would otherwise wait on a promise that settles only when the container
/// exits. After the event, `running` reports the host's state.
pub(super) fn op_container_monitor(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let Ok(cell) = own_cell_container(scope, "monitor()", cell) else {
        return;
    };
    let run = container::engine_if_ready()
        .and_then(|engine| engine.cell(&cell))
        .map_or(0, |cell| cell.current_run());
    let async_id = asyncrt::enqueue_unrefed(async move {
        let (engine, cell) = cell_for(&cell).await?;
        let code = engine.monitor(&cell, run).await?;
        Ok::<String, String>(code.to_string())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_destroy(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let Ok(cell) = own_cell_container(scope, "destroy()", cell) else {
        return;
    };
    let async_id = asyncrt::enqueue(async move {
        let (engine, cell) = cell_for(&cell).await?;
        engine
            .destroy(&cell)
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_signal(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let Ok(cell) = own_cell_container(scope, "signal()", cell) else {
        return;
    };
    let signal = number_arg(scope, &args, 1) as u32;
    if test_wait_enabled() && test_execs().lock().unwrap().scopes.contains(&cell) {
        // Recorded synchronously, like the kill dispatch; the completion
        // records asynchronously, through an op of the same lifetime as
        // the real signal's, so the delivery suite observes delivery.
        test_execs()
            .lock()
            .unwrap()
            .signals
            .entry(cell.clone())
            .or_default()
            .push(signal);
        let async_id = asyncrt::enqueue_io_context(async move {
            tokio::task::yield_now().await;
            test_execs()
                .lock()
                .unwrap()
                .signals_completed
                .entry(cell)
                .or_default()
                .push(signal);
            Ok::<String, String>(String::new())
        });
        rv.set(promise_for(scope, async_id));
        return;
    }
    // IoContext, like `start()` and `kill()`: `signal()` returns void and
    // nobody awaits it, so the op is still pending when the handler
    // answers; on a stateless or failed event any shorter lifetime dies
    // with it before the daemon call runs, and the container is never
    // signaled. (A successful cell event already keeps Handler work.)
    // The event waits out the one daemon call (milliseconds), not the
    // container.
    let async_id = asyncrt::enqueue_io_context(async move {
        let (engine, cell) = cell_for(&cell).await?;
        engine
            .signal(&cell, signal)
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_inactivity(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let Ok(cell) = own_cell_container(scope, "setInactivityTimeout()", cell) else {
        return;
    };
    let duration = number_arg(scope, &args, 1);
    let async_id = asyncrt::enqueue(async move {
        let (_, cell) = cell_for(&cell).await?;
        cell.set_inactivity(Duration::from_millis(duration));
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

#[derive(serde::Deserialize)]
struct ExecArgs {
    cmd: Vec<String>,
    #[serde(default)]
    env: Vec<(String, String)>,
    cwd: Option<String>,
    user: Option<String>,
    #[serde(default)]
    combined: bool,
    #[serde(default)]
    pty: bool,
    cols: Option<u16>,
    rows: Option<u16>,
}

pub(super) fn op_container_exec(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let cell = string_arg(scope, &args, 0);
    let Ok(cell) = own_cell_container(scope, "exec()", cell) else {
        return;
    };
    let raw = string_arg(scope, &args, 1);
    let request: ExecArgs = match serde_json::from_str(&raw) {
        Ok(request) => request,
        Err(error) => return loader_throw(scope, &format!("exec(): {error}")),
    };
    // Ops reach the harness as closure params, not globals, so tenant code
    // cannot call this directly; validate here as defense-in-depth against
    // harness bugs. The boundary is wrapper validation plus the handle's
    // private id. A size rides only with a PTY, as a pair, and both
    // dimensions are positive.
    let (cols, rows) = match (request.pty, request.cols, request.rows) {
        (false, None, None) => (None, None),
        (false, _, _) => return loader_throw(scope, "exec(): pty size without pty"),
        (true, None, None) => (None, None),
        (true, Some(cols), Some(rows)) if cols != 0 && rows != 0 => (Some(cols), Some(rows)),
        _ => return loader_throw(scope, "exec(): pty size must be positive cols and rows"),
    };
    // Past validation, like the engine path: invalid params still throw.
    if test_wait_enabled() {
        let mut execs = test_execs().lock().unwrap();
        if execs.scopes.contains(&cell) {
            if execs.processes.len() >= TEST_EXEC_CAP {
                return loader_throw(scope, "too many test execs");
            }
            let id = execs.next_id;
            execs.next_id += 1;
            let pid = execs.next_pid;
            execs.next_pid += 1;
            execs.processes.insert(
                id,
                TestExecProcess {
                    scope: cell.clone(),
                    params: raw.clone(),
                    kills: Vec::new(),
                    kills_completed: Vec::new(),
                    closes: 0,
                    closes_completed: 0,
                    exit: None,
                    waiters: Vec::new(),
                },
            );
            let response = serde_json::json!({
                "id": id,
                "pid": pid,
                "isPty": request.pty,
            })
            .to_string();
            let value = v8::String::new(scope, &response)
                .map(v8::Local::<v8::Value>::from)
                .unwrap_or_else(|| v8::undefined(scope).into());
            match resolved_promise(scope, value) {
                Ok(promise) => rv.set(promise.into()),
                Err(_) => loader_throw(scope, "could not create exec promise"),
            }
            return;
        }
    }
    let async_id = asyncrt::enqueue(async move {
        let (engine, cell) = cell_for(&cell).await?;
        let process = engine
            .exec(
                &cell,
                ExecParams {
                    cmd: request.cmd,
                    env: request.env,
                    cwd: request.cwd,
                    user: request.user,
                    combined: request.combined,
                    pty: request.pty,
                    cols,
                    rows,
                },
            )
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok::<String, String>(
            serde_json::json!({
                "id": process.id,
                "pid": process.pid,
                "isPty": process.is_pty,
            })
            .to_string(),
        )
    });
    rv.set(promise_for(scope, async_id));
}

// The process read/write/wait/resize ops below are unrefed like
// `monitor()`: a handler awaits them, and a process left running when the
// handler answers does not pin the cell. `kill()` and stdin `close()`
// return void (or fire without a handle), so they run under the start's
// IoContext lifetime instead: any shorter lifetime dies with the event
// before the effect lands.
fn process_arg(
    scope: &mut v8::PinScope,
    args: &v8::FunctionCallbackArguments,
) -> Result<Arc<container::ExecProcess>, String> {
    let id = number_arg(scope, args, 0);
    let process = container::process(id).ok_or_else(|| "the process is closed".to_string())?;
    // Another cell's process answers as a closed one: a distinct refusal
    // would turn a sequential id into a liveness oracle.
    if super::binding_policy::require_executing_cell(scope, process.owner_scope()).is_err() {
        return Err("the process is closed".to_string());
    }
    Ok(process)
}

/// One chunk of stdout (1) or stderr (2); an empty chunk is the end.
/// `lease` names the read and `prev_sub`/`my_sub` chain this
/// subscription after the previous one; a subscription that arrives
/// after another already acted for the lease rejects with the
/// superseded message, which the harness swallows.
pub(super) fn op_container_exec_read(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if test_exec_hit(number_arg(scope, &args, 0)) {
        // A fake process has no output: reads end at once.
        let async_id = asyncrt::enqueue_unrefed(async move { Ok::<Vec<u8>, String>(Vec::new()) });
        rv.set(promise_for(scope, async_id));
        return;
    }
    let process = process_arg(scope, &args);
    let which = number_arg(scope, &args, 1) as u8;
    let lease = number_arg(scope, &args, 2);
    let prev_sub = number_arg(scope, &args, 3);
    let my_sub = number_arg(scope, &args, 4);
    let async_id = asyncrt::enqueue_unrefed(async move {
        let process = process?;
        match process.read_retained(which, lease, prev_sub, my_sub).await {
            container::ExecRead::Bytes(bytes) => Ok::<Vec<u8>, String>(bytes.to_vec()),
            container::ExecRead::Superseded => Err(container::EXEC_READ_SUPERSEDED.to_string()),
        }
    });
    rv.set(promise_for(scope, async_id));
}

/// `__exec_read_seq()` -> a fresh retained-read sequence number.
pub(super) fn op_exec_read_seq(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    rv.set(v8::Number::new(scope, container::next_exec_read_seq() as f64).into());
}

pub(super) fn op_container_exec_write(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if test_exec_hit(number_arg(scope, &args, 0)) {
        if view_bytes(args.get(1)).is_none() {
            return loader_throw(scope, "stdin write needs bytes");
        }
        test_exec_ok(scope, rv);
        return;
    }
    let process = process_arg(scope, &args);
    let Some(bytes) = view_bytes(args.get(1)) else {
        return loader_throw(scope, "stdin write needs bytes");
    };
    let async_id = asyncrt::enqueue_unrefed(async move {
        process?.write(&bytes).await?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_exec_close(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = number_arg(scope, &args, 0);
    if test_exec_hit(id) {
        // The dispatch records synchronously and the completion through
        // an op of the same lifetime as the real close's, like the kill
        // pair: the delivery suite fires close() without awaiting it (the
        // stdin:none path has no handle to await) and then observes
        // whether the op ran at all.
        if let Some(process) = test_execs().lock().unwrap().processes.get_mut(&id) {
            process.closes += 1;
        }
        let async_id = asyncrt::enqueue_io_context(async move {
            tokio::task::yield_now().await;
            if let Some(process) = test_execs().lock().unwrap().processes.get_mut(&id) {
                process.closes_completed += 1;
            }
            Ok::<String, String>(String::new())
        });
        rv.set(promise_for(scope, async_id));
        return;
    }
    let process = process_arg(scope, &args);
    // IoContext, like `start()` and `kill()`: the stdin:none path fires
    // the close without awaiting it (there is no handle to await), so the
    // op is still pending when the handler answers; any shorter lifetime
    // dies with the event before stdin closes, and a process reading
    // stdin to EOF hangs forever. The event waits out the local
    // half-close (microseconds), not the process.
    let async_id = asyncrt::enqueue_io_context(async move {
        process?.close_stdin().await;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_exec_wait(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = number_arg(scope, &args, 0);
    if test_exec_hit(id) {
        // A fired exit stored like an engine exit code, so a subscriber
        // arriving after the fire resolves at once.
        let (tx, rx) = tokio::sync::oneshot::channel();
        let immediate = {
            let mut execs = test_execs().lock().unwrap();
            match execs.processes.get_mut(&id) {
                Some(process) if process.exit.is_some() => process.exit.clone(),
                Some(process) => {
                    process.waiters.push(tx);
                    None
                }
                None => return loader_throw(scope, "the process is closed"),
            }
        };
        if let Some(code) = immediate {
            let value = v8::String::new(scope, &code)
                .map(v8::Local::<v8::Value>::from)
                .unwrap_or_else(|| v8::undefined(scope).into());
            match resolved_promise(scope, value) {
                Ok(promise) => rv.set(promise.into()),
                Err(_) => loader_throw(scope, "could not create wait promise"),
            }
            return;
        }
        let async_id = asyncrt::enqueue_unrefed(async move {
            let _guard = TestExecWaitGuard { id };
            Ok(rx.await.unwrap_or_default())
        });
        rv.set(promise_for(scope, async_id));
        return;
    }
    let process = process_arg(scope, &args);
    let async_id = asyncrt::enqueue_unrefed(async move {
        let code = process?
            .wait()
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok::<String, String>(code.to_string())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_exec_kill(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let id = number_arg(scope, &args, 0);
    // The harness checks the range too; the op checks it again because
    // the number is interpolated into the kill scan.
    let signal = number_arg(scope, &args, 1);
    if !(1..=64).contains(&signal) {
        return loader_throw(scope, "kill(): signal must be 1..=64");
    }
    let signal = signal as u32;
    if test_exec_hit(id) {
        // Recorded synchronously: the abort dispatch and the op run on
        // the same thread, so the suite observes the kill without
        // yielding.
        if let Some(process) = test_execs().lock().unwrap().processes.get_mut(&id) {
            process.kills.push(signal);
        }
        // The completion records asynchronously, through an op of the same
        // lifetime as the real kill's: the delivery suite fires kill()
        // without awaiting it and then observes whether the op ran at
        // all. The yield mirrors the real op's suspension on daemon I/O:
        // a dropped op never reaches the record.
        let async_id = asyncrt::enqueue_io_context(async move {
            tokio::task::yield_now().await;
            if let Some(process) = test_execs().lock().unwrap().processes.get_mut(&id) {
                process.kills_completed.push(signal);
            }
            Ok::<String, String>(String::new())
        });
        rv.set(promise_for(scope, async_id));
        return;
    }
    let process = process_arg(scope, &args);
    // IoContext, like `start()`: `kill()` returns void and nobody
    // awaits it, so the op is still pending when the handler answers;
    // any shorter lifetime dies with the event before the daemon call
    // runs, and the process is never signaled. The event waits out the
    // two daemon calls (milliseconds), not the process.
    let async_id = asyncrt::enqueue_io_context(async move {
        // `kill()` returns nothing to the caller, so a failed delivery
        // (an image without `sh`, a daemon error) is logged here or not
        // at all.
        process?.kill(signal).await.map_err(|error| {
            tracing::warn!(event = "container_exec_kill_failed", signal, error = %format!("{error:#}"), "exec kill was not delivered");
            format!("{error:#}")
        })?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_exec_resize(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let process = process_arg(scope, &args);
    let cols = number_arg(scope, &args, 1);
    let rows = number_arg(scope, &args, 2);
    let (Ok(cols), Ok(rows)) = (u16::try_from(cols), u16::try_from(rows)) else {
        return loader_throw(scope, "resize(): cols and rows must be 1..=65535");
    };
    if cols == 0 || rows == 0 {
        return loader_throw(scope, "resize(): cols and rows must be 1..=65535");
    }
    if test_exec_hit(number_arg(scope, &args, 0)) {
        test_exec_ok(scope, rv);
        return;
    }
    let async_id = asyncrt::enqueue_unrefed(async move {
        process?
            .resize(cols, rows)
            .await
            .map_err(|error| format!("{error:#}"))?;
        Ok::<String, String>(String::new())
    });
    rv.set(promise_for(scope, async_id));
}

pub(super) fn op_container_exec_drop(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    let id = number_arg(scope, &args, 0);
    if test_exec_hit(id) {
        test_execs().lock().unwrap().processes.remove(&id);
        return;
    }
    // Dropping another cell's process would be a cross-cell effect;
    // silently keep what is not ours, as for an unknown id.
    if let Some(process) = container::process(id) {
        if super::binding_policy::require_executing_cell(scope, process.owner_scope()).is_err() {
            return;
        }
    }
    container::drop_process(id);
}

// ---- Test-only fake exec engine ------------------------------------------
// `CELLD_TEST_WAIT=1` only (the exec-signal conformance suite): the test
// environment has no container engine -- image resolution needs Docker,
// so a fixture cannot even declare `containers[]` -- and this table
// stands in for it beneath the real `Container.prototype.exec` path. A
// scope the suite attaches reports running, and its execs mint fake
// processes whose kills, params, and exit the suite drives. Fake ids
// start at 2^40: exactly representable through the JSON round trip, and
// unreachable by the engine's sequential ids. Every branch above throws
// or stays dark unless the flag is set, so production tenants cannot
// reach the fake.
const TEST_EXEC_ID_BASE: u64 = 1 << 40;
const TEST_EXEC_CAP: usize = 64;

struct TestExecProcess {
    scope: String,
    /// The params JSON as the op received it; the suite asserts the
    /// AbortSignal never crosses into it.
    params: String,
    kills: Vec<u32>,
    /// Kills whose op ran to completion (past the yield). A dropped op
    /// records in `kills` but never here.
    kills_completed: Vec<u32>,
    /// Stdin closes dispatched, and closes whose op ran to completion
    /// (past the yield). A dropped op counts in `closes` but never in
    /// `closes_completed`.
    closes: u32,
    closes_completed: u32,
    exit: Option<String>,
    waiters: Vec<tokio::sync::oneshot::Sender<String>>,
}

struct TestExecs {
    scopes: HashSet<String>,
    next_id: u64,
    next_pid: i64,
    processes: HashMap<u64, TestExecProcess>,
    /// Container signals dispatched per scope, and signals whose op ran
    /// to completion (past the yield). A dropped op records in `signals`
    /// but never in `signals_completed`.
    signals: HashMap<String, Vec<u32>>,
    signals_completed: HashMap<String, Vec<u32>>,
}

fn test_execs() -> &'static Mutex<TestExecs> {
    static TEST_EXECS: OnceLock<Mutex<TestExecs>> = OnceLock::new();
    TEST_EXECS.get_or_init(|| {
        Mutex::new(TestExecs {
            scopes: HashSet::new(),
            next_id: TEST_EXEC_ID_BASE,
            next_pid: 1000,
            processes: HashMap::new(),
            signals: HashMap::new(),
            signals_completed: HashMap::new(),
        })
    })
}

/// Whether an exec id serves from the fake table. Dark unless the flag
/// is set; anything else falls through to the engine registry.
fn test_exec_hit(id: u64) -> bool {
    test_wait_enabled() && test_execs().lock().unwrap().processes.contains_key(&id)
}

/// Resolve a fake exec op with the empty string its engine twin answers.
fn test_exec_ok(scope: &mut v8::PinScope, mut rv: v8::ReturnValue<v8::Value>) {
    let value = v8::String::new(scope, "")
        .map(v8::Local::<v8::Value>::from)
        .unwrap_or_else(|| v8::undefined(scope).into());
    match resolved_promise(scope, value) {
        Ok(promise) => rv.set(promise.into()),
        Err(_) => loader_throw(scope, "could not create test exec promise"),
    }
}

/// Drop one fake-exec waiter when its op is dropped (event retirement
/// cancels the future), so a fired exit is not sent into a dead channel
/// and the waiter list cannot grow without bound.
struct TestExecWaitGuard {
    id: u64,
}

impl Drop for TestExecWaitGuard {
    fn drop(&mut self) {
        if let Some(process) = test_execs().lock().unwrap().processes.get_mut(&self.id) {
            process.waiters.retain(|waiter| !waiter.is_closed());
        }
    }
}

/// `__testexec_attach(scope)`; mark one scope as fake-running, so the
/// real exec path runs against the fake table. Throws unless
/// `CELLD_TEST_WAIT=1`.
pub(super) fn op_testexec_attach(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test execs are disabled on this node");
    }
    let cell = string_arg(scope, &args, 0);
    if cell.is_empty() || cell.len() > 256 {
        return loader_throw(scope, "test exec scope must be 1..=256 bytes");
    }
    let mut execs = test_execs().lock().unwrap();
    if execs.scopes.len() >= TEST_EXEC_CAP && !execs.scopes.contains(&cell) {
        return loader_throw(scope, "too many test exec scopes");
    }
    execs.scopes.insert(cell);
}

/// `__testexec_ids(scope)` -> JSON array of the scope's fake exec ids, in
/// spawn order. Throws unless `CELLD_TEST_WAIT=1`.
pub(super) fn op_testexec_ids(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test execs are disabled on this node");
    }
    let cell = string_arg(scope, &args, 0);
    let execs = test_execs().lock().unwrap();
    let mut ids: Vec<u64> = execs
        .processes
        .iter()
        .filter(|(_, process)| process.scope == cell)
        .map(|(id, _)| *id)
        .collect();
    ids.sort_unstable();
    let ids = serde_json::json!(ids).to_string();
    rv.set(v8::String::new(scope, &ids).unwrap().into());
}

/// `__testexec_exit(id, code)`; fire one fake exit, resolving its current
/// subscribers and storing the code for late ones (first fire wins, like
/// a stored engine exit code). Throws unless `CELLD_TEST_WAIT=1`.
pub(super) fn op_testexec_exit(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test execs are disabled on this node");
    }
    let id = number_arg(scope, &args, 0);
    let code = number_arg(scope, &args, 1);
    if code > 255 {
        return loader_throw(scope, "test exec exit code must be 0..=255");
    }
    let mut execs = test_execs().lock().unwrap();
    let Some(process) = execs.processes.get_mut(&id) else {
        return loader_throw(scope, "unknown test exec");
    };
    if process.exit.is_some() {
        return;
    }
    let code = code.to_string();
    process.exit = Some(code.clone());
    for waiter in process.waiters.drain(..) {
        let _ = waiter.send(code.clone());
    }
}

/// `__testexec_kills(id)` -> JSON array of the signals the fake process
/// received, in order. Throws unless `CELLD_TEST_WAIT=1`.
pub(super) fn op_testexec_kills(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test execs are disabled on this node");
    }
    let id = number_arg(scope, &args, 0);
    let execs = test_execs().lock().unwrap();
    let Some(process) = execs.processes.get(&id) else {
        return loader_throw(scope, "unknown test exec");
    };
    let kills = serde_json::json!(process.kills).to_string();
    rv.set(v8::String::new(scope, &kills).unwrap().into());
}

/// `__testexec_kills_completed(id)` -> JSON array of the signals whose
/// kill op ran to completion, in order. Throws unless
/// `CELLD_TEST_WAIT=1`.
pub(super) fn op_testexec_kills_completed(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test execs are disabled on this node");
    }
    let id = number_arg(scope, &args, 0);
    let execs = test_execs().lock().unwrap();
    let Some(process) = execs.processes.get(&id) else {
        return loader_throw(scope, "unknown test exec");
    };
    let kills = serde_json::json!(process.kills_completed).to_string();
    rv.set(v8::String::new(scope, &kills).unwrap().into());
}

/// `__testexec_closes(id)` -> the stdin closes dispatched on the fake
/// process. Throws unless `CELLD_TEST_WAIT=1`.
pub(super) fn op_testexec_closes(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test execs are disabled on this node");
    }
    let id = number_arg(scope, &args, 0);
    let execs = test_execs().lock().unwrap();
    let Some(process) = execs.processes.get(&id) else {
        return loader_throw(scope, "unknown test exec");
    };
    let closes = serde_json::json!(process.closes).to_string();
    rv.set(v8::String::new(scope, &closes).unwrap().into());
}

/// `__testexec_closes_completed(id)` -> the stdin closes whose op ran to
/// completion. Throws unless `CELLD_TEST_WAIT=1`.
pub(super) fn op_testexec_closes_completed(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test execs are disabled on this node");
    }
    let id = number_arg(scope, &args, 0);
    let execs = test_execs().lock().unwrap();
    let Some(process) = execs.processes.get(&id) else {
        return loader_throw(scope, "unknown test exec");
    };
    let closes = serde_json::json!(process.closes_completed).to_string();
    rv.set(v8::String::new(scope, &closes).unwrap().into());
}

/// `__testexec_signals(scope)` -> JSON array of the container signals
/// dispatched on the scope, in order. Throws unless
/// `CELLD_TEST_WAIT=1`.
pub(super) fn op_testexec_signals(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test execs are disabled on this node");
    }
    let cell = string_arg(scope, &args, 0);
    let execs = test_execs().lock().unwrap();
    let empty = Vec::new();
    let signals = execs.signals.get(&cell).unwrap_or(&empty);
    let signals = serde_json::json!(signals).to_string();
    rv.set(v8::String::new(scope, &signals).unwrap().into());
}

/// `__testexec_signals_completed(scope)` -> JSON array of the container
/// signals whose op ran to completion, in order. Throws unless
/// `CELLD_TEST_WAIT=1`.
pub(super) fn op_testexec_signals_completed(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test execs are disabled on this node");
    }
    let cell = string_arg(scope, &args, 0);
    let execs = test_execs().lock().unwrap();
    let empty = Vec::new();
    let signals = execs.signals_completed.get(&cell).unwrap_or(&empty);
    let signals = serde_json::json!(signals).to_string();
    rv.set(v8::String::new(scope, &signals).unwrap().into());
}

/// `__testexec_params(id)` -> the params JSON the fake exec received.
/// Throws unless `CELLD_TEST_WAIT=1`.
pub(super) fn op_testexec_params(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test execs are disabled on this node");
    }
    let id = number_arg(scope, &args, 0);
    let execs = test_execs().lock().unwrap();
    let Some(process) = execs.processes.get(&id) else {
        return loader_throw(scope, "unknown test exec");
    };
    rv.set(v8::String::new(scope, &process.params).unwrap().into());
}

/// `__testproc_spawn()` -> process id; a test-only in-memory exec process
/// the test feeds itself through the real process table. Throws unless
/// `CELLD_TEST_WAIT=1`.
pub(super) fn op_testproc_spawn(
    scope: &mut v8::PinScope,
    _args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test procs are disabled on this node");
    }
    // The fake joins the real process table, so it records its owner like
    // a real exec: the read and drop ops answer only the spawning cell.
    let Some(context) = event_context(scope) else {
        return loader_throw(scope, "test procs need a running event");
    };
    let Some(frame) = current_cell_event(&context) else {
        return loader_throw(scope, "test procs need a running cell event");
    };
    let id = container::test_proc_spawn(&frame.storage);
    rv.set(v8::Number::new(scope, id as f64).into());
}

/// `__testproc_emit(id, which, bytes)`; push one chunk into a test-only
/// in-memory exec pipe. Throws unless `CELLD_TEST_WAIT=1`.
pub(super) fn op_testproc_emit(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test procs are disabled on this node");
    }
    let id = number_arg(scope, &args, 0);
    let which = number_arg(scope, &args, 1) as u8;
    if which != 1 && which != 2 {
        return loader_throw(scope, "test proc pipe must be 1 (stdout) or 2 (stderr)");
    }
    let Some(bytes) = view_bytes(args.get(2)) else {
        return loader_throw(scope, "test proc emit needs bytes");
    };
    if bytes.len() > 1024 * 1024 {
        return loader_throw(scope, "test proc chunk must be 0..=1048576 bytes");
    }
    if let Err(error) = container::test_proc_emit(id, which, bytes.into()) {
        loader_throw(scope, &error);
    }
}

/// `__testproc_eof(id, which)`; end one test-only in-memory exec pipe.
/// Throws unless `CELLD_TEST_WAIT=1`.
pub(super) fn op_testproc_eof(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    _rv: v8::ReturnValue<v8::Value>,
) {
    if !test_wait_enabled() {
        return loader_throw(scope, "test procs are disabled on this node");
    }
    let id = number_arg(scope, &args, 0);
    let which = number_arg(scope, &args, 1) as u8;
    if which != 1 && which != 2 {
        return loader_throw(scope, "test proc pipe must be 1 (stdout) or 2 (stderr)");
    }
    container::test_proc_eof(id, which);
}
