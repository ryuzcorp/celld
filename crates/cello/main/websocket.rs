// Modified by Ryuz Corp (Cello): WebSocket ownership; isolate pipe for loaded-worker 101 joins (upstream #305). Apache-2.0.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! WebSockets on the ingress: accepting them, proxying them to the owner,
//! and the tasks that pump each one.
//!
//! A socket outlives the request that opened it, so each becomes its own
//! task. Three shapes exist — a local socket to a cell on this node, a
//! socket proxied to the owning peer, and a socket the worker opened
//! outbound — and they differ only in what sits on the far end.
use super::*;
use cello::isolate_pipe::{
    forward_isolate_direction, forward_pipe_direction, PipeDirectionEnd, PipeSendOutcome,
    ISOLATE_PIPE_MAX_STAGED_BYTES,
};
use std::future::Future;

/// Where the events of one socket go.
///
/// Resolved once, when the socket's task starts, and passed to every event of
/// the socket. The core's cancel path closes a socket and unregisters it
/// before the socket task dispatches the close, so a route read again at
/// that point would find no facet record and name the facet's scope as a
/// cell. Routing a request to that scope starts an activation for a cell
/// that no Worker exports.
#[derive(Clone)]
enum SocketRoute {
    /// A socket that a cell accepted.
    Cell(String),
    /// A socket that a Durable Object facet accepted. Routing, activity, and
    /// the output gate belong to the root cell. Only the handler runs in
    /// the facet.
    Facet {
        scope: String,
        facet: cello::js::FacetSocket,
    },
}

impl SocketRoute {
    /// Read the route of `target` while it is still registered.
    fn of(target: &cello::js::WsTarget) -> Self {
        match cello::js::ws_facet(target.id) {
            Some(facet) => Self::Facet {
                scope: target.scope.clone(),
                facet,
            },
            None => Self::Cell(target.scope.clone()),
        }
    }

    /// The scope that accepted the socket: the cell, or the facet.
    fn scope(&self) -> &str {
        match self {
            Self::Cell(cell) => cell,
            Self::Facet { scope, .. } => scope,
        }
    }

    /// The cell that owns the socket.
    fn cell(&self) -> &str {
        match self {
            Self::Cell(cell) => cell,
            Self::Facet { facet, .. } => &facet.root,
        }
    }
}

async fn dispatch_ws_message(
    app: &AppHandle,
    socket: &SocketRoute,
    ws_id: u64,
    data: cello::js::WsIn,
) -> anyhow::Result<()> {
    let scope = socket.scope();
    // The auto-response short circuit: a matched text frame is answered here
    // in the shell and never becomes a `webSocketMessage`. No routing, no
    // activity, no wake — a hibernated cell stays hibernated, which is the
    // feature.
    if let cello::js::WsIn::Text(text) = &data {
        if let Some(response) = cello::js::ws_auto_response(scope, ws_id, text) {
            cello::js::ws_emit_batch(vec![(ws_id, cello::js::WsOut::Text(response))]);
            return Ok(());
        }
    }
    let Routed { request, route } = app
        .websocket_request(socket.cell().to_string(), ws_id)
        .await
        .map_err(|error| anyhow::anyhow!("route WebSocket {scope}: {error:?}"))?;
    anyhow::ensure!(route == Route::Local, "WebSocket owner moved off node");
    // The handler's output must reach the gate even when the caller stops
    // waiting. A socket task stops reading, and drops this future, once its own
    // socket closes, but the handler it started runs on regardless. A dropped
    // reply loses a captured batch that already reserved places on other
    // sockets, and a lost batch must close those sockets with 1011 because the
    // gate never answered for it. A task, which a dropped handle only
    // detaches, therefore owns everything from the handoff on. Routing stays
    // above it: a message the cell never received has no output to lose.
    let app = app.clone();
    let scope = scope.to_string();
    let socket = socket.clone();
    let (started, receive) = tokio::sync::oneshot::channel();
    let delivery = cello::asyncrt::spawn(async move {
        deliver_ws_message(&app, &socket, request, ws_id, data, started).await
    });
    // The reader waits only until the first turn. Keep observing completion:
    // ignoring the detached task would hide failures and leave a broken
    // connection open. The delivery still owns its activity and output gate
    // handoff even if this socket closes while the handler is suspended.
    cello::asyncrt::spawn(async move {
        let failure = match delivery.await {
            Ok(Ok(())) => return,
            Ok(Err(error)) => format!("{error:#}"),
            Err(panic) => format!("WebSocket message delivery panicked: {panic}"),
        };
        tracing::warn!(scope, ws_id, %failure, "WebSocket message delivery failed");
        cello::js::ws_close(ws_id, 1011, "WebSocket message delivery failed");
    })
    .detach();
    receive.await.context("WebSocket message did not start")
}

/// Run `webSocketMessage` and hand its output to the cell's barrier queue.
async fn deliver_ws_message(
    app: &AppHandle,
    socket: &SocketRoute,
    request: u64,
    ws_id: u64,
    data: cello::js::WsIn,
    started: tokio::sync::oneshot::Sender<()>,
) -> anyhow::Result<()> {
    let scope = socket.cell();
    let activity = app.activity(request, scope.to_string());
    let runtime = app.runtime.as_ref().context("no cell runtime")?;
    let dispatched = match socket {
        SocketRoute::Cell(cell) => runtime.ws_message(cell.clone(), ws_id, data, started).await,
        SocketRoute::Facet { scope, facet } => {
            runtime
                .facet_ws_message(scope.clone(), facet, ws_id, data, started)
                .await
        }
    };
    let dispatch = match dispatched {
        Ok(dispatch) => dispatch,
        // A handler that failed after it committed opens the barrier a
        // successful writer's batch opens, with no frames behind it: the
        // commit is as unproven either way, and a read-only batch or response
        // that followed would otherwise trail nothing and reveal it (#715).
        // The frames it captured before it failed are dropped, as before.
        //
        // A facet's failed handler opens no barrier. Its position counts in
        // the facet's stream, which the root cell's gate would read as its
        // own, and the facet's next output proves that whole stream anyway.
        Err(error) => {
            let position = match socket {
                SocketRoute::Cell(_) => cello::js::failed_write_position(&error),
                SocketRoute::Facet { .. } => None,
            };
            if let Some(position) = position {
                // The barrier is registered before the activity guard
                // drops: the core reads the still-pinned request when it
                // opens the barrier, and a guard dropped first would let
                // the cell be released under an unregistered write.
                if let Err(stopped) = app
                    .ws_output(request, scope.to_string(), Vec::new(), Some(position), None)
                    .await
                {
                    tracing::warn!(scope, position, %stopped, "no barrier for a failed webSocketMessage handler's write");
                }
            }
            drop(activity);
            return Err(error);
        }
    };
    // A read-only frame can reveal another event's unproven write, so all
    // captured frames pass through the cell's barrier queue before emission.
    if !dispatch.frames.is_empty() || dispatch.write_position.is_some() {
        if let Err(stopped) = app
            .ws_output(
                request,
                scope.to_string(),
                dispatch.frames,
                dispatch.write_position,
                dispatch.observed_position,
            )
            .await
        {
            tracing::warn!(scope, %stopped, "no barrier for a webSocketMessage handler's output");
        }
    }
    drop(activity);
    Ok(())
}

async fn dispatch_ws_closed(
    app: &AppHandle,
    socket: &SocketRoute,
    ws_id: u64,
    code: u16,
    reason: String,
    was_clean: bool,
) -> anyhow::Result<()> {
    let scope = socket.scope();
    let Routed { request, route } = app
        .websocket_request(socket.cell().to_string(), ws_id)
        .await
        .map_err(|error| anyhow::anyhow!("route WebSocket close {scope}: {error:?}"))?;
    anyhow::ensure!(route == Route::Local, "WebSocket owner moved off node");
    let _activity = app.activity(request, socket.cell().to_string());
    let runtime = app.runtime.as_ref().context("no cell runtime")?;
    match socket {
        SocketRoute::Cell(cell) => {
            let answer = runtime
                .ws_closed(cell.clone(), ws_id, code, reason, was_clean)
                .await;
            gate_lifecycle_write(app, request, cell, &answer).await;
            answer.map(|_| ())
        }
        // No lifecycle barrier, for the reason a failed facet message opens
        // none.
        SocketRoute::Facet { scope, facet } => {
            runtime
                .facet_ws_closed(scope.clone(), facet, ws_id, code, reason, was_clean)
                .await
        }
    }
}

/// Open the barrier a lifecycle handler's write needs.
///
/// `webSocketOpen` and `webSocketClose` answer nothing a client sees, so a
/// write they made opens a barrier with no frames behind it, whether the
/// handler returned or failed after it committed. A read-only batch or
/// response that follows then trails that barrier instead of revealing the
/// write while a crash can still lose it (#715).
async fn gate_lifecycle_write(
    app: &AppHandle,
    request: u64,
    scope: &str,
    answer: &anyhow::Result<Option<u64>>,
) {
    let position = match answer {
        Ok(position) => *position,
        Err(error) => cello::js::failed_write_position(error),
    };
    if let Some(position) = position {
        tracing::debug!(
            scope,
            position,
            failed = answer.is_err(),
            "gate lifecycle write"
        );
        if let Err(stopped) = app
            .ws_output(request, scope.to_string(), Vec::new(), Some(position), None)
            .await
        {
            tracing::warn!(scope, position, %stopped, "no barrier for a lifecycle handler's write");
        }
    }
}

async fn finish_websocket(
    app: &AppHandle,
    target: &cello::js::WsTarget,
    code: u16,
    reason: String,
    was_clean: bool,
) {
    let socket = SocketRoute::of(target);
    let _ = dispatch_ws_closed(app, &socket, target.id, code, reason, was_clean).await;
    app.websocket_closed(socket.cell().to_string(), target.id);
    cello::js::ws_unregister(target.id);
}

/// Release a WebSocket that the Worker accepted but that Cello will not upgrade.
///
/// `accept()` registers the socket and charges it to the scope's regular-socket
/// count before the Worker's response reaches this code, and `WsTarget` is plain
/// data: dropping it releases nothing. A rejection that only returns an error
/// status therefore leaks the registration and the count for the life of the
/// process, and the Worker never sees a close. A remote target is registered on
/// the owner node instead, so the entry node has nothing to release — the same
/// split that the upgrade-failure path makes. 1006 is the code for a connection
/// that closed without a close frame, which is exactly what happened.
pub(super) async fn reject_accepted_websocket(
    app: &AppHandle,
    target: &cello::js::WsTarget,
    reason: &str,
) {
    if target.tunnel.is_none() {
        finish_websocket(app, target, 1006, reason.to_string(), false).await;
    }
}

/// Answer a Worker response on a connection that did not upgrade.
///
/// A response with a socket reaches a client only through `handle_websocket`
/// or a tunneled upgrade, so every other response path answers through this
/// function. workerd checks each response against the request it answers and
/// throws a `TypeError`, which an HTTP client receives as a 500. The object
/// that answers a Durable Object call applies the same check in
/// `dispatch_local_fetch` and `dispatch_forwarded_fetch`. This check therefore
/// covers the top-level Worker's own response, and a path such as `/do/` that
/// cannot upgrade at all.
pub(super) async fn reply_without_upgrade(
    app: &AppHandle,
    mut worker_response: HttpResponse,
    preserve_representation_length: bool,
) -> HttpReply {
    let Some(websocket) = worker_response.websocket.take() else {
        return runtime_response(worker_response, preserve_representation_length);
    };
    refuse_websocket_without_upgrade(app, websocket).await
}

/// Release a socket on a path that cannot upgrade, and answer with a 500.
///
/// Dropping a Worker socket or a tunnel claim releases it, and a local cell
/// socket needs the explicit close. workerd's 500 has an empty body; cello
/// names the `TypeError` in the body, as it does for any other Worker failure.
pub(super) async fn refuse_websocket_without_upgrade(
    app: &AppHandle,
    websocket: HttpResponseWebSocket,
) -> HttpReply {
    let error = format!("TypeError: {WEBSOCKET_WITHOUT_UPGRADE}");
    if let Ok(PendingWebSocket::Cell(target)) = PendingWebSocket::new(websocket) {
        reject_accepted_websocket(app, &target, &error).await;
    }
    response(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("Worker failed: {error}"),
    )
}

/// One accepted WebSocket response after it returns from JavaScript.
///
/// Keep the response shape in one value through every validation and executor
/// exit. A tunneled response attaches its parked connection lifetime here,
/// so dropping an unfinished response cannot leave host state behind.
enum PendingWebSocket {
    Worker(cello::js::websocket::WorkerWebSocket),
    Cell(cello::js::WsTarget),
    Tunnel {
        target: cello::js::WsTarget,
        upgrade: super::peer_tunnel::TunnelUpgradeClaim,
    },
}

impl PendingWebSocket {
    fn new(websocket: HttpResponseWebSocket) -> anyhow::Result<Self> {
        Ok(match websocket {
            HttpResponseWebSocket::Worker(worker) => Self::Worker(worker),
            HttpResponseWebSocket::Cell(target) => match target.tunnel {
                Some(parked) => Self::Tunnel {
                    target,
                    upgrade: super::peer_tunnel::claim_upgrade(parked)?,
                },
                None => Self::Cell(target),
            },
        })
    }

    fn target(&self) -> Option<&cello::js::WsTarget> {
        match self {
            Self::Worker(_) => None,
            Self::Cell(target) => Some(target),
            Self::Tunnel { target, .. } => Some(target),
        }
    }
}

enum OutboundWebSocketSink {
    /// A socket that a Durable Object dialed. A facet's socket routes,
    /// registers, and gates through the facet's root, as an accepted facet
    /// socket does.
    Cell {
        app: Box<AppHandle>,
        socket: SocketRoute,
    },
    Isolate(cello::js::WsPullSender),
}

impl OutboundWebSocketSink {
    async fn open(&self, websocket: u64, protocol: String) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, socket } => {
                let cell = socket.cell();
                let Routed { request, route } = app
                    .request(cell.to_string())
                    .await
                    .map_err(|error| anyhow::anyhow!("route outbound WebSocket: {error:?}"))?;
                anyhow::ensure!(
                    route == Route::Local,
                    "outbound WebSocket cell moved off node"
                );
                let _activity = app.activity(request, cell.to_string());
                app.websocket_opened(cell.to_string(), websocket, WebSocketKind::Outbound)
                    .await?;
                let runtime = app.runtime.as_ref().context("no cell runtime")?;
                let answer = match socket {
                    SocketRoute::Cell(cell) => {
                        let answer = runtime.ws_open(cell.clone(), websocket, protocol).await;
                        gate_lifecycle_write(app, request, cell, &answer).await;
                        answer.map(|_| ())
                    }
                    // No lifecycle barrier, for the reason a failed facet
                    // message opens none.
                    SocketRoute::Facet { scope, facet } => {
                        runtime
                            .facet_ws_open(scope.clone(), facet, websocket, protocol)
                            .await
                    }
                };
                if answer.is_err() {
                    app.websocket_closed(cell.to_string(), websocket);
                }
                answer
            }
            Self::Isolate(tx) => tx
                .send(cello::js::WsPull::Open(protocol))
                .await
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    async fn message(&self, websocket: u64, data: cello::js::WsIn) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, socket } => dispatch_ws_message(app, socket, websocket, data).await,
            Self::Isolate(tx) => tx
                .send(data.into())
                .await
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    async fn closed(
        &self,
        websocket: u64,
        code: u16,
        reason: String,
        was_clean: bool,
    ) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, socket } => {
                let result =
                    dispatch_ws_closed(app, socket, websocket, code, reason, was_clean).await;
                app.websocket_closed(socket.cell().to_string(), websocket);
                result
            }
            Self::Isolate(tx) => tx
                .send_close(code, reason, was_clean)
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    /// Register the socket before any event of it can be dispatched. A
    /// facet's socket takes its record in the same insertion, so the
    /// registry never holds it under the facet's scope without the route.
    fn register(&self, websocket: u64) {
        if let Self::Cell { socket, .. } = self {
            let facet = match socket {
                SocketRoute::Cell(_) => None,
                SocketRoute::Facet { facet, .. } => Some(facet.clone()),
            };
            cello::js::ws_register_outbound(
                websocket,
                socket.scope(),
                facet,
                cello::js::WsOwner::Cell(socket.scope().to_string()),
            );
        }
    }
}

/// Carry frames between a Durable Object's socket and a client end another
/// isolate in this process kept, after a `stub.fetch` upgrade.
///
/// A same-isolate pair links its two ends directly and never involves the
/// host. This pair cannot: the cell end lives in the cell's isolate and the
/// client end in the caller's, and neither can reach the other's heap. So
/// each direction takes the route an external client's frames take —
/// `dispatch_ws_message` into the cell, a pull queue out to the caller —
/// which is also why a hibernatable server end needs nothing special here.
///
/// The task ends when either side closes, and it unregisters both.
/// Carry frames between a loaded worker's socket and a client end another
/// isolate in this process kept, after a loader-fetch upgrade.
///
/// The cell pipe above cannot serve this pair: the worker end is an
/// isolate-polled socket, not a cell, so there is no cell to dispatch into.
/// Each direction instead takes the isolate-queue route — the caller's
/// `WsOut` frames cross into the loaded socket's pull queue, and the loaded
/// socket's `WsOut` frames cross into the caller's pull queue — which is the
/// same exchange the top-level upgrade task runs for a Worker socket.
///
/// The task ends when either side closes, and it unregisters both. The
/// stashed handoff is dropped with the task: it owns the only remaining
/// `WsPullSender` for the loaded socket, and dropping it lets that input
/// queue drain.
async fn local_isolate_websocket_pipe(
    id: u64,
    target: cello::js::WsTarget,
    worker: cello::js::WorkerWebSocket,
    pull: cello::js::WsPullSender,
    reply: tokio::sync::oneshot::Sender<anyhow::Result<cello::js::OutboundWsOpen>>,
) -> anyhow::Result<()> {
    let inbound = worker.inbound();
    let loaded_id = worker.id();
    // Both registrations flush whatever each end queued before it had
    // anywhere to send, as in the cell pipe above. Each direction carries
    // an admission budget for its output channel, so neither the backlog
    // flushed here nor live sends can stage more than the cap before the
    // forwarder runs.
    let (caller_tx, from_caller) = mpsc::unbounded_channel();
    let (loaded_tx, from_loaded) = mpsc::unbounded_channel();
    // The registration returns the budget the direction is policed by: a
    // backlog buffered before registration keeps the budget it was charged
    // against, and the forwarder must release against that same one.
    let caller_budget = cello::js::ws_register_budgeted(
        id,
        caller_tx,
        cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
    );
    let loaded_budget = cello::js::ws_register_budgeted(
        loaded_id,
        loaded_tx,
        cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
    );
    let _ = reply.send(Ok(cello::js::OutboundWsOpen {
        protocol: None,
        declined: None,
    }));
    // One loop per direction: capacity is reserved before a frame moves, so a
    // single loop awaiting inline could not observe a close from the other
    // side — or a close queued behind flooded frames on its own side — while
    // a frame is pending. The first direction to see a close, a failed
    // reservation, or its socket's retirement shuts the other one down.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let caller = cello::asyncrt::spawn({
        let shutdown_tx = shutdown_tx.clone();
        let inbound = inbound.clone();
        let shutdown = shutdown_rx.clone();
        async move {
            let end =
                forward_isolate_direction(from_caller, inbound, shutdown, caller_budget).await;
            let _ = shutdown_tx.send(true);
            end
        }
    });
    let loaded = cello::asyncrt::spawn({
        let shutdown_tx = shutdown_tx.clone();
        let pull = pull.clone();
        async move {
            let end =
                forward_isolate_direction(from_loaded, pull, shutdown_rx, loaded_budget).await;
            let _ = shutdown_tx.send(true);
            end
        }
    });
    // Either direction ending shuts the other one down, so both joins
    // complete: a direction only ever waits on its socket's channel, its
    // parked reservation, or the shutdown signal. The extra send covers a
    // task that ended without sending one. A panicked task reports
    // retirement, so the teardown below just unregisters: fail-closed.
    let caller_end = caller.await.unwrap_or(PipeDirectionEnd::Retired);
    let _ = shutdown_tx.send(true);
    let loaded_end = loaded.await.unwrap_or(PipeDirectionEnd::Retired);
    let mut caller_close: Option<(u16, String)> = None;
    let mut loaded_close: Option<(u16, String)> = None;
    let mut overloaded = false;
    for (slot, end) in [
        (&mut caller_close, caller_end),
        (&mut loaded_close, loaded_end),
    ] {
        match end {
            PipeDirectionEnd::Close(code, reason) => *slot = Some((code, reason)),
            PipeDirectionEnd::OverLimit => overloaded = true,
            PipeDirectionEnd::Retired => {}
        }
    }
    // Set only by a close the caller sent. A close from the loaded worker
    // is forwarded to the caller below; telling the loaded socket again
    // would be a second close.
    if overloaded {
        // A flood into a peer that never reads stages frames upstream of the
        // pull queue without bound: close both ends loudly instead. 1011 is
        // the codebase's host-failure code, and the close is abnormal — no
        // peer sent it — so it is not clean. Wins over a raced close, which
        // loses to the teardown either way.
        tracing::warn!(
            caller_ws = id,
            loaded_ws = loaded_id,
            staged_cap = ISOLATE_PIPE_MAX_STAGED_BYTES,
            "isolate WebSocket pipe staged past its cap; closing both ends",
        );
        let _ = pull.send_close(
            cello::js::WS_PIPE_OVERLOAD_CODE,
            cello::js::WS_PIPE_OVERLOAD_REASON.to_string(),
            false,
        );
        let _ = inbound.send_close(
            cello::js::WS_PIPE_OVERLOAD_CODE,
            cello::js::WS_PIPE_OVERLOAD_REASON.to_string(),
            false,
        );
    } else if let Some((code, reason)) = caller_close {
        // A close the caller sent still ends at the caller: without a
        // terminal on its own pull queue, unregistering below would make
        // its pump report an abnormal 1006 instead of the close it sent.
        let _ = pull.send_close(code, reason.clone(), true);
        let _ = inbound.send_close(code, reason, true);
    } else if let Some((code, reason)) = loaded_close {
        let _ = pull.send_close(code, reason, true);
    }
    // Stop accepting output, but keep both pull endpoints: each stays
    // reachable, owner-checked, until its terminal is consumed or its request
    // retires, so a close queued behind text is delivered instead of turning
    // into a synthetic 1006 on the next `__ws_next`. `drop_handoff`
    // unregisters the loaded output registration through its `Drop` (output
    // and budget entries included), so no separate `ws_unregister(loaded_id)`
    // belongs here: only the pull endpoint stays, owned by the handoff the
    // joining request tracks from bind time.
    cello::js::ws_unregister(id);
    worker.drop_handoff();
    let _ = target;
    Ok(())
}

async fn local_websocket_pipe(
    app: AppHandle,
    id: u64,
    target: cello::js::WsTarget,
    pull: Option<cello::js::WsPullSender>,
    reply: tokio::sync::oneshot::Sender<anyhow::Result<cello::js::OutboundWsOpen>>,
) -> anyhow::Result<()> {
    let Some(pull) = pull else {
        let _ = reply.send(Err(anyhow::anyhow!(
            "a bound WebSocket target needs an isolate queue"
        )));
        return Ok(());
    };
    // A loaded worker's own 101 joins isolate-to-isolate: the stash holds
    // its handoff, and only the single-use bind token could have authorized
    // this request. A cell target has no stash and takes the cell pipe.
    if let Some(worker) = cello::js::ws_take_join_handoff(target.id) {
        return local_isolate_websocket_pipe(id, target, worker, pull, reply).await;
    }
    // Both registrations flush whatever each end queued before it had
    // anywhere to send: the cell's greeting frame is queued while its own
    // fetch handler still runs, and the caller can send the moment it
    // accepts. Each direction carries an admission budget for its output
    // channel, so neither the backlog flushed here nor live sends can stage
    // more than the cap before the forwarder runs.
    let socket = SocketRoute::of(&target);
    let (caller_tx, from_caller) = mpsc::unbounded_channel();
    let (cell_tx, from_cell) = mpsc::unbounded_channel();
    // The registration returns the budget the direction is policed by, as
    // in the isolate pipe above.
    let caller_budget = cello::js::ws_register_budgeted(
        id,
        caller_tx,
        cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
    );
    let cell_budget = cello::js::ws_register_budgeted(
        target.id,
        cell_tx,
        cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
    );
    let _ = reply.send(Ok(cello::js::OutboundWsOpen {
        protocol: None,
        declined: None,
    }));
    // One forwarder per direction: a `pull.send` parked on a full queue (or
    // a cell dispatch parked behind backpressure) owns no frame the loop has
    // to keep, so the loop keeps receiving — and keeps observing closes,
    // retirements, and the shutdown signal — while it waits. The first
    // direction to see a close, an overload, or its socket's retirement
    // shuts the other one down.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let caller = cello::asyncrt::spawn({
        let shutdown_tx = shutdown_tx.clone();
        let app = app.clone();
        let socket = socket.clone();
        let target_id = target.id;
        let target_scope = target.scope.clone();
        let shutdown = shutdown_rx.clone();
        async move {
            let end = forward_pipe_direction(
                from_caller,
                move |frame| {
                    let app = app.clone();
                    let socket = socket.clone();
                    let target_scope = target_scope.clone();
                    async move {
                        let data = match frame {
                            cello::js::WsOut::Text(text) => cello::js::WsIn::Text(text),
                            cello::js::WsOut::Binary(bytes) => cello::js::WsIn::Binary(bytes),
                            // The forwarder only sends data frames; anything
                            // else ends the direction retired instead of
                            // panicking it.
                            cello::js::WsOut::Close(..) | cello::js::WsOut::Overload => {
                                return PipeSendOutcome::Gone;
                            }
                        };
                        match dispatch_ws_message(&app, &socket, target_id, data).await {
                            Ok(()) => PipeSendOutcome::Sent,
                            Err(error) => {
                                tracing::warn!(
                                    %error,
                                    scope = %target_scope,
                                    "kept WebSocket message failed"
                                );
                                PipeSendOutcome::Gone
                            }
                        }
                    }
                },
                shutdown,
                caller_budget,
            )
            .await;
            let _ = shutdown_tx.send(true);
            end
        }
    });
    let cell = cello::asyncrt::spawn({
        let shutdown_tx = shutdown_tx.clone();
        let pull = pull.clone();
        async move {
            let end = forward_pipe_direction(
                from_cell,
                move |frame| {
                    let to = pull.clone();
                    async move {
                        let data = match frame {
                            cello::js::WsOut::Text(text) => cello::js::WsPull::Text(text),
                            cello::js::WsOut::Binary(bytes) => cello::js::WsPull::Binary(bytes),
                            // The forwarder only sends data frames; anything
                            // else ends the direction retired instead of
                            // panicking it.
                            cello::js::WsOut::Close(..) | cello::js::WsOut::Overload => {
                                return PipeSendOutcome::Gone;
                            }
                        };
                        let bytes = cello::js::WsPullSender::permit_bytes(&data);
                        match to.reserve(bytes).await {
                            Some(permit) => {
                                if to.send_reserved(data, permit).is_err() {
                                    PipeSendOutcome::Gone
                                } else {
                                    PipeSendOutcome::Sent
                                }
                            }
                            None => PipeSendOutcome::Gone,
                        }
                    }
                },
                shutdown_rx,
                cell_budget,
            )
            .await;
            let _ = shutdown_tx.send(true);
            end
        }
    });
    // Either direction ending shuts the other one down, so both joins
    // complete: a direction only ever waits on its socket's channel, its
    // parked send, or the shutdown signal. The extra send covers a task that
    // ended without sending one. A panicked task reports retirement, so the
    // teardown below just unregisters: fail-closed.
    let caller_end = caller
        .await
        .map(|(end, _)| end)
        .unwrap_or(PipeDirectionEnd::Retired);
    let _ = shutdown_tx.send(true);
    let cell_end = cell
        .await
        .map(|(end, _)| end)
        .unwrap_or(PipeDirectionEnd::Retired);
    // Set only by a close the caller sent. A close from the cell needs no
    // entry of its own: the isolate that sent it has already told the cell's
    // own handler, and telling it again would be a second `webSocketClose`.
    // An overload marker is host-originated, so both ends still need the
    // 1011. A close from the cell is forwarded to the caller's pull queue.
    let mut caller_close: Option<(u16, String)> = None;
    let mut cell_close: Option<(u16, String)> = None;
    // Set by an overload on either side: a flood past the cap ends the pipe
    // loudly instead of growing host memory without bound.
    let mut overloaded = false;
    for (slot, end) in [(&mut caller_close, caller_end), (&mut cell_close, cell_end)] {
        match end {
            PipeDirectionEnd::Close(code, reason) => *slot = Some((code, reason)),
            PipeDirectionEnd::OverLimit => overloaded = true,
            PipeDirectionEnd::Retired => {}
        }
    }
    if overloaded {
        // A flood staged past the cap: close both ends loudly instead of
        // growing host memory without bound. 1011 is the codebase's
        // host-failure code, and the close is abnormal — no peer sent it —
        // so it is not clean. Wins over a raced close, which loses to the
        // teardown either way.
        tracing::warn!(
            scope = %target.scope,
            staged_cap = ISOLATE_PIPE_MAX_STAGED_BYTES,
            "cell WebSocket pipe staged past its cap; closing both ends",
        );
        let _ = pull.send_close(
            cello::js::WS_PIPE_OVERLOAD_CODE,
            cello::js::WS_PIPE_OVERLOAD_REASON.to_string(),
            false,
        );
        let _ = dispatch_ws_closed(
            &app,
            &socket,
            target.id,
            cello::js::WS_PIPE_OVERLOAD_CODE,
            cello::js::WS_PIPE_OVERLOAD_REASON.to_string(),
            false,
        )
        .await;
    } else if let Some((code, reason)) = caller_close {
        // A close the caller sent still ends at the caller: without a
        // terminal on its own pull queue, unregistering below would make
        // its pump report an abnormal 1006 instead of the close it sent.
        let _ = pull.send_close(code, reason.clone(), true);
        let _ = dispatch_ws_closed(&app, &socket, target.id, code, reason, true).await;
    } else if let Some((code, reason)) = cell_close {
        // A close the cell sent ends at the caller: the cell already knows.
        let _ = pull.send_close(code, reason, true);
    }
    app.websocket_closed(socket.cell().to_string(), target.id);
    cello::js::ws_unregister(target.id);
    cello::js::ws_unregister(id);
    // The caller's pull endpoint stays reachable, owner-checked, until its
    // terminal is consumed or its request retires — same rule as the
    // isolate-to-isolate pipe.
    Ok(())
}

pub(crate) async fn outbound_websocket_task(
    app: AppHandle,
    request: cello::js::OutboundWsReq,
) -> anyhow::Result<()> {
    use hyper::header::{HeaderMap, HeaderName, HeaderValue, SEC_WEBSOCKET_PROTOCOL};

    let cello::js::OutboundWsReq {
        scope,
        id,
        url,
        protocols,
        pull,
        headers,
        want_response,
        target,
        facet,
        reply,
    } = request;
    if let Some(target) = target {
        return local_websocket_pipe(app, id, target, pull, reply).await;
    }
    let sink = match pull {
        Some(pull) => OutboundWebSocketSink::Isolate(pull),
        None => OutboundWebSocketSink::Cell {
            app: Box::new(app),
            socket: match facet {
                Some(facet) => SocketRoute::Facet { scope, facet },
                None => SocketRoute::Cell(scope),
            },
        },
    };
    let mut handshake = HeaderMap::new();
    for (name, value) in &headers {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "upgrade"
                | "connection"
                | "sec-websocket-key"
                | "sec-websocket-version"
                | "sec-websocket-protocol"
                | "host"
        ) {
            continue;
        }
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) else {
            continue;
        };
        handshake.insert(name, value);
    }
    if !protocols.is_empty() {
        handshake.insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&protocols.join(", "))
                .context("invalid WebSocket subprotocol")?,
        );
    }
    let timeout = std::time::Duration::from_secs(10);
    let connected = tokio::time::timeout(timeout, cello::ws_client::connect(&url, handshake)).await;
    let connection = match connected {
        Ok(Ok(connection)) => connection,
        Ok(Err(cello::ws_client::Error::Declined(declined))) if want_response => {
            let declined = cello::js::DeclinedUpgrade {
                status: declined.status.as_u16(),
                headers: declined
                    .headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            value.to_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect(),
                body: declined.body,
            };
            let _ = reply.send(Ok(cello::js::OutboundWsOpen {
                protocol: None,
                declined: Some(declined),
            }));
            return Ok(());
        }
        Ok(Err(error)) => {
            let _ = reply.send(Err(anyhow::anyhow!("{error}")));
            return Ok(());
        }
        Err(_) => {
            let _ = reply.send(Err(anyhow::anyhow!(
                "outbound WebSocket handshake timed out after {}ms",
                timeout.as_millis()
            )));
            return Ok(());
        }
    };
    let socket = connection.socket;
    let protocol = connection
        .headers
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if protocol
        .as_ref()
        .is_some_and(|selected| !protocols.iter().any(|offered| offered == selected))
    {
        let _ = reply.send(Err(anyhow::anyhow!(
            "server selected an unrequested WebSocket subprotocol"
        )));
        return Ok(());
    }

    let (outbound, outputs) = mpsc::unbounded_channel();
    sink.register(id);
    // Admit outbound frames under the pipe's staged cap: a cell that floods
    // a peer which never reads trips the budget, and the pump ends the
    // socket loudly with 1011 instead of staging the flood without bound.
    // The registration returns the effective budget, which the pump releases
    // as it receives.
    let budget = cello::js::ws_register_budgeted(
        id,
        outbound,
        cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
    );
    if let Err(error) = sink
        .open(id, protocol.as_deref().unwrap_or_default().to_string())
        .await
    {
        cello::js::ws_unregister(id);
        let _ = reply.send(Err(error));
        return Ok(());
    }
    if reply
        .send(Ok(cello::js::OutboundWsOpen {
            protocol,
            declined: None,
        }))
        .is_err()
    {
        let _ = sink
            .closed(id, 1006, "opening event was cancelled".into(), false)
            .await;
        cello::js::ws_unregister(id);
        return Ok(());
    }

    // Ping and Close are answered by the reader's auto-pong and auto-close, as
    // the previous client library also did unprompted. The pump writes those
    // replies on the same socket, so nothing about that changes here.
    let (close, _writer, _outputs, _budget) = {
        let sink = &sink;
        pump_cell_socket(
            socket,
            outputs,
            true,
            move |data| sink.message(id, data),
            budget,
        )
        .await
    };
    let _ = sink
        .closed(id, close.state.0, close.state.1, close.state.2)
        .await;
    cello::js::ws_unregister(id);
    Ok(())
}

/// A close frame the peer sent is a clean close, as workerd reports it, even
/// when its payload is malformed and the code becomes a protocol error; only
/// a transport end without a frame (1006) is unclean.
fn websocket_close_details(payload: &[u8]) -> (u16, String) {
    match payload {
        [] => (1005, String::new()),
        [_] => (1002, String::new()),
        [first, second, reason @ ..] => {
            let Ok(reason) = std::str::from_utf8(reason) else {
                return (1007, String::new());
            };
            let code = u16::from_be_bytes([*first, *second]);
            if !cello_logic::schedule::websocket_close_code_is_allowed(code) {
                return (1002, String::new());
            }
            (code, reason.to_string())
        }
    }
}

/// The write half of a cell socket, shared by the reader and the writer.
///
/// The reader needs it because auto-pong and auto-close hand their reply back to
/// the caller once a socket is split. The writer needs it for the cell's own
/// frames, and the close path needs it after both directions stop.
type SocketWriter<S> =
    std::sync::Arc<tokio::sync::Mutex<fastwebsockets::WebSocketWrite<tokio::io::WriteHalf<S>>>>;

/// Close details: the code, the reason, and whether the close was clean.
type CloseState = (u16, String, bool);

/// Progress bound for one pump socket write while the pipe is closing — a
/// close flush, an automatic reply, or a loud-1011 attempt — that makes no
/// progress within it ends the wait instead of parking teardown. Normal
/// writes run unbounded, like the pipe forwarder's own sends, so a slow but
/// live peer still drains fully. Same interval as the pipe close-flush
/// bound, for the same reason; well under any caller close timeout.
const PUMP_SEND_PROGRESS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloseInitiator {
    Peer,
    Application,
    Transport,
}

#[derive(Debug, Eq, PartialEq)]
struct PumpClose {
    state: CloseState,
    initiator: CloseInitiator,
}

/// The pump's write half: the shared socket writer plus whether a write may
/// have stopped mid-frame. A write future dropped while it holds the writer
/// — a timed-out close flush, a loud-1011 attempt that never acquired it, an
/// abandoned reply — can leave a partial frame on the socket: the peer would
/// read any frame written after as a continuation of the unfinished one
/// ([RFC 6455](https://www.rfc-editor.org/rfc/rfc6455.html)). Once set, no
/// further frame may be written; teardown drops the transport instead.
struct PumpWriter<S> {
    writer: SocketWriter<S>,
    poisoned: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl<S> Clone for PumpWriter<S> {
    fn clone(&self) -> Self {
        Self {
            writer: self.writer.clone(),
            poisoned: self.poisoned.clone(),
        }
    }
}

impl<S> PumpWriter<S> {
    fn poisoned(&self) -> bool {
        self.poisoned.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Marks the pump writer poisoned unless the write it guards completes: a
/// future dropped mid-write — timed out, or cancelled by teardown — may
/// have left a partial frame on the socket.
struct WriteGuard<'a> {
    poisoned: &'a std::sync::atomic::AtomicBool,
    complete: bool,
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

/// Write one frame on the pump socket, tracking frame-boundary status. An
/// abandoned write poisons the writer: every later pump write refuses and
/// teardown drops the transport instead of writing another frame. A write
/// that never acquires the writer — a close attempt parked behind a stuck
/// outbound write — poisons it too: the in-flight write may finish partial.
///
/// `write_bound` limits only the write itself, never the wait for the
/// writer: a reply queued behind a slow but progressing data write must not
/// time out — and poison the socket — for time the data write spent.
async fn pump_write_frame<S>(
    ws: &PumpWriter<S>,
    frame: fastwebsockets::Frame<'_>,
    write_bound: Option<std::time::Duration>,
) -> Result<(), fastwebsockets::WebSocketError>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    use std::sync::atomic::Ordering;
    if ws.poisoned.load(Ordering::SeqCst) {
        return Err(fastwebsockets::WebSocketError::ConnectionClosed);
    }
    // Guards the lock wait itself: a write cancelled while parked behind a
    // stuck outbound write never acquires the writer, yet the in-flight
    // write may still finish partial — so the wait poisons too, and hands
    // off to the in-flight guard once the lock is held.
    let mut wait_guard = WriteGuard {
        poisoned: &ws.poisoned,
        complete: false,
    };
    let mut writer = ws.writer.lock().await;
    wait_guard.complete = true;
    // Declared after `writer` so it drops first: the poison flag is stored
    // before the lock releases, and a waiter woken by the unlock always
    // observes it instead of writing a frame after the partial one.
    let mut guard = WriteGuard {
        poisoned: &ws.poisoned,
        complete: false,
    };
    // Re-check under the lock: a write abandoned while this one waited may
    // have left a partial frame ahead of it.
    if ws.poisoned.load(Ordering::SeqCst) {
        return Err(fastwebsockets::WebSocketError::ConnectionClosed);
    }
    // A bound that fires drops the write mid-frame; the guard poisons.
    let result = match write_bound {
        Some(bound) => cello::asyncrt::timeout(bound, writer.write_frame(frame))
            .await
            .unwrap_or(Err(fastwebsockets::WebSocketError::ConnectionClosed)),
        None => writer.write_frame(frame).await,
    };
    if result.is_ok() {
        guard.complete = true;
    }
    result
}

async fn write_ws_out<S>(ws: &PumpWriter<S>, out: cello::js::WsOut) -> bool
where
    S: tokio::io::AsyncWrite + Unpin,
{
    use fastwebsockets::Frame;
    let keep_open = matches!(out, cello::js::WsOut::Text(_) | cello::js::WsOut::Binary(_));
    let frame = match out {
        cello::js::WsOut::Text(text) => Frame::text(text.into_bytes().into()),
        cello::js::WsOut::Binary(data) => Frame::binary(data.into()),
        cello::js::WsOut::Close(code, reason) => Frame::close(code, reason.as_bytes()),
        // A pre-registration burst flushes its overload marker into this
        // never-budgeted pipe: end the socket loudly with the 1011 the
        // isolate forwarder would have sent.
        cello::js::WsOut::Overload => Frame::close(
            cello::js::WS_PIPE_OVERLOAD_CODE,
            cello::js::WS_PIPE_OVERLOAD_REASON.as_bytes(),
        ),
    };
    pump_write_frame(ws, frame, None).await.is_ok() && keep_open
}

async fn echo_websocket_close<S>(
    writer: &PumpWriter<S>,
    close: &CloseState,
    application_sent_close: bool,
) where
    S: tokio::io::AsyncWrite + Unpin,
{
    if let Some(code) =
        cello_logic::schedule::websocket_echo_close(close.0, close.2, application_sent_close)
    {
        // Bounded like every other pump socket write: a peer that stopped
        // reading must not park teardown. The echo is best-effort; the cell
        // already learned the close through its own dispatch.
        let _ = cello::asyncrt::timeout(
            PUMP_SEND_PROGRESS_TIMEOUT,
            write_ws_out(writer, cello::js::WsOut::Close(code, close.1.clone())),
        )
        .await;
    }
}

/// One frame's staged bytes, charged the way the pull queue charges its own
/// frames: payload plus the frame value itself.
fn ws_in_staged_bytes(data: &cello::js::WsIn) -> usize {
    let payload = match data {
        cello::js::WsIn::Text(text) => text.len(),
        cello::js::WsIn::Binary(bytes) => bytes.len(),
    };
    std::mem::size_of::<cello::js::WsIn>().saturating_add(payload)
}

/// How the pump's read direction ended.
enum ReadEnd {
    /// The transport ended or a dispatch failed: fail-closed, default close.
    Transport,
    /// The peer sent a close frame, after every data frame ahead of it was
    /// dispatched in order.
    Peer(PumpClose),
    /// Frames staged past the cap behind a cell or isolate that never reads
    /// its inbound queue: end loudly with 1011 instead of growing host
    /// memory without bound. A slow handler never trips this: dispatches
    /// run without any time bound, and reads only stage what fits.
    OverLimit,
    /// One message's fragments passed the message cap (or a single frame
    /// reached it): end loudly with 1009 instead of growing the assembly
    /// buffer without bound. The reader drops the partial assembly with the
    /// error, so no further frame may be read on this socket — the pump
    /// ends, and the teardown below is the last write, on an unpoisoned
    /// writer only.
    MessageTooBig,
}

/// A control-frame reply on a pump socket: the copied-out frame the reader's
/// callback answers pings and echoes with. Boxed: the callback's concrete
/// closure type is unnameable, and the owned read future below must name the
/// state it carries.
type PumpReplyFut =
    std::pin::Pin<Box<dyn Future<Output = Result<(), fastwebsockets::WebSocketError>> + Send>>;
type PumpReply = Box<dyn for<'f> FnMut(fastwebsockets::Frame<'f>) -> PumpReplyFut + Send>;

/// The socket-reader half of the pump: the fragment reader plus the callback
/// its control-frame replies need. Travels into the in-flight read and back
/// with every outcome, so arming a read moves values instead of borrowing
/// the pump loop's locals across iterations.
struct PumpReader<S> {
    reader: cello::ws_message::CappedFragmentReader<tokio::io::ReadHalf<S>>,
    reply: PumpReply,
}

/// What one owned socket read produced. The reader comes back with every
/// outcome — including the transport end — so the pump loop rearms from
/// values it owns.
enum PumpReadEvent {
    /// A data frame, copied out of the reader's buffer.
    Data(cello::js::WsIn),
    /// The peer's close, after every data frame ahead of it was staged.
    PeerClose(u16, String),
    /// Any other control frame: answered by the reply callback, if it needs
    /// one, and otherwise ignored.
    Ignored,
    /// The peer ended its write side with no close frame: orderly EOF, not
    /// a failure. The loop stops reading and drains the accepted staged
    /// messages in order before reporting the transport close.
    Eof,
    /// One message's fragments passed the message cap, or a single frame
    /// reached it: fail loud with 1009, never by buffering further.
    MessageTooBig,
    /// A read failure: fail-closed.
    Transport,
}

/// Read one frame on `state` and hand the state back with what it produced.
/// The future owns everything it polls, so the pump loop can keep it across
/// iterations without borrowing its own locals — and dropping it ends the
/// pump, never a live read: the read half is dropped only once the socket
/// is never read again.
async fn pump_read_one<S>(mut state: PumpReader<S>) -> (PumpReadEvent, PumpReader<S>)
where
    S: tokio::io::AsyncRead + Unpin + Send,
{
    let frame = state.reader.read_frame(&mut state.reply).await;
    let event = match frame {
        Err(fastwebsockets::WebSocketError::UnexpectedEOF) => PumpReadEvent::Eof,
        Err(fastwebsockets::WebSocketError::FrameTooLarge) => PumpReadEvent::MessageTooBig,
        Err(_) => PumpReadEvent::Transport,
        Ok(frame) => match frame.opcode {
            fastwebsockets::OpCode::Text => PumpReadEvent::Data(cello::js::WsIn::Text(
                String::from_utf8_lossy(&frame.payload).into_owned(),
            )),
            fastwebsockets::OpCode::Binary => {
                PumpReadEvent::Data(cello::js::WsIn::Binary(frame.payload.to_vec()))
            }
            fastwebsockets::OpCode::Close => {
                let (code, reason) = websocket_close_details(&frame.payload);
                PumpReadEvent::PeerClose(code, reason)
            }
            _ => PumpReadEvent::Ignored,
        },
    };
    (event, state)
}

/// One owned socket read in flight: the reader and its reply callback travel
/// with the read and come back with every outcome, so arming it moves values
/// instead of borrowing the pump loop's locals across iterations.
type PumpReadInFlight<S> =
    std::pin::Pin<Box<dyn Future<Output = (PumpReadEvent, PumpReader<S>)> + Send>>;

/// Carry frames between a socket and the cell behind it until one side stops.
///
/// The read is never cancelled. `fastwebsockets::read_frame` consumes header
/// bytes from its buffer and then awaits for the payload, holding what it parsed
/// in local variables, so dropping that future keeps the buffer advanced and
/// loses the header. The next read then treats a payload byte as a frame header
/// and the stream never realigns. Earlier versions of both callers read the
/// socket in the same select as the cell's outbound queue, which drops the losing
/// future on every iteration. Cancelling a read mid-payload fails against that
/// shape.
///
/// One async block therefore owns each direction. The halves are split so that
/// the writer can write while the reader reads, and the writer stops on a signal
/// instead of being dropped, because dropping it can leave a partial frame on
/// the socket.
///
/// The caller sets `auto_close` and `auto_pong` on the socket before the pump
/// runs, and the pump keeps that choice: an automatic reply is written on the
/// same socket, exactly as the unsplit collector wrote it.
///
/// `outbound_close_is_clean` reports a close that the application sends as the
/// close state of the socket — but only when the close actually reaches the
/// wire. A close skipped behind a poisoned writer, or one whose own write
/// times out, reports the abnormal 1006 / unclean the peer observes instead
/// of claiming the handshake completed. The local path leaves that false
/// in any case, because it answers an unclean end with its own protocol
/// echo after the pump returns.
/// The initiator remains separate because only a peer close needs that echo.
///
/// Reads stage ahead of a slow cell instead of parking behind its dispatch:
/// dispatches run without any time bound, so a slow handler never trips the
/// pipe — only frames staged past the cap end it loudly. Fragments assemble
/// in the reader under the message cap, uncharged against the staged
/// budget: the worst case per socket is the reader's bound (the assembly
/// plus the parser's read buffer, under about 100 MiB) plus one full staged
/// budget (see `cello::ws_message`), and a message whose fragments
/// pass the cap ends the socket loudly with 1009 instead of staging
/// anything. A peer close
/// pending behind a stuck dispatch ends the read with the pending close
/// after one unrenewed progress interval instead of parking teardown; each
/// completed dispatch restarts that wait. An orderly EOF with no close
/// frame stops new reads but still dispatches every staged message in order
/// before reporting the transport close — under the same progress bound
/// once the dispatch behind it stops completing, because the parser
/// reports a truncation mid-frame exactly like a clean end and a truncated
/// frame must take the Transport path instead of hanging a full pull
/// queue. The write direction
/// is one pipe forwarder: normal writes run unbounded so a slow but live
/// peer still drains fully, while the close flush runs under the progress
/// bound. Each completed send restarts the wait; a peer that stopped
/// reading ends the closing pipe instead of blocking closure.
/// The automatic replies and the loud-close attempts (1011 and 1009) run
/// under the same bound, and every pump socket write is poison tracked: a
/// write abandoned mid-frame poisons the writer before the lock releases,
/// after which no further frame may be written and teardown drops the
/// transport instead.
///
/// The returned writer is still live, so the caller can write the close frames
/// that follow — unless a write may have stopped mid-frame, which the
/// poison flag records: after that no further frame may be written, and the
/// caller drops the transport instead. The returned budget still covers the
/// handed-back receiver: the forwarder released what it received, and the
/// caller releases each data frame its post-pump drain removes, even when
/// its write fails.
async fn pump_cell_socket<S, F, Fut>(
    socket: fastwebsockets::WebSocket<S>,
    outputs: mpsc::UnboundedReceiver<cello::js::WsOut>,
    outbound_close_is_clean: bool,
    mut inbound: F,
    budget: cello::js::WsPipeBudget,
) -> (
    PumpClose,
    PumpWriter<S>,
    mpsc::UnboundedReceiver<cello::js::WsOut>,
    cello::js::WsPipeBudget,
)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    F: FnMut(cello::js::WsIn) -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    use cello::ws_message::CappedFragmentReader;
    use fastwebsockets::{Frame as WsFrame, Payload};

    let (reader, writer) = socket.split(tokio::io::split);
    let reader = CappedFragmentReader::new(reader);
    let writer: PumpWriter<S> = PumpWriter {
        writer: std::sync::Arc::new(tokio::sync::Mutex::new(writer)),
        poisoned: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
    };

    let mut close = PumpClose {
        state: (1006, String::new(), false),
        initiator: CloseInitiator::Transport,
    };
    // The write direction retires on this signal, which the read direction
    // sends when it ends. Either direction ending ends the pump: the write
    // side owns the outputs receiver, so it hands it back for the post-pump
    // drain of the close handler's frames.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let receiver: mpsc::UnboundedReceiver<cello::js::WsOut>;
    // Assigned inside the block below, alongside the receiver: the forwarder
    // releases what it receives, and the caller releases each data frame its
    // post-pump drain removes.
    let budget_out: cello::js::WsPipeBudget;
    {
        let obligated_writer = writer.clone();
        // Boxed: the concrete closure type is unnameable, and the owned read
        // future below must name the state it carries.
        let obligated: PumpReply = Box::new({
            let obligated_writer = obligated_writer.clone();
            move |frame: WsFrame<'_>| {
                let writer = obligated_writer.clone();
                // The reply borrows the reader's buffer, and the write outlives the
                // callback, so the payload is copied out. The reader builds a pong
                // or a close echo unmasked, and `write_frame` masks it if the role
                // needs it, exactly as the unsplit collector did.
                let reply = WsFrame::new(
                    frame.fin,
                    frame.opcode,
                    None,
                    Payload::Owned(frame.payload.to_vec()),
                );
                Box::pin(async move {
                    // Bounded once it holds the writer: a peer that sent a
                    // ping and then stopped reading must end the pump
                    // instead of parking the read behind the reply. A timeout
                    // reports the connection as closed, and the read ends the
                    // pump as an unclean transport. The wait for the writer
                    // is not bounded: a reply queued behind a slow data write
                    // to a live peer waits for it, as the data write does.
                    // The write is poison tracked: abandoning it drops the
                    // transport instead of writing another frame after a
                    // possibly partial one.
                    pump_write_frame(&writer, reply, Some(PUMP_SEND_PROGRESS_TIMEOUT)).await
                }) as PumpReplyFut
            }
        });

        // Read direction: stage socket frames ahead of a slow cell instead
        // of parking the read behind its dispatch. Reads keep flowing while
        // a dispatch is parked — keeping the peer's close observable — and
        // dispatches run without any time bound, so a slow handler never
        // trips the pipe: only frames staged past the cap end it loudly.
        // A peer close pending behind a stuck dispatch bounds that flush,
        // and an orderly EOF drains what was staged before reporting the
        // transport close. The in-flight read is dropped only when the pump
        // ends, never to keep reading afterwards, so the frame stream never
        // realigns.
        let read = async {
            let mut staged: std::collections::VecDeque<cello::js::WsIn> =
                std::collections::VecDeque::new();
            // Bytes staged upstream of the cell. Added on stage, released
            // when a dispatch starts for the head frame.
            let mut staged_bytes: usize = 0;
            // A close waiting behind flushed frames. Set instead of returned
            // while anything remains to dispatch; delivered once the queue
            // drains. New reads stop once one is pending.
            let mut pending: Option<(u16, String)> = None;
            // The peer ended its write side with no close frame. New reads
            // stop, but the staged messages still dispatch in order before
            // the loop reports the transport close.
            let mut read_done = false;
            // Progress bound for the flush once a peer close is pending: each
            // dispatched frame restarts the wait, so a slow but live cell
            // still drains fully, while a cell that stopped pulling ends the
            // pump with the pending close instead of blocking closure
            // indefinitely. Ordinary dispatches run unbounded.
            let mut flush_deadline: Option<cello::asyncrt::Sleep> = None;
            // At most one socket read and one dispatch in flight. The read
            // owns its reader and reply callback and hands them back with
            // every outcome, so arming it moves values instead of borrowing
            // the loop's locals across iterations. Neither future is dropped
            // while the pump keeps reading.
            let mut state = Some(PumpReader {
                reader,
                reply: obligated,
            });
            let mut reading: Option<PumpReadInFlight<S>> = None;
            let mut dispatching: Option<std::pin::Pin<Box<Fut>>> = None;
            loop {
                if dispatching.is_none() {
                    if let Some(data) = staged.pop_front() {
                        staged_bytes = staged_bytes.saturating_sub(ws_in_staged_bytes(&data));
                        dispatching = Some(Box::pin(inbound(data)));
                    } else if let Some((code, reason)) = pending.take() {
                        return ReadEnd::Peer(PumpClose {
                            state: (code, reason, true),
                            initiator: CloseInitiator::Peer,
                        });
                    } else if read_done {
                        return ReadEnd::Transport;
                    }
                }
                // The peer-close flush bound above, extended to an orderly
                // EOF: `UnexpectedEOF` fires both at a frame boundary and
                // mid-frame, and the parser exposes no partial-byte count,
                // so a truncated frame is indistinguishable from a clean
                // end here. A truncated frame must take the Transport path —
                // a full pull queue would otherwise hang the pump behind the
                // idle output channel — so an EOF drain behind a dispatch
                // that stopped completing ends Transport instead of parking
                // teardown. Each completed dispatch restarts the wait, so a
                // slow but live cell still drains fully.
                if (pending.is_some() || read_done)
                    && dispatching.is_some()
                    && flush_deadline.is_none()
                {
                    flush_deadline = Some(cello::asyncrt::sleep(PUMP_SEND_PROGRESS_TIMEOUT));
                }
                if reading.is_none() && pending.is_none() && !read_done {
                    if let Some(reader_state) = state.take() {
                        reading = Some(Box::pin(pump_read_one(reader_state)));
                    }
                }
                // One side is always armed here: a read, a dispatch, or the
                // loop returned above. An absent side pends, so the live one
                // still observes the peer while it is parked.
                cello::asyncrt::select! {
                    outcome = async {
                        match reading.as_mut() {
                            Some(read) => Some(read.as_mut().await),
                            None => std::future::pending().await,
                        }
                    } => {
                        let Some((event, reader_state)) = outcome else { continue };
                        state = Some(reader_state);
                        reading = None;
                        match event {
                            PumpReadEvent::Data(data) => {
                                staged_bytes = staged_bytes
                                    .saturating_add(ws_in_staged_bytes(&data));
                                staged.push_back(data);
                            }
                            PumpReadEvent::PeerClose(code, reason) => {
                                // First close wins; anything later is dropped
                                // with the closing pump. Queued data flushes
                                // first, in order.
                                pending.get_or_insert((code, reason));
                            }
                            PumpReadEvent::Ignored => {}
                            PumpReadEvent::Eof => {
                                read_done = true;
                            }
                            PumpReadEvent::MessageTooBig => {
                                return ReadEnd::MessageTooBig;
                            }
                            PumpReadEvent::Transport => {
                                return ReadEnd::Transport;
                            }
                        }
                        if staged_bytes > ISOLATE_PIPE_MAX_STAGED_BYTES {
                            return ReadEnd::OverLimit;
                        }
                    }
                    result = async {
                        match dispatching.as_mut() {
                            Some(dispatch) => Some(dispatch.as_mut().await),
                            None => std::future::pending().await,
                        }
                    } => {
                        let Some(result) = result else { continue };
                        dispatching = None;
                        // Each completed dispatch restarts the flush bound.
                        flush_deadline = None;
                        if result.is_err() {
                            return ReadEnd::Transport;
                        }
                    }
                    _ = async {
                        match flush_deadline.as_mut() {
                            Some(deadline) => deadline.await,
                            None => std::future::pending().await,
                        }
                    } => {
                        // The cell stopped pulling with a peer close pending,
                        // or with an EOF drain behind a stuck dispatch: end
                        // the pump instead of parking teardown. Staged frames
                        // that never dispatched drop with the pump.
                        match pending.take() {
                            Some((code, reason)) => {
                                return ReadEnd::Peer(PumpClose {
                                    state: (code, reason, true),
                                    initiator: CloseInitiator::Peer,
                                });
                            }
                            // The EOF branch: see the arming comment above.
                            // Indistinguishable from a truncated frame, so
                            // fail-closed like every other transport end.
                            None => return ReadEnd::Transport,
                        }
                        }
                }
            }
        };

        // Write direction: one pipe forwarder over the cell's output channel
        // into socket writes. A parked socket write owns no frame the loop
        // has to keep, so the loop keeps receiving — and keeps observing
        // closes, retirements, and the shutdown signal — while it waits.
        // Normal writes run unbounded, like the forwarder's own sends, so a
        // slow but live peer still drains fully; only the close flush runs
        // under the progress bound. The producer budget the registration
        // returned polices admission, and the forwarder releases what it
        // receives, so live sends never trip their own cap.
        let fwd_writer = writer.clone();
        // The drain after the pump still charges against this budget, so the
        // pump hands it back with the receiver instead of moving it in.
        budget_out = budget.clone();
        let write = async move {
            let (end, receiver) = forward_pipe_direction(
                outputs,
                move |frame| {
                    let writer = fwd_writer.clone();
                    async move {
                        // The forwarder only sends data frames; a socket
                        // write that fails reports the sink gone, and the
                        // pump ends the transport unclean.
                        if write_ws_out(&writer, frame).await {
                            PipeSendOutcome::Sent
                        } else {
                            PipeSendOutcome::Gone
                        }
                    }
                },
                shutdown_rx,
                budget,
            )
            .await;
            (end, receiver)
        };

        let mut read = std::pin::pin!(read);
        let mut write = std::pin::pin!(write);
        receiver = {
            cello::asyncrt::select! {
                result = &mut read => {
                    let read_loud_1011 = matches!(result, ReadEnd::OverLimit);
                    let read_loud_1009 = matches!(result, ReadEnd::MessageTooBig);
                    match result {
                        ReadEnd::Peer(details) => close = details,
                        ReadEnd::OverLimit => {
                            close = PumpClose {
                                state: (
                                    cello::js::WS_PIPE_OVERLOAD_CODE,
                                    cello::js::WS_PIPE_OVERLOAD_REASON.to_string(),
                                    false,
                                ),
                                initiator: CloseInitiator::Application,
                            };
                        }
                        ReadEnd::MessageTooBig => {
                            close = PumpClose {
                                state: (
                                    cello::ws_message::WS_MESSAGE_TOO_BIG_CODE,
                                    cello::ws_message::WS_MESSAGE_TOO_BIG_REASON.to_string(),
                                    false,
                                ),
                                initiator: CloseInitiator::Application,
                            };
                        }
                        ReadEnd::Transport => {}
                    }
                    // Settle the outbound write before attempting the loud
                    // close: the attempt needs the writer mutex, which a
                    // parked outbound write may hold while it waits for the
                    // peer to read. Poll the forwarder first — its settle
                    // keeps the in-flight send alive for the remainder of
                    // the progress bound — and only then attempt the loud
                    // 1011 or 1009 under the close deadline if the writer is
                    // still at a frame boundary. Otherwise drop the
                    // transport instead of writing another frame. The cell
                    // learns the same code through the close dispatch the
                    // caller runs after the pump returns.
                    let _ = shutdown_tx.send(true);
                    let (write_end, receiver) = write.await;
                    // Either direction can trip the overload while the other
                    // wins the race: the forwarder may detect OverLimit and
                    // settle a parked write while the peer's close ends the
                    // read first. The joined write outcome still reports the
                    // loud 1011 — the peer's clean close must not mask an
                    // outbound flood — and the wire attempt below runs when
                    // the parked write settled intact.
                    if matches!(write_end, PipeDirectionEnd::OverLimit) {
                        close = PumpClose {
                            state: (
                                cello::js::WS_PIPE_OVERLOAD_CODE,
                                cello::js::WS_PIPE_OVERLOAD_REASON.to_string(),
                                false,
                            ),
                            initiator: CloseInitiator::Application,
                        };
                    }
                    let loud_1011 =
                        read_loud_1011 || matches!(write_end, PipeDirectionEnd::OverLimit);
                    if loud_1011 && !writer.poisoned() {
                        let _ = cello::asyncrt::timeout(
                            PUMP_SEND_PROGRESS_TIMEOUT,
                            write_ws_out(&writer, cello::js::WsOut::Overload),
                        )
                        .await;
                    }
                    // The same settle-then-write shape as the 1011 above,
                    // with the direct 1009 frame: the attempt still needs a
                    // writer at a frame boundary, and a poisoned writer is
                    // dropped instead of writing after a partial frame. The
                    // cell learns the same 1009 through the close dispatch
                    // the caller runs after the pump returns.
                    if read_loud_1009 && !writer.poisoned() {
                        let _ = cello::asyncrt::timeout(
                            PUMP_SEND_PROGRESS_TIMEOUT,
                            pump_write_frame(
                                &writer,
                                fastwebsockets::Frame::close(
                                    cello::ws_message::WS_MESSAGE_TOO_BIG_CODE,
                                    cello::ws_message::WS_MESSAGE_TOO_BIG_REASON.as_bytes(),
                                ),
                                None,
                            ),
                        )
                        .await;
                    }
                    receiver
                }
                (write_end, receiver) = &mut write => {
                    match write_end {
                        PipeDirectionEnd::Close(code, reason) => {
                            // The forwarder ends the direction on the close
                            // without writing it — only data crosses the sink —
                            // so the pump writes the cell's close frame here,
                            // behind everything the forwarder flushed ahead of
                            // it, as the pre-forwarder pump wrote every output
                            // inline. Without this the peer sees EOF instead of
                            // the close. Best-effort under the closing progress
                            // bound, and only at a frame boundary: after a
                            // timed-out partial write the peer would read the
                            // terminal as a continuation of the unfinished
                            // frame, so a poisoned writer is dropped instead.
                            // The close dispatch below already told the cell,
                            // and the echo after the drain decides the
                            // protocol close.
                            // A close the peer never sees is an abnormal
                            // closure, not a clean one: a timed-out data
                            // write poisons the writer so the queued close
                            // is skipped, and the close write itself can
                            // time out the same way. Either way the peer
                            // observes a transport drop, so report 1006 /
                            // unclean the way every other transport end does
                            // instead of claiming the handshake completed.
                            // Only a close that reaches the wire reports the
                            // clean application close.
                            let close_reached_wire = if writer.poisoned() {
                                false
                            } else {
                                matches!(
                                    cello::asyncrt::timeout(
                                        PUMP_SEND_PROGRESS_TIMEOUT,
                                        pump_write_frame(
                                            &writer,
                                            fastwebsockets::Frame::close(
                                                code,
                                                reason.as_bytes(),
                                            ),
                                            None,
                                        ),
                                    )
                                    .await,
                                    Ok(Ok(()))
                                )
                            };
                            close = PumpClose {
                                state: if outbound_close_is_clean && close_reached_wire {
                                    (code, reason, true)
                                } else {
                                    (1006, String::new(), false)
                                },
                                initiator: CloseInitiator::Application,
                            };
                        }
                        // A flood staged past the cap, or an admission refusal
                        // queued behind admitted data: the same loud 1011 the
                        // forwarder reports for every other pipe, and the same
                        // loud close on the socket the read path writes — the
                        // marker cut admitted data, so the peer must observe it.
                        PipeDirectionEnd::OverLimit => {
                            close = PumpClose {
                                state: (
                                    cello::js::WS_PIPE_OVERLOAD_CODE,
                                    cello::js::WS_PIPE_OVERLOAD_REASON.to_string(),
                                    false,
                                ),
                                initiator: CloseInitiator::Application,
                            };
                            if !writer.poisoned() {
                                let _ = cello::asyncrt::timeout(
                                    PUMP_SEND_PROGRESS_TIMEOUT,
                                    write_ws_out(&writer, cello::js::WsOut::Overload),
                                )
                                .await;
                            }
                        }
                        // The socket is gone: fail-closed, default close. The
                        // read is dropped here. That is safe only because the
                        // pump is over and the socket is never read again.
                        PipeDirectionEnd::Retired => {}
                    }
                    receiver
                }
            }
        };
    }
    (close, writer, receiver, budget_out)
}

/// Carry a client socket into the top-level Worker request that returned it.
///
/// A Durable Object has a stable scope, so its frames can return through a
/// later cell dispatch. A stateless Worker has no such address. Its pending
/// `__ws_next` operation therefore keeps this request and isolate resident,
/// and this task carries each client frame through that operation's queue.
async fn worker_websocket_task<S>(
    worker: cello::js::WorkerWebSocket,
    mut socket: fastwebsockets::WebSocket<S>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    socket.set_auto_close(false);
    let id = worker.id();
    let inbound = worker.inbound();
    let (outbound, outputs) = mpsc::unbounded_channel();
    // Same admission cap as every other pipe direction: an isolate that
    // floods a peer which never reads ends loudly with 1011. The
    // registration returns the effective budget, which the pump releases
    // as it receives.
    let budget = cello::js::ws_register_budgeted(
        id,
        outbound,
        cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
    );
    let (close, writer, _outputs, _budget) = pump_cell_socket(
        socket,
        outputs,
        true,
        move |data| {
            let inbound = inbound.clone();
            async move {
                inbound
                    .send(data.into())
                    .await
                    .map_err(|_| anyhow::anyhow!("Worker stopped reading WebSocket"))
            }
        },
        budget,
    )
    .await;
    if close.initiator == CloseInitiator::Peer {
        echo_websocket_close(&writer, &close.state, false).await;
    }
    let _ = worker
        .inbound()
        .send_close(close.state.0, close.state.1, close.state.2);
}

/// Drain the close handler's queued frames after the pump: the handler runs
/// while `dispatch_ws_closed` drives V8, so its output lands in the channel
/// only after the pump handed the receiver back. Each drained write still
/// runs under the progress bound, and a terminal frame — or a peer that
/// stopped reading mid-drain — ends the drain instead of holding the socket
/// task open. Best-effort: the cell already learned the close, and the write
/// forwarder owns no frames of its own.
///
/// A poisoned writer ends the drain without writing: a write may have
/// stopped mid-frame, so no further frame may follow it — the transport is
/// dropped instead.
///
/// Each data frame the drain removes releases its admission charge, even
/// when its write fails: the pump returned the receiver but not the budget,
/// and a pending output gate can release more frames after the drain. With
/// the drained bytes still charged, the registry would refuse the new frames
/// for bytes no longer queued. Close and Overload markers carry no charge.
async fn drain_close_outputs<S>(
    writer: &PumpWriter<S>,
    outputs: &mut mpsc::UnboundedReceiver<cello::js::WsOut>,
    budget: &cello::js::WsPipeBudget,
) -> bool
where
    S: tokio::io::AsyncWrite + Unpin,
{
    let mut handler_sent_close = false;
    while !writer.poisoned() {
        let Ok(output) = outputs.try_recv() else {
            break;
        };
        if let data @ (cello::js::WsOut::Text(_) | cello::js::WsOut::Binary(_)) = &output {
            budget.release(cello::js::ws_out_bytes(data));
        }
        handler_sent_close |= matches!(output, cello::js::WsOut::Close(_, _));
        let written =
            cello::asyncrt::timeout(PUMP_SEND_PROGRESS_TIMEOUT, write_ws_out(writer, output)).await;
        if !matches!(written, Ok(true)) {
            break;
        }
    }
    handler_sent_close
}

pub(super) async fn websocket_task<S>(
    app: AppHandle,
    target: cello::js::WsTarget,
    mut socket: fastwebsockets::WebSocket<S>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    socket.set_auto_close(false);
    let route = SocketRoute::of(&target);
    let (outbound, outputs) = mpsc::unbounded_channel();
    // Admit the cell's frames under the pipe's staged cap, as in the cell
    // pipe: a flood into a peer that never reads trips the budget, and the
    // pump ends the socket loudly with 1011. The registration returns the
    // effective budget, which the pump releases as it receives.
    let budget = cello::js::ws_register_budgeted(
        target.id,
        outbound,
        cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
    );
    // A close the cell sends does not become the close state here: the echo
    // below decides what the peer observes, so the pump must not claim the
    // close was clean.
    let (close, writer, mut outputs, budget) = {
        let app = &app;
        let route = &route;
        let id = target.id;
        pump_cell_socket(
            socket,
            outputs,
            false,
            move |data| dispatch_ws_message(app, route, id, data),
            budget,
        )
        .await
    };
    // A poisoned writer may have stopped mid-frame, so no further frame may
    // follow it: drop it — closing the transport — before the close handler
    // runs, instead of making the peer wait on the handler for EOF. The
    // drain and the echo below are skipped alike.
    let writer = (!writer.poisoned()).then_some(writer);
    if let Err(error) = dispatch_ws_closed(
        &app,
        &route,
        target.id,
        close.state.0,
        close.state.1.clone(),
        close.state.2,
    )
    .await
    {
        tracing::warn!(
            %error,
            scope = %target.scope,
            websocket = target.id,
            "WebSocket close dispatch failed"
        );
    }

    // The close handler is allowed to choose the response code and reason.
    // Its output is queued while dispatch_ws_closed drives V8, so flush it
    // before unregistering the socket or considering a protocol-level echo.
    //
    // Its output can also still be behind the output gate: the handler may
    // read, and what it reads can belong to a write another request has not
    // proved durable yet. The drain below does not block, so a frame still in
    // flight leaves `handler_sent_close` false and the peer is answered with
    // the echo of its own close -- a clean close carrying the wrong reason,
    // which is indistinguishable from the handler choosing it.
    //
    // Bounded, and skipped outright on a draining node. The drain loop polls
    // the gate calls it already dispatched but never receives a new one: it
    // has no `gate_rx` arm. A ticket taken after the main loop broke is sent
    // to a live but unread channel and parks for good, so waiting on it here
    // would hold the socket task open until the drain hits its deadline. The
    // bound covers the same park reached the other way: `is_draining` is read
    // once, and shutdown can begin between that read and the wait. Giving up
    // costs the reason the handler chose, never a frame the gate has not
    // cleared -- the drain below still finds nothing, the echo still answers
    // the peer, and a frame released later lands on a socket that is gone.
    if !app.is_draining() {
        let waited = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            cello::js::ws_await_flushes(target.id),
        )
        .await;
        if waited.is_err() {
            tracing::warn!(
                scope = %target.scope,
                websocket = target.id,
                "gave up waiting for the close handler's frames"
            );
        }
    }
    // Each drained write still runs under the progress bound, and a terminal
    // frame — or a peer that stopped reading mid-drain — ends the drain
    // instead of holding the socket task open. Best-effort: the cell
    // already learned the close above, and the write forwarder owns no
    // frames of its own.
    if let Some(writer) = &writer {
        let handler_sent_close = drain_close_outputs(writer, &mut outputs, &budget).await;
        echo_websocket_close(writer, &close.state, handler_sent_close).await;
    }
    app.websocket_closed(route.cell().to_string(), target.id);
    cello::js::ws_unregister(target.id);
}

/// Headers that the WebSocket server must compute or remove for a 101.
///
/// `fastwebsockets` creates its response before the Worker runs, so blindly
/// appending the Worker's response afterwards can replace the accept hash or
/// restore framing fields that an upgrade cannot carry. This is the exclusion
/// set in kj's `acceptWebSocket`: it forces the three handshake fields, drops
/// the obsolete or request-only fields, and drops `Sec-WebSocket-Extensions`.
fn forwards_worker_websocket_header(name: &str) -> bool {
    !matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "upgrade"
            | "sec-websocket-accept"
            | "keep-alive"
            | "te"
            | "trailer"
            | "content-length"
            | "transfer-encoding"
            | "sec-websocket-key"
            | "sec-websocket-version"
            | "sec-websocket-extensions"
    )
}

pub(crate) async fn handle_websocket(mut request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let started = Instant::now();
    let cancellation = cello::runtime::RequestCancellationLifetime::stateless();
    let request_id = cancellation.request_id();
    let (mut upgrade_response, upgrade) = match fastwebsockets::upgrade::upgrade(&mut request) {
        Ok(upgrade) => upgrade,
        Err(error) => return response(StatusCode::BAD_REQUEST, format!("ws upgrade: {error}")),
    };
    let runtime = app.runtime.as_ref().expect("WebSocket runtime checked");
    let body_started = Instant::now();
    let (url, method, body, headers) = match request_payload(
        request,
        app.trust_forwarded_headers,
        app.max_request_body_bytes,
    )
    .await
    {
        Ok(payload) => payload,
        Err(response) => return *response,
    };
    let body_read_us = body_started.elapsed().as_micros() as u64;
    let worker_started = Instant::now();
    let mut worker_response = match runtime
        .fetch_worker_pool(url, method, body.into(), headers, cancellation)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            emit_websocket_connection_timing(
                runtime,
                request_id,
                started,
                body_read_us,
                worker_started.elapsed().as_micros() as u64,
                WebSocketConnectionOutcome {
                    outcome: "worker_error",
                    route: "",
                    scope: "",
                    status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                },
            );
            return response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Worker failed: {error:#}"),
            );
        }
    };
    let worker_dispatch_us = worker_started.elapsed().as_micros() as u64;
    let websocket = match worker_response.websocket.take() {
        Some(websocket) => match PendingWebSocket::new(websocket) {
            Ok(websocket) => websocket,
            Err(error) => {
                return response(
                    StatusCode::BAD_GATEWAY,
                    format!("tunneled WebSocket upgrade: {error}"),
                );
            }
        },
        None => {
            emit_websocket_connection_timing(
                runtime,
                request_id,
                started,
                body_read_us,
                worker_dispatch_us,
                WebSocketConnectionOutcome {
                    outcome: "rejected",
                    route: "",
                    scope: "",
                    status: worker_response.status,
                },
            );
            return runtime_response(worker_response, false);
        }
    };
    let target = websocket.target();
    if worker_response.status != 101 {
        emit_websocket_connection_timing(
            runtime,
            request_id,
            started,
            body_read_us,
            worker_dispatch_us,
            WebSocketConnectionOutcome {
                outcome: "rejected",
                route: "",
                scope: target.map_or("", |target| target.scope.as_str()),
                status: worker_response.status,
            },
        );
        if let Some(target) = target {
            reject_accepted_websocket(&app, target, "unsupported WebSocket route").await;
        }
        return response(StatusCode::BAD_GATEWAY, "unsupported WebSocket route");
    }
    // Workerd shallow-copies the Worker's complete header list into
    // `acceptWebSocket`, which preserves duplicate application fields such as
    // `Set-Cookie`. Validate the complete batch before changing the response,
    // because returning a partially copied handshake is never useful.
    let mut worker_headers = Vec::new();
    for (name, value) in &worker_response.headers {
        if !forwards_worker_websocket_header(name) {
            continue;
        }
        let parsed = hyper::header::HeaderName::from_bytes(name.as_bytes())
            .ok()
            .zip(hyper::header::HeaderValue::from_str(value).ok());
        let Some(header) = parsed else {
            let reason = format!("invalid WebSocket response header: {name}");
            emit_websocket_connection_timing(
                runtime,
                request_id,
                started,
                body_read_us,
                worker_dispatch_us,
                WebSocketConnectionOutcome {
                    outcome: "rejected",
                    route: "",
                    scope: target.map_or("", |target| target.scope.as_str()),
                    status: StatusCode::BAD_GATEWAY.as_u16(),
                },
            );
            if let Some(target) = target {
                reject_accepted_websocket(&app, target, &reason).await;
            }
            return response(StatusCode::BAD_GATEWAY, "invalid WebSocket response header");
        };
        worker_headers.push(header);
    }
    for (name, value) in worker_headers {
        upgrade_response.headers_mut().append(name, value);
    }
    emit_websocket_connection_timing(
        runtime,
        request_id,
        started,
        body_read_us,
        worker_dispatch_us,
        WebSocketConnectionOutcome {
            outcome: "accepted",
            route: match target {
                Some(target) if target.tunnel.is_some() => "remote",
                Some(_) => "local",
                None => "worker",
            },
            scope: target.map_or("", |target| target.scope.as_str()),
            status: 101,
        },
    );
    let task_app = app.clone();
    let task = Box::pin(run_websocket_upgrade(task_app, upgrade, websocket));
    if app.websockets.send(task).is_err() {
        return response(
            StatusCode::SERVICE_UNAVAILABLE,
            "WebSocket executor stopped",
        );
    }
    upgrade_response.map(|body| body.map_err(|never| match never {}).boxed_unsync())
}

/// Finish the client upgrade through the same owned response value that
/// survives validation. Keeping this work in one future makes a failed client
/// upgrade and a cancelled executor task drop the same owned response.
async fn run_websocket_upgrade(
    task_app: AppHandle,
    upgrade: fastwebsockets::upgrade::UpgradeFut,
    websocket: PendingWebSocket,
) {
    match (upgrade.await, websocket) {
        (Ok(socket), PendingWebSocket::Worker(worker)) => {
            worker_websocket_task(worker, socket).await;
        }
        (Ok(socket), PendingWebSocket::Tunnel { upgrade, .. }) => {
            if let Err(error) = super::peer_tunnel::splice(socket.into_inner(), upgrade).await {
                eprintln!("cello tunneled WebSocket splice ended: {error:#}");
            }
        }
        (Ok(socket), PendingWebSocket::Cell(target)) => {
            websocket_task(task_app, target, socket).await;
        }
        (Err(error), PendingWebSocket::Worker(_)) => {
            eprintln!("cello Worker WebSocket upgrade failed: {error}");
        }
        (Err(error), PendingWebSocket::Cell(target)) => {
            eprintln!("cello WebSocket upgrade failed: {error}");
            finish_websocket(&task_app, &target, 1006, String::new(), false).await;
        }
        (Err(error), PendingWebSocket::Tunnel { .. }) => {
            eprintln!("cello tunneled WebSocket upgrade failed: {error}");
        }
    }
}

#[cfg(all(test, cello_internal_tests))]
pub(super) async fn run_websocket_upgrade_for_test(
    task_app: AppHandle,
    upgrade: fastwebsockets::upgrade::UpgradeFut,
    websocket: HttpResponseWebSocket,
) {
    let websocket = PendingWebSocket::new(websocket).unwrap();
    run_websocket_upgrade(task_app, upgrade, websocket).await;
}

struct WebSocketConnectionOutcome<'a> {
    outcome: &'a str,
    route: &'a str,
    scope: &'a str,
    status: u16,
}

fn emit_websocket_connection_timing(
    runtime: &RuntimeManager,
    request_id: cello::js::RequestId,
    started: Instant,
    body_read_us: u64,
    worker_dispatch_us: u64,
    event_outcome: WebSocketConnectionOutcome<'_>,
) {
    let WebSocketConnectionOutcome {
        outcome,
        route,
        scope,
        status,
    } = event_outcome;
    tracing::debug!(
        target: "timing",
        event = "websocket_connection_timing",
        outcome,
        route,
        scope,
        request_id = %cello::js::request_id_string(request_id),
        node = runtime.node(),
        region = runtime.region(),
        runtime_version = env!("CARGO_PKG_VERSION"),
        status,
        total_us = started.elapsed().as_micros() as u64,
        body_read_us,
        worker_dispatch_us,
        "WebSocket connection resolved"
    );
}

// Both cell socket loops had the same hazard as the tunnel: they read the socket
// in the same select! as the cell's outbound queue, so an outbound frame that
// won the race dropped a read in progress. A lost frame keeps the workload
// ledger exact while delivery is not, so frame-level observation is required.
#[cfg(all(test, cello_internal_tests))]
mod socket_cancel_private {
    include!(env!("CELLO_CONFORMANCE_SOCKET_CANCEL_TESTS"));
}

#[cfg(test)]
mod overload_tests {
    use super::*;

    /// A pre-registration burst flushes its overload marker into a plain
    /// external pump: the socket must end with a loud 1011 close frame, not
    /// a panic. The reverted `unreachable!` arm fails this test instead of
    /// passing it (in production the same panic aborts the node).
    #[test]
    fn overload_marker_writes_loud_1011_close() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime builds");
        runtime.block_on(async {
            let (mut client, server) = tokio::io::duplex(64 * 1024);
            let socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            let (_reader, writer) = socket.split(tokio::io::split);
            let writer: PumpWriter<tokio::io::DuplexStream> = PumpWriter {
                writer: std::sync::Arc::new(tokio::sync::Mutex::new(writer)),
                poisoned: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            };
            let open = write_ws_out(&writer, cello::js::WsOut::Overload).await;
            assert!(!open, "the overload marker ends the socket");
            drop(writer);
            // The peer observes one unmasked close frame: 1011 + reason.
            use tokio::io::AsyncReadExt;
            let mut head = [0u8; 2];
            client
                .read_exact(&mut head)
                .await
                .expect("close frame head");
            assert_eq!(head[0], 0x88, "FIN + close opcode");
            let len = (head[1] & 0x7f) as usize;
            let mut payload = vec![0u8; len];
            client
                .read_exact(&mut payload)
                .await
                .expect("close payload");
            assert_eq!(
                u16::from_be_bytes([payload[0], payload[1]]),
                cello::js::WS_PIPE_OVERLOAD_CODE,
                "loud 1011 close"
            );
            assert_eq!(
                &payload[2..],
                cello::js::WS_PIPE_OVERLOAD_REASON.as_bytes(),
                "overload reason"
            );
        });
    }
}

#[cfg(test)]
mod pump_tests {
    use super::*;

    /// A process-shared runtime for the pump tests: the crate-root
    /// `bin_test_runtime`, shared with every other bin test module, which
    /// installs the asyncrt host handle exactly once for the whole test
    /// process. The pump's `asyncrt::timeout` reads the process domain
    /// clock, and a second install would trip `set_host_handle`'s
    /// exactly-once assert and poison every later pump test.
    fn test_runtime() -> &'static tokio::runtime::Runtime {
        crate::bin_test_runtime()
    }

    /// One server-to-client frame off a duplex pair: server frames are
    /// unmasked. Handles the short, 16-bit, and 64-bit length forms, so the
    /// trickle test reads its 64 KiB frame through the same helper.
    async fn read_server_frame(client: &mut (impl tokio::io::AsyncRead + Unpin)) -> (u8, Vec<u8>) {
        use tokio::io::AsyncReadExt;
        let mut head = [0u8; 2];
        client.read_exact(&mut head).await.expect("frame head");
        assert_eq!(head[0] & 0x80, 0x80, "FIN must be set");
        assert_eq!(head[1] & 0x80, 0, "server frames are unmasked");
        let len = match head[1] & 0x7f {
            126 => {
                let mut ext = [0u8; 2];
                client.read_exact(&mut ext).await.expect("extended length");
                u16::from_be_bytes(ext) as usize
            }
            127 => {
                let mut ext = [0u8; 8];
                client.read_exact(&mut ext).await.expect("extended length");
                u64::from_be_bytes(ext) as usize
            }
            len => len as usize,
        };
        let mut payload = vec![0u8; len];
        client
            .read_exact(&mut payload)
            .await
            .expect("frame payload");
        (head[0] & 0x0f, payload)
    }

    /// One masked client-to-server frame onto a raw write half: the flood
    /// test splits its client stream so its sender can park while its reader
    /// still observes the loud close. Masked, as a server-role socket
    /// requires; written in chunks so the flood never holds two copies.
    /// Always final: fragmented tests use `write_masked_fragment`.
    async fn write_masked_frame(
        writer: &mut (impl tokio::io::AsyncWrite + Unpin),
        opcode: u8,
        payload: &[u8],
    ) -> std::io::Result<()> {
        write_masked_fragment(writer, true, opcode, payload).await
    }

    /// One masked client-to-server frame with an explicit FIN bit: a
    /// fragment flood starts with FIN clear and never sets it, while the
    /// reassembly test ends its message with FIN set. Same masking and
    /// chunking as `write_masked_frame`.
    async fn write_masked_fragment(
        writer: &mut (impl tokio::io::AsyncWrite + Unpin),
        fin: bool,
        opcode: u8,
        payload: &[u8],
    ) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        let mut head = vec![(if fin { 0x80 } else { 0x00 }) | opcode, 0x80];
        if payload.len() < 126 {
            head[1] |= payload.len() as u8;
        } else if payload.len() <= u16::MAX as usize {
            head[1] |= 126;
            head.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        } else {
            head[1] |= 127;
            head.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        }
        let mask = [0x12u8, 0x34, 0x56, 0x78];
        head.extend_from_slice(&mask);
        writer.write_all(&head).await?;
        for (i, chunk) in payload.chunks(8192).enumerate() {
            let base = i * 8192;
            let masked: Vec<u8> = chunk
                .iter()
                .enumerate()
                .map(|(j, byte)| byte ^ mask[(base + j) % 4])
                .collect();
            writer.write_all(&masked).await?;
        }
        Ok(())
    }

    /// A slow handler keeps its socket: the handler stalls six real seconds
    /// — an AI call, not queue backlog — past the old 5 s dispatch bound,
    /// and the pump still ends on the peer's close, clean. The stall is real
    /// time because the old bound measured it, but nothing synchronizes on
    /// it: the entered and done gates order every assertion. Reintroducing
    /// a 5 s dispatch bound drops the parked handler at 5 s, so the done
    /// gate never arrives and this fails before the change.
    #[test]
    fn slow_handler_keeps_socket() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            // As the production callers do: the pump owns the close echo,
            // so the collector must not auto-echo a peer close through the
            // bounded reply write behind a parked socket write.
            socket.set_auto_close(false);
            let (_outbound, outputs) = mpsc::unbounded_channel();
            let (entered_tx, mut entered_rx) = mpsc::unbounded_channel::<()>();
            let (done_tx, mut done_rx) = mpsc::unbounded_channel::<cello::js::WsIn>();
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                false,
                move |data| {
                    let entered_tx = entered_tx.clone();
                    let done_tx = done_tx.clone();
                    async move {
                        let _ = entered_tx.send(());
                        tokio::time::sleep(std::time::Duration::from_secs(6)).await;
                        let _ = done_tx.send(data);
                        Ok::<(), anyhow::Error>(())
                    }
                },
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            let mut client = fastwebsockets::WebSocket::after_handshake(
                client_stream,
                fastwebsockets::Role::Client,
            );
            client
                .write_frame(fastwebsockets::Frame::text(b"hello".to_vec().into()))
                .await
                .expect("peer sends one frame");
            entered_rx.recv().await.expect("dispatch starts");
            // Six real seconds, past the old bound: the handler must run to
            // completion instead of the pump timing it out.
            let data = tokio::time::timeout(std::time::Duration::from_secs(30), done_rx.recv())
                .await
                .expect("a slow handler must run, not time out")
                .expect("handler records its frame");
            assert!(
                matches!(&data, cello::js::WsIn::Text(text) if text == "hello"),
                "the handler ran to completion on its frame"
            );
            assert!(
                !pump.is_finished(),
                "the pump must still be parked on the live socket"
            );
            client
                .write_frame(fastwebsockets::Frame::close(1000, b"bye"))
                .await
                .expect("peer closes");
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("the peer close must end the pump")
                .expect("pump joins");
            assert_eq!(
                close,
                PumpClose {
                    state: (1000, "bye".to_string(), true),
                    initiator: CloseInitiator::Peer,
                },
                "the pump ends on the peer close, clean"
            );
        });
    }

    /// A slow but live peer is never killed on the normal write path: the
    /// write parks for six real seconds with no progress and still drains
    /// fully — no terminal, no skipped data. The stall is real time because
    /// the old bound measured it, but nothing synchronizes on it: the first
    /// frame the trickle reads is the whole assertion. Reintroducing a 5 s
    /// per-write bound skips the parked data for a terminal, so the trickle
    /// reads a close instead of its data and this fails before the change.
    #[test]
    fn trickle_client_stays_connected() {
        test_runtime().block_on(async {
            let (mut client_stream, server) = tokio::io::duplex(1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            // Held, not dropped: in production the registry owns the sender
            // until the pump returns, so a channel close here would retire
            // the write direction before the test even starts stalling it.
            let (_outbound, outputs) = mpsc::unbounded_channel();
            let data = vec![7u8; 64 * 1024];
            _outbound
                .send(cello::js::WsOut::Binary(data.clone()))
                .expect("stage frame");
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                true,
                |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            // Six real seconds with no progress on a parked write: a slow but
            // live peer must not be killed for it.
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
            assert!(
                !pump.is_finished(),
                "a slow but live peer must not be killed on the normal write path"
            );
            // Then the trickle drains: the full frame, intact, first.
            let (opcode, payload) = read_server_frame(&mut client_stream).await;
            assert_eq!(opcode, 0x2, "the parked data drains, not a terminal");
            assert_eq!(payload, data, "the drained frame is intact");
            // No terminal follows the data: dropping the client ends the pump
            // on the transport instead.
            drop(client_stream);
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("dropping the peer must end the pump")
                .expect("pump joins");
            assert_eq!(close.initiator, CloseInitiator::Transport);
            assert_eq!(close.state, (1006, String::new(), false));
        });
    }

    /// A ping behind a parked data write must not kill a slow but live peer:
    /// the pong waits for the writer as long as the data write does, and its
    /// own bound starts only once it holds the writer. The peer pings while
    /// the server's 64 KiB write is parked on a full pipe, stays silent for
    /// six real seconds, then reads: the data arrives intact, followed by
    /// the pong. A reply bound that also covers the wait for the writer
    /// fires at 5 s, poisons the writer and ends the pump, so this fails
    /// before the change.
    #[test]
    fn ping_behind_parked_write_keeps_socket() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            let (_outbound, outputs) = mpsc::unbounded_channel();
            let data = vec![7u8; 64 * 1024];
            _outbound
                .send(cello::js::WsOut::Binary(data.clone()))
                .expect("stage frame");
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                true,
                |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            let (mut reader, mut writer) = tokio::io::split(client_stream);
            // The data frame's head proves its write holds the writer; the
            // 64 KiB payload still parks on the 1 KiB pipe. Only then ping,
            // so the pong queues behind the parked write.
            use tokio::io::AsyncReadExt;
            let mut head = [0u8; 10];
            reader.read_exact(&mut head).await.expect("data frame head");
            assert_eq!(head[0], 0x82, "the data frame's write starts first");
            assert_eq!(head[1], 127, "64 KiB uses the 64-bit length form");
            assert_eq!(
                u64::from_be_bytes(head[2..].try_into().expect("length")),
                data.len() as u64
            );
            write_masked_frame(&mut writer, 0x9, b"ping")
                .await
                .expect("peer pings");
            tokio::time::sleep(std::time::Duration::from_secs(6)).await;
            assert!(
                !pump.is_finished(),
                "a pong queued behind a parked data write must not end the pump"
            );
            let mut payload = vec![0u8; data.len()];
            reader
                .read_exact(&mut payload)
                .await
                .expect("the parked data drains intact");
            assert_eq!(payload, data, "the drained frame is intact");
            let (opcode, payload) = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                read_server_frame(&mut reader),
            )
            .await
            .expect("the pong must follow the data");
            assert_eq!(opcode, 0xA, "the pong follows the data");
            assert_eq!(payload, b"ping", "the pong echoes the ping payload");
            drop((reader, writer));
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("dropping the peer must end the pump")
                .expect("pump joins");
            assert_eq!(close.initiator, CloseInitiator::Transport);
        });
    }

    /// A flood past the staged cap ends loudly: the cell never reads, the
    /// pump stages past 16 MiB and reports the application-sent 1011, which
    /// also crosses the socket. The bound covers only staged bytes — handler
    /// runtime stays unbounded (see slow_handler_keeps_socket). Removing the
    /// cap check parks the pump forever, so the guard fails before the
    /// change.
    #[test]
    fn flood_past_cap_gets_1011() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            let (_outbound, outputs) = mpsc::unbounded_channel();
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                false,
                |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            let (mut reader, mut writer) = tokio::io::split(client_stream);
            // Twenty megabytes into a cell that never reads, through the
            // independent write direction while the reader still observes.
            // The sender stops on error: the pump stops reading once it
            // reports the flood.
            let sender = tokio::spawn(async move {
                for _ in 0..20 {
                    let chunk = vec![0u8; 1024 * 1024];
                    if write_masked_frame(&mut writer, 0x2, &chunk).await.is_err() {
                        break;
                    }
                }
            });
            // The loud close is the only frame the server ever sends here.
            // Under a deadline of its own: without the pump's close the
            // frame never arrives, and the read must fail rather than stall
            // the test run ahead of the join timeout below.
            let (opcode, payload) = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                read_server_frame(&mut reader),
            )
            .await
            .expect("the loud close must arrive");
            assert_eq!(opcode, 0x8, "the flood ends in a close frame");
            assert_eq!(
                u16::from_be_bytes([payload[0], payload[1]]),
                cello::js::WS_PIPE_OVERLOAD_CODE,
                "loud 1011 close"
            );
            assert_eq!(
                &payload[2..],
                cello::js::WS_PIPE_OVERLOAD_REASON.as_bytes(),
                "overload reason"
            );
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("the flood must end the pump")
                .expect("pump joins");
            assert_eq!(
                close,
                PumpClose {
                    state: (
                        cello::js::WS_PIPE_OVERLOAD_CODE,
                        cello::js::WS_PIPE_OVERLOAD_REASON.to_string(),
                        false,
                    ),
                    initiator: CloseInitiator::Application,
                },
                "the pump reports the application-sent 1011"
            );
            sender.abort();
        });
    }

    /// A fragment flood past the message cap ends loudly with 1009: a FIN=0
    /// frame plus continuations past 32 MiB, into a pump whose dispatch is
    /// parked, ends the socket with the application-sent 1009. The close
    /// arrives while the sender is still parked mid-flood, proving the
    /// server never buffers the whole flood — only the cap plus the frame
    /// in flight. Without the cap the pump keeps buffering and the close
    /// never arrives, so the read times out and this fails before the fix.
    #[test]
    fn fragment_flood_past_cap_gets_1009() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            let (_outbound, outputs) = mpsc::unbounded_channel();
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                false,
                |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            let (mut reader, mut writer) = tokio::io::split(client_stream);
            // Forty-eight megabytes that never ends: the sender parks once
            // the pump stops reading past the cap, and the close below
            // arrives while megabytes are still unsent.
            let sender = tokio::spawn(async move {
                let chunk = vec![0u8; 1024 * 1024];
                if write_masked_fragment(&mut writer, false, 0x2, &chunk)
                    .await
                    .is_err()
                {
                    return;
                }
                for _ in 1..48 {
                    if write_masked_fragment(&mut writer, false, 0x0, &chunk)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
            let (opcode, payload) = tokio::time::timeout(
                std::time::Duration::from_secs(60),
                read_server_frame(&mut reader),
            )
            .await
            .expect("the loud close must arrive");
            assert_eq!(opcode, 0x8, "the fragment flood ends in a close frame");
            assert_eq!(
                u16::from_be_bytes([payload[0], payload[1]]),
                cello::ws_message::WS_MESSAGE_TOO_BIG_CODE,
                "loud 1009 close"
            );
            assert_eq!(
                &payload[2..],
                cello::ws_message::WS_MESSAGE_TOO_BIG_REASON.as_bytes(),
                "message-too-big reason"
            );
            assert!(
                !sender.is_finished(),
                "the close arrives before the endless flood is fully sent"
            );
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("the flood must end the pump")
                .expect("pump joins");
            assert_eq!(
                close,
                PumpClose {
                    state: (
                        cello::ws_message::WS_MESSAGE_TOO_BIG_CODE,
                        cello::ws_message::WS_MESSAGE_TOO_BIG_REASON.to_string(),
                        false,
                    ),
                    initiator: CloseInitiator::Application,
                },
                "the pump reports the application-sent 1009"
            );
            sender.abort();
        });
    }

    /// Fragments reassemble in order below the caps: a FIN=0 frame plus two
    /// continuations arrive as the one message the peer sent, proving the
    /// capped reader still defragments the ordinary case, and the peer's
    /// close then ends the pump as its own clean close. Returning a
    /// fragment per read — instead of waiting for FIN — would dispatch
    /// three messages, so the single dispatch fails before the fix.
    #[test]
    fn fragmented_message_reassembles_in_order() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            let (_outbound, outputs) = mpsc::unbounded_channel();
            let (dispatch_tx, mut dispatch_rx) = mpsc::unbounded_channel::<cello::js::WsIn>();
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                false,
                move |data| {
                    let dispatch_tx = dispatch_tx.clone();
                    async move {
                        let _ = dispatch_tx.send(data);
                        Ok::<(), anyhow::Error>(())
                    }
                },
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            let (_, mut writer) = tokio::io::split(client_stream);
            write_masked_fragment(&mut writer, false, 0x2, b"first")
                .await
                .expect("first fragment writes");
            write_masked_fragment(&mut writer, false, 0x0, b"second")
                .await
                .expect("continuation writes");
            write_masked_fragment(&mut writer, true, 0x0, b"third")
                .await
                .expect("final fragment writes");
            let data = tokio::time::timeout(std::time::Duration::from_secs(30), dispatch_rx.recv())
                .await
                .expect("the reassembled message must dispatch")
                .expect("dispatch channel stays open");
            assert!(
                matches!(&data, cello::js::WsIn::Binary(bytes) if bytes == b"firstsecondthird"),
                "fragments arrive as the one message the peer sent"
            );
            write_masked_frame(&mut writer, 0x8, &[0x03, 0xe8])
                .await
                .expect("peer closes");
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("the peer close must end the pump")
                .expect("pump joins");
            assert_eq!(
                close,
                PumpClose {
                    state: (1000, String::new(), true),
                    initiator: CloseInitiator::Peer,
                },
                "the peer close still ends the socket as its own clean close"
            );
        });
    }

    /// A close queued behind data crosses in order: the pump flushes the
    /// queued frames and writes the close behind them before reporting the
    /// application close. The whole queue is staged before the pump starts,
    /// so no timing is involved. Returning the close without the flush drops
    /// the data, so the first frame is a close and this fails before the
    /// change; never writing the close leaves the peer at EOF, which fails
    /// the terminal read the same way.
    #[test]
    fn close_behind_data_arrives_in_order() {
        test_runtime().block_on(async {
            let (mut client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            // Held, not dropped: a channel close racing the flush would
            // retire the write direction instead of reporting the close.
            let (_outbound, outputs) = mpsc::unbounded_channel();
            for text in ["one", "two", "three"] {
                _outbound
                    .send(cello::js::WsOut::Text(text.into()))
                    .expect("stage data");
            }
            _outbound
                .send(cello::js::WsOut::Close(1000, "done".into()))
                .expect("stage close");
            // Awaited inline: the read parks on a silent peer while the write
            // direction flushes, writes the close, and reports it on its own.
            let (close, writer, _, _) = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                pump_cell_socket(
                    socket,
                    outputs,
                    true,
                    |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                    cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
                ),
            )
            .await
            .expect("the queued close must end the pump");
            assert_eq!(
                close,
                PumpClose {
                    state: (1000, "done".to_string(), true),
                    initiator: CloseInitiator::Application,
                },
                "the pump reports the queued application close"
            );
            // The write half is dropped so a missing terminal ends the reads
            // on EOF instead of parking on an open socket.
            drop(writer);
            for want in ["one", "two", "three"] {
                let (opcode, payload) = read_server_frame(&mut client_stream).await;
                assert_eq!(opcode, 0x1, "data crosses before the close");
                assert_eq!(payload, want.as_bytes(), "frames keep channel order");
            }
            let (opcode, payload) = read_server_frame(&mut client_stream).await;
            assert_eq!(opcode, 0x8, "the terminal is a close frame");
            assert_eq!(
                u16::from_be_bytes([payload[0], payload[1]]),
                1000,
                "the queued close keeps its code"
            );
            assert_eq!(&payload[2..], b"done", "the queued close keeps its reason");
        });
    }

    /// The cell's close reaches the peer on the local path: the pump writes
    /// the queued close frame before the socket shuts, even though the local
    /// close state stays an unclean 1006 for the dispatch. Without the
    /// pump-side write the peer reads EOF instead of the close — the
    /// `server_close_reaches_the_client` conformance regression — so the
    /// terminal read fails before the fix. Staged before the pump starts, so
    /// no timing is involved.
    #[test]
    fn cell_close_reaches_the_peer_before_shutdown() {
        test_runtime().block_on(async {
            let (mut client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            // Held, not dropped: a channel close racing the close would
            // retire the write direction instead of reporting it.
            let (_outbound, outputs) = mpsc::unbounded_channel();
            _outbound
                .send(cello::js::WsOut::Close(1008, "refused".into()))
                .expect("stage close");
            // Awaited inline: the read parks on a silent peer while the write
            // direction writes the close and reports it on its own.
            let (close, writer, _, _) = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                pump_cell_socket(
                    socket,
                    outputs,
                    false,
                    |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                    cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
                ),
            )
            .await
            .expect("the queued close must end the pump");
            assert_eq!(
                close,
                PumpClose {
                    state: (1006, String::new(), false),
                    initiator: CloseInitiator::Application,
                },
                "the local path still reports the close as unclean"
            );
            drop(writer);
            let (opcode, payload) = read_server_frame(&mut client_stream).await;
            assert_eq!(opcode, 0x8, "the terminal is a close frame");
            assert_eq!(
                u16::from_be_bytes([payload[0], payload[1]]),
                1008,
                "the cell close keeps its code"
            );
            assert_eq!(&payload[2..], b"refused", "the cell close keeps its reason");
        });
    }

    /// Shutdown still ends the pump while a socket send is parked: the
    /// 1 MiB write parks at once on a client that never reads, the peer's
    /// close ends the read on the independent direction, and the parked write
    /// retires instead of holding the pump open. A parked send that ignores
    /// the shutdown parks forever, so the guard fails before the change.
    #[test]
    fn shutdown_during_parked_send_ends_pump() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            // Held, not dropped: a channel close racing the parked send
            // would retire the write direction before the peer close lands.
            let (_outbound, outputs) = mpsc::unbounded_channel();
            _outbound
                .send(cello::js::WsOut::Binary(vec![0u8; 1024 * 1024]))
                .expect("stage frame");
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                true,
                |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            let mut client = fastwebsockets::WebSocket::after_handshake(
                client_stream,
                fastwebsockets::Role::Client,
            );
            // The 1 MiB write parks at once — the client never reads — while
            // the close crosses on the independent read direction: the two
            // duplex directions buffer separately.
            client
                .write_frame(fastwebsockets::Frame::close(1000, b"bye"))
                .await
                .expect("peer closes");
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("shutdown must end the parked send")
                .expect("pump joins");
            assert_eq!(
                close,
                PumpClose {
                    state: (1000, "bye".to_string(), true),
                    initiator: CloseInitiator::Peer,
                },
                "the pump ends on the peer close, not on the parked write"
            );
        });
    }

    /// An admission refusal ends the pump with the same loud 1011 the
    /// forwarder reports: the pump reports the application-sent 1011 and the
    /// socket observes the 1011 close frame. Staged alone — with data ahead
    /// of it the forwarder's immediate overload would race the in-flight
    /// head — so the loud close is the whole assertion.
    #[test]
    fn pump_overload_marker_writes_loud_1011_close() {
        test_runtime().block_on(async {
            let (mut client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            // Held, not dropped: in production the registry owns the sender
            // until the pump returns.
            let (_outbound, outputs) = mpsc::unbounded_channel();
            _outbound
                .send(cello::js::WsOut::Overload)
                .expect("stage marker");
            // Awaited inline: the read parks on a silent peer while the write
            // direction reports the overload on its own.
            let (close, _, _, _) = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                pump_cell_socket(
                    socket,
                    outputs,
                    true,
                    |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                    cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
                ),
            )
            .await
            .expect("the marker must end the pump");
            assert_eq!(
                close,
                PumpClose {
                    state: (
                        cello::js::WS_PIPE_OVERLOAD_CODE,
                        cello::js::WS_PIPE_OVERLOAD_REASON.to_string(),
                        false,
                    ),
                    initiator: CloseInitiator::Application,
                },
                "the marker ends the pump loudly"
            );
            let (opcode, payload) = read_server_frame(&mut client_stream).await;
            assert_eq!(opcode, 0x8, "the terminal is a close frame");
            assert_eq!(
                u16::from_be_bytes([payload[0], payload[1]]),
                cello::js::WS_PIPE_OVERLOAD_CODE,
                "loud 1011 close"
            );
            assert_eq!(
                &payload[2..],
                cello::js::WS_PIPE_OVERLOAD_REASON.as_bytes(),
                "overload reason"
            );
        });
    }

    /// An inbound dispatch the test drives one frame at a time: each dispatch
    /// waits for a permit, then records the frame and signals entry. Lets a
    /// test park a dispatch (withhold the permit), stage whatever follows on
    /// the socket, and only then release it — proving what the pump did while
    /// the dispatch was parked. Boxed: the closure type is unnameable, so a
    /// nested `impl Future` return is not expressible.
    fn gated_inbound(
        entered: mpsc::UnboundedSender<Vec<u8>>,
        permits: std::sync::Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<()>>>,
        order: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    ) -> impl FnMut(cello::js::WsIn) -> std::pin::Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>
    {
        move |data: cello::js::WsIn| {
            let entered = entered.clone();
            let permits = permits.clone();
            let order = order.clone();
            Box::pin(async move {
                permits
                    .lock()
                    .await
                    .recv()
                    .await
                    .expect("the test drives dispatches");
                let bytes = match &data {
                    cello::js::WsIn::Text(text) => text.clone().into_bytes(),
                    cello::js::WsIn::Binary(bytes) => bytes.clone(),
                };
                order.lock().unwrap().push(bytes.clone());
                entered.send(bytes).expect("the test observes dispatches");
                Ok(())
            })
        }
    }

    /// A peer close pending behind a stuck dispatch ends the pump with the
    /// pending close instead of parking teardown: the blocked pull-queue
    /// flush runs under the progress bound, one interval unrenewed while no
    /// dispatch completes, while ordinary dispatches stay unbounded (see
    /// slow_handler_keeps_socket). Without the bound the pump parks behind
    /// the stuck dispatch forever, so the join below fails before the fix.
    #[test]
    fn stuck_dispatch_with_pending_close_ends_pump() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            let (_outbound, outputs) = mpsc::unbounded_channel();
            // The inbound dispatch never completes: the isolate stopped
            // pulling.
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                true,
                |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            let mut client = fastwebsockets::WebSocket::after_handshake(
                client_stream,
                fastwebsockets::Role::Client,
            );
            // One data frame parks the stuck dispatch, then the peer close
            // waits behind it — with no bound the pump would park here
            // forever, and the peer would get no close response.
            client
                .write_frame(fastwebsockets::Frame::text(b"stuck".to_vec().into()))
                .await
                .expect("client data");
            client
                .write_frame(fastwebsockets::Frame::close(1000, b"bye"))
                .await
                .expect("peer closes");
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("the progress bound must end the pump")
                .expect("pump joins");
            assert_eq!(
                close,
                PumpClose {
                    state: (1000, "bye".to_string(), true),
                    initiator: CloseInitiator::Peer,
                },
                "the pump ends on the pending peer close"
            );
        });
    }

    /// A dispatch that resumes before the bound delivers its queued data
    /// before the pending peer close: the permit-gated handler parks the
    /// first frame while the second and the close stage behind it, then the
    /// test releases it and everything drains in order. Pins the other half
    /// of the peer-close flush: the bound above ends a stuck flush, this
    /// one proves a live flush still delivers before closing.
    #[test]
    fn resumed_dispatch_flushes_before_peer_close() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            let (_outbound, outputs) = mpsc::unbounded_channel();
            let (entered_tx, mut entered_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let (permit_tx, permit_rx) = mpsc::unbounded_channel::<()>();
            let permits = std::sync::Arc::new(tokio::sync::Mutex::new(permit_rx));
            let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                true,
                gated_inbound(entered_tx, permits, order.clone()),
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            let (_reader, mut writer) = tokio::io::split(client_stream);
            write_masked_frame(&mut writer, 0x2, b"first")
                .await
                .expect("client data");
            // The first dispatch parks on its withheld permit only after it
            // started: entering proves the pump read and staged it.
            permit_tx.send(()).expect("release the first dispatch");
            let first = tokio::time::timeout(std::time::Duration::from_secs(30), entered_rx.recv())
                .await
                .expect("the first dispatch must start")
                .expect("entered signals");
            assert_eq!(first, b"first", "the first frame dispatches first");
            write_masked_frame(&mut writer, 0x2, b"second")
                .await
                .expect("client data");
            write_masked_frame(&mut writer, 0x8, &[0x03, 0xe8, b'b', b'y', b'e'])
                .await
                .expect("peer closes");
            drop(writer);
            // The second frame and the close stage behind the parked first
            // dispatch; releasing it drains everything in order before the
            // pending close ends the pump.
            permit_tx.send(()).expect("release the second dispatch");
            let second =
                tokio::time::timeout(std::time::Duration::from_secs(30), entered_rx.recv())
                    .await
                    .expect("the second dispatch must start")
                    .expect("entered signals");
            assert_eq!(second, b"second", "queued data flushes before the close");
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("the pending close must end the pump")
                .expect("pump joins");
            assert_eq!(
                close,
                PumpClose {
                    state: (1000, "bye".to_string(), true),
                    initiator: CloseInitiator::Peer,
                },
                "the pump ends on the pending peer close, clean"
            );
            assert_eq!(
                *order.lock().unwrap(),
                vec![b"first".to_vec(), b"second".to_vec()],
                "dispatches keep socket order"
            );
        });
    }

    /// An orderly EOF drains staged messages in order before reporting the
    /// transport close: the peer sends A and B then ends its write side
    /// while B's dispatch is parked, so the read-ahead stages B before
    /// observing EOF. The old inline loop dispatched B before reading EOF;
    /// dropping B with the transport close fails the order assertion before
    /// the fix. The drain behind a parked dispatch is bounded by one
    /// unrenewed progress interval — a truncation mid-frame is
    /// indistinguishable from this clean end — so the pump waits for the
    /// in-flight dispatch but not forever (see
    /// truncated_frame_with_stuck_dispatch_ends_transport for the bound
    /// firing).
    #[test]
    fn orderly_eof_drains_staged_in_order() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            let (_outbound, outputs) = mpsc::unbounded_channel();
            let (entered_tx, mut entered_rx) = mpsc::unbounded_channel::<Vec<u8>>();
            let (permit_tx, permit_rx) = mpsc::unbounded_channel::<()>();
            let permits = std::sync::Arc::new(tokio::sync::Mutex::new(permit_rx));
            let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                true,
                gated_inbound(entered_tx, permits, order.clone()),
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            let mut client_stream = client_stream;
            write_masked_frame(&mut client_stream, 0x2, b"aaa")
                .await
                .expect("client data");
            // The first dispatch parks on its withheld permit only after it
            // started: entering proves the pump read and staged it.
            permit_tx.send(()).expect("release the first dispatch");
            let first = tokio::time::timeout(std::time::Duration::from_secs(30), entered_rx.recv())
                .await
                .expect("the first dispatch must start")
                .expect("entered signals");
            assert_eq!(first, b"aaa", "the first frame dispatches first");
            // The second frame is already in the duplex buffer behind the
            // first, so the read-ahead must stage it before it can observe
            // the EOF that follows it in stream order. Dropping the whole
            // stream ends the write side: dropping only a split write half
            // would leave the peer open and the pump would never see EOF.
            write_masked_frame(&mut client_stream, 0x2, b"bbb")
                .await
                .expect("client data");
            drop(client_stream);
            // The pump must wait for the drain: B's dispatch is still
            // parked, so a pump that drops the in-flight dispatch with the
            // transport close returns here instead of timing out. Well under
            // the five-second progress bound the EOF drain runs under — the
            // bound firing is truncated_frame's case, not this one's.
            let waited = tokio::time::timeout(std::time::Duration::from_secs(1), &mut pump).await;
            assert!(
                waited.is_err(),
                "the pump must wait for the staged drain, not report the transport close early"
            );
            permit_tx.send(()).expect("release the second dispatch");
            let second =
                tokio::time::timeout(std::time::Duration::from_secs(30), entered_rx.recv())
                    .await
                    .expect("the second dispatch must start")
                    .expect("entered signals");
            assert_eq!(second, b"bbb", "the staged frame dispatches after EOF");
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("the drained EOF must end the pump")
                .expect("pump joins");
            assert_eq!(close.initiator, CloseInitiator::Transport);
            assert_eq!(close.state, (1006, String::new(), false));
            assert_eq!(
                *order.lock().unwrap(),
                vec![b"aaa".to_vec(), b"bbb".to_vec()],
                "staged messages drain in order before the transport close"
            );
        });
    }

    /// An inbound overload settles the outbound write before attempting the
    /// loud close: the 1 MiB outbound write parks on a client that never
    /// reads, holding the writer mutex, while the inbound flood trips the
    /// cap. The pump retires the write direction first — the settle keeps
    /// the parked send alive — and only then attempts the 1011 at a frame
    /// boundary. Attempting the close on the contended mutex instead would
    /// park behind the stuck write, and dropping the parked send outright
    /// would leave a partial frame ahead of the terminal, so the wire would
    /// carry a truncated frame instead of the intact data plus 1011.
    #[test]
    fn overload_settles_outbound_before_loud_close() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            // Held, not dropped: a channel close racing the parked send
            // would retire the write direction before the flood lands.
            let (_outbound, outputs) = mpsc::unbounded_channel();
            let data = vec![7u8; 1024 * 1024];
            _outbound
                .send(cello::js::WsOut::Binary(data.clone()))
                .expect("stage frame");
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                false,
                |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            let (mut reader, mut writer) = tokio::io::split(client_stream);
            // Twenty megabytes into a dispatch that never runs: the pump
            // stages past the cap and reports the overload. The sender stops
            // on error: the pump stops reading once it reports the flood.
            let sender = tokio::spawn(async move {
                for _ in 0..20 {
                    let chunk = vec![0u8; 1024 * 1024];
                    if write_masked_frame(&mut writer, 0x2, &chunk).await.is_err() {
                        break;
                    }
                }
            });
            // The outbound 1 MiB parks at once — the client never reads — and
            // the flood trips the cap behind it. Only once the test starts
            // reading does the settle complete the parked send whole: the
            // two seconds comfortably clear the read loop but stay well under
            // the five-second progress bound the settle waits out.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            // The parked data drains first, intact, then the loud close: a
            // dropped send would leave a partial frame here, and a close
            // attempted on the contended mutex would have timed out instead.
            // Under a deadline of its own: a regressed pump that never drains
            // must fail rather than stall the test run.
            let (opcode, payload) = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                read_server_frame(&mut reader),
            )
            .await
            .expect("the parked data must drain");
            assert_eq!(opcode, 0x2, "the parked data drains, not a terminal");
            assert_eq!(payload, data, "the drained frame is intact");
            let (opcode, payload) = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                read_server_frame(&mut reader),
            )
            .await
            .expect("the loud close must arrive");
            assert_eq!(opcode, 0x8, "the terminal is a close frame");
            assert_eq!(
                u16::from_be_bytes([payload[0], payload[1]]),
                cello::js::WS_PIPE_OVERLOAD_CODE,
                "loud 1011 close"
            );
            assert_eq!(
                &payload[2..],
                cello::js::WS_PIPE_OVERLOAD_REASON.as_bytes(),
                "overload reason"
            );
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("the flood must end the pump")
                .expect("pump joins");
            assert_eq!(
                close,
                PumpClose {
                    state: (
                        cello::js::WS_PIPE_OVERLOAD_CODE,
                        cello::js::WS_PIPE_OVERLOAD_REASON.to_string(),
                        false,
                    ),
                    initiator: CloseInitiator::Application,
                },
                "the pump reports the application-sent 1011"
            );
            sender.abort();
        });
    }

    /// A test stream whose writes the test gates: while the gate is closed
    /// every write parks forever. Every write attempt is recorded by the
    /// length of the buffer it presents, so a test can abandon a data write
    /// mid-frame — the pump's own progress bound drops it — and then tell
    /// whether the pump attempts any further frame: the data frame's head
    /// polls and the terminal close present different lengths. Counting
    /// attempts cannot tell them apart: each re-poll of the parked write
    /// future presents the head again. Parking forever without a wake is
    /// legal: the parked future is only ever dropped.
    #[derive(Clone)]
    struct Gate {
        open: std::sync::Arc<std::sync::atomic::AtomicBool>,
        attempts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        bytes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        lengths: std::sync::Arc<std::sync::Mutex<Vec<usize>>>,
    }
    impl Gate {
        fn closed() -> Self {
            Self {
                open: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                attempts: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                bytes: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                lengths: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }
        fn open(&self) {
            self.open.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        fn attempts(&self) -> usize {
            self.attempts.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn bytes(&self) -> usize {
            self.bytes.load(std::sync::atomic::Ordering::SeqCst)
        }
        fn lengths(&self) -> Vec<usize> {
            self.lengths.lock().expect("gate lengths").clone()
        }
    }
    struct GateStream {
        gate: Gate,
    }
    impl tokio::io::AsyncRead for GateStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
            _buffer: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            // The read parks: these tests end the pump through the write
            // direction, so an instant EOF must not win the race and report
            // a transport end the test never staged.
            std::task::Poll::Pending
        }
    }
    impl tokio::io::AsyncWrite for GateStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
            buffer: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            use std::sync::atomic::Ordering::SeqCst;
            self.gate.attempts.fetch_add(1, SeqCst);
            self.gate
                .lengths
                .lock()
                .expect("gate lengths")
                .push(buffer.len());
            if self.gate.open.load(SeqCst) {
                self.gate.bytes.fetch_add(buffer.len(), SeqCst);
                std::task::Poll::Ready(Ok(buffer.len()))
            } else {
                std::task::Poll::Pending
            }
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    /// A write cancelled while it holds the writer must poison before it
    /// unlocks: task A parks mid-write holding the mutex while spinners race
    /// for the lock, then A is dropped. Every spinner must observe the
    /// poison instead of writing after the partial frame. Spinners do not
    /// wait to be woken — they loop on `try_lock`, so one is already running
    /// on another worker inside the unlock-to-store gap and would slip
    /// through a poison-after-unlock order. Reversing the drop order lets a
    /// spinner complete its frame, and this fails before the fix.
    #[test]
    fn cancelled_write_poison_is_visible_before_unlock() {
        test_runtime().block_on(async {
            for _ in 0..50 {
                let gate = Gate::closed();
                let socket = fastwebsockets::WebSocket::after_handshake(
                    GateStream { gate: gate.clone() },
                    fastwebsockets::Role::Server,
                );
                let (_reader, writer) = socket.split(tokio::io::split);
                let pump_writer = PumpWriter {
                    writer: std::sync::Arc::new(tokio::sync::Mutex::new(writer)),
                    poisoned: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
                };
                // A acquires the lock and parks mid-write; the gate stays
                // closed so its frame never completes.
                let a_writer = pump_writer.clone();
                let a = tokio::spawn(async move {
                    pump_write_frame(
                        &a_writer,
                        fastwebsockets::Frame::binary(vec![9u8; 1024].into()),
                        None,
                    )
                    .await
                });
                while gate.attempts() == 0 {
                    tokio::task::yield_now().await;
                }
                // Spinners race for the lock A holds, mirroring the re-check
                // every pump write runs under the lock: first to acquire
                // after A drops wins the unlock-to-store gap.
                let mut spinners = Vec::new();
                for _ in 0..4 {
                    let spinner = pump_writer.clone();
                    spinners.push(tokio::spawn(async move {
                        loop {
                            // First to acquire after A drops wins the
                            // unlock-to-store gap. The guard drops before
                            // returning: recording the escape is the
                            // assertion, no frame needs to cross.
                            if let Ok(held) = spinner.writer.try_lock() {
                                let poisoned = spinner.poisoned();
                                drop(held);
                                return !poisoned;
                            }
                            tokio::task::yield_now().await;
                        }
                    }));
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                // Dropping A mid-write must poison before the unlock hands
                // the lock to the race.
                a.abort();
                for spinner in spinners {
                    let wrote = tokio::time::timeout(std::time::Duration::from_secs(30), spinner)
                        .await
                        .expect("spinners must finish once A drops")
                        .expect("spinner joins");
                    assert!(!wrote, "a write behind a cancelled one must see the poison");
                }
            }
        });
    }
    /// A data write abandoned mid-frame poisons the writer: the close-flush
    /// bound expires with the frame parked on the closed gate, so the send
    /// is dropped and no terminal may follow it — the peer would read a
    /// terminal as a continuation of the unfinished frame. The gated stream
    /// records every presented buffer length, so a regression that writes
    /// the terminal anyway presents its eight bytes and fails. The abandoned
    /// close is reported unclean: the peer sees a transport drop, not a
    /// handshake.
    #[test]
    fn partial_write_drops_transport_without_terminal() {
        test_runtime().block_on(async {
            let gate = Gate::closed();
            let mut socket = fastwebsockets::WebSocket::after_handshake(
                GateStream { gate: gate.clone() },
                fastwebsockets::Role::Server,
            );
            socket.set_auto_close(false);
            // Held, not dropped: a channel close racing the flush would
            // retire the write direction instead of reporting the close.
            let (_outbound, outputs) = mpsc::unbounded_channel();
            _outbound
                .send(cello::js::WsOut::Binary(vec![9u8; 64 * 1024]))
                .expect("stage frame");
            _outbound
                .send(cello::js::WsOut::Close(1000, "done".into()))
                .expect("stage close");
            let (close, writer, _, _) = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                pump_cell_socket(
                    socket,
                    outputs,
                    true,
                    |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                    cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
                ),
            )
            .await
            .expect("the flush bound must end the pump");
            assert_eq!(
                close,
                PumpClose {
                    state: (1006, String::new(), false),
                    initiator: CloseInitiator::Application,
                },
                "an abandoned close reports the abnormal closure the peer sees"
            );
            assert!(writer.poisoned(), "the abandoned write poisons the writer");
            // The 64 KiB data frame goes out vectored, so every poll of the
            // parked write presents its ten-byte head; a terminal close
            // would present its own eight bytes (two head, two code, four
            // reason) instead. Each re-poll of the parked write presents the
            // head again, so only the lengths — never the count — tell a
            // re-poll from a terminal attempt.
            let lengths = gate.lengths();
            assert!(
                !lengths.is_empty(),
                "the abandoned data write is attempted"
            );
            assert!(
                lengths.iter().all(|&len| len == 10),
                "only the abandoned data head is presented, never the eight-byte terminal: {lengths:?}"
            );
        });
    }
    /// The frame-boundary control for the gated stream: with the gate open
    /// throughout, the data and the close both cross and the pump reports
    /// the clean application close. Proves the gate observes terminal
    /// writes — the attempt count above is not vacuous.
    #[test]
    fn unpoisoned_close_at_frame_boundary_writes_terminal() {
        test_runtime().block_on(async {
            let gate = Gate::closed();
            gate.open();
            let mut socket = fastwebsockets::WebSocket::after_handshake(
                GateStream { gate: gate.clone() },
                fastwebsockets::Role::Server,
            );
            socket.set_auto_close(false);
            // Held, not dropped: a channel close racing the flush would
            // retire the write direction instead of reporting the close.
            let (_outbound, outputs) = mpsc::unbounded_channel();
            let data = vec![9u8; 1024];
            _outbound
                .send(cello::js::WsOut::Binary(data.clone()))
                .expect("stage frame");
            _outbound
                .send(cello::js::WsOut::Close(1000, "done".into()))
                .expect("stage close");
            let (close, writer, _, _) = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                pump_cell_socket(
                    socket,
                    outputs,
                    true,
                    |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                    cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
                ),
            )
            .await
            .expect("the queued close must end the pump");
            assert_eq!(
                close,
                PumpClose {
                    state: (1000, "done".to_string(), true),
                    initiator: CloseInitiator::Application,
                },
                "a close that reaches the wire reports clean"
            );
            assert!(!writer.poisoned(), "nothing was abandoned");
            assert_eq!(
                gate.attempts(),
                2,
                "the data and the terminal are both attempted"
            );
            // The 1 KiB binary frame (two-byte head, sixteen-bit length)
            // plus the terminal close frame all cross intact.
            assert_eq!(
                gate.bytes(),
                4 + data.len() + 2 + 2 + "done".len(),
                "both frames cross whole"
            );
        });
    }
    /// A close write that times out is an abnormal closure: the gate stays
    /// closed so the terminal never completes, and the pump must report
    /// 1006 / unclean instead of the clean close the application queued.
    /// Reporting the queued close anyway (the pre-fix behavior) fails the
    /// close assertion below.
    #[test]
    fn timed_out_close_reports_unclean() {
        test_runtime().block_on(async {
            let gate = Gate::closed();
            let mut socket = fastwebsockets::WebSocket::after_handshake(
                GateStream { gate: gate.clone() },
                fastwebsockets::Role::Server,
            );
            socket.set_auto_close(false);
            // Held, not dropped: a channel close racing the write would
            // retire the direction instead of reporting the close.
            let (_outbound, outputs) = mpsc::unbounded_channel();
            _outbound
                .send(cello::js::WsOut::Close(1000, "done".into()))
                .expect("stage close");
            let (close, writer, _, _) = tokio::time::timeout(
                std::time::Duration::from_secs(30),
                pump_cell_socket(
                    socket,
                    outputs,
                    true,
                    |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                    cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
                ),
            )
            .await
            .expect("the close bound must end the pump");
            assert_eq!(
                close,
                PumpClose {
                    state: (1006, String::new(), false),
                    initiator: CloseInitiator::Application,
                },
                "a close that never reaches the wire reports unclean"
            );
            assert!(writer.poisoned(), "the timed-out write poisons the writer");
        });
    }
    /// A truncated frame must take the Transport path: one data frame parks
    /// its dispatch forever — a full pull queue — then the peer ends
    /// mid-frame and drops. The parser reports `UnexpectedEOF` for a clean
    /// end and a truncation alike, so the EOF drain runs under the same
    /// progress bound as the peer-close flush: it ends Transport instead of
    /// hanging the pump behind the stuck dispatch. Without the bound the
    /// join below times out and this fails before the fix.
    #[test]
    fn truncated_frame_with_stuck_dispatch_ends_transport() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            let (_outbound, outputs) = mpsc::unbounded_channel();
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                true,
                |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            let mut client_stream = client_stream;
            write_masked_frame(&mut client_stream, 0x2, b"stuck")
                .await
                .expect("client data");
            // A binary head claiming ten payload bytes with only three
            // behind the mask, then EOF: a frame truncated mid-payload.
            {
                use tokio::io::AsyncWriteExt;
                let mask = [0x12u8, 0x34, 0x56, 0x78];
                client_stream
                    .write_all(&[0x82, 0x8A])
                    .await
                    .expect("truncated head");
                client_stream
                    .write_all(&mask)
                    .await
                    .expect("truncated mask");
                let payload = [0u8, 1, 2]
                    .into_iter()
                    .enumerate()
                    .map(|(index, byte)| byte ^ mask[index % mask.len()])
                    .collect::<Vec<_>>();
                client_stream
                    .write_all(&payload)
                    .await
                    .expect("truncated payload");
            }
            drop(client_stream);
            let (close, _, _, _) = tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                .await
                .expect("the bound must end the pump behind the stuck dispatch")
                .expect("pump joins");
            assert_eq!(close.initiator, CloseInitiator::Transport);
            assert_eq!(close.state, (1006, String::new(), false));
        });
    }
    /// An outbound overload is preserved when the read side ends first: a
    /// 1 MiB write parks on a client that never reads, the flood past the
    /// staged cap trips OverLimit while the write settles, and the peer's
    /// close ends the read first. The joined write outcome still reports
    /// the loud 1011 — the peer's clean close must not mask the flood.
    /// Dropping the joined outcome (the pre-fix behavior) reports the
    /// peer's clean close and fails the assertion below.
    #[test]
    fn outbound_overload_survives_read_end_first() {
        test_runtime().block_on(async {
            let (client_stream, server) = tokio::io::duplex(64 * 1024);
            let mut socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            socket.set_auto_close(false);
            // Held, not dropped: a channel close racing the parked send
            // would retire the write direction before the flood lands.
            let (_outbound, outputs) = mpsc::unbounded_channel();
            // One parked megabyte, then past the 16 MiB staged cap: the
            // forwarder detects OverLimit while the first send is still
            // parked and settles it under the progress bound.
            for _ in 0..17 {
                _outbound
                    .send(cello::js::WsOut::Binary(vec![7u8; 1024 * 1024]))
                    .expect("stage frame");
            }
            let pump = tokio::spawn(pump_cell_socket(
                socket,
                outputs,
                true,
                |_: cello::js::WsIn| std::future::pending::<anyhow::Result<()>>(),
                cello::js::WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            // Let the forwarder trip OverLimit and enter its settle: the
            // seventeen buffered channel sends stage in microseconds, while
            // the parked write settles out the five-second bound. The
            // peer's close below then deterministically ends the read
            // first, while the write is still settling.
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            let mut client = fastwebsockets::WebSocket::after_handshake(
                client_stream,
                fastwebsockets::Role::Client,
            );
            client
                .write_frame(fastwebsockets::Frame::close(1000, b"bye"))
                .await
                .expect("peer closes");
            let (close, writer, _, _) =
                tokio::time::timeout(std::time::Duration::from_secs(30), pump)
                    .await
                    .expect("the overload must end the pump")
                    .expect("pump joins");
            assert_eq!(
                close,
                PumpClose {
                    state: (
                        cello::js::WS_PIPE_OVERLOAD_CODE,
                        cello::js::WS_PIPE_OVERLOAD_REASON.to_string(),
                        false,
                    ),
                    initiator: CloseInitiator::Application,
                },
                "the joined write outcome still reports the loud 1011"
            );
            assert!(
                writer.poisoned(),
                "the settled-away parked write poisons the writer"
            );
        });
    }

    /// An `AsyncRead + AsyncWrite` that ends reads at EOF and fails every
    /// write: the closing drain's failed-write path without timing or
    /// duplex-buffer subtleties (a dropped duplex peer still buffers server
    /// writes, so it cannot fail them).
    struct FailingStream;
    impl tokio::io::AsyncRead for FailingStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
            _buffer: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    impl tokio::io::AsyncWrite for FailingStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
            _buffer: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "failing stream",
            )))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "failing stream",
            )))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }

    /// The closing drain refunds admission charges as it removes data
    /// frames, even when their writes fail: the pump returned the receiver
    /// but not the budget, and a pending output gate can release more frames
    /// after the drain — with the drained bytes still charged, the registry
    /// would refuse the new frames for bytes no longer queued. Removing the
    /// release (the pre-fix behavior) leaves the charge behind, so the
    /// budget assertion fails before the fix. Close and Overload markers
    /// carry no charge.
    #[test]
    fn closing_drain_refunds_removed_frames_on_write_failure() {
        test_runtime().block_on(async {
            // Every write fails fast on the failing stream, so the drain
            // removes one data frame, refunds it, and stops.
            let socket = fastwebsockets::WebSocket::after_handshake(
                FailingStream,
                fastwebsockets::Role::Server,
            );
            let (_reader, writer) = socket.split(tokio::io::split);
            let pump_writer = PumpWriter {
                writer: std::sync::Arc::new(tokio::sync::Mutex::new(writer)),
                poisoned: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            };
            // A one-frame budget: only the staged frame fits, so a later
            // charge succeeds exactly when the drain refunded it.
            let frame = cello::js::WsOut::Text("one".into());
            let frame_bytes = cello::js::ws_out_bytes(&frame);
            let budget = cello::js::WsPipeBudget::new(frame_bytes);
            let (outbound, mut outputs) = mpsc::unbounded_channel();
            // Charged the way the registry admit path charges it.
            assert!(
                budget.charge(frame_bytes),
                "the test budget admits the staged frame"
            );
            outbound.send(frame).expect("stage frame");
            outbound
                .send(cello::js::WsOut::Close(1000, "done".into()))
                .expect("stage close");
            drop(outbound);
            let handler_sent_close = drain_close_outputs(&pump_writer, &mut outputs, &budget).await;
            assert!(
                !handler_sent_close,
                "the close marker is never reached on a failed drain"
            );
            assert!(
                budget.charge(frame_bytes),
                "the removed frame is refunded even though its write failed"
            );
            // The rest stays queued: the drain stopped at the failed write,
            // leaving the close marker behind the removed data frame.
            assert!(
                matches!(outputs.try_recv(), Ok(cello::js::WsOut::Close(_, _))),
                "the undrained close marker stays queued"
            );
            assert!(
                matches!(
                    outputs.try_recv(),
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
                ),
                "nothing else remains queued"
            );
        });
    }

    /// The closing drain refunds everything it removes when the peer reads:
    /// both data frames and the handler's close cross, the handler-sent
    /// close is reported, and no charge is left behind.
    #[test]
    fn closing_drain_refunds_everything_it_removes() {
        test_runtime().block_on(async {
            let (mut client_stream, server) = tokio::io::duplex(64 * 1024);
            let socket =
                fastwebsockets::WebSocket::after_handshake(server, fastwebsockets::Role::Server);
            let (_reader, writer) = socket.split(tokio::io::split);
            let pump_writer = PumpWriter {
                writer: std::sync::Arc::new(tokio::sync::Mutex::new(writer)),
                poisoned: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            };
            let first = cello::js::WsOut::Text("one".into());
            let second = cello::js::WsOut::Text("two".into());
            // A two-frame budget: a later charge succeeds exactly when the
            // drain refunded everything it removed.
            let budget = cello::js::WsPipeBudget::new(
                cello::js::ws_out_bytes(&first) + cello::js::ws_out_bytes(&second),
            );
            let (outbound, mut outputs) = mpsc::unbounded_channel();
            // Charged the way the registry admit path charges them.
            for frame in [first, second] {
                assert!(
                    budget.charge(cello::js::ws_out_bytes(&frame)),
                    "the test budget admits the staged frame"
                );
                outbound.send(frame).expect("stage frame");
            }
            outbound
                .send(cello::js::WsOut::Close(1000, "done".into()))
                .expect("stage close");
            drop(outbound);
            let handler_sent_close = drain_close_outputs(&pump_writer, &mut outputs, &budget).await;
            assert!(handler_sent_close, "the handler's close marker is reported");
            assert!(
                budget.charge(cello::js::ws_out_bytes(&cello::js::WsOut::Text(
                    "three".into()
                ))),
                "every removed frame is refunded"
            );
            // The frames crossed in order ahead of the terminal.
            for want in ["one", "two"] {
                let (opcode, payload) = read_server_frame(&mut client_stream).await;
                assert_eq!(opcode, 0x1, "data crosses before the close");
                assert_eq!(payload, want.as_bytes(), "frames keep channel order");
            }
            let (opcode, _) = read_server_frame(&mut client_stream).await;
            assert_eq!(opcode, 0x8, "the terminal is a close frame");
        });
    }

    /// A poisoned writer skips the protocol echo: after a possibly partial
    /// write no further frame may follow, so the transport drops instead.
    /// The unpoisoned control proves the echo would otherwise cross.
    #[test]
    fn poisoned_writer_skips_echo() {
        test_runtime().block_on(async {
            use tokio::io::AsyncReadExt;
            for poisoned in [false, true] {
                let (mut client_stream, server) = tokio::io::duplex(64 * 1024);
                let socket = fastwebsockets::WebSocket::after_handshake(
                    server,
                    fastwebsockets::Role::Server,
                );
                let (_reader, writer) = socket.split(tokio::io::split);
                let pump_writer = PumpWriter {
                    writer: std::sync::Arc::new(tokio::sync::Mutex::new(writer)),
                    poisoned: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(poisoned)),
                };
                // A clean peer close the handler did not answer: the echo
                // would carry the peer's own code back.
                echo_websocket_close(&pump_writer, &(1000, "bye".to_string(), true), false).await;
                // Drop the whole server side: dropping only the write half
                // would leave the peer open and the client would never see
                // EOF below.
                drop(_reader);
                drop(pump_writer);
                if poisoned {
                    // No frame, just EOF: the transport drops instead.
                    let mut head = [0u8; 2];
                    let reader = &mut client_stream;
                    let mut filled = 0;
                    let mut eof = false;
                    while filled < head.len() && !eof {
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(30),
                            reader.read(&mut head[filled..]),
                        )
                        .await
                        .expect("the wire read has a deadline")
                        {
                            Ok(0) => eof = true,
                            Ok(n) => filled += n,
                            Err(_) => eof = true,
                        }
                    }
                    assert!(eof, "a poisoned writer sends no echo");
                } else {
                    let (opcode, payload) = read_server_frame(&mut client_stream).await;
                    assert_eq!(opcode, 0x8, "the echo is a close frame");
                    assert_eq!(
                        u16::from_be_bytes([payload[0], payload[1]]),
                        1000,
                        "the echo carries the peer's code"
                    );
                }
            }
        });
    }
}
