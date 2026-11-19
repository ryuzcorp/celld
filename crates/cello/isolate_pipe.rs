use std::future::Future;

// One direction of a WebSocket pipe: frames read from one socket's output
// channel cross into the other end's sink.
//
// Staged bytes one pipe direction may hold upstream of its sink.
//
// The direction's channel and its staged queue are both unbounded, so a
// flood into a peer that never reads would otherwise grow host memory
// without bound. Well above legitimate bursts (the `/flood-close` test
// stages ~3 MiB) and still a hard bound per stuck pipe. Every pipe
// direction shares it: the isolate-to-isolate pipe, the cell pipe, and the
// producer budgets behind the upgrade pump (see `ws_registry::WsPipeBudget`).
pub const ISOLATE_PIPE_MAX_STAGED_BYTES: usize = 16 * 1024 * 1024;

/// How long a closing flush waits for the peer to make progress before it
/// ends with the pending close instead. Each forwarded frame restarts the
/// wait, so a slow but live peer still drains fully; a peer that stopped
/// reading ends the pipe instead of blocking closure indefinitely. Well
/// above the test peer's quarter-second frames and well under the caller's
/// close timeout.
const ISOLATE_PIPE_CLOSE_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How one direction of a WebSocket pipe ended.
#[derive(Debug)]
pub enum PipeDirectionEnd {
    /// The side sent a close frame; the teardown delivers it.
    Close(u16, String),
    /// The staged frames passed the cap, or an admission refusal queued the
    /// overload marker; the teardown closes both ends loudly.
    OverLimit,
    /// The socket retired, the sink went away, or the other direction shut
    /// the pipe down: fail-closed, no terminal.
    Retired,
}

/// What one sink send attempt did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipeSendOutcome {
    /// The frame reached the sink; the direction keeps forwarding.
    Sent,
    /// The sink is gone; the direction ends retired, fail-closed.
    Gone,
}

/// Buffer one received frame, accounting its bytes and releasing the
/// producer budget it arrived under. Returns the direction's end when the
/// frame ends it: a close with nothing left to flush, a retirement, an
/// overload marker, or a staging bill past the cap. A close behind queued
/// frames waits instead: workerd queues the close behind the data, so the
/// direction reports it only after the flush below delivers everything ahead
/// of it. First close wins; anything later is dropped with the closing
/// pipe. Overload ends immediately either way, like the staged cap.
///
/// `busy` is whether a send is still parked: a close that arrives while one
/// is must wait behind it even when the queue itself just drained, or the
/// parked frame would be dropped with the closing pipe.
fn stage_pipe_frame(
    frame: Option<crate::js::WsOut>,
    queued: &mut std::collections::VecDeque<crate::js::WsOut>,
    staged_bytes: &mut usize,
    budget: &crate::js::WsPipeBudget,
    pending_close: &mut Option<(u16, String)>,
    busy: bool,
) -> Option<PipeDirectionEnd> {
    use crate::js::{ws_out_bytes, WsOut};
    if let Some(data) = frame.as_ref() {
        match data {
            WsOut::Text(_) | WsOut::Binary(_) => budget.release(ws_out_bytes(data)),
            WsOut::Close(..) | WsOut::Overload => {}
        }
    }
    match frame {
        Some(data @ (WsOut::Text(_) | WsOut::Binary(_))) => {
            *staged_bytes = staged_bytes.saturating_add(ws_out_bytes(&data));
            queued.push_back(data);
            if *staged_bytes > ISOLATE_PIPE_MAX_STAGED_BYTES {
                return Some(PipeDirectionEnd::OverLimit);
            }
            None
        }
        Some(WsOut::Close(code, reason)) => {
            if queued.is_empty() && pending_close.is_none() && !busy {
                return Some(PipeDirectionEnd::Close(code, reason));
            }
            pending_close.get_or_insert((code, reason));
            None
        }
        // An admission refusal queued behind the admitted data: like the
        // staged cap it ends the direction loudly, immediately — the marker
        // itself stays in channel order behind everything admitted ahead of
        // it, so either overload trigger reaches the same teardown. Queued
        // frames drop, exactly as past the staged cap.
        Some(WsOut::Overload) => Some(PipeDirectionEnd::OverLimit),
        // The socket's request retired and took its socket with it.
        None => Some(PipeDirectionEnd::Retired),
    }
}

/// Settle a parked sink send under the closing progress bound before the
/// direction reports an end that drops the pipe: the parked send owns the
/// head frame, and dropping it mid socket-write would tear that frame ahead
/// of the terminal the teardown reports (the loud 1011 for an overload,
/// nothing for a retirement). The end stands either way — only the head
/// frame's delivery is at stake — so an expired bound or a freshly observed
/// shutdown still returns promptly. No-op when nothing is parked.
///
/// Delivery only: the producer budget was released when the frame was
/// received, and the staged-bytes ledger drops with the direction, so the
/// outcome only decides whether the head frame crossed whole.
async fn settle_parked_send<SendFut>(
    sending: &mut Option<(std::pin::Pin<Box<SendFut>>, usize)>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) where
    SendFut: Future<Output = PipeSendOutcome>,
{
    let Some((send_head, _)) = sending.take() else {
        return;
    };
    enum Settle {
        Sent,
        Shutdown,
    }
    let _ = crate::asyncrt::timeout(ISOLATE_PIPE_CLOSE_FLUSH_TIMEOUT, async {
        crate::asyncrt::select! {
            _ = send_head => Settle::Sent,
            _ = shutdown.changed() => Settle::Shutdown,
        }
    })
    .await;
}

/// One direction of a WebSocket pipe: data frames staged from `from` cross
/// into the sink one at a time, in channel order.
///
/// `send` runs only for data frames (`WsOut::Text`/`WsOut::Binary`): closes,
/// overload markers, and retirements end the direction instead of reaching
/// it, so a sink that names every `WsOut` variant still never sees one it
/// cannot handle, and no task panics on an unexpected variant. The parked
/// send owns no frame the loop has to keep — `send` takes the frame by
/// value — so the loop keeps receiving, and keeps observing closes,
/// retirements, and the shutdown signal, while it waits.
///
/// A close behind queued frames waits for the flush instead of dropping it,
/// but the flush only moves while the peer can make progress: each
/// forwarded frame restarts the progress wait, so a slow but live peer
/// still drains fully, while a peer that stopped reading ends the pipe with
/// the pending close instead of blocking closure indefinitely. The flush
/// also watches the pipe shutdown, so the other direction ending still tears
/// down promptly instead of draining through one progress interval per
/// queued frame.
///
/// Staged frames are accounted against [`ISOLATE_PIPE_MAX_STAGED_BYTES`]:
/// past the cap the direction reports [`PipeDirectionEnd::OverLimit`] so
/// the teardown closes both ends loudly instead of growing host memory
/// without bound behind a peer that never reads. An end that drops the pipe
/// first settles a parked send under the same progress bound, so the head
/// frame still crosses whole ahead of the terminal the teardown reports.
pub async fn forward_pipe_direction<SendFn, SendFut>(
    mut from: tokio::sync::mpsc::UnboundedReceiver<crate::js::WsOut>,
    mut send: SendFn,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    budget: crate::js::WsPipeBudget,
) -> (
    PipeDirectionEnd,
    tokio::sync::mpsc::UnboundedReceiver<crate::js::WsOut>,
)
where
    SendFn: FnMut(crate::js::WsOut) -> SendFut,
    SendFut: Future<Output = PipeSendOutcome>,
{
    use crate::js::{ws_out_bytes, WsOut};
    let mut queued: std::collections::VecDeque<WsOut> = std::collections::VecDeque::new();
    // Bytes staged upstream of the sink. Added on stage, released when a
    // send completes for the head frame.
    let mut staged_bytes: usize = 0;
    // A parked sink send with the head frame's staged bytes: stored rather
    // than awaited so the loop keeps receiving while it is pending.
    // Re-polling a parked send is cancel-safe. An end that drops the pipe
    // settles it under the closing progress bound instead of tearing the
    // head frame; dropping it outright would leave a partial frame ahead
    // of the terminal the teardown reports.
    let mut sending: Option<(std::pin::Pin<Box<SendFut>>, usize)> = None;
    // A close waiting behind flushed frames. Set instead of returned while
    // anything remains to flush; delivered once the queue drains.
    let mut pending_close: Option<(u16, String)> = None;
    loop {
        if pending_close.is_some() {
            // A close is waiting behind flushed frames: forward while the
            // peer can make progress, and never park behind it without a
            // bound. A parked send owns the head frame — it was popped when
            // the send was created — so settle it under the same progress
            // bound first instead of dropping it: dropping it would lose the
            // head with the closing pipe.
            if let Some((send_head, bytes)) = sending.take() {
                enum Settle {
                    Sent(PipeSendOutcome),
                    Shutdown,
                }
                let outcome = crate::asyncrt::timeout(ISOLATE_PIPE_CLOSE_FLUSH_TIMEOUT, async {
                    crate::asyncrt::select! {
                        outcome = send_head => Settle::Sent(outcome),
                        _ = shutdown.changed() => Settle::Shutdown,
                    }
                })
                .await;
                match outcome {
                    Ok(Settle::Sent(PipeSendOutcome::Sent)) => {
                        staged_bytes = staged_bytes.saturating_sub(bytes);
                    }
                    Ok(Settle::Sent(PipeSendOutcome::Gone)) | Ok(Settle::Shutdown) => {
                        return (PipeDirectionEnd::Retired, from);
                    }
                    Err(_) => {
                        let (code, reason) =
                            pending_close.take().expect("close pending while flushing");
                        return (PipeDirectionEnd::Close(code, reason), from);
                    }
                }
            }
            // Eagerly stage whatever arrived, still under the cap. The other
            // direction shutting down ends the flush instead of draining it.
            loop {
                if shutdown.has_changed().unwrap_or(true) {
                    return (PipeDirectionEnd::Retired, from);
                }
                match from.try_recv() {
                    Ok(frame) => {
                        if let Some(end) = stage_pipe_frame(
                            Some(frame),
                            &mut queued,
                            &mut staged_bytes,
                            &budget,
                            &mut pending_close,
                            false,
                        ) {
                            return (end, from);
                        }
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        return (PipeDirectionEnd::Retired, from);
                    }
                }
            }
            // Reserve the head with a progress bound: each forwarded frame
            // restarts the wait, so a slow but live peer still drains fully,
            // while a peer that stopped reading ends the pipe with the
            // pending close instead of blocking closure indefinitely. The
            // other direction ending wins over the wait.
            let Some(head) = queued.pop_front() else {
                let (code, reason) = pending_close.take().expect("close pending while flushing");
                return (PipeDirectionEnd::Close(code, reason), from);
            };
            let bytes = ws_out_bytes(&head);
            enum FlushWait {
                Sent(PipeSendOutcome),
                Shutdown,
            }
            let outcome = crate::asyncrt::timeout(ISOLATE_PIPE_CLOSE_FLUSH_TIMEOUT, async {
                crate::asyncrt::select! {
                    outcome = send(head) => FlushWait::Sent(outcome),
                    _ = shutdown.changed() => FlushWait::Shutdown,
                }
            })
            .await;
            match outcome {
                Ok(FlushWait::Sent(PipeSendOutcome::Sent)) => {
                    staged_bytes = staged_bytes.saturating_sub(bytes);
                }
                Ok(FlushWait::Sent(PipeSendOutcome::Gone)) | Ok(FlushWait::Shutdown) => {
                    return (PipeDirectionEnd::Retired, from);
                }
                Err(_) => {
                    let (code, reason) =
                        pending_close.take().expect("close pending while flushing");
                    return (PipeDirectionEnd::Close(code, reason), from);
                }
            }
            continue;
        }
        if sending.is_none() {
            if let Some(head) = queued.pop_front() {
                let bytes = ws_out_bytes(&head);
                sending = Some((Box::pin(send(head)), bytes));
            } else {
                // Idle: wait for work or for the other direction's shutdown.
                crate::asyncrt::select! {
                    frame = from.recv() => {
                        if let Some(end) = stage_pipe_frame(
                            frame,
                            &mut queued,
                            &mut staged_bytes,
                            &budget,
                            &mut pending_close,
                            false,
                        ) {
                            return (end, from);
                        }
                    }
                    _ = shutdown.changed() => return (PipeDirectionEnd::Retired, from),
                };
                continue;
            }
        }
        crate::asyncrt::select! {
            frame = from.recv() => {
                if let Some(end) = stage_pipe_frame(
                    frame,
                    &mut queued,
                    &mut staged_bytes,
                    &budget,
                    &mut pending_close,
                    true,
                ) {
                    // The end drops the pipe while a send owns the head
                    // frame: settle it under the closing progress bound
                    // instead of tearing it ahead of the terminal the
                    // teardown reports. The end stands either way.
                    settle_parked_send(&mut sending, &mut shutdown).await;
                    return (end, from);
                }
            }
            _ = shutdown.changed() => {
                // Same settle on the shutdown path: the other direction
                // ending must not tear a live peer's in-flight frame.
                settle_parked_send(&mut sending, &mut shutdown).await;
                return (PipeDirectionEnd::Retired, from);
            }
            outcome = async {
                sending
                    .as_mut()
                    .expect("send armed while forwarding")
                    .0
                    .as_mut()
                    .await
            } => {
                let (_, bytes) = sending.take().expect("send armed while forwarding");
                match outcome {
                    PipeSendOutcome::Sent => {
                        staged_bytes = staged_bytes.saturating_sub(bytes);
                        if queued.is_empty() {
                            if let Some((code, reason)) = pending_close.take() {
                                return (PipeDirectionEnd::Close(code, reason), from);
                            }
                        }
                    }
                    PipeSendOutcome::Gone => return (PipeDirectionEnd::Retired, from),
                }
            }
        }
    }
}

/// One direction of the isolate-to-isolate pipe: frames read from `from`
/// cross into the other socket's pull queue `to`.
///
/// Capacity is reserved before a frame moves, never as part of moving it:
/// the parked reservation owns no frame, so the loop keeps receiving —
/// and keeps observing closes, retirements, and the shutdown signal — while
/// it waits. See [`forward_pipe_direction`] for the shared shape.
pub async fn forward_isolate_direction(
    from: tokio::sync::mpsc::UnboundedReceiver<crate::js::WsOut>,
    to: crate::js::WsPullSender,
    shutdown: tokio::sync::watch::Receiver<bool>,
    budget: crate::js::WsPipeBudget,
) -> PipeDirectionEnd {
    use crate::js::{WsOut, WsPull, WsPullSender};
    forward_pipe_direction(
        from,
        move |frame| {
            let to = to.clone();
            async move {
                let data = match frame {
                    WsOut::Text(text) => WsPull::Text(text),
                    WsOut::Binary(bytes) => WsPull::Binary(bytes),
                    // The forwarder only sends data frames; anything else
                    // ends the direction retired instead of panicking it.
                    WsOut::Close(..) | WsOut::Overload => return PipeSendOutcome::Gone,
                };
                let bytes = WsPullSender::permit_bytes(&data);
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
        shutdown,
        budget,
    )
    .await
    .0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::js::{ws_pull_channel, WsOut, WsPipeBudget, WsPull};

    /// One frame that alone fills the pull queue: the second frame's
    /// reservation is still parked when the close arrives — the ordering
    /// race — with no consumer running yet.
    fn full_queue_frame(tag: u8) -> WsOut {
        WsOut::Binary(vec![tag; crate::ws_registry::WS_PULL_QUEUE_MAX_BYTES])
    }

    fn binary_tag(frame: &WsPull) -> u8 {
        match frame {
            WsPull::Binary(bytes) => {
                assert_eq!(
                    bytes.len(),
                    crate::ws_registry::WS_PULL_QUEUE_MAX_BYTES,
                    "the queued frame must be the flooded one"
                );
                bytes[0]
            }
            WsPull::Text(_) | WsPull::Open(_) => panic!("expected queued data Binary"),
            WsPull::Close(..) => panic!("queued data must arrive before the terminal"),
        }
    }

    /// A close observed while a data send is parked must not drop the queued
    /// data: it flushes in order, then closes. Reverting the pending-close
    /// wait to an immediate return (the pre-fix behavior) loses the second
    /// frame, so this fails before the fix with no sleeps or timeouts.
    #[test]
    fn close_flushes_queued_data_in_order() {
        // The forwarder's progress bound runs on the execution domain clock,
        // so share the test binary's runtime like the other lib tests do.
        let runtime = crate::asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let (to, mut rx) = ws_pull_channel();
            let (tx, from) = tokio::sync::mpsc::unbounded_channel();
            tx.send(full_queue_frame(1)).expect("stage first");
            tx.send(full_queue_frame(2)).expect("stage second");
            tx.send(WsOut::Close(1000, "done".into()))
                .expect("stage close");
            let (_shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
            let direction = crate::asyncrt::spawn(forward_isolate_direction(
                from,
                to.clone(),
                shutdown,
                WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            // Free the parked reservation by consuming the head; the direction
            // can only report the close after the second frame moves behind it.
            let first = rx.recv().await.expect("first queued frame");
            assert_eq!(binary_tag(&first), 1, "frames keep channel order");
            let end = direction.await.expect("direction joins");
            match end {
                PipeDirectionEnd::Close(code, reason) => {
                    assert_eq!(code, 1000, "the pending close is delivered");
                    assert_eq!(reason, "done", "the pending close keeps its reason");
                    // The pipe teardown queues the reported terminal; it must
                    // arrive after the flushed data.
                    if to.send_close(code, reason, true).is_err() {
                        panic!("terminal queues after flush");
                    }
                }
                other => panic!("the pending close must end the direction, got {other:?}"),
            }
            let second = rx.recv().await.expect("flushed frame behind the close");
            assert_eq!(binary_tag(&second), 2, "queued data is not dropped");
            let terminal = rx.recv().await.expect("terminal");
            if !matches!(terminal, WsPull::Close(1000, _, _)) {
                panic!("the close follows the data");
            }
        });
    }

    /// Either overload trigger ends the direction `OverLimit`: an admission
    /// refusal queued behind admitted data (the marker), or data staged past
    /// the cap. Each pipe direction runs its own forwarder, so both run the
    /// same scenarios. No consumer, no sleeps: the overload end is immediate
    /// either way. Reverting the marker to a clean close (the pre-fix
    /// behavior) fails the marker case with `Close`, and removing the
    /// staged check fails the flood case.
    #[test]
    fn overload_marker_ends_both_directions_over_limit() {
        let runtime = crate::asyncrt::shared_test_runtime();
        runtime.block_on(async {
            for direction in ["caller", "loaded"] {
                let (to, _rx) = ws_pull_channel();
                let (tx, from) = tokio::sync::mpsc::unbounded_channel();
                tx.send(WsOut::Text("admitted".into())).expect("stage data");
                tx.send(WsOut::Overload).expect("stage marker");
                let (_shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
                let end = forward_isolate_direction(
                    from,
                    to,
                    shutdown,
                    WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
                )
                .await;
                assert!(
                    matches!(end, PipeDirectionEnd::OverLimit),
                    "{direction} direction: the refusal marker must end OverLimit, got {end:?}"
                );
            }
        });
    }

    #[test]
    fn staged_flood_ends_both_directions_over_limit() {
        let runtime = crate::asyncrt::shared_test_runtime();
        runtime.block_on(async {
            for direction in ["caller", "loaded"] {
                let (to, _rx) = ws_pull_channel();
                let (tx, from) = tokio::sync::mpsc::unbounded_channel();
                // Twenty 1 MiB frames stage past the 16 MiB cap with no
                // consumer draining the pull queue.
                for _ in 0..20 {
                    tx.send(WsOut::Binary(vec![0u8; 1024 * 1024]))
                        .expect("stage flood");
                }
                let (_shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
                let end = forward_isolate_direction(
                    from,
                    to,
                    shutdown,
                    WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
                )
                .await;
                assert!(
                    matches!(end, PipeDirectionEnd::OverLimit),
                    "{direction} direction: staged flood must end OverLimit, got {end:?}"
                );
            }
        });
    }

    /// A bounded test sink: one frame of capacity, drained by the test. A
    /// send parks while it is full, like a pull-queue reservation behind a
    /// peer that stopped reading — and the parked send must not blind the
    /// direction to the close queued behind it. Boxed: the closure type is
    /// unnameable, so a nested `impl Future` return is not expressible.
    fn boxed_test_sink(
        tx: tokio::sync::mpsc::Sender<WsOut>,
    ) -> impl FnMut(WsOut) -> std::pin::Pin<Box<dyn Future<Output = PipeSendOutcome> + Send + 'static>>
    {
        move |frame| {
            let tx = tx.clone();
            Box::pin(async move {
                if tx.send(frame).await.is_err() {
                    PipeSendOutcome::Gone
                } else {
                    PipeSendOutcome::Sent
                }
            })
        }
    }

    /// A close that arrives while a sink send is parked waits behind the
    /// parked frame even when the queue itself just drained: the parked
    /// frame still crosses first, then the close ends the direction. Without
    /// the parked-send check the close returns at once and drops both data
    /// frames — only the pre-fill ever reaches the sink — so the concurrent
    /// drain finds a closed sink and this fails before the fix with no
    /// sleeps or timeouts.
    #[test]
    fn parked_send_still_observes_close_in_order() {
        let runtime = crate::asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let (sink_tx, mut sink_rx) = tokio::sync::mpsc::channel(1);
            // Fill the sink before the direction starts, so the first
            // forward parks with the queue empty behind it.
            sink_tx
                .send(WsOut::Text("pre".into()))
                .await
                .expect("pre-fill the sink");
            let (tx, from) = tokio::sync::mpsc::unbounded_channel();
            tx.send(WsOut::Text("a".into())).expect("stage first");
            tx.send(WsOut::Text("b".into())).expect("stage second");
            tx.send(WsOut::Close(1000, "done".into()))
                .expect("stage close");
            let (_shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
            let direction = crate::asyncrt::spawn(forward_pipe_direction(
                from,
                boxed_test_sink(sink_tx),
                shutdown,
                WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            // Drain concurrently: the flush parks its second forward until
            // the test consumes the first, so awaiting the direction before
            // draining would park the test against the direction. Without the
            // parked-send check the close returns at once and drops both data
            // frames, so the drain finds a closed sink and this fails before
            // the fix with no sleeps or timeouts.
            let drain = crate::asyncrt::spawn(async move {
                let mut seen = Vec::new();
                for _ in 0..3 {
                    seen.push(sink_rx.recv().await.expect("sink drains in order"));
                }
                seen
            });
            let (end, _from) = direction.await.expect("direction joins");
            assert!(
                matches!(end, PipeDirectionEnd::Close(1000, _)),
                "the pending close ends the direction, got {end:?}"
            );
            let seen = drain.await.expect("drain joins");
            let mut texts = Vec::new();
            for frame in seen {
                match frame {
                    WsOut::Text(text) => texts.push(text),
                    _ => panic!("data frames cross before the close"),
                }
            }
            assert_eq!(
                texts,
                vec!["pre".to_string(), "a".to_string(), "b".to_string()],
                "the parked frame and the queued frame cross before the close"
            );
        });
    }

    /// The closing flush watches the pipe shutdown: the other direction
    /// ending retires a flush parked behind a peer that never reads, instead
    /// of draining through one progress interval per queued frame. The gate
    /// holds the first forward until both data frames and the close are
    /// staged — observed through the producer budget draining to zero, not
    /// through timing — so the shutdown deterministically lands in the flush.
    /// Without the shutdown arm the direction only returns the pending close
    /// after the progress bound expires, so this fails before the fix.
    #[test]
    fn closing_flush_observes_shutdown() {
        let runtime = crate::asyncrt::shared_test_runtime();
        runtime.block_on(async {
            use std::sync::atomic::{AtomicUsize, Ordering};
            let budget = WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES);
            let (tx, from) = tokio::sync::mpsc::unbounded_channel();
            for text in ["one", "two"] {
                let frame = WsOut::Text(text.into());
                assert!(
                    budget.charge(crate::js::ws_out_bytes(&frame)),
                    "the test budget admits the staged frame"
                );
                tx.send(frame).expect("stage data");
            }
            tx.send(WsOut::Close(1000, "done".into()))
                .expect("stage close");
            // The first forward holds until the test staged everything
            // behind it; every later forward parks: the peer never reads.
            // Each call reports its index, so the test knows the flush —
            // not the normal parked send — is what the shutdown interrupts.
            let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel::<u8>();
            let (gate_tx, gate_rx) = tokio::sync::oneshot::channel::<()>();
            let gate_rx = std::sync::Arc::new(std::sync::Mutex::new(Some(gate_rx)));
            let calls = std::sync::Arc::new(AtomicUsize::new(0));
            let sink = {
                let calls = calls.clone();
                move |frame: WsOut| {
                    let entered_tx = entered_tx.clone();
                    let gate_rx = gate_rx.clone();
                    let calls = calls.clone();
                    let _ = frame;
                    // Taken here, outside the async block: holding a
                    // `std` mutex guard across an await is not `Send`.
                    let gate = gate_rx.lock().unwrap().take();
                    Box::pin(async move {
                        let call = calls.fetch_add(1, Ordering::SeqCst);
                        let _ = entered_tx.send(call as u8);
                        if call == 0 {
                            if let Some(rx) = gate {
                                let _ = rx.await;
                            }
                            return PipeSendOutcome::Sent;
                        }
                        std::future::pending::<()>().await;
                        PipeSendOutcome::Gone
                    })
                        as std::pin::Pin<Box<dyn Future<Output = PipeSendOutcome> + Send + 'static>>
                }
            };
            let (shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
            let direction =
                crate::asyncrt::spawn(forward_pipe_direction(from, sink, shutdown, budget.clone()));
            // The first forward is parked at the gate; the budget drains to
            // zero exactly when both data frames are received, which also
            // means the close is staged and the direction is pending. No
            // timing: the releases must happen, and the spin only waits.
            assert_eq!(
                entered_rx.recv().await,
                Some(0),
                "the first forward parks at the gate"
            );
            while budget.used() > 0 {
                tokio::task::yield_now().await;
            }
            gate_tx.send(()).expect("release the first forward");
            assert_eq!(
                entered_rx.recv().await,
                Some(1),
                "the closing flush parks its forward"
            );
            shutdown_tx.send(true).expect("the other direction ended");
            let (end, _from) = direction.await.expect("direction joins");
            assert!(
                matches!(end, PipeDirectionEnd::Retired),
                "shutdown during the closing flush retires the direction, got {end:?}"
            );
        });
    }

    /// The generic forwarder ends an admission refusal `OverLimit` for any
    /// sink, so every pipe direction tears down loudly on the same marker.
    #[test]
    fn generic_direction_overload_marker_ends_over_limit() {
        let runtime = crate::asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let (sink_tx, _sink_rx) = tokio::sync::mpsc::channel::<WsOut>(1);
            let (tx, from) = tokio::sync::mpsc::unbounded_channel();
            tx.send(WsOut::Text("admitted".into())).expect("stage data");
            tx.send(WsOut::Overload).expect("stage marker");
            let (_shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
            let (end, _from) = forward_pipe_direction(
                from,
                boxed_test_sink(sink_tx),
                shutdown,
                WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            )
            .await;
            assert!(
                matches!(end, PipeDirectionEnd::OverLimit),
                "the refusal marker must end OverLimit, got {end:?}"
            );
        });
    }
    /// An overload marker that lands while a sink send is parked settles the
    /// parked send under the closing progress bound before reporting
    /// `OverLimit`: the head frame still crosses whole ahead of the loud
    /// 1011 the teardown writes. Dropping the parked send (the pre-fix
    /// behavior) loses the head frame, so the drain finds a closed sink and
    /// this fails before the fix. Deterministic without sleeps: the
    /// direction runs on a single-threaded runtime, so each yield runs it to
    /// pending, and the test frees the sink only after the marker was
    /// processed — no scheduling can free it first.
    #[test]
    fn overload_settles_parked_send_before_over_limit() {
        // The closing bound reads the process domain clock; the direction
        // itself runs on a single-threaded runtime below for exact
        // scheduling control.
        let _ = crate::asyncrt::shared_test_runtime();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime builds");
        runtime.block_on(async {
            let (sink_tx, mut sink_rx) = tokio::sync::mpsc::channel(1);
            // Fill the sink before the direction starts, so the first
            // forward parks with the queue empty behind it.
            sink_tx
                .send(WsOut::Text("pre".into()))
                .await
                .expect("pre-fill the sink");
            let (tx, from) = tokio::sync::mpsc::unbounded_channel();
            tx.send(WsOut::Text("head".into())).expect("stage data");
            let (_shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
            // Onto this test's own runtime, not the process domain:
            // `asyncrt::spawn` would run the direction on the shared
            // multi-thread runtime and lose the exact scheduling control
            // the yields below rely on.
            let direction = runtime.spawn(forward_pipe_direction(
                from,
                boxed_test_sink(sink_tx),
                shutdown,
                WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            // One yield runs the direction to pending: the head send is
            // parked on the pre-filled sink, so the direction cannot have
            // finished — and a single-threaded yield cannot return before
            // every ready task ran to pending.
            tokio::task::yield_now().await;
            assert!(
                !direction.is_finished(),
                "the parked send must hold the direction"
            );
            tx.send(WsOut::Overload).expect("stage marker");
            // Yields run the direction through the marker: without the
            // settle it returns at once and drops the parked send, while
            // the settle parks in the closing bound until the drain below
            // frees the sink.
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
            // Free the sink only now, after the marker was processed: the
            // settle completes the parked send, while the pre-fix return
            // left a closed sink behind and the second receive fails.
            let mut seen = Vec::new();
            for _ in 0..2 {
                seen.push(sink_rx.recv().await.expect("sink drains in order"));
            }
            let (end, _from) = direction.await.expect("direction joins");
            assert!(
                matches!(end, PipeDirectionEnd::OverLimit),
                "the refusal marker must end OverLimit, got {end:?}"
            );
            let mut texts = Vec::new();
            for frame in seen {
                match frame {
                    WsOut::Text(text) => texts.push(text),
                    _ => panic!("data frames cross before the overload end"),
                }
            }
            assert_eq!(
                texts,
                vec!["pre".to_string(), "head".to_string()],
                "the parked frame crosses whole before the overload end"
            );
        });
    }

    /// A shutdown observed while a sink send is parked retires the direction
    /// only after the parked frame crosses: same settle, `Retired` instead
    /// of `OverLimit`. Without the settle the shutdown drops the parked
    /// send, so the drain finds a closed sink and this fails before the fix.
    /// Deterministic without sleeps: the direction runs on a
    /// single-threaded runtime, so each yield runs it to pending, and the
    /// test frees the sink only after the shutdown was processed — no
    /// scheduling can free it first.
    #[test]
    fn shutdown_settles_parked_send_before_retire() {
        // The closing bound reads the process domain clock; the direction
        // itself runs on a single-threaded runtime below for exact
        // scheduling control.
        let _ = crate::asyncrt::shared_test_runtime();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime builds");
        runtime.block_on(async {
            let (sink_tx, mut sink_rx) = tokio::sync::mpsc::channel(1);
            sink_tx
                .send(WsOut::Text("pre".into()))
                .await
                .expect("pre-fill the sink");
            let (tx, from) = tokio::sync::mpsc::unbounded_channel();
            tx.send(WsOut::Text("head".into())).expect("stage data");
            let (shutdown_tx, shutdown) = tokio::sync::watch::channel(false);
            // Onto this test's own runtime, not the process domain: the
            // yields below rely on single-threaded scheduling.
            let direction = runtime.spawn(forward_pipe_direction(
                from,
                boxed_test_sink(sink_tx),
                shutdown,
                WsPipeBudget::new(ISOLATE_PIPE_MAX_STAGED_BYTES),
            ));
            // One yield runs the direction to pending: the head send is
            // parked on the pre-filled sink, so the direction cannot have
            // finished.
            tokio::task::yield_now().await;
            assert!(
                !direction.is_finished(),
                "the parked send must hold the direction"
            );
            shutdown_tx.send(true).expect("the other direction ended");
            // Yields run the direction through the shutdown: without the
            // settle it returns at once and drops the parked send, while
            // the settle parks in the closing bound until the drain below
            // frees the sink.
            for _ in 0..5 {
                tokio::task::yield_now().await;
            }
            // Free the sink only now, after the shutdown was processed: the
            // settle completes the parked send, while the pre-fix return
            // left a closed sink behind and the second receive fails.
            let mut seen = Vec::new();
            for _ in 0..2 {
                seen.push(sink_rx.recv().await.expect("sink drains in order"));
            }
            let (end, _from) = direction.await.expect("direction joins");
            assert!(
                matches!(end, PipeDirectionEnd::Retired),
                "shutdown during a parked send retires the direction, got {end:?}"
            );
            let mut texts = Vec::new();
            for frame in seen {
                match frame {
                    WsOut::Text(text) => texts.push(text),
                    _ => panic!("data frames cross before the retire"),
                }
            }
            assert_eq!(
                texts,
                vec!["pre".to_string(), "head".to_string()],
                "the parked frame crosses whole before the retire"
            );
        });
    }
}
