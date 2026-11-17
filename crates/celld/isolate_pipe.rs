// One direction of the isolate-to-isolate WebSocket pipe: frames read from
// one socket's output channel cross into the other socket's pull queue.
//
// Staged bytes one pipe direction may hold upstream of the pull queue.
//
// The direction's channel and its staged queue are both unbounded, so a
// flood into a peer that never reads would otherwise grow host memory
// without bound. Well above legitimate bursts (the `/flood-close` test
// stages ~3 MiB) and still a hard bound per stuck pipe.
pub const ISOLATE_PIPE_MAX_STAGED_BYTES: usize = 16 * 1024 * 1024;

/// How long a closing flush waits for the peer to make progress before it
/// ends with the pending close instead. Each forwarded frame restarts the
/// wait, so a slow but live peer still drains fully; a peer that stopped
/// reading ends the pipe instead of blocking closure indefinitely. Well
/// above the test peer's quarter-second frames and well under the caller's
/// close timeout.
const ISOLATE_PIPE_CLOSE_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// How one direction of the isolate-to-isolate pipe ended.
#[derive(Debug)]
pub enum IsolateDirectionEnd {
    /// The side sent a close frame; the teardown delivers it.
    Close(u16, String),
    /// The staged frames passed the cap; the teardown closes both ends loudly.
    OverLimit,
    /// The socket retired, or its queue went away: fail-closed, no terminal.
    Retired,
}

/// One frame's staged bytes, charged the way the pull queue charges them.
fn isolate_staged_bytes(frame: &crate::js::WsPull) -> usize {
    let payload = match frame {
        crate::js::WsPull::Text(text) => text.len(),
        crate::js::WsPull::Open(protocol) => protocol.len(),
        crate::js::WsPull::Binary(bytes) => bytes.len(),
        crate::js::WsPull::Close(_, reason, _) => reason.len(),
    };
    std::mem::size_of::<crate::js::WsPull>().saturating_add(payload)
}

/// Buffer one received frame, accounting its bytes and releasing the
/// producer budget it arrived under. Returns the direction's end when the
/// frame ends it: a close with nothing left to flush, a retirement, an
/// overload marker, or a staging bill past the cap. A close behind queued
/// frames waits instead: workerd queues the close behind the data, so the
/// direction reports it only after the flush below delivers everything ahead
/// of it. First close wins; anything later is dropped with the closing
/// pipe. Overload ends immediately either way, like the staged cap.
fn stage_isolate_frame(
    frame: Option<crate::js::WsOut>,
    queued: &mut std::collections::VecDeque<crate::js::WsPull>,
    staged_bytes: &mut usize,
    budget: &crate::js::WsPipeBudget,
    pending_close: &mut Option<(u16, String)>,
) -> Option<IsolateDirectionEnd> {
    use crate::js::{WsOut, WsPull};
    if let Some(data) = frame.as_ref() {
        match data {
            WsOut::Text(_) | WsOut::Binary(_) => budget.release(crate::js::ws_out_bytes(data)),
            WsOut::Close(..) | WsOut::Overload => {}
        }
    }
    let data = match frame {
        Some(WsOut::Text(text)) => WsPull::Text(text),
        Some(WsOut::Binary(bytes)) => WsPull::Binary(bytes),
        Some(WsOut::Close(code, reason)) => {
            if queued.is_empty() && pending_close.is_none() {
                return Some(IsolateDirectionEnd::Close(code, reason));
            }
            pending_close.get_or_insert((code, reason));
            return None;
        }
        // An admission refusal queued behind the admitted data: like the
        // staged cap it ends the direction loudly, immediately — the marker
        // itself stays in channel order behind everything admitted ahead of
        // it, so either overload trigger reaches the same teardown. Queued
        // frames drop, exactly as past the staged cap.
        Some(WsOut::Overload) => return Some(IsolateDirectionEnd::OverLimit),
        // The socket's request retired and took its socket with it.
        None => return Some(IsolateDirectionEnd::Retired),
    };
    *staged_bytes = staged_bytes.saturating_add(isolate_staged_bytes(&data));
    queued.push_back(data);
    if *staged_bytes > ISOLATE_PIPE_MAX_STAGED_BYTES {
        return Some(IsolateDirectionEnd::OverLimit);
    }
    None
}

/// One direction of the isolate-to-isolate pipe: frames read from `from`
/// cross into the other socket's pull queue `to`.
///
/// Capacity is reserved before a frame moves, never as part of moving it:
/// the parked reservation owns no frame, so the loop keeps receiving —
/// and keeps observing closes, retirements, and the shutdown signal — while
/// it waits. A close behind queued frames waits for the flush instead of
/// dropping it, but the flush only moves while the peer can make progress:
/// each forwarded frame restarts the progress wait, so a slow but live peer
/// still drains fully, while a peer that stopped reading ends the pipe with
/// the pending close instead of blocking closure indefinitely. Each
/// direction sends one frame at a time, in channel order, so per-direction
/// frame order is preserved.
///
/// Staged frames are accounted against [`ISOLATE_PIPE_MAX_STAGED_BYTES`]:
/// past the cap the direction reports [`IsolateDirectionEnd::OverLimit`] so
/// the teardown closes both ends loudly instead of growing host memory
/// without bound behind a peer that never reads.
pub async fn forward_isolate_direction(
    mut from: tokio::sync::mpsc::UnboundedReceiver<crate::js::WsOut>,
    to: crate::js::WsPullSender,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
    budget: crate::js::WsPipeBudget,
) -> IsolateDirectionEnd {
    use crate::js::{WsPull, WsPullSender};
    let mut queued: std::collections::VecDeque<WsPull> = std::collections::VecDeque::new();
    // Bytes staged upstream of the pull queue. Added on stage, released when
    // a reservation moves the head frame into the queue.
    let mut staged_bytes: usize = 0;
    // A parked capacity reservation: stored rather than awaited so the loop
    // keeps receiving while it is pending. Re-polling a parked reservation
    // is cancel-safe, and dropping it owns no frame, so nothing is lost.
    let mut acquiring = None;
    // A close waiting behind flushed frames. Set instead of returned while
    // anything remains to flush; delivered once the queue drains.
    let mut pending_close: Option<(u16, String)> = None;
    loop {
        if pending_close.is_some() {
            // A close is waiting behind flushed frames: forward while the
            // peer can make progress, and never park behind it without a
            // bound. Drop any parked acquire first — it owns no frame, so
            // nothing is lost.
            acquiring = None;
            // Eagerly stage whatever arrived, still under the cap.
            loop {
                match from.try_recv() {
                    Ok(frame) => {
                        if let Some(end) = stage_isolate_frame(
                            Some(frame),
                            &mut queued,
                            &mut staged_bytes,
                            &budget,
                            &mut pending_close,
                        ) {
                            return end;
                        }
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        return IsolateDirectionEnd::Retired;
                    }
                }
            }
            // Reserve the head with a progress bound: each forwarded frame
            // restarts the wait, so a slow but live peer still drains fully,
            // while a peer that stopped reading ends the pipe with the
            // pending close instead of blocking closure indefinitely.
            let Some(head) = queued.front() else {
                let (code, reason) = pending_close.take().expect("close pending while flushing");
                return IsolateDirectionEnd::Close(code, reason);
            };
            let bytes = WsPullSender::permit_bytes(head);
            match crate::asyncrt::timeout(ISOLATE_PIPE_CLOSE_FLUSH_TIMEOUT, to.reserve(bytes)).await
            {
                Ok(Some(permit)) => {
                    let head = queued.pop_front().expect("head staged above");
                    staged_bytes = staged_bytes.saturating_sub(isolate_staged_bytes(&head));
                    if to.send_reserved(head, permit).is_err() {
                        return IsolateDirectionEnd::Retired;
                    }
                }
                Ok(None) => {
                    // The queue is gone: the terminal sender closed the
                    // semaphore before queueing the close.
                    return IsolateDirectionEnd::Retired;
                }
                Err(_) => {
                    let (code, reason) =
                        pending_close.take().expect("close pending while flushing");
                    return IsolateDirectionEnd::Close(code, reason);
                }
            }
            continue;
        }
        if acquiring.is_none() {
            if let Some(head) = queued.front() {
                acquiring = Some(Box::pin(to.reserve(WsPullSender::permit_bytes(head))));
            } else {
                // Idle: wait for work or for the other direction's shutdown.
                crate::asyncrt::select! {
                    frame = from.recv() => {
                        if let Some(end) = stage_isolate_frame(
                            frame,
                            &mut queued,
                            &mut staged_bytes,
                            &budget,
                            &mut pending_close,
                        ) {
                            return end;
                        }
                    }
                    _ = shutdown.changed() => return IsolateDirectionEnd::Retired,
                };
                continue;
            }
        }
        crate::asyncrt::select! {
            frame = from.recv() => {
                if let Some(end) = stage_isolate_frame(
                    frame,
                    &mut queued,
                    &mut staged_bytes,
                    &budget,
                    &mut pending_close,
                ) {
                    return end;
                }
            }
            _ = shutdown.changed() => return IsolateDirectionEnd::Retired,
            permit = async { acquiring.as_mut().expect("acquire armed while forwarding").await } => {
                acquiring = None;
                let Some(permit) = permit else {
                    // The queue is gone: the terminal sender closed the
                    // semaphore before queueing the close.
                    return IsolateDirectionEnd::Retired;
                };
                let head = queued.pop_front().expect("acquire reserved the head frame");
                staged_bytes = staged_bytes.saturating_sub(isolate_staged_bytes(&head));
                if to.send_reserved(head, permit).is_err() {
                    return IsolateDirectionEnd::Retired;
                }
                if queued.is_empty() {
                    if let Some((code, reason)) = pending_close.take() {
                        return IsolateDirectionEnd::Close(code, reason);
                    }
                }
            }
        }
    }
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
                IsolateDirectionEnd::Close(code, reason) => {
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
                    matches!(end, IsolateDirectionEnd::OverLimit),
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
                    matches!(end, IsolateDirectionEnd::OverLimit),
                    "{direction} direction: staged flood must end OverLimit, got {end:?}"
                );
            }
        });
    }
}
