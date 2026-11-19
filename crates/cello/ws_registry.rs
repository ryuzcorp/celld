// Modified by Ryuz Corp (Cello): WebSocket ownership and join tokens; isolate-join senders for loaded-worker 101s (upstream #305). Apache-2.0.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The WebSocket registry: the sockets an instance holds, the frames waiting
//! to leave, and the isolate-polled input queues. Engine-neutral; the V8 ops
//! stay in `crate::js::websocket`, which re-exports every item at its old
//! path.
//!
//! Every socket has an owner, and every op that takes a socket id from script
//! answers only for it. Script names sockets by number (`new WebSocket(id)`),
//! so the ids are unguessable and the ownership is host-side: a cell socket
//! belongs to its scope (which a reloaded cell still holds after hibernation),
//! and an isolate-polled socket belongs to the request context that opened it.
use crate::asyncrt;
use crate::engine_api::WsTarget;
use crate::js::IoContext;
use anyhow::Result;
use rand::RngCore;
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct OutboundWsReq {
    pub scope: String,
    pub id: u64,
    pub url: String,
    pub protocols: Vec<String>,
    /// Present for an isolate-polled (Worker) socket. Created and registered
    /// on the JS thread before the request is sent, so `__ws_next` can never
    /// run ahead of its own queue.
    pub pull: Option<WsPullSender>,
    /// Extra request headers, for the `fetch()` upgrade form.
    pub headers: Vec<(String, String)>,
    /// A `fetch()` upgrade wants the whole handshake outcome, including the
    /// ordinary response a server that declines to upgrade sent instead.
    pub want_response: bool,
    /// A socket already upgraded in this process, which this request joins
    /// instead of dialing `url`. It is the cell end of a Durable Object
    /// subrequest whose caller kept the client end, so there is no handshake
    /// to run and no connection to open: the host only has to carry frames
    /// between two isolates.
    pub target: Option<WsTarget>,
    /// The facet that dialed, when `scope` names a Durable Object facet. The
    /// op derives it from the running event, so the socket routes through
    /// the facet's root and never names the facet as a cell.
    pub facet: Option<FacetSocket>,
    pub reply: tokio::sync::oneshot::Sender<Result<OutboundWsOpen>>,
}

/// The ordinary HTTP response a server sent instead of upgrading. `fetch()`
/// returns it verbatim rather than turning it into a connection error.
pub struct DeclinedUpgrade {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// What an outbound handshake produced.
pub struct OutboundWsOpen {
    pub protocol: Option<String>,
    pub declined: Option<DeclinedUpgrade>,
}

/// Outbound WebSocket traffic from a DO's `ws.send`/`ws.close`. The host holds
/// the socket in a task decoupled from the isolate (so the cell can hibernate
/// while the socket lives); `ws.send` routes here by wsId.
pub enum WsOut {
    Text(String),
    Binary(Vec<u8>),
    Close(u16, String),
    /// An admission refusal queued in order behind the admitted data: the
    /// forwarder ends the direction `OverLimit` when it reaches this marker,
    /// so both triggers (admission refusal, staged cap) reach the same
    /// unclean-1011 overload teardown. Application closes keep `Close`.
    Overload,
}

/// Outbound frames together with any positions already reserved in their sockets.
/// Captured sends reserve positions inside the isolate, before a later RPC can
/// send. The reservation travels through the handler and Actor gate with the
/// batch; reconstructing a plain vector at either handoff loses send order.
pub struct WsBatch(WsBatchState);

enum WsBatchState {
    Frames(Vec<(u64, WsOut)>),
    Captured(CapturedWsBatch),
}

struct CapturedWsBatch {
    registry: Arc<std::sync::Mutex<WsRegistry>>,
    state: Arc<WsFlushState>,
    /// The one flush that owns every segment this batch reserved. `None`
    /// only until the first frame, since a guard must name a segment.
    flush: Option<WsFlushGuard>,
    /// The newest segment `flush` holds on each socket, by token. A map, so a
    /// broadcast to many sockets does not scan every earlier segment per frame.
    tails: HashMap<u64, u64>,
    frames: usize,
}

impl Default for WsBatch {
    fn default() -> Self {
        Self(WsBatchState::Frames(Vec::new()))
    }
}

impl WsBatch {
    pub fn len(&self) -> usize {
        match &self.0 {
            WsBatchState::Frames(frames) => frames.len(),
            WsBatchState::Captured(batch) => batch.frames,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn capture(&mut self, id: u64, out: WsOut) {
        if let WsBatchState::Frames(frames) = &self.0 {
            assert!(frames.is_empty(), "capture starts with an empty batch");
            self.0 = WsBatchState::Captured(CapturedWsBatch {
                registry: ws_registry(),
                state: ws_flush_state(),
                flush: None,
                tails: HashMap::new(),
                frames: 0,
            });
        }
        let WsBatchState::Captured(batch) = &mut self.0 else {
            unreachable!("capture installs its ordered batch");
        };
        batch
            .state
            .capture(&mut batch.flush, &mut batch.tails, id, out);
        batch.frames += 1;
    }

    pub(crate) fn release(self, verdict: Result<(), String>) {
        match self.0 {
            WsBatchState::Captured(mut batch) => {
                if let Some(flush) = batch.flush.take() {
                    flush.release(&batch.registry, verdict);
                }
            }
            WsBatchState::Frames(frames) if verdict.is_ok() && !frames.is_empty() => {
                let flushing = ws_flush_state().emit_or_defer(&ws_registry(), frames, false);
                debug_assert!(flushing.is_none(), "a released batch opens no flush");
            }
            WsBatchState::Frames(_) => {}
        }
    }

    /// A rejected handler never published these frames. Remove its reservations
    /// without cancelling independent events queued behind them.
    pub(crate) fn discard(self) {
        if let WsBatchState::Captured(mut batch) = self.0 {
            if let Some(flush) = batch.flush.take() {
                flush.discard(&batch.registry);
            }
        }
    }
}

impl From<Vec<(u64, WsOut)>> for WsBatch {
    fn from(frames: Vec<(u64, WsOut)>) -> Self {
        Self(WsBatchState::Frames(frames))
    }
}

impl Drop for CapturedWsBatch {
    fn drop(&mut self) {
        // A lost dispatch or cancelled flush has no successful gate verdict.
        // Refuse its reservations so successors cannot hang or cross the gap.
        if let Some(flush) = self.flush.take() {
            flush.release(&self.registry, Err("WebSocket batch abandoned".into()));
        }
    }
}

/// The result of delivering one `webSocketMessage`. `frames` are the outbound
/// frames of the turn the handler answered in, captured by the output gate; a
/// turn the handler suspended in released its own frames as it ended, so those
/// are already gone. `write_position` is the cell's committed-write count
/// after the handler when it advanced past where it stood before — i.e. the
/// handler wrote, and its frames must be held until that position is durable.
/// `None` means no write: flush the frames.
pub struct WsDispatch {
    pub frames: WsBatch,
    pub write_position: Option<u64>,
    /// As on `HttpResponse`: what a handler that wrote nothing observed above
    /// the cell's published baseline, for the gate to hold its frames behind
    /// the proof of another handler's commit.
    pub observed_position: Option<u64>,
}

/// One inbound event on a socket the ISOLATE polls, rather than one the host
/// pushes into a cell.
///
/// A Durable Object socket must survive between events and wake a hibernated
/// cell, so its frames arrive as `CellJob`s. A Worker socket cannot work that
/// way: the stateless pool has no addressable isolate to push into. That is
/// not a limitation to route around — it is exactly the lifetime Cloudflare
/// gives a Worker socket, which lives and dies with its `IoContext`, and
/// `IoContext::close_sockets` is what enforces it here. So the isolate pulls,
/// the same way it already pulls a streamed response body.
pub enum WsPull {
    Open(String),
    Text(String),
    Binary(Vec<u8>),
    Close(u16, String, bool),
}

/// Maximum charged bytes for non-terminal frames in one isolate-polled
/// WebSocket input queue.
pub(crate) const WS_PULL_QUEUE_MAX_BYTES: usize = 1024 * 1024;

/// Tags for the byte frame `__ws_next` resolves with. A tagged buffer keeps a
/// binary message on its fast path instead of base64 through a JSON envelope.
const WS_PULL_TAG_TEXT: u8 = 0;
const WS_PULL_TAG_BINARY: u8 = 1;
const WS_PULL_TAG_OPEN: u8 = 2;
const WS_PULL_TAG_CLOSE: u8 = 3;

impl WsPull {
    pub(crate) fn encode(self) -> Vec<u8> {
        let (tag, mut body) = match self {
            WsPull::Text(text) => (WS_PULL_TAG_TEXT, text.into_bytes()),
            WsPull::Binary(bytes) => (WS_PULL_TAG_BINARY, bytes),
            WsPull::Open(protocol) => (WS_PULL_TAG_OPEN, protocol.into_bytes()),
            WsPull::Close(code, reason, was_clean) => (
                WS_PULL_TAG_CLOSE,
                serde_json::json!({
                    "code": code,
                    "reason": reason,
                    "wasClean": was_clean,
                })
                .to_string()
                .into_bytes(),
            ),
        };
        let mut framed = Vec::with_capacity(body.len() + 1);
        framed.push(tag);
        framed.append(&mut body);
        framed
    }
}

struct WsPullCharge {
    #[cfg(all(test, cello_internal_tests))]
    bytes: usize,
    #[cfg(all(test, cello_internal_tests))]
    queued_bytes: Arc<std::sync::atomic::AtomicUsize>,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

#[cfg(all(test, cello_internal_tests))]
impl Drop for WsPullCharge {
    fn drop(&mut self) {
        self.queued_bytes
            .fetch_sub(self.bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

struct QueuedWsPull {
    frame: WsPull,
    // Capacity and each non-terminal frame move through the channel together.
    // Every path that consumes or drops that frame therefore returns its queue
    // budget. One terminal frame has no permit because it must release a full
    // queue, but the test observer still measures its retained bytes.
    _charge: WsPullCharge,
}

struct WsPullSendState {
    tx: tokio::sync::mpsc::UnboundedSender<QueuedWsPull>,
    terminal_sent: bool,
}

#[derive(Clone)]
pub struct WsPullSender {
    state: Arc<std::sync::Mutex<WsPullSendState>>,
    #[cfg(all(test, cello_internal_tests))]
    queued_bytes: Arc<std::sync::atomic::AtomicUsize>,
    capacity: Arc<tokio::sync::Semaphore>,
}

pub struct WsPullReceiver {
    rx: tokio::sync::mpsc::UnboundedReceiver<QueuedWsPull>,
    #[cfg(all(test, cello_internal_tests))]
    queued_bytes: Arc<std::sync::atomic::AtomicUsize>,
}

pub fn ws_pull_channel() -> (WsPullSender, WsPullReceiver) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    #[cfg(all(test, cello_internal_tests))]
    let queued_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let capacity = Arc::new(tokio::sync::Semaphore::new(WS_PULL_QUEUE_MAX_BYTES));
    (
        WsPullSender {
            state: Arc::new(std::sync::Mutex::new(WsPullSendState {
                tx,
                terminal_sent: false,
            })),
            #[cfg(all(test, cello_internal_tests))]
            queued_bytes: queued_bytes.clone(),
            capacity,
        },
        WsPullReceiver {
            rx,
            #[cfg(all(test, cello_internal_tests))]
            queued_bytes,
        },
    )
}

impl WsPull {
    fn queue_bytes(&self) -> usize {
        let payload = match self {
            Self::Open(protocol) | Self::Text(protocol) => protocol.len(),
            Self::Binary(bytes) => bytes.len(),
            Self::Close(_, reason, _) => reason.len(),
        };
        std::mem::size_of::<Self>().saturating_add(payload)
    }
}

impl WsPullSender {
    pub fn is_closed(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.terminal_sent || state.tx.is_closed()
    }

    pub async fn send(&self, frame: WsPull) -> Result<(), WsPull> {
        let frame = match frame {
            WsPull::Close(code, reason, was_clean) => {
                return self.send_close(code, reason, was_clean);
            }
            frame => frame,
        };
        let permit = match self.reserve(Self::permit_bytes(&frame)).await {
            Some(permit) => permit,
            None => return Err(frame),
        };
        self.send_reserved(frame, permit)
    }

    /// Capacity units `frame` reserves, charged the way `send` charges them.
    /// One message can exceed the queue limit until the separate incoming
    /// message limit rejects it. Reserving the complete budget admits that
    /// message only into an empty queue and prevents a second message from
    /// increasing its retained peak.
    pub fn permit_bytes(frame: &WsPull) -> u32 {
        frame.queue_bytes().min(WS_PULL_QUEUE_MAX_BYTES) as u32
    }

    /// Reserve queue capacity for a frame without sending it. Dropping the
    /// future holds no frame and leaks no capacity, so a forwarding loop can
    /// park here while still observing closes. `None` when the queue is gone:
    /// the terminal sender closes the semaphore before it queues the close.
    pub async fn reserve(&self, bytes: u32) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.capacity.clone().acquire_many_owned(bytes).await.ok()
    }

    /// Queue a frame against a held reservation. Synchronous: the permit
    /// proves capacity, and the terminal check plus unbounded send cannot
    /// block.
    pub fn send_reserved(
        &self,
        frame: WsPull,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), WsPull> {
        // The terminal sender closes the semaphore before it queues the close.
        // A data sender can already own a permit at that point, so this lock
        // either orders that data before the close or rejects it after the
        // close. Without the shared order, a data frame could enter behind the
        // only terminal frame and keep the receiver open.
        let state = self.state.lock().unwrap();
        if state.terminal_sent {
            return Err(frame);
        }
        #[cfg(all(test, cello_internal_tests))]
        let bytes = frame.queue_bytes();
        #[cfg(all(test, cello_internal_tests))]
        self.queued_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        state
            .tx
            .send(QueuedWsPull {
                frame,
                _charge: WsPullCharge {
                    #[cfg(all(test, cello_internal_tests))]
                    bytes,
                    #[cfg(all(test, cello_internal_tests))]
                    queued_bytes: self.queued_bytes.clone(),
                    _permit: Some(permit),
                },
            })
            .map_err(|error| error.0.frame)
    }

    /// Queue the one terminal event without waiting for data capacity.
    ///
    /// The shared state orders the close against a data sender that already
    /// owns capacity. Closing the semaphore wakes every sender that still waits
    /// for capacity, so a stopped isolate cannot keep a host socket task alive.
    pub fn send_close(&self, code: u16, reason: String, was_clean: bool) -> Result<(), WsPull> {
        let frame = WsPull::Close(code, reason, was_clean);
        #[cfg(all(test, cello_internal_tests))]
        let bytes = frame.queue_bytes();
        let mut state = self.state.lock().unwrap();
        if state.terminal_sent {
            return Err(frame);
        }
        state.terminal_sent = true;
        self.capacity.close();
        #[cfg(all(test, cello_internal_tests))]
        self.queued_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        state
            .tx
            .send(QueuedWsPull {
                frame,
                _charge: WsPullCharge {
                    #[cfg(all(test, cello_internal_tests))]
                    bytes,
                    #[cfg(all(test, cello_internal_tests))]
                    queued_bytes: self.queued_bytes.clone(),
                    _permit: None,
                },
            })
            .map_err(|error| error.0.frame)
    }
}

impl WsPullReceiver {
    /// Poll the queue without waiting: `Empty` while senders live and
    /// nothing is queued, `Disconnected` once the last sender is gone.
    /// Test-only: lets a unit test observe sender drops without blocking.
    #[cfg(test)]
    pub(crate) fn try_recv(
        &mut self,
    ) -> Result<Option<WsPull>, tokio::sync::mpsc::error::TryRecvError> {
        let queued = self.rx.try_recv()?;
        let QueuedWsPull { frame, _charge } = queued;
        if matches!(&frame, WsPull::Close(..)) {
            self.rx.close();
        }
        drop(_charge);
        Ok(Some(frame))
    }

    pub(crate) async fn recv(&mut self) -> Option<WsPull> {
        let queued = self.rx.recv().await?;
        let QueuedWsPull { frame, _charge } = queued;
        if matches!(&frame, WsPull::Close(..)) {
            // The sender state prevents a frame from entering behind this one.
            // Close the receiver as part of consuming the terminal event, so a
            // later pull observes the end instead of waiting for sender drops.
            self.rx.close();
        }
        drop(_charge);
        Some(frame)
    }

    #[cfg(all(test, cello_internal_tests))]
    fn queued_bytes(&self) -> usize {
        self.queued_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(all(test, cello_internal_tests))]
mod input_queue_private {
    include!(env!("CELLO_INTERNAL_WEBSOCKET_TESTS"));
}

/// One socket's inbound queue. Shared so an op can await it without holding
/// the registry lock; one isolate polls a given socket serially.
type WsPullQueue = Arc<tokio::sync::Mutex<WsPullReceiver>>;

/// An isolate-polled queue and the owner whose `__ws_next` may drain it.
/// The queue and the owner are one entry so neither can outlive the other.
#[doc(hidden)]
pub struct WsPullEntry {
    pub queue: WsPullQueue,
    pub owner: WsOwner,
    /// The cell scope that reclaims this endpoint at shutdown, set at bind
    /// for cell/facet binds. Read authorization stays with `owner`: the
    /// re-owner this replaces broke `op_ws_next` for loaded workers whose
    /// next poll runs outside the joining cell, and hid queued frames
    /// behind a synthetic 1006 once output metadata was gone.
    pub cleanup_scope: Option<String>,
}
type WsPullRegistry = std::sync::Mutex<HashMap<u64, WsPullEntry>>;

/// Inbound queues for isolate-polled sockets, keyed by wsId.
pub(crate) fn ws_pull() -> Arc<WsPullRegistry> {
    asyncrt::services().websockets().pull.clone()
}

pub fn ws_pull_register(id: u64, rx: WsPullReceiver, owner: WsOwner) {
    ws_pull().lock().unwrap().insert(
        id,
        WsPullEntry {
            queue: Arc::new(tokio::sync::Mutex::new(rx)),
            owner,
            cleanup_scope: None,
        },
    );
}

pub fn ws_pull_unregister(id: u64) {
    ws_pull().lock().unwrap().remove(&id);
}

/// Record the cell scope that reclaims a pull endpoint at shutdown, set at
/// bind time for cell/facet binds: those binds are not tracked for request
/// retirement, so without this the endpoint's owner names a dead request no
/// sweep key matches. The read owner is unchanged. No-op for unknown ids.
pub fn ws_pull_set_cleanup_scope(id: u64, scope: &str) {
    if let Some(entry) = ws_pull().lock().unwrap().get_mut(&id) {
        entry.cleanup_scope = Some(scope.to_string());
    }
}

/// Drop every pull endpoint a dying cell scope owns: terminal consumption
/// and request retirement did not reclaim these, so cell shutdown reclaims
/// them by scope, mirroring the output unregister loop. Facet scopes of a
/// dying root die with it, so their endpoints go too — discovered through
/// metadata the way `ws_close_scope` discovers them, which is why this must
/// run before output unregister removes that metadata. An endpoint matches
/// by its read owner or by the cleanup scope bind recorded for it, so a
/// cell-bound target stays readable by its original owner until the sweep
/// takes it. Dropping an endpoint frees its queued frames. Returns the
/// count removed, so tests and callers observe the sweep. Locking stays
/// sequential (registry, then pull), never nested, matching the existing
/// registry-then-pull order.
pub fn ws_pull_unregister_cell(scope: &str) -> usize {
    let mut scopes = vec![scope.to_string()];
    {
        let registry = ws_registry();
        let registry = registry.lock().unwrap();
        for meta in registry.metadata.values() {
            if let Some(facet) = meta.facet.as_ref() {
                if facet.root == scope && !scopes.contains(&meta.scope) {
                    scopes.push(meta.scope.clone());
                }
            }
        }
    }
    let pull = ws_pull();
    let mut pull = pull.lock().unwrap();
    let owned: Vec<u64> = pull
        .iter()
        .filter_map(|(id, entry)| {
            let owned = matches!(&entry.owner, WsOwner::Cell(owner) if scopes.contains(owner));
            let bound = entry
                .cleanup_scope
                .as_ref()
                .is_some_and(|cleanup| scopes.contains(cleanup));
            (owned || bound).then_some(*id)
        })
        .collect();
    let count = owned.len();
    for id in owned {
        pull.remove(&id);
    }
    count
}

/// The frame channel that a top-level Worker transfers with its 101 response.
///
/// The channel and the socket id move together. Dropping the response before
/// the HTTP upgrade, or ending the upgrade task, therefore removes every host
/// registration instead of leaving a socket that no isolate can reach.
pub struct WorkerWebSocket {
    pub(crate) id: u64,
    pub(crate) inbound: WsPullSender,
    /// The pull endpoint outlives this handoff: the pipe teardown keeps it
    /// until its terminal is consumed or the joining request retires. Set by
    /// `drop_handoff`; every other constructor leaves it false so a dropped
    /// handoff still unregisters the endpoint and fails closed.
    pub(crate) keep_pull: bool,
}

impl WorkerWebSocket {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn inbound(&self) -> WsPullSender {
        self.inbound.clone()
    }

    /// Drop the handoff after a pipe teardown that keeps the pull endpoint:
    /// output is already unregistered, and the endpoint lives until its
    /// terminal is consumed or the joining request retires (which tracks the
    /// loaded id from bind time). Runs the field destructors — only the
    /// endpoint cleanup is skipped — so no send state leaks per joined
    /// pipe. Every other drop path keeps unregistering the endpoint, so an
    /// unjoined upgrade still fails closed.
    pub fn drop_handoff(mut self) {
        self.keep_pull = true;
    }
}

impl Drop for WorkerWebSocket {
    fn drop(&mut self) {
        if !self.keep_pull {
            ws_pull_unregister(self.id);
        }
        ws_unregister(self.id);
    }
}

/// Whether the host still holds an inbound queue for `id`.
///
/// A leak here is invisible from outside the process: the queue holds the
/// receiver whose survival keeps a connector task — and its TCP connection —
/// alive, and a request that dies before its socket is ever registered for
/// output leaves nothing else to observe.
#[cfg(cello_internal_tests)]
#[doc(hidden)]
pub fn ws_pull_registered(id: u64) -> bool {
    ws_pull().lock().unwrap().contains_key(&id)
}

pub enum WsIn {
    Text(String),
    Binary(Vec<u8>),
}

impl From<WsIn> for WsPull {
    fn from(frame: WsIn) -> Self {
        match frame {
            WsIn::Text(text) => Self::Text(text),
            WsIn::Binary(bytes) => Self::Binary(bytes),
        }
    }
}

/// Who may name a socket id from script. A cell socket belongs to its scope,
/// compared against the executing cell event — a scope, not an isolate, so
/// a cell reloaded after hibernation still owns its sockets. An
/// isolate-polled socket belongs to the request context that opened it, held
/// weakly so ownership never extends the request: a retired request owns
/// nothing, and its sockets are already closed by retirement anyway.
#[derive(Clone)]
#[doc(hidden)]
pub enum WsOwner {
    Cell(String),
    Request(std::sync::Weak<IoContext>),
}

impl WsOwner {
    /// Whether the running event — executing cell `cell`, if it runs in one,
    /// with request context `context` — may use a socket this owner holds.
    pub(crate) fn allows(&self, cell: Option<&str>, context: &Arc<IoContext>) -> bool {
        match self {
            WsOwner::Cell(scope) => cell.is_some_and(|cell| cell == scope),
            WsOwner::Request(owner) => owner
                .upgrade()
                .is_some_and(|owner| Arc::ptr_eq(&owner, context)),
        }
    }
}

#[doc(hidden)]
pub struct WsMeta {
    pub scope: String,
    pub hibernatable: bool,
    pub tags: Vec<String>,
    /// Structured-clone bytes, not JSON: `serializeAttachment` accepts
    /// anything cloneable, so Date, Map and Set must survive a round trip.
    pub attachment: Option<Vec<u8>>,
    pub pending: Vec<WsOut>,
    /// When the shell last answered this socket with the cell's auto-response,
    /// unix ms. Lives here rather than in the isolate because the reply is
    /// sent while the cell may not be resident at all.
    pub auto_response_at: Option<f64>,
    /// The root cell and facet stream behind a socket that a Durable Object
    /// facet accepted, dialed, or bound. `None` for a socket of an ordinary
    /// cell. `scope` then names the facet.
    pub facet: Option<FacetSocket>,
    /// Who may name this socket's id from script. Never serialized to script:
    /// `__ws_list` answers ids, tags, and attachments only.
    pub owner: WsOwner,
    /// The outstanding join token for this socket's target, minted when a 101
    /// carrying it is serialized for a caller and consumed by the join (or by
    /// the next mint). `None` while no join is in flight.
    pub bind_token: Option<u64>,
}

/// Where the events of a facet's socket go, and what proves their output.
///
/// A facet is not a cell: no Worker exports its class under the facet's
/// scope, so routing an event by that scope activated a cell that cannot
/// start (denoland/celld#252). The root cell owns the facet, its residency,
/// and its gate, so the socket routes and registers under `root`. The facet's
/// own writes are proved on `stream` at `epoch`.
///
/// Recorded by the host, never read from a `WsTarget`. A target crosses
/// application JavaScript, which could otherwise name another root.
#[derive(Clone, Debug)]
pub struct FacetSocket {
    pub root: String,
    pub stream: String,
    pub epoch: u64,
    /// The opening of the facet that holds the socket. An event reaches
    /// that opening only, never a later one at the same scope.
    pub opening: u64,
}
/// Close code a pipe uses when its staged backlog passes the cap: the
/// codebase's host-failure code (a lost batch closes sockets with it).
pub const WS_PIPE_OVERLOAD_CODE: u16 = 1011;

/// Reason for an overload close: abnormal, no peer sent it.
pub const WS_PIPE_OVERLOAD_REASON: &str = "isolate WebSocket pipe over its staged-frame cap";

/// One frame's staged bytes on an output channel, charged the way the pull
/// queue charges its own frames: payload plus the frame value itself.
pub fn ws_out_bytes(out: &WsOut) -> usize {
    let payload = match out {
        WsOut::Text(text) => text.len(),
        WsOut::Binary(bytes) => bytes.len(),
        WsOut::Close(_, reason) => reason.len(),
        // The overload marker carries no application payload; it is never
        // charged, so its accounting size never matters.
        WsOut::Overload => 0,
    };
    std::mem::size_of::<WsOut>().saturating_add(payload)
}

/// Admission budget for one pipe direction's output channel. The channel and
/// the forwarder's staged queue are both unbounded, so a flood into a peer
/// that never reads would otherwise grow host memory without bound. The
/// producer charges every data frame before it enqueues; the forwarder
/// releases what it receives. Closes bypass the budget, so a close stays
/// signalable when the data budget is full. Shared per direction: the
/// producer half lives in the output registry, the release half in the
/// forwarding task, and both die with the pipe.
#[derive(Clone)]
pub struct WsPipeBudget {
    used: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    cap: usize,
    tripped: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl WsPipeBudget {
    pub fn new(cap: usize) -> Self {
        Self {
            used: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            cap,
            tripped: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
    /// Bytes currently charged. Test-only: lets a forwarder test observe
    /// that every staged frame was received without racing the forwarder.
    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.used.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Charge `bytes`; false when the budget is full and the frame must not
    /// enqueue. Saturating, so a burst past the cap refuses rather than
    /// wrapping the count. Terminal once tripped: after the first refusal
    /// ends the pipe loudly, every later data frame refuses even when
    /// releases refill the count, so no data accepted after the refusal
    /// reaches the peer.
    pub fn charge(&self, bytes: usize) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        if self.tripped.load(Relaxed) {
            return false;
        }
        self.used
            .fetch_update(Relaxed, Relaxed, |used| {
                let total = used.saturating_add(bytes);
                (total <= self.cap).then_some(total)
            })
            .is_ok()
    }

    /// Release `bytes` the forwarder received. Exactly balances a charge:
    /// every enqueued data frame is received once, even when the pipe later
    /// drops it while closing.
    pub fn release(&self, bytes: usize) {
        use std::sync::atomic::Ordering::Relaxed;
        let _ = self
            .used
            .fetch_update(Relaxed, Relaxed, |used| Some(used.saturating_sub(bytes)));
    }

    /// Record an overload trip; true only for the first caller, so the pipe
    /// warns and queues its terminal close once per direction no matter how
    /// much floods past the cap.
    pub fn trip(&self) -> bool {
        !self
            .tripped
            .swap(true, std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(Default)]
#[doc(hidden)]
pub struct WsRegistry {
    pub(crate) outputs: HashMap<u64, tokio::sync::mpsc::UnboundedSender<WsOut>>,
    pub metadata: HashMap<u64, WsMeta>,
    pub(crate) worker_handoffs: HashMap<u64, WsPullSender>,
    /// Admission budgets for pipe directions that bound their output
    /// channel. Only directions that register one are policed; every other
    /// socket sends exactly as before.
    pub(crate) budgets: HashMap<u64, WsPipeBudget>,
    /// Host-owned quota identities for stashed join handoffs, by socket id.
    /// Routing keeps the empty scope for isolate-polled sockets; quota must
    /// not share a bucket across deployments solely because of that.
    pub(crate) quota_keys: HashMap<u64, WsQuotaKey>,
}
impl WsRegistry {
    #[doc(hidden)]
    pub fn register(&mut self, id: u64, tx: tokio::sync::mpsc::UnboundedSender<WsOut>) {
        let _ = self.register_budgeted(id, tx, None);
    }

    /// Register an output channel with an admission budget. A budget created
    /// while frames buffered pre-registration is adopted with its used count
    /// and tripped state, so the backlog is not charged twice; a backlog
    /// under the cap still arrives in order. The backlog was already charged
    /// frame by frame as it buffered, so the flush sends it directly instead
    /// of re-admitting it. Without a pre-existing budget the passed one
    /// polices live sends, and without either — the plain `register` path —
    /// the adopted accounting is dropped with the flush.
    #[doc(hidden)]
    pub fn register_budgeted(
        &mut self,
        id: u64,
        tx: tokio::sync::mpsc::UnboundedSender<WsOut>,
        budget: Option<WsPipeBudget>,
    ) -> Option<WsPipeBudget> {
        let effective = match budget {
            // Buffered frames already charged against the existing budget;
            // keep their accounting, including a trip, instead of charging
            // twice. Otherwise the passed budget polices live sends.
            Some(budget) => Some(self.budgets.entry(id).or_insert(budget).clone()),
            // The plain `register` path stays unpoliced: drop any accounting
            // the buffered backlog created, now that it has flushed.
            None => {
                self.budgets.remove(&id);
                None
            }
        };
        let pending = if let Some(meta) = self.metadata.get_mut(&id) {
            std::mem::take(&mut meta.pending)
        } else {
            Vec::new()
        };
        for frame in pending {
            let _ = tx.send(frame);
        }
        self.outputs.insert(id, tx);
        effective
    }

    /// Admit one frame onto `tx`, charging the direction's budget when it has
    /// one. Closes and the overload marker bypass the budget so a terminal
    /// stays signalable when the data budget is full. The first refused data
    /// frame ends the pipe loudly: it queues the overload marker behind
    /// everything admitted ahead of it, and the tripped budget refuses every
    /// later data frame, so no data accepted after the refusal reaches the
    /// peer. True when the frame was sent.
    fn admit(&self, id: u64, out: WsOut, tx: &tokio::sync::mpsc::UnboundedSender<WsOut>) -> bool {
        match (&out, self.budgets.get(&id)) {
            (WsOut::Close(..), _) | (WsOut::Overload, _) | (_, None) => {}
            (_, Some(budget)) => {
                if !budget.charge(ws_out_bytes(&out)) {
                    if budget.trip() {
                        tracing::warn!(
                            ws_id = id,
                            "dropping outbound WebSocket frames past the pipe's staged cap",
                        );
                        // Terminal overload: the forwarder reports
                        // `OverLimit` when it reaches this marker, after
                        // everything admitted ahead of it.
                        let _ = tx.send(WsOut::Overload);
                    }
                    return false;
                }
            }
        }
        // Only a charged data frame holds budget: closes, the marker, and
        // unpoliced directions bypassed the charge above.
        let charged =
            !matches!(out, WsOut::Close(..) | WsOut::Overload) && self.budgets.contains_key(&id);
        let bytes = if charged { ws_out_bytes(&out) } else { 0 };
        if tx.send(out).is_err() {
            // The frame never reached its channel: release the admission
            // charge, or a retired receiver trips the budget for live sends
            // that will never be received either.
            if charged {
                if let Some(budget) = self.budgets.get(&id) {
                    budget.release(bytes);
                }
            }
            return false;
        }
        true
    }

    fn unregister(&mut self, id: u64) -> Option<WsMeta> {
        self.outputs.remove(&id);
        self.budgets.remove(&id);
        self.quota_keys.remove(&id);
        self.metadata.remove(&id)
    }

    #[doc(hidden)]
    pub fn emit(&mut self, id: u64, out: WsOut) {
        if let Some(tx) = self.outputs.get(&id) {
            tracing::debug!(ws_id = id, "queued outbound WebSocket frame");
            let tx = tx.clone();
            self.admit(id, out, &tx);
        } else if let Some(meta) = self.metadata.get_mut(&id) {
            match out {
                // A terminal stays bufferable when the byte budget is full.
                WsOut::Close(..) | WsOut::Overload => {
                    tracing::debug!(ws_id = id, "buffered pre-upgrade WebSocket frame");
                    meta.pending.push(out);
                }
                _ => {
                    // The byte budget exists before pre-registration frames
                    // are retained, so a caller that never accepts the
                    // upgrade cannot stash unbounded bytes: charging starts
                    // at the first buffered frame, and past the cap the
                    // backlog keeps one in-order overload marker whose trip
                    // registration carries forward.
                    let budget = self
                        .budgets
                        .entry(id)
                        .or_insert_with(|| {
                            WsPipeBudget::new(crate::isolate_pipe::ISOLATE_PIPE_MAX_STAGED_BYTES)
                        })
                        .clone();
                    if budget.charge(ws_out_bytes(&out)) {
                        tracing::debug!(ws_id = id, "buffered pre-upgrade WebSocket frame");
                        meta.pending.push(out);
                    } else if budget.trip() {
                        tracing::warn!(
                            ws_id = id,
                            "dropping buffered WebSocket frames past the pipe's staged cap",
                        );
                        meta.pending.push(WsOut::Overload);
                    }
                }
            }
        } else {
            // The socket is gone and the frame has nowhere to go. Silence here
            // is what made a held frame indistinguishable from a sent one.
            tracing::warn!(ws_id = id, "dropped a frame for a closed WebSocket");
        }
    }
}
/// Every piece of one instance's WebSocket state.
///
/// A socket's id is allocated by `next_id` here, so an id identifies a socket
/// only within one instance. Each map a socket reaches must therefore belong
/// to the same instance: a map that stayed process-wide would be shared by
/// the sockets that two instances both numbered, and a second instance is not
/// hypothetical: a test can run multiple instances, and a per-isolate or
/// per-generation instance in production would inherit the
/// collision.
pub(crate) struct WebSocketService {
    registry: Arc<std::sync::Mutex<WsRegistry>>,
    regular_counts: Arc<std::sync::Mutex<HashMap<String, usize>>>,
    auto_responses: Arc<std::sync::Mutex<HashMap<String, (String, String)>>>,
    pull: Arc<WsPullRegistry>,
    flush: Arc<WsFlushState>,
}

impl Default for WebSocketService {
    fn default() -> Self {
        Self {
            registry: Arc::new(std::sync::Mutex::new(WsRegistry::default())),
            regular_counts: Arc::new(std::sync::Mutex::new(HashMap::new())),
            auto_responses: Arc::new(std::sync::Mutex::new(HashMap::new())),
            pull: Arc::new(std::sync::Mutex::new(HashMap::new())),
            flush: Arc::new(WsFlushState::default()),
        }
    }
}

pub(crate) fn ws_registry() -> Arc<std::sync::Mutex<WsRegistry>> {
    asyncrt::services().websockets().registry.clone()
}

fn regular_ws_counts() -> Arc<std::sync::Mutex<HashMap<String, usize>>> {
    asyncrt::services().websockets().regular_counts.clone()
}
pub(crate) fn increment_regular_ws(scope: &str) {
    *regular_ws_counts()
        .lock()
        .unwrap()
        .entry(scope.to_string())
        .or_default() += 1;
}
pub(crate) fn decrement_regular_ws(scope: &str) {
    let counts = regular_ws_counts();
    let mut counts = counts.lock().unwrap();
    let Some(count) = counts.get_mut(scope) else {
        return;
    };
    *count = count.saturating_sub(1);
    if *count == 0 {
        counts.remove(scope);
    }
}
pub fn has_regular_websocket(scope: &str) -> bool {
    regular_ws_counts()
        .lock()
        .unwrap()
        .get(scope)
        .is_some_and(|count| *count > 0)
}
/// The auto-response pair per cell scope, set by
/// `state.setWebSocketAutoResponse`. Shell state, like the socket registry:
/// the whole point of the feature is answering a matched message while the
/// cell is not resident, so the isolate cannot hold it.
pub(crate) fn ws_auto_responses() -> Arc<std::sync::Mutex<HashMap<String, (String, String)>>> {
    asyncrt::services().websockets().auto_responses.clone()
}

/// The shell's read path asks here before dispatching a text frame. A match
/// returns the response to send on the same socket and stamps the socket's
/// timestamp; the frame then never reaches the cell — no dispatch, no wake.
/// Only hibernatable sockets participate, as in workerd, where matching
/// lives in the hibernation manager's read loop.
pub fn ws_auto_response(scope: &str, id: u64, text: &str) -> Option<String> {
    let response = {
        let pairs = ws_auto_responses();
        let pairs = pairs.lock().unwrap();
        let (request, response) = pairs.get(scope)?;
        if request != text {
            return None;
        }
        response.clone()
    };
    let registry = ws_registry();
    let mut registry = registry.lock().unwrap();
    let meta = registry.metadata.get_mut(&id)?;
    if !meta.hibernatable {
        return None;
    }
    meta.auto_response_at = Some(unix_ms());
    Some(response)
}

fn unix_ms() -> f64 {
    asyncrt::wall_ms() as f64
}

/// Mark a socket as accepted by the facet `scope`, so its events reach the
/// facet through `facet.root`. A socket that `scope` did not accept keeps its
/// own route: a facet can return a socket that another cell accepted.
pub(crate) fn ws_attach_facet(id: u64, scope: &str, facet: FacetSocket) {
    if let Some(meta) = ws_registry().lock().unwrap().metadata.get_mut(&id) {
        if meta.scope == scope {
            meta.facet = Some(facet);
        }
    }
}

/// The facet route of a socket, when a facet accepted it.
pub fn ws_facet(id: u64) -> Option<FacetSocket> {
    ws_registry()
        .lock()
        .unwrap()
        .metadata
        .get(&id)
        .and_then(|meta| meta.facet.clone())
}

/// The cell the core holds `target` under, and whether that registration can
/// hibernate.
///
/// A facet's socket registers under its root, as a socket that cannot
/// hibernate. A root that hibernated would close its facets, and nothing can
/// restart a facet without the root's code: the class came from the root's
/// startup callback. The socket therefore keeps the root resident, as a
/// regular socket does.
pub fn ws_core_registration(target: &WsTarget) -> (String, bool) {
    let registry = ws_registry();
    let registry = registry.lock().unwrap();
    match registry.metadata.get(&target.id) {
        Some(WsMeta {
            facet: Some(facet), ..
        }) => (facet.root.clone(), false),
        meta => (
            target.scope.clone(),
            meta.is_some_and(|meta| meta.hibernatable),
        ),
    }
}

/// Remove the registration of a socket that the facet `scope` accepted for a
/// response that never left. No socket task exists for it, so nothing else
/// would remove it.
pub(crate) fn ws_release_unanswered(id: u64, scope: &str) {
    let accepted = ws_registry()
        .lock()
        .unwrap()
        .metadata
        .get(&id)
        .is_some_and(|meta| meta.scope == scope);
    if accepted {
        ws_unregister(id);
    }
}

/// Register a socket that a facet accepted, through a cfg-gated execution
/// backend that has no facet call to record it.
#[cfg(cello_internal_tests)]
#[doc(hidden)]
pub fn ws_register_facet_for_test(
    id: u64,
    scope: &str,
    facet: FacetSocket,
    tx: tokio::sync::mpsc::UnboundedSender<WsOut>,
) {
    ws_register_hibernatable_for_test(id, scope, tx);
    ws_attach_facet(id, scope, facet);
}

/// One unguessable socket number: nonzero, and exactly representable as the
/// f64 it crosses V8 and JSON as. Script names sockets by number, so a
/// sequential counter lets any script adopt any socket; randomness closes
/// the guessing, and per-socket ownership closes the replay of a leaked id.
fn random_socket_number() -> u64 {
    const F64_SAFE: u64 = 0x1FFF_FFFF_FFFF; // 2^53 - 1
    loop {
        let id = rand::rngs::OsRng.next_u64() & F64_SAFE;
        if id != 0 {
            return id;
        }
    }
}

pub fn ws_next_id() -> u64 {
    loop {
        let id = random_socket_number();
        if !ws_id_registered(id) {
            return id;
        }
    }
}

/// Whether `id` is registered anywhere script can reach: metadata, outputs,
/// an isolate-polled queue, or a Worker handoff. Registration ops refuse an
/// id another owner holds, so a guessed id cannot be adopted or re-scoped.
pub(crate) fn ws_id_registered(id: u64) -> bool {
    let registry = ws_registry();
    let registry = registry.lock().unwrap();
    registry.outputs.contains_key(&id)
        || registry.metadata.contains_key(&id)
        || registry.worker_handoffs.contains_key(&id)
        || ws_pull().lock().unwrap().contains_key(&id)
}

/// The owner of `id`: its metadata first, else its isolate-polled queue.
/// `None` for an id nothing registered, which the ops answer exactly as a
/// missing socket — a dropped frame, a 1006, a null — with no check and no
/// oracle distinguishing a foreign socket from a closed one.
pub(crate) fn ws_owner(id: u64) -> Option<WsOwner> {
    if let Some(owner) = ws_registry()
        .lock()
        .unwrap()
        .metadata
        .get(&id)
        .map(|meta| meta.owner.clone())
    {
        return Some(owner);
    }
    ws_pull()
        .lock()
        .unwrap()
        .get(&id)
        .map(|entry| entry.owner.clone())
}

/// Whether `id` names a socket the cell `scope` owns: registered metadata
/// held by exactly that scope. The 101 check and the accept check share it.
pub(crate) fn ws_owned_by_cell(id: u64, scope: &str) -> bool {
    ws_registry()
        .lock()
        .unwrap()
        .metadata
        .get(&id)
        .is_some_and(|meta| matches!(&meta.owner, WsOwner::Cell(owner) if owner == scope))
}

/// The scope `id` is registered under, if it is registered at all. The join
/// checks the target's scope against this, so a valid token cannot be
/// retargeted at another scope by rewriting the target JSON.
pub(crate) fn ws_registered_scope(id: u64) -> Option<String> {
    ws_registry()
        .lock()
        .unwrap()
        .metadata
        .get(&id)
        .map(|meta| meta.scope.clone())
}

/// Mint the single-use join token for `id`, for a 101 serialized for a
/// caller that may join it. `None` when `id` holds no metadata — an already
/// released socket — and the join then fails closed.
pub(crate) fn ws_mint_bind_token(id: u64) -> Option<u64> {
    let token = random_socket_number();
    let registry = ws_registry();
    let mut registry = registry.lock().unwrap();
    let meta = registry.metadata.get_mut(&id)?;
    meta.bind_token = Some(token);
    Some(token)
}

/// Join handoffs for a loaded worker's own 101, stashed when its response is
/// serialized for the caller isolate. Moving the whole `WorkerWebSocket`
/// here (instead of dropping it at serialization) keeps the loaded socket's
/// metadata, bind token, and input-queue sender alive for the join: the
/// handoff's `Drop` would otherwise unregister the socket before the caller
/// could bind it. The joiner takes it — the caller-side bind pipe, or the
/// final host response conversion on pass-through — and an unjoined upgrade
/// fails closed.
///
/// A caller that will join does so within its own request flow, so an entry
/// that is still here past `MAX_JOIN_HANDOFFS` newer stashes belongs to an
/// upgrade nobody ever joined: the caller discarded its 101 without binding
/// or passing it through, and no retirement path can name it. Capped shares
/// per scope (below) plus the process-wide bound keep the map bounded;
/// dropping the handoff unregisters its socket, and the late join fails
/// closed exactly like a join for an unknown id.
const MAX_JOIN_HANDOFFS: usize = 1024;

/// A scope that holds this many unjoined handoffs yields its own oldest
/// entry first when room runs out: one tenant's answered-but-never-joined
/// 101s can fill its own share but can never push out another scope's
/// pending join (denoland/celld#305).
const MAX_JOIN_HANDOFFS_PER_SCOPE: usize = 64;

/// A stashed handoff older than this never joins. The stash only bridges a
/// 101 to its join within the same request flow — the pass-through take runs
/// in the same synchronous conversion as the stash, and the caller-bind take
/// fires when the caller's upgrade continues — so a legitimate entry is
/// taken in well under a second, while host operations around it time out at
/// 30 s. Sixty seconds is pure safety margin; anything older is an upgrade
/// nobody ever joined, swept on every stash so abandoned entries cannot
/// accumulate into a blockade (denoland/celld#305).
const MAX_JOIN_HANDOFF_AGE_MS: u64 = 60_000;

/// Quota identity for a stashed join handoff: the host-owned deployment
/// that prepared the socket, plus the cell scope where one applies.
/// Routing keeps the empty scope for isolate-polled sockets, so keying the
/// eviction share by routing scope alone would put every deployment's
/// handoffs in one bucket and let one tenant evict another's pending join.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WsQuotaKey {
    pub deployment: String,
    pub scope: Option<String>,
}

struct JoinHandoff {
    worker: WorkerWebSocket,
    // Insertion order, not wall time: the oldest stash is the smallest
    // sequence, with no ties and no clock to inject in tests.
    seq: u64,
    // Quota identity at stash time. The handoff drops with its entry, so the
    // key only has to be right while the entry is stashed — exactly when
    // eviction reads it.
    quota: WsQuotaKey,
    // When the entry was stashed, in execution-domain monotonic
    // milliseconds, for the abandonment sweep below.
    stashed_at_ms: u64,
}

static JOIN_HANDOFF_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

static ISOLATE_JOIN_HANDOFFS: std::sync::LazyLock<std::sync::Mutex<HashMap<u64, JoinHandoff>>> =
    std::sync::LazyLock::new(Default::default);

fn isolate_join_handoffs() -> &'static std::sync::Mutex<HashMap<u64, JoinHandoff>> {
    &ISOLATE_JOIN_HANDOFFS
}

/// Drop handoffs older than `MAX_JOIN_HANDOFF_AGE_MS`: upgrades nobody ever
/// joined. Dropping a handoff unregisters its socket, so a late join fails
/// closed exactly like a join for an unknown id. Stamps only ever move
/// forward — the domain clock is monotonic and tests pass explicit times —
/// so the saturating subtraction below cannot hide a real age.
fn sweep_expired_handoffs(handoffs: &mut HashMap<u64, JoinHandoff>, now_ms: u64) {
    handoffs.retain(|_, handoff| {
        now_ms.saturating_sub(handoff.stashed_at_ms) < MAX_JOIN_HANDOFF_AGE_MS
    });
}

/// Stash the loaded socket's handoff for a later caller-isolate join.
/// Replaces any previous stash for the id: a 101 serialization mints a
/// fresh single-use bind token, so only the newest target can join. Expired
/// handoffs are swept first; past its quota share the stash then displaces
/// the oldest handoff of that same quota, and past the process-wide bound a
/// quota still under its share displaces the oldest handoff of a quota at or
/// over its share — the globally oldest when every quota is under its share.
/// Either way a quota under its share never loses a pending join to another
/// quota's churn.
pub fn ws_stash_join_handoff(worker: WorkerWebSocket) {
    ws_stash_join_handoff_at(worker, crate::asyncrt::mono_ms());
}

/// Stash with an explicit clock, so tests sweep and displace
/// deterministically without sleeping.
fn ws_stash_join_handoff_at(worker: WorkerWebSocket, now_ms: u64) {
    let id = worker.id();
    // Read before taking the handoff lock: the registry and handoff mutexes
    // are never held together, in either order, anywhere on this path.
    // Quota falls back to the routing scope only for sockets no preparation
    // op ever keyed — handoff sockets always carry the preparing deployment.
    let quota = ws_quota_key(id).unwrap_or_else(|| WsQuotaKey {
        deployment: String::new(),
        scope: ws_registered_scope(id),
    });
    let mut handoffs = isolate_join_handoffs().lock().unwrap();
    // A re-stash replaces its own entry: the 101 serialization minted a
    // fresh single-use bind token, so only the newest target can join, and
    // replacing never grows the map. Pull it out first, so the bound checks
    // below only ever weigh other entries; dropping the old worker here
    // unregisters the id exactly as the replaced insert always did.
    handoffs.remove(&id);
    sweep_expired_handoffs(&mut handoffs, now_ms);
    // Per-quota counts after the sweep: displacement below only ever takes
    // from a quota at or over its share while one exists.
    let mut share_counts: HashMap<&WsQuotaKey, usize> = HashMap::new();
    for handoff in handoffs.values() {
        *share_counts.entry(&handoff.quota).or_default() += 1;
    }
    let over_share = |quota: &WsQuotaKey| {
        share_counts
            .get(quota)
            .is_some_and(|&count| count >= MAX_JOIN_HANDOFFS_PER_SCOPE)
    };
    let same_quota = share_counts.get(&quota).copied().unwrap_or(0);
    if same_quota >= MAX_JOIN_HANDOFFS_PER_SCOPE || handoffs.len() >= MAX_JOIN_HANDOFFS {
        // The newcomer's own quota is at its share: evict only from that
        // quota, so a second full quota never loses a pending join to this
        // stash's churn. Only when the map is full while the incoming
        // quota still has headroom does the stash displace the oldest
        // entry of a quota at or over its share — and only when every
        // quota is under its share the globally oldest entry. A quota
        // under its share never loses an entry to another quota's stash.
        let oldest = if same_quota >= MAX_JOIN_HANDOFFS_PER_SCOPE {
            handoffs
                .iter()
                .filter(|(_, handoff)| handoff.quota == quota)
                .min_by_key(|(_, handoff)| (handoff.stashed_at_ms, handoff.seq))
                .map(|(id, _)| *id)
        } else {
            handoffs
                .iter()
                .filter(|(_, handoff)| over_share(&handoff.quota))
                .min_by_key(|(_, handoff)| (handoff.stashed_at_ms, handoff.seq))
                .map(|(id, _)| *id)
                .or_else(|| {
                    handoffs
                        .iter()
                        .min_by_key(|(_, handoff)| (handoff.stashed_at_ms, handoff.seq))
                        .map(|(id, _)| *id)
                })
        };
        match oldest {
            Some(oldest) => {
                handoffs.remove(&oldest);
            }
            // Unreachable: the branch runs only over a non-empty map — the
            // share check implies entries of the newcomer's quota, the bound
            // check a full map. Refuse rather than displace wrongly, failing
            // this join closed.
            None => return,
        }
    }
    handoffs.insert(
        id,
        JoinHandoff {
            worker,
            seq: JOIN_HANDOFF_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            quota,
            stashed_at_ms: now_ms,
        },
    );
}

/// Take the stashed join handoff for `id`, if its 101 was serialized for a
/// caller join and nobody has joined it yet.
pub fn ws_take_join_handoff(id: u64) -> Option<WorkerWebSocket> {
    isolate_join_handoffs()
        .lock()
        .unwrap()
        .remove(&id)
        .map(|handoff| handoff.worker)
}

/// Consume `token` against `id`: true only when it matches the outstanding
/// mint. Every join verification consumes; every 101 serialization mints.
pub(crate) fn ws_consume_bind_token(id: u64, token: u64) -> bool {
    let registry = ws_registry();
    let mut registry = registry.lock().unwrap();
    let Some(meta) = registry.metadata.get_mut(&id) else {
        return false;
    };
    if meta.bind_token != Some(token) {
        return false;
    }
    meta.bind_token = None;
    true
}
/// Register an output channel with an admission budget for one pipe
/// direction. Only directions that register a budget are policed; the
/// pre-registration backlog flushes through the same check as live sends.
/// Returns the budget the direction is policed by: a backlog buffered
/// before registration keeps the budget it was charged against, so the
/// forwarder must release what it receives against the returned one, not
/// the passed one, or releases leak and live sends trip early.
pub fn ws_register_budgeted(
    id: u64,
    tx: tokio::sync::mpsc::UnboundedSender<WsOut>,
    budget: WsPipeBudget,
) -> WsPipeBudget {
    ws_registry()
        .lock()
        .unwrap()
        .register_budgeted(id, tx, Some(budget))
        .expect("a passed budget always leaves an effective one")
}

/// Record the host-owned quota identity of a socket prepared for a join
/// handoff: the deployment that prepared it, plus the cell scope where one
/// applies. Routing keeps its own (possibly empty) scope; this key exists
/// only so eviction shares do not collide across deployments.
pub fn ws_set_quota_key(id: u64, quota: WsQuotaKey) {
    ws_registry().lock().unwrap().quota_keys.insert(id, quota);
}

/// The quota identity recorded for `id`, if preparation keyed it.
pub(crate) fn ws_quota_key(id: u64) -> Option<WsQuotaKey> {
    ws_registry().lock().unwrap().quota_keys.get(&id).cloned()
}

pub fn ws_register(id: u64, tx: tokio::sync::mpsc::UnboundedSender<WsOut>) {
    ws_registry().lock().unwrap().register(id, tx);
}

/// Install one hibernatable socket through a cfg-gated execution backend.
#[cfg(cello_internal_tests)]
pub(crate) fn ws_register_hibernatable_for_test(
    id: u64,
    scope: &str,
    tx: tokio::sync::mpsc::UnboundedSender<WsOut>,
) {
    let registry = ws_registry();
    let mut registry = registry.lock().unwrap();
    registry.register(id, tx);
    registry.metadata.insert(
        id,
        WsMeta {
            scope: scope.to_string(),
            hibernatable: true,
            tags: Vec::new(),
            attachment: None,
            pending: Vec::new(),
            auto_response_at: None,
            facet: None,
            owner: WsOwner::Cell(scope.to_string()),
            bind_token: None,
        },
    );
}
/// Register an outbound socket of `scope`, held by `owner`. A socket that a
/// facet opened takes its facet record in the same insertion, so no event,
/// close, or `ws_close_facet` can find the entry without it.
pub fn ws_register_outbound(id: u64, scope: &str, facet: Option<FacetSocket>, owner: WsOwner) {
    let inserted = {
        let registry = ws_registry();
        let mut registry = registry.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) = registry.metadata.entry(id) {
            entry.insert(WsMeta {
                scope: scope.to_string(),
                hibernatable: false,
                tags: Vec::new(),
                attachment: None,
                pending: Vec::new(),
                auto_response_at: None,
                facet,
                owner,
                bind_token: None,
            });
            true
        } else {
            false
        }
    };
    if inserted {
        increment_regular_ws(scope);
    }
}
pub fn ws_unregister(id: u64) {
    let meta = ws_registry().lock().unwrap().unregister(id);
    if let Some(meta) = meta.filter(|meta| !meta.hibernatable) {
        decrement_regular_ws(&meta.scope);
    }
}

/// Which channel a frame on this socket leaves by.
///
/// A hibernatable transport is `WsHibernatable`: the host pushes its messages
/// into the cell and captures what the handler emits. A turn the handler
/// suspends in releases its own batch on a ticket of its own, and the turn the
/// handler answers in releases its batch from the cell's barrier queue.
///
/// A socket the isolate opened and polls itself is `WsSelf`, and the two are
/// separate channels in the model for a concrete reason. That handler runs
/// inside the isolate's event loop, and that loop is what a captured frame
/// would be waiting on: the reply to a frame the gate is withholding never
/// arrives, so the loop never finishes, so the frame is never released. It
/// takes a durability ticket on the host runtime instead, which is a different
/// way of being held rather than not being held at all.
pub(crate) fn ws_channel(id: u64) -> cello_logic::Channel {
    let registry = ws_registry();
    let registry = registry.lock().unwrap();
    if registry
        .metadata
        .get(&id)
        .is_some_and(|meta| meta.hibernatable)
    {
        cello_logic::Channel::WsHibernatable
    } else {
        cello_logic::Channel::WsSelf
    }
}

/// The frames of one flush inside a socket's ordered queue.
///
/// A segment rather than a flat list of frames, because every flush holds a
/// gate ticket of its own. A later flush's frames can trail a write that an
/// earlier flush's ticket does not cover, so releasing a socket's whole queue
/// on the earlier verdict would send them before their own proof arrived. The
/// queue therefore drains its settled prefix and stops.
struct WsSegment {
    /// Names this segment to the flush that owns it. The position in the
    /// queue cannot: an earlier segment can drain first and move it.
    token: u64,
    frames: Vec<WsOut>,
    /// What the gate answered for this segment, once its flush has an answer.
    /// A segment with no owning flush carries `Ok` from the start: the frames
    /// are already released and only wait for the order in front of them.
    settled: Option<Result<(), String>>,
}

/// The frames one instance holds behind its output gates, and the flushes
/// that own them.
///
/// The fields are one invariant, so they are one value: a socket has a queue
/// exactly while the flushes counted in `flushes` will drain it, and a count
/// that falls must notify `done`. Held together, no caller can take the queue
/// of one instance of the services and the count of another.
#[derive(Default)]
pub(crate) struct WsFlushState {
    /// Frames held back because the handler that produced them has written
    /// something not yet durable, one queue of segments per socket.
    ///
    /// Every later frame for a socket that already holds frames joins the
    /// queue rather than overtaking it: a socket's frames must arrive in the
    /// order the script sent them. Ordering is a property of one socket,
    /// which is what the key says.
    ///
    /// Owned by the services, not by a thread. It is filled inside the
    /// isolate and drained by an op, and an op runs on the host runtime — so
    /// a thread-local queue was filled on one thread and taken, empty, on
    /// another. The frames never left the process, and because the queue
    /// stayed non-empty every later frame joined them.
    deferred: std::sync::Mutex<HashMap<u64, std::collections::VecDeque<WsSegment>>>,
    /// How many flushes still hold frames for a socket. A count, not a flag:
    /// one flush can finish and drain its segment while a later frame starts
    /// another.
    flushes: std::sync::Mutex<HashMap<u64, usize>>,
    /// Names the next segment. Monotonic within one instance's services, so
    /// a token identifies a segment for as long as any flush can name it.
    next_token: AtomicU64,
    done: tokio::sync::Notify,
}

impl WsFlushState {
    /// Send `frames` on their sockets, or hold each behind its socket's gate.
    ///
    /// One call is one flush: every socket the batch touches takes at most
    /// one segment, so a broadcast that one gate ticket covers costs one
    /// ticket rather than one for each client.
    ///
    /// `gated` is the core's answer for this batch's event. A socket that
    /// already holds frames joins them whatever that answer is, so a later
    /// frame cannot overtake an earlier one. An ungated batch therefore joins
    /// as a segment that is settled from the start, and a gated one joins as
    /// a segment this flush must release.
    ///
    /// Returns the guard for the flush this batch started, and only then, so
    /// the caller cannot leave a held segment with no flush behind it and
    /// cannot await a gate that holds nothing. The counts are taken while the
    /// queue lock is held: a teardown that reads them must not observe a
    /// queued frame with no flush behind it.
    pub(crate) fn emit_or_defer(
        self: &Arc<Self>,
        registry: &std::sync::Mutex<WsRegistry>,
        frames: impl IntoIterator<Item = (u64, WsOut)>,
        gated: bool,
    ) -> Option<WsFlushGuard> {
        // Held until every frame is either queued or sent. A flush runs on
        // another thread, so releasing between the check and the send would
        // let it drain its queue in between, putting a later frame ahead of
        // an earlier one.
        let mut deferred = self.deferred.lock().unwrap();
        let mut direct = Vec::new();
        let mut mine = std::collections::HashSet::new();
        let mut segments = Vec::new();
        for (id, out) in frames {
            if mine.contains(&id) {
                deferred
                    .get_mut(&id)
                    .and_then(std::collections::VecDeque::back_mut)
                    .expect("a socket this batch queued still holds its segment")
                    .frames
                    .push(out);
                continue;
            }
            let token = self.next_token.fetch_add(1, Ordering::Relaxed);
            match deferred.entry(id) {
                std::collections::hash_map::Entry::Vacant(_) if !gated => direct.push((id, out)),
                entry => {
                    entry.or_default().push_back(WsSegment {
                        token,
                        frames: vec![out],
                        settled: (!gated).then_some(Ok(())),
                    });
                    mine.insert(id);
                    if gated {
                        segments.push((id, token));
                    }
                }
            }
        }
        let flushing = (!segments.is_empty()).then(|| {
            let mut flushes = self.flushes.lock().unwrap();
            for (id, _) in &segments {
                *flushes.entry(*id).or_default() += 1;
            }
            drop(flushes);
            WsFlushGuard {
                state: self.clone(),
                segments,
            }
        });
        // Still under the queue lock: a frame this batch sends directly has
        // no queue on its socket, and a flush that acquired the lock first
        // cannot install one between the check above and this send.
        emit_frames(registry, direct);
        flushing
    }

    /// Reserve one captured frame's place in its socket's queue, inside the
    /// batch's single flush.
    ///
    /// The frame joins the flush's own segment while that segment is still
    /// the socket's tail, so a handler that sends many frames, or broadcasts
    /// one frame to many sockets, holds one segment per socket, as a batch
    /// through `emit_or_defer` does. A segment for each frame would cost a
    /// queue lock, a count, and a waiter wake-up for each frame at release.
    /// A frame that another event queued in between ends that segment, and
    /// this frame opens a new one behind it, so send order still holds.
    fn capture(
        self: &Arc<Self>,
        flush: &mut Option<WsFlushGuard>,
        tails: &mut HashMap<u64, u64>,
        id: u64,
        out: WsOut,
    ) {
        // One queue lock covers the tail check and the count, as in
        // `emit_or_defer`: a teardown must not observe a queued frame with no
        // flush behind it.
        let mut deferred = self.deferred.lock().unwrap();
        let queue = deferred.entry(id).or_default();
        let own_tail = tails
            .get(&id)
            .and_then(|token| queue.back_mut().filter(|segment| segment.token == *token));
        if let Some(segment) = own_tail {
            segment.frames.push(out);
            return;
        }
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        queue.push_back(WsSegment {
            token,
            frames: vec![out],
            settled: None,
        });
        *self.flushes.lock().unwrap().entry(id).or_default() += 1;
        tails.insert(id, token);
        flush
            .get_or_insert_with(|| WsFlushGuard {
                state: self.clone(),
                segments: Vec::new(),
            })
            .segments
            .push((id, token));
    }

    /// Wait until no flush holds frames for `id` any more.
    async fn await_flushes(&self, id: u64) {
        loop {
            // Registered before the count is read. A flush that finishes in
            // between must find a waiter to wake, or this parks forever on a
            // notification that already happened.
            let notified = self.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.flushes.lock().unwrap().contains_key(&id) {
                return;
            }
            notified.await;
        }
    }

    /// Wait until no flush of this instance still owns deferred frames.
    async fn await_all_flushes(&self) {
        loop {
            // Register before observing the map, exactly as the per-socket
            // wait does. The final guard can otherwise notify between the
            // empty check and waiter registration, leaving shutdown parked
            // forever.
            let notified = self.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.flushes.lock().unwrap().is_empty() {
                return;
            }
            notified.await;
        }
    }
}

/// The flush state of the services the caller runs under.
///
/// A flush task outlives the dispatch that spawned it and runs on a runtime
/// with no services of its own, so every caller that hands work to one
/// resolves the state here first and moves it in. Resolving inside the task
/// would answer from whichever instance that runtime reaches, which is the
/// process-wide sharing this state exists to end.
pub(crate) fn ws_flush_state() -> Arc<WsFlushState> {
    asyncrt::services().websockets().flush.clone()
}

/// One held segment and the flush that owns it, for a test that stands in for
/// the durability ticket. Holding the guard is what keeps the socket's
/// teardown wait honest while the queue exists.
#[cfg(cello_internal_tests)]
#[doc(hidden)]
pub struct WsHeldFlush(WsFlushGuard);

/// Hold `out` behind `id`'s gate exactly as a gated frame does, and return
/// the flush that holds it.
#[cfg(cello_internal_tests)]
#[doc(hidden)]
pub fn ws_defer_frame(id: u64, out: WsOut) -> WsHeldFlush {
    let flush = ws_flush_state();
    let registry = ws_registry();
    WsHeldFlush(
        flush
            .emit_or_defer(&registry, std::iter::once((id, out)), true)
            .expect("a gated frame always opens a segment of its own"),
    )
}

/// How many frames this instance holds for `id`.
#[cfg(cello_internal_tests)]
#[doc(hidden)]
pub fn ws_deferred_count(id: u64) -> usize {
    ws_flush_state()
        .deferred
        .lock()
        .unwrap()
        .get(&id)
        .map_or(0, |queue| {
            queue.iter().map(|segment| segment.frames.len()).sum()
        })
}

/// Release a held flush as a settled durability ticket does, then count it
/// out. Consuming the guard is what makes the two happen together.
#[cfg(cello_internal_tests)]
#[doc(hidden)]
pub fn ws_release_flush(held: WsHeldFlush) {
    let registry = ws_registry();
    held.0.release(&registry, Ok(()));
}

/// Answer a held flush as a refused durability ticket does.
#[cfg(cello_internal_tests)]
#[doc(hidden)]
pub fn ws_refuse_flush(held: WsHeldFlush) {
    let registry = ws_registry();
    held.0
        .release(&registry, Err("refused for a test".to_string()));
}

/// The segments one flush holds, and the counts that name it to a teardown.
///
/// The two travel together because either alone is wrong: a segment with no
/// count lets a socket tear down over frames that have not left, and a count
/// with no segment parks that teardown forever.
///
/// The guard counts itself out on every exit path, including the ones that
/// run no code: a flush spawned onto a runtime that is already shutting down
/// is dropped unpolled, and a panic unwinds through it. It cannot help a
/// flush that is merely parked -- nothing drops, so nothing runs -- which is
/// what the teardown wait is bounded for.
///
/// It carries the state rather than resolving it on drop, because the drop
/// can run on a runtime that answers with another instance, or with none.
pub(crate) struct WsFlushGuard {
    state: Arc<WsFlushState>,
    /// The segment this flush owns on each socket the batch reached, as
    /// (socket, token).
    segments: Vec<(u64, u64)>,
}

impl WsFlushGuard {
    fn discard(self, registry: &std::sync::Mutex<WsRegistry>) {
        {
            let mut deferred = self.state.deferred.lock().unwrap();
            for (id, token) in &self.segments {
                if let Some(segment) = deferred
                    .get_mut(id)
                    .and_then(|queue| queue.iter_mut().find(|segment| segment.token == *token))
                {
                    segment.frames.clear();
                }
            }
        }
        self.release(registry, Ok(()));
    }

    /// Give the gate's verdict to every segment this flush holds, then
    /// deliver what each socket can now release.
    ///
    /// `held` is what the gate answered. `Err` means the frames describe a
    /// write the fleet may never have, so they must not be delivered.
    /// Dropping them and leaving the socket open is not an option -- a
    /// WebSocket is an ordered stream, and a peer cannot see a hole in one.
    /// It would read the frames on either side of the gap as consecutive.
    ///
    /// Close instead. A truncated stream is something the peer can detect and
    /// resynchronise from; a silently incomplete one is not. The cell is
    /// reset underneath this as well, but that is a separate path and this
    /// must not depend on its timing.
    ///
    /// Takes the guard by value, so the count this flush holds falls once its
    /// frames have left and never before.
    pub(crate) fn release(self, registry: &std::sync::Mutex<WsRegistry>, held: Result<(), String>) {
        // Keep the lock from the verdict through the registry send. Once a
        // queue is drained away, another release would otherwise look direct
        // and overtake these frames.
        let mut deferred = self.state.deferred.lock().unwrap();
        let mut sending = Vec::new();
        for (id, token) in &self.segments {
            if let Some(segment) = deferred
                .get_mut(id)
                .and_then(|queue| queue.iter_mut().find(|segment| segment.token == *token))
            {
                segment.settled = Some(held.clone());
            }
            drain_settled(&mut deferred, *id, &mut sending);
        }
        emit_frames(registry, sending);
    }
}

/// Take the frames a socket's queue can release: every segment up to the
/// first one whose gate has not answered.
///
/// A segment the gate refused ends the socket instead. Everything queued
/// behind it trails the same unproven write, so it is dropped with the
/// segment rather than sent as though the gap were not there.
fn drain_settled(
    deferred: &mut HashMap<u64, std::collections::VecDeque<WsSegment>>,
    id: u64,
    sending: &mut Vec<(u64, WsOut)>,
) {
    let Some(queue) = deferred.get_mut(&id) else {
        return;
    };
    let mut refused = false;
    while let Some(segment) = queue.front() {
        match &segment.settled {
            None => break,
            Some(Err(_)) => {
                refused = true;
                break;
            }
            Some(Ok(())) => {
                let segment = queue.pop_front().expect("the front segment is present");
                sending.extend(segment.frames.into_iter().map(|frame| (id, frame)));
            }
        }
    }
    if refused {
        deferred.remove(&id);
        sending.push((
            id,
            WsOut::Close(
                1011,
                "cello could not prove the write behind this message durable".to_string(),
            ),
        ));
    } else if queue.is_empty() {
        deferred.remove(&id);
    }
}

impl Drop for WsFlushGuard {
    fn drop(&mut self) {
        {
            let mut flushes = self.state.flushes.lock().unwrap();
            for (id, _) in &self.segments {
                let Some(count) = flushes.get_mut(id) else {
                    continue;
                };
                *count -= 1;
                if *count == 0 {
                    flushes.remove(id);
                }
            }
        }
        self.state.done.notify_waiters();
    }
}

/// Wait until no flush holds frames for `id` any more.
///
/// The socket's own teardown calls this. Closing reads the handler's output
/// with a non-blocking drain and, finding none, answers the peer with the
/// protocol echo of the close the peer itself sent -- so a close frame still
/// behind the gate is not merely late, it is replaced. The wait is bounded by
/// the gate: a cell with no barrier answers at once, a barrier settles when
/// its proof does, and an unprovable one still resolves the flush through its
/// fail-closed arm.
pub async fn ws_await_flushes(id: u64) {
    ws_flush_state().await_flushes(id).await;
}

/// Wait until no WebSocket flush of these services still owns deferred
/// frames.
///
/// Shutdown calls this after the cell handoff has settled every output gate
/// and before connection tasks can unregister their sockets. Waiting per
/// socket cannot enforce that ordering because socket teardown deliberately
/// skips its gate wait once the node is draining.
#[doc(hidden)]
pub async fn ws_await_all_flushes() {
    ws_flush_state().await_all_flushes().await;
}

/// Hand frames that cross no queue to the registry, under one lock.
///
/// The registry is a parameter rather than a lookup: a flush runs on a
/// runtime with no services of its own, so it must send to the instance that
/// owns the socket instead of to whichever instance that runtime reaches.
fn emit_frames(
    registry: &std::sync::Mutex<WsRegistry>,
    frames: impl IntoIterator<Item = (u64, WsOut)>,
) {
    let mut frames = frames.into_iter().peekable();
    if frames.peek().is_none() {
        return;
    }
    let mut registry = registry.lock().unwrap();
    for (id, out) in frames {
        registry.emit(id, out);
    }
}

/// Release a batch into each socket's ordered stream. A socket whose
/// durability-ticket queue is still present joins that queue, so an Actor
/// barrier release cannot overtake an earlier frame on the same socket.
pub fn ws_emit_batch(frames: impl Into<WsBatch>) {
    frames.into().release(Ok(()));
}

/// Close one socket. A forced generation swap closes the regular and
/// outbound sockets that pin a cell to its old isolate and nothing else: the
/// cell stays on this node, so its hibernatable sockets and auto-response
/// pair survive, which `ws_close_scope` would take with it.
pub fn ws_close(id: u64, code: u16, reason: &str) {
    ws_registry()
        .lock()
        .unwrap()
        .emit(id, WsOut::Close(code, reason.to_string()));
}

/// Break a cell's sockets: the output gate could not prove a write durable, so
/// close every socket the cell owns rather than let a client keep a connection
/// whose acknowledged effects may not have persisted (a reset DO).
///
/// The sockets of the cell's facets close too. A facet's writes trail the
/// root cell's gate, and the facet does not outlive its root.
pub fn ws_close_scope(scope: &str, code: u16, reason: &str) {
    let mut scopes = vec![scope.to_string()];
    {
        let registry = ws_registry();
        let mut registry = registry.lock().unwrap();
        let ids: Vec<u64> = registry
            .metadata
            .iter()
            .filter(|(_, meta)| {
                let facet_of_scope = meta.facet.as_ref().is_some_and(|facet| facet.root == scope);
                if facet_of_scope && !scopes.contains(&meta.scope) {
                    scopes.push(meta.scope.clone());
                }
                meta.scope == scope || facet_of_scope
            })
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            registry.emit(id, WsOut::Close(code, reason.to_string()));
        }
    }
    // A reset cell is a new actor; workerd's hibernation manager dies with
    // the old one and takes the auto-response pair with it.
    let pairs = ws_auto_responses();
    let mut pairs = pairs.lock().unwrap();
    for scope in &scopes {
        pairs.remove(scope);
    }
}

/// Close the sockets of a facet that stopped: an abort, a delete, or its
/// root's give-back.
///
/// No later event can reach the facet, and a facet restarted under the same
/// name is a new actor that never accepted these sockets. Leaving them open
/// would accept frames that no handler can receive. 1001 is the code for an
/// endpoint that is going away.
pub(crate) fn ws_close_facet(scope: &str, reason: &str) {
    ws_auto_responses().lock().unwrap().remove(scope);
    {
        let registry = ws_registry();
        let mut registry = registry.lock().unwrap();
        let ids: Vec<u64> = registry
            .metadata
            .iter()
            .filter(|(_, meta)| meta.scope == scope && meta.facet.is_some())
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            registry.emit(id, WsOut::Close(1001, reason.to_string()));
        }
    }
    // The facet's own pull endpoints die with it: no later event can reach
    // a stopped facet, so queued inbound frames are garbage and the
    // endpoints would otherwise leak like the abandoned binds above.
    ws_pull_unregister_cell(scope);
}

#[cfg(test)]
mod join_handoff_tests {
    use super::*;

    /// The handoff map is process-global and shared by every unit test in
    /// this binary: a test that stashes holds this while any stash entry or
    /// registry metadata of its is still outstanding, so exact counts and
    /// takes below never observe a sibling test's entries.
    static STASH_TEST_SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn handoff_worker(id: u64) -> WorkerWebSocket {
        WorkerWebSocket {
            id,
            inbound: ws_pull_channel().0,
            keep_pull: false,
        }
    }

    /// A bind tracks both ids for retirement only when request-owned: a
    /// Durable Object caller owns its sockets through the cell, so tracking
    /// the joined target under a cell bind would close the peer when the
    /// request ends. Reverting to unconditional tracking fails this.
    #[test]
    fn bind_tracks_joined_target_only_for_request_binds() {
        use crate::js::websocket::track_bind_for_retirement;
        let sockets = std::sync::Mutex::new(Vec::new());
        track_bind_for_retirement(&sockets, 7, 8, "");
        assert_eq!(
            *sockets.lock().unwrap(),
            vec![7, 8],
            "a request-owned bind tracks both ids"
        );
        let sockets = std::sync::Mutex::new(Vec::new());
        track_bind_for_retirement(&sockets, 7, 8, "cell-scope");
        assert!(
            sockets.lock().unwrap().is_empty(),
            "a cell-owned bind tracks neither id"
        );
    }

    /// Stash `id` as a handoff owned by `scope` at `now_ms`, the way
    /// `joinable_worker_target` only ever stashes a registered socket: the
    /// metadata lookup at stash time is what attributes the entry. Taking,
    /// displacing, or sweeping the stash drops the worker, which unregisters
    /// the id again, so no metadata outlives the test. Times are explicit
    /// millisecond stamps — no clock reads, no sleeps — so expiry and
    /// displacement stay deterministic. The quota deployment matches the
    /// scope, the way a single deployment's sockets share both.
    fn stash_scoped_at(id: u64, scope: &str, now_ms: u64) {
        ws_registry().lock().unwrap().metadata.insert(
            id,
            WsMeta {
                scope: scope.to_string(),
                hibernatable: false,
                tags: Vec::new(),
                attachment: None,
                pending: Vec::new(),
                auto_response_at: None,
                facet: None,
                owner: WsOwner::Cell(scope.to_string()),
                bind_token: None,
            },
        );
        ws_set_quota_key(
            id,
            WsQuotaKey {
                deployment: scope.to_string(),
                scope: Some(scope.to_string()),
            },
        );
        ws_stash_join_handoff_at(handoff_worker(id), now_ms);
    }

    fn scoped_count(scope: &str) -> usize {
        isolate_join_handoffs()
            .lock()
            .unwrap()
            .values()
            .filter(|handoff| handoff.quota.deployment == scope)
            .count()
    }

    /// Unjoined handoffs are process-bounded: stashing past a scope's share
    /// evicts that scope's own oldest entry, and re-stashing an id replaces
    /// instead of growing the map. Ids ride far above any socket the other
    /// unit tests mint, and every stash is taken back, so the map is empty
    /// on the way out. Tests that stash serialize on `STASH_TEST_SERIAL`,
    /// so the counts below are exact.
    #[test]
    fn join_handoffs_are_bounded_and_replaceable() {
        let base: u64 = 1 << 60;
        let _serial = STASH_TEST_SERIAL.lock().unwrap();
        let t0: u64 = 1_000_000;
        // Fill the process-wide bound with full per-scope shares. A further
        // stash from any full scope evicts that scope's own oldest entry,
        // never another scope's.
        for scope in 0..MAX_JOIN_HANDOFFS / MAX_JOIN_HANDOFFS_PER_SCOPE {
            let name = format!("scope-{scope}");
            for index in 0..MAX_JOIN_HANDOFFS_PER_SCOPE {
                stash_scoped_at(base + scope as u64 * 256 + index as u64, &name, t0);
            }
        }
        assert_eq!(
            isolate_join_handoffs().lock().unwrap().len(),
            MAX_JOIN_HANDOFFS,
            "the stash must stay process-bounded",
        );
        // Scope 0 overflows its share: its own oldest goes, and every other
        // scope's entries still join. Sequence order makes this
        // deterministic: `base` went in first, so it is the eviction.
        let overflow = base + 10_000;
        stash_scoped_at(overflow, "scope-0", t0);
        assert!(
            ws_take_join_handoff(base).is_none(),
            "the scope's own oldest handoff must have been evicted",
        );
        assert!(
            ws_take_join_handoff(overflow).is_some(),
            "the overflowing stash must still join",
        );
        for scope in 1..MAX_JOIN_HANDOFFS / MAX_JOIN_HANDOFFS_PER_SCOPE {
            for index in 0..MAX_JOIN_HANDOFFS_PER_SCOPE {
                assert!(
                    ws_take_join_handoff(base + scope as u64 * 256 + index as u64).is_some(),
                    "another scope's handoff must survive the eviction",
                );
            }
        }
        for index in 1..MAX_JOIN_HANDOFFS_PER_SCOPE {
            assert!(
                ws_take_join_handoff(base + index as u64).is_some(),
                "a retained handoff must still join",
            );
        }
        assert!(isolate_join_handoffs().lock().unwrap().is_empty());

        // Re-stashing an id replaces its entry instead of growing the map.
        let id = base + MAX_JOIN_HANDOFFS as u64 + 1;
        stash_scoped_at(id, "scope-replace", t0);
        stash_scoped_at(id, "scope-replace", t0);
        assert_eq!(isolate_join_handoffs().lock().unwrap().len(), 1);
        assert!(ws_take_join_handoff(id).is_some());
        assert!(ws_take_join_handoff(id).is_none());
        assert!(isolate_join_handoffs().lock().unwrap().is_empty());
    }

    /// Two full shares, then one scope overflows: the overflowing scope
    /// evicts its own oldest entry, never the other full scope's. Scope A
    /// fills its share first, so a cross-scope pick would take A's oldest;
    /// the overflow must still recycle B's own oldest instead.
    #[test]
    fn join_handoff_full_scope_overflow_evicts_only_its_own_scope() {
        let _serial = STASH_TEST_SERIAL.lock().unwrap();
        let base: u64 = (1 << 59) + (1 << 53);
        let t0: u64 = 4_000_000;
        for index in 0..MAX_JOIN_HANDOFFS_PER_SCOPE {
            stash_scoped_at(base + index as u64, "scope-a", t0);
        }
        for index in 0..MAX_JOIN_HANDOFFS_PER_SCOPE {
            stash_scoped_at(base + 1024 + index as u64, "scope-b", t0 + 1_000);
        }
        // B overflows its share: its own oldest goes, and every one of
        // A's older entries still join.
        let overflow = base + 2048;
        stash_scoped_at(overflow, "scope-b", t0 + 2_000);
        assert!(
            ws_take_join_handoff(base + 1024).is_none(),
            "the overflowing scope's own oldest handoff must have been evicted",
        );
        assert!(
            ws_take_join_handoff(overflow).is_some(),
            "the overflowing stash must still join",
        );
        for index in 0..MAX_JOIN_HANDOFFS_PER_SCOPE {
            assert!(
                ws_take_join_handoff(base + index as u64).is_some(),
                "the other full scope's handoff must survive the overflow",
            );
        }
        for index in 1..MAX_JOIN_HANDOFFS_PER_SCOPE {
            assert!(
                ws_take_join_handoff(base + 1024 + index as u64).is_some(),
                "the overflowing scope's retained handoff must still join",
            );
        }
        assert!(isolate_join_handoffs().lock().unwrap().is_empty());
    }

    /// One tenant's 101 churn never evicts another tenant's pending join:
    /// past its own share a scope only recycles its own oldest handoff. The
    /// victim's stash lands first and still takes after the flooder fills
    /// and churns its whole share on top.
    #[test]
    fn join_handoff_flood_evicts_only_its_own_scope() {
        let _serial = STASH_TEST_SERIAL.lock().unwrap();
        let flooder: u64 = 1 << 59;
        let victim: u64 = (1 << 59) + (1 << 40);
        let t0: u64 = 2_000_000;
        stash_scoped_at(victim, "scope-victim", t0);
        for index in 0..MAX_JOIN_HANDOFFS_PER_SCOPE + 8 {
            stash_scoped_at(flooder + index as u64, "scope-flood", t0);
        }
        assert_eq!(
            scoped_count("scope-flood"),
            MAX_JOIN_HANDOFFS_PER_SCOPE,
            "the flooder must hold exactly its own share",
        );
        assert!(
            ws_take_join_handoff(victim).is_some(),
            "the victim's pending join must survive the flood",
        );
        // The flood recycled its own oldest entries: the first 8 are gone,
        // the newest share still joins, and the map drains empty.
        for index in 0..8 {
            assert!(
                ws_take_join_handoff(flooder + index as u64).is_none(),
                "the flooder's own oldest handoff must have been evicted",
            );
        }
        for index in 8..MAX_JOIN_HANDOFFS_PER_SCOPE + 8 {
            assert!(
                ws_take_join_handoff(flooder + index as u64).is_some(),
                "the flooder's retained handoff must still join",
            );
        }
        assert!(isolate_join_handoffs().lock().unwrap().is_empty());
    }

    /// A full map still admits a scope under its share: the stash displaces
    /// the oldest entry of a scope at its share instead of refusing, so an
    /// abandoned full map never blockades other scopes. The victim's own
    /// pending join then takes, and only the displaced entry is gone.
    #[test]
    fn join_handoff_displaces_an_over_share_scope() {
        let _serial = STASH_TEST_SERIAL.lock().unwrap();
        let base: u64 = (1 << 59) + (1 << 50);
        let t0: u64 = 3_000_000;
        for scope in 0..MAX_JOIN_HANDOFFS / MAX_JOIN_HANDOFFS_PER_SCOPE {
            let name = format!("scope-{scope}");
            for index in 0..MAX_JOIN_HANDOFFS_PER_SCOPE {
                stash_scoped_at(base + scope as u64 * 1024 + index as u64, &name, t0);
            }
        }
        assert_eq!(
            isolate_join_handoffs().lock().unwrap().len(),
            MAX_JOIN_HANDOFFS,
            "the stash must stay process-bounded",
        );
        // A scope with nothing stashed still has headroom, so its newcomer
        // displaces another scope's oldest instead of being refused. All
        // entries share one timestamp, so the smallest sequence — scope 0's
        // first stash — is the displacement.
        let victim = base + (1 << 40);
        stash_scoped_at(victim, "scope-new", t0 + 1_000);
        assert_eq!(
            isolate_join_handoffs().lock().unwrap().len(),
            MAX_JOIN_HANDOFFS,
            "displacement must not grow the map",
        );
        assert!(
            ws_take_join_handoff(victim).is_some(),
            "the under-share stash must still join",
        );
        assert!(
            ws_take_join_handoff(base).is_none(),
            "only the over-share scope's oldest handoff must yield",
        );
        for index in 1..MAX_JOIN_HANDOFFS_PER_SCOPE {
            assert!(
                ws_take_join_handoff(base + index as u64).is_some(),
                "the displaced scope's retained handoffs must still join",
            );
        }
        for scope in 1..MAX_JOIN_HANDOFFS / MAX_JOIN_HANDOFFS_PER_SCOPE {
            for index in 0..MAX_JOIN_HANDOFFS_PER_SCOPE {
                assert!(
                    ws_take_join_handoff(base + scope as u64 * 1024 + index as u64).is_some(),
                    "an untouched scope's handoff must still join",
                );
            }
        }
        assert!(isolate_join_handoffs().lock().unwrap().is_empty());
    }

    /// When every scope is under its share — 1024 scopes with one pending
    /// join each — a further stash displaces the globally oldest entry, so
    /// the map still admits newcomers instead of blockading.
    #[test]
    fn join_handoff_displaces_globally_oldest_when_all_scopes_share_room() {
        let _serial = STASH_TEST_SERIAL.lock().unwrap();
        let base: u64 = (1 << 59) + (1 << 51);
        let t0: u64 = 3_000_000;
        for scope in 0..MAX_JOIN_HANDOFFS {
            stash_scoped_at(base + scope as u64, &format!("scope-{scope}"), t0);
        }
        assert_eq!(
            isolate_join_handoffs().lock().unwrap().len(),
            MAX_JOIN_HANDOFFS,
            "the stash must stay process-bounded",
        );
        // One timestamp for every entry, so the smallest sequence — scope
        // 0's single stash — is the globally oldest.
        let victim = base + MAX_JOIN_HANDOFFS as u64 + 7;
        stash_scoped_at(victim, "scope-new", t0 + 1_000);
        assert!(
            ws_take_join_handoff(victim).is_some(),
            "the newcomer must still join",
        );
        assert!(
            ws_take_join_handoff(base).is_none(),
            "only the globally oldest handoff must yield",
        );
        for scope in 1..MAX_JOIN_HANDOFFS {
            assert!(
                ws_take_join_handoff(base + scope as u64).is_some(),
                "every other scope's handoff must still join",
            );
        }
        assert!(isolate_join_handoffs().lock().unwrap().is_empty());
    }

    /// Entries older than the age bound are swept on the next stash, so
    /// abandoned handoffs cannot accumulate: a lone expired entry is gone
    /// after one further stash, and the map holds only the live entry.
    #[test]
    fn join_handoff_sweeps_expired_handoffs() {
        let _serial = STASH_TEST_SERIAL.lock().unwrap();
        let base: u64 = (1 << 59) + (1 << 52);
        let t0: u64 = 3_000_000;
        stash_scoped_at(base, "scope-old", t0);
        stash_scoped_at(base + 1, "scope-new", t0 + MAX_JOIN_HANDOFF_AGE_MS + 1_000);
        assert_eq!(
            isolate_join_handoffs().lock().unwrap().len(),
            1,
            "the expired entry must have been swept",
        );
        assert!(
            ws_take_join_handoff(base).is_none(),
            "an expired handoff must never join",
        );
        assert!(
            ws_take_join_handoff(base + 1).is_some(),
            "the live handoff must still join",
        );
        assert!(isolate_join_handoffs().lock().unwrap().is_empty());
    }
    /// Stash `id` for `deployment` with an empty routing scope, exactly like
    /// a loaded-worker handoff: no cell scope applies, so only the deployment
    /// distinguishes its quota share.
    fn stash_deployment_at(id: u64, deployment: &str, now_ms: u64) {
        ws_registry().lock().unwrap().metadata.insert(
            id,
            WsMeta {
                scope: String::new(),
                hibernatable: false,
                tags: Vec::new(),
                attachment: None,
                pending: Vec::new(),
                auto_response_at: None,
                facet: None,
                owner: WsOwner::Cell(String::new()),
                bind_token: None,
            },
        );
        ws_set_quota_key(
            id,
            WsQuotaKey {
                deployment: deployment.to_string(),
                scope: None,
            },
        );
        ws_stash_join_handoff_at(handoff_worker(id), now_ms);
    }

    /// Quota shares follow the preparing deployment, not the routing scope:
    /// two deployments whose sockets all route under the empty scope still
    /// hold disjoint 64-entry shares, so one tenant's churn can never evict
    /// another's pending join. Keying by routing scope alone would land both
    /// deployments in one bucket and evict the first deployment's oldest
    /// entry on the second deployment's very first stash.
    #[test]
    fn join_handoff_quota_is_per_deployment_not_routing_scope() {
        let _serial = STASH_TEST_SERIAL.lock().unwrap();
        let base_a: u64 = (1 << 59) + (1 << 45);
        let base_b: u64 = (1 << 59) + (1 << 46);
        let t0: u64 = 9_000_000;
        for index in 0..MAX_JOIN_HANDOFFS_PER_SCOPE {
            stash_deployment_at(base_a + index as u64, "deployment-a", t0);
            stash_deployment_at(base_b + index as u64, "deployment-b", t0);
        }
        assert_eq!(
            isolate_join_handoffs().lock().unwrap().len(),
            2 * MAX_JOIN_HANDOFFS_PER_SCOPE,
            "two deployments must hold disjoint shares",
        );
        // B overflows its share: its own oldest goes, and every one of A's
        // entries still joins.
        let overflow = base_b + 10_000;
        stash_deployment_at(overflow, "deployment-b", t0);
        assert!(
            ws_take_join_handoff(base_a).is_some(),
            "another deployment's pending join must survive the overflow",
        );
        assert!(
            ws_take_join_handoff(base_b).is_none(),
            "the overflowing deployment's own oldest handoff must have been evicted",
        );
        assert!(
            ws_take_join_handoff(overflow).is_some(),
            "the overflowing stash must still join",
        );
        for index in 1..MAX_JOIN_HANDOFFS_PER_SCOPE {
            assert!(
                ws_take_join_handoff(base_a + index as u64).is_some(),
                "the other deployment's retained handoff must still join",
            );
            assert!(
                ws_take_join_handoff(base_b + index as u64).is_some(),
                "the overflowing deployment's retained handoff must still join",
            );
        }
        assert!(isolate_join_handoffs().lock().unwrap().is_empty());
    }

    /// The producer budget bounds one direction's output channel: the first
    /// frame past the cap is refused and queues the overload marker behind
    /// the admitted data instead of leaving a silent gap, and no later data
    /// frame is admitted even when releases refill the count, while closes
    /// still enqueue. Reverting the marker to a silent drop leaves the
    /// channel without its terminal, so this fails before the fix.
    #[test]
    fn pipe_budget_bounds_output_and_spares_closes() {
        let id: u64 = 1 << 61;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let budget = WsPipeBudget::new(1024);
        let _ = ws_registry()
            .lock()
            .unwrap()
            .register_budgeted(id, tx, Some(budget.clone()));
        let frame = || WsOut::Binary(vec![0u8; 700]);
        {
            let services = ws_registry();
            let mut registry = services.lock().unwrap();
            registry.emit(id, frame());
            registry.emit(id, frame());
        }
        // The first frame fits the budget; the second passes the cap, so it
        // is refused and the overload marker follows it onto the channel.
        assert!(rx.try_recv().is_ok(), "the first frame fits the budget");
        assert!(
            matches!(rx.try_recv(), Ok(WsOut::Overload)),
            "the first refusal must queue the overload marker"
        );
        // A release must not re-admit data past the terminal refusal, and no
        // second terminal follows the first.
        budget.release(ws_out_bytes(&frame()));
        {
            let services = ws_registry();
            let mut registry = services.lock().unwrap();
            registry.emit(id, frame());
            registry.emit(id, frame());
        }
        assert!(
            rx.try_recv().is_err(),
            "no data is admitted after the overload trip"
        );
        // Closes bypass the data budget so a close stays signalable when
        // the data budget is tripped.
        {
            let services = ws_registry();
            let mut registry = services.lock().unwrap();
            registry.emit(id, WsOut::Close(1000, "bye".into()));
        }
        assert!(
            matches!(rx.try_recv(), Ok(WsOut::Close(1000, _))),
            "a close must enqueue past a tripped budget"
        );
        ws_unregister(id);
        // The trip flag fires once per overload.
        let tripped = WsPipeBudget::new(10);
        assert!(tripped.trip(), "the first trip reports");
        assert!(!tripped.trip(), "a trip reports only once");
    }

    /// The byte budget exists before pre-registration frames are retained: a
    /// flood into a socket whose upgrade is never accepted charges at the
    /// first buffered frame and trips past the cap, so the retained backlog
    /// stays bounded and ends with one in-order overload marker, whose trip
    /// registration carries forward. Buffering without charging (the
    /// pre-fix behavior) retains all 300 frames with no marker, so this
    /// fails before the fix.
    #[test]
    fn pre_registration_backlog_stays_bounded_and_trips() {
        // Pin the execution domain before touching the registry: the shared
        // test runtime installs it once, swapping the fallback services for
        // a fresh instance. This test's emit loop is slow enough to straddle
        // that swap, so without the pin the buffered backlog and the flush
        // below can land on different registries. After this call the domain
        // — and the registry — is fixed for the rest of the process.
        let _ = crate::asyncrt::shared_test_runtime();
        let id: u64 = (1 << 61) + 1;
        {
            let services = ws_registry();
            let mut registry = services.lock().unwrap();
            registry.metadata.insert(
                id,
                WsMeta {
                    scope: String::new(),
                    hibernatable: false,
                    tags: Vec::new(),
                    attachment: None,
                    pending: Vec::new(),
                    auto_response_at: None,
                    facet: None,
                    owner: WsOwner::Cell(String::new()),
                    bind_token: None,
                },
            );
            for _ in 0..300 {
                registry.emit(id, WsOut::Binary(vec![0u8; 64 * 1024]));
            }
        }
        let retained = ws_registry()
            .lock()
            .unwrap()
            .metadata
            .get(&id)
            .map(|meta| meta.pending.len())
            .unwrap_or(0);
        assert!(
            retained < 300,
            "the retained backlog must stay bounded, kept {retained} of 300"
        );
        assert!(
            matches!(
                ws_registry()
                    .lock()
                    .unwrap()
                    .metadata
                    .get(&id)
                    .and_then(|meta| meta.pending.last()),
                Some(WsOut::Overload)
            ),
            "the bounded backlog must end with the overload marker"
        );
        // Registration adopts the tripped budget instead of re-charging, so
        // every buffered frame flushes exactly once and the trip is observed
        // after registration. Re-charging the flush against a fresh budget
        // (or none) drops the backlog here, so this fails before the fix.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        ws_registry()
            .lock()
            .unwrap()
            .register_budgeted(id, tx, Some(WsPipeBudget::new(1024)));
        let mut data = 0;
        loop {
            match rx.try_recv() {
                Ok(WsOut::Binary(_)) => data += 1,
                Ok(WsOut::Overload) => break,
                Ok(_) => panic!("only the buffered backlog and its marker flush"),
                Err(_) => panic!("the overload marker must close the flushed backlog"),
            }
        }
        assert_eq!(
            data,
            retained - 1,
            "the flush must deliver every buffered frame exactly once"
        );
        assert!(rx.try_recv().is_err(), "nothing follows the terminal");
        // The adopted trip survives registration: a live frame past the
        // trip is refused without a second marker. Overwriting the budget at
        // registration (the pre-fix flush) admits it, so this fails before
        // the fix.
        ws_registry()
            .lock()
            .unwrap()
            .emit(id, WsOut::Binary(vec![0u8; 8]));
        assert!(
            rx.try_recv().is_err(),
            "no second terminal follows the trip"
        );
        ws_unregister(id);
    }

    /// A backlog under the cap still arrives in order: buffered frames were
    /// already charged, so registration flushes them directly — data ahead
    /// of the terminal, the terminal last — instead of re-admitting them.
    #[test]
    fn pre_registration_backlog_under_cap_arrives_in_order() {
        let id: u64 = (1 << 61) + 2;
        {
            let services = ws_registry();
            let mut registry = services.lock().unwrap();
            registry.metadata.insert(
                id,
                WsMeta {
                    scope: String::new(),
                    hibernatable: false,
                    tags: Vec::new(),
                    attachment: None,
                    pending: Vec::new(),
                    auto_response_at: None,
                    facet: None,
                    owner: WsOwner::Cell(String::new()),
                    bind_token: None,
                },
            );
            registry.emit(id, WsOut::Text("a".into()));
            registry.emit(id, WsOut::Text("b".into()));
            registry.emit(id, WsOut::Close(1000, "done".into()));
        }
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        ws_registry()
            .lock()
            .unwrap()
            .register_budgeted(id, tx, Some(WsPipeBudget::new(1024)));
        assert!(
            matches!(rx.try_recv(), Ok(WsOut::Text(text)) if text == "a"),
            "the backlog head arrives first"
        );
        assert!(
            matches!(rx.try_recv(), Ok(WsOut::Text(text)) if text == "b"),
            "buffered data keeps its order"
        );
        assert!(
            matches!(rx.try_recv(), Ok(WsOut::Close(1000, _))),
            "the buffered terminal closes the backlog"
        );
        assert!(rx.try_recv().is_err(), "nothing follows the terminal");
        ws_unregister(id);
    }

    /// A burst staged past the cap before registration flushes its overload
    /// marker through a plain `register` unfiltered: the marker reaches the
    /// never-budgeted pipe's channel behind the admitted backlog. That
    /// delivery is the precondition the pump arms must survive — it holds
    /// with and without the arm fix, so the arm test above is the one that
    /// fails before it.
    #[test]
    fn plain_register_flushes_the_overload_marker() {
        // Same domain pin as above: this test's emit loop can straddle the
        // one-time services swap, orphaning its backlog on the fallback.
        let _ = crate::asyncrt::shared_test_runtime();
        let id: u64 = (1 << 61) + 3;
        {
            let services = ws_registry();
            let mut registry = services.lock().unwrap();
            registry.metadata.insert(
                id,
                WsMeta {
                    scope: String::new(),
                    hibernatable: false,
                    tags: Vec::new(),
                    attachment: None,
                    pending: Vec::new(),
                    auto_response_at: None,
                    facet: None,
                    owner: WsOwner::Cell(String::new()),
                    bind_token: None,
                },
            );
            for _ in 0..300 {
                registry.emit(id, WsOut::Binary(vec![0u8; 64 * 1024]));
            }
        }
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        ws_registry().lock().unwrap().register(id, tx);
        let mut data = 0;
        loop {
            match rx.try_recv() {
                Ok(WsOut::Binary(_)) => data += 1,
                Ok(WsOut::Overload) => break,
                Ok(_) => panic!("only the buffered backlog and its marker flush"),
                Err(_) => panic!("the overload marker must flush through a plain register"),
            }
        }
        assert!(data > 0, "the admitted backlog flushes ahead of the marker");
        assert!(rx.try_recv().is_err(), "nothing follows the terminal");
        ws_unregister(id);
    }

    /// Dropping the handoff frees its send state while keeping the pull
    /// endpoint: the output registration is gone, the endpoint stays until
    /// its terminal is consumed, and the last sender's drop ends the queue
    /// instead of leaking it. A `mem::forget` drop leaves a live sender, so
    /// the queue never disconnects and this fails before the fix.
    #[test]
    fn drop_handoff_frees_senders_and_keeps_the_endpoint() {
        let id: u64 = (1 << 61) + 7;
        let (out_tx, _out_rx) = tokio::sync::mpsc::unbounded_channel();
        let (pull_sender, pull_rx) = ws_pull_channel();
        ws_pull_register(id, pull_rx, WsOwner::Cell(String::new()));
        ws_registry().lock().unwrap().outputs.insert(id, out_tx);
        WorkerWebSocket {
            id,
            inbound: pull_sender,
            keep_pull: false,
        }
        .drop_handoff();
        // Output is gone but the endpoint stays for its terminal.
        assert!(
            !ws_registry().lock().unwrap().outputs.contains_key(&id),
            "dropping the handoff unregisters output"
        );
        let queue = ws_pull()
            .lock()
            .unwrap()
            .get(&id)
            .expect("the pull endpoint stays registered")
            .queue
            .clone();
        // The handoff owned the only sender: dropping it disconnects the
        // queue instead of leaking a live sender behind.
        let runtime = crate::asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let mut queue = queue.lock().await;
            assert!(
                matches!(
                    queue.try_recv(),
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
                ),
                "the last sender is dropped, not forgotten"
            );
        });
        ws_pull_unregister(id);
    }

    /// A cell-owned bind's abandoned endpoint is reclaimed by cell shutdown:
    /// bind-time cleanup-scope recording marks the target endpoint for the
    /// cell scope, so the shutdown sweep removes it even though its terminal
    /// was never consumed — while the read owner keeps the preparing
    /// request, so the endpoint stays readable by its original owner after
    /// output metadata is gone. Restoring the re-owner (a `Cell` read
    /// owner) hides the endpoint from its owner outside the cell; dropping
    /// the cleanup match from the sweep leaves it registered. Either revert
    /// fails this before the fix.
    #[test]
    fn cell_shutdown_sweeps_abandoned_cell_endpoint() {
        let id: u64 = (1 << 61) + 21;
        let other: u64 = (1 << 61) + 22;
        let facet_ep: u64 = (1 << 61) + 23;
        let facet_meta: u64 = (1 << 61) + 24;
        let (_held1, rx1) = ws_pull_channel();
        let (_held2, rx2) = ws_pull_channel();
        let (_held3, rx3) = ws_pull_channel();
        // A transferred handoff: the owner names the preparing request, which
        // is already retired by bind time.
        ws_pull_register(id, rx1, WsOwner::Request(std::sync::Weak::new()));
        ws_pull_register(other, rx2, WsOwner::Cell("other-scope".to_string()));
        // A facet-owned endpoint of the same dying root: the scope is a
        // hash, so the sweep discovers it through metadata the way
        // `ws_close_scope` does. Without facet discovery the sweep misses
        // it and this fails before the fix.
        ws_registry().lock().unwrap().metadata.insert(
            facet_meta,
            WsMeta {
                scope: "facet-scope".to_string(),
                hibernatable: false,
                tags: Vec::new(),
                attachment: None,
                pending: Vec::new(),
                auto_response_at: None,
                facet: Some(FacetSocket {
                    root: "cell-s".to_string(),
                    stream: String::new(),
                    epoch: 0,
                    opening: 0,
                }),
                owner: WsOwner::Cell("facet-scope".to_string()),
                bind_token: None,
            },
        );
        ws_pull_register(facet_ep, rx3, WsOwner::Cell("facet-scope".to_string()));
        // Bind-time cleanup scope for a cell-owned bind: the read owner is
        // untouched, so with no output metadata the owner still names the
        // preparing request — readable by its original owner from outside
        // the cell — while shutdown reclaims it by cleanup scope.
        ws_pull_set_cleanup_scope(id, "cell-s");
        assert!(
            matches!(ws_owner(id), Some(WsOwner::Request(_))),
            "the bind keeps the original read owner"
        );
        assert_eq!(
            ws_pull()
                .lock()
                .unwrap()
                .get(&id)
                .and_then(|entry| entry.cleanup_scope.clone()),
            Some("cell-s".to_string()),
            "the bind records the cell cleanup scope"
        );
        // The terminal is never consumed; the cell shuts down:
        let removed = ws_pull_unregister_cell("cell-s");
        assert_eq!(removed, 2, "the sweep reclaims the endpoint and its facet");
        assert!(
            ws_pull().lock().unwrap().get(&id).is_none(),
            "the abandoned endpoint is gone after shutdown"
        );
        assert!(
            ws_pull().lock().unwrap().get(&facet_ep).is_none(),
            "a facet scope of the dying root is swept too"
        );
        assert!(
            ws_pull().lock().unwrap().get(&other).is_some(),
            "other scopes are untouched by the sweep"
        );
        ws_pull_unregister(other);
        ws_unregister(facet_meta);
        // A facet release reclaims its own endpoints without root shutdown:
        let f2: u64 = (1 << 61) + 25;
        let (_held4, rx4) = ws_pull_channel();
        ws_pull_register(f2, rx4, WsOwner::Cell("facet2-scope".to_string()));
        ws_close_facet("facet2-scope", "test release");
        assert!(
            ws_pull().lock().unwrap().get(&f2).is_none(),
            "a released facet's endpoints do not leak"
        );
    }
}
