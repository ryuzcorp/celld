// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): cross-isolate RPC
// stubs and streams; receiver-held stream lifetime. Apache-2.0.

//! Host-side channels for caller-pushed byte streams (Worker to Durable
//! Object).
//!
//! A Durable Object owns an addressable cell, so a stream it exports is
//! pulled on demand through the stub table. A stateless Worker isolate is
//! not addressable: nothing can route a pull back to it. A caller-owned
//! `ReadableStream` in RPC arguments therefore crosses the other way
//! around: the caller's isolate pumps it into a bounded host-side channel
//! here, and the receiver pulls from the channel.
//!
//! Lifetime: an upload is owned by the sender's request/invocation. The
//! sender's context end or abort terminates it (the pump stops and the
//! receiver observes EOF or a disconnect), the receiver's cancel stops
//! the pump, and sender-isolate retirement terminates whatever the freed
//! heap still owned. The receiver holds its end past the receiving call
//! (a quiet-but-open carrier stays usable however long nothing flows),
//! so the current receiving heap is tracked too, and receiver-isolate
//! retirement cancels whatever the freed heap still held. Both sides
//! are tracked: an entry both left removes itself whatever its
//! terminal, so a partial read (touched, no EOF) or a
//! sender-abort-first ordering cannot strand it. There is no
//! wall-clock expiry.
//!
//! Flow control: the buffer holds at most [`UPLOAD_CHUNK_CAP`] chunks and
//! [`UPLOAD_BYTE_CAP`] bytes; a push beyond either waits for the receiver
//! to pull. Cancellation propagates both ways: receiver cancel rejects
//! the sender's next push (the pump then cancels the origin), and sender
//! termination wakes the receiver's pending pull.
//!
//! Trust: upload ids are process-wide bearer tokens without the stub table's
//! holder binding -- any isolate presenting one can pull, push, or cancel
//! it. There is no exfil channel for one in-tree (markers never surface
//! to tenant scripts, and forwarding moves the stream wrapper rather
//! than the id), so 128-bit unguessability plus uniform errors and
//! counts-only observability contain them, exactly like pre-binding
//! stubs. Reader-hold tracking stays out deliberately: forwarding moves
//! no reader state by design.

use bytes::Bytes;
use celld_logic::isolate::HeapId;
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::OnceLock;

/// Live uploads the whole process may hold. Past this the creation fails
/// closed with a clear error instead of minting.
pub(crate) const UPLOAD_MAX_PER_PROCESS: usize = 8192;

/// The effective upload cap: the default unless `CELLD_UPLOAD_CAP`
/// overrides it (conformance tests boot with a tiny cap; garbage falls
/// back to the default).
pub(crate) fn upload_cap() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        crate::env_vars::optional::<usize>("CELLD_UPLOAD_CAP")
            .ok()
            .flatten()
            .filter(|cap| *cap > 0)
            .unwrap_or(UPLOAD_MAX_PER_PROCESS)
    })
}
/// Buffered chunks per upload before a push waits for a pull.
pub(crate) const UPLOAD_CHUNK_CAP: usize = 4;
/// Buffered bytes per upload before a push waits for a pull.
pub(crate) const UPLOAD_BYTE_CAP: usize = 64 * 1024;
/// The largest single push. The sender slices larger reads; anything
/// bigger is rejected instead of buffered.
pub(crate) const UPLOAD_CHUNK_MAX_BYTES: usize = 64 * 1024;
/// Creation attempts before an id-collision run gives up. At 128 bits
/// one attempt already never collides.
const CREATE_ATTEMPTS: usize = 8;

/// Why an upload creation was refused. Fails the export; mints nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadRejection {
    /// The process already holds [`UPLOAD_MAX_PER_PROCESS`] live uploads.
    ProcessCap,
}

/// How an upload ended. Buffered chunks still drain first; the terminal
/// resolves once the buffer is empty.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UploadTerminal {
    /// The sender closed the origin: the receiver reads EOF.
    Closed,
    /// The receiver cancelled: the receiver reads EOF and the sender's
    /// next push is rejected so its pump stops.
    Cancelled,
    /// The origin errored, or the sender is gone (context end, abort,
    /// isolate retirement): the receiver's pull rejects with a
    /// disconnect after draining what buffered.
    Failed,
}

#[derive(Debug)]
struct UploadInner {
    buffer: VecDeque<Bytes>,
    buffered_bytes: usize,
    terminal: Option<UploadTerminal>,
    /// False once the sender's invocation ended or its isolate retired.
    /// A drained upload with no sender removes itself instead of
    /// lingering for a receiver that already left.
    sender_alive: bool,
    /// True once the receiver left (cancel, dispose, context end). An
    /// upload both sides left removes itself whatever its terminal: a
    /// partial read is done draining, and a disconnect already
    /// resolved. Without this a `Closed` or `Failed` entry that a pull
    /// touched but never drained to EOF would linger forever.
    receiver_gone: bool,
    /// Whether any pull ran. A sender that goes with no pull ever
    /// (a lift that failed before its envelope sent, a revived reader
    /// that never read) removes the upload outright: no engaged
    /// receiver exists to drain it, and a late first pull fails
    /// closed with a disconnect.
    touched: bool,
    /// Whether a pull resolved EOF. The reader is done then: when the
    /// sender goes too, nothing remains for either side.
    eof_observed: bool,
    /// The heap currently holding the receiving end, recorded when the
    /// stream revives there (a forward re-records). The retirement
    /// backstop cancels whatever a freed heap still held, so a stream
    /// the receiver kept past its call cannot strand its entry.
    receiver: Option<HeapId>,
}

/// One caller-pushed stream: its owning sender heap plus the buffered
/// state. Cloned out of the table per operation so no table lock is
/// held across an await. The state lock is a plain mutex held only
/// across microsecond transitions, never across an await (the waits
/// park on [`Notify`] outside it), so the synchronous terminal ops
/// below cannot miss: a `try_lock` there could skip a terminal under
/// contention and hang the waiter it was meant to wake.
#[derive(Debug)]
pub(crate) struct UploadState {
    owner: HeapId,
    inner: Mutex<UploadInner>,
    changed: tokio::sync::Notify,
}

/// What a pull resolved to: one chunk, or the end of the stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PulledChunk {
    /// Set when the stream ended cleanly (closed origin, or cancelled).
    /// Failures reject the pull instead of resolving.
    pub done: bool,
    pub bytes: Vec<u8>,
}

/// The table itself, free of globals so unit tests can drive it
/// directly. The process-wide registry below is a thin locked wrapper
/// around the same methods.
#[derive(Debug, Default)]
pub(crate) struct UploadTable {
    uploads: HashMap<String, std::sync::Arc<UploadState>>,
}

impl UploadTable {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn len(&self) -> usize {
        self.uploads.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.uploads.is_empty()
    }

    fn get(&self, id: &str) -> Option<std::sync::Arc<UploadState>> {
        self.uploads.get(id).cloned()
    }

    /// Open one upload owned by `owner` under `id`. Refuses a taken id
    /// without touching the victim, and refuses past the process cap
    /// without evicting.
    pub(crate) fn create(&mut self, id: String, owner: HeapId) -> Result<(), UploadCreateError> {
        if self.uploads.contains_key(&id) {
            return Err(UploadCreateError::Occupied);
        }
        if self.uploads.len() >= upload_cap() {
            return Err(UploadCreateError::ProcessCap);
        }
        self.uploads.insert(
            id,
            std::sync::Arc::new(UploadState {
                owner,
                inner: Mutex::new(UploadInner {
                    buffer: VecDeque::new(),
                    buffered_bytes: 0,
                    terminal: None,
                    sender_alive: true,
                    receiver_gone: false,
                    touched: false,
                    eof_observed: false,
                    receiver: None,
                }),
                changed: tokio::sync::Notify::new(),
            }),
        );
        Ok(())
    }

    /// Push one chunk, waiting for buffer room. Test-only: production
    /// pushes resolve the upload under the process lock, then run
    /// [`push_to`] without holding any table lock across the await.
    #[cfg(test)]
    pub(crate) async fn push(&self, id: &str, chunk: Vec<u8>) -> Result<(), String> {
        let Some(upload) = self.get(id) else {
            return Err("the upload is no longer live".to_string());
        };
        push_to(&upload, chunk).await
    }

    /// Pull one chunk, waiting for data or the terminal. Test-only, like
    /// [`Self::push`].
    #[cfg(test)]
    pub(crate) async fn pull(&self, id: &str) -> Result<PulledChunk, String> {
        let Some(upload) = self.get(id) else {
            return Err(disconnect_error());
        };
        pull_from(&upload).await
    }

    /// The sender closed the origin cleanly: buffered chunks still
    /// drain, then the receiver reads EOF.
    pub(crate) fn close(&self, id: &str) {
        let Some(upload) = self.get(id) else {
            return;
        };
        // The state lock is held only across microsecond transitions,
        // never across an await, so this take cannot stall the runtime
        // and the terminal cannot be skipped: a `try_lock` here could
        // miss under contention and hang the waiter it was meant to
        // wake.
        let mut inner = upload.inner.lock().expect("upload state poisoned");
        if inner.terminal.is_none() {
            inner.terminal = Some(UploadTerminal::Closed);
        }
        drop(inner);
        upload.changed.notify_waiters();
    }

    /// The sender's origin errored: buffered chunks still drain, then
    /// the receiver's pull rejects.
    pub(crate) fn fail(&self, id: &str) {
        self.terminate(id, UploadTerminal::Failed);
    }

    /// The receiver is done: the next sender push is rejected so its
    /// pump stops and cancels the origin. An open upload terminates as
    /// `Cancelled`; a terminal already reached stands, so a parked pull
    /// still observes the sender's close or disconnect. Either way the
    /// receiver is gone: buffered chunks are dropped (nobody drains
    /// them), and when the sender is already gone the entry removes
    /// itself whatever the terminal, including `Closed` or `Failed`
    /// after a partial read.
    pub(crate) fn cancel(&mut self, id: &str) {
        let Some(upload) = self.get(id) else {
            return;
        };
        let remove = {
            let mut inner = upload.inner.lock().expect("upload state poisoned");
            inner.receiver_gone = true;
            if inner.terminal.is_none() {
                inner.terminal = Some(UploadTerminal::Cancelled);
            }
            inner.buffer.clear();
            inner.buffered_bytes = 0;
            !inner.sender_alive
        };
        if remove {
            self.uploads.remove(id);
        }
        upload.changed.notify_waiters();
    }

    /// The sender is gone (context end, abort, isolate retirement):
    /// an open upload fails after draining what buffered, a closed one
    /// keeps draining toward EOF. An upload both sides left removes
    /// itself whatever its terminal: no pull ever touched it, its EOF
    /// already resolved, the receiver cancelled it, or the receiver
    /// left after a partial read (`receiver_gone`). The terminal lands
    /// before any removal, so a parked pull still resolves instead of
    /// hanging on the removed entry's surviving state.
    pub(crate) fn abort(&mut self, id: &str) {
        let Some(upload) = self.get(id) else {
            return;
        };
        let remove = {
            let mut inner = upload.inner.lock().expect("upload state poisoned");
            inner.sender_alive = false;
            if inner.terminal.is_none() {
                inner.terminal = Some(UploadTerminal::Failed);
            }
            !inner.touched
                || inner.eof_observed
                || inner.receiver_gone
                || inner.terminal == Some(UploadTerminal::Cancelled)
        };
        if remove {
            self.uploads.remove(id);
        }
        upload.changed.notify_waiters();
    }

    /// Reap a drained upload whose sender is gone: EOF observed it, and
    /// nothing remains for either side. Pulls do not call this inline
    /// (the pull path holds no table lock); the pull op calls it after
    /// resolving EOF.
    pub(crate) fn reap_drained(&mut self, id: &str) {
        let reap = self
            .uploads
            .get(id)
            .map(|upload| upload.inner.lock().expect("upload state poisoned"))
            .is_some_and(|inner| {
                !inner.sender_alive
                    && inner.buffer.is_empty()
                    && matches!(
                        inner.terminal,
                        Some(UploadTerminal::Closed) | Some(UploadTerminal::Cancelled)
                    )
            });
        if reap {
            self.uploads.remove(id);
        }
    }

    /// The retirement backstop: terminate every upload the freed heap
    /// owned. Pending receiver pulls resolve the terminal; the sender's
    /// pump is gone with its isolate.
    pub(crate) fn terminate_owner(&mut self, owner: HeapId) {
        let owned: Vec<String> = self
            .uploads
            .iter()
            .filter(|(_, upload)| upload.owner == owner)
            .map(|(id, _)| id.clone())
            .collect();
        for id in owned {
            self.abort(&id);
        }
    }

    /// Record the heap currently holding the receiving end. The revive
    /// runs in the holder's isolate, so its heap is the holder; a
    /// forward re-records when the reference moves. Unknown ids are
    /// ignored: a retired channel needs no holder.
    pub(crate) fn acquire_receiver(&self, id: &str, heap: HeapId) {
        let Some(upload) = self.get(id) else {
            return;
        };
        upload.inner.lock().expect("upload state poisoned").receiver = Some(heap);
    }

    /// The retirement backstop: cancel every upload the freed heap still
    /// held the receiving end of. The sender's next push is rejected so
    /// its pump stops, and an entry the sender already left removes
    /// itself; any other heap's holds are untouched.
    pub(crate) fn terminate_receiver(&mut self, heap: HeapId) {
        let held: Vec<String> = self
            .uploads
            .iter()
            .filter(|(_, upload)| {
                upload.inner.lock().expect("upload state poisoned").receiver == Some(heap)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for id in held {
            self.cancel(&id);
        }
    }

    fn terminate(&self, id: &str, terminal: UploadTerminal) {
        let Some(upload) = self.get(id) else {
            return;
        };
        let mut inner = upload.inner.lock().expect("upload state poisoned");
        if inner.terminal.is_none() {
            inner.terminal = Some(terminal);
        }
        drop(inner);
        upload.changed.notify_waiters();
    }
}

/// Why a table-level creation was refused, before id generation retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UploadCreateError {
    Occupied,
    ProcessCap,
}

fn disconnect_error() -> String {
    "ReadableStream received over RPC disconnected prematurely.".to_string()
}

/// Push one chunk onto a resolved upload, waiting for buffer room.
/// Split from the table so the process-wide op can drop the table lock
/// before awaiting: no table lock is ever held across an await.
async fn push_to(upload: &UploadState, chunk: Vec<u8>) -> Result<(), String> {
    if chunk.len() > UPLOAD_CHUNK_MAX_BYTES {
        return Err(format!(
            "upload chunk is {} bytes, above the {} byte bound",
            chunk.len(),
            UPLOAD_CHUNK_MAX_BYTES
        ));
    }
    let chunk = Bytes::from(chunk);
    loop {
        let notified = upload.changed.notified();
        {
            // A plain take: the guard drops at the block end, before the
            // park below, so no lock is ever held across an await.
            let mut inner = upload.inner.lock().expect("upload state poisoned");
            match inner.terminal {
                Some(UploadTerminal::Cancelled) => {
                    return Err("the receiver cancelled the upload".to_string());
                }
                Some(_) => {
                    return Err("the upload is no longer live".to_string());
                }
                None => {}
            }
            if inner.buffer.len() < UPLOAD_CHUNK_CAP
                && inner.buffered_bytes + chunk.len() <= UPLOAD_BYTE_CAP
            {
                inner.buffered_bytes += chunk.len();
                inner.buffer.push_back(chunk);
                upload.changed.notify_waiters();
                return Ok(());
            }
        }
        notified.await;
    }
}

/// Pull one chunk from a resolved upload, waiting for data or the
/// terminal. Buffered chunks drain before any terminal resolves.
async fn pull_from(upload: &UploadState) -> Result<PulledChunk, String> {
    loop {
        let notified = upload.changed.notified();
        {
            // A plain take, dropped before the park below: no lock is
            // ever held across an await.
            let mut inner = upload.inner.lock().expect("upload state poisoned");
            inner.touched = true;
            if let Some(chunk) = inner.buffer.pop_front() {
                inner.buffered_bytes -= chunk.len();
                upload.changed.notify_waiters();
                return Ok(PulledChunk {
                    done: false,
                    bytes: chunk.to_vec(),
                });
            }
            match inner.terminal {
                None => {}
                Some(UploadTerminal::Closed) | Some(UploadTerminal::Cancelled) => {
                    inner.eof_observed = true;
                    return Ok(PulledChunk {
                        done: true,
                        bytes: Vec::new(),
                    });
                }
                Some(UploadTerminal::Failed) => {
                    return Err(disconnect_error());
                }
            }
        }
        notified.await;
    }
}

static UPLOADS: OnceLock<Mutex<UploadTable>> = OnceLock::new();

fn uploads() -> &'static Mutex<UploadTable> {
    UPLOADS.get_or_init(|| Mutex::new(UploadTable::new()))
}

/// Open one upload owned by `owner`, returning its wire id: the same
/// minted 32-hex shape as a stub id, validated the same way. Creation
/// retries past a collision so a taken id never reroutes a victim
/// upload, and fails closed past the process cap.
pub fn create_upload(owner: HeapId) -> Result<String, UploadRejection> {
    let mut table = uploads().lock().expect("upload table poisoned");
    for _ in 0..CREATE_ATTEMPTS {
        let id = crate::stub_table::new_host_stub_id();
        match table.create(id.clone(), owner) {
            Ok(()) => return Ok(id),
            Err(UploadCreateError::Occupied) => continue,
            Err(UploadCreateError::ProcessCap) => return Err(UploadRejection::ProcessCap),
        }
    }
    // Inconceivable at 128 bits; fail closed rather than loop.
    Err(UploadRejection::ProcessCap)
}

/// Push one chunk, waiting for buffer room.
pub async fn push_upload(id: &str, chunk: Vec<u8>) -> Result<(), String> {
    let upload = uploads().lock().expect("upload table poisoned").get(id);
    let Some(upload) = upload else {
        return Err("the upload is no longer live".to_string());
    };
    push_to(&upload, chunk).await
}

/// Pull one chunk, waiting for data or the terminal.
pub async fn pull_upload(id: &str) -> Result<PulledChunk, String> {
    let upload = uploads().lock().expect("upload table poisoned").get(id);
    let Some(upload) = upload else {
        return Err(disconnect_error());
    };
    pull_from(&upload).await
}

/// The sender closed the origin cleanly.
pub fn close_upload(id: &str) {
    uploads().lock().expect("upload table poisoned").close(id);
}

/// The sender's origin errored.
pub fn fail_upload(id: &str) {
    uploads().lock().expect("upload table poisoned").fail(id);
}

/// The receiver is done: the sender's next push is rejected.
pub fn cancel_upload(id: &str) {
    uploads().lock().expect("upload table poisoned").cancel(id);
}

/// The sender is gone: fail what is open, drain what is closed, reap
/// what is cancelled.
pub fn abort_upload(id: &str) {
    uploads().lock().expect("upload table poisoned").abort(id);
}

/// Reap a drained upload whose sender is gone, after a pull resolved
/// its EOF.
pub fn reap_drained_upload(id: &str) {
    uploads()
        .lock()
        .expect("upload table poisoned")
        .reap_drained(id);
}

/// The retirement backstop: terminate every upload the freed heap
/// owned.
pub fn terminate_upload_owner(owner: HeapId) {
    uploads()
        .lock()
        .expect("upload table poisoned")
        .terminate_owner(owner);
}

/// Record the heap holding an upload's receiving end.
pub fn acquire_upload_receiver(id: &str, heap: HeapId) {
    uploads()
        .lock()
        .expect("upload table poisoned")
        .acquire_receiver(id, heap);
}

/// The retirement backstop: cancel every upload the freed heap still
/// held the receiving end of.
pub fn terminate_upload_receiver(heap: HeapId) {
    uploads()
        .lock()
        .expect("upload table poisoned")
        .terminate_receiver(heap);
}

/// How many uploads the table holds, for `/state` and leak tests.
pub fn upload_count() -> usize {
    uploads().lock().expect("upload table poisoned").len()
}

#[cfg(test)]
mod upload_table_tests {
    use super::*;

    const ID: &str = "0123456789abcdef0123456789abcdef";

    fn table() -> UploadTable {
        UploadTable::new()
    }

    fn owner(id: u64) -> HeapId {
        HeapId::new(id)
    }

    fn chunk(text: &str) -> Vec<u8> {
        text.as_bytes().to_vec()
    }

    #[test]
    fn create_refuses_a_taken_id_without_touching_the_victim() {
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        assert_eq!(
            table.create(ID.to_string(), owner(2)),
            Err(UploadCreateError::Occupied)
        );
        // The victim upload still belongs to its owner: retiring another
        // heap leaves it open.
        table.terminate_owner(owner(2));
        assert!(!table.is_empty());
    }

    #[test]
    fn create_fails_closed_at_the_process_cap() {
        let mut table = table();
        for index in 0..UPLOAD_MAX_PER_PROCESS {
            table
                .create(format!("{index:032x}"), owner(1))
                .expect("create under the cap");
        }
        assert_eq!(
            table.create("ffffffffffffffffffffffffffffffff".to_string(), owner(1)),
            Err(UploadCreateError::ProcessCap)
        );
    }

    #[tokio::test]
    async fn push_pull_round_trips_chunks_then_eof() {
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        table.push(ID, chunk("one")).await.expect("push one");
        table.push(ID, chunk("two")).await.expect("push two");
        table.close(ID);
        let first = table.pull(ID).await.expect("pull one");
        assert!(!first.done);
        assert_eq!(first.bytes, chunk("one"));
        let second = table.pull(ID).await.expect("pull two");
        assert!(!second.done);
        assert_eq!(second.bytes, chunk("two"));
        let end = table.pull(ID).await.expect("pull eof");
        assert!(end.done, "close resolves EOF after the buffer drains");
    }

    #[tokio::test]
    async fn origin_failure_delivers_buffered_chunks_then_rejects() {
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        table.push(ID, chunk("one")).await.expect("push");
        table.fail(ID);
        let first = table.pull(ID).await.expect("buffered chunk first");
        assert!(!first.done);
        assert_eq!(first.bytes, chunk("one"));
        assert_eq!(table.pull(ID).await, Err(disconnect_error()));
        assert_eq!(
            table.push(ID, chunk("late")).await,
            Err("the upload is no longer live".to_string())
        );
    }

    #[tokio::test]
    async fn receiver_retirement_cancels_only_what_the_heap_held() {
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        table.acquire_receiver(ID, owner(2));
        let other = "ffffffffffffffffffffffffffffffff";
        table.create(other.to_string(), owner(1)).expect("create");
        table.acquire_receiver(other, owner(3));
        // A forward moves the hold: the old heap's retirement must not
        // touch the channel anymore.
        table.acquire_receiver(other, owner(4));
        table.terminate_receiver(owner(2));
        assert_eq!(table.len(), 2, "a live sender keeps a cancelled entry");
        assert_eq!(
            table.push(ID, chunk("late")).await,
            Err("the receiver cancelled the upload".to_string())
        );
        // The forwarded channel is untouched by either heap's retirement.
        table.terminate_receiver(owner(3));
        table
            .push(other, chunk("one"))
            .await
            .expect("unheld push still buffers");
        let first = table.pull(other).await.expect("pull one");
        assert_eq!(first.bytes, chunk("one"));
        table.abort(other);
        assert_eq!(table.len(), 2, "failed entries stay for their readers");
        table.terminate_receiver(owner(4));
        assert_eq!(table.len(), 1, "sender gone plus receiver gone reaps");
    }

    #[tokio::test]
    async fn receiver_cancel_rejects_the_next_push() {
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        table.push(ID, chunk("one")).await.expect("push");
        table.cancel(ID);
        assert_eq!(
            table.push(ID, chunk("late")).await,
            Err("the receiver cancelled the upload".to_string())
        );
        let end = table.pull(ID).await.expect("cancelled reads EOF");
        assert!(end.done);
    }

    #[tokio::test]
    async fn sender_abort_fails_an_open_upload_but_drains_a_closed_one() {
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        table.push(ID, chunk("one")).await.expect("push");
        table.push(ID, chunk("two")).await.expect("push");
        // An engaged receiver (a pull ran) drains what buffered, then
        // observes the sender's death.
        let first = table.pull(ID).await.expect("pull one");
        assert_eq!(first.bytes, chunk("one"));
        table.abort(ID);
        let second = table.pull(ID).await.expect("buffered chunk still drains");
        assert!(!second.done);
        assert_eq!(second.bytes, chunk("two"));
        assert_eq!(
            table.pull(ID).await,
            Err(disconnect_error()),
            "sender gone rejects after the buffer drains"
        );

        let closed = "ffffffffffffffffffffffffffffffff";
        table.create(closed.to_string(), owner(1)).expect("create");
        table.push(closed, chunk("one")).await.expect("push");
        table.push(closed, chunk("two")).await.expect("push");
        table.close(closed);
        let first = table.pull(closed).await.expect("pull one");
        assert_eq!(first.bytes, chunk("one"));
        table.abort(closed);
        let second = table
            .pull(closed)
            .await
            .expect("buffered chunk still drains");
        assert_eq!(second.bytes, chunk("two"));
        let end = table.pull(closed).await.expect("closed still EOFs");
        assert!(end.done);
        table.reap_drained(closed);
        // The drained one reaped; the failed one stays for its engaged
        // receiver, then reaps when the receiver leaves too: both sides
        // gone removes the entry whatever its terminal.
        assert_eq!(table.len(), 1);
        table.cancel(ID);
        assert!(
            table.is_empty(),
            "cancel on a sender-gone terminal reaps the entry"
        );
        assert_eq!(table.pull(ID).await, Err(disconnect_error()));
    }

    #[tokio::test]
    async fn sender_abort_removes_an_upload_whose_eof_resolved() {
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        table.push(ID, chunk("one")).await.expect("push");
        table.close(ID);
        let first = table.pull(ID).await.expect("pull one");
        assert_eq!(first.bytes, chunk("one"));
        let end = table.pull(ID).await.expect("pull EOF");
        assert!(end.done);
        // Reader done, then sender goes: nothing remains for either
        // side, whatever order the two ends land in.
        table.abort(ID);
        assert!(table.is_empty());
    }

    #[tokio::test]
    async fn sender_abort_removes_an_upload_no_pull_touched() {
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        table.push(ID, chunk("one")).await.expect("push");
        table.close(ID);
        // No pull ever ran (a lift that failed before its envelope
        // sent): the sender going removes the upload outright instead
        // of leaving a closed entry no reader will drain.
        table.abort(ID);
        assert!(table.is_empty());
        assert_eq!(table.pull(ID).await, Err(disconnect_error()));
    }

    #[tokio::test]
    async fn abort_reaps_a_cancelled_upload_outright() {
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        table.cancel(ID);
        table.abort(ID);
        assert!(table.is_empty(), "reader and sender both gone");
        assert_eq!(table.pull(ID).await, Err(disconnect_error()));
    }

    #[tokio::test]
    async fn partial_read_reaps_when_both_sides_leave() {
        // A closed upload a pull touched but never drained to EOF: the
        // receiver stopping early must not strand the entry, whichever
        // side leaves first.
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        table.push(ID, chunk("one")).await.expect("push");
        table.close(ID);
        let first = table.pull(ID).await.expect("pull one");
        assert_eq!(first.bytes, chunk("one"));
        // Receiver leaves first: the entry stays for its live sender.
        table.cancel(ID);
        assert_eq!(table.len(), 1);
        // Then the sender goes: both gone removes it, no EOF needed.
        table.abort(ID);
        assert!(table.is_empty(), "cancel-then-abort reaps a partial read");

        // The other order: sender goes while the partial read is still
        // engaged, then the receiver leaves.
        let other = "ffffffffffffffffffffffffffffffff";
        table.create(other.to_string(), owner(1)).expect("create");
        table.push(other, chunk("one")).await.expect("push");
        table.close(other);
        let first = table.pull(other).await.expect("pull one");
        assert_eq!(first.bytes, chunk("one"));
        table.abort(other);
        assert_eq!(table.len(), 1, "sender gone, receiver engaged");
        table.cancel(other);
        assert!(table.is_empty(), "abort-then-cancel reaps a partial read");
        assert_eq!(table.pull(other).await, Err(disconnect_error()));
    }

    #[tokio::test]
    async fn sender_abort_first_reaps_on_receiver_cancel() {
        // Sender-abort-first: the sender's context ends while the upload
        // is open, failing it; the receiver's later cancel used to no-op
        // on the Failed terminal and strand the entry.
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        table.push(ID, chunk("one")).await.expect("push");
        let first = table.pull(ID).await.expect("pull one");
        assert_eq!(first.bytes, chunk("one"));
        table.abort(ID);
        assert_eq!(table.len(), 1, "failed entries stay for their readers");
        table.cancel(ID);
        assert!(table.is_empty(), "cancel after abort reaps the entry");
        assert_eq!(table.pull(ID).await, Err(disconnect_error()));
    }

    #[tokio::test]
    async fn origin_error_with_partial_drain_reaps_when_both_sides_leave() {
        // Same as a partial read, with a Failed terminal from the origin
        // error instead of Closed: buffered chunks still drain, then both
        // sides leaving reaps.
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        table.push(ID, chunk("one")).await.expect("push");
        table.push(ID, chunk("two")).await.expect("push");
        table.fail(ID);
        let first = table.pull(ID).await.expect("buffered chunk first");
        assert_eq!(first.bytes, chunk("one"));
        // Receiver leaves with chunks still buffered: the buffer drops
        // (nobody drains it), and the sender going reaps.
        table.cancel(ID);
        assert_eq!(table.len(), 1);
        table.abort(ID);
        assert!(table.is_empty(), "both gone reaps a failed partial read");
    }

    #[tokio::test]
    async fn retirement_terminates_only_the_freed_heaps_uploads() {
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        let other = "ffffffffffffffffffffffffffffffff";
        table.create(other.to_string(), owner(2)).expect("create");
        // Engage a receiver on the first upload so the backstop fails
        // it (instead of removing it untouched): engage, then retire.
        table.push(ID, chunk("one")).await.expect("push");
        let first = table.pull(ID).await.expect("pull one");
        assert_eq!(first.bytes, chunk("one"));
        table.terminate_owner(owner(1));
        assert_eq!(table.len(), 2, "failed entries stay for their readers");
        assert_eq!(table.pull(ID).await, Err(disconnect_error()));
        table
            .push(other, chunk("live"))
            .await
            .expect("other owner unaffected");
        let live = table.pull(other).await.expect("pull");
        assert_eq!(live.bytes, chunk("live"));
    }

    #[tokio::test]
    async fn oversize_push_is_rejected_not_buffered() {
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        let big = vec![0_u8; UPLOAD_CHUNK_MAX_BYTES + 1];
        assert!(table.push(ID, big).await.is_err());
        table.close(ID);
        let end = table.pull(ID).await.expect("EOF");
        assert!(end.done);
    }

    /// A waker that does nothing, for polling one future by hand: the
    /// test below drives a push and a pull against each other without
    /// spawning, so the rendezvous is fully deterministic.
    fn noop_waker() -> std::task::Waker {
        use std::task::{RawWaker, RawWakerVTable, Waker};
        fn raw() -> RawWaker {
            fn clone(_: *const ()) -> RawWaker {
                raw()
            }
            fn noop(_: *const ()) {}
            static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        unsafe { Waker::from_raw(raw()) }
    }

    #[tokio::test]
    async fn push_waits_for_room_and_pull_wakes_it() {
        use std::future::Future as _;
        use std::task::{Context, Poll};
        let mut table = table();
        table.create(ID.to_string(), owner(1)).expect("create");
        for _ in 0..UPLOAD_CHUNK_CAP {
            table.push(ID, chunk("x")).await.expect("fill the buffer");
        }
        // The buffer is full: a further push parks instead of resolving.
        let mut push = Box::pin(table.push(ID, chunk("y")));
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(
            matches!(push.as_mut().poll(&mut cx), Poll::Pending),
            "a push past the buffer waits for a pull"
        );
        // One pull frees room and wakes the parked push, which then fits.
        let freed = table.pull(ID).await.expect("pull frees room");
        assert_eq!(freed.bytes, chunk("x"));
        assert!(
            matches!(push.as_mut().poll(&mut cx), Poll::Ready(Ok(()))),
            "the pull wakes the parked push"
        );
        // Symmetry: an empty upload parks its pull until a push lands.
        for _ in 0..UPLOAD_CHUNK_CAP {
            table.pull(ID).await.expect("drain the buffer");
        }
        let mut pull = Box::pin(table.pull(ID));
        assert!(
            matches!(pull.as_mut().poll(&mut cx), Poll::Pending),
            "a pull past the buffer waits for a push"
        );
        table.push(ID, chunk("z")).await.expect("push wakes");
        match pull.as_mut().poll(&mut cx) {
            Poll::Ready(Ok(got)) => assert_eq!(got.bytes, chunk("z")),
            other => panic!("the push wakes the parked pull, got {other:?}"),
        }
    }
}
