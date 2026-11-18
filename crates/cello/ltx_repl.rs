// Modified by ryuzcorp (ryuzcorp/celld): an eviction skips the visibility LISTs its own uploads already prove and the time-based checkpoint before the handoff; an eviction deletes the per-cell L0/L1 objects its handoff snapshot supersedes; bucket-posture group commit: paged gate judges PAGED_FORMAT (0059); bucket-posture group commit: bucket bundle gate, sync pacing and event-driven bundle flush (0060); the eviction guard proves a journaled epoch with a HEAD on the newest journaled key, falling back to the listing on a miss, an error, or an empty journal (0061); bucket-posture group commit: bundled-cell eviction guard, graceful-close per-cell drain, bundle GC without ensemble, linger holds only while the dirty set is shared (0064).; bucket-posture group commit: bucket switch on by default (0070); bucket-posture group commit: a sync worker yields once the bundle loop owns tiering, and a lone-cell round uploads per-cell instead of bundling (0071); bucket-posture group commit: bundle switch defaults off (0075); MergeTree-style lazy handoff: an eviction skips the snapshot for a short chain born in this epoch (0078); local L0/epoch LTX retention on compaction, activation, and startup (upstream #311); unclean-restart prune of live-named epochs (upstream #290).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! In-process replication backend built on `cello-ltx`.
//!
//! One shared `object_store` client for the whole node, and a managed
//! `cello_ltx::Db` per resident cell that captures the cell's committed WAL
//! and uploads it on demand. No external process, no directory-watch lag — a
//! just-written cell is registered the instant it activates, so the output
//! gate can prove a fresh cell durable with no cold-start window.
//!
//! The object layout is `cells/<cell>/ltx/e<epoch>/` in the bucket, mirroring
//! the local `<watch>/<cell>/ltx/e<epoch>/db.sqlite` tree. This backend builds
//! its own object-store clients rather than going through `bucket::Bucket`, so
//! it carries the fleet's key prefix itself: without that, two fleets sharing
//! one bucket would replicate over each other.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::time::Duration;

use anyhow::anyhow;
use cello_ltx::client::epochs::EpochChain;
use cello_ltx::object_store::ObjectStore;
use cello_ltx::replica;
use cello_ltx::replica_compactor::{CompactionResult, ReplicaCompactor};
use cello_ltx::Db;
use cello_ltx::HostTaskError;
use cello_ltx::LtxHost;
use cello_ltx::ObjectStoreClient;
use cello_ltx::ObjectStoreConfig;
use cello_ltx::Pos;
use cello_ltx::Replica;
use cello_ltx::ReplicaClient;
use cello_ltx::TimestampMetadataKey;
use cello_ltx::TXID;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tracing::info;
use tracing::warn;

use crate::asyncrt;
use crate::replication::sqlite_snapshot;
use crate::replication::ActivationOptions;
use crate::replication::ActivationResult;
use crate::replication::RestoredSnapshot;
use crate::replication::StorageCredentials;
use crate::replication::SyncWait;
use crate::replication::{EvictionAbandon, EvictionRestoreArtifact};
use cello_logic::durability::{proof_deadline, ProofProgress, ProofWait};

/// Max cells uploading concurrently across the node. Caps blocking-pool threads
/// and in-flight object-store requests under high write fan-out.
const SYNC_CONCURRENCY: usize = 64;

/// The upload slots this node starts with. The simulation narrows them to
/// put a second cell's upload in the queue on purpose, which is the state
/// the durability wait's queued regime exists for.
fn sync_concurrency() -> usize {
    #[cfg(cello_internal_tests)]
    if let Some(slots) = sync_concurrency_for_world() {
        return slots;
    }
    SYNC_CONCURRENCY
}

/// Max LTX object downloads across every restore on this node. A hot cell can
/// contain thousands of L0 files, so serial reads turn a takeover into minutes
/// of terminal failures. This shared ceiling hides round-trip latency without
/// multiplying the bound by the activation count.
const RESTORE_DOWNLOAD_CONCURRENCY: usize = 64;

/// A cheap clone of one cell-epoch client. Without the wrapper, the replica,
/// uploader, and compactor each retain a full copy of the immutable string
/// configuration. Keep this wrapper private instead of adding a blanket
/// `ReplicaClient for Arc<_>` implementation to the public LTX crate.
///
/// The implementation below must forward every method of `ReplicaClient`,
/// including the three that the trait provides a default for. A method that
/// this wrapper does not forward still compiles, and it then silently replaces
/// the object store behavior with the default: `write_ltx_file_from_file`
/// refuses every oversized compaction, `read_range` downloads a whole object
/// for each paged fault, and `ltx_files_bounded` lists a whole prefix.
/// `shared_object_store_client_forwards_the_provided_methods` guards this.
#[derive(Clone)]
struct SharedObjectStoreClient(Arc<ObjectStoreClient>);

#[async_trait::async_trait]
impl ReplicaClient for SharedObjectStoreClient {
    async fn ltx_files(
        &self,
        level: i32,
        seek: TXID,
    ) -> cello_ltx::Result<Vec<cello_ltx::FileInfo>> {
        self.0.ltx_files(level, seek).await
    }

    async fn ltx_files_bounded(
        &self,
        level: i32,
        seek: TXID,
        limit: usize,
    ) -> cello_ltx::Result<Vec<cello_ltx::FileInfo>> {
        self.0.ltx_files_bounded(level, seek, limit).await
    }

    async fn open_ltx_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
    ) -> cello_ltx::Result<Vec<u8>> {
        self.0.open_ltx_file(level, min_txid, max_txid).await
    }

    async fn read_range(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        offset: u64,
        len: u64,
    ) -> cello_ltx::Result<Vec<u8>> {
        self.0
            .read_range(level, min_txid, max_txid, offset, len)
            .await
    }

    async fn write_ltx_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        data: &[u8],
    ) -> cello_ltx::Result<cello_ltx::FileInfo> {
        self.0.write_ltx_file(level, min_txid, max_txid, data).await
    }

    async fn write_ltx_file_from_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        file: cello_ltx::host::HostFile,
        host: LtxHost,
    ) -> cello_ltx::Result<cello_ltx::FileInfo> {
        self.0
            .write_ltx_file_from_file(level, min_txid, max_txid, file, host)
            .await
    }

    async fn delete_ltx_files(&self, files: &[cello_ltx::FileInfo]) -> cello_ltx::Result<()> {
        self.0.delete_ltx_files(files).await
    }

    async fn delete_all(&self) -> cello_ltx::Result<()> {
        self.0.delete_all().await
    }
}

impl SharedObjectStoreClient {
    /// Single-key deletes for the eviction trim (see
    /// `ObjectStoreClient::delete_ltx_keys`): one `DeleteObject` per key,
    /// never bulk `POST ?delete`.
    async fn delete_ltx_keys(&self, files: &[cello_ltx::FileInfo], concurrency: usize) -> usize {
        self.0.delete_ltx_keys(files, concurrency).await
    }
}

/// One attempt consumes at most this many source objects. This bound keeps a
/// first compaction of an old, write-hot cell from reading its complete L0
/// history into memory.
const COMPACTION_MAX_FILES: usize = 256;

/// Bound buffered source data. An indivisible object above this budget uses
/// scratch files and bounded range reads instead of stalling the level.
const COMPACTION_MAX_INPUT_BYTES: u64 = 64 * 1024 * 1024;

/// One captured-but-not-yet-uploaded L0 segment, the log tier's
/// replication unit (`crate::node_log`).
pub struct ShipEntry {
    pub cell: String,
    pub epoch: u64,
    pub txid: u64,
    pub bytes: Vec<u8>,
}

/// The result of one submitted ship round. The private reservation stays
/// live until the ship loop applies or discards the result, so dropping either
/// a pending future or a completed result releases the reconfigure barrier.
pub struct ShipCompletion {
    last_seq: Option<u64>,
    _reservation: Option<Box<dyn Send>>,
}

impl ShipCompletion {
    pub fn unreserved(last_seq: Option<u64>) -> Self {
        Self {
            last_seq,
            _reservation: None,
        }
    }

    pub fn reserved(last_seq: Option<u64>, reservation: impl Send + 'static) -> Self {
        Self {
            last_seq,
            _reservation: Some(Box::new(reservation)),
        }
    }

    pub fn last_seq(&self) -> Option<u64> {
        self.last_seq
    }
}

/// The fleet shipper the log tier installs: pipelined, all-member fsync
/// confirmation. A completion without a sequence means the batch is not
/// fleet-durable and the gate must ride the bucket upload instead.
pub trait Shipper: Send + Sync + 'static {
    /// Ship one batch; `covered_seq` is the highest sequence whose frames
    /// are all bucket-covered, which followers may truncate behind.
    /// `completion.last_seq() == Some(last_seq)` means every member confirmed
    /// the whole batch. The completion owns the round's barrier reservation.
    /// Owned arguments and a 'static future: the shipper runs its
    /// synchronous prefix (sequence allocation, per-member lane enqueue)
    /// before returning, so SUBMISSION order — not poll order — fixes the
    /// per-member append order when several rounds are in flight.
    fn ship(
        &self,
        batch: Vec<ShipEntry>,
        covered_seq: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ShipCompletion> + Send + 'static>>;

    /// Ship a batch captured under `expected_epoch`. A stable manager whose
    /// delegate can change must override this method and select plus reserve
    /// the matching delegate atomically.
    fn ship_at_epoch(
        &self,
        expected_epoch: u64,
        batch: Vec<ShipEntry>,
        covered_seq: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ShipCompletion> + Send + 'static>> {
        if self.epoch() != expected_epoch {
            return Box::pin(std::future::ready(ShipCompletion::unreserved(None)));
        }
        self.ship(batch, covered_seq)
    }

    /// Rounds this shipper wants in flight at once. The loop applies
    /// credits strictly in submission order regardless of the depth.
    fn pipeline(&self) -> usize {
        1
    }

    /// True while the shipper can take one more batch. The stream window
    /// closes this on the slowest member's lane — appends or bytes — and
    /// the loop then waits on a completion exactly as it does at depth.
    fn admit(&self) -> bool {
        true
    }

    /// A degraded shipper refuses instantly; the ship loop skips capture.
    fn active(&self) -> bool;
    /// The log epoch this shipper writes. Sequences restart at zero each
    /// epoch, so the ship loop's truncation ledger must reset with it — a
    /// stale covered watermark from the previous epoch would tell fresh
    /// followers to delete entries they just fsync'd.
    fn epoch(&self) -> u64;
}

/// The compactor-facing fetcher: one cell-epoch's view over whatever sink
/// is currently installed. Reading the slot per call makes activation
/// order irrelevant — a cell activated before the sink existed still sees
/// bundles once it does.
struct SinkFetcher {
    registration: Arc<Mutex<RegistrationState>>,
    cell: String,
    epoch: u64,
    // One snapshot per compaction round. Opening every L0 row must not
    // repeat a persisted-index fallback for the same slow cell.
    rows: tokio::sync::Mutex<Option<Vec<cello_ltx::LocatedRow>>>,
}

impl cello_ltx::BundleFetcher for SinkFetcher {
    fn rows<'a>(
        &'a self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = cello_ltx::Result<Vec<cello_ltx::LocatedRow>>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mut rows = self.rows.lock().await;
            if let Some(rows) = rows.as_ref() {
                return Ok(rows.clone());
            }
            let sink =
                registered_durability(&self.registration).map(|targets| targets.manager.clone());
            let fetched = match sink {
                Some(sink) => sink
                    .rows_for(&self.cell, self.epoch)
                    .await
                    .map_err(|error| cello_ltx::Error::Other(error.to_string().into()))?,
                None => Vec::new(),
            };
            *rows = Some(fetched.clone());
            Ok(fetched)
        })
    }

    fn fetch<'a>(
        &'a self,
        located: &'a cello_ltx::LocatedRow,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = cello_ltx::Result<Vec<u8>>> + Send + 'a>>
    {
        Box::pin(async move {
            let sink =
                registered_durability(&self.registration).map(|targets| targets.manager.clone());
            let Some(sink) = sink else {
                return Err(cello_ltx::Error::Other("no bundle sink".into()));
            };
            let bytes = sink
                .fetch_bundle(&located.source)
                .await
                .map_err(|e| cello_ltx::Error::Other(e.to_string().into()))?;
            Ok(cello_ltx::bundle::slice(&bytes, &located.row)?.to_vec())
        })
    }

    fn fetch_range<'a>(
        &'a self,
        located: &'a cello_ltx::LocatedRow,
        offset: u64,
        len: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = cello_ltx::Result<Vec<u8>>> + Send + 'a>>
    {
        Box::pin(async move {
            let sink = registered_durability(&self.registration)
                .map(|targets| targets.manager.clone())
                .ok_or_else(|| cello_ltx::Error::Other("no bundle sink".into()))?;
            sink.fetch_bundle_range(located, offset, len)
                .await
                .map_err(|error| cello_ltx::Error::Other(error.to_string().into()))
        })
    }
}

struct RegisteredDurability {
    generation: u64,
    targets: Weak<RegisteredTargets>,
}

struct RegisteredTargets {
    shipper: Arc<dyn Shipper>,
    manager: Arc<crate::node_log::NodeLogManager>,
}

#[derive(Default)]
struct RegistrationState {
    next_generation: u64,
    current: Option<RegisteredDurability>,
}

fn registered_durability(
    registration: &Arc<Mutex<RegistrationState>>,
) -> Option<Arc<RegisteredTargets>> {
    let state = registration.lock().unwrap();
    state
        .current
        .as_ref()
        .and_then(|current| current.targets.upgrade())
}

/// Removes one coupled shipper and bundle-sink registration on drop.
///
/// A newer installation supersedes an older guard. The generation check keeps
/// the old guard from clearing the replacement when construction retries.
#[must_use = "keep the registration alive for the durability owner's lifetime"]
pub(crate) struct DurabilityRegistration {
    registration: Weak<Mutex<RegistrationState>>,
    generation: u64,
    _targets: Arc<RegisteredTargets>,
}

#[derive(Clone)]
#[doc(hidden)]
pub struct StopToken {
    state: Arc<StopState>,
}

struct StopState {
    stopped: AtomicBool,
    notify: Notify,
}

impl StopToken {
    #[allow(clippy::new_without_default)]
    #[doc(hidden)]
    pub fn new() -> Self {
        Self {
            state: Arc::new(StopState {
                stopped: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    pub(crate) fn request_stop(&self) {
        self.state.stopped.store(true, Ordering::SeqCst);
        self.state.notify.notify_waiters();
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.state.stopped.load(Ordering::SeqCst)
    }

    pub(crate) async fn stopped(&self) {
        loop {
            let notified = self.state.notify.notified();
            if self.is_stopped() {
                return;
            }
            notified.await;
        }
    }
}

struct OwnedTask {
    id: u64,
    role: &'static str,
    handle: asyncrt::TaskHandle<()>,
}

struct TaskGroupInner {
    next_id: AtomicU64,
    tasks: Mutex<Vec<OwnedTask>>,
    #[cfg(cello_internal_tests)]
    started_roles: Mutex<std::collections::BTreeSet<&'static str>>,
    #[cfg(cello_internal_tests)]
    joined_failures: AtomicU64,
}

struct TaskCompletion {
    group: Weak<TaskGroupInner>,
    id: u64,
}

struct JoiningTask {
    group: Arc<TaskGroupInner>,
    task: Option<OwnedTask>,
}

impl Drop for JoiningTask {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.group.tasks.lock().unwrap().push(task);
        }
    }
}

impl Drop for TaskCompletion {
    fn drop(&mut self) {
        // A panic drops this guard before the runtime resolves the matching
        // task handle to an error. Keep that handle for `join`, so shutdown
        // reports the failed durability role instead of silently reaping it.
        if std::thread::panicking() {
            return;
        }
        let Some(group) = self.group.upgrade() else {
            return;
        };
        group
            .tasks
            .lock()
            .unwrap()
            .retain(|task| task.id != self.id);
    }
}

#[derive(Clone)]
pub(crate) struct TaskGroup {
    stop: StopToken,
    inner: Arc<TaskGroupInner>,
}

impl TaskGroup {
    pub(crate) fn new(stop: StopToken) -> Self {
        Self {
            stop,
            inner: Arc::new(TaskGroupInner {
                next_id: AtomicU64::new(0),
                tasks: Mutex::new(Vec::new()),
                #[cfg(cello_internal_tests)]
                started_roles: Mutex::new(std::collections::BTreeSet::new()),
                #[cfg(cello_internal_tests)]
                joined_failures: AtomicU64::new(0),
            }),
        }
    }

    pub(crate) fn spawn_owned(
        &self,
        role: &'static str,
        future: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> bool {
        // Keep the lock until the handle is installed. A task can complete on
        // another executor thread immediately after spawn, so its completion
        // guard must not run before the matching handle becomes visible.
        let mut tasks = self.inner.tasks.lock().unwrap();
        if self.stop.is_stopped() {
            return false;
        }
        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        #[cfg(cello_internal_tests)]
        self.inner.started_roles.lock().unwrap().insert(role);
        let completion = TaskCompletion {
            group: Arc::downgrade(&self.inner),
            id,
        };
        tasks.push(OwnedTask {
            id,
            role,
            handle: asyncrt::spawn(async move {
                let _completion = completion;
                future.await;
            }),
        });
        true
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.inner.tasks.lock().unwrap().is_empty()
    }

    pub(crate) fn stop_token(&self) -> StopToken {
        self.stop.clone()
    }

    pub(crate) async fn join(&self) {
        loop {
            let Some(task) = self.inner.tasks.lock().unwrap().pop() else {
                return;
            };
            let role = task.role;
            // The lease puts its handle back when this join future is
            // cancelled. A later shutdown join can therefore still prove
            // completion for every admitted task.
            let mut joining = JoiningTask {
                group: self.inner.clone(),
                task: Some(task),
            };
            let result = (&mut joining.task.as_mut().unwrap().handle).await;
            let _completed = joining.task.take().unwrap();
            if let Err(error) = result {
                #[cfg(cello_internal_tests)]
                self.inner.joined_failures.fetch_add(1, Ordering::SeqCst);
                warn!(role, %error, "durability task stopped with an error");
            }
        }
    }

    #[cfg(cello_internal_tests)]
    pub(crate) fn roles_for_world(&self) -> Vec<&'static str> {
        self.inner
            .started_roles
            .lock()
            .unwrap()
            .iter()
            .copied()
            .collect()
    }

    #[cfg(cello_internal_tests)]
    pub(crate) fn live_roles_for_world(&self) -> Vec<&'static str> {
        self.inner
            .tasks
            .lock()
            .unwrap()
            .iter()
            .map(|task| task.role)
            .collect()
    }
}

pub(crate) struct LtxTaskOwner {
    tasks: TaskGroup,
    replica_close_tasks: TaskGroup,
}

impl LtxTaskOwner {
    pub(crate) fn request_stop(&self) {
        self.tasks.stop.request_stop();
    }

    pub(crate) async fn join(&self) {
        // Roots and their descendants share admission and completion tracking.
        // Once stopped, no successor can enter; joining this group therefore
        // needs no ordering between sync, compaction, and delayed requeues.
        self.tasks.join().await;
        // Release closes remain admissible after background work stops, until
        // shutdown seals their separate admission gate.
        self.replica_close_tasks.join().await;
    }
}

impl Drop for DurabilityRegistration {
    fn drop(&mut self) {
        let Some(registration) = self.registration.upgrade() else {
            return;
        };
        let mut state = registration.lock().unwrap();
        if state
            .current
            .as_ref()
            .is_some_and(|current| current.generation == self.generation)
        {
            state.current = None;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    pub min_txids: u64,
    /// L0 bytes since the last fold that queue one independently of the txid
    /// distance, so large rows do not accumulate a long uncompacted tail.
    pub min_bytes: u64,
    pub concurrency: usize,
}

/// The node settings that [`LtxRepl`] fixes at construction from the
/// environment, after parsing. [`LtxConfig::from_env`] is their only reader.
///
/// The settings travel as one value because a test process has one
/// environment. Every node of a simulated fleet runs in that process, so a
/// constructor that read the environment itself gave every node the same
/// values. A test that needed one node with its own value for a setting
/// such as the grace then needed a builder that overwrote a field after
/// construction, one per setting, and that test entered through a path
/// production never takes. Here production and a test both pass a config
/// to the same constructor body; only the source of the values differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LtxConfig {
    /// How old the newest object of a superseded epoch must be before epoch
    /// GC deletes the epoch, or `None` when epoch GC is off
    /// (`CELLO_LTX_RETENTION_SECS`, unset or `0`). `Some(0)` is on with no
    /// grace, a value the environment cannot express.
    pub(crate) epoch_gc_grace_ms: Option<u64>,
    /// Bytes per second a paged cell hydrates in the background, 0 for none
    /// (`CELLO_LTX_HYDRATE_MBPS`).
    pub(crate) hydrate_bytes_per_s: u64,
    /// The operator's switch for paged restore (`CELLO_LTX_PAGED`).
    pub(crate) paged_restore: bool,
    /// The chain size, in bytes, from which a restore pages instead of
    /// cloning (`CELLO_LTX_PAGED_MIN_MB`).
    pub(crate) paged_min_bytes: u64,
    /// The TRUNCATE checkpoint threshold of a non-Queue cell, 0 to disable
    /// truncation (`CELLO_LTX_TRUNCATE_PAGES`).
    pub(crate) truncate_pages: u32,
}

impl LtxConfig {
    /// Read the settings from the process environment.
    pub(crate) fn from_env() -> Self {
        Self::from_lookup(crate::env_vars::value)
    }

    /// Read the settings through `lookup`, which answers like
    /// [`crate::env_vars::value`]. A test supplies values here without
    /// mutating the process environment, which every parallel test shares.
    ///
    /// An unreadable or unparsable value falls back to the default instead
    /// of failing construction. `env_vars::validate` rejects some of these
    /// values at startup, but not all of them: it does not check
    /// `CELLO_LTX_HYDRATE_MBPS` or `CELLO_LTX_PAGED_MIN_MB`, and it checks
    /// `CELLO_LTX_TRUNCATE_PAGES` as a `u64`. A production node can
    /// therefore reach a fallback.
    fn from_lookup(lookup: impl Fn(&str) -> anyhow::Result<Option<String>>) -> Self {
        let optional_u64 = |name: &str| {
            lookup(name).and_then(|value| crate::env_vars::parse_optional::<u64>(name, value))
        };
        Self {
            epoch_gc_grace_ms: optional_u64("CELLO_LTX_RETENTION_SECS")
                .unwrap_or(None)
                .filter(|secs| *secs > 0)
                .map(|secs| secs.saturating_mul(1000)),
            hydrate_bytes_per_s: optional_u64("CELLO_LTX_HYDRATE_MBPS")
                .unwrap_or(None)
                .unwrap_or(DEFAULT_HYDRATE_MBPS)
                .saturating_mul(1 << 20),
            paged_restore: lookup("CELLO_LTX_PAGED")
                .and_then(|value| {
                    crate::env_vars::parse_flag("CELLO_LTX_PAGED", value.as_deref(), true)
                })
                .unwrap_or(true),
            paged_min_bytes: optional_u64("CELLO_LTX_PAGED_MIN_MB")
                .unwrap_or(None)
                .unwrap_or(DEFAULT_PAGED_MIN_MB)
                .saturating_mul(1 << 20),
            truncate_pages: lookup("CELLO_LTX_TRUNCATE_PAGES")
                .and_then(|value| {
                    crate::env_vars::parse_optional::<u32>("CELLO_LTX_TRUNCATE_PAGES", value)
                })
                .ok()
                .flatten()
                .unwrap_or(DEFAULT_TRUNCATE_PAGES),
        }
    }
}

struct CellCompaction {
    cell: String,
    epoch: u64,
    client: cello_ltx::BundleOverlayClient<SharedObjectStoreClient>,
    fetcher: Arc<SinkFetcher>,
    local_path: PathBuf,
    host: LtxHost,
    queue: mpsc::UnboundedSender<CompactionWork>,
    /// The epoch's first txid: 1, or the cut a paged epoch continues from.
    base_txid: u64,
    min_txids: u64,
    min_bytes: u64,
    /// L0 bytes uploaded since the last fold, the size trigger's measure.
    pending_bytes: AtomicU64,
    compacted_txid: AtomicU64,
    /// Ship rounds also queue work, so the failure delay must gate admission
    /// rather than only delaying the worker's own requeue task.
    retry_after_ms: AtomicU64,
    failures: AtomicU64,
    queued: AtomicBool,
    /// Set for good when the replica closes or a handoff completes. Nothing
    /// clears it.
    cancelled: AtomicBool,
    /// Set while a handoff snapshot holds compaction off, and cleared when
    /// that handoff attempt ends. See `HandoffCompactionPause`.
    handoff_paused: AtomicBool,
    cancel: Notify,
    #[cfg(all(test, cello_internal_tests))]
    finish_pause: Mutex<Option<Arc<CompactionFinishPauseForWorld>>>,
    /// Serializes threshold compaction with the final handoff snapshot. The
    /// handoff path pauses background work, then waits here before it reads
    /// the quiesced database image.
    run: tokio::sync::Mutex<()>,
}

impl CellCompaction {
    /// Whether a round must not start or continue: the replica closed, or a
    /// handoff snapshot holds compaction off.
    fn halted(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst) || self.handoff_paused.load(Ordering::SeqCst)
    }
}

/// Compaction held off for one handoff attempt, from the snapshot's start
/// until `evict` returns.
///
/// The pause is scoped to the attempt because the attempt can end without a
/// handoff. An abandoned eviction, or one whose release retries run past the
/// eviction deadline, restarts the cell in place on the same handle
/// (`StartRuntime` with no `Restore`). Any other failed attempt is retried,
/// and each retry takes a new pause. The pause used to be the permanent
/// `cancelled` flag, which nothing cleared, so the cell refused every
/// threshold fold and every epoch GC opener fold for the rest of its
/// activation. A reset at each failing return was rejected because a
/// new return path can forget it. The pause has its own flag so that its drop
/// cannot undo a `close_replica` cancel that raced the eviction.
///
/// A round that waited on the run lock behind the pause can still see a
/// stale cancel wake after an abandoned attempt and end `Cancelled` without
/// a requeue. Its fold then waits for the next threshold crossing or the
/// next epoch GC pass, instead of the rest of the activation.
struct HandoffCompactionPause(CellHandle);

impl HandoffCompactionPause {
    fn new(handle: &CellHandle) -> Option<Self> {
        let compaction = handle.compaction.as_ref()?;
        compaction.handoff_paused.store(true, Ordering::SeqCst);
        compaction.cancel.notify_waiters();
        Some(Self(handle.clone()))
    }

    /// End the pause with the handoff. Compaction stays cancelled for good,
    /// because a fold after the handoff would upload into an epoch that a
    /// successor has already superseded.
    fn handed_off(self) {
        cancel_compaction(&self.0);
    }
}

impl Drop for HandoffCompactionPause {
    fn drop(&mut self) {
        if let Some(compaction) = &self.0.compaction {
            compaction.handoff_paused.store(false, Ordering::SeqCst);
        }
    }
}

struct CompactionWork {
    cell: Weak<Cell>,
    queued_at_mono_ms: u64,
    /// The `trigger` label of the round's log event. It travels with the
    /// work, so an operator can tell a forced epoch opener from a threshold
    /// round.
    trigger: &'static str,
}

struct RemoteRestoreTiming {
    started_mono_ms: u64,
    from: u64,
    source_lookup_us: u64,
    plan_us: u64,
    /// The page map's build, on a paged activation; zero on a download.
    map_us: u64,
    download_us: u64,
    apply_us: u64,
    objects: usize,
    bytes: u64,
    levels: String,
    paged: bool,
}

/// A paged VFS registration that unregisters itself unless the activation
/// reaches its handle: every failure after the registration (the managed
/// db's open, the seed, the marker's upload, admission) otherwise leaked a
/// registration and the page map it holds, once per retry.
struct PagedRegistration(Option<String>);

impl PagedRegistration {
    fn keep(mut self) {
        self.0 = None;
    }
}

impl Drop for PagedRegistration {
    fn drop(&mut self) {
        if let Some(name) = self.0.take() {
            let _ = cello_ltx::paged_vfs::unregister_paged_vfs(&name);
        }
    }
}

/// `L0:n L1:m ...` for a plan, as the restore_plan event prints it.
fn by_level(plan: &[cello_ltx::FileInfo]) -> String {
    let mut counts = std::collections::BTreeMap::new();
    for info in plan {
        *counts.entry(info.level).or_insert(0usize) += 1;
    }
    counts
        .iter()
        .map(|(level, count)| format!("L{level}:{count}"))
        .collect::<Vec<_>>()
        .join(" ")
}

struct HandoffSnapshot {
    max_txid: TXID,
    data: Vec<u8>,
}

/// One resident cell's replication state: the `cello_ltx::Db` shadowing its WAL
/// (behind a `std::sync::Mutex` because the `rusqlite` handle is `!Sync` and
/// must never cross an `.await`, so every capture+upload runs inside a
/// `spawn_blocking` closure) plus the durability tickets the output gate waits
/// on. `req_seq` counts durability requests; `synced_seq` is the highest ticket
/// a completed background sync captured. A write waits for `synced_seq >= its
/// ticket`, so concurrent writes to one cell ride a single batched upload —
/// and, because a sync credits only tickets whose writes committed before it
/// started (which the sync's `db.sync` captures), never one it did not upload.
struct Cell {
    /// A handoff snapshot of this cell did not publish inside the durability
    /// deadline, or its size showed that it cannot. Every later eviction of
    /// this epoch proves durability through the L0 chain instead of building
    /// and timing out on the same snapshot again.
    snapshot_declined: AtomicBool,
    /// The paged VFS registered for this activation (paged restore only). The
    /// local db is then a sparse cache of the cut: every reader of the file
    /// must go through this VFS, the file is not a reusable eviction baseline,
    /// and a handoff snapshot built from it would publish hole-zeros as data.
    paged_vfs: Option<String>,
    hydration: Option<Arc<CellHydration>>,
    replica: Mutex<Option<Replica<SharedObjectStoreClient>>>,
    /// The same epoch-prefix client the replica holds, for uploads that run
    /// off the replica mutex.
    client: SharedObjectStoreClient,
    req_seq: AtomicU64,
    synced_seq: AtomicU64,
    /// Highest ticket whose write is fsync'd on every ensemble member —
    /// the log tier's proof. The gate accepts either proof, so this stays 0
    /// forever when no shipper is installed.
    shipped_seq: AtomicU64,
    /// Highest ticket included in a submitted fleet round. It can run ahead
    /// of `shipped_seq` while the pipeline is in flight, so the next capture
    /// does not submit the same write again.
    submitted_seq: AtomicU64,
    /// Highest TXID credited by the fleet (or already covered by the bucket).
    /// A captured round can credit after removal, so stopped-tail checks
    /// retain this live watermark rather than a snapshot of the last credit.
    shipped_txid: Arc<AtomicU64>,
    /// Highest TXID included in a submitted fleet round. A pipeline reset
    /// rolls it back to `shipped_txid`, so the failed tail is retried once.
    submitted_txid: AtomicU64,
    /// A staged upload can finish after the active handle is removed. Retain
    /// only this monotone proof with its dirty tail, not the closed runtime.
    durable_txid: Arc<AtomicU64>,
    /// Highest TXID the PER-CELL prefix provably covers through this
    /// handle: the restored position at open, advanced only by the
    /// per-cell sync upload. `durable_txid` cannot serve this role —
    /// the bundle flush credits it, and the graceful seal already
    /// learned that every ack counter counts bundle credits. Together
    /// with `compacted_txid` (the drain's per-cell L1 fold) it bounds
    /// what a successor restore will actually see, which is what
    /// `note_undrained_tail` compares against (#473).
    percell_txid: AtomicU64,
    /// Whether this residency itself PUT an object under this cell epoch's
    /// per-cell prefix and got a success response. That response is the
    /// same evidence the eviction visibility LISTs look for: on the
    /// qualified stores the successor's LIST finds what our PUT acked, and
    /// nothing deletes under a live epoch prefix, so the mark cannot go
    /// stale. Set only by this handle's own uploads — never by the restore
    /// seed, bundle credits, or recovered tails — so every other case keeps
    /// the LIST.
    uploaded_epoch_objects: AtomicBool,
    /// Whether this epoch's restorable chain starts at TXID 1 in this
    /// epoch's per-cell prefix with every object in this handle's upload
    /// journal, so an eviction may skip its handoff snapshot (0078 lazy
    /// handoff). Set once at activation: true for a fresh activation and
    /// for a reused `.evicted`/`.hibernated` image (eviction stripped the
    /// LTX metadata, so the reopened image starts a new writer generation
    /// at TXID 1 whose first L0 carries the full image, exactly like a
    /// fresh cell), false for a remote restore or paged continuation
    /// (txids continue an older epoch), a same-epoch resume (pre-resume
    /// uploads are unjournaled), and uncertified local rows.
    chain_from_txid1_journaled: bool,
    /// Every per-cell object key this residency itself uploaded under this
    /// cell epoch's prefix (level, min txid, max txid), in upload order,
    /// capped at `EPOCH_UPLOAD_JOURNAL_MAX`. The eviction trim deletes the
    /// entries its handoff snapshot supersedes from these keys alone, with
    /// no LIST: the keys name only this epoch (sync L0s, compaction L1s,
    /// same-epoch recovery uploads), so an older epoch's objects and this
    /// epoch's snapshot are never candidates. A hot cell over a long epoch
    /// uploads far more rounds than the cap; once full, later uploads stay
    /// unjournaled and remain as restore-invisible garbage for epoch GC.
    epoch_uploads: Mutex<Vec<(i32, u64, u64)>>,
    /// The newest handoff ticket (0: none). While `synced_seq` has not reached
    /// it, per-cell captures skip the time-based checkpoint
    /// (`Db::sync_before_close`): its sealing write would commit a
    /// content-free txid that this cell would upload as its own L0 just
    /// before the snapshot that supersedes it. A ticket instead of a flag
    /// keeps the exemption until a capture covers the handoff, even when a
    /// capture that started before `handoff_wait` runs in between.
    handoff_ticket: AtomicU64,
    /// Whether the upload journal above ever filled and dropped an entry.
    /// The lazy handoff counts journaled objects to bound the successor
    /// restore; a filled journal makes that count a lie, so an overflowed
    /// journal keeps the snapshot (0078).
    epoch_uploads_overflowed: AtomicBool,
    /// Set while a sync for this cell is in flight, so the loop never runs two
    /// at once for one cell (they would serialize on the mutex and waste work).
    syncing: AtomicBool,
    /// Wall-clock ms of the last completed sync, the pacing anchor: with a
    /// healthy shipper the bucket runs at most one upload per flush interval
    /// behind, which is the tier's stated lag budget.
    last_sync_ms: AtomicU64,
    /// The highest `req_seq` a capture of this cell has begun for, and the
    /// monotonic ms when that capture began. A waiter reads them to learn
    /// whether the upload covering its ticket is in flight (the fixed proof
    /// budget then runs from the capture) or still queued behind other cells'
    /// uploads (the wait extends while the node proves anything). See
    /// `cello_logic::durability`. A retry of the same tickets keeps the
    /// original start, so a persistently failing upload still gives up one
    /// budget after it first began.
    capture_seq: AtomicU64,
    capture_started_ms: AtomicU64,
    /// Node-wide: monotonic ms of the last proof any cell landed, shared by
    /// every handle. The queued-wait rule anchors on it.
    node_proof_ms: Arc<AtomicU64>,
    /// Notified when `synced_seq` advances (or a sync fails), waking waiters.
    ready: Notify,
    compaction: Option<CellCompaction>,
    #[cfg(all(test, cello_internal_tests))]
    sync_credit_pause: Mutex<Option<Arc<SyncCreditPauseForWorld>>>,
    #[cfg(all(test, cello_internal_tests))]
    observer_cell: String,
    #[cfg(all(test, cello_internal_tests))]
    observer_epoch: u64,
    #[cfg(all(test, cello_internal_tests))]
    durability_ticket_receipts: Mutex<Vec<LtxDurabilityTicketReceiptForWorldV1>>,
    #[cfg(all(test, cello_internal_tests))]
    upload_round_receipts: Mutex<Vec<LtxUploadRoundReceiptForWorldV1>>,
    #[cfg(all(test, cello_internal_tests))]
    fleet_credit_receipts: Mutex<Vec<LtxFleetCreditReceiptForWorldV1>>,
    #[cfg(all(test, cello_internal_tests))]
    fleet_capture_receipts: Mutex<Vec<LtxFleetCaptureReceiptForWorld>>,
}
type CellHandle = Arc<Cell>;

/// The successor still needs a per-cell fold, but epoch replacement needs
/// only bucket coverage. Keep the ending bound and both live proofs together:
/// a late upload can release the epoch barrier, but a late fleet credit must
/// extend that barrier before any old fragments become abandonable.
struct RetainedTail {
    acked_txid: u64,
    shipped_txid: Arc<AtomicU64>,
    durable_txid: Arc<AtomicU64>,
}

/// Both direct L0 drains and L1 folds publish per-cell coverage. Reading only
/// the direct watermark makes recovery re-upload rows that L1 already holds.
fn percell_coverage(handle: &Cell) -> u64 {
    handle.percell_txid.load(Ordering::SeqCst).max(
        handle.compaction.as_ref().map_or(0, |compaction| {
            compaction.compacted_txid.load(Ordering::SeqCst)
        }),
    )
}

/// Credit an upload that the per-cell prefix holds through `txid`. A per-cell
/// object is also bucket coverage, so both watermarks move together. Per-cell
/// coverage is published first: a concurrent stop reads the bucket credit
/// before coverage, and the reverse order can invent an undrained tail in a
/// bucket-only session with no log record, which makes its next restore fail.
fn credit_percell_upload(handle: &Cell, txid: u64) {
    handle.percell_txid.fetch_max(txid, Ordering::SeqCst);
    handle.durable_txid.fetch_max(txid, Ordering::SeqCst);
}
/// Whether the operator enabled bucket-posture bundles: off by default, and
/// only an explicit `CELLO_BUCKET_BUNDLES=1` enables it. The format-3 fleet
/// gate still governs (see `bucket_bundles_enabled`). Pure value helper
/// keeps tests deterministic without touching the process environment;
/// startup rejects any other value before a flush can read it
/// (`env_vars::validate`).
fn bucket_bundles_env_value(value: Option<&str>) -> bool {
    matches!(value.map(str::trim), Some("1"))
}

/// Production read of the operator switch, per flush (not per process).
pub(crate) fn bucket_bundles_env_on() -> bool {
    bucket_bundles_env_value(std::env::var("CELLO_BUCKET_BUNDLES").ok().as_deref())
}

/// The longest linger `CELLO_BUCKET_BUNDLE_LINGER_MS` accepts: one bucket round
/// trip's worth of extra ack latency is the point of the knob, and anything near
/// the durability timeout would only turn into failed writes.
const BUCKET_BUNDLE_LINGER_MAX_MS: u64 = 1_000;

/// Parse `CELLO_BUCKET_BUNDLE_LINGER_MS`: absent, empty or unparsable is 0 (no
/// linger), and values above the cap clamp to it.
fn bucket_bundle_linger_value(value: Option<&str>) -> Duration {
    let ms = value
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(0)
        .min(BUCKET_BUNDLE_LINGER_MAX_MS);
    Duration::from_millis(ms)
}

fn bucket_bundle_linger() -> Duration {
    bucket_bundle_linger_value(
        std::env::var("CELLO_BUCKET_BUNDLE_LINGER_MS")
            .ok()
            .as_deref(),
    )
}
/// Cap a bucket bundle by rows and bytes; the flush splits above the cap into one
/// PUT per chunk, each with its own sequence. The limits are policy (triggers stay
/// out of the core): small enough to bound one object, large enough that ordinary
/// agent workloads flush in one PUT.
const BUCKET_BUNDLE_MAX_ROWS: usize = 1024;
const BUCKET_BUNDLE_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Split captured entries into PUT-sized chunks by rows and payload bytes. Empty
/// input yields no chunks; the caller credits without a PUT in that case.
fn split_bundle_entries(
    entries: Vec<cello_ltx::bundle::BundleEntry>,
) -> Vec<Vec<cello_ltx::bundle::BundleEntry>> {
    let mut chunks: Vec<Vec<cello_ltx::bundle::BundleEntry>> = Vec::new();
    let mut current: Vec<cello_ltx::bundle::BundleEntry> = Vec::new();
    let mut current_bytes: usize = 0;
    for entry in entries {
        let entry_bytes = entry.bytes.len();
        if !current.is_empty()
            && (current.len() >= BUCKET_BUNDLE_MAX_ROWS
                || current_bytes.saturating_add(entry_bytes) > BUCKET_BUNDLE_MAX_BYTES)
        {
            chunks.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
        current_bytes = current_bytes.saturating_add(entry_bytes);
        current.push(entry);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}
/// Whether `sync_loop` must skip per-cell L0 uploads because the bundle loop owns
/// them. Fleet bundling is paced (`paced && bundle_active`); bucket bundling stops
/// per-cell uploads even though `paced` is false (no shipper) — the bundle PUT is
/// the ack path there. Pure predicate so tests pin the pacing without a daemon.
fn sync_should_bundle(paced: bool, bundle_active: bool, bucket_bundling: bool) -> bool {
    (paced && bundle_active) || bucket_bundling
}

/// Whether the bundle loop currently owns tiering, for a sync worker to yield
/// to. Mirrors the scan's `sync_should_bundle` predicate exactly (same inputs,
/// same order): a worker spawned before the arm must observe the same answer
/// the next scan would give, not a second convention that can drift.
fn bundle_owns_tiering(registration: &Arc<Mutex<RegistrationState>>, paced: bool) -> bool {
    registered_durability(registration).is_some_and(|targets| {
        sync_should_bundle(
            paced,
            targets.manager.bundle_active(),
            targets.manager.bucket_bundling(),
        )
    })
}

/// Cells with uncredited tickets, for the bucket linger gate: the linger is a
/// batching window, and batching needs concurrency. Holding it while a lone
/// cell is dirty would add the whole window to one ack for no fewer PUTs.
fn dirty_cells(cells: &Mutex<BTreeMap<(String, u64), CellHandle>>) -> usize {
    cells
        .lock()
        .unwrap()
        .values()
        .filter(|cell| cell.req_seq.load(Ordering::SeqCst) > cell.synced_seq.load(Ordering::SeqCst))
        .count()
}

/// Cap the per-cell upload journal: entries are one upload round each, and a
/// long-lived hot cell uploads far more rounds in one epoch than the trim can
/// usefully delete in one pass (8192 entries ≈ 200 KiB). Every upload's max
/// is at or below the closing snapshot's, so every journaled entry is
/// trimmable; once full, later uploads simply stay unjournaled and remain as
/// restore-invisible garbage for epoch GC.
const EPOCH_UPLOAD_JOURNAL_MAX: usize = 8192;

/// Remember one per-cell object key this residency uploaded, so the eviction
/// trim can delete it from the journal alone, with no LIST. Call only after
/// the PUT's success response: a journaled key names an object the successor
/// restore provably finds, and the trim filter keeps only keys the handoff
/// snapshot supersedes.
fn note_epoch_upload(handle: &Cell, level: i32, min_txid: u64, max_txid: u64) {
    let mut journal = handle
        .epoch_uploads
        .lock()
        .expect("epoch upload journal poisoned");
    if journal.len() < EPOCH_UPLOAD_JOURNAL_MAX {
        journal.push((level, min_txid, max_txid));
    } else {
        // The journal stopped recording, so the chain is longer than the
        // count says. The lazy handoff (0078) counts journaled objects to
        // bound the successor restore; an overflowed journal keeps the
        // snapshot instead of trusting that count.
        handle
            .epoch_uploads_overflowed
            .store(true, Ordering::SeqCst);
    }
}

/// Whether the bucket covers every cell row in one shipped batch, so that
/// followers can truncate it. The ledger holds each handle after the cell
/// stops, so every path that leaves rows only in the bucket, and every path
/// that deletes them, must credit `durable_txid`, or this batch and every
/// later batch stay on follower disks until the next epoch.
fn ship_batch_covered(cells: &[(CellHandle, u64)]) -> bool {
    cells
        .iter()
        .all(|(handle, txid)| handle.durable_txid.load(Ordering::SeqCst) >= *txid)
}

/// A capture covering tickets up to `captured` has begun for this cell. Only
/// a capture that reaches new tickets moves the start: a retry of the same
/// tickets keeps the budget it has already spent.
fn note_capture(handle: &Cell, captured: u64) {
    if handle.capture_seq.load(Ordering::SeqCst) < captured {
        handle
            .capture_started_ms
            .store(asyncrt::mono_ms(), Ordering::SeqCst);
        handle.capture_seq.fetch_max(captured, Ordering::SeqCst);
    }
}

/// A proof landed for this cell: the node is making progress.
fn note_proof(handle: &Cell) {
    handle
        .node_proof_ms
        .fetch_max(asyncrt::mono_ms(), Ordering::SeqCst);
}

/// A writer that refuses to grow past its budget, so an oversized snapshot
/// stops at the budget instead of materializing the whole database.
struct BoundedWriter<'a> {
    data: &'a mut Vec<u8>,
    budget: usize,
}

impl std::io::Write for BoundedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.data.len().saturating_add(buf.len()) > self.budget {
            self.data.resize(self.budget, 0);
            return Err(std::io::Error::other("handoff snapshot exceeds its budget"));
        }
        self.data.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Bytes per millisecond of deadline that a handoff snapshot may occupy:
/// 8 MiB/s, below the fleet-to-bucket rate seen on every lab run, so a
/// snapshot inside the budget uploads inside the deadline with margin. The
/// default deadline therefore carries 80 MiB; a larger database hands off
/// through its L0 chain. Without this bound a 120 MB cell built its image,
/// timed out, and retried forever, unreachable, on the 2026-09-03 fleet.
const SNAPSHOT_BYTES_PER_MS: u64 = 8 * 1024;

/// The default deadline for one durability proof, in seconds. A sustained
/// write burst (a large upload landing in one cell) can legitimately need
/// more than one capture+upload cycle to prove; on a slow or busy store a
/// fixed 10s fences the cell out from under an active request, so operators
/// can raise it via `CELLO_LTX_DURABILITY_TIMEOUT_SECS`.
const DEFAULT_DURABILITY_TIMEOUT_SECS: u64 = 10;

/// The TRUNCATE checkpoint threshold, in pages. Chosen from a measured
/// threshold curve (2026-08-26): the read per sync is flat from 64 to 128
/// pages (~68 KB against ~2.4 MB untruncated) and 128 pays half the
/// boundary snapshots of 64, while 256 and above let the stale-tail reads
/// back in. A 512 KB WAL cap keeps every capture's read small for the price
/// of one boundary snapshot of the database per checkpoint cycle — which is
/// only a small-cell price: the Db also requires the WAL to outgrow the
/// database before it truncates, so a whale pays that image once per
/// database's worth of writes, not once per 512 KB (its chain grew as the
/// square of its size on the 2026-09-02 fleet before that bound).
const DEFAULT_TRUNCATE_PAGES: u32 = 128;

/// The chain size from which a restore pages, in MiB. A clone of this much
/// takes a few seconds at fleet bandwidth and fits a node's memory with
/// room to spare; the 5 GB whale's clone did not (OOM at 7.75 GB RSS on an
/// 8 GB node), and its paged takeover moved 90 MiB.
const DEFAULT_PAGED_MIN_MB: u64 = 256;

/// The default background hydration rate of a paged cell, in MiB/s. A 2 GB
/// whale fills in about two minutes; a fault of the foreground never waits
/// on a hydration step, which fetches outside the hydration lock.
const DEFAULT_HYDRATE_MBPS: u64 = 16;

/// Pages one hydration step faults before it yields to the pacer: one run.
const HYDRATE_STEP_PAGES: u32 = 256;

/// The background fill of a paged cell: cancelled when the cell closes.
struct CellHydration {
    cancelled: AtomicBool,
    complete: AtomicBool,
}

/// How long a read of a stream's retired mark answers coverage questions
/// before the next question reads it again.
const RETIRED_MARK_TTL_MS: u64 = 5 * 60 * 1000;

/// How long a read that found no retired mark answers before the next
/// question reads it again. A mark is read only for a stream this node does
/// not hold and only for a row its per-cell prefix does not cover, which is
/// rare outside recovery, so a short wait costs little; it bounds how long a
/// node keeps the rows of an epoch another owner retired.
const RETIRED_MARK_ABSENT_TTL_MS: u64 = 30 * 1000;

/// Restores of an unowned snapshot before a missing object is an error. Each
/// retry lists the epochs again, so it fails only when the bucket keeps losing
/// an object that every fresh chain still needs.
const RESTORE_SNAPSHOT_ATTEMPTS: usize = 3;

/// The body of `cells/<stream>/ltx/retired.json`.
#[derive(serde::Serialize, serde::Deserialize)]
struct RetiredMark {
    retired_below: u64,
}

/// What one epoch-GC pass did for one stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochGcOutcome {
    /// The chain's base; the stream's rows below it are covered.
    pub retired_below: u64,
    /// The epoch prefixes this pass deleted.
    pub deleted: Vec<u64>,
    /// No further pass for this activation can delete more.
    pub settled: bool,
}

/// One superseded epoch below a stream's chain base.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochBelowBase {
    pub listed: cello_logic::epoch_gc::ListedEpoch,
    /// Bytes under the epoch's prefix.
    pub bytes: u64,
}

fn retired_mark_key(prefix: &str, cell: &str) -> cello_ltx::object_store::path::Path {
    cello_ltx::object_store::path::Path::from(format!("{prefix}cells/{cell}/ltx/retired.json"))
}

/// Read one stream's retired mark from the bucket; `None` when it has none.
pub(crate) async fn read_retired_mark(
    store: &dyn ObjectStore,
    prefix: &str,
    cell: &str,
) -> anyhow::Result<Option<u64>> {
    match store.get(&retired_mark_key(prefix, cell)).await {
        Ok(result) => {
            let bytes = result.bytes().await?;
            Ok(Some(
                serde_json::from_slice::<RetiredMark>(&bytes)?.retired_below,
            ))
        }
        Err(cello_ltx::object_store::Error::NotFound { .. }) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// The most superseded epochs one owner pass summarizes and deletes. A cell
/// with thousands of them spends one listing per epoch, so a pass takes the
/// oldest few and the next pass continues.
const EPOCH_GC_EPOCHS_PER_PASS: usize = 64;

/// A read-only view of one stream's epochs for epoch GC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochScan {
    /// The newest listed epoch.
    pub newest: u64,
    /// The restore chain over every listed epoch, oldest first.
    pub chain: Vec<u64>,
    /// The oldest listed epochs below the chain's base, ascending, at most
    /// the scan's bound.
    pub below: Vec<EpochBelowBase>,
    /// More epochs below the base exist than the scan summarized.
    pub more_below: bool,
}

/// Scan one stream's epochs the way a restore sees them: build the restore
/// chain over every listed epoch with the restore code, require its restore
/// plan to resolve, and summarize each epoch below its base. `None` when the
/// stream has no epoch or no complete chain. The owner's pass and
/// `cello cell gc --dry-run` share this, so the dry run reports what the
/// owner would decide.
pub async fn scan_epochs(
    store: &dyn ObjectStore,
    prefix: &str,
    cell: &str,
    max_below: usize,
    client_for: impl Fn(u64) -> ObjectStoreClient,
) -> anyhow::Result<Option<EpochScan>> {
    use cello_ltx::object_store::path::Path as ObjPath;
    use futures_util::StreamExt as _;
    let base_path = ObjPath::from(format!("{prefix}cells/{cell}/ltx"));
    let mut listed: Vec<u64> = store
        .list_with_delimiter(Some(&base_path))
        .await?
        .common_prefixes
        .iter()
        .filter_map(|p| p.filename()?.strip_prefix('e')?.parse().ok())
        .collect();
    listed.sort_unstable();
    let Some(&newest) = listed.last() else {
        return Ok(None);
    };
    let clients = listed.iter().map(|e| (*e, client_for(*e))).collect();
    let chain = match EpochChain::build(clients).await {
        Ok(chain) => chain,
        Err(cello_ltx::Error::TxNotAvailable) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if replica::calc_restore_plan(&chain, TXID(0)).await.is_err() {
        return Ok(None);
    }
    let chain: Vec<u64> = chain.spans().into_iter().map(|(e, _)| e).collect();
    let base = chain.first().copied().unwrap_or(newest);
    let mut below = Vec::new();
    let superseded: Vec<u64> = listed.into_iter().filter(|e| *e < base).collect();
    let more_below = superseded.len() > max_below;
    for epoch in superseded.into_iter().take(max_below) {
        let path = ObjPath::from(format!("{prefix}cells/{cell}/ltx/e{epoch}"));
        let mut objects = store.list(Some(&path));
        let (mut newest_ms, mut bytes) = (0_u64, 0_u64);
        while let Some(object) = objects.next().await {
            let object = object?;
            newest_ms = newest_ms.max(object.last_modified.timestamp_millis().max(0) as u64);
            bytes = bytes.saturating_add(object.size);
        }
        below.push(EpochBelowBase {
            listed: cello_logic::epoch_gc::ListedEpoch { epoch, newest_ms },
            bytes,
        });
    }
    Ok(Some(EpochScan {
        newest,
        chain,
        below,
        more_below,
    }))
}

pub struct LtxRepl {
    /// Local root: cell dbs live at `watch/<cell>/ltx/e<epoch>/db.sqlite`.
    watch: PathBuf,
    /// The object-metadata name for an LTX header timestamp. Azure refuses
    /// a hyphen in a metadata name, so the fleet bucket's dialect picks it.
    timestamp_metadata_key: TimestampMetadataKey,
    bucket: String,
    /// The bucket spec's key prefix: empty, or slash-terminated.
    prefix: String,
    endpoint: Option<String>,
    region: String,
    credentials: Option<StorageCredentials>,
    /// One connection pool for the whole node, shared by every cell client.
    store: Arc<dyn ObjectStore>,
    ltx_host: LtxHost,
    /// An explicit SQLite VFS for both managed connections. Production keeps
    /// this unset.
    vfs_name: Option<String>,
    cells: Arc<Mutex<BTreeMap<(String, u64), CellHandle>>>,
    stopped_cells: Arc<Mutex<BTreeMap<(String, u64), CellHandle>>>,
    /// Serializes a release admission with the final shutdown snapshot. A
    /// release that crosses shutdown must belong to either the retained close
    /// group or the stopped-cell snapshot, never neither and never both.
    replica_close_gate: Mutex<()>,
    /// Keys whose owned blocking close failed before it proved that it took
    /// the replica. A later release can admit one retry for the same handle.
    failed_release_closes: Arc<Mutex<BTreeSet<(String, u64)>>>,
    replica_close_stop: StopToken,
    replica_close_tasks: TaskGroup,
    /// The node's root task group, for the per-cell background hydration.
    tasks: TaskGroup,
    /// Woken when a cell's `committed` advances, so the background loop syncs
    /// without polling; a slow tick backstops any missed notification.
    dirty: Arc<Notify>,
    /// Shared by every activation, so the restore bound is per node, not per
    /// cell. The generic LTX restore keeps its sequential compatibility path.
    restore_slots: Arc<Semaphore>,
    compaction_queue: Option<mpsc::UnboundedSender<CompactionWork>>,
    compaction_min_txids: u64,
    compaction_min_bytes: u64,
    /// Restore a taken-over cell by paging its pages in on demand (a fault-in
    /// SQLite VFS) instead of downloading the whole chain, and continue its
    /// chain from the cut. On unless `CELLO_LTX_PAGED=0`.
    paged_restore: AtomicBool,
    /// Whether every live lease in the fleet reads a paged epoch
    /// (`cello_logic::format::PAGED_FORMAT`). Off until the fleet sampler has
    /// seen every lease at that minimum, so a node in a rolling update clones
    /// the way the old release does instead of writing an epoch the old
    /// release cannot restore. `CELLO_LTX_PAGED` is the operator's switch;
    /// this one is the fleet's.
    paged_fleet: AtomicBool,
    /// Whether every live lease reads bucket-posture bundle proofs
    /// (`cello_logic::format::BUCKET_BUNDLES_FORMAT`). Off until the first
    /// fleet sample (mirrors the paged gate): a failed or missing sample
    /// closes it. The per-flush bundle path additionally requires the
    /// operator switch (opt-in; `CELLO_BUCKET_BUNDLES=1` enables it),
    /// see `bucket_bundles_enabled`.
    bucket_bundles_fleet: AtomicBool,
    /// Test-only override for the `CELLO_BUCKET_BUNDLES` leg, so deterministic
    /// tests do not mutate the process environment. `None` (production) reads
    /// the env per flush; `Some(on)` forces the answer.
    #[cfg(test)]
    bucket_env_override: Mutex<Option<bool>>,
    /// The chain size, in bytes, from which a restore pages instead of
    /// cloning (`CELLO_LTX_PAGED_MIN_MB`). Below it the clone is cheaper:
    /// one download the node can afford, no fault path, no hydration.
    paged_min_bytes: AtomicU64,
    /// Bytes per second a paged cell hydrates in the background, 0 for none
    /// (`CELLO_LTX_HYDRATE_MBPS`). A hydrated cell reads only its file.
    hydrate_bytes_per_s: u64,
    /// One paged cell hydrates at a time: a node with several whales pages
    /// them all and fills them in turn.
    hydrations: Arc<Semaphore>,
    /// Preserved eviction snapshots, tracked in memory so the local-cache
    /// prune answers without walking the data directory. See
    /// [`crate::replication::PreservedCache`].
    preserved: Mutex<crate::replication::PreservedCache>,
    /// The live-named epochs `prune_unclean_restart` deleted. The ownership
    /// confirmation after the first node lease reads them once, so a crash
    /// restart still finds the cells that were resident when it died.
    pruned_at_boot: Mutex<Vec<cello_logic::LocalCell>>,
    /// Woken when a gate ticket arrives, so the ship loop group-commits
    /// without polling.
    dirty_ship: Arc<Notify>,
    /// Woken when a gate ticket arrives, so the bucket-posture bundle loop
    /// flushes without polling. Its own `Notify`, not `dirty`: `notify_one`
    /// hands one permit to one waiter, and `sync_loop` always waits on
    /// `dirty`, so a shared `Notify` would let the sync loop take the bundle
    /// loop's wakeup and stall the bucket flush.
    dirty_bundle: Arc<Notify>,
    /// Cells whose last epoch ended quietly with acked rows outside the
    /// per-cell layout (`note_undrained_tail`'s predicate). The ending
    /// itself cannot write — release serves fenced nodes — so the
    /// SUCCESSOR folds: the next activation gathers the cell's stranded
    /// tail per-cell before it restores (#473). In-memory only; a
    /// process death hands the same duty to boot recovery, which drains
    /// whole predecessor sessions.
    dirty_tails: Mutex<BTreeMap<String, BTreeMap<u64, RetainedTail>>>,
    registration: Arc<Mutex<RegistrationState>>,
    stop: StopToken,
    task_owner: Mutex<Option<LtxTaskOwner>>,
    /// Background epoch trims in flight (evict hands off deletes and
    /// returns). The pair lets tests await a trim deterministically;
    /// production code never waits on it.
    trim_inflight: Arc<AtomicU64>,
    trim_changed: Arc<Notify>,
    #[cfg(cello_internal_tests)]
    activation_install_pause: Mutex<Option<Arc<LtxActivationInstallPause>>>,
    #[cfg(cello_internal_tests)]
    close_local_replicas_pause: Mutex<Option<Arc<LtxReplicaClosePause>>>,
    #[cfg(cello_internal_tests)]
    panic_next_release_close: AtomicBool,
    /// The budget for one durability proof, in milliseconds. Fixed at
    /// construction, so the hot write path pays no environment lookup.
    durability_timeout_ms: u64,
    /// Monotonic ms of the last proof any cell on this node landed. Every
    /// cell handle shares it; see `Cell::node_proof_ms`.
    node_proof_ms: Arc<AtomicU64>,
    /// The largest handoff snapshot the durability deadline can carry. A
    /// larger database hands off through its L0 chain without an attempt.
    snapshot_budget_bytes: AtomicU64,
    /// The highest txid this process has put in the bucket per cell epoch,
    /// through a per-cell ship, a handoff snapshot, a recovered segment, or
    /// an epoch marker, plus any bucket listing it had to do. Bundle GC and
    /// the recovery gather ask for this watermark once per cell per pass;
    /// answering from the bucket cost ten LISTs per cell, so a pass of a
    /// 2 s budget judged about one bundle against roughly one new bundle a
    /// second, and the retained backlog grew for the life of the process.
    /// On 2026-09-03 a dead session's recovery read 209 of them.
    covered_by_cell: Mutex<BTreeMap<(String, u64), u64>>,
    /// Each stream's retired mark (`cells/<stream>/ltx/retired.json`) as
    /// last read or written, with the monotonic time of that read. A
    /// coverage question for a stream this node does not hold reads the mark
    /// when the per-cell answer does not reach the row, so a read is reused
    /// for [`RETIRED_MARK_TTL_MS`], or [`RETIRED_MARK_ABSENT_TTL_MS`] when it
    /// found none; a mark only rises, so a stale one only under-reports and
    /// keeps a row.
    retired_marks: Mutex<BTreeMap<String, (Option<u64>, u64)>>,
    /// How old the newest object of a superseded epoch must be before epoch
    /// GC deletes the epoch, or `None` when epoch GC is off
    /// (`CELLO_LTX_RETENTION_SECS`, unset or `0`). Deletion is off by default:
    /// turning it on is the operator's decision, not a side effect of an
    /// upgrade.
    epoch_gc_grace_ms: Option<u64>,
    /// The cell's TRUNCATE checkpoint threshold. Passive checkpoints never
    /// shrink the WAL FILE, so after every periodic restart the capture's
    /// tail read spans the stale high-water region — ~350 KB per sync on
    /// the fleet ledger at the upstream threshold. `DEFAULT_TRUNCATE_PAGES`
    /// unless `CELLO_LTX_TRUNCATE_PAGES` overrides it; 0 disables
    /// truncation (the upstream behavior). Queue cells always disable it
    /// because each truncate boundary emits a full image of their unbounded
    /// backlog.
    truncate_pages: Option<u32>,
}

impl LtxRepl {
    /// Cfg-gated constructor over an injected store.
    #[cfg(any(test, cello_internal_tests))]
    pub fn start_with_store_for_test(watch: &Path, store: Arc<dyn ObjectStore>) -> Self {
        Self::start_with_store(watch, store, None, 0)
    }

    /// Build the production loop topology over an injected object store,
    /// with the settings that production reads from the environment.
    #[cfg(any(test, cello_internal_tests))]
    pub fn start_with_store(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        compaction: Option<CompactionConfig>,
        flush_ms: u64,
    ) -> Self {
        Self::start_with_store_and_optional_vfs(
            watch,
            store,
            compaction,
            flush_ms,
            None,
            LtxConfig::from_env(),
        )
    }

    /// Build the same loop topology with `config`, and route managed SQLite
    /// through `vfs`. A simulated fleet passes each node its own config.
    #[cfg(cello_internal_tests)]
    pub(crate) fn start_with_store_on_vfs(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        compaction: Option<CompactionConfig>,
        flush_ms: u64,
        vfs: &str,
        config: LtxConfig,
    ) -> Self {
        Self::start_with_store_and_optional_vfs(
            watch,
            store,
            compaction,
            flush_ms,
            Some(vfs.to_string()),
            config,
        )
    }

    #[cfg(any(test, cello_internal_tests))]
    fn start_with_store_and_optional_vfs(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        compaction: Option<CompactionConfig>,
        flush_ms: u64,
        vfs_name: Option<String>,
        config: LtxConfig,
    ) -> Self {
        Self::assemble(
            watch,
            store,
            TimestampMetadataKey::default(),
            "test".into(),
            String::new(),
            None,
            "auto".into(),
            None,
            compaction,
            flush_ms,
            deterministic_ltx_host(),
            vfs_name,
            DEFAULT_DURABILITY_TIMEOUT_SECS * 1_000,
            config,
        )
    }

    /// Cfg-gated constructor with additive L1 compaction enabled.
    #[cfg(cello_internal_tests)]
    pub fn start_with_compaction_for_test(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        min_txids: u64,
        concurrency: usize,
    ) -> Self {
        Self::assemble(
            watch,
            store,
            TimestampMetadataKey::default(),
            "test".into(),
            String::new(),
            None,
            "auto".into(),
            None,
            Some(CompactionConfig {
                min_txids,
                min_bytes: COMPACTION_MAX_INPUT_BYTES / 2,
                concurrency,
            }),
            0,
            deterministic_ltx_host(),
            None,
            DEFAULT_DURABILITY_TIMEOUT_SECS * 1_000,
            LtxConfig::from_env(),
        )
    }

    pub(crate) fn start(
        watch: &Path,
        backend: crate::bucket::StorageBackend,
        bucket: String,
        prefix: String,
        endpoint: Option<String>,
        region: String,
        credentials: Option<StorageCredentials>,
    ) -> anyhow::Result<Self> {
        let compaction = compaction_config_from_env()?;
        // Everything downstream of the store is backend-agnostic already,
        // so the dialect decides construction and the metadata name, and
        // nothing else.
        let store = match backend {
            crate::bucket::StorageBackend::Gcs => crate::bucket::gcs_replica_store(&bucket)?,
            crate::bucket::StorageBackend::Azure => crate::bucket::azure_replica_store(&bucket)?,
            crate::bucket::StorageBackend::S3 => {
                node_config(&bucket, endpoint.as_deref(), &region, credentials.as_ref())
                    .build_store()
                    .map_err(|error| anyhow!("build shared object store: {error}"))?
            }
            crate::bucket::StorageBackend::Local => {
                Arc::new(crate::local_store::LocalStore::open(&bucket)?)
            }
        };
        // Azure blob metadata names must be C# identifiers, so the standard
        // Litestream key cannot carry its hyphen there. External Litestream
        // tooling reads that key, therefore an az:// replica gives up
        // Litestream-tool timestamp restore. cello never reads it back.
        let timestamp_metadata_key = match backend {
            crate::bucket::StorageBackend::Azure => TimestampMetadataKey::Underscore,
            crate::bucket::StorageBackend::S3
            | crate::bucket::StorageBackend::Gcs
            | crate::bucket::StorageBackend::Local => TimestampMetadataKey::Litestream,
        };
        // The tiering flush interval: with a healthy shipper, at most one
        // upload per cell per interval; it is simultaneously the bucket lag
        // budget. 0 disables pacing.
        let flush_ms = crate::env_vars::with_default("CELLO_LOG_FLUSH_MS", 2000)? as u64;
        // Saturating: an absurd seconds value must clamp, not wrap through
        // `as_millis() as u64` into a short deadline.
        let durability_timeout_ms = crate::env_vars::positive_or(
            "CELLO_LTX_DURABILITY_TIMEOUT_SECS",
            DEFAULT_DURABILITY_TIMEOUT_SECS,
        )?
        .saturating_mul(1_000);
        let repl = Self::assemble(
            watch,
            store,
            timestamp_metadata_key,
            bucket,
            prefix,
            endpoint,
            region,
            credentials,
            compaction,
            flush_ms,
            production_ltx_host(),
            None,
            durability_timeout_ms,
            LtxConfig::from_env(),
        );
        // Local retention at startup (upstream #311): drop live-named epoch
        // dirs a newer local epoch supersedes. Clean-reload boots already
        // pruned to the exact resident set at shutdown, so this is a no-op
        // there; other boots restore from the bucket.
        repl.prune_superseded_epochs_all();
        Ok(repl)
    }

    /// The one constructor body for every field and every background loop.
    #[allow(clippy::too_many_arguments)]
    fn assemble(
        watch: &Path,
        store: Arc<dyn ObjectStore>,
        timestamp_metadata_key: TimestampMetadataKey,
        bucket: String,
        prefix: String,
        endpoint: Option<String>,
        region: String,
        credentials: Option<StorageCredentials>,
        compaction: Option<CompactionConfig>,
        flush_ms: u64,
        ltx_host: LtxHost,
        vfs_name: Option<String>,
        durability_timeout_ms: u64,
        config: LtxConfig,
    ) -> Self {
        let cells: Arc<Mutex<BTreeMap<(String, u64), CellHandle>>> = Arc::default();
        let dirty = Arc::new(Notify::new());
        let dirty_ship = Arc::new(Notify::new());
        let dirty_bundle = Arc::new(Notify::new());
        let registration: Arc<Mutex<RegistrationState>> = Arc::default();
        let stop = StopToken::new();
        let tasks = TaskGroup::new(stop.clone());
        let replica_close_stop = StopToken::new();
        let replica_close_tasks = TaskGroup::new(replica_close_stop.clone());
        let preserved = Mutex::new(crate::replication::PreservedCache::new(
            ltx_host.filesystem(),
        ));
        // Retain every root until the unique durability owner claims them.
        // Dropping a task handle detaches the task, so a cloneable replicator
        // cannot be the final lifecycle capability.
        tasks.spawn_owned(
            "ltx_sync",
            sync_loop(
                cells.clone(),
                dirty.clone(),
                Arc::new(Semaphore::new(sync_concurrency())),
                registration.clone(),
                stop.clone(),
                tasks.clone(),
                flush_ms,
            ),
        );
        tasks.spawn_owned(
            "ltx_ship",
            ship_loop(
                cells.clone(),
                dirty_ship.clone(),
                registration.clone(),
                stop.clone(),
                crate::queue_batching::timing().log_ms,
            ),
        );
        tasks.spawn_owned(
            "ltx_bundle",
            bundle_loop(
                cells.clone(),
                registration.clone(),
                stop.clone(),
                flush_ms,
                dirty_bundle.clone(),
            ),
        );
        let compaction_queue =
            compaction.map(|config| start_compaction_loop(config, tasks.clone()));
        Self {
            watch: watch.to_path_buf(),
            timestamp_metadata_key,
            bucket,
            prefix,
            endpoint,
            region,
            credentials,
            store,
            ltx_host,
            vfs_name,
            cells,
            stopped_cells: Arc::new(Mutex::new(BTreeMap::new())),
            replica_close_gate: Mutex::new(()),
            failed_release_closes: Arc::new(Mutex::new(BTreeSet::new())),
            replica_close_stop,
            replica_close_tasks: replica_close_tasks.clone(),
            tasks: tasks.clone(),
            dirty,
            restore_slots: Arc::new(Semaphore::new(RESTORE_DOWNLOAD_CONCURRENCY)),
            compaction_queue,
            compaction_min_txids: compaction.map_or(0, |config| config.min_txids),
            compaction_min_bytes: compaction.map_or(u64::MAX, |config| config.min_bytes),
            paged_restore: AtomicBool::new(config.paged_restore),
            paged_fleet: AtomicBool::new(false),
            bucket_bundles_fleet: AtomicBool::new(false),
            #[cfg(test)]
            bucket_env_override: Mutex::new(None),
            paged_min_bytes: AtomicU64::new(config.paged_min_bytes),
            hydrate_bytes_per_s: config.hydrate_bytes_per_s,
            hydrations: Arc::new(Semaphore::new(1)),
            preserved,
            pruned_at_boot: Mutex::new(Vec::new()),
            dirty_ship,
            dirty_bundle,
            dirty_tails: Mutex::new(BTreeMap::new()),
            registration,
            stop: stop.clone(),
            task_owner: Mutex::new(Some(LtxTaskOwner {
                tasks,
                replica_close_tasks,
            })),
            trim_inflight: Arc::new(AtomicU64::new(0)),
            trim_changed: Arc::new(Notify::new()),
            #[cfg(cello_internal_tests)]
            activation_install_pause: Mutex::new(None),
            #[cfg(cello_internal_tests)]
            close_local_replicas_pause: Mutex::new(None),
            #[cfg(cello_internal_tests)]
            panic_next_release_close: AtomicBool::new(false),
            durability_timeout_ms,
            node_proof_ms: Arc::new(AtomicU64::new(0)),
            snapshot_budget_bytes: AtomicU64::new(
                durability_timeout_ms.saturating_mul(SNAPSHOT_BYTES_PER_MS),
            ),
            covered_by_cell: Mutex::new(BTreeMap::new()),
            retired_marks: Mutex::new(BTreeMap::new()),
            epoch_gc_grace_ms: config.epoch_gc_grace_ms,
            truncate_pages: Some(config.truncate_pages),
        }
    }

    pub(crate) fn take_task_owner(&self) -> LtxTaskOwner {
        self.task_owner
            .lock()
            .unwrap()
            .take()
            .expect("the LTX task owner was already claimed")
    }

    pub(crate) fn shutdown_local_fallback(&self) {
        self.stop.request_stop();
        self.registration.lock().unwrap().current = None;
        let _close_gate = self.replica_close_gate.lock().unwrap();
        self.replica_close_stop.request_stop();
        // A capture holds the replica mutex across filesystem work. Closing a
        // replica here would turn the process-deadline fallback into another
        // unbounded wait. Detach every handle without touching its replica, so
        // awaited shutdown can close it after all admitted workers have joined.
        // The close gate makes this snapshot atomic with release admission.
        let cells = std::mem::take(&mut *self.cells.lock().unwrap());
        self.stopped_cells.lock().unwrap().extend(cells);
    }

    pub(crate) fn start_close_local_replicas(&self) -> Option<asyncrt::TaskHandle<()>> {
        let _close_gate = self.replica_close_gate.lock().unwrap();
        let mut cells = std::mem::take(&mut *self.cells.lock().unwrap());
        cells.append(&mut *self.stopped_cells.lock().unwrap());
        let mut failed_release_closes = self.failed_release_closes.lock().unwrap();
        for key in cells.keys() {
            failed_release_closes.remove(key);
        }
        drop(failed_release_closes);
        if cells.is_empty() {
            return None;
        }
        #[cfg(cello_internal_tests)]
        let pause = self.take_replica_close_pause_for_world();
        Some(asyncrt::blocking(move || {
            #[cfg(cello_internal_tests)]
            pause_replica_close_for_world(pause);
            for ((cell, epoch), handle) in cells {
                close_replica_or_warn(&handle, &cell, epoch);
            }
        }))
    }

    /// Install the manager's shipping and bundle operations as one unit.
    ///
    /// The shipper gates fleet proofs on its open bucket log record.
    /// Registration can precede that opening. The registration stores
    /// weak references because the manager already owns this replicator.
    pub(crate) fn register_durability(
        &self,
        manager: Arc<crate::node_log::NodeLogManager>,
    ) -> Option<DurabilityRegistration> {
        self.register_targets(RegisteredTargets {
            shipper: manager.clone(),
            manager,
        })
    }

    #[cfg(all(test, cello_internal_tests))]
    pub(crate) fn register_durability_for_world(
        &self,
        manager: Arc<crate::node_log::NodeLogManager>,
        shipper: Arc<dyn Shipper>,
    ) -> Option<DurabilityRegistration> {
        self.register_targets(RegisteredTargets { shipper, manager })
    }

    fn register_targets(&self, targets: RegisteredTargets) -> Option<DurabilityRegistration> {
        let targets = Arc::new(targets);
        let mut state = self.registration.lock().unwrap();
        // Shutdown publishes stop before it clears this slot under the same
        // mutex. A registration that linearizes first is cleared by shutdown;
        // one that linearizes later observes stop and cannot resurrect it.
        if self.stop.is_stopped() {
            return None;
        }
        state.next_generation = state
            .next_generation
            .checked_add(1)
            .expect("durability registration generation exhausted");
        let generation = state.next_generation;
        state.current = Some(RegisteredDurability {
            generation,
            targets: Arc::downgrade(&targets),
        });
        drop(state);
        self.dirty_ship.notify_one();
        self.dirty_bundle.notify_one();
        self.dirty.notify_one();
        Some(DurabilityRegistration {
            registration: Arc::downgrade(&self.registration),
            generation,
            _targets: targets,
        })
    }

    /// Wait for the active replicas' shipped frames to reach the bucket.
    /// This excludes stopped cells, so abandoning a fragment also requires
    /// `all_fragment_rows_tiered` to check their retained coverage proofs.
    pub fn all_shipped_tiered(&self) -> bool {
        Self::all_cells_shipped_tiered(&self.cells.lock().unwrap())
    }

    fn all_cells_shipped_tiered(cells: &BTreeMap<(String, u64), CellHandle>) -> bool {
        cells.values().all(|cell| {
            cell.durable_txid.load(Ordering::SeqCst) >= cell.shipped_txid.load(Ordering::SeqCst)
        })
    }

    /// The epoch-change barrier includes stopped cells. Their staged uploads
    /// can complete after removal, so consult their retained live watermarks.
    /// Requiring their fold markers to disappear would strand fleet repair
    /// until each stopped cell receives another activation or the node exits.
    pub(crate) fn all_fragment_rows_tiered(&self) -> bool {
        let cells = self.cells.lock().unwrap();
        Self::all_cells_shipped_tiered(&cells)
            && self.dirty_tails.lock().unwrap().values().all(|epochs| {
                epochs.values().all(|tail| {
                    tail.durable_txid.load(Ordering::SeqCst)
                        >= tail
                            .acked_txid
                            .max(tail.shipped_txid.load(Ordering::SeqCst))
                })
            })
    }

    /// The final process-seal barrier. A cell that leaves the active map can
    /// still have fleet-acked rows outside the per-cell layout. Ending a cell
    /// installs that fact in `dirty_tails` before it removes the active handle,
    /// and this method reads the same two stores in that order. Therefore, the
    /// final check observes either the undrained active handle or its marker.
    pub(crate) fn all_tails_tiered(&self) -> bool {
        let cells = self.cells.lock().unwrap();
        Self::all_cells_shipped_tiered(&cells) && self.dirty_tails.lock().unwrap().is_empty()
    }

    /// The highest TXID the cell's per-cell prefix already covers, over
    /// every level, or `u64::MAX` for a retired epoch. The caller passes the
    /// highest row it needs covered. Recovery uses the answer to skip
    /// re-uploading rows the drain points (compaction, eviction sync) have
    /// already folded in, bundle GC and the graceful seal's scan to decide
    /// which bundle rows are safe to drop, and the compaction overlay to
    /// skip rows the prefix holds.
    ///
    /// An epoch below the stream's retired mark is covered through every
    /// txid: the chain's base holds all of its acknowledged rows, and epoch
    /// GC may have deleted its prefix, so a listing would answer zero and
    /// keep its bundles and re-upload its rows forever. The mark is read only
    /// when the per-cell answer does not reach `needed`, so a covered row
    /// costs no mark read on any fleet, and a mark read never stands between
    /// a question and a per-cell answer that already settles it. Every node
    /// reads the mark, whether or not it runs epoch GC itself: while a fleet
    /// rolls the setting out, a node without it must still seal over, drop,
    /// and not re-upload the rows of an epoch another node deleted. An
    /// absent mark is cached for [`RETIRED_MARK_ABSENT_TTL_MS`], which bounds
    /// how long such a node lags a new mark.
    ///
    /// A stream this node holds a handle for answers from its per-cell
    /// watermark alone and reads no mark. A busy cell's bundle rows sit above
    /// that watermark until the next drain point, so reading the mark there
    /// would cost a GET on every bundle-GC tick for every active cell, on
    /// every fleet. Holding the handle means the epoch is this node's current
    /// one, or a fenced node's whose successor may have retired it, because
    /// teardown is asynchronous; that answer is lower than the truth, which
    /// keeps rows and is safe. Once the handle goes, its coverage moves to
    /// the remembered watermark and the mark decides rows above it. The
    /// compaction overlay therefore reads no mark while its cell is
    /// resident, and a fenced compactor can write merged rows back under a
    /// retired prefix; no restore reads a prefix below the base, and a later
    /// owner pass deletes it again once the grace passes, measured from the
    /// rewritten objects. The current owner keeps it when it is the epoch
    /// just before its own, because a plan never deletes that epoch.
    ///
    /// This is safe only because the base holds every acknowledged row of
    /// every epoch below it. A successor claims a cell only after the prior
    /// owner's node-log session is recovered and sealed, and recovery puts
    /// every acknowledged row in the per-cell prefix first, so the
    /// successor's restore, and with it the base, contains those rows; a
    /// local wake restores from the file its own rows were shipped from. A
    /// recovery upload that lands after the seal can only repeat rows the
    /// base already holds.
    pub async fn covered_txid(&self, cell: &str, epoch: u64, needed: u64) -> u64 {
        let key = (cell.to_string(), epoch);
        // Held is decided by the handle, not its value: a fresh cell that has
        // not reached a drain point holds a zero watermark and must not fall
        // through to the mark on every tick either.
        let (held, resident) = {
            let cells = self.cells.lock().unwrap();
            let handle = cells.get(&key);
            (
                handle.is_some(),
                handle
                    .map(|handle| percell_coverage(handle))
                    .filter(|covered| *covered > 0),
            )
        };
        // A handoff snapshot raises the remembered watermark while the
        // evicting handle is still resident, so the higher of the two holds.
        let known = self.covered_by_cell.lock().unwrap().get(&key).copied();
        if held {
            return resident.max(known).unwrap_or(0);
        }
        let covered = match resident.max(known) {
            Some(covered) => covered,
            // Nothing this process put there: a cell it never served, or one
            // it has not shipped yet. Ask the bucket once, with one listing
            // of the epoch's whole prefix rather than one per level, and
            // remember the answer; the watermark only rises, so a remembered
            // value never over-reports. A restarting node recovers thousands
            // of cells it never served, so this listing, and a mark read for
            // a row it does not cover, is its recovery cost.
            None => match self.client_for(cell, epoch).max_txid_all_levels().await {
                Ok(txid) => {
                    // Zero is a useful answer too: undrained cells otherwise
                    // force the same empty-prefix LIST on every GC tick. A
                    // read error is not zero coverage and must remain
                    // retryable.
                    self.note_covered(cell, epoch, txid.0);
                    txid.0
                }
                Err(_) => 0,
            },
        };
        if covered >= needed {
            return covered;
        }
        if cello_logic::epoch_gc::retired(epoch, self.retired_mark(cell).await) {
            return u64::MAX;
        }
        covered
    }

    /// Remember that the bucket holds this cell epoch through `txid`.
    fn note_covered(&self, cell: &str, epoch: u64, txid: u64) {
        let mut covered = self.covered_by_cell.lock().unwrap();
        let entry = covered.entry((cell.to_string(), epoch)).or_insert(0);
        *entry = (*entry).max(txid);
    }

    fn retired_mark_key(&self, cell: &str) -> cello_ltx::object_store::path::Path {
        retired_mark_key(&self.prefix, cell)
    }

    /// The stream's retired mark, by its exact stream name. A read error
    /// answers the cached mark, or none: a missing mark keeps rows, which
    /// costs space, never data.
    pub async fn retired_mark(&self, cell: &str) -> Option<u64> {
        let now = asyncrt::mono_ms();
        if let Some((mark, read_at)) = self.retired_marks.lock().unwrap().get(cell) {
            let ttl = if mark.is_some() {
                RETIRED_MARK_TTL_MS
            } else {
                RETIRED_MARK_ABSENT_TTL_MS
            };
            if now.saturating_sub(*read_at) < ttl {
                return *mark;
            }
        }
        let Ok(mark) = read_retired_mark(self.store.as_ref(), &self.prefix, cell).await else {
            // A failed read answers the cached mark and restarts its wait:
            // 30 s with nothing cached, and 5 min for an expired cached mark,
            // which a failed reread keeps. A store outage therefore does not
            // cost a GET on every question, and a stale answer is only low.
            return self.remember_retired(cell, None, now);
        };
        self.remember_retired(cell, mark, now)
    }

    /// Record that the stream's epochs below `below` are retired. The owner
    /// writes this before it deletes any of them, so a prefix is never gone
    /// while its bundle rows still look uncovered. The write is not
    /// conditional: an owner that stalled after its checks can write an
    /// older, lower base over a successor's mark. Every written base was a
    /// real base once, so a lower mark only makes some rows look uncovered
    /// again, which costs a re-upload and a later pass, not data.
    async fn record_retired(&self, cell: &str, below: u64) -> anyhow::Result<()> {
        let body = serde_json::to_vec(&RetiredMark {
            retired_below: below,
        })?;
        self.store
            .put(&self.retired_mark_key(cell), body.into())
            .await
            .map_err(|error| anyhow!("record retired epochs of {cell} below e{below}: {error}"))?;
        self.remember_retired(cell, Some(below), asyncrt::mono_ms());
        Ok(())
    }

    /// Cache a mark read or written at `at`. The cached value never falls,
    /// because an older read racing a newer write must not lower it.
    /// Returns the mark the cache now holds.
    fn remember_retired(&self, cell: &str, mark: Option<u64>, at: u64) -> Option<u64> {
        let mut marks = self.retired_marks.lock().unwrap();
        let entry = marks.entry(cell.to_string()).or_insert((None, at));
        entry.0 = entry.0.max(mark);
        entry.1 = at;
        entry.0
    }

    /// The epoch-GC grace in milliseconds, or `None` when epoch GC is off.
    pub(crate) fn epoch_gc_grace_ms(&self) -> Option<u64> {
        self.epoch_gc_grace_ms
    }

    /// The streams this node holds, with the epoch it holds each at.
    pub(crate) fn resident_epochs(&self) -> Vec<(String, u64)> {
        self.cells.lock().unwrap().keys().cloned().collect()
    }

    /// Delete one stream's superseded epoch prefixes, as the owner of
    /// `epoch` (denoland/celld#240).
    ///
    /// The steps run in a fixed order, and the order is the fence:
    ///
    /// 1. Build the restore chain over every listed epoch, the owner's
    ///    included, with the restore code. Stop unless its newest span is
    ///    `epoch` and the chain's full restore plan resolves. The first
    ///    proves the owner's own opener is listed, so a successor that claims
    ///    the cell after step 2 lists it too and restores from a base at or
    ///    above this one. A successor that claimed before step 2 fails it.
    /// 2. `confirm_owner`: read the ownership record and require this node
    ///    at `epoch`. Reading before step 1 would let a fenced owner delete
    ///    the base of a successor that restored while its epoch was still
    ///    empty.
    /// 3. Record the stream's retired mark, so coverage treats the rows of
    ///    every epoch below the base as covered before any prefix is gone.
    /// 4. Delete the planned prefixes. A crash here leaves the mark, and the
    ///    next owner deletes the rest.
    ///
    /// A conditional write on the ownership record would add nothing: the
    /// cell can move between any check and the delete. What makes the late
    /// delete safe is that an epoch below the base never re-enters a chain.
    /// The argument assumes a list-after-write consistent store.
    pub async fn gc_superseded_epochs<F, Fut>(
        &self,
        cell: &str,
        epoch: u64,
        grace_ms: u64,
        confirm_owner: F,
    ) -> anyhow::Result<Option<EpochGcOutcome>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<bool>>,
    {
        // The cheap facts first, so a cell that cannot decide anything costs
        // no listing. A cell that is not resident here has no owner here to
        // run this pass. A paged cell reads pages from its activation chain's
        // objects until its fill completes, and a paged cell whose fill is
        // off never completes one, so only a positive completion unpins it.
        let pinned_reads = match self.cells.lock().unwrap().get(&(cell.to_string(), epoch)) {
            None => return Ok(None),
            Some(handle) => {
                handle.paged_vfs.is_some()
                    && handle
                        .hydration
                        .as_ref()
                        .is_none_or(|fill| !fill.complete.load(Ordering::SeqCst))
            }
        };
        // The plan below refuses pinned reads too; returning here skips the
        // listings it would not use.
        if pinned_reads {
            return Ok(None);
        }
        let Some(scan) = scan_epochs(
            self.store.as_ref(),
            &self.prefix,
            cell,
            EPOCH_GC_EPOCHS_PER_PASS,
            |e| self.client_for(cell, e),
        )
        .await?
        else {
            return Ok(None);
        };
        if scan.newest != epoch {
            if scan.newest < epoch {
                self.queue_epoch_opener(cell, epoch);
            }
            return Ok(None);
        }
        let below: Vec<_> = scan.below.iter().map(|b| b.listed).collect();
        // The grace compares the store's upload times with this node's
        // clock, so skew shortens or lengthens it. It is a margin for a slow
        // unowned reader, not part of the safety argument.
        let now_ms = asyncrt::wall_ms().max(0) as u64;
        let Some(plan) = cello_logic::epoch_gc::plan(
            epoch,
            &scan.chain,
            &below,
            self.retired_mark(cell).await,
            pinned_reads,
            now_ms,
            grace_ms,
        ) else {
            return Ok(None);
        };
        if !confirm_owner().await? {
            return Ok(None);
        }
        if plan.record {
            self.record_retired(cell, plan.retired_below).await?;
        }
        for victim in &plan.delete {
            self.client_for(cell, *victim)
                .delete_all()
                .await
                .map_err(|error| anyhow!("delete {cell} e{victim}: {error}"))?;
            self.note_covered(cell, *victim, u64::MAX);
        }
        // Only the previous epoch may stay below the base once the grace has
        // passed; the next activation's pass deletes it.
        let settled = !scan.more_below
            && below
                .iter()
                .all(|l| plan.delete.contains(&l.epoch) || l.epoch + 1 == epoch);
        Ok(Some(EpochGcOutcome {
            retired_below: plan.retired_below,
            deleted: plan.delete,
            settled,
        }))
    }

    /// Folds a cloned activation's durable rows into its first L1 object, the
    /// opener that epoch GC waits for. Under bundled tiering a write reaches
    /// only the node's bundles, and the first per-cell object is otherwise
    /// the threshold fold (`CELLO_LTX_COMPACTION_MIN_TXIDS`) or a handoff
    /// snapshot. A cell with fewer writes per activation then never lists
    /// its opener and never deletes a superseded epoch; the 2026-09-30 GCE
    /// run measured that. The fold opens at TXID 1, so it makes the owner's
    /// epoch the chain's base, and the next pass decides. A paged epoch
    /// already lists its marker, and a cell without a durable row has
    /// nothing to fold. A handoff snapshot would upload the whole database
    /// where the fold uploads only the rows the activation wrote, and it
    /// would pause compaction and wait out a running round while it reads
    /// the image.
    ///
    /// A failed forced fold retries only on the next epoch GC pass that runs
    /// after the failure backoff; a pass inside the backoff enqueues nothing.
    /// The failure requeue in `compact_cell` goes through the threshold
    /// check, which a cell below the threshold does not pass, and that is
    /// intended; a threshold round after the backoff can also produce the
    /// opener. An unconditional requeue would turn one GC pass into a
    /// standing retry loop on the failure backoff, whether or not a pass
    /// still wants the opener, so a store fault would put every such cell
    /// into that loop.
    fn queue_epoch_opener(&self, cell: &str, epoch: u64) {
        let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        else {
            return;
        };
        let Some(compaction) = &handle.compaction else {
            return;
        };
        if compaction.base_txid == 1
            && compaction.compacted_txid.load(Ordering::SeqCst) == 0
            && handle.durable_txid.load(Ordering::SeqCst) > 0
        {
            enqueue_compaction(&handle, "epoch_opener");
        }
    }

    /// Highest contiguous TXID prefix proved by the live per-cell L0/L1 objects.
    ///
    /// Unlike `covered_txid`, this is an activation safety proof. A maximum
    /// past a hole cannot seed a replica because doing so would preserve the
    /// hole forever. Store errors also fail the proof instead of becoming zero.
    /// A zero would claim that the local directory can replay the complete
    /// epoch even though the store did not provide the required durable bound.
    async fn contiguous_covered_txid(&self, cell: &str, epoch: u64) -> anyhow::Result<u64> {
        let client = self.client_for(cell, epoch);
        let mut files = Vec::new();
        for level in 0..=1 {
            files.extend(
                client
                    .ltx_files(level, TXID(0))
                    .await
                    .map_err(|error| anyhow!("list {cell} e{epoch} L{level}: {error}"))?,
            );
        }
        files.sort_unstable_by_key(|file| (file.min_txid.0, file.max_txid.0));

        let mut covered = 0_u64;
        for file in files {
            if file.min_txid.0 > covered.saturating_add(1) {
                break;
            }
            covered = covered.max(file.max_txid.0);
        }
        Ok(covered)
    }

    /// Recovery's primitive: PUT one gathered L0 segment to the exact key
    /// the dead leader's own upload would have used. Idempotent by key.
    pub async fn upload_raw_l0(
        &self,
        cell: &str,
        epoch: u64,
        min_txid: u64,
        max_txid: u64,
        bytes: &[u8],
    ) -> anyhow::Result<()> {
        self.client_for(cell, epoch)
            .write_ltx_file(0, TXID(min_txid), TXID(max_txid), bytes)
            .await
            .map_err(|error| {
                anyhow!("upload recovered l0 {cell} e{epoch} t{min_txid}-{max_txid}: {error}")
            })?;
        self.note_covered(cell, epoch, max_txid);
        // A same-epoch upload (a fold into a still-resident epoch) joins the
        // trim journal; uploads into dead epochs have no resident handle and
        // stay out of it, so the trim never names another epoch's objects.
        if let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        {
            note_epoch_upload(&handle, 0, min_txid, max_txid);
        }
        Ok(())
    }

    /// Merge contiguous single-transaction LTX segments into one segment
    /// covering the whole range. Recovery uses it to upload each cell's
    /// gathered tail as ONE object instead of one per row: the lab's chaos
    /// soak measured per-kill outages growing 133 s -> 353 s because every
    /// crash added hundreds of single-row L0 objects for the object
    /// store's same-key throttling to fight and the next restore plan to
    /// read. Returns None when the rows are not a contiguous ascending
    /// chain — the caller falls back to per-row uploads, never guessing.
    pub fn merge_l0_rows(rows: &[(u64, Vec<u8>)]) -> Option<Vec<u8>> {
        if rows.len() < 2 {
            return None;
        }
        if rows.windows(2).any(|pair| pair[1].0 != pair[0].0 + 1) {
            return None;
        }
        let readers: Vec<std::io::Cursor<&[u8]>> = rows
            .iter()
            .map(|(_, bytes)| std::io::Cursor::new(bytes.as_slice()))
            .collect();
        let mut compactor = cello_ltx::compactor::Compactor::new(Vec::new(), readers);
        // The gathered frames are no-checksum WAL-segment files, so the
        // merged header must be too — a checksummed output header fails
        // encode validation against checksum-less inputs (the same flag
        // ReplicaCompactor sets for the L0->L1 fold).
        compactor.header_flags = cello_ltx::ltx::HEADER_FLAG_NO_CHECKSUM;
        match compactor.compact() {
            Ok(()) => Some(compactor.into_writer()),
            Err(error) => {
                // The fallback is safe (per-row uploads) but must never be
                // silent again: a swallowed error here hid a dead merge
                // through a full fleet round.
                warn!(%error, rows = rows.len(), "recovery tail merge failed; per-row fallback");
                None
            }
        }
    }

    fn db_path(&self, cell: &str, epoch: u64) -> PathBuf {
        self.watch
            .join(cell)
            .join("ltx")
            .join(format!("e{epoch}"))
            .join("db.sqlite")
    }

    fn truncate_pages_for_cell(&self, cell: &str) -> Option<u32> {
        let queue = cell
            .split_once(':')
            .is_some_and(|(class, _)| class == crate::deploy::QUEUE_CLASS);
        if queue {
            // A Queue database grows with its backlog. A fixed WAL threshold
            // therefore turns every TRUNCATE boundary into an allocation and
            // LTX object proportional to every undelivered message (#472).
            // PASSIVE checkpoints still backfill the database, and the sparse
            // capture path reads the live tail without retaining the stale WAL
            // region, so Queue cells do not need that small-cell tradeoff.
            Some(0)
        } else {
            self.truncate_pages
        }
    }

    /// A per-cell client over the shared store, keyed to the cell's epoch
    /// prefix. `cells/<cell>/ltx/e<epoch>` matches [`Self::db_path`]'s remote
    /// twin so the same coordinates address local and replica state.
    fn client_for(&self, cell: &str, epoch: u64) -> ObjectStoreClient {
        let mut config = node_config(
            &self.bucket,
            self.endpoint.as_deref(),
            &self.region,
            self.credentials.as_ref(),
        );
        config.path = format!("{}cells/{cell}/ltx/e{epoch}", self.prefix);
        config.timestamp_metadata_key = self.timestamp_metadata_key;
        ObjectStoreClient::with_store(config, self.store.clone())
    }

    /// Highest epoch under `cells/<cell>/ltx/` that holds any LTX — the newest
    /// durable copy to restore on takeover.
    /// Every epoch under `cells/<cell>/ltx/` that holds any LTX, ascending. A
    /// restore composes them into one chain: a paged epoch continues the
    /// chain it paged in instead of opening with a snapshot, so the objects a
    /// restore needs can span epochs.
    async fn nonempty_epochs(&self, cell: &str) -> anyhow::Result<Vec<u64>> {
        use cello_ltx::object_store::path::Path as ObjPath;
        let base = ObjPath::from(format!("{}cells/{cell}/ltx", self.prefix));
        let listing = self.store.list_with_delimiter(Some(&base)).await?;
        let mut epochs: Vec<u64> = listing
            .common_prefixes
            .iter()
            .filter_map(|prefix| {
                prefix
                    .filename()
                    .and_then(|name| name.strip_prefix('e'))
                    .and_then(|value| value.parse::<u64>().ok())
            })
            .collect();
        epochs.sort_unstable();
        Ok(epochs)
    }

    /// The restore view over a cell's epochs below `epoch`: the chain from the
    /// newest non-empty epoch down to the one that opens with a snapshot.
    async fn epoch_chain(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Option<EpochChain<ObjectStoreClient>>> {
        let epochs = self.nonempty_epochs(cell).await?;
        let Some(&newest) = epochs.last() else {
            return Ok(None);
        };
        anyhow::ensure!(
            newest < epoch,
            "refusing to restore {cell} from used epoch {newest} into writer epoch {epoch}"
        );
        let clients = epochs
            .into_iter()
            .map(|e| (e, self.client_for(cell, e)))
            .collect();
        Ok(Some(EpochChain::build(clients).await?))
    }

    /// Does the bucket hold any LTX for this cell at this epoch? The fail-closed
    /// eviction gate: never delete the last local copy of state the bucket
    /// cannot restore.
    pub async fn epoch_replicated(&self, cell: &str, epoch: u64) -> bool {
        if self.journaled_key_replicated(cell, epoch).await {
            return true;
        }
        let client = self.client_for(cell, epoch);
        matches!(client.has_any_object().await, Ok(true))
    }

    /// Whether the bucket holds the newest key this residency's upload
    /// journal names, via a HEAD on that exact key — the same key the
    /// uploader wrote and the eviction trim deletes. A miss or an error, or
    /// an empty journal (an upload-free or resumed residency with no handle
    /// entry), answers false so the caller falls back to the listing, which
    /// stays the fail-closed proof. Costs one Class-B HEAD instead of one
    /// Class-A LIST when the journal names the epoch's objects.
    async fn journaled_key_replicated(&self, cell: &str, epoch: u64) -> bool {
        let journaled = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .and_then(|handle| handle.epoch_uploads.lock().unwrap().last().copied());
        let Some((level, min_txid, max_txid)) = journaled else {
            return false;
        };
        matches!(
            self.client_for(cell, epoch)
                .ltx_file_exists(level, TXID(min_txid), TXID(max_txid))
                .await,
            Ok(true)
        )
    }

    /// Whether this residency itself PUT an object under the cell epoch's
    /// per-cell prefix. The mark is set only on a success response through
    /// a per-cell epoch client of this handle, so it names the same objects
    /// `epoch_replicated` would list. A missing handle (or an upload-free
    /// one) answers false and keeps the LIST.
    fn residency_uploaded(&self, cell: &str, epoch: u64) -> bool {
        self.cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .is_some_and(|handle| handle.uploaded_epoch_objects.load(Ordering::SeqCst))
    }

    pub async fn activate(
        &self,
        options: ActivationOptions<'_>,
    ) -> anyhow::Result<ActivationResult> {
        self.activate_with(options, true).await
    }

    /// `activate`, where `allow_paged` false always clones: a facet's
    /// database is opened by a plain connection that has no fault-in VFS.
    pub(crate) async fn activate_with(
        &self,
        options: ActivationOptions<'_>,
        allow_paged: bool,
    ) -> anyhow::Result<ActivationResult> {
        anyhow::ensure!(
            !self.stop.is_stopped(),
            "LTX replication stopped before activation started"
        );
        let ActivationOptions {
            cell,
            epoch,
            fresh,
            took_over,
            resume_local,
            // Consumed by the interlock before the fold; the field stays
            // on the options because the core's claim still names it.
            prior: _,
        } = options;
        {
            let _close_gate = self.replica_close_gate.lock().unwrap();
            anyhow::ensure!(
                !self
                    .stopped_cells
                    .lock()
                    .unwrap()
                    .contains_key(&(cell.to_string(), epoch)),
                "managed replica close is incomplete for {cell} epoch {epoch}"
            );
        }
        let dst = self.db_path(cell, epoch);
        self.ltx_host.create_dir_all(dst.parent().unwrap())?;

        // The takeover interlock moved OUT of this path (lease-fold): the
        // decision core gates foreign takeovers on the folded lease state
        // it already reads (Effect::RecoverNodeLog), and the boot order
        // recovers this node's own predecessor before the lease installs,
        // so by the time any activation reaches here the bucket is
        // provably complete for both the takeover and the named-me path.

        // Reuse a preserved local eviction snapshot only as the previous
        // epoch's baseline. Eviction removes the LTX metadata, so reopening
        // that SQLite image starts a new writer generation at TXID 1. Pairing
        // it with the same remote epoch would mix that new lineage with the
        // old tail (#158). A clean process reload is the separate
        // `resume_local` path: it retains both the live database and its LTX
        // metadata, so it can safely continue the existing epoch.
        // `.evicted` is the current name; `.hibernated` is what releases
        // before 2026-08-05 wrote. Accept both, so an upgrade reuses the
        // snapshots already on disk instead of restoring every cell from the
        // bucket. Writes always use the new name, so the old one dies out.
        let legacy = |path: &PathBuf| path.with_extension("hibernated");
        let previous = cello_logic::restore::previous_epoch_reusable(epoch, took_over)
            .then(|| self.db_path(cell, epoch - 1).with_extension("evicted"));
        let is_file = |path: &PathBuf| {
            self.ltx_host
                .metadata(path)
                .is_ok_and(|metadata| metadata.is_file)
        };
        let first_present = |path: PathBuf| {
            if is_file(&path) {
                Some(path)
            } else {
                Some(legacy(&path)).filter(is_file)
            }
        };
        let local_snapshot = (!fresh && !resume_local)
            .then(|| previous.and_then(first_present))
            .flatten();

        // Sample before a restore can replace the path. A non-certified live
        // database can contain rows which a crash or self-fence staged but did
        // not upload, so its local position needs a remote bound.
        let dst_existed = is_file(&dst);
        let mut restored = resume_local;
        let mut remote_restore = None;
        let mut paged_vfs_name: Option<String> = None;
        let mut registration = PagedRegistration(None);
        // A paged activation continues its chain from the cut: (last txid of
        // the chain, page count at the cut).
        let mut continuation: Option<(TXID, u32)> = None;
        if resume_local {
            anyhow::ensure!(
                is_file(&dst),
                "clean reload database is missing: {}",
                dst.display()
            );
            info!(cell, epoch, "resumed clean local replica");
            // 0078: a resume continues the SAME epoch prefix, but the uploads
            // the previous residency made are not in this handle's journal,
            // so the object count is unknown: not chain_from_txid1_journaled.
        } else if let Some(snapshot) = local_snapshot {
            self.ltx_host.rename(&snapshot, &dst)?;
            self.preserved
                .lock()
                .expect("preserved cache poisoned")
                .forget(&snapshot);
            info!(cell, epoch, "reused local eviction snapshot");
            // 0078: this reuse still starts a new writer generation at TXID 1
            // (eviction stripped the LTX metadata, so `db.pos()` below reads
            // ZERO and the replica seeded at 0 uploads every page in its
            // first L0, exactly like a fresh cell): chain_from_txid1_journaled.
            restored = true;
        } else if !fresh {
            // Restore the newest durable epoch's full contiguous chain. The
            // epoch seal that once capped this read is deleted: the
            // cut it fixed defended only never-acked resurrection — not a
            // promise anyone holds — and under the log tier late arrival of
            // ACKED rows into a per-cell prefix is normal (recovery gathers,
            // the drain folds, the healing pass repairs), so a permanent cap
            // turned ordering slips into permanent loss. Without it the
            // healed rows are simply picked up here.
            //
            // A predecessor epoch that ended quietly left fleet-acked rows
            // in node bundles, and this restore reads per-cell prefixes
            // only. Fold that tail first — this activation holds the new
            // epoch's authority, so the write the fenced ending could not
            // make is lawful here, and the fold must precede the source
            // lookup because the tail can name an epoch the per-cell
            // listing has never seen. Failing the activation on a failed
            // fold is deliberate: restoring past it would serve a
            // truncated database as read-write (#473).
            if self.dirty_tails.lock().unwrap().contains_key(cell) {
                let sink = registered_durability(&self.registration)
                    .map(|targets| targets.manager.clone())
                    .ok_or_else(|| {
                        anyhow!("{cell} has an un-drained bundle tail and no bundle sink")
                    })?;
                sink.fold_cell(cell)
                    .await
                    .map_err(|error| anyhow!("fold bundle tail for {cell}: {error}"))?;
                self.dirty_tails.lock().unwrap().remove(cell);
            }
            let remote_started_mono_ms = asyncrt::mono_ms();
            let source_lookup_started_mono_ms = asyncrt::mono_ms();
            let chain = self.epoch_chain(cell, epoch).await?;
            let source_lookup_us = asyncrt::mono_ms()
                .saturating_sub(source_lookup_started_mono_ms)
                .saturating_mul(1_000);
            if let Some(chain) = chain {
                let spans = chain.spans();
                let from = spans.last().map_or(0, |(e, _)| *e);
                let _ = self.ltx_host.remove_file(&dst);
                // Page the cell in on demand instead of downloading its
                // chain when the chain is large enough to be worth it: build
                // the page map from the plan, then open the db through a
                // fault-in VFS over an empty local file. The first request
                // reads only the pages it touches. The epoch then continues
                // the chain it paged in (`seed_continuation`) rather than
                // opening with a whole-database snapshot. A smaller chain is
                // cloned: the plan is over the chain's cached listings, so
                // deciding costs no request.
                let mut paged_plan = None;
                if allow_paged
                    && self.paged_restore.load(Ordering::Relaxed)
                    && self.paged_fleet.load(Ordering::Relaxed)
                {
                    let plan_started = asyncrt::mono_ms();
                    let plan = replica::calc_restore_plan(&chain, TXID(0))
                        .await
                        .map_err(|error| anyhow!("paged plan {cell} e{from}: {error}"))?;
                    let plan_us = asyncrt::mono_ms()
                        .saturating_sub(plan_started)
                        .saturating_mul(1_000);
                    let bytes: u64 = plan.iter().map(|info| info.size.max(0) as u64).sum();
                    let min = self.paged_min_bytes.load(Ordering::Relaxed);
                    if bytes >= min {
                        paged_plan = Some((plan, plan_us));
                    } else {
                        info!(
                            cell,
                            from,
                            to = epoch,
                            bytes,
                            min,
                            "cloned below the paged threshold"
                        );
                    }
                }
                if let Some((plan, plan_us)) = paged_plan {
                    let map_started = asyncrt::mono_ms();
                    let map = cello_ltx::paged::build_page_map(&chain, &plan)
                        .await
                        .map_err(|error| anyhow!("paged map {cell} e{from}: {error}"))?;
                    let map_us = asyncrt::mono_ms()
                        .saturating_sub(map_started)
                        .saturating_mul(1_000);
                    let mut readers: Vec<(TXID, Box<dyn cello_ltx::paged::RangeReader>)> =
                        Vec::with_capacity(spans.len());
                    for (span_epoch, lo) in &spans {
                        let reader = self
                            .client_for(cell, *span_epoch)
                            .blocking_range_reader()
                            .await
                            .map_err(|error| {
                                anyhow!("paged reader {cell} e{span_epoch}: {error}")
                            })?;
                        readers.push((*lo, reader));
                    }
                    // The epoch continues at the txid after the cut. Its
                    // marker, a zero-page object at that txid, is uploaded
                    // before the cell serves: an empty epoch could not pass
                    // the eviction barrier, and a fenced owner's late
                    // snapshot in a lower epoch would outrank its chain
                    // (CelloPersistencePaged.tla, `BreakNoMarker`).
                    continuation = Some((TXID(chain.max_txid().0 + 1), map.commit));
                    let reader = cello_ltx::paged::EpochChainReader::new(readers);
                    let source = cello_ltx::paged::PageSource::new(map, Box::new(reader));
                    let name = cello_ltx::paged_vfs::next_registration_name();
                    cello_ltx::paged_vfs::register_paged_vfs(
                        &name,
                        self.vfs_name.as_deref(),
                        &dst,
                        std::sync::Arc::new(source),
                    )
                    .map_err(|error| anyhow!("register paged vfs {cell}: {error}"))?;
                    registration = PagedRegistration(Some(name.clone()));
                    remote_restore = Some(RemoteRestoreTiming {
                        started_mono_ms: remote_started_mono_ms,
                        from,
                        source_lookup_us,
                        plan_us,
                        map_us,
                        download_us: 0,
                        apply_us: 0,
                        objects: plan.len(),
                        bytes: plan.iter().map(|info| info.size.max(0) as u64).sum(),
                        levels: by_level(&plan),
                        paged: true,
                    });
                    info!(
                        cell,
                        from,
                        to = epoch,
                        chain = ?spans.iter().map(|(e, _)| *e).collect::<Vec<_>>(),
                        cut = chain.max_txid().0,
                        vfs = %name,
                        "paged remote replica"
                    );
                    paged_vfs_name = Some(name);
                    // 0078: the epoch continues the cut's txids from an older
                    // epoch's chain: not chain_from_txid1_journaled.
                    restored = true;
                } else {
                    let stats = replica::restore_timed_with_host_and_download_slots(
                        &chain,
                        &dst,
                        TXID(0),
                        self.ltx_host.clone(),
                        self.restore_slots.clone(),
                    )
                    .await
                    .map_err(|error| anyhow!("restore {cell} e{from}: {error}"))?;
                    let levels = stats
                        .plan
                        .by_level
                        .iter()
                        .map(|(level, count)| format!("L{level}:{count}"))
                        .collect::<Vec<_>>()
                        .join(" ");
                    remote_restore = Some(RemoteRestoreTiming {
                        started_mono_ms: remote_started_mono_ms,
                        from,
                        source_lookup_us,
                        plan_us: stats.plan_us,
                        map_us: 0,
                        download_us: stats.download_us,
                        apply_us: stats.apply_us,
                        objects: stats.plan.objects,
                        bytes: stats.plan.bytes,
                        paged: false,
                        levels,
                    });
                    info!(cell, from, to = epoch, "restored remote replica");
                    // 0078: the seed carries an older epoch's chain, so this
                    // epoch's prefix continues past it: not
                    // chain_from_txid1_journaled.
                    restored = true;
                }
            }
        }

        // 0078: a leftover database with no restore behind it holds local rows
        // a same-epoch predecessor staged but never journaled here: not
        // chain_from_txid1_journaled even though the epoch prefix is this one.
        let remote_seed = if !restored && dst_existed {
            Some(self.contiguous_covered_txid(cell, epoch).await?)
        } else {
            None
        };

        // Open the managed Db (creates a fresh WAL db when nothing was restored)
        // and pair it with this epoch's client. Registration is immediate: the
        // cell can be proved durable on its very first write. The just-opened
        // db's position is the replica's candidate seed -- 0 for a fresh cell
        // and the restored max for a proved image. An uncertified reused image
        // is clamped to its remote contiguous prefix above. The first sync can
        // therefore skip the `calc_pos` listing that otherwise storms a
        // rate-limiting store without skipping a staged local row. On the rare
        // decode error, proved images fall back to that listing and uncertified
        // images fail closed because their local bound is unknown.
        let db_open_started_mono_ms = asyncrt::mono_ms();
        let dst_ = dst.clone();
        let ltx_host = self.ltx_host.clone();
        // The paged VFS (this activation) takes precedence over the fault VFS.
        let vfs_name = paged_vfs_name.clone().or_else(|| self.vfs_name.clone());
        let truncate_pages = self.truncate_pages_for_cell(cell);
        // Capture must finish opening before the application's SQLite connection
        // or schema setup runs. An earlier write can hide a torn WAL header and
        // let capture resume from a stale LTX cursor after a crash.
        let (db, mut seed, marker) = asyncrt::blocking(move || {
            let open_db = |ltx_host: LtxHost| match vfs_name.as_deref() {
                Some(vfs_name) => Db::open_with_host_and_vfs(&dst_, ltx_host, vfs_name),
                None => Db::open_with_host(&dst_, ltx_host),
            };
            #[cfg(cello_internal_tests)]
            let mut db = crate::fault::with_connection_role("cello_ltx_db", || open_db(ltx_host))?;
            #[cfg(not(cello_internal_tests))]
            let mut db = open_db(ltx_host)?;
            if let Some(pages) = truncate_pages {
                db.truncate_page_n = pages;
            }
            let marker = match continuation {
                Some((txid, commit)) => Some((txid, db.seed_continuation(txid, commit)?)),
                None => None,
            };
            let seed = db.pos().ok();
            anyhow::Ok((db, seed, marker))
        })
        .await?
        .map_err(|error| anyhow!("open managed db {}: {error}", dst.display()))?;
        let client = SharedObjectStoreClient(Arc::new(self.client_for(cell, epoch)));
        // The epoch marker is this residency's own upload into the epoch
        // prefix; record it with the handle below so the eviction gates can
        // skip their visibility LISTs.
        let marker_uploaded = marker.is_some();
        if let Some((txid, bytes)) = marker {
            client
                .write_ltx_file(0, txid, txid, &bytes)
                .await
                .map_err(|error| anyhow!("upload the epoch marker for {cell} e{epoch}: {error}"))?;
            self.note_covered(cell, epoch, txid.0);
        }
        if let Some(covered) = remote_seed {
            let local = seed.ok_or_else(|| {
                anyhow!("cannot prove local replica position for {cell} epoch {epoch}")
            })?;
            if local.txid.0 > covered {
                seed = Some(Pos::new(TXID(covered), 0));
            }
        }
        let db_open_us = asyncrt::mono_ms()
            .saturating_sub(db_open_started_mono_ms)
            .saturating_mul(1_000);
        if let Some(timing) = &remote_restore {
            info!(
                event = "restore_plan",
                cell,
                epoch = timing.from,
                to = epoch,
                objects = timing.objects,
                bytes = timing.bytes,
                levels = %timing.levels,
                total_us = asyncrt::mono_ms()
                    .saturating_sub(timing.started_mono_ms)
                    .saturating_mul(1_000),
                source_lookup_us = timing.source_lookup_us,
                plan_us = timing.plan_us,
                map_us = timing.map_us,
                download_us = timing.download_us,
                apply_us = timing.apply_us,
                paged = timing.paged,
                db_open_us,
                "computed restore plan"
            );
        }
        let mut replica = Replica::new(db, client.clone());
        if let Some(pos) = seed {
            replica.seed_pos(pos);
        }
        let hydration = paged_vfs_name
            .as_ref()
            .filter(|_| self.hydrate_bytes_per_s > 0)
            .map(|_| {
                Arc::new(CellHydration {
                    cancelled: AtomicBool::new(false),
                    complete: AtomicBool::new(false),
                })
            });
        let handle = Arc::new(Cell {
            snapshot_declined: AtomicBool::new(false),
            paged_vfs: paged_vfs_name.clone(),
            hydration: hydration.clone(),
            replica: Mutex::new(Some(replica)),
            client: client.clone(),
            req_seq: AtomicU64::new(0),
            synced_seq: AtomicU64::new(0),
            shipped_seq: AtomicU64::new(0),
            submitted_seq: AtomicU64::new(0),
            // Frames at or below the seed came from the bucket (or a proven
            // snapshot); the followers only ever need what follows.
            shipped_txid: Arc::new(AtomicU64::new(seed.map_or(0, |pos| pos.txid.0))),
            submitted_txid: AtomicU64::new(seed.map_or(0, |pos| pos.txid.0)),
            last_sync_ms: AtomicU64::new(asyncrt::wall_ms().max(0) as u64),
            capture_seq: AtomicU64::new(0),
            capture_started_ms: AtomicU64::new(0),
            node_proof_ms: self.node_proof_ms.clone(),
            durable_txid: Arc::new(AtomicU64::new(seed.map_or(0, |pos| pos.txid.0))),
            // The restore read the per-cell prefix, so the seed IS the
            // per-cell coverage at open.
            percell_txid: AtomicU64::new(seed.map_or(0, |pos| pos.txid.0)),
            uploaded_epoch_objects: AtomicBool::new(marker_uploaded),
            // 0078: this epoch's chain is restorable from this handle's journal
            // alone (so an eviction may skip its snapshot) iff no older history
            // feeds it: no remote restore or paged continuation (the seed
            // would carry an older epoch's chain), no same-epoch resume (the
            // previous residency's uploads are unjournaled), and no
            // uncertified leftover rows. A reused `.evicted`/`.hibernated`
            // image qualifies: its generation starts at TXID 1. Otherwise the
            // chain is born here: a fresh cell, or an empty bucket with no
            // local state.
            chain_from_txid1_journaled: remote_restore.is_none()
                && !resume_local
                && remote_seed.is_none(),
            epoch_uploads: Mutex::new(Vec::new()),
            epoch_uploads_overflowed: AtomicBool::new(false),
            handoff_ticket: AtomicU64::new(0),
            syncing: AtomicBool::new(false),
            ready: Notify::new(),
            #[cfg(all(test, cello_internal_tests))]
            sync_credit_pause: Mutex::new(None),
            #[cfg(all(test, cello_internal_tests))]
            observer_cell: cell.to_string(),
            #[cfg(all(test, cello_internal_tests))]
            observer_epoch: epoch,
            #[cfg(all(test, cello_internal_tests))]
            durability_ticket_receipts: Mutex::new(Vec::new()),
            #[cfg(all(test, cello_internal_tests))]
            upload_round_receipts: Mutex::new(Vec::new()),
            #[cfg(all(test, cello_internal_tests))]
            fleet_credit_receipts: Mutex::new(Vec::new()),
            #[cfg(all(test, cello_internal_tests))]
            fleet_capture_receipts: Mutex::new(Vec::new()),
            compaction: self.compaction_queue.as_ref().map(|queue| {
                let fetcher = Arc::new(SinkFetcher {
                    registration: self.registration.clone(),
                    cell: cell.to_string(),
                    epoch,
                    rows: tokio::sync::Mutex::new(None),
                });
                CellCompaction {
                    cell: cell.to_string(),
                    epoch,
                    // The overlay lets compaction read bundle-resident frames
                    // beside the per-cell objects; its output stays pure
                    // per-cell L1s, which is the continuous drain.
                    client: cello_ltx::BundleOverlayClient::new(
                        client.clone(),
                        Some(fetcher.clone()),
                    ),
                    fetcher,
                    local_path: Db::meta_path_for_path(&dst),
                    host: self.ltx_host.clone(),
                    queue: queue.clone(),
                    base_txid: continuation.map_or(1, |(txid, _)| txid.0),
                    min_txids: self.compaction_min_txids,
                    min_bytes: self.compaction_min_bytes,
                    pending_bytes: AtomicU64::new(0),
                    compacted_txid: AtomicU64::new(0),
                    retry_after_ms: AtomicU64::new(0),
                    failures: AtomicU64::new(0),
                    queued: AtomicBool::new(false),
                    cancelled: AtomicBool::new(false),
                    handoff_paused: AtomicBool::new(false),
                    cancel: Notify::new(),
                    #[cfg(all(test, cello_internal_tests))]
                    finish_pause: Mutex::new(None),
                    run: tokio::sync::Mutex::new(()),
                }
            }),
        });
        #[cfg(cello_internal_tests)]
        self.pause_activation_install_for_world().await;
        // Shutdown publishes stop before it takes the close gate and this map.
        // An activation that linearizes first is in shutdown's snapshot; one
        // that linearizes later closes its just-opened database and cannot
        // recreate a post-owner persistence resource. A failed release also
        // keeps its key in the stopped map, so the same gate prevents a second
        // managed handle from installing on the path before owner cleanup.
        // Keep the on-disk image: a `resume_local` activation owns the clean-reload
        // baseline, and a restored activation can own LTX metadata that this
        // losing path cannot safely classify for deletion.
        let (admitted, close_incomplete) = {
            let _close_gate = self.replica_close_gate.lock().unwrap();
            let mut cells = self.cells.lock().unwrap();
            let close_incomplete = self
                .stopped_cells
                .lock()
                .unwrap()
                .contains_key(&(cell.to_string(), epoch));
            if self.stop.is_stopped() || close_incomplete {
                (false, close_incomplete)
            } else {
                cells.insert((cell.to_string(), epoch), handle.clone());
                registration.keep();
                (true, false)
            }
        };
        if !admitted {
            close_replica(&handle).map_err(|error| {
                anyhow!("close stopped activation for {cell} epoch {epoch}: {error}")
            })?;
            if close_incomplete {
                anyhow::bail!("managed replica close is incomplete for {cell} epoch {epoch}");
            }
            anyhow::bail!("LTX replication stopped before activation installed");
        }
        if let Some(pos) = seed {
            maybe_queue_compaction(&handle, pos.txid.0);
        }
        if let (Some(name), Some(hydration)) = (&paged_vfs_name, hydration) {
            self.spawn_hydration(cell, epoch, name.clone(), dst.clone(), hydration);
        }

        // Local retention during normal operation (upstream #311): this
        // activation supersedes older live-named epoch dirs of the same
        // cell. Best-effort — a prune failure must not fail the activation
        // it follows — and resident epochs are never touched.
        self.prune_superseded_epochs(cell, epoch);

        Ok(ActivationResult {
            path: dst,
            restored,
            vfs: paged_vfs_name,
        })
    }

    /// The paged VFS name of a resident activation, when its restore paged.
    /// The runtime opens the actor's connection through it; a plain open of a
    /// paged cell's sparse database file reads holes as data.
    /// Fills a paged cell's file in the background, a run per step at
    /// `CELLO_LTX_HYDRATE_MBPS`, one cell at a time per node, until the cut
    /// is complete or the cell closes. Steps fault through the VFS's own
    /// read path, so hydration is foreground faulting at a pace.
    fn spawn_hydration(
        &self,
        cell: &str,
        epoch: u64,
        name: String,
        path: PathBuf,
        hydration: Arc<CellHydration>,
    ) {
        let cell = cell.to_string();
        let permits = self.hydrations.clone();
        let step_ms =
            (u64::from(HYDRATE_STEP_PAGES) * 4096 * 1_000 / self.hydrate_bytes_per_s).max(1);
        self.tasks.spawn_owned("ltx-hydration", async move {
            let Ok(_permit) = permits.acquire().await else {
                return;
            };
            let started_mono_ms = asyncrt::mono_ms();
            let mut hydrator = match asyncrt::blocking({
                let name = name.clone();
                move || cello_ltx::paged_vfs::Hydrator::open(&name, &path)
            })
            .await
            {
                Ok(Ok(hydrator)) => hydrator,
                Ok(Err(error)) => {
                    warn!(cell, epoch, %error, "paged hydration could not open the cell");
                    return;
                }
                Err(_) => return,
            };
            let mut steps = 0u64;
            loop {
                if hydration.cancelled.load(Ordering::SeqCst) {
                    return;
                }
                let step = asyncrt::blocking(move || {
                    let progress = hydrator.step(HYDRATE_STEP_PAGES);
                    (hydrator, progress)
                })
                .await;
                let progress = match step {
                    Ok((hydrator_, Ok(progress))) => {
                        hydrator = hydrator_;
                        progress
                    }
                    Ok((_, Err(error))) => {
                        warn!(cell, epoch, %error, "paged hydration stopped");
                        return;
                    }
                    Err(_) => return,
                };
                steps += 1;
                if progress.complete() {
                    hydration.complete.store(true, Ordering::SeqCst);
                    info!(
                        event = "paged_hydrated",
                        cell,
                        epoch,
                        pages = progress.total,
                        faults = progress.faults,
                        steps,
                        elapsed_ms = asyncrt::mono_ms().saturating_sub(started_mono_ms),
                        "paged cell holds its whole cut"
                    );
                    return;
                }
                asyncrt::sleep(Duration::from_millis(step_ms)).await;
            }
        });
    }

    /// How much of the cut a paged activation's file holds, for a world
    /// that waits on the background fill.
    #[cfg(all(test, cello_internal_tests))]
    pub fn hydration_for_test(
        &self,
        cell: &str,
        epoch: u64,
    ) -> Option<cello_ltx::paged_vfs::Hydration> {
        cello_ltx::paged_vfs::hydration(&self.paged_vfs_name(cell, epoch)?)
    }

    /// The test chain-size threshold for paging, so a world can
    /// page a small cut or clone one.
    #[cfg(all(test, cello_internal_tests))]
    pub fn set_paged_min_bytes_for_test(&self, bytes: u64) {
        self.paged_min_bytes.store(bytes, Ordering::Relaxed);
    }

    /// The test switch for paged restore, so a world runs its
    /// schedules over the fault path without the process environment. It
    /// pages every cut; `set_paged_min_bytes_for_test` restores a threshold.
    #[cfg(all(test, cello_internal_tests))]
    pub fn set_paged_restore_for_test(&self, on: bool) {
        self.paged_restore.store(on, Ordering::Relaxed);
        self.paged_fleet.store(on, Ordering::Relaxed);
        self.paged_min_bytes.store(0, Ordering::Relaxed);
    }

    /// The fleet's answer to "can every live node read a paged epoch". The
    /// fleet sampler sets it from the leases; returns the prior value so the
    /// caller logs a transition once.
    pub fn set_paged_fleet(&self, ready: bool) -> bool {
        self.paged_fleet.swap(ready, Ordering::Relaxed)
    }

    /// Whether this node would page a large takeover now.
    pub fn paged_fleet(&self) -> bool {
        self.paged_fleet.load(Ordering::Relaxed)
    }

    /// The fleet's answer to "may this node write bucket-posture bundle proofs".
    /// The fleet sampler sets it from the leases via
    /// `cello_logic::format::bucket_bundles_allowed`; returns the prior value so
    /// the caller logs a transition once. Off until the first sample, mirroring
    /// the paged gate: a failed or missing sample closes it. The per-flush path
    /// additionally requires the operator switch (opt-in;
    /// `CELLO_BUCKET_BUNDLES=1` enables it), see `bucket_bundles_enabled`.
    pub fn set_bucket_bundles_fleet(&self, ready: bool) -> bool {
        self.bucket_bundles_fleet.swap(ready, Ordering::Relaxed)
    }

    /// Whether the fleet sample currently permits bucket bundles (format gate
    /// only; the operator switch is checked per flush in `bucket_bundles_enabled`).
    pub fn bucket_bundles_fleet(&self) -> bool {
        self.bucket_bundles_fleet.load(Ordering::Relaxed)
    }

    /// Whether this flush may write bucket-posture bundles: the fleet gate
    /// (set by the sampler, off until the first sample) AND the operator switch
    /// (opt-in; `CELLO_BUCKET_BUNDLES=1` enables it). Re-checked per
    /// flush, not per process: when the gate closes, the node falls back at once
    /// to per-cell L0 uploads for new writes. Already-credited bundle rows stay
    /// covered by the existing drain and GC paths.
    pub(crate) fn bucket_bundles_enabled(&self) -> bool {
        if !self.bucket_bundles_fleet.load(Ordering::Relaxed) {
            return false;
        }
        #[cfg(test)]
        if let Some(forced) = *self.bucket_env_override.lock().unwrap() {
            return forced;
        }
        bucket_bundles_env_on()
    }

    /// Test override for the operator switch, so deterministic tests do not touch
    /// the process environment. `Some(on)` forces the env leg; `None` restores the
    /// real `CELLO_BUCKET_BUNDLES` read.
    #[cfg(test)]
    pub fn set_bucket_bundles_env_override_for_test(&self, on: Option<bool>) {
        *self.bucket_env_override.lock().unwrap() = on;
    }

    /// The VFS the actor's connection must open through, for a resident
    /// activation: `Some` when it paged, `None` when its file is whole. An
    /// activation that is no longer resident is an error, not `None`: a
    /// plain open of an absent or sparse file would create or read the
    /// wrong database.
    pub fn activation_vfs(&self, cell: &str, epoch: u64) -> anyhow::Result<Option<String>> {
        let cells = self.cells.lock().unwrap();
        let handle = cells
            .get(&(cell.to_string(), epoch))
            .ok_or_else(|| anyhow!("{cell} epoch {epoch} is not resident"))?;
        Ok(handle.paged_vfs.clone())
    }

    pub fn paged_vfs_name(&self, cell: &str, epoch: u64) -> Option<String> {
        self.cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .and_then(|handle| handle.paged_vfs.clone())
    }

    /// Run the managed database's real checkpoint at a controlled test cut.
    /// A second SQLite connection keeps the managed reader's lock held and
    /// cannot exercise its truncate path. The test driver selects timing;
    /// the shipping Db still owns capture, lock release, and checkpoint order.
    /// Observe the result on the native lane before waking its async caller.
    /// A later writer can repair a failed WAL header before that caller runs,
    /// so delayed classification against the current image loses the cause.
    #[cfg(all(test, cello_internal_tests))]
    pub(crate) async fn checkpoint_for_world<T, F>(
        &self,
        cell: &str,
        epoch: u64,
        mode: cello_ltx::CheckpointMode,
        observe: F,
    ) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(anyhow::Result<()>) -> anyhow::Result<T> + Send + 'static,
    {
        let handle = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_owned(), epoch))
            .cloned()
            .ok_or_else(|| anyhow!("checkpoint requires a resident {cell} epoch {epoch}"))?;
        asyncrt::blocking(move || {
            let mut replica = handle.replica.lock().unwrap();
            let result = managed_db_mut(&mut replica)
                .ok_or_else(|| anyhow!("checkpoint lost its managed database"))
                .and_then(|db| db.checkpoint(mode).map_err(anyhow::Error::new));
            observe(result)
        })
        .await
        .map_err(|error| anyhow!("checkpoint task failed: {error}"))?
    }

    // Reopening a frozen image must use the same execution-domain host as
    // activation. A direct test host would bypass clock and filesystem adapters.
    #[cfg(all(test, cello_internal_tests))]
    pub(crate) fn host_for_world(&self) -> LtxHost {
        self.ltx_host.clone()
    }

    /// The output gate's primitive: take a durability ticket and return once a
    /// background sync that captured this write has completed, coalescing
    /// concurrent writes to one cell into a single upload. The write committed
    /// before this call, so any sync starting after our ticket captures it —
    /// we wait for `synced_seq >= my ticket`, not for a position, sidestepping
    /// the total_changes↔LTX-txid mismatch that a position compare would hit.
    /// Returns `position` (which the completed sync provably covered) for the
    /// core's coverage check.
    pub async fn await_durable(
        &self,
        cell: &str,
        epoch: u64,
        position: u64,
    ) -> anyhow::Result<(u64, cello_logic::ProofSource)> {
        let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        else {
            anyhow::bail!("ltx cell not resident: {cell} epoch {epoch}");
        };
        let ticket = handle.req_seq.fetch_add(1, Ordering::SeqCst) + 1;
        #[cfg(all(test, cello_internal_tests))]
        handle.record_durability_ticket_for_world(position, ticket);
        self.dirty.notify_one();
        self.dirty_ship.notify_one();
        self.dirty_bundle.notify_one();
        let source = self
            .wait_for_durability_ticket(&handle, cell, epoch, ticket)
            .await?;
        Ok((position, source))
    }

    /// Prove that every write before handoff nomination reached either the
    /// bucket or the live durability ensemble. The fleet proof is sufficient
    /// to close the runtime because ownership still names this donor. The
    /// eviction barrier publishes the closed database before ownership can
    /// move, so a process loss between these phases still enters ordinary
    /// dead-node recovery.
    pub async fn handoff_wait(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<cello_logic::ProofSource> {
        let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        else {
            anyhow::bail!("ltx cell not resident: {cell} epoch {epoch}");
        };
        // Publish a lower bound before taking the ticket, so a capture that
        // reads the new `req_seq` also sees a pending handoff, then raise it
        // to the ticket itself.
        handle
            .handoff_ticket
            .fetch_max(handle.req_seq.load(Ordering::SeqCst) + 1, Ordering::SeqCst);
        let ticket = handle.req_seq.fetch_add(1, Ordering::SeqCst) + 1;
        handle.handoff_ticket.fetch_max(ticket, Ordering::SeqCst);
        self.dirty.notify_one();
        self.dirty_ship.notify_one();
        self.dirty_bundle.notify_one();
        self.wait_for_durability_ticket(&handle, cell, epoch, ticket)
            .await
    }

    /// The eviction gate: prove the cell is safe to give up, while the node
    /// can still change its mind.
    ///
    /// This runs in `Phase::EnsuringDurability`. The cell still serves there,
    /// and a request that arrives cancels the eviction and puts the cell back
    /// into service at its own epoch. `stop_cell` then takes close exclusion,
    /// drops the runtime, and only afterwards calls `evict`, which publishes
    /// the handoff snapshot and asks the bucket to confirm it. A request that
    /// arrives during that second half finds `Phase::Cleaning` and waits for a
    /// whole reactivation at a new epoch, so every second the eviction spends
    /// after the close is a second in which an idle sweep can still evict a
    /// cell that traffic made live again. A revocable eviction therefore does
    /// the remote work it can do here, where the answer can still be acted on.
    ///
    /// `handoff_wait` reports which mechanism proved the cell durable, and
    /// that names the successor's restore source. A bucket proof means the
    /// per-cell tiering is the acknowledgement path, so the objects under
    /// `cells/<cell>/ltx/e<epoch>` are already there. Ask the bucket whether
    /// they survived: an upload this process completed does not prove that
    /// they did. A fleet proof means the paced shipper is active, and
    /// `bundle_loop` then leaves the per-cell prefix untouched by design, so
    /// that prefix is legitimately empty and an unconditional question here
    /// would refuse every fleet-posture eviction.
    ///
    /// The fleet proof therefore keeps the window this closes for the bucket
    /// proof. Moving the handoff snapshot forward would not close it: the
    /// snapshot is a full image at one txid, `evict` publishes it as the
    /// successor's authoritative restore artifact, and a cell that keeps
    /// serving can commit past it. A cancelled eviction would then leave an
    /// image in the bucket that a later restore reads as complete. The cost
    /// of the remaining window is revocability and not durability, because
    /// `evict` still refuses to give up state the bucket cannot restore.
    /// Closing it needs the core to accept a request during `Phase::Cleaning`,
    /// which is an open decision (#818). Do not read the scoping as a claim
    /// that the fleet path needs no gate.
    ///
    /// A drain passes `revocable: false`. It has nowhere to put a cell back,
    /// so the answer could change nothing, and a drain that spent one bucket
    /// LIST per resident cell would pay for it out of its shutdown budget.
    pub async fn handoff_gate(
        &self,
        cell: &str,
        epoch: u64,
        revocable: bool,
    ) -> anyhow::Result<()> {
        let proof = self.handoff_wait(cell, epoch).await?;
        // A bucket proof this residency's own uploads earned carries its own
        // visibility proof: the successful PUT responses already prove what
        // the LIST would check, so it goes. A cell whose acked rows sit
        // outside the per-cell layout holds bundle-credited rows the LIST
        // cannot see and the coming handoff snapshot covers, so it also goes:
        // the snapshot attempt — and the L0 fallback with its own LIST —
        // stays the fail-closed guard for state the bucket cannot restore.
        // Anything else — a resumed copy, recovered tails, an upload-free
        // residency — keeps the LIST.
        if revocable
            && matches!(proof, cello_logic::ProofSource::Bucket)
            && !self.residency_uploaded(cell, epoch)
            && !self.has_bundle_only_rows(cell, epoch)
            && !self.epoch_replicated(cell, epoch).await
        {
            return Err(anyhow!(
                "no replica objects for {cell} epoch {epoch}; refusing to \
                 evict state the bucket cannot restore"
            ));
        }
        Ok(())
    }

    /// Whether this residency holds acked rows outside the per-cell layout:
    /// `durable_txid` past everything any per-cell object covers. Only the
    /// bundle path credits durable without per-cell coverage (the per-cell
    /// sync and the compaction drain both publish per-cell objects and feed
    /// the same coverage watermark), so the gap names bundle-credited rows.
    /// Deliberately flag-free: rows bundled while the gate was open stay
    /// bundle rows after it closes, and the eviction this defers to covers
    /// them either way — by snapshot, or by its L0 fallback which refuses
    /// when neither artifact lands.
    fn has_bundle_only_rows(&self, cell: &str, epoch: u64) -> bool {
        self.cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .is_some_and(|handle| {
                handle.durable_txid.load(Ordering::SeqCst) > percell_coverage(handle)
            })
    }

    async fn wait_for_durability_ticket(
        &self,
        handle: &CellHandle,
        cell: &str,
        epoch: u64,
        ticket: u64,
    ) -> anyhow::Result<cello_logic::ProofSource> {
        let wait = ProofWait {
            started_ms: asyncrt::mono_ms(),
            ticket,
            budget_ms: self.durability_timeout_ms,
        };
        let started = wait.started_ms;
        loop {
            // Register the waiter before checking, so a sync that completes
            // between the check and the await is not missed. Either proof
            // releases the gate: the bucket upload, or every ensemble
            // member's fsync — whichever lands first.
            let ready = handle.ready.notified();
            // Prefer the fleet proof when both hold: it is the arbitrated
            // one, and it spares the caller an ownership read.
            let shipped = handle.shipped_seq.load(Ordering::SeqCst) >= ticket;
            if handle.synced_seq.load(Ordering::SeqCst) >= ticket || shipped {
                let source = if shipped {
                    cello_logic::ProofSource::Fleet
                } else {
                    cello_logic::ProofSource::Bucket
                };
                tracing::debug!(
                    target: "timing",
                    event = "durable_wait",
                    cell,
                    wait_us = asyncrt::mono_ms().saturating_sub(started).saturating_mul(1_000),
                    proof = if shipped { "fleet" } else { "bucket" },
                    "durability proof reached"
                );
                return Ok(source);
            }
            // The deadline moves: a proof landing anywhere on the node while
            // this write is queued, or this cell's capture beginning, each
            // push it out (`cello_logic::durability`). So a timer firing is
            // a cue to recompute, and only a deadline that is still in the
            // past fails the proof.
            let deadline = proof_deadline(
                &wait,
                &ProofProgress {
                    node_proof_ms: handle.node_proof_ms.load(Ordering::SeqCst),
                    capture_seq: handle.capture_seq.load(Ordering::SeqCst),
                    capture_started_ms: handle.capture_started_ms.load(Ordering::SeqCst),
                },
            );
            if asyncrt::mono_ms() >= deadline {
                anyhow::bail!("ltx durability timed out for {cell} epoch {epoch}");
            }
            let _ = asyncrt::timeout_at(deadline, ready).await;
        }
    }

    /// A direct, synchronous durability pass for the rare eviction
    /// gates (not the hot write path). Also advances the cell's durable position
    /// so any output-gate waiters ride it.
    pub async fn sync_wait(&self, cell: &str, epoch: u64, timeout: Duration) -> SyncWait {
        let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        else {
            return SyncWait::Unsupported;
        };
        match asyncrt::timeout(timeout, sync_cell(handle.clone())).await {
            Ok(Some(true)) => SyncWait::Durable,
            Ok(Some(false)) | Err(_) => SyncWait::Failed,
            Ok(None) => SyncWait::Unsupported,
        }
    }

    /// Return the configured durability deadline to a deterministic
    /// world, so a bounded-liveness assertion uses the production value.
    #[cfg(all(test, cello_internal_tests))]
    pub(crate) fn durability_timeout_ms_for_world(&self) -> u64 {
        self.durability_timeout_ms
    }

    /// Lower the handoff snapshot budget, so a test world can hand off a
    /// small database the way production hands off a whale.
    #[cfg(any(test, cello_internal_tests))]
    pub(crate) fn set_handoff_snapshot_budget_for_world(&self, bytes: u64) {
        self.snapshot_budget_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Close this cell after a final durability pass and install its database
    /// as the previous-epoch cache entry which the next activation consumes.
    ///
    /// A remote restore creates a live database at the successor epoch. Closing
    /// that database in place makes the next epoch ignore it because only a
    /// `.evicted` file is a certified reactivation base. The activation then
    /// downloads the same older snapshot again (#479). Use [`Self::close_in_place`]
    /// only when the caller cannot authorize a final durability pass.
    pub async fn release(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        self.final_durability_barrier(cell, epoch).await?;

        // Cleanup does not transfer ownership, so it does not need the L9
        // handoff snapshot that eviction publishes. The durable L0 chain and
        // this certified local base cover both a later node and this node's
        // successor activation.
        self.remove_local(cell, epoch, true);
        Ok(())
    }

    /// Close this cell's managed database, leaving every file in place.
    ///
    /// Background work can retain the handle after registry removal. The method
    /// installs the close in the durability owner's task group before it waits
    /// for completion. Therefore, a cancelled caller cannot detach the close,
    /// and local shutdown joins the same operation instead of closing twice.
    /// If the blocking task fails before it takes the replica, the method
    /// returns an error and retains the handle. A later call retries the close,
    /// and the final owner shutdown also closes a retained handle.
    ///
    /// No durability pass here, deliberately. This runs on the stops that are
    /// not an orderly handoff, and a fenced node has lost the authority that
    /// would make writing more of this cell's history safe. `evict` below
    /// still syncs, and still refuses to drop a handle whose final pass
    /// failed, because that is the path where the node keeps the cell and is
    /// giving it up on purpose. The caller must require a successful result
    /// before it reuses the local path for another activation. Use
    /// [`Self::release`] when the caller can authorize a final durability pass.
    pub async fn close_in_place(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        let key = (cell.to_string(), epoch);
        let (completed, admitted) = {
            // Keep the gate through active-to-stopped transfer and task
            // admission. Shutdown closes admission and snapshots both maps
            // under this same gate, so it cannot miss an in-flight release.
            let _close_gate = self.replica_close_gate.lock().unwrap();
            let handle = match self.remove_active_and_record_tail(cell, epoch) {
                Some(handle) => handle,
                None => {
                    let stopped = self.stopped_cells.lock().unwrap().get(&key).cloned();
                    let Some(handle) = stopped else {
                        return Ok(());
                    };
                    if !self.failed_release_closes.lock().unwrap().remove(&key) {
                        return Err(anyhow!(
                            "managed replica close is incomplete for {cell} epoch {epoch}; wait for it or finish local shutdown"
                        ));
                    }
                    handle
                }
            };
            self.note_undrained_tail(cell, epoch, &handle);
            self.stopped_cells
                .lock()
                .unwrap()
                .insert(key.clone(), handle.clone());
            if self.replica_close_stop.is_stopped() {
                self.failed_release_closes.lock().unwrap().insert(key);
                return Err(anyhow!(
                    "managed replica close was deferred to local shutdown for {cell} epoch {epoch}"
                ));
            }
            #[cfg(cello_internal_tests)]
            let pause = self.take_replica_close_pause_for_world();
            #[cfg(cello_internal_tests)]
            let panic_close = self.take_release_close_panic_for_world();
            let stopped_cells = self.stopped_cells.clone();
            let failed_release_closes = self.failed_release_closes.clone();
            let close_key = key.clone();
            let close_cell = cell.to_string();
            let close_handle = handle.clone();
            let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();
            let admitted = self.replica_close_tasks.spawn_owned(
                "ltx_replica_close",
                async move {
                    let task_handle = close_handle.clone();
                    let close_result = asyncrt::blocking(move || {
                        #[cfg(cello_internal_tests)]
                        assert!(!panic_close, "injected managed release-close panic");
                        #[cfg(cello_internal_tests)]
                        pause_replica_close_for_world(pause);
                        close_replica(&task_handle)
                    })
                    .await;
                    let (close_result, retry_during_shutdown) = match close_result {
                        Ok(result) => (result, false),
                        Err(error) => (
                            Err(anyhow!("managed replica close task failed: {error}")),
                            true,
                        ),
                    };
                    if retry_during_shutdown {
                        failed_release_closes
                            .lock()
                            .unwrap()
                            .insert(close_key.clone());
                    } else {
                        let mut stopped = stopped_cells.lock().unwrap();
                        if stopped
                            .get(&close_key)
                            .is_some_and(|current| Arc::ptr_eq(current, &close_handle))
                        {
                            stopped.remove(&close_key);
                        }
                    }
                    if let Err(error) = close_result {
                        if retry_during_shutdown {
                            // The blocking lane failed before it proved that
                            // it took the replica. Keep the stopped entry so a
                            // later release or final shutdown retries it.
                            warn!(cell = close_cell, epoch, %error, "managed replica close task failed during removal");
                            let _ = completed_tx.send(Err(error.to_string()));
                        } else {
                            // close_replica took and dropped the database even
                            // when read-lock release failed, so this path needs
                            // no second close attempt.
                            warn!(cell = close_cell, epoch, %error, "close managed replica failed during removal");
                            let _ = completed_tx.send(Ok(()));
                        }
                    } else {
                        let _ = completed_tx.send(Ok(()));
                    }
                },
            );
            (completed_rx, admitted)
        };
        if admitted {
            completed
                .await
                .map_err(|_| anyhow!("managed replica close task ended without completion"))?
                .map_err(anyhow::Error::msg)
        } else {
            // Shutdown already owns the stopped entry and closes it from its
            // final snapshot. No new runtime can activate after this point.
            self.failed_release_closes.lock().unwrap().insert(key);
            Err(anyhow!(
                "managed replica close was deferred to local shutdown for {cell} epoch {epoch}"
            ))
        }
    }

    /// Take the cell, or give it back.
    ///
    /// `abandon` carries the core's answer to "a request arrived for this
    /// cell". Reading it is the whole revocable half of an eviction stop, and
    /// where it can be read is fixed by what is already visible to a
    /// successor:
    ///
    /// - Before `prepare_handoff_snapshot`, nothing remote has happened and
    ///   the isolate turn this pass waited for can be the slow part on a
    ///   loaded node. Abandoning here costs nothing.
    /// - Before `publish_handoff_snapshot`, the snapshot exists only in this
    ///   process. Abandoning here throws away one local capture.
    /// - After that PUT there is no way back. The snapshot is a full image at
    ///   one txid, and `restore_plan` takes the highest snapshot at or below
    ///   its target and then extends it only with the per-cell files above it.
    ///   A resumed cell commits past that txid, and under a fleet proof those
    ///   rows go to node bundles and not to the per-cell prefix, so a restore
    ///   would read the image as a complete database that stops short of an
    ///   acknowledged write. The rows are recoverable while the node-log
    ///   session stays open, but that is a second mechanism and not a proof,
    ///   so this pass does not rely on it.
    ///
    /// The PUT and the visibility LIST therefore stay unrevocable in both
    /// postures. A request that arrives during those two round trips still
    /// waits for the cell to start again at a new epoch.
    pub async fn evict(
        &self,
        cell: &str,
        epoch: u64,
        preserve_local: bool,
        abandon: Option<&crate::replication::EvictionAbandon>,
    ) -> anyhow::Result<EvictionRestoreArtifact> {
        // The runtime is closed before this pass, so the snapshot is the
        // definitive durability barrier. The paced shipper normally stores a
        // hot tail in node bundles. Re-reading every local L0 row to duplicate
        // that tail under the cell prefix made shutdown cost grow with the
        // cell's lifetime. One full snapshot has constant object count and
        // gives the successor the same closed database.
        let barrier_started_mono_ms = asyncrt::mono_ms();
        let handle = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
            .ok_or_else(|| anyhow!("ltx cell not resident: {cell} epoch {epoch}"))?;
        let abandoned = || {
            anyhow::Error::new(crate::replication::EvictionAbandoned {
                cell: cell.to_string(),
                epoch,
            })
        };
        if abandon.is_some_and(EvictionAbandon::requested) {
            return Err(abandoned());
        }
        // 0078 lazy handoff (MergeTree-style): a cell whose restorable chain
        // was born in this epoch, is fully per-cell covered, and fits in a
        // short journal needs no handoff snapshot: the L0 chain below is
        // already the complete restore source, and the snapshot PUT would
        // rewrite the whole database on every eviction. The skip declines
        // exactly like the over-budget path (compaction pause held, no trim,
        // L0-chain fallback with its own proof), so refusal behaviour is
        // unchanged. Watermarks can only move together here (per-cell uploads
        // credit both; the runtime is closed), and the fallback re-proves
        // whatever moved, so the decision needs no re-check inside.
        let journal_len = handle.epoch_uploads.lock().unwrap().len();
        let skip_snapshot = !cello_logic::handoff::handoff_snapshot_needed(
            &cello_logic::handoff::HandoffChain {
                chain_from_txid1_journaled: handle.chain_from_txid1_journaled,
                percell_txid: percell_coverage(&handle),
                durable_txid: handle.durable_txid.load(Ordering::SeqCst),
                journal_len,
                journal_overflowed: handle
                    .epoch_uploads_overflowed
                    .load(Ordering::SeqCst),
            },
        );
        if skip_snapshot {
            info!(
                event = "eviction_snapshot_skipped",
                cell,
                epoch,
                objects = journal_len,
                reason = "chain_from_txid1_journaled,percell_covered,short_chain",
                "this epoch's L0 chain is the complete handoff; skipping the snapshot PUT"
            );
        }
        let (snapshot, compaction_pause) = self
            .prepare_handoff_snapshot(&handle, skip_snapshot)
            .await?;
        if abandon.is_some_and(EvictionAbandon::requested) {
            return Err(abandoned());
        }
        let deadline = barrier_started_mono_ms.saturating_add(self.durability_timeout_ms);
        let mut artifact = None;
        let mut trim_max: Option<u64> = None;
        if let Some(snapshot) = snapshot {
            let published = asyncrt::timeout_at(
                deadline,
                self.publish_handoff_snapshot(cell, epoch, &snapshot),
            )
            .await;
            // The snapshot PUT this residency just completed is itself the
            // visibility proof the LIST used to supply: on the qualified
            // stores the successor's LIST finds what our PUT acked, and
            // nothing deletes under a live epoch prefix. The extra LIST
            // therefore goes; the L0 fallback below keeps its own.
            if matches!(published, Ok(Ok(()))) {
                handle.uploaded_epoch_objects.store(true, Ordering::SeqCst);
                // The snapshot is a per-cell object through its txid, so it
                // covers every fleet-acked row of this closed database. No
                // later sync or bundle flush credits a removed handle, so
                // without this credit the ship ledger's batch for these rows
                // never counts as covered and followers keep it, and every
                // later batch, until the next epoch. Removal below also
                // records a false undrained tail without the per-cell credit.
                // A failed snapshot does not credit: the L0 fallback reads
                // `durable_txid` to choose which rows to upload.
                //
                // A row credited past the image is not in it: the snapshot
                // cannot be the authoritative artifact. Fall through to the
                // L0 chain, which covers every credited row or refuses.
                if handle.durable_txid.load(Ordering::SeqCst) <= snapshot.max_txid.0 {
                    credit_percell_upload(&handle, snapshot.max_txid.0);
                    trim_max = Some(snapshot.max_txid.0);
                    artifact = Some(EvictionRestoreArtifact::Snapshot);
                } else {
                    warn!(
                        event = "eviction_snapshot_stale",
                        cell,
                        epoch,
                        max_txid = snapshot.max_txid.0,
                        "a credited row advanced past the snapshot, so the handoff requires its L0 chain"
                    );
                }
            } else {
                handle.snapshot_declined.store(true, Ordering::Relaxed);
                warn!(
                    event = "eviction_snapshot_fallback",
                    cell,
                    epoch,
                    "the authoritative snapshot failed, so the handoff requires its L0 chain"
                );
            }
        }
        let artifact = match artifact {
            Some(artifact) => artifact,
            None => {
                // 0078: the skip declined the snapshot, so this final pass proves
                // the L0 chain with no new image. Read the local position without
                // syncing: the fallback's ordinary sync would mint a
                // content-free txid (the due time-based checkpoint's sealing
                // write, or `ensure_wal_exists` after the close) and upload it
                // as its own L0 just to prove commits the per-cell prefix
                // already covers. `Db::pos` cannot see a WAL-only tail, so ask
                // `has_uncaptured_changes` instead: if the WAL holds nothing
                // past the capture AND the per-cell prefix covers every local
                // commit, skip the sync and go straight to `epoch_replicated`;
                // only a real pending commit runs `sync_wait` as today (it
                // uploads the commit, so a content-free txid is not at issue).
                // Neither `ensure_wal_exists` nor `sync_before_close` runs on
                // the skip path.
                let local_covered = if skip_snapshot {
                    let handle_ = handle.clone();
                    let percell = percell_coverage(&handle);
                    asyncrt::blocking(move || {
                        let mut replica = handle_.replica.lock().unwrap();
                        let db = replica.as_mut()?.db_mut()?;
                        let clean = !db.has_uncaptured_changes().unwrap_or(true);
                        let covered = db.pos().ok().is_some_and(|pos| pos.txid.0 <= percell);
                        Some(clean && covered)
                    })
                    .await
                    .unwrap_or(None)
                    .is_some_and(|covered| covered)
                } else {
                    false
                };
                if !local_covered {
                    let remaining_ms = deadline.saturating_sub(asyncrt::mono_ms());
                    if remaining_ms == 0
                        || !matches!(
                            self.sync_wait(cell, epoch, Duration::from_millis(remaining_ms))
                                .await,
                            SyncWait::Durable
                        )
                    {
                        return Err(anyhow!("final durability failed for {cell} epoch {epoch}"));
                    }
                }
                if !self.epoch_replicated(cell, epoch).await {
                    return Err(anyhow!(
                        "no replica objects for {cell} epoch {epoch}; refusing to evict state the bucket cannot restore"
                    ));
                }
                EvictionRestoreArtifact::L0Chain
            }
        };
        info!(
            event = "eviction_durability_barrier",
            cell,
            epoch,
            artifact = ?artifact,
            elapsed_ms = asyncrt::mono_ms().saturating_sub(barrier_started_mono_ms),
            "final durability barrier passed"
        );
        // The snapshot covers the whole closed database, so the journaled
        // per-cell objects at or below its max are garbage no restore reads
        // (the planner anchors on the snapshot and only extends past it).
        // Hand their single-key DELETEs to a background task and return: the
        // deletes may land at any time after the snapshot is visible,
        // including after release and a successor's activation at its own new
        // epoch and prefix. Best effort: leftovers stay as restore-invisible
        // garbage for epoch GC and never fail the eviction.
        if let Some(snapshot_max) = trim_max {
            self.spawn_epoch_trim(
                cell,
                epoch,
                handle.client.clone(),
                Self::trim_candidates(&handle, snapshot_max),
            );
        }

        // Keep the local Db until one authoritative restore artifact is
        // remotely visible. A failed snapshot and failed L0 fallback retain
        // the handle, so the actor cannot release ownership. Every such
        // return drops the compaction pause, so a cell that the actor
        // restarts in place folds again, and a retried attempt takes a new
        // pause. Only this handoff keeps compaction off for good.
        if let Some(pause) = compaction_pause {
            pause.handed_off();
        }
        self.remove_local(cell, epoch, preserve_local);
        Ok(artifact)
    }

    /// Resident cells holding acked rows outside the per-cell layout
    /// (bundle-credited), for the graceful-close drain. Each is one
    /// per-cell snapshot away from covering its bundle rows; the seal scan
    /// decides, this only lists the work.
    pub(crate) fn bundle_drain_candidates(&self) -> Vec<(String, u64)> {
        self.cells
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, handle)| {
                handle.durable_txid.load(Ordering::SeqCst) > percell_coverage(handle)
            })
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// Cover one bundled cell per-cell without giving up the handle: publish
    /// the same authoritative snapshot the eviction publishes and feed the
    /// same `covered_txid` watermark the fleet path uses, so the graceful
    /// seal's retained scan can see the rows. A snapshot the store cannot
    /// carry (paged, over budget) or cannot publish falls back to the direct
    /// per-cell drain, exactly like the eviction's L0 fallback. Returns
    /// whether the cell is per-cell-covered now; anything uncovered keeps the
    /// record Open for successor recovery, which folds the bundles.
    pub(crate) async fn drain_bundled_cell(&self, cell: &str, epoch: u64) -> bool {
        let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        else {
            // Raced away: an eviction covered it or nothing was ever acked.
            // The retained tail, not the missing handle, decides.
            return !self
                .dirty_tails
                .lock()
                .unwrap()
                .get(cell)
                .is_some_and(|epochs| epochs.contains_key(&epoch));
        };
        if handle.durable_txid.load(Ordering::SeqCst) <= percell_coverage(&handle) {
            return true;
        }
        // 0078 leaves the graceful seal untouched: it only runs with a
        // durable-ahead-of-percell gap, where the lazy-handoff policy would
        // demand the snapshot anyway, so it always attempts it.
        match self.prepare_handoff_snapshot(&handle, false).await {
            Ok((Some(snapshot), pause)) => {
                let published = self
                    .publish_handoff_snapshot(cell, epoch, &snapshot)
                    .await
                    .is_ok();
                // A row credited past the image is not in it: report
                // uncovered and let the L0 fallback below cover it, instead
                // of certifying a snapshot that stops short of an ack.
                let covered =
                    published && handle.durable_txid.load(Ordering::SeqCst) <= snapshot.max_txid.0;
                if covered {
                    handle.uploaded_epoch_objects.store(true, Ordering::SeqCst);
                    credit_percell_upload(&handle, snapshot.max_txid.0);
                }
                drop(pause);
                if covered {
                    return true;
                }
            }
            Ok((None, _pause)) => {}
            Err(_) => {}
        }
        // The snapshot cannot carry this cell: drain per-cell like the
        // eviction fallback, under the same barrier budget. A failed drain
        // leaves the rows uncovered and the record Open.
        if !matches!(
            self.sync_wait(cell, epoch, Duration::from_secs(10)).await,
            SyncWait::Durable
        ) {
            return false;
        }
        handle.durable_txid.load(Ordering::SeqCst) <= percell_coverage(&handle)
    }

    /// Test-only: stage the bundle-loop credit for one cell without a
    /// per-cell upload, exactly as `bundle_loop` advances the handle after a
    /// credited `put_bundle`: durable and ticket watermarks move, per-cell
    /// coverage does not. Lets close/eviction tests build bundle-credited
    /// rows deterministically.
    #[cfg(test)]
    pub(crate) fn stage_bundle_credit_for_test(&self, cell: &str, epoch: u64, position: u64) {
        if let Some(handle) = self
            .cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .cloned()
        {
            handle.durable_txid.fetch_max(position, Ordering::SeqCst);
            handle.ready.notify_waiters();
        }
    }

    /// Test-only: hold (or release) one cell's sync claim, as a sync worker
    /// does between its CAS and its exit, so loop tests can pin what the
    /// bundle loop does when the claim is taken. Returns whether the claim
    /// changed hands.
    #[cfg(test)]
    pub(crate) fn set_sync_claim_for_test(&self, cell: &str, epoch: u64, held: bool) -> bool {
        self.cells
            .lock()
            .unwrap()
            .get(&(cell.to_string(), epoch))
            .is_some_and(|handle| {
                handle
                    .syncing
                    .compare_exchange(!held, held, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            })
    }

    /// Drain this handle's upload journal to the trim candidate list: only
    /// keys with `max_txid <= snapshot_max` that are not the snapshot
    /// itself. The restore planner anchors on the latest snapshot and only
    /// extends past it, so a deleted object is in no restore plan — whenever
    /// its DELETE lands, including after ownership is released and a
    /// successor activated (always at a new epoch: `next_writer_epoch`;
    /// the only same-epoch path, `resume_local` on a same-generation clean
    /// reload, needs live local files that eviction removes and uploads only
    /// strictly-new keys). Another epoch's objects are never journaled.
    fn trim_candidates(handle: &CellHandle, snapshot_max: u64) -> Vec<cello_ltx::FileInfo> {
        let uploads = std::mem::take(
            &mut *handle
                .epoch_uploads
                .lock()
                .expect("epoch upload journal poisoned"),
        );
        uploads
            .into_iter()
            .filter(|(level, _, max_txid)| {
                *level != replica::SNAPSHOT_LEVEL && *max_txid <= snapshot_max
            })
            .map(|(level, min_txid, max_txid)| cello_ltx::FileInfo {
                level,
                min_txid: TXID(min_txid),
                max_txid: TXID(max_txid),
                ..Default::default()
            })
            .collect()
    }

    /// Hand the trim keys to a background task and return at once, so an
    /// 8192-key journal never stalls the handoff or the shutdown drain (the
    /// background trim needs no evict-level deadline). The task abandons
    /// remaining keys on stop, and shutdown never waits on it beyond that;
    /// whatever stays is restore-invisible garbage for epoch GC. Eviction
    /// itself is unaffected: a refused spawn still returns its artifact.
    fn spawn_epoch_trim(
        &self,
        cell: &str,
        epoch: u64,
        client: SharedObjectStoreClient,
        files: Vec<cello_ltx::FileInfo>,
    ) {
        if files.is_empty() {
            return;
        }
        let stop = self.tasks.stop_token();
        // Count the trim before it can finish, so a waiter that sees zero
        // never misses one handed off before it looked.
        self.trim_inflight.fetch_add(1, Ordering::SeqCst);
        let inflight = self.trim_inflight.clone();
        let changed = self.trim_changed.clone();
        let cell = cell.to_string();
        let stopped_cell = cell.clone();
        if !self.tasks.spawn_owned("ltx_epoch_trim", {
            let inflight = inflight.clone();
            let changed = changed.clone();
            async move {
                Self::run_epoch_trim(client, cell.clone(), epoch, files, stop).await;
                if inflight.fetch_sub(1, Ordering::SeqCst) == 1 {
                    changed.notify_waiters();
                }
            }
        }) {
            if inflight.fetch_sub(1, Ordering::SeqCst) == 1 {
                changed.notify_waiters();
            }
            warn!(
                event = "eviction_epoch_trim",
                cell = stopped_cell,
                epoch,
                "node is stopping; superseded objects stay for epoch GC"
            );
        }
    }

    /// Delete trim keys in the background with bounded concurrency, abandoning
    /// the rest on stop. Single-key DELETEs only (`DeleteObject`, free on S3
    /// and R2); never bulk `POST ?delete`. Missing keys are ignored; other
    /// failures are counted and reported, never retried here — the leftovers
    /// stay as restore-invisible garbage for epoch GC.
    async fn run_epoch_trim(
        client: SharedObjectStoreClient,
        cell: String,
        epoch: u64,
        files: Vec<cello_ltx::FileInfo>,
        stop: StopToken,
    ) {
        const TRIM_KEYS_PER_CHUNK: usize = 64;
        const TRIM_DELETE_CONCURRENCY: usize = 8;
        let mut failed = 0_usize;
        let mut done = 0_usize;
        for chunk in files.chunks(TRIM_KEYS_PER_CHUNK) {
            let chunk_failed = asyncrt::select_biased! {
                "a stop signal abandons the remaining trim keys as GC garbage";
                _ = stop.stopped() => {
                    warn!(
                        event = "eviction_epoch_trim",
                        cell,
                        epoch,
                        done,
                        files = files.len(),
                        "node is stopping; abandoning the remaining trim keys"
                    );
                    return;
                }
                chunk_failed = client.delete_ltx_keys(chunk, TRIM_DELETE_CONCURRENCY) => chunk_failed,
            };
            done += chunk.len();
            failed += chunk_failed;
        }
        if failed == 0 {
            info!(
                event = "eviction_epoch_trim",
                cell,
                epoch,
                files = files.len(),
                "deleted per-cell objects the handoff snapshot supersedes"
            );
        } else {
            warn!(
                event = "eviction_epoch_trim",
                cell,
                epoch,
                files = files.len(),
                failed,
                "eviction trim left garbage for epoch GC"
            );
        }
    }

    async fn final_durability_barrier(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        let barrier_started_mono_ms = asyncrt::mono_ms();
        match self.sync_wait(cell, epoch, Duration::from_secs(10)).await {
            SyncWait::Durable => {}
            SyncWait::Unsupported => {
                return Err(anyhow!(
                    "final durability is unsupported for {cell} epoch {epoch}"
                ));
            }
            SyncWait::Failed => {
                return Err(anyhow!("final durability failed for {cell} epoch {epoch}"));
            }
        }
        info!(
            event = "final_durability_barrier",
            cell,
            epoch,
            elapsed_ms = asyncrt::mono_ms().saturating_sub(barrier_started_mono_ms),
            "final durability barrier passed"
        );
        Ok(())
    }

    /// Remove a stream and every stream nested below it, in the bucket and
    /// locally: `ctx.facets.delete()` of a facet and its descendants. The
    /// caller has removed their resident handles with [`Self::delete_local`].
    pub(crate) async fn delete_streams(&self, cell: &str) -> anyhow::Result<()> {
        use cello_ltx::object_store::path::Path as ObjPath;
        use futures_util::StreamExt as _;
        use futures_util::TryStreamExt as _;
        self.forget_deleted_tails(cell);
        let base = ObjPath::from(format!("{}cells/{cell}", self.prefix));
        let locations: Vec<_> = self
            .store
            .list(Some(&base))
            .map_ok(|meta| meta.location)
            .try_collect()
            .await?;
        let mut deleted = self
            .store
            .delete_stream(futures_util::stream::iter(locations.into_iter().map(Ok)).boxed());
        while let Some(result) = deleted.next().await {
            result?;
        }
        let local = self.watch.join(cell);
        if self.ltx_host.filesystem().metadata(&local).is_ok() {
            self.ltx_host.remove_dir_all(&local)?;
        }
        Ok(())
    }

    /// Drop the undrained tails of a deleted stream and every stream below it.
    /// A tail names rows that the next activation folds into the bucket, so a
    /// kept tail restores deleted rows into a recreated facet of the same
    /// name. Its live bucket watermark also stays in each shipped batch that
    /// carried those rows, and no fold will credit it now, so the deletion
    /// credits the ending's acknowledged bound.
    fn forget_deleted_tails(&self, cell: &str) {
        let below = format!("{cell}/");
        self.dirty_tails.lock().unwrap().retain(|stream, epochs| {
            let deleted = stream == cell || stream.starts_with(&below);
            if deleted {
                for tail in epochs.values() {
                    let acked = tail
                        .acked_txid
                        .max(tail.shipped_txid.load(Ordering::SeqCst));
                    tail.durable_txid.fetch_max(acked, Ordering::SeqCst);
                }
            }
            !deleted
        });
    }

    /// Whether a stream is resident: a stop that is retried after the stream
    /// stopped must not stop it again.
    pub(crate) fn is_resident(&self, cell: &str, epoch: u64) -> bool {
        self.cells
            .lock()
            .unwrap()
            .contains_key(&(cell.to_string(), epoch))
    }

    /// Discard a reset runtime without another durability attempt.
    ///
    /// The proof that triggered Reset already failed. Retrying it here can keep
    /// the unproved database resident and contradicts Reset's keep-nothing
    /// contract, so this path removes the handle and every live local file.
    /// Its fleet-acked rows are still acknowledged writes, so it keeps them
    /// uncredited behind an undrained tail until a successor fold uploads them.
    pub(crate) fn discard(&self, cell: &str, epoch: u64) {
        self.remove_local(cell, epoch, false);
    }

    /// Remove the handle of a stream that `ctx.facets.delete()` destroys.
    ///
    /// No upload or successor fold covers a deleted stream's rows, but each
    /// shipped batch that carries them still names this handle. Without a
    /// credit, the ship ledger never releases that batch, and followers keep it
    /// and every later batch until the next epoch (denoland/celld#246). The
    /// position staged at the close bounds every row a capture can ship from
    /// this handle, so crediting it releases every such batch. No undrained
    /// tail is recorded, because a fold would restore the deleted rows into a
    /// recreated facet. [`Self::discard`] must not credit the same way: a reset
    /// cell's acknowledged rows still need that fold.
    pub(crate) fn delete_local(&self, cell: &str, epoch: u64) {
        let removed = self
            .cells
            .lock()
            .unwrap()
            .remove(&(cell.to_string(), epoch));
        let Some(handle) = removed.clone() else {
            return;
        };
        match self.close_removed(cell, epoch, removed, false) {
            Some(staged) => {
                handle.durable_txid.fetch_max(staged, Ordering::SeqCst);
            }
            // Crediting a guess could release a batch above it. Keeping the
            // batch costs follower disk until the next epoch, never a row.
            None => warn!(
                cell,
                epoch, "deleted stream has no staged position; its shipped batches stay retained"
            ),
        }
    }

    fn remove_local(&self, cell: &str, epoch: u64, preserve_local: bool) {
        let removed = self.remove_active_and_record_tail(cell, epoch);
        self.close_removed(cell, epoch, removed, preserve_local);
    }

    /// Close a handle that has left the active map, and return the position
    /// staged at the close.
    fn close_removed(
        &self,
        cell: &str,
        epoch: u64,
        removed: Option<CellHandle>,
        preserve_local: bool,
    ) -> Option<u64> {
        if let Some(handle) = &removed {
            self.note_covered(cell, epoch, percell_coverage(handle));
        }
        // A paged activation's file is a sparse cache of the cut, never a
        // certified reactivation baseline, and its VFS registration ends with
        // the activation. The io methods hold their own Arcs, so open files
        // outliving the unregistration stay safe.
        let paged_vfs = removed.as_ref().and_then(|handle| handle.paged_vfs.clone());
        let preserve_local = preserve_local && paged_vfs.is_none();
        if let Some(hydration) = removed
            .as_ref()
            .and_then(|handle| handle.hydration.as_ref())
        {
            hydration.cancelled.store(true, Ordering::SeqCst);
        }
        let (staged, close_result) =
            removed.map_or((None, Ok(())), |handle| close_replica_staged(&handle));
        if let Some(name) = paged_vfs {
            let _ = cello_ltx::paged_vfs::unregister_paged_vfs(&name);
        }
        self.finish_remove_local(cell, epoch, preserve_local, close_result);
        staged
    }

    /// Move an active handle out only after its undrained-tail marker is
    /// visible. The graceful-seal check takes these locks in the same order,
    /// so it cannot observe the handle as absent before it can observe the
    /// marker that replaces it.
    fn remove_active_and_record_tail(&self, cell: &str, epoch: u64) -> Option<CellHandle> {
        let key = (cell.to_string(), epoch);
        let mut cells = self.cells.lock().unwrap();
        let handle = cells.get(&key)?;
        self.note_undrained_tail(cell, epoch, handle);
        cells.remove(&key)
    }

    /// Record an ending that leaves acked rows outside the per-cell
    /// layout. Retain each epoch's bound and live fleet and bucket watermarks
    /// without keeping the replica or database alive. Acked is the max of
    /// the fleet credit and the tiering credit — the bundle flush advances
    /// `durable_txid`, so neither alone is per-cell coverage. The live fleet
    /// watermark also retains a captured round's later credit. Covered is
    /// what a successor restore
    /// will actually see: the per-cell watermark plus the drain's L1
    /// fold. Acked rows above it can remain only in node bundles or follower
    /// fragments, so restore must gather them first. Conservative on
    /// purpose: a stale `compacted_txid` costs another recovery check,
    /// never a lost row.
    fn note_undrained_tail(&self, cell: &str, epoch: u64, handle: &Cell) {
        let acked = handle
            .shipped_txid
            .load(Ordering::SeqCst)
            .max(handle.durable_txid.load(Ordering::SeqCst));
        let covered = percell_coverage(handle);
        if acked > covered {
            let mut tails = self.dirty_tails.lock().unwrap();
            let epochs = tails.entry(cell.to_string()).or_default();
            let acked_txid = epochs
                .get(&epoch)
                .map_or(acked, |tail| tail.acked_txid.max(acked));
            epochs.insert(
                epoch,
                RetainedTail {
                    acked_txid,
                    shipped_txid: handle.shipped_txid.clone(),
                    durable_txid: handle.durable_txid.clone(),
                },
            );
        }
    }

    fn finish_remove_local(
        &self,
        cell: &str,
        epoch: u64,
        preserve_local: bool,
        close_result: anyhow::Result<()>,
    ) {
        let preserve_local = match close_result {
            Ok(()) => preserve_local,
            Err(error) => {
                warn!(
                    cell,
                    epoch,
                    %error,
                    "close managed replica failed; discarding local snapshot"
                );
                // A failed close cannot qualify the live database as a
                // reusable baseline. An orderly eviction already published
                // its final snapshot, and every other caller requested no
                // local reuse, so force any next epoch through remote restore.
                false
            }
        };
        let db = self.db_path(cell, epoch);
        if preserve_local {
            let preserved = db.with_extension("evicted");
            if let Err(error) = self.ltx_host.rename(&db, &preserved) {
                warn!(cell, epoch, %error, "preserve local snapshot failed");
            } else {
                if let Err(error) = self
                    .preserved
                    .lock()
                    .expect("preserved cache poisoned")
                    .insert(preserved)
                {
                    warn!(cell, epoch, %error, "index preserved local snapshot failed");
                }
            }
        }
        // Clear the WAL/meta siblings and the live db regardless: a reactivation
        // restores or reuses the `.hibernated` copy.
        for suffix in ["-wal", "-shm"] {
            let mut sibling = db.clone().into_os_string();
            sibling.push(suffix);
            let _ = self.ltx_host.remove_file(&PathBuf::from(sibling));
        }
        let _ = self.ltx_host.remove_dir_all(&Db::meta_path_for_path(&db));
        if !preserve_local {
            let _ = self.ltx_host.remove_file(&db);
        }
    }

    /// Capture one full snapshot after the runtime closes.
    ///
    /// A retry reuses these bytes. Recreating the image for each failed PUT
    /// would spend local I/O without changing the closed database.
    ///
    /// The returned pause holds compaction off until the caller drops it or
    /// hands the cell off. It comes back with the snapshot because a
    /// snapshot read without the pause can race a fold of the same rows.
    async fn prepare_handoff_snapshot(
        &self,
        handle: &CellHandle,
        skip_snapshot: bool,
    ) -> anyhow::Result<(Option<HandoffSnapshot>, Option<HandoffCompactionPause>)> {
        // A paged activation's local file is sparse. The snapshot page
        // collector reads non-WAL pages from the file directly, so a handoff
        // snapshot built here would publish hole-zeros as authoritative data.
        // Skip it; the eviction then proves durability through the L0 chain,
        // which is complete by construction (every write synced through WAL).
        if handle.paged_vfs.is_some() {
            return Ok((None, None));
        }
        let pause = HandoffCompactionPause::new(handle);
        let _compaction_run = match &handle.compaction {
            Some(compaction) => Some(compaction.run.lock().await),
            None => None,
        };
        // A declined snapshot still pauses compaction and waits out a running
        // round, so the retry of a failed attempt, or a later eviction after
        // an abandoned one, holds compaction off exactly as the first attempt
        // did. The first attempt's permanent cancel used to cover these
        // attempts. Returning before the run lock would let a round that was
        // already folding keep running into the L0-chain proof.
        if handle.snapshot_declined.load(Ordering::Relaxed) {
            return Ok((None, pause));
        }
        // 0078: the eviction caller evaluated the lazy-handoff policy above.
        // Declining here keeps the declined path's semantics (compaction
        // pause held, run lock taken) and flows into the existing L0-chain
        // fallback, which re-proves durability with no new proof.
        if skip_snapshot {
            return Ok((None, pause));
        }

        let snapshot_handle = handle.clone();
        // The deadline covers the snapshot's upload. A database the deadline
        // cannot carry at a conservative store rate is not attempted, so a
        // whale does not allocate and time out on its whole image at every
        // retry; the L0 chain is its restore artifact, as for a paged cell.
        let budget = usize::try_from(self.snapshot_budget_bytes.load(Ordering::Relaxed))
            .unwrap_or(usize::MAX);
        let snapshot = asyncrt::blocking(move || -> anyhow::Result<Option<HandoffSnapshot>> {
            let mut replica_slot = snapshot_handle.replica.lock().unwrap();
            let replica = replica_slot
                .as_mut()
                .ok_or_else(|| anyhow!("handoff snapshot replica is closed"))?;
            let db = replica
                .db_mut()
                .ok_or_else(|| anyhow!("handoff snapshot database is unavailable"))?;
            db.sync_before_close()
                .map_err(|error| anyhow!("capture handoff database: {error}"))?;
            let durable_txid = db.pos()?.txid;
            if durable_txid == TXID(0) {
                return Ok(None);
            }
            let mut data = Vec::new();
            let position = match db.snapshot_to_writer(&mut BoundedWriter {
                data: &mut data,
                budget,
            }) {
                Ok(position) => position,
                Err(_) if data.len() >= budget => {
                    snapshot_handle
                        .snapshot_declined
                        .store(true, Ordering::Relaxed);
                    return Ok(None);
                }
                Err(error) => return Err(anyhow!("create handoff snapshot: {error}")),
            };
            anyhow::ensure!(
                position.txid == durable_txid,
                "handoff snapshot position {} does not match durable position {}",
                position.txid.0,
                durable_txid.0,
            );
            Ok(Some(HandoffSnapshot {
                max_txid: position.txid,
                data,
            }))
        })
        .await
        .map_err(|error| anyhow!("join handoff snapshot task: {error}"))??;
        Ok((snapshot, pause))
    }

    /// Publish one full L9 snapshot of a closed cell. A successful visibility
    /// check makes this snapshot the authoritative handoff proof.
    async fn publish_handoff_snapshot(
        &self,
        cell: &str,
        epoch: u64,
        snapshot: &HandoffSnapshot,
    ) -> anyhow::Result<()> {
        let started_mono_ms = asyncrt::mono_ms();
        let info = self
            .client_for(cell, epoch)
            .write_ltx_file(
                replica::SNAPSHOT_LEVEL,
                TXID(1),
                snapshot.max_txid,
                &snapshot.data,
            )
            .await
            .map_err(|error| anyhow!("publish handoff snapshot: {error}"))?;
        anyhow::ensure!(
            info.level == replica::SNAPSHOT_LEVEL
                && info.min_txid == TXID(1)
                && info.max_txid == snapshot.max_txid,
            "handoff snapshot metadata does not match the closed database",
        );
        info!(
            event = "ltx_handoff_snapshot",
            cell,
            epoch,
            max_txid = snapshot.max_txid.0,
            bytes = info.size,
            elapsed_ms = asyncrt::mono_ms().saturating_sub(started_mono_ms),
            "published authoritative handoff snapshot"
        );
        self.note_covered(cell, epoch, snapshot.max_txid.0);
        Ok(())
    }

    /// Copy the live epoch into a private read-only snapshot for inspection.
    pub fn snapshot_active(
        &self,
        cell: &str,
        epoch: u64,
    ) -> anyhow::Result<Option<RestoredSnapshot>> {
        let source = self.db_path(cell, epoch);
        if !self
            .ltx_host
            .metadata(&source)
            .is_ok_and(|metadata| metadata.is_file)
        {
            return Ok(None);
        }
        let directory = self.watch.join(format!(".inspect-{cell}-e{epoch}"));
        let _ = self.ltx_host.remove_dir_all(&directory);
        self.ltx_host.create_dir_all(&directory)?;
        let path = directory.join("db.sqlite");
        // A paged activation's file is a sparse cache behind its VFS. A copy
        // of it in place would open the destination through the same
        // registration, and every page the backup writes there would mark
        // the cell's own hydration set without the cell's file holding it
        // (CelloPagedVfs.tla, `BreakSecondFileShares`): the live cell then
        // reads holes. Inspect a paged cell through the bucket instead.
        anyhow::ensure!(
            self.paged_vfs_name(cell, epoch).is_none(),
            "{cell} epoch {epoch} is paged in; inspect it from the bucket"
        );
        sqlite_snapshot(&source, &path, self.vfs_name.as_deref())?;
        Ok(Some(RestoredSnapshot::new(
            epoch,
            path,
            directory,
            self.ltx_host.filesystem(),
        )))
    }

    /// Restore the newest durable replica into a private snapshot without
    /// claiming or activating the cell.
    ///
    /// This reader holds no ownership, so the owner's epoch GC can delete an
    /// epoch this restore planned against. A missing planned object therefore
    /// rebuilds the chain from a fresh epoch listing and restores again; the
    /// new chain starts at a base at or above the deleted epochs. Replanning
    /// over the old chain would pick the same deleted objects, because the
    /// chain caches its listings.
    pub async fn restore_snapshot(&self, cell: &str) -> anyhow::Result<Option<RestoredSnapshot>> {
        let mut attempts_left = RESTORE_SNAPSHOT_ATTEMPTS;
        loop {
            // GC can delete a listed epoch while the chain lists its levels,
            // which leaves a continuation without its base; list again.
            let chain = match self.epoch_chain(cell, u64::MAX).await {
                Ok(Some(chain)) => chain,
                Ok(None) => return Ok(None),
                Err(_) if attempts_left > 1 => {
                    attempts_left -= 1;
                    continue;
                }
                Err(error) => return Err(error),
            };
            // A base deleted between the epoch listing and its level listing
            // does not fail the build: the walk skips the empty epoch and
            // returns a continuation whose oldest span starts past TXID 1.
            if chain.spans().first().is_none_or(|(_, lo)| *lo != TXID(1)) && attempts_left > 1 {
                attempts_left -= 1;
                continue;
            }
            let epoch = chain.spans().last().map_or(0, |(e, _)| *e);
            let directory = self.watch.join(format!(".restore-{cell}"));
            let _ = self.ltx_host.remove_dir_all(&directory);
            self.ltx_host.create_dir_all(&directory)?;
            let path = directory.join("db.sqlite");
            match replica::restore_with_host_and_download_slots(
                &chain,
                &path,
                TXID(0),
                self.ltx_host.clone(),
                self.restore_slots.clone(),
            )
            .await
            {
                Ok(_) => {
                    return Ok(Some(RestoredSnapshot::new(
                        epoch,
                        path,
                        directory,
                        self.ltx_host.filesystem(),
                    )))
                }
                Err(cello_ltx::Error::LTXMissing | cello_ltx::Error::TxNotAvailable)
                    if attempts_left > 1 =>
                {
                    attempts_left -= 1
                }
                Err(error) => return Err(anyhow!("restore snapshot {cell} e{epoch}: {error}")),
            }
        }
    }

    pub fn prune_local_cache(&self, max_bytes: u64) -> std::io::Result<(usize, usize, u64)> {
        self.preserved
            .lock()
            .expect("preserved cache poisoned")
            .prune(&self.watch, max_bytes)
    }

    /// Prove the stopped database position durable, then close the replicator
    /// while retaining the live database and WAL at their encoded path.
    pub async fn close_for_reload(&self, cell: &str, epoch: u64) -> anyhow::Result<()> {
        // Admission is already stopped, so this capture covers the exact
        // position that a clean reload preserves. Closing first can let the
        // next activation trust a local position that the bucket cannot
        // restore, and an aggregate shipper watermark cannot detect that gap.
        self.final_durability_barrier(cell, epoch).await?;
        // A paged cell's file is a sparse cache behind a VFS this process
        // registered; the next process has neither, so it cannot resume the
        // file in place (CelloPersistencePaged.tla, `BreakReloadKeepsPaged`).
        // Discard it after the proof, so the successor pages the complete
        // position in again from the bucket.
        if self.paged_vfs_name(cell, epoch).is_some() {
            self.remove_local(cell, epoch, false);
            return Ok(());
        }
        let removed = self
            .cells
            .lock()
            .unwrap()
            .remove(&(cell.to_string(), epoch));
        if let Some(handle) = removed {
            close_replica_for_reload(&handle, cell, epoch)?;
        }
        let path = self.db_path(cell, epoch);
        anyhow::ensure!(
            self.ltx_host
                .metadata(&path)
                .is_ok_and(|metadata| metadata.is_file),
            "resident database is missing: {}",
            path.display()
        );
        Ok(())
    }

    /// Enumerate live-named databases. Cached `.evicted` files are separate
    /// and remain under the ordinary cache byte limit.
    pub fn local_cells(&self) -> Vec<cello_logic::LocalCell> {
        let mut cells = Vec::new();
        let filesystem = self.ltx_host.filesystem();
        let Ok(cell_dirs) = filesystem.read_dir(&self.watch) else {
            return cells;
        };
        for cell_dir in cell_dirs {
            if !cell_dir.is_dir {
                continue;
            }
            let Some(cell) = cell_dir.file_name.to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(epochs) = filesystem.read_dir(&cell_dir.path.join("ltx")) else {
                continue;
            };
            for epoch_dir in epochs {
                let Some(epoch) = epoch_dir
                    .file_name
                    .to_str()
                    .and_then(|name| name.strip_prefix('e'))
                    .and_then(|epoch| epoch.parse::<u64>().ok())
                else {
                    continue;
                };
                let is_file = |name: &str| {
                    filesystem
                        .metadata(&epoch_dir.path.join(name))
                        .is_ok_and(|metadata| metadata.is_file)
                };
                // `.evicted` is what an eviction preserves; `.hibernated`
                // is the pre-2026-08-05 name of the same copy.
                let live = is_file("db.sqlite");
                if live || is_file("db.evicted") || is_file("db.hibernated") {
                    cells.push(cello_logic::LocalCell {
                        id: cell.clone(),
                        epoch,
                        live,
                    });
                }
            }
        }
        cells.sort();
        cells.dedup();
        cells
    }

    /// Delete stale live-named epochs after the runtime has identified and
    /// closed its exact resident set. Remote replicas remain authoritative.
    pub fn prune_stale_live(
        &self,
        keep: &std::collections::BTreeSet<(String, u64)>,
    ) -> anyhow::Result<usize> {
        let stale: Vec<_> = self
            .local_cells()
            .into_iter()
            .filter(|cell| cell.live && !keep.contains(&(cell.id.clone(), cell.epoch)))
            .collect();
        for cell in &stale {
            let db = self.db_path(&cell.id, cell.epoch);
            if let Some(parent) = db.parent() {
                self.ltx_host.remove_dir_all(parent)?;
                let mut preserved = self.preserved.lock().expect("preserved cache poisoned");
                preserved.forget(&db.with_extension("evicted"));
                preserved.forget(&db.with_extension("hibernated"));
            }
        }
        let remaining: std::collections::BTreeSet<_> = self
            .local_cells()
            .into_iter()
            .map(|cell| (cell.id, cell.epoch))
            .collect();
        anyhow::ensure!(
            &remaining == keep,
            "clean reload inventory mismatch after pruning: expected {}, found {}",
            keep.len(),
            remaining.len()
        );
        Ok(stale.len())
    }

    /// Delete every live-named epoch that an unclean predecessor (a crash, or
    /// a stop without a clean-reload marker) left. Only a clean reload resumes
    /// a live-named epoch, and a restart claims each cell at a new epoch from
    /// the bucket, so no activation opens these files again. The caller runs
    /// this after predecessor recovery and before the core starts.
    pub fn prune_unclean_restart(&self) -> anyhow::Result<usize> {
        let (live, keep): (Vec<_>, Vec<_>) =
            self.local_cells().into_iter().partition(|cell| cell.live);
        let keep = keep.into_iter().map(|cell| (cell.id, cell.epoch)).collect();
        *self
            .pruned_at_boot
            .lock()
            .expect("boot prune list poisoned") = live;
        self.prune_stale_live(&keep)
    }

    /// The live-named epochs `prune_unclean_restart` deleted, at most once.
    pub fn take_pruned_at_boot(&self) -> Vec<cello_logic::LocalCell> {
        std::mem::take(
            &mut *self
                .pruned_at_boot
                .lock()
                .expect("boot prune list poisoned"),
        )
    }

    /// Delete live-named epoch dirs of `cell` below `keep_epoch`.
    ///
    /// A newer live epoch supersedes the older ones: the owner has moved
    /// on, and a future activation restores from the bucket, never from the
    /// stale local image (upstream #311). Epochs with a resident replica
    /// are never touched — the activation hook below runs after the new
    /// handle installs, so a still-draining old epoch keeps its files until
    /// its own eviction removes them. Best-effort: a failure is logged and
    /// skipped, a leftover dir costs inodes, never correctness. Returns the
    /// number of removed epoch dirs.
    pub fn prune_superseded_epochs(&self, cell: &str, keep_epoch: u64) -> usize {
        let active: std::collections::BTreeSet<(String, u64)> =
            self.cells.lock().unwrap().keys().cloned().collect();
        let mut pruned = 0;
        for local in self.local_cells() {
            if local.id != cell || !local.live || local.epoch >= keep_epoch {
                continue;
            }
            if active.contains(&(local.id.clone(), local.epoch)) {
                continue;
            }
            let db = self.db_path(&local.id, local.epoch);
            let Some(parent) = db.parent() else {
                continue;
            };
            match self.ltx_host.remove_dir_all(parent) {
                Ok(()) => {
                    pruned += 1;
                    let mut preserved = self.preserved.lock().expect("preserved cache poisoned");
                    preserved.forget(&db.with_extension("evicted"));
                    preserved.forget(&db.with_extension("hibernated"));
                }
                Err(error) => {
                    warn!(
                        cell = %local.id,
                        epoch = local.epoch,
                        %error,
                        "superseded epoch prune failed"
                    );
                }
            }
        }
        if pruned > 0 {
            info!(
                event = "ltx_epoch_prune",
                cell, keep_epoch, pruned, "removed superseded live-named epoch dirs"
            );
        }
        pruned
    }

    /// Startup sweep: for every cell with more than one live-named epoch,
    /// drop the older dirs. Nothing is resident yet, so every older live
    /// dir is superseded by definition (upstream #311). A clean-reload
    /// boot is unaffected: its shutdown already pruned to the exact
    /// resident set, so the sweep is a no-op there; every other boot
    /// restores from the bucket, so a wrongly-aged dir costs a refetch,
    /// never data.
    pub fn prune_superseded_epochs_all(&self) -> usize {
        let mut newest: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
        for local in self.local_cells() {
            if !local.live {
                continue;
            }
            newest
                .entry(local.id)
                .and_modify(|epoch| *epoch = (*epoch).max(local.epoch))
                .or_insert(local.epoch);
        }
        newest
            .iter()
            .map(|(cell, epoch)| self.prune_superseded_epochs(cell, *epoch))
            .sum()
    }

    /// There is no external process, so the in-process replicator is healthy while cello runs.
    pub fn process_status(&self) -> std::io::Result<Option<std::process::ExitStatus>> {
        Ok(None)
    }
}

/// One capture+upload for a cell: advance its durable position on success and
/// wake its waiters. Everything committed before the capture is durable once
/// uploaded, so the target is read before `db.sync`.
///
/// The capture runs under the replica mutex on a blocking thread; the upload
/// runs OFF the mutex. A slow bucket PUT held the lock for its whole round
/// trip, and the log tier's ship capture queued behind it — the lab measured
/// 4-6 s ack spikes at every flush collision. Uploads are idempotent
/// overwrites keyed by TXID, and `syncing` already guarantees one pass per
/// cell, so staging then uploading lock-free is the same protocol
/// `Replica::sync` runs, minus the contention.
///
/// `Some(true)` means success, `Some(false)` means failure, and `None` means
/// that the replica lost its database.
async fn sync_cell(handle: CellHandle) -> Option<bool> {
    // Tickets taken before the capture: their writes committed before
    // `db.sync` runs, so it captures them. Read before the capture so a
    // ticket taken during the sync is credited by the next one, not this.
    type Staged = (u64, Vec<(u64, Vec<u8>)>);
    let handle_ = handle.clone();
    let staged: Option<Result<Staged, ()>> = asyncrt::blocking(move || {
        let captured = handle_.req_seq.load(Ordering::SeqCst);
        note_capture(&handle_, captured);
        let mut replica = handle_.replica.lock().unwrap();
        let from = replica.as_mut()?.pos().txid.0 + 1;
        let db = replica.as_mut()?.db_mut()?;
        let handoff_pending = handle_.handoff_ticket.load(Ordering::SeqCst)
            > handle_.synced_seq.load(Ordering::SeqCst);
        let synced = if handoff_pending {
            db.sync_before_close()
        } else {
            db.sync()
        };
        if let Err(error) = synced {
            warn!(%error, "ltx wal capture failed");
            return Some(Err(()));
        }
        let dpos = match db.pos() {
            Ok(pos) => pos,
            Err(error) => {
                warn!(%error, "ltx position read failed");
                return Some(Err(()));
            }
        };
        let mut files = Vec::new();
        for txid in from..=dpos.txid.0 {
            match db.read_ltx_file(0, TXID(txid), TXID(txid)) {
                Ok(bytes) => files.push((txid, bytes)),
                Err(error) => {
                    warn!(%error, txid, "read staged l0 failed");
                    return Some(Err(()));
                }
            }
        }
        Some(Ok((captured, files)))
    })
    .await
    .unwrap_or(Some(Err(())));
    let (captured, files) = match staged {
        None => return None,
        Some(Err(())) => {
            handle.ready.notify_waiters();
            return Some(false);
        }
        Some(Ok(staged)) => staged,
    };
    let last = files.last().map(|(txid, _)| *txid);
    // A paced fleet normally keeps this tail in node bundles instead of the
    // per-cell prefix. A handoff must materialize that prefix before it can
    // delete the local copy, but one PUT per transaction makes the drain cost
    // grow with the hot cell's lifetime. Fold the contiguous tail exactly as
    // dead-node recovery does, so the durability cut has one remote write.
    // A malformed or discontinuous tail keeps the conservative per-row path.
    let merged = LtxRepl::merge_l0_rows(&files);
    let uploads: Vec<(u64, u64, &[u8])> = match (files.first(), files.last(), merged.as_ref()) {
        (Some((min_txid, _)), Some((max_txid, _)), Some(bytes)) => {
            vec![(*min_txid, *max_txid, bytes.as_slice())]
        }
        _ => files
            .iter()
            .map(|(txid, bytes)| (*txid, *txid, bytes.as_slice()))
            .collect(),
    };
    for (min_txid, max_txid, bytes) in &uploads {
        if let Some(compaction) = &handle.compaction {
            compaction
                .pending_bytes
                .fetch_add(bytes.len() as u64, Ordering::SeqCst);
        }
        #[cfg(all(test, cello_internal_tests))]
        let round_ordinal = handle.begin_upload_round_for_world(captured, *min_txid, *max_txid);
        if let Err(error) = handle
            .client
            .write_ltx_file(0, TXID(*min_txid), TXID(*max_txid), bytes)
            .await
        {
            #[cfg(all(test, cello_internal_tests))]
            handle.finish_upload_round_for_world(
                round_ordinal,
                LtxUploadRoundStatusForWorldV1::Failed,
            );
            warn!(%error, min_txid, max_txid, "ltx upload failed");
            handle
                .last_sync_ms
                .store(asyncrt::wall_ms().max(0) as u64, Ordering::SeqCst);
            handle.ready.notify_waiters();
            return Some(false);
        }
        #[cfg(all(test, cello_internal_tests))]
        handle.finish_upload_round_for_world(
            round_ordinal,
            LtxUploadRoundStatusForWorldV1::Completed,
        );
        // The PUT acked: journal the key so the eviction trim can delete this
        // L0 from the journal alone, with no LIST.
        note_epoch_upload(&handle, 0, *min_txid, *max_txid);
    }
    // Every staged file uploaded: this residency holds bucket proof of its
    // own that the successor's LIST would find. Record it so the eviction
    // gates can skip their visibility LISTs. An empty stage uploaded
    // nothing and marks nothing, so an upload-free residency keeps the LIST.
    if !uploads.is_empty() {
        handle.uploaded_epoch_objects.store(true, Ordering::SeqCst);
    }
    if let Some(last) = last {
        // Advance the replica's uploaded watermark; `syncing` serializes
        // passes, so nothing else moved it meanwhile.
        // A close can win after staging, so credit only a still-open replica.
        // The durable watermark remains valid when the replica is closed.
        if let Some(replica) = handle.replica.lock().unwrap().as_mut() {
            replica.seed_pos(Pos::new(TXID(last), 0));
        }
        credit_percell_upload(&handle, last);
        #[cfg(all(test, cello_internal_tests))]
        handle.pause_after_sync_credit_for_world().await;
    }
    handle.synced_seq.fetch_max(captured, Ordering::SeqCst);
    maybe_queue_compaction(&handle, handle.durable_txid.load(Ordering::SeqCst));
    handle
        .last_sync_ms
        .store(asyncrt::wall_ms().max(0) as u64, Ordering::SeqCst);
    note_proof(&handle);
    handle.ready.notify_waiters();
    Some(true)
}

fn maybe_queue_compaction(handle: &CellHandle, durable_txid: u64) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    let due_by_txids = durable_txid
        .saturating_sub(compaction.compacted_txid.load(Ordering::SeqCst))
        >= compaction.min_txids;
    let due_by_bytes = compaction.pending_bytes.load(Ordering::SeqCst) >= compaction.min_bytes;
    if due_by_txids || due_by_bytes {
        enqueue_compaction(handle, "threshold");
    }
}

/// Queues one round regardless of the thresholds, behind the same
/// cancellation, backoff and single-flight guards as a threshold round.
fn enqueue_compaction(handle: &CellHandle, trigger: &'static str) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    if compaction.halted()
        || asyncrt::mono_ms() < compaction.retry_after_ms.load(Ordering::SeqCst)
        || compaction
            .queued
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
    {
        return;
    }
    if compaction
        .queue
        .send(CompactionWork {
            cell: Arc::downgrade(handle),
            queued_at_mono_ms: asyncrt::mono_ms(),
            trigger,
        })
        .is_err()
    {
        compaction.queued.store(false, Ordering::SeqCst);
    }
}

fn cancel_compaction(handle: &CellHandle) {
    let Some(compaction) = &handle.compaction else {
        return;
    };
    compaction.cancelled.store(true, Ordering::SeqCst);
    compaction.cancel.notify_waiters();
}

fn start_compaction_loop(
    config: CompactionConfig,
    tasks: TaskGroup,
) -> mpsc::UnboundedSender<CompactionWork> {
    let (queue, mut work) = mpsc::unbounded_channel::<CompactionWork>();
    let slots = Arc::new(Semaphore::new(config.concurrency));
    let stop = tasks.stop_token();
    tasks
        .clone()
        .spawn_owned("ltx_compaction_dispatcher", async move {
            loop {
                let next = asyncrt::select_biased! {
                    "a stop signal that ties queued compaction work starts no new worker";
                    _ = stop.stopped() => break,
                    next = work.recv() => next,
                };
                let Some(work) = next else { break };
                let permit = asyncrt::select_biased! {
                    "a stop signal that ties slot acquisition starts no new compaction";
                    _ = stop.stopped() => break,
                    permit = slots.clone().acquire_owned() => permit,
                };
                let Ok(permit) = permit else {
                    break;
                };
                let Some(cell) = work.cell.upgrade() else {
                    continue;
                };
                let requeues = tasks.clone();
                tasks.spawn_owned("ltx_compaction_worker", async move {
                    let _permit = permit;
                    compact_cell(
                        cell,
                        work.queued_at_mono_ms,
                        work.trigger,
                        true,
                        Some(requeues),
                    )
                    .await;
                });
            }
        });
    queue
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompactionOutcome {
    Advanced,
    Current,
    Failed,
    Cancelled,
}

async fn compact_cell(
    handle: CellHandle,
    queued_at_mono_ms: u64,
    trigger: &'static str,
    cancellable: bool,
    requeues: Option<TaskGroup>,
) -> CompactionOutcome {
    let Some(compaction) = &handle.compaction else {
        return CompactionOutcome::Current;
    };
    let cancelled = compaction.cancel.notified();
    tokio::pin!(cancelled);
    if cancellable && compaction.halted() {
        compaction.queued.store(false, Ordering::SeqCst);
        return CompactionOutcome::Cancelled;
    }
    let _run = compaction.run.lock().await;
    if cancellable && compaction.halted() {
        compaction.queued.store(false, Ordering::SeqCst);
        return CompactionOutcome::Cancelled;
    }

    let source_position = || {
        (
            handle.durable_txid.load(Ordering::SeqCst),
            handle.percell_txid.load(Ordering::SeqCst),
        )
    };
    let source_at_start = source_position();
    let queue_ms = asyncrt::mono_ms().saturating_sub(queued_at_mono_ms);
    let started = asyncrt::mono_ms();
    // The run lock keeps the snapshot stable through listing and every row
    // open. The next round must see bundles published since this one began.
    *compaction.fetcher.rows.lock().await = None;
    let compactor = ReplicaCompactor::new(&compaction.client)
        .with_host(compaction.host.clone())
        .with_verification(true)
        .with_local_path(&compaction.local_path)
        .with_limits(COMPACTION_MAX_FILES, COMPACTION_MAX_INPUT_BYTES)
        .with_base(TXID(compaction.base_txid));
    let result = {
        let worker = compactor.compact_with_progress(1);
        tokio::pin!(worker);
        if cancellable {
            asyncrt::select_biased! {
                "a cancellation that ties compaction completion discards the cancelled round";
                _ = &mut cancelled => None,
                result = &mut worker => Some(result),
            }
        } else {
            Some(worker.as_mut().await)
        }
    };

    *compaction.fetcher.rows.lock().await = None;
    #[cfg(all(test, cello_internal_tests))]
    {
        let pause = compaction.finish_pause.lock().unwrap().take();
        if let Some(pause) = pause {
            pause.reached.store(true, Ordering::SeqCst);
            pause.release.notified().await;
        }
    }
    let outcome = match result {
        Some(Ok(CompactionResult {
            output: Some(output),
            ..
        })) => {
            let info = output.info;
            compaction
                .compacted_txid
                .store(info.max_txid.0, Ordering::SeqCst);
            // The fold consumed the tail it saw; bytes that landed during it
            // count toward the next one only approximately, which delays it
            // by at most one window.
            compaction.pending_bytes.store(0, Ordering::SeqCst);
            // The L1 PUT acked inside the round: journal the key so the
            // eviction trim can delete it from the journal alone, with no
            // LIST. The trim filter keeps only keys the handoff snapshot
            // supersedes.
            note_epoch_upload(&handle, info.level, info.min_txid.0, info.max_txid.0);
            // Local L0 retention (upstream #311): the fold just proved
            // every covered TXID durable in L1, so the local L0 files are
            // redundant. Best-effort: a leftover file costs inodes, never
            // correctness.
            let retain_host = compaction.host.clone();
            let retain_meta = compaction.local_path.clone();
            let retain_through = info.max_txid;
            let retain_cell = compaction.cell.clone();
            let retain_epoch = compaction.epoch;
            let _ = asyncrt::blocking(move || {
                match cello_ltx::Db::retain_l0_through(&retain_host, &retain_meta, retain_through) {
                    Ok(deleted) if deleted > 0 => {
                        info!(
                            event = "ltx_l0_retention",
                            cell = %retain_cell,
                            epoch = retain_epoch,
                            deleted,
                            max_txid = retain_through.0,
                            "deleted compacted local L0 files"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        warn!(
                            cell = %retain_cell,
                            epoch = retain_epoch,
                            %error,
                            "local L0 retention failed"
                        );
                    }
                }
            })
            .await;
            info!(
                event = "ltx_compaction",
                trigger,
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = info.level,
                min_txid = info.min_txid.0,
                max_txid = info.max_txid.0,
                input_objects = output.input_files,
                input_bytes = output.input_bytes,
                local_input_objects = output.local_input_files,
                remote_input_objects = output.input_files - output.local_input_files,
                output_bytes = info.size,
                queue_ms,
                elapsed_ms = asyncrt::mono_ms().saturating_sub(started),
                result = "ok",
                "compacted an additive LTX level"
            );
            CompactionOutcome::Advanced
        }
        Some(Ok(CompactionResult {
            covered_txid,
            output: None,
        })) => {
            compaction
                .compacted_txid
                .store(covered_txid.0, Ordering::SeqCst);
            info!(
                event = "ltx_compaction",
                trigger,
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = asyncrt::mono_ms().saturating_sub(started),
                result = "no_work",
                max_txid = covered_txid.0,
                "the additive LTX level has no visible source tail"
            );
            CompactionOutcome::Current
        }
        Some(Err(error)) => {
            warn!(
                event = "ltx_compaction",
                trigger,
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = asyncrt::mono_ms().saturating_sub(started),
                result = "error",
                %error,
                "additive LTX compaction failed"
            );
            CompactionOutcome::Failed
        }
        None => {
            info!(
                event = "ltx_compaction",
                trigger,
                cell = %compaction.cell,
                epoch = compaction.epoch,
                source_level = 0,
                destination_level = 1,
                queue_ms,
                elapsed_ms = asyncrt::mono_ms().saturating_sub(started),
                result = "cancelled",
                "cancelled an additive LTX compaction"
            );
            CompactionOutcome::Cancelled
        }
    };
    let failure_pause = if outcome == CompactionOutcome::Failed {
        let failures = compaction.failures.fetch_add(1, Ordering::SeqCst);
        let pause = Duration::from_secs((30u64 << failures.min(4)).min(300));
        compaction.retry_after_ms.store(
            asyncrt::mono_ms().saturating_add(pause.as_millis() as u64),
            Ordering::SeqCst,
        );
        Some(pause)
    } else {
        compaction.failures.store(0, Ordering::SeqCst);
        compaction.retry_after_ms.store(0, Ordering::SeqCst);
        None
    };
    // Publish the delay before admitting another ship-triggered attempt.
    compaction.queued.store(false, Ordering::SeqCst);

    // A no-work result proves coverage only through the sources that the
    // overlay can see, which can end below durable_txid. Self-requeueing
    // would spin on the same empty listing. Preserve a publication that raced
    // this attempt: its queue request can lose the queued CAS. Read its two
    // watermarks after clearing queued, so a later publisher queues itself.
    // A failed round schedules its own retry after the backoff, so a cell
    // with no later ship round still retries.
    let requeue = match outcome {
        CompactionOutcome::Advanced | CompactionOutcome::Failed => true,
        CompactionOutcome::Current => source_position() != source_at_start,
        CompactionOutcome::Cancelled => false,
    };
    // Only the permanent cancel drops the requeue. A handoff pause can lift
    // before the requeue fires, and `enqueue_compaction` re-checks `halted`
    // then, so gating on the pause here would lose a round that completed
    // during an abandoned eviction.
    if requeue && !compaction.cancelled.load(Ordering::SeqCst) {
        // Pace consecutive rounds for one cell: a restart with a large tail
        // otherwise drains back-to-back for minutes. The pause matches the
        // round it follows (capped), so a cell compacts at half duty cycle
        // while the worker slot frees for other cells immediately. The owner
        // retains this delayed task, but the task does not retain the permit.
        let pause = failure_pause.unwrap_or_else(|| {
            Duration::from_millis(asyncrt::mono_ms().saturating_sub(started))
                .min(Duration::from_secs(2))
        });
        let handle_ = Arc::downgrade(&handle);
        let Some(requeues) = requeues else {
            return outcome;
        };
        let stop = requeues.stop_token();
        requeues.spawn_owned("ltx_compaction_requeue", async move {
            asyncrt::select_biased! {
                "a stop signal that ties the requeue pause prevents another compaction round";
                _ = stop.stopped() => return,
                _ = asyncrt::sleep(pause) => {},
            }
            let Some(handle_) = handle_.upgrade() else {
                return;
            };
            let durable_txid = handle_.durable_txid.load(Ordering::SeqCst);
            maybe_queue_compaction(&handle_, durable_txid);
        });
    }
    outcome
}

fn compaction_config_from_env() -> anyhow::Result<Option<CompactionConfig>> {
    // On by default. A mixed fleet must set `0` until every node can read
    // v0.5.2 block objects. An old reader cannot take over a cell after its
    // first L1 publication.
    let enabled = crate::env_vars::flag("CELLO_LTX_COMPACTION", true)?;
    if !enabled {
        return Ok(None);
    }

    let min_txids = crate::env_vars::with_default("CELLO_LTX_COMPACTION_MIN_TXIDS", 256)?;
    let min_mb: u64 = crate::env_vars::with_default("CELLO_LTX_COMPACTION_MIN_MB", 32)?;
    let concurrency = crate::env_vars::with_default("CELLO_LTX_COMPACTIONS", 2)?;
    anyhow::ensure!(
        min_mb > 0 && min_mb << 20 <= COMPACTION_MAX_INPUT_BYTES,
        "CELLO_LTX_COMPACTION_MIN_MB must be between 1 and {}",
        COMPACTION_MAX_INPUT_BYTES >> 20
    );
    anyhow::ensure!(
        min_txids >= 2,
        "CELLO_LTX_COMPACTION_MIN_TXIDS must be at least 2"
    );
    anyhow::ensure!(concurrency > 0, "CELLO_LTX_COMPACTIONS must be positive");
    Ok(Some(CompactionConfig {
        min_txids: min_txids as u64,
        min_bytes: min_mb << 20,
        concurrency,
    }))
}

/// The node's background sync loop: wake on a dirty cell (or a slow tick) and
/// launch a sync for every cell whose committed position runs ahead of its
/// durable one. Each cell's sync is an independent, self-rescheduling task —
/// the loop does *not* wait for the batch to finish — so one slow cell's upload
/// never stalls the others (a cell keeps its own cadence up to the concurrency
/// bound). A cell's writes reported between its syncs still clear on one upload:
/// the batching win, without the cross-cell head-of-line blocking.
async fn sync_loop(
    cells: Arc<Mutex<BTreeMap<(String, u64), CellHandle>>>,
    dirty: Arc<Notify>,
    slots: Arc<Semaphore>,
    registration: Arc<Mutex<RegistrationState>>,
    stop: StopToken,
    sync_tasks: TaskGroup,
    flush_ms: u64,
) {
    loop {
        asyncrt::select_biased! {
            "a stop signal that ties a sync-loop wake prevents another cell scan";
            _ = stop.stopped() => break,
            _ = async {
                asyncrt::select_biased! {
                    "a dirty notification wins a tie with the fallback tick to retain wake order";
                    _ = dirty.notified() => {},
                    _ = asyncrt::sleep(Duration::from_millis(25)) => {},
                }
            } => {},
        }
        if stop.is_stopped() {
            break;
        }
        // The upload-cadence dial. With a healthy shipper installed, acks
        // ride the followers, so uploads become tiering and are PACED: an
        // immediate upload would hold the replica mutex for a bucket round
        // trip and the ship capture would queue behind it, putting the
        // bucket back on the ack path — the lab measured exactly that.
        // Without a shipper (or degraded), uploads run immediately: they
        // are the ack path again.
        let registered = registered_durability(&registration);
        let paced = flush_ms > 0
            && registered
                .as_ref()
                .is_some_and(|targets| targets.shipper.active());
        // With an active bundle sink, the bundle loop owns paced tiering
        // entirely — one PUT per node-flush instead of one per cell. This
        // loop then serves only the unpaced (degraded) mode and the direct
        // sync_wait callers, which are the drain points.
        // Layer 2 bucket path: with no shipper, `paced` is false, but per-cell
        // L0 uploads must still stop while bucket bundling is active — the bundle
        // PUT is the ack path there, not the per-cell one. When the gate closes
        // (or credit is refused and bundling stops), this falls back at once to
        // today's per-cell uploads for new writes.
        let bucket_bundling = registered
            .as_ref()
            .is_some_and(|targets| targets.manager.bucket_bundling());
        let bundle_active = registered
            .as_ref()
            .is_some_and(|targets| targets.manager.bundle_active());
        let bundling = sync_should_bundle(paced, bundle_active, bucket_bundling);
        let now = asyncrt::wall_ms().max(0) as u64;
        let work: Vec<CellHandle> = {
            let map = cells.lock().unwrap();
            map.values()
                .filter(|c| {
                    c.req_seq.load(Ordering::SeqCst) > c.synced_seq.load(Ordering::SeqCst)
                        && !bundling
                        && (!paced
                            || now.saturating_sub(c.last_sync_ms.load(Ordering::SeqCst))
                                >= flush_ms)
                })
                .cloned()
                .collect()
        };
        for cell in work {
            // Claim the cell; skip if a sync is already in flight for it.
            if cell
                .syncing
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
            {
                continue;
            }
            let slots = slots.clone();
            let dirty = dirty.clone();
            let worker_stop = stop.clone();
            let worker_registration = registration.clone();
            sync_tasks.spawn_owned("ltx_cell_sync", async move {
                // Keep syncing this cell while it stays dirty, rather than
                // notifying the main loop to re-scan every completion — that made
                // the loop wake O(cells) times and starved throughput as cells
                // accumulated. This is not a busy loop: each iteration awaits an
                // object-store upload (~one round-trip). A *failed* sync would
                // not, so it backs off, keeping the only tight iterations the
                // ones that actually uploaded.
                loop {
                    if worker_stop.is_stopped() {
                        break;
                    }
                    // The bundle loop owns tiering while armed: a worker spawned
                    // before the arm (or during a gap) must yield once the bundle
                    // path takes over, or it uploads per-cell L0s beside every
                    // bundle flush. A continuously-dirty cell never catches up to
                    // break on clean, so without this the two loops duplicate
                    // every row (measured on the hot workload with store
                    // latency). Breaking is safe: the bundle loop credits the
                    // same watermarks, and if bundling drops the next scan
                    // re-spawns this worker.
                    if bundle_owns_tiering(&worker_registration, paced) {
                        break;
                    }
                    let ok = {
                        let _permit = slots.acquire().await;
                        sync_cell(cell.clone()).await
                    };
                    // Registry removal closes the replica while this owned
                    // task can still own the Cell. A closed slot cannot become
                    // dirty again, so stop instead of retrying it forever.
                    if ok.is_none() {
                        break;
                    }
                    if cell.req_seq.load(Ordering::SeqCst) <= cell.synced_seq.load(Ordering::SeqCst)
                    {
                        break;
                    }
                    // Under pacing, one upload per wake: the next round waits
                    // for the flush interval instead of re-syncing here.
                    if paced {
                        break;
                    }
                    if ok != Some(true) {
                        asyncrt::select_biased! {
                            "a stop signal that ties retry backoff prevents another sync attempt";
                            _ = worker_stop.stopped() => break,
                            _ = asyncrt::sleep(Duration::from_millis(50)) => {},
                        }
                    }
                }
                cell.syncing.store(false, Ordering::SeqCst);
                // A write landing in the clear window is picked up next tick;
                // nudge the loop so it does not wait the full interval.
                if !worker_stop.is_stopped()
                    && cell.req_seq.load(Ordering::SeqCst) > cell.synced_seq.load(Ordering::SeqCst)
                {
                    dirty.notify_one();
                }
            });
        }
    }
}

/// The bundle loop: paced like the per-cell tiering it replaces, but the
/// unit is the node, not the cell. Every dirty cell's captured L0 segments
/// go up as ONE object per flush interval — the Class A collapse — and the
/// per-cell prefixes stay untouched until a drain point needs them. The
/// crediting mirrors sync_cell: `durable_txid` means bucket-covered,
/// whether by a per-cell object or a bundle row; the replica's own
/// position deliberately does NOT advance, so the direct sync_wait drain
/// still knows exactly which frames lack per-cell objects.
async fn bundle_loop(
    cells: Arc<Mutex<BTreeMap<(String, u64), CellHandle>>>,
    registration: Arc<Mutex<RegistrationState>>,
    stop: StopToken,
    flush_ms: u64,
    dirty: Arc<Notify>,
) {
    // Fleet path keeps its interval pacing; bucket path is event-driven: flush
    // when dirty and no PUT is in flight, accumulating while one is in flight
    // and flushing again as soon as it completes. The loop awaits each PUT, so
    // writes arriving mid-PUT are captured by the next iteration — batching
    // grows with load. An optional linger (`CELLO_BUCKET_BUNDLE_LINGER_MS`,
    // default 0) holds a shared round open that long after the first dirty
    // wake — but only while batching can help: a lone dirty cell has nothing
    // to batch with, so it flushes at once instead of paying the full linger
    // for no fewer PUTs. At moderate load with spread-out writes, no linger
    // means ~one bundle per write (the s3bench agent workload measured 1206
    // bundle PUTs for 1200 writes); a linger batches concurrent dirt into
    // fewer PUTs at that much ack latency each.
    let bucket_linger = bucket_bundle_linger();
    let fleet_tick_ms = flush_ms;
    let mut fleet_tick = (fleet_tick_ms > 0).then(|| {
        let mut tick = asyncrt::interval(Duration::from_millis(fleet_tick_ms));
        tick.set_missed_tick_behavior(asyncrt::MissedTickBehavior::Delay);
        tick
    });
    loop {
        // Choose the wait by posture: bucket waits on dirty (event-driven), fleet on
        // its interval tick. Re-evaluate every iteration so a gate close/open flips
        // at once without waiting out the other posture's cadence.
        let bucket_wake = registered_durability(&registration)
            .is_some_and(|targets| targets.manager.bucket_candidate());
        if bucket_wake {
            asyncrt::select_biased! {
                "a stop signal that ties the bucket bundle wait prevents another flush";
                _ = stop.stopped() => break,
                _ = dirty.notified() => {},
            }
            if stop.is_stopped() {
                break;
            }
            // The linger is a batching window, not a pacing delay: hold it
            // open only while more than one cell is dirty. A lone cell
            // flushes at once — waiting would add the whole linger to one
            // ack without batching anything (writes arriving mid-PUT still
            // join the next round).
            if !bucket_linger.is_zero() && dirty_cells(&cells) > 1 {
                asyncrt::select_biased! {
                    "a stop signal that ties the bucket bundle linger prevents another flush";
                    _ = stop.stopped() => break,
                    _ = asyncrt::sleep(bucket_linger) => {},
                }
            }
        } else if let Some(tick) = fleet_tick.as_mut() {
            asyncrt::select_biased! {
                "a stop signal that ties the bundle tick prevents another bucket flush";
                _ = stop.stopped() => break,
                _ = tick.tick() => {},
            }
            if stop.is_stopped() {
                break;
            }
        } else {
            // No fleet interval and no bucket demand: park until dirty or stop, then
            // re-evaluate. This keeps `flush_ms == 0` test topologies (which never
            // pace) able to bundle event-driven while fleet stays off.
            asyncrt::select_biased! {
                "a stop signal that ties the idle bundle wait prevents another flush";
                _ = stop.stopped() => break,
                _ = dirty.notified() => {},
            }
            if stop.is_stopped() {
                break;
            }
            continue;
        }
        let installed = registered_durability(&registration).map(|targets| targets.manager.clone());
        let Some(active) = installed else {
            continue;
        };
        // Bucket record-before-first-ack: the lease chain publishes the
        // bucket-session record before any bundle-credited ack, and the record
        // is never deleted. A fenced session, a failed lease write or a closed
        // gate refuses here; `bucket_bundling` stays false, so `sync_loop`
        // keeps uploading per cell and the acks ride those uploads.
        if active.bucket_candidate() && !active.ensure_bucket_record().await.unwrap_or(false) {
            continue;
        }
        if !active.bundle_active() {
            continue;
        }
        let work: Vec<((String, u64), CellHandle)> = {
            let map = cells.lock().unwrap();
            map.iter()
                .filter(|(_, cell)| {
                    cell.req_seq.load(Ordering::SeqCst) > cell.synced_seq.load(Ordering::SeqCst)
                })
                .map(|(key, cell)| (key.clone(), cell.clone()))
                .collect()
        };
        if work.is_empty() {
            continue;
        }

        // A round whose dirty set is a single cell with a single pending
        // ticket uploads that cell per-cell, exactly as the gate-closed
        // fallback does today: a one-cell bundle saves no PUT but adds a
        // record re-read, a GC delete and bookkeeping. `sync_cell` IS that
        // fallback's function (journal, uploaded flag, per-cell + durable
        // credit, waiter notify), so there is no second upload convention.
        // A lone cell with more pending rides a one-cell bundle as before:
        // batching those rows into one PUT is the whole point, and the hot
        // shape is one cell with many concurrent writers — routing it
        // per-cell would serialize one PUT per round and never batch
        // (measured: 135 per-cell PUTs and zero bundles). Rounds with two
        // or more dirty cells bundle as now. Fleet posture is untouched:
        // followers need the bundle rows, so the paced path keeps batching
        // single cells into node bundles.
        if active.bucket_bundling() && work.len() == 1 {
            let pending = work[0]
                .1
                .req_seq
                .load(Ordering::SeqCst)
                .saturating_sub(work[0].1.synced_seq.load(Ordering::SeqCst));
            if pending <= 1 {
                let handle = work[0].1.clone();
                // Claim the cell the way the sync scan does. A sync worker
                // that already holds the claim may be about to yield to this
                // loop (`bundle_owns_tiering`) without uploading, and its exit
                // nudges only the scan, which bundling suppresses: skipping the
                // round here would park the ticket until the next write or the
                // durability timeout. So a failed claim falls through to a
                // one-cell bundle instead; a worker upload that races it only
                // duplicates rows the `fetch_max` watermarks absorb.
                if handle
                    .syncing
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    // The verdict needs no branch: credited rows ride per-cell
                    // acks through `sync_cell`'s own notify, and anything still
                    // dirty stays dirty for the next round.
                    if sync_cell(handle.clone()).await != Some(true) {
                        // A failed lone-cell upload must fall back like a failed bundle PUT:
                        // drop bundling so the scan re-spawns a sync worker that retries with
                        // backoff, instead of parking this round with the scan suppressed.
                        active.note_bucket_bundle_stalled();
                    }
                    handle.syncing.store(false, Ordering::SeqCst);
                    continue;
                }
            }
        }
        type Credits = Vec<(CellHandle, u64, u64)>;
        let (entries, credits): (Vec<cello_ltx::bundle::BundleEntry>, Credits) =
            asyncrt::blocking(move || {
                let mut entries = Vec::new();
                let mut credits = Vec::new();
                for ((cell, epoch), handle) in work {
                    let tickets = handle.req_seq.load(Ordering::SeqCst);
                    note_capture(&handle, tickets);
                    let mut replica = handle.replica.lock().unwrap();
                    let Some(db) = managed_db_mut(&mut replica) else {
                        continue;
                    };
                    if db.sync().is_err() {
                        continue;
                    }
                    let Ok(pos) = db.pos() else { continue };
                    let from = handle.durable_txid.load(Ordering::SeqCst) + 1;
                    let mut complete = true;
                    for txid in from..=pos.txid.0 {
                        match db.read_ltx_file(0, TXID(txid), TXID(txid)) {
                            Ok(bytes) => entries.push(cello_ltx::bundle::BundleEntry {
                                cell: cell.clone(),
                                cell_epoch: epoch,
                                txid,
                                bytes,
                            }),
                            Err(error) => {
                                warn!(%error, txid, "read staged l0 for bundle failed");
                                complete = false;
                                break;
                            }
                        }
                    }
                    drop(replica);
                    if complete {
                        credits.push((handle, tickets, pos.txid.0));
                    }
                }
                (entries, credits)
            })
            .await
            .unwrap_or_default();
        if credits.is_empty() {
            continue;
        }
        let count = entries.len();
        // Cap a bundle by rows and bytes and split above the cap: one PUT per chunk,
        // each with its own sequence, so a hot node never builds an unbounded object.
        // Credit requires every chunk credited; any refusal falls back to per-cell.
        let is_bucket = active.bucket_bundling();
        let mut flushed_ok = false;
        if entries.is_empty() {
            flushed_ok = true;
        } else if is_bucket {
            flushed_ok = true;
            for chunk in split_bundle_entries(entries) {
                if !active.put_bundle(chunk).await {
                    flushed_ok = false;
                    break;
                }
            }
        } else if active.put_bundle(entries).await {
            flushed_ok = true;
        }
        if flushed_ok {
            if count > 0 {
                info!(
                    event = "log_bundle_flush",
                    entries = count,
                    cells = credits.len(),
                    "flushed a bundle"
                );
            }
            for (handle, tickets, position) in credits {
                handle.durable_txid.fetch_max(position, Ordering::SeqCst);
                handle.synced_seq.fetch_max(tickets, Ordering::SeqCst);
                handle
                    .last_sync_ms
                    .store(asyncrt::wall_ms().max(0) as u64, Ordering::SeqCst);
                // Bundle credits also queue the overlay compactor.
                maybe_queue_compaction(&handle, position);
                note_proof(&handle);
                handle.ready.notify_waiters();
            }
        }
    }
}

/// The log tier's group-commit loop, `sync_loop`'s fleet twin: wake on a
/// gate ticket, capture every dirty cell's new L0 segments in one blocking
/// pass, ship them as one batch, and credit the tickets the capture covered.
/// Ordered member lanes keep each follower's pipelined fragment contiguous,
/// and nothing on this path waits for the bucket.
/// Advance a lap clock and return the elapsed microseconds — the ship
/// loop's closed-book accounting primitive: every await and every stretch
/// of work between two laps lands in exactly one bucket, so the buckets
/// plus the residual sum to the loop's wall time.
fn lap_us(lap: &mut u64) -> u64 {
    let now = asyncrt::mono_us();
    let delta = now.saturating_sub(*lap);
    *lap = now;
    delta
}

async fn ship_loop(
    cells: Arc<Mutex<BTreeMap<(String, u64), CellHandle>>>,
    dirty_ship: Arc<Notify>,
    registration: Arc<Mutex<RegistrationState>>,
    stop: StopToken,
    group_commit_ms: u64,
) {
    // The truncation ledger is a core decision
    // (cello_logic::log_tier::ShipLedger): outstanding batches carry the
    // cells and top TXIDs, a batch is covered once every cell's durable
    // position passes its top TXID — bundle credits do this within a
    // flush interval — and the covered watermark rides the next append as
    // the followers' truncate_to. This is what bounds follower disks, and
    // the ledger's epoch reset is what keeps a stale watermark from
    // truncating a fresh fragment.
    let mut ledger: cello_logic::log_tier::ShipLedger<Vec<(CellHandle, u64)>> =
        cello_logic::log_tier::ShipLedger::default();
    // Rounds in flight, completed strictly in submission order. A later
    // round's ack waits here until every earlier round has credited or
    // failed, so a gate never releases past an unresolved earlier round.
    let mut inflight: futures_util::stream::FuturesOrdered<
        std::pin::Pin<Box<dyn std::future::Future<Output = ShipRound> + Send>>,
    > = futures_util::stream::FuturesOrdered::new();
    let mut current: Option<Arc<dyn Shipper>> = None;
    let mut current_epoch = None;
    let mut last_submit = asyncrt::mono_ms();
    const CAPTURE_WORKERS: usize = 8;
    // The loop's own time ledger: idle (nothing to do), stall (admission
    // or depth closed, waiting on a round), apply, scan, capture wall,
    // submit. Emitted cumulatively about once a second; wall minus the
    // sum is the loop's untracked residual and must stay small.
    let mut lap = asyncrt::mono_us();
    let (mut idle_us, mut stall_us, mut apply_us, mut scan_us) = (0_u64, 0_u64, 0_u64, 0_u64);
    let (mut group_us, mut capture_us, mut submit_us) = (0_u64, 0_u64, 0_u64);
    let mut loop_rounds = 0_u64;
    let mut ledger_emit = asyncrt::mono_us();
    'shipping: loop {
        if asyncrt::mono_us().saturating_sub(ledger_emit) >= 1_000_000 {
            info!(
                event = "log_ship_loop",
                wall_us = asyncrt::mono_us().saturating_sub(ledger_emit),
                idle_us,
                stall_us,
                apply_us,
                scan_us,
                group_us,
                capture_us,
                submit_us,
                rounds = loop_rounds,
                "ship loop time ledger"
            );
            ledger_emit = asyncrt::mono_us();
            idle_us = 0;
            stall_us = 0;
            apply_us = 0;
            scan_us = 0;
            group_us = 0;
            capture_us = 0;
            submit_us = 0;
            loop_rounds = 0;
        }
        if inflight.is_empty() {
            let _ = lap_us(&mut lap);
            asyncrt::select_biased! {
                "a stop signal that ties an idle ship-loop wake prevents another scan";
                _ = stop.stopped() => break 'shipping,
                _ = async {
                    asyncrt::select_biased! {
                        "a dirty notification wins a tie with the fallback tick to retain wake order";
                        _ = dirty_ship.notified() => {},
                        _ = asyncrt::sleep(Duration::from_millis(25)) => {},
                    }
                } => {},
            }
            idle_us += lap_us(&mut lap);
        } else {
            let depth = current
                .as_ref()
                .map_or(1, |shipper| shipper.pipeline().max(1));
            let admitted = current.as_ref().is_none_or(|shipper| shipper.admit());
            if inflight.len() >= depth || !admitted {
                let _ = lap_us(&mut lap);
                let round = asyncrt::select_biased! {
                    "a stop signal that ties a completed ship round prevents another ledger update";
                    _ = stop.stopped() => break 'shipping,
                    round = futures_util::StreamExt::next(&mut inflight) => round,
                };
                stall_us += lap_us(&mut lap);
                if let Some(round) = round {
                    if !apply_round(&mut ledger, round) {
                        inflight = futures_util::stream::FuturesOrdered::new();
                        reset_submitted(&cells);
                    }
                }
                apply_us += lap_us(&mut lap);
                continue;
            }
            let _ = lap_us(&mut lap);
            let event = asyncrt::select_biased! {
                "a stop signal that ties active shipping work ends the ship loop first";
                _ = stop.stopped() => break 'shipping,
                event = async {
                    asyncrt::select_biased! {
                        "a completed ship round wins a tie so its ordered ledger update runs first";
                        round = futures_util::StreamExt::next(&mut inflight) => {
                            futures_util::future::Either::Left(round)
                        },
                        _ = async {
                            asyncrt::select_biased! {
                                "a dirty notification wins a tie with the fallback tick to retain wake order";
                                _ = dirty_ship.notified() => {},
                                _ = asyncrt::sleep(Duration::from_millis(25)) => {},
                            }
                        } => futures_util::future::Either::Right(()),
                    }
                } => event,
            };
            idle_us += lap_us(&mut lap);
            if let futures_util::future::Either::Left(round) = event {
                if let Some(round) = round {
                    if !apply_round(&mut ledger, round) {
                        inflight = futures_util::stream::FuturesOrdered::new();
                        reset_submitted(&cells);
                    }
                }
                apply_us += lap_us(&mut lap);
                continue;
            }
        }
        // Producer grouping shares a transaction within one Queue. This wait
        // also gathers writes from different Queues into one fleet capture.
        // Only pending Queue writes trigger it; other cell classes alone do
        // not add the wait. This preliminary scan is only a delay decision;
        // the authoritative work and shipper epoch are read after the wait.
        let queue_pending = if group_commit_ms == 0 {
            false
        } else {
            let map = cells.lock().unwrap();
            map.iter().any(|((cell, _), handle)| {
                cell.split_once(':')
                    .is_some_and(|(class, _)| class == crate::deploy::QUEUE_CLASS)
                    && {
                        let req = handle.req_seq.load(Ordering::SeqCst);
                        req > handle.submitted_seq.load(Ordering::SeqCst)
                            && req > handle.synced_seq.load(Ordering::SeqCst)
                    }
            })
        };
        if queue_pending {
            let _ = lap_us(&mut lap);
            asyncrt::select_biased! {
                "a stop signal that ties the group-commit window prevents another capture";
                _ = stop.stopped() => break 'shipping,
                _ = asyncrt::sleep(Duration::from_millis(group_commit_ms)) => {},
            }
            group_us += lap_us(&mut lap);
        }
        let installed = registered_durability(&registration).map(|targets| targets.shipper.clone());
        let Some(active) = installed.filter(|shipper| shipper.active()) else {
            // The shipper is gone or degraded: outstanding rounds can never
            // credit under it, so they die uncredited — conservative, the
            // gates ride the bucket proof.
            if !inflight.is_empty() {
                inflight = futures_util::stream::FuturesOrdered::new();
                reset_submitted(&cells);
            }
            current = None;
            current_epoch = None;
            continue;
        };
        let capture_epoch = active.epoch();
        if current_epoch != Some(capture_epoch) {
            // An ensemble swap orphans the old shipper's rounds: their
            // credits belong to the retired epoch and must not apply.
            if !inflight.is_empty() {
                inflight = futures_util::stream::FuturesOrdered::new();
                reset_submitted(&cells);
            }
            current = Some(active.clone());
            current_epoch = Some(capture_epoch);
        }
        ledger.observe_epoch(capture_epoch);
        let work: Vec<((String, u64), CellHandle)> = {
            let map = cells.lock().unwrap();
            map.iter()
                .filter(|(_, cell)| {
                    let req = cell.req_seq.load(Ordering::SeqCst);
                    req > cell.submitted_seq.load(Ordering::SeqCst)
                        && req > cell.synced_seq.load(Ordering::SeqCst)
                })
                .map(|(key, cell)| (key.clone(), cell.clone()))
                .collect()
        };
        scan_us += lap_us(&mut lap);
        if work.is_empty() {
            continue;
        }
        let round = asyncrt::mono_ms();
        // Capture fans out across cells: each chunk syncs and reads its
        // cells exactly as the serial walk did — per-cell contiguity and
        // the completeness check are per cell, so cross-cell order is
        // free — and the fan-out is what keeps a network disk's per-cell
        // sync latency out of the round's serial cost (#140, stage 2).
        let workers = work.len().clamp(1, CAPTURE_WORKERS);
        let mut chunks: Vec<Vec<((String, u64), CellHandle)>> =
            (0..workers).map(|_| Vec::new()).collect();
        for (index, item) in work.into_iter().enumerate() {
            chunks[index % workers].push(item);
        }
        type Credits = Vec<(CellHandle, u64, u64)>;
        let spawned = asyncrt::mono_us();
        let captures = futures_util::future::join_all(chunks.into_iter().map(|chunk| {
            asyncrt::blocking(move || {
                // The closed-book ledger: pool_wait is the blocking-pool
                // queue delay before this chunk ran at all; phases below
                // sum to the chunk's busy time.
                let pool_wait_us = asyncrt::mono_us().saturating_sub(spawned);
                let mut entries = Vec::new();
                let mut credits = Vec::new();
                let mut sync_us = 0_u64;
                let mut lock_us = 0_u64;
                let mut read_us = 0_u64;
                let mut timing = cello_ltx::db::SyncTiming::default();
                let mut snap_reasons = [0_u32; 9];
                let mut read_kinds = [0_u32; 4];
                let mut read_bytes = 0_u64;
                // The null-work probe: eight clock reads cost well under a
                // microsecond of intrinsic work, so this value is the
                // scheduler's preemption tax on this worker — the
                // discriminator between a phase that is slow and a phase
                // that was interrupted.
                let probe_started = asyncrt::mono_us();
                for _ in 0..8 {
                    std::hint::black_box(asyncrt::mono_us());
                }
                let probe_us = asyncrt::mono_us().saturating_sub(probe_started);
                for ((cell, epoch), handle) in chunk {
                    // Tickets taken before the capture are covered by it —
                    // the same discipline as sync_cell.
                    let tickets = handle.req_seq.load(Ordering::SeqCst);
                    note_capture(&handle, tickets);
                    let lock_started = asyncrt::mono_us();
                    let mut replica = handle.replica.lock().unwrap();
                    let sync_started = asyncrt::mono_us();
                    lock_us += sync_started.saturating_sub(lock_started);
                    let Some(db) = managed_db_mut(&mut replica) else {
                        continue;
                    };
                    let synced = db.sync();
                    sync_us += asyncrt::mono_us().saturating_sub(sync_started);
                    let cell_timing = db.last_sync_timing();
                    timing.prepare_us += cell_timing.prepare_us;
                    timing.verify_us += cell_timing.verify_us;
                    timing.encode_write_us += cell_timing.encode_write_us;
                    timing.fsync_us += cell_timing.fsync_us;
                    timing.checkpoint_us += cell_timing.checkpoint_us;
                    timing.checkpoint_runs += cell_timing.checkpoint_runs;
                    timing.checkpoint_wal_frames += cell_timing.checkpoint_wal_frames;
                    timing.checkpoint_backfilled += cell_timing.checkpoint_backfilled;
                    timing.checkpoint_busy += cell_timing.checkpoint_busy;
                    timing.checkpoint_busy_errors += cell_timing.checkpoint_busy_errors;
                    timing.checkpoint_restarts += cell_timing.checkpoint_restarts;
                    timing.pos_us += cell_timing.pos_us;
                    timing.wal_read_us += cell_timing.wal_read_us;
                    timing.map_collect_us += cell_timing.map_collect_us;
                    timing.ltx_encode_us += cell_timing.ltx_encode_us;
                    timing.file_write_us += cell_timing.file_write_us;
                    timing.wal_len_bytes += cell_timing.wal_len_bytes;
                    if cell_timing.snapshot {
                        timing.snapshot = true;
                        snap_reasons[cell_timing.snapshot_reason.min(8) as usize] += 1;
                    }
                    read_kinds[cell_timing.wal_read_kind.min(3) as usize] += 1;
                    read_bytes += cell_timing.wal_read_bytes;
                    if synced.is_err() {
                        continue;
                    }
                    let Ok(pos) = db.pos() else { continue };
                    let from = handle.submitted_txid.load(Ordering::SeqCst) + 1;
                    let mut complete = true;
                    let read_started = asyncrt::mono_us();
                    for txid in from..=pos.txid.0 {
                        match db.read_ltx_file(0, TXID(txid), TXID(txid)) {
                            Ok(bytes) => entries.push(ShipEntry {
                                cell: cell.clone(),
                                epoch,
                                txid,
                                bytes,
                            }),
                            // A pruned L0 the bucket already holds is not a
                            // gap the followers need filled; anything else
                            // leaves the cell uncredited for this round.
                            Err(_) if txid <= handle.durable_txid.load(Ordering::SeqCst) => {}
                            Err(_) => {
                                complete = false;
                                break;
                            }
                        }
                    }
                    read_us += asyncrt::mono_us().saturating_sub(read_started);
                    drop(replica);
                    if complete {
                        credits.push((handle, tickets, pos.txid.0));
                    }
                }
                (
                    entries,
                    credits,
                    sync_us,
                    lock_us,
                    read_us,
                    pool_wait_us,
                    timing,
                    snap_reasons,
                    read_kinds,
                    read_bytes,
                    probe_us,
                )
            })
        }))
        .await;
        let mut entries: Vec<ShipEntry> = Vec::new();
        let mut credits: Credits = Vec::new();
        let mut sync_us_total = 0_u64;
        let mut lock_us_total = 0_u64;
        let mut read_us_total = 0_u64;
        let mut pool_wait_us_max = 0_u64;
        let mut timing_total = cello_ltx::db::SyncTiming::default();
        let mut snap_reason_totals = [0_u32; 9];
        let mut read_kind_totals = [0_u32; 4];
        let mut read_bytes_total = 0_u64;
        let mut probe_us_max = 0_u64;
        for capture in captures {
            let (
                chunk_entries,
                chunk_credits,
                chunk_sync_us,
                chunk_lock_us,
                chunk_read_us,
                chunk_pool_wait_us,
                chunk_timing,
                chunk_snap_reasons,
                chunk_read_kinds,
                chunk_read_bytes,
                chunk_probe_us,
            ) = capture.unwrap_or_default();
            entries.extend(chunk_entries);
            credits.extend(chunk_credits);
            sync_us_total += chunk_sync_us;
            lock_us_total += chunk_lock_us;
            read_us_total += chunk_read_us;
            pool_wait_us_max = pool_wait_us_max.max(chunk_pool_wait_us);
            timing_total.prepare_us += chunk_timing.prepare_us;
            timing_total.verify_us += chunk_timing.verify_us;
            timing_total.encode_write_us += chunk_timing.encode_write_us;
            timing_total.fsync_us += chunk_timing.fsync_us;
            timing_total.checkpoint_us += chunk_timing.checkpoint_us;
            timing_total.checkpoint_runs += chunk_timing.checkpoint_runs;
            timing_total.checkpoint_wal_frames += chunk_timing.checkpoint_wal_frames;
            timing_total.checkpoint_backfilled += chunk_timing.checkpoint_backfilled;
            timing_total.checkpoint_busy += chunk_timing.checkpoint_busy;
            timing_total.checkpoint_busy_errors += chunk_timing.checkpoint_busy_errors;
            timing_total.checkpoint_restarts += chunk_timing.checkpoint_restarts;
            timing_total.pos_us += chunk_timing.pos_us;
            timing_total.wal_read_us += chunk_timing.wal_read_us;
            timing_total.map_collect_us += chunk_timing.map_collect_us;
            timing_total.ltx_encode_us += chunk_timing.ltx_encode_us;
            timing_total.file_write_us += chunk_timing.file_write_us;
            timing_total.wal_len_bytes += chunk_timing.wal_len_bytes;
            if chunk_timing.snapshot {
                timing_total.snapshot = true;
            }
            for (slot, count) in snap_reason_totals.iter_mut().zip(chunk_snap_reasons) {
                *slot += count;
            }
            for (slot, count) in read_kind_totals.iter_mut().zip(chunk_read_kinds) {
                *slot += count;
            }
            read_bytes_total += chunk_read_bytes;
            probe_us_max = probe_us_max.max(chunk_probe_us);
        }
        if credits.is_empty() {
            continue;
        }
        ledger.advance(|cells| ship_batch_covered(cells));
        let covered_seq = ledger.covered_seq();
        let captured_ms = asyncrt::mono_ms().saturating_sub(round);
        capture_us += lap_us(&mut lap);
        let entry_count = entries.len();
        let byte_count = entries.iter().map(|entry| entry.bytes.len()).sum::<usize>();
        let since_last = asyncrt::mono_ms().saturating_sub(last_submit);
        // No entries can mean that an earlier in-flight round owns every
        // frame, not only that the bucket already covers them. Queue the
        // empty credit through the same ordered pipeline, so an earlier
        // failure discards it instead of releasing an unproved fleet ticket.
        #[cfg(all(test, cello_internal_tests))]
        let observer_rounds = credits
            .iter()
            .map(|(handle, tickets, position)| {
                let count = entries
                    .iter()
                    .filter(|entry| {
                        entry.cell == handle.observer_cell && entry.epoch == handle.observer_epoch
                    })
                    .count();
                handle.begin_fleet_capture_for_world(*tickets, *position, count, capture_epoch)
            })
            .collect::<Vec<_>>();
        let append = if entries.is_empty() {
            None
        } else {
            last_submit = asyncrt::mono_ms();
            // The shipper's synchronous prefix runs HERE, at submission: the
            // sequence range and every member lane enqueue happen before the
            // future is queued, so pipelined rounds stay ordered per member.
            Some(active.ship_at_epoch(capture_epoch, entries, covered_seq))
        };
        for (handle, tickets, position) in &credits {
            handle.submitted_txid.fetch_max(*position, Ordering::SeqCst);
            handle.submitted_seq.fetch_max(*tickets, Ordering::SeqCst);
        }
        let submitted = asyncrt::mono_ms();
        inflight.push_back(Box::pin(async move {
            ShipRound {
                #[cfg(all(test, cello_internal_tests))]
                observer_rounds,
                completion: match append {
                    Some(append) => ShipRoundCompletion::Append(append.await),
                    None => ShipRoundCompletion::OrderedEmpty,
                },
                credits,
                covered_seq,
                entries: entry_count,
                bytes: byte_count,
                capture_ms: captured_ms,
                sync_ms: sync_us_total / 1000,
                lock_ms: lock_us_total / 1000,
                gap_ms: since_last,
                submitted,
                read_us: read_us_total,
                pool_wait_us: pool_wait_us_max,
                sync_timing: timing_total,
                snap_reasons: snap_reason_totals,
                read_kinds: read_kind_totals,
                read_bytes: read_bytes_total,
                probe_us: probe_us_max,
            }
        }));
        submit_us += lap_us(&mut lap);
        loop_rounds += 1;
    }
}

/// The proof carried by one ordered ship-loop round. `FuturesOrdered` holds an
/// `OrderedEmpty` no-op behind each earlier append, and a failed append
/// discards the complete later tail before the no-op can apply.
enum ShipRoundCompletion {
    Append(ShipCompletion),
    OrderedEmpty,
}

/// One pipelined round's completion, applied strictly in submission order.
struct ShipRound {
    #[cfg(all(test, cello_internal_tests))]
    observer_rounds: Vec<LtxFleetCaptureForWorld>,
    completion: ShipRoundCompletion,
    credits: Vec<(CellHandle, u64, u64)>,
    covered_seq: u64,
    entries: usize,
    bytes: usize,
    capture_ms: u64,
    /// Worker-summed per-cell db.sync time inside the capture.
    sync_ms: u64,
    /// Worker-summed replica-lock wait inside the capture.
    lock_ms: u64,
    /// Time since the previous round's submission — the cadence.
    gap_ms: u64,
    submitted: u64,
    /// The closed-book capture interior, worker-summed per round:
    /// frame reads, the worst chunk's blocking-pool queue delay, and the
    /// per-phase split of every db.sync (the log-lazy-local-sync
    /// attribution ledger).
    read_us: u64,
    pool_wait_us: u64,
    sync_timing: cello_ltx::db::SyncTiming,
    snap_reasons: [u32; 9],
    read_kinds: [u32; 4],
    read_bytes: u64,
    probe_us: u64,
}

/// Apply one completed round. `false` means the round failed: the caller
/// must discard every later in-flight round uncredited — their frames may
/// be durable on the followers, which is safe, but crediting them would
/// release gates past an unresolved earlier round.
fn apply_round(
    ledger: &mut cello_logic::log_tier::ShipLedger<Vec<(CellHandle, u64)>>,
    round: ShipRound,
) -> bool {
    let proof_last_seq = match &round.completion {
        ShipRoundCompletion::Append(completion) => {
            let Some(last_seq) = completion.last_seq() else {
                #[cfg(all(test, cello_internal_tests))]
                for observer in &round.observer_rounds {
                    observer.finish(LtxFleetCaptureStatusForWorld::Failed);
                }
                return false;
            };
            if last_seq > round.covered_seq {
                ledger.shipped(
                    last_seq,
                    round
                        .credits
                        .iter()
                        .map(|(handle, _, position)| (handle.clone(), *position))
                        .collect(),
                );
            }
            Some(last_seq)
        }
        // No append completed for this round. Its credits are safe because
        // the ordered queue first applied every earlier append, or because
        // the bucket already held the captured position. Do not invent a
        // fleet sequence for this credit.
        ShipRoundCompletion::OrderedEmpty => None,
    };
    info!(
        event = "log_ship_round",
        entries = round.entries,
        cells = round.credits.len(),
        bytes = round.bytes,
        capture_ms = round.capture_ms,
        sync_ms = round.sync_ms,
        lock_ms = round.lock_ms,
        gap_ms = round.gap_ms,
        ship_ms = asyncrt::mono_ms().saturating_sub(round.submitted),
        read_us = round.read_us,
        pool_wait_us = round.pool_wait_us,
        prep_us = round.sync_timing.prepare_us,
        verify_us = round.sync_timing.verify_us,
        encode_us = round.sync_timing.encode_write_us,
        fsync_us = round.sync_timing.fsync_us,
        ckpt_us = round.sync_timing.checkpoint_us,
        ckpt_n = round.sync_timing.checkpoint_runs,
        ckpt_frames = round.sync_timing.checkpoint_wal_frames,
        ckpt_done = round.sync_timing.checkpoint_backfilled,
        ckpt_busy = round.sync_timing.checkpoint_busy,
        ckpt_busy_err = round.sync_timing.checkpoint_busy_errors,
        ckpt_restart = round.sync_timing.checkpoint_restarts,
        pos_us = round.sync_timing.pos_us,
        wal_read_us = round.sync_timing.wal_read_us,
        map_collect_us = round.sync_timing.map_collect_us,
        ltx_encode_us = round.sync_timing.ltx_encode_us,
        file_write_us = round.sync_timing.file_write_us,
        wal_len_bytes = round.sync_timing.wal_len_bytes,
        snapshot = round.sync_timing.snapshot,
        snap_first = round.snap_reasons[1],
        snap_truncated = round.snap_reasons[2],
        snap_salt = round.snap_reasons[3],
        snap_lastpage = round.snap_reasons[4],
        snap_ckpt = round.snap_reasons[5],
        snap_boundary = round.snap_reasons[6],
        snap_other = round.snap_reasons[7],
        snap_prebarrier = round.snap_reasons[8],
        read_tail = round.read_kinds[0],
        read_snap = round.read_kinds[1],
        read_start = round.read_kinds[2],
        read_fallback = round.read_kinds[3],
        read_bytes = round.read_bytes,
        probe_us = round.probe_us,
        proof_last_seq = ?proof_last_seq,
        "shipped a log batch"
    );
    #[cfg(all(test, cello_internal_tests))]
    for observer in &round.observer_rounds {
        observer.finish(LtxFleetCaptureStatusForWorld::Applied);
    }
    for (handle, tickets, position) in round.credits {
        #[cfg(all(test, cello_internal_tests))]
        handle.record_fleet_credit_for_world(tickets, position, proof_last_seq);
        handle.shipped_txid.fetch_max(position, Ordering::SeqCst);
        handle.shipped_seq.fetch_max(tickets, Ordering::SeqCst);
        note_proof(&handle);
        handle.ready.notify_waiters();
    }
    true
}

/// Discarding a pipeline makes every uncredited range eligible for capture
/// again. The credited watermarks are monotone, so concurrent bucket progress
/// remains intact while a failed fleet tail rolls back.
fn reset_submitted(cells: &Arc<Mutex<BTreeMap<(String, u64), CellHandle>>>) {
    for handle in cells.lock().unwrap().values() {
        handle
            .submitted_txid
            .store(handle.shipped_txid.load(Ordering::SeqCst), Ordering::SeqCst);
        handle
            .submitted_seq
            .store(handle.shipped_seq.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}

/// Node-level object-store config (no per-cell prefix). `build_store` on this
/// yields the one shared client; per-cell clients set only `path`.
fn node_config(
    bucket: &str,
    endpoint: Option<&str>,
    region: &str,
    credentials: Option<&StorageCredentials>,
) -> ObjectStoreConfig {
    let endpoint = endpoint.unwrap_or_default().to_string();
    // Static credentials come from the managed control plane when present,
    // else the `AWS_*` env the node already carries. Without this,
    // `build_store` sees empty keys and object_store walks the refreshable
    // chain instead: web identity, ECS task credentials, EKS Pod Identity,
    // then the instance credential provider, which off-EC2 sends unsigned
    // requests (R2 answers "404 page not found"). A node that carries none of
    // those variables still reaches that unsigned case, and a node that
    // carries some of them silently replicates under an identity the control
    // plane did not issue.
    let env = |key: &str| std::env::var(key).ok().filter(|value| !value.is_empty());
    let access_key_id = credentials
        .map(|c| c.access_key_id.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_ACCESS_KEY_ID"))
        .unwrap_or_default();
    let secret_access_key = credentials
        .map(|c| c.secret_access_key.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_SECRET_ACCESS_KEY"))
        .unwrap_or_default();
    // Temporary R2/STS credentials require the session token, or signing fails.
    let session_token = credentials
        .and_then(|c| c.session_token.clone())
        .filter(|value| !value.is_empty())
        .or_else(|| env("AWS_SESSION_TOKEN"))
        .unwrap_or_default();
    ObjectStoreConfig {
        bucket: bucket.to_string(),
        path: String::new(),
        region: region.to_string(),
        // A custom endpoint (R2/MinIO) uses path-style addressing, matching
        // `ObjectStoreConfig::from_url`'s default for non-AWS hosts.
        force_path_style: !endpoint.is_empty(),
        endpoint,
        access_key_id,
        secret_access_key,
        session_token,
        skip_verify: false,
        part_size: 0,
        timestamp_metadata_key: TimestampMetadataKey::default(),
    }
}

fn production_ltx_host() -> LtxHost {
    execution_domain_ltx_host()
}

#[cfg(any(test, cello_internal_tests))]
fn deterministic_ltx_host() -> LtxHost {
    execution_domain_ltx_host()
}

fn execution_domain_ltx_host() -> LtxHost {
    let filesystem = asyncrt::fs();
    let age_filesystem = filesystem.clone();
    let read_filesystem = filesystem.clone();
    LtxHost::new(
        asyncrt::wall_ms,
        move |path| file_age(age_filesystem.as_ref(), path),
        move |path| {
            let filesystem = read_filesystem.clone();
            async move {
                asyncrt::blocking(move || filesystem.read(&path))
                    .await
                    .map_err(|error| std::io::Error::other(error.to_string()))?
            }
        },
        |job| async move {
            asyncrt::blocking(job)
                .await
                .map_err(|error| HostTaskError::new(error.to_string()))
        },
    )
    .with_filesystem(filesystem)
}

fn file_age(filesystem: &dyn cello_ltx::FileSystem, path: &Path) -> std::io::Result<Duration> {
    let modified = filesystem.metadata(path)?.modified_unix_millis;
    Ok(Duration::from_millis(
        asyncrt::wall_ms().saturating_sub(modified).max(0) as u64,
    ))
}

fn close_replica_or_warn(handle: &CellHandle, cell: &str, epoch: u64) {
    if let Err(error) = close_replica(handle) {
        warn!(cell, epoch, %error, "close managed replica failed during removal");
    }
}

fn managed_db_mut(
    replica: &mut Option<Replica<SharedObjectStoreClient>>,
) -> Option<&mut cello_ltx::Db> {
    replica.as_mut()?.db_mut()
}

fn close_replica_for_reload(handle: &CellHandle, cell: &str, epoch: u64) -> anyhow::Result<()> {
    close_replica(handle)
        .map_err(|error| anyhow!("close managed replica for {cell} epoch {epoch}: {error}"))
}

/// Stop new file users and close the managed database before its live path is
/// renamed or retained for another process. An upload or a compaction can keep
/// the `Cell` alive after registry removal, but none can keep the database once
/// this function takes it through the same mutex used by every capture.
fn close_replica(handle: &CellHandle) -> anyhow::Result<()> {
    close_replica_staged(handle).1
}

/// Close the managed database, and return the position staged at the close
/// with the close result. Every capture syncs and reads its position under the
/// replica mutex that the close takes the database from, so no capture can
/// ship a later position from this handle. A failed close still returns the
/// position, because no capture can reach the database after the take.
fn close_replica_staged(handle: &CellHandle) -> (Option<u64>, anyhow::Result<()>) {
    cancel_compaction(handle);
    let replica = handle.replica.lock().unwrap().take();
    let Some(mut db) = replica.and_then(Replica::into_db) else {
        return (None, Ok(()));
    };
    let staged = db.pos().ok().map(|pos| pos.txid.0);
    (staged, db.close().map_err(|error| anyhow!(error)))
}

impl Drop for LtxRepl {
    fn drop(&mut self) {
        self.shutdown_local_fallback();
    }
}
#[cfg(test)]
#[allow(clippy::disallowed_methods)] // Test tempdirs live outside the injected filesystem.
mod evict_trim_tests {
    use super::*;
    use cello_ltx::object_store::memory::InMemory;
    use cello_ltx::object_store::path::Path as ObjPath;
    // `futures::stream::BoxStream`, spelled out: the cello crate carries
    // futures-util only, and this names the same object type.
    type ListStream = std::pin::Pin<
        Box<dyn futures_util::Stream<Item = StoreResult<ObjectMeta>> + Send + 'static>,
    >;
    use cello_ltx::object_store::GetOptions;
    use cello_ltx::object_store::GetResult;
    use cello_ltx::object_store::ListResult;
    use cello_ltx::object_store::MultipartUpload;
    use cello_ltx::object_store::ObjectMeta;
    use cello_ltx::object_store::PutMultipartOptions;
    use cello_ltx::object_store::PutOptions;
    use cello_ltx::object_store::PutPayload;
    use cello_ltx::object_store::PutResult;
    use cello_ltx::object_store::Result as StoreResult;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;

    /// An in-memory store that counts PUTs, LISTs, HEADs, and DELETEs under
    /// one per-cell prefix, and can fail PUTs and DELETEs on demand, so the
    /// tests below can assert the eviction trim's effect and its best-effort
    /// failure mode, the guard's HEAD-before-LIST discipline, and the
    /// eviction barrier's refusal when no artifact can land. Every other
    /// operation delegates to the inner store untouched.
    #[derive(Debug)]
    struct CountingStore {
        inner: InMemory,
        prefix: String,
        puts: AtomicUsize,
        lists: AtomicUsize,
        heads: AtomicUsize,
        deletes: AtomicUsize,
        fail_puts: AtomicBool,
        fail_deletes: AtomicBool,
        /// Counted deletes at or past this total fail. `u64::MAX` (the
        /// default) fails nothing; `set_fail_after(n)` lets the next `n`
        /// succeed.
        fail_after: AtomicU64,
        /// While set, deletes under the prefix park until `release_deletes`.
        hold_deletes: AtomicBool,
        deletes_released: Notify,
    }

    impl CountingStore {
        fn new(prefix: &str) -> Self {
            Self {
                inner: InMemory::new(),
                prefix: prefix.to_string(),
                puts: AtomicUsize::new(0),
                lists: AtomicUsize::new(0),
                heads: AtomicUsize::new(0),
                deletes: AtomicUsize::new(0),
                fail_puts: AtomicBool::new(false),
                fail_deletes: AtomicBool::new(false),
                fail_after: AtomicU64::new(u64::MAX),
                hold_deletes: AtomicBool::new(false),
                deletes_released: Notify::new(),
            }
        }

        fn counts(&self) -> (usize, usize) {
            (
                self.puts.load(Ordering::SeqCst),
                self.lists.load(Ordering::SeqCst),
            )
        }

        fn heads(&self) -> usize {
            self.heads.load(Ordering::SeqCst)
        }

        fn deletes(&self) -> usize {
            self.deletes.load(Ordering::SeqCst)
        }

        fn set_fail_puts(&self, fail: bool) {
            self.fail_puts.store(fail, Ordering::SeqCst);
        }

        fn set_fail_deletes(&self, fail: bool) {
            self.fail_deletes.store(fail, Ordering::SeqCst);
        }

        fn set_fail_after(&self, count: u64) {
            let done = self.deletes.load(Ordering::SeqCst) as u64;
            self.fail_after.store(done + count, Ordering::SeqCst);
        }

        fn hold_deletes(&self) {
            self.hold_deletes.store(true, Ordering::SeqCst);
        }

        fn release_deletes(&self) {
            self.hold_deletes.store(false, Ordering::SeqCst);
            self.deletes_released.notify_waiters();
        }

        fn under(&self, location: &ObjPath) -> bool {
            location.as_ref().starts_with(self.prefix.as_str())
        }
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.prefix)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &ObjPath,
            payload: PutPayload,
            opts: PutOptions,
        ) -> StoreResult<PutResult> {
            if self.under(location) {
                self.puts.fetch_add(1, Ordering::SeqCst);
                if self.fail_puts.load(Ordering::SeqCst) {
                    return Err(cello_ltx::object_store::Error::Generic {
                        store: "CountingStore",
                        source: "injected put failure".into(),
                    });
                }
            }
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjPath,
            opts: PutMultipartOptions,
        ) -> StoreResult<Box<dyn MultipartUpload>> {
            if self.under(location) {
                self.puts.fetch_add(1, Ordering::SeqCst);
                if self.fail_puts.load(Ordering::SeqCst) {
                    return Err(cello_ltx::object_store::Error::Generic {
                        store: "CountingStore",
                        source: "injected put failure".into(),
                    });
                }
            }
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjPath,
            options: GetOptions,
        ) -> StoreResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn head(&self, location: &ObjPath) -> StoreResult<ObjectMeta> {
            if self.under(location) {
                self.heads.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.head(location).await
        }

        async fn delete(&self, location: &ObjPath) -> StoreResult<()> {
            if self.fail_deletes.load(Ordering::SeqCst) {
                return Err(cello_ltx::object_store::Error::Generic {
                    store: "CountingStore",
                    source: "injected delete failure".into(),
                });
            }
            if self.under(location) {
                loop {
                    let released = self.deletes_released.notified();
                    if !self.hold_deletes.load(Ordering::SeqCst) {
                        break;
                    }
                    released.await;
                }
                let call = self.deletes.fetch_add(1, Ordering::SeqCst);
                if (call as u64) >= self.fail_after.load(Ordering::SeqCst) {
                    return Err(cello_ltx::object_store::Error::Generic {
                        store: "CountingStore",
                        source: "injected delete failure".into(),
                    });
                }
            }
            self.inner.delete(location).await
        }

        fn list(&self, prefix: Option<&ObjPath>) -> ListStream {
            if prefix.is_some_and(|p| p.as_ref().starts_with(self.prefix.as_str())) {
                self.lists.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(&self, prefix: Option<&ObjPath>) -> StoreResult<ListResult> {
            if prefix.is_some_and(|p| p.as_ref().starts_with(self.prefix.as_str())) {
                self.lists.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy(&self, from: &ObjPath, to: &ObjPath) -> StoreResult<()> {
            self.inner.copy(from, to).await
        }

        async fn copy_if_not_exists(&self, from: &ObjPath, to: &ObjPath) -> StoreResult<()> {
            self.inner.copy_if_not_exists(from, to).await
        }
    }

    fn test_watch() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn db_path(watch: &Path, cell: &str, epoch: u64) -> PathBuf {
        watch
            .join(cell)
            .join("ltx")
            .join(format!("e{epoch}"))
            .join("db.sqlite")
    }

    /// Commit one row through a second connection, the way the isolate's
    /// own connection writes beside the replicator's. The writer holds no
    /// open transaction past the call, so the managed capture never blocks.
    fn sql_write(db: &Path) {
        let connection = rusqlite::Connection::open(db).unwrap();
        connection.busy_timeout(Duration::from_secs(5)).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS t(x TEXT); INSERT INTO t(x) VALUES('hello');",
            )
            .unwrap();
    }

    fn row_count(db: &Path) -> i64 {
        let connection = rusqlite::Connection::open(db).unwrap();
        connection
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .unwrap()
    }

    /// A durable `sync_wait` returns only after its upload's PUT was issued,
    /// so the count is already settled here.
    fn assert_puts(store: &CountingStore, want: usize) {
        assert_eq!(store.counts().0, want, "the write's L0 upload");
    }

    async fn shutdown(repl: LtxRepl) {
        let owner = repl.take_task_owner();
        drop(repl);
        owner.join().await;
    }

    /// One write, then eviction once the time-based checkpoint is due: the
    /// handoff capture skips that checkpoint, so its sealing write commits
    /// no txid for an L0 PUT of its own, and both visibility LISTs go
    /// because this residency's own PUTs prove the prefix. The eviction
    /// costs exactly the snapshot PUT and still restores.
    #[test]
    fn evict_after_write_costs_one_snapshot_put() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            repl.activate(ActivationOptions {
                cell: "c",
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            })
            .await
            .unwrap();
            sql_write(&db_path(&watch, "c", 1));
            // Land the write's L0 first, so the handoff capture below has
            // nothing of the application's left to upload.
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            assert_puts(&store, 1);
            // Make the time-based checkpoint due, as it is for a cell that idles
            // past the 60 s interval before its eviction: the write above left
            // `synced_since_checkpoint` set.
            {
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                let mut replica = handle.replica.lock().unwrap();
                replica
                    .as_mut()
                    .unwrap()
                    .db_mut()
                    .unwrap()
                    .checkpoint_interval = Duration::from_nanos(1);
            }
            // 0078: pad the journal past HANDOFF_CHAIN_MAX_OBJECTS so this
            // fresh chain keeps its snapshot; the padded keys name txids past
            // it, so the trim filter drops them and the assertions below are
            // unchanged.
            {
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                for _ in 0..cello_logic::handoff::HANDOFF_CHAIN_MAX_OBJECTS {
                    note_epoch_upload(&handle, 0, 2, 2);
                }
            }
            let (puts_before, lists_before) = store.counts();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::Snapshot);
            let (puts, lists) = store.counts();
            assert_eq!(
                (puts - puts_before, lists - lists_before),
                (1, 0),
                "eviction must cost exactly the snapshot PUT"
            );
            // The trimmed eviction still restores: the next epoch reads the row back.
            repl.activate(ActivationOptions {
                cell: "c",
                epoch: 2,
                fresh: false,
                took_over: false,
                resume_local: false,
                prior: None,
            })
            .await
            .unwrap();
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 1);
            shutdown(repl).await;
        });
    }

    /// A failed snapshot keeps the L0 fallback, but the fallback proves the
    /// epoch through the journaled L0 with one HEAD instead of a visibility
    /// LIST: the sync upload journaled the exact key the guard probes.
    #[test]
    fn evict_snapshot_fallback_proves_through_the_journal() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            repl.activate(ActivationOptions {
                cell: "c",
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            })
            .await
            .unwrap();
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            assert_puts(&store, 1);
            repl.set_handoff_snapshot_budget_for_world(1);
            let (puts_before, lists_before) = store.counts();
            let heads_before = store.heads();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::L0Chain);
            let (puts, lists) = store.counts();
            assert_eq!(
                puts - puts_before,
                0,
                "no snapshot PUT on the fallback path"
            );
            assert_eq!(
                lists - lists_before,
                0,
                "the journaled fallback proves the epoch without a LIST"
            );
            assert_eq!(
                store.heads() - heads_before,
                1,
                "the fallback HEADs the journaled L0 key once"
            );
            repl.activate(ActivationOptions {
                cell: "c",
                epoch: 2,
                fresh: false,
                took_over: false,
                resume_local: false,
                prior: None,
            })
            .await
            .unwrap();
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 1);
            shutdown(repl).await;
        });
    }

    /// The handoff gate proves a journaled epoch with one HEAD and no LIST.
    /// The sync upload journaled its key, and the test clears the
    /// self-uploaded mark to reach the guard the way a residency the fast
    /// path cannot skip does (recovered tails, compaction output): without
    /// the journaled probe this gate costs a LIST.
    #[test]
    fn handoff_gate_proves_journaled_epoch_with_one_head() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            let handle = repl
                .cells
                .lock()
                .unwrap()
                .get(&("c".to_string(), 1))
                .cloned()
                .unwrap();
            assert_eq!(
                handle.epoch_uploads.lock().unwrap().len(),
                1,
                "the sync upload journaled its key"
            );
            handle.uploaded_epoch_objects.store(false, Ordering::SeqCst);
            let (_, lists_before) = store.counts();
            let heads_before = store.heads();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let (_, lists) = store.counts();
            assert_eq!(
                lists - lists_before,
                0,
                "a journaled guard proves the epoch without a LIST"
            );
            assert_eq!(
                store.heads() - heads_before,
                1,
                "a journaled guard HEADs the newest journaled key once"
            );
            shutdown(repl).await;
        });
    }

    /// Without a journal the guard keeps the LIST: `epoch_replicated` on a
    /// write-free residency lists once and answers false, then answers true
    /// with one HEAD and no further LIST once the first upload journals a key.
    #[test]
    fn epoch_replicated_lists_only_without_a_journal() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            let (_, lists_before) = store.counts();
            assert!(
                !repl.epoch_replicated("c", 1).await,
                "an empty prefix is not replicated"
            );
            let (_, lists) = store.counts();
            assert_eq!(
                lists - lists_before,
                1,
                "an upload-free residency keeps the LIST"
            );
            assert_eq!(store.heads(), 0, "no journal means no HEAD");
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            let (_, lists_before) = store.counts();
            let heads_before = store.heads();
            assert!(
                repl.epoch_replicated("c", 1).await,
                "the journaled upload is replicated"
            );
            let (_, lists) = store.counts();
            assert_eq!(
                lists - lists_before,
                0,
                "a journaled residency skips the LIST"
            );
            assert_eq!(
                store.heads() - heads_before,
                1,
                "a journaled residency HEADs the journaled key once"
            );
            shutdown(repl).await;
        });
    }

    /// A journaled key the bucket lost falls back to the LIST, and the gate
    /// still refuses on an empty prefix: the test deletes the journaled L0
    /// through the trim's own single-key path, so the HEAD misses, the LIST
    /// finds nothing, and the gate fails closed.
    #[test]
    fn handoff_gate_falls_back_to_list_on_journal_miss_and_refuses() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            let handle = repl
                .cells
                .lock()
                .unwrap()
                .get(&("c".to_string(), 1))
                .cloned()
                .unwrap();
            let (level, min_txid, max_txid) = handle
                .epoch_uploads
                .lock()
                .unwrap()
                .last()
                .copied()
                .expect("the sync upload journaled its key");
            let gone = cello_ltx::FileInfo {
                level,
                min_txid: TXID(min_txid),
                max_txid: TXID(max_txid),
                ..Default::default()
            };
            assert_eq!(
                repl.client_for("c", 1).delete_ltx_keys(&[gone], 1).await,
                0,
                "the trim's single-key path deletes the journaled L0"
            );
            handle.uploaded_epoch_objects.store(false, Ordering::SeqCst);
            let (_, lists_before) = store.counts();
            let heads_before = store.heads();
            assert!(
                repl.handoff_gate("c", 1, true).await.is_err(),
                "a missed HEAD with an empty prefix refuses the eviction"
            );
            let (_, lists) = store.counts();
            assert_eq!(
                lists - lists_before,
                1,
                "a missed HEAD falls back to the LIST"
            );
            assert_eq!(
                store.heads() - heads_before,
                1,
                "the guard HEADs the journaled key before the LIST"
            );
            shutdown(repl).await;
        });
    }

    /// The probe, the uploader, and the trim share one key constructor: the
    /// journaled L0 the sync wrote probes true, and after the snapshot's
    /// trim it probes false while the snapshot itself probes true.
    #[test]
    fn journal_probe_round_trips_uploader_and_trim_keys() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            let handle = repl
                .cells
                .lock()
                .unwrap()
                .get(&("c".to_string(), 1))
                .cloned()
                .unwrap();
            let (level, min_txid, max_txid) = handle
                .epoch_uploads
                .lock()
                .unwrap()
                .last()
                .copied()
                .expect("the sync upload journaled its key");
            let client = repl.client_for("c", 1);
            assert!(
                client
                    .ltx_file_exists(level, TXID(min_txid), TXID(max_txid))
                    .await
                    .unwrap(),
                "the probe names the key the uploader wrote"
            );
            // 0078: pad the journal past HANDOFF_CHAIN_MAX_OBJECTS so this
            // fresh chain keeps its snapshot; the padded keys name txids past
            // it, so the trim filter drops them and the assertions below are
            // unchanged. After the probe above, so `.last()` still names the
            // real upload.
            for _ in 0..cello_logic::handoff::HANDOFF_CHAIN_MAX_OBJECTS {
                note_epoch_upload(&handle, 0, 2, 2);
            }
            repl.handoff_gate("c", 1, true).await.unwrap();
            assert_eq!(
                repl.evict("c", 1, false, None).await.unwrap(),
                EvictionRestoreArtifact::Snapshot
            );
            trim_idle(&repl).await;
            assert!(
                !client
                    .ltx_file_exists(level, TXID(min_txid), TXID(max_txid))
                    .await
                    .unwrap(),
                "the trim deletes the key the probe named"
            );
            let snapshots = ltx_level_files(&repl, "c", 1, 9).await;
            assert_eq!(snapshots.len(), 1, "the handoff snapshot remains");
            assert!(
                client
                    .ltx_file_exists(
                        snapshots[0].level,
                        snapshots[0].min_txid,
                        snapshots[0].max_txid
                    )
                    .await
                    .unwrap(),
                "the probe names the snapshot key too"
            );
            shutdown(repl).await;
        });
    }

    /// A bundle-only epoch evicts through one snapshot PUT with no LIST: the
    /// rows were acked by bundle proofs, so no per-cell objects exist for the
    /// revocable gate to find — and the gate must not demand them. The staged
    /// credit mirrors `bundle_loop` after a credited `put_bundle` (durable
    /// and tickets advance, per-cell coverage stays zero). Fail-before:
    /// without the `has_bundle_only_rows` skip the gate LISTs and refuses
    /// with "no replica objects".
    #[test]
    fn bundle_only_eviction_costs_one_snapshot_put_no_list() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            repl.set_bucket_bundles_env_override_for_test(Some(true));
            repl.set_bucket_bundles_fleet(true);
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            {
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                handle.durable_txid.store(1, Ordering::SeqCst);
                handle.synced_seq.store(1, Ordering::SeqCst);
            }
            let (puts_before, lists_before) = store.counts();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let (puts_gated, lists_gated) = store.counts();
            assert_eq!(
                (puts_gated - puts_before, lists_gated - lists_before),
                (0, 0),
                "the revocable gate proves a bundled cell with no LIST"
            );
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::Snapshot);
            let (puts, lists) = store.counts();
            assert_eq!(
                (puts - puts_before, lists - lists_before),
                (1, 0),
                "eviction must cost exactly the snapshot PUT"
            );
            repl.activate(ActivationOptions {
                cell: "c",
                epoch: 2,
                fresh: false,
                took_over: false,
                resume_local: false,
                prior: None,
            })
            .await
            .unwrap();
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 1);
            shutdown(repl).await;
        });
    }

    /// A bundle-only eviction whose snapshot PUT cannot land refuses instead
    /// of giving up the last copy: with every PUT failing the L0 fallback
    /// cannot materialize its chain either, so the barrier fails closed at
    /// the final durability pass. Fail-before: without the
    /// `has_bundle_only_rows` skip the gate refuses first with "no replica
    /// objects" instead of this barrier error.
    #[test]
    fn bundle_only_eviction_refuses_when_no_artifact_lands() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            repl.set_bucket_bundles_env_override_for_test(Some(true));
            repl.set_bucket_bundles_fleet(true);
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            {
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                handle.durable_txid.store(1, Ordering::SeqCst);
                handle.synced_seq.store(1, Ordering::SeqCst);
            }
            store.set_fail_puts(true);
            repl.handoff_gate("c", 1, true).await.unwrap();
            let error = repl
                .evict("c", 1, false, None)
                .await
                .expect_err("no snapshot and no L0 chain must refuse");
            assert!(
                error.to_string().contains("final durability failed"),
                "the evict-time barrier fails closed, not the gate: {error:#}"
            );
            shutdown(repl).await;
        });
    }

    /// A bundled cell the snapshot budget cannot carry still drains per-cell
    /// through the L0 fallback, which proves its upload with the journaled-key
    /// HEAD (`0061`; LIST only without a journal). Pins the
    /// fallback for bundle-credited rows: the snapshot is the cheap path,
    /// not the only one. Fail-before: without the `has_bundle_only_rows`
    /// skip the gate refuses before the fallback runs.
    #[test]
    fn bundle_only_eviction_falls_back_to_l0_chain() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            repl.set_bucket_bundles_env_override_for_test(Some(true));
            repl.set_bucket_bundles_fleet(true);
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            {
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                handle.durable_txid.store(1, Ordering::SeqCst);
                handle.synced_seq.store(1, Ordering::SeqCst);
            }
            repl.set_handoff_snapshot_budget_for_world(1);
            let (puts_before, lists_before) = store.counts();
            let heads_before = store.heads();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::L0Chain);
            let (puts, lists) = store.counts();
            assert!(
                puts - puts_before > 0,
                "the L0 fallback drains the rows per-cell"
            );
            // `0061`: the fallback drain journaled its upload, so the barrier
            // proves visibility with one HEAD on that key instead of a LIST.
            assert_eq!(
                (lists - lists_before, store.heads() - heads_before),
                (0, 1),
                "the L0 fallback proves its upload with one HEAD and no LIST"
            );
            repl.activate(ActivationOptions {
                cell: "c",
                epoch: 2,
                fresh: false,
                took_over: false,
                resume_local: false,
                prior: None,
            })
            .await
            .unwrap();
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 1);
            shutdown(repl).await;
        });
    }

    /// A snapshot that stops short of a credited row is not authoritative: a
    /// row credited past the image routes through the L0 fallback and its
    /// visibility guard instead of releasing the snapshot. Fail-before: without the
    /// post-publish re-check the eviction certifies the stale snapshot.
    #[test]
    fn bundle_only_eviction_refuses_a_stale_snapshot() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            repl.set_bucket_bundles_env_override_for_test(Some(true));
            repl.set_bucket_bundles_fleet(true);
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            {
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                // A credit that landed past the image the handoff will
                // capture: the local database holds txid 1, the ack covers
                // through 5.
                handle.durable_txid.store(5, Ordering::SeqCst);
                handle.synced_seq.store(1, Ordering::SeqCst);
            }
            let (puts_before, lists_before) = store.counts();
            let heads_before = store.heads();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(
                artifact,
                EvictionRestoreArtifact::L0Chain,
                "a stale snapshot must not be the handoff artifact"
            );
            let (puts, lists) = store.counts();
            assert_eq!(
                (lists - lists_before, store.heads() - heads_before),
                (0, 1),
                "the L0 fallback proves its upload with one HEAD and no LIST"
            );
            assert!(
                puts - puts_before > 1,
                "the snapshot PUT plus the L0 fallback drain both land"
            );
            shutdown(repl).await;
        });
    }

    /// A mixed history evicts through one snapshot: row 1 is per-cell-covered
    /// (compaction-style credit, no residency upload flag) and row 2 is
    /// bundle-credited only, and the image from the complete local database
    /// covers both — so the guard needs no LIST and the next epoch restores
    /// every row. Fail-before: without the bundle-only skip the gate refuses,
    /// because this residency never PUT a per-cell object for either row.
    #[test]
    fn bundle_mixed_history_evicts_through_one_snapshot() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            repl.set_bucket_bundles_env_override_for_test(Some(true));
            repl.set_bucket_bundles_fleet(true);
            activate_epoch(&repl, "c", 1, true).await;
            {
                let sync_position = || {
                    let handle = repl
                        .cells
                        .lock()
                        .unwrap()
                        .get(&("c".to_string(), 1))
                        .cloned()
                        .unwrap();
                    let mut replica = handle.replica.lock().unwrap();
                    let db = managed_db_mut(&mut replica).expect("managed db");
                    db.sync().expect("sync advances the position");
                };
                // One sync per write: each capture mints its own txid, so the
                // history below is txid 1 (per-cell-covered) then txid 2
                // (bundle-credited), with nothing uploaded by this residency.
                sql_write(&db_path(&watch, "c", 1));
                sync_position();
                sql_write(&db_path(&watch, "c", 1));
                sync_position();
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                // Row 1 covered the way a compaction L1 covers rows: watermark
                // without a residency upload. Row 2 is bundle-credited only.
                credit_percell_upload(&handle, 1);
                handle.durable_txid.store(2, Ordering::SeqCst);
                handle.synced_seq.store(3, Ordering::SeqCst);
            }

            let (puts_before, lists_before) = store.counts();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let (puts_gated, lists_gated) = store.counts();
            assert_eq!(
                (puts_gated - puts_before, lists_gated - lists_before),
                (0, 0),
                "a mixed history proves through the coming snapshot, with no LIST"
            );
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::Snapshot);
            let (puts, lists) = store.counts();
            assert_eq!(
                (puts - puts_before, lists - lists_before),
                (1, 0),
                "eviction must cost exactly the snapshot PUT"
            );
            repl.activate(ActivationOptions {
                cell: "c",
                epoch: 2,
                fresh: false,
                took_over: false,
                resume_local: false,
                prior: None,
            })
            .await
            .unwrap();
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 2);
            shutdown(repl).await;
        });
    }

    async fn ltx_level_files(
        repl: &LtxRepl,
        cell: &str,
        epoch: u64,
        level: i32,
    ) -> Vec<cello_ltx::FileInfo> {
        repl.client_for(cell, epoch)
            .ltx_files(level, TXID(0))
            .await
            .unwrap()
    }

    async fn activate_epoch(repl: &LtxRepl, cell: &str, epoch: u64, fresh: bool) {
        repl.activate(ActivationOptions {
            cell,
            epoch,
            fresh,
            took_over: false,
            resume_local: false,
            prior: None,
        })
        .await
        .unwrap();
    }

    /// Await every background trim handed off so far. The spawn increments
    /// the counter before `evict` returns, and each task decrements it when
    /// done and wakes waiters only on the last one, so this is
    /// deterministic: no sleep, no polling loop.
    async fn trim_idle(repl: &LtxRepl) {
        loop {
            let notified = repl.trim_changed.notified();
            if repl.trim_inflight.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Three write+sync rounds, then eviction: the trim deletes exactly the
    /// three covered L0 objects from the journal alone (no LIST), keeps the
    /// snapshot, and the next epoch restores every row from the snapshot.
    #[test]
    fn evict_trims_covered_l0_and_restores() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            for _ in 0..3 {
                sql_write(&db_path(&watch, "c", 1));
                assert!(matches!(
                    repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                    SyncWait::Durable
                ));
            }
            assert_puts(&store, 3);
            // 0078: pad the journal past HANDOFF_CHAIN_MAX_OBJECTS so this
            // fresh chain keeps its snapshot; the padded keys name txids past
            // it, so the trim filter drops them and the assertions below are
            // unchanged.
            {
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                for _ in 0..14 {
                    note_epoch_upload(&handle, 0, 9, 9);
                }
            }
            let deletes_before = store.deletes();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::Snapshot);
            assert_eq!(
                store.deletes() - deletes_before,
                3,
                "the trim deletes exactly the three journaled L0 objects"
            );
            assert!(
                ltx_level_files(&repl, "c", 1, 0).await.is_empty(),
                "the covered L0 objects are gone"
            );
            let snapshots = ltx_level_files(&repl, "c", 1, 9).await;
            assert_eq!(snapshots.len(), 1, "the handoff snapshot remains");
            assert_eq!(
                (snapshots[0].min_txid, snapshots[0].max_txid),
                (TXID(1), TXID(3)),
                "the snapshot covers the whole closed database"
            );
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 3);
            shutdown(repl).await;
        });
    }

    /// The trim runs after the eviction returns: with every DELETE parked,
    /// `evict` still returns its snapshot artifact, the next epoch restores
    /// from the snapshot while the superseded L0 objects still exist, and
    /// the deletes land once released.
    #[test]
    fn evict_returns_before_the_trim_deletes_land() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            for _ in 0..3 {
                sql_write(&db_path(&watch, "c", 1));
                assert!(matches!(
                    repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                    SyncWait::Durable
                ));
            }
            // 0078: pad the journal past HANDOFF_CHAIN_MAX_OBJECTS so this
            // fresh chain keeps its snapshot; the padded keys name txids past
            // it, so the trim filter drops them and the assertions below are
            // unchanged.
            {
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                for _ in 0..14 {
                    note_epoch_upload(&handle, 0, 9, 9);
                }
            }
            store.hold_deletes();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            assert_eq!(artifact, EvictionRestoreArtifact::Snapshot);
            assert_eq!(
                ltx_level_files(&repl, "c", 1, 0).await.len(),
                3,
                "the parked deletes have not landed when evict returns"
            );
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 3);
            store.release_deletes();
            trim_idle(&repl).await;
            assert!(
                ltx_level_files(&repl, "c", 1, 0).await.is_empty(),
                "the deletes land after release"
            );
            shutdown(repl).await;
        });
    }

    /// The L0-chain fallback deletes nothing: without a snapshot no object
    /// is provably superseded, so the L0 objects stay for the restore.
    #[test]
    fn evict_l0_fallback_deletes_nothing() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            repl.set_handoff_snapshot_budget_for_world(1);
            let deletes_before = store.deletes();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::L0Chain);
            assert_eq!(
                store.deletes() - deletes_before,
                0,
                "the fallback path deletes nothing"
            );
            assert_eq!(
                ltx_level_files(&repl, "c", 1, 0).await.len(),
                1,
                "the fallback restore keeps its L0 chain"
            );
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 1);
            shutdown(repl).await;
        });
    }

    /// 0078: a fresh cell with a short journaled chain skips the handoff
    /// snapshot by policy (no budget forcing): the eviction PUTs no L9,
    /// returns the L0-chain artifact, deletes nothing, and the next epoch
    /// restores the rows. Fail-before: forcing the old behaviour (always
    /// snapshot) publishes one snapshot PUT and returns `Snapshot`.
    #[test]
    fn lazy_handoff_skips_snapshot_for_fresh_short_chain() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            for _ in 0..2 {
                sql_write(&db_path(&watch, "c", 1));
                assert!(matches!(
                    repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                    SyncWait::Durable
                ));
            }
            assert_puts(&store, 2);
            let (puts_before, lists_before) = store.counts();
            let heads_before = store.heads();
            let deletes_before = store.deletes();
            repl.handoff_gate("c", 1, true).await.unwrap();
            // Mirror a production-old cell: make the time-based checkpoint due,
            // as it is for a cell that lives past the 60 s interval before its
            // eviction. Without the pos-check below, the fallback's ordinary
            // sync would fire that checkpoint and commit a content-free sealing
            // txid, uploading it as its own L0 (fail-before: forcing the sync
            // path adds one PUT here). The gate's capture is exempt and the
            // earlier syncs ran at the default interval, so only the fallback
            // can mint it.
            {
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                let mut replica = handle.replica.lock().unwrap();
                replica
                    .as_mut()
                    .unwrap()
                    .db_mut()
                    .unwrap()
                    .checkpoint_interval = Duration::from_nanos(1);
            }
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::L0Chain);
            let (puts, lists) = store.counts();
            assert_eq!(
                puts - puts_before,
                0,
                "the policy skip PUTs no L9 snapshot"
            );
            assert_eq!(
                (lists - lists_before, store.heads() - heads_before),
                (0, 1),
                "the journaled fallback proves the epoch with one HEAD and no LIST"
            );
            assert_eq!(
                store.deletes() - deletes_before,
                0,
                "no snapshot means no trim"
            );
            assert!(
                ltx_level_files(&repl, "c", 1, 9).await.is_empty(),
                "no snapshot object is published"
            );
            assert_eq!(
                ltx_level_files(&repl, "c", 1, 0).await.len(),
                2,
                "the L0 chain stays for the restore"
            );
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 2);
            shutdown(repl).await;
        });
    }

    /// 0078: a WAL-only tail — a write after the last sync, with no waiter,
    /// as background or waitUntil work after the handoff gate lands — is
    /// invisible to `Db::pos` but not to the handoff. The skip path must sync
    /// it before handing off, or the successor restores without the row.
    /// Fail-before: with the blind pos-check the eviction skips the sync and
    /// the next epoch is missing the row.
    #[test]
    fn lazy_handoff_syncs_wal_only_tail_before_handoff() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            assert_puts(&store, 1);
            repl.handoff_gate("c", 1, true).await.unwrap();
            // A write with no waiter after the gate: nothing re-syncs it
            // before the eviction, so it sits in the WAL past the capture.
            sql_write(&db_path(&watch, "c", 1));
            let (puts_before, lists_before) = store.counts();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::L0Chain);
            let (puts, lists) = store.counts();
            assert_eq!(
                (puts - puts_before, lists - lists_before),
                (1, 0),
                "the skip path uploads the WAL-only tail as one L0"
            );
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(
                row_count(&db_path(&watch, "c", 2)),
                2,
                "the successor restores the WAL-only row"
            );
            shutdown(repl).await;
        });
    }

    /// 0078: a `.evicted` reuse starts a new writer generation at TXID 1
    /// whose first L0 carries the full image, so the chain is still born in
    /// this epoch and journaled here: the second eviction skips the snapshot
    /// too, and the third epoch restores every row. Fail-before for the
    /// steering question: treating reuse as older history would publish a
    /// snapshot at the second eviction.
    #[test]
    fn lazy_handoff_evict_reuse_keeps_short_chain() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            repl.handoff_gate("c", 1, true).await.unwrap();
            assert_eq!(
                repl.evict("c", 1, true, None).await.unwrap(),
                EvictionRestoreArtifact::L0Chain,
                "the fresh first epoch skips by policy"
            );
            trim_idle(&repl).await;
            assert!(
                repl.ltx_host
                    .metadata(&db_path(&watch, "c", 1).with_extension("evicted"))
                    .is_ok_and(|metadata| metadata.is_file),
                "the preserved image is the next epoch's reuse base"
            );
            repl.activate(ActivationOptions {
                cell: "c",
                epoch: 2,
                fresh: false,
                took_over: false,
                resume_local: false,
                prior: None,
            })
            .await
            .unwrap();
            assert_eq!(
                row_count(&db_path(&watch, "c", 2)),
                1,
                "the reused image carries the first epoch's row"
            );
            sql_write(&db_path(&watch, "c", 2));
            assert!(matches!(
                repl.sync_wait("c", 2, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            let (puts_before, _) = store.counts();
            repl.handoff_gate("c", 2, true).await.unwrap();
            assert_eq!(
                repl.evict("c", 2, false, None).await.unwrap(),
                EvictionRestoreArtifact::L0Chain,
                "the reused epoch's chain is still born here"
            );
            trim_idle(&repl).await;
            assert_eq!(
                store.counts().0 - puts_before,
                0,
                "the reuse skip PUTs no L9 snapshot either"
            );
            activate_epoch(&repl, "c", 3, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 3)), 2);
            shutdown(repl).await;
        });
    }

    /// 0078: a cell activated by restoring from an earlier epoch carries
    /// older history, so its eviction still publishes the snapshot: exactly
    /// one PUT past the gate, the `Snapshot` artifact, and the next epoch
    /// restores every row.
    #[test]
    fn lazy_handoff_restored_cell_still_snapshots() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            repl.handoff_gate("c", 1, true).await.unwrap();
            assert_eq!(
                repl.evict("c", 1, false, None).await.unwrap(),
                EvictionRestoreArtifact::L0Chain,
                "the fresh first epoch skips by policy"
            );
            trim_idle(&repl).await;
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(
                row_count(&db_path(&watch, "c", 2)),
                1,
                "the second epoch clones the first epoch's L0 chain"
            );
            sql_write(&db_path(&watch, "c", 2));
            assert!(matches!(
                repl.sync_wait("c", 2, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            let (puts_before, lists_before) = store.counts();
            repl.handoff_gate("c", 2, true).await.unwrap();
            let artifact = repl.evict("c", 2, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::Snapshot);
            let (puts, lists) = store.counts();
            assert_eq!(
                (puts - puts_before, lists - lists_before),
                (1, 0),
                "the restored epoch still costs exactly the snapshot PUT"
            );
            assert_eq!(
                ltx_level_files(&repl, "c", 2, 9).await.len(),
                1,
                "the handoff snapshot remains"
            );
            activate_epoch(&repl, "c", 3, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 3)), 2);
            shutdown(repl).await;
        });
    }

    /// 0078: a journal over 16 entries keeps the snapshot even for a chain
    /// born in this epoch with full per-cell coverage.
    #[test]
    fn lazy_handoff_long_journal_still_snapshots() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            let handle = repl
                .cells
                .lock()
                .unwrap()
                .get(&("c".to_string(), 1))
                .cloned()
                .unwrap();
            for _ in 0..cello_logic::handoff::HANDOFF_CHAIN_MAX_OBJECTS {
                note_epoch_upload(&handle, 0, 1, 1);
            }
            assert_eq!(
                handle.epoch_uploads.lock().unwrap().len(),
                cello_logic::handoff::HANDOFF_CHAIN_MAX_OBJECTS + 1,
                "one real upload plus sixteen padded entries"
            );
            let (puts_before, lists_before) = store.counts();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::Snapshot);
            let (puts, lists) = store.counts();
            assert_eq!(
                (puts - puts_before, lists - lists_before),
                (1, 0),
                "the long journal still costs exactly the snapshot PUT"
            );
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 1);
            shutdown(repl).await;
        });
    }

    /// 0078: a cell whose durable watermark runs ahead of its per-cell
    /// coverage (bundle-credited rows) still attempts the snapshot: the L9
    /// object lands even though the stale-image guard then falls back to the
    /// L0 chain, which proves the epoch. Fail-before for the coverage
    /// condition: a covered cell would PUT nothing.
    #[test]
    fn lazy_handoff_bundle_gap_still_snapshots() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            repl.stage_bundle_credit_for_test("c", 1, 2);
            let (puts_before, lists_before) = store.counts();
            let heads_before = store.heads();
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(
                artifact,
                EvictionRestoreArtifact::L0Chain,
                "the snapshot cannot cover the credited row, so the fallback proves the epoch"
            );
            let (puts, lists) = store.counts();
            assert_eq!(
                (puts - puts_before, lists - lists_before),
                (1, 0),
                "the gap still attempts the snapshot PUT"
            );
            assert_eq!(
                store.heads() - heads_before,
                1,
                "the fallback proves the epoch with one HEAD"
            );
            assert_eq!(
                ltx_level_files(&repl, "c", 1, 9).await.len(),
                1,
                "the attempted snapshot object remains"
            );
            // The staged credit is synthetic: no bundle segments exist to fold,
            // so drop the tail the eviction retained for them. A production
            // bundle credit always names real segments the next activation
            // folds through its sink.
            repl.dirty_tails.lock().unwrap().remove("c");
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 1);
            shutdown(repl).await;
        });
    }

    /// A failed DELETE leaves garbage but never fails the eviction: the
    /// snapshot already proved durability, and the next epoch restores.
    #[test]
    fn evict_trim_failure_keeps_eviction() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            // 0078: pad the journal past HANDOFF_CHAIN_MAX_OBJECTS so this
            // fresh chain keeps its snapshot; the padded keys name txids past
            // it, so the trim filter drops them and the assertions below are
            // unchanged.
            {
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                for _ in 0..cello_logic::handoff::HANDOFF_CHAIN_MAX_OBJECTS {
                    note_epoch_upload(&handle, 0, 2, 2);
                }
            }
            store.set_fail_deletes(true);
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::Snapshot);
            assert_eq!(
                ltx_level_files(&repl, "c", 1, 0).await.len(),
                1,
                "a failed DELETE leaves the L0 object as garbage"
            );
            store.set_fail_deletes(false);
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 1);
            shutdown(repl).await;
        });
    }

    /// The journal is bounded: overfilling it keeps the first 8192 entries,
    /// and the eviction still trims what was journaled.
    #[test]
    fn epoch_upload_journal_is_bounded() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            let handle = repl
                .cells
                .lock()
                .unwrap()
                .get(&("c".to_string(), 1))
                .cloned()
                .unwrap();
            for _ in 0..EPOCH_UPLOAD_JOURNAL_MAX + 100 {
                note_epoch_upload(&handle, 0, 1, 1);
            }
            assert_eq!(
                handle.epoch_uploads.lock().unwrap().len(),
                EPOCH_UPLOAD_JOURNAL_MAX,
                "the journal stops growing at the cap"
            );
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::Snapshot);
            assert!(
                ltx_level_files(&repl, "c", 1, 0).await.is_empty(),
                "the trim still deletes what was journaled"
            );
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 1);
            shutdown(repl).await;
        });
    }

    /// Partial failure: the first delete succeeds, the rest fail, and the
    /// epoch still restores fully; the leftovers stay as garbage.
    #[test]
    fn evict_trim_partial_failure_still_restores() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            for _ in 0..3 {
                sql_write(&db_path(&watch, "c", 1));
                assert!(matches!(
                    repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                    SyncWait::Durable
                ));
            }
            // 0078: pad the journal past HANDOFF_CHAIN_MAX_OBJECTS so this
            // fresh chain keeps its snapshot; the padded keys name txids past
            // it, so the trim filter drops them and the assertions below are
            // unchanged.
            {
                let handle = repl
                    .cells
                    .lock()
                    .unwrap()
                    .get(&("c".to_string(), 1))
                    .cloned()
                    .unwrap();
                for _ in 0..14 {
                    note_epoch_upload(&handle, 0, 9, 9);
                }
            }
            store.set_fail_after(1);
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::Snapshot);
            assert_eq!(
                ltx_level_files(&repl, "c", 1, 0).await.len(),
                2,
                "two failed deletes stay as garbage"
            );
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 3);
            shutdown(repl).await;
        });
    }

    /// An L1-shaped journal entry flows through the filter and the worker
    /// like an L0 one. True compaction output cannot be forced
    /// deterministically in CI (`start_with_compaction_for_test` is
    /// `cello_internal_tests`-gated, and threshold timing is not
    /// deterministic), so the test journals one L1 key directly; its
    /// delete is attempted and its absence ignored.
    #[test]
    fn evict_trims_journaled_l1_keys() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(CountingStore::new("cells/c/ltx/"));
            let repl = LtxRepl::start_with_store_for_test(&watch, store.clone());
            activate_epoch(&repl, "c", 1, true).await;
            sql_write(&db_path(&watch, "c", 1));
            assert!(matches!(
                repl.sync_wait("c", 1, Duration::from_secs(10)).await,
                SyncWait::Durable
            ));
            let handle = repl
                .cells
                .lock()
                .unwrap()
                .get(&("c".to_string(), 1))
                .cloned()
                .unwrap();
            note_epoch_upload(&handle, 1, 1, 1);
            // 0078: pad the journal past HANDOFF_CHAIN_MAX_OBJECTS so this
            // fresh chain keeps its snapshot; the padded keys name txids past
            // it, so the trim filter drops them and the assertions below are
            // unchanged.
            for _ in 0..15 {
                note_epoch_upload(&handle, 0, 2, 2);
            }
            repl.handoff_gate("c", 1, true).await.unwrap();
            let artifact = repl.evict("c", 1, false, None).await.unwrap();
            trim_idle(&repl).await;
            assert_eq!(artifact, EvictionRestoreArtifact::Snapshot);
            assert_eq!(
                store.deletes(),
                2,
                "both the L0 and the L1 journal keys are deleted"
            );
            assert!(
                ltx_level_files(&repl, "c", 1, 0).await.is_empty(),
                "the covered L0 object is gone"
            );
            activate_epoch(&repl, "c", 2, false).await;
            assert_eq!(row_count(&db_path(&watch, "c", 2)), 1);
            shutdown(repl).await;
        });
    }

    /// A stale live-named dir planted by hand: the retaining dir and the
    /// database file that make `local_cells` report it live.
    fn plant_live_epoch(watch: &Path, cell: &str, epoch: u64) {
        let db = db_path(watch, cell, epoch);
        std::fs::create_dir_all(db.parent().unwrap()).unwrap();
        std::fs::write(&db, b"stale").unwrap();
    }

    /// Upstream #311: live-named epoch dirs a newer local epoch supersedes
    /// are removed by the startup sweep and by each activation; resident
    /// epochs are never touched.
    #[test]
    fn superseded_live_epochs_pruned_at_startup_and_activation() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(InMemory::new());
            let repl = LtxRepl::start_with_store_for_test(&watch, store);
            // Two live dirs for c (e2 supersedes e1), one for d.
            plant_live_epoch(&watch, "c", 1);
            plant_live_epoch(&watch, "c", 2);
            plant_live_epoch(&watch, "d", 1);
            assert_eq!(repl.prune_superseded_epochs_all(), 1);
            assert!(
                !db_path(&watch, "c", 1).exists(),
                "the superseded e1 dir is gone"
            );
            assert!(db_path(&watch, "c", 2).exists(), "the newest e2 stays");
            assert!(db_path(&watch, "d", 1).exists(), "a sole epoch stays");
            assert_eq!(
                repl.prune_superseded_epochs_all(),
                0,
                "the second sweep is a no-op"
            );
            // Activation prunes: a fresh e3 supersedes the planted e2 stub.
            activate_epoch(&repl, "c", 3, true).await;
            assert!(
                !db_path(&watch, "c", 2).exists(),
                "activation prunes the older live dir"
            );
            // Resident epochs are never touched: f/1 stays active while a
            // prune for the newer f/2 names it.
            activate_epoch(&repl, "f", 1, true).await;
            activate_epoch(&repl, "f", 2, true).await;
            assert_eq!(repl.prune_superseded_epochs("f", 2), 0);
            assert!(
                db_path(&watch, "f", 1).exists(),
                "the resident older epoch stays"
            );
            shutdown(repl).await;
        });
    }

    /// Upstream #290: an unclean restart deletes every live-named epoch its
    /// predecessor left — no activation reopens them — while preserved
    /// snapshots stay cached. The deleted cells still read back once, so
    /// the ownership confirmation after the first node lease finds the
    /// cells that were resident when the predecessor died.
    #[test]
    fn unclean_restart_prunes_live_keeps_snapshots() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = test_watch();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(InMemory::new());
            let repl = LtxRepl::start_with_store_for_test(&watch, store);
            // Two live epochs of a plus a preserved snapshot of b.
            plant_live_epoch(&watch, "a", 1);
            plant_live_epoch(&watch, "a", 2);
            let snapshot = watch.join("b").join("ltx").join("e5");
            std::fs::create_dir_all(&snapshot).unwrap();
            std::fs::write(snapshot.join("db.evicted"), b"stale").unwrap();
            assert_eq!(repl.prune_unclean_restart().unwrap(), 2);
            assert!(!db_path(&watch, "a", 1).exists(), "the dead e1 dir is gone");
            assert!(!db_path(&watch, "a", 2).exists(), "the dead e2 dir is gone");
            assert!(
                snapshot.join("db.evicted").exists(),
                "the preserved snapshot stays"
            );
            assert_eq!(
                repl.local_cells(),
                vec![cello_logic::LocalCell {
                    id: "b".to_string(),
                    epoch: 5,
                    live: false,
                }],
                "the inventory lists only the snapshot"
            );
            let mut listed = repl.take_pruned_at_boot();
            listed.sort();
            assert_eq!(
                listed,
                vec![
                    cello_logic::LocalCell {
                        id: "a".to_string(),
                        epoch: 1,
                        live: true,
                    },
                    cello_logic::LocalCell {
                        id: "a".to_string(),
                        epoch: 2,
                        live: true,
                    },
                ],
                "confirmation still finds the resident-at-death cells"
            );
            assert!(
                repl.take_pruned_at_boot().is_empty(),
                "the boot list reads back at most once"
            );
            shutdown(repl).await;
        });
    }
}
/// Bucket-posture group-commit unit tests (layer 2, executor policy). Plain
/// `#[cfg(test)]`: CI runs without `cello_internal_tests`. Deterministic, no
/// sleeps or polling; each behaviour has a fail-before shown by reverting.
#[cfg(test)]
mod bucket_bundles_tests {
    use super::*;
    async fn shutdown(repl: LtxRepl) {
        let owner = repl.take_task_owner();
        drop(repl);
        owner.join().await;
    }
    /// The operator switch is OFF by default; only an explicit
    /// `CELLO_BUCKET_BUNDLES=1` enables it. The format-3 fleet gate still
    /// governs (see `bucket_gate_stays_closed_until_the_first_sample`).
    /// Fail-before: `!matches!(Some("1"))` enables with no env set.
    #[test]
    fn bucket_env_is_off_unless_explicit_one() {
        assert!(!bucket_bundles_env_value(None), "absent env disables");
        assert!(!bucket_bundles_env_value(Some("")), "empty disables");
        assert!(bucket_bundles_env_value(Some("1")), "explicit 1 enables");
        assert!(!bucket_bundles_env_value(Some("0")), "explicit 0 disables");
        assert!(
            !bucket_bundles_env_value(Some(" 0 ")),
            "surrounding whitespace does not save a 0"
        );
    }

    /// The linger defaults to none and clamps to its cap, so a typo can never
    /// stretch every bucket ack toward the durability timeout.
    #[test]
    fn bucket_linger_defaults_to_zero_and_clamps() {
        assert_eq!(bucket_bundle_linger_value(None), Duration::ZERO);
        assert_eq!(bucket_bundle_linger_value(Some("")), Duration::ZERO);
        assert_eq!(bucket_bundle_linger_value(Some("x")), Duration::ZERO);
        assert_eq!(
            bucket_bundle_linger_value(Some(" 200 ")),
            Duration::from_millis(200)
        );
        assert_eq!(
            bucket_bundle_linger_value(Some("60000")),
            Duration::from_millis(BUCKET_BUNDLE_LINGER_MAX_MS)
        );
    }

    /// Gate: off until the first fleet sample, and closed while any live reader is
    /// format-2. The fleet flag mirrors `set_paged_fleet` (off until first sample;
    /// failed sample publishes an empty view, which closes it). Fail-before: initial
    /// `AtomicBool::new(true)` opens bundles before the first sample.
    #[test]
    fn bucket_gate_stays_closed_until_the_first_sample() {
        let runtime = asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let _temp = tempfile::tempdir().unwrap();
            let watch = _temp.path().to_path_buf();
            let store = Arc::new(cello_ltx::object_store::memory::InMemory::new());
            let repl = LtxRepl::start_with_store_for_test(&watch, store);
            repl.set_bucket_bundles_env_override_for_test(Some(true));
            assert!(!repl.bucket_bundles_enabled(), "off until the first sample");
            repl.set_bucket_bundles_fleet(true);
            assert!(repl.bucket_bundles_enabled(), "own format-3 lease opens");
            repl.set_bucket_bundles_fleet(false);
            assert!(
                !repl.bucket_bundles_enabled(),
                "a live format-2 lease closes"
            );
            repl.set_bucket_bundles_env_override_for_test(Some(false));
            repl.set_bucket_bundles_fleet(true);
            assert!(!repl.bucket_bundles_enabled(), "operator switch off closes");
            repl.set_bucket_bundles_env_override_for_test(None);
            shutdown(repl).await;
        });
    }

    /// `sync_loop` pacing: per-cell L0 uploads stop while bucket bundling is active
    /// even though no shipper is installed (`paced` false) — the bundle PUT is the
    /// ack path there. When the gate closes (or credit is refused), per-cell resumes
    /// at once. Fail-before: `paced && bundle_active` alone keeps per-cell uploads
    /// running under bucket bundling.
    #[test]
    fn sync_pacing_stops_per_cell_while_bucket_bundling() {
        assert!(
            sync_should_bundle(true, true, false),
            "fleet paced bundling"
        );
        assert!(
            sync_should_bundle(false, false, true),
            "bucket bundling stops per-cell with no shipper"
        );
        assert!(
            !sync_should_bundle(false, false, false),
            "gate closed: per-cell as today"
        );
        assert!(
            !sync_should_bundle(true, false, false),
            "paced but no sink: per-cell"
        );
        assert!(
            !sync_should_bundle(false, true, false),
            "bundle without pace or bucket: per-cell"
        );
    }

    fn bundle_entry(cell: &str, txid: u64, bytes: usize) -> cello_ltx::bundle::BundleEntry {
        cello_ltx::bundle::BundleEntry {
            cell: cell.to_string(),
            cell_epoch: 1,
            txid,
            bytes: vec![0u8; bytes],
        }
    }

    /// Cross-cell batching: N dirty cells under the cap ride ONE bundle PUT, where
    /// per-cell flush would need N PUTs. Fail-before: flushing per-cell (one chunk
    /// per entry) needs N PUTs here.
    #[test]
    fn four_cells_batch_into_one_bundle_put() {
        let entries: Vec<_> = (0..4)
            .map(|i| bundle_entry(&format!("c{i}"), 1, 100))
            .collect();
        let chunks = split_bundle_entries(entries);
        assert_eq!(chunks.len(), 1, "four small cells share one PUT");
        assert_eq!(chunks[0].len(), 4, "the chunk carries all four rows");
    }

    /// Cap and split: above the row cap the flush splits into one PUT per chunk.
    /// Fail-before: a single unbounded PUT never splits.
    #[test]
    fn oversized_bundle_splits_above_the_cap() {
        let entries: Vec<_> = (0..(BUCKET_BUNDLE_MAX_ROWS + 10))
            .map(|i| bundle_entry("hot", (i as u64) + 1, 10))
            .collect();
        let chunks = split_bundle_entries(entries);
        assert!(chunks.len() >= 2, "above-cap rows split");
        assert!(
            chunks.iter().all(|c| c.len() <= BUCKET_BUNDLE_MAX_ROWS),
            "no chunk exceeds the row cap"
        );
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(total, BUCKET_BUNDLE_MAX_ROWS + 10, "split loses no rows");
        assert!(
            split_bundle_entries(Vec::new()).is_empty(),
            "empty captures credit without a PUT"
        );
    }
}

#[cfg(cello_internal_tests)]
include!(env!("CELLO_INTERNAL_LTX_REPL_OBSERVERS"));
