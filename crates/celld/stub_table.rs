// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): cross-isolate RPC
// stubs and streams. Apache-2.0.

//! The host-side RPC stub table: live JS capabilities a Durable Object
//! exported across an isolate boundary.
//!
//! Same-isolate stub entries live in a per-isolate JS `Map` under small
//! numeric ids, which collide across isolates and are trivially forgeable.
//! A capability that crosses isolates is instead exported here first, under
//! a 128-bit CSPRNG id, together with the owning cell's scope, activation
//! epoch, and application generation. Every stub operation validates the
//! presented id against all four before it routes, so a guessed id is
//! infeasible and a leaked id from another app, generation, or superseded
//! epoch is useless. The table is the security boundary; the marker fields
//! on the wire are routing hints the table checks, never trusts.
//!
//! Lifetime: a stub received by a caller is owned by the caller's
//! request/invocation. The caller's isolate registers each live handle
//! ([`acquire_host_stub`], keyed by the holder's heap so forwarding and
//! `dup()` balance) and drops it on dispose, context end, or context abort;
//! when the last hold goes the entry's dispose is routed to the owner, which
//! runs the target disposer. An in-flight operation pins its entry, so a
//! release racing it cannot pull the id out from underneath: the removal
//! waits for the pin. Owner eviction unexports synchronously. Isolate
//! retirement releases whatever the freed heap still held, which is the
//! only backstop, and it fires solely for holders that are provably gone.
//! There is deliberately no wall-clock expiry: a stub whose holding
//! invocation is alive stays usable however long it stays quiet.

use celld_logic::isolate::HeapId;
use rand::RngCore as _;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::OnceLock;

/// Live entries one owning scope may hold. Past this the export fails
/// closed with a clear error instead of minting: evicting the oldest
/// would break live handles.
pub(crate) const HOST_STUB_MAX_PER_SCOPE: usize = 4096;
/// Live entries the whole process may hold, across every scope.
pub(crate) const HOST_STUB_MAX_PER_PROCESS: usize = 65536;

/// The effective per-scope mint cap: the default unless
/// `CELLD_STUB_SCOPE_CAP` overrides it. Conformance tests boot with a
/// tiny cap so the fail-closed error is reachable without churning
/// thousands of entries; garbage falls back to the default.
pub(crate) fn scope_cap() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        crate::env_vars::optional::<usize>("CELLD_STUB_SCOPE_CAP")
            .ok()
            .flatten()
            .filter(|cap| *cap > 0)
            .unwrap_or(HOST_STUB_MAX_PER_SCOPE)
    })
}

/// The effective per-process mint cap: the default unless
/// `CELLD_STUB_PROCESS_CAP` overrides it.
pub(crate) fn process_cap() -> usize {
    static CAP: OnceLock<usize> = OnceLock::new();
    *CAP.get_or_init(|| {
        crate::env_vars::optional::<usize>("CELLD_STUB_PROCESS_CAP")
            .ok()
            .flatten()
            .filter(|cap| *cap > 0)
            .unwrap_or(HOST_STUB_MAX_PER_PROCESS)
    })
}

/// Whether the harness defines the test-only stub-stats global. Set only
/// by `CELLD_TEST_STUB_STATS=1` in the node's process environment, which
/// tenant code cannot reach (worker bindings never become process
/// variables); anything else, including garbage, leaves production
/// isolates without the hook. The conformance suites boot with it set.
pub(crate) fn test_stub_stats_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("CELLD_TEST_STUB_STATS").as_deref() == Ok("1"))
}
/// A stub id is exactly the minted shape: 32 lowercase hex digits.
/// Anything else is rejected before any lookup.
pub(crate) const HOST_STUB_ID_LEN: usize = 32;
/// How deep a stub-operation path may walk. Method paths are a few
/// parts; anything deeper is a caller bug or owner-CPU burn.
pub(crate) const STUB_PATH_MAX_DEPTH: usize = 32;
/// How large one path part may be. Parts are property names.
pub(crate) const STUB_PATH_PART_MAX_BYTES: usize = 1024;
/// Mint attempts before a collision run (or a table wedged at the instant
/// of mint) gives up and lets the lifter fall back to a same-isolate
/// marker. At 128 bits one attempt already never collides.
const MINT_ATTEMPTS: usize = 8;

/// Whether `id` is exactly the minted shape: 32 lowercase hex digits.
/// Uppercase hex, truncated, overlong, and non-hex inputs all fail:
/// the check runs before any table lookup, so malformed input costs
/// no hash and reaches no V8 string.
pub fn valid_host_stub_id(id: &str) -> bool {
    id.len() == HOST_STUB_ID_LEN
        && id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

/// Whether `path` fits the stub-operation walk budget: at most
/// [`STUB_PATH_MAX_DEPTH`] non-empty parts of at most
/// [`STUB_PATH_PART_MAX_BYTES`] bytes each.
pub fn valid_stub_path(path: &[String]) -> bool {
    path.len() <= STUB_PATH_MAX_DEPTH
        && path
            .iter()
            .all(|part| !part.is_empty() && part.len() <= STUB_PATH_PART_MAX_BYTES)
}

/// What the exported JS value is. Recorded for operators and tests; the
/// owner runs every kind through the same `__rpcWalk`, so validation does
/// not gate on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StubKind {
    RpcTarget,
    Readable,
    Writable,
    Disposer,
}

impl StubKind {
    pub(crate) fn parse(kind: &str) -> Option<Self> {
        match kind {
            "target" => Some(Self::RpcTarget),
            "readable" => Some(Self::Readable),
            "writable" => Some(Self::Writable),
            "disposer" => Some(Self::Disposer),
            _ => None,
        }
    }
}

/// One exported capability: who owns it and who holds it.
#[derive(Clone, Debug)]
pub struct HostStub {
    /// The owning cell scope. Facets export under their root's scope: a
    /// facet stub never outlives its root, and the root's residency is the
    /// authority boundary.
    pub(crate) scope: String,
    /// The owner's activation epoch at mint time. An eviction followed by a
    /// reactivation mints a new epoch, so a stale id fails closed.
    pub(crate) epoch: u64,
    pub(crate) generation: crate::generation::GenerationId,
    pub kind: StubKind,
    /// Live handles per holder heap: one count per isolate holding the id.
    /// Forwarding moves one count from sender to receiver; `dup()` adds
    /// one; dispose and invocation end remove one. The entry's dispose
    /// routes to the owner when the last count goes.
    holders: HashMap<HeapId, usize>,
    /// Validated operations currently driving against the owner. A release
    /// that empties the holders while pins are out marks the entry instead
    /// of removing it; the last unpin completes the removal.
    pins: usize,
    /// Set once the holders are empty: the dispose is routed (or was
    /// attempted and orphaned on route failure), and the entry goes at
    /// the last pin. Cleared if a late in-transit revive re-acquires.
    release_requested: bool,
    /// Set when a handoff drops the last hold: the reference is in an
    /// envelope no isolate holds yet, and unpin/take must keep the entry
    /// for the receiver. Separate from `release_requested`, which marks
    /// a routed dispose; `acquire` clears both.
    in_transit: bool,
}

/// Why an operation on a presented id was refused. Carries no id: ids are
/// never logged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StubRejection {
    /// No such entry: unknown, already released, or unexported.
    Unknown,
    /// The id is not the minted shape, or the path breaks the walk
    /// budget. Answered with a clear error: the format is public, so
    /// naming it leaks nothing.
    Malformed,
    /// The presented scope is not the scope the id was minted for.
    ScopeMismatch,
    /// The owning cell is not resident on this node.
    NotResident,
    /// The owning cell is resident at another epoch: it was evicted and
    /// reactivated after the mint.
    EpochMismatch,
    /// The owning cell is resident in another application generation: a
    /// reload moved it after the mint. The caller's own generation is
    /// deliberately not compared: a draining caller from the previous
    /// deployment must still reach the residency that minted the id, the
    /// way its plain RPCs do.
    GenerationMismatch,
    /// The calling isolate holds no live handle for this id. A stolen id
    /// alone is useless, and so is a forged release: only a holder may
    /// use or free the handle.
    Forbidden,
}

/// Why a mint was refused. Every variant fails the export; none evicts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MintRejection {
    /// The id is already taken. The existing entry is untouched: a
    /// collision must never reroute the victim id.
    Occupied,
    /// The owning scope already holds [`HOST_STUB_MAX_PER_SCOPE`] live
    /// entries.
    ScopeCap,
    /// The process already holds [`HOST_STUB_MAX_PER_PROCESS`] live
    /// entries.
    ProcessCap,
}

/// The residency currently serving a scope: its activation epoch and the
/// application generation whose isolate holds it. The triple of scope,
/// epoch, and generation identifies the minting residency exactly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResidentCell {
    pub epoch: u64,
    pub generation: crate::generation::GenerationId,
}

/// What a validated operation needs: the minted epoch, to re-check against
/// residency after routing pins the cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidatedStub {
    pub epoch: u64,
    pub generation: crate::generation::GenerationId,
}

/// What dropping one hold means for the entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReleaseAction {
    /// Other holders remain: the entry stays and nothing routes.
    Held,
    /// The last hold went: route a dispose of `epoch` to `scope`, then
    /// take the entry. The scope and epoch come from the table, never
    /// the caller.
    Route { scope_epoch: (String, u64) },
    /// No such entry: release is idempotent.
    Unknown,
}

/// What taking an entry after its routed dispose found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TakeAction {
    /// The entry was removed.
    Removed,
    /// A late in-transit revive re-acquired while the dispose drove:
    /// the entry stays for the new holder.
    StillHeld,
    /// An operation validated before the release is still driving, or
    /// the entry is in transit to a receiver: the entry stays until the
    /// last unpin or the receiver's acquire.
    Pinned,
    /// The entry is already gone (owner eviction unexported it).
    Gone,
}

/// An entry whose last holder was a retired isolate: route a dispose of
/// `epoch` to `scope`, then take the entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrphanedStub {
    pub id: String,
    pub scope: String,
    pub epoch: u64,
}

/// The table itself, free of globals and clocks so unit tests can drive
/// it directly. The process-wide registry below is a thin locked wrapper.
#[derive(Debug, Default)]
pub(crate) struct HostStubTable {
    stubs: HashMap<String, HostStub>,
    /// Live entries per owning scope, for the per-scope mint cap and the
    /// per-scope counts in `/state`. Maintained incrementally so mint
    /// stays O(1); every removal goes through [`Self::remove`].
    scopes: HashMap<String, usize>,
}

impl HostStubTable {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn len(&self) -> usize {
        self.stubs.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.stubs.is_empty()
    }

    pub(crate) fn count_by_scope(&self) -> HashMap<String, usize> {
        self.scopes.clone()
    }

    fn remove(&mut self, id: &str) -> Option<HostStub> {
        let stub = self.stubs.remove(id)?;
        if let Some(count) = self.scopes.get_mut(&stub.scope) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.scopes.remove(&stub.scope);
            }
        }
        Some(stub)
    }

    /// Record one export. Refuses a taken id without touching the victim
    /// entry, and refuses past either cap without evicting: the export
    /// fails closed and the lifter reports it.
    pub(crate) fn mint(
        &mut self,
        id: String,
        scope: String,
        epoch: u64,
        generation: crate::generation::GenerationId,
        kind: StubKind,
    ) -> Result<(), MintRejection> {
        if self.stubs.contains_key(&id) {
            return Err(MintRejection::Occupied);
        }
        if self.stubs.len() >= process_cap() {
            return Err(MintRejection::ProcessCap);
        }
        if self.scopes.get(&scope).copied().unwrap_or(0) >= scope_cap() {
            return Err(MintRejection::ScopeCap);
        }
        self.scopes
            .entry(scope.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);
        self.stubs.insert(
            id,
            HostStub {
                scope,
                epoch,
                generation,
                kind,
                holders: HashMap::new(),
                pins: 0,
                release_requested: false,
                in_transit: false,
            },
        );
        Ok(())
    }

    /// Validate one operation: the id is well-formed and exists, the scope
    /// matches the mint, the owner is still resident at the minted epoch
    /// in the minted generation, and the calling isolate holds a live
    /// handle. A success pins the entry for the driving operation, so a
    /// concurrent release cannot remove the id mid-op; dispatch unpins on
    /// every exit path.
    pub(crate) fn validate(
        &mut self,
        id: &str,
        scope: &str,
        resident: Option<ResidentCell>,
        caller: HeapId,
    ) -> Result<ValidatedStub, StubRejection> {
        let validated = self.check(id, scope, resident, caller)?;
        if let Some(stub) = self.stubs.get_mut(id) {
            stub.pins += 1;
        }
        Ok(validated)
    }

    /// Re-check a validated operation after routing pins the cell, to
    /// close the eviction race. Takes no pin: the first validation's
    /// pin already covers the drive.
    pub(crate) fn revalidate(
        &self,
        id: &str,
        scope: &str,
        resident: Option<ResidentCell>,
        caller: HeapId,
    ) -> Result<ValidatedStub, StubRejection> {
        self.check(id, scope, resident, caller)
    }

    fn check(
        &self,
        id: &str,
        scope: &str,
        resident: Option<ResidentCell>,
        caller: HeapId,
    ) -> Result<ValidatedStub, StubRejection> {
        if !valid_host_stub_id(id) {
            return Err(StubRejection::Malformed);
        }
        let stub = self.stubs.get(id).ok_or(StubRejection::Unknown)?;
        if stub.scope != scope {
            return Err(StubRejection::ScopeMismatch);
        }
        let Some(resident) = resident else {
            return Err(StubRejection::NotResident);
        };
        if resident.epoch != stub.epoch {
            return Err(StubRejection::EpochMismatch);
        }
        if resident.generation != stub.generation {
            return Err(StubRejection::GenerationMismatch);
        }
        if !stub.holders.contains_key(&caller) {
            return Err(StubRejection::Forbidden);
        }
        Ok(ValidatedStub {
            epoch: stub.epoch,
            generation: stub.generation,
        })
    }

    /// Drop one operation pin. When the last pin goes on an entry whose
    /// holders are all gone, the entry goes too: the release already
    /// routed its dispose. An in-transit entry stays for its receiver.
    /// Unknown ids are not an error: the owner eviction path removes
    /// pinned entries outright, and the orphaned pins land here.
    pub(crate) fn unpin(&mut self, id: &str) {
        let Some(stub) = self.stubs.get_mut(id) else {
            return;
        };
        stub.pins = stub.pins.saturating_sub(1);
        if stub.pins == 0 && stub.release_requested && !stub.in_transit && stub.holders.is_empty() {
            self.remove(id);
        }
    }

    /// Register one live handle for `holder`: a revive, or a `dup()` of a
    /// host handle. Best-effort: a revive racing owner eviction finds no
    /// entry, and its handle then fails on use like any released stub.
    /// A live holder ends any transit or routed release: both flags clear.
    pub(crate) fn acquire(&mut self, id: &str, holder: HeapId) -> bool {
        let Some(stub) = self.stubs.get_mut(id) else {
            return false;
        };
        stub.holders
            .entry(holder)
            .and_modify(|count| *count += 1)
            .or_insert(1);
        stub.in_transit = false;
        stub.release_requested = false;
        true
    }

    /// Move one hold off the sending isolate: the sender's handle is dead
    /// (its reference moved into the envelope) while the receiver has not
    /// revived yet. When the sender held the only counts the entry goes
    /// in-transit: it stays, flagged, until the receiver acquires or the
    /// owner goes. A sender holding nothing moves nothing. Unknown ids
    /// are not an error.
    pub(crate) fn handoff(&mut self, id: &str, holder: HeapId) {
        let Some(stub) = self.stubs.get_mut(id) else {
            return;
        };
        if Self::drop_hold(stub, holder) && stub.holders.is_empty() {
            stub.in_transit = true;
        }
    }

    /// Drop one hold of `holder`: a dispose, an invocation end, or a
    /// context abort. Only a holder may free a hold, so a forged release
    /// from a non-holder frees nothing, including an in-transit entry no
    /// isolate holds. When the last hold goes the caller must route the
    /// owner's dispose, then take the entry.
    pub(crate) fn release_hold(&mut self, id: &str, holder: HeapId) -> ReleaseAction {
        let Some(stub) = self.stubs.get_mut(id) else {
            return ReleaseAction::Unknown;
        };
        if !stub.holders.contains_key(&holder) {
            return ReleaseAction::Held;
        }
        Self::drop_hold(stub, holder);
        if !stub.holders.is_empty() {
            return ReleaseAction::Held;
        }
        stub.release_requested = true;
        ReleaseAction::Route {
            scope_epoch: (stub.scope.clone(), stub.epoch),
        }
    }

    /// Drop one hold of `holder`, answering whether one was held.
    fn drop_hold(stub: &mut HostStub, holder: HeapId) -> bool {
        match stub.holders.get_mut(&holder) {
            Some(count) if *count > 1 => {
                *count -= 1;
                true
            }
            Some(_) => {
                stub.holders.remove(&holder);
                true
            }
            None => false,
        }
    }

    /// Take the entry after its dispose drove against the owner. A pinned
    /// entry stays for the last unpin, an in-transit entry stays for its
    /// receiver, and a re-acquired entry stays for its new holder. Takes
    /// only an entry with no holders: while any hold is out the entry is
    /// live by definition.
    pub(crate) fn take_for_route(&mut self, id: &str) -> TakeAction {
        let Some(stub) = self.stubs.get(id) else {
            return TakeAction::Gone;
        };
        if !stub.holders.is_empty() {
            if let Some(stub) = self.stubs.get_mut(id) {
                stub.release_requested = false;
            }
            return TakeAction::StillHeld;
        }
        if stub.pins > 0 || stub.in_transit {
            return TakeAction::Pinned;
        }
        self.remove(id);
        TakeAction::Removed
    }

    /// Forget one entry outright: the owner is already gone, so nothing
    /// routes anywhere. Unknown ids are not an error.
    pub(crate) fn unexport(&mut self, id: &str) -> bool {
        self.remove(id).is_some()
    }

    /// The retirement backstop: drop every hold of the freed heap, and
    /// collect the entries that lost their last hold so the caller can
    /// route each one's dispose to its owner. Entries with other holders
    /// or in-transit to a live receiver are untouched: the backstop only
    /// fires for holders that are provably gone.
    pub(crate) fn release_holder(&mut self, holder: HeapId) -> Vec<OrphanedStub> {
        let mut orphaned = Vec::new();
        for (id, stub) in self.stubs.iter_mut() {
            if stub.holders.remove(&holder).is_none() {
                continue;
            }
            if stub.holders.is_empty() {
                stub.release_requested = true;
                orphaned.push(OrphanedStub {
                    id: id.clone(),
                    scope: stub.scope.clone(),
                    epoch: stub.epoch,
                });
            }
        }
        orphaned
    }
}

static HOST_STUBS: OnceLock<Mutex<HostStubTable>> = OnceLock::new();

fn host_stubs() -> &'static Mutex<HostStubTable> {
    HOST_STUBS.get_or_init(|| Mutex::new(HostStubTable::new()))
}

/// Mint one 128-bit id, hex-encoded. The labeled execution-domain RNG is the
/// only randomness source the boundary lint admits.
pub fn new_host_stub_id() -> String {
    let mut bytes = [0_u8; 16];
    crate::asyncrt::rng("rpc-stub").fill_bytes(&mut bytes);
    format!("{:032x}", u128::from_ne_bytes(bytes))
}

/// Export one capability of `scope` at `epoch` in `generation`. Returns the
/// wire id, minting until vacant so a collision never reroutes a victim
/// id, or the reason the export fails closed.
pub fn mint_host_stub(
    scope: String,
    epoch: u64,
    generation: crate::generation::GenerationId,
    kind: StubKind,
) -> Result<String, MintRejection> {
    let mut table = host_stubs().lock().expect("stub table poisoned");
    for _ in 0..MINT_ATTEMPTS {
        let id = new_host_stub_id();
        match table.mint(id.clone(), scope.clone(), epoch, generation, kind) {
            Ok(()) => return Ok(id),
            Err(MintRejection::Occupied) => continue,
            Err(other) => return Err(other),
        }
    }
    Err(MintRejection::Occupied)
}

/// Validate one stub operation against the table, pinning the entry for
/// the driving operation. `resident` is the residency currently serving
/// the scope, when the cell is resident on this node; `caller` is the
/// heap driving the op, which must hold a live handle for the id.
pub fn validate_host_stub(
    id: &str,
    scope: &str,
    resident: Option<ResidentCell>,
    caller: HeapId,
) -> Result<ValidatedStub, StubRejection> {
    host_stubs()
        .lock()
        .expect("stub table poisoned")
        .validate(id, scope, resident, caller)
}

/// Re-check a validated operation after routing pins the cell. Takes no
/// pin: the first validation's pin already covers the drive.
pub fn revalidate_host_stub(
    id: &str,
    scope: &str,
    resident: Option<ResidentCell>,
    caller: HeapId,
) -> Result<ValidatedStub, StubRejection> {
    host_stubs()
        .lock()
        .expect("stub table poisoned")
        .revalidate(id, scope, resident, caller)
}

/// Drop one operation pin, completing a raced release when the last pin
/// goes on a holderless entry.
pub fn unpin_host_stub(id: &str) {
    host_stubs().lock().expect("stub table poisoned").unpin(id);
}

/// Register one live handle of `holder` for `id`: a revive, or a `dup()`.
/// Best-effort: false when the entry is already gone.
pub fn acquire_host_stub(id: &str, holder: HeapId) -> bool {
    host_stubs()
        .lock()
        .expect("stub table poisoned")
        .acquire(id, holder)
}

/// Move one hold off the sending isolate for a forwarded handle.
pub fn handoff_host_stub(id: &str, holder: HeapId) {
    host_stubs()
        .lock()
        .expect("stub table poisoned")
        .handoff(id, holder);
}

/// Drop one hold of `holder` for `id`. When the last hold goes the caller
/// must route the owner's dispose, then take the entry.
pub fn release_host_stub_hold(id: &str, holder: HeapId) -> ReleaseAction {
    host_stubs()
        .lock()
        .expect("stub table poisoned")
        .release_hold(id, holder)
}

/// Take the entry after its dispose drove against the owner.
pub fn take_routed_stub_release(id: &str) -> TakeAction {
    host_stubs()
        .lock()
        .expect("stub table poisoned")
        .take_for_route(id)
}

/// Re-drive a stub release whose inline attempts all failed transiently:
/// run `drive` up to `rounds` more times, calling `wait` before every
/// round so the retries pace out instead of busy-looping. `drive`
/// reports whether the release settled (disposed or unroutable).
/// Returns true once a drive settles; false exhausts the rounds, and
/// the caller then frees the entry anyway, with owner eviction as the
/// backstop, so a retained entry never holds host caps until eviction.
pub async fn requeue_release_drive<D, W, DFut, WFut>(
    rounds: usize,
    mut drive: D,
    mut wait: W,
) -> bool
where
    D: FnMut() -> DFut,
    DFut: std::future::Future<Output = bool>,
    W: FnMut() -> WFut,
    WFut: std::future::Future<Output = ()>,
{
    for _ in 0..rounds {
        wait().await;
        if drive().await {
            return true;
        }
    }
    false
}

/// Forget one entry without routing anywhere: the owner is already gone.
pub fn unexport_host_stub(id: &str) -> bool {
    host_stubs()
        .lock()
        .expect("stub table poisoned")
        .unexport(id)
}

/// The retirement backstop: drop every hold of the freed heap and collect
/// the entries that lost their last hold, so the caller can route each
/// one's dispose to its owner.
pub fn release_stub_holder(holder: HeapId) -> Vec<OrphanedStub> {
    host_stubs()
        .lock()
        .expect("stub table poisoned")
        .release_holder(holder)
}

/// How many entries the table holds, for `/state` and leak tests.
pub fn host_stub_count() -> usize {
    host_stubs().lock().expect("stub table poisoned").len()
}

/// Live entries per owning scope, for `/state`: a count that grows
/// without bound names the leaking scope.
pub fn host_stub_counts_by_scope() -> HashMap<String, usize> {
    host_stubs()
        .lock()
        .expect("stub table poisoned")
        .count_by_scope()
}

#[cfg(test)]
mod host_stub_table_tests {
    use super::*;

    const SCOPE: &str = "Counter:abc";
    const OTHER_SCOPE: &str = "Counter:def";
    const EPOCH: u64 = 7;
    const GENERATION: u64 = 1;
    const ID: &str = "0123456789abcdef0123456789abcdef";

    fn table() -> HostStubTable {
        HostStubTable::new()
    }

    fn holder(id: u64) -> HeapId {
        HeapId::new(id)
    }

    fn resident() -> Option<ResidentCell> {
        Some(ResidentCell {
            epoch: EPOCH,
            generation: GENERATION,
        })
    }

    fn mint(table: &mut HostStubTable, id: &str) {
        table
            .mint(
                id.to_string(),
                SCOPE.to_string(),
                EPOCH,
                GENERATION,
                StubKind::RpcTarget,
            )
            .expect("mint under the caps");
    }

    fn minted_held(table: &mut HostStubTable, id: &str, holder: HeapId) {
        mint(table, id);
        assert!(table.acquire(id, holder), "acquire a fresh mint");
    }

    #[test]
    fn validate_accepts_a_held_mint_and_revalidate_is_harmless() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        let validated = table
            .validate(ID, SCOPE, resident(), holder(1))
            .expect("held mint validates");
        assert_eq!(validated.epoch, EPOCH);
        assert_eq!(validated.generation, GENERATION);
        // Re-running a passed validation is harmless: dispatch validates
        // once before routing and re-checks after the pin.
        table
            .revalidate(ID, SCOPE, resident(), holder(1))
            .expect("revalidation passes");
        table.unpin(ID);
    }

    #[test]
    fn validate_rejects_malformed_ids_before_any_lookup() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        for bad in [
            "",
            "id",
            "0123456789abcdef0123456789abcde",
            "0123456789abcdef0123456789abcdef0",
            "0123456789ABCDEF0123456789ABCDEF",
            "0123456789abcdef0123456789abcdeg",
            "gggggggggggggggggggggggggggggggg",
            " 123456789abcdef0123456789abcdef",
        ] {
            assert_eq!(
                table.validate(bad, SCOPE, resident(), holder(1)),
                Err(StubRejection::Malformed),
                "{bad:?} is not the minted shape"
            );
        }
        // A well-formed but unknown id is Unknown, not Malformed.
        assert_eq!(
            table.validate(
                "ffffffffffffffffffffffffffffffff",
                SCOPE,
                resident(),
                holder(1)
            ),
            Err(StubRejection::Unknown)
        );
        // The failed checks consume nothing.
        table
            .revalidate(ID, SCOPE, resident(), holder(1))
            .expect("entry survives malformed checks");
    }

    #[test]
    fn validate_rejects_a_scope_mismatch() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        assert_eq!(
            table.validate(ID, OTHER_SCOPE, resident(), holder(1)),
            Err(StubRejection::ScopeMismatch)
        );
        // The failed check does not consume the entry.
        table
            .revalidate(ID, SCOPE, resident(), holder(1))
            .expect("entry survives a scope mismatch");
    }

    #[test]
    fn validate_rejects_a_moved_owner() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        assert_eq!(
            table.validate(ID, SCOPE, None, holder(1)),
            Err(StubRejection::NotResident)
        );
        assert_eq!(
            table.validate(
                ID,
                SCOPE,
                Some(ResidentCell {
                    epoch: EPOCH + 1,
                    generation: GENERATION,
                }),
                holder(1)
            ),
            Err(StubRejection::EpochMismatch)
        );
        table
            .revalidate(ID, SCOPE, resident(), holder(1))
            .expect("entry survives residency failures");
    }

    #[test]
    fn validate_rejects_a_reload_moved_owner() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        // The scope reactivated under a new deployment while this id was
        // out: same scope and epoch, another generation.
        assert_eq!(
            table.validate(
                ID,
                SCOPE,
                Some(ResidentCell {
                    epoch: EPOCH,
                    generation: GENERATION + 1,
                }),
                holder(1)
            ),
            Err(StubRejection::GenerationMismatch)
        );
        table
            .revalidate(ID, SCOPE, resident(), holder(1))
            .expect("entry survives a generation mismatch");
    }

    #[test]
    fn validate_rejects_a_non_holder() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        // A stolen id without a live handle is useless.
        assert_eq!(
            table.validate(ID, SCOPE, resident(), holder(2)),
            Err(StubRejection::Forbidden)
        );
        table
            .revalidate(ID, SCOPE, resident(), holder(1))
            .expect("entry survives a forbidden check");
    }

    #[test]
    fn release_from_a_non_holder_frees_nothing() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        // A forged release from an isolate holding nothing for the id
        // must not dispose the victim's handle.
        assert_eq!(
            table.release_hold(ID, holder(9)),
            ReleaseAction::Held,
            "non-holder release routes nothing"
        );
        table
            .revalidate(ID, SCOPE, resident(), holder(1))
            .expect("victim hold survives a forged release");
        assert_eq!(
            table.release_hold(ID, holder(1)),
            ReleaseAction::Route {
                scope_epoch: (SCOPE.to_string(), EPOCH),
            }
        );
        assert_eq!(table.take_for_route(ID), TakeAction::Removed);
        assert!(table.is_empty());
    }

    #[test]
    fn last_release_routes_and_take_removes() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        assert_eq!(
            table.release_hold(ID, holder(1)),
            ReleaseAction::Route {
                scope_epoch: (SCOPE.to_string(), EPOCH),
            }
        );
        // The entry stays until the routed dispose drives.
        assert_eq!(table.len(), 1);
        assert_eq!(table.take_for_route(ID), TakeAction::Removed);
        assert!(table.is_empty());
        assert_eq!(
            table.release_hold(ID, holder(1)),
            ReleaseAction::Unknown,
            "release stays idempotent"
        );
    }

    #[test]
    fn dup_shares_one_entry_until_the_last_release() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        assert!(table.acquire(ID, holder(1)), "dup adds a count");
        assert_eq!(table.release_hold(ID, holder(1)), ReleaseAction::Held);
        assert_eq!(table.len(), 1, "first dispose routes nothing");
        table
            .revalidate(ID, SCOPE, resident(), holder(1))
            .expect("dup'd handle still validates");
        assert!(matches!(
            table.release_hold(ID, holder(1)),
            ReleaseAction::Route { .. }
        ));
        assert_eq!(table.take_for_route(ID), TakeAction::Removed);
    }

    #[test]
    fn handoff_moves_one_hold_to_the_receiver() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        table.handoff(ID, holder(1));
        // In transit: no holder, but the entry stays for the receiver.
        assert_eq!(table.len(), 1);
        assert_eq!(
            table.validate(ID, SCOPE, resident(), holder(2)),
            Err(StubRejection::Forbidden),
            "unrevived envelope grants nothing"
        );
        assert!(table.acquire(ID, holder(2)), "receiver revives");
        table
            .revalidate(ID, SCOPE, resident(), holder(2))
            .expect("receiver validates");
        assert_eq!(
            table.validate(ID, SCOPE, resident(), holder(1)),
            Err(StubRejection::Forbidden),
            "sender lost its hold at handoff"
        );
        assert!(matches!(
            table.release_hold(ID, holder(2)),
            ReleaseAction::Route { .. }
        ));
        assert_eq!(table.take_for_route(ID), TakeAction::Removed);
    }

    #[test]
    fn unpin_preserves_an_in_transit_entry_for_its_receiver() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        table
            .validate(ID, SCOPE, resident(), holder(1))
            .expect("op validates and pins");
        // The sender forwards its handle while its own op drives: the
        // entry goes in-transit, flagged, with no holder.
        table.handoff(ID, holder(1));
        table.unpin(ID);
        assert_eq!(table.len(), 1, "unpin keeps the in-transit entry");
        assert!(table.acquire(ID, holder(2)), "receiver revives");
        table
            .revalidate(ID, SCOPE, resident(), holder(2))
            .expect("receiver validates");
    }

    #[test]
    fn release_from_a_non_holder_of_an_in_transit_entry_routes_nothing() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        table.handoff(ID, holder(1));
        // The envelope is in transit: no holder exists, so any release
        // is from a non-holder and must route nothing.
        assert_eq!(
            table.release_hold(ID, holder(9)),
            ReleaseAction::Held,
            "forged release of an in-transit entry routes nothing"
        );
        assert_eq!(table.len(), 1, "entry survives the forged release");
        assert!(table.acquire(ID, holder(2)), "receiver still revives");
    }

    #[test]
    fn second_release_from_a_former_holder_routes_nothing() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        assert!(matches!(
            table.release_hold(ID, holder(1)),
            ReleaseAction::Route { .. }
        ));
        // The former holder holds nothing now: a duplicate release is a
        // non-holder release and must not route a second dispose.
        assert_eq!(
            table.release_hold(ID, holder(1)),
            ReleaseAction::Held,
            "duplicate release routes nothing"
        );
    }

    #[test]
    fn in_flight_ops_pin_against_a_racing_release() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        table
            .validate(ID, SCOPE, resident(), holder(1))
            .expect("op validates and pins");
        assert!(matches!(
            table.release_hold(ID, holder(1)),
            ReleaseAction::Route { .. }
        ));
        // The dispose drove while the op was in flight: the take defers.
        assert_eq!(table.take_for_route(ID), TakeAction::Pinned);
        assert_eq!(table.len(), 1, "pinned entry survives the take");
        table.unpin(ID);
        assert!(table.is_empty(), "last unpin completes the release");
        table.unpin(ID);
        assert!(table.is_empty(), "unpinning a gone entry is a no-op");
    }

    #[test]
    fn unexport_removes_despite_pins() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        table
            .validate(ID, SCOPE, resident(), holder(1))
            .expect("op validates and pins");
        assert!(table.unexport(ID), "eviction unexports");
        assert!(table.is_empty());
        assert_eq!(
            table.revalidate(ID, SCOPE, resident(), holder(1)),
            Err(StubRejection::Unknown),
            "in-flight op fails closed after eviction"
        );
        table.unpin(ID);
    }

    #[test]
    fn mint_refuses_a_taken_id_without_touching_the_victim() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        assert_eq!(
            table.mint(
                ID.to_string(),
                OTHER_SCOPE.to_string(),
                EPOCH + 1,
                GENERATION,
                StubKind::Writable,
            ),
            Err(MintRejection::Occupied)
        );
        // The victim entry still routes to its own scope and epoch.
        assert_eq!(
            table.revalidate(ID, OTHER_SCOPE, resident(), holder(1)),
            Err(StubRejection::ScopeMismatch)
        );
        table
            .revalidate(ID, SCOPE, resident(), holder(1))
            .expect("victim entry intact");
    }

    #[test]
    fn mint_fails_closed_at_either_cap_and_release_frees_budget() {
        let mut table = table();
        for index in 0..HOST_STUB_MAX_PER_SCOPE {
            table
                .mint(
                    format!("{index:032x}"),
                    SCOPE.to_string(),
                    EPOCH,
                    GENERATION,
                    StubKind::RpcTarget,
                )
                .expect("mint under the scope cap");
        }
        assert_eq!(
            table.mint(
                format!("{:032x}", HOST_STUB_MAX_PER_SCOPE),
                SCOPE.to_string(),
                EPOCH,
                GENERATION,
                StubKind::RpcTarget,
            ),
            Err(MintRejection::ScopeCap),
            "scope cap fails closed"
        );
        // Another scope still mints: the cap is per scope, not global.
        table
            .mint(
                format!("{:032x}", HOST_STUB_MAX_PER_SCOPE),
                OTHER_SCOPE.to_string(),
                EPOCH,
                GENERATION,
                StubKind::RpcTarget,
            )
            .expect("other scope unaffected");
        // A release frees budget: drop the only hold, drive, take.
        let freed = format!("{:032x}", 0);
        assert!(table.acquire(&freed, holder(1)));
        assert!(matches!(
            table.release_hold(&freed, holder(1)),
            ReleaseAction::Route { .. }
        ));
        assert_eq!(table.take_for_route(&freed), TakeAction::Removed);
        table
            .mint(
                format!("{:032x}", HOST_STUB_MAX_PER_SCOPE + 1),
                SCOPE.to_string(),
                EPOCH,
                GENERATION,
                StubKind::RpcTarget,
            )
            .expect("released budget mints again");
        assert_eq!(
            table.count_by_scope().get(SCOPE).copied().unwrap_or(0),
            HOST_STUB_MAX_PER_SCOPE
        );
    }

    #[test]
    fn mint_fails_closed_at_the_process_cap() {
        let mut table = table();
        // Fill the process cap across many scopes so the scope cap never
        // binds first.
        let scopes = HOST_STUB_MAX_PER_PROCESS / HOST_STUB_MAX_PER_SCOPE;
        for scope_index in 0..scopes {
            for index in 0..HOST_STUB_MAX_PER_SCOPE {
                let id = format!("{scope_index:016x}{index:016x}");
                table
                    .mint(
                        id,
                        format!("Counter:s{scope_index}"),
                        EPOCH,
                        GENERATION,
                        StubKind::RpcTarget,
                    )
                    .expect("mint under the process cap");
            }
        }
        assert_eq!(table.len(), HOST_STUB_MAX_PER_PROCESS);
        assert_eq!(
            table.mint(
                "ffffffffffffffffffffffffffffffff".to_string(),
                "Counter:over".to_string(),
                EPOCH,
                GENERATION,
                StubKind::RpcTarget,
            ),
            Err(MintRejection::ProcessCap),
            "process cap fails closed"
        );
    }

    #[test]
    fn retirement_releases_only_the_freed_heaps_holds() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        let other = "ffffffffffffffffffffffffffffffff";
        minted_held(&mut table, other, holder(1));
        assert!(table.acquire(other, holder(2)), "second holder");
        let orphaned = table.release_holder(holder(1));
        assert_eq!(orphaned.len(), 1, "only the solely-held entry orphans");
        assert_eq!(orphaned[0].id, ID);
        assert_eq!(orphaned[0].scope, SCOPE);
        assert_eq!(orphaned[0].epoch, EPOCH);
        // The shared entry stays for its live holder.
        table
            .revalidate(other, SCOPE, resident(), holder(2))
            .expect("live holder survives the backstop");
        assert_eq!(
            table.revalidate(ID, SCOPE, resident(), holder(2)),
            Err(StubRejection::Forbidden),
            "retired hold grants nothing further"
        );
        // The orphan's routed dispose takes it.
        assert_eq!(table.take_for_route(ID), TakeAction::Removed);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn retirement_skips_in_flight_pins_until_the_last_unpin() {
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        table
            .validate(ID, SCOPE, resident(), holder(1))
            .expect("op validates and pins");
        let orphaned = table.release_holder(holder(1));
        assert_eq!(orphaned.len(), 1);
        assert_eq!(
            table.take_for_route(ID),
            TakeAction::Pinned,
            "backstop take defers to the pin"
        );
        table.unpin(ID);
        assert!(table.is_empty());
    }

    #[test]
    fn churn_without_release_keeps_every_entry_until_released() {
        // There is no idle expiry: an entry lives until its holders
        // release it, its owner unexports it, or its holder retires.
        // Churn that drops JS references without disposing is freed by
        // invocation end (which releases every hold), not by a timer.
        let mut table = table();
        for index in 0..1_000 {
            table
                .mint(
                    format!("{index:032x}"),
                    SCOPE.to_string(),
                    EPOCH,
                    GENERATION,
                    StubKind::RpcTarget,
                )
                .expect("mint under the caps");
        }
        assert_eq!(table.len(), 1_000);
        for index in 0..1_000 {
            let id = format!("{index:032x}");
            assert!(table.acquire(&id, holder(1)));
            assert!(matches!(
                table.release_hold(&id, holder(1)),
                ReleaseAction::Route { .. }
            ));
            assert_eq!(table.take_for_route(&id), TakeAction::Removed);
        }
        assert!(table.is_empty());
    }

    #[test]
    fn stub_path_budget_rejects_deep_and_wide_paths() {
        assert!(valid_stub_path(&[]));
        assert!(valid_stub_path(&["invoke".to_string()]));
        assert!(!valid_stub_path(&["".to_string()]));
        assert!(!valid_stub_path(
            &["x".repeat(STUB_PATH_PART_MAX_BYTES + 1)]
        ));
        assert!(valid_stub_path(&["x".repeat(STUB_PATH_PART_MAX_BYTES)]));
        let deep = vec!["p".to_string(); STUB_PATH_MAX_DEPTH + 1];
        assert!(!valid_stub_path(&deep));
        let wide = vec!["p".to_string(); STUB_PATH_MAX_DEPTH];
        assert!(valid_stub_path(&wide));
    }

    #[tokio::test]
    async fn requeue_completes_a_release_whose_first_drives_fail() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // The last hold drops while every inline drive fails: the entry
        // is retained, still counted against the caps.
        let mut table = table();
        minted_held(&mut table, ID, holder(1));
        assert!(matches!(
            table.release_hold(ID, holder(1)),
            ReleaseAction::Route { .. }
        ));
        assert_eq!(table.len(), 1);
        // The requeue paces its rounds (one wait per drive, no busy
        // loop) and completes once a drive settles.
        let drives = AtomicUsize::new(0);
        let waits = AtomicUsize::new(0);
        let settled = super::requeue_release_drive(
            5,
            || {
                let round = drives.fetch_add(1, Ordering::SeqCst);
                async move { round >= 2 }
            },
            || async {
                waits.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await;
        assert!(settled, "transient failures still complete");
        assert_eq!(drives.load(Ordering::SeqCst), 3);
        assert_eq!(waits.load(Ordering::SeqCst), 3);
        // The completed release takes the entry and frees cap capacity:
        // the id mints again.
        assert_eq!(table.take_for_route(ID), TakeAction::Removed);
        mint(&mut table, ID);
    }

    #[tokio::test]
    async fn requeue_gives_up_after_bounded_rounds() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // A drive that never settles runs exactly its rounds, then
        // reports exhaustion so the caller frees the entry anyway.
        let drives = AtomicUsize::new(0);
        let waits = AtomicUsize::new(0);
        let settled = super::requeue_release_drive(
            4,
            || {
                drives.fetch_add(1, Ordering::SeqCst);
                async { false }
            },
            || async {
                waits.fetch_add(1, Ordering::SeqCst);
            },
        )
        .await;
        assert!(!settled, "sustained failure exhausts the rounds");
        assert_eq!(drives.load(Ordering::SeqCst), 4);
        assert_eq!(waits.load(Ordering::SeqCst), 4);
    }
}
