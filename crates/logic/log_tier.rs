// Modified by ryuzcorp (ryuzcorp/celld): per-round fence credit (0062).
// Modified by ryuzcorp (ryuzcorp/celld): ported from meln1k/cello: seal a dead lease that never opened a log (01522b3); lone-node repair backoff schedule; bucket-posture group commit: bucket session flavour, loss/witness predicates (0059).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Decision core for the in-fleet replicated log tier (design stage; not
//! yet wired into the engine).
//!
//! One write-ahead log per node, streamed to a small follower ensemble. A
//! durable write acknowledges when every member holds it — write-all,
//! ack-all — and the bucket record `log/<node>.json` is the CAS-guarded
//! root of truth for membership, epoch, and the tiered-through offset.
//! Nothing here performs I/O: the executor feeds these decisions facts and
//! performs the returned intents.
//!
//! The safety rules, each load-bearing:
//! - a follower's (ensemble, epoch) view changes only with the record,
//!   never on the leader's stream alone;
//! - a follower refuses appends at or below its persisted seal mark;
//! - reconfiguration force-tiers the open fragment through the new
//!   fragment base before the CAS, so an empty joiner is covered entirely
//!   by the bucket;
//! - recovery seals before it certifies, and certifies from a sealed
//!   member only;
//! - the record is created before the first fleet-durable ack and never
//!   deleted, so record absence proves the bucket is complete.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::CellId;
use crate::Epoch;
use crate::NodeId;

pub type Offset = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogState {
    Open,
    Recovering,
    Sealed,
}

/// Which durability proof a session's acknowledgements ride. Readers branch
/// recovery on it: a bucket session never declares fleet-style bounded loss and
/// never demands follower witnesses (see [`may_declare_bounded_loss`] and
/// [`requires_fleet_witness`]). There is deliberately no `Default`: every
/// constructor names the kind it creates, so a new session flavour cannot
/// silently inherit the wrong recovery rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionKind {
    /// Fleet posture: frames stream to a follower ensemble, and a durable ack
    /// means every member holds the frame.
    Fleet,
    /// Bucket posture: rows ride shared bundle PUTs proven by the
    /// bucket-session record. There is no follower ensemble and never was;
    /// recovery drains the bundle prefix and nothing else.
    Bucket,
}

/// `log/<node>.json`. Every mutation is a conditional write; the executor
/// must treat a lost CAS as anothers' turn, never retry blindly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogRecord {
    pub epoch: Epoch,
    pub ensemble: BTreeSet<NodeId>,
    /// Offsets at or below this are durable in the bucket.
    pub tiered: Offset,
    pub state: LogState,
    pub kind: SessionKind,
    /// The node recovering this log while `state` is `Recovering`, and the
    /// last instant it said so. A recovery reads every retained bundle of
    /// the dead session, which takes minutes on a loaded fleet, and every
    /// other node used to take the claim over after a fixed wait and repeat
    /// the same gather: on 2026-09-03 three peers and the restarting node
    /// fetched the same 209 bundles at once. A claimant that keeps its
    /// heartbeat fresh is left alone; only a stale one is taken over.
    pub claimant: Option<NodeId>,
    pub claimed_ms: Option<u64>,
}

/// Created before the first fleet-durable ack. The open fragment begins at
/// the bucket's current end, so every frame a member will ever be asked
/// about is above what the bucket already holds.
pub fn create_record(ensemble: BTreeSet<NodeId>, bucket_end: Offset) -> Option<LogRecord> {
    if ensemble.is_empty() {
        return None;
    }
    Some(LogRecord {
        epoch: 1,
        ensemble,
        tiered: bucket_end,
        state: LogState::Open,
        kind: SessionKind::Fleet,
        claimant: None,
        claimed_ms: None,
    })
}

/// Created before the first bundle-credited ack of a bucket-posture node: a
/// bucket node with format 3 writes this record first, so "no record" keeps
/// proving the bucket complete (invariant (a)) while the format gate
/// ([`crate::format::bucket_bundles_allowed`]) keeps any reader that could
/// misread absence from ever being live. The ensemble is empty and stays
/// empty: a bucket session never recruits followers, and the maintenance loop
/// must never reconfigure it into an ensemble — no silent bucket-to-fleet
/// conversion. `tiered` is 0: a bucket session has no log fragment, so there
/// is no fragment base to anchor; per-cell watermarks bound its rows, and
/// recovery folds the whole bundle prefix regardless of this floor.
pub fn create_bucket_record() -> LogRecord {
    LogRecord {
        epoch: 1,
        ensemble: BTreeSet::new(),
        tiered: 0,
        state: LogState::Open,
        kind: SessionKind::Bucket,
        claimant: None,
        claimed_ms: None,
    }
}

/// One follower's copy of one node log: the open fragment `(base, end]` at
/// its fragment epoch, plus the persisted seal mark.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FollowerLog {
    pub fragment_epoch: Epoch,
    pub base: Offset,
    pub end: Offset,
    /// Highest epoch sealed. Appends at or below it are refused forever.
    pub sealed_to: Epoch,
}

impl FollowerLog {
    /// An empty joiner: covered by the bucket through `base`, holding
    /// nothing.
    pub fn join(fragment_epoch: Epoch, base: Offset) -> Self {
        FollowerLog {
            fragment_epoch,
            base,
            end: base,
            sealed_to: 0,
        }
    }

    /// Accept one frame. Refuses the wrong fragment epoch, anything at or
    /// below the seal mark, and gaps — a fragment is contiguous or it is
    /// not a fragment.
    pub fn accept_append(&mut self, epoch: Epoch, offset: Offset) -> bool {
        if epoch != self.fragment_epoch || epoch <= self.sealed_to || offset != self.end + 1 {
            return false;
        }
        self.end = offset;
        true
    }

    /// Persist a seal through `epoch` and report the final contiguous end.
    /// The fragment is frozen from here; write-all makes this one answer
    /// sufficient to certify every acknowledged frame.
    pub fn seal(&mut self, epoch: Epoch) -> Offset {
        if epoch > self.sealed_to {
            self.sealed_to = epoch;
        }
        self.end
    }

    /// Drop frames the record already proves tiered.
    pub fn truncate(&mut self, tiered: Offset) {
        if tiered > self.base {
            self.base = tiered;
        }
    }
}

/// The leader's believed (epoch, ensemble). Deliberately a belief: a fenced
/// leader still acts on it, and safety must come from seal marks and lost
/// CASes, not from wishful record reads on the ack path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LeaderView {
    pub epoch: Epoch,
    pub ensemble: BTreeSet<NodeId>,
}

/// Write-all, ack-all: `next` may acknowledge only when every ensemble
/// member has confirmed it at the leader's fragment epoch. `ends` carries
/// each member's confirmed contiguous end; a member missing from it has
/// confirmed nothing.
pub fn ack_fleet_allowed(view: &LeaderView, ends: &BTreeMap<NodeId, Offset>, next: Offset) -> bool {
    !view.ensemble.is_empty()
        && view
            .ensemble
            .iter()
            .all(|member| ends.get(member).is_some_and(|end| *end >= next))
}

/// A reconfiguration the CAS may publish. `tier_through` must be durable in
/// the bucket before the CAS lands: the new members join empty, and the
/// bucket is the only thing covering their emptiness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconfigPlan {
    pub tier_through: Offset,
    pub record: LogRecord,
}

pub fn plan_reconfigure(
    current: &LogRecord,
    log_end: Offset,
    ensemble: BTreeSet<NodeId>,
) -> Option<ReconfigPlan> {
    if current.state != LogState::Open || ensemble.is_empty() || ensemble == current.ensemble {
        return None;
    }
    Some(ReconfigPlan {
        tier_through: log_end,
        record: LogRecord {
            epoch: current.epoch + 1,
            ensemble,
            tiered: log_end,
            state: LogState::Open,
            // Carried through, never converted: a reconfiguration must not
            // silently turn a bucket session into a fleet one (or back). The
            // maintenance loop must never offer a bucket session here at all;
            // the carry keeps the flavour visible if it ever is.
            kind: current.kind,
            claimant: None,
            claimed_ms: None,
        },
    })
}

/// Recovery step one: fence via the record. S3 is the root of truth, so
/// this CAS is what makes every later tiering or reconfiguration attempt by
/// the old leader fail. Sealing followers comes after, never before.
pub fn start_recovery(record: &LogRecord, claimant: &str, now_ms: u64) -> Option<LogRecord> {
    if record.state != LogState::Open {
        return None;
    }
    Some(LogRecord {
        state: LogState::Recovering,
        claimant: Some(claimant.to_string()),
        claimed_ms: Some(now_ms),
        ..record.clone()
    })
}

/// Whether `claimant` owns the recovery in progress.
pub fn recovery_claimed_by(record: &LogRecord, claimant: &str) -> bool {
    record.state == LogState::Recovering && record.claimant.as_deref() == Some(claimant)
}

/// Whether another node's claim is fresh: its last heartbeat is younger
/// than `stale_after_ms`. A record from before claims carried a heartbeat
/// reads as stale, so an upgrade never waits on a claim it cannot judge.
pub fn recovery_claim_live(record: &LogRecord, now_ms: u64, stale_after_ms: u64) -> bool {
    record.state == LogState::Recovering
        && record.claimant.is_some()
        && record
            .claimed_ms
            .is_some_and(|claimed| now_ms.saturating_sub(claimed) < stale_after_ms)
}

/// The claimant's heartbeat. `None` when the claim is no longer this
/// node's: the recovery was taken over and this node must stop.
pub fn refresh_recovery(record: &LogRecord, claimant: &str, now_ms: u64) -> Option<LogRecord> {
    if !recovery_claimed_by(record, claimant) {
        return None;
    }
    Some(LogRecord {
        claimed_ms: Some(now_ms),
        ..record.clone()
    })
}

/// Take a recovery over from a claimant whose heartbeat went stale. `None`
/// while the claim is live or the log is not recovering.
pub fn take_over_recovery(
    record: &LogRecord,
    claimant: &str,
    now_ms: u64,
    stale_after_ms: u64,
) -> Option<LogRecord> {
    if record.state != LogState::Recovering || recovery_claim_live(record, now_ms, stale_after_ms) {
        return None;
    }
    Some(LogRecord {
        claimant: Some(claimant.to_string()),
        claimed_ms: Some(now_ms),
        ..record.clone()
    })
}

/// The certified cut: the final contiguous end of any sealed member, floored
/// at what is already tiered. Any one sealed member suffices for acked
/// frames (write-all); taking a longer end preserves acked-but-unconfirmed
/// frames, which is safe — delivering an unacknowledged write is allowed,
/// losing an acknowledged one is not.
pub fn certify(tiered: Offset, sealed_end: Offset) -> Offset {
    tiered.max(sealed_end)
}

/// Recovery final step: the certified tail is durable in the bucket, so the
/// record seals at the cut and every per-cell takeover may treat the bucket
/// as complete.
pub fn finish_recovery(record: &LogRecord, cert: Offset) -> Option<LogRecord> {
    if record.state != LogState::Recovering {
        return None;
    }
    Some(LogRecord {
        tiered: cert,
        state: LogState::Sealed,
        claimant: None,
        claimed_ms: None,
        ..record.clone()
    })
}

/// May a per-cell takeover of this node's cells treat the bucket as
/// complete? Absence is a proof, not a guess: the record is created before
/// the first fleet-durable ack — and, for bucket-posture group commit, before
/// the first bundle-credited ack — and never deleted, so no record means the
/// node never acknowledged anything the bucket does not hold. It stays a
/// proof only while no record can appear, so a takeover first seals an
/// expired lease that carries none ([`sealed_absent`]).
///
/// A bucket session is never `BucketComplete` while its record exists:
/// acked rows may live only in `log/<node>/bundle/`, so every live bucket
/// record (`SessionKind::Bucket`) recovers first, and only a sealed one
/// proves the bucket complete. The absence leg of invariant (a) is upheld
/// outside this function, by record-before-first-ack plus the format gate
/// ([`crate::format::bucket_bundles_allowed`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TakeoverGate {
    BucketComplete,
    RecoverFirst,
}

/// The record a takeover writes into a dead lease that carries no log. The
/// CAS moves the lease's etag, so a session that still believes it holds
/// the lease cannot open a log through a renewal: the renewal is rejected,
/// its readback finds a record it never wrote, and the session fences.
pub fn sealed_absent() -> LogRecord {
    LogRecord {
        epoch: 0,
        ensemble: BTreeSet::new(),
        tiered: 0,
        state: LogState::Sealed,
        // Fleet-shaped by construction: this seals a lease that carries no
        // log at all, so there is no bucket session to name.
        kind: SessionKind::Fleet,
        claimant: None,
        claimed_ms: None,
    }
}

pub fn takeover_gate(record: Option<&LogRecord>) -> TakeoverGate {
    match record {
        None => TakeoverGate::BucketComplete,
        Some(record) if record.state == LogState::Sealed => TakeoverGate::BucketComplete,
        Some(_) => TakeoverGate::RecoverFirst,
    }
}

/// May recovery of this session declare fleet-style bounded loss? This is the
/// static part of the `recover_serial` witness verdict, lifted into a pure
/// predicate so layer 3 calls it instead of inlining a second copy: with zero
/// complete witnesses, a fleet session whose epoch may hold fleet acks and whose
/// ensemble could still hide them must declare the loss as a record before it
/// seals. A bucket session must never: its acks rode bundle PUTs, there are no
/// follower fragments to be missing, and the empty-ensemble conjunct keeps even
/// a corrupted ack-history marker from declaring. The drain either finds the rows
/// in the bundle prefix or the session never acked them.
pub fn may_declare_bounded_loss(record: &LogRecord, may_have_fleet_acks: bool) -> bool {
    record.kind == SessionKind::Fleet && may_have_fleet_acks && !record.ensemble.is_empty()
}

/// Must a single-cell fold of this session see a complete follower tail before
/// restoring? This is the static part of the `fold_cell` witness demand: an epoch
/// with possible fleet acknowledgements needs a ranged tail proving a current
/// member returned every retained sequence. A bucket session never demands one:
/// the bundle prefix is the only witness it ever had, so the fold proceeds on
/// bundle coverage alone.
pub fn requires_fleet_witness(record: &LogRecord, may_have_fleet_acks: bool) -> bool {
    record.kind == SessionKind::Fleet && may_have_fleet_acks
}

/// What the maintenance loop must do with its OWN session's record before
/// it may open a fleet ensemble. The record is keyed by (node,
/// generation), so the only record this session can ever read here is one
/// it wrote itself: absent on the session's first open, present for a
/// within-session reconfiguration. A predecessor session's record is a
/// dead session's record like any other, recovered by the sweep or the
/// takeover interlock, never stepped past here — which is what deleted the
/// RecoverFirst arm, the revive/reopen guard, and the recovery epoch pin.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MaintainStep {
    /// No record yet: create one before the first fleet-durable ack.
    CreateFresh,
    /// Our open (or, after a failed close, sealed) record: step the epoch
    /// and reopen with a fresh ensemble.
    Reopen(Epoch),
    /// Our own record is mid-recovery: a rival is fencing this session,
    /// which can only mean our lease looks expired from where it stands.
    /// Reopening would race its seal CAS — both outcomes are safe under
    /// write-all plus the seal marks, but the churn buys nothing. Hold
    /// this tick; the next pass finds Sealed and reopens, or our own
    /// lease loss fences us first.
    Wait,
}

pub fn maintain_step(record: Option<&LogRecord>) -> MaintainStep {
    match record {
        None => MaintainStep::CreateFresh,
        Some(record) if record.state == LogState::Recovering => MaintainStep::Wait,
        Some(record) => MaintainStep::Reopen(record.epoch + 1),
    }
}

/// The graceful shutdown's seal guard — the only seal producer besides
/// recovery. The record must still be Open at the closing shipper's
/// epoch, no batch may sit between capture and credit, and every shipped
/// frame must live in the PER-CELL layout: a sealed record tells every
/// future recovery there is nothing to gather, so credited bundle
/// coverage must never satisfy this guard — the bundle-resident acked
/// tail would be orphaned behind the seal. Refusal is always safe: an
/// Open record just means the next incarnation's recovery drains the
/// bundles.
pub fn graceful_seal_allowed(
    record: &LogRecord,
    epoch: Epoch,
    batch_in_flight: bool,
    per_cell_complete: bool,
) -> bool {
    record.state == LogState::Open && record.epoch == epoch && !batch_in_flight && per_cell_complete
}

/// A bundle flush may credit durability only if the record is still Open
/// at the shipper's epoch AFTER the PUT. The PUT itself is unconditional
/// and proves nothing: a healed zombie whose record a recoverer already
/// sealed can still land bundle objects, and crediting them would ack
/// rows no future takeover reads — the sealed record says the bucket is
/// complete, and takeovers read per-cell prefixes.
pub fn bundle_credit_allowed(record: Option<&LogRecord>, epoch: Epoch) -> bool {
    record.is_some_and(|record| record.epoch == epoch && record.state == LogState::Open)
}
/// Proof-then-read ordering witness for one flush round's fence check. The
/// executor mints both markers from one monotonic completion counter: the
/// bundle PUT's completion side and the fence-entries read's issue side. A
/// fence read credits only when issued strictly after the proof it follows.
/// `read_seq <= proof_seq` names a read that can predate the proof — or the
/// takeover the proof raced — so it proves nothing and never credits.
// Modified by ryuzcorp (ryuzcorp/celld): per-round fence credit (0062).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProofThenRead {
    /// Completion marker of the round's bundle PUT (the proof).
    pub proof_seq: u64,
    /// Issue marker of the round's fence-entries read.
    pub read_seq: u64,
}

impl ProofThenRead {
    /// True only when the fence read was issued after the proof completed.
    pub fn ordered(self) -> bool {
        self.read_seq > self.proof_seq
    }
}

/// What one fence-entry read returned for one cell of a flush round. The
/// entry is the cell's ownership record (`cells/<cell>/own.json` today, one
/// shard object per shard once ownership shards land); the shape is the same
/// either way, so the credit path does not change when the backend does.
// Modified by ryuzcorp (ryuzcorp/celld): per-round fence credit (0062).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoundFenceOutcome {
    /// The entry exists and names this owner at this cell epoch.
    Entry { node: NodeId, epoch: Epoch },
    /// The entry is gone: the cell was released, or was never claimed here.
    /// Either way the prefix may be orphaned, so this fails, never credits.
    Missing,
    /// The entry could not be read (refused GET, timeout, torn body). A read
    /// error fails exactly like a refused entry: the round is uncredited.
    ReadError,
}

/// The reader abstraction for a flush round's fence entries: one outcome per
/// cell the round's bundle carries. Layer 2 backs this with per-cell
/// `own.json` reads issued after the bundle PUT; ownership shards back it
/// with one shard read per shard afterwards. The predicate only sees
/// outcomes, so swapping the backend touches no credit logic.
// Modified by ryuzcorp (ryuzcorp/celld): per-round fence credit (0062).
pub trait RoundFence {
    /// The fence outcome for `cell`: its entry, or why there is none.
    fn entry(&self, cell: &str) -> RoundFenceOutcome;
}

/// Map-backed fences for tests and callers that already hold the round's
/// entries. A cell with no entry reads as `Missing`, like a 404 from the
/// bucket: absent is a verdict, not a gap.
// Modified by ryuzcorp (ryuzcorp/celld): per-round fence credit (0062).
impl RoundFence for BTreeMap<CellId, RoundFenceOutcome> {
    fn entry(&self, cell: &str) -> RoundFenceOutcome {
        self.get(cell)
            .cloned()
            .unwrap_or(RoundFenceOutcome::Missing)
    }
}

/// Per-cell credit verdict for one flush round. `Credit` acknowledges the
/// cell's covered writes; `Uncredited` fails their gates exactly like a
/// failed ownership read today — never a blind ack, never a hang.
// Modified by ryuzcorp (ryuzcorp/celld): per-round fence credit (0062).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellCredit {
    Credit,
    Uncredited,
}

impl CellCredit {
    /// True only for `Credit`.
    pub fn credited(self) -> bool {
        matches!(self, CellCredit::Credit)
    }
}

/// Per-cell arm of the round credit check: the fence entry read AFTER the
/// round's bundle PUT must still name this node at this cell epoch. Pair
/// with the session-record arm ([`bundle_credit_allowed`]); see
/// [`round_cell_credit`].
// Modified by ryuzcorp (ryuzcorp/celld): per-round fence credit (0062).
pub fn fence_cell_credit(
    node: &str,
    cell_epoch: Epoch,
    fence: RoundFenceOutcome,
    order: ProofThenRead,
) -> CellCredit {
    if !order.ordered() {
        return CellCredit::Uncredited;
    }
    match fence {
        RoundFenceOutcome::Entry { node: owner, epoch } if owner == node && epoch == cell_epoch => {
            CellCredit::Credit
        }
        _ => CellCredit::Uncredited,
    }
}

/// A flush round's credit predicate, both arms. The session-record arm
/// ([`bundle_credit_allowed`]) covers the whole round: a record no longer
/// `Open` at the shipper's epoch after the PUT means a recovery sealed the
/// session mid-flight, and no cell of the round credits. The per-cell fence
/// arm ([`fence_cell_credit`]) covers each cell: a live-donor single-cell
/// takeover never seals the session (`fold_cell` leaves it `Open`), so the
/// record arm still passes for the stale writer and only the fence entry —
/// read after the PUT — refuses its round. Either arm refuses, and a refused
/// cell's gates fail rather than hang. Proof-then-read ordering is part of
/// the verdict: a fence read issued at or before the proof never credits.
// Modified by ryuzcorp (ryuzcorp/celld): per-round fence credit (0062).
pub fn round_cell_credit(
    record: Option<&LogRecord>,
    shipper_epoch: Epoch,
    node: &str,
    cell_epoch: Epoch,
    fence: RoundFenceOutcome,
    order: ProofThenRead,
) -> CellCredit {
    if !bundle_credit_allowed(record, shipper_epoch) {
        return CellCredit::Uncredited;
    }
    fence_cell_credit(node, cell_epoch, fence, order)
}

/// Credit for every cell of a flush round, in input order, through one
/// fence reader. Mixed rounds stay mixed: a bumped cell goes uncredited
/// while its intact neighbours credit, and the executor fails only the
/// uncredited cells' gates.
// Modified by ryuzcorp (ryuzcorp/celld): per-round fence credit (0062).
pub fn round_credit(
    record: Option<&LogRecord>,
    shipper_epoch: Epoch,
    node: &str,
    cells: &[(CellId, Epoch)],
    fence: &impl RoundFence,
    order: ProofThenRead,
) -> Vec<(CellId, CellCredit)> {
    cells
        .iter()
        .map(|(cell, cell_epoch)| {
            (
                cell.clone(),
                round_cell_credit(
                    record,
                    shipper_epoch,
                    node,
                    *cell_epoch,
                    fence.entry(cell),
                    order,
                ),
            )
        })
        .collect()
}

/// One cell's share of a round-level fence settlement handed to the output
/// gate. The executor computes `credit` with [`round_cell_credit`] (both
/// arms, proof-then-read ordering included) and the gate settles only the
/// gates this round's proof captured — positions at or below `cover`.
// Modified by ryuzcorp (ryuzcorp/celld): per-round fence credit (0062).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FenceCellSettlement {
    /// The cell the fence entry was read for.
    pub cell: CellId,
    /// The cell epoch the round served.
    pub epoch: Epoch,
    /// Highest gate position this round's proof captured for the cell.
    /// Gates above it were committed after the proof (or never covered by
    /// it), so this settlement never answers them.
    pub cover: u64,
    /// The two-arm verdict for the cell.
    pub credit: CellCredit,
}

/// A round-level fence read handed back to the output gate: every fence
/// entry it carries was read AFTER the round's bundle proof completed, and
/// `order` witnesses that. An unordered round (read issued at or before the
/// proof) settles nothing as credited: covered gates fail instead, so a
/// read that predates the proof is never accepted.
// Modified by ryuzcorp (ryuzcorp/celld): per-round fence credit (0062).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FenceRoundSettlement {
    /// The proof-then-read ordering witness for this round.
    pub order: ProofThenRead,
    /// One settlement per cell the round's bundle carried.
    pub cells: Vec<FenceCellSettlement>,
}

/// A bundle object is deletable exactly when every row it carries is
/// covered by the per-cell layout — the watermark that keeps recovery's
/// whole-prefix gather bounded without ever deleting the only copy of an
/// acked row. `rows` pairs each row's TXID with the covered watermark of
/// its (cell, cell epoch).
pub fn bundle_deletable(rows: impl IntoIterator<Item = (u64, u64)>) -> bool {
    rows.into_iter().all(|(txid, covered)| txid <= covered)
}

/// A follower fragment is garbage exactly when its epoch is closed: the
/// record moved past it (a reconfiguration or a reopened incarnation), or
/// recovery sealed the record at it. An open current-epoch fragment is
/// the durability witness and must never be collected.
pub fn fragment_closed(record: &LogRecord, fragment_epoch: Epoch) -> bool {
    record.epoch > fragment_epoch
        || (record.epoch == fragment_epoch && record.state == LogState::Sealed)
}

/// maintain()'s reconfiguration barrier: the epoch may step only when no
/// batch sits between capture and credit (its frames are invisible to the
/// coverage counters) and every fleet-shipped frame is bucket-covered —
/// the precondition that makes the old fragments abandonable garbage and
/// the new members' emptiness sound.
pub fn may_reconfigure(batch_in_flight: bool, all_shipped_covered: bool) -> bool {
    !batch_in_flight && all_shipped_covered
}

/// The ship loop's truncation ledger: outstanding batches oldest-first,
/// and the covered watermark that rides the next append as the followers'
/// `truncate_to`. Sequences restart with the epoch, so the ledger resets
/// with it — a covered watermark surviving an epoch swap instructs fresh
/// followers to delete entries they just fsync'd. `Batch`
/// is whatever the executor needs to decide coverage; the ledger decides
/// only ordering, the watermark, and the reset.
#[derive(Debug)]
pub struct ShipLedger<Batch> {
    epoch: Epoch,
    covered_seq: Offset,
    outstanding: std::collections::VecDeque<(Offset, Batch)>,
}

impl<Batch> Default for ShipLedger<Batch> {
    fn default() -> Self {
        ShipLedger {
            epoch: 0,
            covered_seq: 0,
            outstanding: std::collections::VecDeque::new(),
        }
    }
}

impl<Batch> ShipLedger<Batch> {
    /// The stream identity check, before anything else each round: a new
    /// epoch is a new sequence space and the ledger starts over.
    pub fn observe_epoch(&mut self, epoch: Epoch) {
        if epoch != self.epoch {
            self.epoch = epoch;
            self.outstanding.clear();
            self.covered_seq = 0;
        }
    }

    /// A batch was acknowledged by every member through `last_seq`.
    pub fn shipped(&mut self, last_seq: Offset, batch: Batch) {
        self.outstanding.push_back((last_seq, batch));
    }

    /// Advance the covered watermark over every leading batch `covered`
    /// accepts. `covered_seq` is the one way to read the result — the
    /// followers' `truncate_to`.
    pub fn advance(&mut self, mut covered: impl FnMut(&Batch) -> bool) {
        while let Some((last_seq, batch)) = self.outstanding.front() {
            if covered(batch) {
                self.covered_seq = *last_seq;
                self.outstanding.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn covered_seq(&self) -> Offset {
        self.covered_seq
    }
}

/// Why a node that wants the fleet posture is not serving fleet
/// acknowledgements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shortfall {
    /// No live peer exists. An ordinary single-node fleet: the write waits
    /// for the bucket because there is nobody to replicate to.
    NoPeer,
    /// Live peers exist and none can be recruited. An operator can act on
    /// this; a single-node fleet is not a fault.
    NoEligiblePeer,
}

/// Should the node report its fleet shortfall now?
///
/// `maintain` runs on a ticker and returns as soon as it finds no member to
/// recruit, so a fleet that never formed an ensemble reported nothing at all:
/// `log ensemble degraded` fires only when an ensemble that existed was lost.
/// A single-node fleet therefore ran the default fleet posture on bucket
/// acknowledgements forever, with no line explaining the latency or naming
/// the fix. Legible failure is the point; silence is the defect.
///
/// The interval bounds the repetition, and `last_ms` of `None` reports at
/// once so the first tick after a start says what the node is doing.
pub fn fleet_shortfall(
    live_peers: usize,
    now_ms: u64,
    last_ms: Option<u64>,
    interval_ms: u64,
) -> Option<Shortfall> {
    if let Some(last) = last_ms {
        if now_ms.saturating_sub(last) < interval_ms {
            return None;
        }
    }
    Some(if live_peers == 0 {
        Shortfall::NoPeer
    } else {
        Shortfall::NoEligiblePeer
    })
}

/// The posture watch's wait before its next `maintain` pass.
///
/// A lone node never becomes healthy — no peer can be a follower — so a
/// flat 1 s retry replays the peer listing (one `nodes/` LIST plus one GET
/// per lease) every second forever, about 2.6M of each a month. While the
/// pass found no eligible peer there is nobody to recruit, so the wait
/// doubles from the 1 s base up to `lone_cap_ms` (the caller passes the
/// maintenance tick, so the lone scan never runs ahead of it). Every other
/// arm is the old policy unchanged: healthy resets to the base, a peer
/// that exists but lost to the drain barrier retries at once, and a pass
/// that stepped the epoch keeps the swap rate cap's doubling to 10 s. The
/// stepped arm wins over the no-peer arm: the maintenance tick can step the
/// epoch while the watch's discovery is pending, and a moving epoch must
/// not stretch the wait past the old cap.
pub fn repair_interval_after(
    healthy: bool,
    epoch_stepped: bool,
    no_eligible_peer: bool,
    current_ms: u64,
    lone_cap_ms: u64,
) -> u64 {
    const BASE_MS: u64 = 1_000;
    const STEPPED_CAP_MS: u64 = 10_000;
    if healthy {
        return BASE_MS;
    }
    if epoch_stepped {
        return current_ms.saturating_mul(2).min(STEPPED_CAP_MS);
    }
    if no_eligible_peer {
        return current_ms.saturating_mul(2).min(lone_cap_ms);
    }
    BASE_MS
}

#[cfg(test)]
mod repair_interval_tests {
    use super::*;

    /// A lone node's successive "no eligible peer" passes back the wait off
    /// exponentially until the maintenance tick is the bound.
    #[test]
    fn lone_node_scans_back_off_to_the_tick() {
        let mut interval = 1_000;
        let mut waits = Vec::with_capacity(6);
        for _ in 0..6 {
            interval = repair_interval_after(false, false, true, interval, 30_000);
            waits.push(interval);
        }
        assert_eq!(waits, [2_000, 4_000, 8_000, 16_000, 30_000, 30_000]);
    }

    /// A peer found (or health regained) resets to the 1 s base at once, so
    /// a node that starts next to a lone node is recruited without waiting
    /// out the backoff. The healthy arm takes the asserted pair too: the
    /// watch re-reads health after the pass, so a node that became healthy
    /// while its pass found nobody still resets, and health must dominate
    /// the backoff in any order the two observations arrive in.
    #[test]
    fn peer_found_resets_to_the_base() {
        assert_eq!(
            repair_interval_after(false, false, false, 30_000, 30_000),
            1_000
        );
        assert_eq!(
            repair_interval_after(true, false, true, 30_000, 30_000),
            1_000
        );
    }

    /// A pass that stepped the epoch keeps the old swap rate cap: doubling
    /// to 10 s, while a pass that lost to the drain barrier with a peer
    /// present still retries at once.
    #[test]
    fn stepped_keeps_its_ten_second_cap() {
        let mut interval = 1_000;
        for _ in 0..5 {
            interval = repair_interval_after(false, true, false, interval, 30_000);
        }
        assert_eq!(interval, 10_000);
        assert_eq!(
            repair_interval_after(false, false, false, 10_000, 30_000),
            1_000
        );
        assert_eq!(
            repair_interval_after(false, true, true, 30_000, 30_000),
            10_000,
            "a stepped pass keeps the 10 s cap even when it also found no peer"
        );
    }
}

#[cfg(test)]
mod bucket_session_tests {
    use super::*;

    fn fleet_record(state: LogState) -> LogRecord {
        let mut ensemble = BTreeSet::new();
        ensemble.insert(NodeId::from("n1"));
        LogRecord {
            epoch: 7,
            ensemble,
            tiered: 100,
            state,
            kind: SessionKind::Fleet,
            claimant: None,
            claimed_ms: None,
        }
    }

    fn bucket_record(state: LogState) -> LogRecord {
        let mut record = create_bucket_record();
        record.state = state;
        record
    }

    /// The constructor names the flavour: open, ensemble-less, bucket.
    #[test]
    fn bucket_record_is_created_open_and_ensemble_less() {
        let record = create_bucket_record();
        assert_eq!(record.state, LogState::Open);
        assert_eq!(record.kind, SessionKind::Bucket);
        assert!(record.ensemble.is_empty());
        assert_eq!(record.epoch, 1);
    }

    /// Invariant (a), record leg: no live record flavour is ever "complete"
    /// while bundles may exist. A bucket record recovers first unless sealed;
    /// absence stays complete only via record-before-first-ack + the format
    /// gate, which this gate cannot see.
    #[test]
    fn takeover_gate_never_completes_a_live_bucket_session() {
        assert_eq!(takeover_gate(None), TakeoverGate::BucketComplete);
        assert_eq!(
            takeover_gate(Some(&fleet_record(LogState::Open))),
            TakeoverGate::RecoverFirst
        );
        assert_eq!(
            takeover_gate(Some(&bucket_record(LogState::Open))),
            TakeoverGate::RecoverFirst,
            "an open bucket session may hold the only copy of acked rows"
        );
        assert_eq!(
            takeover_gate(Some(&bucket_record(LogState::Recovering))),
            TakeoverGate::RecoverFirst,
            "a recovering bucket session is mid-fold, not complete"
        );
        assert_eq!(
            takeover_gate(Some(&bucket_record(LogState::Sealed))),
            TakeoverGate::BucketComplete,
            "only a sealed bucket record proves the bucket complete"
        );
    }

    /// Invariant (b): credit only under an Open record at the shipper's epoch,
    /// after the PUT. A sealed record (zombie fenced mid-flight), a recovering
    /// one, a wrong epoch, or no record at all refuses the credit.
    #[test]
    fn credit_is_open_at_epoch_after_the_put() {
        let open = bucket_record(LogState::Open);
        assert!(bundle_credit_allowed(Some(&open), open.epoch));
        let fleet_open = fleet_record(LogState::Open);
        assert!(bundle_credit_allowed(Some(&fleet_open), fleet_open.epoch));

        let sealed = bucket_record(LogState::Sealed);
        assert!(
            !bundle_credit_allowed(Some(&sealed), sealed.epoch),
            "a zombie PUT that lands after the seal must never credit"
        );
        let recovering = bucket_record(LogState::Recovering);
        assert!(!bundle_credit_allowed(Some(&recovering), recovering.epoch));
        assert!(!bundle_credit_allowed(Some(&open), open.epoch + 1));
        assert!(!bundle_credit_allowed(None, open.epoch));
    }

    /// Invariant (c): Sealed implies every acked row is per-cell. The seal
    /// demands the retained-bundle scan be empty; an Open record with an
    /// undrained bundle is always safe to leave open.
    #[test]
    fn seal_demands_per_cell_complete() {
        let open = bucket_record(LogState::Open);
        assert!(graceful_seal_allowed(&open, open.epoch, false, true));
        assert!(
            !graceful_seal_allowed(&open, open.epoch, false, false),
            "sealing over an undrained bundle would orphan acked rows"
        );
        assert!(!graceful_seal_allowed(&open, open.epoch, true, false));
        assert!(!graceful_seal_allowed(&open, open.epoch + 1, false, true));
        let sealed = bucket_record(LogState::Sealed);
        assert!(!graceful_seal_allowed(&sealed, sealed.epoch, false, true));
    }

    /// Layer-3 contract: a bucket session never declares fleet-style bounded
    /// loss and never demands follower witnesses — even with a corrupted
    /// ack-history marker and a non-empty ensemble. The fleet rows pin today's
    /// behaviour unchanged, so the predicates are safe to swap in.
    #[test]
    fn bucket_sessions_never_declare_loss_or_demand_witnesses() {
        // Fleet behaviour is preserved exactly: marker + ensemble declares;
        // marker alone demands a witness.
        let fleet_active = fleet_record(LogState::Recovering);
        assert!(may_declare_bounded_loss(&fleet_active, true));
        assert!(requires_fleet_witness(&fleet_active, true));
        assert!(!may_declare_bounded_loss(&fleet_active, false));
        assert!(!requires_fleet_witness(&fleet_active, false));
        let mut bare = fleet_active.clone();
        bare.ensemble.clear();
        assert!(!may_declare_bounded_loss(&bare, true));
        // Bucket sessions: never, under any marker or ensemble.
        for state in [LogState::Open, LogState::Recovering, LogState::Sealed] {
            let bucket = bucket_record(state);
            assert!(
                !may_declare_bounded_loss(&bucket, true),
                "a bucket session must drain bundles, never declare loss ({state:?})"
            );
            assert!(
                !requires_fleet_witness(&bucket, true),
                "a bucket session folds on bundle coverage alone ({state:?})"
            );
            assert!(!may_declare_bounded_loss(&bucket, false));
            assert!(!requires_fleet_witness(&bucket, false));
        }
        let mut bucket_with_ensemble = bucket_record(LogState::Recovering);
        bucket_with_ensemble.ensemble.insert(NodeId::from("n9"));
        assert!(!may_declare_bounded_loss(&bucket_with_ensemble, true));
        assert!(!requires_fleet_witness(&bucket_with_ensemble, true));
    }
}

#[cfg(test)]
mod fence_credit_tests {
    use super::*;

    const NODE: &str = "n0";
    const SHIPPER_EPOCH: Epoch = 7;
    const CELL_EPOCH: Epoch = 3;

    fn open_record() -> LogRecord {
        let mut record = create_bucket_record();
        record.epoch = SHIPPER_EPOCH;
        record
    }

    fn sealed_record() -> LogRecord {
        let mut record = open_record();
        record.state = LogState::Sealed;
        record
    }

    fn intact_entry() -> RoundFenceOutcome {
        RoundFenceOutcome::Entry {
            node: NodeId::from(NODE),
            epoch: CELL_EPOCH,
        }
    }

    fn ordered() -> ProofThenRead {
        ProofThenRead {
            proof_seq: 11,
            read_seq: 12,
        }
    }

    fn credit(
        record: Option<&LogRecord>,
        fence: RoundFenceOutcome,
        order: ProofThenRead,
    ) -> CellCredit {
        round_cell_credit(record, SHIPPER_EPOCH, NODE, CELL_EPOCH, fence, order)
    }

    /// The credit table: both arms must pass. The session-record arm covers
    /// the round (sealed/recovering/wrong-epoch/absent refuses everything);
    /// the fence arm covers the cell (a bumped, missing, or unreadable entry
    /// refuses it); and a read issued at or before the proof never credits.
    #[test]
    fn round_credit_truth_table() {
        let open = open_record();
        // Baseline: open record, intact entry, ordered read.
        assert_eq!(
            credit(Some(&open), intact_entry(), ordered()),
            CellCredit::Credit
        );
        // Session-record arm: a seal (or recovery, or epoch move, or absence)
        // after the PUT refuses the round even with an intact entry.
        assert_eq!(
            credit(Some(&sealed_record()), intact_entry(), ordered()),
            CellCredit::Uncredited,
            "a record sealed after the PUT refuses the credit"
        );
        let mut recovering = open_record();
        recovering.state = LogState::Recovering;
        assert_eq!(
            credit(Some(&recovering), intact_entry(), ordered()),
            CellCredit::Uncredited
        );
        assert_eq!(
            credit(None, intact_entry(), ordered()),
            CellCredit::Uncredited
        );
        assert_eq!(
            round_cell_credit(
                Some(&open),
                SHIPPER_EPOCH + 1,
                NODE,
                CELL_EPOCH,
                intact_entry(),
                ordered()
            ),
            CellCredit::Uncredited,
            "a shipper-epoch move refuses the credit"
        );
        // Per-cell fence arm: with an open record, only an entry still
        // naming this node at this cell epoch credits.
        assert_eq!(
            credit(
                Some(&open),
                RoundFenceOutcome::Entry {
                    node: NodeId::from("n1"),
                    epoch: CELL_EPOCH,
                },
                ordered()
            ),
            CellCredit::Uncredited,
            "a live-donor takeover uncredits the stale writer"
        );
        assert_eq!(
            credit(
                Some(&open),
                RoundFenceOutcome::Entry {
                    node: NodeId::from(NODE),
                    epoch: CELL_EPOCH + 1,
                },
                ordered()
            ),
            CellCredit::Uncredited,
            "a superseded cell epoch uncredits the stale writer"
        );
        assert_eq!(
            credit(Some(&open), RoundFenceOutcome::Missing, ordered()),
            CellCredit::Uncredited,
            "a missing entry fails, never hangs"
        );
        assert_eq!(
            credit(Some(&open), RoundFenceOutcome::ReadError, ordered()),
            CellCredit::Uncredited,
            "a read error fails, never hangs"
        );
        // Ordering: a fence read issued at or before the proof never credits,
        // even with an open record and an intact entry.
        for order in [
            ProofThenRead {
                proof_seq: 12,
                read_seq: 12,
            },
            ProofThenRead {
                proof_seq: 13,
                read_seq: 12,
            },
        ] {
            assert!(
                !order.ordered(),
                "the witness must refuse a read that predates the proof"
            );
            assert_eq!(
                credit(Some(&open), intact_entry(), order),
                CellCredit::Uncredited,
                "a fence read issued before the proof is never accepted"
            );
        }
        assert!(ordered().ordered());
    }

    /// The live-donor hole from parity §3 scenario 1: `fold_cell` never seals
    /// the donor session, so the session-record arm still passes for the
    /// stale writer after a single-cell takeover. Only the fence entry —
    /// read after the PUT — refuses the stale round. Without the fence arm
    /// this test credits a lost acknowledged write.
    #[test]
    fn live_donor_takeover_uncredits_the_stale_writer() {
        let open = open_record();
        assert!(
            bundle_credit_allowed(Some(&open), SHIPPER_EPOCH),
            "the fold leaves the session open, so the record arm still passes"
        );
        assert_eq!(
            credit(
                Some(&open),
                RoundFenceOutcome::Entry {
                    node: NodeId::from("n1"),
                    epoch: CELL_EPOCH,
                },
                ordered()
            ),
            CellCredit::Uncredited,
            "the post-PUT entry read names the donor, not the stale writer"
        );
    }

    /// Mixed rounds stay mixed: the bumped cell goes uncredited while its
    /// intact neighbours credit, in input order, through one reader.
    #[test]
    fn round_credit_fans_out_per_cell() {
        let open = open_record();
        let cells = vec![
            (CellId::from("c0"), CELL_EPOCH),
            (CellId::from("c1"), CELL_EPOCH),
            (CellId::from("c2"), CELL_EPOCH),
        ];
        let fence: BTreeMap<CellId, RoundFenceOutcome> = BTreeMap::from([
            (CellId::from("c0"), intact_entry()),
            // c1's entry names the live donor after a single-cell takeover.
            (
                CellId::from("c1"),
                RoundFenceOutcome::Entry {
                    node: NodeId::from("n1"),
                    epoch: CELL_EPOCH,
                },
            ),
            // c2 has no entry at all: absent reads as Missing, not as a gap.
        ]);
        assert_eq!(
            fence.entry("c2"),
            RoundFenceOutcome::Missing,
            "a cell the reader never saw reads as Missing"
        );
        let verdicts = round_credit(Some(&open), SHIPPER_EPOCH, NODE, &cells, &fence, ordered());
        assert_eq!(
            verdicts,
            vec![
                (CellId::from("c0"), CellCredit::Credit),
                (CellId::from("c1"), CellCredit::Uncredited),
                (CellId::from("c2"), CellCredit::Uncredited),
            ]
        );
        assert!(verdicts[0].1.credited());
        assert!(!verdicts[1].1.credited());
    }
}
