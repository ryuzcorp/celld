// Modified by ryuzcorp (ryuzcorp/celld): MergeTree-style lazy handoff policy (0078).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! MergeTree-style handoff policy: skip the eviction snapshot for a short
//! chain born in this epoch.
//!
//! Every eviction today captures and PUTs a full-DB L9 handoff snapshot, then
//! trims the journaled L0s it supersedes: an eager full merge on every
//! eviction. In bucket posture without group commit every write is already a
//! per-cell L0 under `cells/<cell>/ltx/e<epoch>/`, so a cell born in this
//! epoch has a complete restorable chain from TXID 1 and the snapshot is pure
//! overhead (measured: 200 of the 1934 Class A requests on the s3bench
//! agent-200 shape are eviction snapshot PUTs). ClickHouse MergeTree, the
//! model here, writes small parts on insert and merges them lazily by count
//! and size policy. It never merges just because an event happened.
//!
//! The decision is pure; the caller (`ltx_repl::evict`) supplies the facts by
//! I/O and calls [`handoff_snapshot_needed`]. A skip declines the snapshot
//! exactly like the over-budget/paged paths (compaction pause held, run lock
//! taken) and ends in the existing L0-chain fallback (`sync_wait` Durable,
//! then `epoch_replicated`), so no new proof is introduced and refusal
//! behaviour is unchanged. No trim runs without a published snapshot
//! (`trim_max` stays `None`): the L0s are the data. Snapshot behaviour is
//! byte-identical whenever the policy says a snapshot is needed.
//!
//! The skip applies iff ALL of:
//! 1. **Born in this epoch, journaled here.** The restorable chain starts at
//!    TXID 1 in THIS epoch's per-cell prefix, and every object of it is in
//!    this handle's upload journal. A successor's restore then lists exactly
//!    the epochs it would list with a snapshot (one). A cell carrying older
//!    history (remote restore, paged continuation, resumed same-epoch prefix
//!    with pre-resume uploads, uncertified local rows) keeps the snapshot:
//!    each extra epoch costs extra LISTs on every future restore.
//! 2. **Per-cell covered.** The per-cell prefix covers every acked row
//!    (`percell_txid >= durable_txid`). Bundle-credited (group commit) or
//!    fleet-credited rows not yet per-cell keep the snapshot, because the
//!    fallback would drain them as extra PUTs.
//! 3. **Bounded read amplification.** This epoch's upload journal holds at
//!    most [`HANDOFF_CHAIN_MAX_OBJECTS`] entries, and the journal never hit
//!    its cap. A successor restore is then at most 16 parallel GETs, two
//!    rounds of the 10 download slots. An overflowed or longer journal keeps
//!    the snapshot.

/// Maximum per-cell objects in this epoch's journal for a snapshot skip: a
/// successor restore then replays at most this many L0s.
pub const HANDOFF_CHAIN_MAX_OBJECTS: usize = 16;

/// Facts about one residency's restorable chain at eviction time. The caller
/// gathers these from the handle; see each field for its source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HandoffChain {
    /// This epoch's chain starts at TXID 1 in this epoch's per-cell prefix
    /// and every object of it is in this handle's upload journal (fresh
    /// activation, or `.evicted`/`.hibernated` reuse which opens a new
    /// writer generation at TXID 1). False once older history is involved:
    /// a remote restore or paged continuation (txids continue an older
    /// epoch), a same-epoch resume (pre-resume uploads are unjournaled), or
    /// uncertified local rows clamped from a leftover database.
    pub chain_from_txid1_journaled: bool,
    /// Highest TXID the per-cell prefix provably covers through this handle
    /// (restored position at open, advanced only by per-cell uploads, plus
    /// the compaction drain's L1 fold).
    pub percell_txid: u64,
    /// Highest TXID credited by any path: per-cell uploads, bundle flushes,
    /// and fleet credits all move it.
    pub durable_txid: u64,
    /// Entries in this epoch's per-cell upload journal at decision time.
    pub journal_len: usize,
    /// Whether the journal ever filled and dropped an entry: the chain is
    /// then longer than `journal_len` says.
    pub journal_overflowed: bool,
}

/// Whether eviction must publish the handoff snapshot for this chain. False
/// only when the L0 chain alone is already the complete, bounded restore
/// source: born in this epoch and journaled here, per-cell covered, and
/// short with no overflow. Every uncertain fact keeps the snapshot: a leak
/// of one PUT costs less than a successor restore that lists extra epochs
/// or replays an unbounded chain.
pub fn handoff_snapshot_needed(chain: &HandoffChain) -> bool {
    if !chain.chain_from_txid1_journaled {
        return true;
    }
    if chain.percell_txid < chain.durable_txid {
        return true;
    }
    if chain.journal_overflowed {
        return true;
    }
    if chain.journal_len > HANDOFF_CHAIN_MAX_OBJECTS {
        return true;
    }
    false
}
