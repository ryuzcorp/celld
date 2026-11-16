// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Unit tests for the lazy handoff policy: [`celld_logic::handoff`].
//!
//! Each policy condition forces the snapshot on its own; only a chain that is
//! born in this epoch and journaled here, per-cell covered, short, and never
//! overflowed skips it. The boundary (16 skips, 17 snapshots) is pinned so a
//! later constant change re-examines the restore-amplification budget.

use celld_logic::handoff::{HANDOFF_CHAIN_MAX_OBJECTS, HandoffChain, handoff_snapshot_needed};

fn fresh_short_chain() -> HandoffChain {
    HandoffChain {
        chain_from_txid1_journaled: true,
        percell_txid: 3,
        durable_txid: 3,
        journal_len: 3,
        journal_overflowed: false,
    }
}

#[test]
fn fresh_short_covered_chain_skips_the_snapshot() {
    assert!(
        !handoff_snapshot_needed(&fresh_short_chain()),
        "a short chain born in this epoch needs no snapshot"
    );
}

#[test]
fn empty_fresh_chain_skips_the_snapshot() {
    let chain = HandoffChain {
        percell_txid: 0,
        durable_txid: 0,
        journal_len: 0,
        ..fresh_short_chain()
    };
    assert!(
        !handoff_snapshot_needed(&chain),
        "a write-free residency declines like the TXID-0 path and refuses in the fallback"
    );
}

#[test]
fn chain_at_the_object_cap_still_skips() {
    let chain = HandoffChain {
        journal_len: HANDOFF_CHAIN_MAX_OBJECTS,
        ..fresh_short_chain()
    };
    assert!(
        !handoff_snapshot_needed(&chain),
        "exactly 16 objects stay one bounded restore"
    );
}

#[test]
fn chain_over_the_object_cap_keeps_the_snapshot() {
    let chain = HandoffChain {
        journal_len: HANDOFF_CHAIN_MAX_OBJECTS + 1,
        ..fresh_short_chain()
    };
    assert!(
        handoff_snapshot_needed(&chain),
        "17 objects exceed the restore-amplification budget"
    );
}

#[test]
fn restored_chain_keeps_the_snapshot() {
    let chain = HandoffChain {
        chain_from_txid1_journaled: false,
        ..fresh_short_chain()
    };
    assert!(
        handoff_snapshot_needed(&chain),
        "history from an older epoch costs extra LISTs on every future restore"
    );
}

#[test]
fn resumed_chain_keeps_the_snapshot() {
    // A same-epoch resume continues the epoch prefix, but pre-resume uploads
    // are not in this handle's journal, so the object count is unknown: not
    // born here even though no remote restore happened.
    let chain = HandoffChain {
        chain_from_txid1_journaled: false,
        journal_len: 1,
        ..fresh_short_chain()
    };
    assert!(
        handoff_snapshot_needed(&chain),
        "an unjournaled prefix cannot prove its length"
    );
}

#[test]
fn percell_gap_keeps_the_snapshot() {
    let chain = HandoffChain {
        durable_txid: 4,
        ..fresh_short_chain()
    };
    assert!(
        handoff_snapshot_needed(&chain),
        "bundle- or fleet-credited rows outside the per-cell prefix need the snapshot"
    );
}

#[test]
fn overflowed_journal_keeps_the_snapshot() {
    // The journal stopped recording, so the chain is longer than the count
    // says — even when the visible count is short.
    let chain = HandoffChain {
        journal_len: 3,
        journal_overflowed: true,
        ..fresh_short_chain()
    };
    assert!(
        handoff_snapshot_needed(&chain),
        "an overflowed journal cannot bound the restore"
    );
}

#[test]
fn overflowed_full_journal_keeps_the_snapshot() {
    let chain = HandoffChain {
        journal_len: 8192,
        journal_overflowed: true,
        ..fresh_short_chain()
    };
    assert!(
        handoff_snapshot_needed(&chain),
        "a hot cell over a long epoch always merges at eviction"
    );
}
