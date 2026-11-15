// Modified by ryuzcorp (ryuzcorp/celld): bucket-posture group commit: BUCKET_FORMAT 3 and the bundle-write gate (0059). bucket-posture group commit: published-format boot self-check (0070).
//! The bucket format a node can read, and the fleet-wide gate that keeps a
//! node from writing what a live peer cannot read.
//!
//! A rolling update runs two releases against one bucket for the length of
//! the rollout. Anything the newer release writes that the older one cannot
//! restore is a cell that goes dark when it fails over to an old node, and a
//! cell that cannot come back on a downgrade. The rule that prevents both is
//! structural: each node publishes the newest format it reads, and a node
//! writes a format only while every live lease reads it.

use crate::CapacityPeer;

/// The newest bucket format this release reads.
///
/// - `1`: every epoch opens with a whole-database snapshot, so a restore
///   reads one epoch. Every release before paged restore reads this.
/// - `2`: an epoch can page in and continue its predecessor's chain from a
///   marker at the cut. A format-1 reader finds no snapshot in that epoch and
///   cannot restore the cell.
/// - `3`: a bucket-posture node may write shared bundle proofs
///   (`log/<node>/bundle/`) proven by a bucket-session node-log record
///   (`log_tier::SessionKind::Bucket`). A format-2 reader decodes the bundle
///   envelope but never looks in a bucket node's bundle prefix, and treats a
///   missing record as proof the bucket is complete — so bundles are written
///   only while [`bucket_bundles_allowed`] holds, never on format alone.
pub const BUCKET_FORMAT: u16 = 3;

/// The minimum bucket format that reads a paged epoch (format 2). The paged-
/// restore gate (`set_paged_fleet`) judges the fleet against this, not against
/// [`BUCKET_FORMAT`]: a format-2 lease reads paged epochs, so a rolling update
/// with a format-2 lease live must keep paging. Sharing the newest-format
/// constant here closed the paged gate for the length of every later rollout —
/// an unrelated feature regressed by a format bump.
pub const PAGED_FORMAT: u16 = 2;

/// The minimum bucket format that reads bucket-posture bundle proofs (format 3).
/// [`bucket_bundles_allowed`] judges the fleet against this.
pub const BUCKET_BUNDLES_FORMAT: u16 = 3;

/// The format a lease without the field reads: the releases before the
/// field existed all read format 1.
const FIELDLESS_FORMAT: u16 = 1;

/// Whether every live lease reads `format`, so a node can write it.
///
/// A lease counts while it has not expired, so a crashed old node holds the
/// gate closed for one lease lifetime, which is the same bound its cells
/// wait before a peer takes them over. No live lease at all is `false`: a
/// node that has not seen the fleet does not know who is in it, and the
/// older format is always safe to write.
pub fn fleet_reads(peers: &[CapacityPeer], now_ms: u64, format: u16) -> bool {
    let mut live = peers
        .iter()
        .filter(|peer| peer.expires_ms > now_ms)
        .peekable();
    live.peek().is_some()
        && live.all(|peer| peer.bucket_format.unwrap_or(FIELDLESS_FORMAT) >= format)
}

/// Whether this node may write bucket-posture bundle proofs: every live lease
/// reads format 3, so no reader can mistake "no record" for "complete" while
/// bundles exist. This is the fleet-wide rule behind invariant (a): a bucket node
/// writes its session record before its first bundle-credited ack, and the gate
/// keeps any format-2 reader that could misread absence from ever being live.
///
/// The fleet sample carries every live lease including this node's own, so a
/// single-node fleet's own format-3 lease opens the gate — the main consumer of
/// bucket bundles. No live lease at all is `false`: a node that has not seen the
/// fleet does not know who is in it. A failed or missing sample therefore closes
/// the gate; callers stay closed until the first sample, mirroring the paged gate.
pub fn bucket_bundles_allowed(peers: &[CapacityPeer], now_ms: u64) -> bool {
    fleet_reads(peers, now_ms, BUCKET_BUNDLES_FORMAT)
}

/// Boot self-check (defence in depth): the lease this binary publishes must
/// advertise the newest bucket format it reads. A binary that advertised
/// less would hold the whole fleet's bundle gate closed — or split it, with
/// some nodes bundling while others cannot read the bundles — so the publish
/// path asserts instead of running degraded. `process_load` (the single
/// stamp point, read before the first lease publish) calls this on the
/// assembled record, not on the constant, so a future refactor that drops
/// or mistypes the stamp fails loudly instead of silently closing the gate.
pub fn assert_published_bucket_format(bucket_format: Option<u16>) {
    assert_eq!(
        bucket_format,
        Some(BUCKET_FORMAT),
        "this binary reads bucket format {BUCKET_FORMAT} but would publish \
         {bucket_format:?}: refusing to run a fleet-split build",
    );
}

#[cfg(test)]
mod bucket_gate_tests {
    use super::*;
    use crate::NodeId;

    const NOW_MS: u64 = 1_800_000_000_000;

    fn peer(node: &str, expires_ms: u64, bucket_format: Option<u16>) -> CapacityPeer {
        CapacityPeer {
            node: NodeId::from(node),
            addr: String::new(),
            expires_ms,
            peer_protocol: 1,
            sampled_ms: NOW_MS,
            owned_cells: None,
            placement_weight: None,
            bucket_format,
            resident_cells: 0,
            host_websockets: 0,
            rss_bytes: 0,
            in_use_bytes: None,
            pressured: false,
            memory_headroom: None,
            restoring: 0,
            paced_handoff: false,
            rebalance_paused: false,
            draining: false,
        }
    }

    /// A single-node fleet's own format-3 lease opens the gate: the sample
    /// carries the refresher's own record, so "no peers" never means "no fleet"
    /// once the first sample lands.
    #[test]
    fn single_node_fleet_opens_on_its_own_lease() {
        let peers = [peer("n0", NOW_MS + 10_000, Some(3))];
        assert!(bucket_bundles_allowed(&peers, NOW_MS));
    }

    /// No live lease at all stays closed: a node that has not seen the fleet
    /// must not write what it cannot prove every reader reads.
    #[test]
    fn empty_view_stays_closed() {
        assert!(!bucket_bundles_allowed(&[], NOW_MS));
    }

    /// Any live reader at format 2 or below (or predating the field, which reads
    /// as 1) holds the gate closed, whatever else the fleet reads.
    #[test]
    fn live_old_reader_holds_the_gate_closed() {
        let live = NOW_MS + 10_000;
        for old in [None, Some(1), Some(2)] {
            let peers = [
                peer("n0", live, Some(3)),
                peer("n1", live, Some(3)),
                peer("n2", live, old),
            ];
            assert!(
                !bucket_bundles_allowed(&peers, NOW_MS),
                "live reader at {old:?} must hold the gate closed"
            );
        }
    }

    /// An expired old lease is not a reader: the cells it owned waited out the
    /// lease before a peer took them over, so it cannot misread anything.
    #[test]
    fn expired_old_reader_does_not_hold_the_gate() {
        let peers = [
            peer("n0", NOW_MS + 10_000, Some(3)),
            peer("n1", NOW_MS - 1, Some(2)),
        ];
        assert!(bucket_bundles_allowed(&peers, NOW_MS));
    }

    /// A fleet that all reads format 3 writes bundles.
    #[test]
    fn all_format3_opens() {
        let live = NOW_MS + 10_000;
        let peers = [
            peer("n0", live, Some(3)),
            peer("n1", live, Some(3)),
            peer("n2", live, Some(3)),
        ];
        assert!(bucket_bundles_allowed(&peers, NOW_MS));
    }

    /// A lone node whose own lease lapsed is not a live fleet: closed.
    #[test]
    fn lapsed_own_lease_stays_closed() {
        let peers = [peer("n0", NOW_MS - 1, Some(3))];
        assert!(!bucket_bundles_allowed(&peers, NOW_MS));
    }

    /// The paged-epoch gate must not move with the bundle format: a live
    /// format-2 lease reads paged epochs, so a mixed 3/2 fleet keeps paging
    /// while the bundle gate stays closed. Fail-before: judging the paged gate
    /// against the shared newest-format constant closes it here.
    #[test]
    fn paged_gate_stays_open_beside_a_format2_reader() {
        let live = NOW_MS + 10_000;
        let peers = [peer("n0", live, Some(3)), peer("n1", live, Some(2))];
        assert!(fleet_reads(&peers, NOW_MS, PAGED_FORMAT));
        assert!(!bucket_bundles_allowed(&peers, NOW_MS));
    }

    /// The boot self-check accepts the format this binary reads: the
    /// publish path calls it on the assembled lease record.
    #[test]
    fn published_format_check_accepts_the_current_format() {
        assert_published_bucket_format(Some(BUCKET_FORMAT));
    }

    /// The boot self-check refuses an older advertisement: a binary that
    /// stamped this would hold the fleet's bundle gate closed (or split
    /// it). Fail-before: neutering the assertion lets this publish.
    #[test]
    #[should_panic(expected = "fleet-split")]
    fn published_format_check_refuses_an_older_advertisement() {
        assert_published_bucket_format(Some(BUCKET_BUNDLES_FORMAT - 1));
    }

    /// The boot self-check refuses a missing advertisement for the same
    /// reason: an unstamped lease reads as format 1 to old peers.
    #[test]
    #[should_panic(expected = "fleet-split")]
    fn published_format_check_refuses_a_missing_advertisement() {
        assert_published_bucket_format(None);
    }
}
