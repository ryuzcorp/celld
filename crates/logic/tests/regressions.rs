// Modified by ryuzcorp (ryuzcorp/celld): ported from meln1k/celld: scripted core-bug reproductions (ea471ee, e46a685, 6d61462, 18d3bf9, 9d4c163, 468b456, 01522b3, 65fdb13).
//! Scripted reproductions of core bugs the deterministic simulator
//! (`crates/sim`) found. Each drives the core through a minimal shell that
//! answers every effect at once, except the ones a test holds back to
//! recreate the interleaving.
//!
//! `State::validate` runs after every event in debug builds, which is what
//! these tests rely on; they also call it directly so a release build still
//! checks the final state.

use celld_logic::wake::{AlarmSnapshot, AlarmSource, PublicationId};
use celld_logic::{
    on_event, pressure, CasOutcome, Channel, Config, Effect, Event, Failure, LeaseCasOutcome,
    LocalCell, NodeLeaseRecord, NodeLeaseSpec, OwnerRecord, OwnershipOnEvict, Probe, ProofSource,
    RestoreOutcome, State, Timer, WakeHintScope, WebSocketKind,
};
use std::collections::{BTreeMap, VecDeque};

const NOW_MS: u64 = 1_800_000_000_000;
const MONO_MS: u64 = 1_000;
const TTL_MS: u64 = 10_000;

fn config(max_resident: usize) -> Config {
    Config {
        max_resident,
        max_activations: 4,
        max_evictions: 2,
        max_releases: 2,
        max_outbound_websockets: 32,
        ownership_on_evict: OwnershipOnEvict::Release,
        require_node_lease: true,
        peer_protocol: 1,
        operation_deadline_ms: Some(15_000),
        owner_log_recovery_backoff_ms: 1_000,
        owner_log_recovery_attempts: 240,
        alarm_resident_ms: 3_600_000,
        idle_evict_ms: None,
        pressure: pressure::PressureConfig::from_limits(None, Some(0)),
    }
}

/// A shell that completes every effect immediately against an in-memory
/// ownership table, except those `hold` selects, which it keeps unanswered.
struct Shell {
    state: State,
    owners: BTreeMap<String, OwnerRecord>,
    next_etag: u64,
    local_cells: Vec<LocalCell>,
    hold: fn(&Effect) -> bool,
    held: Vec<Effect>,
    timers: Vec<Timer>,
}

impl Shell {
    fn new(max_resident: usize, hold: fn(&Effect) -> bool) -> Self {
        Self {
            state: State::new("n0", config(max_resident)),
            owners: BTreeMap::new(),
            next_etag: 1,
            local_cells: Vec::new(),
            hold,
            held: Vec::new(),
            timers: Vec::new(),
        }
    }

    fn start(&mut self) {
        self.send(Event::StartNodeLease {
            now_ms: NOW_MS,
            now_mono_ms: MONO_MS,
            spec: NodeLeaseSpec {
                addr: "n0".into(),
                peer_protocol: 1,
                generation: "g1".into(),
                resume_generation: None,
                ttl_ms: TTL_MS,
            },
        });
        assert!(self.state.node_authoritative());
    }

    fn send(&mut self, event: Event) -> Vec<Effect> {
        let mut seen = Vec::new();
        let mut queue = VecDeque::from([event]);
        while let Some(event) = queue.pop_front() {
            for effect in on_event(&mut self.state, event) {
                self.state.validate().expect("state invariant");
                if (self.hold)(&effect) {
                    self.held.push(effect.clone());
                } else if let Some(reply) = self.answer(&effect) {
                    queue.push_back(reply);
                }
                seen.push(effect);
            }
            self.state.validate().expect("state invariant");
        }
        seen
    }

    fn answer(&mut self, effect: &Effect) -> Option<Event> {
        Some(match effect.clone() {
            Effect::ScheduleTimer { timer, .. } => {
                self.timers.push(timer);
                return None;
            }
            Effect::ReadSelfNodeLease { op } => Event::SelfNodeLeaseRead {
                op,
                now_ms: NOW_MS,
                now_mono_ms: MONO_MS,
                result: Ok(None),
            },
            Effect::CasNodeLease { op, .. } => Event::NodeLeaseCasCompleted {
                op,
                now_mono_ms: MONO_MS,
                result: Ok(LeaseCasOutcome::Applied {
                    etag: "lease".into(),
                }),
                stamped_log_state: None,
            },
            // A fleet of one: no peer can take a cell this node cannot hold.
            Effect::ReadCapacityPeers { op, .. } => Event::CapacityPeersRead {
                op,
                now_ms: NOW_MS,
                result: Ok(Vec::new()),
            },
            Effect::ReadLocalCells => Event::LocalCellsRead {
                result: Ok(self.local_cells.clone()),
            },
            Effect::ReadOwner { op, cell } => Event::OwnerRead {
                op,
                now_ms: NOW_MS,
                result: Ok(self.owners.get(&cell).cloned()),
            },
            Effect::CasOwner {
                op, cell, epoch, ..
            } => {
                let etag = self.etag();
                self.owners.insert(
                    cell,
                    OwnerRecord {
                        node: Some("n0".into()),
                        epoch,
                        etag,
                    },
                );
                Event::OwnerCasCompleted {
                    op,
                    result: Ok(CasOutcome::Applied),
                }
            }
            Effect::ReleaseOwner { op, cell, epoch } => {
                let current = self.owners.get(&cell).cloned();
                let applies = current.is_some_and(|record| {
                    record.node.as_deref() == Some("n0") && record.epoch == epoch
                });
                if applies {
                    let etag = self.etag();
                    self.owners.insert(
                        cell,
                        OwnerRecord {
                            node: None,
                            epoch,
                            etag,
                        },
                    );
                }
                Event::OwnerReleased {
                    op,
                    result: Ok(if applies {
                        CasOutcome::Applied
                    } else {
                        CasOutcome::Rejected
                    }),
                }
            }
            Effect::Restore { op, .. } => Event::RestoreCompleted {
                op,
                result: Ok(RestoreOutcome {
                    restored: false,
                    alarm: None,
                }),
            },
            Effect::StartRuntime { op, .. } => Event::RuntimeStarted {
                op,
                isolate: None,
                generation: self.state.current_generation(),
                result: Ok(()),
            },
            Effect::Publish { op, .. } => Event::Published { op, result: Ok(()) },
            Effect::EnsureDurable { op, .. } => Event::DurabilityChecked { op, result: Ok(()) },
            Effect::StopRuntime { op, .. } => Event::RuntimeStopped { op },
            Effect::Complete { .. }
            | Effect::Release { .. }
            | Effect::Halt { .. }
            | Effect::ReconcileWakeEntry { .. }
            | Effect::EvictionAdmission { .. }
            | Effect::EvictionFinished { .. }
            | Effect::CancelCellActivity { .. } => return None,
            other => panic!("unexpected effect {other:?}"),
        })
    }

    fn etag(&mut self) -> String {
        self.next_etag += 1;
        self.next_etag.to_string()
    }

    /// Report `cell`'s alarm at `at_ms` as its runtime does, covered by a
    /// wake entry.
    fn observe_alarm(&mut self, cell: &str, at_ms: i64) {
        let epoch = self.owners[cell].epoch;
        let alarm = AlarmSnapshot::committed(
            Some(at_ms),
            AlarmSource {
                id: PublicationId { epoch, sequence: 1 },
                position: 1,
            },
        );
        self.send(Event::AlarmObserved {
            cell: cell.into(),
            alarm,
            covered: true,
            now_ms: NOW_MS,
            now_mono_ms: MONO_MS,
        });
    }

    /// Route one request locally and finish its activity.
    fn serve(&mut self, request: u64, cell: &str) {
        let effects = self.send(Event::RequestAt {
            request,
            cell: cell.into(),
            now_mono_ms: MONO_MS,
        });
        assert!(
            effects.iter().any(|effect| matches!(
                effect,
                Effect::Complete { request: r, result: Ok(celld_logic::Route::Local) } if *r == request
            )),
            "request {request} was not served locally: {effects:?}"
        );
        self.send(Event::ActivityFinished { request });
    }
}

/// A full node sheds a resident to admit another cell, and with
/// `ownership_on_evict = Release` the shed publishes the record as unowned.
/// A request reactivates the shed cell while that release is still in
/// flight, and a second shed ends the new residency first. The second
/// release used to overwrite the first in the cell without retiring it from
/// the operation index: `validate` failed, and release builds leaked the
/// entry.
#[test]
fn a_second_release_supersedes_one_still_in_flight() {
    let releases_of = |held: &[Effect], cell: &str| {
        held.iter()
            .filter(|effect| matches!(effect, Effect::ReleaseOwner { cell: c, .. } if c == cell))
            .count()
    };
    let mut shell = Shell::new(1, |effect| matches!(effect, Effect::ReleaseOwner { .. }));
    shell.start();
    shell.serve(1, "a");
    shell.serve(2, "b");
    assert_eq!(releases_of(&shell.held, "a"), 1, "admitting b sheds a");
    shell.serve(3, "a");
    shell.serve(4, "b");
    assert_eq!(
        releases_of(&shell.held, "a"),
        2,
        "a is shed again while its first release is in flight"
    );

    // The store applies them in order: a's first release finds its epoch
    // superseded, and every later one releases its record.
    let held = std::mem::take(&mut shell.held);
    shell.hold = |_| false;
    for effect in held {
        if let Some(reply) = shell.answer(&effect) {
            shell.send(reply);
        }
    }
    shell.state.validate().expect("state invariant");
    assert_eq!(shell.owners["a"].node, None);
    assert_eq!(shell.owners["a"].epoch, 2);
}

/// A restarted node reads the ownership record of every cell on its disk.
/// If the node lease fences while one of those reads is in flight, the fence
/// used to leave the cell marked as confirming in `Fenced`, and `validate`
/// failed on the very event that halts the node.
#[test]
fn a_fence_settles_startup_confirmation_reads() {
    let mut shell = Shell::new(4, |effect| matches!(effect, Effect::ReadOwner { .. }));
    shell.local_cells = vec![LocalCell {
        id: "c".into(),
        epoch: 3,
        live: false,
    }];
    shell.start();
    assert_eq!(shell.held.len(), 1, "the confirmation read is in flight");
    assert!(!shell.state.ownership_confirmed());

    let fence = shell
        .timers
        .iter()
        .find(|timer| matches!(timer, Timer::NodeLeaseFence { .. }))
        .cloned()
        .expect("the held lease arms its fence");
    let effects = shell.send(Event::TimerFired {
        timer: fence,
        now_ms: NOW_MS + TTL_MS,
        now_mono_ms: MONO_MS + TTL_MS,
    });
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::Halt { .. })));
    shell.state.validate().expect("state invariant");
}

/// A request's durability proof fails while an alarm handler is running on
/// the same runtime. The reset discards the runtime, and it used to leave the
/// alarm `Firing` on a cell that was no longer resident: `validate` failed,
/// and the firing op stayed half retired until the stop completed.
#[test]
fn a_failed_proof_settles_an_alarm_still_running() {
    let mut shell = Shell::new(4, |effect| {
        matches!(
            effect,
            Effect::AwaitDurable { .. } | Effect::FireAlarm { .. }
        )
    });
    shell.start();
    let effects = shell.send(Event::RequestAt {
        request: 1,
        cell: "a".into(),
        now_mono_ms: MONO_MS,
    });
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::Complete { request: 1, .. })));
    shell.observe_alarm("a", NOW_MS as i64 - 1);
    shell.send(Event::OutputAt {
        request: 1,
        channel: Channel::Response,
        position: Some(1),
        observed: None,
        epoch: None,
        now_mono_ms: MONO_MS,
    });
    let alarm = shell
        .timers
        .iter()
        .rev()
        .find(|timer| matches!(timer, Timer::CellAlarm { .. }))
        .cloned()
        .expect("the observed alarm arms its timer");
    shell.send(Event::TimerFired {
        timer: alarm,
        now_ms: NOW_MS,
        now_mono_ms: MONO_MS,
    });
    let held = std::mem::take(&mut shell.held);
    let fire = held
        .iter()
        .find_map(|effect| match effect {
            Effect::FireAlarm {
                op, cell, epoch, ..
            } => Some((*op, cell.clone(), *epoch)),
            _ => None,
        })
        .expect("the due alarm fires");
    let proof = held
        .iter()
        .find_map(|effect| match effect {
            Effect::AwaitDurable { op, .. } => Some(*op),
            _ => None,
        })
        .expect("the write waits for its proof");

    shell.hold = |_| false;
    shell.send(Event::DurableReached {
        op: proof,
        result: Err(Failure::Ambiguous),
        source: ProofSource::Bucket,
    });
    // The handler's answer arrives after its runtime is gone.
    let (op, cell, epoch) = fire;
    shell.send(Event::AlarmFinished {
        op,
        cell,
        epoch,
        now_ms: NOW_MS,
        now_mono_ms: MONO_MS,
        result: Ok((AlarmSnapshot::without_wake(None), true, Some(2))),
    });
    shell.state.validate().expect("state invariant");
}

/// A node at its resident ceiling, holding only a cell whose alarm is due
/// within `alarm_resident_ms`, gets a request for another cell. Holding a
/// cell to save a wake is meant only for a node with room to spare, but the
/// refusal applied to demand shedding too: the waiter stayed parked until
/// its deadline, on every node whose residents all held near alarms.
#[test]
fn a_full_node_sheds_a_cell_whose_alarm_is_near() {
    let mut shell = Shell::new(1, |_| false);
    shell.start();
    shell.serve(1, "a");
    shell.observe_alarm("a", NOW_MS as i64 + 600_000);
    shell.serve(2, "b");
    shell.state.validate().expect("state invariant");
}

/// A drain holds a full batch of releases when a policy eviction arrives
/// for a resident the pump has not reached. The eviction used to take a
/// new handoff past `max_releases`: `validate` failed, and shutdown
/// flooded the store. During a drain any non-quiescing eviction now
/// finds no room and is refused.
#[test]
fn a_drain_refuses_a_policy_eviction_past_its_release_ceiling() {
    let mut shell = Shell::new(3, |effect| matches!(effect, Effect::ReleaseOwner { .. }));
    shell.start();
    shell.serve(1, "a");
    shell.serve(2, "b");
    shell.serve(3, "c");
    shell.send(Event::ReleaseAll);
    assert_eq!(shell.held.len(), 2, "the drain holds a full batch");
    assert_eq!(shell.state.handoffs_in_flight(), 2);

    // One of the three is still resident; the other two refuse as
    // transitioning. Only the resident can overrun the ceiling.
    for cell in ["a", "b", "c"] {
        let effects = shell.send(Event::Evict { cell: cell.into() });
        assert!(
            !effects.iter().any(|effect| matches!(
                effect,
                Effect::EnsureDurable { cell: c, .. } | Effect::StopRuntime { cell: c, .. }
                if c == cell
            )),
            "policy eviction of {cell} starts no handoff past the ceiling: {effects:?}"
        );
    }
    assert_eq!(shell.state.handoffs_in_flight(), 2);
    shell.state.validate().expect("state invariant");
}

/// Shed a cell, then let a peer claim its record before the release
/// lands: the release is rejected and the cell parks `Dormant` with no
/// release in flight, ready for a balancing move.
fn park_dormant(shell: &mut Shell) {
    shell.serve(1, "a");
    shell.serve(2, "b");
    assert_eq!(shell.held.len(), 1, "admitting b sheds a");
    let epoch = shell.owners["a"].epoch;
    shell.owners.insert(
        "a".into(),
        OwnerRecord {
            node: Some("n1".into()),
            epoch,
            etag: "peer".into(),
        },
    );
    let held = std::mem::take(&mut shell.held);
    for effect in held {
        if let Some(reply) = shell.answer(&effect) {
            shell.send(reply);
        }
    }
}

/// A wake hint arrives for a dormant cell while a balancing move is
/// releasing it. The hint used to reactivate the cell mid-move, leaving
/// the handoff mark set outside the pipeline: `validate` failed. Hints
/// now wait for the release, as requests already do.
#[test]
fn a_wake_hint_waits_for_a_balancing_move() {
    let mut shell = Shell::new(1, |effect| matches!(effect, Effect::ReleaseOwner { .. }));
    shell.start();
    park_dormant(&mut shell);
    let effects = shell.send(Event::Rebalance { cells: 1 });
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::ReleaseOwner { cell, .. } if cell == "a")),
        "the balancing move releases a: {effects:?}"
    );

    let effects = shell.send(Event::WakeHint {
        cell: "a".into(),
        entry_ms: NOW_MS as i64,
        scope: WakeHintScope::Owned,
    });
    assert!(
        effects.is_empty(),
        "the hint waits for the move: {effects:?}"
    );
    shell.state.validate().expect("state invariant");
}

/// The node lease fences while a balancing move holds a dormant cell.
/// The fence used to leave the handoff mark set on the fenced cell:
/// `validate` failed. A fenced node moves nothing, so the fence now
/// clears the mark.
#[test]
fn a_fence_clears_a_balancing_handoff() {
    let mut shell = Shell::new(1, |effect| matches!(effect, Effect::ReleaseOwner { .. }));
    shell.start();
    park_dormant(&mut shell);
    shell.send(Event::Rebalance { cells: 1 });

    let fence = shell
        .timers
        .iter()
        .find(|timer| matches!(timer, Timer::NodeLeaseFence { .. }))
        .cloned()
        .expect("the held lease arms its fence");
    let effects = shell.send(Event::TimerFired {
        timer: fence,
        now_ms: NOW_MS + TTL_MS,
        now_mono_ms: MONO_MS + TTL_MS,
    });
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::Halt { .. })));
    shell.state.validate().expect("state invariant");
}

/// A request's durability proof fails on a cell holding a regular socket.
/// The reset discards the runtime, and it used to drop the sockets only
/// once the stop landed: until then a stopping cell held a live
/// transport and `validate` failed. The sockets now go with the runtime.
#[test]
fn a_reset_drops_a_stopping_cell_s_sockets() {
    let mut shell = Shell::new(4, |effect| matches!(effect, Effect::AwaitDurable { .. }));
    shell.start();
    shell.serve(1, "a");
    shell.send(Event::WebSocketOpened {
        cell: "a".into(),
        websocket: 7,
        kind: WebSocketKind::Regular,
    });
    let effects = shell.send(Event::RequestAt {
        request: 2,
        cell: "a".into(),
        now_mono_ms: MONO_MS,
    });
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::Complete { request: 2, .. })));
    shell.send(Event::OutputAt {
        request: 2,
        channel: Channel::Response,
        position: Some(1),
        observed: None,
        epoch: None,
        now_mono_ms: MONO_MS,
    });
    let proof = shell
        .held
        .iter()
        .find_map(|effect| match effect {
            Effect::AwaitDurable { op, .. } => Some(*op),
            _ => None,
        })
        .expect("the write waits for its proof");

    shell.hold = |_| false;
    shell.send(Event::DurableReached {
        op: proof,
        result: Err(Failure::Ambiguous),
        source: ProofSource::Bucket,
    });
    shell.state.validate().expect("state invariant");
}

/// A hibernatable socket's event reaches a quiescing cell. It used to run
/// locally, keeping a swapping cell busy past its single forced cancel
/// until a gate it raised waited for the swap that waited for it: the
/// cell never swapped and stopped serving. The event now waits for the
/// restarted runtime like any request.
#[test]
fn a_hibernatable_socket_event_waits_on_a_quiescing_cell() {
    let mut shell = Shell::new(1, |_| false);
    shell.start();
    shell.serve(1, "a");
    shell.send(Event::WebSocketOpened {
        cell: "a".into(),
        websocket: 7,
        kind: WebSocketKind::Hibernatable,
    });
    // An unfinished request keeps the cell active, so the drain quiesces
    // it without starting its eviction.
    let effects = shell.send(Event::RequestAt {
        request: 2,
        cell: "a".into(),
        now_mono_ms: MONO_MS,
    });
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, Effect::Complete { request: 2, .. })));
    shell.send(Event::ReleaseAll);

    let effects = shell.send(Event::WebSocketRequestAt {
        request: 3,
        cell: "a".into(),
        websocket: 7,
        now_ms: NOW_MS,
        now_mono_ms: MONO_MS,
    });
    assert!(
        !effects.iter().any(|effect| matches!(
            effect,
            Effect::Complete {
                request: 3,
                result: Ok(celld_logic::Route::Local)
            }
        )),
        "the hibernatable event waits for the restarted runtime: {effects:?}"
    );
    shell.state.validate().expect("state invariant");
}

/// A preserve retires a startup confirmation read that is still in
/// flight. It used to retire the owner read but leave the cell in the
/// confirmation set: `validate` failed. The preserve now settles the
/// confirmation too.
#[test]
fn a_preserve_settles_a_confirming_read() {
    let mut shell = Shell::new(4, |effect| matches!(effect, Effect::ReadOwner { .. }));
    shell.local_cells = vec![LocalCell {
        id: "c".into(),
        epoch: 3,
        live: false,
    }];
    shell.start();
    assert_eq!(shell.held.len(), 1, "the confirmation read is in flight");

    shell.send(Event::BeginPreserve);
    assert!(shell.state.ownership_confirmed());
    shell.state.validate().expect("state invariant");
}

/// Two cells' restores fail, so their folds never run and the acknowledged
/// tail stays unfolded on this node alone. The balancer and the drain used
/// to release such dormant records anyway, and the peer that claimed one
/// next restored from the bucket alone: the writes were gone. Both walks
/// now keep a cell whose last restore failed, and count the keep.
#[test]
fn a_drain_and_the_balancer_keep_a_cell_whose_fold_failed() {
    let mut shell = Shell::new(4, |_| false);
    shell.start();
    shell.serve(1, "a");
    shell.hold = |effect| matches!(effect, Effect::Restore { .. } | Effect::ReleaseOwner { .. });
    for (request, cell) in [(2, "b"), (3, "__Queue:q")] {
        shell.send(Event::RequestAt {
            request,
            cell: cell.into(),
            now_mono_ms: MONO_MS,
        });
    }
    for cell in ["b", "__Queue:q"] {
        let op = shell
            .held
            .iter()
            .find_map(|effect| match effect {
                Effect::Restore { op, cell: c, .. } if c == cell => Some(*op),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{cell} waits for its restore"));
        shell.send(Event::RestoreCompleted {
            op,
            result: Err(Failure::Ambiguous),
        });
    }

    let effects = shell.send(Event::Rebalance { cells: 2 });
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::ReleaseOwner { .. })),
        "the balancer keeps cells whose fold failed: {effects:?}"
    );
    let kept = shell
        .state
        .probes()
        .find(|(probe, _)| *probe == Probe::DormantKeptUnfolded)
        .map(|(_, count)| count);
    assert_eq!(kept, Some(1), "the keep is counted");

    let effects = shell.send(Event::ReleaseAll);
    assert!(
        !effects.iter().any(|effect| matches!(
            effect,
            Effect::ReleaseOwner { cell, .. } if cell == "b" || cell == "__Queue:q"
        )),
        "the drain keeps cells whose fold failed: {effects:?}"
    );
    shell.state.validate().expect("state invariant");
}

/// A takeover of a cell whose owner's lease expired without opening a log.
/// That session can still believe it holds the lease, by its own clock or
/// through a renewal that lands late, and open a log through a renewal
/// whose etag still matches; its fleet proofs then acknowledge writes into
/// the superseded epoch. The claim must wait for recovery, which seals the
/// absent log and so moves the etag (seeds 863, 9258, 15037).
#[test]
fn a_takeover_seals_a_lease_that_never_opened_a_log() {
    let mut shell = Shell::new(2, |effect| {
        matches!(
            effect,
            Effect::ReadNodeLease { .. } | Effect::RecoverNodeLog { .. }
        )
    });
    shell.start();
    shell.owners.insert(
        "c".into(),
        OwnerRecord {
            node: Some("n1".into()),
            epoch: 3,
            etag: "owned".into(),
        },
    );
    shell.send(Event::RequestAt {
        request: 1,
        cell: "c".into(),
        now_mono_ms: MONO_MS,
    });
    let Some(Effect::ReadNodeLease { op, .. }) = shell.held.pop() else {
        panic!("expected a lease read: {:?}", shell.held);
    };
    let effects = shell.send(Event::NodeLeaseRead {
        op,
        now_ms: NOW_MS,
        result: Ok(Some(NodeLeaseRecord {
            node: "n1".into(),
            addr: "n1".into(),
            expires_ms: NOW_MS - 1,
            peer_protocol: 1,
            generation: "g1".into(),
            log_state: None,
            etag: "lease-n1".into(),
        })),
    });
    assert!(
        !effects
            .iter()
            .any(|effect| matches!(effect, Effect::CasOwner { .. })),
        "claimed before recovery: {effects:?}"
    );
    let Some(Effect::RecoverNodeLog { op, .. }) = shell.held.pop() else {
        panic!("expected node-log recovery: {effects:?}");
    };
    let effects = shell.send(Event::NodeLogRecovered { op, result: Ok(()) });
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::CasOwner {
                epoch: 4,
                takeover: true,
                ..
            }
        )),
        "{effects:?}"
    );
}

/// A wake-maintenance coverage confirmation lands while a policy
/// eviction is proving durability. The alarm matches the one the core
/// already holds, so the report is maintenance finishing, not new
/// demand: it used to cancel the eviction, which the policy then
/// re-emitted while the cell had no pin. The eviction now runs on.
#[test]
fn a_coverage_confirmation_does_not_cancel_an_eviction() {
    let mut shell = Shell::new(2, |effect| matches!(effect, Effect::EnsureDurable { .. }));
    shell.start();
    shell.serve(1, "a");
    // Past the imminence window, or the policy eviction is refused.
    let at_ms = NOW_MS as i64 + 7_200_000;
    shell.observe_alarm("a", at_ms);
    let effects = shell.send(Event::Evict { cell: "a".into() });
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::EnsureDurable { cell, .. } if cell == "a")),
        "the policy eviction proves durability: {effects:?}"
    );

    // Maintenance confirms the installation the core already holds.
    shell.observe_alarm("a", at_ms);
    let cancelled = shell
        .state
        .probes()
        .find(|(probe, _)| *probe == Probe::AlarmCancelledEviction)
        .map(|(_, count)| count);
    assert_eq!(cancelled, Some(0), "coverage alone cancels no eviction");

    // The eviction is still proving: answering its held proof stops
    // the runtime instead of reaching a retired operation.
    let held = std::mem::take(&mut shell.held);
    shell.hold = |_| false;
    let mut stopped = false;
    for effect in held {
        if let Some(reply) = shell.answer(&effect) {
            stopped |= shell
                .send(reply)
                .iter()
                .any(|effect| matches!(effect, Effect::StopRuntime { cell, .. } if cell == "a"));
        }
    }
    assert!(stopped, "the eviction runs on after the confirmation");
    shell.state.validate().expect("state invariant");
}

/// A covered observation that moves the alarm still rescues the cell:
/// only coverage-only confirmations skip the eviction cancel.
#[test]
fn a_covered_alarm_change_still_cancels_an_eviction() {
    let mut shell = Shell::new(2, |effect| matches!(effect, Effect::EnsureDurable { .. }));
    shell.start();
    shell.serve(1, "a");
    let at_ms = NOW_MS as i64 + 7_200_000;
    shell.observe_alarm("a", at_ms);
    let effects = shell.send(Event::Evict { cell: "a".into() });
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Effect::EnsureDurable { cell, .. } if cell == "a")),
        "the policy eviction proves durability: {effects:?}"
    );

    shell.observe_alarm("a", at_ms + 60_000);
    let cancelled = shell
        .state
        .probes()
        .find(|(probe, _)| *probe == Probe::AlarmCancelledEviction)
        .map(|(_, count)| count);
    assert_eq!(cancelled, Some(1), "a moved alarm still cancels");

    // The proof now reaches a retired operation: no stop follows.
    let held = std::mem::take(&mut shell.held);
    shell.hold = |_| false;
    let mut stopped = false;
    for effect in held {
        if let Some(reply) = shell.answer(&effect) {
            stopped |= shell
                .send(reply)
                .iter()
                .any(|effect| matches!(effect, Effect::StopRuntime { cell, .. } if cell == "a"));
        }
    }
    assert!(!stopped, "the cancelled eviction stops nothing");
    shell.state.validate().expect("state invariant");
}
