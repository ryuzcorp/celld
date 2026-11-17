// Modified by ryuzcorp (ryuzcorp/celld): ported from meln1k/celld: scripted core-bug reproductions (ea471ee, e46a685, 6d61462, 18d3bf9, 9d4c163, 468b456, 01522b3, 65fdb13); shared bucket-posture ownership reads (0056); round-level fence settlement (0062); bucket-posture group commit: live-lease routing pin (0063); renewal readback authority bound (upstream #264); alarm deadline waits for the running copy (upstream #256).
//! Scripted reproductions of core bugs the deterministic simulator
//! (`crates/sim`) found. Each drives the core through a minimal shell that
//! answers every effect at once, except the ones a test holds back to
//! recreate the interleaving.
//!
//! `State::validate` runs after every event in debug builds, which is what
//! these tests rely on; they also call it directly so a release build still
//! checks the final state.

use celld_logic::log_tier::{CellCredit, FenceCellSettlement, FenceRoundSettlement, ProofThenRead};
use celld_logic::wake::{AlarmSnapshot, AlarmSource, PublicationId};
use celld_logic::{
    on_event, pressure, CasOutcome, Channel, Config, Effect, Event, Failure, LeaseCasOutcome,
    LocalCell, NodeLeaseRecord, NodeLeaseSpec, OwnerRecord, OwnershipOnEvict, Probe, ProofSource,
    RequestError, RestoreOutcome, State, Timer, WakeHintScope, WebSocketKind,
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
            Effect::ReadSelfNodeLease { op, .. } => Event::SelfNodeLeaseRead {
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
            // The bucket-posture ownership read: confirmed exactly when the
            // record still names this node at this epoch, as the shell does.
            Effect::VerifyOwnership { op, cell, epoch } => {
                let owned = self.owners.get(&cell).is_some_and(|record| {
                    record.node.as_deref() == Some("n0") && record.epoch == epoch
                });
                Event::OwnershipVerified {
                    op,
                    result: if owned {
                        Ok(())
                    } else {
                        Err(Failure::Definite)
                    },
                }
            }
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

/// Renewals keep flowing while the alarm runs, as in production: without
/// them the test node would lose authority and the timer under test would
/// never fire for unrelated reasons.
fn renew_lease(shell: &mut Shell, now_mono_ms: u64) {
    let renew = shell
        .timers
        .iter()
        .rev()
        .find(|timer| matches!(timer, Timer::NodeLeaseRenew { .. }))
        .cloned()
        .expect("the held lease arms its renew timer");
    shell.send(Event::TimerFired {
        timer: renew,
        now_ms: NOW_MS,
        now_mono_ms,
    });
}

/// Upstream #256: an alarm handler that outlives the operation deadline is
/// still running, not failed, so the first deadline must not re-fire it.
/// The repro armed a 20 s alarm under a 5 s deadline and got copies at 0,
/// 5.5, 11 and 16.5 s. The first expiry now only marks the firing; the
/// running copy settles the alarm when it answers, with no second `FireAlarm`.
/// A second expiry with the op still outstanding treats the op as lost and
/// re-fires, preserving at-least-once. Fail-before: the first expiry retired
/// the op and re-armed, so the held effects held two concurrent `FireAlarm`s.
#[test]
fn an_alarm_deadline_waits_for_the_running_copy() {
    let mut shell = Shell::new(4, |effect| {
        matches!(
            effect,
            Effect::AwaitDurable { .. } | Effect::FireAlarm { .. }
        )
    });
    shell.start();
    shell.serve(1, "a");
    shell.observe_alarm("a", NOW_MS as i64 - 1);
    let alarm = shell
        .timers
        .iter()
        .rev()
        .find(|timer| matches!(timer, Timer::CellAlarm { .. }))
        .cloned()
        .expect("the observed alarm arms its timer");
    let mut seen = shell.send(Event::TimerFired {
        timer: alarm,
        now_ms: NOW_MS,
        now_mono_ms: MONO_MS,
    });
    let (op, cell, epoch) = shell
        .held
        .iter()
        .find_map(|effect| match effect {
            Effect::FireAlarm {
                op, cell, epoch, ..
            } => Some((*op, cell.clone(), *epoch)),
            _ => None,
        })
        .expect("the due alarm fires");
    for at in [
        MONO_MS + 4_000,
        MONO_MS + 7_000,
        MONO_MS + 10_000,
        MONO_MS + 13_000,
    ] {
        renew_lease(&mut shell, at);
    }
    let deadline = shell
        .timers
        .iter()
        .rev()
        .find(|timer| matches!(timer, Timer::OperationDeadline { op: current } if current == &op))
        .cloned()
        .expect("the firing arms its deadline");
    seen.extend(shell.send(Event::TimerFired {
        timer: deadline,
        now_ms: NOW_MS,
        now_mono_ms: MONO_MS + 15_000,
    }));
    // The deadline marked the firing and re-armed its own timer, but fired
    // nothing: still exactly one copy running. Firing the latest cell timer
    // proves it: the old code re-armed a `CellAlarm` here whose tick starts
    // the second copy, while the new code arms only the fallback deadline.
    assert!(
        seen.iter().any(|effect| matches!(
            effect,
            Effect::ScheduleTimer {
                timer: Timer::OperationDeadline { op: current },
                ..
            } if current == &op
        )),
        "the first deadline re-arms its fallback"
    );
    if let Some(tick) = shell
        .timers
        .iter()
        .rev()
        .find(|timer| matches!(timer, Timer::CellAlarm { .. }))
        .cloned()
    {
        shell.send(Event::TimerFired {
            timer: tick,
            now_ms: NOW_MS,
            now_mono_ms: MONO_MS + 16_000,
        });
    }
    assert_eq!(
        shell
            .held
            .iter()
            .filter(|effect| matches!(effect, Effect::FireAlarm { .. }))
            .count(),
        1,
        "the first deadline must not re-fire a running alarm"
    );
    // The running copy answers late and settles the alarm: no refire, no
    // dangling firing state.
    shell.hold = |_| false;
    let settled = shell.send(Event::AlarmFinished {
        op,
        cell,
        epoch,
        now_ms: NOW_MS,
        now_mono_ms: MONO_MS + 20_000,
        result: Ok((AlarmSnapshot::without_wake(None), true, None)),
    });
    assert!(
        !settled
            .iter()
            .any(|effect| matches!(effect, Effect::FireAlarm { .. })),
        "the late answer settles without a second copy"
    );
    shell.state.validate().expect("state invariant");
}

/// The other half of upstream #256: an op that never answers is still a lost
/// alarm, not a silent one. A second deadline expiry with the firing op
/// still outstanding retires it exactly as a failed handler would, so the
/// alarm re-fires. Fail-before: already re-fired at the first expiry.
#[test]
fn a_twice_expired_alarm_still_refires() {
    let mut shell = Shell::new(4, |effect| {
        matches!(
            effect,
            Effect::AwaitDurable { .. } | Effect::FireAlarm { .. }
        )
    });
    shell.start();
    shell.serve(1, "a");
    shell.observe_alarm("a", NOW_MS as i64 - 1);
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
    let op = shell
        .held
        .iter()
        .find_map(|effect| match effect {
            Effect::FireAlarm { op, .. } => Some(*op),
            _ => None,
        })
        .expect("the due alarm fires");
    for at in [
        MONO_MS + 4_000,
        MONO_MS + 7_000,
        MONO_MS + 10_000,
        MONO_MS + 13_000,
    ] {
        renew_lease(&mut shell, at);
    }
    for now_mono_ms in [MONO_MS + 15_000, MONO_MS + 30_000] {
        if now_mono_ms == MONO_MS + 30_000 {
            for at in [
                MONO_MS + 19_000,
                MONO_MS + 22_000,
                MONO_MS + 25_000,
                MONO_MS + 28_000,
            ] {
                renew_lease(&mut shell, at);
            }
        }
        let deadline = shell
            .timers
            .iter()
            .rev()
            .find(
                |timer| matches!(timer, Timer::OperationDeadline { op: current } if current == &op),
            )
            .cloned()
            .expect("the firing arms its deadline");
        shell.send(Event::TimerFired {
            timer: deadline,
            now_ms: NOW_MS,
            now_mono_ms,
        });
    }
    // The op never answered, so the second expiry retired it and re-armed:
    // firing the re-armed cell timer starts the second copy.
    let realarm = shell
        .timers
        .iter()
        .rev()
        .find(|timer| matches!(timer, Timer::CellAlarm { .. }))
        .cloned()
        .expect("the lost op re-arms the alarm");
    shell.send(Event::TimerFired {
        timer: realarm,
        now_ms: NOW_MS,
        now_mono_ms: MONO_MS + 31_000,
    });
    assert!(
        shell.held.iter().any(|effect| matches!(
            effect,
            Effect::FireAlarm { op: current, .. } if current != &op
        )),
        "a lost alarm copy still re-fires"
    );
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

/// Route one request per position on cell "a" and hold each response at the
/// output gate, so every request owns an open barrier waiting on its proof.
fn open_gated_writes(shell: &mut Shell, positions: &[u64]) {
    for (index, position) in positions.iter().enumerate() {
        let request = index as u64 + 1;
        shell.send(Event::RequestAt {
            request,
            cell: "a".into(),
            now_mono_ms: MONO_MS,
        });
        shell.send(Event::OutputAt {
            request,
            channel: Channel::Response,
            position: Some(*position),
            observed: None,
            epoch: None,
            now_mono_ms: MONO_MS,
        });
    }
}

/// Find the held durability gate proving `position`.
fn held_gate_op_for_position(held: &[Effect], position: u64) -> u64 {
    held.iter()
        .find_map(|effect| match effect {
            Effect::AwaitDurable {
                op, position: p, ..
            } if *p == position => Some(*op),
            _ => None,
        })
        .expect("the write waits for its proof")
}

/// Hold every durability proof and ownership read, so the test drives the
/// bucket-posture acknowledgement interleaving by hand.
fn bucket_gate_shell() -> Shell {
    Shell::new(4, |effect| {
        matches!(
            effect,
            Effect::AwaitDurable { .. } | Effect::VerifyOwnership { .. }
        )
    })
}

/// Prove one gate and append the effects the proof produced.
fn prove_gate(shell: &mut Shell, seen: &mut Vec<Effect>, position: u64) {
    let op = held_gate_op_for_position(&shell.held, position);
    seen.extend(shell.send(Event::DurableReached {
        op,
        result: Ok(position),
        source: ProofSource::Bucket,
    }));
}

fn verify_reads(seen: &[Effect]) -> Vec<u64> {
    seen.iter()
        .filter_map(|effect| match effect {
            Effect::VerifyOwnership { op, .. } => Some(*op),
            _ => None,
        })
        .collect()
}

/// Concurrent bucket-proof gates on one cell share a single ownership read:
/// the first proof to land opens the round, and every gate at or below its
/// cover joins it instead of issuing a read of its own.
#[test]
fn bucket_proof_gates_share_one_ownership_read() {
    const N: u64 = 8;
    let mut shell = bucket_gate_shell();
    shell.start();
    open_gated_writes(&mut shell, &(1..=N).collect::<Vec<_>>());
    // Proofs land largest-first, so the first one covers every gate and the
    // round never grows a queue.
    let mut seen = Vec::new();
    for position in (1..=N).rev() {
        prove_gate(&mut shell, &mut seen, position);
    }
    let reads = verify_reads(&seen);
    assert_eq!(
        reads.len(),
        1,
        "eight proven gates share one read: {seen:?}"
    );
    let released = shell.send(Event::OwnershipVerified {
        op: reads[0],
        result: Ok(()),
    });
    for request in 1..=N {
        assert!(
            released.iter().any(|effect| matches!(
                effect,
                Effect::Release {
                    request: r,
                    result: Ok(()),
                    ..
                } if *r == request
            )),
            "write {request} is acknowledged after the shared read: {released:?}"
        );
    }
    let joined = shell
        .state
        .probes()
        .find(|(probe, _)| *probe == Probe::GateJoinedVerifyRound)
        .map(|(_, count)| count);
    assert_eq!(joined, Some(N - 1), "every later gate joined the round");
    let proven = shell
        .state
        .probes()
        .find(|(probe, _)| *probe == Probe::GateProvenByBucket)
        .map(|(_, count)| count);
    assert_eq!(proven, Some(1), "one read proves every gate");
    shell.state.validate().expect("state invariant");
}

/// A gate past the in-flight read's cover does not join it: it issues its own
/// read at once instead of waiting for the first to finish, so sharing adds
/// no latency. Each read releases only the gates it answers, in either
/// completion order.
#[test]
fn a_gate_past_the_in_flight_read_gets_its_own_round() {
    let mut shell = bucket_gate_shell();
    shell.start();
    open_gated_writes(&mut shell, &[1, 2]);
    let mut seen = Vec::new();
    prove_gate(&mut shell, &mut seen, 1);
    let first = verify_reads(&seen);
    assert_eq!(first.len(), 1, "the first proof opens a round: {seen:?}");
    prove_gate(&mut shell, &mut seen, 2);
    let reads = verify_reads(&seen);
    assert_eq!(
        reads.len(),
        2,
        "the past-cover gate reads at once instead of queueing: {seen:?}"
    );
    let second = held_gate_op_for_position(&shell.held, 2);
    assert_eq!(
        reads[1], second,
        "the second read answers the past-cover gate"
    );
    shell.state.validate().expect("state invariant");
    let seen = shell.send(Event::OwnershipVerified {
        op: reads[1],
        result: Ok(()),
    });
    assert!(
        seen.iter().any(|effect| matches!(
            effect,
            Effect::Release {
                request: 2,
                result: Ok(()),
                ..
            }
        )),
        "the second read releases its gate: {seen:?}"
    );
    assert!(
        !seen
            .iter()
            .any(|effect| matches!(effect, Effect::Release { request: 1, .. })),
        "the first gate still waits for the read issued after its proof: {seen:?}"
    );
    let seen = shell.send(Event::OwnershipVerified {
        op: reads[0],
        result: Ok(()),
    });
    assert!(
        seen.iter().any(|effect| matches!(
            effect,
            Effect::Release {
                request: 1,
                result: Ok(()),
                ..
            }
        )),
        "the first read releases its gate: {seen:?}"
    );
    shell.state.validate().expect("state invariant");
}

/// A read's cover is its issuing gate's own position, not the `durable` the
/// proof reported. A proof reporting more than its gate's position names no
/// capture of later commits, so a later gate must not join the read it
/// triggered.
#[test]
fn a_reported_watermark_does_not_widen_a_read_cover() {
    let mut shell = bucket_gate_shell();
    shell.start();
    open_gated_writes(&mut shell, &[1, 2]);
    let op = held_gate_op_for_position(&shell.held, 1);
    let mut seen = shell.send(Event::DurableReached {
        op,
        result: Ok(5),
        source: ProofSource::Bucket,
    });
    prove_gate(&mut shell, &mut seen, 2);
    assert_eq!(
        verify_reads(&seen).len(),
        2,
        "the later gate reads for itself: {seen:?}"
    );
    shell.state.validate().expect("state invariant");
}

/// A refused ownership read fails every gate it answered and resets the
/// cell; a stale result after the reset releases nothing.
#[test]
fn a_refused_read_fails_every_joined_gate_and_resets() {
    let mut shell = bucket_gate_shell();
    shell.start();
    open_gated_writes(&mut shell, &[1, 2, 3]);
    let mut seen = Vec::new();
    for position in [3, 2, 1] {
        prove_gate(&mut shell, &mut seen, position);
    }
    let reads = verify_reads(&seen);
    assert_eq!(reads.len(), 1, "the joined gates share one read: {seen:?}");
    let seen = shell.send(Event::OwnershipVerified {
        op: reads[0],
        result: Err(Failure::Definite),
    });
    for request in 1..=3 {
        assert!(
            seen.iter().any(|effect| matches!(
                effect,
                Effect::Release {
                    request: r,
                    result: Err(RequestError::DurabilityUnproven),
                    ..
                } if *r == request
            )),
            "write {request} fails after the refused read: {seen:?}"
        );
    }
    assert!(
        seen.iter()
            .any(|effect| matches!(effect, Effect::StopRuntime { cell, .. } if cell == "a")),
        "the refused read resets the cell: {seen:?}"
    );
    let refused = shell
        .state
        .probes()
        .find(|(probe, _)| *probe == Probe::GateOwnershipRefused)
        .map(|(_, count)| count);
    assert_eq!(refused, Some(1), "the refused round is counted once");
    // A stale result after the reset releases nothing.
    let seen = shell.send(Event::OwnershipVerified {
        op: reads[0],
        result: Err(Failure::Definite),
    });
    assert!(
        !seen
            .iter()
            .any(|effect| matches!(effect, Effect::Release { .. })),
        "the stale read result releases nothing: {seen:?}"
    );
    let swept = held_gate_op_for_position(&shell.held, 1);
    let seen = shell.send(Event::DurableReached {
        op: swept,
        result: Ok(1),
        source: ProofSource::Bucket,
    });
    assert!(
        !seen
            .iter()
            .any(|effect| matches!(effect, Effect::Release { .. })),
        "a proof for a swept gate releases nothing: {seen:?}"
    );
    shell.state.validate().expect("state invariant");
}

/// A write at a position a verified proof already covers releases with no
/// new proof and no read; a write at a newer position still waits for its
/// own.
#[test]
fn a_write_at_a_verified_position_releases_without_a_new_read() {
    let mut shell = bucket_gate_shell();
    shell.start();
    open_gated_writes(&mut shell, &[5]);
    let mut seen = Vec::new();
    prove_gate(&mut shell, &mut seen, 5);
    let reads = verify_reads(&seen);
    assert_eq!(reads.len(), 1, "the proof opens a round: {seen:?}");
    shell.send(Event::OwnershipVerified {
        op: reads[0],
        result: Ok(()),
    });
    // A write the verified proof already covered releases at once.
    shell.send(Event::RequestAt {
        request: 2,
        cell: "a".into(),
        now_mono_ms: MONO_MS,
    });
    let seen = shell.send(Event::OutputAt {
        request: 2,
        channel: Channel::Response,
        position: Some(3),
        observed: None,
        epoch: None,
        now_mono_ms: MONO_MS,
    });
    assert!(
        seen.iter().any(|effect| matches!(
            effect,
            Effect::Release {
                request: 2,
                result: Ok(()),
                ..
            }
        )),
        "the covered write releases at once: {seen:?}"
    );
    assert!(
        !seen
            .iter()
            .any(|effect| matches!(effect, Effect::AwaitDurable { .. })),
        "the covered write proves nothing anew: {seen:?}"
    );
    let fast = shell
        .state
        .probes()
        .find(|(probe, _)| *probe == Probe::GateReleasedProven)
        .map(|(_, count)| count);
    assert_eq!(fast, Some(1), "the fast path is counted");
    // A write past the verified position still waits for its own proof.
    shell.send(Event::RequestAt {
        request: 3,
        cell: "a".into(),
        now_mono_ms: MONO_MS,
    });
    let seen = shell.send(Event::OutputAt {
        request: 3,
        channel: Channel::Response,
        position: Some(6),
        observed: None,
        epoch: None,
        now_mono_ms: MONO_MS,
    });
    assert!(
        seen.iter()
            .any(|effect| matches!(effect, Effect::AwaitDurable { position: 6, .. })),
        "the newer write waits for its proof: {seen:?}"
    );
    assert!(
        !seen
            .iter()
            .any(|effect| matches!(effect, Effect::Release { request: 3, .. })),
        "the newer write is not released: {seen:?}"
    );
    shell.state.validate().expect("state invariant");
}

/// Find the held durability gate proving `position` on `cell`.
fn held_gate_op_for_cell_position(held: &[Effect], cell: &str, position: u64) -> u64 {
    held.iter()
        .find_map(|effect| match effect {
            Effect::AwaitDurable {
                op,
                cell: c,
                position: p,
                ..
            } if c == cell && *p == position => Some(*op),
            _ => None,
        })
        .expect("the write waits for its proof")
}

/// The epoch a bucket proof's ownership round names for `cell`.
fn verify_epoch_for_cell(held: &[Effect], cell: &str) -> u64 {
    held.iter()
        .find_map(|effect| match effect {
            Effect::VerifyOwnership { cell: c, epoch, .. } if c == cell => Some(*epoch),
            _ => None,
        })
        .expect("a proof opened an ownership read for the cell")
}

/// Prove one gate on `cell` and append the effects the proof produced.
fn prove_cell_gate(shell: &mut Shell, seen: &mut Vec<Effect>, cell: &str, position: u64) {
    let op = held_gate_op_for_cell_position(&shell.held, cell, position);
    seen.extend(shell.send(Event::DurableReached {
        op,
        result: Ok(position),
        source: ProofSource::Bucket,
    }));
}

/// A round-level fence read settles the gates its proof captured with no
/// per-gate ownership answer: the credited cell's writes release proven
/// while the uncredited cell's writes fail, and a gate past the cover keeps
/// waiting. The trapped per-gate reads stay unanswered, and a late answer to
/// one releases nothing.
#[test]
fn fence_round_verified_settles_covered_gates_without_per_gate_reads() {
    let mut shell = bucket_gate_shell();
    shell.start();
    open_gated_writes(&mut shell, &[1, 2, 9]);
    // One gated write on a second cell.
    shell.send(Event::RequestAt {
        request: 100,
        cell: "b".into(),
        now_mono_ms: MONO_MS,
    });
    shell.send(Event::OutputAt {
        request: 100,
        channel: Channel::Response,
        position: Some(7),
        observed: None,
        epoch: None,
        now_mono_ms: MONO_MS,
    });
    let mut seen = Vec::new();
    prove_cell_gate(&mut shell, &mut seen, "a", 1);
    prove_cell_gate(&mut shell, &mut seen, "a", 2);
    prove_cell_gate(&mut shell, &mut seen, "b", 7);
    let reads = verify_reads(&seen);
    assert_eq!(reads.len(), 3, "every proof opens its own round: {seen:?}");
    let epoch_a = verify_epoch_for_cell(&shell.held, "a");
    let epoch_b = verify_epoch_for_cell(&shell.held, "b");
    let settled = shell.send(Event::FenceRoundVerified {
        round: FenceRoundSettlement {
            order: ProofThenRead {
                proof_seq: 1,
                read_seq: 2,
            },
            cells: vec![
                FenceCellSettlement {
                    cell: "a".into(),
                    epoch: epoch_a,
                    cover: 2,
                    credit: CellCredit::Credit,
                },
                FenceCellSettlement {
                    cell: "b".into(),
                    epoch: epoch_b,
                    cover: 7,
                    credit: CellCredit::Uncredited,
                },
            ],
        },
    });
    for request in [1, 2] {
        assert!(
            settled.iter().any(|effect| matches!(
                effect,
                Effect::Release {
                    request: r,
                    result: Ok(()),
                    ..
                } if *r == request
            )),
            "credited write {request} is acknowledged: {settled:?}"
        );
    }
    assert!(
        settled.iter().any(|effect| matches!(
            effect,
            Effect::Release {
                request: 100,
                result: Err(RequestError::DurabilityUnproven),
                ..
            }
        )),
        "the uncredited write fails instead of hanging: {settled:?}"
    );
    assert!(
        !settled
            .iter()
            .any(|effect| matches!(effect, Effect::Release { request: 3, .. })),
        "the gate past the cover keeps waiting: {settled:?}"
    );
    assert!(
        !settled
            .iter()
            .any(|effect| matches!(effect, Effect::VerifyOwnership { .. })),
        "the round settles with no per-gate ownership read: {settled:?}"
    );
    for probe in [Probe::GateProvenByFenceRound, Probe::GateFenceRoundRefused] {
        let count = shell
            .state
            .probes()
            .find(|(found, _)| *found == probe)
            .map(|(_, count)| count);
        assert_eq!(count, Some(1), "the round outcome is counted once");
    }
    // The trapped per-gate reads stay unanswered, and a late answer to one
    // releases nothing: the fence read superseded them.
    assert_eq!(
        shell
            .held
            .iter()
            .filter(|effect| matches!(effect, Effect::VerifyOwnership { .. }))
            .count(),
        3,
        "no per-gate read was answered"
    );
    for op in reads {
        let quiet = shell.send(Event::OwnershipVerified { op, result: Ok(()) });
        assert!(
            !quiet
                .iter()
                .any(|effect| matches!(effect, Effect::Release { .. })),
            "a late read answers nothing: {quiet:?}"
        );
    }
    shell.state.validate().expect("state invariant");
}

/// A fence read issued at or before the proof is never accepted: even a
/// `Credit` verdict on the cell fails the covered gates instead of
/// acknowledging them.
#[test]
fn fence_round_before_its_proof_is_never_accepted() {
    for order in [
        ProofThenRead {
            proof_seq: 5,
            read_seq: 5,
        },
        ProofThenRead {
            proof_seq: 6,
            read_seq: 5,
        },
    ] {
        let mut shell = bucket_gate_shell();
        shell.start();
        open_gated_writes(&mut shell, &[1]);
        let mut seen = Vec::new();
        prove_gate(&mut shell, &mut seen, 1);
        let epoch = verify_epoch_for_cell(&shell.held, "a");
        let settled = shell.send(Event::FenceRoundVerified {
            round: FenceRoundSettlement {
                order,
                cells: vec![FenceCellSettlement {
                    cell: "a".into(),
                    epoch,
                    cover: 1,
                    credit: CellCredit::Credit,
                }],
            },
        });
        assert!(
            settled.iter().any(|effect| matches!(
                effect,
                Effect::Release {
                    request: 1,
                    result: Err(RequestError::DurabilityUnproven),
                    ..
                }
            )),
            "a read issued before the proof never settles as proven ({order:?}): {settled:?}"
        );
        let unordered = shell
            .state
            .probes()
            .find(|(probe, _)| *probe == Probe::GateFenceRoundUnordered)
            .map(|(_, count)| count);
        assert_eq!(unordered, Some(1), "the refused order is counted");
        shell.state.validate().expect("state invariant");
    }
}
/// A live owner lease that still names the cell's owner routes remotely: the
/// core never claims a cell — and never runs the takeover interlock — while
/// the owner is alive. A single-cell takeover from a live donor is refused
/// here, not folded: the fold only runs after whole-session recovery fences
/// the donor (see `NodeLogManager::{recover_serial, fold_cell}`). Fail-before:
/// claiming on a live lease routes `Local` (or emits `RecoverNodeLog`) here.
#[test]
fn a_live_matching_owner_lease_routes_remote_without_a_claim() {
    let mut shell = Shell::new(2, |effect| matches!(effect, Effect::ReadNodeLease { .. }));
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
            expires_ms: NOW_MS + TTL_MS,
            peer_protocol: 1,
            generation: "g1".into(),
            log_state: None,
            etag: "lease-n1".into(),
        })),
    });
    assert!(
        effects.iter().any(|effect| matches!(
            effect,
            Effect::Complete {
                request: 1,
                result: Ok(celld_logic::Route::Remote { .. }),
            }
        )),
        "a live matching owner routes remote: {effects:?}"
    );
    assert!(
        !effects.iter().any(|effect| matches!(
            effect,
            Effect::RecoverNodeLog { .. } | Effect::CasOwner { .. }
        )),
        "no claim and no takeover interlock while the owner is live: {effects:?}"
    );
    shell.state.validate().expect("state invariant");
}

/// Upstream #264: every contested renewal funnels through a readback, and a
/// readback that hangs on a stalled store used to wedge every renewal until
/// the fence timer — even with proven headroom left — because the read
/// carried no bound for the shell to enforce. The renewal readback now
/// carries the prior record's peer-visible expiry; the initial acquisition
/// (no prior authority) stays unbounded. Fail-before: the readback carried
/// no expiry, so the shell could not bound it.
#[test]
fn a_renewal_readback_carries_the_authority_bound() {
    let mut shell = Shell::new(4, |_| false);
    shell.start();
    shell.hold = |effect| {
        matches!(
            effect,
            Effect::CasNodeLease { .. } | Effect::ReadSelfNodeLease { .. }
        )
    };
    let renew = shell
        .timers
        .iter()
        .find(|timer| matches!(timer, Timer::NodeLeaseRenew { .. }))
        .cloned()
        .expect("the held lease arms its renew timer");
    shell.send(Event::TimerFired {
        timer: renew,
        now_ms: NOW_MS,
        now_mono_ms: MONO_MS,
    });
    let (cas_op, prior_expires) = match shell.held.as_slice() {
        [Effect::CasNodeLease { op, record, .. }] => (*op, record.expires_ms),
        held => panic!("the renew timer issues one lease CAS: {held:?}"),
    };
    shell.send(Event::NodeLeaseCasCompleted {
        op: cas_op,
        now_mono_ms: MONO_MS,
        result: Err(Failure::Ambiguous),
        stamped_log_state: None,
    });
    let bound = shell
        .held
        .iter()
        .find_map(|effect| match effect {
            Effect::ReadSelfNodeLease {
                authority_expires_ms,
                ..
            } => Some(*authority_expires_ms),
            _ => None,
        })
        .expect("an ambiguous renewal reads back");
    assert_eq!(
        bound,
        Some(prior_expires),
        "the readback carries the prior expiry so the shell can bound it"
    );
    shell.state.validate().expect("state invariant");
}
