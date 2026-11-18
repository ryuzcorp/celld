// Modified by ryuzcorp (ryuzcorp/celld): validate CELLD_MEMORY_SCOPE; pin the node-session id in the data dir (upstream #249).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! What the process can learn about where it is running.
//!
//! Two kinds of fact, both read once at startup or sampled on a timer:
//! the environment the operator set, and the machine underneath — memory,
//! CPU ticks, page size — which is per-platform and therefore duplicated
//! behind `cfg` for each one.
use cello_logic::OwnershipOnEvict;
use rand::RngCore as _;

/// Validate one node name before it enters a storage key, a local path, or a
/// peer message. All three surfaces use this same restricted identity.
pub(crate) fn validate_node_name(node: &str) -> anyhow::Result<()> {
    if cello_logic::peer::valid_identity(node) {
        Ok(())
    } else {
        anyhow::bail!(
            "must be a safe node name of 1 to 128 ASCII letters, numbers, dots, dashes, or underscores"
        )
    }
}

/// The file beside the follower peerlog that pins this disk's node-session
/// id across restarts (upstream #249).
pub const NODE_ID_FILE: &str = "node-id";

/// Resolve the node-session id: an explicit `CELLD_NODE` wins outright and
/// is recorded for the next boot; otherwise reuse the id the data directory
/// already pins, generating and pinning one on first boot. The pin lives
/// beside the follower fragments, so the id and the disks it recovers share
/// a fate: a restarted process answers for its predecessor's leases and
/// fragments instead of stranding them behind `WrongTarget` (upstream
/// #249). A missing pin (first boot) generates and pins a fresh id. A pin
/// that cannot be read at all — I/O error, permissions, or a non-file at
/// the pin path — degrades to today's ephemeral id with a loud line and
/// leaves the pin untouched: minting a fresh identity over retained
/// fragments would orphan them, and the safest repair is operator action,
/// never a boot failure. A corrupt pin warns and regenerates, naming the
/// discarded id for operator recovery.
#[allow(clippy::disallowed_methods)] // The pin is read before any injected filesystem exists.
pub fn resolve_node_session_id(explicit: Option<String>, data_dir: &std::path::Path) -> String {
    if let Some(node) = explicit {
        if validate_node_name(&node).is_ok() {
            record_node_session_id(data_dir, &node);
        }
        return node;
    }
    let path = data_dir.join(NODE_ID_FILE);
    match std::fs::read_to_string(&path) {
        Ok(pinned) => {
            let pinned = pinned.trim().to_string();
            if validate_node_name(&pinned).is_ok() {
                return pinned;
            }
            tracing::warn!(
                path = %path.display(),
                discarded = %pinned,
                "pinned node id is corrupt; generating a fresh session id"
            );
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(
                path = %path.display(),
                %error,
                "pinned node id is unreadable; starting with an ephemeral session id without overwriting the pin"
            );
            return random_node_session_id();
        }
    }
    let node = random_node_session_id();
    record_node_session_id(data_dir, &node);
    node
}

/// Durably pin the node-session id: tmp file, fsync, rename, dir fsync —
/// the follower-fragment pattern — so the pin survives a power loss or OS
/// crash before the next boot reads it. Best effort: an unwritable
/// directory keeps today's ephemeral behavior, loudly.
#[allow(clippy::disallowed_methods)] // The pin is written before any injected filesystem exists.
fn persist_node_session_id(data_dir: &std::path::Path, node: &str) -> std::io::Result<()> {
    use std::io::Write as _;
    std::fs::create_dir_all(data_dir)?;
    let path = data_dir.join(NODE_ID_FILE);
    let tmp = data_dir.join(format!("{NODE_ID_FILE}.tmp"));
    let mut file = std::fs::File::create(&tmp)?;
    file.write_all(format!("{node}\n").as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&tmp, &path)?;
    std::fs::File::open(data_dir)?.sync_all()?;
    Ok(())
}

/// Reflect the effective node-session id in the data directory's pin. Best
/// effort: an unwritable directory keeps today's ephemeral behavior, loudly.
fn record_node_session_id(data_dir: &std::path::Path, node: &str) {
    if let Err(error) = persist_node_session_id(data_dir, node) {
        tracing::warn!(
            path = %data_dir.join(NODE_ID_FILE).display(),
            %error,
            "cannot pin the node-session id; restarts will mint fresh ids"
        );
    }
}

/// The default bound on concurrent cold activations for a machine with this
/// many hardware threads: eight per thread, at least 16, at most 128.
///
/// A cold activation is object-store latency, not compute. Measured on GCS
/// from a 4-vCPU node (2026-09-01), the route was ~100 ms of round trips
/// (owner read 25 ms, ownership CAS 60 ms, restore 10 ms) and ~10 ms of CPU,
/// so a bound equal to the thread count admitted 35-40 activations a second
/// per node whatever the client offered, and 1,000 hibernated alarms due in
/// one minute were 29 s late at the tail. The same ceiling appeared on
/// two-core nodes. Eight per thread keeps the CPU share of the route
/// below one core per thread at full duty (measured 30 % of four vCPUs at
/// 16 slots, 43 % at 64); the floor keeps a one- or two-thread node from
/// serializing an I/O-bound path; the cap is the point past which more
/// concurrent cold I/O can itself become an object-store storm, and it is
/// the value the previous default already capped at.
pub fn default_max_activations(available_parallelism: usize) -> usize {
    const PER_THREAD: usize = 8;
    const FLOOR: usize = 16;
    const CAP: usize = 128;
    available_parallelism
        .saturating_mul(PER_THREAD)
        .clamp(FLOOR, CAP)
}

pub fn random_node_session_id() -> String {
    let mut bytes = [0_u8; 16];
    crate::asyncrt::rng("session").fill_bytes(&mut bytes);
    let suffix = bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("node_{suffix}")
}

pub fn random_peer_key() -> [u8; 32] {
    let mut key = [0_u8; 32];
    crate::asyncrt::rng("peer_key").fill_bytes(&mut key);
    key
}

pub fn random_process_generation() -> String {
    let mut generation = [0_u8; 32];
    crate::asyncrt::rng("generation").fill_bytes(&mut generation);
    generation
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Watermarks from celld's public environment contract.
/// `CELLD_PRESSURE_OWNERSHIP`: `release` (the default) or `sticky`.
/// The lease lifetime, as the capacity listing needs it to decide which node
/// records are stale enough to skip reading. Same variable and default the
/// lease itself uses.
/// Concurrent outbound WebSockets one cell may hold
/// (`CELLD_MAX_OUTBOUND_WEBSOCKETS`).
pub const DEFAULT_MAX_OUTBOUND_WEBSOCKETS: usize = 32;

/// Backoff between dead-owner log recovery retries. One second keeps a
/// waiting takeover responsive to the sweep finishing without hammering
/// the recovery lock.
pub const DEFAULT_OWNER_LOG_RECOVERY_BACKOFF_MS: u64 = 1_000;

/// Recovery retry cycles before a takeover request fails. 240 cycles at
/// the default backoff covers the ~155 s dead-leader sweep measured for
/// a thousand-cell owner with a wide margin;
/// clients bound their own waits far earlier, so the cap only fires on
/// a recovery that is truly wedged.
pub const DEFAULT_OWNER_LOG_RECOVERY_ATTEMPTS: u32 = 240;

pub const DEFAULT_LOCAL_CACHE_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Matches the fleet and ownership-store clients. Long enough to survive a
/// slow but live peer, short enough that a stale address does not hold a
/// request for the kernel's own connect timeout.
pub const PEER_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

pub fn lease_ttl_ms_from_environment() -> u64 {
    crate::env_vars::positive_or("CELLD_TTL_MS", 10_000).expect("validated CELLD_TTL_MS")
}

pub fn ownership_on_evict_from_environment() -> anyhow::Result<OwnershipOnEvict> {
    match std::env::var("CELLD_PRESSURE_OWNERSHIP") {
        Err(_) => Ok(OwnershipOnEvict::Release),
        Ok(value) => match value.trim() {
            "release" => Ok(OwnershipOnEvict::Release),
            "sticky" => Ok(OwnershipOnEvict::Sticky),
            other => Err(anyhow::anyhow!(
                "CELLD_PRESSURE_OWNERSHIP must be `release` or `sticky`, not `{other}`"
            )),
        },
    }
}

/// Total memory this process may use: the cgroup limit when one applies
/// (containers; the process leaf's under `CELLD_MEMORY_SCOPE=self`), the
/// machine otherwise.
#[cfg(target_os = "linux")]
#[allow(clippy::disallowed_methods)] // cgroup and `/proc` files describe the host.
fn total_memory_bytes() -> Option<u64> {
    if let Some(limit) = crate::memory::cgroup_limit_bytes() {
        return Some(limit);
    }
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let kb = meminfo
        .lines()
        .find(|line| line.starts_with("MemTotal:"))?
        .split_whitespace()
        .nth(1)?
        .parse::<u64>()
        .ok()?;
    Some(kb.saturating_mul(1024))
}

#[cfg(target_os = "macos")]
fn total_memory_bytes() -> Option<u64> {
    let mut size: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    let ok = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            (&raw mut size).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    (ok == 0 && size > 0).then_some(size)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn total_memory_bytes() -> Option<u64> {
    None
}

pub fn pressure_config_from_environment() -> anyhow::Result<cello_logic::pressure::PressureConfig> {
    // Residency is a hard cap (`CELLD_MAX_RESIDENT_CELLS` -> `Config::max_resident`),
    // enforced at admission -- not a pressure watermark, so it is not built here.
    // Memory shedding is on by default: a node that runs into its memory ceiling
    // must give cells back, not be killed. The arithmetic lives in the core;
    // the shell supplies only the two facts it can read.
    // Reject a misspelled memory scope here too: startup validation runs
    // first, but the config build must not depend on that ordering, and the
    // sampler itself can only warn and measure the container.
    crate::memory::MemoryScope::from_environment()?;
    let config = cello_logic::pressure::PressureConfig::from_limits(
        total_memory_bytes(),
        crate::env_vars::optional::<u64>("CELLD_MAX_RSS_MB")?,
    );
    if config.ceiling_above_cap() {
        tracing::warn!(
            high_bytes = config.high_bytes,
            rss_hard_bytes = config.rss_hard_bytes,
            "CELLD_MAX_RSS_MB is at or above the absolute cap, so the node \
             decides on its complete cgroup charge or RSS fallback"
        );
    }
    Ok(config)
}

pub fn local_cache_max_bytes_from_environment() -> anyhow::Result<Option<u64>> {
    let bytes = crate::env_vars::with_default(
        "CELLD_LOCAL_CACHE_MAX_BYTES",
        DEFAULT_LOCAL_CACHE_MAX_BYTES,
    )?;
    Ok((bytes > 0).then_some(bytes))
}

#[cfg(all(test, cello_internal_tests))]
mod internal_tests {
    include!(env!("CELLD_INTERNAL_MACHINE_TESTS"));
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // Test tempdirs live outside the injected filesystem.
mod node_id_tests {
    use super::*;

    /// An explicit CELLD_NODE wins and is recorded for the next boot.
    #[test]
    fn explicit_id_wins_and_is_recorded() {
        let dir = tempfile::tempdir().expect("test dir");
        let node = resolve_node_session_id(Some("node_fixed".into()), dir.path());
        assert_eq!(node, "node_fixed");
        assert_eq!(
            std::fs::read_to_string(dir.path().join(NODE_ID_FILE)).expect("pin written"),
            "node_fixed\n"
        );
    }

    /// A restart with no explicit id reuses the pin: the process answers
    /// for its predecessor's leases and fragments (upstream #249).
    /// Fail-before: every boot minted a stranger and recovery looped on
    /// `WrongTarget` forever.
    #[test]
    fn restart_reuses_the_pinned_id() {
        let dir = tempfile::tempdir().expect("test dir");
        let first = resolve_node_session_id(None, dir.path());
        assert!(validate_node_name(&first).is_ok());
        let second = resolve_node_session_id(None, dir.path());
        assert_eq!(second, first, "the restart keeps its predecessor's id");
    }

    /// A corrupt pin warns and regenerates instead of bricking boot or
    /// pinning an unusable identity.
    #[test]
    fn corrupt_pin_regenerates() {
        let dir = tempfile::tempdir().expect("test dir");
        std::fs::write(dir.path().join(NODE_ID_FILE), "not a node id!!!\n").expect("pin written");
        let node = resolve_node_session_id(None, dir.path());
        assert!(validate_node_name(&node).is_ok());
        assert_ne!(node, "not a node id!!!");
    }

    /// An unreadable pin is left untouched: the boot takes an ephemeral id
    /// without minting over the retained fragments. A directory stands in
    /// for the unreadable pin (mode bits are no barrier to root).
    /// Fail-before: the read error fell through to mint-and-record, which
    /// left a `node-id.tmp` beside the pin it could not replace.
    #[test]
    fn unreadable_pin_stays_untouched_and_ephemeral() {
        let dir = tempfile::tempdir().expect("test dir");
        std::fs::create_dir(dir.path().join(NODE_ID_FILE)).expect("pin dir");
        let node = resolve_node_session_id(None, dir.path());
        assert!(validate_node_name(&node).is_ok());
        assert!(
            dir.path().join(NODE_ID_FILE).is_dir(),
            "the unreadable pin is never replaced"
        );
        assert!(
            !dir.path().join(format!("{NODE_ID_FILE}.tmp")).exists(),
            "no mint-and-record attempt touches the pin"
        );
    }

    /// The pin lands through tmp + rename: a stale tmp from a crashed boot
    /// is consumed, never left beside the pin, and the pin carries the id.
    /// Fail-before: `std::fs::write` straight to the pin left the stale
    /// tmp in place.
    #[test]
    fn pin_write_consumes_a_stale_tmp() {
        let dir = tempfile::tempdir().expect("test dir");
        std::fs::write(dir.path().join(format!("{NODE_ID_FILE}.tmp")), "stale\n")
            .expect("stale tmp");
        let node = resolve_node_session_id(Some("node_durable".into()), dir.path());
        assert_eq!(node, "node_durable");
        assert_eq!(
            std::fs::read_to_string(dir.path().join(NODE_ID_FILE)).expect("pin written"),
            "node_durable\n"
        );
        assert!(
            !dir.path().join(format!("{NODE_ID_FILE}.tmp")).exists(),
            "the rename consumes the tmp; no stale tmp survives beside the pin"
        );
    }
}
