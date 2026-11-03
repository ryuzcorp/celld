// Modified by ryuzcorp (ryuzcorp/celld): exec() waits for start(); exec pty/resize; fence node-netns exception; process-granularity exec kill; kill conformance tests; exec reads survive their subscribing event; worker container endpoints for the tenant egress policy; exec-kill tests share the common test runtime; bounded engine control calls; tiered lifecycle bound; orphan cleanup on failed start and exec; capped retry loops; binding-forgery enforcement.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Containers attached to Durable Objects: the engine side of
//! `ctx.container`.
//!
//! A cell of a class named in the deployment's `containers` config owns at
//! most one container, named after the cell, on the node that owns the
//! cell. The Durable Object supervises it through the ops in
//! `js/container.rs`; this module performs the effects against the
//! container engine, which is the Docker Engine API on a unix socket
//! (Podman serves the same API).
//!
//! The container is bound to the cell's ownership on this node, not to its
//! residency: an idle eviction leaves it running under an inactivity timer
//! and the next activation of the same cell reconnects to it by name, which
//! is what Cloudflare does and what the `@cloudflare/containers` class's
//! `sleepAfter` alarm relies on. Every other stop destroys it. The
//! container's disk is ephemeral on Cloudflare too, so a takeover that
//! starts fresh is conformant.
//!
//! Images travel through the bucket. `celld deploy` saves the image the
//! config names as a tar at `deploy/images/<id>.tar`, and a node loads it
//! into its engine the first time a cell of that class starts, so a node
//! never talks to a registry.

use crate::asyncrt;
use crate::docker::{frame_header, Docker, Stream, LIFECYCLE_TIMEOUT};
use anyhow::{anyhow, Context};
use bytes::Bytes;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

/// One `containers[]` entry of a deployment, as the node sees it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContainerSpec {
    pub class_name: String,
    /// The image reference the engine starts, `celld-image:<content key>`,
    /// so two deployments of one image share one tar and one load.
    pub image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_instances: Option<u64>,
    /// The OCI runtime this class runs under, such as `runsc` (gVisor) for
    /// untrusted code. Overrides the node's `CELLD_CONTAINER_RUNTIME` for
    /// this class; `None` takes the node default. A node whose daemon does
    /// not have the runtime fails every start of the class, so the class
    /// runs only where its isolation is available. celld extends the
    /// Cloudflare config here, which has no per-class runtime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
}

/// The reference every engine holds an image under, from its content key.
/// The key hashes the layer digests and the image config, so an engine
/// that rebuilds identical content under a new id still resolves it.
pub fn image_reference(key: &str) -> String {
    format!("celld-image:{key}")
}

/// The bucket key of one saved image, from its reference.
pub fn image_key(image: &str) -> String {
    format!(
        "deploy/images/{}.tar",
        image.trim_start_matches("celld-image:")
    )
}

/// The resources an instance type reserves. Cloudflare's published table;
/// a name outside it is refused at deploy.
pub fn instance_resources(instance_type: &str) -> Option<(f64, u64)> {
    let gib = 1024 * 1024 * 1024;
    Some(match instance_type {
        "lite" | "dev" => (1.0 / 16.0, 256 * 1024 * 1024),
        "basic" => (0.25, gib),
        "standard-1" | "standard" => (0.5, 4 * gib),
        "standard-2" => (1.0, 6 * gib),
        "standard-3" => (2.0, 8 * gib),
        "standard-4" => (4.0, 12 * gib),
        _ => return None,
    })
}

/// The instance type of a class that declares none, as on Cloudflare.
const DEFAULT_INSTANCE_TYPE: &str = "dev";

/// The resolvers a container gets when it runs under a non-default runtime
/// and the operator set none. gVisor's network stack does not reach
/// Docker's embedded resolver at `127.0.0.11`, so a container under it
/// cannot resolve a hostname though it reaches the Internet by address; a
/// public resolver, which the fence permits, restores name resolution.
/// `CELLD_CONTAINER_DNS` overrides this.
const DEFAULT_CONTAINER_DNS: &[&str] = &["1.1.1.1", "1.0.0.1"];

/// The memory a container of this instance type reserves on the node. A
/// class that declares none gets the default type, as `create_and_start`
/// does. An unknown name is refused at deploy, so a running container
/// always maps; this returns 0 for a name that somehow does not, which
/// undercounts rather than blocks a sample.
pub fn instance_memory_bytes(instance_type: Option<&str>) -> u64 {
    instance_resources(instance_type.unwrap_or(DEFAULT_INSTANCE_TYPE))
        .map(|(_, memory)| memory)
        .unwrap_or(0)
}

/// The memory this node commits to its running containers, for the node's
/// capacity accounting. Zero when no engine has connected. See
/// `celld_logic::pressure::Load::container_reserved_bytes` for why the node
/// counts the cap rather than the container's live usage.
pub fn reserved_memory_bytes() -> u64 {
    engine_if_ready().map_or(0, |engine| engine.reserved_memory_bytes())
}

/// This node's running containers per class, published in the node lease so
/// peers can sum a class's instances across the fleet for `max_instances`.
/// Empty when no engine has connected.
pub fn running_instances_by_class() -> std::collections::BTreeMap<String, u64> {
    engine_if_ready().map_or_else(Default::default, |engine| {
        engine.running_instances_by_class()
    })
}
/// Processes one container can hold. Cloudflare publishes no number; this
/// is room for a build tool's process tree and far short of a fork bomb.
const PIDS_LIMIT: u64 = 1024;
/// The host interfaces of the two bridges, named so the fence can address
/// them; Docker's default `br-<id>` changes with every recreation.
const OPEN_BRIDGE: &str = "celld0";
const INTERNAL_BRIDGE: &str = "celld1";
const BRIDGE_NAME_OPTION: &str = "com.docker.network.bridge.name";
/// The fence, installed on the node from a one-shot privileged container of
/// the `celld-fence` image: a container may reach the Internet and nothing
/// of the node's own. Hooks before Docker's own chains (priority filter -
/// 10) so a verdict here is final. Input: no new connection from a bridge
/// to the node itself, which is where the internal listener and the public
/// listener bind; replies to connections the node opened still pass.
/// Forward: nothing to the private ranges, where the fleet, the VPC, and
/// the metadata service live. Rules on the host side of the bridge cover
/// every runtime, gVisor included, and no process inside a container can
/// see them, let alone remove them.
const FENCE_RULES: &str = r#"table inet celld
delete table inet celld
table inet celld {
  chain input {
    type filter hook input priority -10; policy accept;
    iifname { "celld0", "celld1" } ct state established,related accept
    iifname { "celld0", "celld1" } reject
  }
  chain forward {
    type filter hook forward priority -10; policy accept;
    iifname { "celld0", "celld1" } ip daddr { 169.254.0.0/16, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10 } reject
    iifname { "celld0", "celld1" } ip6 daddr { fe80::/10, fc00::/7 } reject
  }
}
"#;

/// Docker's bridge option that forbids traffic between two containers on
/// the same bridge. The node still reaches every container, because it
/// speaks from the host side of the bridge.
const ICC_OPTION: &str = "com.docker.network.bridge.enable_icc";

/// The forward chain's private-range reject lines, as anchors for the
/// node-netns exception. They must match `FENCE_RULES` verbatim, exactly
/// once each; the fence tests pin that.
const FENCE_FORWARD_REJECT_V4: &str = r#"    iifname { "celld0", "celld1" } ip daddr { 169.254.0.0/16, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10 } reject"#;
const FENCE_FORWARD_REJECT_V6: &str =
    r#"    iifname { "celld0", "celld1" } ip6 daddr { fe80::/10, fc00::/7 } reject"#;

/// The private ranges the forward chain rejects, as data mirroring
/// `FENCE_RULES` (the fence tests keep the two in sync). A node-netns
/// exception is only ever cut for an address inside these ranges: traffic
/// to anything outside them is never fenced, so an exception for it would
/// be a no-op.
const FENCE_REJECT_V4: &[(Ipv4Addr, u32)] = &[
    (Ipv4Addr::new(169, 254, 0, 0), 16),
    (Ipv4Addr::new(10, 0, 0, 0), 8),
    (Ipv4Addr::new(172, 16, 0, 0), 12),
    (Ipv4Addr::new(192, 168, 0, 0), 16),
    (Ipv4Addr::new(100, 64, 0, 0), 10),
];
const FENCE_REJECT_V6: &[(Ipv6Addr, u32)] = &[
    (Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 0), 10),
    (Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 0), 7),
];

/// `CELLD_FENCE_NODE_NETNS`: `auto` cuts the node-netns exception into the
/// fence (see `fence_ruleset`); unset or `off` installs `FENCE_RULES`
/// unchanged. Anything else fails the fence install, so no container
/// starts on a node whose fence mode is misspelled.
const FENCE_NODE_NETNS_ENV: &str = "CELLD_FENCE_NODE_NETNS";
/// `CELLD_FENCE_NODE_ADDRS`: comma-separated IPs the exception covers
/// instead of the discovered addresses. Nodes sharing one engine install
/// one shared table, so each must pin the same union of node addresses;
/// the last install wins, and a union keeps every node's replies passing.
const FENCE_NODE_ADDRS_ENV: &str = "CELLD_FENCE_NODE_ADDRS";

/// The node-netns fence mode of this node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FenceNodeNetns {
    Off,
    Auto,
}

impl FenceNodeNetns {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value.trim() {
            "" | "off" => Ok(Self::Off),
            "auto" => Ok(Self::Auto),
            other => Err(anyhow!(
                "{FENCE_NODE_NETNS_ENV} is {other:?}: want \"off\" or \"auto\""
            )),
        }
    }

    fn from_env() -> anyhow::Result<Self> {
        match std::env::var(FENCE_NODE_NETNS_ENV) {
            Ok(value) => Self::parse(&value),
            Err(std::env::VarError::NotPresent) => Ok(Self::Off),
            Err(error) => Err(anyhow!(
                "{FENCE_NODE_NETNS_ENV} is not valid unicode: {error}"
            )),
        }
    }
}

fn v4_contains(net: Ipv4Addr, prefix: u32, addr: Ipv4Addr) -> bool {
    if !(1..=32).contains(&prefix) {
        return false;
    }
    let mask = !0u32 << (32 - prefix);
    u32::from(addr) & mask == u32::from(net) & mask
}

fn v6_contains(net: Ipv6Addr, prefix: u32, addr: Ipv6Addr) -> bool {
    if !(1..=128).contains(&prefix) {
        return false;
    }
    let (addr, net) = (addr.octets(), net.octets());
    let full = (prefix / 8) as usize;
    if addr[..full] != net[..full] {
        return false;
    }
    let rest = prefix % 8;
    if rest == 0 {
        return true;
    }
    let mask = !0u8 << (8 - rest);
    addr[full] & mask == net[full] & mask
}

/// Whether the forward chain rejects traffic to `addr`.
fn fenced_range(addr: &IpAddr) -> bool {
    match addr {
        IpAddr::V4(addr) => FENCE_REJECT_V4
            .iter()
            .any(|(net, prefix)| v4_contains(*net, *prefix, *addr)),
        IpAddr::V6(addr) => FENCE_REJECT_V6
            .iter()
            .any(|(net, prefix)| v6_contains(*net, *prefix, *addr)),
    }
}

/// The exception-worthy subset of `candidates`: assigned addresses (never
/// loopback or unspecified) inside the fenced ranges, sorted and
/// deduplicated so the rendered ruleset is deterministic.
fn select_fence_node_addrs(candidates: impl IntoIterator<Item = IpAddr>) -> Vec<IpAddr> {
    let mut out: Vec<IpAddr> = candidates
        .into_iter()
        .filter(|addr| match addr {
            IpAddr::V4(addr) => !addr.is_loopback() && !addr.is_unspecified(),
            IpAddr::V6(addr) => !addr.is_loopback() && !addr.is_unspecified(),
        })
        .filter(fenced_range)
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Parse `CELLD_FENCE_NODE_ADDRS`: comma-separated exact IPs. Anything that
/// is not an IP — a hostname, a CIDR, a typo — fails the fence install
/// outright, so a misspelled exception list never silently fences the wrong
/// addresses. The survivors go through the same selection as discovered
/// addresses, so out-of-range entries (whose traffic the fence never
/// touches) are dropped as documented no-ops.
fn parse_fence_node_addrs(value: &str) -> anyhow::Result<Vec<IpAddr>> {
    let mut out = Vec::new();
    for part in value.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let addr: IpAddr = part.parse().map_err(|_| {
            anyhow!("{FENCE_NODE_ADDRS_ENV} has an entry that is not an IP address: {part:?}")
        })?;
        out.push(addr);
    }
    Ok(select_fence_node_addrs(out))
}

/// The node's own addresses from this netns, for the fence exception. The
/// fence installs on the engine host while the node dials from here, so
/// these are the source addresses whose replies cross the forward chain.
#[cfg(target_os = "linux")]
fn discover_node_addrs() -> Vec<IpAddr> {
    let mut found = Vec::new();
    // SAFETY: getifaddrs/freeifaddrs pairing; the list is only read, and
    // each sockaddr is read only as the family it claims.
    unsafe {
        let mut list: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut list) != 0 || list.is_null() {
            return Vec::new();
        }
        let mut item = list;
        while !item.is_null() {
            let ifa = &*item;
            if !ifa.ifa_addr.is_null() {
                let family = (*ifa.ifa_addr).sa_family as libc::c_int;
                if family == libc::AF_INET {
                    let addr = std::ptr::read_unaligned(ifa.ifa_addr as *const libc::sockaddr_in);
                    found.push(IpAddr::V4(Ipv4Addr::from(u32::from_be(
                        addr.sin_addr.s_addr,
                    ))));
                } else if family == libc::AF_INET6 {
                    let addr = std::ptr::read_unaligned(ifa.ifa_addr as *const libc::sockaddr_in6);
                    found.push(IpAddr::V6(Ipv6Addr::from(addr.sin6_addr.s6_addr)));
                }
            }
            item = ifa.ifa_next;
        }
        libc::freeifaddrs(list);
    }
    select_fence_node_addrs(found)
}

/// Discovery needs the node's netns to be the engine host's own, which only
/// a Linux node has: off Linux, `auto` without a pinned list installs the
/// unchanged fence, and an operator with a remote engine pins
/// `CELLD_FENCE_NODE_ADDRS` explicitly.
#[cfg(not(target_os = "linux"))]
fn discover_node_addrs() -> Vec<IpAddr> {
    tracing::warn!(
        event = "container_fence_no_discovery",
        "node address discovery needs Linux; set {FENCE_NODE_ADDRS_ENV} to cut the exception, or the fence installs unchanged"
    );
    Vec::new()
}

/// The fence with a node-netns exception for `node_addrs`: a node that
/// itself lives in a container netns dials its containers from a private
/// address, and the forward rejects drop the replies, so the node cannot
/// reach its own containers.
///
/// An empty list installs `FENCE_RULES` byte for byte, so `auto` with no
/// addresses and every `off` node run exactly today's ruleset. Otherwise one
/// accept line per family goes directly ahead of that family's private-range
/// reject, admitting only `established,related` packets toward the node's
/// own addresses: replies to connections the node opened. A container can
/// never open a connection through the exception — a new connection's first
/// packet is never established — so the node's listeners stay closed to its
/// containers exactly as the input chain keeps them on a host-netns node.
/// `related` rides along deliberately, as in the input chain: it admits the
/// ICMP errors (unreachable, packet-too-big) that belong to a live flow, so
/// path-MTU discovery keeps working; it opens nothing without that flow.
/// The exception says nothing about which containers the node may dial; a
/// hoster running mutually untrusting tenants on one engine still enforces
/// that above celld.
fn fence_ruleset(node_addrs: &[IpAddr]) -> String {
    let mut v4: Vec<Ipv4Addr> = Vec::new();
    let mut v6: Vec<Ipv6Addr> = Vec::new();
    for addr in node_addrs {
        match addr {
            IpAddr::V4(addr) => v4.push(*addr),
            IpAddr::V6(addr) => v6.push(*addr),
        }
    }
    if v4.is_empty() && v6.is_empty() {
        return FENCE_RULES.to_string();
    }
    v4.sort();
    v4.dedup();
    v6.sort();
    v6.dedup();
    let mut rules = FENCE_RULES.to_string();
    // The anchors are pinned to `FENCE_RULES` by the fence tests; if one
    // ever misses, `replacen` leaves that family without an exception,
    // which fails closed (broken replies, never a hole).
    if !v4.is_empty() {
        let line = format!(
            "    iifname {{ \"celld0\", \"celld1\" }} ip daddr {{ {} }} ct state established,related accept\n{FENCE_FORWARD_REJECT_V4}",
            v4.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
        rules = rules.replacen(FENCE_FORWARD_REJECT_V4, &line, 1);
    }
    if !v6.is_empty() {
        let line = format!(
            "    iifname {{ \"celld0\", \"celld1\" }} ip6 daddr {{ {} }} ct state established,related accept\n{FENCE_FORWARD_REJECT_V6}",
            v6.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        );
        rules = rules.replacen(FENCE_FORWARD_REJECT_V6, &line, 1);
    }
    rules
}

/// The ruleset `ensure_fence` installs: `FENCE_RULES` unless
/// `CELLD_FENCE_NODE_NETNS=auto` cuts the node-netns exception. Every
/// misconfiguration fails here, before any container starts: an unknown
/// mode, an unreadable address list, or an unparseable entry all refuse the
/// fence rather than install a surprising one.
fn fence_rules_for_install() -> anyhow::Result<String> {
    if FenceNodeNetns::from_env()? == FenceNodeNetns::Off {
        return Ok(FENCE_RULES.to_string());
    }
    let addrs = fence_node_addrs(std::env::var(FENCE_NODE_ADDRS_ENV))?;
    if addrs.is_empty() {
        tracing::warn!(
            event = "container_fence_no_node_addrs",
            "auto found no node address in the fenced ranges; the fence installs unchanged"
        );
    } else {
        tracing::info!(
            event = "container_fence_node_addrs",
            addrs = %addrs
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(","),
            "the fence admits replies to the node's own addresses"
        );
    }
    Ok(fence_ruleset(&addrs))
}

/// The node addresses `auto` admits replies to, from the read of
/// `CELLD_FENCE_NODE_ADDRS`: unset discovers them, a list pins them.
fn fence_node_addrs(value: Result<String, std::env::VarError>) -> anyhow::Result<Vec<IpAddr>> {
    match value {
        // Set but listing nothing is ambiguous — "discover" or "no
        // exception" — so it refuses rather than guess.
        Ok(value) if value.split(',').all(|part| part.trim().is_empty()) => Err(anyhow!(
            "{FENCE_NODE_ADDRS_ENV} is set but empty; unset it to discover the node's \
             addresses or list them"
        )),
        Ok(value) => parse_fence_node_addrs(&value),
        Err(std::env::VarError::NotPresent) => Ok(discover_node_addrs()),
        Err(error) => Err(anyhow!(
            "{FENCE_NODE_ADDRS_ENV} is not valid unicode: {error}"
        )),
    }
}

/// How a cell stop treats its container.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Release {
    /// An idle eviction: keep the container under its inactivity timer.
    Keep,
    /// Ownership leaves this node, or the cell is reset: destroy it.
    Destroy,
}

/// The default inactivity window after an idle eviction when the object
/// never set one. The `@cloudflare/containers` alarm wakes the object well
/// inside this to enforce `sleepAfter`, so this is the backstop for an
/// object that never wakes, not the policy.
const DEFAULT_INACTIVITY: Duration = Duration::from_secs(10 * 60);

/// Environment every container sees. The values mirror workerd's local
/// engine: applications read the names, never the values.
const DEFAULT_ENV: &[&str] = &[
    "CLOUDFLARE_COUNTRY_A2=XX",
    "CLOUDFLARE_DEPLOYMENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "CLOUDFLARE_LOCATION=loc01",
    "CLOUDFLARE_REGION=REGN",
    "CLOUDFLARE_APPLICATION_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
];

pub struct StartParams {
    pub entrypoint: Option<Vec<String>>,
    pub env: Vec<(String, String)>,
    pub enable_internet: bool,
    pub labels: Vec<(String, String)>,
}

#[derive(Default)]
struct CellState {
    running: bool,
    /// Where the node dials the container: the bridge address on Linux,
    /// the published loopback ports elsewhere.
    address: Address,
    inactivity: Option<Duration>,
    /// The pending destroy after an idle eviction. Dropping it cancels.
    sweeper: Option<asyncrt::TaskHandle<()>>,
    /// Bumped per start so a wait from a previous run cannot report for
    /// this one.
    run: u64,
}

#[derive(Clone, Debug, Default)]
enum Address {
    #[default]
    None,
    Ip(String),
    Published(HashMap<u16, u16>),
}

pub struct CellContainer {
    scope: String,
    name: String,
    spec: Arc<ContainerSpec>,
    state: Mutex<CellState>,
    /// Processes `exec()` started in this container. An object drops one
    /// after `output()`; the rest go with the container.
    processes: Mutex<Vec<u64>>,
    /// The exit of the current run, `None` while it runs. `Some(Err)` is
    /// an engine failure the wait could not attribute to the process.
    /// Written with `send_replace`: a plain `send` discards the value
    /// while nobody subscribes, and `monitor()` usually subscribes late.
    exit: watch::Sender<Option<(u64, Result<i64, String>)>>,
    /// The newest run whose start has finished, created or failed. The
    /// object's `start()` answers before the container exists, as on
    /// Cloudflare, where the calls that follow it on the same handle wait
    /// for it; `exec()` waits on this.
    started: watch::Sender<u64>,
}

impl CellContainer {
    pub fn running(&self) -> bool {
        self.state.lock().unwrap().running
    }

    /// `host:port` for `getTcpPort(port)`.
    pub fn address(&self, port: u16) -> Result<String, String> {
        let state = self.state.lock().unwrap();
        if !state.running {
            return Err("the container is not running".to_string());
        }
        match &state.address {
            Address::Ip(ip) => Ok(format!("{ip}:{port}")),
            Address::Published(ports) => ports
                .get(&port)
                .map(|host| format!("127.0.0.1:{host}"))
                .ok_or_else(|| {
                    format!(
                        "The container is not listening on port {port}: on this platform a port \
                         must be declared with EXPOSE in the image"
                    )
                }),
            Address::None => Err(format!("The container is not listening on port {port}")),
        }
    }

    pub fn set_inactivity(&self, duration: Duration) {
        self.state.lock().unwrap().inactivity = Some(duration);
    }

    /// Open the next run, synchronously, before the start is even queued.
    /// The object's `start()` returns at once and its `monitor()` follows
    /// immediately, so the run they both mean must exist before either
    /// effect runs, or the monitor would find the previous run's exit and
    /// report the new container dead on arrival.
    pub fn begin_run(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        state.run += 1;
        state.running = true;
        state.address = Address::None;
        let run = state.run;
        drop(state);
        self.exit.send_replace(None);
        run
    }

    /// Mark `run`'s start finished (an older run never moves the mark back).
    pub fn finish_start(&self, run: u64) {
        self.started.send_if_modified(|done| {
            let newer = run > *done;
            if newer {
                *done = run;
            }
            newer
        });
    }

    /// The run `monitor()` waits on: the newest one.
    pub fn current_run(&self) -> u64 {
        self.state.lock().unwrap().run
    }
}

/// A run whose start is in flight: dropping it marks the start finished,
/// whichever way the start ends (done, failed, or cancelled and dropped),
/// so an `exec()` waiting on the run never waits forever.
pub struct StartGuard {
    cell: Arc<CellContainer>,
    run: u64,
}

impl StartGuard {
    pub fn new(cell: Arc<CellContainer>, run: u64) -> Self {
        Self { cell, run }
    }

    pub fn cell(&self) -> &Arc<CellContainer> {
        &self.cell
    }

    pub fn run(&self) -> u64 {
        self.run
    }
}

impl Drop for StartGuard {
    fn drop(&mut self) {
        self.cell.finish_start(self.run);
    }
}

pub struct ContainerEngine {
    /// See `Config::runtime`.
    runtime: Option<String>,
    /// See `Config::dns`.
    dns: Vec<String>,
    /// The absolute path of the container resolv.conf this engine wrote and
    /// bind-mounts; `None` when it could not be written.
    resolv_conf: Option<PathBuf>,
    docker: Docker,
    node: String,
    bucket: Option<crate::bucket::Bucket>,
    /// Per-image load lock, so two cells of one class starting together
    /// load the tar once.
    images: Mutex<HashMap<String, Arc<tokio::sync::Mutex<bool>>>>,
    cells: Mutex<HashMap<String, Arc<CellContainer>>>,
    networks: tokio::sync::Mutex<Option<(String, String)>>,
    /// Whether this process has installed the fence on the node's bridges.
    fenced: tokio::sync::Mutex<bool>,
}

/// What the runtime told this module about the node: set once at start.
struct Config {
    node: String,
    bucket: Option<crate::bucket::Bucket>,
    /// `CELLD_CONTAINER_RUNTIME`: the OCI runtime every container starts
    /// under, such as `runsc` (gVisor) or `kata` (a VM). `None` is the
    /// daemon's default, which is `runc` unless the operator changed it. A
    /// class can override it, see `ContainerSpec::runtime`.
    runtime: Option<String>,
    /// `CELLD_CONTAINER_DNS`: the resolvers a container gets, overriding
    /// `DEFAULT_CONTAINER_DNS`. Empty takes the default when a runtime is in
    /// use and the daemon's otherwise.
    dns: Vec<String>,
    /// The node's state directory. celld writes the container resolv.conf
    /// here and bind-mounts it, so the path must be the same on the host as
    /// celld sees it; a self-hosted node runs celld as a host process, and
    /// the lab mounts the directory one-to-one.
    data_dir: PathBuf,
}

static CONFIG: RwLock<Option<Config>> = RwLock::new(None);
/// The deployment's container classes. Replaced on every generation swap;
/// a running container keeps the spec it started with.
static SPECS: RwLock<Vec<Arc<ContainerSpec>>> = RwLock::new(Vec::new());
/// The deployment's `celld-fence` image; see `Manifest::fence_image`.
static FENCE_IMAGE: RwLock<Option<String>> = RwLock::new(None);
/// The engine, connected on first use. A node whose deployment declares no
/// container class never opens the socket.
static ENGINE: tokio::sync::OnceCell<Arc<ContainerEngine>> = tokio::sync::OnceCell::const_new();

pub fn configure(node: String, bucket: Option<crate::bucket::Bucket>, data_dir: PathBuf) {
    let runtime = std::env::var("CELLD_CONTAINER_RUNTIME")
        .ok()
        .filter(|runtime| !runtime.is_empty());
    let dns = std::env::var("CELLD_CONTAINER_DNS")
        .unwrap_or_default()
        .split(',')
        .map(|resolver| resolver.trim().to_string())
        .filter(|resolver| !resolver.is_empty())
        .collect();
    *CONFIG.write().unwrap() = Some(Config {
        node,
        bucket,
        runtime,
        dns,
        data_dir,
    });
}

pub fn install_specs(specs: Vec<ContainerSpec>, fence_image: Option<String>) {
    *SPECS.write().unwrap() = specs.into_iter().map(Arc::new).collect();
    *FENCE_IMAGE.write().unwrap() = fence_image;
}

pub fn spec(class: &str) -> Option<Arc<ContainerSpec>> {
    SPECS
        .read()
        .unwrap()
        .iter()
        .find(|spec| spec.class_name == class)
        .cloned()
}

/// The engine, connecting on the first call. A failure is returned rather
/// than cached, so a daemon that comes up later is found by the next call.
pub async fn engine() -> anyhow::Result<Arc<ContainerEngine>> {
    ENGINE
        .get_or_try_init(|| async {
            let (node, bucket, runtime, dns, data_dir) = {
                let config = CONFIG.read().unwrap();
                let config = config.as_ref().ok_or_else(|| {
                    anyhow!("the container engine is not configured on this node")
                })?;
                (
                    config.node.clone(),
                    config.bucket.clone(),
                    config.runtime.clone(),
                    config.dns.clone(),
                    config.data_dir.clone(),
                )
            };
            let docker = Docker::discover().ok_or_else(|| {
                anyhow!(
                    "no container engine: set DOCKER_HOST to a unix socket, or run a Docker or \
                     Podman daemon on this node"
                )
            })?;
            Ok(Arc::new(
                ContainerEngine::connect(docker, node, bucket, runtime, dns, data_dir).await?,
            ))
        })
        .await
        .cloned()
}

/// The engine only if a previous call connected it.
pub fn engine_if_ready() -> Option<Arc<ContainerEngine>> {
    ENGINE.get().cloned()
}

/// Destroy every container of this node at process exit. A preserve
/// shutdown (`celld dev` on Ctrl-C) keeps its cells resident and stops
/// none of them, and a handoff cut by the deadline leaves cells behind
/// too; without this their containers ran on until the next start of the
/// same node reaped them, invisible to the object that owned them.
pub async fn shutdown() {
    let Some(engine) = engine_if_ready() else {
        return;
    };
    if let Err(error) = engine.reap().await {
        tracing::warn!(
            event = "container_shutdown_reap_failed",
            error = %format!("{error:#}"),
            "containers of this node may still be running"
        );
    }
}

/// Connect and load every image of the installed specs, ahead of the
/// first cell that needs one. Failures are logged: a node without an
/// engine still serves every other class, and the container class fails
/// at its first `start()` with the same message.
pub async fn prewarm() {
    let engine = match engine().await {
        Ok(engine) => engine,
        Err(error) => {
            tracing::warn!(error = %format!("{error:#}"), "containers are unavailable on this node");
            return;
        }
    };
    if let Err(error) = engine.ensure_fence().await {
        tracing::warn!(
            error = %format!("{error:#}"),
            "the container bridges are not fenced; no container starts on this node"
        );
    }
    let specs = SPECS.read().unwrap().clone();
    for spec in specs {
        if let Err(error) = engine.ensure_image(&spec.image).await {
            tracing::warn!(
                class = %spec.class_name,
                image = %spec.image,
                error = %format!("{error:#}"),
                "container image is unavailable"
            );
        }
    }
}

impl ContainerEngine {
    /// Connect to the engine and reap every container a previous process
    /// of this node left behind. A restarted node cannot know which of its
    /// containers still belong to cells it will own again, and a container
    /// whose object has lost track of it is a leak, so a restart starts
    /// clean. The next `start()` of each object creates a fresh one.
    pub async fn connect(
        docker: Docker,
        node: String,
        bucket: Option<crate::bucket::Bucket>,
        runtime: Option<String>,
        dns: Vec<String>,
        data_dir: PathBuf,
    ) -> anyhow::Result<Self> {
        // Write the resolv.conf once, to bind into every container that runs
        // under a non-default runtime. gVisor cannot reach Docker's embedded
        // resolver, so a bind-mounted file with a public resolver, which the
        // fence permits, is the only resolv.conf the container can use.
        let resolvers: Vec<String> = if dns.is_empty() {
            DEFAULT_CONTAINER_DNS
                .iter()
                .map(|r| r.to_string())
                .collect()
        } else {
            dns.clone()
        };
        let resolv_conf = {
            let path = data_dir.join("container-resolv.conf");
            let body: String = resolvers
                .iter()
                .map(|resolver| format!("nameserver {resolver}\n"))
                .collect();
            let fs = asyncrt::fs();
            match fs
                .create_dir_all(&data_dir)
                .and_then(|()| fs.write(&path, body.as_bytes()))
            {
                Ok(()) => Some(path),
                Err(error) => {
                    tracing::warn!(event = "container_resolv_write_failed", %error);
                    None
                }
            }
        };
        let engine = Self {
            docker,
            node,
            bucket,
            runtime,
            dns,
            resolv_conf,
            images: Mutex::new(HashMap::new()),
            cells: Mutex::new(HashMap::new()),
            networks: tokio::sync::Mutex::new(None),
            fenced: tokio::sync::Mutex::new(false),
        };
        engine.reap().await?;
        Ok(engine)
    }

    pub fn socket(&self) -> &std::path::Path {
        self.docker.socket()
    }

    async fn reap(&self) -> anyhow::Result<()> {
        let filters = json!({ "label": [format!("celld.node={}", self.node)] }).to_string();
        let reply = self
            .docker
            .expect_control(
                "GET",
                &format!(
                    "/containers/json?all=true&filters={}",
                    percent_encoding::utf8_percent_encode(
                        &filters,
                        percent_encoding::NON_ALPHANUMERIC
                    )
                ),
                None,
                "list containers",
            )
            .await?;
        let list = reply.json()?;
        for entry in list.as_array().into_iter().flatten() {
            if let Some(id) = entry.get("Id").and_then(Value::as_str) {
                let _ = self
                    .docker
                    .call_control(
                        "DELETE",
                        &format!("/containers/{id}?force=true"),
                        None,
                        "remove container",
                    )
                    .await;
            }
        }
        Ok(())
    }

    /// Make the class's image present in the engine, loading it from the
    /// bucket when it is not. Idempotent and serialized per image.
    pub async fn ensure_image(&self, image: &str) -> anyhow::Result<()> {
        let lock = self
            .images
            .lock()
            .unwrap()
            .entry(image.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(false)))
            .clone();
        let mut loaded = lock.lock().await;
        if *loaded {
            return Ok(());
        }
        let reply = self
            .docker
            .call_control(
                "GET",
                &format!("/images/{image}/json"),
                None,
                "inspect image",
            )
            .await?;
        if reply.status.is_success() {
            tracing::info!(image, "container image present in the engine");
            *loaded = true;
            return Ok(());
        }
        let bucket = self
            .bucket
            .as_ref()
            .ok_or_else(|| anyhow!("image {image} is not present in the container engine"))?;
        let key = image_key(image);
        let (tar, _) = bucket.get(&key).await?.ok_or_else(|| {
            anyhow!("image {image} is not in the bucket at {key}; run `celld deploy`")
        })?;
        let bytes = tar.len();
        // An image transfer, not a control call: sized by the image,
        // unbounded.
        let reply = self
            .docker
            .post_octets("/images/load?quiet=true", tar, "load image")
            .await?;
        tracing::info!(
            image,
            bytes,
            answer = %String::from_utf8_lossy(&reply.body).trim_end(),
            "container image loaded from the bucket"
        );
        *loaded = true;
        Ok(())
    }

    /// The two bridges every container joins: one with egress, one
    /// without. Docker implements an internal network by omitting the
    /// masquerade rule and the default route; the node still reaches the
    /// bridge address, which is all ingress needs.
    async fn networks(&self) -> anyhow::Result<(String, String)> {
        let mut guard = self.networks.lock().await;
        if let Some(names) = guard.as_ref() {
            return Ok(names.clone());
        }
        for (name, internal) in [("celld", false), ("celld-internal", true)] {
            // A bridge keeps the options it was created with, so one from
            // before containers were isolated from each other is replaced.
            // The replacement fails while a container is attached, which a
            // node start after its reap never has; a failure keeps the old
            // bridge and says so rather than refusing every container.
            let current = self
                .docker
                .call_control("GET", &format!("/networks/{name}"), None, "inspect network")
                .await?;
            if current.status.is_success() {
                let bridge = if internal {
                    INTERNAL_BRIDGE
                } else {
                    OPEN_BRIDGE
                };
                let current_options = current
                    .json()
                    .ok()
                    .and_then(|network| network.get("Options").cloned())
                    .unwrap_or(Value::Null);
                let option = |key: &str| current_options.get(key).and_then(Value::as_str);
                if option(ICC_OPTION) == Some("false") && option(BRIDGE_NAME_OPTION) == Some(bridge)
                {
                    continue;
                }
                let removed = self
                    .docker
                    .call_control(
                        "DELETE",
                        &format!("/networks/{name}"),
                        None,
                        "remove network",
                    )
                    .await?;
                if !removed.status.is_success() {
                    tracing::warn!(
                        network = name,
                        error = %removed.message(),
                        "the container bridge predates container isolation and is in use; \
                         containers on it can reach each other until the node restarts idle"
                    );
                    continue;
                }
            }
            let reply = self
                .docker
                .call_control(
                    "POST",
                    "/networks/create",
                    Some(json!({
                        "Name": name,
                        "Driver": "bridge",
                        "Internal": internal,
                        "Options": {
                            ICC_OPTION: "false",
                            BRIDGE_NAME_OPTION: if internal { INTERNAL_BRIDGE } else { OPEN_BRIDGE },
                        },
                    })),
                    "create network",
                )
                .await?;
            // 409: it exists, which is the steady state.
            if !reply.status.is_success() && reply.status.as_u16() != 409 {
                return Err(anyhow!(
                    "create network {name} failed with [{}] {}",
                    reply.status.as_u16(),
                    reply.message()
                ));
            }
        }
        let names = ("celld".to_string(), "celld-internal".to_string());
        *guard = Some(names.clone());
        Ok(names)
    }

    /// Install the fence on the node's bridges, once per process. Every
    /// container start waits on this and fails when it fails: a node that
    /// cannot fence its bridges runs no container, because an unfenced
    /// container can reach the node's internal listener and the cloud's
    /// metadata service. The rules live in the kernel and outlive this
    /// process; a restart re-applies them, which is idempotent.
    pub async fn ensure_fence(&self) -> anyhow::Result<()> {
        let mut fenced = self.fenced.lock().await;
        if *fenced {
            return Ok(());
        }
        let image = FENCE_IMAGE.read().unwrap().clone().ok_or_else(|| {
            anyhow!(
                "this deployment has no fence image; deploy it again with this celld, which \
                 saves the celld-fence image beside the container images"
            )
        })?;
        self.ensure_image(&image).await?;
        self.networks().await?;
        let rules = fence_rules_for_install()?;
        let body = json!({
            "Image": image,
            "Env": [format!("CELLD_NFT={rules}")],
            "Cmd": ["sh", "-c", "printf '%s' \"$CELLD_NFT\" | nft -f -"],
            "Labels": { "celld.node": self.node, "celld.fence": "1" },
            "HostConfig": { "NetworkMode": "host", "CapAdd": ["NET_ADMIN"] },
        });
        // A timed-out create may leave a stopped fence container whose
        // id this call never learned; it carries the celld.node label,
        // so the next restart's reap removes it. Accepted as-is: eager
        // cleanup by label is not worth the extra calls for a stopped,
        // tiny orphan, and `fenced` stays false so the next start
        // retries.
        let created = self
            .docker
            .expect_lifecycle(
                "POST",
                "/containers/create",
                Some(body),
                "create fence container",
            )
            .await?
            .json()?;
        let id = created
            .get("Id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("create fence container answered without an id"))?
            .to_string();
        let outcome = async {
            self.docker
                .expect_lifecycle(
                    "POST",
                    &format!("/containers/{id}/start"),
                    None,
                    "start fence container",
                )
                .await?;
            // A wait, not a control call: inherently long-polled, unbounded.
            let waited = self
                .docker
                .expect(
                    "POST",
                    &format!("/containers/{id}/wait"),
                    None,
                    "wait fence container",
                )
                .await?
                .json()?;
            let code = waited
                .get("StatusCode")
                .and_then(Value::as_i64)
                .unwrap_or(-1);
            if code != 0 {
                let logs = self.logs(&id).await.unwrap_or_default();
                return Err(anyhow!(
                    "the fence container exited with {code}: {}",
                    logs.trim()
                ));
            }
            Ok(())
        }
        .await;
        let _ = self
            .docker
            .call_lifecycle(
                "DELETE",
                &format!("/containers/{id}?force=true"),
                None,
                "remove fence container",
            )
            .await;
        outcome.context("fence the container bridges")?;
        tracing::info!(
            event = "container_bridges_fenced",
            bridges = format!("{OPEN_BRIDGE},{INTERNAL_BRIDGE}"),
            "containers can reach the Internet and nothing of the node's own"
        );
        *fenced = true;
        Ok(())
    }

    /// Both output streams of a stopped container, for an error message.
    /// The sole caller is the fence error path, reading bytes of nft
    /// output; `tail` bounds the body structurally so the control bound
    /// stays safe whatever the caller reads.
    async fn logs(&self, id: &str) -> anyhow::Result<String> {
        let reply = self
            .docker
            .expect_control("GET", &container_logs_path(id), None, "container logs")
            .await?;
        let mut text = Vec::new();
        let mut rest: &[u8] = &reply.body;
        while rest.len() >= 8 {
            let (_, length) = frame_header(rest[..8].try_into().unwrap());
            let end = (8 + length).min(rest.len());
            text.extend_from_slice(&rest[8..end]);
            rest = &rest[end..];
        }
        Ok(String::from_utf8_lossy(&text).into_owned())
    }

    /// The cell's container handle, adopting a container a previous
    /// activation on this node left running. Called at cell start for a
    /// class with a spec; the object's `running` reads the answer.
    pub async fn attach(&self, scope: &str, class: &str) -> anyhow::Result<Arc<CellContainer>> {
        let spec = spec(class).ok_or_else(|| anyhow!("class {class} has no container"))?;
        let cell = {
            let mut cells = self.cells.lock().unwrap();
            let cell = cells.entry(scope.to_string()).or_insert_with(|| {
                Arc::new(CellContainer {
                    scope: scope.to_string(),
                    name: container_name(&self.node, scope),
                    spec,
                    state: Mutex::new(CellState::default()),
                    processes: Mutex::new(Vec::new()),
                    exit: watch::channel(None).0,
                    started: watch::channel(0).0,
                })
            });
            // A returning cell cancels the destroy its eviction armed.
            cell.state.lock().unwrap().sweeper = None;
            cell.clone()
        };
        let running = self.inspect_running(&cell).await?;
        // A container this handle did not start, left running by an
        // earlier owner of the name: give it a run so `monitor()` has an
        // exit to wait for. The run opens before the address lands, because
        // opening one clears it.
        let adopted = running.is_some() && !cell.running();
        if adopted {
            let run = cell.begin_run();
            cell.finish_start(run);
            self.watch_exit(&cell, run);
        }
        let mut state = cell.state.lock().unwrap();
        state.running = running.is_some();
        state.address = running.unwrap_or_default();
        drop(state);
        Ok(cell)
    }

    /// The cell's handle, if the cell started on this node.
    pub fn cell(&self, scope: &str) -> Option<Arc<CellContainer>> {
        self.cells.lock().unwrap().get(scope).cloned()
    }

    /// Whether the node published `(ip, port)` to Workers as a container
    /// endpoint: a bridge address on Linux (every port reaches the same
    /// container), or exactly `127.0.0.1` plus a mapped host port where
    /// ports are published. Only running containers count, so a stale
    /// address stops matching when its container stops. The tenant
    /// egress policy exempts these endpoints, because the `getTcpPort()`
    /// path dials them by design.
    pub fn is_worker_container_endpoint(&self, ip: std::net::IpAddr, port: u16) -> bool {
        use std::net::IpAddr;
        for cell in self.cells.lock().unwrap().values() {
            let state = cell.state.lock().unwrap();
            if !state.running {
                continue;
            }
            match &state.address {
                Address::Ip(addr) => {
                    if addr.parse::<IpAddr>().is_ok_and(|known| known == ip) {
                        return true;
                    }
                }
                Address::Published(ports) => {
                    if ip == IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                        && ports.values().any(|mapped| *mapped == port)
                    {
                        return true;
                    }
                }
                Address::None => {}
            }
        }
        false
    }

    /// Whether `ip` is one of this node's container bridge addresses,
    /// without regard to port. Only the Linux form qualifies: a bridge
    /// IP names its container on every port, while a published loopback
    /// port cannot be verified without the port.
    pub fn is_worker_container_ip(&self, ip: std::net::IpAddr) -> bool {
        use std::net::IpAddr;
        for cell in self.cells.lock().unwrap().values() {
            let state = cell.state.lock().unwrap();
            if !state.running {
                continue;
            }
            if let Address::Ip(addr) = &state.address {
                if addr.parse::<IpAddr>().is_ok_and(|known| known == ip) {
                    return true;
                }
            }
        }
        false
    }

    /// The memory the node's running containers reserve, summed over their
    /// instance-type caps. A cell whose container is not running reserves
    /// nothing: a stopped container holds no memory, and an idle eviction
    /// stops it before the sample would count it.
    pub fn reserved_memory_bytes(&self) -> u64 {
        self.cells
            .lock()
            .unwrap()
            .values()
            .filter(|cell| cell.running())
            .map(|cell| instance_memory_bytes(cell.spec.instance_type.as_deref()))
            .sum()
    }

    /// This node's running containers per class.
    fn running_instances_by_class(&self) -> std::collections::BTreeMap<String, u64> {
        let mut counts = std::collections::BTreeMap::new();
        for cell in self.cells.lock().unwrap().values() {
            if cell.running() {
                *counts.entry(cell.spec.class_name.clone()).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Refuse a start that would put the class over its fleet-wide
    /// `max_instances`. The fleet count is this node's own running
    /// containers of the class, read live, plus every peer's count from the
    /// shared capacity sample; `begin_run` already marked the starting cell
    /// running, so the live count includes it. A class with no
    /// `max_instances`, or a node with no bucket to read the sample from as
    /// under `celld dev`, has no fleet ceiling. See
    /// [`crate::ownership_store::fleet_class_instances`] for the staleness
    /// the sample admits.
    async fn enforce_instance_ceiling(&self, cell: &CellContainer) -> anyhow::Result<()> {
        let Some(max) = cell.spec.max_instances else {
            return Ok(());
        };
        let class = &cell.spec.class_name;
        let here = self
            .cells
            .lock()
            .unwrap()
            .values()
            .filter(|other| other.running() && other.spec.class_name == *class)
            .count() as u64;
        let elsewhere = match &self.bucket {
            Some(bucket) => {
                crate::ownership_store::fleet_class_instances(bucket, class, &self.node).await
            }
            None => 0,
        };
        anyhow::ensure!(
            here + elsewhere <= max,
            "container class {class} is at its max_instances limit of {max}: \
             {here} on this node and {elsewhere} on other nodes",
        );
        Ok(())
    }

    async fn inspect_running(&self, cell: &CellContainer) -> anyhow::Result<Option<Address>> {
        let reply = self
            .docker
            .call_control(
                "GET",
                &format!("/containers/{}/json", cell.name),
                None,
                "inspect container",
            )
            .await?;
        if reply.status.as_u16() == 404 {
            return Ok(None);
        }
        if !reply.status.is_success() {
            return Err(anyhow!(
                "inspect container failed with [{}] {}",
                reply.status.as_u16(),
                reply.message()
            ));
        }
        let info = reply.json()?;
        let running = info
            .pointer("/State/Running")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        Ok(running.then(|| address_of(&info)))
    }

    /// Start the cell's container. The object's `start()` returns before
    /// this completes, as on Cloudflare; a failure surfaces through
    /// `monitor()`.
    /// Start the cell's container for the run `begin_run` opened. The
    /// object's `start()` returns before this completes, as on Cloudflare;
    /// a failure surfaces through `monitor()`.
    pub async fn start(
        &self,
        cell: &Arc<CellContainer>,
        run: u64,
        params: StartParams,
    ) -> anyhow::Result<()> {
        let started = self.create_and_start(cell, params).await;
        let address = match started {
            Ok(address) => address,
            Err(error) => {
                // `start()` is fire-and-forget for the object, and its
                // `monitor()` sees this error only if it was already
                // waiting; say it here too, or a start that fails fast
                // leaves no trace anywhere.
                tracing::warn!(
                    event = "container_start_failed",
                    cell = %cell.scope,
                    error = %format!("{error:#}"),
                    "the container did not start"
                );
                let mut state = cell.state.lock().unwrap();
                state.running = false;
                drop(state);
                cell.exit
                    .send_replace(Some((run, Err(format!("{error:#}")))));
                cell.finish_start(run);
                return Err(error);
            }
        };
        {
            let mut state = cell.state.lock().unwrap();
            state.running = true;
            state.address = address;
        }
        self.watch_exit(cell, run);
        cell.finish_start(run);
        Ok(())
    }

    /// Wait for the container's root process to end and publish the exit
    /// for `run`; a later run's start has already replaced the state.
    fn watch_exit(&self, cell: &Arc<CellContainer>, run: u64) {
        let docker = self.docker.clone();
        let cell_ = cell.clone();
        asyncrt::spawn(async move {
            // A wait, not a control call: the container runs indefinitely.
            let result = docker
                .call("POST", &format!("/containers/{}/wait", cell_.name), None)
                .await
                .and_then(|reply| {
                    if !reply.status.is_success() {
                        return Err(anyhow!(
                            "wait failed with [{}] {}",
                            reply.status.as_u16(),
                            reply.message()
                        ));
                    }
                    reply
                        .json()?
                        .get("StatusCode")
                        .and_then(Value::as_i64)
                        .ok_or_else(|| anyhow!("wait answered without a status code"))
                })
                .map_err(|error| format!("{error:#}"));
            let mut state = cell_.state.lock().unwrap();
            if state.run == run {
                state.running = false;
                state.address = Address::None;
            }
            drop(state);
            cell_.exit.send_replace(Some((run, result)));
        })
        .detach();
    }

    async fn create_and_start(
        &self,
        cell: &CellContainer,
        params: StartParams,
    ) -> anyhow::Result<Address> {
        self.ensure_fence().await?;
        self.enforce_instance_ceiling(cell).await?;
        self.ensure_image(&cell.spec.image).await?;
        let (open, internal) = self.networks().await?;
        // A previous run's container may still be named: the wait task
        // observed its exit but nothing removed it, or a restart adopted a
        // stopped one. The name is the cell's, so it is ours to remove.
        let _ = self
            .docker
            .call_lifecycle(
                "DELETE",
                &format!("/containers/{}?force=true", cell.name),
                None,
                "remove container",
            )
            .await;
        let mut env: Vec<String> = DEFAULT_ENV.iter().map(|entry| entry.to_string()).collect();
        env.push(format!("CLOUDFLARE_DURABLE_OBJECT_ID={}", cell.scope));
        // A container's own env must not carry an exec's kill marker, or a
        // kill scan would signal the container's main process.
        env.extend(
            params
                .env
                .iter()
                .filter(|(name, _)| name != EXEC_ID_ENV)
                .map(|(name, value)| format!("{name}={value}")),
        );
        let mut labels = serde_json::Map::new();
        labels.insert("celld.node".into(), json!(self.node));
        labels.insert("celld.cell".into(), json!(cell.scope));
        labels.insert("celld.class".into(), json!(cell.spec.class_name));
        for (name, value) in &params.labels {
            labels.insert(format!("celld.user.{name}"), json!(value));
        }
        // Off Linux the node reaches a container only through published
        // ports, and Docker publishes nothing on an internal network, so
        // `enableInternet: false` would make the container unreachable.
        // Development there keeps egress; the compat page says so.
        let offline = !params.enable_internet && cfg!(target_os = "linux");
        if !params.enable_internet && !offline {
            tracing::warn!(
                cell = %cell.scope,
                "enableInternet: false is not enforced on this platform; the container keeps egress"
            );
        }
        // A container is a tenant's process tree, not the operator's: no
        // capability, no privilege gain through setuid binaries, the
        // daemon's seccomp profile, and a process ceiling. What a class
        // legitimately needs beyond this is a config question for later,
        // not a default. The kernel boundary itself is the runtime's.
        let mut host_config = json!({
            "NetworkMode": if offline { &internal } else { &open },
            "PublishAllPorts": !cfg!(target_os = "linux"),
            "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges"],
            "PidsLimit": PIDS_LIMIT,
            // An init as PID 1 reaps orphaned children. Without it a process
            // whose parent exits becomes a zombie the container holds until
            // it stops, and enough of them exhaust the PID ceiling; a fleet
            // fork bomb left exactly these behind.
            "Init": true,
        });
        // A class can name its own runtime, else the node default. A class
        // that needs isolation names `runsc`, and the node's daemon must
        // have it or the start fails, so the class runs only where its
        // isolation is real.
        let runtime = cell
            .spec
            .runtime
            .as_deref()
            .or(self.runtime.as_deref())
            .filter(|runtime| !runtime.is_empty());
        if let Some(runtime) = runtime {
            host_config["Runtime"] = json!(runtime);
        }
        // A container under a non-default runtime gets an explicit resolver
        // through a bind-mounted resolv.conf: gVisor cannot reach Docker's
        // embedded resolver at `127.0.0.11`, and on a user bridge Docker
        // keeps that address in resolv.conf whatever `--dns` says, so the
        // file itself must name a reachable resolver. An operator who set
        // `CELLD_CONTAINER_DNS` gets it for every container.
        let wants_resolver = runtime.is_some() || !self.dns.is_empty();
        if wants_resolver {
            if let Some(resolv) = &self.resolv_conf {
                let bind = format!("{}:/etc/resolv.conf:ro", resolv.display());
                host_config["Binds"] = json!([bind]);
            }
        }
        // Every container has a limit: a class without an instance type
        // gets Cloudflare's default type rather than the node.
        let instance_type = cell
            .spec
            .instance_type
            .as_deref()
            .unwrap_or(DEFAULT_INSTANCE_TYPE);
        let (vcpu, memory) =
            instance_resources(instance_type).expect("the deploy refused this instance type");
        host_config["NanoCpus"] = json!((vcpu * 1e9) as u64);
        host_config["Memory"] = json!(memory);
        host_config["MemorySwap"] = json!(memory);
        let mut body = json!({
            "Image": cell.spec.image,
            "Env": env,
            "Labels": labels,
            "HostConfig": host_config,
        });
        if let Some(entrypoint) = params.entrypoint {
            body["Cmd"] = json!(entrypoint);
        }
        // Everything from the first create on may leave a container
        // behind: a timed-out start or post-start inspect still lands
        // late on the daemon, RUNNING, with no watcher armed and state
        // saying stopped. Any error at or after the create therefore
        // force-removes the name before returning, best-effort and
        // idempotent (a 404 is fine); only a double failure leaves an
        // orphan, for the next start, eviction, or restart to reap.
        // The pre-create path above (fence, ceiling, image, networks)
        // created nothing and stays outside.
        let started = async {
            // The daemon can answer 409 for a name whose previous container is
            // still being removed, so a fresh start after `destroy()` retries
            // briefly, as workerd's engine does.
            let mut reply = self
                .docker
                .call_lifecycle(
                    "POST",
                    &format!("/containers/create?name={}", cell.name),
                    Some(body.clone()),
                    "create container",
                )
                .await?;
            let loop_started = asyncrt::mono_us();
            for _ in 0..20 {
                if reply.status.as_u16() != 409 {
                    break;
                }
                // Aggregate cap: each iteration makes two lifecycle
                // calls, and a start runs inside the IoContext
                // lifetime, so the loop must not spin for minutes.
                if aggregate_budget_spent(loop_started, asyncrt::mono_us()) {
                    break;
                }
                asyncrt::sleep(Duration::from_millis(100)).await;
                let _ = self
                    .docker
                    .call_lifecycle(
                        "DELETE",
                        &format!("/containers/{}?force=true", cell.name),
                        None,
                        "remove container",
                    )
                    .await;
                reply = self
                    .docker
                    .call_lifecycle(
                        "POST",
                        &format!("/containers/create?name={}", cell.name),
                        Some(body.clone()),
                        "create container",
                    )
                    .await?;
            }
            if reply.status.as_u16() == 404 {
                return Err(anyhow!("No such image available named {}", cell.spec.image));
            }
            if !reply.status.is_success() {
                return Err(anyhow!(
                    "Create container failed with [{}] {}",
                    reply.status.as_u16(),
                    reply.message()
                ));
            }
            self.docker
                .expect_lifecycle(
                    "POST",
                    &format!("/containers/{}/start", cell.name),
                    None,
                    "start container",
                )
                .await?;
            let info = self
                .docker
                .expect_control(
                    "GET",
                    &format!("/containers/{}/json", cell.name),
                    None,
                    "inspect container",
                )
                .await?
                .json()?;
            Ok::<_, anyhow::Error>(address_of(&info))
        }
        .await;
        if started.is_err() {
            // Best-effort: the start error is what the caller sees.
            if let Err(cleanup) = self
                .docker
                .call_lifecycle(
                    "DELETE",
                    &format!("/containers/{}?force=true", cell.name),
                    None,
                    "remove container",
                )
                .await
            {
                tracing::warn!(
                    event = "container_orphan_cleanup_failed",
                    cell = %cell.scope,
                    error = %format!("{cleanup:#}"),
                    "a failed start could not remove its container; the next start, eviction, or restart reaps it"
                );
            }
        }
        started
    }

    /// Wait for the current run to end. `Ok(code)` is the root process's
    /// exit code; a destroyed container reports 137.
    pub async fn monitor(&self, cell: &Arc<CellContainer>, run: u64) -> Result<i64, String> {
        let mut receiver = cell.exit.subscribe();
        loop {
            if let Some((ended, result)) = receiver.borrow_and_update().clone() {
                if ended >= run {
                    return result;
                }
            }
            if receiver.changed().await.is_err() {
                return Err("the container engine went away".to_string());
            }
        }
    }

    pub async fn destroy(&self, cell: &Arc<CellContainer>) -> anyhow::Result<()> {
        // Kill first so the wait task reports 137 before the removal makes
        // the name disappear under it.
        let _ = self
            .docker
            .call_control(
                "POST",
                &format!("/containers/{}/kill?signal=SIGKILL", cell.name),
                None,
                "kill container",
            )
            .await;
        let _ = self
            .docker
            .call_control(
                "DELETE",
                &format!("/containers/{}?force=true", cell.name),
                None,
                "remove container",
            )
            .await;
        let mut state = cell.state.lock().unwrap();
        state.running = false;
        state.address = Address::None;
        Ok(())
    }

    pub async fn signal(&self, cell: &Arc<CellContainer>, signal: u32) -> anyhow::Result<()> {
        self.docker
            .expect_control(
                "POST",
                &format!("/containers/{}/kill?signal={signal}", cell.name),
                None,
                "signal container",
            )
            .await?;
        Ok(())
    }

    /// The cell left this node's runtime. An idle eviction keeps the
    /// container for its inactivity window; anything else destroys it, and
    /// the stop waits for that: a drain ends the process right after its
    /// last stop, and a destroy left to a detached task did not always get
    /// its two daemon calls in before the exit.
    pub async fn release(self: &Arc<Self>, scope: &str, release: Release) {
        let Some(cell) = self.cell(scope) else {
            return;
        };
        match release {
            Release::Keep => {
                let window = cell
                    .state
                    .lock()
                    .unwrap()
                    .inactivity
                    .unwrap_or(DEFAULT_INACTIVITY);
                let engine = self.clone();
                let cell_ = cell.clone();
                let sweeper = asyncrt::spawn(async move {
                    asyncrt::sleep(window).await;
                    engine.forget(&cell_).await;
                });
                cell.state.lock().unwrap().sweeper = Some(sweeper);
            }
            Release::Destroy => self.forget(&cell).await,
        }
    }

    async fn forget(&self, cell: &Arc<CellContainer>) {
        let _ = self.destroy(cell).await;
        for id in cell.processes.lock().unwrap().drain(..) {
            drop_process(id);
        }
        let mut cells = self.cells.lock().unwrap();
        if cells
            .get(&cell.scope)
            .is_some_and(|current| Arc::ptr_eq(current, cell))
        {
            cells.remove(&cell.scope);
        }
    }

    pub async fn exec(
        &self,
        cell: &Arc<CellContainer>,
        mut params: ExecParams,
    ) -> anyhow::Result<Arc<ExecProcess>> {
        // An exec right after `start()` runs in the container that start
        // creates, as on Cloudflare: wait for it instead of answering the
        // engine's "no such container".
        let run = cell.current_run();
        let _ = cell.started.subscribe().wait_for(|done| *done >= run).await;
        if !cell.running() {
            return Err(anyhow!("exec() requires a running container."));
        }
        // Fail before the daemon creates anything: a size rides only with a
        // PTY, as a positive pair. The op already enforces this for JS
        // callers; the engine refuses it here so a direct caller cannot
        // orphan a started exec on a resize the daemon would refuse.
        match (params.pty, params.cols, params.rows) {
            (false, None, None) | (true, None, None) => {}
            (true, Some(cols), Some(rows)) if cols != 0 && rows != 0 => {}
            (false, _, _) => return Err(anyhow!("exec(): pty size without pty")),
            _ => return Err(anyhow!("exec(): pty size must be positive cols and rows")),
        }
        // The id doubles as the process's in-container marker (see
        // `kill_via_exec`), so it is minted before the daemon creates
        // anything. A forged marker in the caller's env would let one
        // exec's kill signal another, so it goes first.
        let id = next_exec_id();
        params.env.retain(|(name, _)| name != EXEC_ID_ENV);
        params.env.push((EXEC_ID_ENV.to_string(), format!("{id};")));
        let created = self
            .docker
            .expect_control(
                "POST",
                &format!("/containers/{}/exec", cell.name),
                Some(exec_create_body(&params)),
                "create exec",
            )
            .await?
            .json()?;
        let exec_id = created
            .get("Id")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("exec create answered without an id"))?
            .to_string();
        // The stream attach, not a control call: the connection outlives
        // the call by design and stays unbounded.
        let stream = self
            .docker
            .hijack(
                &format!("/exec/{exec_id}/start"),
                json!({ "Detach": false, "Tty": params.pty }),
            )
            .await?;
        // Docker reports `Running: false` with no pid before it has spawned
        // the process, and a finished exec keeps its pid, so a pid of zero
        // is the one answer that means "not yet": retry it briefly, as
        // workerd does. Breaking on `Running: false` here lost the pid of a
        // short command whenever the inspect landed in that window.
        let polled = async {
            let mut pid = 0;
            let loop_started = asyncrt::mono_us();
            for _ in 0..20 {
                // Aggregate cap, as in the create 409 loop.
                if aggregate_budget_spent(loop_started, asyncrt::mono_us()) {
                    break;
                }
                let info = self
                    .docker
                    .expect_control(
                        "GET",
                        &format!("/exec/{exec_id}/json"),
                        None,
                        "inspect exec",
                    )
                    .await?
                    .json()?;
                pid = info.get("Pid").and_then(Value::as_i64).unwrap_or(0);
                if pid != 0 {
                    break;
                }
                asyncrt::sleep(Duration::from_millis(50)).await;
            }
            Ok::<_, anyhow::Error>(pid)
        }
        .await;
        let pid = match polled {
            Ok(pid) => pid,
            Err(error) => {
                // The daemon-side process is already running, and no
                // handle is ever registered on this path; kill it so a
                // failed inspect doesn't orphan a process, mirroring the
                // resize cleanup below. Best-effort: the inspect error
                // is what the caller sees.
                let _ =
                    kill_via_exec(&self.docker, &cell.name, id, 9, params.user.as_deref()).await;
                return Err(error);
            }
        };
        if params.pty {
            if let (Some(cols), Some(rows)) = (params.cols, params.rows) {
                if let Err(resize_err) = self
                    .docker
                    .expect_control(
                        "POST",
                        &exec_resize_path(&exec_id, cols, rows),
                        None,
                        "resize exec",
                    )
                    .await
                {
                    // The daemon-side process is already running; kill it so
                    // a failed resize doesn't orphan a process with no handle.
                    // Best-effort: the resize error is what the caller sees.
                    let _ = kill_via_exec(&self.docker, &cell.name, id, 9, params.user.as_deref())
                        .await;
                    return Err(resize_err);
                }
            }
        }
        let (read, write) = tokio::io::split(stream);
        let (stdout_tx, stdout_rx) = mpsc::channel(16);
        let (stderr_tx, stderr_rx) = mpsc::channel(16);
        let combined = params.combined;
        let is_pty = params.pty;
        let ended = watch::channel(false).0;
        let ended_ = ended.clone();
        if is_pty {
            // A TTY stream is raw bytes, not multiplexed frames, with
            // stderr merged into stdout by the daemon; there is no
            // separate stderr, so its sender is dropped at once.
            drop(stderr_tx);
            asyncrt::spawn(async move {
                pump_raw(read, stdout_tx).await;
                ended_.send_replace(true);
            })
            .detach();
        } else {
            asyncrt::spawn(async move {
                demux(read, stdout_tx, stderr_tx, combined).await;
                ended_.send_replace(true);
            })
            .detach();
        }
        let process = Arc::new(ExecProcess {
            id,
            exec_id,
            container: cell.name.clone(),
            scope: cell.scope.clone(),
            pid,
            user: params.user.clone(),
            is_pty,
            docker: self.docker.clone(),
            stdin: tokio::sync::Mutex::new(Some(write)),
            stdout: tokio::sync::Mutex::new(ExecPipe::new(stdout_rx)),
            stderr: tokio::sync::Mutex::new(ExecPipe::new(stderr_rx)),
            ended,
            exit_code: tokio::sync::Mutex::new(None),
        });
        processes()
            .lock()
            .unwrap()
            .insert(process.id, process.clone());
        cell.processes.lock().unwrap().push(process.id);
        Ok(process)
    }
}

pub struct ExecParams {
    pub cmd: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<String>,
    pub user: Option<String>,
    /// stderr folded into stdout.
    pub combined: bool,
    /// Allocate a pseudo-TTY (`Tty`), with an initial size when set.
    pub pty: bool,
    pub cols: Option<u16>,
    pub rows: Option<u16>,
}

/// The `POST /containers/{id}/exec` body for one exec, pure so tests pin the
/// `Tty` shape without an engine.
fn exec_create_body(params: &ExecParams) -> Value {
    let mut body = json!({
        "AttachStdin": true,
        "AttachStdout": true,
        "AttachStderr": true,
        "Tty": params.pty,
        "Cmd": params.cmd,
    });
    if !params.env.is_empty() {
        body["Env"] = json!(params
            .env
            .iter()
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>());
    }
    if let Some(cwd) = &params.cwd {
        body["WorkingDir"] = json!(cwd);
    }
    if let Some(user) = &params.user {
        body["User"] = json!(user);
    }
    body
}

/// The `POST /exec/{id}/resize` path: `h` is rows, `w` is cols.
fn exec_resize_path(exec_id: &str, cols: u16, rows: u16) -> String {
    format!("/exec/{exec_id}/resize?h={rows}&w={cols}")
}

/// The env var that marks one exec's process for its kill: `exec()`
/// sets it to `<id>;`, and the kill scan matches that exact entry. The
/// trailing `;` keeps id 12 from matching id 123.
const EXEC_ID_ENV: &str = "CELLD_EXEC_ID";

/// The shell scan that signals one exec's process: every `/proc` entry
/// whose environment carries the exec's marker gets the signal. `kill`
/// is the shell builtin, so the image needs no `kill` binary; it does
/// need `sh`, `grep`, and `/proc`. Both interpolations are numeric, so
/// the command cannot inject.
fn kill_scan_script(id: u64, signal: u32) -> String {
    format!(
        "for d in /proc/[0-9]*;do grep -qa \"{EXEC_ID_ENV}={id};\" \"$d/environ\" 2>/dev/null&&kill -{signal} \"${{d#/proc/}}\";done"
    )
}

/// The `POST /containers/{id}/exec` body that runs the kill scan, pure
/// so tests pin the shape without an engine. Runs as the exec's user,
/// so it can read the target's environment.
fn kill_exec_body(id: u64, signal: u32, user: Option<&str>) -> Value {
    let mut body = json!({
        "AttachStdin": false, "AttachStdout": false, "AttachStderr": false,
        "Cmd": ["sh", "-c", kill_scan_script(id, signal)],
    });
    if let Some(user) = user {
        body["User"] = json!(user);
    }
    body
}

/// Deliver a signal by a second exec that scans for the process's
/// marker: the Engine API has no exec kill, and the pid the daemon
/// reports is a host pid, meaningless inside the container's pid
/// namespace. Used by `ExecProcess::kill` and, best-effort with
/// SIGKILL, to clean up a daemon-side process whose post-start resize
/// failed before its handle was registered.
///
/// A scan that matches nothing signals nothing: a dead exec's marker
/// died with it, so unlike a pid the marker cannot be reused onto an
/// innocent process. An exec that forked children signals them too,
/// since they inherit the marker. An image without `sh`, `grep`, or
/// `/proc` degrades to a no-op kill, as before, but never misdelivers.
async fn kill_via_exec(
    docker: &Docker,
    container: &str,
    id: u64,
    signal: u32,
    user: Option<&str>,
) -> anyhow::Result<()> {
    let created = docker
        .expect_control(
            "POST",
            &format!("/containers/{container}/exec"),
            Some(kill_exec_body(id, signal, user)),
            "create exec",
        )
        .await?
        .json()?;
    let exec = created
        .get("Id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("exec create answered without an id"))?;
    docker
        .expect_control(
            "POST",
            &format!("/exec/{exec}/start"),
            Some(json!({ "Detach": true })),
            "start exec",
        )
        .await?;
    Ok(())
}

type WriteHalf = tokio::io::WriteHalf<Box<dyn Stream>>;

/// One exec output pipe (stdout or stderr): the pump's receiver plus the
/// exactly-once state that lets a read survive its subscribing event.
/// A read is one lease (one pull); each event that finds it still
/// pending files a new subscription chained after the previous one.
struct ExecPipe {
    rx: mpsc::Receiver<Bytes>,
    /// The last consumed chunk, keyed by the subscription that consumed
    /// it. A re-subscription chained after a dropped op takes these
    /// bytes instead of reading ahead: the drop may have discarded a
    /// completed result, and reading on would lose the chunk.
    stash: Option<(u64, Bytes)>,
    /// The latest read lease and the subscription that last acted for
    /// it. A late pile-up — a re-subscription from an event that began
    /// while an earlier one was still alive — aborts instead of
    /// consuming the next chunk into a promise nobody awaits.
    slot: Option<(u64, u64)>,
}

impl ExecPipe {
    fn new(rx: mpsc::Receiver<Bytes>) -> Self {
        Self {
            rx,
            stash: None,
            slot: None,
        }
    }
}

/// What one retained read subscription found.
pub enum ExecRead {
    Bytes(Bytes),
    /// Another subscription already acted for this lease: the caller
    /// owns no bytes and must not settle the read.
    Superseded,
}

/// The harness matches this message to swallow a superseded
/// subscription; keep it identical to `__EXEC_READ_SUPERSEDED`.
pub const EXEC_READ_SUPERSEDED: &str = "exec read superseded by a newer subscription";

pub struct ExecProcess {
    pub id: u64,
    exec_id: String,
    container: String,
    /// The cell scope that opened this exec. Ids are process-global
    /// sequential numbers, so the ops refuse an id another cell opened
    /// rather than trust the caller's handle.
    scope: String,
    pub pid: i64,
    user: Option<String>,
    pub is_pty: bool,
    docker: Docker,
    stdin: tokio::sync::Mutex<Option<WriteHalf>>,
    stdout: tokio::sync::Mutex<ExecPipe>,
    stderr: tokio::sync::Mutex<ExecPipe>,
    /// True once the hijacked stream reached EOF, which the daemon sends
    /// when the process exits.
    ended: watch::Sender<bool>,
    exit_code: tokio::sync::Mutex<Option<i64>>,
}

fn processes() -> &'static Mutex<HashMap<u64, Arc<ExecProcess>>> {
    static PROCESSES: OnceLock<Mutex<HashMap<u64, Arc<ExecProcess>>>> = OnceLock::new();
    PROCESSES.get_or_init(Default::default)
}

fn next_exec_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// Fresh retained-read sequence numbers, starting at 1 (`0` is "no
/// previous subscription"). One counter serves leases and
/// subscriptions alike; uniqueness is all the pipe slot and stash
/// compare.
pub fn next_exec_read_seq() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

pub fn process(id: u64) -> Option<Arc<ExecProcess>> {
    processes().lock().unwrap().get(&id).cloned()
}

pub fn drop_process(id: u64) {
    processes().lock().unwrap().remove(&id);
}

/// Senders feeding test-only in-memory exec processes, by (process id, pipe).
/// The ops guard every entry point with the test-wait flag; without it
/// this map stays empty and no fake process exists.
type TestProcSenders = Mutex<HashMap<(u64, u8), mpsc::Sender<Bytes>>>;

fn test_proc_senders() -> &'static TestProcSenders {
    static SENDERS: OnceLock<TestProcSenders> = OnceLock::new();
    SENDERS.get_or_init(Default::default)
}

/// Spawn a test-only in-memory exec process: a real table entry whose pipes a test
/// feeds with [`test_proc_emit`] instead of a daemon pump, so the
/// cross-event read path runs where no container engine exists. Only the
/// read path is usable: `wait`/`kill`/`resize` would dial the dummy
/// socket and fail, and the harness never calls them on a fake. `scope`
/// is the spawning cell, like the real exec path records: the read and
/// drop ops refuse an id another cell opened.
pub fn test_proc_spawn(scope: &str) -> u64 {
    let id = next_exec_id();
    let (stdout_tx, stdout_rx) = mpsc::channel(16);
    let (stderr_tx, stderr_rx) = mpsc::channel(16);
    let process = Arc::new(ExecProcess {
        id,
        exec_id: format!("test-exec-{id}"),
        container: "test-container".to_string(),
        scope: scope.to_string(),
        pid: 0,
        user: None,
        is_pty: false,
        docker: Docker::new("/nonexistent-celld-test.sock"),
        stdin: tokio::sync::Mutex::new(None),
        stdout: tokio::sync::Mutex::new(ExecPipe::new(stdout_rx)),
        stderr: tokio::sync::Mutex::new(ExecPipe::new(stderr_rx)),
        ended: watch::channel(false).0,
        exit_code: tokio::sync::Mutex::new(None),
    });
    processes().lock().unwrap().insert(id, process);
    let mut senders = test_proc_senders().lock().unwrap();
    senders.insert((id, 1), stdout_tx);
    senders.insert((id, 2), stderr_tx);
    id
}

/// Push one chunk into a test-only exec pipe. Fails when the pipe is
/// full, so an over-eager test fails loudly instead of wedging the
/// emitting event.
pub fn test_proc_emit(id: u64, which: u8, bytes: Bytes) -> Result<(), String> {
    let senders = test_proc_senders().lock().unwrap();
    let sender = senders
        .get(&(id, which))
        .ok_or_else(|| "no such test proc pipe".to_string())?;
    sender
        .try_send(bytes)
        .map_err(|_| "test proc pipe is full".to_string())
}

/// End one test-only exec pipe: later reads see end of stream.
pub fn test_proc_eof(id: u64, which: u8) {
    test_proc_senders().lock().unwrap().remove(&(id, which));
}

impl ExecProcess {
    /// The cell scope that opened this exec, for the op layer's
    /// ownership check.
    pub(crate) fn owner_scope(&self) -> &str {
        &self.scope
    }

    /// One chunk of stdout (`1`) or stderr (`2`); empty at end of stream.
    ///
    /// `lease` names the read (one pull) and `prev_sub`/`my_sub` chain
    /// this subscription after the previous one, so a read dropped
    /// with its subscribing event resumes in a later one without
    /// losing, duplicating, or reordering bytes. Everything below runs
    /// under the pipe lock, so the stash and slot the next subscriber
    /// sees are exactly what this one left: a completed result the
    /// event discarded is taken from the stash, and a subscription
    /// that arrives after another already acted for the lease comes
    /// back [`ExecRead::Superseded`] instead of consuming the next
    /// chunk into a promise nobody awaits.
    pub async fn read_retained(
        &self,
        which: u8,
        lease: u64,
        prev_sub: u64,
        my_sub: u64,
    ) -> ExecRead {
        let mut pipe = match which {
            1 => self.stdout.lock().await,
            _ => self.stderr.lock().await,
        };
        if pipe
            .slot
            .is_some_and(|(slot_lease, taker)| slot_lease == lease && taker != prev_sub)
        {
            return ExecRead::Superseded;
        }
        if let Some((_, bytes)) = pipe.stash.take_if(|(sub, _)| *sub == prev_sub) {
            pipe.stash = Some((my_sub, bytes.clone()));
            pipe.slot = Some((lease, my_sub));
            return ExecRead::Bytes(bytes);
        }
        let bytes = pipe.rx.recv().await.unwrap_or_default();
        pipe.stash = Some((my_sub, bytes.clone()));
        pipe.slot = Some((lease, my_sub));
        ExecRead::Bytes(bytes)
    }

    pub async fn write(&self, bytes: &[u8]) -> Result<(), String> {
        let mut guard = self.stdin.lock().await;
        let stdin = guard.as_mut().ok_or("stdin is closed")?;
        stdin
            .write_all(bytes)
            .await
            .map_err(|error| format!("stdin write failed: {error}"))?;
        stdin
            .flush()
            .await
            .map_err(|error| format!("stdin flush failed: {error}"))
    }

    /// End stdin. Half-closing the hijacked connection is how the daemon
    /// learns the process's stdin reached EOF.
    pub async fn close_stdin(&self) {
        if let Some(mut stdin) = self.stdin.lock().await.take() {
            let _ = stdin.shutdown().await;
        }
    }

    pub async fn wait(&self) -> anyhow::Result<i64> {
        let mut exit = self.exit_code.lock().await;
        if let Some(code) = *exit {
            return Ok(code);
        }
        let mut ended = self.ended.subscribe();
        while !*ended.borrow_and_update() {
            if ended.changed().await.is_err() {
                break;
            }
        }
        // The stream closes when the process exits, but the daemon records
        // the exit code a moment later.
        let mut code = None;
        let loop_started = asyncrt::mono_us();
        for _ in 0..40 {
            // Aggregate cap, as in the create 409 loop. Falling out
            // without a code keeps the existing error below.
            if aggregate_budget_spent(loop_started, asyncrt::mono_us()) {
                break;
            }
            let info = self
                .docker
                .expect_control(
                    "GET",
                    &format!("/exec/{}/json", self.exec_id),
                    None,
                    "inspect exec",
                )
                .await?
                .json()?;
            if !info
                .get("Running")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                code = info.get("ExitCode").and_then(Value::as_i64);
                break;
            }
            asyncrt::sleep(Duration::from_millis(50)).await;
        }
        let code = code.ok_or_else(|| anyhow!("the process did not report an exit code"))?;
        *exit = Some(code);
        Ok(code)
    }

    /// The Engine API has no exec kill, so the signal is delivered by
    /// a second exec that scans for this process's marker (see
    /// `kill_via_exec`): `self.pid` is a host pid and meaningless
    /// inside the container.
    pub async fn kill(&self, signal: u32) -> anyhow::Result<()> {
        kill_via_exec(
            &self.docker,
            &self.container,
            self.id,
            signal,
            self.user.as_deref(),
        )
        .await
    }

    /// Resize a PTY exec's terminal. Refused on a non-TTY process, where the
    /// daemon has nothing to resize.
    pub async fn resize(&self, cols: u16, rows: u16) -> anyhow::Result<()> {
        if !self.is_pty {
            anyhow::bail!("resize() requires a PTY process");
        }
        if cols == 0 || rows == 0 {
            anyhow::bail!("resize() dimensions must be positive");
        }
        self.docker
            .expect_control(
                "POST",
                &exec_resize_path(&self.exec_id, cols, rows),
                None,
                "resize exec",
            )
            .await?;
        Ok(())
    }
}

/// Forward a TTY exec's raw stream to stdout until the daemon closes it.
/// Unlike [`demux`], there are no multiplex frames to split.
async fn pump_raw(mut read: tokio::io::ReadHalf<Box<dyn Stream>>, stdout: mpsc::Sender<Bytes>) {
    let mut buf = vec![0u8; 8192];
    loop {
        match read.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => {
                if stdout
                    .send(Bytes::copy_from_slice(&buf[..n]))
                    .await
                    .is_err()
                {
                    // The reader went away; keep draining so the process is
                    // not blocked on a full pipe.
                    continue;
                }
            }
        }
    }
}

/// Split Docker's multiplexed stream into stdout and stderr chunks until
/// the daemon closes it.
async fn demux(
    mut read: tokio::io::ReadHalf<Box<dyn Stream>>,
    stdout: mpsc::Sender<Bytes>,
    stderr: mpsc::Sender<Bytes>,
    combined: bool,
) {
    let mut header = [0u8; 8];
    loop {
        if read.read_exact(&mut header).await.is_err() {
            return;
        }
        let (stream, length) = frame_header(&header);
        let mut payload = vec![0u8; length];
        if read.read_exact(&mut payload).await.is_err() {
            return;
        }
        let target = if stream == 2 && !combined {
            &stderr
        } else {
            &stdout
        };
        if target.send(Bytes::from(payload)).await.is_err() {
            // The reader went away; keep draining so the process is not
            // blocked on a full pipe.
            continue;
        }
    }
}

fn address_of(info: &Value) -> Address {
    if cfg!(target_os = "linux") {
        let ip = info
            .pointer("/NetworkSettings/Networks")
            .and_then(Value::as_object)
            .and_then(|networks| networks.values().next())
            .and_then(|network| network.get("IPAddress"))
            .and_then(Value::as_str)
            .filter(|ip| !ip.is_empty());
        return ip.map_or(Address::None, |ip| Address::Ip(ip.to_string()));
    }
    let mut ports = HashMap::new();
    if let Some(map) = info
        .pointer("/NetworkSettings/Ports")
        .and_then(Value::as_object)
    {
        for (key, bindings) in map {
            let Some(port) = key
                .strip_suffix("/tcp")
                .and_then(|port| port.parse::<u16>().ok())
            else {
                continue;
            };
            let host = bindings
                .as_array()
                .into_iter()
                .flatten()
                .find_map(|binding| binding.get("HostPort")?.as_str()?.parse::<u16>().ok());
            if let Some(host) = host {
                ports.insert(port, host);
            }
        }
    }
    Address::Published(ports)
}

/// Whether a retry loop has spent its aggregate wall-clock budget.
/// The per-call bounds cap one call; without this, the 409 loop (20
/// iterations of two lifecycle calls) could pin a start inside the
/// IoContext lifetime for minutes against a slow-but-answering
/// daemon. The loops share `LIFECYCLE_TIMEOUT` as their total, read
/// off the execution-domain monotonic clock; pure over its two stamps
/// so tests pin both branches without a domain.
fn aggregate_budget_spent(start_us: u64, now_us: u64) -> bool {
    Duration::from_micros(now_us.saturating_sub(start_us)) >= LIFECYCLE_TIMEOUT
}

/// The stopped-container logs query: `tail` bounds the body
/// structurally, pure so tests pin the bound without an engine.
fn container_logs_path(id: &str) -> String {
    format!("/containers/{id}/logs?stdout=true&stderr=true&tail=50")
}

/// A Docker name from a node and a cell scope: the scope's characters are
/// not all legal, so the name is a hash and the scope rides in a label.
/// The node is part of it because two nodes can share one engine in a
/// development or test setup, and a name from the scope alone let one
/// node adopt, or reap, the other's container for the same object.
fn container_name(node: &str, scope: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(format!("{node}\n{scope}").as_bytes());
    format!("celld-{:x}", digest)[..30].to_string()
}

#[cfg(test)]
mod start_order_tests {
    use super::*;

    fn cell() -> CellContainer {
        CellContainer {
            scope: "s".into(),
            name: "n".into(),
            spec: Arc::new(ContainerSpec {
                class_name: "C".into(),
                image: "celld-image:x".into(),
                instance_type: None,
                max_instances: None,
                runtime: None,
            }),
            state: Mutex::new(CellState::default()),
            processes: Mutex::new(Vec::new()),
            exit: watch::channel(None).0,
            started: watch::channel(0).0,
        }
    }

    /// An exec issued right after `start()` waits for that start, and only
    /// that one: the mark never moves back for an older run.
    #[tokio::test]
    async fn exec_waits_for_the_current_runs_start() {
        let cell = Arc::new(cell());
        let run = cell.begin_run();
        let mut started = cell.started.subscribe();
        // What `exec()` waits on is not met until the start finishes.
        assert!(*started.borrow() < run, "the run counted as started early");
        cell.finish_start(run);
        started.wait_for(|done| *done >= run).await.unwrap();
        let next = cell.begin_run();
        cell.finish_start(run);
        assert!(*cell.started.borrow() < next, "an older run moved the mark");
        cell.finish_start(next);
        assert_eq!(*cell.started.borrow(), next);
    }

    /// A start cancelled before it finished (its task dropped) still
    /// releases the exec() calls waiting on its run.
    #[test]
    fn a_dropped_start_releases_its_run() {
        let cell = Arc::new(cell());
        let run = cell.begin_run();
        let guard = StartGuard::new(cell.clone(), run);
        assert!(*cell.started.borrow() < run);
        drop(guard);
        assert_eq!(*cell.started.borrow(), run);
    }

    /// The guard moves into the start task when the run opens, so a task
    /// cancelled before it ever runs still releases the run.
    #[test]
    fn a_start_task_dropped_before_it_runs_releases_its_run() {
        let cell = Arc::new(cell());
        let guard = StartGuard::new(cell.clone(), cell.begin_run());
        let run = guard.run();
        let task = async move {
            let _guard = guard;
        };
        assert!(*cell.started.borrow() < run);
        drop(task);
        assert_eq!(*cell.started.borrow(), run);
    }
}

#[cfg(test)]
mod pty_tests {
    use super::*;

    fn params(pty: bool) -> ExecParams {
        ExecParams {
            cmd: vec!["sh".to_string()],
            env: vec![("A".to_string(), "b".to_string())],
            cwd: Some("/work".to_string()),
            user: Some("nobody".to_string()),
            combined: false,
            pty,
            cols: None,
            rows: None,
        }
    }

    #[test]
    fn exec_create_carries_tty() {
        let plain = exec_create_body(&params(false));
        assert_eq!(plain["Tty"], serde_json::json!(false));
        assert_eq!(plain["Cmd"], serde_json::json!(["sh"]));
        assert_eq!(plain["Env"], serde_json::json!(["A=b"]));
        assert_eq!(plain["WorkingDir"], serde_json::json!("/work"));
        assert_eq!(plain["User"], serde_json::json!("nobody"));
        let tty = exec_create_body(&params(true));
        assert_eq!(tty["Tty"], serde_json::json!(true));
        assert_eq!(tty["Cmd"], serde_json::json!(["sh"]));
    }

    #[test]
    fn resize_path_puts_rows_in_h_and_cols_in_w() {
        assert_eq!(
            exec_resize_path("abc123", 100, 30),
            "/exec/abc123/resize?h=30&w=100"
        );
    }

    #[test]
    fn kill_body_scans_for_the_exec_marker() {
        let body = kill_exec_body(12, 9, None);
        assert_eq!(body["Cmd"][0], serde_json::json!("sh"));
        assert_eq!(body["Cmd"][1], serde_json::json!("-c"));
        let script = body["Cmd"][2].as_str().expect("scan script");
        assert!(script.contains("CELLD_EXEC_ID=12;"), "{script}");
        assert!(script.contains("kill -9 "), "{script}");
        assert!(body.get("User").is_none());
        assert_eq!(body["AttachStdin"], serde_json::json!(false));
        assert_eq!(body["AttachStdout"], serde_json::json!(false));
        assert_eq!(body["AttachStderr"], serde_json::json!(false));
        let user = kill_exec_body(12, 15, Some("nobody"));
        assert_eq!(user["User"], serde_json::json!("nobody"));
    }

    fn test_cell() -> CellContainer {
        CellContainer {
            scope: "s".into(),
            name: "n".into(),
            spec: Arc::new(ContainerSpec {
                class_name: "C".into(),
                image: "celld-image:x".into(),
                instance_type: None,
                max_instances: None,
                runtime: None,
            }),
            state: Mutex::new(CellState::default()),
            processes: Mutex::new(Vec::new()),
            exit: watch::channel(None).0,
            started: watch::channel(0).0,
        }
    }

    fn test_engine() -> ContainerEngine {
        ContainerEngine {
            runtime: None,
            dns: Vec::new(),
            resolv_conf: None,
            docker: Docker::new("/nonexistent-celld-test.sock"),
            node: "test".to_string(),
            bucket: None,
            images: Mutex::new(HashMap::new()),
            cells: Mutex::new(HashMap::new()),
            networks: tokio::sync::Mutex::new(None),
            fenced: tokio::sync::Mutex::new(false),
        }
    }

    /// The tenant egress policy exempts exactly the endpoints the node
    /// published: a bridge IP on any port, a published loopback port on
    /// its mapped port only, and nothing once the container stops.
    #[test]
    fn worker_container_endpoints_match_published_addresses() {
        use std::net::{IpAddr, Ipv4Addr};
        let engine = test_engine();
        let cell = Arc::new(test_cell());
        {
            let mut state = cell.state.lock().unwrap();
            state.running = true;
            state.address = Address::Ip("172.18.0.5".to_string());
        }
        engine
            .cells
            .lock()
            .unwrap()
            .insert("s".to_string(), cell.clone());
        let bridge: IpAddr = "172.18.0.5".parse().unwrap();
        assert!(engine.is_worker_container_endpoint(bridge, 80));
        assert!(engine.is_worker_container_endpoint(bridge, 5432));
        assert!(engine.is_worker_container_ip(bridge));
        assert!(!engine.is_worker_container_endpoint("172.18.0.6".parse().unwrap(), 80));
        assert!(!engine.is_worker_container_ip("172.18.0.6".parse().unwrap()));
        // A published port matches only 127.0.0.1 with its mapped port,
        // and never the port-less check.
        {
            let mut state = cell.state.lock().unwrap();
            state.address = Address::Published(HashMap::from([(8080, 49152)]));
        }
        let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert!(engine.is_worker_container_endpoint(loopback, 49152));
        assert!(!engine.is_worker_container_endpoint(loopback, 8080));
        assert!(!engine.is_worker_container_endpoint("127.0.0.2".parse().unwrap(), 49152));
        assert!(!engine.is_worker_container_ip(loopback));
        // A stopped container exempts nothing.
        cell.state.lock().unwrap().running = false;
        assert!(!engine.is_worker_container_endpoint(loopback, 49152));
        assert!(!engine.is_worker_container_endpoint(bridge, 80));
        assert!(!engine.is_worker_container_ip(bridge));
    }

    /// P4: a PTY size that the daemon would refuse fails before the daemon
    /// creates anything, so no started exec is orphaned. The engine points
    /// at an unreachable socket: a validation failure must come back as the
    /// validation message, never as a connection error.
    #[tokio::test]
    async fn exec_rejects_pty_size_before_start() {
        let engine = test_engine();
        let cell = Arc::new(test_cell());
        let run = cell.begin_run();
        cell.finish_start(run);

        let mut bad = params(false);
        bad.cols = Some(80);
        bad.rows = Some(24);
        // `ExecProcess` has no `Debug`, so no `unwrap_err()`; `.err()` needs none.
        let err = engine.exec(&cell, bad).await.err().expect("exec must fail");
        assert!(err.to_string().contains("pty size without pty"), "{err:#}");

        let mut bad = params(true);
        bad.cols = Some(0);
        bad.rows = Some(24);
        let err = engine.exec(&cell, bad).await.err().expect("exec must fail");
        assert!(err.to_string().contains("must be positive"), "{err:#}");

        let mut bad = params(true);
        bad.cols = Some(80);
        bad.rows = None;
        let err = engine.exec(&cell, bad).await.err().expect("exec must fail");
        assert!(err.to_string().contains("must be positive"), "{err:#}");

        // Valid sizes pass validation and reach the daemon (which fails to
        // connect here, proving the validation passed rather than bailed).
        let mut good = params(true);
        good.cols = Some(80);
        good.rows = Some(24);
        let err = engine
            .exec(&cell, good)
            .await
            .err()
            .expect("exec must fail");
        assert!(
            err.to_string().contains("connect to the container engine"),
            "{err:#}"
        );
    }

    fn test_process(is_pty: bool) -> ExecProcess {
        let (_stdout_tx, stdout_rx) = mpsc::channel(1);
        let (_stderr_tx, stderr_rx) = mpsc::channel(1);
        ExecProcess {
            id: 1,
            exec_id: "test-exec".to_string(),
            container: "test-container".to_string(),
            scope: "TestClass:test-cell".to_string(),
            pid: 1234,
            user: None,
            is_pty,
            docker: Docker::new("/nonexistent-celld-test.sock"),
            stdin: tokio::sync::Mutex::new(None),
            stdout: tokio::sync::Mutex::new(ExecPipe::new(stdout_rx)),
            stderr: tokio::sync::Mutex::new(ExecPipe::new(stderr_rx)),
            ended: watch::channel(false).0,
            exit_code: tokio::sync::Mutex::new(None),
        }
    }

    /// `resize()` refuses a non-PTY process and zero dims before touching
    /// the daemon; valid dims reach it.
    #[tokio::test]
    async fn resize_refuses_non_pty_and_zero_dims() {
        let plain = test_process(false);
        let err = plain.resize(80, 24).await.unwrap_err();
        assert!(err.to_string().contains("requires a PTY"), "{err:#}");

        let pty = test_process(true);
        let err = pty.resize(0, 24).await.unwrap_err();
        assert!(err.to_string().contains("must be positive"), "{err:#}");
        let err = pty.resize(80, 0).await.unwrap_err();
        assert!(err.to_string().contains("must be positive"), "{err:#}");

        let err = pty.resize(80, 24).await.unwrap_err();
        assert!(
            err.to_string().contains("connect to the container engine"),
            "{err:#}"
        );
    }
}

#[cfg(test)]
mod fence_node_netns_tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    /// `off` — and `auto` with no addresses — install exactly today's
    /// ruleset: default behavior is byte-identical.
    #[test]
    fn empty_exception_installs_the_legacy_ruleset_byte_for_byte() {
        assert_eq!(fence_ruleset(&[]), FENCE_RULES);
    }

    /// A pin set but empty refuses the fence instead of guessing between
    /// discovery and no exception; a listed pin is used as given.
    #[test]
    fn empty_node_addrs_pin_refuses() {
        for value in ["", " ", " , ", ","] {
            let error = fence_node_addrs(Ok(value.to_string()))
                .expect_err("a pin that lists no address refuses");
            assert!(
                error.to_string().contains(FENCE_NODE_ADDRS_ENV),
                "{value:?}: {error}"
            );
        }
        assert_eq!(
            fence_node_addrs(Ok("172.18.0.4".to_string())).unwrap(),
            vec!["172.18.0.4".parse::<IpAddr>().unwrap()]
        );
    }

    /// The anchors the exception is inserted at match the legacy ruleset
    /// verbatim, exactly once each, so the exception lands in the forward
    /// chain and nowhere else.
    #[test]
    fn forward_reject_anchors_match_the_legacy_ruleset() {
        for anchor in [FENCE_FORWARD_REJECT_V4, FENCE_FORWARD_REJECT_V6] {
            assert!(
                FENCE_RULES.contains(anchor),
                "anchor drifted from FENCE_RULES: {anchor}"
            );
            assert_eq!(
                FENCE_RULES.matches(anchor).count(),
                1,
                "anchor matches more than the forward chain: {anchor}"
            );
        }
    }

    /// The range data the selection filters against mirrors the legacy
    /// ruleset's reject sets.
    #[test]
    fn reject_range_data_mirrors_the_legacy_ruleset() {
        for (net, prefix) in FENCE_REJECT_V4 {
            assert!(
                FENCE_RULES.contains(&format!("{net}/{prefix}")),
                "v4 range missing from FENCE_RULES: {net}/{prefix}"
            );
        }
        for (net, prefix) in FENCE_REJECT_V6 {
            assert!(
                FENCE_RULES.contains(&format!("{net}/{prefix}")),
                "v6 range missing from FENCE_RULES: {net}/{prefix}"
            );
        }
    }

    /// Host-netns topology: a node dialing from loopback or a public
    /// address selects no exception, so `auto` degrades to the legacy
    /// ruleset there.
    #[test]
    fn host_netns_topology_needs_no_exception() {
        let addrs = select_fence_node_addrs([
            ip("127.0.0.1"),
            ip("::1"),
            ip("203.0.113.7"),
            ip("2001:db8::1"),
        ]);
        assert!(addrs.is_empty(), "unexpected exception: {addrs:?}");
        assert_eq!(fence_ruleset(&addrs), FENCE_RULES);
    }

    /// Container-netns topology (node on a private bridge, same engine):
    /// the v4 exception admits replies to the node's address, sits ahead
    /// of the reject so nft takes it first, and leaves every reject
    /// standing for all other private hosts.
    #[test]
    fn container_netns_v4_topology_admits_only_replies_to_the_node() {
        let rules = fence_ruleset(&[ip("172.18.0.4")]);
        let accept = "iifname { \"celld0\", \"celld1\" } ip daddr { 172.18.0.4 } ct state established,related accept";
        assert!(rules.contains(accept), "{rules}");
        assert!(
            rules.find(accept).unwrap() < rules.find(FENCE_FORWARD_REJECT_V4).unwrap(),
            "exception must precede the reject: {rules}"
        );
        assert!(rules.contains(FENCE_FORWARD_REJECT_V4), "{rules}");
        assert!(rules.contains(FENCE_FORWARD_REJECT_V6), "{rules}");
        // No v6 exception for a v4-only node.
        assert_eq!(rules.matches("ip6 daddr").count(), 1, "{rules}");
    }

    /// The v6 exception mirrors the v4 one: link-local and unique-local
    /// node addresses each get an `ip6` line ahead of the v6 reject, and
    /// no v4 line appears for a v6-only node.
    #[test]
    fn v6_node_addresses_get_an_ip6_exception_only() {
        for addr in ["fe80::1", "fd00::4"] {
            let rules = fence_ruleset(&[ip(addr)]);
            let accept = format!(
                "iifname {{ \"celld0\", \"celld1\" }} ip6 daddr {{ {addr} }} ct state established,related accept"
            );
            assert!(rules.contains(&accept), "{rules}");
            assert!(
                rules.find(&accept).unwrap() < rules.find(FENCE_FORWARD_REJECT_V6).unwrap(),
                "exception must precede the reject: {rules}"
            );
            assert_eq!(rules.matches(" ip daddr").count(), 1, "{rules}");
        }
    }

    /// Mixed node (v4 + v6): both families get an exception line, each
    /// ahead of its own reject.
    #[test]
    fn mixed_node_gets_both_exceptions() {
        let rules = fence_ruleset(&[ip("10.9.0.2"), ip("fd00::9")]);
        assert!(
            rules.contains("ip daddr { 10.9.0.2 } ct state established,related accept"),
            "{rules}"
        );
        assert!(
            rules.contains("ip6 daddr { fd00::9 } ct state established,related accept"),
            "{rules}"
        );
    }

    /// Shared-engine topology: the pinned union of node addresses renders
    /// every node into one set, sorted and deduplicated for a stable
    /// ruleset no matter the install order.
    #[test]
    fn shared_engine_union_renders_every_node() {
        let rules = fence_ruleset(&[ip("172.18.0.9"), ip("172.18.0.4"), ip("172.18.0.9")]);
        assert!(
            rules.contains(
                "ip daddr { 172.18.0.4, 172.18.0.9 } ct state established,related accept"
            ),
            "{rules}"
        );
    }

    /// The security property: nothing in the auto ruleset accepts a new
    /// connection from a bridge. Every accept line carries
    /// `established,related`, so a container can answer the node but never
    /// open a connection to it — the node's listeners stay closed to
    /// tenant code.
    #[test]
    fn new_connections_from_a_bridge_stay_rejected() {
        let rules = fence_ruleset(&[ip("172.18.0.4"), ip("fd00::4")]);
        for line in rules
            .lines()
            .filter(|line| line.contains("accept") && !line.contains("policy accept"))
        {
            assert!(
                line.contains("ct state established,related"),
                "accepts new flows: {line}"
            );
        }
    }

    /// Denied tuples: the metadata address, the neighbouring private
    /// ranges, and the v6 fenced ranges stay covered by the rejects, with
    /// no exception naming them.
    #[test]
    fn metadata_and_neighbour_ranges_stay_rejected() {
        let rules = fence_ruleset(&[ip("172.18.0.4")]);
        for range in [
            "169.254.0.0/16",
            "10.0.0.0/8",
            "172.16.0.0/12",
            "192.168.0.0/16",
            "100.64.0.0/10",
            "fe80::/10",
            "fc00::/7",
        ] {
            assert!(rules.contains(range), "{range} missing: {rules}");
        }
        assert!(!rules.contains("169.254.169.254"), "{rules}");
    }

    /// Selection covers every fenced range including the edges, and drops
    /// loopback, unspecified, and public addresses.
    #[test]
    fn selection_covers_every_fenced_range() {
        for addr in [
            "169.254.0.1",
            "169.254.255.254",
            "10.0.0.1",
            "10.255.255.254",
            "172.16.0.1",
            "172.31.255.254",
            "192.168.0.1",
            "192.168.255.254",
            "100.64.0.1",
            "100.127.255.254",
            "fe80::1",
            "febf:ffff::1",
            "fc00::1",
            "fdff:ffff::1",
        ] {
            assert!(fenced_range(&ip(addr)), "{addr} should be fenced");
        }
        for addr in [
            "169.253.255.255",
            "169.255.0.1",
            "9.255.255.255",
            "11.0.0.1",
            "172.15.255.255",
            "172.32.0.1",
            "192.167.255.255",
            "192.169.0.1",
            "100.63.255.255",
            "100.128.0.1",
            "203.0.113.7",
            "2001:db8::1",
            "ff02::1",
        ] {
            assert!(!fenced_range(&ip(addr)), "{addr} should not be fenced");
        }
        let addrs = select_fence_node_addrs([
            ip("127.0.0.1"),
            ip("::1"),
            ip("0.0.0.0"),
            ip("::"),
            ip("203.0.113.7"),
            ip("192.168.1.5"),
            ip("10.1.2.3"),
            ip("10.1.2.3"),
        ]);
        assert_eq!(addrs, vec![ip("10.1.2.3"), ip("192.168.1.5")]);
    }

    /// Mode parsing defaults to off and refuses anything but `off`/`auto`,
    /// so a misspelled mode fails the fence install instead of silently
    /// installing one ruleset or the other.
    #[test]
    fn mode_parsing_defaults_off_and_refuses_garbage() {
        assert_eq!(FenceNodeNetns::parse("").unwrap(), FenceNodeNetns::Off);
        assert_eq!(FenceNodeNetns::parse("off").unwrap(), FenceNodeNetns::Off);
        assert_eq!(
            FenceNodeNetns::parse("  off ").unwrap(),
            FenceNodeNetns::Off
        );
        assert_eq!(FenceNodeNetns::parse("auto").unwrap(), FenceNodeNetns::Auto);
        assert!(FenceNodeNetns::parse("on").is_err());
        assert!(FenceNodeNetns::parse("AUTO").is_err());
        assert!(FenceNodeNetns::parse("yes").is_err());
    }

    /// Pinned lists parse, dedupe, and fail closed: a hostname, a CIDR, or
    /// a typo fails the install rather than fencing the wrong addresses.
    /// Out-of-range entries drop as documented no-ops.
    #[test]
    fn pinned_addrs_parse_dedupe_and_fail_closed() {
        assert_eq!(
            parse_fence_node_addrs("172.18.0.4, 172.18.0.5,172.18.0.4").unwrap(),
            vec![ip("172.18.0.4"), ip("172.18.0.5")]
        );
        assert_eq!(parse_fence_node_addrs("  ").unwrap(), Vec::<IpAddr>::new());
        assert!(parse_fence_node_addrs("172.18.0.4, bogus").is_err());
        assert!(parse_fence_node_addrs("172.18.0.300").is_err());
        assert!(parse_fence_node_addrs("172.18.0.0/16").is_err());
        assert!(parse_fence_node_addrs("node1.local").is_err());
        assert_eq!(
            parse_fence_node_addrs("203.0.113.7").unwrap(),
            Vec::<IpAddr>::new()
        );
    }
}

#[cfg(test)]
mod kill_tests {
    use super::*;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::os::unix::net::{UnixListener, UnixStream};

    /// A scripted Engine API over a unix socket: the daemon side of the
    /// kill conformance test. Synchronous std threads only (no second
    /// runtime): the engine under test is the only async party. Serves
    /// the sleep exec's hijacked stream, answers exec inspect, and
    /// simulates the kill only when the kill exec carries the sleep's
    /// in-container marker: a host-pid `kill` or a missing `kill`
    /// binary would leave the process running, and so does this mock.
    struct MockDaemon {
        _dir: tempfile::TempDir,
        socket: PathBuf,
        state: Arc<Mutex<MockState>>,
    }

    struct MockState {
        exec_creates: Vec<Value>,
        /// Container-scope calls (kill, DELETE): the kill must never
        /// touch the container itself.
        destructive: Vec<String>,
        sleep_running: bool,
        sleep_closer: Option<std::sync::mpsc::Sender<()>>,
        /// Fail the exec inspect, so `exec()` errors after the daemon
        /// started the process.
        fail_exec_inspect: bool,
    }

    impl MockDaemon {
        fn start() -> Self {
            let dir = tempfile::Builder::new()
                .prefix("celld-kill-")
                .tempdir()
                .expect("mock daemon tempdir");
            let socket = dir.path().join("d.sock");
            let listener = UnixListener::bind(&socket).expect("mock daemon bind");
            let state = Arc::new(Mutex::new(MockState {
                exec_creates: Vec::new(),
                destructive: Vec::new(),
                sleep_running: true,
                sleep_closer: None,
                fail_exec_inspect: false,
            }));
            let serving = state.clone();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(stream) = stream else { break };
                    let state = serving.clone();
                    std::thread::spawn(move || serve(stream, state));
                }
            });
            Self {
                _dir: dir,
                socket,
                state,
            }
        }
    }

    pub(super) fn read_request(
        stream: &UnixStream,
    ) -> (String, String, Vec<(String, String)>, Vec<u8>) {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).expect("request line");
        let mut head = line.split_whitespace();
        let method = head.next().unwrap_or_default().to_string();
        let path = head.next().unwrap_or_default().to_string();
        let mut headers = Vec::new();
        loop {
            line.clear();
            reader.read_line(&mut line).expect("request header");
            let trimmed = line.trim_end();
            if trimmed.is_empty() {
                break;
            }
            if let Some((name, value)) = trimmed.split_once(':') {
                headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
            }
        }
        let length = headers
            .iter()
            .find(|(name, _)| name == "content-length")
            .and_then(|(_, value)| value.parse::<usize>().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; length];
        reader.read_exact(&mut body).expect("request body");
        assert!(
            reader.buffer().is_empty(),
            "mock daemon over-read the request"
        );
        (method, path, headers, body)
    }

    pub(super) fn respond(stream: &mut UnixStream, status: &str, body: &[u8]) {
        write!(
            stream,
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .expect("mock response head");
        stream.write_all(body).expect("mock response body");
        stream.flush().expect("mock response flush");
    }

    /// Whether the recorded kill exec could actually signal the sleep:
    /// a shell scan for the sleep's marker, not a host-pid `kill`.
    fn kill_shape_matches(creates: &[Value]) -> bool {
        let [sleep, kill] = creates else {
            return false;
        };
        let Some(marker) = sleep.get("Env").and_then(Value::as_array).and_then(|env| {
            env.iter().find_map(|entry| {
                entry
                    .as_str()?
                    .strip_prefix("CELLD_EXEC_ID=")
                    .map(str::to_string)
            })
        }) else {
            return false;
        };
        let Some(script) =
            kill.get("Cmd")
                .and_then(Value::as_array)
                .and_then(|cmd| match cmd.as_slice() {
                    [sh, flag, script] => (sh.as_str()? == "sh" && flag.as_str()? == "-c")
                        .then(|| script.as_str())
                        .flatten(),
                    _ => None,
                })
        else {
            return false;
        };
        script.contains(&format!("CELLD_EXEC_ID={marker}")) && script.contains("-9")
    }

    fn serve(stream: UnixStream, state: Arc<Mutex<MockState>>) {
        let (method, path, headers, body) = read_request(&stream);
        let upgraded = headers
            .iter()
            .any(|(name, value)| name == "upgrade" && value.eq_ignore_ascii_case("tcp"));
        let mut stream = stream;
        if method == "POST" && path == "/containers/n/exec" {
            let body: Value = serde_json::from_slice(&body).expect("exec create JSON");
            let id = {
                let mut state = state.lock().unwrap();
                let id = if state.exec_creates.is_empty() {
                    "sleep-exec".to_string()
                } else {
                    format!("kill-exec-{}", state.exec_creates.len())
                };
                state.exec_creates.push(body);
                id
            };
            respond(
                &mut stream,
                "201 Created",
                json!({ "Id": id }).to_string().as_bytes(),
            );
            return;
        }
        if method == "POST" && path == "/exec/sleep-exec/start" && upgraded {
            write!(
                stream,
                "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n"
            )
            .expect("mock 101");
            stream.flush().expect("mock 101 flush");
            let (tx, rx) = std::sync::mpsc::channel();
            state.lock().unwrap().sleep_closer = Some(tx);
            // Park the stream until the kill lands; dropping it is the
            // EOF the engine reads as the process exiting.
            let _ = rx.recv();
            return;
        }
        if method == "POST" && path.starts_with("/exec/") && path.ends_with("/start") && !upgraded {
            let mut state = state.lock().unwrap();
            if kill_shape_matches(&state.exec_creates) {
                state.sleep_running = false;
                if let Some(closer) = state.sleep_closer.take() {
                    let _ = closer.send(());
                }
            }
            drop(state);
            respond(&mut stream, "204 No Content", b"");
            return;
        }
        if method == "GET" && path == "/exec/sleep-exec/json" {
            if state.lock().unwrap().fail_exec_inspect {
                respond(
                    &mut stream,
                    "500 Internal Server Error",
                    json!({ "message": "boom" }).to_string().as_bytes(),
                );
                return;
            }
            let running = state.lock().unwrap().sleep_running;
            let code = (!running).then_some(137);
            respond(
                &mut stream,
                "200 OK",
                json!({ "Pid": 424242, "Running": running, "ExitCode": code })
                    .to_string()
                    .as_bytes(),
            );
            return;
        }
        if (method == "DELETE" && path.starts_with("/containers/"))
            || (method == "POST" && path.starts_with("/containers/") && path.ends_with("/kill"))
        {
            state
                .lock()
                .unwrap()
                .destructive
                .push(format!("{method} {path}"));
            respond(&mut stream, "204 No Content", b"");
            return;
        }
        respond(
            &mut stream,
            "404 Not Found",
            json!({ "message": format!("mock daemon: no such call {method} {path}") })
                .to_string()
                .as_bytes(),
        );
    }

    fn kill_cell() -> CellContainer {
        CellContainer {
            scope: "s".into(),
            name: "n".into(),
            spec: Arc::new(ContainerSpec {
                class_name: "C".into(),
                image: "celld-image:x".into(),
                instance_type: None,
                max_instances: None,
                runtime: None,
            }),
            state: Mutex::new(CellState::default()),
            processes: Mutex::new(Vec::new()),
            exit: watch::channel(None).0,
            started: watch::channel(0).0,
        }
    }

    /// The engine futures these tests run spawn their demux pumps
    /// through the execution domain. The shared test runtime installs
    /// it once for the test binary; installing a second handle here
    /// would trip `set_host_handle`'s exactly-once assert now that
    /// other lib tests share the same installer.
    pub(super) fn ensure_domain() {
        let _ = asyncrt::shared_test_runtime();
    }

    /// exec sleep -> kill(9) -> exitCode settles 137, container alive:
    /// the kill runs by an in-container marker (the daemon reports host
    /// PIDs, meaningless inside the container), needs no `kill` binary
    /// (the shell builtin sends the signal), and never touches the
    /// container itself.
    #[tokio::test]
    async fn kill_settles_the_exec_exit_and_spares_the_container() {
        ensure_domain();
        let mock = MockDaemon::start();
        let engine = ContainerEngine {
            runtime: None,
            dns: Vec::new(),
            resolv_conf: None,
            docker: Docker::new(&mock.socket),
            node: "test".to_string(),
            bucket: None,
            images: Mutex::new(HashMap::new()),
            cells: Mutex::new(HashMap::new()),
            networks: tokio::sync::Mutex::new(None),
            fenced: tokio::sync::Mutex::new(false),
        };
        let cell = Arc::new(kill_cell());
        let run = cell.begin_run();
        cell.finish_start(run);
        let proc = engine
            .exec(
                &cell,
                ExecParams {
                    cmd: vec!["sleep".to_string(), "60".to_string()],
                    env: vec![
                        ("A".to_string(), "b".to_string()),
                        ("CELLD_EXEC_ID".to_string(), "forged".to_string()),
                    ],
                    cwd: None,
                    user: None,
                    combined: false,
                    pty: false,
                    cols: None,
                    rows: None,
                },
            )
            .await
            .expect("exec sleep");
        // The exec create carries the marker (ours, not the forged one).
        let marker = format!("CELLD_EXEC_ID={};", proc.id);
        {
            let state = mock.state.lock().unwrap();
            assert_eq!(state.exec_creates.len(), 1);
            let env = state.exec_creates[0]["Env"].clone();
            assert!(
                env.as_array()
                    .is_some_and(|env| env.iter().any(|entry| entry.as_str() == Some("A=b"))),
                "{env}"
            );
            assert!(
                env.as_array().is_some_and(|env| env
                    .iter()
                    .any(|entry| entry.as_str() == Some(marker.as_str()))),
                "{env}"
            );
            assert!(!env.to_string().contains("forged"), "{env}");
        }
        proc.kill(9).await.expect("kill the sleep");
        // The kill is a shell scan for the marker, not `kill -9 <host pid>`.
        {
            let state = mock.state.lock().unwrap();
            assert_eq!(state.exec_creates.len(), 2);
            let cmd = state.exec_creates[1]["Cmd"].clone();
            let script = match cmd.as_array().map(Vec::as_slice) {
                Some([sh, flag, script]) => {
                    assert_eq!(sh.as_str(), Some("sh"));
                    assert_eq!(flag.as_str(), Some("-c"));
                    script.as_str().expect("scan script").to_string()
                }
                other => panic!("kill runs a shell scan, got {other:?}"),
            };
            assert!(script.contains(&marker), "{script}");
            assert!(script.contains("-9"), "{script}");
            assert!(!script.contains("424242"), "{script}");
        }
        assert_eq!(proc.wait().await.expect("exit code"), 137);
        assert!(
            mock.state.lock().unwrap().destructive.is_empty(),
            "the kill must not touch the container"
        );
    }

    /// N2: an exec whose post-hijack inspect fails kills the started
    /// process, mirroring the resize cleanup: the daemon side is
    /// already running and no handle is ever registered, so without
    /// the kill it is orphaned. The fast 500 exercises the same error
    /// path as a timed-out inspect.
    #[tokio::test]
    async fn a_failed_exec_inspect_kills_the_started_process() {
        ensure_domain();
        let mock = MockDaemon::start();
        mock.state.lock().unwrap().fail_exec_inspect = true;
        let engine = ContainerEngine {
            runtime: None,
            dns: Vec::new(),
            resolv_conf: None,
            docker: Docker::new(&mock.socket),
            node: "test".to_string(),
            bucket: None,
            images: Mutex::new(HashMap::new()),
            cells: Mutex::new(HashMap::new()),
            networks: tokio::sync::Mutex::new(None),
            fenced: tokio::sync::Mutex::new(false),
        };
        let cell = Arc::new(kill_cell());
        let run = cell.begin_run();
        cell.finish_start(run);
        let error = engine
            .exec(
                &cell,
                ExecParams {
                    cmd: vec!["sleep".to_string(), "60".to_string()],
                    env: Vec::new(),
                    cwd: None,
                    user: None,
                    combined: false,
                    pty: false,
                    cols: None,
                    rows: None,
                },
            )
            .await
            .err()
            .expect("exec must fail");
        assert!(error.to_string().contains("boom"), "{error:#}");
        // The cleanup is a marker scan for the sleep, not a container kill.
        let state = mock.state.lock().unwrap();
        assert_eq!(state.exec_creates.len(), 2);
        let marker = state.exec_creates[0]["Env"]
            .as_array()
            .and_then(|env| {
                env.iter().find_map(|entry| {
                    entry
                        .as_str()?
                        .strip_prefix("CELLD_EXEC_ID=")
                        .map(str::to_string)
                })
            })
            .expect("the sleep carries its marker");
        let script = state.exec_creates[1]["Cmd"][2]
            .as_str()
            .expect("kill scan script");
        assert!(
            script.contains(&format!("CELLD_EXEC_ID={marker}")),
            "{script}"
        );
        assert!(script.contains("-9"), "{script}");
        assert!(state.destructive.is_empty(), "no container-scope kill");
    }

    /// The same kill against a real daemon: ignored by default (CI has
    /// no daemon), run explicitly with one behind `DOCKER_HOST` and the
    /// image below present in it. The mock above is the gate; this is
    /// the independent oracle for the scan script itself.
    #[tokio::test]
    #[ignore]
    async fn kill_settles_a_real_sleep_and_spares_its_container() {
        ensure_domain();
        const IMAGE: &str = "node:26-trixie-slim";
        let docker =
            Docker::discover().expect("a real daemon behind DOCKER_HOST (celld-test-dind)");
        let name = "celld-kill-test".to_string();
        docker
            .call("DELETE", &format!("/containers/{name}?force=true"), None)
            .await
            .expect("remove stale test container");
        let created = docker
            .expect(
                "POST",
                &format!("/containers/create?name={name}"),
                Some(json!({
                    "Image": IMAGE,
                    "Cmd": ["sleep", "300"],
                    "HostConfig": { "Init": true },
                })),
                "create test container",
            )
            .await
            .expect("create test container");
        assert!(
            created.status.is_success(),
            "pull {IMAGE} into the test daemon first: {}",
            created.message()
        );
        docker
            .expect(
                "POST",
                &format!("/containers/{name}/start"),
                None,
                "start test container",
            )
            .await
            .expect("start test container");
        let engine = ContainerEngine {
            runtime: None,
            dns: Vec::new(),
            resolv_conf: None,
            docker: docker.clone(),
            node: "test".to_string(),
            bucket: None,
            images: Mutex::new(HashMap::new()),
            cells: Mutex::new(HashMap::new()),
            networks: tokio::sync::Mutex::new(None),
            fenced: tokio::sync::Mutex::new(false),
        };
        let mut cell = kill_cell();
        cell.name = name.clone();
        let cell = Arc::new(cell);
        let run = cell.begin_run();
        cell.finish_start(run);
        let proc = engine
            .exec(
                &cell,
                ExecParams {
                    cmd: vec!["sleep".to_string(), "60".to_string()],
                    env: Vec::new(),
                    cwd: None,
                    user: None,
                    combined: false,
                    pty: false,
                    cols: None,
                    rows: None,
                },
            )
            .await
            .expect("exec sleep");
        proc.kill(9).await.expect("kill the sleep");
        assert_eq!(proc.wait().await.expect("exit code"), 137);
        let info = docker
            .expect("GET", &format!("/containers/{name}/json"), None, "inspect")
            .await
            .expect("inspect test container")
            .json()
            .expect("inspect JSON");
        assert_eq!(info["State"]["Running"], json!(true));
        docker
            .call("DELETE", &format!("/containers/{name}?force=true"), None)
            .await
            .expect("remove test container");
    }
}

#[cfg(test)]
mod retained_read_tests {
    use super::*;
    use futures_util::task::noop_waker;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    fn retained_pipe() -> (ExecProcess, mpsc::Sender<Bytes>) {
        let (stdout_tx, stdout_rx) = mpsc::channel(16);
        let (_stderr_tx, stderr_rx) = mpsc::channel(1);
        let process = ExecProcess {
            id: 1,
            exec_id: "test-exec".to_string(),
            container: "test-container".to_string(),
            scope: "TestClass:test-cell".to_string(),
            pid: 1234,
            user: None,
            is_pty: false,
            docker: Docker::new("/nonexistent-celld-test.sock"),
            stdin: tokio::sync::Mutex::new(None),
            stdout: tokio::sync::Mutex::new(ExecPipe::new(stdout_rx)),
            stderr: tokio::sync::Mutex::new(ExecPipe::new(stderr_rx)),
            ended: watch::channel(false).0,
            exit_code: tokio::sync::Mutex::new(None),
        };
        (process, stdout_tx)
    }

    /// Poll one retained read once. The tests drive the protocol, not
    /// the clock: every assertion below is a poll, never a wait.
    fn poll_read(process: &ExecProcess, lease: u64, prev_sub: u64, my_sub: u64) -> Poll<ExecRead> {
        let mut future = Box::pin(process.read_retained(1, lease, prev_sub, my_sub));
        Pin::new(&mut future).poll(&mut Context::from_waker(&noop_waker()))
    }

    fn read_bytes(outcome: Poll<ExecRead>) -> Bytes {
        match outcome {
            Poll::Ready(ExecRead::Bytes(bytes)) => bytes,
            Poll::Ready(ExecRead::Superseded) => panic!("read was superseded"),
            Poll::Pending => panic!("read is pending"),
        }
    }

    /// A subscription whose completed result the event discarded (the
    /// op future ran to Ready, the driver dropped the outcome) hands
    /// its bytes to the chained re-subscription: the chunk arrives
    /// exactly once instead of being lost to a read-ahead.
    #[tokio::test]
    async fn chained_resubscription_takes_discarded_bytes() {
        let (process, stdout) = retained_pipe();
        stdout.try_send(Bytes::from("chunk-1\n")).unwrap();
        assert!(
            matches!(poll_read(&process, 7, 0, 1), Poll::Ready(_)),
            "first poll consumes"
        );
        let taken = read_bytes(poll_read(&process, 7, 1, 2));
        assert_eq!(taken, Bytes::from("chunk-1\n"));
        // The take re-stashes under its own subscription, so a second
        // discard chains onward instead of losing the chunk.
        assert!(matches!(
            poll_read(&process, 7, 2, 3),
            Poll::Ready(ExecRead::Bytes(_))
        ));
        let retaken = read_bytes(poll_read(&process, 7, 3, 4));
        assert_eq!(retaken, Bytes::from("chunk-1\n"));
    }

    /// A subscription dropped while still pending consumed nothing, so
    /// the chained re-subscription reads the channel itself.
    #[tokio::test]
    async fn pending_drop_consumes_nothing() {
        let (process, stdout) = retained_pipe();
        assert!(matches!(poll_read(&process, 7, 0, 1), Poll::Pending));
        stdout.try_send(Bytes::from("chunk-1\n")).unwrap();
        let taken = read_bytes(poll_read(&process, 7, 1, 2));
        assert_eq!(taken, Bytes::from("chunk-1\n"));
    }

    /// A late pile-up — a subscription chained after a subscription
    /// that is no longer the latest taker — aborts instead of
    /// consuming the next chunk into a promise nobody awaits, and the
    /// abort itself consumes nothing.
    #[tokio::test]
    async fn late_pileup_is_superseded_and_consumes_nothing() {
        let (process, stdout) = retained_pipe();
        stdout.try_send(Bytes::from("chunk-1\n")).unwrap();
        assert!(
            matches!(poll_read(&process, 7, 0, 1), Poll::Ready(_)),
            "first poll consumes"
        );
        let second = read_bytes(poll_read(&process, 7, 1, 2));
        assert_eq!(second, Bytes::from("chunk-1\n"));
        let stale = poll_read(&process, 7, 1, 3);
        assert!(
            matches!(stale, Poll::Ready(ExecRead::Superseded)),
            "stale chain aborts"
        );
        stdout.try_send(Bytes::from("chunk-2\n")).unwrap();
        let fresh = read_bytes(poll_read(&process, 8, 0, 4));
        assert_eq!(fresh, Bytes::from("chunk-2\n"));
    }

    /// A fresh lease (the next pull) ignores a stale stash and reads
    /// the channel.
    #[tokio::test]
    async fn fresh_lease_reads_past_a_stale_stash() {
        let (process, stdout) = retained_pipe();
        stdout.try_send(Bytes::from("chunk-1\n")).unwrap();
        let first = read_bytes(poll_read(&process, 7, 0, 1));
        assert_eq!(first, Bytes::from("chunk-1\n"));
        stdout.try_send(Bytes::from("chunk-2\n")).unwrap();
        let fresh = read_bytes(poll_read(&process, 8, 0, 2));
        assert_eq!(fresh, Bytes::from("chunk-2\n"));
    }

    /// End of stream reads empty, repeatedly.
    #[tokio::test]
    async fn eof_reads_empty() {
        let (process, _stdout) = retained_pipe();
        drop(_stdout);
        let end = read_bytes(poll_read(&process, 7, 0, 1));
        assert!(end.is_empty());
        let again = read_bytes(poll_read(&process, 8, 0, 2));
        assert!(again.is_empty());
    }
}

#[cfg(test)]
mod control_timeout_tests {
    use super::kill_tests::ensure_domain;
    use super::*;
    use std::os::unix::net::UnixListener;

    /// A daemon that accepts and never answers: holds every
    /// connection, reads nothing, answers nothing. Dropping a
    /// connection would fail the call with EOF instead of hanging it.
    fn silent_daemon() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::Builder::new()
            .prefix("celld-control-timeout-")
            .tempdir()
            .expect("silent daemon tempdir");
        let socket = dir.path().join("d.sock");
        let listener = UnixListener::bind(&socket).expect("silent daemon bind");
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                std::thread::spawn(move || {
                    let _held = stream;
                    std::thread::park();
                });
            }
        });
        (dir, socket)
    }

    /// A control call against a daemon that accepts and never answers
    /// fails with the control error instead of hanging: the bound, not
    /// the daemon, ends the call. The injected bound is milliseconds,
    /// so the test never waits out the production one.
    #[tokio::test]
    async fn control_call_times_out_on_a_silent_daemon() {
        ensure_domain();
        let (_dir, socket) = silent_daemon();
        let docker = Docker::new(&socket);
        let Err(error) = docker
            .call_control_with_timeout(
                "POST",
                "/containers/n/kill?signal=15",
                None,
                "signal container",
                Duration::from_millis(50),
            )
            .await
        else {
            panic!("a silent daemon must fail the control call");
        };
        assert_eq!(
            error.to_string(),
            "container engine did not answer signal container within 50ms"
        );
    }

    /// N3: a lifecycle call against the silent daemon fails the same
    /// way through its own entry point: the tier routes through the
    /// shared timeout, not around it.
    #[tokio::test]
    async fn lifecycle_call_times_out_on_a_silent_daemon() {
        ensure_domain();
        let (_dir, socket) = silent_daemon();
        let docker = Docker::new(&socket);
        let Err(error) = docker
            .call_lifecycle_with_timeout(
                "POST",
                "/containers/n/start",
                None,
                "start container",
                Duration::from_millis(50),
            )
            .await
        else {
            panic!("a silent daemon must fail the lifecycle call");
        };
        assert_eq!(
            error.to_string(),
            "container engine did not answer start container within 50ms"
        );
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::kill_tests::{ensure_domain, read_request, respond};
    use super::*;
    use std::os::unix::net::{UnixListener, UnixStream};

    /// A scripted Engine API over a unix socket: answers the create,
    /// then fails the start, and records every call in order.
    /// Synchronous std threads only (no second runtime): the engine
    /// under test is the only async party.
    struct StartFailingDaemon {
        _dir: tempfile::TempDir,
        socket: PathBuf,
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl StartFailingDaemon {
        fn start(name: &str) -> Self {
            let dir = tempfile::Builder::new()
                .prefix("celld-start-failing-")
                .tempdir()
                .expect("mock daemon tempdir");
            let socket = dir.path().join("d.sock");
            let listener = UnixListener::bind(&socket).expect("mock daemon bind");
            let calls = Arc::new(Mutex::new(Vec::new()));
            let serving = calls.clone();
            let name = name.to_string();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(stream) = stream else { break };
                    let calls = serving.clone();
                    let name = name.clone();
                    std::thread::spawn(move || serve(stream, calls, &name));
                }
            });
            Self {
                _dir: dir,
                socket,
                calls,
            }
        }
    }

    fn serve(stream: UnixStream, calls: Arc<Mutex<Vec<String>>>, name: &str) {
        let (method, path, _, _) = read_request(&stream);
        calls.lock().unwrap().push(format!("{method} {path}"));
        let mut stream = stream;
        if method == "POST" && path == format!("/containers/create?name={name}") {
            respond(
                &mut stream,
                "201 Created",
                json!({ "Id": "n1deadbeef", "Warnings": [] })
                    .to_string()
                    .as_bytes(),
            );
            return;
        }
        if method == "POST" && path == format!("/containers/{name}/start") {
            respond(
                &mut stream,
                "500 Internal Server Error",
                json!({ "message": "boom" }).to_string().as_bytes(),
            );
            return;
        }
        if method == "DELETE" && path == format!("/containers/{name}?force=true") {
            respond(&mut stream, "204 No Content", b"");
            return;
        }
        respond(
            &mut stream,
            "404 Not Found",
            json!({ "message": format!("mock daemon: no such call {method} {path}") })
                .to_string()
                .as_bytes(),
        );
    }

    fn start_cell(name: &str) -> CellContainer {
        CellContainer {
            scope: "s".into(),
            name: name.to_string(),
            spec: Arc::new(ContainerSpec {
                class_name: "C".into(),
                image: "celld-image:x".into(),
                instance_type: None,
                max_instances: None,
                runtime: None,
            }),
            state: Mutex::new(CellState::default()),
            processes: Mutex::new(Vec::new()),
            exit: watch::channel(None).0,
            started: watch::channel(0).0,
        }
    }

    /// N1: a start that fails after its create force-removes the
    /// container by name, so no untracked container is left while
    /// state says stopped. The fast 500 exercises the same error path
    /// as a timed-out start; the test asserts the exact call order,
    /// ending in the cleanup DELETE.
    #[tokio::test]
    async fn a_start_failed_after_create_removes_the_container() {
        ensure_domain();
        let name = "n1-cell";
        let mock = StartFailingDaemon::start(name);
        let mut images = HashMap::new();
        images.insert(
            "celld-image:x".to_string(),
            Arc::new(tokio::sync::Mutex::new(true)),
        );
        let engine = ContainerEngine {
            runtime: None,
            dns: Vec::new(),
            resolv_conf: None,
            docker: Docker::new(&mock.socket),
            node: "test".to_string(),
            bucket: None,
            images: Mutex::new(images),
            cells: Mutex::new(HashMap::new()),
            networks: tokio::sync::Mutex::new(Some((
                "celld".to_string(),
                "celld-internal".to_string(),
            ))),
            fenced: tokio::sync::Mutex::new(true),
        };
        let cell = start_cell(name);
        let error = engine
            .create_and_start(
                &cell,
                StartParams {
                    entrypoint: None,
                    env: Vec::new(),
                    enable_internet: true,
                    labels: Vec::new(),
                },
            )
            .await
            .expect_err("the start must fail");
        assert!(error.to_string().contains("boom"), "{error:#}");
        assert_eq!(
            *mock.calls.lock().unwrap(),
            vec![
                format!("DELETE /containers/{name}?force=true"),
                format!("POST /containers/create?name={name}"),
                format!("POST /containers/{name}/start"),
                format!("DELETE /containers/{name}?force=true"),
            ]
        );
    }

    /// N4: the retry loops share the lifecycle bound as their
    /// aggregate budget, so a slow-but-answering daemon cannot spin
    /// them for minutes. Both branches, with no waiting and no
    /// domain: the budget is a pure function of two clock stamps.
    #[test]
    fn retry_loops_share_the_lifecycle_bound_as_their_budget() {
        let bound = LIFECYCLE_TIMEOUT.as_micros() as u64;
        assert!(!aggregate_budget_spent(1_000, 1_000));
        assert!(!aggregate_budget_spent(1_000, 1_000 + bound - 1));
        assert!(aggregate_budget_spent(1_000, 1_000 + bound));
        assert!(!aggregate_budget_spent(2_000, 1_000));
    }

    /// N4: the stopped-container logs query tails, so its size bound
    /// is structural rather than coupled to the fence caller.
    #[test]
    fn stopped_container_logs_query_tails() {
        assert_eq!(
            container_logs_path("abc"),
            "/containers/abc/logs?stdout=true&stderr=true&tail=50"
        );
    }
}
