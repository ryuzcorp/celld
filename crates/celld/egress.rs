// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): tenant egress policy. Apache-2.0.

// Tenant egress policy for Worker-initiated outbound connections.
// It dials sockets and reads the wall clock for its DNS cache, like
// `ws_client`, so it lives outside the Actor execution domain.
#![allow(clippy::disallowed_methods)]

//! Tenant egress policy: which addresses Worker code may dial.
//!
//! `CELLD_EGRESS=open` (the default) preserves today's behavior: Worker
//! `fetch()`, `connect()` sockets, and outbound WebSockets may reach any
//! address. `CELLD_EGRESS=public` refuses dials to every non-public
//! address: loopback, private (RFC 1918), link-local, carrier-grade NAT
//! (RFC 6598), IPv6 unique-local, unspecified, multicast, reserved
//! (`240.0.0.0/4`), the IPv4-mapped and transition-embedded (NAT64, 6to4,
//! Teredo, IPv4-compatible) forms of those, and the cloud metadata
//! address. `CELLD_EGRESS_ALLOW` lists operator-permitted exceptions as
//! comma-separated CIDRs (any port) or `host:port` pairs (that host on
//! that port only), for example the object store endpoint a deployment
//! keeps on a private address.
//!
//! Enforcement happens on the resolved address at connect time, never on
//! the hostname string, so DNS rebinding cannot bypass it:
//!
//! - `fetch()` resolves the initial URL before sending (precise port
//!   match), resolves every redirect hop through a filtering
//!   [`reqwest::dns::Resolve`] that drops refused addresses, and checks
//!   redirect targets that are IP literals — hyper never consults the
//!   resolver for literals — in the redirect policy, which knows the
//!   hop's port and checks it precisely. `host:port` entries match the
//!   port exactly on every hop: reqwest's resolver never learns the
//!   destination port, so each request under a port-scoped policy builds
//!   its own client whose redirect policy and resolver share a pin
//!   holding the current hop's port, re-pinned from every hop's full
//!   URL. Policies without port-scoped entries share one client per
//!   thread, which is exactly equivalent because only port-agnostic
//!   entries remain to match.
//! - `connect()` and outbound WebSockets resolve, filter, and dial the
//!   surviving addresses directly as `SocketAddr`s, so no second
//!   resolution can substitute an unchecked address between check and
//!   connect.
//!
//! One deliberate limit follows from those APIs: a hostname allowlist
//! entry cannot be consulted where no DNS is possible, so a redirect to
//! a bare IP literal matches only IP, CIDR, and IP:port entries, and an
//! operator whose redirect targets are literals must allowlist them in
//! one of those forms.
//!
//! Tenant clients never use a proxy in public mode: through a proxy the
//! target would resolve with the proxy's DNS instead of the checked one,
//! and redirect hops would expose only the proxy's address to the
//! resolver, so a proxied tenant fetch cannot be policed. Open mode keeps
//! reqwest's default proxy handling byte-identically.
//!
//! What the policy does not cover: celld's own traffic — the object
//! store, peer, control, telemetry, and container-engine clients — never
//! goes through these checks (each uses its own client, proxies
//! included), and dials to container endpoints this node published to
//! Workers via `getTcpPort()` are exempt, because refusing them would
//! break the sandbox path the policy is meant to protect rather than
//! replace. That exemption is node-global: any Worker on the node may
//! dial a published container endpoint by its address and port.
//!
//! A refused `fetch()` rejects with a `TypeError` whose message starts
//! `fetch failed: egress policy refused ...`, matching the Fetch standard
//! and workerd, which surface network failures as `TypeError`. A refused
//! `connect()` rejects the socket's `opened`/`closed` promises, and a
//! refused WebSocket fires `error` and closes with 1006, each carrying
//! the refused address and reason. Every refusal also logs a warning
//! with the dialed host, the refused address, and the reason.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{
    atomic::{AtomicU16, Ordering},
    Arc, Mutex, OnceLock,
};
use std::time::{Duration, Instant};

/// Prefix on every fetch refusal the host hands to JavaScript. The
/// harness strips it and raises `TypeError`; nothing else may emit it.
pub const FETCH_REFUSAL_MARKER: &str = "CELLD_EGRESS_REFUSED:";

/// How long a hostname resolution is shared between the pre-send check
/// and the reqwest resolver: long enough that one fetch resolves once,
/// short enough that a moved host is re-resolved promptly. Sharing the
/// addresses (rather than re-resolving) is what closes the rebinding
/// window between the precise pre-send check and the connect.
const RESOLUTION_TTL: Duration = Duration::from_secs(5);

/// Bound on the resolution cache. Distinct-hostname fetches (the DNS
/// exfiltration shape) must not grow it without limit; past the cap the
/// table is swept and, if still full, dropped outright, which only costs
/// re-resolution.
const RESOLUTION_CACHE_CAP: usize = 1024;

/// `CELLD_EGRESS`: the tenant egress mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EgressMode {
    /// Workers may dial anything. Today's behavior, and the default.
    Open,
    /// Workers may dial public addresses plus the allowlist and this
    /// node's container endpoints.
    Public,
}

/// One `CELLD_EGRESS_ALLOW` entry.
#[derive(Clone, Debug, Eq, PartialEq)]
enum AllowEntry {
    /// A subnet (or bare IP, which parses as a host route): any port.
    Subnet(Cidr),
    /// One host on one port. An IP host matches exactly; a hostname is
    /// resolved on every check, so a moved store is re-allowed without a
    /// restart and a stale address stops matching on its own.
    HostPort { host: AllowHost, port: u16 },
}

/// The host half of a `host:port` allowlist entry.
#[derive(Clone, Debug, Eq, PartialEq)]
enum AllowHost {
    Ip(IpAddr),
    Name(String),
}

/// An IP subnet, parsed without new dependencies (the build is offline).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Cidr {
    addr: IpAddr,
    prefix: u8,
}

impl Cidr {
    fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(addr)) => {
                let shift = 32 - self.prefix;
                shift >= 32 || u32::from(net) >> shift == u32::from(addr) >> shift
            }
            (IpAddr::V6(net), IpAddr::V6(addr)) => {
                let shift = 128 - self.prefix;
                shift >= 128 || u128::from(net) >> shift == u128::from(addr) >> shift
            }
            _ => false,
        }
    }
}

/// The parsed tenant egress policy. Constructed from the environment at
/// the enforcement sites; tests build it directly so no test depends on
/// process-global state.
#[derive(Clone, Debug)]
pub struct EgressPolicy {
    mode: EgressMode,
    allow: Vec<AllowEntry>,
}

impl EgressMode {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "open" => Ok(EgressMode::Open),
            "public" => Ok(EgressMode::Public),
            other => Err(format!(
                "CELLD_EGRESS must be `open` or `public`, not {other:?}"
            )),
        }
    }
}

fn parse_cidr(text: &str) -> Result<Cidr, String> {
    let (addr, prefix) = text
        .split_once('/')
        .ok_or_else(|| format!("{text:?} is not a CIDR: expected `address/prefix`"))?;
    let addr: IpAddr = addr
        .parse()
        .map_err(|_| format!("{text:?} is not a CIDR: {addr:?} is not an IP address"))?;
    let max = if addr.is_ipv4() { 32 } else { 128 };
    let prefix: u8 = prefix
        .parse()
        .map_err(|_| format!("{text:?} is not a CIDR: {prefix:?} is not a prefix length"))?;
    if prefix > max {
        return Err(format!("{text:?} is not a CIDR: /{prefix} exceeds /{max}"));
    }
    Ok(Cidr { addr, prefix })
}

/// Split a `host:port` entry. IPv6 hosts with ports must use brackets
/// (`[::1]:8080`), as in a URL; anything else splits on the last colon.
fn split_host_port(text: &str) -> Result<(String, u16), String> {
    if let Some(rest) = text.strip_prefix('[') {
        let (host, port) = rest.split_once("]:").ok_or_else(|| {
            format!("{text:?} is not `host:port`: a bracketed host needs `]:port`")
        })?;
        let port = parse_port(text, port)?;
        return Ok((host.to_string(), port));
    }
    let (host, port) = text
        .rsplit_once(':')
        .ok_or_else(|| format!("{text:?} is not `host:port`: a hostname entry needs a port"))?;
    if host.contains(':') {
        return Err(format!(
            "{text:?} is not `host:port`: bracket an IPv6 host (`[{host}]:port`)"
        ));
    }
    let port = parse_port(text, port)?;
    Ok((host.to_string(), port))
}

fn parse_port(entry: &str, port: &str) -> Result<u16, String> {
    let port: u16 = port
        .parse()
        .map_err(|_| format!("{entry:?} is not `host:port`: {port:?} is not a port"))?;
    if port == 0 {
        return Err(format!(
            "{entry:?} is not `host:port`: the port must not be 0"
        ));
    }
    Ok(port)
}

fn parse_allow_entry(text: &str) -> Result<AllowEntry, String> {
    if text.contains('/') {
        return parse_cidr(text).map(AllowEntry::Subnet);
    }
    if let Ok(addr) = text.parse::<IpAddr>() {
        let prefix = if addr.is_ipv4() { 32 } else { 128 };
        return Ok(AllowEntry::Subnet(Cidr { addr, prefix }));
    }
    if let Ok(addr) = text.parse::<SocketAddr>() {
        if addr.port() == 0 {
            return Err(format!(
                "{text:?} is not `host:port`: the port must not be 0"
            ));
        }
        return Ok(AllowEntry::HostPort {
            host: AllowHost::Ip(addr.ip()),
            port: addr.port(),
        });
    }
    let (host, port) = split_host_port(text)?;
    if host.is_empty()
        || host.len() > 255
        || host
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(format!(
            "{text:?} is not `host:port`: {host:?} is not a host"
        ));
    }
    let host = match host.parse::<IpAddr>() {
        Ok(addr) => AllowHost::Ip(addr),
        Err(_) => AllowHost::Name(host),
    };
    Ok(AllowEntry::HostPort { host, port })
}

fn parse_allow_list(value: &str) -> Result<Vec<AllowEntry>, String> {
    if value.trim().is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|entry| {
            let entry = entry.trim();
            if entry.is_empty() {
                return Err(
                    "CELLD_EGRESS_ALLOW has an empty entry; remove the stray comma".to_string(),
                );
            }
            parse_allow_entry(entry)
        })
        .collect()
}

impl EgressPolicy {
    /// Parse the policy from the environment. Unset `CELLD_EGRESS` means
    /// `open`; unset or empty `CELLD_EGRESS_ALLOW` means no exceptions.
    /// Startup validation refuses to boot on an invalid value, so a
    /// runtime parse failure here means validation was skipped.
    pub fn from_env() -> Result<Self, String> {
        let mode = match std::env::var("CELLD_EGRESS") {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => return Err(format!("read CELLD_EGRESS: {error}")),
        };
        let allow = match std::env::var("CELLD_EGRESS_ALLOW") {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => return Err(format!("read CELLD_EGRESS_ALLOW: {error}")),
        };
        Self::from_values(mode.as_deref(), allow.as_deref())
    }

    /// Parse the policy from the two variable values, `None` meaning
    /// unset. [`from_env`](Self::from_env) is the only production caller;
    /// tests call this directly so none of them touch the process
    /// environment.
    fn from_values(mode: Option<&str>, allow: Option<&str>) -> Result<Self, String> {
        let mode = match mode {
            Some(value) => EgressMode::parse(value)?,
            None => EgressMode::Open,
        };
        let allow = match allow {
            Some(value) => parse_allow_list(value)?,
            None => Vec::new(),
        };
        Ok(Self { mode, allow })
    }

    /// The process policy, parsed once. Fails closed: an invalid
    /// environment runs `public` with no exceptions rather than silently
    /// running open.
    pub fn global() -> Arc<Self> {
        static POLICY: OnceLock<Arc<EgressPolicy>> = OnceLock::new();
        POLICY
            .get_or_init(|| match EgressPolicy::from_env() {
                Ok(policy) => Arc::new(policy),
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "invalid egress configuration; failing closed to CELLD_EGRESS=public"
                    );
                    closed_policy()
                }
            })
            .clone()
    }

    pub fn is_open(&self) -> bool {
        self.mode == EgressMode::Open
    }
}

/// Why an address is refused: the dialed host spelling, the refused
/// address, and the denied range it fell in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EgressRefusal {
    pub host: String,
    pub ip: IpAddr,
    pub reason: &'static str,
}

impl std::fmt::Display for EgressRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self
            .host
            .parse::<IpAddr>()
            .is_ok_and(|addr| addr == self.ip)
        {
            write!(
                f,
                "egress policy refused {}: {}; CELLD_EGRESS=public",
                self.ip, self.reason
            )
        } else {
            write!(
                f,
                "egress policy refused {} ({}): {}; CELLD_EGRESS=public",
                self.host, self.ip, self.reason
            )
        }
    }
}

impl std::error::Error for EgressRefusal {}

/// The refusal as reqwest sees it: [`fetch_request_error`] walks the
/// error chain for this type the way it walks for the redirect limit,
/// so the marker survives reqwest's top-level message.
pub struct FetchRefused(pub EgressRefusal);

impl std::fmt::Debug for FetchRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FetchRefused({})", self.0)
    }
}

impl std::fmt::Display for FetchRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{FETCH_REFUSAL_MARKER}fetch failed: {}", self.0)
    }
}

impl std::error::Error for FetchRefused {}

/// The refusal message for a fetch the op refuses before sending. Carries
/// the marker so the harness raises `TypeError`.
pub fn fetch_refusal_message(refusal: &EgressRefusal) -> String {
    format!("{FETCH_REFUSAL_MARKER}fetch failed: {refusal}")
}

/// The identifying parts of a refused fetch URL for the operator log:
/// scheme, host, and port — never userinfo, path, query, or fragment,
/// which may carry credentials. `None` when the URL does not parse, in
/// which case the caller falls back to the refusal's host spelling.
pub fn log_target_parts(url: &str) -> Option<(String, String, u16)> {
    let parsed = reqwest::Url::parse(url).ok()?;
    let host = parsed.host_str()?.to_string();
    let port = parsed.port_or_known_default()?;
    Some((parsed.scheme().to_string(), host, port))
}

/// The denied ranges, as an explicit table rather than `std` predicates
/// so the set is visible in one place for review. Returns the reason to
/// report. IPv4-mapped IPv6 addresses are judged by their embedded IPv4
/// address, as are the transition forms that embed one (NAT64
/// `64:ff9b::/96`, 6to4 `2002::/16`, Teredo `2001::/32`, and
/// IPv4-compatible `::/96`); the metadata address is named explicitly
/// even though it also falls under link-local.
fn denied_reason(ip: IpAddr) -> Option<&'static str> {
    match ip {
        IpAddr::V4(addr) => denied_reason_v4(addr, false),
        IpAddr::V6(addr) => {
            if let Some(mapped) = addr.to_ipv4_mapped() {
                return denied_reason_v4(mapped, true);
            }
            let o = addr.octets();
            if addr.is_loopback() {
                Some("loopback addresses are blocked")
            } else if addr.is_unspecified() {
                Some("unspecified addresses are blocked")
            } else if addr.is_multicast() {
                Some("multicast addresses are blocked")
            } else if o[0] & 0xfe == 0xfc {
                Some("IPv6 unique-local addresses are blocked")
            } else if o[0] == 0xfe && o[1] & 0xc0 == 0x80 {
                Some("link-local addresses are blocked")
            } else if let Some(embedded) = embedded_ipv4(&o) {
                denied_reason_embedded(embedded)
            } else {
                None
            }
        }
    }
}

/// An IPv4 address embedded in an IPv6 transition form, with the form it
/// came from so the refusal can name it.
struct EmbeddedV4 {
    kind: EmbeddedKind,
    addr: Ipv4Addr,
}

#[derive(Clone, Copy)]
enum EmbeddedKind {
    Nat64,
    SixTo4,
    Teredo,
    Compatible,
}

/// Extract the embedded IPv4 address from a transition address: the last
/// 32 bits for NAT64 `64:ff9b::/96` and IPv4-compatible `::/96`, bits
/// 16..48 for 6to4 `2002::/16`, and the de-obfuscated client field (last
/// 32 bits XORed with all ones) for Teredo `2001::/32`.
fn embedded_ipv4(o: &[u8; 16]) -> Option<EmbeddedV4> {
    if o[0..12] == [0x00, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0] {
        Some(EmbeddedV4 {
            kind: EmbeddedKind::Nat64,
            addr: Ipv4Addr::new(o[12], o[13], o[14], o[15]),
        })
    } else if o[0] == 0x20 && o[1] == 0x02 {
        Some(EmbeddedV4 {
            kind: EmbeddedKind::SixTo4,
            addr: Ipv4Addr::new(o[2], o[3], o[4], o[5]),
        })
    } else if o[0] == 0x20 && o[1] == 0x01 && o[2] == 0x00 && o[3] == 0x00 {
        let obfuscated = u32::from_be_bytes([o[12], o[13], o[14], o[15]]);
        Some(EmbeddedV4 {
            kind: EmbeddedKind::Teredo,
            addr: Ipv4Addr::from(!obfuscated),
        })
    } else if o[0..12] == [0; 12] {
        Some(EmbeddedV4 {
            kind: EmbeddedKind::Compatible,
            addr: Ipv4Addr::new(o[12], o[13], o[14], o[15]),
        })
    } else {
        None
    }
}

/// Judge an embedded IPv4 address against the same deny ranges as a
/// plain one, naming the transition form in the reason. The metadata
/// address keeps its plain reason, as the mapped form does.
fn denied_reason_embedded(embedded: EmbeddedV4) -> Option<&'static str> {
    let reason = denied_reason_v4(embedded.addr, false)?;
    if reason == "the cloud metadata address is blocked" {
        return Some(reason);
    }
    Some(match (embedded.kind, reason) {
        (EmbeddedKind::Nat64, "loopback addresses are blocked") => {
            "NAT64 loopback addresses are blocked"
        }
        (EmbeddedKind::Nat64, "private addresses are blocked") => {
            "NAT64 private addresses are blocked"
        }
        (EmbeddedKind::Nat64, "link-local addresses are blocked") => {
            "NAT64 link-local addresses are blocked"
        }
        (EmbeddedKind::Nat64, "carrier-grade NAT addresses are blocked") => {
            "NAT64 carrier-grade NAT addresses are blocked"
        }
        (EmbeddedKind::Nat64, "unspecified addresses are blocked") => {
            "NAT64 unspecified addresses are blocked"
        }
        (EmbeddedKind::Nat64, "multicast addresses are blocked") => {
            "NAT64 multicast addresses are blocked"
        }
        (EmbeddedKind::Nat64, "reserved addresses are blocked") => {
            "NAT64 reserved addresses are blocked"
        }
        (EmbeddedKind::SixTo4, "loopback addresses are blocked") => {
            "6to4 loopback addresses are blocked"
        }
        (EmbeddedKind::SixTo4, "private addresses are blocked") => {
            "6to4 private addresses are blocked"
        }
        (EmbeddedKind::SixTo4, "link-local addresses are blocked") => {
            "6to4 link-local addresses are blocked"
        }
        (EmbeddedKind::SixTo4, "carrier-grade NAT addresses are blocked") => {
            "6to4 carrier-grade NAT addresses are blocked"
        }
        (EmbeddedKind::SixTo4, "unspecified addresses are blocked") => {
            "6to4 unspecified addresses are blocked"
        }
        (EmbeddedKind::SixTo4, "multicast addresses are blocked") => {
            "6to4 multicast addresses are blocked"
        }
        (EmbeddedKind::SixTo4, "reserved addresses are blocked") => {
            "6to4 reserved addresses are blocked"
        }
        (EmbeddedKind::Teredo, "loopback addresses are blocked") => {
            "Teredo loopback addresses are blocked"
        }
        (EmbeddedKind::Teredo, "private addresses are blocked") => {
            "Teredo private addresses are blocked"
        }
        (EmbeddedKind::Teredo, "link-local addresses are blocked") => {
            "Teredo link-local addresses are blocked"
        }
        (EmbeddedKind::Teredo, "carrier-grade NAT addresses are blocked") => {
            "Teredo carrier-grade NAT addresses are blocked"
        }
        (EmbeddedKind::Teredo, "unspecified addresses are blocked") => {
            "Teredo unspecified addresses are blocked"
        }
        (EmbeddedKind::Teredo, "multicast addresses are blocked") => {
            "Teredo multicast addresses are blocked"
        }
        (EmbeddedKind::Teredo, "reserved addresses are blocked") => {
            "Teredo reserved addresses are blocked"
        }
        (EmbeddedKind::Compatible, "loopback addresses are blocked") => {
            "IPv4-compatible loopback addresses are blocked"
        }
        (EmbeddedKind::Compatible, "private addresses are blocked") => {
            "IPv4-compatible private addresses are blocked"
        }
        (EmbeddedKind::Compatible, "link-local addresses are blocked") => {
            "IPv4-compatible link-local addresses are blocked"
        }
        (EmbeddedKind::Compatible, "carrier-grade NAT addresses are blocked") => {
            "IPv4-compatible carrier-grade NAT addresses are blocked"
        }
        (EmbeddedKind::Compatible, "unspecified addresses are blocked") => {
            "IPv4-compatible unspecified addresses are blocked"
        }
        (EmbeddedKind::Compatible, "multicast addresses are blocked") => {
            "IPv4-compatible multicast addresses are blocked"
        }
        (EmbeddedKind::Compatible, "reserved addresses are blocked") => {
            "IPv4-compatible reserved addresses are blocked"
        }
        (_, reason) => reason,
    })
}

fn denied_reason_v4(addr: Ipv4Addr, mapped: bool) -> Option<&'static str> {
    // The metadata address first, so it is named rather than reported
    // as generic link-local.
    if addr == Ipv4Addr::new(169, 254, 169, 254) {
        return Some("the cloud metadata address is blocked");
    }
    let o = addr.octets();
    let plain = if o[0] == 127 {
        Some("loopback addresses are blocked")
    } else if o[0] == 10 || o[0] == 172 && o[1] & 0xf0 == 16 || o[0] == 192 && o[1] == 168 {
        Some("private addresses are blocked")
    } else if o[0] == 169 && o[1] == 254 {
        Some("link-local addresses are blocked")
    } else if o[0] == 100 && o[1] & 0xc0 == 64 {
        Some("carrier-grade NAT addresses are blocked")
    } else if o[0] == 0 {
        Some("unspecified addresses are blocked")
    } else if o[0] & 0xf0 == 0xe0 {
        Some("multicast addresses are blocked")
    } else if o[0] & 0xf0 == 0xf0 {
        Some("reserved addresses are blocked")
    } else {
        None
    };
    match (plain, mapped) {
        (None, _) => None,
        (Some(reason), false) => Some(reason),
        (Some("loopback addresses are blocked"), true) => {
            Some("IPv4-mapped loopback addresses are blocked")
        }
        (Some("private addresses are blocked"), true) => {
            Some("IPv4-mapped private addresses are blocked")
        }
        (Some("link-local addresses are blocked"), true) => {
            Some("IPv4-mapped link-local addresses are blocked")
        }
        (Some("carrier-grade NAT addresses are blocked"), true) => {
            Some("IPv4-mapped carrier-grade NAT addresses are blocked")
        }
        (Some("unspecified addresses are blocked"), true) => {
            Some("IPv4-mapped unspecified addresses are blocked")
        }
        (Some("multicast addresses are blocked"), true) => {
            Some("IPv4-mapped multicast addresses are blocked")
        }
        (Some("reserved addresses are blocked"), true) => {
            Some("IPv4-mapped reserved addresses are blocked")
        }
        (Some(reason), true) => Some(reason),
    }
}

/// Whether `(ip, port)` is a container endpoint this node published to
/// Workers. The engine knows every cell container of this fleet and no
/// other fleet's, so the exemption preserves exactly the reachability
/// the nft gateway's ownership check used to grant: own containers pass,
/// everything else in the denied ranges does not.
fn is_container_endpoint(ip: IpAddr, port: u16) -> bool {
    crate::container::engine_if_ready()
        .is_some_and(|engine| engine.is_worker_container_endpoint(ip, port))
}

/// Whether `ip` names one of this node's containers without regard to
/// port. Only the Linux form (a bridge IP, where every port reaches the
/// same container) qualifies: the published form (`127.0.0.1` plus a
/// mapped port) cannot be verified without the port, so port-less
/// checks fail closed on it.
fn is_container_ip(ip: IpAddr) -> bool {
    crate::container::engine_if_ready().is_some_and(|engine| engine.is_worker_container_ip(ip))
}

impl EgressPolicy {
    /// Whether a denied address is allowlisted for this port. Hostname
    /// entries resolve on every check; the resolution cache keeps that
    /// to one lookup per name per few seconds.
    async fn allowlisted(&self, ip: IpAddr, port: u16) -> bool {
        for entry in &self.allow {
            match entry {
                AllowEntry::Subnet(cidr) if cidr.contains(ip) => return true,
                AllowEntry::HostPort { host, port: allow } if *allow == port => match host {
                    AllowHost::Ip(addr) if *addr == ip => return true,
                    AllowHost::Name(name) => {
                        if let Ok(addrs) = resolve_host(name).await {
                            if addrs.contains(&ip) {
                                return true;
                            }
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }
        false
    }

    /// Whether any allowlist entry is port-scoped. Tenant fetch
    /// clients consult this to choose between a shared client (exact
    /// without port knowledge when no `host:port` entry exists) and a
    /// per-request client pinned to the hop's port.
    pub fn has_port_scoped_entries(&self) -> bool {
        self.allow
            .iter()
            .any(|entry| matches!(entry, AllowEntry::HostPort { .. }))
    }

    /// The full verdict for one endpoint: public passes, denied passes
    /// only when allowlisted for this port or a container endpoint this
    /// node published.
    pub async fn check_endpoint(
        &self,
        host: &str,
        ip: IpAddr,
        port: u16,
    ) -> Result<(), EgressRefusal> {
        if self.is_open() {
            return Ok(());
        }
        let Some(reason) = denied_reason(ip) else {
            return Ok(());
        };
        if self.allowlisted(ip, port).await {
            return Ok(());
        }
        if is_container_endpoint(ip, port) {
            return Ok(());
        }
        Err(EgressRefusal {
            host: host.to_string(),
            ip,
            reason,
        })
    }

    /// The verdict where no DNS is possible (the redirect policy runs in
    /// a synchronous callback): IP, CIDR, and IP:port allowlist entries
    /// plus the container exemption. Hostname allowlist entries cannot be
    /// resolved here and never match, so a redirect to a bare IP literal
    /// needs an IP or CIDR entry.
    pub fn check_endpoint_sync(
        &self,
        host: &str,
        ip: IpAddr,
        port: u16,
    ) -> Result<(), EgressRefusal> {
        if self.is_open() {
            return Ok(());
        }
        let Some(reason) = denied_reason(ip) else {
            return Ok(());
        };
        for entry in &self.allow {
            match entry {
                AllowEntry::Subnet(cidr) if cidr.contains(ip) => return Ok(()),
                AllowEntry::HostPort {
                    host: AllowHost::Ip(addr),
                    port: allow,
                } if *addr == ip && *allow == port => return Ok(()),
                _ => {}
            }
        }
        if is_container_endpoint(ip, port) {
            return Ok(());
        }
        Err(EgressRefusal {
            host: host.to_string(),
            ip,
            reason,
        })
    }

    /// Resolve a hostname and check every address precisely for this
    /// port. Returns the addresses the dial may try, the refusal when
    /// none survive, or the DNS failure, which is not a refusal. The
    /// published container form (`127.0.0.1` plus a mapped port) is
    /// excluded here on purpose: the reqwest resolver cannot verify a
    /// mapped port without knowing the port, so it would refuse what
    /// this check allowed. Container dials use the literal the host
    /// published, which takes the precise path instead.
    pub async fn check_host(&self, host: &str, port: u16) -> Result<Vec<IpAddr>, HostCheckError> {
        let addrs = resolve_host(host).await.map_err(HostCheckError::Resolve)?;
        if self.is_open() {
            return Ok(addrs);
        }
        let mut allowed = Vec::new();
        let mut refused: Option<EgressRefusal> = None;
        for ip in addrs {
            let verdict = match denied_reason(ip) {
                None => Ok(()),
                Some(reason) => {
                    if self.allowlisted(ip, port).await || is_container_ip(ip) {
                        Ok(())
                    } else {
                        Err(EgressRefusal {
                            host: host.to_string(),
                            ip,
                            reason,
                        })
                    }
                }
            };
            match verdict {
                Ok(()) => allowed.push(ip),
                Err(refusal) => {
                    refused.get_or_insert(refusal);
                }
            };
        }
        if allowed.is_empty() {
            let refusal = refused.expect("a refused address exists when none are allowed");
            return Err(HostCheckError::Refused(refusal));
        }
        Ok(allowed)
    }
}

/// How a hostname check failed.
#[derive(Debug)]
pub enum HostCheckError {
    /// Every resolved address was refused by the policy.
    Refused(EgressRefusal),
    /// The hostname did not resolve.
    Resolve(String),
}

/// Check one redirect hop's target. Hostnames pass through: the resolver
/// checks them after DNS at connect time. IP literals never reach the
/// resolver, so they are checked here, where the hop's port is known.
pub fn check_redirect_target(
    policy: &EgressPolicy,
    url: &reqwest::Url,
) -> Result<(), EgressRefusal> {
    if policy.is_open() {
        return Ok(());
    }
    let Some(host) = url.host_str() else {
        return Ok(());
    };
    let trimmed = host.trim_start_matches('[').trim_end_matches(']');
    let Ok(ip) = trimmed.parse::<IpAddr>() else {
        return Ok(());
    };
    let Some(port) = url.port_or_known_default() else {
        return Ok(());
    };
    policy.check_endpoint_sync(host, ip, port)
}

/// Judge a fetch URL before anything is sent: literals precisely,
/// hostnames after resolution (which also warms the cache the reqwest
/// resolver shares, closing the rebinding window between check and
/// connect). Malformed URLs, non-HTTP schemes, and DNS failures pass
/// through: reqwest reports those itself, exactly as before.
pub async fn check_fetch_url(policy: &EgressPolicy, url: &str) -> Result<(), EgressRefusal> {
    if policy.is_open() {
        return Ok(());
    }
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return Ok(());
    };
    if !matches!(parsed.scheme(), "http" | "https") {
        return Ok(());
    }
    let (Some(host), Some(port)) = (parsed.host_str(), parsed.port_or_known_default()) else {
        return Ok(());
    };
    let trimmed = host.trim_start_matches('[').trim_end_matches(']');
    if let Ok(ip) = trimmed.parse::<IpAddr>() {
        return policy.check_endpoint(host, ip, port).await;
    }
    match policy.check_host(host, port).await {
        Ok(_) => Ok(()),
        Err(HostCheckError::Refused(refusal)) => Err(refusal),
        Err(HostCheckError::Resolve(_)) => Ok(()),
    }
}

/// Follow redirects like reqwest's default (at most ten hops) while
/// refusing hops to refused literals. Hostname hops pass through to the
/// filtering resolver, which judges them after DNS. Shared clients use
/// this: their policies hold no `host:port` entry, so the resolver needs
/// no port to be exact.
pub fn follow_redirect_policy(policy: Arc<EgressPolicy>) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt: reqwest::redirect::Attempt| {
        if attempt.previous().len() >= 10 {
            return attempt.error("too many redirects");
        }
        if let Err(refusal) = check_redirect_target(&policy, attempt.url()) {
            return attempt.error(FetchRefused(refusal));
        }
        attempt.follow()
    })
}

/// The destination port of the current fetch hop, shared between one
/// request's redirect policy and its filtering resolver. The redirect
/// policy sees every hop's full URL and re-pins the port before
/// following; the resolver matches `host:port` allowlist entries against
/// it. The pin starts at the initial request's port, so the initial hop
/// and every redirect hop match the port exactly. A pin is single-use:
/// each request builds its own client holding its own pin, so concurrent
/// requests to one host on different ports can never share it.
#[derive(Clone, Debug)]
pub struct HopPort(Arc<AtomicU16>);

impl HopPort {
    /// Pin the hop port `port`.
    pub fn new(port: u16) -> Self {
        Self(Arc::new(AtomicU16::new(port)))
    }

    /// A pin that matches no port-scoped entry: allowlist ports are
    /// never 0, so a resolver holding this fails closed on every
    /// `host:port` entry. Shared clients hold one; their policies have
    /// no such entries, which the client selection enforces and asserts.
    pub fn unpinned() -> Self {
        Self::new(0)
    }

    fn get(&self) -> u16 {
        self.0.load(Ordering::SeqCst)
    }

    fn set(&self, port: u16) {
        self.0.store(port, Ordering::SeqCst);
    }
}

/// The initial hop's port for a tenant fetch client pin: the URL's
/// explicit port, or the scheme default (`port_or_known_default`
/// resolves both). Anything unparsable pins 0, which matches no
/// port-scoped entry; reqwest reports such URLs itself before any
/// resolution runs.
pub fn initial_hop_port(url: &str) -> u16 {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.port_or_known_default())
        .unwrap_or(0)
}

/// Judge one redirect hop for a pinned client: re-pin the hop port from
/// the hop's full URL, then refuse refused literals. Hostname hops pass
/// through to the pinned resolver, which judges them after DNS against
/// the re-pinned port.
pub fn check_redirect_hop(
    policy: &EgressPolicy,
    pin: &HopPort,
    url: &reqwest::Url,
) -> Result<(), EgressRefusal> {
    if let Some(hop_port) = url.port_or_known_default() {
        pin.set(hop_port);
    }
    check_redirect_target(policy, url)
}

/// Follow redirects like [`follow_redirect_policy`], re-pinning the hop
/// port from every hop's full URL so `host:port` entries match exactly.
/// Per-request clients use this with their own pin.
pub fn follow_redirect_policy_pinned(
    policy: Arc<EgressPolicy>,
    pin: HopPort,
) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt: reqwest::redirect::Attempt| {
        if attempt.previous().len() >= 10 {
            return attempt.error("too many redirects");
        }
        if let Err(refusal) = check_redirect_hop(&policy, &pin, attempt.url()) {
            return attempt.error(FetchRefused(refusal));
        }
        attempt.follow()
    })
}

/// The per-hop subrequest charge for a pinned counting client: the fetch
/// op supplies the invocation's charge, which the policy calls after the
/// egress check on every followed hop.
pub type SubrequestCharge = Arc<dyn Fn() -> Result<(), String> + Send + Sync>;

/// Follow redirects like [`follow_redirect_policy_pinned`], additionally
/// charging one subrequest per followed hop. Per-request counting
/// clients use this with their own pin.
pub fn follow_redirect_policy_pinned_counted(
    policy: Arc<EgressPolicy>,
    pin: HopPort,
    charge: SubrequestCharge,
) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt: reqwest::redirect::Attempt| {
        if attempt.previous().len() >= 10 {
            return attempt.error("too many redirects");
        }
        if let Err(refusal) = check_redirect_hop(&policy, &pin, attempt.url()) {
            return attempt.error(FetchRefused(refusal));
        }
        // A refused literal never reaches the charge: the hop dies above.
        if let Err(error) = charge() {
            return attempt.error(crate::js::RedirectSubrequestLimit(error));
        }
        attempt.follow()
    })
}

/// The shared hostname resolution: test overrides, then a short-lived
/// cache shared by the pre-send check and the reqwest resolver, then
/// the system resolver. Literal hosts never reach DNS.
pub async fn resolve_host(host: &str) -> Result<Vec<IpAddr>, String> {
    if let Ok(addr) = host.parse::<IpAddr>() {
        return Ok(vec![addr]);
    }
    #[cfg(test)]
    if let Some(addrs) = test_dns_lookup(host) {
        return Ok(addrs);
    }
    if let Some(addrs) = resolution_cache_lookup(host) {
        return Ok(addrs);
    }
    let mut addrs: Vec<IpAddr> = tokio::net::lookup_host((host, 0))
        .await
        .map_err(|error| format!("resolve {host}: {error}"))?
        .map(|addr| addr.ip())
        .collect();
    addrs.dedup();
    if addrs.is_empty() {
        return Err(format!("resolve {host}: no addresses"));
    }
    resolution_cache_store(host, &addrs);
    Ok(addrs)
}

type ResolutionCache = Mutex<HashMap<String, (Instant, Vec<IpAddr>)>>;

fn resolution_cache() -> &'static ResolutionCache {
    static CACHE: OnceLock<ResolutionCache> = OnceLock::new();
    CACHE.get_or_init(Default::default)
}

fn cache_key(host: &str) -> String {
    host.to_ascii_lowercase()
}

fn resolution_cache_lookup(host: &str) -> Option<Vec<IpAddr>> {
    let cache = resolution_cache().lock().unwrap();
    let (at, addrs) = cache.get(&cache_key(host))?;
    (*at + RESOLUTION_TTL > Instant::now()).then(|| addrs.clone())
}

fn resolution_cache_store(host: &str, addrs: &[IpAddr]) {
    let mut cache = resolution_cache().lock().unwrap();
    if cache.len() >= RESOLUTION_CACHE_CAP {
        cache.retain(|_, (at, _)| *at + RESOLUTION_TTL > Instant::now());
        if cache.len() >= RESOLUTION_CACHE_CAP {
            cache.clear();
        }
    }
    cache.insert(cache_key(host), (Instant::now(), addrs.to_vec()));
}

#[cfg(test)]
fn test_dns() -> &'static Mutex<HashMap<String, Vec<IpAddr>>> {
    static OVERRIDES: OnceLock<Mutex<HashMap<String, Vec<IpAddr>>>> = OnceLock::new();
    OVERRIDES.get_or_init(Default::default)
}

/// Resolve `host` to fixed addresses in unit tests, bypassing the cache
/// and the system resolver. Each test clears the table when done.
#[cfg(test)]
pub fn set_test_dns(host: &str, addrs: Vec<IpAddr>) {
    test_dns().lock().unwrap().insert(cache_key(host), addrs);
}

/// Clear one test override.
#[cfg(test)]
pub fn clear_test_dns(host: &str) {
    test_dns().lock().unwrap().remove(&cache_key(host));
}

#[cfg(test)]
fn test_dns_lookup(host: &str) -> Option<Vec<IpAddr>> {
    test_dns().lock().unwrap().get(&cache_key(host)).cloned()
}

/// A reqwest resolver that enforces the tenant policy after DNS: public
/// addresses pass, denied addresses pass only when allowlisted for the
/// pinned hop port or naming one of this node's containers. Refusing
/// everything fails the request, which is what makes redirect hops
/// re-check per hop.
#[derive(Debug)]
pub struct EgressResolver {
    policy: Arc<EgressPolicy>,
    port: HopPort,
}

impl EgressResolver {
    /// Resolve hostnames under `policy`, matching `host:port` entries
    /// against the pinned hop port.
    pub fn new(policy: Arc<EgressPolicy>, port: HopPort) -> Self {
        Self { policy, port }
    }
}

impl reqwest::dns::Resolve for EgressResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let policy = self.policy.clone();
        let port = self.port.clone();
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs = resolve_host(&host).await.map_err(|error| {
                Box::new(std::io::Error::other(error)) as Box<dyn std::error::Error + Send + Sync>
            })?;
            if policy.is_open() {
                let addrs: reqwest::dns::Addrs =
                    Box::new(addrs.into_iter().map(|ip| SocketAddr::new(ip, 0)));
                return Ok(addrs);
            }
            let port = port.get();
            let mut allowed = Vec::new();
            let mut refused: Option<EgressRefusal> = None;
            for ip in addrs {
                let verdict = match denied_reason(ip) {
                    None => true,
                    Some(reason) => {
                        if policy.allowlisted(ip, port).await || is_container_ip(ip) {
                            true
                        } else {
                            refused.get_or_insert(EgressRefusal {
                                host: host.clone(),
                                ip,
                                reason,
                            });
                            false
                        }
                    }
                };
                if verdict {
                    allowed.push(SocketAddr::new(ip, 0));
                }
            }
            if allowed.is_empty() {
                let refusal = refused.expect("a refused address exists when none are allowed");
                tracing::warn!(
                    event = "egress_refused",
                    host = %refusal.host,
                    ip = %refusal.ip,
                    reason = refusal.reason,
                    "worker egress refused"
                );
                return Err(
                    Box::new(FetchRefused(refusal)) as Box<dyn std::error::Error + Send + Sync>
                );
            }
            let addrs: reqwest::dns::Addrs = Box::new(allowed.into_iter());
            Ok(addrs)
        })
    }
}

/// Add the tenant policy to a reqwest client builder. Open mode leaves
/// the builder untouched, so the default path builds exactly the client
/// it always has, proxy handling included. Public mode installs the
/// filtering resolver, pinned to the hop port, and disables proxies
/// outright: through a proxy the target would resolve with the proxy's
/// DNS instead of the checked one, and redirect hops would expose only
/// the proxy's address to the resolver, so a proxied tenant fetch cannot
/// be policed.
pub fn apply_tenant_policy(
    builder: reqwest::ClientBuilder,
    policy: &Arc<EgressPolicy>,
    port: HopPort,
) -> reqwest::ClientBuilder {
    if policy.is_open() {
        builder
    } else {
        builder
            .dns_resolver(Arc::new(EgressResolver::new(policy.clone(), port)))
            .no_proxy()
    }
}

/// How a policed TCP dial failed.
#[derive(Debug)]
pub enum DialError {
    /// Every resolved address was refused by the policy.
    Refused(EgressRefusal),
    /// The hostname did not resolve.
    Resolve(String),
    /// Every allowed address failed TCP; carries the last error.
    Tcp(String),
}

/// Dial `host:port` through the tenant policy: resolve, drop refused
/// addresses, and try the survivors in order as bare `SocketAddr`s, so
/// nothing re-resolves between the check and the connect. Open mode
/// dials exactly as before, so its errors read byte-identically.
pub async fn policed_tcp_connect(
    policy: &EgressPolicy,
    host: &str,
    port: u16,
) -> Result<tokio::net::TcpStream, DialError> {
    if policy.is_open() {
        return tokio::net::TcpStream::connect((host, port))
            .await
            .map_err(|error| DialError::Tcp(error.to_string()));
    }
    let addrs = resolve_host(host).await.map_err(DialError::Resolve)?;
    let mut allowed = Vec::new();
    let mut refused: Option<EgressRefusal> = None;
    for ip in addrs {
        match policy.check_endpoint(host, ip, port).await {
            Ok(()) => allowed.push(SocketAddr::new(ip, port)),
            Err(refusal) => {
                refused.get_or_insert(refusal);
            }
        };
    }
    if allowed.is_empty() {
        let refusal = refused.expect("a refused address exists when none are allowed");
        tracing::warn!(
            event = "egress_refused",
            host = %refusal.host,
            ip = %refusal.ip,
            reason = refusal.reason,
            "worker egress refused"
        );
        return Err(DialError::Refused(refusal));
    }
    dial_sockets(&allowed).await.map_err(DialError::Tcp)
}

async fn dial_sockets(addrs: &[SocketAddr]) -> Result<tokio::net::TcpStream, String> {
    let mut last = String::new();
    for addr in addrs {
        match tokio::net::TcpStream::connect(*addr).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last = error.to_string(),
        }
    }
    Err(last)
}

/// The fail-closed policy: `public` with no exceptions. [`EgressPolicy::global`]
/// runs this when the environment does not parse, so an invalid
/// configuration can never silently run open.
fn closed_policy() -> Arc<EgressPolicy> {
    Arc::new(EgressPolicy {
        mode: EgressMode::Public,
        allow: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn public(allow: &str) -> EgressPolicy {
        EgressPolicy {
            mode: EgressMode::Public,
            allow: parse_allow_list(allow).unwrap(),
        }
    }

    fn open() -> EgressPolicy {
        EgressPolicy {
            mode: EgressMode::Open,
            allow: Vec::new(),
        }
    }

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    #[test]
    fn mode_parses_open_public_and_rejects_the_rest() {
        assert_eq!(EgressMode::parse("open").unwrap(), EgressMode::Open);
        assert_eq!(EgressMode::parse("public").unwrap(), EgressMode::Public);
        for bad in ["", "OPEN", "deny", "public ", " open"] {
            assert!(EgressMode::parse(bad).is_err(), "{bad:?} must not parse");
        }
    }

    #[test]
    fn allow_list_accepts_cidrs_ips_and_host_ports() {
        assert!(parse_allow_list("").unwrap().is_empty());
        assert!(parse_allow_list("   ").unwrap().is_empty());
        let entries = parse_allow_list(
            "10.89.0.0/24, 2001:db8::/32,192.0.2.7,::1, 10.89.0.7:9000, [fd00::1]:443, rustfs:9000",
        )
        .unwrap();
        assert_eq!(entries.len(), 7);
        assert!(matches!(
            entries[0],
            AllowEntry::Subnet(Cidr { prefix: 24, .. })
        ));
        assert!(matches!(
            entries[2],
            AllowEntry::Subnet(Cidr { prefix: 32, .. })
        ));
        assert!(matches!(
            entries[3],
            AllowEntry::Subnet(Cidr { prefix: 128, .. })
        ));
        assert!(matches!(
            entries[4],
            AllowEntry::HostPort {
                host: AllowHost::Ip(_),
                port: 9000
            }
        ));
        assert!(matches!(
            entries[6],
            AllowEntry::HostPort {
                host: AllowHost::Name(_),
                port: 9000
            }
        ));
    }

    #[test]
    fn allow_list_rejects_malformed_entries() {
        // A bare hostname without a port is too broad to allowlist: the
        // operator must name the port, as in the nft rule this replaces.
        for bad in [
            "rustfs",
            "10.0.0.0/33",
            "10.0.0.0/8:9000",
            "10.0.0.0/-1",
            "999.0.0.0/8",
            "fe80::/129",
            "host:0",
            "10.9.8.7:0",
            "[::1]:0",
            "host:99999",
            "host:http",
            "[::1]",
            "[::1]8080",
            "10.0.0.1,",
            ",10.0.0.1",
            "10.0.0.1,,10.0.0.2",
            "with space:80",
        ] {
            assert!(parse_allow_list(bad).is_err(), "{bad:?} must not parse");
        }
    }

    #[test]
    fn policy_values_default_to_open_with_no_exceptions() {
        // The values, not the process environment: no test in this
        // binary mutates `CELLD_EGRESS`/`CELLD_EGRESS_ALLOW`, so parallel
        // tests cannot observe each other's configuration.
        let policy = EgressPolicy::from_values(None, None).unwrap();
        assert!(policy.is_open());
        assert!(policy.allow.is_empty());
        let policy = EgressPolicy::from_values(Some("public"), Some("10.0.0.0/8")).unwrap();
        assert!(!policy.is_open());
        assert_eq!(policy.allow.len(), 1);
        assert!(EgressPolicy::from_values(Some("bogus"), None).is_err());
        assert!(EgressPolicy::from_values(Some("public"), Some("bogus")).is_err());
    }

    #[test]
    fn closed_policy_refuses_loopback() {
        let policy = closed_policy();
        assert!(!policy.is_open());
        assert!(policy.allow.is_empty());
        assert!(denied_reason(ip("127.0.0.1")).is_some());
    }

    #[test]
    fn denied_table_covers_every_non_public_range() {
        let denied = [
            // Loopback.
            "127.0.0.1",
            "127.0.0.2",
            "127.255.255.255",
            "::1",
            // Private.
            "10.0.0.1",
            "10.255.255.255",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.0.1",
            "192.168.255.255",
            // Link-local.
            "169.254.1.1",
            "fe80::1",
            "fe80:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            "febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
            // Carrier-grade NAT.
            "100.64.0.1",
            "100.127.255.255",
            // IPv6 unique-local.
            "fc00::1",
            "fd12:3456:789a::1",
            // Unspecified.
            "0.0.0.0",
            "0.1.2.3",
            "::",
            // Multicast.
            "224.0.0.1",
            "239.255.255.255",
            "ff02::1",
            "ff00::1",
            // IPv4-mapped forms.
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
            "::ffff:192.168.1.1",
            "::ffff:172.16.0.1",
            "::ffff:169.254.1.1",
            "::ffff:100.64.0.1",
            "::ffff:0.0.0.0",
            "::ffff:224.0.0.1",
            // The cloud metadata address.
            "169.254.169.254",
            // Reserved 240.0.0.0/4, including broadcast.
            "240.0.0.1",
            "255.255.255.255",
            "::ffff:240.0.0.1",
            // Private/loopback IPv4 embedded in transition forms.
            "64:ff9b::0a00:0001",
            "64:ff9b::7f00:0001",
            "2002:0a00:0001::1",
            "2002:7f00:0001::1",
            "::10.0.0.1",
            "::127.0.0.1",
            // Teredo 2001::/32 with the client field de-obfuscated to
            // 10.0.0.1 (!0x0a000001) and 127.0.1.1 (!0x7f000101).
            "2001::8000:0:f5ff:fffe",
            "2001::8000:0:80ff:fefe",
        ];
        for text in denied {
            assert!(denied_reason(ip(text)).is_some(), "{text} must be denied");
        }
        let public = [
            "8.8.8.8",
            "1.1.1.1",
            "93.184.216.34",
            "100.63.255.255",
            "100.128.0.1",
            "172.15.255.255",
            "172.32.0.1",
            "192.167.255.255",
            "192.169.0.1",
            "169.253.255.255",
            "169.255.0.1",
            "126.255.255.255",
            "128.0.0.1",
            "223.255.255.255",
            "2001:4860:4860::8888",
            "2001:db8::1",
            "fe00::1",
            "fec0::1",
            // A mapped public address is public.
            "::ffff:8.8.8.8",
            // Transition forms embedding a public address stay public.
            "64:ff9b::0808:0808",
            "2002:0808:0808::1",
            "::8.8.8.8",
            "2001::8000:0:f7f7:f7f7",
        ];
        for text in public {
            assert_eq!(denied_reason(ip(text)), None, "{text} must pass");
        }
    }

    #[test]
    fn metadata_and_mapped_reasons_name_the_range() {
        assert_eq!(
            denied_reason(ip("169.254.169.254")),
            Some("the cloud metadata address is blocked")
        );
        assert_eq!(
            denied_reason(ip("::ffff:127.0.0.1")),
            Some("IPv4-mapped loopback addresses are blocked")
        );
        assert_eq!(
            denied_reason(ip("::ffff:10.9.8.7")),
            Some("IPv4-mapped private addresses are blocked")
        );
    }

    #[test]
    fn embedded_and_reserved_reasons_name_the_range() {
        assert_eq!(
            denied_reason(ip("240.0.0.1")),
            Some("reserved addresses are blocked")
        );
        assert_eq!(
            denied_reason(ip("::ffff:255.255.255.255")),
            Some("IPv4-mapped reserved addresses are blocked")
        );
        assert_eq!(
            denied_reason(ip("64:ff9b::0a00:0001")),
            Some("NAT64 private addresses are blocked")
        );
        assert_eq!(
            denied_reason(ip("2002:7f00:0001::1")),
            Some("6to4 loopback addresses are blocked")
        );
        assert_eq!(
            denied_reason(ip("2001::8000:0:f5ff:fffe")),
            Some("Teredo private addresses are blocked")
        );
        assert_eq!(
            denied_reason(ip("::10.0.0.1")),
            Some("IPv4-compatible private addresses are blocked")
        );
    }

    #[test]
    fn cidr_matching_respects_prefix_boundaries() {
        let cidr = parse_cidr("172.16.0.0/12").unwrap();
        assert!(cidr.contains(ip("172.16.0.1")));
        assert!(cidr.contains(ip("172.31.255.255")));
        assert!(!cidr.contains(ip("172.15.255.255")));
        assert!(!cidr.contains(ip("172.32.0.1")));
        assert!(!cidr.contains(ip("::1")));
        let host = parse_cidr("192.0.2.7/32").unwrap();
        assert!(host.contains(ip("192.0.2.7")));
        assert!(!host.contains(ip("192.0.2.8")));
        let v6 = parse_cidr("fc00::/7").unwrap();
        assert!(v6.contains(ip("fc00::1")));
        assert!(v6.contains(ip("fdff::1")));
        assert!(!v6.contains(ip("fe00::1")));
        assert!(!v6.contains(ip("10.0.0.1")));
    }

    #[tokio::test]
    async fn open_mode_allows_everything() {
        let policy = open();
        for text in [
            "127.0.0.1",
            "10.0.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "::1",
            "::ffff:10.0.0.1",
        ] {
            policy.check_endpoint("h", ip(text), 80).await.unwrap();
            policy.check_endpoint_sync("h", ip(text), 80).unwrap();
        }
    }

    #[tokio::test]
    async fn allowlisted_cidr_passes_on_any_port() {
        let policy = public("10.0.0.0/8,fd00::/8");
        policy
            .check_endpoint("h", ip("10.1.2.3"), 80)
            .await
            .unwrap();
        policy
            .check_endpoint("h", ip("10.1.2.3"), 9000)
            .await
            .unwrap();
        policy
            .check_endpoint("h", ip("fd00::9"), 443)
            .await
            .unwrap();
        assert!(policy
            .check_endpoint("h", ip("192.168.1.1"), 80)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn allowlisted_host_port_is_port_scoped() {
        let policy = public("10.9.8.7:9000");
        policy
            .check_endpoint("h", ip("10.9.8.7"), 9000)
            .await
            .unwrap();
        let refused = policy
            .check_endpoint("h", ip("10.9.8.7"), 9001)
            .await
            .unwrap_err();
        assert!(refused.to_string().contains("10.9.8.7"));
        assert!(policy
            .check_endpoint("h", ip("10.9.8.8"), 9000)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn allowlisted_hostname_resolves_on_every_check() {
        set_test_dns("egress-store-1.test", vec![ip("10.44.0.9")]);
        let policy = public("egress-store-1.test:9000");
        policy
            .check_endpoint("h", ip("10.44.0.9"), 9000)
            .await
            .unwrap();
        assert!(policy
            .check_endpoint("h", ip("10.44.0.9"), 9001)
            .await
            .is_err());
        assert!(policy
            .check_endpoint("h", ip("10.44.0.10"), 9000)
            .await
            .is_err());
        // The store moves: the stale address stops matching on its own.
        set_test_dns("egress-store-1.test", vec![ip("10.44.0.10")]);
        policy
            .check_endpoint("h", ip("10.44.0.10"), 9000)
            .await
            .unwrap();
        assert!(policy
            .check_endpoint("h", ip("10.44.0.9"), 9000)
            .await
            .is_err());
        clear_test_dns("egress-store-1.test");
    }

    #[tokio::test]
    async fn sync_check_cannot_match_hostname_entries() {
        set_test_dns("egress-store-2.test", vec![ip("10.45.0.9")]);
        let policy = public("egress-store-2.test:9000,10.45.0.0/24");
        // The CIDR entry matches synchronously; the hostname entry never
        // does, so redirect targets that are literals need IP/CIDR form.
        policy
            .check_endpoint_sync("h", ip("10.45.0.9"), 9000)
            .unwrap();
        let ip_only = public("egress-store-2.test:9000");
        assert!(ip_only
            .check_endpoint_sync("h", ip("10.45.0.9"), 9000)
            .is_err());
        // The async check matches the same hostname entry.
        ip_only
            .check_endpoint("h", ip("10.45.0.9"), 9000)
            .await
            .unwrap();
        clear_test_dns("egress-store-2.test");
    }

    #[tokio::test]
    async fn check_host_returns_only_surviving_addresses() {
        set_test_dns(
            "egress-mixed-1.test",
            vec![ip("10.0.0.1"), ip("93.184.216.34")],
        );
        let policy = public("");
        let allowed = policy.check_host("egress-mixed-1.test", 80).await.unwrap();
        assert_eq!(allowed, vec![ip("93.184.216.34")]);
        clear_test_dns("egress-mixed-1.test");
    }

    #[tokio::test]
    async fn check_host_refuses_when_nothing_survives() {
        set_test_dns(
            "egress-denied-1.test",
            vec![ip("10.0.0.1"), ip("192.168.1.1")],
        );
        let policy = public("");
        let Err(HostCheckError::Refused(refusal)) =
            policy.check_host("egress-denied-1.test", 80).await
        else {
            panic!("all-private hostnames must be refused");
        };
        assert!(refusal.to_string().contains("egress-denied-1.test"));
        assert!(refusal.to_string().contains("CELLD_EGRESS=public"));
        clear_test_dns("egress-denied-1.test");
    }

    #[tokio::test]
    async fn check_host_reports_dns_failure_as_dns_failure() {
        // A space can never resolve, so no network is touched.
        let policy = public("");
        let Err(HostCheckError::Resolve(_)) = policy.check_host("not a host!", 80).await else {
            panic!("an unresolvable hostname must report DNS failure, not refusal");
        };
    }

    #[tokio::test]
    async fn resolver_drops_refused_addresses_and_keeps_the_rest() {
        use reqwest::dns::Resolve;
        set_test_dns(
            "egress-mixed-2.test",
            vec![ip("10.0.0.1"), ip("93.184.216.34")],
        );
        let policy = Arc::new(public(""));
        let resolver = EgressResolver::new(policy, HopPort::unpinned());
        let name = reqwest::dns::Name::from_str("egress-mixed-2.test").unwrap();
        let addrs: Vec<SocketAddr> = resolver.resolve(name).await.unwrap().collect();
        assert_eq!(addrs, vec![SocketAddr::new(ip("93.184.216.34"), 0)]);
        clear_test_dns("egress-mixed-2.test");
    }

    #[tokio::test]
    async fn resolver_refuses_with_the_fetch_marker_when_nothing_survives() {
        use reqwest::dns::Resolve;
        set_test_dns("egress-denied-2.test", vec![ip("169.254.169.254")]);
        let policy = Arc::new(public(""));
        let resolver = EgressResolver::new(policy, HopPort::unpinned());
        let name = reqwest::dns::Name::from_str("egress-denied-2.test").unwrap();
        let Err(error) = resolver.resolve(name).await else {
            panic!("an all-denied hostname must fail resolution");
        };
        let refused = error.downcast_ref::<FetchRefused>().expect("FetchRefused");
        assert!(refused.to_string().starts_with(FETCH_REFUSAL_MARKER));
        assert!(refused.to_string().contains("metadata"));
        clear_test_dns("egress-denied-2.test");
    }

    #[tokio::test]
    async fn resolver_matches_host_port_entries_against_the_pin() {
        use reqwest::dns::Resolve;
        set_test_dns("egress-store-3.test", vec![ip("10.46.0.9")]);
        set_test_dns("egress-target-3.test", vec![ip("10.46.0.9")]);
        let policy = Arc::new(public("egress-store-3.test:9000"));
        // Pinned to the entry's port, the allowlisted address resolves.
        let resolver = EgressResolver::new(policy.clone(), HopPort::new(9000));
        let name = reqwest::dns::Name::from_str("egress-target-3.test").unwrap();
        let addrs: Vec<SocketAddr> = resolver.resolve(name).await.unwrap().collect();
        assert_eq!(addrs, vec![SocketAddr::new(ip("10.46.0.9"), 0)]);
        // Pinned anywhere else, the same address is refused: the
        // redirect policy re-pins per hop, so a hop to another port
        // never inherits this entry.
        let resolver = EgressResolver::new(policy.clone(), HopPort::new(80));
        let name = reqwest::dns::Name::from_str("egress-target-3.test").unwrap();
        let Err(error) = resolver.resolve(name).await else {
            panic!("the pinned port must scope host:port entries");
        };
        assert!(error.downcast_ref::<FetchRefused>().is_some());
        // An unpinned resolver fails closed on port-scoped entries.
        let resolver = EgressResolver::new(policy, HopPort::unpinned());
        let name = reqwest::dns::Name::from_str("egress-target-3.test").unwrap();
        assert!(resolver.resolve(name).await.is_err());
        clear_test_dns("egress-store-3.test");
        clear_test_dns("egress-target-3.test");
    }

    #[test]
    fn redirect_hops_repin_the_port_from_the_full_url() {
        let policy = public("egress-store-hop.test:9000");
        let pin = HopPort::new(9000);
        let hop = |url: &str| check_redirect_hop(&policy, &pin, &url::Url::parse(url).unwrap());
        // Hostname hops pass through with the pin moved to the hop's
        // port, explicit or scheme-default.
        assert!(hop("http://egress-store-hop.test:80/next").is_ok());
        assert_eq!(pin.get(), 80);
        assert!(hop("https://egress-store-hop.test/next").is_ok());
        assert_eq!(pin.get(), 443);
        // Literal hops re-pin too, then judge the literal precisely.
        assert!(hop("http://10.9.8.7:9000/next").is_err());
        assert_eq!(pin.get(), 9000);
        // Open mode still passes everything through.
        assert!(check_redirect_hop(
            &open(),
            &pin,
            &url::Url::parse("http://127.0.0.1/x").unwrap()
        )
        .is_ok());
    }

    #[test]
    fn initial_hop_port_resolves_explicit_and_default_ports() {
        assert_eq!(initial_hop_port("http://store:9000/x"), 9000);
        assert_eq!(initial_hop_port("http://store/x"), 80);
        assert_eq!(initial_hop_port("https://store/x"), 443);
        assert_eq!(initial_hop_port("http://store:80/x"), 80);
        assert_eq!(initial_hop_port("not a url"), 0);
    }

    #[test]
    fn redirect_targets_check_literals_and_pass_hostnames_through() {
        let policy = public("10.9.8.7:9000,192.168.0.0/16");
        let check = |url: &str| check_redirect_target(&policy, &url::Url::parse(url).unwrap());
        // A private literal on a non-allowlisted port is refused.
        assert!(check("http://10.9.8.7:9001/next").is_err());
        assert!(check("http://127.0.0.1:8080/next").is_err());
        assert!(check("http://[::1]:8080/next").is_err());
        assert!(check("http://[::ffff:10.0.0.1]:80/next").is_err());
        // The allowlisted literal passes, on its port only.
        assert!(check("http://10.9.8.7:9000/next").is_ok());
        assert!(check("http://192.168.4.5:1234/next").is_ok());
        // Hostnames pass through to the resolver, which judges them
        // after DNS; public literals pass outright.
        assert!(check("http://example.com/next").is_ok());
        assert!(check("http://93.184.216.34/next").is_ok());
    }

    #[tokio::test]
    async fn fetch_precheck_judges_literals_and_hostnames() {
        set_test_dns("egress-denied-5.test", vec![ip("10.0.0.1")]);
        set_test_dns("egress-public-5.test", vec![ip("93.184.216.34")]);
        let policy = public("");
        assert!(check_fetch_url(&policy, "http://127.0.0.1:8080/x")
            .await
            .is_err());
        assert!(check_fetch_url(&policy, "http://[::1]/x").await.is_err());
        assert!(check_fetch_url(&policy, "http://egress-denied-5.test/x")
            .await
            .is_err());
        assert!(check_fetch_url(&policy, "http://egress-public-5.test/x")
            .await
            .is_ok());
        assert!(check_fetch_url(&policy, "https://93.184.216.34/x")
            .await
            .is_ok());
        // Anything reqwest itself reports passes through untouched.
        assert!(check_fetch_url(&policy, "not a url").await.is_ok());
        assert!(check_fetch_url(&policy, "ftp://10.0.0.1/x").await.is_ok());
        assert!(check_fetch_url(&policy, "http://not a host!/x")
            .await
            .is_ok());
        clear_test_dns("egress-denied-5.test");
        clear_test_dns("egress-public-5.test");
    }

    #[test]
    fn refusal_log_target_drops_userinfo_path_and_query() {
        let (scheme, host, port) =
            log_target_parts("http://user:pass@127.0.0.1:8080/a?b=c#f").unwrap();
        assert_eq!(scheme, "http");
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 8080);
        let rendered = format!("{scheme} {host} {port}");
        for secret in ["user", "pass", "/a", "b=c", "#f"] {
            assert!(!rendered.contains(secret), "{rendered} leaks {secret}");
        }
        assert_eq!(
            log_target_parts("https://example.com/x").unwrap(),
            ("https".to_string(), "example.com".to_string(), 443)
        );
        assert!(log_target_parts("not a url").is_none());
    }

    #[test]
    fn refusal_messages_name_host_address_and_reason() {
        let literal = EgressRefusal {
            host: "10.0.0.1".to_string(),
            ip: ip("10.0.0.1"),
            reason: "private addresses are blocked",
        };
        assert_eq!(
            literal.to_string(),
            "egress policy refused 10.0.0.1: private addresses are blocked; CELLD_EGRESS=public"
        );
        let named = EgressRefusal {
            host: "evil.example".to_string(),
            ip: ip("10.0.0.1"),
            reason: "private addresses are blocked",
        };
        assert!(named.to_string().contains("evil.example (10.0.0.1)"));
        let fetch = fetch_refusal_message(&named);
        assert!(fetch.starts_with(FETCH_REFUSAL_MARKER));
        assert!(fetch.contains("fetch failed:"));
    }

    #[tokio::test]
    async fn policed_connect_refuses_without_dialing() {
        set_test_dns("egress-denied-4.test", vec![ip("10.255.255.1")]);
        let policy = public("");
        // 10.255.255.1 may or may not route here; a refusal returns
        // before any dial, so this is fast either way.
        let Err(DialError::Refused(refusal)) =
            policed_tcp_connect(&policy, "egress-denied-4.test", 80).await
        else {
            panic!("a denied hostname must be refused, not dialed");
        };
        assert!(refusal.to_string().contains("egress-denied-4.test"));
        clear_test_dns("egress-denied-4.test");
    }

    #[tokio::test]
    async fn policed_connect_dials_allowed_addresses_in_order() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        // The first address refuses the connection; the dial moves on to
        // the allowed loopback address. A connect succeeds against the
        // backlog, so nothing needs to accept.
        set_test_dns("egress-mixed-4.test", vec![ip("10.0.0.1"), ip("127.0.0.1")]);
        let policy = public("127.0.0.0/8");
        let stream = policed_tcp_connect(&policy, "egress-mixed-4.test", port)
            .await
            .expect("the allowlisted address must be dialed");
        assert_eq!(stream.peer_addr().unwrap().ip(), ip("127.0.0.1"));
        clear_test_dns("egress-mixed-4.test");
    }

    #[tokio::test]
    async fn policed_connect_reports_tcp_failure_for_allowed_hosts() {
        // A closed loopback port refuses fast; open mode passes it and
        // TCP fails. The failure must read as a connection failure, not
        // a refusal — the same mapping the public path uses for allowed
        // addresses.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let policy = open();
        let Err(DialError::Tcp(_)) = policed_tcp_connect(&policy, "127.0.0.1", port).await else {
            panic!("a refused dial must fail TCP, not policy");
        };
    }

    #[test]
    fn resolution_cache_holds_and_bounds_entries() {
        resolution_cache_store("egress-cache-1.test", &[ip("93.184.216.34")]);
        assert_eq!(
            resolution_cache_lookup("EGRESS-CACHE-1.TEST"),
            Some(vec![ip("93.184.216.34")])
        );
        for n in 0..(RESOLUTION_CACHE_CAP + 64) {
            resolution_cache_store(&format!("egress-fill-{n}.test"), &[ip("93.184.216.34")]);
        }
        assert!(resolution_cache().lock().unwrap().len() <= RESOLUTION_CACHE_CAP);
    }

    #[tokio::test]
    async fn open_mode_resolver_returns_denied_addresses_unfiltered() {
        use reqwest::dns::Resolve;
        set_test_dns("egress-open-1.test", vec![ip("10.0.0.1")]);
        let policy = Arc::new(open());
        let resolver = EgressResolver::new(policy, HopPort::unpinned());
        let name = reqwest::dns::Name::from_str("egress-open-1.test").unwrap();
        let addrs: Vec<SocketAddr> = resolver.resolve(name).await.unwrap().collect();
        assert_eq!(addrs, vec![SocketAddr::new(ip("10.0.0.1"), 0)]);
        clear_test_dns("egress-open-1.test");
    }

    #[test]
    fn tenant_client_builders_build_in_both_modes() {
        for policy in [Arc::new(open()), Arc::new(public(""))] {
            let builder =
                apply_tenant_policy(reqwest::Client::builder(), &policy, HopPort::unpinned());
            builder.build().unwrap();
        }
    }

    /// Serve one connection with a fixed response; return the request
    /// bytes. Both sides close, so neither task can hang.
    async fn serve_once(listener: tokio::net::TcpListener, response: &'static [u8]) -> Vec<u8> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        loop {
            let n = stream.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            request.extend_from_slice(&buf[..n]);
            if request.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        stream.write_all(response).await.unwrap();
        stream.shutdown().await.ok();
        request
    }

    #[tokio::test]
    async fn public_tenant_client_ignores_proxy() {
        let target = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let target_addr = target.local_addr().unwrap();
        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();
        let target_task = tokio::spawn(async move {
            serve_once(
                target,
                b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: close\r\n\r\ndirect-ok",
            )
            .await
        });
        let proxy_task = tokio::spawn(async move {
            serve_once(
                proxy,
                b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 7\r\nConnection: close\r\n\r\nproxied",
            )
            .await
        });
        // An explicit proxy injected before the policy: adding any
        // proxy also disables reqwest's automatic system proxy, so the
        // builder carries exactly this proxy into `apply_tenant_policy`,
        // whatever the ambient environment holds. Public mode must clear
        // it via `.no_proxy()`, which drops explicit proxies too.
        let injected = reqwest::Proxy::all(format!("http://{proxy_addr}")).unwrap();
        let policy = Arc::new(public(""));
        let client = apply_tenant_policy(
            reqwest::Client::builder().proxy(injected),
            &policy,
            HopPort::unpinned(),
        )
        .build()
        .unwrap();
        // A private target is refused before anything is sent, proxy or
        // none; the check runs ahead of the client.
        assert!(check_fetch_url(&policy, "http://10.255.255.1/")
            .await
            .is_err());
        // A request the policy passes must travel direct: through a
        // proxy the target would resolve with the proxy's DNS and the
        // resolver would see only the proxy's address.
        let url = format!("http://{target_addr}/");
        let response = client.get(&url).send().await.expect("direct fetch");
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), "direct-ok");
        target_task.await.unwrap();
        // The direct response proves the single request went to the
        // target, so the proxy task is still waiting for a connection
        // that never arrives.
        assert!(
            !proxy_task.is_finished(),
            "the proxy listener must see no connection"
        );
        proxy_task.abort();
    }

    /// Serve up to `max_conns` sequential connections, answering each
    /// from its request path; return the paths seen.
    async fn serve_paths(
        listener: tokio::net::TcpListener,
        max_conns: usize,
        respond: impl Fn(&str) -> Vec<u8>,
    ) -> Vec<String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut seen = Vec::new();
        for _ in 0..max_conns {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while let Ok(n) = stream.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                request.extend_from_slice(&buf[..n]);
                if request.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let text = String::from_utf8_lossy(&request);
            let path = text
                .lines()
                .next()
                .and_then(|line| line.split_whitespace().nth(1))
                .unwrap_or("/")
                .to_string();
            if stream.write_all(&respond(&path)).await.is_err() {
                break;
            }
            stream.shutdown().await.ok();
            seen.push(path);
        }
        seen
    }

    /// Walk a reqwest error chain for the fetch refusal, as the op's
    /// error mapping does.
    fn find_fetch_refused(error: &reqwest::Error) -> Option<String> {
        let mut source: &(dyn std::error::Error + 'static) = error;
        loop {
            if let Some(refused) = source.downcast_ref::<FetchRefused>() {
                return Some(refused.to_string());
            }
            source = source.source()?;
        }
    }

    #[tokio::test]
    async fn allowlisted_host_port_is_exact_on_every_hop() {
        set_test_dns("egress-store-m2.test", vec![ip("127.0.0.1")]);
        let store = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let allowed = store.local_addr().unwrap().port();
        let other = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let wrong = other.local_addr().unwrap().port();
        let redirect_to = format!("http://egress-store-m2.test:{wrong}/fin");
        let store_task = tokio::spawn(async move {
            serve_paths(store, 2, |path| match path {
                "/start" => format!(
                    "HTTP/1.1 302 Found\r\nLocation: {redirect_to}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .into_bytes(),
                _ => b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nstore-ok"
                    .to_vec(),
            })
            .await
        });
        let other_task = tokio::spawn(async move {
            serve_paths(other, 1, |_| {
                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\nwrong-port"
                    .to_vec()
            })
            .await
        });
        let policy = Arc::new(public(&format!("egress-store-m2.test:{allowed}")));
        // One pinned client per request, as the fetch op builds them:
        // the pin starts at the request's own initial port.
        let client_for = |url: &str| {
            let pin = HopPort::new(initial_hop_port(url));
            apply_tenant_policy(
                reqwest::Client::builder()
                    .redirect(follow_redirect_policy_pinned(policy.clone(), pin.clone())),
                &policy,
                pin,
            )
            .build()
            .unwrap()
        };
        // The redirect carries the full next URL: a hop to the allowed
        // host on any other port is refused, before any dial.
        let start = format!("http://egress-store-m2.test:{allowed}/start");
        assert!(check_fetch_url(&policy, &start).await.is_ok());
        let Err(error) = client_for(&start).get(&start).send().await else {
            panic!("a redirect to the allowed host on another port must be refused");
        };
        let refused = find_fetch_refused(&error).expect("the refusal marker");
        assert!(refused.starts_with(FETCH_REFUSAL_MARKER), "{refused}");
        assert!(refused.contains("egress-store-m2.test"), "{refused}");
        assert!(
            !other_task.is_finished(),
            "the refused hop must never be dialed"
        );
        // The initial hop is judged on its own port too.
        let direct = format!("http://egress-store-m2.test:{wrong}/fin");
        assert!(check_fetch_url(&policy, &direct).await.is_err());
        // And the allowlisted endpoint itself keeps working.
        let ok = format!("http://egress-store-m2.test:{allowed}/ok");
        assert!(check_fetch_url(&policy, &ok).await.is_ok());
        let response = client_for(&ok)
            .get(&ok)
            .send()
            .await
            .expect("allowed fetch");
        assert_eq!(response.text().await.unwrap(), "store-ok");
        let seen = store_task.await.unwrap();
        assert_eq!(seen, vec!["/start".to_string(), "/ok".to_string()]);
        other_task.abort();
        clear_test_dns("egress-store-m2.test");
    }

    #[tokio::test]
    async fn open_tenant_client_keeps_proxy() {
        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = proxy.local_addr().unwrap();
        // The explicit proxy stands in for the configured proxy: open
        // mode is identity, so whatever proxy the builder carries —
        // explicit here, reqwest's system proxy in production — survives.
        let injected = reqwest::Proxy::all(format!("http://{proxy_addr}")).unwrap();
        let policy = Arc::new(open());
        let client = apply_tenant_policy(
            reqwest::Client::builder().proxy(injected),
            &policy,
            HopPort::unpinned(),
        )
        .build()
        .unwrap();
        // The target port is closed on purpose: in open mode the client
        // sends to the proxy, which is what accepts below.
        let fetch = tokio::spawn(async move { client.get("http://127.0.0.1:9/").send().await });
        let request = serve_once(
            proxy,
            b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await;
        let head = String::from_utf8_lossy(&request);
        assert!(
            head.starts_with("GET http://127.0.0.1:9/ "),
            "open mode proxies in absolute form: {head}"
        );
        fetch.abort();
    }
}
