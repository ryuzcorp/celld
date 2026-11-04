// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): per-node Workers Cache API store; deployment cache scope.
// Apache-2.0.

//! The process-local backing store for the Workers Cache API.
//!
//! `caches.default` and `caches.open(name)` share one in-memory, per-node
//! store, so every isolate of a script on the node sees the same entries.
//! The store mirrors Cloudflare's documented Cache semantics: freshness
//! from `s-maxage` / `max-age` / `Expires` with the edge-TTL defaults by
//! status when no freshness header is present, `no-store` / `private` /
//! `Set-Cookie` never cached, `Vary` ignored, conditional requests answered
//! 304, range requests answered 206 or 416, and `CF-Cache-Status: HIT` on
//! every answer built from a stored entry.
//!
//! Three boundaries matter here:
//!
//! - Isolation: the key starts with a scope the host read from isolate
//!   state, never from script input, so no script can name another scope's
//!   entries. `CELLD_CACHE_SCOPE=script` (the default) scopes by script
//!   name: a node serves one fleet from one process, which makes the
//!   script name the whole tenant boundary. `CELLD_CACHE_SCOPE=deployment`
//!   scopes by the serving deployment's identity instead, so every script
//!   of the deployment shares one `caches.default` and one set of
//!   `caches.open(name)` namespaces, like one Cloudflare zone — and every
//!   script of the deployment can read, overwrite, and purge every other
//!   script's entries, which is why the scope is opt-in.
//! - Memory: each scope is capped by `CELLD_CACHE_MAX_BYTES` (64 MiB by
//!   default, 0 disables the cache) with least-recently-used eviction, and
//!   one entry can never exceed `MAX_ENTRY_BYTES`. Every byte an entry
//!   accounts is measured from the stored key, headers, and body plus a
//!   fixed per-entry overhead, so the cap bounds real memory.
//! - Time: every decision takes wall-clock milliseconds from the
//!   execution-domain clock (`asyncrt::wall_ms`); the tests inject their
//!   own clock, so no test ever sleeps for an expiry.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Mutex, OnceLock};

/// Default process-wide cache size: large enough for API responses, small
/// enough to never surprise an operator. Overridden by
/// `CELLD_CACHE_MAX_BYTES`.
pub const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Largest single entry, body plus headers plus key. An entry above this
/// is never stored: one huge `put` must not evict the whole cache, and a
/// caller that caches multi-megabyte bodies one byte at a time would
/// otherwise churn the store on every request.
pub const MAX_ENTRY_BYTES: u64 = 16 * 1024 * 1024;

/// Fixed overhead charged per entry for what the byte counts below do
/// not measure: the two inline key structs (`entries` holds one, the
/// recency index its clone), the entry struct, the hash-table slot and
/// B-tree node share, and allocator size-class rounding. Roughly 160 +
/// 80 + 185 + 135 bytes on 64-bit; 640 covers it with headroom, erring
/// toward early eviction rather than past-cap memory.
const ENTRY_OVERHEAD_BYTES: u64 = 640;

/// Fixed overhead charged per stored header: the 48-byte
/// `(String, String)` vector slot plus vector growth slack and allocator
/// size-class rounding. Header name and value bytes are charged
/// separately; without this term a thousand tiny headers account a few
/// kilobytes while occupying tens.
const HEADER_OVERHEAD_BYTES: u64 = 96;

/// Resolve the process-wide cap once: an unset variable selects the
/// default, a supplied one must parse, so a typo cannot silently resize a
/// running node. `0` disables the cache. Loud on garbage even without the
/// startup `validate()` pass, so no caller can observe a silent default.
pub fn max_bytes() -> u64 {
    static LIMIT: OnceLock<Result<u64, String>> = OnceLock::new();
    LIMIT
        .get_or_init(|| parse_max_bytes(std::env::var("CELLD_CACHE_MAX_BYTES").ok()))
        .as_ref()
        .copied()
        .unwrap_or_else(|error| panic!("{error}"))
}

fn parse_max_bytes(value: Option<String>) -> Result<u64, String> {
    let Some(value) = value else {
        return Ok(DEFAULT_MAX_BYTES);
    };
    value
        .parse::<u64>()
        .map_err(|error| format!("CELLD_CACHE_MAX_BYTES has invalid value {value:?}: {error}"))
}

/// Which entries a cache op addresses: one script's, or the serving
/// deployment's. Read from `CELLD_CACHE_SCOPE`, resolved once per process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheScope {
    Script,
    Deployment,
}

/// Resolve the process-wide scope once: an unset variable selects
/// per-script entries, a supplied one must name a scope, so a typo cannot
/// silently share a running node's cache across scripts. Loud on garbage
/// even without the startup `validate()` pass, so no caller can observe a
/// silent default.
pub fn cache_scope() -> CacheScope {
    static SCOPE: OnceLock<Result<CacheScope, String>> = OnceLock::new();
    SCOPE
        .get_or_init(|| parse_cache_scope(std::env::var("CELLD_CACHE_SCOPE").ok()))
        .as_ref()
        .copied()
        .unwrap_or_else(|error| panic!("{error}"))
}

fn parse_cache_scope(value: Option<String>) -> Result<CacheScope, String> {
    let Some(value) = value else {
        return Ok(CacheScope::Script);
    };
    match value.as_str() {
        "script" => Ok(CacheScope::Script),
        "deployment" => Ok(CacheScope::Deployment),
        _ => Err(format!(
            "CELLD_CACHE_SCOPE must be script or deployment, not {value:?}"
        )),
    }
}

/// Which cache a key belongs to. `Default` is `caches.default` and `Named`
/// is one `caches.open(name)` namespace; the two never share entries, so
/// `open("default")` is a different cache from `caches.default`, as in
/// Cloudflare's own local implementation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Namespace {
    Default,
    Named(String),
}

impl Namespace {
    /// Parse the namespace word the `cache.js` handle passes: `default`
    /// for `caches.default`, `named:{name}` for `caches.open(name)`.
    pub fn parse(word: &str) -> Option<Self> {
        if word == "default" {
            Some(Namespace::Default)
        } else {
            word.strip_prefix("named:")
                .map(|name| Namespace::Named(name.to_string()))
        }
    }
}

/// A cache key: the owning scope, the namespace, and the request URL.
/// The scope is the script name in script scope, or the serving
/// deployment's identity in deployment scope — always host-held, never
/// script input. The method is always GET by the time a key is built —
/// `put` refuses any other method, and `match`/`delete` only look up
/// non-GET keys when `ignoreMethod` rewrites them to GET.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Key {
    scope: String,
    namespace: Namespace,
    url: String,
}

struct Entry {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    expires_at_ms: i64,
    bytes: u64,
    sequence: u64,
}

/// What `put` stores, borrowed from the op arguments.
pub struct PutRequest<'a> {
    pub method: &'a str,
    pub url: &'a str,
    pub status: u16,
    pub headers: &'a [(String, String)],
    pub body: &'a [u8],
}

/// What `match` looks up, borrowed from the op arguments.
pub struct LookupRequest<'a> {
    pub url: &'a str,
    pub method: &'a str,
    pub ignore_method: bool,
    pub request_headers: &'a [(String, String)],
}

/// Why `put` threw, with workerd's messages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutRejection {
    NonGet,
    PartialContent,
    VaryStar,
}

impl PutRejection {
    pub fn message(self) -> &'static str {
        match self {
            PutRejection::NonGet => "Cannot cache response to non-GET request.",
            PutRejection::PartialContent => {
                "Cannot cache response to a range request (206 Partial Content)."
            }
            PutRejection::VaryStar => "Cannot cache response with 'Vary: *' header.",
        }
    }
}

/// Whether `put` stored the response. `NotStored` still resolves the
/// `put()` promise with `undefined`: Cloudflare answers this case with an
/// internal 413 the Worker never sees.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PutOutcome {
    Stored,
    NotStored,
}

/// What `match` answers: a miss (Cloudflare's internal 504, which the
/// Worker reads as `undefined`), or a response to build. `body: None`
/// means a null body — a 304, a 416, or a stored null-body status — which
/// the caller must construct with `null` and not an empty byte string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MatchOutcome {
    Miss,
    Hit {
        status: u16,
        headers: Vec<(String, String)>,
        body: Option<Vec<u8>>,
    },
}

/// The Workers cache of one scope: entries keyed by scope, namespace, and
/// URL, with recency in a sequence-ordered map so eviction drops the
/// least-recently-used entry first.
pub struct WorkerCache {
    cap_bytes: u64,
    used_bytes: u64,
    sequence: u64,
    entries: HashMap<Key, Entry>,
    recency: BTreeMap<u64, Key>,
}

impl WorkerCache {
    pub fn new(cap_bytes: u64) -> Self {
        Self {
            cap_bytes,
            used_bytes: 0,
            sequence: 0,
            entries: HashMap::new(),
            recency: BTreeMap::new(),
        }
    }

    fn remove(&mut self, key: &Key) -> Option<Entry> {
        let entry = self.entries.remove(key)?;
        self.recency.remove(&entry.sequence);
        self.used_bytes = self.used_bytes.saturating_sub(entry.bytes);
        Some(entry)
    }

    fn touch(&mut self, key: &Key) {
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        self.recency.remove(&entry.sequence);
        self.sequence = self.sequence.wrapping_add(1);
        entry.sequence = self.sequence;
        self.recency.insert(entry.sequence, key.clone());
    }

    /// Store one response under its request URL. Validation failures throw
    /// (`PutRejection`); anything else that cannot be stored — directives,
    /// staleness, size — resolves `NotStored` without an error.
    pub fn put(
        &mut self,
        scope: &str,
        namespace: &Namespace,
        request: &PutRequest,
        now_ms: i64,
    ) -> Result<PutOutcome, PutRejection> {
        if request.method != "GET" {
            return Err(PutRejection::NonGet);
        }
        if request.status == 206 {
            return Err(PutRejection::PartialContent);
        }
        if request
            .headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("vary") && value.contains('*'))
        {
            return Err(PutRejection::VaryStar);
        }
        let Some((headers, expires_at_ms)) =
            storable_response(request.status, request.headers, now_ms)
        else {
            return Ok(PutOutcome::NotStored);
        };
        let bytes = entry_bytes(scope, namespace, request.url, &headers, request.body);
        if bytes > MAX_ENTRY_BYTES || bytes > self.cap_bytes {
            return Ok(PutOutcome::NotStored);
        }
        let key = Key {
            scope: scope.to_string(),
            namespace: namespace.clone(),
            url: request.url.to_string(),
        };
        self.remove(&key);
        self.sequence = self.sequence.wrapping_add(1);
        self.used_bytes = self.used_bytes.saturating_add(bytes);
        self.recency.insert(self.sequence, key.clone());
        self.entries.insert(
            key,
            Entry {
                status: request.status,
                headers,
                body: request.body.to_vec(),
                expires_at_ms,
                bytes,
                sequence: self.sequence,
            },
        );
        while self.used_bytes > self.cap_bytes {
            let Some((&oldest, _)) = self.recency.iter().next() else {
                break;
            };
            let oldest = self.recency.remove(&oldest).expect("recency key exists");
            self.remove(&oldest);
        }
        Ok(PutOutcome::Stored)
    }

    /// Look up one request URL. A hit refreshes recency; an expired entry
    /// is removed and reads as a miss.
    pub fn lookup(
        &mut self,
        scope: &str,
        namespace: &Namespace,
        request: &LookupRequest,
        now_ms: i64,
    ) -> MatchOutcome {
        if !request.ignore_method && request.method != "GET" {
            return MatchOutcome::Miss;
        }
        let key = Key {
            scope: scope.to_string(),
            namespace: namespace.clone(),
            url: request.url.to_string(),
        };
        let Some(entry) = self.entries.get(&key) else {
            return MatchOutcome::Miss;
        };
        if now_ms >= entry.expires_at_ms {
            self.remove(&key);
            return MatchOutcome::Miss;
        }
        self.touch(&key);
        let entry = self.entries.get(&key).expect("touched entry exists");
        answer(entry, request.request_headers)
    }

    /// Drop every entry. The node calls this when memory pressure starts
    /// shedding load: best-effort cache bytes go before billable cells.
    pub fn shed(&mut self) {
        self.entries.clear();
        self.recency.clear();
        self.used_bytes = 0;
    }

    /// Delete one entry. An expired entry is removed but reports `false`:
    /// it was already gone as far as any reader could tell.
    pub fn delete(
        &mut self,
        scope: &str,
        namespace: &Namespace,
        url: &str,
        method: &str,
        ignore_method: bool,
        now_ms: i64,
    ) -> bool {
        if !ignore_method && method != "GET" {
            return false;
        }
        let key = Key {
            scope: scope.to_string(),
            namespace: namespace.clone(),
            url: url.to_string(),
        };
        let expired = self
            .entries
            .get(&key)
            .is_some_and(|entry| now_ms >= entry.expires_at_ms);
        if self.remove(&key).is_none() {
            return false;
        }
        !expired
    }
}

/// What one entry costs against the cap: the key bytes twice (the map
/// holds one `Key`, the recency index its clone), the header bytes plus
/// a per-header slot charge, the body, and the fixed overhead. Every
/// term errs toward over-counting, so `used_bytes` bounds real memory
/// instead of reporting a fraction of it.
fn entry_bytes(
    scope: &str,
    namespace: &Namespace,
    url: &str,
    headers: &[(String, String)],
    body: &[u8],
) -> u64 {
    let namespace = match namespace {
        Namespace::Default => 0,
        Namespace::Named(name) => name.len() as u64,
    };
    let key = scope.len() as u64 + namespace + url.len() as u64;
    let headers: u64 = headers
        .iter()
        .map(|(name, value)| name.len() as u64 + value.len() as u64 + HEADER_OVERHEAD_BYTES)
        .sum();
    key * 2 + headers + body.len() as u64 + ENTRY_OVERHEAD_BYTES
}

fn header_values<'a>(headers: &'a [(String, String)], name: &str) -> Vec<&'a str> {
    headers
        .iter()
        .filter(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
        .collect()
}

/// The combined value of a single-valued header, or `None` when absent.
/// Repeated headers join with `, `, as `Headers.get()` reads them.
fn header_value(headers: &[(String, String)], name: &str) -> Option<String> {
    let values = header_values(headers, name);
    if values.is_empty() {
        None
    } else {
        Some(values.join(", "))
    }
}

/// One parsed `Cache-Control` directive: the lower-cased name and its
/// unquoted value, if it carries one.
struct Directive {
    name: String,
    value: Option<String>,
}

fn parse_cache_control(headers: &[(String, String)]) -> Vec<Directive> {
    let mut directives = Vec::new();
    for value in header_values(headers, "cache-control") {
        for part in value.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (name, value) = match part.split_once('=') {
                Some((name, value)) => {
                    let value = value.trim();
                    let unquoted = value
                        .strip_prefix('"')
                        .and_then(|inner| inner.strip_suffix('"'))
                        .unwrap_or(value);
                    (name, Some(unquoted.to_string()))
                }
                None => (part, None),
            };
            directives.push(Directive {
                name: name.trim().to_ascii_lowercase(),
                value,
            });
        }
    }
    directives
}

/// The value of the last occurrence of a `Cache-Control` directive.
/// Doubled directives are invalid; the last one wins, as Cloudflare's own
/// local implementation parses them.
fn last_directive_value<'a>(directives: &'a [Directive], name: &str) -> Option<&'a str> {
    directives
        .iter()
        .rev()
        .find(|directive| directive.name == name)
        .map(|directive| directive.value.as_deref().unwrap_or(""))
}

/// Parse a delta-seconds value the way a cache must: digits saturate at
/// the largest representable age, anything else is zero — an invalid
/// `max-age` is already stale, never fresh.
fn parse_delta_seconds(value: &str) -> u64 {
    let value = value.trim();
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return 0;
    }
    value.parse::<u64>().unwrap_or(u64::MAX)
}

/// Delta-seconds as milliseconds, saturating at the largest representable
/// span: a plain `as i64` would wrap a huge age negative and read it as
/// already stale.
fn delta_seconds_ms(delta: u64) -> i64 {
    delta.saturating_mul(1000).min(i64::MAX as u64) as i64
}

fn system_time_ms(time: std::time::SystemTime) -> i64 {
    time.duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn parse_http_date_ms(value: &str) -> Option<i64> {
    httpdate::parse_http_date(value).ok().map(system_time_ms)
}

/// Cloudflare's edge-TTL defaults: the TTL by status when neither
/// `Cache-Control` nor `Expires` names one. Every other status needs
/// explicit freshness to be stored.
fn default_ttl_ms(status: u16) -> Option<i64> {
    match status {
        200 | 301 => Some(120 * 60 * 1000),
        302 | 303 => Some(20 * 60 * 1000),
        404 | 410 => Some(3 * 60 * 1000),
        _ => None,
    }
}

/// Decide whether a response is storable and, when it is, which headers to
/// keep and when the entry expires. Returns `None` for every response
/// Cloudflare would answer with its internal 413: `no-store`, `private`
/// (other than the `Set-Cookie` exception below), `no-cache`, a surviving
/// `Set-Cookie`, an already-stale freshness, or a status with neither
/// explicit freshness nor a default TTL.
///
/// `no-cache` is not stored because `match` never revalidates: serving a
/// stored `no-cache` entry without contacting the origin would violate the
/// directive, so the entry reads as a miss instead.
///
/// The documented exception: `Cache-Control: private=set-cookie` stores
/// the response with the `Set-Cookie` headers — and the spent directive —
/// removed.
fn storable_response(
    status: u16,
    headers: &[(String, String)],
    now_ms: i64,
) -> Option<(Vec<(String, String)>, i64)> {
    let directives = parse_cache_control(headers);
    if directives.iter().any(|d| d.name == "no-store") {
        return None;
    }
    if directives.iter().any(|d| d.name == "no-cache") {
        return None;
    }
    let mut strips_cookies = false;
    for directive in directives.iter().filter(|d| d.name == "private") {
        let fields = directive.value.as_deref()?;
        let only_cookies = fields
            .split([',', ' ', '\t'])
            .filter(|field| !field.is_empty())
            .all(|field| field.eq_ignore_ascii_case("set-cookie"));
        if !only_cookies {
            return None;
        }
        strips_cookies = true;
    }
    // Presence alone disqualifies: an empty `Set-Cookie` still marks a
    // response Cloudflare refuses to store.
    if !header_values(headers, "set-cookie").is_empty() && !strips_cookies {
        return None;
    }
    // Freshness, highest precedence first: `s-maxage`, `max-age`,
    // `Expires`, then the edge-TTL default. A present-but-invalid
    // delta-seconds shadows the weaker sources with zero, as Cloudflare's
    // own local implementation computes it.
    let max_age = last_directive_value(&directives, "s-maxage")
        .or_else(|| last_directive_value(&directives, "max-age"));
    let date_ms = header_value(headers, "date")
        .and_then(|value| parse_http_date_ms(&value))
        .unwrap_or(now_ms);
    let age_ms = header_value(headers, "age")
        .map(|value| delta_seconds_ms(parse_delta_seconds(&value)))
        .unwrap_or(0);
    let apparent_age_ms = now_ms.saturating_sub(date_ms).max(0);
    let current_age_ms = apparent_age_ms.max(age_ms);
    let lifetime_ms = if let Some(delta) = max_age {
        delta_seconds_ms(parse_delta_seconds(delta))
    } else if let Some(expires) = header_value(headers, "expires") {
        let expires_ms = parse_http_date_ms(&expires)?;
        expires_ms.saturating_sub(date_ms).max(0)
    } else {
        default_ttl_ms(status)?
    };
    let ttl_ms = lifetime_ms.saturating_sub(current_age_ms);
    if ttl_ms <= 0 {
        return None;
    }
    let mut stored: Vec<(String, String)> = headers
        .iter()
        .filter(|(name, _)| !(strips_cookies && name.eq_ignore_ascii_case("set-cookie")))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("cache-control"))
        .cloned()
        .collect();
    let kept: Vec<String> = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("cache-control"))
        .flat_map(|(_, value)| value.split(','))
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .filter(|part| {
            !(strips_cookies && {
                let name = part.split('=').next().unwrap_or("").trim();
                name.eq_ignore_ascii_case("private")
            })
        })
        .map(str::to_string)
        .collect();
    if !kept.is_empty() {
        stored.push(("cache-control".to_string(), kept.join(", ")));
    }
    Some((stored, now_ms.saturating_add(ttl_ms)))
}

/// Build the answer for a live entry: conditionals first, then ranges,
/// then the stored response itself. Every answer carries
/// `CF-Cache-Status: HIT` and never a `Cache-Tag`.
fn answer(entry: &Entry, request_headers: &[(String, String)]) -> MatchOutcome {
    let mut headers: Vec<(String, String)> = entry
        .headers
        .iter()
        .filter(|(name, _)| !name.eq_ignore_ascii_case("cache-tag"))
        .filter(|(name, _)| !name.eq_ignore_ascii_case("cf-cache-status"))
        .cloned()
        .collect();
    headers.push(("cf-cache-status".to_string(), "HIT".to_string()));

    if is_not_modified(&entry.headers, request_headers) {
        headers.retain(|(name, _)| !name.eq_ignore_ascii_case("content-length"));
        return MatchOutcome::Hit {
            status: 304,
            headers,
            body: None,
        };
    }
    if let Some(range) = header_value(request_headers, "range") {
        match parse_ranges(&range, entry.body.len() as u64) {
            RangeOutcome::Unsatisfiable => {
                return MatchOutcome::Hit {
                    status: 416,
                    headers: vec![
                        (
                            "content-range".to_string(),
                            format!("bytes */{}", entry.body.len()),
                        ),
                        ("cf-cache-status".to_string(), "HIT".to_string()),
                    ],
                    body: None,
                };
            }
            RangeOutcome::Full => {}
            RangeOutcome::Ranges(ranges) if ranges.len() == 1 => {
                let (start, end) = ranges[0];
                headers.retain(|(name, _)| {
                    !name.eq_ignore_ascii_case("content-length")
                        && !name.eq_ignore_ascii_case("content-range")
                });
                headers.push((
                    "content-range".to_string(),
                    format!("bytes {start}-{end}/{}", entry.body.len()),
                ));
                let body = entry.body[start as usize..=end as usize].to_vec();
                headers.push(("content-length".to_string(), body.len().to_string()));
                return MatchOutcome::Hit {
                    status: 206,
                    headers,
                    body: Some(body),
                };
            }
            RangeOutcome::Ranges(ranges) => {
                let boundary = multipart_boundary();
                let content_type = header_value(&entry.headers, "content-type")
                    .unwrap_or_else(|| "application/octet-stream".to_string());
                headers.retain(|(name, _)| {
                    !name.eq_ignore_ascii_case("content-length")
                        && !name.eq_ignore_ascii_case("content-range")
                        && !name.eq_ignore_ascii_case("content-type")
                });
                headers.push((
                    "content-type".to_string(),
                    format!("multipart/byteranges; boundary={boundary}"),
                ));
                let mut body = Vec::new();
                for (start, end) in &ranges {
                    body.extend_from_slice(format!(
                        "--{boundary}\r\ncontent-type: {content_type}\r\ncontent-range: bytes {start}-{end}/{}\r\n\r\n",
                        entry.body.len()
                    ).as_bytes());
                    body.extend_from_slice(&entry.body[*start as usize..=*end as usize]);
                    body.extend_from_slice(b"\r\n");
                }
                body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
                headers.push(("content-length".to_string(), body.len().to_string()));
                return MatchOutcome::Hit {
                    status: 206,
                    headers,
                    body: Some(body),
                };
            }
        }
    }
    if matches!(entry.status, 204 | 205 | 304) {
        headers.retain(|(name, _)| !name.eq_ignore_ascii_case("content-length"));
        return MatchOutcome::Hit {
            status: entry.status,
            headers,
            body: None,
        };
    }
    headers.retain(|(name, _)| !name.eq_ignore_ascii_case("content-length"));
    headers.push(("content-length".to_string(), entry.body.len().to_string()));
    MatchOutcome::Hit {
        status: entry.status,
        headers,
        body: Some(entry.body.clone()),
    }
}

/// Whether the request's preconditions match the stored validators: a
/// matching `If-None-Match` or a covering `If-Modified-Since` earns a 304.
fn is_not_modified(stored: &[(String, String)], request: &[(String, String)]) -> bool {
    if let (Some(if_none_match), Some(etag)) = (
        header_value(request, "if-none-match"),
        header_value(stored, "etag"),
    ) {
        if let Some(current) = parse_etag(&etag) {
            if if_none_match.trim() == "*" {
                return true;
            }
            if if_none_match
                .split(',')
                .any(|candidate| parse_etag(candidate) == Some(current))
            {
                return true;
            }
        }
    }
    if let (Some(since), Some(last_modified)) = (
        header_value(request, "if-modified-since"),
        header_value(stored, "last-modified"),
    ) {
        if let (Some(since_ms), Some(modified_ms)) =
            (parse_strict_imf(&since), parse_strict_imf(&last_modified))
        {
            if modified_ms <= since_ms {
                return true;
            }
        }
    }
    false
}

/// Parse an entity-tag for weak comparison: an optional `W/` prefix and a
/// non-empty quoted string. Returns the opaque tag without its quotes.
fn parse_etag(value: &str) -> Option<&str> {
    let tag = value.trim();
    let tag = tag.strip_prefix("W/").unwrap_or(tag);
    if tag.len() >= 2 && tag.starts_with('"') && tag.ends_with('"') {
        let inner = &tag[1..tag.len() - 1];
        if !inner.is_empty() {
            return Some(inner);
        }
    }
    None
}

/// Parse an IMF-fixdate, strictly: Cloudflare's own local implementation
/// only honors the exact `Sun, 06 Nov 1994 08:49:37 GMT` shape for
/// `If-Modified-Since` handling, and anything else skips the 304.
fn parse_strict_imf(value: &str) -> Option<i64> {
    const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let bytes = value.as_bytes();
    if bytes.len() != 29
        || !WEEKDAYS.contains(&value.get(0..3).unwrap_or(""))
        || bytes.get(3..5) != Some(b", ".as_slice())
        || !bytes
            .get(5..7)
            .is_some_and(|day| day.iter().all(|b| b.is_ascii_digit()))
        || bytes.get(7) != Some(&b' ')
        || !MONTHS.contains(&value.get(8..11).unwrap_or(""))
        || bytes.get(11) != Some(&b' ')
        || !bytes
            .get(12..16)
            .is_some_and(|year| year.iter().all(|b| b.is_ascii_digit()))
        || bytes.get(16) != Some(&b' ')
        || !bytes
            .get(17..19)
            .is_some_and(|h| h.iter().all(|b| b.is_ascii_digit()))
        || bytes.get(19) != Some(&b':')
        || !bytes
            .get(20..22)
            .is_some_and(|m| m.iter().all(|b| b.is_ascii_digit()))
        || bytes.get(22) != Some(&b':')
        || !bytes
            .get(23..25)
            .is_some_and(|s| s.iter().all(|b| b.is_ascii_digit()))
        || bytes.get(25..) != Some(b" GMT".as_slice())
    {
        return None;
    }
    parse_http_date_ms(value)
}

enum RangeOutcome {
    Full,
    Ranges(Vec<(u64, u64)>),
    Unsatisfiable,
}

fn parse_decimal(value: &str) -> Option<u64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(value.parse::<u64>().unwrap_or(u64::MAX))
}

/// Parse a `Range` header against a body length, mirroring Cloudflare's own
/// local implementation quirk for quirk: only the `bytes` unit, an empty
/// range set means the full body, a suffix that covers the body means the
/// full body, an empty suffix is skipped, and anything malformed or
/// past the end is unsatisfiable.
fn parse_ranges(header: &str, len: u64) -> RangeOutcome {
    let after_unit = header.trim_start();
    let after_unit = match after_unit.get(0..5) {
        Some(unit) if unit.eq_ignore_ascii_case("bytes") => &after_unit[5..],
        _ => return RangeOutcome::Unsatisfiable,
    };
    let Some(ranges) = after_unit.trim_start().strip_prefix('=') else {
        return RangeOutcome::Unsatisfiable;
    };
    if ranges.trim_start().is_empty() {
        return RangeOutcome::Full;
    }
    let mut result = Vec::new();
    for range in ranges.split(',') {
        let Some((start, end)) = range.split_once('-') else {
            return RangeOutcome::Unsatisfiable;
        };
        let (start, end) = (start.trim(), end.trim());
        if end.contains('-') {
            return RangeOutcome::Unsatisfiable;
        }
        match (parse_decimal(start), parse_decimal(end)) {
            (Some(first), Some(last)) => {
                if first > last || first >= len {
                    return RangeOutcome::Unsatisfiable;
                }
                result.push((first, last.min(len.saturating_sub(1))));
            }
            (Some(first), None) if end.is_empty() => {
                if first >= len {
                    return RangeOutcome::Unsatisfiable;
                }
                result.push((first, len.saturating_sub(1)));
            }
            (None, Some(suffix)) if start.is_empty() => {
                if suffix >= len {
                    return RangeOutcome::Full;
                }
                if suffix == 0 {
                    continue;
                }
                result.push((len - suffix, len - 1));
            }
            _ => return RangeOutcome::Unsatisfiable,
        }
    }
    if result.is_empty() {
        RangeOutcome::Full
    } else {
        RangeOutcome::Ranges(result)
    }
}

/// The node's cache: the script-scoped store plus one store per fleet
/// deployment — the fleet's bucket identity plus the serving deployment's
/// identity. Script scope keeps today's single node budget with per-script
/// entry keys, byte for byte; each deployment scope carries its own
/// `CELLD_CACHE_MAX_BYTES` budget and recency, so one deployment's writes
/// never evict another's and a drained deployment's entries leave with it
/// (see `purge_deployment_scope`).
struct Store {
    cap_bytes: u64,
    script: WorkerCache,
    deployments: HashMap<String, WorkerCache>,
}

/// Which scope's entries one op addresses. `script` and `deployment`
/// both come from isolate state, never from script input. `deployment`
/// is the fleet deployment key — the fleet's bucket identity plus the
/// serving deployment's identity — present only for tenant deployment
/// scripts: the runtime pool, a loaded worker, and a test isolate carry
/// none and stay script-scoped in every mode.
#[derive(Clone, Copy)]
struct OpScope<'a> {
    scope: CacheScope,
    script: &'a str,
    deployment: Option<&'a str>,
}

impl Store {
    fn new(cap_bytes: u64) -> Self {
        Self {
            cap_bytes,
            script: WorkerCache::new(cap_bytes),
            deployments: HashMap::new(),
        }
    }

    /// Route one op to its scope's store and entry scope.
    fn select<'a, 'b>(&'b mut self, op: OpScope<'a>) -> (&'a str, &'b mut WorkerCache) {
        match (op.scope, op.deployment) {
            (CacheScope::Deployment, Some(key)) => {
                let cap_bytes = self.cap_bytes;
                let store = self
                    .deployments
                    .entry(key.to_owned())
                    .or_insert_with(|| WorkerCache::new(cap_bytes));
                (key, store)
            }
            _ => (op.script, &mut self.script),
        }
    }

    fn put(
        &mut self,
        op: OpScope<'_>,
        namespace: &Namespace,
        request: &PutRequest,
        now_ms: i64,
    ) -> Result<PutOutcome, PutRejection> {
        let (entry_scope, store) = self.select(op);
        store.put(entry_scope, namespace, request, now_ms)
    }

    fn lookup(
        &mut self,
        op: OpScope<'_>,
        namespace: &Namespace,
        request: &LookupRequest,
        now_ms: i64,
    ) -> MatchOutcome {
        let (entry_scope, store) = self.select(op);
        store.lookup(entry_scope, namespace, request, now_ms)
    }

    fn delete(
        &mut self,
        op: OpScope<'_>,
        namespace: &Namespace,
        url: &str,
        method: &str,
        ignore_method: bool,
        now_ms: i64,
    ) -> bool {
        let (entry_scope, store) = self.select(op);
        store.delete(entry_scope, namespace, url, method, ignore_method, now_ms)
    }

    /// Drop every entry in every scope. The node calls this when memory
    /// pressure starts shedding load: best-effort cache bytes go before
    /// billable cells.
    fn shed(&mut self) {
        self.script.shed();
        for store in self.deployments.values_mut() {
            store.shed();
        }
    }

    /// Drop one deployment's entries. The node calls this when the
    /// deployment's last live generation is freed, so a redeployed fleet
    /// never pays a dead deployment's budget.
    fn purge_deployment(&mut self, key: &str) {
        self.deployments.remove(key);
    }
}

/// The node's cache: one store for the process, first sized from the
/// environment. The mutex only guards short map operations — no I/O, no
/// awaits — so an op never blocks an isolate on anything but a memcpy.
static STORE: OnceLock<Mutex<Store>> = OnceLock::new();

fn store() -> &'static Mutex<Store> {
    STORE.get_or_init(|| Mutex::new(Store::new(max_bytes())))
}

fn lock() -> std::sync::MutexGuard<'static, Store> {
    store().lock().unwrap_or_else(|poison| poison.into_inner())
}

/// Store one response. `script` and `deployment` must come from isolate
/// state, never from script input: together they are the tenant boundary.
pub fn cache_put(
    script: &str,
    deployment: Option<&str>,
    namespace: &Namespace,
    request: &PutRequest,
    now_ms: i64,
) -> Result<PutOutcome, PutRejection> {
    lock().put(
        OpScope {
            scope: cache_scope(),
            script,
            deployment,
        },
        namespace,
        request,
        now_ms,
    )
}

/// Look up one request. `script` and `deployment` must come from isolate
/// state, never from script input: together they are the tenant boundary.
pub fn cache_lookup(
    script: &str,
    deployment: Option<&str>,
    namespace: &Namespace,
    request: &LookupRequest,
    now_ms: i64,
) -> MatchOutcome {
    lock().lookup(
        OpScope {
            scope: cache_scope(),
            script,
            deployment,
        },
        namespace,
        request,
        now_ms,
    )
}

/// Drop the node's cache when memory pressure starts shedding load:
/// best-effort bytes go before billable cells. Puts keep working while
/// shedding; only entering pressure clears.
pub fn shed_cache() {
    lock().shed();
}

/// Drop one deployment's entries once its last live generation is freed.
/// `key` is the deployment identity the host stamped at build, compared
/// by value: an unknown key is a no-op, never an error.
pub fn purge_deployment_scope(key: &str) {
    lock().purge_deployment(key);
}

/// Delete one entry. `script` and `deployment` must come from isolate
/// state, never from script input: together they are the tenant boundary.
pub fn cache_delete(
    script: &str,
    deployment: Option<&str>,
    namespace: &Namespace,
    url: &str,
    method: &str,
    ignore_method: bool,
    now_ms: i64,
) -> bool {
    lock().delete(
        OpScope {
            scope: cache_scope(),
            script,
            deployment,
        },
        namespace,
        url,
        method,
        ignore_method,
        now_ms,
    )
}

fn multipart_boundary() -> String {
    use rand::RngCore as _;
    let mut bytes = [0_u8; 16];
    crate::asyncrt::rng("cache-multipart").fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
impl WorkerCache {
    fn used_bytes(&self) -> u64 {
        self.used_bytes
    }

    fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The lab clock: every test injects its own `now`, so none sleeps.
    const NOW: i64 = 1_792_000_000_000;
    const SCRIPT: &str = "cache-app";
    const URL: &str = "https://cache.test/key";

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect()
    }

    fn store() -> WorkerCache {
        WorkerCache::new(DEFAULT_MAX_BYTES)
    }

    fn put(
        cache: &mut WorkerCache,
        url: &str,
        status: u16,
        pairs: &[(&str, &str)],
        body: &[u8],
        now_ms: i64,
    ) -> Result<PutOutcome, PutRejection> {
        let held = headers(pairs);
        cache.put(
            SCRIPT,
            &Namespace::Default,
            &PutRequest {
                method: "GET",
                url,
                status,
                headers: &held,
                body,
            },
            now_ms,
        )
    }

    fn lookup(
        cache: &mut WorkerCache,
        url: &str,
        pairs: &[(&str, &str)],
        now_ms: i64,
    ) -> MatchOutcome {
        let held = headers(pairs);
        let request = LookupRequest {
            url,
            method: "GET",
            ignore_method: false,
            request_headers: &held,
        };
        cache.lookup(SCRIPT, &Namespace::Default, &request, now_ms)
    }

    fn lookup_as(
        cache: &mut WorkerCache,
        script: &str,
        namespace: &Namespace,
        method: &str,
        ignore_method: bool,
        now_ms: i64,
    ) -> MatchOutcome {
        let held: Vec<(String, String)> = Vec::new();
        let request = LookupRequest {
            url: URL,
            method,
            ignore_method,
            request_headers: &held,
        };
        cache.lookup(script, namespace, &request, now_ms)
    }

    fn hit_body(outcome: &MatchOutcome) -> &[u8] {
        let MatchOutcome::Hit {
            body: Some(body), ..
        } = outcome
        else {
            panic!("expected a hit with a body, got {outcome:?}");
        };
        body
    }

    fn hit_headers(outcome: &MatchOutcome) -> &[(String, String)] {
        let MatchOutcome::Hit { headers, .. } = outcome else {
            panic!("expected a hit, got {outcome:?}");
        };
        headers
    }

    fn header(outcome: &MatchOutcome, name: &str) -> Option<String> {
        header_value(hit_headers(outcome), name)
    }

    fn status(outcome: &MatchOutcome) -> u16 {
        let MatchOutcome::Hit { status, .. } = outcome else {
            panic!("expected a hit, got {outcome:?}");
        };
        *status
    }

    #[test]
    fn namespace_words_parse() {
        assert_eq!(Namespace::parse("default"), Some(Namespace::Default));
        assert_eq!(
            Namespace::parse("named:a"),
            Some(Namespace::Named("a".to_string()))
        );
        assert_eq!(
            Namespace::parse("named:default"),
            Some(Namespace::Named("default".to_string()))
        );
        assert_eq!(
            Namespace::parse("named:"),
            Some(Namespace::Named(String::new()))
        );
        assert_eq!(Namespace::parse("a"), None);
        assert_eq!(Namespace::parse(""), None);
    }

    #[test]
    fn put_rejects_non_get_range_and_vary_star() {
        let mut cache = store();
        let held = headers(&[("cache-control", "max-age=60")]);
        let non_get = cache.put(
            SCRIPT,
            &Namespace::Default,
            &PutRequest {
                method: "POST",
                url: URL,
                status: 200,
                headers: &held,
                body: b"x",
            },
            NOW,
        );
        assert_eq!(non_get, Err(PutRejection::NonGet));
        assert_eq!(
            PutRejection::NonGet.message(),
            "Cannot cache response to non-GET request."
        );
        let partial = cache.put(
            SCRIPT,
            &Namespace::Default,
            &PutRequest {
                method: "GET",
                url: URL,
                status: 206,
                headers: &held,
                body: b"x",
            },
            NOW,
        );
        assert_eq!(partial, Err(PutRejection::PartialContent));
        let vary = headers(&[("vary", "*"), ("cache-control", "max-age=60")]);
        let star = cache.put(
            SCRIPT,
            &Namespace::Default,
            &PutRequest {
                method: "GET",
                url: URL,
                status: 200,
                headers: &vary,
                body: b"x",
            },
            NOW,
        );
        assert_eq!(star, Err(PutRejection::VaryStar));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn round_trip_keeps_status_headers_body() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60"), ("x-a", "b")],
                b"hello",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let found = lookup(&mut cache, URL, &[], NOW);
        assert_eq!(status(&found), 200);
        assert_eq!(hit_body(&found), b"hello");
        assert_eq!(header(&found, "x-a").as_deref(), Some("b"));
        assert_eq!(header(&found, "cf-cache-status").as_deref(), Some("HIT"));
    }

    #[test]
    fn unknown_key_misses() {
        let mut cache = store();
        assert_eq!(lookup(&mut cache, URL, &[], NOW), MatchOutcome::Miss);
    }

    #[test]
    fn urls_match_exactly() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                "https://cache.test/a?x=1",
                200,
                &[("cache-control", "max-age=60")],
                b"q",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert!(matches!(
            lookup(&mut cache, "https://cache.test/a?x=1", &[], NOW),
            MatchOutcome::Hit { .. }
        ));
        assert_eq!(
            lookup(&mut cache, "https://cache.test/a?x=2", &[], NOW),
            MatchOutcome::Miss
        );
        assert_eq!(
            lookup(&mut cache, "https://cache.test/a", &[], NOW),
            MatchOutcome::Miss
        );
        assert_eq!(
            lookup(&mut cache, "https://cache.test/A?x=1", &[], NOW),
            MatchOutcome::Miss
        );
    }

    #[test]
    fn scripts_and_namespaces_are_isolated() {
        let mut cache = store();
        let held = headers(&[("cache-control", "max-age=60")]);
        for (script, namespace, body) in [
            (SCRIPT, Namespace::Default, b"app".as_slice()),
            ("other", Namespace::Default, b"probe".as_slice()),
            (
                SCRIPT,
                Namespace::Named("a".to_string()),
                b"named".as_slice(),
            ),
        ] {
            cache
                .put(
                    script,
                    &namespace,
                    &PutRequest {
                        method: "GET",
                        url: URL,
                        status: 200,
                        headers: &held,
                        body,
                    },
                    NOW,
                )
                .unwrap();
        }
        let app = lookup_as(&mut cache, SCRIPT, &Namespace::Default, "GET", false, NOW);
        assert_eq!(hit_body(&app), b"app");
        let probe = lookup_as(&mut cache, "other", &Namespace::Default, "GET", false, NOW);
        assert_eq!(hit_body(&probe), b"probe");
        let named = lookup_as(
            &mut cache,
            SCRIPT,
            &Namespace::Named("a".to_string()),
            "GET",
            false,
            NOW,
        );
        assert_eq!(hit_body(&named), b"named");
        let other_ns = lookup_as(
            &mut cache,
            SCRIPT,
            &Namespace::Named("b".to_string()),
            "GET",
            false,
            NOW,
        );
        assert_eq!(other_ns, MatchOutcome::Miss);
        // `open("default")` is not `caches.default`.
        let named_default = lookup_as(
            &mut cache,
            SCRIPT,
            &Namespace::Named("default".to_string()),
            "GET",
            false,
            NOW,
        );
        assert_eq!(named_default, MatchOutcome::Miss);
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn uncacheable_directives_do_not_store() {
        for cc in [
            "no-store",
            "no-store, max-age=60",
            "private",
            "private, max-age=60",
            "private=foo",
            "private=set-cookie, private=foo",
            "no-cache",
            "no-cache=set-cookie",
            "max-age=0",
            "s-maxage=0, max-age=60",
            "max-age=abc",
            "max-age=",
        ] {
            let mut cache = store();
            assert_eq!(
                put(&mut cache, URL, 200, &[("cache-control", cc)], b"x", NOW),
                Ok(PutOutcome::NotStored),
                "Cache-Control: {cc}"
            );
            assert_eq!(lookup(&mut cache, URL, &[], NOW), MatchOutcome::Miss);
        }
    }

    #[test]
    fn set_cookie_is_never_cached() {
        for extra in [
            vec![("set-cookie", "a=b")],
            vec![("set-cookie", "a=b"), ("set-cookie", "c=d")],
            vec![("set-cookie", "a=b"), ("cache-control", "max-age=60")],
            vec![("set-cookie", ""), ("cache-control", "max-age=60")],
        ] {
            let mut cache = store();
            assert_eq!(
                put(&mut cache, URL, 200, &extra, b"x", NOW),
                Ok(PutOutcome::NotStored),
                "{extra:?}"
            );
        }
    }

    #[test]
    fn private_set_cookie_strips_and_stores() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[
                    ("cache-control", "private=set-cookie, max-age=60"),
                    ("set-cookie", "a=b"),
                    ("set-cookie", "c=d"),
                ],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let found = lookup(&mut cache, URL, &[], NOW);
        assert!(header(&found, "set-cookie").is_none());
        assert_eq!(
            header(&found, "cache-control").as_deref(),
            Some("max-age=60")
        );
        // The quoted and cased spellings strip too.
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[
                    ("cache-control", "Private=\"Set-Cookie\", max-age=60"),
                    ("set-cookie", "a=b"),
                ],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let found = lookup(&mut cache, URL, &[], NOW);
        assert!(header(&found, "set-cookie").is_none());
    }

    #[test]
    fn private_set_cookie_without_cookies_still_stores() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "private=set-cookie, max-age=60")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert!(matches!(
            lookup(&mut cache, URL, &[], NOW),
            MatchOutcome::Hit { .. }
        ));
    }

    #[test]
    fn freshness_precedence_is_s_maxage_max_age_expires() {
        // `s-maxage` shadows `max-age`: dead in ten seconds, not a hundred.
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "s-maxage=10, max-age=100")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert!(matches!(
            lookup(&mut cache, URL, &[], NOW + 9_999),
            MatchOutcome::Hit { .. }
        ));
        assert_eq!(
            lookup(&mut cache, URL, &[], NOW + 10_000),
            MatchOutcome::Miss
        );
        // `max-age` shadows `Expires`.
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[
                    ("cache-control", "max-age=10"),
                    ("expires", "Wed, 01 Jan 2037 00:00:00 GMT")
                ],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(
            lookup(&mut cache, URL, &[], NOW + 10_000),
            MatchOutcome::Miss
        );
        // `Expires` alone governs.
        let mut cache = store();
        let expires = httpdate::fmt_http_date(
            std::time::UNIX_EPOCH + std::time::Duration::from_millis((NOW + 10_000) as u64),
        );
        assert_eq!(
            put(&mut cache, URL, 200, &[("expires", &expires)], b"x", NOW),
            Ok(PutOutcome::Stored)
        );
        assert!(matches!(
            lookup(&mut cache, URL, &[], NOW + 9_999),
            MatchOutcome::Hit { .. }
        ));
        assert_eq!(
            lookup(&mut cache, URL, &[], NOW + 10_000),
            MatchOutcome::Miss
        );
    }

    #[test]
    fn bad_expires_never_stores() {
        for expires in ["not a date", "Wed, 01 Jan 2020 00:00:00 GMT", ""] {
            let mut cache = store();
            assert_eq!(
                put(&mut cache, URL, 200, &[("expires", expires)], b"x", NOW),
                Ok(PutOutcome::NotStored),
                "Expires: {expires:?}"
            );
        }
        // But explicit `max-age` still wins over a bad `Expires`.
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60"), ("expires", "not a date")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
    }

    #[test]
    fn edge_defaults_cache_some_statuses_and_no_others() {
        for (status, ttl_ms) in [
            (200, 120 * 60 * 1000),
            (301, 120 * 60 * 1000),
            (302, 20 * 60 * 1000),
            (303, 20 * 60 * 1000),
            (404, 3 * 60 * 1000),
            (410, 3 * 60 * 1000),
        ] {
            let mut cache = store();
            assert_eq!(
                put(&mut cache, URL, status, &[], b"x", NOW),
                Ok(PutOutcome::Stored),
                "status {status}"
            );
            assert!(matches!(
                lookup(&mut cache, URL, &[], NOW + ttl_ms - 1),
                MatchOutcome::Hit { .. }
            ));
            assert_eq!(
                lookup(&mut cache, URL, &[], NOW + ttl_ms),
                MatchOutcome::Miss
            );
        }
        for status in [201, 204, 400, 500, 502, 503] {
            let mut cache = store();
            assert_eq!(
                put(&mut cache, URL, status, &[], b"x", NOW),
                Ok(PutOutcome::NotStored),
                "status {status}"
            );
        }
        // Explicit freshness caches any status.
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                500,
                &[("cache-control", "max-age=60")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
    }

    #[test]
    fn age_and_date_discount_freshness() {
        // An `Age` past the lifetime is already stale.
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=100"), ("age", "200")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::NotStored)
        );
        // A smaller `Age` shortens the TTL.
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=100"), ("age", "50")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert!(matches!(
            lookup(&mut cache, URL, &[], NOW + 49_999),
            MatchOutcome::Hit { .. }
        ));
        assert_eq!(
            lookup(&mut cache, URL, &[], NOW + 50_000),
            MatchOutcome::Miss
        );
        // A `Date` a minute old is a minute of age against `max-age`.
        let mut cache = store();
        let date = httpdate::fmt_http_date(
            std::time::UNIX_EPOCH + std::time::Duration::from_millis((NOW - 60_000) as u64),
        );
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=30"), ("date", &date)],
                b"x",
                NOW
            ),
            Ok(PutOutcome::NotStored)
        );
    }

    #[test]
    fn huge_max_age_saturates_instead_of_zeroing() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=99999999999999999999")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert!(matches!(
            lookup(&mut cache, URL, &[], NOW + 1_000_000_000),
            MatchOutcome::Hit { .. }
        ));
    }

    #[test]
    fn expiry_removes_the_entry() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=10")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(cache.len(), 1);
        assert_eq!(
            lookup(&mut cache, URL, &[], NOW + 10_000),
            MatchOutcome::Miss
        );
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.used_bytes(), 0);
    }

    #[test]
    fn if_none_match_answers_304() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60"), ("etag", "\"v1\"")],
                b"data",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        for inm in ["\"v1\"", "W/\"v1\"", "*", "\"other\", \"v1\"", " \"v1\" "] {
            let found = lookup(&mut cache, URL, &[("if-none-match", inm)], NOW);
            assert_eq!(status(&found), 304, "If-None-Match: {inm}");
            assert!(matches!(found, MatchOutcome::Hit { body: None, .. }));
            assert_eq!(header(&found, "etag").as_deref(), Some("\"v1\""));
            assert_eq!(header(&found, "cf-cache-status").as_deref(), Some("HIT"));
            assert!(header(&found, "content-length").is_none());
        }
        for inm in ["\"other\"", "v1", ""] {
            let found = lookup(&mut cache, URL, &[("if-none-match", inm)], NOW);
            assert_eq!(status(&found), 200, "If-None-Match: {inm:?}");
            assert_eq!(hit_body(&found), b"data");
        }
    }

    #[test]
    fn weak_etag_matches_strong_candidate() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60"), ("etag", "W/\"v1\"")],
                b"data",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let found = lookup(&mut cache, URL, &[("if-none-match", "\"v1\"")], NOW);
        assert_eq!(status(&found), 304);
    }

    #[test]
    fn if_modified_since_answers_304() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[
                    ("cache-control", "max-age=60"),
                    ("last-modified", "Wed, 01 Jan 2020 00:00:00 GMT")
                ],
                b"data",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let after = lookup(
            &mut cache,
            URL,
            &[("if-modified-since", "Thu, 02 Jan 2020 00:00:00 GMT")],
            NOW,
        );
        assert_eq!(status(&after), 304);
        assert!(matches!(after, MatchOutcome::Hit { body: None, .. }));
        let same = lookup(
            &mut cache,
            URL,
            &[("if-modified-since", "Wed, 01 Jan 2020 00:00:00 GMT")],
            NOW,
        );
        assert_eq!(status(&same), 304);
        let before = lookup(
            &mut cache,
            URL,
            &[("if-modified-since", "Tue, 31 Dec 2019 00:00:00 GMT")],
            NOW,
        );
        assert_eq!(status(&before), 200);
        // Anything but the strict IMF shape skips the 304.
        for ims in [
            "not a date",
            "01 Jan 2020 00:00:00 GMT",
            "Wed, 1 Jan 2020 00:00:00 GMT",
        ] {
            let found = lookup(&mut cache, URL, &[("if-modified-since", ims)], NOW);
            assert_eq!(status(&found), 200, "If-Modified-Since: {ims:?}");
        }
    }

    #[test]
    fn ranges_slice_and_416() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[
                    ("cache-control", "max-age=60"),
                    ("content-type", "text/plain")
                ],
                b"0123456789",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let part = lookup(&mut cache, URL, &[("range", "bytes=0-3")], NOW);
        assert_eq!(status(&part), 206);
        assert_eq!(hit_body(&part), b"0123");
        assert_eq!(
            header(&part, "content-range").as_deref(),
            Some("bytes 0-3/10")
        );
        assert_eq!(header(&part, "content-length").as_deref(), Some("4"));
        assert_eq!(header(&part, "cf-cache-status").as_deref(), Some("HIT"));
        let suffix = lookup(&mut cache, URL, &[("range", "bytes=-3")], NOW);
        assert_eq!(hit_body(&suffix), b"789");
        let open = lookup(&mut cache, URL, &[("range", "bytes=8-")], NOW);
        assert_eq!(hit_body(&open), b"89");
        let clamped = lookup(&mut cache, URL, &[("range", "bytes=8-99")], NOW);
        assert_eq!(hit_body(&clamped), b"89");
        assert_eq!(
            header(&clamped, "content-range").as_deref(),
            Some("bytes 8-9/10")
        );
        // A suffix covering the body, an empty set, and a skipped empty
        // suffix all read the full body.
        for range in ["bytes=-10", "bytes=-99", "bytes=", "bytes=-0"] {
            let full = lookup(&mut cache, URL, &[("range", range)], NOW);
            assert_eq!(status(&full), 200, "Range: {range}");
            assert_eq!(hit_body(&full), b"0123456789");
        }
        // Malformed or past-the-end ranges are 416 with no stored headers
        // beyond the range report and the HIT marker.
        for range in [
            "bytes=20-30",
            "bytes=10-",
            "bytes=5-4",
            "bytes=1-2-3",
            "bytes",
            "items=0-1",
            "bytes=a-b",
        ] {
            let missing = lookup(&mut cache, URL, &[("range", range)], NOW);
            assert_eq!(status(&missing), 416, "Range: {range}");
            assert!(matches!(missing, MatchOutcome::Hit { body: None, .. }));
            assert_eq!(
                header(&missing, "content-range").as_deref(),
                Some("bytes */10")
            );
            assert_eq!(header(&missing, "cf-cache-status").as_deref(), Some("HIT"));
            assert_eq!(hit_headers(&missing).len(), 2);
        }
    }

    #[test]
    fn multi_range_answers_multipart() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[
                    ("cache-control", "max-age=60"),
                    ("content-type", "text/plain")
                ],
                b"0123456789",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let found = lookup(&mut cache, URL, &[("range", "bytes=0-1,4-5")], NOW);
        assert_eq!(status(&found), 206);
        let content_type = header(&found, "content-type").expect("multipart content type");
        let boundary = content_type
            .strip_prefix("multipart/byteranges; boundary=")
            .expect("multipart boundary");
        assert_eq!(boundary.len(), 32);
        assert!(boundary.bytes().all(|byte| byte.is_ascii_hexdigit()));
        let body = std::str::from_utf8(hit_body(&found)).expect("textual multipart body");
        assert!(body.contains(&format!("--{boundary}\r\n")), "{body:?}");
        assert!(body.contains("content-range: bytes 0-1/10"), "{body:?}");
        assert!(body.contains("content-range: bytes 4-5/10"), "{body:?}");
        assert!(body.contains("content-type: text/plain"), "{body:?}");
        assert!(body.ends_with(&format!("--{boundary}--\r\n")), "{body:?}");
        // The slices ride inside the parts.
        assert!(body.contains("\r\n\r\n01\r\n"), "{body:?}");
        assert!(body.contains("\r\n\r\n45\r\n"), "{body:?}");
    }

    #[test]
    fn match_strips_cache_tag_and_overwrites_hit_marker() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[
                    ("cache-control", "max-age=60"),
                    ("cache-tag", "t1"),
                    ("cf-cache-status", "MISS")
                ],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let found = lookup(&mut cache, URL, &[], NOW);
        assert!(header(&found, "cache-tag").is_none());
        assert_eq!(header(&found, "cf-cache-status").as_deref(), Some("HIT"));
    }

    #[test]
    fn match_corrects_content_length() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60"), ("content-length", "999")],
                b"0123456789",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let found = lookup(&mut cache, URL, &[], NOW);
        assert_eq!(header(&found, "content-length").as_deref(), Some("10"));
    }

    #[test]
    fn null_body_statuses_answer_without_a_body() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                204,
                &[("cache-control", "max-age=60")],
                b"",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let found = lookup(&mut cache, URL, &[], NOW);
        assert_eq!(status(&found), 204);
        assert!(matches!(found, MatchOutcome::Hit { body: None, .. }));
        assert!(header(&found, "content-length").is_none());
    }

    #[test]
    fn vary_is_ignored() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60"), ("vary", "Accept-Encoding")],
                b"VA",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        for accept in ["gzip", "br", "identity"] {
            let found = lookup(&mut cache, URL, &[("accept-encoding", accept)], NOW);
            assert_eq!(hit_body(&found), b"VA");
        }
        assert_eq!(hit_body(&lookup(&mut cache, URL, &[], NOW)), b"VA");
    }

    #[test]
    fn non_get_lookup_misses_without_ignore_method() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let miss = lookup_as(&mut cache, SCRIPT, &Namespace::Default, "POST", false, NOW);
        assert_eq!(miss, MatchOutcome::Miss);
        let hit = lookup_as(&mut cache, SCRIPT, &Namespace::Default, "POST", true, NOW);
        assert!(matches!(hit, MatchOutcome::Hit { .. }));
        assert!(!cache.delete(SCRIPT, &Namespace::Default, URL, "POST", false, NOW));
        assert!(cache.delete(SCRIPT, &Namespace::Default, URL, "POST", true, NOW));
    }

    #[test]
    fn delete_reports_and_purges() {
        let mut cache = store();
        assert!(!cache.delete(SCRIPT, &Namespace::Default, URL, "GET", false, NOW));
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert!(cache.delete(SCRIPT, &Namespace::Default, URL, "GET", false, NOW));
        assert!(!cache.delete(SCRIPT, &Namespace::Default, URL, "GET", false, NOW));
        assert_eq!(lookup(&mut cache, URL, &[], NOW), MatchOutcome::Miss);
        assert_eq!(cache.used_bytes(), 0);
        // Deleting an expired entry reports false: it was already gone.
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=10")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert!(!cache.delete(SCRIPT, &Namespace::Default, URL, "GET", false, NOW + 10_000));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn lru_evicts_oldest_first() {
        // Two ~1.1 KiB entries fit; the third put evicts exactly the oldest.
        let mut cache = WorkerCache::new(2400);
        let one = "https://cache.test/1";
        let two = "https://cache.test/2";
        let three = "https://cache.test/3";
        let body = vec![b'v'; 300];
        for url in [one, two] {
            assert_eq!(
                put(
                    &mut cache,
                    url,
                    200,
                    &[("cache-control", "max-age=60")],
                    &body,
                    NOW
                ),
                Ok(PutOutcome::Stored)
            );
        }
        assert!(cache.used_bytes() <= 2400);
        assert_eq!(
            put(
                &mut cache,
                three,
                200,
                &[("cache-control", "max-age=60")],
                &body,
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(lookup(&mut cache, one, &[], NOW), MatchOutcome::Miss);
        assert!(matches!(
            lookup(&mut cache, two, &[], NOW),
            MatchOutcome::Hit { .. }
        ));
        assert!(matches!(
            lookup(&mut cache, three, &[], NOW),
            MatchOutcome::Hit { .. }
        ));
        assert!(cache.used_bytes() <= 2400);
    }

    #[test]
    fn lookup_refreshes_recency() {
        let mut cache = WorkerCache::new(2400);
        let body = vec![b'v'; 300];
        for url in ["https://cache.test/1", "https://cache.test/2"] {
            assert_eq!(
                put(
                    &mut cache,
                    url,
                    200,
                    &[("cache-control", "max-age=60")],
                    &body,
                    NOW
                ),
                Ok(PutOutcome::Stored)
            );
        }
        assert!(matches!(
            lookup(&mut cache, "https://cache.test/1", &[], NOW),
            MatchOutcome::Hit { .. }
        ));
        assert_eq!(
            put(
                &mut cache,
                "https://cache.test/3",
                200,
                &[("cache-control", "max-age=60")],
                &body,
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert!(matches!(
            lookup(&mut cache, "https://cache.test/1", &[], NOW),
            MatchOutcome::Hit { .. }
        ));
        assert_eq!(
            lookup(&mut cache, "https://cache.test/2", &[], NOW),
            MatchOutcome::Miss
        );
    }

    #[test]
    fn replace_frees_before_accounting() {
        let mut cache = WorkerCache::new(1200);
        let body = vec![b'v'; 300];
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60")],
                &body,
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let used = cache.used_bytes();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60")],
                b"small",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert!(cache.used_bytes() < used);
        assert_eq!(cache.len(), 1);
        assert_eq!(hit_body(&lookup(&mut cache, URL, &[], NOW)), b"small");
    }

    #[test]
    fn oversized_entries_do_not_store() {
        let mut cache = store();
        let huge = vec![b'v'; 17 * 1024 * 1024];
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60")],
                &huge,
                NOW
            ),
            Ok(PutOutcome::NotStored)
        );
        assert_eq!(cache.len(), 0);
        // An entry that fits the per-entry cap but not the store cap
        // does not store either.
        let mut small = WorkerCache::new(1000);
        assert_eq!(
            put(
                &mut small,
                URL,
                200,
                &[("cache-control", "max-age=60")],
                &vec![b'v'; 900],
                NOW
            ),
            Ok(PutOutcome::NotStored)
        );
        // A zero cap disables the cache.
        let mut off = WorkerCache::new(0);
        assert_eq!(
            put(
                &mut off,
                URL,
                200,
                &[("cache-control", "max-age=60")],
                b"x",
                NOW
            ),
            Ok(PutOutcome::NotStored)
        );
    }

    /// A conservative floor for what one entry really occupies: the key
    /// heap bytes twice over (the map holds one `Key`, the recency index
    /// its clone), the header bytes plus the inline pair slots, the body,
    /// and the inline structs via `size_of` — not the charged constants,
    /// so the model stays independent of the formula it checks. Map-node
    /// shares, `Vec` growth slack, and allocator rounding are all positive
    /// and all omitted, so real memory is always at or above this floor.
    fn honest_floor(
        scope: &str,
        namespace: &Namespace,
        url: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> u64 {
        let namespace = match namespace {
            Namespace::Default => 0,
            Namespace::Named(name) => name.len() as u64,
        };
        let key_heap = scope.len() as u64 + namespace + url.len() as u64;
        let header_heap: u64 = headers
            .iter()
            .map(|(name, value)| name.len() as u64 + value.len() as u64)
            .sum();
        key_heap * 2
            + std::mem::size_of::<Key>() as u64 * 2
            + std::mem::size_of::<Entry>() as u64
            + header_heap
            + std::mem::size_of::<(String, String)>() as u64 * headers.len() as u64
            + body.len() as u64
    }

    #[test]
    fn tiny_entries_charge_real_memory() {
        let mut cache = WorkerCache::new(64 * 1024);
        let held = headers(&[("cache-control", "max-age=60")]);
        for i in 0..1000 {
            let url = format!("https://cache.test/tiny/{i:04}");
            assert_eq!(
                cache.put(
                    SCRIPT,
                    &Namespace::Default,
                    &PutRequest {
                        method: "GET",
                        url: &url,
                        status: 200,
                        headers: &held,
                        body: b"x",
                    },
                    NOW
                ),
                Ok(PutOutcome::Stored)
            );
        }
        assert!(cache.used_bytes() <= 64 * 1024);
        // Every resident entry is the same shape, so one floor covers
        // them all. Undercharging here lets tiny entries push real memory
        // far past the cap while `used_bytes` reports compliance.
        let floor = honest_floor(
            SCRIPT,
            &Namespace::Default,
            "https://cache.test/tiny/0000",
            &held,
            b"x",
        );
        assert!(
            cache.used_bytes() >= cache.len() as u64 * floor,
            "used {} below honest floor {} x {floor}",
            cache.used_bytes(),
            cache.len()
        );
    }

    #[test]
    fn header_heavy_entries_charge_per_header() {
        let mut pairs: Vec<(String, String)> = (0..1000)
            .map(|i| (format!("x-hdr-{i:04}"), format!("value-{i:04}")))
            .collect();
        pairs.push(("cache-control".to_string(), "max-age=60".to_string()));
        let mut cache = store();
        assert_eq!(
            cache.put(
                SCRIPT,
                &Namespace::Default,
                &PutRequest {
                    method: "GET",
                    url: URL,
                    status: 200,
                    headers: &pairs,
                    body: b"x",
                },
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        let floor = honest_floor(SCRIPT, &Namespace::Default, URL, &pairs, b"x");
        assert!(
            cache.used_bytes() >= floor,
            "used {} below honest floor {floor}",
            cache.used_bytes()
        );
        // A store cap below the honest charge refuses the entry instead
        // of holding tens of kilobytes against a few kilobytes of
        // accounting.
        let mut small = WorkerCache::new(64 * 1024);
        assert_eq!(
            small.put(
                SCRIPT,
                &Namespace::Default,
                &PutRequest {
                    method: "GET",
                    url: URL,
                    status: 200,
                    headers: &pairs,
                    body: b"x",
                },
                NOW
            ),
            Ok(PutOutcome::NotStored)
        );
    }

    #[test]
    fn shedding_drops_every_entry() {
        let mut cache = store();
        assert_eq!(
            put(
                &mut cache,
                URL,
                200,
                &[("cache-control", "max-age=60")],
                b"one",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(
            put(
                &mut cache,
                "https://cache.test/other",
                200,
                &[("cache-control", "max-age=60")],
                b"two",
                NOW
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(cache.len(), 2);
        assert!(cache.used_bytes() > 0);
        cache.shed();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.used_bytes(), 0);
        assert_eq!(lookup(&mut cache, URL, &[], NOW), MatchOutcome::Miss);
        assert_eq!(
            lookup(&mut cache, "https://cache.test/other", &[], NOW),
            MatchOutcome::Miss
        );
    }

    #[test]
    fn max_bytes_parses() {
        assert_eq!(parse_max_bytes(None), Ok(DEFAULT_MAX_BYTES));
        assert_eq!(parse_max_bytes(Some("4096".to_string())), Ok(4096));
        assert_eq!(parse_max_bytes(Some("0".to_string())), Ok(0));
        assert!(parse_max_bytes(Some("abc".to_string())).is_err());
        assert!(parse_max_bytes(Some(String::new())).is_err());
        assert!(parse_max_bytes(Some("-1".to_string())).is_err());
    }

    #[test]
    fn cache_scope_parses() {
        assert_eq!(parse_cache_scope(None), Ok(CacheScope::Script));
        assert_eq!(
            parse_cache_scope(Some("script".to_string())),
            Ok(CacheScope::Script)
        );
        assert_eq!(
            parse_cache_scope(Some("deployment".to_string())),
            Ok(CacheScope::Deployment)
        );
        assert!(parse_cache_scope(Some(String::new())).is_err());
        assert!(parse_cache_scope(Some("Script".to_string())).is_err());
        assert!(parse_cache_scope(Some("zone".to_string())).is_err());
    }

    fn scoped_put(
        store: &mut Store,
        scope: CacheScope,
        script: &str,
        deployment: Option<&str>,
        url: &str,
        body: &[u8],
    ) -> Result<PutOutcome, PutRejection> {
        let held = headers(&[("cache-control", "max-age=60")]);
        let op = OpScope {
            scope,
            script,
            deployment,
        };
        store.put(
            op,
            &Namespace::Default,
            &PutRequest {
                method: "GET",
                url,
                status: 200,
                headers: &held,
                body,
            },
            NOW,
        )
    }

    fn scoped_lookup(
        store: &mut Store,
        scope: CacheScope,
        script: &str,
        deployment: Option<&str>,
        url: &str,
    ) -> MatchOutcome {
        let held: Vec<(String, String)> = Vec::new();
        let op = OpScope {
            scope,
            script,
            deployment,
        };
        store.lookup(
            op,
            &Namespace::Default,
            &LookupRequest {
                url,
                method: "GET",
                ignore_method: false,
                request_headers: &held,
            },
            NOW,
        )
    }

    #[test]
    fn deployment_scope_shares_across_scripts() {
        let mut store = Store::new(DEFAULT_MAX_BYTES);
        assert_eq!(
            scoped_put(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                URL,
                b"shared"
            ),
            Ok(PutOutcome::Stored)
        );
        // Another script of the same deployment matches the entry.
        let found = scoped_lookup(
            &mut store,
            CacheScope::Deployment,
            "probe",
            Some("dep-1"),
            URL,
        );
        assert_eq!(hit_body(&found), b"shared");
        // And deletes it: the purge lands in the shared scope.
        assert!(store.delete(
            OpScope {
                scope: CacheScope::Deployment,
                script: "probe",
                deployment: Some("dep-1"),
            },
            &Namespace::Default,
            URL,
            "GET",
            false,
            NOW
        ));
        assert_eq!(
            scoped_lookup(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                URL
            ),
            MatchOutcome::Miss
        );
    }

    #[test]
    fn deployment_scopes_are_isolated_from_each_other_and_script() {
        let mut store = Store::new(DEFAULT_MAX_BYTES);
        assert_eq!(
            scoped_put(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                URL,
                b"one"
            ),
            Ok(PutOutcome::Stored)
        );
        // A different deployment never sees the entry, and neither does
        // the script store, even for the same script and URL.
        assert_eq!(
            scoped_lookup(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-2"),
                URL
            ),
            MatchOutcome::Miss
        );
        assert_eq!(
            scoped_lookup(&mut store, CacheScope::Script, "app", None, URL),
            MatchOutcome::Miss
        );
        // The reverse direction holds too.
        let other = "https://cache.test/other-scope";
        assert_eq!(
            scoped_put(
                &mut store,
                CacheScope::Script,
                "app",
                Some("dep-1"),
                other,
                b"two"
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(
            scoped_lookup(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                other
            ),
            MatchOutcome::Miss
        );
    }

    #[test]
    fn missing_deployment_key_stays_script_scoped() {
        let mut store = Store::new(DEFAULT_MAX_BYTES);
        // The runtime pool, a loaded worker, and a test isolate carry no
        // deployment key: their entries land in the script store whatever
        // the mode, so no deployment script can read them.
        assert_eq!(
            scoped_put(
                &mut store,
                CacheScope::Deployment,
                "runtime",
                None,
                URL,
                b"r"
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(
            hit_body(&scoped_lookup(
                &mut store,
                CacheScope::Script,
                "runtime",
                None,
                URL
            )),
            b"r"
        );
        assert_eq!(
            scoped_lookup(
                &mut store,
                CacheScope::Deployment,
                "runtime",
                Some("dep-1"),
                URL
            ),
            MatchOutcome::Miss
        );
    }

    #[test]
    fn deployment_scopes_have_independent_budgets() {
        // Each entry below charges about 1.3 KiB, so a 2000-byte cap
        // holds one entry per scope: the second put in a scope evicts the
        // first, while the other scope's entry is untouched.
        let mut store = Store::new(2000);
        let body = vec![b'v'; 500];
        assert_eq!(
            scoped_put(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                "https://cache.test/budget/0",
                &body
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(
            scoped_put(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                "https://cache.test/budget/1",
                &body
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(
            scoped_lookup(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                "https://cache.test/budget/0"
            ),
            MatchOutcome::Miss
        );
        assert_eq!(
            hit_body(&scoped_lookup(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                "https://cache.test/budget/1"
            ))
            .len(),
            500
        );
        assert_eq!(
            scoped_put(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-2"),
                "https://cache.test/budget/0",
                &body
            ),
            Ok(PutOutcome::Stored)
        );
        let survived = scoped_lookup(
            &mut store,
            CacheScope::Deployment,
            "app",
            Some("dep-2"),
            "https://cache.test/budget/0",
        );
        assert_eq!(hit_body(&survived).len(), 500);
        // Filling the second scope evicted nothing from the first.
        assert_eq!(
            hit_body(&scoped_lookup(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                "https://cache.test/budget/1"
            ))
            .len(),
            500
        );
    }

    #[test]
    fn purge_deployment_scope_drops_only_that_scope() {
        let mut store = Store::new(DEFAULT_MAX_BYTES);
        assert_eq!(
            scoped_put(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                URL,
                b"one"
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(
            scoped_put(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-2"),
                URL,
                b"two"
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(
            scoped_put(&mut store, CacheScope::Script, "app", None, URL, b"three"),
            Ok(PutOutcome::Stored)
        );
        store.purge_deployment("dep-1");
        assert_eq!(
            scoped_lookup(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                URL
            ),
            MatchOutcome::Miss
        );
        assert_eq!(
            hit_body(&scoped_lookup(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-2"),
                URL
            )),
            b"two"
        );
        assert_eq!(
            hit_body(&scoped_lookup(
                &mut store,
                CacheScope::Script,
                "app",
                None,
                URL
            )),
            b"three"
        );
        // An unknown key is a no-op, never an error.
        store.purge_deployment("dep-1");
        store.purge_deployment("never-existed");
    }

    #[test]
    fn shedding_drops_deployment_scopes() {
        let mut store = Store::new(DEFAULT_MAX_BYTES);
        assert_eq!(
            scoped_put(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                URL,
                b"one"
            ),
            Ok(PutOutcome::Stored)
        );
        assert_eq!(
            scoped_put(&mut store, CacheScope::Script, "app", None, URL, b"two"),
            Ok(PutOutcome::Stored)
        );
        store.shed();
        assert_eq!(
            scoped_lookup(
                &mut store,
                CacheScope::Deployment,
                "app",
                Some("dep-1"),
                URL
            ),
            MatchOutcome::Miss
        );
        assert_eq!(
            scoped_lookup(&mut store, CacheScope::Script, "app", None, URL),
            MatchOutcome::Miss
        );
    }

    #[test]
    fn multipart_boundaries_are_hex() {
        for _ in 0..8 {
            let boundary = multipart_boundary();
            assert_eq!(boundary.len(), 32);
            assert!(boundary.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
    }
}
