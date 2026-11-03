// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): per-node Workers Cache
// API conformance. Apache-2.0.

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Conformance for the per-node Workers Cache API: `caches.default` and
//! `caches.open(name)` with `match`/`put`/`delete` over Cloudflare's
//! cache semantics (freshness from `s-maxage`/`max-age`/`Expires`, edge-TTL
//! defaults by status, `no-store`/`private`/`Set-Cookie` never cached,
//! `Vary` ignored, conditional and range requests honored).
//!
//! One `celld dev` boots the app fixture; the probe fixture (a second
//! script) is pre-seeded into the dev bucket via the library (the same
//! `build` + `write` `dev` itself runs), so the node loads both scripts and
//! the service binding resolves cross-isolate. There are no fixed sleeps:
//! every wait polls an observable condition with a deadline, and the server
//! guard kills the child on every exit path.

use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn celld_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_celld"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/cache")
        .canonicalize()
        .expect("fixture exists")
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("probe bind");
    listener.local_addr().expect("probe addr").port()
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// Percent-encode a query value: cache directives and HTTP dates carry
/// commas, spaces, quotes, and slashes no raw query string can hold.
fn encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Deploy the probe into the app project's dev bucket before `dev`
/// boots, so the node loads both scripts. `dev` deploys the app itself and
/// becomes primary (last write wins `deploy/current.json`); the
/// probe's named pointer stays for the service binding to resolve.
fn preseed_probe(app_project: &Path, probe_src: &Path) {
    let state = app_project.join(".celld/dev");
    std::fs::create_dir_all(state.join("runtime")).expect("state dir");
    let database = state.join("objects.sqlite3");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    celld::asyncrt::set_host_handle(rt.handle().clone());
    rt.block_on(async {
        let bucket = celld::dev::open_local_bucket(&database).expect("open dev bucket");
        celld::fleet::validate_bucket(&bucket)
            .await
            .expect("validate bucket");
        bucket.probe_cas().await.expect("probe cas");
        let built = celld::deploy::build(&celld::deploy::Options {
            config: Some(probe_src.to_path_buf()),
            bucket: None,
            endpoint: None,
            region: None,
            dry_run: false,
            json: false,
            vars: celld::deploy::VarOverrides::None,
            local_images: true,
            sourcemap: None,
            no_activate: false,
        })
        .expect("build probe");
        celld::deploy::write(&bucket, &built)
            .await
            .expect("write probe");
        celld::wake_format::ensure_ready(&bucket)
            .await
            .expect("ensure ready");
    });
}

/// A booted `celld dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path, envs: &[(&str, &str)]) -> Self {
        let port = free_port();
        let mut command = Command::new(celld_binary());
        command
            .arg("dev")
            .arg(app_project)
            .arg("--port")
            .arg(port.to_string())
            .arg("--no-watch")
            .arg("--logs");
        for (name, value) in envs {
            command.env(name, value);
        }
        let mut child = command
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn celld dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let mut stderr = child.stderr.take().expect("piped stderr");
        // Drain both pipes so a chatty child can never block on a full one.
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stderr.read_to_end(&mut sink);
        });
        let public = format!("127.0.0.1:{port}");
        // The kill-on-drop guard exists before the readiness poll, so a
        // boot timeout or an early child exit cannot leak the server.
        let mut node = Self {
            child,
            public: public.clone(),
        };
        // Readiness is the public port answering with the app loaded.
        poll_until("boot", Duration::from_secs(120), || {
            if !probe(&public, "/health") {
                return None;
            }
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("celld dev exited during boot: {status}");
            }
            Some(public.clone())
        });
        node
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn request_timeout(
    addr: &str,
    method: &str,
    path: &str,
    timeout: Duration,
) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
    );
    stream
        .write_all(wire.as_bytes())
        .map_err(|error| format!("write: {error}"))?;
    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|error| format!("read: {error}"))?;
    let raw = String::from_utf8(raw).map_err(|error| format!("utf8: {error}"))?;
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| format!("no header/body split in {raw:?}"))?;
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .ok_or_else(|| format!("no status in {head:?}"))?;
    Ok((status, body.to_string()))
}

fn get(addr: &str, path: &str) -> Result<(u16, String), String> {
    request_timeout(addr, "GET", path, Duration::from_secs(60))
}

/// Readiness probe with a short timeout: a bound-but-not-ready node holds
/// connections open, so a long read would eat the boot deadline.
fn probe(addr: &str, path: &str) -> bool {
    matches!(
        request_timeout(addr, "GET", path, Duration::from_secs(2)),
        Ok((200, _))
    )
}

fn get_json(node: &TestNode, path: &str) -> Value {
    let (status, body) = get(&node.public, path).expect("public GET");
    assert_eq!(status, 200, "GET {path}: {body}");
    serde_json::from_str(&body).expect("JSON body")
}

fn poll_until(
    name: &str,
    deadline: Duration,
    mut condition: impl FnMut() -> Option<String>,
) -> String {
    let start = Instant::now();
    loop {
        if let Some(done) = condition() {
            return done;
        }
        if start.elapsed() > deadline {
            panic!("{name}: condition unmet within the deadline");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn query(endpoint: &str, params: &[(&str, &str)]) -> String {
    let query = params
        .iter()
        .map(|(name, value)| format!("{name}={}", encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    format!("/{endpoint}?{query}")
}

/// `put` then `match` one key; the match is a separate request, so a hit
/// proves the entry survived outside the putting isolate.
fn put(node: &TestNode, params: &[(&str, &str)]) -> Value {
    let stored = get_json(node, &query("put", params));
    assert_eq!(stored["ok"], true, "{stored}");
    assert_eq!(stored["result"]["bodyUsed"], true, "{stored}");
    stored
}

fn matched(node: &TestNode, params: &[(&str, &str)]) -> Value {
    let found = get_json(node, &query("match", params));
    assert_eq!(found["ok"], true, "{found}");
    assert_eq!(found["result"]["hit"], true, "{found}");
    assert_eq!(
        found["result"]["headers"]["cf-cache-status"], "HIT",
        "{found}"
    );
    found["result"].clone()
}

fn missed(node: &TestNode, params: &[(&str, &str)]) {
    let found = get_json(node, &query("match", params));
    assert_eq!(found["ok"], true, "{found}");
    assert_eq!(found["result"]["hit"], false, "{found}");
}

fn boot_node(envs: &[(&str, &str)]) -> (tempfile::TempDir, TestNode) {
    let dir = tempfile::Builder::new()
        .prefix("celld-cache-")
        .tempdir()
        .expect("tempdir");
    let app_project = dir.path().join("app");
    let probe_src = dir.path().join("probe");
    copy_dir(&fixture_dir().join("app"), &app_project);
    copy_dir(&fixture_dir().join("probe"), &probe_src);
    preseed_probe(&app_project, &probe_src);
    let node = TestNode::boot(&app_project, envs);
    (dir, node)
}

#[test]
fn cache_conformance() {
    let (_dir, node) = boot_node(&[]);

    // A basic round trip: put consumes the response, match answers a
    // separate request with the stored body and a HIT marker.
    put(
        &node,
        &[("key", "basic"), ("body", "hello"), ("cc", "max-age=120")],
    );
    let basic = matched(&node, &[("key", "basic")]);
    assert_eq!(basic["status"], 200, "{basic}");
    assert_eq!(basic["body"], "hello", "{basic}");

    // A key never stored misses.
    missed(&node, &[("key", "absent")]);

    // Edge-TTL defaults when no freshness header is present: 200 and 404
    // cache, 500 does not.
    put(&node, &[("key", "plain200"), ("body", "p200")]);
    matched(&node, &[("key", "plain200")]);
    put(
        &node,
        &[("key", "plain404"), ("body", "p404"), ("status", "404")],
    );
    let not_found = matched(&node, &[("key", "plain404")]);
    assert_eq!(not_found["status"], 404, "{not_found}");
    put(
        &node,
        &[("key", "plain500"), ("body", "p500"), ("status", "500")],
    );
    missed(&node, &[("key", "plain500")]);
    // Explicit freshness caches even a 500.
    put(
        &node,
        &[
            ("key", "fresh500"),
            ("body", "f500"),
            ("status", "500"),
            ("cc", "max-age=120"),
        ],
    );
    let fresh = matched(&node, &[("key", "fresh500")]);
    assert_eq!(fresh["status"], 500, "{fresh}");

    // `no-store`, `private`, and `Set-Cookie` are never cached.
    put(&node, &[("key", "nostore"), ("cc", "no-store")]);
    missed(&node, &[("key", "nostore")]);
    put(&node, &[("key", "priv"), ("cc", "private")]);
    missed(&node, &[("key", "priv")]);
    put(&node, &[("key", "privfield"), ("cc", "private=foo")]);
    missed(&node, &[("key", "privfield")]);
    put(&node, &[("key", "cookie"), ("sc", "a=b")]);
    missed(&node, &[("key", "cookie")]);
    put(&node, &[("key", "cookies"), ("sc", "a=b"), ("sc2", "c=d")]);
    missed(&node, &[("key", "cookies")]);
    // The documented exception: `private=set-cookie` stores the response
    // with the `Set-Cookie` headers stripped.
    put(
        &node,
        &[
            ("key", "privcookie"),
            ("sc", "a=b"),
            ("cc", "private=set-cookie, max-age=120"),
        ],
    );
    let stripped = matched(&node, &[("key", "privcookie")]);
    assert!(
        stripped["headers"].get("set-cookie").is_none(),
        "{stripped}"
    );
    assert_eq!(
        stripped["headers"]["cache-control"], "max-age=120",
        "{stripped}"
    );

    // Freshness precedence: `s-maxage` wins over `max-age`.
    put(&node, &[("key", "smax"), ("cc", "s-maxage=100, max-age=1")]);

    // Expiry: a one-second `max-age` entry turns into a miss. Whether it is
    // a hit before then depends on how fast this host answers, so that half
    // is the unit tests' (injected clock); the long-lived entries above
    // prove hits end to end.
    put(
        &node,
        &[("key", "exp1"), ("body", "short"), ("cc", "max-age=1")],
    );
    poll_until("max-age expiry", Duration::from_secs(30), || {
        let found = get_json(&node, &query("match", &[("key", "exp1")]));
        assert_eq!(found["ok"], true, "{found}");
        if found["result"]["hit"] == false {
            return Some("expired".to_string());
        }
        None
    });

    // `Expires` is honored the same way.
    let soon = httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(3));
    put(
        &node,
        &[("key", "exp2"), ("body", "dated"), ("expires", &soon)],
    );
    poll_until("expires expiry", Duration::from_secs(30), || {
        let found = get_json(&node, &query("match", &[("key", "exp2")]));
        assert_eq!(found["ok"], true, "{found}");
        if found["result"]["hit"] == false {
            return Some("expired".to_string());
        }
        None
    });
    // An `Expires` in the past never stores.
    put(
        &node,
        &[
            ("key", "expast"),
            ("expires", "Wed, 01 Jan 2020 00:00:00 GMT"),
        ],
    );
    missed(&node, &[("key", "expast")]);

    // `s-maxage=1` beats `max-age=100`: the entry expires in a second.
    put(
        &node,
        &[("key", "smax1"), ("cc", "s-maxage=1, max-age=100")],
    );
    poll_until("s-maxage expiry", Duration::from_secs(30), || {
        let found = get_json(&node, &query("match", &[("key", "smax1")]));
        assert_eq!(found["ok"], true, "{found}");
        if found["result"]["hit"] == false {
            return Some("expired".to_string());
        }
        None
    });

    // `ignoreMethod` treats a non-GET key as GET for match and delete.
    put(
        &node,
        &[("key", "ign"), ("body", "gettable"), ("cc", "max-age=120")],
    );
    missed(&node, &[("key", "ign"), ("method", "POST")]);
    let ignored = matched(
        &node,
        &[("key", "ign"), ("method", "POST"), ("ignore", "1")],
    );
    assert_eq!(ignored["body"], "gettable", "{ignored}");
    let kept = get_json(
        &node,
        &query("delete", &[("key", "ign"), ("method", "POST")]),
    );
    assert_eq!(kept["result"]["deleted"], false, "{kept}");
    let purged = get_json(
        &node,
        &query(
            "delete",
            &[("key", "ign"), ("method", "POST"), ("ignore", "1")],
        ),
    );
    assert_eq!(purged["result"]["deleted"], true, "{purged}");
    missed(&node, &[("key", "ign")]);

    // Named caches are separate namespaces, and `open("default")` is not
    // `caches.default`.
    put(
        &node,
        &[
            ("key", "n1"),
            ("body", "in-a"),
            ("cache", "a"),
            ("cc", "max-age=120"),
        ],
    );
    matched(&node, &[("key", "n1"), ("cache", "a")]);
    missed(&node, &[("key", "n1"), ("cache", "b")]);
    missed(&node, &[("key", "n1")]);
    put(
        &node,
        &[
            ("key", "d1"),
            ("body", "in-named-default"),
            ("cache", "default"),
            ("cc", "max-age=120"),
        ],
    );
    matched(&node, &[("key", "d1"), ("cache", "default")]);
    missed(&node, &[("key", "d1")]);

    // `delete` reports whether an entry was there.
    put(&node, &[("key", "del1"), ("cc", "max-age=120")]);
    let first = get_json(&node, &query("delete", &[("key", "del1")]));
    assert_eq!(first["result"]["deleted"], true, "{first}");
    let second = get_json(&node, &query("delete", &[("key", "del1")]));
    assert_eq!(second["result"]["deleted"], false, "{second}");
    missed(&node, &[("key", "del1")]);
    let missing = get_json(&node, &query("delete", &[("key", "never")]));
    assert_eq!(missing["result"]["deleted"], false, "{missing}");

    // `put` still validates like workerd.
    for (endpoint, message) in [
        ("vary-star", "Cannot cache response with 'Vary: *' header."),
        (
            "put-206",
            "Cannot cache response to a range request (206 Partial Content).",
        ),
        ("put-post", "Cannot cache response to non-GET request."),
        (
            "put-used",
            "Cannot cache a response whose body is already used.",
        ),
        ("put-error", "cache.put: status is out of range"),
    ] {
        let threw = get_json(&node, &format!("/{endpoint}"));
        assert_eq!(threw["ok"], true, "{threw}");
        assert_eq!(threw["result"]["threw"], true, "{threw}");
        assert_eq!(threw["result"]["name"], "TypeError", "{threw}");
        assert_eq!(threw["result"]["message"], message, "{threw}");
    }
    let long = get_json(&node, "/open-long");
    assert_eq!(long["result"]["threw"], true, "{long}");
    assert_eq!(long["result"]["name"], "TypeError", "{long}");

    // `Vary` is ignored: one entry answers every variant.
    put(
        &node,
        &[
            ("key", "vary1"),
            ("body", "VA"),
            ("vary", "Accept-Encoding"),
            ("cc", "max-age=120"),
        ],
    );
    let gzip = matched(&node, &[("key", "vary1"), ("accept", "gzip")]);
    assert_eq!(gzip["body"], "VA", "{gzip}");
    let br = matched(&node, &[("key", "vary1"), ("accept", "br")]);
    assert_eq!(br["body"], "VA", "{br}");

    // Conditional requests: `If-None-Match` and `If-Modified-Since` get a
    // 304 with no body.
    put(
        &node,
        &[
            ("key", "cond1"),
            ("body", "data"),
            ("eta", "\"v1\""),
            ("cc", "max-age=120"),
        ],
    );
    let not_modified = get_json(
        &node,
        &query("match", &[("key", "cond1"), ("inm", "\"v1\"")]),
    );
    assert_eq!(not_modified["result"]["hit"], true, "{not_modified}");
    assert_eq!(not_modified["result"]["status"], 304, "{not_modified}");
    assert_eq!(not_modified["result"]["body"], "", "{not_modified}");
    assert_eq!(
        not_modified["result"]["headers"]["cf-cache-status"], "HIT",
        "{not_modified}"
    );
    let modified = matched(&node, &[("key", "cond1"), ("inm", "\"other\"")]);
    assert_eq!(modified["status"], 200, "{modified}");
    assert_eq!(modified["body"], "data", "{modified}");
    put(
        &node,
        &[
            ("key", "cond2"),
            ("body", "data"),
            ("lm", "Wed, 01 Jan 2020 00:00:00 GMT"),
            ("cc", "max-age=120"),
        ],
    );
    let since_after = get_json(
        &node,
        &query(
            "match",
            &[("key", "cond2"), ("ims", "Thu, 02 Jan 2020 00:00:00 GMT")],
        ),
    );
    assert_eq!(since_after["result"]["status"], 304, "{since_after}");
    let since_before = matched(
        &node,
        &[("key", "cond2"), ("ims", "Tue, 31 Dec 2019 00:00:00 GMT")],
    );
    assert_eq!(since_before["status"], 200, "{since_before}");

    // Range requests slice the stored body; an unsatisfiable one is a 416.
    put(
        &node,
        &[("key", "r1"), ("body", "0123456789"), ("cc", "max-age=120")],
    );
    let part = get_json(
        &node,
        &query("match", &[("key", "r1"), ("range", "bytes=0-3")]),
    );
    assert_eq!(part["result"]["hit"], true, "{part}");
    assert_eq!(part["result"]["status"], 206, "{part}");
    assert_eq!(part["result"]["body"], "0123", "{part}");
    assert_eq!(
        part["result"]["headers"]["content-range"], "bytes 0-3/10",
        "{part}"
    );
    assert_eq!(part["result"]["headers"]["content-length"], "4", "{part}");
    let suffix = matched(&node, &[("key", "r1"), ("range", "bytes=-3")]);
    assert_eq!(suffix["status"], 206, "{suffix}");
    assert_eq!(suffix["body"], "789", "{suffix}");
    let outside = get_json(
        &node,
        &query("match", &[("key", "r1"), ("range", "bytes=20-30")]),
    );
    assert_eq!(outside["result"]["hit"], true, "{outside}");
    assert_eq!(outside["result"]["status"], 416, "{outside}");
    assert_eq!(
        outside["result"]["headers"]["content-range"], "bytes */10",
        "{outside}"
    );
    let multi = get_json(
        &node,
        &query("match", &[("key", "r1"), ("range", "bytes=0-1,4-5")]),
    );
    assert_eq!(multi["result"]["status"], 206, "{multi}");
    let content_type = multi["result"]["headers"]["content-type"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        content_type.starts_with("multipart/byteranges; boundary="),
        "{multi}"
    );
    let multi_body = multi["result"]["body"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        multi_body.contains("01") && multi_body.contains("45"),
        "{multi}"
    );

    // `match` strips `Cache-Tag`: tags purge, they are not content.
    put(
        &node,
        &[("key", "tag1"), ("tag", "t1"), ("cc", "max-age=120")],
    );
    let tagged = matched(&node, &[("key", "tag1")]);
    assert!(tagged["headers"].get("cache-tag").is_none(), "{tagged}");

    // Every `match` builds a fresh response: two reads never share a body.
    put(
        &node,
        &[("key", "dr1"), ("body", "xyz"), ("cc", "max-age=120")],
    );
    let reread = get_json(&node, &query("double-read", &[("key", "dr1")]));
    assert_eq!(reread["result"]["b1"], "xyz", "{reread}");
    assert_eq!(reread["result"]["b2"], "xyz", "{reread}");

    // Isolation: the probe script sees none of the app's entries, and the
    // app sees none of the probe's, even for the same URL on one node.
    put(
        &node,
        &[
            ("key", "iso1"),
            ("body", "secret-app"),
            ("cc", "max-age=120"),
        ],
    );
    matched(&node, &[("key", "iso1")]);
    let peer_miss = get_json(&node, &query("peer-check", &[("key", "iso1")]));
    assert_eq!(peer_miss["result"]["peerHit"], false, "{peer_miss}");
    get_json(
        &node,
        &query("peer-put", &[("key", "iso2"), ("body", "secret-probe")]),
    );
    missed(&node, &[("key", "iso2")]);
    put(
        &node,
        &[("key", "iso3"), ("body", "APP"), ("cc", "max-age=120")],
    );
    get_json(
        &node,
        &query("peer-put", &[("key", "iso3"), ("body", "PROBE")]),
    );
    let app_side = matched(&node, &[("key", "iso3")]);
    assert_eq!(app_side["body"], "APP", "{app_side}");
    let probe_side = get_json(&node, &query("peer-get", &[("key", "iso3")]));
    assert_eq!(probe_side["result"]["hit"], true, "{probe_side}");
    assert_eq!(probe_side["result"]["body"], "PROBE", "{probe_side}");

    // Freshness precedence, the slow direction: `max-age=1` expired a while
    // ago, so a hit now proves `s-maxage=100` governed. Every sample must
    // hit, for at least a second and a half of sampling.
    let start = Instant::now();
    poll_until("s-maxage holds", Duration::from_secs(15), || {
        let found = get_json(&node, &query("match", &[("key", "smax")]));
        assert_eq!(found["ok"], true, "{found}");
        assert_eq!(found["result"]["hit"], true, "{found}");
        if start.elapsed() >= Duration::from_millis(1500) {
            return Some("held".to_string());
        }
        None
    });

    // An infinite body aborts at the 16 MiB per-entry cap instead of
    // buffering forever: put() settles like any over-cap put (no throw,
    // nothing stored, body consumed), the producer sees cancel, and the
    // bytes pulled stay within the cap plus stream pull-ahead — memory
    // bounded by construction, asserted on bytes, not time.
    let capped = get_json(&node, "/oversize-stream");
    assert_eq!(capped["ok"], true, "{capped}");
    assert!(capped["result"]["threw"].is_null(), "{capped}");
    assert_eq!(capped["result"]["bodyUsed"], true, "{capped}");
    assert_eq!(capped["result"]["cancelled"], true, "{capped}");
    assert_eq!(capped["result"]["hit"], false, "{capped}");
    let pulled = capped["result"]["pulled"].as_u64().expect("pulled bytes");
    const MAX_ENTRY: u64 = 16 * 1024 * 1024;
    const CHUNK: u64 = 64 * 1024;
    assert!(pulled > MAX_ENTRY, "drained short of the cap: {pulled}");
    assert!(
        pulled <= MAX_ENTRY + 4 * CHUNK,
        "buffered past the cap: {pulled}"
    );

    // A finite stream under the cap still stores and reads back whole.
    let roundtrip = get_json(&node, "/stream-roundtrip");
    assert_eq!(roundtrip["ok"], true, "{roundtrip}");
    assert_eq!(roundtrip["result"]["hit"], true, "{roundtrip}");
    assert_eq!(roundtrip["result"]["length"], 1024 * 1024, "{roundtrip}");
}
