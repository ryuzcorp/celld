// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): writable /tmp + node:url regression test (upstream #280).

// A black-box runner, not node code: it boots the binary, copies fixtures,
// and polls TCP with wall-clock deadlines.
#![allow(clippy::disallowed_methods)]

//! Upstream denoland/celld#280: with `nodejs_compat`, `node:fs` writes and
//! streams threw `not implemented in celld`, and `node:url` lacked
//! `pathToFileURL`/`fileURLToPath`. `/tmp` is now a writable in-memory tree
//! per request, and the URL file helpers exist.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

fn cello_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cello"))
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/tmp-fs")
        .canonicalize()
        .expect("fixture exists")
}

mod common;
use common::{announced, forward_announce};

/// Tempdir for one fixture copy: `/dev/shm` when usable, else the
/// default tempdir.
fn fixture_tempdir(prefix: &str) -> tempfile::TempDir {
    let mut builder = tempfile::Builder::new();
    builder.prefix(prefix);
    match builder.tempdir_in("/dev/shm") {
        Ok(dir) => dir,
        Err(error) => {
            eprintln!("fixture tempdir fell back to disk (/dev/shm unusable: {error}");
            builder.tempdir().expect("fixture tempdir")
        }
    }
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create fixture dir");
    for file in ["wrangler.json", "worker.js"] {
        std::fs::copy(src.join(file), dst.join(file)).expect("copy fixture");
    }
}

/// A booted `celld dev` and its address. Kills the child on drop, so a
/// failing assertion cannot leak a server onto its port.
struct TestNode {
    child: Child,
    public: String,
}

impl TestNode {
    fn boot(app_project: &Path) -> Self {
        Self::boot_with_env(app_project, &[])
    }

    /// Boot with extra process environment, so a test can set a knob such
    /// as `CELLO_TMP_MAX_BYTES` for the node under test.
    fn boot_with_env(app_project: &Path, extra_env: &[(&str, &str)]) -> Self {
        let mut command = Command::new(cello_binary());
        for (key, value) in extra_env {
            command.env(key, value);
        }
        let mut child = command
            .arg("dev")
            .env("NO_COLOR", "1")
            .arg(app_project)
            .arg("--port")
            .arg("0")
            .arg("--no-watch")
            .arg("--logs")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn celld dev");
        let mut stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        std::thread::spawn(move || {
            let mut sink = Vec::new();
            let _ = stdout.read_to_end(&mut sink);
        });
        let announced_lines: Arc<Mutex<Vec<String>>> = Arc::default();
        forward_announce(stderr, Arc::clone(&announced_lines));
        let mut node = Self {
            child,
            public: String::new(),
        };
        node.public = poll_until("boot", Duration::from_secs(120), || {
            if let Some(status) = node.child.try_wait().expect("poll child") {
                panic!("celld dev exited during boot: {status}");
            }
            let addr = announced(&announced_lines, "celld listening on ")?;
            probe(&addr, "/health").then_some(addr)
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

fn get(addr: &str, path: &str) -> Result<(u16, String), String> {
    let mut stream = TcpStream::connect(addr).map_err(|error| format!("connect: {error}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .map_err(|error| format!("timeout: {error}"))?;
    let wire = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
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

fn probe(addr: &str, path: &str) -> bool {
    matches!(get(addr, path), Ok((200, _)))
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

#[test]
fn tmp_is_writable_and_file_urls_round_trip() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-fs-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    let (status, body) = get(&node.public, "/").expect("GET /");
    assert_eq!(status, 200, "GET /: {body}");
    let out: serde_json::Value = serde_json::from_str(&body).expect("result JSON");
    let get = |name: &str| {
        out.get(name)
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("{name} missing or not a string: {body}"))
    };
    // All three writeFile forms land and read back.
    assert_eq!(get("a"), "x");
    assert_eq!(get("c"), "sync");
    assert_eq!(get("d"), "cb");
    // The write stream commits on finish and the read stream replays it.
    assert_eq!(get("b"), "hello world");
    // The node:url file helpers round-trip.
    assert_eq!(get("url"), "file:///tmp/a.txt");
    assert_eq!(get("back"), "/tmp/a.txt");
}

// Exclusive-create flags fail before mutating, streams honor their
// encoding and open timing, and POSIX file URLs keep `%5c`.
#[test]
fn tmp_flags_streams_and_backslash_urls() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-fs-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    // `wx`/`ax` on an existing path keep the old bytes; plain `w`
    // truncates; a read-mode flag is refused loudly.
    let out = get_json(&node.public, "/flag-exclusive");
    let body = out.to_string();
    assert_eq!(code_of(&out, "wx", &body), "EEXIST", "wx: {body}");
    assert_eq!(code_of(&out, "keptWx", &body), "old", "wx kept: {body}");
    assert_eq!(code_of(&out, "ax", &body), "EEXIST", "ax: {body}");
    assert_eq!(code_of(&out, "keptAx", &body), "old", "ax kept: {body}");
    assert_eq!(code_of(&out, "w", &body), "ok", "w: {body}");
    assert_eq!(code_of(&out, "afterW", &body), "new", "w wrote: {body}");
    assert_eq!(
        code_of(&out, "readMode", &body),
        "ERR_INVALID_ARG_VALUE",
        "read flag: {body}"
    );

    // An exclusive stream errors and never opens on an existing file.
    let out = get_json(&node.public, "/stream-exclusive");
    let body = out.to_string();
    assert_eq!(code_of(&out, "code", &body), "EEXIST", "stream wx: {body}");
    assert_eq!(
        out.get("opened").and_then(|v| v.as_bool()),
        Some(false),
        "no open: {body}"
    );
    assert_eq!(code_of(&out, "kept", &body), "old", "stream kept: {body}");

    // `"4142"` under `hex` is the two bytes 0x41 0x42.
    let out = get_json(&node.public, "/stream-encoding");
    let body = out.to_string();
    assert_eq!(code_of(&out, "code", &body), "ok", "hex stream: {body}");
    assert_eq!(code_of(&out, "hex", &body), "4142", "hex bytes: {body}");
    assert_eq!(
        out.get("length").and_then(|v| v.as_u64()),
        Some(2),
        "hex length: {body}"
    );
    let out = get_json(&node.public, "/stream-bad-encoding");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "code", &body),
        "ERR_UNKNOWN_ENCODING",
        "bad encoding: {body}"
    );

    // The file exists before `open`: a new file reads empty, a `w` file
    // reads truncated, and a bad parent errors without opening.
    let out = get_json(&node.public, "/stream-open-read");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "created", &body),
        "\"\"",
        "open sees new file: {body}"
    );
    assert_eq!(
        code_of(&out, "truncated", &body),
        "\"\"",
        "open sees truncation: {body}"
    );
    assert_eq!(
        out.get("badOpened").and_then(|v| v.as_bool()),
        Some(false),
        "no open on error: {body}"
    );
    assert_eq!(
        code_of(&out, "badCode", &body),
        "ENOENT",
        "open error: {body}"
    );

    // An encoded backslash is a valid POSIX filename character; an
    // encoded slash is still rejected.
    let out = get_json(&node.public, "/url-backslash");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "roundTrip", &body),
        "/tmp/a\\b",
        "backslash round trip: {body}"
    );
    assert_eq!(
        code_of(&out, "slash", &body),
        "ERR_INVALID_FILE_URL_PATH",
        "slash still rejected: {body}"
    );
}

// Unsupported stream ranges fail loudly with `ERR_INVALID_ARG_VALUE`
// instead of silently serving whole bytes, and directory file URLs keep
// their trailing slash through the round trip.
#[test]
fn tmp_stream_ranges_reject_and_dir_urls_keep_slash() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-fs-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    let out = get_json(&node.public, "/stream-opts");
    let body = out.to_string();
    for name in ["writeStart", "readStart", "readEnd", "readEncoding"] {
        assert_eq!(
            code_of(&out, name, &body),
            "ERR_INVALID_ARG_VALUE",
            "{name} must fail loudly: {body}"
        );
    }

    let out = get_json(&node.public, "/dir-url");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "href", &body),
        "file:///tmp/d/",
        "directory URL keeps its slash: {body}"
    );
    assert_eq!(
        code_of(&out, "back", &body),
        "/tmp/d/",
        "directory URL round-trips: {body}"
    );
}

/// GET a JSON route, asserting 200 first so a handler crash reads as its
/// body instead of a JSON parse error.
fn get_json(addr: &str, path: &str) -> serde_json::Value {
    let (status, body) = get(addr, path).expect("GET");
    assert_eq!(status, 200, "GET {path}: {body}");
    serde_json::from_str(&body).expect("result JSON")
}

fn code_of(out: &serde_json::Value, name: &str, body: &str) -> String {
    out.get(name)
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("{name} missing or not a string: {body}"))
        .to_string()
}

/// A write past the per-request cap fails with `ENOSPC`, while a write
/// under the cap on the same node succeeds.
#[test]
fn tmp_cap_rejects_over_cap_writes_with_enospc() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-cap-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot_with_env(project.path(), &[("CELLO_TMP_MAX_BYTES", "256")]);

    let out = get_json(&node.public, "/cap-write?bytes=16");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "code", &body),
        "ok",
        "under-cap write: {body}"
    );

    let out = get_json(&node.public, "/cap-write?bytes=1024");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "code", &body),
        "ENOSPC",
        "over-cap write: {body}"
    );
}

/// Overwrites free the replaced bytes, and appends count toward the cap:
/// two 48-byte overwrites fit a 64-byte cap, a 32-byte append on top fails.
#[test]
fn tmp_cap_counts_overwrites_and_appends() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-cap-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot_with_env(project.path(), &[("CELLO_TMP_MAX_BYTES", "256")]);

    let out = get_json(&node.public, "/cap-overwrite");
    let body = out.to_string();
    assert_eq!(code_of(&out, "first", &body), "ok", "first write: {body}");
    assert_eq!(
        code_of(&out, "second", &body),
        "ok",
        "overwrite frees: {body}"
    );
    assert_eq!(
        code_of(&out, "append", &body),
        "ENOSPC",
        "append counts: {body}"
    );
}

/// A stream past the cap errors with `ENOSPC` as data arrives, while an
/// under-cap stream finishes and replays its bytes.
#[test]
fn tmp_stream_applies_cap_as_data_arrives() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-cap-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot_with_env(project.path(), &[("CELLO_TMP_MAX_BYTES", "256")]);

    let out = get_json(&node.public, "/cap-stream?bytes=32");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "code", &body),
        "ok",
        "under-cap stream: {body}"
    );
    assert_eq!(
        out.get("length").and_then(|v| v.as_u64()),
        Some(32),
        "replay: {body}"
    );

    let out = get_json(&node.public, "/cap-stream?bytes=256");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "code", &body),
        "ENOSPC",
        "over-cap stream: {body}"
    );
}

/// A file written in request A is invisible in request B: per-request
/// isolation, the headline tenant property of the request tree.
#[test]
fn tmp_is_isolated_between_requests() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-fs-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    let out = get_json(&node.public, "/iso-write");
    let body = out.to_string();
    assert_eq!(code_of(&out, "code", &body), "ok", "write: {body}");

    let out = get_json(&node.public, "/iso-read");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "code", &body),
        "ENOENT",
        "other request: {body}"
    );
}

/// Writes outside `/tmp` fail instead of reaching the host filesystem.
/// The tree answers `EPERM` for any non-`/tmp` path.
#[test]
fn tmp_rejects_writes_outside_tmp() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-fs-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    let out = get_json(&node.public, "/outside");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "code", &body),
        "EPERM",
        "outside /tmp: {body}"
    );
}

/// Entry metadata counts against the same cap: each new path costs its
/// length plus a fixed overhead, so zero-byte files cannot grow the tree
/// past the budget for free.
#[test]
fn tmp_cap_counts_empty_files() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-cap-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot_with_env(project.path(), &[("CELLO_TMP_MAX_BYTES", "256")]);

    // Zero-byte files still cost an entry charge: 100 empties exceed 256.
    let out = get_json(&node.public, "/many-empty?n=100");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "code", &body),
        "ENOSPC",
        "many empty files: {body}"
    );
}

/// A single path whose entry charge alone exceeds the cap fails outright.
#[test]
fn tmp_cap_counts_long_paths() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-cap-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot_with_env(project.path(), &[("CELLO_TMP_MAX_BYTES", "256")]);

    let out = get_json(&node.public, "/long-path?len=2000");
    let body = out.to_string();
    assert_eq!(code_of(&out, "code", &body), "ENOSPC", "long path: {body}");
}

/// Directories cost entries too.
#[test]
fn tmp_cap_counts_directories() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-cap-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot_with_env(project.path(), &[("CELLO_TMP_MAX_BYTES", "256")]);

    let out = get_json(&node.public, "/many-dirs?n=50");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "code", &body),
        "ENOSPC",
        "many directories: {body}"
    );
}

/// An oversize single write fails `ENOSPC`, and an oversize write outside
/// `/tmp` still reports `EPERM`: the length pre-check preserves the op's
/// error precedence. Both pins pass before and after the pre-check — the
/// pre-check's allocation avoidance is not externally observable (the code
/// is identical either way), so it is guaranteed by construction and by
/// the `would_exceed`-agrees-with-`write_file` unit test instead.
#[test]
fn tmp_oversize_write_fails_enospc() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-cap-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot_with_env(project.path(), &[("CELLO_TMP_MAX_BYTES", "256")]);

    let out = get_json(&node.public, "/big-write?bytes=8388608");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "code", &body),
        "ENOSPC",
        "oversize write: {body}"
    );

    let out = get_json(&node.public, "/big-outside?bytes=8388608");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "code", &body),
        "EPERM",
        "oversize write outside /tmp: {body}"
    );
}

/// `/tmp` files report writable for `W_OK` and carry write bits in their
/// stat mode, while `/bundle` files stay read-only on both counts. A
/// recursive `mkdir` over an existing file is `EEXIST`, and an `open`
/// listener that rewrites the file before a chunkless `end` keeps its
/// bytes instead of losing them to a second truncate in `final`.
#[test]
fn tmp_files_report_writable_and_final_keeps_open_bytes() {
    let fixture = fixture_dir();
    let project = fixture_tempdir("celld-tmp-fs-");
    copy_dir(&fixture, project.path());
    let node = TestNode::boot(project.path());

    let out = get_json(&node.public, "/writable-stat");
    let body = out.to_string();
    assert_eq!(code_of(&out, "tmpWOk", &body), "ok", "tmp W_OK: {body}");
    assert_eq!(code_of(&out, "tmpROk", &body), "ok", "tmp R_OK: {body}");
    assert_eq!(
        code_of(&out, "bundleWOk", &body),
        "ENOENT",
        "bundle W_OK: {body}"
    );
    assert_eq!(
        code_of(&out, "bundleROk", &body),
        "ok",
        "bundle R_OK: {body}"
    );
    assert_eq!(
        code_of(&out, "fileWriteBits", &body),
        "writable",
        "tmp mode: {body}"
    );
    assert_eq!(
        code_of(&out, "bundleWriteBits", &body),
        "read-only",
        "bundle mode: {body}"
    );
    assert_eq!(
        out.get("tmpIsFile").and_then(|v| v.as_bool()),
        Some(true),
        "tmp isFile: {body}"
    );
    assert_eq!(
        out.get("bundleIsFile").and_then(|v| v.as_bool()),
        Some(true),
        "bundle isFile: {body}"
    );

    let out = get_json(&node.public, "/recursive-on-file");
    let body = out.to_string();
    assert_eq!(
        code_of(&out, "fileCode", &body),
        "EEXIST",
        "recursive mkdir on file: {body}"
    );
    assert_eq!(code_of(&out, "kept", &body), "x", "file kept: {body}");
    assert_eq!(
        code_of(&out, "dirCode", &body),
        "ok",
        "recursive mkdir on dir: {body}"
    );

    let out = get_json(&node.public, "/open-listener-keep");
    let body = out.to_string();
    assert_eq!(code_of(&out, "data", &body), "keep", "open bytes: {body}");
}
