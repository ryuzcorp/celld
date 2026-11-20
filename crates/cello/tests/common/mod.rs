// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): shared ephemeral-port boot helpers (0077).
// Modified by Ryuz Corp (Cello): hermetic spawn helper (cleared env).

//! Shared boot helpers for the black-box runner tests: each test boots the
//! real `cello dev` binary and polls TCP with wall-clock deadlines.
//!
//! The dev node's public port is ephemeral (`--port 0`); the node's own
//! announce lines on the supervisor's stderr (`--logs` forwards them) name
//! the addresses it bound. Tests take them through `forward_announce` +
//! `announced` instead of probing a port first, so no probe-then-bind race
//! can hand a test another test's node. The node is non-dumpable (patch
//! 0053), so `/proc/<pid>/fd` no longer names its listeners.
//!
//! Every spawn goes through [`cello_command`] (or [`apply_hermetic_env`]
//! for the one wrapper spawn), which clears the inherited environment and
//! rebuilds it from an explicit allowlist. A test that inherits the
//! developer's shell would otherwise pick up `CELLO_DEV_STATE` (state
//! outside the fixture), `RUST_LOG` (log-gated boot probes), ambient proxy
//! or TLS variables — and any stray `CELLD_*`, which makes cello refuse to
//! start at all.

use std::ffi::OsStr;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

/// No extra environment entries: `cello_command(project, NO_EXTRA_ENV)`.
/// A typed empty array, since a bare `[]` gives the compiler nothing to
/// infer the pair types from.
#[allow(dead_code)] // Each test binary uses only the helpers it needs.
pub const NO_EXTRA_ENV: [(&str, &str); 0] = [];

/// The `cello` binary under test. The one place allowed to name
/// `CARGO_BIN_EXE_cello` (a guardrail test enforces it): test files take
/// the path through here so every spawn also gets the hermetic
/// environment from [`cello_command`].
pub fn cello_binary() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cello"))
}

/// Build the `cello` command with a hermetic environment: the inherited
/// environment is cleared, then rebuilt from an explicit allowlist —
/// `PATH`, `TMPDIR`, and `CELLO_ESBUILD` pass through when set, `HOME`
/// points at a fresh `.test-home` inside the test's own temp `project`
/// (docker socket discovery and certificate stores consult it, and scoping
/// it to the fixture keeps the developer's real home out of reach) — plus
/// exactly the `extra_env` the test passes. Entries applied last win, so a
/// test can still override an allowlisted key deliberately.
///
/// `CELLO_ESBUILD` is allowlisted (and nothing else is) so a developer or
/// CI run whose `esbuild` on `PATH` is a wrapper script can point the
/// `cello dev` child at the real binary without breaking hermeticity: the
/// `CELLD_HERMETIC_CANARY` tripwire stays cleared, so a spawn that leaked
/// the ambient environment would still refuse to start.
#[allow(dead_code)] // Each test binary uses only the helpers it needs.
pub fn cello_command(
    project: &Path,
    extra_env: impl IntoIterator<Item = (impl AsRef<OsStr>, impl AsRef<OsStr>)>,
) -> Command {
    let mut command = Command::new(cello_binary());
    apply_hermetic_env(&mut command, project, extra_env);
    command
}

/// Apply the hermetic environment from [`cello_command`] to an already
/// built command. For the wrapper spawn that transitively runs the cello
/// binary (the `bash` trap in the disk-full test, which `exec`s the
/// binary, preserving this environment into it).
#[allow(dead_code)] // Only the wrapper spawn uses this directly.
pub fn apply_hermetic_env<'a>(
    command: &'a mut Command,
    project: &Path,
    extra_env: impl IntoIterator<Item = (impl AsRef<OsStr>, impl AsRef<OsStr>)>,
) -> &'a mut Command {
    // The fixture tempdir outlives the child (tests hold it beside the
    // node), so this directory cannot vanish mid-run.
    let home = project.join(".test-home");
    std::fs::create_dir_all(&home).expect("create hermetic HOME");
    command.env_clear();
    if let Some(path) = std::env::var_os("PATH") {
        command.env("PATH", path);
    }
    if let Some(tmpdir) = std::env::var_os("TMPDIR") {
        command.env("TMPDIR", tmpdir);
    }
    // `deploy`/`dev` bundle through `CELLO_ESBUILD` when it names the esbuild
    // binary, falling back to `esbuild` on `PATH`. Forward it when set so a
    // developer whose `PATH` esbuild is a wrapper script can still run the
    // suite; everything else ambient stays cleared (in particular the
    // `CELLD_HERMETIC_CANARY` tripwire, which keeps proving the child sees
    // nothing ambient).
    if let Some(esbuild) = std::env::var_os("CELLO_ESBUILD") {
        command.env("CELLO_ESBUILD", esbuild);
    }
    command.env("HOME", &home);
    for (name, value) in extra_env {
        command.env(name, value);
    }
    command
}

/// One address of a `cello dev` node, from the announce line the supervisor
/// forwards to its stderr: `cello listening on <addr> (` names the public
/// listener, `cello internal listening on <addr> (` the internal one.
#[allow(dead_code)] // Each test binary uses only the helpers it needs.
pub fn announced(lines: &Mutex<Vec<String>>, prefix: &str) -> Option<String> {
    lines
        .lock()
        .expect("announce lock")
        .iter()
        .find_map(|line| {
            let address = line.strip_prefix(prefix)?.split_once(' ')?.0;
            Some(address.to_string())
        })
}

/// Collect the supervisor's stderr lines, which carry the node's announce
/// lines. If the child exits before announcing, the caller panics with the
/// lines collected so far, so a future boot failure stays diagnosable.
#[allow(dead_code)] // Each test binary uses only the helpers it needs.
pub fn forward_announce(stderr: std::process::ChildStderr, lines: Arc<Mutex<Vec<String>>>) {
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        while reader.read_line(&mut line).is_ok_and(|read| read > 0) {
            lines
                .lock()
                .expect("announce lock")
                .push(std::mem::take(&mut line));
        }
    });
}
