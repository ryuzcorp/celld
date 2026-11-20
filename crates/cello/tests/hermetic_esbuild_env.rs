// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): hermetic CELLO_ESBUILD forwarding test.

// A hermeticity unit test, not node code: it inspects the built `Command`'s
// environment without spawning anything, so the execution boundary's
// injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! The hermetic spawn helper clears the inherited environment and rebuilds
//! it from an explicit allowlist. `CELLO_ESBUILD` is allowlisted so a
//! developer whose `esbuild` on `PATH` is a wrapper script (a mise shim)
//! can point the `cello dev` child at the real binary; everything else
//! ambient — in particular the `CELLD_HERMETIC_CANARY` tripwire — must
//! stay cleared.

mod common;

use common::cello_command;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::path::Path;

fn child_env(project: &Path) -> HashMap<OsString, Option<OsString>> {
    cello_command(project, common::NO_EXTRA_ENV)
        .get_envs()
        .map(|(name, value)| (name.to_owned(), value.map(|value| value.to_owned())))
        .collect()
}

/// One process-global ambient variable, restored on drop so this test's
/// manipulation cannot leak into anything else in this test binary.
struct Stash {
    name: &'static str,
    prior: Option<OsString>,
}

impl Stash {
    fn set(name: &'static str, value: &str) -> Self {
        let prior = std::env::var_os(name);
        std::env::set_var(name, value);
        Self { name, prior }
    }

    fn remove(name: &'static str) -> Self {
        let prior = std::env::var_os(name);
        std::env::remove_var(name);
        Self { name, prior }
    }
}

impl Drop for Stash {
    fn drop(&mut self) {
        if let Some(prior) = self.prior.take() {
            std::env::set_var(self.name, prior);
        } else {
            std::env::remove_var(self.name);
        }
    }
}

#[test]
fn hermetic_env_forwards_cello_esbuild_and_nothing_else_ambient() {
    let project = tempfile::TempDir::new().expect("hermetic env test project");

    // A set `CELLO_ESBUILD` reaches the child; ambient tripwires set right
    // here in the parent still do not.
    let (_esbuild, _canary, _log) = (
        Stash::set("CELLO_ESBUILD", "/opt/real/esbuild"),
        Stash::set("CELLD_HERMETIC_CANARY", "1"),
        Stash::set("RUST_LOG", "debug"),
    );
    let forwarded = child_env(project.path());
    assert_eq!(
        forwarded
            .get(OsStr::new("CELLO_ESBUILD"))
            .and_then(|value| value.as_deref()),
        Some(OsStr::new("/opt/real/esbuild")),
        "CELLO_ESBUILD must reach the cello child",
    );
    for tripwire in ["CELLD_HERMETIC_CANARY", "RUST_LOG"] {
        assert!(
            !forwarded.contains_key(OsStr::new(tripwire)),
            "{tripwire} must stay cleared from the cello child",
        );
    }
    drop((_esbuild, _canary, _log));

    // Unset: the child must not gain the key at all.
    let _unset = Stash::remove("CELLO_ESBUILD");
    let cleared = child_env(project.path());
    assert!(
        !cleared.contains_key(OsStr::new("CELLO_ESBUILD")),
        "an unset CELLO_ESBUILD must not appear in the cello child",
    );
}
