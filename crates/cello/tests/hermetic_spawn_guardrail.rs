// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): hermetic spawn guardrail.
#![allow(clippy::disallowed_methods)]

//! Scans test sources on disk, not node code: it reads the repo's own
//! test files, so the execution boundary's injected filesystem rules do
//! not apply to it.
//!
//! Guardrail: every spawn of the cello binary in `crates/cello/tests/`
//! goes through `common::cello_command` (or `common::apply_hermetic_env`
//! for the one wrapper spawn), which clears the inherited environment and
//! rebuilds it from an explicit allowlist. A test that inherits the
//! developer's shell picks up `CELLO_DEV_STATE`, `RUST_LOG`, ambient proxy
//! or TLS variables — and any stray `CELLD_*`, which makes cello refuse to
//! start. This test scans the test sources so a new spawn that bypasses
//! the helper fails CI instead of flaking later.

use std::path::PathBuf;

/// The binary path macro, split so this file's own source does not contain
/// the needle it scans for.
const BINARY_NEEDLE: &str = concat!("CARGO_BIN_EXE_", "cello");

/// No `tests/*.rs` outside `common/` may name the cello binary: the helper
/// owns that path, so every spawn gets the cleared environment.
#[test]
fn cello_spawns_use_the_hermetic_helper() {
    let tests = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests");
    let own = PathBuf::from(file!())
        .file_name()
        .expect("guardrail file name")
        .to_owned();
    let mut offenders = Vec::new();
    let mut scanned = 0;
    let entries = std::fs::read_dir(&tests).expect("read tests dir");
    for entry in entries {
        let entry = entry.expect("tests entry");
        let path = entry.path();
        if !path.is_file() || !matches!(path.extension().and_then(|ext| ext.to_str()), Some("rs")) {
            continue;
        }
        if path.file_name().expect("test file name") == own {
            continue;
        }
        scanned += 1;
        let source = std::fs::read_to_string(&path).expect("read test source");
        if source.contains(BINARY_NEEDLE) {
            offenders.push(path.file_name().expect("test file name").to_owned());
        }
    }
    assert!(scanned > 0, "expected test sources beside this guardrail");
    assert!(
        offenders.is_empty(),
        "these test files spawn the cello binary without the hermetic helper: \
         {offenders:?}; build the command with common::cello_command \
         (or common::apply_hermetic_env for a wrapper spawn)",
    );
}
