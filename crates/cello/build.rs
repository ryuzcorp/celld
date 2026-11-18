// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified by ryuzcorp (ryuzcorp/celld): bake CELLD_VERSION into the binary for --version.

//! Bake the full build version into `CELLD_VERSION` for `version::version`.
//!
//! Release builds set `CELLD_VERSION` (`<version>-ryuzcorp.<n>`, and
//! `<version>-ryuzcorp.ci` in CI); a plain `cargo build` leaves it unset
//! and the binary reports the crate version instead.

fn main() {
    // `println!` is forbidden workspace-wide, so the cargo directives go
    // through an explicit stdout handle instead.
    let mut stdout = std::io::stdout();
    use std::io::Write as _;
    writeln!(stdout, "cargo:rerun-if-env-changed=CELLD_VERSION")
        .expect("write the rerun directive");
    let version = std::env::var("CELLD_VERSION")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            std::env::var("CARGO_PKG_VERSION").expect("cargo always sets CARGO_PKG_VERSION")
        });
    // The value lands in a cargo directive line, so a newline would let it
    // inject directives of its own (link args, env, rerun lies). Versions
    // are SemVer-shaped; refuse anything else outright.
    assert!(
        !version.is_empty()
            && version
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric()
                    || matches!(byte, b'.' | b'-' | b'+' | b'_')),
        "CELLD_VERSION must contain only [0-9A-Za-z._+-], got {version:?}"
    );
    writeln!(stdout, "cargo:rustc-env=CELLD_VERSION={version}").expect("write the version");
}
