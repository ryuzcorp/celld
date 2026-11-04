// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified by ryuzcorp (ryuzcorp/celld): the full build version for --version.

//! The version `celld --version` reports.
//!
//! `build.rs` bakes `CELLD_VERSION` into the binary when the build sets it
//! (`<version>-ryuzcorp.<n>` on fork releases, `<version>-ryuzcorp.ci` in
//! CI) and falls back to the crate version otherwise, so a plain `cargo
//! build` still reports something sane.

/// The full build version: the fork release name when the build baked one
/// in, else the crate version.
pub fn version() -> &'static str {
    env!("CELLD_VERSION")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The baked version always names the crate release it was built from:
    /// the fallback is the crate version itself, and a fork build extends
    /// it with a `-ryuzcorp.<n>` suffix.
    #[test]
    fn version_names_the_crate_release() {
        assert!(!version().is_empty());
        assert!(
            version().starts_with(env!("CARGO_PKG_VERSION")),
            "the build version must extend the crate version, got {:?}",
            version()
        );
    }
}
