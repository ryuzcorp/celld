// Modified by ryuzcorp (ryuzcorp/celld): shared TLS trust roots (Mozilla plus OS store/SSL_CERT_FILE) for worker sockets and tenant fetch (upstream #230).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The trust roots for every outbound TLS client celld builds for tenant
//! code. `fetch()` honored the OS store and `SSL_CERT_FILE` while
//! `cloudflare:sockets` and outbound WebSockets verified against the Mozilla
//! roots only, so a private-CA database driver failed over `startTls()` where
//! `fetch()` to the same host succeeded (denoland/celld#230). Every tenant
//! TLS client now builds from this one store: Mozilla's roots plus whatever
//! the platform store (and `SSL_CERT_FILE`/`SSL_CERT_DIR`) holds.
//!
//! The native set loads once per process. A host with no readable store
//! contributes nothing, and Mozilla's roots still verify public CAs, so a
//! store failure never newly fails a handshake — it only forgoes the private
//! roots this module exists to trust.

use std::sync::OnceLock;

/// Read the platform store now, without the process cache. The cached
/// accessors below call this once; tests call it directly so each case can
/// point `SSL_CERT_FILE` at its own fixture.
fn load_native_der() -> Vec<Vec<u8>> {
    let result = rustls_native_certs::load_native_certs();
    // A corrupt entry must not poison the rest of the store: keep the
    // certificates that parsed and count what did not, the same way the
    // loader itself separates `certs` from `errors`.
    if !result.errors.is_empty() {
        tracing::debug!(
            errors = result.errors.len(),
            "native certificate store had unreadable entries; trusting the rest"
        );
    }
    result
        .certs
        .into_iter()
        .map(|cert| cert.as_ref().to_vec())
        .collect()
}

fn cached_native_der() -> &'static Vec<Vec<u8>> {
    static NATIVE: OnceLock<Vec<Vec<u8>>> = OnceLock::new();
    NATIVE.get_or_init(load_native_der)
}

/// The DER bytes of every native trust anchor, for clients that cannot take
/// a `RootCertStore` (reqwest's tenant builders add them one certificate at
/// a time).
pub fn native_der() -> &'static [Vec<u8>] {
    cached_native_der()
}

/// Mozilla's roots plus the platform store, for the rustls clients (worker
/// TCP sockets, outbound WebSockets). Duplicates are harmless: adding a root
/// the store already holds fails, and the failure is ignored.
pub fn root_store() -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    for der in cached_native_der() {
        let _ = roots.add(rustls::pki_types::CertificateDer::from(der.clone()));
    }
    roots
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A self-signed CA fixture: `SSL_CERT_FILE` pointed here must surface
    /// its DER through the loader, which is the whole of the private-CA fix.
    const TEST_CA_PEM: &str = include_str!("testdata/test-ca-230.pem");

    fn test_ca_der() -> Vec<u8> {
        let mut reader = std::io::BufReader::new(TEST_CA_PEM.as_bytes());
        let cert = rustls_pemfile::certs(&mut reader)
            .next()
            .expect("fixture holds a certificate")
            .expect("fixture certificate parses");
        cert.to_vec()
    }

    #[test]
    fn native_loader_honors_ssl_cert_file() {
        use std::io::Write as _;
        // A named temp file, written through `io::Write` rather than the
        // banned `std::fs` helpers: the test needs a real path for
        // `SSL_CERT_FILE` but must not touch node storage.
        let mut ca = tempfile::NamedTempFile::new().expect("test ca file");
        ca.write_all(TEST_CA_PEM.as_bytes())
            .expect("write fixture");
        let previous = std::env::var_os("SSL_CERT_FILE");
        std::env::set_var("SSL_CERT_FILE", ca.path());
        let loaded = load_native_der();
        match previous {
            Some(value) => std::env::set_var("SSL_CERT_FILE", value),
            None => std::env::remove_var("SSL_CERT_FILE"),
        }
        assert!(
            loaded.contains(&test_ca_der()),
            "the loader must surface the SSL_CERT_FILE anchor"
        );
    }

    #[test]
    fn root_store_holds_mozilla_anchors() {
        // Mozilla's set is large; the store must never come back with only
        // the (possibly empty) native set inside it.
        assert!(root_store().roots.len() > 100);
    }
}
