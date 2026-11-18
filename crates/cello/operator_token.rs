// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified by ryuzcorp (ryuzcorp/celld): operator token for the internal listener.

//! The operator token: one shared bearer credential for the internal listener.
//!
//! The internal listener serves the peer protocol and the operator API, and
//! its security model assumes a trusted private network. A host that cannot
//! give tenant code a network without a route to that listener sets
//! `CELLD_OPERATOR_TOKEN` to the same value on every node of the fleet, and
//! every internal route then requires `Authorization: Bearer <token>`.
//! There are no exempt routes: liveness and readiness probes use the public
//! listener's `/.well-known/celld/health`, which this token does not guard.
//!
//! An absent variable selects today's behavior: the internal listener stays
//! open and every client sends no bearer header, so a fleet that never sets
//! the variable interoperates with nodes that do not know it. An empty value
//! refuses to start (or fails the command that reads it) instead of running
//! a node the operator believes is guarded.
//!
//! The token is compared in constant time and is never logged. Every
//! in-tree client of the internal listener — peer forwarding, handoff,
//! abort, the log tier, the signed probe, the operator CLIs, and the `dev`
//! supervisor's shutdown — attaches it through [`authenticate`].

use anyhow::{anyhow, Context};
use subtle::ConstantTimeEq;

/// The environment variable that carries the fleet's operator token.
pub const ENV_VAR: &str = "CELLD_OPERATOR_TOKEN";

/// A parsed operator token. The bearer form is validated once, at parse, so
/// attaching it to a request cannot fail and a token that could never match
/// (line breaks, DEL) refuses the boot with the reason instead of serving
/// a node no caller can authenticate to.
#[derive(Clone)]
pub struct OperatorToken {
    bearer: reqwest::header::HeaderValue,
}

impl std::fmt::Debug for OperatorToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OperatorToken(<redacted>)")
    }
}

impl OperatorToken {
    /// Whether `headers` presents this token as `Authorization: Bearer
    /// <token>`, compared in constant time. The scheme match is exact:
    /// only this one spelling authenticates.
    pub fn verify(&self, headers: &axum::http::HeaderMap) -> bool {
        let Some(presented) = headers.get(axum::http::header::AUTHORIZATION) else {
            return false;
        };
        bool::from(presented.as_bytes().ct_eq(self.bearer.as_bytes()))
    }

    fn header_value(&self) -> reqwest::header::HeaderValue {
        self.bearer.clone()
    }
}

/// Parse one configured value. `None` is the unset variable: no token.
/// Pure so tests can cover every shape without touching the process
/// environment.
pub fn parse(value: Option<&str>) -> anyhow::Result<Option<OperatorToken>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_empty() {
        return Err(anyhow!(
            "{ENV_VAR} is set but empty; unset it for an open internal listener or set a token"
        ));
    }
    // HTTP drops whitespace around a header value on receipt, so a token
    // that starts or ends with a space or tab can never be presented back
    // intact: the node would boot and refuse every caller.
    if value.starts_with([' ', '\t']) || value.ends_with([' ', '\t']) {
        return Err(anyhow!(
            "{ENV_VAR} starts or ends with whitespace, which an Authorization header drops"
        ));
    }
    let bearer = format!("Bearer {value}");
    let bearer = reqwest::header::HeaderValue::from_str(&bearer)
        .with_context(|| format!("{ENV_VAR} cannot travel in an Authorization header"))?;
    Ok(Some(OperatorToken { bearer }))
}

/// Read the token from the environment: the one reader every client and the
/// node startup share, so all of them agree on what refuses and what opens.
pub fn from_env() -> anyhow::Result<Option<OperatorToken>> {
    let Some(value) = std::env::var_os(ENV_VAR) else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| anyhow!("{ENV_VAR} must be UTF-8"))?;
    parse(Some(&value))
}

/// Attach the environment's token to an internal-listener request, when one
/// is configured. Every in-tree client of the internal listener calls this,
/// so a fleet that sets the variable keeps working without per-caller flags.
pub fn authenticate(builder: reqwest::RequestBuilder) -> anyhow::Result<reqwest::RequestBuilder> {
    Ok(authenticate_with(from_env()?.as_ref(), builder))
}

/// Attach one already-read token. Pure so tests can prove the header without
/// touching the process environment.
pub fn authenticate_with(
    token: Option<&OperatorToken>,
    builder: reqwest::RequestBuilder,
) -> reqwest::RequestBuilder {
    match token {
        Some(token) => builder.header(reqwest::header::AUTHORIZATION, token.header_value()),
        None => builder,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(authorization: Option<&str>) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        if let Some(value) = authorization {
            headers.insert(
                axum::http::header::AUTHORIZATION,
                axum::http::HeaderValue::from_str(value).expect("test header"),
            );
        }
        headers
    }

    #[test]
    fn absent_token_is_no_token() {
        assert!(parse(None).expect("parse").is_none());
    }

    #[test]
    fn empty_token_refuses() {
        let error = parse(Some("")).expect_err("empty must refuse");
        assert!(
            error.to_string().contains(ENV_VAR),
            "the refusal names the variable: {error}"
        );
    }

    #[test]
    fn surrounding_whitespace_refuses() {
        for value in ["token ", " token", "token\t", "\ttoken"] {
            let error = parse(Some(value)).expect_err("surrounding whitespace must refuse");
            assert!(error.to_string().contains(ENV_VAR), "{value:?}: {error}");
        }
        assert!(parse(Some("to ken"))
            .expect("inner space travels")
            .is_some());
    }

    #[test]
    fn unshippable_token_refuses() {
        // What cannot travel in a header can never be presented back, so a
        // node configured with it would lock every caller out: refuse the
        // boot with the reason instead.
        for value in ["trailing\n", "carriage\rreturn", "del\x7f"] {
            let error = parse(Some(value)).expect_err("must refuse");
            assert!(
                error.to_string().contains(ENV_VAR),
                "the refusal names the variable for {value:?}: {error}"
            );
        }
    }

    #[test]
    fn shippable_token_parses() {
        // Spaces, tab, and obs-text survive the header round trip
        // byte-identical, and the comparison is exact, so these
        // authenticate. The refusal above stays narrow: only what cannot
        // ship refuses.
        for value in ["has space", "tab\there", "ünïcodé"] {
            assert!(
                parse(Some(value)).expect("parse").is_some(),
                "must accept {value:?}"
            );
        }
    }

    #[test]
    fn right_token_verifies() {
        let token = parse(Some("fleet-secret")).expect("parse").expect("token");
        assert!(token.verify(&headers(Some("Bearer fleet-secret"))));
    }

    #[test]
    fn wrong_token_does_not_verify() {
        let token = parse(Some("fleet-secret")).expect("parse").expect("token");
        for presented in [
            None,
            Some("Bearer wrong"),
            Some("Bearer fleet-secret "),
            Some("Bearer fleet-secre"),
            Some("Bearer fleet-secretx"),
            Some("Bearer"),
            Some("Basic ZmxlZXQtc2VjcmV0"),
            Some("bearer fleet-secret"),
            Some("BEARER fleet-secret"),
        ] {
            assert!(
                !token.verify(&headers(presented)),
                "must reject {presented:?}"
            );
        }
    }

    #[test]
    fn debug_redacts_the_token() {
        let token = parse(Some("fleet-secret")).expect("parse").expect("token");
        let rendered = format!("{token:?}");
        assert!(!rendered.contains("fleet-secret"), "leaks: {rendered}");
        assert!(rendered.contains("redacted"), "unmarked: {rendered}");
    }

    #[test]
    fn authenticate_with_injects_the_bearer_header() {
        let token = parse(Some("fleet-secret")).expect("parse").expect("token");
        let request = authenticate_with(
            Some(&token),
            reqwest::Client::new().get("http://127.0.0.1:9/state"),
        )
        .build()
        .expect("build");
        assert_eq!(
            request.headers().get(reqwest::header::AUTHORIZATION),
            Some(&reqwest::header::HeaderValue::from_static(
                "Bearer fleet-secret"
            )),
            "the peer client must send the token"
        );
    }

    #[test]
    fn authenticate_with_without_a_token_sends_no_header() {
        let request =
            authenticate_with(None, reqwest::Client::new().get("http://127.0.0.1:9/state"))
                .build()
                .expect("build");
        assert!(
            !request
                .headers()
                .contains_key(reqwest::header::AUTHORIZATION),
            "an unset variable keeps today's behavior"
        );
    }
}
