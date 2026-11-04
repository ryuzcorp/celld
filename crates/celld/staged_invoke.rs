// Modified by ryuzcorp (ryuzcorp/celld): staged invoke serves one operator request from staged versions. Apache-2.0.

//! Operator-only HTTP invocation of staged script versions.
//!
//! A version uploaded with `celld deploy --no-activate` serves no traffic
//! until `celld activate` names it, so an operator cannot migrate a live
//! database to the staged schema before the flip: the migration endpoint
//! only exists in code nothing serves yet. This module closes that window:
//! it builds an ephemeral shadow [`Generation`](crate::generation::Generation)
//! from staged pins overlaid on the live map, serves one request through
//! it, then retires it. `POST /invoke` authenticates the caller (the
//! operator token plus the fleet HMAC, like `/runtime/`) and this module
//! does the rest.
//!
//! The shadow is never adopted and nothing serving reads it. It parks in
//! the draining list only so the staged isolates' generation tag resolves
//! — without that, a service call from staged code would fall through to
//! the live generation — and a [`ShadowGuard`](crate::cell_runtime::ShadowGuard)
//! retires it before unregistering it once every driver on it has settled,
//! past the reply, through `waitUntil`. Cells always serve from the live
//! generation (`start_cell` resolves the current one), so D1, KV, Queue,
//! and Durable Object bindings reach the real data. The invoke writes
//! nothing to the bucket: the pointer, the staged versions, and what
//! serves public traffic are untouched.

use std::collections::BTreeMap;
use std::str::FromStr as _;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use base64::Engine as _;
use futures_util::StreamExt;

use crate::bucket::Bucket;
use crate::deploy::VersionRef;
use crate::generation::{DeploymentGraph, Generation, GenerationOptions, ServingIdentity};
use crate::runtime::RuntimeManager;

/// What an invoke can carry back: the staged worker's status, headers, and
/// body bytes. Buffered, so a streaming response is drained against
/// [`MAX_INVOKE_RESPONSE_BYTES`] rather than proxied.
pub struct StagedResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// The largest staged-worker response body an invoke buffers: a streamed
/// answer is drained, not proxied, so it needs a bound. The peer protocol
/// body cap, which the outer request already observes.
const MAX_INVOKE_RESPONSE_BYTES: usize = 64 << 20;

/// The most staged pins one invoke may name. Each pin is a bucket read
/// made while the invoke holds one of the node's few permits; a real
/// deployment pins a handful of cohosted scripts.
const MAX_INVOKE_PINS: usize = 64;

/// One validated invoke: the staged versions to serve plus the HTTP
/// request to deliver to the target's `fetch` handler.
#[derive(Debug)]
pub struct StagedRequest {
    target: VersionRef,
    pins: BTreeMap<String, String>,
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl StagedRequest {
    /// The script receiving the request.
    pub fn target(&self) -> &VersionRef {
        &self.target
    }
}

/// Why an invoke was refused or failed, with the HTTP status that carries it.
#[derive(Debug)]
pub enum InvokeError {
    /// The envelope or inner request is malformed. 400.
    Malformed(String),
    /// The inner body exceeds the node's ingress body limit. 413.
    BodyTooLarge { limit: usize },
    /// A named script version is not staged in the fleet bucket. 404.
    UnknownVersion { script: String, version: String },
    /// The fleet pointer moved while the invoke loaded its graph: a
    /// concurrent activation. The invoke ran nothing; retry it. 409.
    Conflict(String),
    /// The invoke failed after validation: a carried load, the build, the
    /// dispatch, or the response. 500.
    Failed(anyhow::Error),
}

impl InvokeError {
    pub fn failed(error: anyhow::Error) -> Self {
        Self::Failed(error)
    }

    pub fn status_code(&self) -> u16 {
        match self {
            Self::Malformed(_) => 400,
            Self::BodyTooLarge { .. } => 413,
            Self::UnknownVersion { .. } => 404,
            Self::Conflict(_) => 409,
            Self::Failed(_) => 500,
        }
    }

    fn message(&self) -> String {
        match self {
            Self::Malformed(message) | Self::Conflict(message) => message.clone(),
            Self::BodyTooLarge { limit } => {
                format!("invoke request body exceeds the node's {limit}-byte ingress limit")
            }
            Self::UnknownVersion { script, version } => {
                format!("no staged version {version} of script {script:?} in the fleet bucket")
            }
            Self::Failed(error) => format!("{error:#}"),
        }
    }

    /// The `{"error": ...}` reply, JSON-encoded so an operator-supplied
    /// name cannot break out of the string.
    pub fn error_json(&self) -> String {
        serde_json::json!({ "error": self.message() }).to_string()
    }
}

impl StagedResponse {
    /// The `{"result": ...}` reply, shaped as `/runtime/` shapes its own
    /// so the operator clients decode one answer. The body travels base64:
    /// the worker's bytes cross exactly, whatever they hold.
    pub fn result_json(&self) -> String {
        serde_json::json!({
            "result": {
                "status": self.status,
                "headers": self.headers,
                "body": base64::engine::general_purpose::STANDARD.encode(&self.body),
            }
        })
        .to_string()
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    target: Ref,
    #[serde(default)]
    pins: BTreeMap<String, String>,
    request: InnerRequest,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Ref {
    script: String,
    version: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct InnerRequest {
    method: String,
    path: String,
    #[serde(default)]
    headers: Vec<(String, String)>,
    #[serde(default)]
    body: String,
}

/// Refuse an inner request header that must never come from the
/// operator: the `x-cells-*` control-plane namespace, which peers set
/// and negotiate among themselves, and the hop-by-hop framing headers,
/// which are meaningless on a request object and would only confuse the
/// worker reading them. Everything else passes through as data; the
/// worker's answer carries its own headers back the same way.
pub(crate) fn check_inner_header(name: &str) -> Result<(), &'static str> {
    let lower = name.to_ascii_lowercase();
    if lower.starts_with("x-cells-") {
        return Err("the x-cells-* headers are the control plane's, not the caller's");
    }
    if matches!(
        lower.as_str(),
        "connection" | "keep-alive" | "transfer-encoding" | "upgrade" | "trailer" | "te"
    ) {
        return Err("hop-by-hop framing headers have no meaning on an inner request");
    }
    Ok(())
}

/// Parse and validate one `POST /invoke` body. Every shape is checked
/// before any bucket key is built from it, so a malformed envelope fails
/// here rather than as a bucket read.
pub fn parse_envelope(body: &[u8], max_body_bytes: usize) -> Result<StagedRequest, InvokeError> {
    let malformed = InvokeError::Malformed;
    let envelope: Envelope = serde_json::from_slice(body)
        .map_err(|error| malformed(format!("invalid invoke envelope: {error}")))?;
    if !crate::deploy::valid_script_name(&envelope.target.script) {
        return Err(malformed(format!(
            "invalid script name {:?}: lowercase ASCII letters, digits, and internal hyphens",
            envelope.target.script
        )));
    }
    if !crate::protocol::valid_deployment_version(&envelope.target.version) {
        return Err(malformed(format!(
            "invalid version {:?}: a deployment version as `celld deploy` printed it, 16 lowercase hex characters",
            envelope.target.version
        )));
    }
    if envelope.pins.len() > MAX_INVOKE_PINS {
        return Err(malformed(format!(
            "too many pins: {} (at most {MAX_INVOKE_PINS})",
            envelope.pins.len()
        )));
    }
    for (script, version) in &envelope.pins {
        if !crate::deploy::valid_script_name(script) {
            return Err(malformed(format!(
                "invalid pin script name {script:?}: lowercase ASCII letters, digits, and internal hyphens"
            )));
        }
        if !crate::protocol::valid_deployment_version(version) {
            return Err(malformed(format!(
                "invalid pin version {version:?} for script {script:?}: 16 lowercase hex characters"
            )));
        }
        if script == &envelope.target.script {
            return Err(malformed(format!(
                "pin for {:?} names the target twice; pass the target once",
                envelope.target.script
            )));
        }
    }
    if axum::http::Method::from_str(&envelope.request.method).is_err() {
        return Err(malformed(format!(
            "invalid method {:?}",
            envelope.request.method
        )));
    }
    if !envelope.request.path.starts_with('/') {
        return Err(malformed(format!(
            "invalid path {:?}: an origin-form path starting with '/'",
            envelope.request.path
        )));
    }
    // Hyper refuses a request target with ASCII whitespace or controls,
    // so a normal request can never carry one: the invoke envelope
    // refuses them too, rather than delivering a URL no ingress could.
    if envelope
        .request
        .path
        .bytes()
        .any(|b| b <= 0x20 || b == 0x7f)
    {
        return Err(malformed(format!(
            "invalid path {:?}: no ASCII whitespace or controls",
            envelope.request.path
        )));
    }
    let url = format!("http://{}{}", envelope.target.script, envelope.request.path);
    if url::Url::parse(&url).is_err() {
        return Err(malformed(format!(
            "invalid path {:?}: not a valid request URL",
            envelope.request.path
        )));
    }
    let mut headers = Vec::with_capacity(envelope.request.headers.len());
    for (name, value) in envelope.request.headers {
        if axum::http::HeaderName::from_str(&name).is_err() {
            return Err(malformed(format!("invalid header name {name:?}")));
        }
        if let Err(reason) = check_inner_header(&name) {
            return Err(malformed(format!("invalid header name {name:?}: {reason}")));
        }
        if axum::http::HeaderValue::from_str(&value).is_err() {
            return Err(malformed(format!("invalid value for header {name:?}")));
        }
        headers.push((name, value));
    }
    let body = base64::engine::general_purpose::STANDARD
        .decode(envelope.request.body.as_bytes())
        .map_err(|error| malformed(format!("invalid base64 request body: {error}")))?;
    if body.len() > max_body_bytes {
        return Err(InvokeError::BodyTooLarge {
            limit: max_body_bytes,
        });
    }
    Ok(StagedRequest {
        target: VersionRef {
            script: envelope.target.script,
            version: envelope.target.version,
        },
        pins: envelope.pins,
        method: envelope.request.method,
        path: envelope.request.path,
        headers,
        body,
    })
}

/// Serve one validated invoke on `runtime` from `bucket`: build the shadow
/// generation, dispatch through its target pool past `waitUntil`, and
/// retire it. Reads the bucket, writes nothing: the pointer never moves.
///
/// The caller holds no generation lock and awaits the whole invoke: the
/// shadow guard lives in this future, so it outlives every driver on the
/// shadow. The HTTP layer runs this in a detached task, so a caller that
/// stops waiting cannot retire the shadow under a running driver.
pub async fn invoke(
    runtime: &RuntimeManager,
    bucket: &Bucket,
    request: StagedRequest,
) -> Result<StagedResponse, InvokeError> {
    let StagedRequest {
        target,
        pins,
        method,
        path,
        headers,
        body,
    } = request;
    // Unknown before unbuildable: a manifest read proves the operator
    // named something staged, so a typo answers 404 rather than a load
    // error. Unreachable pins are checked too — ignoring them silently
    // would hide the same typo one request later.
    for (script, version) in std::iter::once((&target.script, &target.version)).chain(pins.iter()) {
        let key = format!("deploy/{script}/{version}/manifest.json");
        let staged = bucket
            .get(&key)
            .await
            .with_context(|| format!("read staged manifest {key}"))
            .map_err(InvokeError::failed)?;
        if staged.is_none() {
            return Err(InvokeError::UnknownVersion {
                script: script.clone(),
                version: version.clone(),
            });
        }
    }
    let pointer = crate::fleet::read_current_pointer(bucket)
        .await
        .map_err(InvokeError::failed)?;
    let before = ServingIdentity::of_pointer(&pointer);
    // The node's own identity, read from what it serves rather than from a
    // parameter that could disagree with it.
    let serving = runtime.generation();
    let node = serving.stateless.node.to_string();
    let region = serving.stateless.region.to_string();
    let mut graph = DeploymentGraph::load_staged(bucket, node.clone(), &pointer, &target, &pins)
        .await
        .map_err(InvokeError::failed)?;
    // An activation that lands between the reads above would serve the
    // staged pins beside a generation they were never validated with.
    // The load ran nothing, so refusing is free and retrying is safe.
    let after = crate::fleet::read_current_pointer(bucket)
        .await
        .map_err(InvokeError::failed)?;
    if !before.names(&after) {
        return Err(InvokeError::Conflict(
            "the fleet pointer moved while the invoke loaded its graph; retry the invoke"
                .to_string(),
        ));
    }
    let mut shadow = runtime.reserve_shadow();
    let id = shadow.id();
    // The shadow's cache scope is its own, per invoke: salting the fleet
    // with the reserved id keeps it apart from the live deployment's scope
    // even when the invoke targets the live versions, and the guard purges
    // it when the shadow retires.
    graph.fleet = format!("{}#staged-invoke:{id}", graph.fleet);
    let built = crate::asyncrt::blocking(move || {
        Generation::build(
            id,
            graph,
            GenerationOptions {
                node: node.clone(),
                region: region.clone(),
            },
        )
    })
    .await;
    let generation = built
        .map_err(|error| InvokeError::failed(anyhow!("shadow generation build panicked: {error}")))?
        .map_err(InvokeError::failed)?;
    let generation = Arc::new(generation);
    shadow.install(generation.clone());
    let pool = generation.service(&target.script).ok_or_else(|| {
        InvokeError::failed(anyhow!(
            "shadow generation has no service Worker for target script {:?}",
            target.script
        ))
    })?;
    // A stateless lifetime with no publisher: nothing cancels the invoke
    // short of shutdown and the settle deadline, so a caller that stops
    // waiting cannot leave a half-applied migration behind. The handler
    // budget bounds the response, exactly as for a normal request, and a
    // further budget bounds the settle wait past it: hung background work
    // fails the invoke instead of pinning the shadow forever.
    let cancellation = crate::runtime::RequestCancellationLifetime::stateless();
    let url = format!("http://{}{}", target.script, path);
    let reply = pool
        .fetch_settled(
            url,
            method,
            crate::js::RequestBody::Bytes(body.into()),
            headers,
            cancellation,
            crate::engine_api::handler_budget(),
        )
        .await
        .map_err(InvokeError::failed)?;
    let response = collect_response(reply).await.map_err(InvokeError::failed)?;
    // Retired before unregistered, by the guard: every driver on the
    // shadow has settled, so nothing still resolves its tag.
    drop(shadow);
    Ok(response)
}

/// Buffer one staged-worker reply into the envelope shape. A streamed body
/// is drained against the cap; an upgrade is refused, because an operator
/// invoke is a plain request with a plain answer.
async fn collect_response(
    mut reply: crate::engine_api::HttpResponse,
) -> anyhow::Result<StagedResponse> {
    if reply.websocket.is_some() {
        anyhow::bail!(
            "the staged version answered with a WebSocket upgrade, which invoke does not serve"
        );
    }
    let mut body = std::mem::take(&mut reply.body);
    // A buffered answer is bounded the same as a streamed one.
    if body.len() > MAX_INVOKE_RESPONSE_BYTES {
        anyhow::bail!(
            "staged response body exceeds the {MAX_INVOKE_RESPONSE_BYTES}-byte invoke limit"
        );
    }
    if let Some(mut stream) = reply.stream.take() {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| anyhow!("staged response stream failed: {error}"))?;
            body.extend_from_slice(&chunk);
            if body.len() > MAX_INVOKE_RESPONSE_BYTES {
                anyhow::bail!(
                    "staged response body exceeds the {MAX_INVOKE_RESPONSE_BYTES}-byte invoke limit"
                );
            }
        }
    }
    Ok(StagedResponse {
        status: reply.status,
        headers: reply.headers,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(target: &str, version: &str, path: &str) -> serde_json::Value {
        serde_json::json!({
            "target": { "script": target, "version": version },
            "request": { "method": "POST", "path": path },
        })
    }

    fn parse(value: &serde_json::Value) -> Result<StagedRequest, InvokeError> {
        parse_envelope(value.to_string().as_bytes(), 1024)
    }

    fn message(error: &InvokeError) -> String {
        match error {
            InvokeError::Malformed(message) => message.clone(),
            other => panic!("want Malformed, got status {}", other.status_code()),
        }
    }

    #[test]
    fn envelope_accepts_a_minimal_request() {
        let parsed = parse(&envelope("app", "0123456789abcdef", "/migrate")).expect("parse");
        assert_eq!(parsed.target.script, "app");
        assert_eq!(parsed.target.version, "0123456789abcdef");
        assert_eq!(parsed.method, "POST");
        assert_eq!(parsed.path, "/migrate");
        assert!(parsed.headers.is_empty());
        assert!(parsed.body.is_empty());
        assert!(parsed.pins.is_empty());
    }

    /// The pin count is bounded before any pin is read: at the cap the
    /// envelope parses, one past it is a 400.
    #[test]
    fn envelope_refuses_more_pins_than_the_cap() {
        let with_pins = |count: usize| {
            let pins: serde_json::Map<String, serde_json::Value> = (0..count)
                .map(|n| (format!("s{n}"), serde_json::json!("fedcba9876543210")))
                .collect();
            let mut value = envelope("app", "0123456789abcdef", "/migrate");
            value["pins"] = serde_json::Value::Object(pins);
            parse_envelope(value.to_string().as_bytes(), 1 << 20)
        };
        assert_eq!(
            with_pins(MAX_INVOKE_PINS).expect("at the cap").pins.len(),
            MAX_INVOKE_PINS
        );
        let refused = with_pins(MAX_INVOKE_PINS + 1).expect_err("past the cap");
        assert!(
            message(&refused).contains("too many pins"),
            "{}",
            message(&refused)
        );
    }

    #[test]
    fn envelope_carries_pins_headers_and_body() {
        let value = serde_json::json!({
            "target": { "script": "app", "version": "0123456789abcdef" },
            "pins": { "helper": "fedcba9876543210" },
            "request": {
                "method": "PUT",
                "path": "/migrate?step=2",
                "headers": [["authorization", "Bearer x"], ["x-tenant", "t1"]],
                "body": base64::engine::general_purpose::STANDARD.encode(b"{}"),
            },
        });
        let parsed = parse(&value).expect("parse");
        assert_eq!(
            parsed.pins.get("helper").map(String::as_str),
            Some("fedcba9876543210")
        );
        assert_eq!(
            parsed.headers,
            vec![
                ("authorization".to_string(), "Bearer x".to_string()),
                ("x-tenant".to_string(), "t1".to_string()),
            ]
        );
        assert_eq!(parsed.body, b"{}");
    }

    #[test]
    fn envelope_refuses_unknown_fields_and_bad_shapes() {
        for (name, value) in [
            (
                "unknown top-level field",
                serde_json::json!({
                    "target": { "script": "app", "version": "0123456789abcdef" },
                    "request": { "method": "GET", "path": "/" },
                    "debug": true,
                }),
            ),
            (
                "unknown target field",
                serde_json::json!({
                    "target": { "script": "app", "version": "0123456789abcdef", "prefix": "x" },
                    "request": { "method": "GET", "path": "/" },
                }),
            ),
            ("uppercase script", envelope("App", "0123456789abcdef", "/")),
            ("empty script", envelope("", "0123456789abcdef", "/")),
            (
                "path traversal script",
                envelope("../x", "0123456789abcdef", "/"),
            ),
            ("short version", envelope("app", "0123456789abcde", "/")),
            (
                "uppercase version",
                envelope("app", "0123456789ABCDEF", "/"),
            ),
            ("non-hex version", envelope("app", "0123456789abcdeg", "/")),
            (
                "empty method",
                serde_json::json!({
                    "target": { "script": "app", "version": "0123456789abcdef" },
                    "request": { "method": "", "path": "/" },
                }),
            ),
            (
                "method with a space",
                serde_json::json!({
                    "target": { "script": "app", "version": "0123456789abcdef" },
                    "request": { "method": "GE T", "path": "/" },
                }),
            ),
            (
                "absolute-form path",
                envelope("app", "0123456789abcdef", "http://x/"),
            ),
            (
                "path with a space",
                envelope("app", "0123456789abcdef", "/mig rate"),
            ),
            (
                "bad header name",
                serde_json::json!({
                    "target": { "script": "app", "version": "0123456789abcdef" },
                    "request": { "method": "GET", "path": "/", "headers": [["a b", "c"]] },
                }),
            ),
            (
                "bad header value",
                serde_json::json!({
                    "target": { "script": "app", "version": "0123456789abcdef" },
                    "request": { "method": "GET", "path": "/", "headers": [["a", "b\nc"]] },
                }),
            ),
            (
                "control-plane header",
                serde_json::json!({
                    "target": { "script": "app", "version": "0123456789abcdef" },
                    "request": { "method": "GET", "path": "/", "headers": [["x-cells-operator", "x"]] },
                }),
            ),
            (
                "uppercase control-plane header",
                serde_json::json!({
                    "target": { "script": "app", "version": "0123456789abcdef" },
                    "request": { "method": "GET", "path": "/", "headers": [["X-CELLS-PEER-SOURCE", "x"]] },
                }),
            ),
            (
                "framing header",
                serde_json::json!({
                    "target": { "script": "app", "version": "0123456789abcdef" },
                    "request": { "method": "GET", "path": "/", "headers": [["connection", "close"]] },
                }),
            ),
            (
                "bad base64 body",
                serde_json::json!({
                    "target": { "script": "app", "version": "0123456789abcdef" },
                    "request": { "method": "POST", "path": "/", "body": "!!!" },
                }),
            ),
        ] {
            let error = parse(&value).expect_err(name);
            assert_eq!(error.status_code(), 400, "{name}");
            assert!(!message(&error).is_empty(), "{name}");
        }
    }

    #[test]
    fn envelope_refuses_a_pin_naming_the_target() {
        let value = serde_json::json!({
            "target": { "script": "app", "version": "0123456789abcdef" },
            "pins": { "app": "fedcba9876543210" },
            "request": { "method": "GET", "path": "/" },
        });
        let error = parse(&value).expect_err("pin names target");
        assert_eq!(error.status_code(), 400);
    }

    #[test]
    fn envelope_refuses_an_oversized_body() {
        let value = serde_json::json!({
            "target": { "script": "app", "version": "0123456789abcdef" },
            "request": {
                "method": "POST",
                "path": "/",
                "body": base64::engine::general_purpose::STANDARD.encode(vec![0u8; 1025]),
            },
        });
        let error = parse(&value).expect_err("oversized body");
        assert_eq!(error.status_code(), 413);
    }

    #[test]
    fn envelope_rejects_invalid_json() {
        let error = parse_envelope(b"{not json", 1024).expect_err("invalid JSON");
        assert_eq!(error.status_code(), 400);
    }

    #[test]
    fn error_statuses_cover_every_variant() {
        assert_eq!(InvokeError::Malformed("x".to_string()).status_code(), 400);
        assert_eq!(InvokeError::BodyTooLarge { limit: 1 }.status_code(), 413);
        assert_eq!(
            InvokeError::UnknownVersion {
                script: "s".to_string(),
                version: "v".to_string(),
            }
            .status_code(),
            404
        );
        assert_eq!(InvokeError::Conflict("x".to_string()).status_code(), 409);
        assert_eq!(InvokeError::failed(anyhow!("x")).status_code(), 500);
    }

    #[test]
    fn error_json_escapes_operator_input() {
        let error = InvokeError::UnknownVersion {
            script: "a\"b".to_string(),
            version: "c\nd".to_string(),
        };
        let value: serde_json::Value =
            serde_json::from_str(&error.error_json()).expect("error JSON parses");
        assert_eq!(
            value.get("error").and_then(serde_json::Value::as_str),
            Some("no staged version c\nd of script \"a\\\"b\" in the fleet bucket")
        );
    }

    #[test]
    fn result_json_carries_bytes_exactly() {
        let response = StagedResponse {
            status: 201,
            headers: vec![("content-type".to_string(), "application/json".to_string())],
            body: vec![0, 159, 146, 150],
        };
        let value: serde_json::Value =
            serde_json::from_str(&response.result_json()).expect("result JSON parses");
        let result = value.get("result").expect("result");
        assert_eq!(
            result.get("status").and_then(serde_json::Value::as_u64),
            Some(201)
        );
        let body = result
            .get("body")
            .and_then(serde_json::Value::as_str)
            .expect("body");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(body)
            .expect("body decodes");
        assert_eq!(decoded, vec![0, 159, 146, 150]);
    }
}
