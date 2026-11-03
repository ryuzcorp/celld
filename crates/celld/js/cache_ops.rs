// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): per-node Workers Cache API ops.
// Apache-2.0.

//! The V8 surface over `crate::worker_cache`.
//!
//! Each op converts V8 values to Rust, calls into the store, and converts
//! the answer back. The script name always comes from isolate state, never
//! from an argument: it is the tenant boundary, and script input must not
//! be able to name another script's entries.
use super::storage_ops::actor_runtime_state;
use super::*;
use crate::worker_cache;

fn throw_cache_error(scope: &mut v8::PinScope, operation: &str, error: impl std::fmt::Display) {
    let message = v8::String::new(scope, &format!("cache.{operation}: {error}")).unwrap();
    let exception = v8::Exception::error(scope, message);
    scope.throw_exception(exception);
}

fn throw_cache_type_error(scope: &mut v8::PinScope, message: &str) {
    let message = v8::String::new(scope, message).unwrap();
    scope.throw_exception(v8::Exception::type_error(scope, message));
}

fn header_pairs(json: &str) -> Option<Vec<(String, String)>> {
    serde_json::from_str(json).ok()
}

/// `__cache_put(namespace, url, method, status, headersJson, body)`:
/// store one response, answering whether it stuck. `namespace` is the word
/// the `cache.js` handle passes (`default`, or `named:{name}`).
pub(super) fn op_cache_put(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let namespace = worker_cache::Namespace::parse(&args.get(0).to_rust_string_lossy(scope));
    let Some(namespace) = namespace else {
        throw_cache_error(scope, "put", "namespace is malformed");
        return;
    };
    let url = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let status = args.get(3).uint32_value(scope).unwrap_or(0);
    let Ok(status) = u16::try_from(status) else {
        throw_cache_type_error(scope, "cache.put: status is out of range");
        return;
    };
    if !(200..=599).contains(&status) {
        throw_cache_type_error(scope, "cache.put: status is out of range");
        return;
    }
    let headers_json = args.get(4).to_rust_string_lossy(scope);
    let Some(headers) = header_pairs(&headers_json) else {
        throw_cache_error(scope, "put", "headers are malformed");
        return;
    };
    let Some(body) = view_bytes(args.get(5)) else {
        throw_cache_error(scope, "put", "body must be bytes");
        return;
    };
    let script = actor_runtime_state(scope).script_name.clone();
    let request = worker_cache::PutRequest {
        method: &method,
        url: &url,
        status,
        headers: &headers,
        body: &body,
    };
    match worker_cache::cache_put(&script, &namespace, &request, asyncrt::wall_ms()) {
        Ok(worker_cache::PutOutcome::Stored) => rv.set(v8::Boolean::new(scope, true).into()),
        Ok(worker_cache::PutOutcome::NotStored) => rv.set(v8::Boolean::new(scope, false).into()),
        Err(rejection) => throw_cache_type_error(scope, rejection.message()),
    }
}

/// `__cache_match(namespace, url, method, ignoreMethod, headersJson)`:
/// look up one request, answering `null` on a miss or
/// `[status, headersJson, body | null]` on a hit.
pub(super) fn op_cache_match(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let namespace = worker_cache::Namespace::parse(&args.get(0).to_rust_string_lossy(scope));
    let Some(namespace) = namespace else {
        throw_cache_error(scope, "match", "namespace is malformed");
        return;
    };
    let url = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let ignore_method = args.get(3).boolean_value(scope);
    let headers_json = args.get(4).to_rust_string_lossy(scope);
    let Some(request_headers) = header_pairs(&headers_json) else {
        throw_cache_error(scope, "match", "headers are malformed");
        return;
    };
    let script = actor_runtime_state(scope).script_name.clone();
    let request = worker_cache::LookupRequest {
        url: &url,
        method: &method,
        ignore_method,
        request_headers: &request_headers,
    };
    let outcome = worker_cache::cache_lookup(&script, &namespace, &request, asyncrt::wall_ms());
    let worker_cache::MatchOutcome::Hit {
        status,
        headers,
        body,
    } = outcome
    else {
        rv.set(v8::null(scope).into());
        return;
    };
    let status = v8::Integer::new(scope, status as i32).into();
    let headers = v8::String::new(scope, &serde_json::to_string(&headers).unwrap_or_default())
        .map(v8::Local::<v8::Value>::from)
        .unwrap_or_else(|| v8::null(scope).into());
    let body = body
        .map(|bytes| bytes_value(scope, bytes))
        .unwrap_or_else(|| v8::null(scope).into());
    rv.set(v8::Array::new_with_elements(scope, &[status, headers, body]).into());
}

/// `__cache_delete(namespace, url, method, ignoreMethod)`: delete one
/// entry, answering whether one was there.
pub(super) fn op_cache_delete(
    scope: &mut v8::PinScope,
    args: v8::FunctionCallbackArguments,
    mut rv: v8::ReturnValue<v8::Value>,
) {
    let namespace = worker_cache::Namespace::parse(&args.get(0).to_rust_string_lossy(scope));
    let Some(namespace) = namespace else {
        throw_cache_error(scope, "delete", "namespace is malformed");
        return;
    };
    let url = args.get(1).to_rust_string_lossy(scope);
    let method = args.get(2).to_rust_string_lossy(scope);
    let ignore_method = args.get(3).boolean_value(scope);
    let script = actor_runtime_state(scope).script_name.clone();
    let deleted = worker_cache::cache_delete(
        &script,
        &namespace,
        &url,
        &method,
        ignore_method,
        asyncrt::wall_ms(),
    );
    rv.set(v8::Boolean::new(scope, deleted).into());
}
