// Modified by Ryuz Corp (Cello): validate CELLO_MEMORY_SCOPE; CELLO_CACHE_MAX_BYTES validation; tenant egress policy; CELLO_CACHE_SCOPE validation; CELLO_DEPLOY_AWS_* validation; CELLO_SQLITE_CACHE_KB validation; CELLO_C_TRIM_MB validation; CELLO_BUCKET_BUNDLES startup validation (0070); CELLO_BUCKET_BUNDLES defaults off (0075); CELLO_TMP_MAX_BYTES validation (upstream #280); CELLO_SQL_STATEMENT_MS validation (upstream #274); CELLO_MAX_CELLS_PER_ISOLATE validation (upstream #278).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Strict runtime environment parsing.
//!
//! An unset variable selects its caller's documented default. A supplied
//! variable must contain a valid value, so a typo cannot silently change the
//! configuration of a running node.

use anyhow::{anyhow, bail};
use std::ffi::OsString;

pub const DEFAULT_SHUTDOWN_TOTAL_MS: u64 = 40_000;

/// Shared by deployment assembly, managed deployment validation and serving
/// nodes. Operators must configure the same policy on all three paths.
pub fn max_asset_file_bytes() -> anyhow::Result<u64> {
    // A manifest can contain thousands of entries. Cache the policy so each
    // entry neither allocates an environment string nor observes a new limit.
    // Keep parse failures fallible for library callers that skip startup validation.
    static LIMIT: std::sync::OnceLock<Result<u64, String>> = std::sync::OnceLock::new();
    LIMIT
        .get_or_init(|| {
            positive_or("CELLO_MAX_ASSET_FILE_BYTES", 25 * 1024 * 1024)
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .copied()
        .map_err(|message| anyhow!("{message}"))
}

/// One process stop budget and the internal waits derived from it.
/// The absolute deadline also bounds progress extensions, so neither wait
/// grants time beyond the supervisor-facing process budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownTiming {
    pub total_ms: u64,
    pub drain_token_wait_ms: u64,
    pub drain_no_progress_ms: u64,
}

impl ShutdownTiming {
    fn from_total_ms(total_ms: u64) -> Self {
        // Preserve the 30-second token wait and 25-second no-progress window
        // at the default 40-second bound. Derive both when that bound changes:
        // independent defaults made a shorter supervisor grace fail startup.
        // Split the arithmetic so even the largest accepted u64 cannot wrap.
        let drain_token_wait_ms = (total_ms / 4) * 3 + (total_ms % 4) * 3 / 4;
        let drain_no_progress_ms = ((total_ms / 8) * 5 + (total_ms % 8) * 5 / 8).max(1);
        Self {
            total_ms,
            drain_token_wait_ms,
            drain_no_progress_ms,
        }
    }
}

/// A variable cello no longer reads, and the sentence that names what an
/// operator must write instead.
///
/// The lists below enumerate the names cello knows, so a name that is not on
/// one of them is never looked at and never reported. That is correct for a
/// name cello never had, but wrong for one it removed: the operator set the
/// removed name on purpose, and the behaviour it bought is gone. A node whose
/// unit file still carries the name would boot clean, log nothing, and serve a
/// deployment that no longer has the binding, so the defect would surface as a
/// `TypeError` in a request handler on a node the operator believes they just
/// configured. Refusing the boot moves that failure to the moment the operator
/// can still read the unit file.
struct Removed {
    name: &'static str,
    /// What the operator must write in place of the line they delete.
    replacement: &'static str,
    /// Whether the removed reader ignored an empty value. Only a value that
    /// once changed the node is stale, so this decides whether `NAME=` is a
    /// line to delete or a line that never did anything. It is a property of
    /// the reader that went away, so each entry must state it for itself: a
    /// removed variable whose reader treated `NAME=` as meaningful sets
    /// `false`, and an operator who templated it to an empty string then still
    /// gets the refusal they need.
    empty_was_inert: bool,
}

const REMOVED: &[Removed] = &[
    Removed {
        name: "CELLO_TEST_OTEL_SWEEP_MS",
        replacement: "remove this setting; cello manages the telemetry retention cadence",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_OTEL_SINK",
        replacement: "set CELLO_OTEL=1 for the fleet bucket or CELLO_OTEL=<collector URL> for OTLP",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_AI_BINDING",
        replacement: "remove this setting; call the AI provider from application code",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_AI_URL",
        replacement: "remove this setting; call the AI provider from application code",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_PACED_HANDOFF",
        replacement:
            "remove this setting; cello hands off ownership within CELLO_SHUTDOWN_TOTAL_MS",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_LOG_GROUP_COMMIT_MS",
        replacement: "remove this setting; cello uses its built-in Queue batching policy",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_QUEUE_PRODUCER_GROUP_MS",
        replacement: "remove this setting; cello uses its built-in Queue batching policy",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_LOG_BUNDLE",
        replacement: "remove this setting; fleet durability uses bundled tiering",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_PRESENCE_SHADOW",
        replacement:
            "remove this setting; managed presence no longer compares a shadow lease report",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_STORAGE_PROBE",
        replacement: "remove this setting; cello checks the storage contract before serving",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_EVICTIONS",
        replacement: "remove this setting; cello uses its built-in scheduling limits",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_LOG_CAPTURE_WORKERS",
        replacement: "remove this setting; cello uses its built-in scheduling limits",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_REBALANCE_BATCH_CELLS",
        replacement: "remove this setting; cello uses its built-in scheduling limits",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_CLOUD_RESTART_ON_DEPLOY",
        replacement: "remove this setting; managed deployments are adopted in place",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_OUTPUT_GATE",
        replacement: "remove this setting; cello always waits for durability proof",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_DRAIN_TOKEN_WAIT_MS",
        replacement: "set only CELLO_SHUTDOWN_TOTAL_MS; cello derives the drain-token wait",
        // Zero disabled the token wait, and an empty value failed parsing.
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_SHUTDOWN_DRAIN_MS",
        replacement:
            "set only CELLO_SHUTDOWN_TOTAL_MS; cello derives the handoff no-progress interval",
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_MAX_LOADED_WORKERS",
        replacement: "remove this setting; cello manages Dynamic Worker admission",
        // Even an empty value failed in the old typed reader. Refuse it rather
        // than silently changing the operator's configured admission policy.
        empty_was_inert: false,
    },
    Removed {
        name: "CELLO_WORKER_LOADER",
        replacement: "declare `worker_loaders` in the project config",
        // The reader this replaced was
        // `var("CELLO_WORKER_LOADER").ok().filter(|name| !name.is_empty())`, so
        // an empty value never bound a loader. A deployment system that
        // templates every known name to an empty string must not lose an
        // upgrade on a node that never used the feature.
        empty_was_inert: true,
    },
    Removed {
        name: "CELLO_VARS_FILE",
        replacement: "set `vars` in the Wrangler config, or `.dev.vars` for `cello dev`",
        // The former reader used the path verbatim, so `CELLO_VARS_FILE=`
        // selected an empty path and failed instead of doing nothing. An
        // operator must therefore delete the empty setting.
        empty_was_inert: false,
    },
];

/// The removed `CELLO_VAR_<NAME>` family. It is a prefix and not a name, so
/// the table above cannot hold it: cello never knew which names an operator
/// used. A node started with any of them would run with no override at all,
/// which reads inside a Worker as a missing secret with nothing in the log.
const REMOVED_PREFIX: (&str, &str) = (
    "CELLO_VAR_",
    "set `vars` in the Wrangler config, or `.dev.vars` for `cello dev`",
);

/// The pre-rename `CELLD_*` family. Cello 0.1.0 renamed every `CELLD_*`
/// variable to `CELLO_*` with no fallback: a node started with e.g.
/// `CELLD_OPERATOR_TOKEN` set would otherwise run with no operator token
/// and leave the internal listener open with nothing in the log. Fail
/// closed and name the new spelling. This check runs inside `validate`,
/// which every process entry point (node, `cello dev`, operator CLIs)
/// calls before serving anything.
const RENAMED_PREFIX: (&str, &str) = ("CELLD_", "CELLO_");

/// Refuse a stale pre-rename `CELLD_*` setting found in `vars`, naming the
/// new spelling. Pure over the injected iterator so tests pin the refusal
/// without touching the process environment (parallel tests share it);
/// `validate` passes the real environment.
fn refuse_renamed(mut vars: impl Iterator<Item = (OsString, OsString)>) -> anyhow::Result<()> {
    let (old_prefix, new_prefix) = RENAMED_PREFIX;
    if let Some(name) = vars.find_map(|(name, _)| {
        let name = name.to_string_lossy().into_owned();
        name.starts_with(old_prefix).then_some(name)
    }) {
        let renamed = format!("{new_prefix}{}", &name[old_prefix.len()..]);
        bail!(
            "{name} was renamed to {renamed} and is no longer read; \
             refusing to start rather than running with silently ignored configuration"
        );
    }
    Ok(())
}

/// Validate every typed production variable before the runtime starts.
///
/// Some consumers cache a value or read it from a synchronous callback, so
/// they cannot return a configuration error at the point of use. This pass
/// makes those reads infallible without giving malformed values a default.
pub fn validate() -> anyhow::Result<()> {
    // The renamed prefix first: a stale `CELLD_*` setting is the most likely
    // operator error after an upgrade, and everything below would silently
    // ignore it.
    refuse_renamed(std::env::vars_os())?;
    // Each entry carries its own empty-value rule, because whether `NAME=` was
    // inert is a property of the reader that went away and not of removal.
    for removed in REMOVED {
        let Some(value) = std::env::var_os(removed.name) else {
            continue;
        };
        if removed.empty_was_inert && value.is_empty() {
            continue;
        }
        bail!("{} is removed; {}", removed.name, removed.replacement);
    }
    let (prefix, replacement) = REMOVED_PREFIX;
    if let Some(name) = std::env::vars_os().find_map(|(name, _)| {
        let name = name.to_string_lossy().into_owned();
        name.starts_with(prefix).then_some(name)
    }) {
        bail!("{name} is removed; {replacement}");
    }

    for name in [
        "CELLO_CLOUD",
        "CELLO_LTX_COMPACTION",
        "CELLO_LTX_PAGED",
        "CELLO_TRUST_FORWARDED_HEADERS",
        "CELLO_UNSAFE_PUBLIC_ADVERTISE",
    ] {
        flag(name, false)?;
    }

    // Off by default: only an explicit `1` enables. `flag` rejects anything
    // else at startup, so the per-flush read never sees garbage.
    flag("CELLO_BUCKET_BUNDLES", false)?;

    for name in [
        "CELLO_ACTIVATIONS",
        "CELLO_C_TRIM_MB",
        "CELLO_DEPLOY_POLL_S",
        "CELLO_FETCH_TIMEOUT_S",
        "CELLO_HANDLER_BUDGET_S",
        "CELLO_IDLE_EVICT_S",
        "CELLO_LOG_PIPELINE",
        "CELLO_LTX_COMPACTIONS",
        "CELLO_LTX_COMPACTION_MIN_TXIDS",
        "CELLO_LTX_DURABILITY_TIMEOUT_SECS",
        "CELLO_MAX_CELL_REQUESTS",
        "CELLO_MAX_ASSET_FILE_BYTES",
        "CELLO_MAX_OUTBOUND_WEBSOCKETS",
        "CELLO_MAX_REQUEST_BODY_BYTES",
        "CELLO_MAX_REQUESTS",
        "CELLO_RECOVERY_RETRY_MS",
        "CELLO_RECOVERY_RETRIES",
        "CELLO_MAX_STATELESS_ISOLATES",
        "CELLO_OPERATION_DEADLINE_MS",
        "CELLO_SQL_STATEMENT_MS",
        "CELLO_PLACEMENT_WEIGHT",
        "CELLO_RELEASES",
        "CELLO_SQLITE_CACHE_KB",
        "CELLO_TOKIO_THREADS",
        "CELLO_TTL_MS",
        "CELLO_TMP_MAX_BYTES",
        "CELLO_WAKER_TICK_MS",
    ] {
        positive::<u64>(name)?;
    }

    for name in [
        "CELLO_ADMISSION_WAIT_MS",
        "CELLO_ALARM_RESIDENT_MS",
        "CELLO_ASSET_CACHE_BYTES",
        "CELLO_CACHE_MAX_BYTES",
        "CELLO_DEPLOY_MAX_AGE_S",
        "CELLO_LOCAL_CACHE_MAX_BYTES",
        "CELLO_LTX_TRUNCATE_PAGES",
        "CELLO_LTX_RETENTION_SECS",
        "CELLO_LOG_HEDGE_MS",
        "CELLO_LOG_WINDOW",
        "CELLO_LOG_WINDOW_BYTES",
        "CELLO_MAX_RESIDENT_CELLS",
        "CELLO_MAX_RSS_MB",
        "CELLO_READY_FLEET_GATE_MS",
        "CELLO_REBALANCE_INTERVAL_MS",
    ] {
        optional::<u64>(name)?;
    }

    shutdown_timing()?;

    positive::<usize>("CELLO_MAX_DYNAMIC_WORKER_CODE_BYTES")?;
    positive::<usize>("CELLO_MAX_CELLS_PER_ISOLATE")?;

    if let Some(value) = optional::<u64>("CELLO_PRESENCE_HEARTBEAT_MS")? {
        if !(50..=30_000).contains(&value) {
            bail!("CELLO_PRESENCE_HEARTBEAT_MS must be between 50 and 30000, not {value}");
        }
    }

    if let Some(megabytes) = positive::<usize>("CELLO_V8_HEAP_LIMIT_MB")? {
        if megabytes.checked_mul(1024 * 1024).is_none() {
            bail!("CELLO_V8_HEAP_LIMIT_MB is too large: {megabytes}");
        }
    }

    if let Some(megabytes) = positive::<usize>("CELLO_V8_EXTERNAL_LIMIT_MB")? {
        if megabytes.checked_mul(1024 * 1024).is_none() {
            bail!("CELLO_V8_EXTERNAL_LIMIT_MB is too large: {megabytes}");
        }
    }

    if let Some(value) = value("CELLO_LOG_TRANSPORT")? {
        if !matches!(value.as_str(), "http" | "stream") {
            bail!("CELLO_LOG_TRANSPORT must be http or stream, not {value:?}");
        }
    }

    // The tenant egress policy refuses to boot on invalid values: an
    // operator who misspells CELLO_EGRESS opted into a policy, and
    // silently running open (or closed) would betray that intent.
    if let Err(error) = crate::egress::EgressPolicy::from_env() {
        bail!("{error}");
    }

    if let Some(value) = value("CELLO_PRESSURE_OWNERSHIP")? {
        if !matches!(value.as_str(), "release" | "sticky") {
            bail!("CELLO_PRESSURE_OWNERSHIP must be release or sticky, not {value:?}");
        }
    }
    if let Some(value) = value("CELLO_MEMORY_SCOPE")? {
        if !matches!(value.as_str(), "container" | "self") {
            bail!("CELLO_MEMORY_SCOPE must be container or self, not {value:?}");
        }
    }
    if let Some(value) = value("CELLO_CACHE_SCOPE")? {
        if !matches!(value.as_str(), "script" | "deployment") {
            bail!("CELLO_CACHE_SCOPE must be script or deployment, not {value:?}");
        }
    }
    if let Some(node) = value("CELLO_NODE")? {
        crate::machine::validate_node_name(&node).map_err(|error| anyhow!("CELLO_NODE {error}"))?;
    }
    // All-or-nothing, so a node never reads a deployment with a partial
    // credential pair (see `deploy_credentials`).
    deploy_credentials()?;
    Ok(())
}

/// Resolve all shutdown timing from the operator's complete process bound.
/// Token acquisition consumes at most three quarters of that bound. The
/// no-progress interval is five eighths, with a one-millisecond minimum.
/// The process deadline still caps every shutdown phase and progress reset.
pub fn shutdown_timing() -> anyhow::Result<ShutdownTiming> {
    let total_ms = positive("CELLO_SHUTDOWN_TOTAL_MS")?.unwrap_or(DEFAULT_SHUTDOWN_TOTAL_MS);
    Ok(ShutdownTiming::from_total_ms(total_ms))
}

/// The deployment-artifact credential, if the process was given one: the
/// access key and secret are both required, and the session token is
/// optional with them. A node reads `deploy/` and `deploy-blobs/` through
/// this pair, so it can read a deployment without holding the credential
/// that publishes one; unset keeps one credential for everything.
///
/// A partial setting is a configuration error, never a fallback: reading a
/// deployment with the publishing credential is exactly what the separate
/// pair exists to avoid. An empty value counts as unset.
pub fn deploy_credentials() -> anyhow::Result<Option<(String, String, Option<String>)>> {
    deploy_credentials_from(value)
}

/// [`deploy_credentials`] over an injected lookup, so the all-or-nothing
/// rule is testable without touching the process environment.
fn deploy_credentials_from(
    get: impl Fn(&str) -> anyhow::Result<Option<String>>,
) -> anyhow::Result<Option<(String, String, Option<String>)>> {
    let get = |name: &str| get(name).map(|value| value.filter(|value| !value.is_empty()));
    let access_key_id = get("CELLO_DEPLOY_AWS_ACCESS_KEY_ID")?;
    let secret_access_key = get("CELLO_DEPLOY_AWS_SECRET_ACCESS_KEY")?;
    let session_token = get("CELLO_DEPLOY_AWS_SESSION_TOKEN")?;
    match (access_key_id, secret_access_key) {
        (None, None) if session_token.is_none() => Ok(None),
        (None, None) => bail!(
            "CELLO_DEPLOY_AWS_SESSION_TOKEN needs CELLO_DEPLOY_AWS_ACCESS_KEY_ID and \
             CELLO_DEPLOY_AWS_SECRET_ACCESS_KEY"
        ),
        (Some(_), None) => bail!(
            "CELLO_DEPLOY_AWS_ACCESS_KEY_ID is set without CELLO_DEPLOY_AWS_SECRET_ACCESS_KEY"
        ),
        (None, Some(_)) => bail!(
            "CELLO_DEPLOY_AWS_SECRET_ACCESS_KEY is set without CELLO_DEPLOY_AWS_ACCESS_KEY_ID"
        ),
        (Some(access_key_id), Some(secret_access_key)) => {
            Ok(Some((access_key_id, secret_access_key, session_token)))
        }
    }
}

pub fn value(name: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(anyhow!("read {name}: {error}")),
    }
}

pub fn flag(name: &str, default: bool) -> anyhow::Result<bool> {
    let value = value(name)?;
    parse_flag(name, value.as_deref(), default)
}

pub fn parse_flag(name: &str, value: Option<&str>, default: bool) -> anyhow::Result<bool> {
    match value {
        None => Ok(default),
        Some("0") => Ok(false),
        Some("1") => Ok(true),
        Some(other) => bail!("{name} must be 0 or 1, not {other:?}"),
    }
}

pub fn optional<T>(name: &str) -> anyhow::Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    parse_optional(name, value(name)?)
}

pub fn parse_optional<T>(name: &str, value: Option<String>) -> anyhow::Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .map(|value| {
            value
                .parse::<T>()
                .map_err(|error| anyhow!("{name} has invalid value {value:?}: {error}"))
        })
        .transpose()
}

pub fn with_default<T>(name: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    Ok(optional(name)?.unwrap_or(default))
}

pub fn positive<T>(name: &str) -> anyhow::Result<Option<T>>
where
    T: std::str::FromStr + Default + PartialOrd + std::fmt::Display,
    T::Err: std::fmt::Display,
{
    parse_positive(name, value(name)?)
}

pub fn parse_positive<T>(name: &str, value: Option<String>) -> anyhow::Result<Option<T>>
where
    T: std::str::FromStr + Default + PartialOrd + std::fmt::Display,
    T::Err: std::fmt::Display,
{
    let Some(value) = parse_optional::<T>(name, value)? else {
        return Ok(None);
    };
    if value <= T::default() {
        bail!("{name} must be greater than zero, not {value}");
    }
    Ok(Some(value))
}

pub fn positive_or<T>(name: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr + Default + PartialOrd + std::fmt::Display,
    T::Err: std::fmt::Display,
{
    Ok(positive(name)?.unwrap_or(default))
}

#[cfg(test)]
mod deploy_credential_tests {
    use super::*;

    fn parse(pairs: &[(&str, &str)]) -> anyhow::Result<Option<(String, String, Option<String>)>> {
        deploy_credentials_from(|name| {
            Ok(pairs
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_string()))
        })
    }

    #[test]
    fn unset_means_one_credential() {
        assert_eq!(parse(&[]).unwrap(), None);
    }

    #[test]
    fn the_pair_rides_together() {
        let parsed = parse(&[
            ("CELLO_DEPLOY_AWS_ACCESS_KEY_ID", "id"),
            ("CELLO_DEPLOY_AWS_SECRET_ACCESS_KEY", "secret"),
        ])
        .unwrap();
        assert_eq!(parsed, Some(("id".into(), "secret".into(), None)));
    }

    #[test]
    fn the_session_token_rides_with_the_pair() {
        let parsed = parse(&[
            ("CELLO_DEPLOY_AWS_ACCESS_KEY_ID", "id"),
            ("CELLO_DEPLOY_AWS_SECRET_ACCESS_KEY", "secret"),
            ("CELLO_DEPLOY_AWS_SESSION_TOKEN", "token"),
        ])
        .unwrap();
        assert_eq!(
            parsed,
            Some(("id".into(), "secret".into(), Some("token".into())))
        );
    }

    #[test]
    fn a_partial_pair_is_a_configuration_error() {
        for pairs in [
            vec![("CELLO_DEPLOY_AWS_ACCESS_KEY_ID", "id")],
            vec![("CELLO_DEPLOY_AWS_SECRET_ACCESS_KEY", "secret")],
            vec![("CELLO_DEPLOY_AWS_SESSION_TOKEN", "token")],
        ] {
            assert!(parse(&pairs).is_err(), "{pairs:?} must not parse");
        }
    }

    #[test]
    fn an_empty_value_is_unset() {
        let parsed = parse(&[
            ("CELLO_DEPLOY_AWS_ACCESS_KEY_ID", ""),
            ("CELLO_DEPLOY_AWS_SECRET_ACCESS_KEY", ""),
        ])
        .unwrap();
        assert_eq!(parsed, None);
    }
}

/// The stale-prefix refusal. Fails before the `RENAMED_PREFIX` check exists
/// (`validate` returns `Ok` and the stale setting is silently ignored).
#[cfg(test)]
mod renamed_prefix_tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
        pairs
            .iter()
            .map(|(name, value)| (OsString::from(name), OsString::from(value)))
            .collect()
    }

    #[test]
    fn stale_prefix_setting_refuses_to_start_and_names_the_cello_spelling() {
        // Pure over explicit vars: no process environment is touched, so
        // this test can never race a sibling test or the harness env.
        let message = refuse_renamed(
            vars(&[
                ("CELLO_OPERATOR_TOKEN", "token"),
                ("CELLD_OPERATOR_TOKEN", "stale"),
            ])
            .into_iter(),
        )
        .expect_err("a stale CELLD_* setting must refuse to start")
        .to_string();
        assert!(
            message.contains("CELLD_OPERATOR_TOKEN was renamed to CELLO_OPERATOR_TOKEN"),
            "refusal must name the new spelling, got: {message}"
        );
    }

    #[test]
    fn clean_environment_still_validates() {
        refuse_renamed(vars(&[("CELLO_OPERATOR_TOKEN", "token"), ("PATH", "/bin")]).into_iter())
            .expect("an env without CELLD_* validates");
    }
}
