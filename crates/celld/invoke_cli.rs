// Modified by ryuzcorp (ryuzcorp/celld): staged invoke operator command; invoke --header-env. Apache-2.0.

// Operator commands read operator-owned host paths and file descriptors.
#![allow(clippy::disallowed_methods)]

//! `celld invoke`: send one HTTP request to a staged script version.
//!
//! A version uploaded with `celld deploy --no-activate` serves no traffic
//! until `celld activate` names it. This command delivers one request to a
//! staged version on one node of the fleet — served by that version's code
//! with its declared bindings against the real data — so an operator runs
//! a migration endpoint before the activation that would serve it. It
//! moves no pointer and changes nothing serving: the staged code answers
//! this request and this request only.
//!
//! Like `celld d1 execute`, it finds a live node through the bucket's node
//! leases and signs the request with the fleet secret; the node also
//! requires the operator token when `CELLD_OPERATOR_TOKEN` is set. The
//! request runs to completion server-side even if this command stops
//! waiting: stopping the wait cannot leave a half-applied migration
//! behind, but it also cannot report the answer, so a timeout means
//! "unknown", never "failed".

use std::path::PathBuf;
use std::str::FromStr as _;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use base64::Engine as _;

use crate::cli_options::{FleetFlags, FLEET_HELP};
use crate::cli_output::{Format, Output};
use crate::deploy::VersionRef;

/// How long one invoke waits: the build plus the handler budget plus
/// `waitUntil`, which settles before the answer returns. The wait ending
/// stops nothing server-side — see the module doc — so this is generous
/// rather than tight.
const INVOKE_TIMEOUT: Duration = Duration::from_secs(600);

struct Command {
    pins: Vec<VersionRef>,
    target: String,
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Option<String>,
    body_file: Option<PathBuf>,
    fleet: FleetFlags,
    json: bool,
    unsafe_public_advertise: bool,
}

/// Header values and the body are redacted: a header may carry a secret
/// (`--header-env` exists for exactly that), so no debug print shows one.
impl std::fmt::Debug for Command {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let header_names: Vec<&str> = self.headers.iter().map(|(name, _)| name.as_str()).collect();
        formatter
            .debug_struct("Command")
            .field("pins", &self.pins)
            .field("target", &self.target)
            .field("method", &self.method)
            .field("path", &self.path)
            .field("header_names", &header_names)
            .field("body", &self.body.as_ref().map(|_| "<redacted>"))
            .field("body_file", &self.body_file)
            .field("fleet", &self.fleet)
            .field("json", &self.json)
            .field("unsafe_public_advertise", &self.unsafe_public_advertise)
            .finish()
    }
}

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        Self::parse_with(arguments, |name| std::env::var(name).ok())
    }

    /// `parse` with the environment lookup `--header-env` reads, so tests
    /// pass their own instead of mutating the process environment.
    fn parse_with(
        arguments: Vec<String>,
        environment: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter().peekable();
        let mut pins = Vec::new();
        // Positionals come first, like `celld activate`: every leading
        // argument that is not a flag is a SCRIPT=VERSION pin.
        while let Some(argument) = arguments.peek() {
            if argument == "--help" || argument == "-h" || argument == "help" {
                return Ok(None);
            }
            if argument.starts_with('-') {
                break;
            }
            let argument = arguments.next().expect("peeked argument");
            pins.push(crate::deploy::parse_version_ref(&argument, "invoke")?);
        }
        if pins.is_empty() {
            bail!("celld invoke needs at least one SCRIPT=VERSION pin");
        }
        let mut seen = std::collections::BTreeSet::new();
        for pin in &pins {
            if !seen.insert(pin.script.clone()) {
                bail!("celld invoke names script {:?} twice", pin.script);
            }
        }
        let mut target = None;
        let mut method = "GET".to_string();
        let mut path = None;
        let mut headers = Vec::new();
        let mut body = None;
        let mut body_file = None;
        let mut fleet = FleetFlags::default();
        let mut json = false;
        let mut unsafe_public_advertise = false;
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--target" => target = Some(value("--target")?),
                "--method" => method = value("--method")?,
                "--path" => path = Some(value("--path")?),
                "--header" => headers.push(parse_header(&value("--header")?)?),
                "--header-env" => {
                    headers.push(header_from_env(&value("--header-env")?, &environment)?)
                }
                "--body" => body = Some(value("--body")?),
                "--body-file" => body_file = Some(PathBuf::from(value("--body-file")?)),
                "--json" => json = true,
                "--unsafe-public-advertise" => unsafe_public_advertise = true,
                "--help" | "-h" => return Ok(None),
                other => {
                    if fleet.consume(other, &mut value)? {
                        continue;
                    }
                    bail!("unknown option: {other}; run `celld invoke --help` for usage")
                }
            }
        }
        if body.is_some() && body_file.is_some() {
            bail!("celld invoke takes either --body or --body-file, not both");
        }
        let path = path.ok_or_else(|| anyhow!("celld invoke needs --path /PATH"))?;
        if !path.starts_with('/') {
            bail!("--path takes an origin-form path starting with '/', not {path:?}");
        }
        if axum::http::Method::from_str(&method).is_err() {
            bail!("invalid --method {method:?}");
        }
        let target = match target {
            Some(target) => {
                if !pins.iter().any(|pin| pin.script == target) {
                    bail!("--target {target:?} names no pinned script; pin it as {target:?}=VERSION");
                }
                target
            }
            None if pins.len() == 1 => pins[0].script.clone(),
            None => bail!(
                "celld invoke with more than one SCRIPT=VERSION needs --target SCRIPT to name the receiver"
            ),
        };
        Ok(Some(Self {
            pins,
            target,
            method,
            path,
            headers,
            body,
            body_file,
            // Resolution is deferred to `run`, so the missing-bucket message
            // is the one every command shares.
            fleet: fleet.with_environment(),
            json,
            unsafe_public_advertise,
        }))
    }
}

/// One `--header NAME:VALUE`: the name must be exact, the value sheds the
/// conventional single space after the colon.
fn parse_header(argument: &str) -> anyhow::Result<(String, String)> {
    let Some((name, value)) = argument.split_once(':') else {
        bail!("--header takes NAME:VALUE, not {argument:?}");
    };
    checked_header("--header", name, value.trim_start())
}

/// One `--header-env NAME=VAR`: header NAME with the value of environment
/// variable VAR. A secret header value on argv is readable by every local
/// user through `/proc/<pid>/cmdline`, while the environment is readable
/// only by the same user, so secrets travel this way. The value is never
/// echoed, not even in an error.
fn header_from_env(
    argument: &str,
    environment: &impl Fn(&str) -> Option<String>,
) -> anyhow::Result<(String, String)> {
    let Some((name, variable)) = argument.split_once('=') else {
        bail!("--header-env takes NAME=VAR, not {argument:?}");
    };
    let Some(value) = environment(variable).filter(|value| !value.trim().is_empty()) else {
        bail!("--header-env {name}: environment variable {variable:?} is not set");
    };
    checked_header("--header-env", name, &value)
}

fn checked_header(flag: &str, name: &str, value: &str) -> anyhow::Result<(String, String)> {
    if axum::http::HeaderName::from_str(name).is_err() {
        bail!("invalid {flag} name {name:?}");
    }
    if let Err(reason) = crate::staged_invoke::check_inner_header(name) {
        bail!("invalid {flag} name {name:?}: {reason}");
    }
    if axum::http::HeaderValue::from_str(value).is_err() {
        bail!("invalid {flag} value for {name:?}");
    }
    Ok((name.to_string(), value.to_string()))
}

/// One `/invoke` envelope for a parsed command. The `pins` map carries
/// the cohosted scripts only: the target travels in `target`, and the
/// server refuses a pin that names it twice, so sending it in both
/// places would fail every invocation.
fn envelope(command: &Command, target: &VersionRef, body: &[u8]) -> serde_json::Value {
    let pins: std::collections::BTreeMap<&str, &str> = command
        .pins
        .iter()
        .filter(|pin| pin.script != target.script)
        .map(|pin| (pin.script.as_str(), pin.version.as_str()))
        .collect();
    serde_json::json!({
        "target": { "script": target.script, "version": target.version },
        "pins": pins,
        "request": {
            "method": command.method,
            "path": command.path,
            "headers": command.headers,
            "body": base64::engine::general_purpose::STANDARD.encode(body),
        },
    })
}

fn read_body(command: &Command) -> anyhow::Result<Vec<u8>> {
    if let Some(body) = &command.body {
        return Ok(body.as_bytes().to_vec());
    }
    let Some(path) = &command.body_file else {
        return Ok(Vec::new());
    };
    if path.as_os_str() == "-" {
        let mut body = Vec::new();
        std::io::Read::read_to_end(&mut std::io::stdin(), &mut body)
            .context("read the request body from stdin")?;
        return Ok(body);
    }
    std::fs::read(path).with_context(|| format!("read the request body from {}", path.display()))
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    let storage = command.fleet.clone().resolve("celld invoke")?;
    let body = read_body(&command)?;
    let target = command
        .pins
        .iter()
        .find(|pin| pin.script == command.target)
        .expect("target is a pinned script");
    let envelope = envelope(&command, target, &body);
    let reachable = crate::operator_cell::Reachable::open_path(
        crate::operator_cell::Fleet {
            bucket: &storage.bucket,
            endpoint: storage.endpoint.as_deref(),
            region: &storage.region,
            unsafe_public_advertise: command.unsafe_public_advertise,
        },
        crate::operator_cell::Subject {
            noun: "staged script",
            source: "invoke",
            timeout: INVOKE_TIMEOUT,
        },
        "/invoke".to_string(),
    )
    .await?;
    let result = reachable.call(envelope).await?;
    let status = result
        .get("status")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| anyhow!("the reply carried no status: {result}"))?;
    let body = result
        .get("body")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("the reply carried no body: {result}"))?;
    let body = base64::engine::general_purpose::STANDARD
        .decode(body)
        .context("decode the reply body")?;
    if command.json {
        Output::new(Format::Text).line(format_args!("{result}"))?;
    } else {
        Output::new(Format::Text).bytes(&body)?;
    }
    crate::note!("invoked {}@{}: {}", target.script, target.version, status);
    if !(200..300).contains(&status) {
        bail!(
            "staged {}@{} answered {status}",
            target.script,
            target.version
        );
    }
    Ok(())
}

pub fn print_help() {
    let text = format!(
        r#"celld invoke — send one HTTP request to a staged script version

USAGE:
  celld invoke SCRIPT=VERSION [SCRIPT=VERSION...] --path PATH --bucket NAME

A version uploaded with `celld deploy --no-activate` serves no traffic
until `celld activate` names it. This command delivers one request to
the staged version on one node of the fleet, served by that version's
code with its declared bindings against the real data, so a migration
endpoint runs before the activation that would serve it. It moves no
pointer and changes nothing serving.

SCRIPT=VERSION pins name the staged generation: the receiver plus every
cohosted script the request must see staged. Scripts the pins omit
resolve from the live generation, exactly as an activation of the same
pins would carry them. With one pin it receives the request; with more,
--target names the receiver.

The request runs to completion on the node even if this command stops
waiting: a timeout means the answer is unknown, never that the request
failed. `request.url` carries the path below on the target script as
its host. Only a buffered request and answer cross: the body is capped
by the node's ingress limit, the answer at 64 MiB, and upgrades are
refused.

  --target SCRIPT   The pinned script receiving the request (default:
                    the pin, when there is exactly one)
  --path PATH       Origin-form request path, with any query (required)
  --method METHOD   Request method (default: GET)
  --header N:V      Request header; repeatable
  --header-env N=VAR
                    Request header N with the value of environment
                    variable VAR; repeatable. Use it for secrets: argv
                    is readable by every local user, the environment
                    only by the same user
  --body TEXT       Request body bytes
  --body-file PATH  Request body from a file, or from stdin with -
  --json            Print the result envelope instead of the body bytes
  --unsafe-public-advertise
                    Reach a node that advertises a literal public IP
{FLEET_HELP}

Stdout carries the staged version's answer body, or the envelope with
--json; anything else goes to stderr. A non-2xx answer still prints
its body, then exits nonzero."#,
    );
    let _ = Output::new(Format::Text).help(&text);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> anyhow::Result<Option<Command>> {
        Command::parse(arguments.iter().map(|value| value.to_string()).collect())
    }

    /// `--header-env` takes the value from the environment, never from
    /// argv; an unset variable is refused without echoing anything, and
    /// the same name and value checks as `--header` apply.
    #[test]
    fn header_env_reads_the_value_from_the_environment() {
        let environment = |name: &str| match name {
            "PROXY_TOKEN" => Some("s3cret-value".to_string()),
            "EMPTY" => Some(String::new()),
            "BLANK" => Some("  ".to_string()),
            "BAD" => Some("line\nbreak".to_string()),
            _ => None,
        };
        let parse_env = |extra: &[&str]| {
            let mut arguments: Vec<String> =
                ["app=0123456789abcdef", "--path", "/m", "--bucket", "b"]
                    .iter()
                    .map(|value| value.to_string())
                    .collect();
            arguments.extend(extra.iter().map(|value| value.to_string()));
            Command::parse_with(arguments, environment)
        };
        let command = parse_env(&["--header-env", "x-void-internal=PROXY_TOKEN"])
            .expect("parse")
            .expect("command");
        assert_eq!(
            command.headers,
            vec![("x-void-internal".to_string(), "s3cret-value".to_string())]
        );
        let debug = format!("{command:?}");
        assert!(debug.contains("x-void-internal"), "{debug}");
        assert!(!debug.contains("s3cret-value"), "the value leaked: {debug}");
        for (argument, want) in [
            ("x-void-internal=MISSING", "is not set"),
            ("x-void-internal=EMPTY", "is not set"),
            ("x-void-internal=BLANK", "is not set"),
            ("x-void-internal=BAD", "invalid --header-env value"),
            ("no-equals", "takes NAME=VAR"),
            ("x-cells-node=PROXY_TOKEN", "invalid --header-env name"),
        ] {
            let error = match parse_env(&["--header-env", argument]) {
                Ok(_) => panic!("{argument} must be refused"),
                Err(error) => format!("{error:#}"),
            };
            assert!(error.contains(want), "{argument}: {error}");
            assert!(
                !error.contains("s3cret-value"),
                "{argument}: the value leaked: {error}"
            );
        }
    }

    #[test]
    fn minimal_invocation_targets_its_only_pin() {
        let command = parse(&[
            "app=0123456789abcdef",
            "--path",
            "/migrate",
            "--bucket",
            "b",
        ])
        .expect("parse")
        .expect("command");
        assert_eq!(command.target, "app");
        assert_eq!(command.method, "GET");
        assert_eq!(command.path, "/migrate");
        assert_eq!(command.pins.len(), 1);
        assert!(command.headers.is_empty());
        assert!(!command.json);
    }

    #[test]
    fn full_invocation_keeps_every_option() {
        let command = parse(&[
            "app=0123456789abcdef",
            "helper=fedcba9876543210",
            "--target",
            "app",
            "--method",
            "POST",
            "--path",
            "/migrate?step=2",
            "--header",
            "authorization: Bearer x",
            "--header",
            "x-empty:",
            "--body",
            "{}",
            "--json",
            "--bucket",
            "b",
            "--endpoint",
            "e",
            "--region",
            "r",
            "--unsafe-public-advertise",
        ])
        .expect("parse")
        .expect("command");
        assert_eq!(command.target, "app");
        assert_eq!(command.method, "POST");
        assert_eq!(command.path, "/migrate?step=2");
        assert_eq!(
            command.headers,
            vec![
                ("authorization".to_string(), "Bearer x".to_string()),
                ("x-empty".to_string(), String::new()),
            ]
        );
        assert_eq!(command.body.as_deref(), Some("{}"));
        assert!(command.json);
        assert!(command.unsafe_public_advertise);
        assert_eq!(command.fleet.bucket.as_deref(), Some("b"));
    }

    #[test]
    fn ambiguous_or_unknown_targets_are_refused() {
        let error = parse(&[
            "app=0123456789abcdef",
            "helper=fedcba9876543210",
            "--path",
            "/",
        ])
        .expect_err("ambiguous target");
        assert!(format!("{error:#}").contains("--target"), "{error:#}");
        let error = parse(&["app=0123456789abcdef", "--target", "helper", "--path", "/"])
            .expect_err("unknown target");
        assert!(format!("{error:#}").contains("helper"), "{error:#}");
    }

    #[test]
    fn bad_arguments_fail_with_usage() {
        for (name, arguments) in [
            ("no pins", vec!["--path", "/"]),
            (
                "duplicate pin",
                vec![
                    "app=0123456789abcdef",
                    "app=fedcba9876543210",
                    "--path",
                    "/",
                ],
            ),
            ("malformed pin", vec!["app", "--path", "/"]),
            ("malformed version", vec!["app=xyz", "--path", "/"]),
            ("missing path", vec!["app=0123456789abcdef"]),
            (
                "absolute-form path",
                vec!["app=0123456789abcdef", "--path", "http://x/"],
            ),
            (
                "bad method",
                vec!["app=0123456789abcdef", "--path", "/", "--method", "GE T"],
            ),
            (
                "header without a colon",
                vec!["app=0123456789abcdef", "--path", "/", "--header", "abc"],
            ),
            (
                "control-plane header",
                vec![
                    "app=0123456789abcdef",
                    "--path",
                    "/",
                    "--header",
                    "x-cells-operator: x",
                ],
            ),
            (
                "body and body-file",
                vec![
                    "app=0123456789abcdef",
                    "--path",
                    "/",
                    "--body",
                    "x",
                    "--body-file",
                    "y",
                ],
            ),
            (
                "unknown flag",
                vec!["app=0123456789abcdef", "--path", "/", "--frobnicate"],
            ),
        ] {
            parse(&arguments).expect_err(name);
        }
    }

    /// The CLI's envelope must match the server's contract: the server
    /// refuses a pin naming the target, so the target travels in
    /// `target` and the other pins in `pins`. The black-box suite
    /// covers the server side of that shape; this pins the CLI side,
    /// which no live fleet exists to test here.
    #[test]
    fn envelope_keeps_the_target_out_of_pins() {
        let command = parse(&[
            "app=0123456789abcdef",
            "helper=fedcba9876543210",
            "--target",
            "app",
            "--method",
            "POST",
            "--path",
            "/migrate",
            "--header",
            "x-tenant: t1",
        ])
        .expect("parse")
        .expect("command");
        let target = command
            .pins
            .iter()
            .find(|pin| pin.script == command.target)
            .expect("target");
        let value = envelope(&command, target, b"{}");
        assert_eq!(
            value.get("target"),
            Some(&serde_json::json!({ "script": "app", "version": "0123456789abcdef" }))
        );
        assert_eq!(
            value.get("pins"),
            Some(&serde_json::json!({ "helper": "fedcba9876543210" }))
        );
        let request = value.get("request").expect("request");
        assert_eq!(
            request.get("method").and_then(serde_json::Value::as_str),
            Some("POST")
        );
        assert_eq!(
            request.get("path").and_then(serde_json::Value::as_str),
            Some("/migrate")
        );
        // And the envelope the CLI builds parses server-side.
        let parsed = crate::staged_invoke::parse_envelope(value.to_string().as_bytes(), 1024)
            .expect("server parses the CLI envelope");
        assert_eq!(parsed.target().script, "app");
    }

    #[test]
    fn help_short_circuits() {
        assert!(parse(&["--help"]).expect("parse").is_none());
        assert!(parse(&["app=0123456789abcdef", "--path", "/", "--help"])
            .expect("parse")
            .is_none());
    }
}
