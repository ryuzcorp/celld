// Modified by ryuzcorp (ryuzcorp/celld): staged invoke operator command. Apache-2.0.

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

#[derive(Debug)]
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

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
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
    if axum::http::HeaderName::from_str(name).is_err() {
        bail!("invalid --header name {name:?}");
    }
    if let Err(reason) = crate::staged_invoke::check_inner_header(name) {
        bail!("invalid --header name {name:?}: {reason}");
    }
    let value = value.trim_start();
    if axum::http::HeaderValue::from_str(value).is_err() {
        bail!("invalid --header value for {name:?}");
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
