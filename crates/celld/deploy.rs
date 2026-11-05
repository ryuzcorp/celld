// Modified by ryuzcorp (ryuzcorp/celld): idempotent content-tagged image create; tag-race tests run the fake engine through sh; deploy --sourcemap writes the bundle's external map; service binding targets validated at deploy; staged activation; deploy --sourcemap collects the no_bundle sibling map; staged invoke shares the version-ref parsing; deploy reads stay inside the project; deploy --read-root bounds bundling and container builds.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Deployment is an operator control-plane path outside the Actor execution
// domain.
#![allow(clippy::disallowed_methods)]
// Deploy narrates progress; every line of it is for a person.

//! `celld deploy` — build a Wrangler project and write it to the fleet bucket.
//!
//! Bundling is esbuild's job; this module does config, identity, and durable
//! bucket publication. Nothing here shells out to wrangler or speaks a
//! Cloudflare-shaped API. Config keys are an allowlist: anything we do not
//! model is refused, never silently dropped.
use crate::bucket::Bucket;
use crate::container::ContainerSpec;
use crate::note;
use crate::protocol::{
    asset_blob_key, AssetConfig, AssetEntry, AssetIndex, AssetManifestRef, ComponentRef,
    DeployPointer, Manifest, ModuleKind, ModuleRef, QueueConsumerAttachment, QueueConsumerConfig,
    QueueConsumerDeployment, Rollout, RunWorkerFirst, FEATURE_ASSETS_V1, FEATURE_CONTAINERS_V1,
    FEATURE_CRON_V1, FEATURE_D1_V1, FEATURE_KV_V1, FEATURE_PYTHON_WORKERS_V1, FEATURE_QUEUES_V1,
    FEATURE_R2_V1, FEATURE_SQLITE_VEC_V1, FEATURE_WASM_V1, FEATURE_WORKFLOWS_V1,
    QUEUE_CONSUMER_ATTACHMENT_SCHEMA_VERSION,
};
use anyhow::{anyhow, bail, Context};
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::{stream, StreamExt, TryStreamExt};
use serde_json::{json, Map, Value};
use sha2::Digest;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

mod confine;
mod python;

/// Config keys we understand. Anything else is an error: refusing is
/// compat-safe, guessing produces confusing activation failures later.
const SUPPORTED_KEYS: &[&str] = &[
    "$schema",
    "name",
    "main",
    "compatibility_date",
    "compatibility_flags",
    "durable_objects",
    "migrations",
    "assets",
    "services",
    "triggers",
    "vars",
    "d1_databases",
    "kv_namespaces",
    "queues",
    "workflows",
    "r2_buckets",
    "worker_loaders",
    "containers",
    "no_bundle",
    "define",
    "rules",
];

/// The Durable Object class every D1 database runs as. It is supplied by the
/// runtime, not by the worker, so a config naming it in `durable_objects` or
/// `migrations` is refused: the harness registers this class in every isolate,
/// and a user binding onto it would silently reach a D1 database cell instead
/// of the user's class. A bare module export of this name is not refused —
/// the loader skips it rather than let it shadow the built-in.
pub const D1_CLASS: &str = "__D1Database";

/// The Durable Object class every workflow instance runs as. Like `D1_CLASS`,
/// it is supplied by the runtime and refused as a user class name.
pub const WORKFLOW_CLASS: &str = "__Workflow";

/// The Durable Object class every KV namespace runs as.
///
/// Fleet-wide, like `D1_CLASS` and unlike `workflow_class`: a namespace is a
/// resource several Workers bind, so two scripts naming one namespace mean to
/// reach one set of cells.
pub const KV_CLASS: &str = celld_logic::kv::RESERVED_CLASS;

/// The Durable Object class every Queue broker runs as.
pub const QUEUE_CLASS: &str = celld_logic::queue::RESERVED_CLASS;

/// Every runtime-supplied Durable Object class.
pub const RESERVED_CLASSES: &[&str] = &[D1_CLASS, WORKFLOW_CLASS, KV_CLASS, QUEUE_CLASS];

/// The Durable Object class a script's workflow instances run as.
///
/// Script-scoped, because a workflow instance already is. Its namespace key is
/// `cells:v1:<len>:<script>:__Workflow`, so two co-hosted scripts address
/// different cells — but both used to register the *same* class name, and a
/// deployment's class registry is one flat map, so the second script collided
/// with the first and the node refused to start at all. The name now says what
/// the namespace key always said.
///
/// D1 deliberately does not get this treatment. Its namespace is fleet-wide so
/// that several Workers can bind one database and a Worker rename cannot rename
/// it, which means a D1 cell genuinely is shared and one entry is correct.
///
/// `.` is the separator for three reasons, and all three are constraints
/// rather than taste. A JavaScript class name cannot contain one, so no user
/// export can collide. It is not `:`, which is what the cell-scope parser
/// splits a class from an id on. And it is inside
/// `celld_logic::cell::valid_cell_scope`'s charset, which is a security fence
/// on a name that becomes a path component and an object-store key — `@` is
/// not, and a scope carrying one is refused before it reaches storage. Cron
/// picked `.` for the first of those reasons already.
pub fn workflow_class(script_name: &str) -> String {
    format!("{WORKFLOW_CLASS}.{script_name}")
}

/// Whether a reserved class names cells that several scripts share on purpose.
///
/// Only D1. Its namespace key is fleet-wide precisely so that a database
/// outlives the name of any one Worker that binds it, so two scripts declaring
/// `d1_databases` mean to reach the same cells, and one registry entry serving
/// both is correct rather than merely tolerated. A workflow class carries its
/// script, so it is never shared and a collision there is a real one.
pub fn is_shared_reserved_class(class: &str) -> bool {
    class == D1_CLASS || class == KV_CLASS || class == QUEUE_CLASS
}

/// Whether a class name is a script-scoped workflow class.
///
/// One reading of the shape, because three call sites wanted it and three
/// copies of `strip_prefix(WORKFLOW_CLASS)` is how the separator comes to mean
/// two things.
pub fn is_workflow_class(class: &str) -> bool {
    class
        .strip_prefix(WORKFLOW_CLASS)
        .is_some_and(|rest| rest.starts_with('.'))
}

pub fn is_reserved_class(class: &str) -> bool {
    RESERVED_CLASSES.contains(&class) || is_workflow_class(class)
}

/// The reserved class a cell scope names, or `None` for an ordinary Durable
/// Object.
///
/// A scope is `<class>:<instance>` and the instance may itself contain a `:`,
/// so the class is everything before the first one — the same split
/// `Runtime::start_cell` makes when it resolves a cell to its Worker config.
pub fn reserved_class_of(scope: &str) -> Option<&str> {
    let (class, _) = scope.split_once(':')?;
    is_reserved_class(class).then_some(class)
}

/// Whether a cell scope names any runtime-supplied class.
///
/// This is what the unauthenticated `/do/` route refuses, and it is deliberately
/// one question rather than one question per class. `/do/` used to ask
/// one predicate per class, so every reserved class added a
/// third, a fourth, and a place to forget one — and a forgotten refusal is not
/// a missing feature, it is an unauthenticated route onto a cell whose whole
/// surface is an operator protocol. Adding a class to `RESERVED_CLASSES` now
/// closes that door with no new code.
pub fn is_reserved_scope(scope: &str) -> bool {
    reserved_class_of(scope).is_some()
}

/// What to tell an operator who reached a reserved class on the wrong route.
///
/// A hint, never a gate. The refusal is `is_reserved_scope`, which cannot be
/// forgotten; this only decides how helpful the message is, so a class with no
/// entry here is still refused and simply gets the general answer.
pub fn operator_hint(class: &str) -> &'static str {
    if class == D1_CLASS {
        "use `celld d1`"
    } else if class == KV_CLASS {
        "use `celld kv`"
    } else if class == QUEUE_CLASS {
        "use `celld queue`"
    } else if is_workflow_class(class) {
        "use the workflow binding"
    } else {
        "reach it through its binding"
    }
}

const MAX_ASSET_FILES: usize = 20_000;
const MAX_ASSET_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_ASSET_DIRECTIVE_BYTES: u64 = 100 * 1024;
const ASSET_UPLOAD_CONCURRENCY: usize = 16;

/// Wrangler's Worker-name bound. The charset below is ASCII, so the byte and
/// character counts are identical. This also keeps a generated Workflow class
/// far below the cell-scope limit.
const MAX_SCRIPT_NAME_BYTES: usize = 63;
const _: () =
    assert!(WORKFLOW_CLASS.len() + 1 + MAX_SCRIPT_NAME_BYTES <= celld_logic::cell::MAX_CELL_SCOPE);

pub struct Options {
    pub config: Option<PathBuf>,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub dry_run: bool,
    pub json: bool,
    /// Worker variables that override the `vars` of the config. `celld dev`
    /// reads them from `.dev.vars` or the dotenv files; `celld deploy`
    /// supplies none, so a local credential cannot reach a fleet.
    pub vars: VarOverrides,
    /// Keep container images in the local engine instead of saving them to
    /// the bucket. `celld dev` runs its node on the same engine that built
    /// them; a fleet needs the tar.
    pub local_images: bool,
    /// Write the Worker's source map to this local path: the esbuild bundle's
    /// external map, or the `no_bundle` entry's sibling `<entry>.map`.
    /// Anything else warns and writes nothing. The deployed bytes are
    /// identical with or without the flag.
    pub sourcemap: Option<PathBuf>,
    /// Upload and validate the version without moving any pointer: the
    /// version is staged, invisible to serving, until `celld activate`
    /// names it. Queue attachments stay too — an attachment names an exact
    /// deployment, so moving it early would leak the staged version into
    /// the next adoption's queue resolution.
    pub no_activate: bool,
    /// Directories beyond the project that build inputs may come from:
    /// esbuild's inputs and `containers[].image` Dockerfiles with their
    /// build contexts. Only the operator names them, on the command line;
    /// every other project read stays inside the project directory.
    pub read_roots: Vec<PathBuf>,
}

/// Local Worker variables, tagged with the kind of file that supplied them.
/// The kind decides what an entry that cannot become a var binding does, so
/// it travels with the entries instead of beside them.
#[derive(Default)]
pub enum VarOverrides {
    #[default]
    None,
    /// `.dev.vars` exists only for the Worker, so an entry that cannot become
    /// a binding is a mistake, and it fails the build.
    DevVars(BTreeMap<String, String>),
    /// `.env` and `.env.local` also serve other tools, such as a bundler or an
    /// ORM. An entry with a name that is not a valid binding, or a name that
    /// another binding of the config uses, is skipped with a warning. Failing
    /// instead stops a `celld dev` that `wrangler dev` runs, and the only
    /// workaround is an empty `.dev.vars`.
    DotEnv(BTreeMap<String, String>),
}

pub fn print_help() {
    let text = "celld deploy — build a Worker with esbuild and write it to the fleet bucket\n\n\
USAGE:\n  celld deploy [PROJECT] --bucket [s3://|gs://|az://]NAME[/PREFIX] [OPTIONS]\n\n\
PROJECT is a directory or a Wrangler config; it defaults to the working\n\
directory, where celld looks for wrangler.jsonc or wrangler.json.\n\n\
OPTIONS:\n  --sourcemap PATH       Write the Worker's source map to PATH: the bundled JS\n                         entry's external map, or the `no_bundle` entry's sibling\n                         `<entry>.map`; never wasm siblings. Asset-only and Python\n                         projects, and `no_bundle` entries without a sibling map,\n                         warn and write nothing. The deployed bytes are identical\n                         with or without it\n  --read-root DIR        A directory beyond the project that build inputs may\n                         come from: esbuild's inputs and `containers[].image`\n                         Dockerfiles with their build contexts. Repeatable\n  --config PATH          Same as passing PROJECT positionally\n  --bucket [s3://|gs://|az://]NAME[/PREFIX]\n                         Fleet bucket and prefix; defaults to CELLD_BUCKET.\n                         gs:// selects a Google Cloud Storage bucket, az://\n                         an Azure Blob Storage container with its account in\n                         AZURE_STORAGE_ACCOUNT_NAME; celld then rejects\n                         --endpoint and ignores --region\n  --endpoint URL         S3-compatible endpoint; defaults to S3_ENDPOINT\n  --region REGION        Storage region; defaults to AWS_REGION\n  --dry-run              Bundle and print the version without writing\n  --no-activate          Upload and validate without moving any pointer;\n                         the staged version serves only once `celld activate`\n                         names it\n  --json                 Print the deployment as one JSON object\n  -h, --help             Show this help\n\n\
Credentials come from the standard AWS credential chain, from Google\n\
Application Default Credentials for a gs:// bucket, or from an Azure storage\n\
account key, managed identity, or workload identity for an az:// bucket.\n\n\
Worker projects require `esbuild` on PATH; asset-only projects do not. Static\n\
assets, service bindings, and string vars are supported. Routes are not; use\n\
Wrangler for route configuration.\n\
A running node polls the deployment pointer and adopts the new version in\n\
place; nothing restarts. Never run deploy concurrently with another deploy\n\
or activate."
    ;
    let text = format!(
        "{text}\nThe config `name` uses lowercase ASCII letters, digits, and internal hyphens. \
         It is at most {MAX_SCRIPT_NAME_BYTES} bytes."
    );
    let _ = crate::cli_output::Output::new(crate::cli_output::Format::Text).help(&text);
}

pub fn options_from_arguments(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<Option<Options>> {
    let mut options = Options {
        config: None,
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        local_images: false,
        json: false,
        vars: VarOverrides::None,
        sourcemap: None,
        no_activate: false,
        read_roots: Vec::new(),
    };
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--dry-run" => options.dry_run = true,
            "--json" => options.json = true,
            "--no-activate" => options.no_activate = true,
            "--config" => {
                options.config = Some(PathBuf::from(
                    arguments.next().context("--config requires a value")?,
                ));
            }
            "--bucket" => {
                let value = arguments.next().context("--bucket requires a value")?;
                options.bucket = Some(value);
            }
            "--endpoint" => {
                options.endpoint = Some(arguments.next().context("--endpoint requires a value")?);
            }
            "--region" => {
                options.region = Some(arguments.next().context("--region requires a value")?);
            }
            "--sourcemap" => {
                options.sourcemap = Some(PathBuf::from(
                    arguments.next().context("--sourcemap requires a value")?,
                ));
            }
            "--read-root" => {
                options.read_roots.push(PathBuf::from(
                    arguments.next().context("--read-root requires a value")?,
                ));
            }
            other if other.starts_with('-') => {
                bail!("unknown argument for `celld deploy`: {other}")
            }
            other if options.config.is_none() => options.config = Some(PathBuf::from(other)),
            other => bail!("`celld deploy` takes one project path, and already has one: {other}"),
        }
    }
    Ok(Some(options))
}

#[derive(Debug)]
pub struct ActivateOptions {
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub primary: Option<String>,
    pub json: bool,
    pub scripts: Vec<VersionRef>,
}

pub fn print_activate_help() {
    let text = "celld activate — serve staged versions as one atomic generation\n\n\
USAGE:\n  celld activate SCRIPT=VERSION [SCRIPT=VERSION...] --bucket [s3://|gs://|az://]NAME[/PREFIX] [OPTIONS]\n\n\
Each version is a `celld deploy --no-activate` upload. The fleet-wide pointer is\n\
replaced by one write naming every script's version — the named ones move, the\n\
rest keep their activated versions — so nodes adopt the whole generation at\n\
once. A running node polls the deployment pointer and adopts the new version in\n\
place; nothing restarts. A concurrent deploy or activate that lands first wins;\n\
the loser heals what it converged toward the winner and fails with a re-run\n\
error. A crashed writer's residue heals on the next activate, or a deploy\n\
of the skewed script. Never run activate concurrently with another activate\n\
or deploy.\n\n\
OPTIONS:\n  --primary SCRIPT       The script ingress serves. Defaults to the serving\n                         primary, or to the one named script on a first deploy\n  --bucket [s3://|gs://|az://]NAME[/PREFIX]\n                         Fleet bucket and prefix; defaults to CELLD_BUCKET\n  --endpoint URL         S3-compatible endpoint; defaults to S3_ENDPOINT\n  --region REGION        Storage region; defaults to AWS_REGION\n  --json                 Print the activation as one JSON object\n  -h, --help             Show this help"
    ;
    let _ = crate::cli_output::Output::new(crate::cli_output::Format::Text).help(text);
}

pub fn activate_options_from_arguments(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<Option<ActivateOptions>> {
    let mut options = ActivateOptions {
        bucket: None,
        endpoint: None,
        region: None,
        primary: None,
        json: false,
        scripts: Vec::new(),
    };
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--json" => options.json = true,
            "--primary" => {
                options.primary = Some(arguments.next().context("--primary requires a value")?);
            }
            "--bucket" => {
                let value = arguments.next().context("--bucket requires a value")?;
                options.bucket = Some(value);
            }
            "--endpoint" => {
                options.endpoint = Some(arguments.next().context("--endpoint requires a value")?);
            }
            "--region" => {
                options.region = Some(arguments.next().context("--region requires a value")?);
            }
            other if other.starts_with('-') => {
                bail!("unknown argument for `celld activate`: {other}")
            }
            other => options.scripts.push(parse_version_ref(other, "activate")?),
        }
    }
    if options.scripts.is_empty() {
        bail!("`celld activate` names at least one SCRIPT=VERSION");
    }
    Ok(Some(options))
}

#[derive(Debug)]
pub struct DiscardOptions {
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub json: bool,
    pub scripts: Vec<VersionRef>,
}

pub fn print_discard_help() {
    let text = "celld discard — delete staged versions from the fleet bucket\n\n\
USAGE:\n  celld discard SCRIPT=VERSION [SCRIPT=VERSION...] --bucket [s3://|gs://|az://]NAME[/PREFIX] [OPTIONS]\n\n\
Each version is a `celld deploy --no-activate` upload that will never serve.\n\
A version any pointer names is refused; anything else is inert, so removing\n\
it is safe. Shared content-addressed objects (asset bodies, image tars) stay:\n\
other versions reference them. Never run discard concurrently with activate\n\
or deploy, and hold a superseded version past CELLD_DEPLOY_MAX_AGE_S with\n\
the replacement's adoption verified before discarding a rollback target.\n\
A commit landing after the re-check names deleted code: adoption fails loudly\n\
on the missing manifest, and re-deploying from the same source reproduces the\n\
version for a re-activate.\n\n\
OPTIONS:\n  --bucket [s3://|gs://|az://]NAME[/PREFIX]\n                         Fleet bucket and prefix; defaults to CELLD_BUCKET\n  --endpoint URL         S3-compatible endpoint; defaults to S3_ENDPOINT\n  --region REGION        Storage region; defaults to AWS_REGION\n  --json                 Print the discard as one JSON object\n  -h, --help             Show this help"
    ;
    let _ = crate::cli_output::Output::new(crate::cli_output::Format::Text).help(text);
}

pub fn discard_options_from_arguments(
    arguments: impl IntoIterator<Item = String>,
) -> anyhow::Result<Option<DiscardOptions>> {
    let mut options = DiscardOptions {
        bucket: None,
        endpoint: None,
        region: None,
        json: false,
        scripts: Vec::new(),
    };
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--json" => options.json = true,
            "--bucket" => {
                let value = arguments.next().context("--bucket requires a value")?;
                options.bucket = Some(value);
            }
            "--endpoint" => {
                options.endpoint = Some(arguments.next().context("--endpoint requires a value")?);
            }
            "--region" => {
                options.region = Some(arguments.next().context("--region requires a value")?);
            }
            other if other.starts_with('-') => {
                bail!("unknown argument for `celld discard`: {other}")
            }
            other => options.scripts.push(parse_version_ref(other, "discard")?),
        }
    }
    if options.scripts.is_empty() {
        bail!("`celld discard` names at least one SCRIPT=VERSION");
    }
    Ok(Some(options))
}

/// What a config resolves to once the allowlist has been applied.
struct Project {
    script_name: String,
    /// `no_bundle: true` uploads the entry file as written instead of running
    /// esbuild. A Vite build has already bundled the Worker, and bundling it
    /// twice is what breaks that output.
    no_bundle: bool,
    /// Entry relative to the project root. esbuild stamps this path into the
    /// bundle, so it must not depend on the working directory celld was
    /// invoked from — identical source would otherwise hash two ways.
    entry: Option<String>,
    /// What the config's `define` and `rules` add to the esbuild run.
    bundle: BundleConfig,
    assets: Option<ProjectAssets>,
    metadata: Value,
    do_classes: Vec<String>,
    sqlite_classes: Vec<String>,
    crons: Vec<String>,
    has_workflows: bool,
    has_kv: bool,
    queue_consumers: Vec<QueueConsumerConfig>,
    has_queues: bool,
    has_r2: bool,
    containers: Vec<ContainerDecl>,
    /// Set when `main` is a `.py` file: the Python behavior its
    /// compatibility date and flags select.
    python: Option<python::PythonCompat>,
}

/// The two Wrangler bundling knobs that celld forwards to esbuild.
///
/// Both are pass-throughs, so celld validates their shape and leaves their
/// meaning to esbuild. They travel together because both are build inputs and
/// neither reaches the deployment metadata: a change to either changes the
/// bundle bytes, and the deployment version hashes those.
struct BundleConfig {
    /// `define` as `--define:KEY=VALUE`. Each value is a JavaScript
    /// expression, which is what Wrangler and esbuild both take.
    define: BTreeMap<String, String>,
    /// `rules` as `--loader:.EXT=LOADER`, plus celld's own default rule.
    loaders: BTreeMap<String, Loader>,
}

/// The esbuild loaders a Wrangler module rule can ask for.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Loader {
    /// Wrangler's `Text`. The file becomes a string in the bundle.
    Text,
    /// Wrangler's `Data`. The file becomes a byte array in the bundle.
    Binary,
    /// Wrangler's `CompiledWasm`. The file becomes a sibling module that the
    /// runtime serves as a compiled `WebAssembly.Module`.
    Copy,
}

impl Loader {
    fn esbuild_name(self) -> &'static str {
        match self {
            Loader::Text => "text",
            Loader::Binary => "binary",
            Loader::Copy => "copy",
        }
    }

    /// The `rules[].type` a config writes for this loader. An error names the
    /// config's own word, not celld's internal one.
    fn rule_type(self) -> &'static str {
        match self {
            Loader::Text => "Text",
            Loader::Binary => "Data",
            Loader::Copy => "CompiledWasm",
        }
    }
}

impl Default for BundleConfig {
    fn default() -> Self {
        // Wasm is a sibling module (Wrangler's built-in `CompiledWasm` rule).
        // The `copy` loader makes esbuild resolve each wasm import like any
        // other import (importer-relative, node_modules, deduplicated) and
        // rewrite the specifier to the copied file, so the bundle and the
        // emitted files agree on names. It lives in the same map as the
        // configured rules, so a config that gives `.wasm` a conflicting rule
        // is refused by the same check that catches two conflicting rules,
        // rather than emitting two `--loader` arguments for one extension.
        Self {
            define: BTreeMap::new(),
            loaders: BTreeMap::from([(".wasm".to_string(), Loader::Copy)]),
        }
    }
}

/// One `containers[]` entry as written, before its image is resolved.
#[derive(Clone, Debug, serde::Serialize)]
struct ContainerDecl {
    class_name: String,
    /// A Dockerfile path relative to the project, or an image reference.
    image: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    instance_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_instances: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runtime: Option<String>,
}

struct ProjectAssets {
    directory: PathBuf,
    config: AssetConfig,
    raw_metadata: Value,
}

pub struct BuiltAssets {
    pub index: Vec<u8>,
    pub blobs: BTreeMap<String, Vec<u8>>,
    pub file_count: u32,
    pub total_bytes: u64,
}

/// The built deployment, before anything is written.
pub struct Built {
    pub script_name: String,
    pub version: String,
    pub prefix: String,
    pub manifest: Manifest,
    pub modules: Vec<(String, Vec<u8>)>,
    pub assets: Option<BuiltAssets>,
    pub bundled_in: Duration,
    /// The resolved image ids of `containers[]`, in config order.
    pub images: Vec<String>,
    /// Save each image to the bucket. `celld dev` leaves them in the local
    /// engine, which is where the node loads them from.
    /// See `Manifest::fence_image`; saved beside `images`, reported apart.
    pub fence_image: Option<String>,
    pub upload_images: bool,
    /// The entry's source map, when `--sourcemap` asked for one and the build
    /// produced one: the esbuild bundle's external map, or the `no_bundle`
    /// entry's sibling map. Never part of the deployment identity: the
    /// version hashes the modules only.
    pub sourcemap: Option<Vec<u8>>,
}

/// Write the built map to the `--sourcemap` path. The deploy command calls
/// this after a successful build, including dry runs, and before anything
/// reaches the bucket, so a bad path fails the deploy without publishing.
/// Warns and writes nothing when the build produced no map.
pub fn write_sourcemap(options: &Options, built: &Built) -> anyhow::Result<()> {
    let Some(path) = &options.sourcemap else {
        return Ok(());
    };
    match &built.sourcemap {
        Some(map) => {
            std::fs::write(path, map)
                .with_context(|| format!("write source map to {}", path.display()))?;
            note!("Wrote source map to {}", path.display());
        }
        None => note!(
            "--sourcemap: this deployment has no source map to write \
             (asset-only, Python, or `no_bundle` without a sibling map); wrote nothing"
        ),
    }
    Ok(())
}

impl Built {
    pub fn bytes(&self) -> usize {
        self.modules.iter().map(|(_, body)| body.len()).sum()
    }

    /// What the deployment weighs, the bindings it will have, and nothing we
    /// cannot stand behind: no URL, because celld routes nothing, and no
    /// startup time, because deploying does not start an isolate.
    pub fn report(&self) {
        note!(" celld {}", env!("CARGO_PKG_VERSION"));
        note!("{}", "─".repeat(47));
        note!(
            "Total Upload: {} / gzip: {}",
            kib(self.bytes()),
            kib(gzipped(&self.modules)),
        );
        if let Some(assets) = &self.assets {
            note!(
                "Static Assets: {} files / {} ({} unique bodies)",
                assets.file_count,
                kib(assets.total_bytes as usize),
                assets.blobs.len(),
            );
        }
        if !self.images.is_empty() {
            note!("Containers: {} image(s)", self.images.len());
        }
        let bindings = self.bindings();
        if bindings.is_empty() {
            note!("Your Worker has no bindings.");
        } else {
            let width = bindings
                .iter()
                .map(|(binding, _)| binding.len())
                .chain(std::iter::once("Binding".len()))
                .max()
                .unwrap_or_default()
                + 6;
            note!("Your Worker has access to the following bindings:");
            note!("{:width$}Resource", "Binding");
            for (binding, resource) in bindings {
                note!("{binding:width$}{resource}");
            }
        }
        note!(
            "Bundled {} ({})",
            self.script_name,
            seconds(self.bundled_in)
        );
    }

    /// `env.NAME (Class)` against the resource it resolves to, the way
    /// Wrangler renders it.
    fn bindings(&self) -> Vec<(String, String)> {
        self.manifest
            .raw_metadata
            .get("bindings")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .filter_map(|binding| {
                let name = binding.get("name").and_then(Value::as_str)?;
                match binding.get("type").and_then(Value::as_str) {
                    Some("assets") => {
                        Some((format!("env.{name} (Assets)"), "Static Assets".to_string()))
                    }
                    Some("durable_object_namespace") => {
                        let class = binding.get("class_name").and_then(Value::as_str)?;
                        let sqlite = self.manifest.sqlite_classes.iter().any(|c| c == class);
                        Some((
                            format!("env.{name} ({class})"),
                            match sqlite {
                                true => "Durable Object (SQLite)".to_string(),
                                false => "Durable Object".to_string(),
                            },
                        ))
                    }
                    Some("service") => {
                        let service = binding.get("service").and_then(Value::as_str)?;
                        let target = binding
                            .get("entrypoint")
                            .and_then(Value::as_str)
                            .map_or_else(
                                || service.to_string(),
                                |entrypoint| format!("{service}#{entrypoint}"),
                            );
                        Some((format!("env.{name} (Service)"), target))
                    }
                    Some("d1") => {
                        let database = binding.get("database_name").and_then(Value::as_str)?;
                        Some((format!("env.{name} (D1)"), database.to_string()))
                    }
                    Some("workflow") => {
                        let workflow = binding.get("workflow_name").and_then(Value::as_str)?;
                        Some((format!("env.{name} (Workflow)"), workflow.to_string()))
                    }
                    Some("kv") => {
                        let id = binding.get("id").and_then(Value::as_str)?;
                        Some((format!("env.{name} (KV)"), id.to_string()))
                    }
                    Some("queue") => {
                        let queue = binding.get("queue").and_then(Value::as_str)?;
                        Some((format!("env.{name} (Queue)"), queue.to_string()))
                    }
                    Some("r2_bucket") => {
                        let bucket = binding.get("bucket_name").and_then(Value::as_str)?;
                        Some((format!("env.{name} (R2)"), bucket.to_string()))
                    }
                    Some("worker_loader") => Some((
                        format!("env.{name} (Worker Loader)"),
                        "Dynamic Workers".to_string(),
                    )),
                    Some("plain_text") => Some((
                        format!("env.{name} (Text)"),
                        "Environment Variable".to_string(),
                    )),
                    _ => None,
                }
            })
            .collect()
    }
}

fn kib(bytes: usize) -> String {
    format!("{:.2} KiB", bytes as f64 / 1024.0)
}

fn seconds(elapsed: Duration) -> String {
    format!("{:.2} sec", elapsed.as_secs_f64())
}

fn gzipped(modules: &[(String, Vec<u8>)]) -> usize {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    for (_, body) in modules {
        encoder.write_all(body).ok();
    }
    encoder.finish().map(|out| out.len()).unwrap_or_default()
}

pub fn build(options: &Options) -> anyhow::Result<Built> {
    let (config_path, root) = resolve_project_config(options.config.clone())?;
    let bounds = ReadBounds::new(&root, &options.read_roots)?;
    let mut project = read_project(&config_path, &root, &options.vars)?;
    // Resolve every image before the version is computed: the id is part of
    // the deployment's identity, so a rebuilt image is a new deployment.
    // A fleet deploy builds for the platform the nodes run, which Wrangler
    // also fixes at linux/amd64; a machine that builds for itself, as
    // `celld dev` does, passes none.
    let platform = (!options.local_images).then(|| {
        std::env::var("CELLD_CONTAINER_PLATFORM").unwrap_or_else(|_| "linux/amd64".to_string())
    });
    let images = project
        .containers
        .iter()
        .enumerate()
        .map(|(index, decl)| {
            resolve_image(
                &bounds,
                &decl.image,
                &format!("config `containers[{index}].image`"),
                platform.as_deref(),
            )
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let container_specs: Vec<ContainerSpec> = project
        .containers
        .iter()
        .zip(&images)
        .map(|(decl, image)| ContainerSpec {
            class_name: decl.class_name.clone(),
            image: image.clone(),
            instance_type: decl.instance_type.clone(),
            max_instances: decl.max_instances,
            runtime: decl.runtime.clone(),
        })
        .collect();
    if !container_specs.is_empty() {
        project.metadata["containers"] = json!(container_specs);
    }
    // The fence image rides with every deployment that has containers: a
    // node runs it once, privileged, to fence its bridges before the first
    // container starts. Built like an app image, keyed by content, so every
    // deployment made by one celld shares one tar.
    let fence_image = (!container_specs.is_empty())
        .then(|| resolve_fence_image(platform.as_deref()))
        .transpose()?;
    let started = Instant::now();
    let built_assets = project.assets.as_ref().map(build_assets).transpose()?;
    let bundle = project
        .entry
        .as_deref()
        .map(|entry| {
            if let Some(compat) = &project.python {
                let (output, descriptor) = python::build(&root, entry, compat)?;
                project.metadata["python_runtime"] = descriptor;
                Ok(output)
            } else if project.no_bundle {
                // Already bundled by the caller's toolchain. Read it as it is;
                // running esbuild over a Vite build is what corrupts it.
                // The read goes through the contained path, not the raw join:
                // a directory link above the entry would otherwise redirect
                // both this read and the wasm scan below out of the project.
                let path = contained_entry(&root, entry)?;
                let bundle = std::fs::read(&path)
                    .with_context(|| format!("read entry point {}", path.display()))?;
                let mut wasm = Vec::new();
                collect_unbundled_wasm(
                    path.parent().context("entry has no directory")?,
                    "",
                    &path,
                    &mut wasm,
                )?;
                // Directory iteration order must not change deployment identity.
                wasm.sort_by(|a, b| a.0.cmp(&b.0));
                // Pre-bundled: the caller's toolchain owns the bytes, so
                // celld produces no map for them — but the entry deploys as
                // written, so its sibling `<entry>.map` still describes the
                // deployed module, and `--sourcemap` collects it.
                let sourcemap = options
                    .sourcemap
                    .is_some()
                    .then(|| sibling_sourcemap(&path, &root))
                    .flatten();
                Ok(BundleOutput {
                    bundle,
                    wasm,
                    sourcemap,
                })
            } else {
                // esbuild resolves the entry itself, so it is handed the
                // contained path rather than the raw config value.
                let path = contained_entry(&root, entry)?;
                run_esbuild(
                    &root,
                    &bounds,
                    &path,
                    &project.bundle,
                    options.sourcemap.is_some(),
                )
            }
        })
        .transpose()?;
    let bundled_in = started.elapsed();

    // Both build paths emit one JS module and its sibling wasm modules.
    let module_name = "index.js".to_string();
    let (mut modules, wasm_modules, sourcemap) = match bundle {
        Some(output) => (
            vec![(module_name.clone(), output.bundle)],
            output.wasm,
            output.sourcemap,
        ),
        None => (Vec::new(), Vec::new(), None),
    };
    let wasm_names: BTreeSet<String> = wasm_modules.iter().map(|(name, _)| name.clone()).collect();
    modules.extend(wasm_modules);
    // Identity is over the exact metadata bytes the manifest retains, so the
    // serialization happens once and is reused for both.
    let metadata_json = serde_json::to_vec(&project.metadata)?;
    let version = crate::protocol::deployment_version(
        &modules,
        &metadata_json,
        built_assets.as_ref().map(|assets| assets.index.as_slice()),
    );
    let prefix = format!("deploy/{}/{}", project.script_name, version);
    let asset_reference = built_assets.as_ref().map(|assets| AssetManifestRef {
        index: "assets.json".to_string(),
        sha256: format!("{:x}", Sha256::digest(&assets.index)),
        file_count: assets.file_count,
        total_bytes: assets.total_bytes,
    });
    let sqlite_vec = project
        .metadata
        .get("compatibility_flags")
        .and_then(Value::as_array)
        .is_some_and(|flags| flags.iter().any(|flag| flag.as_str() == Some("sqlite_vec")));
    let uses_d1 = project.do_classes.iter().any(|class| class == D1_CLASS);
    let python_runtime = project.python.is_some();
    let manifest = Manifest {
        schema_version: if asset_reference.is_some() { 2 } else { 1 },
        version: version.clone(),
        script_name: project.script_name.clone(),
        main_module: project.entry.as_ref().map(|_| module_name.clone()),
        do_classes: project.do_classes,
        sqlite_classes: project.sqlite_classes,
        modules: modules
            .iter()
            .map(|(name, bytes)| ModuleRef {
                name: name.clone(),
                bytes: bytes.len(),
                sha256: format!("{:x}", Sha256::digest(bytes)),
                kind: wasm_names.contains(name).then_some(ModuleKind::Wasm),
            })
            .collect(),
        assets: asset_reference,
        crons: project.crons.clone(),
        containers: container_specs.clone(),
        fence_image: fence_image.clone(),
        queue_consumers: project.queue_consumers,
        // Each capability the manifest depends on is named here, so a node
        // that predates it rejects the deployment up front instead of
        // partially deserializing the manifest and failing at worker load.
        required_features: {
            let mut features = Vec::new();
            if built_assets.is_some() {
                features.push(FEATURE_ASSETS_V1.to_string());
            }
            if !project.crons.is_empty() {
                features.push(FEATURE_CRON_V1.to_string());
            }
            if !container_specs.is_empty() {
                features.push(FEATURE_CONTAINERS_V1.to_string());
            }
            if uses_d1 {
                features.push(FEATURE_D1_V1.to_string());
            }
            if project.has_workflows {
                features.push(FEATURE_WORKFLOWS_V1.to_string());
            }
            if project.has_kv {
                features.push(FEATURE_KV_V1.to_string());
            }
            if project.has_queues {
                features.push(FEATURE_QUEUES_V1.to_string());
            }
            if project.has_r2 {
                features.push(FEATURE_R2_V1.to_string());
            }
            if sqlite_vec {
                features.push(FEATURE_SQLITE_VEC_V1.to_string());
            }
            if !wasm_names.is_empty() {
                features.push(FEATURE_WASM_V1.to_string());
            }
            if python_runtime {
                features.push(FEATURE_PYTHON_WORKERS_V1.to_string());
            }
            features
        },
        raw_metadata: project.metadata,
    };
    Ok(Built {
        script_name: project.script_name,
        version,
        prefix,
        manifest,
        modules,
        assets: built_assets,
        bundled_in,
        images,
        fence_image,
        upload_images: !options.local_images,
        sourcemap,
    })
}

/// The Dockerfile `image` names, when it names one rather than an image
/// reference: a project-relative path to a regular file, resolved inside the
/// project root. Wrangler draws the same line between a path and a reference.
/// Anything else is a reference — including a symlink, which is never
/// followed here. A value that is absolute or climbs with `..` is a path, never
/// a reference: it must name a regular file inside an operator's read root,
/// and is refused otherwise. It is joined lexically and the filesystem is
/// probed only once it lands inside a bound, since branching on whether a
/// path outside them exists would leak host file existence one deploy error
/// at a time.
fn dockerfile_path(
    bounds: &ReadBounds,
    image: &str,
    what: &str,
) -> anyhow::Result<Option<PathBuf>> {
    let root = bounds.project();
    let Ok(relative) = project_relative_path(image, what) else {
        // One refusal for every failure here, so a value cannot tell a path
        // outside the bounds from a missing file inside them.
        let refuse = || {
            anyhow!(
                "{what} must be a project-relative path, an image reference, or a \
                 regular file inside a read root: {image:?}"
            )
        };
        let path = lexical_join(root, image);
        if !bounds.contains(&path) {
            return Err(refuse());
        }
        let regular =
            std::fs::symlink_metadata(&path).is_ok_and(|metadata| metadata.file_type().is_file());
        if !regular {
            return Err(refuse());
        }
        return bounds.contain(&path, what).map(Some);
    };
    let path = root.join(relative);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error).with_context(|| format!("inspect {}", path.display()));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Ok(None);
    }
    bounds.contain(&path, what).map(Some)
}

/// `base` joined with `value` with `.` and `..` resolved by name alone, never
/// by the filesystem. An absolute `value` replaces `base`.
fn lexical_join(base: &Path, value: &str) -> PathBuf {
    let mut path = base.to_path_buf();
    for component in Path::new(value).components() {
        match component {
            std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                path = PathBuf::from(component.as_os_str());
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                path.pop();
            }
            std::path::Component::Normal(name) => path.push(name),
        }
    }
    path
}

fn engine_cli() -> String {
    std::env::var("CELLD_DOCKER").unwrap_or_else(|_| "docker".to_string())
}

/// Run the engine CLI: `cli` is the program and any leading arguments.
fn run_engine_cli(cli: &[&str], args: &[&str], what: &str) -> anyhow::Result<String> {
    let (program, leading) = cli
        .split_first()
        .context("the engine CLI names no program")?;
    let output = Command::new(program)
        .args(leading)
        .args(args)
        .stdin(std::process::Stdio::null())
        .output()
        .with_context(|| format!("{what}: run `{program}`; is a Docker or Podman CLI on PATH?"))?;
    if !output.status.success() {
        bail!(
            "{what} failed:\n{}",
            String::from_utf8_lossy(&output.stderr).trim_end()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// The fleet reference of the image a `containers[].image` names: build
/// the Dockerfile, or pull the reference, then tag the result by its
/// content. A platform pins the build and the pull to the nodes'
/// architecture; without one the engine's own is used, and a reference the
/// engine already holds is not pulled again.
///
/// The engine's own image id is not the identity. Docker's containerd
/// image store mints a new id for every build of identical content, so an
/// id would make each deploy a new deployment version and a new tar in the
/// bucket. The layer digests and the image config are stable across such
/// builds, so their hash names the image everywhere: in the manifest, in
/// the bucket key, and as the tag every engine holds it under.
fn resolve_image(
    bounds: &ReadBounds,
    image: &str,
    what: &str,
    platform: Option<&str>,
) -> anyhow::Result<String> {
    resolve_image_cli(&[&engine_cli()], bounds, image, what, platform)
}

/// `resolve_image` against an explicit engine CLI: the program and any
/// leading arguments. `what` names the config value in the error when the
/// value escapes the project.
fn resolve_image_cli(
    cli: &[&str],
    bounds: &ReadBounds,
    image: &str,
    what: &str,
    platform: Option<&str>,
) -> anyhow::Result<String> {
    let run_engine = |args: &[&str], what: &str| run_engine_cli(cli, args, what);
    let platform_args = platform
        .map(|platform| vec!["--platform", platform])
        .unwrap_or_default();
    // The Dockerfile is canonical and inside a bound, so its directory, the
    // build context, is too.
    let id = if let Some(path) = dockerfile_path(bounds, image, what)? {
        let context = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| bounds.project().to_path_buf());
        let path = path.display().to_string();
        let context = context.display().to_string();
        let mut args = vec!["build", "-q"];
        args.extend(&platform_args);
        args.extend(["-f", &path, &context]);
        run_engine(&args, "build container image")?
    } else {
        let inspect = ["image", "inspect", "--format", "{{.Id}}", image];
        let held = platform.is_none() && run_engine(&inspect, "inspect container image").is_ok();
        if !held {
            let mut args = vec!["pull"];
            args.extend(&platform_args);
            args.push(image);
            run_engine(&args, "pull container image")?;
        }
        run_engine(&inspect, "inspect container image")?
    };
    let content = run_engine(
        &[
            "image",
            "inspect",
            "--format",
            "{{json .RootFS.Layers}}{{json .Config}}",
            &id,
        ],
        "inspect container image",
    )?;
    let key = format!("{:x}", Sha256::digest(&content));
    let reference = crate::container::image_reference(&key);
    if let Err(tag_error) = run_engine(&["tag", &id, &reference], "tag container image") {
        // Two deploys of one image tag the same content reference at once.
        // The engine replaces a tag by deleting then creating it, so the
        // loser can fail with AlreadyExists after the winner recreates the
        // tag. The tag names the content, so re-inspect it: when it holds
        // the content just built, the concurrent deploy won and this one
        // continues; a different image under the tag still fails.
        if !format!("{tag_error:?}").contains("AlreadyExists") {
            return Err(tag_error);
        }
        let existing = match run_engine(
            &[
                "image",
                "inspect",
                "--format",
                "{{json .RootFS.Layers}}{{json .Config}}",
                &reference,
            ],
            "inspect container image",
        ) {
            Ok(existing) => existing,
            Err(_) => return Err(tag_error),
        };
        if format!("{:x}", Sha256::digest(&existing)) != key {
            return Err(tag_error).with_context(|| {
                format!("the container image tag {reference} names a different image")
            });
        }
    }
    Ok(reference)
}

/// The image the node fences its bridges with: `nft` and nothing else.
/// Pinned so the content key, and with it the tar in the bucket, is
/// stable across deployments.
const FENCE_DOCKERFILE: &str = "FROM alpine:3.20\nRUN apk add --no-cache nftables\n";

/// Build the fence image for `platform` and tag it by content, as
/// `resolve_image` does for an application image.
fn resolve_fence_image(platform: Option<&str>) -> anyhow::Result<String> {
    let context = tempfile::tempdir().context("fence image build context")?;
    std::fs::write(context.path().join("Dockerfile"), FENCE_DOCKERFILE)
        .context("write fence Dockerfile")?;
    resolve_image(
        &ReadBounds::new(context.path(), &[])?,
        "Dockerfile",
        "the fence Dockerfile",
        platform,
    )
    .context("build the celld-fence image")
}

/// Save the image to the bucket under its content key unless it is there.
/// Saving by the reference writes the tag into the tar, so a node's load
/// holds the image under the same name.
async fn ensure_image_tar(bucket: &Bucket, image: &str) -> anyhow::Result<()> {
    let key = crate::container::image_key(image);
    if bucket.head(&key).await?.is_some() {
        return Ok(());
    }
    let output = Command::new(engine_cli())
        .args(["save", image])
        .stdin(std::process::Stdio::null())
        .output()
        .context("save container image")?;
    if !output.status.success() {
        bail!(
            "save container image failed:\n{}",
            String::from_utf8_lossy(&output.stderr).trim_end()
        );
    }
    bucket.put(&key, output.stdout).await
}

pub async fn write(bucket: &Bucket, built: &Built) -> anyhow::Result<()> {
    // The fleet pointer's token before anything moves: the commit below
    // CASes on it, so a concurrent deploy or activate that lands first
    // produces a loser, not a lost write.
    let commit_token = bucket
        .head(crate::fleet::CURRENT_POINTER_KEY)
        .await?
        .map(|(_, token)| token);
    // Read every attachment before uploading immutable deployment objects.
    // A competing consumer is a deploy refusal, so it must not leave a new
    // version in the bucket that an operator can mistake for a published one.
    let queue_attachments = prepare_queue_attachments(bucket, &built.manifest).await?;
    upload_version(bucket, built).await?;

    // An attachment names this exact immutable prefix. Publish it only after
    // the prefix is complete, so a node can never resolve a consumer to a
    // half-uploaded deployment.
    //
    // Everything below moves node-visible state, so any error heals
    // first: the current pointer is re-read and the attachments and named
    // pointers converge toward it before the error returns. A lost commit
    // race therefore heals toward the winner instead of wedging it.
    let commit: anyhow::Result<Vec<u8>> = async {
        publish_queue_attachments(
            bucket,
            &built.manifest,
            &built.prefix,
            queue_attachments,
            "deploy",
        )
        .await?;

        let pointer = DeployPointer {
            script_name: Some(built.script_name.clone()),
            version: built.version.clone(),
            prefix: built.prefix.clone(),
            rollout: Rollout { percent: 100 },
            components: None,
        };
        let encoded = serde_json::to_vec_pretty(&pointer)?;
        commit_pointer(
            bucket,
            crate::fleet::CURRENT_POINTER_KEY,
            encoded.clone(),
            commit_token.as_deref(),
            "deploy",
        )
        .await?;
        Ok(encoded)
    }
    .await;
    let encoded = match commit {
        Ok(encoded) => encoded,
        Err(error) => return Err(heal_on_error(bucket, error, "deploy").await),
    };
    // The named pointer resolves service-binding components; it moves
    // AFTER the commit, so a deploy that never commits leaves every
    // named pointer at its committed version. A failure here lands after
    // the commit: the deployment is serving, and the error says to
    // re-run to converge.
    if let Err(error) = put_pointer(
        bucket,
        &format!("deploy/{}/current.json", built.script_name),
        encoded,
        "deploy",
    )
    .await
    {
        if let Err(heal) = heal_derivative_state(bucket, "deploy").await {
            note!("warning: healing toward the serving pointer failed: {heal:#}");
        }
        anyhow::bail!(
            "the deployment committed, but its named pointer did not converge: {error:#}; \
             re-run `celld deploy` to converge it"
        );
    }
    Ok(())
}

/// Upload the immutable version objects — images, asset bodies, modules,
/// the asset index, the manifest — without moving anything that serving
/// reads: no queue attachment, no named pointer, no fleet pointer.
///
/// This is `celld deploy --no-activate`. The version is complete and
/// validated (bundling already failed the tenant errors), but the next
/// adoption cannot see it: the pointer watcher and `POST /reload` both
/// read `deploy/current.json`, and the graph walk reads the named
/// pointers and the queue attachments, all untouched. `celld activate`
/// moves them all at once; `celld discard` removes the version.
pub async fn write_staged(bucket: &Bucket, built: &Built) -> anyhow::Result<()> {
    // No attachment prepare here, deliberately: it reads the *current*
    // named pointer, but the attachments publish at activate time against
    // whatever is current then. Validating now would refuse a queue whose
    // owner is itself replaced by the same activation. `activate`
    // prepares and publishes together, still before its pointer write.
    upload_version(bucket, built).await
}

/// One `SCRIPT=VERSION` argument to `celld activate` or `celld discard`.
/// The version is a deployment version as `celld deploy` printed it, not a
/// pointer: the script disambiguates, because a version alone names no
/// script and two scripts hash independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionRef {
    pub script: String,
    pub version: String,
}

pub(crate) fn parse_version_ref(argument: &str, command: &str) -> anyhow::Result<VersionRef> {
    let Some((script, version)) = argument.split_once('=') else {
        bail!("`celld {command}` takes SCRIPT=VERSION arguments, not {argument:?}");
    };
    anyhow::ensure!(
        valid_script_name(script),
        "invalid script name {script:?}: lowercase ASCII letters, digits, and internal hyphens, \
         at most {MAX_SCRIPT_NAME_BYTES} bytes"
    );
    anyhow::ensure!(
        crate::protocol::valid_deployment_version(version),
        "invalid version {version:?}: a deployment version as `celld deploy` printed it, 16 \
         lowercase hex characters"
    );
    Ok(VersionRef {
        script: script.to_string(),
        version: version.to_string(),
    })
}

/// What `celld activate` concluded.
#[derive(Debug)]
pub struct Activated {
    /// The primary script: the one ingress serves.
    pub primary: String,
    pub version: String,
    pub prefix: String,
    /// Every script's activated version, as written to the fleet pointer.
    pub components: BTreeMap<String, ComponentRef>,
    /// The pointer already named this exact generation; nothing was
    /// rewritten, but the attachments and named pointers were converged.
    pub unchanged: bool,
}

/// Activate staged versions as one atomic generation.
///
/// Every named `SCRIPT=VERSION` must exist in the bucket (a `celld deploy
/// --no-activate` wrote it); scripts the call does not name keep their
/// currently activated versions. Validation (staged manifests present
/// and identified, primary resolvable, no competing queue consumer)
/// happens first, then the queue attachments converge, and the
/// fleet-wide pointer is replaced by one compare-and-swap write naming
/// the primary script and every script's version, so the poll watcher
/// and `POST /reload` adopt either the whole old generation's code or the
/// whole new one's — never a mix of code. (The asset fallback on a miss
/// may serve a newer committed deployment's assets during a rolling
/// restart; those bytes are immutable and content-verified.) The named
/// pointers converge after the commit: a generation's code resolves from
/// the pointer's map, never from the named pointers, so the post-commit
/// convergence moves no code a new node adopts; the named pointers serve
/// older nodes, later plain deploys, and the asset fallback.
///
/// The commit CASes on the token of the opening pointer read: a
/// concurrent deploy or activate that moved the pointer first wins, and
/// the loser heals what it converged toward the winner before its re-run
/// error returns, instead of overwriting it. Any error after convergence
/// started heals the same way; a crash cannot heal itself, but the next
/// activate — or a deploy of the skewed script — converges the same
/// state toward its own generation, which heals crash residue. A plain
/// deploy converges only its own script's attachments, so residue on
/// another script's queue survives it. Never run writers concurrently:
/// only sequential runs converge cleanly.
pub async fn activate(
    bucket: &Bucket,
    scripts: &[VersionRef],
    primary: Option<&str>,
) -> anyhow::Result<Activated> {
    anyhow::ensure!(
        !scripts.is_empty(),
        "celld activate names at least one SCRIPT=VERSION"
    );
    if let Some(primary) = primary {
        anyhow::ensure!(
            valid_script_name(primary),
            "invalid primary script name {primary:?}: lowercase ASCII letters, digits, and \
             internal hyphens, at most {MAX_SCRIPT_NAME_BYTES} bytes"
        );
    }
    let mut seen = BTreeSet::new();
    for reference in scripts {
        anyhow::ensure!(
            seen.insert(reference.script.clone()),
            "script {:?} is named twice; one version per script",
            reference.script
        );
    }

    // The token is the commit's compare-and-swap precondition: the
    // pointer must still be this value when the fleet pointer moves, or a
    // concurrent writer landed first and this activation re-runs.
    let (current, commit_token): (Option<DeployPointer>, Option<String>) =
        match bucket.get(crate::fleet::CURRENT_POINTER_KEY).await? {
            Some((body, token)) => (
                Some(serde_json::from_slice(&body).context("decode deploy/current.json")?),
                Some(token),
            ),
            None => (None, None),
        };

    // Every named version must exist and identify itself before anything
    // moves. The prefix is deterministic — `deploy/<script>/<version>` —
    // so a staged version needs no registry to be found.
    let mut staged = Vec::with_capacity(scripts.len());
    for reference in scripts {
        let prefix = format!("deploy/{}/{}", reference.script, reference.version);
        let key = format!("{prefix}/manifest.json");
        let Some((body, _)) = bucket.get(&key).await? else {
            bail!(
                "no staged version {:?} of script {:?}; run `celld deploy --no-activate` first",
                reference.version,
                reference.script
            );
        };
        let manifest: Manifest =
            serde_json::from_slice(&body).with_context(|| format!("decode {key}"))?;
        anyhow::ensure!(
            manifest.script_name == reference.script,
            "staged version {:?} identifies script {:?}, not {:?}",
            reference.version,
            manifest.script_name,
            reference.script
        );
        anyhow::ensure!(
            manifest.version == reference.version,
            "staged version {:?} of script {:?} holds manifest version {:?}",
            reference.version,
            reference.script,
            manifest.version
        );
        staged.push((reference.clone(), prefix, manifest));
    }

    // Scripts the call does not name keep what is activated now. A
    // components generation carries itself; a legacy pointer (plain
    // `celld deploy`, or a managed apply) carries its primary from the
    // top level — what serves today — and every other script from its
    // named pointer. Carried entries are trusted, not re-verified: their
    // manifests are what the fleet already serves, and a stale named
    // pointer for a script nothing references must not block an
    // activation that does not touch it.
    let mut components: BTreeMap<String, ComponentRef> = BTreeMap::new();
    if let Some(pointer) = &current {
        if let Some(generation) = &pointer.components {
            components.clone_from(generation);
        } else {
            for (script, component) in legacy_components(bucket).await? {
                components.insert(script, component);
            }
            let primary_script = pointer.script_name.as_deref().with_context(|| {
                "deploy/current.json names no primary script; the pointer is corrupt"
            })?;
            components.insert(
                primary_script.to_string(),
                ComponentRef {
                    version: pointer.version.clone(),
                    prefix: pointer.prefix.clone(),
                },
            );
        }
    }
    for (reference, prefix, _) in &staged {
        components.insert(
            reference.script.clone(),
            ComponentRef {
                version: reference.version.clone(),
                prefix: prefix.clone(),
            },
        );
    }

    let primary = primary
        .map(str::to_string)
        .or_else(|| {
            current
                .as_ref()
                .and_then(|pointer| pointer.script_name.clone())
        })
        .or_else(|| (scripts.len() == 1).then(|| scripts[0].script.clone()))
        .with_context(|| {
            "no fleet pointer yet and several scripts named; pass --primary SCRIPT to choose \
             the one ingress serves"
        })?;
    let primary_component = components.get(&primary).with_context(|| {
        format!("primary script {primary:?} names no activated version; name it as SCRIPT=VERSION")
    })?;
    let desired = DeployPointer {
        script_name: Some(primary.clone()),
        version: primary_component.version.clone(),
        prefix: primary_component.prefix.clone(),
        rollout: Rollout { percent: 100 },
        components: Some(components.clone()),
    };

    // Convergence, before the commit: the staged scripts' queue
    // attachments validate handoff-aware across all staged manifests at
    // once — a queue handed from one staged script to another resolves
    // the same regardless of CLI argument order — and publish, then the
    // carried scripts' attachments converge to the generation about to
    // commit. Adoption fails closed on a stale attachment, so the new
    // attachments must be in place before any node can adopt. All
    // idempotent, so a re-run repairs a partial one. The named pointers
    // do NOT converge here: they move after the commit, so an activation
    // that never commits leaves every named pointer at its committed
    // version, and a node polling mid-activation still reads the old
    // generation whole.
    let carried = carried_manifests(bucket, &staged, &components).await?;
    let activation_queues = prepare_activation_queues(bucket, &staged, &carried).await?;
    // Everything below moves node-visible state, so any error heals
    // first: the current pointer is re-read and the attachments and
    // named pointers converge toward it before the error returns. A
    // lost commit race therefore heals toward the winner instead of
    // wedging it.
    let commit: anyhow::Result<bool> = async {
        publish_activation_queues(bucket, activation_queues).await?;
        converge_carried_queues(bucket, &carried).await?;
        if std::env::var("CELLD_TEST_ACTIVATE_ABORT").as_deref() == Ok("1") {
            bail!("injected activate abort after convergence, before the fleet-pointer commit");
        }
        // The commit point: one pointer write, CASed on the opening
        // read. A concurrent deploy or activate that moved the pointer
        // first wins; the loser heals and re-runs. When the pointer
        // already names this generation the write is skipped, but the
        // convergence above still ran: attachments skewed from under a
        // committed generation heal on every activate, including a
        // re-run.
        let unchanged = current.as_ref().is_some_and(|pointer| {
            pointer.script_name.as_deref() == Some(primary.as_str())
                && pointer.version == desired.version
                && pointer.prefix == desired.prefix
                && pointer.components.as_ref() == desired.components.as_ref()
        });
        if !unchanged {
            if std::env::var("CELLD_TEST_DISCARD_MID_ACTIVATE").as_deref() == Ok("1") {
                // A discard that landed after the opening reads and
                // before the commit: the manifest re-check below must
                // refuse to commit a generation naming deleted code.
                // Deletes the first staged script's manifest outright,
                // exactly what the discard's delete leaves behind.
                let (reference, prefix, _) = &staged[0];
                bucket
                    .delete(&format!("{prefix}/manifest.json"))
                    .await
                    .with_context(|| {
                        format!(
                            "delete the injected discard victim {}={}",
                            reference.script, reference.version
                        )
                    })?;
            }
            verify_precommit_manifests(bucket, &staged, &components).await?;
            commit_pointer(
                bucket,
                crate::fleet::CURRENT_POINTER_KEY,
                serde_json::to_vec_pretty(&desired)?,
                commit_token.as_deref(),
                "activate",
            )
            .await?;
        }
        Ok(unchanged)
    }
    .await;
    let unchanged = match commit {
        Ok(unchanged) => unchanged,
        Err(error) => return Err(heal_on_error(bucket, error, "activate").await),
    };

    // The named pointers converge AFTER the commit. A generation's
    // components resolve from the pointer's map — never from the named
    // pointers — so this moves nothing a new node adopts; it serves
    // older nodes and later plain deploys. On the unchanged path the
    // pointer already named this generation, and this still heals skew
    // from under it. A failure here lands after the commit: the
    // generation is serving, and the error says to re-run to converge.
    if let Err(error) = converge_named_pointers(bucket, &components, "activate").await {
        if let Err(heal) = heal_derivative_state(bucket, "activate").await {
            note!("warning: healing toward the serving pointer failed: {heal:#}");
        }
        if unchanged {
            anyhow::bail!(
                "the serving generation's named pointers did not converge: {error:#}; \
                 re-run `celld activate` to converge them"
            );
        }
        anyhow::bail!(
            "the generation committed, but its named pointers did not converge: {error:#}; \
             re-run `celld activate` to converge them"
        );
    }

    Ok(Activated {
        primary,
        version: desired.version,
        prefix: desired.prefix,
        components,
        unchanged,
    })
}

pub(crate) struct ActivationQueueState {
    queue: String,
    key: String,
    current: Option<QueueConsumerAttachment>,
    token: Option<String>,
    final_consumer: Option<QueueConsumerDeployment>,
}

/// The queue attachments an activation will publish, validated before
/// anything moves.
///
/// Every staged manifest's desired queues resolve against one fresh
/// read each: two staged scripts consuming one queue fail, and a queue
/// owned by a script the activation does not name fails, but a handoff
/// between two staged scripts is allowed — the releasing script's new
/// manifest drops the queue, so the current owner being staged means
/// the queue is genuinely changing hands. Argument order plays no
/// part: no publish happens until every queue validated.
async fn prepare_activation_queues(
    bucket: &Bucket,
    staged: &[(VersionRef, String, Manifest)],
    carried: &[(String, String, Manifest)],
) -> anyhow::Result<Vec<ActivationQueueState>> {
    let mut desired: BTreeMap<String, QueueConsumerDeployment> = BTreeMap::new();
    for (reference, prefix, manifest) in staged {
        for consumer in &manifest.queue_consumers {
            let deployment = QueueConsumerDeployment {
                script_name: manifest.script_name.clone(),
                version: manifest.version.clone(),
                prefix: prefix.clone(),
            };
            if let Some(previous) = desired.insert(consumer.queue.clone(), deployment) {
                bail!(
                    "queue {:?} is consumed by both {} and {} in one activation; \
                     stage versions that consume it from one script",
                    consumer.queue,
                    previous.script_name,
                    reference.script
                );
            }
        }
    }
    // A queue a serving script keeps consuming cannot move to a staged
    // script in the same activation: the carried script's version is
    // not changing, so both manifests would consume it at once.
    let mut kept: BTreeMap<String, String> = BTreeMap::new();
    for (script, _, manifest) in carried {
        for consumer in &manifest.queue_consumers {
            kept.insert(consumer.queue.clone(), script.clone());
        }
    }
    for (queue, deployment) in &desired {
        if let Some(keeper) = kept.get(queue) {
            bail!(
                "queue {:?} is consumed by serving script {:?}, which this activation carries \
                 unchanged; stage a version of {:?} that drops the queue before activating {}",
                queue,
                keeper,
                keeper,
                deployment.script_name
            );
        }
    }
    let staged_scripts: BTreeSet<&str> = staged
        .iter()
        .map(|(reference, _, _)| reference.script.as_str())
        .collect();
    let mut previous = BTreeSet::new();
    for (reference, _, _) in staged {
        previous.extend(current_queue_consumers(bucket, &reference.script).await?);
    }
    let mut states = Vec::new();
    for queue in desired.keys().chain(&previous).collect::<BTreeSet<_>>() {
        let key = crate::protocol::queue_consumer_attachment_key(queue)
            .expect("deploy validated the queue name");
        let (current, token) = match bucket.get(&key).await? {
            Some((body, token)) => {
                let attachment: QueueConsumerAttachment = serde_json::from_slice(&body)
                    .with_context(|| format!("decode queue consumer attachment {key}"))?;
                crate::protocol::validate_queue_consumer_attachment(queue, &attachment)?;
                (Some(attachment), Some(token))
            }
            None => (None, None),
        };
        let final_consumer = match desired.get(queue.as_str()) {
            Some(deployment) => {
                if let Some(owner) = current
                    .as_ref()
                    .and_then(|attachment| attachment.consumer.as_ref())
                    .filter(|consumer| {
                        consumer.script_name != deployment.script_name
                            && !staged_scripts.contains(consumer.script_name.as_str())
                    })
                {
                    bail!(
                        "queue {:?} already has consumer script {:?}; remove that consumer \
                         before activating {}",
                        queue,
                        owner.script_name,
                        deployment.script_name
                    );
                }
                Some(deployment.clone())
            }
            // A queue no staged script wants is detached only when a
            // staged script owns it; an owner outside this activation
            // keeps its queue.
            None => match current
                .as_ref()
                .and_then(|attachment| attachment.consumer.as_ref())
            {
                None => None,
                Some(owner) if staged_scripts.contains(owner.script_name.as_str()) => None,
                Some(_) => continue,
            },
        };
        states.push(ActivationQueueState {
            queue: (*queue).clone(),
            key,
            current,
            token,
            final_consumer,
        });
    }
    Ok(states)
}

/// Publish one activation's queue attachments: one conditional write
/// per queue that changes, against the token [`prepare_activation_queues`]
/// read. Every state here was blessed by that prepare; an unchanged
/// attachment is still skipped, so a healthy fleet converges quietly.
async fn publish_activation_queues(
    bucket: &Bucket,
    states: Vec<ActivationQueueState>,
) -> anyhow::Result<()> {
    for state in states {
        let next = QueueConsumerAttachment {
            schema_version: QUEUE_CONSUMER_ATTACHMENT_SCHEMA_VERSION,
            queue: state.queue,
            consumer: state.final_consumer,
        };
        if state.current.as_ref() == Some(&next) {
            continue;
        }
        publish_one_queue_attachment(
            bucket,
            &state.key,
            state.token.as_deref(),
            &next,
            "activate",
        )
        .await?;
    }
    Ok(())
}

/// A script's manifest at one version prefix, when it reads and
/// identifies that script and version. `None` is not an error here:
/// callers decide whether an unreadable manifest warns (convergence,
/// which heals what it can) or refuses (the pre-commit re-check, which
/// guards the commit).
async fn read_identified_manifest(
    bucket: &Bucket,
    script: &str,
    version: &str,
    prefix: &str,
) -> anyhow::Result<Option<Manifest>> {
    let key = format!("{prefix}/manifest.json");
    match bucket.get(&key).await? {
        Some((body, _)) => Ok(serde_json::from_slice::<Manifest>(&body)
            .ok()
            .filter(|manifest| manifest.script_name == script && manifest.version == version)),
        None => Ok(None),
    }
}

/// The carried scripts' manifests for queue convergence: script, prefix,
/// manifest. A carried version without a readable, self-identifying
/// manifest cannot converge, so it warns instead; the pre-commit
/// re-check refuses to name it, so the warning never becomes a wedge.
async fn carried_manifests(
    bucket: &Bucket,
    staged: &[(VersionRef, String, Manifest)],
    components: &BTreeMap<String, ComponentRef>,
) -> anyhow::Result<Vec<(String, String, Manifest)>> {
    let staged_scripts: BTreeSet<&str> = staged
        .iter()
        .map(|(reference, _, _)| reference.script.as_str())
        .collect();
    let mut carried = Vec::new();
    for (script, component) in components {
        if staged_scripts.contains(script.as_str()) {
            continue;
        }
        let usable =
            read_identified_manifest(bucket, script, &component.version, &component.prefix).await?;
        match usable {
            Some(manifest) => carried.push((script.clone(), component.prefix.clone(), manifest)),
            None => note!(
                "warning: carried script {script}={} has no readable manifest; its queue \
                 attachments cannot converge — stage and activate a replacement",
                component.version
            ),
        }
    }
    Ok(carried)
}

/// Re-read every manifest this commit is about to name — staged and
/// carried — immediately before the pointer moves.
///
/// A discard that landed after the opening reads would otherwise commit
/// a generation naming deleted code: the next adoption fails loudly,
/// and the fleet cannot advance until someone re-deploys. Failing here
/// instead keeps the serving generation adoptable. Unlike the opening
/// carried read — which warns so convergence still heals what it can —
/// this refuses: the commit would name the unreadable version either
/// way, so refusing is fail-closed at write time instead of a wedge at
/// read time.
async fn verify_precommit_manifests(
    bucket: &Bucket,
    staged: &[(VersionRef, String, Manifest)],
    components: &BTreeMap<String, ComponentRef>,
) -> anyhow::Result<()> {
    for (reference, prefix, _) in staged {
        let usable =
            read_identified_manifest(bucket, &reference.script, &reference.version, prefix).await?;
        anyhow::ensure!(
            usable.is_some(),
            "staged version {:?} of script {:?} vanished before the commit — a concurrent \
             `celld discard` may have removed it; re-run `celld deploy --no-activate` for it, \
             then re-run `celld activate`",
            reference.version,
            reference.script
        );
    }
    let staged_scripts: BTreeSet<&str> = staged
        .iter()
        .map(|(reference, _, _)| reference.script.as_str())
        .collect();
    for (script, component) in components {
        if staged_scripts.contains(script.as_str()) {
            continue;
        }
        let usable =
            read_identified_manifest(bucket, script, &component.version, &component.prefix).await?;
        anyhow::ensure!(
            usable.is_some(),
            "carried script {script}={} has no readable manifest — its version may have been \
             discarded; restore it with `celld deploy` from the same source, then re-run \
             `celld activate`",
            component.version
        );
    }
    Ok(())
}

/// Converge the carried scripts' queue attachments to their serving
/// manifests: idempotent when the fleet is healthy, healing when an
/// attachment skewed from under its generation. Runs after the staged
/// publish, so a queue a staged script releases to a carried one ends
/// attached to the carried script, not detached.
async fn converge_carried_queues(
    bucket: &Bucket,
    carried: &[(String, String, Manifest)],
) -> anyhow::Result<()> {
    for (_, prefix, manifest) in carried {
        let states = prepare_queue_attachments(bucket, manifest).await?;
        publish_queue_attachments(bucket, manifest, prefix, states, "activate").await?;
    }
    Ok(())
}

/// Move one named pointer to its activated version, unless it already
/// names it. The fleet pointer's component map is the committed truth;
/// the named pointers exist for older nodes and later plain deploys, so
/// every activate heals any skew rather than trusting a previous run.
/// `command` names the re-run — `deploy` or `activate` — because the
/// hint must repair the path that failed.
async fn converge_named_pointer(
    bucket: &Bucket,
    script: &str,
    component: &ComponentRef,
    command: &str,
) -> anyhow::Result<()> {
    let key = format!("deploy/{script}/current.json");
    if let Some((body, _)) = bucket.get(&key).await? {
        let pointer: DeployPointer =
            serde_json::from_slice(&body).with_context(|| format!("decode {key}"))?;
        if pointer.script_name.as_deref() == Some(script)
            && pointer.version == component.version
            && pointer.prefix == component.prefix
        {
            return Ok(());
        }
    }
    let pointer = DeployPointer {
        script_name: Some(script.to_string()),
        version: component.version.clone(),
        prefix: component.prefix.clone(),
        rollout: Rollout { percent: 100 },
        components: None,
    };
    put_pointer(bucket, &key, serde_json::to_vec_pretty(&pointer)?, command).await
}

/// Converge every serving script's named pointer. A script that fails
/// warns and the rest still converge, so one corrupt pointer cannot pin
/// the others; the first error returns after the pass.
async fn converge_named_pointers(
    bucket: &Bucket,
    components: &BTreeMap<String, ComponentRef>,
    command: &str,
) -> anyhow::Result<()> {
    let mut first: Option<anyhow::Error> = None;
    for (script, component) in components {
        if let Err(error) = converge_named_pointer(bucket, script, component, command).await {
            note!("warning: named pointer for script {script} did not converge: {error:#}");
            if first.is_none() {
                first = Some(error);
            }
        }
    }
    match first {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Re-read the fleet-wide pointer and converge every node-visible
/// derivative of it — the queue attachments and the named pointers —
/// toward the generation it commits right now.
///
/// A writer that fails after its convergence started (a lost commit
/// race, an attachment write that raced, the pre-commit manifest
/// re-check) has already published queue attachments toward a generation
/// that never committed; left alone, the next adoption of the committed
/// generation fails on them — a queue attachment names an exact
/// deployment — and the watcher caches the failure fleet-wide. Healing
/// first turns the loser into the winner's converger: its net effect
/// points at the committed generation instead of away from it.
///
/// The serving generation is the pointer's components map when it
/// carries one; under a legacy pointer it is the top-level primary —
/// other scripts' named pointers are read live there, so a named
/// pointer is the committed truth for its own script and there is
/// nothing to converge it toward. Attachments converge for every
/// serving script either way, each from the manifest its committed
/// version names — under a legacy pointer that is the named version
/// for a non-primary script. A script whose manifest is unreadable
/// warns and is skipped: adoption stays fail-closed, and the next
/// writer retries.
///
/// Best-effort under concurrent writers — a heal write can itself lose
/// a race — and a crash cannot heal at all; both converge on the next
/// activate, or on a deploy of the skewed script, which runs this same
/// convergence toward its own generation. Never run writers
/// concurrently: only sequential runs converge cleanly.
async fn heal_derivative_state(bucket: &Bucket, command: &str) -> anyhow::Result<()> {
    let Some((body, _)) = bucket.get(crate::fleet::CURRENT_POINTER_KEY).await? else {
        // No committed pointer, so no adoption can read anything: a
        // first generation that never committed leaves nothing serving
        // to heal, and the next writer converges from scratch.
        return Ok(());
    };
    let current: DeployPointer =
        serde_json::from_slice(&body).context("decode deploy/current.json")?;
    let mut first: Option<anyhow::Error> = None;
    let mut components: BTreeMap<String, ComponentRef> = BTreeMap::new();
    if let Some(generation) = &current.components {
        components.clone_from(generation);
    } else if let Some(primary) = current.script_name.clone() {
        components.insert(
            primary,
            ComponentRef {
                version: current.version.clone(),
                prefix: current.prefix.clone(),
            },
        );
    }
    // Under a legacy pointer the attachments of every named script
    // converge too, from the named version — the primary keeps its
    // top-level truth, which wins over a stale named pointer.
    let mut attachment_scripts = components.clone();
    if current.components.is_none() {
        match legacy_components(bucket).await {
            Ok(named) => {
                for (script, component) in named {
                    attachment_scripts.entry(script).or_insert(component);
                }
            }
            Err(error) => {
                note!("warning: named pointers did not list for the heal: {error:#}");
                first = Some(error);
            }
        }
    }
    if let Err(error) = converge_named_pointers(bucket, &components, command).await {
        if first.is_none() {
            first = Some(error);
        }
    }
    for (script, component) in &attachment_scripts {
        if let Err(error) = heal_script_queues(bucket, script, component, command).await {
            note!("warning: queue attachments for script {script} did not heal: {error:#}");
            if first.is_none() {
                first = Some(error);
            }
        }
    }
    match first {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Converge one serving script's queue attachments from its committed
/// manifest. An unreadable manifest warns and is skipped: the next
/// writer retries, and adoption stays fail-closed in the meantime.
async fn heal_script_queues(
    bucket: &Bucket,
    script: &str,
    component: &ComponentRef,
    command: &str,
) -> anyhow::Result<()> {
    let usable =
        read_identified_manifest(bucket, script, &component.version, &component.prefix).await?;
    let Some(manifest) = usable else {
        note!(
            "warning: serving script {script}={} has no readable manifest; its queue \
             attachments cannot heal — re-run `celld {command}` once its version is restored",
            component.version
        );
        return Ok(());
    };
    let states = prepare_queue_attachments(bucket, &manifest).await?;
    publish_queue_attachments(bucket, &manifest, &component.prefix, states, command).await
}

/// Run [`heal_derivative_state`] after a writer error, returning the
/// original error: the heal's own failure is reported beside it, never
/// masking what failed.
async fn heal_on_error(bucket: &Bucket, error: anyhow::Error, command: &str) -> anyhow::Error {
    if let Err(heal) = heal_derivative_state(bucket, command).await {
        note!("warning: healing toward the serving pointer failed: {heal:#}");
        return error.context(format!(
            "healing toward the serving pointer also failed: {heal:#}; re-run `celld {command}` \
             to converge"
        ));
    }
    error
}

/// The activated versions a legacy fleet-wide pointer (no components map)
/// implies, from every named pointer in the bucket. Children of `deploy/`
/// without a `current.json` — the image tars under `deploy/images/`, the
/// queue attachments under `deploy/queues/` — are not scripts and are
/// skipped; a named pointer that does not identify its own directory is
/// corrupt and fails the activation rather than activating the wrong
/// script's version under a script's name.
async fn legacy_components(bucket: &Bucket) -> anyhow::Result<BTreeMap<String, ComponentRef>> {
    let mut components = BTreeMap::new();
    for child in bucket.common_prefixes("deploy/").await? {
        let Some(script) = child
            .strip_prefix("deploy/")
            .filter(|name| !name.is_empty() && !name.contains('/'))
        else {
            continue;
        };
        let key = format!("deploy/{script}/current.json");
        let Some((body, _)) = bucket.get(&key).await? else {
            continue;
        };
        let pointer: DeployPointer =
            serde_json::from_slice(&body).with_context(|| format!("decode {key}"))?;
        // Older named pointers omit the script name; only a present name
        // that disagrees is corruption.
        if let Some(name) = pointer.script_name.as_deref() {
            anyhow::ensure!(
                name == script,
                "named deployment pointer {key} identifies script {name:?}"
            );
        }
        components.insert(
            script.to_string(),
            ComponentRef {
                version: pointer.version,
                prefix: pointer.prefix,
            },
        );
    }
    Ok(components)
}

/// What `celld discard` removed.
#[derive(Debug)]
pub struct Discarded {
    pub removed: Vec<VersionRef>,
    /// Version-dir objects deleted. Shared content-addressed objects —
    /// asset bodies, image tars — are left: other versions reference them.
    pub objects: usize,
}

/// Delete staged versions from the bucket.
///
/// A version any pointer names — the serving primary, a components entry,
/// a named pointer, or a queue attachment — is refused: deleting
/// activated code would break the next adoption. Anything else is inert
/// (serving only reads pointers), so removing it is safe; that covers
/// staged versions and superseded ones alike.
///
/// Deletion is scoped to the version directory, and the directory's own
/// manifest must identify the script and version (when a manifest is
/// present at all) before anything is deleted. The pointers are re-read
/// after the delete under the same tokens: a concurrent activate or
/// deploy that moved one mid-delete fails loudly instead of leaving a
/// generation that points at missing code. No re-check can cover a
/// commit landing after it — that interleaving names deleted code — but
/// it fails loudly (adoption refuses a generation whose manifest is
/// missing, never serving wrong code) and recovers by re-deploying from
/// the same source, which reproduces the version byte-identically for a
/// re-activate. Never run `celld discard` concurrently with `celld
/// activate` or `celld deploy` — the re-check reports the race, it
/// cannot undo the delete — and hold a superseded version until
/// draining nodes have moved past it (past `CELLD_DEPLOY_MAX_AGE_S`,
/// with the replacement's adoption verified) before discarding a
/// rollback target.
pub async fn discard(bucket: &Bucket, scripts: &[VersionRef]) -> anyhow::Result<Discarded> {
    anyhow::ensure!(
        !scripts.is_empty(),
        "celld discard names at least one SCRIPT=VERSION"
    );
    let mut removed = Vec::with_capacity(scripts.len());
    let mut objects = 0;
    for reference in scripts {
        let (fleet_token, named_token) = ensure_not_activated(bucket, reference).await?;
        let prefix = format!("deploy/{}/{}", reference.script, reference.version);
        let keys: Vec<String> = bucket
            .list(&prefix)
            .await?
            .into_iter()
            .map(|object| object.location.to_string())
            .collect();
        // An S3 listing is a byte prefix, so a listing that escapes the
        // version directory — a sibling, a pointer key, an exact match —
        // refuses the whole discard rather than deleting what it names.
        let keys = discard_deletion_scope(&prefix, &keys, reference)?;
        anyhow::ensure!(
            !keys.is_empty(),
            "no such version {:?} of script {:?}; nothing to discard",
            reference.version,
            reference.script
        );
        ensure_discard_manifest(bucket, reference, &prefix).await?;
        let gone = bucket.delete_many(&keys).await;
        anyhow::ensure!(
            gone.len() == keys.len(),
            "discard of {}={} deleted {}/{} objects; re-run `celld discard`",
            reference.script,
            reference.version,
            gone.len(),
            keys.len()
        );
        objects += gone.len();
        if std::env::var("CELLD_TEST_DISCARD_RACE").as_deref() == Ok("1") {
            // A concurrent writer that moved the fleet pointer after the
            // pre-check: the re-check below must report it. Shaped as a
            // valid pointer naming another version, so post-delete reads
            // decode.
            let rival = serde_json::json!({
                "script_name": "staged-rival",
                "version": "ffffffffffffffff",
                "prefix": "deploy/staged-rival/ffffffffffffffff",
                "rollout": { "percent": 100 },
                "celld_test_rival": true,
            });
            bucket
                .put(
                    crate::fleet::CURRENT_POINTER_KEY,
                    serde_json::to_vec_pretty(&rival)?,
                )
                .await
                .context("write the injected rival pointer")?;
        }
        // A concurrent activate or deploy that moved a pointer after the
        // first check: report it loudly. If the version is named now,
        // serving code is already gone and the operator must re-deploy
        // it before the next adoption needs it; if it is still named by
        // nothing, the delete was safe but the race still says so.
        let (fleet_moved, named_moved) = pointers_moved(
            bucket,
            reference,
            fleet_token.as_deref(),
            named_token.as_deref(),
        )
        .await?;
        if ensure_not_activated(bucket, reference).await.is_err() {
            bail!(
                "a concurrent activate or deploy named {}={} while it was discarded; re-run \
                 `celld deploy --no-activate` from the same source and `celld activate {}={}` \
                 before any node adopts",
                reference.script,
                reference.version,
                reference.script,
                reference.version
            );
        }
        if fleet_moved || named_moved {
            bail!(
                "a concurrent deploy or activate moved the {} pointer while {}={} was \
                 discarded; the version is named by no pointer, so the delete was safe — \
                 re-run `celld discard {}={}` to confirm",
                if fleet_moved { "fleet" } else { "named" },
                reference.script,
                reference.version,
                reference.script,
                reference.version
            );
        }
        removed.push(reference.clone());
    }
    Ok(Discarded { removed, objects })
}

/// Refuse a version any pointer names: the fleet pointer's primary and
/// components, the script's named pointer, and every queue attachment.
/// Returns the fleet and named pointer tokens, so the post-delete
/// re-check runs against the same generation it verified.
async fn ensure_not_activated(
    bucket: &Bucket,
    reference: &VersionRef,
) -> anyhow::Result<(Option<String>, Option<String>)> {
    let mut fleet_token = None;
    if let Some((body, token)) = bucket.get(crate::fleet::CURRENT_POINTER_KEY).await? {
        fleet_token = Some(token);
        let pointer: DeployPointer =
            serde_json::from_slice(&body).context("decode deploy/current.json")?;
        anyhow::ensure!(
            pointer.script_name.as_deref() != Some(reference.script.as_str())
                || pointer.version != reference.version,
            "version {:?} of script {:?} is the serving primary; activate a replacement first",
            reference.version,
            reference.script
        );
        if let Some(component) = pointer
            .components
            .as_ref()
            .and_then(|generation| generation.get(&reference.script))
        {
            anyhow::ensure!(
                component.version != reference.version,
                "version {:?} of script {:?} is activated; activate a replacement first",
                reference.version,
                reference.script
            );
        }
    }
    let key = format!("deploy/{}/current.json", reference.script);
    let mut named_token = None;
    if let Some((body, token)) = bucket.get(&key).await? {
        named_token = Some(token);
        let pointer: DeployPointer =
            serde_json::from_slice(&body).with_context(|| format!("decode {key}"))?;
        anyhow::ensure!(
            pointer.version != reference.version,
            "version {:?} of script {:?} is the activated version of that script; \
             activate a replacement first",
            reference.version,
            reference.script
        );
    }
    let queues = attachments_naming(bucket, &reference.script, &reference.version).await?;
    anyhow::ensure!(
        queues.is_empty(),
        "version {:?} of script {:?} is the attached consumer of queue {:?}; activate a \
         replacement first",
        reference.version,
        reference.script,
        queues.join(", ")
    );
    Ok((fleet_token, named_token))
}

/// The queues whose consumer attachment names one deployment. A version
/// no pointer names can still be attached — skew from under a committed
/// generation — and discarding it would break the next adoption that
/// loads through that queue.
async fn attachments_naming(
    bucket: &Bucket,
    script: &str,
    version: &str,
) -> anyhow::Result<Vec<String>> {
    let mut naming = Vec::new();
    for child in bucket.common_prefixes("deploy/queues/").await? {
        let Some(queue) = child
            .strip_prefix("deploy/queues/")
            .filter(|name| !name.is_empty() && !name.contains('/'))
        else {
            continue;
        };
        let key = format!("deploy/queues/{queue}/consumer.json");
        let Some((body, _)) = bucket.get(&key).await? else {
            continue;
        };
        let attachment: QueueConsumerAttachment =
            serde_json::from_slice(&body).with_context(|| format!("decode {key}"))?;
        crate::protocol::validate_queue_consumer_attachment(queue, &attachment)?;
        if attachment
            .consumer
            .as_ref()
            .is_some_and(|consumer| consumer.script_name == script && consumer.version == version)
        {
            naming.push(queue.to_string());
        }
    }
    naming.sort();
    Ok(naming)
}

/// Whether the fleet and named pointers still carry the tokens the
/// pre-delete check read. A changed token — including a pointer created
/// or removed under us — is a concurrent writer.
async fn pointers_moved(
    bucket: &Bucket,
    reference: &VersionRef,
    fleet_token: Option<&str>,
    named_token: Option<&str>,
) -> anyhow::Result<(bool, bool)> {
    let fleet_now = bucket
        .head(crate::fleet::CURRENT_POINTER_KEY)
        .await?
        .map(|(_, token)| token);
    let named_now = bucket
        .head(&format!("deploy/{}/current.json", reference.script))
        .await?
        .map(|(_, token)| token);
    Ok((
        fleet_now.as_deref() != fleet_token,
        named_now.as_deref() != named_token,
    ))
}

/// The keys a discard may delete: exactly the version directory. Every
/// listed key must sit under `prefix/`; anything else — a sibling
/// version an S3 byte-prefix listing also matched, a pointer key, an
/// object at the prefix itself — fails the discard, because deleting
/// outside the version directory is never what was asked.
fn discard_deletion_scope(
    prefix: &str,
    keys: &[String],
    reference: &VersionRef,
) -> anyhow::Result<Vec<String>> {
    let directory = format!("{prefix}/");
    for key in keys {
        anyhow::ensure!(
            key.starts_with(&directory),
            "refusing to discard {}={}: listing returned {key:?}, which is outside the version \
             directory — a byte-prefix match on a sibling or pointer key, not the version",
            reference.script,
            reference.version
        );
    }
    Ok(keys.to_vec())
}

/// The version directory's own manifest must identify the script and
/// version before anything is deleted — the same check `activate`
/// applies. A directory with no manifest at all is inert (no manifest,
/// no adoption) and discards, so a partial upload can still be cleaned.
async fn ensure_discard_manifest(
    bucket: &Bucket,
    reference: &VersionRef,
    prefix: &str,
) -> anyhow::Result<()> {
    let key = format!("{prefix}/manifest.json");
    let Some((body, _)) = bucket.get(&key).await? else {
        return Ok(());
    };
    let manifest: Manifest =
        serde_json::from_slice(&body).with_context(|| format!("decode {key}"))?;
    anyhow::ensure!(
        manifest.script_name == reference.script && manifest.version == reference.version,
        "version directory {prefix} holds manifest for {}={}, not {}={}; refusing to discard",
        manifest.script_name,
        manifest.version,
        reference.script,
        reference.version
    );
    Ok(())
}

/// The immutable half of [`write`]: everything under the version prefix
/// plus the shared content-addressed objects it references.
async fn upload_version(bucket: &Bucket, built: &Built) -> anyhow::Result<()> {
    // Images are fleet-wide and content-addressed like asset bodies, and
    // they are what a node loads before a container class can start.
    if built.upload_images {
        for image in built.images.iter().chain(&built.fence_image) {
            ensure_image_tar(bucket, image).await?;
        }
    }
    // Asset bodies are fleet-wide and content-addressed. Finish every body
    // before publishing the deployment-local index or manifest so a reader
    // can never observe a pointer whose assets are incomplete.
    if let Some(assets) = &built.assets {
        stream::iter(&assets.blobs)
            .map(|(sha256, body)| ensure_asset_blob(bucket, sha256, body))
            .buffer_unordered(ASSET_UPLOAD_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
    }
    for (name, bytes) in &built.modules {
        bucket
            .put(&format!("{}/{name}", built.prefix), bytes.clone())
            .await?;
    }
    if let Some(assets) = &built.assets {
        bucket
            .put(
                &format!("{}/assets.json", built.prefix),
                assets.index.clone(),
            )
            .await?;
    }
    bucket
        .put(
            &format!("{}/manifest.json", built.prefix),
            serde_json::to_vec_pretty(&built.manifest)?,
        )
        .await?;
    Ok(())
}

pub(crate) struct QueueAttachmentState {
    queue: String,
    key: String,
    current: Option<QueueConsumerAttachment>,
    token: Option<String>,
    desired: bool,
}

pub(crate) async fn prepare_queue_attachments(
    bucket: &Bucket,
    manifest: &Manifest,
) -> anyhow::Result<Vec<QueueAttachmentState>> {
    let desired = manifest
        .queue_consumers
        .iter()
        .map(|consumer| consumer.queue.clone())
        .collect::<BTreeSet<_>>();
    let previous = current_queue_consumers(bucket, &manifest.script_name).await?;
    let queues = desired.union(&previous).cloned().collect::<Vec<_>>();
    let mut states = Vec::with_capacity(queues.len());

    for queue in queues {
        let key = crate::protocol::queue_consumer_attachment_key(&queue)
            .expect("deploy validated the queue name");
        let (current, token) = match bucket.get(&key).await? {
            Some((body, token)) => {
                let attachment: QueueConsumerAttachment = serde_json::from_slice(&body)
                    .with_context(|| format!("decode queue consumer attachment {key}"))?;
                crate::protocol::validate_queue_consumer_attachment(&queue, &attachment)?;
                (Some(attachment), Some(token))
            }
            None => (None, None),
        };
        let is_desired = desired.contains(&queue);
        if is_desired {
            if let Some(owner) = current
                .as_ref()
                .and_then(|attachment| attachment.consumer.as_ref())
                .filter(|consumer| consumer.script_name != manifest.script_name)
            {
                bail!(
                    "queue {:?} already has consumer script {:?}; remove that consumer before deploying {}",
                    queue,
                    owner.script_name,
                    manifest.script_name
                );
            }
        }
        states.push(QueueAttachmentState {
            queue,
            key,
            current,
            token,
            desired: is_desired,
        });
    }
    Ok(states)
}

async fn current_queue_consumers(
    bucket: &Bucket,
    script_name: &str,
) -> anyhow::Result<BTreeSet<String>> {
    let pointer_key = format!("deploy/{script_name}/current.json");
    let Some((pointer_body, _)) = bucket.get(&pointer_key).await? else {
        return Ok(BTreeSet::new());
    };
    let pointer: DeployPointer =
        serde_json::from_slice(&pointer_body).with_context(|| format!("decode {pointer_key}"))?;
    anyhow::ensure!(
        pointer.script_name.as_deref() == Some(script_name),
        "named deployment pointer {pointer_key} does not identify script {script_name:?}"
    );
    let manifest_key = format!("{}/manifest.json", pointer.prefix);
    let (manifest_body, _) = bucket
        .get(&manifest_key)
        .await?
        .with_context(|| format!("read {manifest_key}"))?;
    let manifest: Manifest =
        serde_json::from_slice(&manifest_body).with_context(|| format!("decode {manifest_key}"))?;
    anyhow::ensure!(
        manifest.script_name == script_name,
        "named deployment pointer {pointer_key} resolved script {:?}",
        manifest.script_name
    );
    Ok(manifest
        .queue_consumers
        .into_iter()
        .map(|consumer| consumer.queue)
        .collect())
}

pub(crate) async fn publish_queue_attachments(
    bucket: &Bucket,
    manifest: &Manifest,
    prefix: &str,
    states: Vec<QueueAttachmentState>,
    command: &str,
) -> anyhow::Result<()> {
    let consumer = QueueConsumerDeployment {
        script_name: manifest.script_name.clone(),
        version: manifest.version.clone(),
        prefix: prefix.to_string(),
    };
    for state in states {
        let next_consumer = if state.desired {
            Some(consumer.clone())
        } else if state
            .current
            .as_ref()
            .and_then(|attachment| attachment.consumer.as_ref())
            .is_some_and(|owner| owner.script_name == manifest.script_name)
        {
            None
        } else {
            continue;
        };
        let next = QueueConsumerAttachment {
            schema_version: QUEUE_CONSUMER_ATTACHMENT_SCHEMA_VERSION,
            queue: state.queue,
            consumer: next_consumer,
        };
        if state.current.as_ref() == Some(&next) {
            continue;
        }
        publish_one_queue_attachment(bucket, &state.key, state.token.as_deref(), &next, command)
            .await?;
    }
    Ok(())
}

/// One queue attachment's conditional write, against the token its
/// prepare read. `command` names the re-run — `deploy` or `activate` —
/// because the hint must repair the path that failed.
async fn publish_one_queue_attachment(
    bucket: &Bucket,
    key: &str,
    token: Option<&str>,
    next: &QueueConsumerAttachment,
    command: &str,
) -> anyhow::Result<()> {
    let body = serde_json::to_vec_pretty(next)?;
    match bucket.put_cas(key, body, token).await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(anyhow!(
            "queue consumer attachment {key} lost a race; another deploy or activate may have \
             landed first; re-run `celld {command}`"
        )),
        Err(error) => Err(error.context(format!(
            "a queue consumer attachment write may have committed; re-run `celld {command}`"
        ))),
    }
}

async fn ensure_asset_blob(bucket: &Bucket, sha256: &str, body: &[u8]) -> anyhow::Result<()> {
    let key = asset_blob_key(sha256).expect("built asset digest is valid");
    if let Ok(Some((size, meta))) = bucket.head_with_meta(&key, "sha256").await {
        if size == body.len() as u64 && meta.as_deref() == Some(sha256) {
            return Ok(());
        }
    }
    bucket
        .put_with_meta(&key, body.to_vec(), &[("sha256", sha256)])
        .await
}

/// Compare-and-swap on a pointer: create it if absent, otherwise replace
/// exactly the value we read. `command` names the re-run — `deploy` or
/// `activate` — because the hint must repair the path that failed.
async fn put_pointer(
    bucket: &Bucket,
    key: &str,
    body: Vec<u8>,
    command: &str,
) -> anyhow::Result<()> {
    let etag = bucket.head(key).await.ok().flatten().map(|(_, etag)| etag);
    match bucket.put_cas(key, body, etag.as_deref()).await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(anyhow!(
            "write {}://{}/{key} lost a race\n\
             Another deploy or activate may have landed first; re-run `celld {command}`.",
            bucket.scheme(),
            bucket.name
        )),
        Err(error) => Err(error.context(format!(
            "Another deploy or activate may have landed first; re-run `celld {command}`."
        ))),
    }
}

/// The fleet-pointer commit: a compare-and-swap on `token`, the token of
/// the opening pointer read — not a fresh head. A writer that landed
/// between that read and this write changed the token, so the CAS
/// rejects and the loser heals toward the winner and re-runs instead of
/// silently overwriting it. `command` names the re-run, `deploy` or
/// `activate`.
async fn commit_pointer(
    bucket: &Bucket,
    key: &str,
    body: Vec<u8>,
    token: Option<&str>,
    command: &str,
) -> anyhow::Result<()> {
    if std::env::var("CELLD_TEST_POINTER_RACE").as_deref() == Ok("1") {
        // A concurrent writer that landed after the opening read: the
        // commit must reject against its stale token and leave this
        // rival generation in place. Shaped as a valid pointer naming
        // another version, so post-write reads decode.
        let rival = serde_json::json!({
            "script_name": "staged-rival",
            "version": "ffffffffffffffff",
            "prefix": "deploy/staged-rival/ffffffffffffffff",
            "rollout": { "percent": 100 },
            "celld_test_rival": true,
        });
        bucket
            .put(key, serde_json::to_vec_pretty(&rival)?)
            .await
            .context("write the injected rival pointer")?;
    }
    match bucket.put_cas(key, body, token).await {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(anyhow!(
            "write {}://{}/{key} lost a race\n\
             Another deploy or activate landed first; re-run `celld {command}`.",
            bucket.scheme(),
            bucket.name
        )),
        Err(error) => Err(error.context(format!(
            "Another deploy or activate may have landed first; re-run `celld {command}`."
        ))),
    }
}

/// What `celld d1` needs out of a project: the declared databases, so a name
/// the project never declared fails here instead of creating an empty one.
pub struct D1Project {
    /// One entry per declaration, in config order.
    pub databases: Vec<D1Declaration>,
}

pub struct D1Declaration {
    pub database_name: String,
    /// Cloudflare's stable resource ID when present, otherwise the database
    /// name for a celld-only project.
    pub database_identity: String,
    /// Wrangler scopes `migrations_dir` to the binding, so this is per
    /// database and never shared. A single shared directory would apply one
    /// database's migrations to another.
    pub migrations_dir: PathBuf,
    /// The bookkeeping table, per binding as on wrangler
    /// (`migrations_table`). Hard-coding `d1_migrations` here made celld
    /// read an empty table on a project that had renamed it, and every
    /// already-applied migration re-ran.
    pub migrations_table: String,
}

/// Read a project's D1 declarations. This reads the same config `build` reads,
/// but it does not bundle: `celld d1` acts on a database that is already
/// deployed, and a project that fails to build must still be migratable.
pub fn read_d1_project(given: Option<PathBuf>) -> anyhow::Result<D1Project> {
    let (path, root) = resolve_project_config(given)?;
    let source =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    let config: Value = serde_json::from_str(&strip_jsonc(&source))
        .with_context(|| format!("parse {}", path.display()))?;
    let object = config
        .as_object()
        .ok_or_else(|| anyhow!("{} is not a JSON object", path.display()))?;
    object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{} has no `name`", path.display()))?;
    let mut databases = Vec::new();
    if let Some(Value::Array(entries)) = object.get("d1_databases") {
        for entry in entries {
            let Some(name) = entry.get("database_name").and_then(Value::as_str) else {
                continue;
            };
            // Wrangler discovers migrations through more knobs than the two
            // celld reads. An unread knob must be a refusal, not a silent
            // default: honoring half of a project's migration config applies
            // the wrong files in the wrong order.
            if entry.get("migrations_pattern").is_some() {
                bail!(
                    "d1 database {name:?} sets `migrations_pattern`, which celld \
                     does not support; celld reads `*.sql` from `migrations_dir`"
                );
            }
            let migrations_table = match entry.get("migrations_table") {
                None => "d1_migrations".to_string(),
                Some(Value::String(table)) => table.clone(),
                Some(_) => bail!("d1 database {name:?} has a non-string `migrations_table`"),
            };
            // The table name is joined into SQL as an identifier, both here
            // and in the cell, so anything but a plain identifier is refused
            // before it can reach either.
            let mut characters = migrations_table.chars();
            let plain = characters
                .next()
                .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
                && characters.all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !plain {
                bail!(
                    "d1 database {name:?} has a `migrations_table` that is not a \
                     plain identifier: {migrations_table:?}"
                );
            }
            let database_identity = match entry.get("database_id") {
                None => name.to_string(),
                Some(Value::String(identity)) if !identity.is_empty() => identity.clone(),
                Some(_) => bail!("d1 database {name:?} has an invalid `database_id`"),
            };
            let migrations_dir = match entry.get("migrations_dir") {
                None => root.join("migrations"),
                Some(Value::String(directory)) => {
                    root.join(project_relative_path(directory, "migrations_dir")?)
                }
                Some(_) => bail!("d1 database {name:?} has a non-string `migrations_dir`"),
            };
            // The lexical check above cannot see directory links: the joined
            // path must resolve inside the project. A missing directory stays
            // missing so `celld d1` can report it when it reads.
            let migrations_dir = match std::fs::symlink_metadata(&migrations_dir) {
                Ok(_) => contain_path(
                    &root,
                    &migrations_dir,
                    &format!("d1 database {name:?} `migrations_dir`"),
                )?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => migrations_dir,
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("inspect {}", migrations_dir.display()));
                }
            };
            databases.push(D1Declaration {
                database_name: name.to_string(),
                database_identity,
                migrations_dir,
                migrations_table,
            });
        }
    }
    let mut unique = Vec::<D1Declaration>::new();
    for declaration in databases {
        if let Some(previous) = unique
            .iter()
            .find(|candidate| candidate.database_name == declaration.database_name)
        {
            if previous.database_identity != declaration.database_identity
                || previous.migrations_dir != declaration.migrations_dir
                || previous.migrations_table != declaration.migrations_table
            {
                bail!(
                    "d1 database {:?} has ambiguous aliases with different database_id, \
                     migrations_dir, or migrations_table values",
                    declaration.database_name
                );
            }
            continue;
        }
        if let Some(previous) = unique
            .iter()
            .find(|candidate| candidate.database_identity == declaration.database_identity)
        {
            if previous.migrations_dir != declaration.migrations_dir
                || previous.migrations_table != declaration.migrations_table
            {
                bail!(
                    "d1 database identity {:?} has ambiguous aliases {:?} and {:?} with \
                     different migrations_dir or migrations_table values",
                    declaration.database_identity,
                    previous.database_name,
                    declaration.database_name
                );
            }
        }
        unique.push(declaration);
    }
    Ok(D1Project { databases: unique })
}

/// A path may name the config itself or the directory holding it; with no
/// path at all, the working directory.
pub(crate) fn resolve_config(given: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    let directory = match given {
        Some(path) if path.is_dir() => path,
        Some(path) if path.exists() => return Ok(path),
        Some(path) => bail!(
            "no Wrangler config or project directory at {}",
            path.display()
        ),
        None => PathBuf::from("."),
    };
    for candidate in ["wrangler.jsonc", "wrangler.json"] {
        let path = directory.join(candidate);
        if path.exists() {
            return Ok(path);
        }
    }
    if directory.join("wrangler.toml").exists() {
        bail!("wrangler.toml is not supported; convert it to wrangler.jsonc");
    }
    bail!(
        "no wrangler.jsonc or wrangler.json in {}",
        directory.display()
    )
}

fn read_project(path: &Path, root: &Path, overrides: &VarOverrides) -> anyhow::Result<Project> {
    let source =
        std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let config: Value = serde_json::from_str(&strip_jsonc(&source))
        .with_context(|| format!("parse {}", path.display()))?;
    let object = config
        .as_object()
        .ok_or_else(|| anyhow!("{} is not a JSON object", path.display()))?;

    let unsupported = object
        .keys()
        .filter(|key| !SUPPORTED_KEYS.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        bail!(
            "`celld deploy` does not support these config keys: {}.\n\
             Deploy this project with Wrangler instead, or remove them.",
            unsupported.join(", ")
        );
    }

    let script_name = object
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("config has no `name`"))?
        .to_string();
    if !valid_script_name(&script_name) {
        bail!(
            "config `name` must contain 1 to {MAX_SCRIPT_NAME_BYTES} bytes of lowercase \
             ASCII letters, digits, or internal hyphens: {script_name:?}"
        );
    }
    let main = object
        .get("main")
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| anyhow!("config `main` must be a string"))
                .and_then(|value| project_relative_path(value, "main"))
        })
        .transpose()?;
    if let Some(main) = &main {
        // `symlink_metadata` sees only the final component: a directory link
        // above the entry would still escape the project, so the whole
        // resolution must stay inside the root.
        contained_entry(root, main)?;
    }
    let no_bundle = match object.get("no_bundle") {
        None => false,
        Some(Value::Bool(value)) => *value,
        Some(_) => bail!("config `no_bundle` must be a boolean"),
    };
    if no_bundle && main.is_none() {
        bail!("config sets `no_bundle` without `main`");
    }
    let python = main
        .as_deref()
        .filter(|main| main.ends_with(".py"))
        .map(|_| {
            python::check_supported_events(object)?;
            python::python_compat(object)
        })
        .transpose()?;
    let bundle = read_bundle_config(object)?;
    // `define` and `rules` describe the esbuild run, and `no_bundle` is the
    // absence of one. Accepting both would produce a deployment that silently
    // omits every substitution the config asks for, which is the failure the
    // caller wrote those keys to prevent.
    if no_bundle {
        let ignored: Vec<&str> = ["define", "rules"]
            .into_iter()
            .filter(|key| object.contains_key(*key))
            .collect();
        if !ignored.is_empty() {
            bail!(
                "config sets `no_bundle` with `{}`; those keys only change the \
                 esbuild run, and `no_bundle` does not run esbuild",
                ignored.join("` and `")
            );
        }
    }
    let assets = object
        .get("assets")
        .map(|value| read_asset_project(value, root, object))
        .transpose()?;
    if main.is_none() && assets.is_none() {
        bail!("config has neither `main` nor `assets`");
    }
    if main.is_none()
        && assets.as_ref().is_some_and(|assets| {
            matches!(
                &assets.config.run_worker_first,
                RunWorkerFirst::Bool(true) | RunWorkerFirst::Routes(_)
            )
        })
    {
        bail!("an asset-only project cannot set `assets.run_worker_first`");
    }

    let mut sqlite_classes = read_sqlite_classes(object)?;
    if let Some(class) = sqlite_classes.iter().find(|class| is_reserved_class(class)) {
        bail!("`{class}` is a runtime class; remove it from `migrations`");
    }
    let crons = read_crons(object)?;
    if !crons.is_empty() && main.is_none() {
        bail!("config sets `triggers.crons` without `main`; a cron trigger needs a `scheduled` handler to call");
    }

    // Wrangler-shaped upload metadata, so a manifest written here and one
    // written by the control plane describe a deployment the same way.
    let mut bindings = Vec::new();
    let mut do_classes = Vec::new();
    for binding in object
        .get("durable_objects")
        .and_then(|value| value.get("bindings"))
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
    {
        let name = binding
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("durable object binding has no `name`"))?;
        let class_name = binding
            .get("class_name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("durable object binding {name} has no `class_name`"))?;
        do_classes.push(class_name.to_string());
        bindings.push(json!({
            "type": "durable_object_namespace",
            "name": name,
            "class_name": class_name,
        }));
    }
    let services = match object.get("services") {
        None => &[][..],
        Some(Value::Array(services)) => services.as_slice(),
        Some(_) => bail!("config `services` must be an array"),
    };
    let mut service_count = 0_usize;
    for service in services {
        let binding = service
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("service binding has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid service binding name: {binding:?}");
        }
        let target = service
            .get("service")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("service binding {binding} has no `service`"))?;
        if !valid_script_name(target) {
            bail!(
                "service binding {binding} `service` must contain 1 to {MAX_SCRIPT_NAME_BYTES} bytes of lowercase \
                 ASCII letters, digits, or internal hyphens: {target:?}"
            );
        }
        let entrypoint = service
            .get("entrypoint")
            .map(|value| {
                value.as_str().ok_or_else(|| {
                    anyhow!("service binding {binding} `entrypoint` must be a string")
                })
            })
            .transpose()?;
        let mut encoded = json!({
            "type": "service",
            "name": binding,
            "service": target,
        });
        if let Some(entrypoint) = entrypoint {
            encoded["entrypoint"] = json!(entrypoint);
        }
        bindings.push(encoded);
        service_count += 1;
    }
    let d1_databases = match object.get("d1_databases") {
        None => &[][..],
        Some(Value::Array(databases)) => databases.as_slice(),
        Some(_) => bail!("config `d1_databases` must be an array"),
    };
    // The reserved class must be refused whether or not the project declares
    // any `d1_databases`: the harness registers its own `__D1Database` class
    // in every isolate, so a durable_objects binding naming it would silently
    // resolve to the D1 database cell instead of the user's class.
    if let Some(class) = do_classes.iter().find(|class| is_reserved_class(class)) {
        bail!("`{class}` is a runtime class; rename the Durable Object class");
    }
    for database in d1_databases {
        let binding = database
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("d1 database has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid d1 binding name: {binding:?}");
        }
        let database_name = database
            .get("database_name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow!("d1 binding {binding} has no `database_name`; celld uses the name to select the database from the CLI")
            })?;
        if database_name.is_empty() {
            bail!("d1 binding {binding} has an empty `database_name`");
        }
        let mut encoded = json!({
            "type": "d1",
            "name": binding,
            "database_name": database_name,
        });
        if let Some(database_id) = database.get("database_id") {
            let database_id = database_id
                .as_str()
                .filter(|database_id| !database_id.is_empty())
                .ok_or_else(|| anyhow!("d1 binding {binding} has an invalid `database_id`"))?;
            encoded["database_id"] = json!(database_id);
        }
        bindings.push(encoded);
    }
    if !d1_databases.is_empty() {
        // A D1 database is a cell of a runtime-supplied class. Declaring it
        // here is what registers its namespace key and marks it SQLite-backed;
        // it is never a worker export, so it stays out of `ctx.exports`.
        do_classes.push(D1_CLASS.to_string());
        sqlite_classes.push(D1_CLASS.to_string());
    }
    let kv_namespaces = match object.get("kv_namespaces") {
        None => &[][..],
        Some(Value::Array(namespaces)) => namespaces.as_slice(),
        Some(_) => bail!("config `kv_namespaces` must be an array"),
    };
    let mut kv_ids = BTreeSet::new();
    for namespace in kv_namespaces {
        // Every key celld does not model stops the deploy, per the loud-gap
        // rule. `preview_id` is the exception and is accepted below: it selects
        // a different namespace under `wrangler dev`, which celld does not run,
        // so ignoring it costs a developer nothing at deploy time.
        let unsupported = namespace
            .as_object()
            .map(|object| {
                object
                    .keys()
                    .filter(|key| !matches!(key.as_str(), "binding" | "id" | "preview_id"))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if let Some(key) = unsupported.first() {
            bail!(
                "kv namespace declares `{key}`, which celld does not model; \
                 celld models `binding`, `id`, and `preview_id`"
            );
        }
        let binding = namespace
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("kv namespace has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid kv binding name: {binding:?}");
        }
        // `id` is the namespace identity, verbatim, and celld invents no
        // `namespace_name` key beside it. Upstream's `kv_namespaces` carries no
        // human-readable name at all -- only `binding`, `id` and `preview_id` --
        // and celld's accepted configuration is a strict subset of Wrangler's:
        // a key celld accepts and Wrangler diagnoses would make the file
        // unportable, which is the one property the deploy story rests on. A
        // project ported from Cloudflare therefore keeps its hex id and works;
        // a project that was never there writes a readable string.
        let id = namespace.get("id").and_then(Value::as_str).ok_or_else(|| {
            anyhow!(
                "kv binding {binding} has no `id`; celld uses the id as the \
                     namespace identity, so any stable string serves"
            )
        })?;
        if id.is_empty() {
            bail!("kv binding {binding} has an empty `id`");
        }
        // The id becomes a cell name and therefore a path component and an
        // object-store key, so it answers to the same charset fence every cell
        // scope does rather than to a looser one invented here.
        if !celld_logic::cell::valid_cell_scope(id) {
            bail!(
                "kv binding {binding} has an `id` that cannot name a cell: {id:?}; \
                 use ASCII letters, digits, and `_ - . : $`"
            );
        }
        if !kv_ids.insert(id.to_string()) {
            bail!("duplicate kv namespace id: {id:?}");
        }
        bindings.push(json!({
            "type": "kv",
            "name": binding,
            "id": id,
        }));
    }
    if !kv_namespaces.is_empty() {
        // A KV namespace is a cell of a runtime-supplied class, declared here
        // so its namespace key is minted and its cells are SQLite-backed. Like
        // D1's, it is never a worker export and stays out of `ctx.exports`.
        do_classes.push(KV_CLASS.to_string());
        sqlite_classes.push(KV_CLASS.to_string());
    }
    let queues = match object.get("queues") {
        None => None,
        Some(Value::Object(queues)) => Some(queues),
        Some(_) => bail!("config `queues` must be an object"),
    };
    if let Some(key) = queues
        .into_iter()
        .flat_map(|queues| queues.keys())
        .find(|key| !matches!(key.as_str(), "producers" | "consumers"))
    {
        bail!("config `queues` declares `{key}`, which celld does not model");
    }
    let queue_producers = match queues.and_then(|queues| queues.get("producers")) {
        None => &[][..],
        Some(Value::Array(producers)) => producers.as_slice(),
        Some(_) => bail!("config `queues.producers` must be an array"),
    };
    for producer in queue_producers {
        reject_queue_keys(
            producer,
            &["binding", "queue", "delivery_delay"],
            "producer",
        )?;
        let binding = producer
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("queue producer has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid queue binding name: {binding:?}");
        }
        let queue = queue_name(producer, "producer")?;
        let delivery_delay = optional_queue_u32(producer, "delivery_delay")?.unwrap_or(0);
        if delivery_delay > celld_logic::queue::MAX_DELAY_SECONDS {
            bail!("queue producer delivery_delay must be between 0 and 86400 seconds");
        }
        let mut encoded = json!({
            "type": "queue",
            "name": binding,
            "queue": queue,
        });
        if producer.get("delivery_delay").is_some() {
            encoded["delivery_delay"] = json!(delivery_delay);
        }
        bindings.push(encoded);
    }
    let queue_consumers_raw = match queues.and_then(|queues| queues.get("consumers")) {
        None => &[][..],
        Some(Value::Array(consumers)) => consumers.as_slice(),
        Some(_) => bail!("config `queues.consumers` must be an array"),
    };
    let mut queue_consumers = Vec::new();
    let mut consumed_queues = BTreeSet::new();
    for consumer in queue_consumers_raw {
        if consumer.get("script_name").is_some() {
            bail!("queue consumer declares `script_name`; celld attaches it to the current script");
        }
        reject_queue_keys(
            consumer,
            &[
                "queue",
                "max_batch_size",
                "max_batch_timeout",
                "max_retries",
                "dead_letter_queue",
                "max_concurrency",
                "retry_delay",
            ],
            "consumer",
        )?;
        let queue = queue_name(consumer, "consumer")?.to_string();
        if !consumed_queues.insert(queue.clone()) {
            bail!("duplicate queue consumer: {queue:?}");
        }
        let dead_letter_queue = consumer
            .get("dead_letter_queue")
            .map(|value| {
                let name = value
                    .as_str()
                    .ok_or_else(|| anyhow!("queue consumer dead_letter_queue must be a string"))?;
                validate_queue_name(name, "dead-letter queue")?;
                anyhow::ensure!(
                    name != queue,
                    "a queue cannot use itself as its dead-letter queue"
                );
                Ok::<_, anyhow::Error>(name.to_string())
            })
            .transpose()?;
        let config = QueueConsumerConfig {
            queue,
            max_batch_size: optional_queue_u16(consumer, "max_batch_size")?
                .unwrap_or(celld_logic::queue::DEFAULT_MAX_BATCH_SIZE),
            max_batch_timeout: optional_queue_u16(consumer, "max_batch_timeout")?
                .unwrap_or(celld_logic::queue::DEFAULT_MAX_BATCH_TIMEOUT_SECONDS),
            max_retries: optional_queue_u16(consumer, "max_retries")?
                .unwrap_or(celld_logic::queue::DEFAULT_MAX_RETRIES),
            dead_letter_queue,
            max_concurrency: optional_queue_u16(consumer, "max_concurrency")?,
            retry_delay: optional_queue_u32(consumer, "retry_delay")?,
        };
        celld_logic::queue::validate_config(&celld_logic::queue::QueueConfig {
            max_batch_size: config.max_batch_size,
            max_batch_timeout_seconds: config.max_batch_timeout,
            max_retries: config.max_retries,
            max_concurrency: config.max_concurrency,
            delivery_delay_seconds: 0,
            retry_delay_seconds: config.retry_delay,
        })
        .map_err(anyhow::Error::new)?;
        queue_consumers.push(config);
    }
    if !queue_producers.is_empty() || !queue_consumers.is_empty() {
        if !queue_consumers.is_empty() && main.is_none() {
            bail!("config declares a queue consumer without `main`; a consumer needs a Worker module with a queue handler");
        }
        do_classes.push(QUEUE_CLASS.to_string());
        sqlite_classes.push(QUEUE_CLASS.to_string());
    }
    let workflows = match object.get("workflows") {
        None => &[][..],
        Some(Value::Array(workflows)) => workflows.as_slice(),
        Some(_) => bail!("config `workflows` must be an array"),
    };
    let mut workflow_names = BTreeSet::new();
    for workflow in workflows {
        // Any key celld does not model stops the deploy, per the loud-gap
        // rule: an unread knob (`schedules` is the notable one) must be a
        // refusal, never a workflow that silently lacks the behavior it
        // declares.
        let unsupported = workflow
            .as_object()
            .map(|object| {
                object
                    .keys()
                    .filter(|key| {
                        !matches!(
                            key.as_str(),
                            "name" | "binding" | "class_name" | "script_name"
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !unsupported.is_empty() {
            bail!(
                "`celld deploy` does not support these workflow keys: {}.\n\
                 celld models `name`, `binding`, `class_name`, and `script_name`; \
                 remove the rest (`schedules` included).",
                unsupported.join(", ")
            );
        }
        let binding = workflow
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("workflow has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid workflow binding name: {binding:?}");
        }
        let workflow_name = workflow
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("workflow binding {binding} has no `name`"))?;
        if !valid_resource_name(workflow_name) {
            bail!(
                "invalid workflow name {workflow_name:?}: use letters, digits, `-` and `_` \
                 (not starting with `-`), at most 64 characters"
            );
        }
        if !workflow_names.insert(workflow_name.to_string()) {
            bail!("duplicate workflow name: {workflow_name:?}");
        }
        let class_name = workflow
            .get("class_name")
            .and_then(Value::as_str)
            .filter(|class| !class.is_empty())
            .ok_or_else(|| anyhow!("workflow binding {binding} has no `class_name`"))?;
        if is_reserved_class(class_name) {
            bail!(
                "workflow {workflow_name:?} names the reserved class {class_name:?} as its \
                 `class_name`; export a WorkflowEntrypoint subclass of your own"
            );
        }
        if let Some(script) = workflow.get("script_name").and_then(Value::as_str) {
            if script != script_name {
                bail!(
                    "workflow {workflow_name:?} sets `script_name` {script:?}; celld runs a \
                     workflow only in the script that declares it"
                );
            }
        }
        bindings.push(json!({
            "type": "workflow",
            "name": binding,
            "workflow_name": workflow_name,
            "class_name": class_name,
        }));
    }
    if !workflows.is_empty() {
        if main.is_none() {
            bail!("config declares `workflows` without `main`; a workflow needs a Worker module to export its class");
        }
        do_classes.push(workflow_class(&script_name));
        sqlite_classes.push(workflow_class(&script_name));
    }
    let r2_buckets = match object.get("r2_buckets") {
        None => &[][..],
        Some(Value::Array(buckets)) => buckets.as_slice(),
        Some(_) => bail!("config `r2_buckets` must be an array"),
    };
    let mut r2_binding_names = BTreeSet::new();
    let mut r2_bucket_names = BTreeSet::new();
    for bucket in r2_buckets {
        let binding = bucket
            .get("binding")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("r2 bucket has no `binding`"))?;
        if !valid_binding(binding) {
            bail!("invalid r2 binding name: {binding:?}");
        }
        if !r2_binding_names.insert(binding.to_string()) {
            bail!("duplicate r2 binding name: {binding:?}");
        }
        let bucket_name = bucket
            .get("bucket_name")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                anyhow!("r2 binding {binding} has no `bucket_name`; celld uses the name to place the bucket's keys in the fleet bucket")
            })?;
        if !valid_resource_name(bucket_name) {
            bail!(
                "invalid r2 bucket name {bucket_name:?}: use letters, digits, `-` and `_` \
                 (not starting with `-`), at most 64 characters"
            );
        }
        if !r2_bucket_names.insert(bucket_name.to_string()) {
            bail!("two r2 bindings name the same bucket: {bucket_name:?}");
        }
        // Cloudflare scopes a jurisdiction to an account's R2; celld has
        // neither, and silently ignoring it would put the keys somewhere
        // the operator did not ask for.
        if bucket.get("jurisdiction").is_some() {
            bail!("r2 binding {binding} sets `jurisdiction`, which celld does not have");
        }
        bindings.push(json!({
            "type": "r2_bucket",
            "name": binding,
            "bucket_name": bucket_name,
        }));
    }
    // `worker_loaders` is how a Dynamic Workers project asks for a Worker
    // Loader. workerd declares the loaders in a `List(Binding)`, so a config
    // can name more than one, and each one holds its own cache of the Workers
    // it loaded. Two loaders are therefore two cache namespaces and not a
    // duplicate of one namespace, so celld binds every entry. The shared
    // binding-name check below refuses two entries that carry the same name.
    let worker_loaders = match object.get("worker_loaders") {
        None => &[][..],
        Some(Value::Array(loaders)) => loaders.as_slice(),
        Some(_) => bail!("config `worker_loaders` must be an array"),
    };
    for loader in worker_loaders {
        let Value::Object(loader) = loader else {
            bail!("worker loader must be an object with a `binding` name");
        };
        let binding = match loader.get("binding") {
            None => bail!("worker loader has no `binding` name"),
            Some(Value::String(binding)) => binding.as_str(),
            Some(_) => bail!("worker loader `binding` must be a string"),
        };
        if !valid_binding(binding) {
            bail!("invalid worker loader binding name: {binding:?}");
        }
        // Wrangler's worker_loaders entries contain only a binding name.
        // Resource limits and tail Fetchers belong to WorkerCode, because the
        // loader can create Workers with different limits and tail targets.
        // Refuse misplaced options instead of deploying without their effect.
        for key in loader.keys() {
            if key != "binding" {
                match key.as_str() {
                    "limits" => bail!("worker loader {binding} sets `limits`; set limits in WorkerCode or getEntrypoint()"),
                    "tails" => bail!("worker loader {binding} sets `tails`; set tails in WorkerCode"),
                    "allowExperimental" => bail!("worker loader {binding} sets `allowExperimental`, which celld does not support"),
                    _ => bail!("worker loader {binding} sets `{key}`, which is not a supported worker_loaders option"),
                }
            }
        }
        bindings.push(json!({
            "type": "worker_loader",
            "name": binding,
        }));
    }
    let mut vars = BTreeMap::new();
    match object.get("vars") {
        None => {}
        Some(Value::Object(object)) => {
            for (name, value) in object {
                let value = value
                    .as_str()
                    .ok_or_else(|| anyhow!("var binding {name} must be a string"))?;
                vars.insert(name.as_str(), value);
            }
        }
        Some(_) => bail!("config `vars` must be an object"),
    }
    // An asset-only project has no Worker to hand a variable to, so a
    // `.dev.vars` or `.env` beside it is residue, not a binding the guard below
    // should refuse in the config's name.
    let declared_vars = !vars.is_empty();
    if main.is_some() {
        match overrides {
            VarOverrides::None => {}
            VarOverrides::DevVars(entries) => vars.extend(
                entries
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str())),
            ),
            VarOverrides::DotEnv(entries) => {
                // A config var of the same name is the override the entry
                // exists for; every other binding owns its name.
                let taken = bindings
                    .iter()
                    .filter_map(|binding| binding.get("name").and_then(Value::as_str))
                    .chain(
                        assets
                            .as_ref()
                            .and_then(|assets| assets.config.binding.as_deref()),
                    )
                    .collect::<BTreeSet<_>>();
                for (name, value) in entries {
                    if !valid_binding(name) {
                        note!("warning: skipped the dotenv entry {name:?}, which is not a valid binding name");
                    } else if taken.contains(name.as_str()) {
                        note!("warning: skipped the dotenv entry {name:?}, whose name another binding uses");
                    } else {
                        vars.insert(name, value);
                    }
                }
            }
        }
    }
    for (name, value) in &vars {
        if !valid_binding(name) {
            bail!("invalid var binding name: {name:?}");
        }
        bindings.push(json!({
            "type": "plain_text",
            "name": name,
            "text": value,
        }));
    }
    if main.is_none()
        && (!do_classes.is_empty()
            || !sqlite_classes.is_empty()
            || service_count > 0
            || declared_vars
            || !r2_binding_names.is_empty()
            || !worker_loaders.is_empty())
    {
        bail!("an asset-only project cannot declare Worker bindings");
    }
    if let Some(binding) = assets
        .as_ref()
        .and_then(|assets| assets.config.binding.as_ref())
    {
        bindings.push(json!({
            "type": "assets",
            "name": binding,
        }));
    }
    // build_env installs every binding into one JavaScript object. Validate
    // that structural invariant once here, after every binding source has
    // contributed, so a new binding kind cannot silently shadow an old one.
    let mut binding_names = BTreeMap::new();
    for binding in &bindings {
        let name = binding.get("name").and_then(Value::as_str).unwrap_or("");
        let kind = binding.get("type").and_then(Value::as_str).unwrap_or("");
        if let Some(previous) = binding_names.insert(name, kind) {
            bail!(
                "binding name {name:?} is declared by both a {previous} and a {kind} \
                 binding; every name in `env` must be unique"
            );
        }
    }
    let mut metadata = Map::new();
    if main.is_some() {
        metadata.insert("main_module".into(), json!("index.js"));
    }
    if let Some(assets) = &assets {
        metadata.insert("assets".into(), assets.raw_metadata.clone());
    }
    if let Some(date) = object.get("compatibility_date") {
        metadata.insert("compatibility_date".into(), date.clone());
    }
    if let Some(flags) = object.get("compatibility_flags") {
        metadata.insert("compatibility_flags".into(), flags.clone());
    }
    metadata.insert("bindings".into(), Value::Array(bindings));
    if !queue_consumers.is_empty() {
        // Include consumer policy in deployment identity. The typed manifest
        // field below is the runtime contract; this copy keeps a settings-only
        // deploy from reusing the previous code-and-bindings version.
        metadata.insert("queue_consumers".into(), json!(queue_consumers));
    }
    if !sqlite_classes.is_empty() {
        metadata.insert(
            "migrations".into(),
            json!({ "new_sqlite_classes": sqlite_classes }),
        );
    }

    let containers = read_containers(object, &do_classes, &sqlite_classes)?;
    Ok(Project {
        script_name,
        no_bundle,
        entry: main,
        bundle,
        assets,
        metadata: Value::Object(metadata),
        do_classes,
        sqlite_classes,
        crons,
        has_workflows: !workflows.is_empty(),
        has_kv: !kv_namespaces.is_empty(),
        has_queues: !queue_producers.is_empty() || !queue_consumers.is_empty(),
        queue_consumers,
        has_r2: !r2_buckets.is_empty(),
        containers,
        python,
    })
}

/// The config's `define` and `rules`, as esbuild arguments.
///
/// Wrangler owns the spelling of both keys, so celld reads them as written
/// rather than inventing a celld-only escape hatch: a project keeps one
/// config, and a later move back to Wrangler bundles the same way.
fn read_bundle_config(object: &Map<String, Value>) -> anyhow::Result<BundleConfig> {
    let mut bundle = BundleConfig::default();
    match object.get("define") {
        None => {}
        Some(Value::Object(entries)) => {
            for (key, value) in entries {
                let Some(value) = value.as_str() else {
                    bail!("config `define` value for {key:?} must be a string");
                };
                // esbuild takes one `--define:KEY=VALUE` argument and splits
                // it at the first `=`. A key carrying `=` would move that
                // split and define a different name than the config names,
                // and a key carrying whitespace or a control character cannot
                // be a JavaScript reference at all.
                if key.is_empty()
                    || key.contains('=')
                    || key.chars().any(|c| c.is_whitespace() || c.is_control())
                {
                    bail!("invalid `define` key: {key:?}");
                }
                bundle.define.insert(key.clone(), value.to_string());
            }
        }
        Some(_) => bail!("config `define` must be an object"),
    }
    let rules = match object.get("rules") {
        None => &[][..],
        Some(Value::Array(rules)) => rules.as_slice(),
        Some(_) => bail!("config `rules` must be an array"),
    };
    for rule in rules {
        let Value::Object(rule) = rule else {
            bail!("module rule must be an object with a `type` and `globs`");
        };
        for key in rule.keys() {
            if !matches!(key.as_str(), "type" | "globs") {
                bail!("module rule sets `{key}`, which celld does not have");
            }
        }
        let Some(rule_type) = rule.get("type").and_then(Value::as_str) else {
            bail!("module rule has no `type` string");
        };
        let loader = match rule_type {
            "Text" => Loader::Text,
            "Data" => Loader::Binary,
            "CompiledWasm" => Loader::Copy,
            other => bail!(
                "module rule type {other:?} is not supported; celld supports \
                 Text, Data, and CompiledWasm"
            ),
        };
        let Some(Value::Array(globs)) = rule.get("globs") else {
            bail!("module rule {rule_type} must have a `globs` array");
        };
        if globs.is_empty() {
            bail!("module rule {rule_type} has no globs");
        }
        for glob in globs {
            let Some(glob) = glob.as_str() else {
                bail!("module rule glob must be a string");
            };
            let extension = rule_extension(glob)?;
            // Two rules that claim one extension have no single answer, and
            // picking either silently loads half the files the config lists
            // the wrong way.
            if let Some(previous) = bundle.loaders.insert(extension.clone(), loader) {
                if previous != loader {
                    bail!(
                        "two module rules claim {extension:?}: {} and {}",
                        previous.rule_type(),
                        loader.rule_type()
                    );
                }
            }
        }
    }
    Ok(bundle)
}

/// The file extension a Wrangler module glob selects.
///
/// esbuild chooses a loader by extension, and Wrangler chooses one by glob, so
/// only a glob that is exactly an extension match crosses that gap. celld
/// refuses every other glob rather than load a different set of files than the
/// config asks for.
fn rule_extension(glob: &str) -> anyhow::Result<String> {
    let extension = glob
        .strip_prefix("**/*")
        .or_else(|| glob.strip_prefix('*'))
        .filter(|extension| {
            extension.strip_prefix('.').is_some_and(|name| {
                !name.is_empty() && !name.contains(['*', '?', '.', '/', '\\', '[', '{'])
            })
        })
        .ok_or_else(|| {
            anyhow!(
                "module rule glob {glob:?} is not an extension match; celld \
                 supports a glob of the form `**/*.ext`"
            )
        })?;
    Ok(extension.to_string())
}

/// The config's `containers[]`. Each entry attaches a container to one
/// SQLite-backed Durable Object class of this Worker, which is what
/// Cloudflare requires too.
fn read_containers(
    object: &Map<String, Value>,
    do_classes: &[String],
    sqlite_classes: &[String],
) -> anyhow::Result<Vec<ContainerDecl>> {
    let Some(value) = object.get("containers") else {
        return Ok(Vec::new());
    };
    let entries = value
        .as_array()
        .ok_or_else(|| anyhow!("config `containers` must be an array"))?;
    let mut declared: Vec<ContainerDecl> = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let entry = entry
            .as_object()
            .ok_or_else(|| anyhow!("config `containers[{index}]` must be an object"))?;
        if let Some(key) = entry.keys().find(|key| {
            !matches!(
                key.as_str(),
                "class_name" | "image" | "name" | "instance_type" | "max_instances" | "runtime"
            )
        }) {
            bail!("config `containers[{index}]` declares `{key}`, which celld does not model");
        }
        let class_name = entry
            .get("class_name")
            .and_then(Value::as_str)
            .filter(|class| !class.is_empty())
            .ok_or_else(|| anyhow!("config `containers[{index}].class_name` must be a string"))?;
        if !do_classes.iter().any(|class| class == class_name) {
            bail!(
                "config `containers[{index}].class_name` {class_name:?} is not a Durable Object \
                 class of this Worker; declare it in `durable_objects.bindings`"
            );
        }
        if !sqlite_classes.iter().any(|class| class == class_name) {
            bail!(
                "config `containers[{index}].class_name` {class_name:?} must be SQLite-backed; \
                 add it to `migrations[].new_sqlite_classes`"
            );
        }
        if declared.iter().any(|decl| decl.class_name == class_name) {
            bail!("config `containers` names class {class_name:?} twice");
        }
        let image = entry
            .get("image")
            .and_then(Value::as_str)
            .filter(|image| !image.is_empty())
            .ok_or_else(|| anyhow!("config `containers[{index}].image` must be a string"))?;
        let name = entry
            .get("name")
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .ok_or_else(|| anyhow!("config `containers[{index}].name` must be a string"))
            })
            .transpose()?;
        let instance_type = entry
            .get("instance_type")
            .map(|value| {
                let name = value.as_str().ok_or_else(|| {
                    anyhow!("config `containers[{index}].instance_type` must be a string")
                })?;
                if crate::container::instance_resources(name).is_none() {
                    bail!(
                        "config `containers[{index}].instance_type` {name:?} is not an instance type"
                    );
                }
                Ok(name.to_string())
            })
            .transpose()?;
        let max_instances = entry
            .get("max_instances")
            .map(|value| {
                value.as_u64().ok_or_else(|| {
                    anyhow!(
                        "config `containers[{index}].max_instances` must be a non-negative integer"
                    )
                })
            })
            .transpose()?;
        let runtime = entry
            .get("runtime")
            .map(|value| {
                value
                    .as_str()
                    .filter(|runtime| !runtime.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        anyhow!(
                            "config `containers[{index}].runtime` must be a non-empty string, \
                             the name of an OCI runtime the node's daemon has, such as `runsc`"
                        )
                    })
            })
            .transpose()?;
        declared.push(ContainerDecl {
            class_name: class_name.to_string(),
            image: image.to_string(),
            name,
            instance_type,
            max_instances,
            runtime,
        });
    }
    Ok(declared)
}

fn reject_queue_keys(value: &Value, accepted: &[&str], kind: &str) -> anyhow::Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("queue {kind} must be an object"))?;
    if let Some(key) = object.keys().find(|key| !accepted.contains(&key.as_str())) {
        bail!("queue {kind} declares `{key}`, which celld does not model");
    }
    Ok(())
}

fn queue_name<'a>(value: &'a Value, kind: &str) -> anyhow::Result<&'a str> {
    let name = value
        .get("queue")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("queue {kind} has no `queue` name"))?;
    validate_queue_name(name, kind)?;
    Ok(name)
}

fn validate_queue_name(name: &str, kind: &str) -> anyhow::Result<()> {
    anyhow::ensure!(!name.is_empty(), "queue {kind} has an empty queue name");
    anyhow::ensure!(
        celld_logic::cell::valid_cell_scope(name),
        "queue {kind} has a name that cannot name a cell: {name:?}; use ASCII letters, digits, and `_ - . : $`"
    );
    Ok(())
}

fn optional_queue_u64(value: &Value, field: &str) -> anyhow::Result<Option<u64>> {
    value
        .get(field)
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| anyhow!("queue {field} must be a non-negative integer"))
        })
        .transpose()
}

fn optional_queue_u16(value: &Value, field: &str) -> anyhow::Result<Option<u16>> {
    optional_queue_u64(value, field)?
        .map(|value| {
            u16::try_from(value).map_err(|_| anyhow!("queue {field} is too large: {value}"))
        })
        .transpose()
}

fn optional_queue_u32(value: &Value, field: &str) -> anyhow::Result<Option<u32>> {
    optional_queue_u64(value, field)?
        .map(|value| {
            u32::try_from(value).map_err(|_| anyhow!("queue {field} is too large: {value}"))
        })
        .transpose()
}

/// The upstream workflow-name rule (`^[a-zA-Z0-9_][a-zA-Z0-9-_]*$`, at most 64
/// characters). Instance ids follow the same rule; the harness validates those
/// because they arrive at run time, not at deploy. An R2 `bucket_name` takes
/// the same rule for a second reason: the name becomes a key prefix inside the
/// fleet bucket, and this rule is what keeps it a single path segment, so a
/// binding cannot address the fleet's own deployment, cell, or lease keys.
pub(crate) fn valid_resource_name(name: &str) -> bool {
    name.len() <= 64
        && name
            .chars()
            .next()
            .is_some_and(|first| first == '_' || first.is_ascii_alphanumeric())
        && name
            .chars()
            .skip(1)
            .all(|value| value == '_' || value == '-' || value.is_ascii_alphanumeric())
}

/// The Wrangler-compatible Worker name that is safe in every place where
/// celld reuses it. Checking only the generated Workflow class would leave
/// deployment keys and operator output with a different accepted language.
pub(crate) fn valid_script_name(name: &str) -> bool {
    name.len() <= MAX_SCRIPT_NAME_BYTES
        && name
            .bytes()
            .next()
            .is_some_and(|first| first.is_ascii_lowercase() || first.is_ascii_digit())
        && name
            .bytes()
            .last()
            .is_some_and(|last| last.is_ascii_lowercase() || last.is_ascii_digit())
        && name
            .bytes()
            .all(|value| value.is_ascii_lowercase() || value.is_ascii_digit() || value == b'-')
}

/// `triggers.crons`, validated here so a malformed expression stops the deploy
/// the developer is watching instead of an activation an hour later. Wrangler
/// accepts `triggers` with other keys we do not model; only `crons` is read.
fn read_crons(project: &Map<String, Value>) -> anyhow::Result<Vec<String>> {
    let Some(value) = project.get("triggers") else {
        return Ok(Vec::new());
    };
    let triggers = value
        .as_object()
        .ok_or_else(|| anyhow!("config `triggers` must be an object"))?;
    let Some(value) = triggers.get("crons") else {
        return Ok(Vec::new());
    };
    let entries = value
        .as_array()
        .ok_or_else(|| anyhow!("config `triggers.crons` must be an array of strings"))?;
    let mut crons = Vec::new();
    for entry in entries {
        let expression = entry
            .as_str()
            .ok_or_else(|| anyhow!("config `triggers.crons` must be an array of strings"))?;
        celld_logic::cron::parse(expression).map_err(|error| anyhow!("{error}"))?;
        crons.push(expression.trim().to_string());
    }
    Ok(crons)
}

fn read_sqlite_classes(project: &Map<String, Value>) -> anyhow::Result<Vec<String>> {
    let Some(value) = project.get("migrations") else {
        return Ok(Vec::new());
    };
    let migrations = value
        .as_array()
        .ok_or_else(|| anyhow!("config `migrations` must be an array"))?;
    let mut tags = BTreeSet::new();
    let mut classes = BTreeSet::new();
    let mut result = Vec::new();
    for (index, migration) in migrations.iter().enumerate() {
        let migration = migration
            .as_object()
            .ok_or_else(|| anyhow!("config `migrations[{index}]` must be an object"))?;
        let unsupported = migration
            .keys()
            .filter(|key| !matches!(key.as_str(), "tag" | "new_sqlite_classes"))
            .cloned()
            .collect::<Vec<_>>();
        if !unsupported.is_empty() {
            bail!(
                "`celld deploy` does not support these migration keys: {}.\n\
                 Class rename, delete, transfer, and non-SQLite migration semantics need an explicit persisted-state contract before deployment.",
                unsupported.join(", ")
            );
        }
        let tag = migration
            .get("tag")
            .and_then(Value::as_str)
            .filter(|tag| !tag.is_empty())
            .ok_or_else(|| {
                anyhow!("config `migrations[{index}].tag` must be a non-empty string")
            })?;
        if !tags.insert(tag.to_string()) {
            bail!("config has duplicate migration tag {tag:?}");
        }
        let Some(value) = migration.get("new_sqlite_classes") else {
            continue;
        };
        let new_classes = value.as_array().ok_or_else(|| {
            anyhow!("config `migrations[{index}].new_sqlite_classes` must be an array")
        })?;
        for (class_index, class) in new_classes.iter().enumerate() {
            let class = class.as_str().filter(|class| !class.is_empty()).ok_or_else(|| {
                anyhow!(
                    "config `migrations[{index}].new_sqlite_classes[{class_index}]` must be a non-empty string"
                )
            })?;
            if !classes.insert(class.to_string()) {
                bail!("SQLite class {class:?} is introduced by more than one migration");
            }
            result.push(class.to_string());
        }
    }
    Ok(result)
}

fn read_asset_project(
    value: &Value,
    root: &Path,
    project: &Map<String, Value>,
) -> anyhow::Result<ProjectAssets> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow!("config `assets` must be an object"))?;
    let supported = [
        "directory",
        "binding",
        "html_handling",
        "not_found_handling",
        "run_worker_first",
    ];
    let unsupported = object
        .keys()
        .filter(|key| !supported.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !unsupported.is_empty() {
        bail!(
            "`celld deploy` does not support these assets keys: {}",
            unsupported.join(", ")
        );
    }
    let directory = object
        .get("directory")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("config `assets.directory` must be a string"))?;
    let directory = project_relative_path(directory, "assets.directory")?;
    let directory = root.join(directory);
    let metadata = std::fs::symlink_metadata(&directory)
        .with_context(|| format!("inspect asset directory {}", directory.display()))?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
        bail!(
            "asset directory {} is not a regular directory",
            directory.display()
        );
    }
    // The check above sees only the final component: a directory link on the
    // way there would still escape the project. Everything below — the
    // directives, the walk, the reads — starts from the contained path.
    let directory = contain_path(root, &directory, "config `assets.directory`")?;

    let binding = object
        .get("binding")
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| anyhow!("config `assets.binding` must be a string"))
                .and_then(|binding| {
                    if valid_binding(binding) {
                        Ok(binding.to_string())
                    } else {
                        bail!("invalid asset binding name: {binding:?}")
                    }
                })
        })
        .transpose()?;
    let html_handling = optional_asset_mode(
        object,
        "html_handling",
        "auto-trailing-slash",
        &[
            "auto-trailing-slash",
            "force-trailing-slash",
            "drop-trailing-slash",
            "none",
        ],
    )?;
    let not_found_handling = optional_asset_mode(
        object,
        "not_found_handling",
        "none",
        &["none", "single-page-application", "404-page"],
    )?;
    let run_worker_first = object
        .get("run_worker_first")
        .map(|value| {
            serde_json::from_value::<RunWorkerFirst>(value.clone())
                .context("config `assets.run_worker_first` must be a boolean or route list")
        })
        .transpose()?
        .unwrap_or_default();
    validate_worker_first(&run_worker_first)?;

    let headers = read_asset_directive(&directory, "_headers")?;
    let redirects = read_asset_directive(&directory, "_redirects")?;
    let compatibility_date = project
        .get("compatibility_date")
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| anyhow!("config `compatibility_date` must be a string"))
        })
        .transpose()?;
    let compatibility_flags = project
        .get("compatibility_flags")
        .map(|value| {
            value
                .as_array()
                .ok_or_else(|| anyhow!("config `compatibility_flags` must be an array"))?
                .iter()
                .map(|flag| {
                    flag.as_str()
                        .map(str::to_string)
                        .ok_or_else(|| anyhow!("compatibility flags must be strings"))
                })
                .collect::<anyhow::Result<Vec<_>>>()
        })
        .transpose()?
        .unwrap_or_default();

    // Retain the Wrangler-shaped upload metadata as well as the normalized
    // index config. Defaults and compatibility settings belong in the index;
    // upload metadata includes only the values Wrangler would send.
    let mut upload_config = Map::new();
    for key in ["html_handling", "not_found_handling", "run_worker_first"] {
        if let Some(value) = object.get(key) {
            upload_config.insert(key.to_string(), value.clone());
        }
    }
    if let Some(headers) = &headers {
        upload_config.insert("_headers".to_string(), Value::String(headers.clone()));
    }
    if let Some(redirects) = &redirects {
        upload_config.insert("_redirects".to_string(), Value::String(redirects.clone()));
    }

    Ok(ProjectAssets {
        directory,
        config: AssetConfig {
            binding,
            html_handling: Some(html_handling),
            not_found_handling: Some(not_found_handling),
            run_worker_first,
            headers,
            redirects,
            compatibility_date,
            compatibility_flags,
        },
        raw_metadata: json!({ "config": Value::Object(upload_config) }),
    })
}

fn project_relative_path(value: &str, key: &str) -> anyhow::Result<String> {
    let value = value.strip_prefix("./").unwrap_or(value);
    let path = Path::new(value);
    if value.is_empty()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        bail!("config `{key}` must be a path inside the project");
    }
    Ok(value.to_string())
}

/// The canonical project root: `root` with every link resolved. Every
/// deploy-time read of the project tree must resolve inside it, and
/// `contain_path` enforces that per path. Canonicalizing is portable across
/// the Linux and macOS hosts this builds on, where `openat2` is not.
fn canonical_project_root(root: &Path) -> anyhow::Result<PathBuf> {
    root.canonicalize()
        .with_context(|| format!("resolve project directory {}", root.display()))
}

/// Canonicalize `path` and refuse it when it resolves outside
/// `canonical_root`. `what` names the config value in the error. A path that
/// stays inside — including one that crosses a link to elsewhere in the
/// tree — resolves to the path the read must go through. As with every
/// metadata-then-read check in the deploy, the tree is assumed static while
/// the deploy runs: a link swapped between this check and the read is out of
/// model, as a hard link to the same bytes would be regardless.
fn contain_path(canonical_root: &Path, path: &Path, what: &str) -> anyhow::Result<PathBuf> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("resolve {what} {}", path.display()))?;
    if !canonical.starts_with(canonical_root) {
        bail!("{what} escapes the project directory: {}", path.display());
    }
    Ok(canonical)
}

/// Where a deploy's build inputs may come from: the canonical project
/// directory plus the operator's `--read-root` directories. esbuild's inputs
/// and container builds (the Dockerfile and its context) must resolve inside
/// one of them; every other project read stays inside the project directory.
#[derive(Debug)]
struct ReadBounds {
    project: PathBuf,
    extra: Vec<PathBuf>,
}

impl ReadBounds {
    fn new(project: &Path, read_roots: &[PathBuf]) -> anyhow::Result<Self> {
        let project = canonical_project_root(project)?;
        let extra = read_roots
            .iter()
            .map(|root| {
                let canonical = root
                    .canonicalize()
                    .with_context(|| format!("resolve --read-root {}", root.display()))?;
                if !canonical.is_dir() {
                    bail!("--read-root {} is not a directory", root.display());
                }
                Ok(canonical)
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Self { project, extra })
    }

    /// The canonical project directory.
    fn project(&self) -> &Path {
        &self.project
    }

    /// Every directory a build input may come from, the project first.
    fn roots(&self) -> impl Iterator<Item = &Path> {
        std::iter::once(self.project.as_path()).chain(self.extra.iter().map(PathBuf::as_path))
    }

    fn contains(&self, path: &Path) -> bool {
        self.roots().any(|root| path.starts_with(root))
    }

    /// `contain_path` against every bound.
    fn contain(&self, path: &Path, what: &str) -> anyhow::Result<PathBuf> {
        let canonical = path
            .canonicalize()
            .with_context(|| format!("resolve {what} {}", path.display()))?;
        if !self.contains(&canonical) {
            bail!("{what} escapes the project directory: {}", path.display());
        }
        Ok(canonical)
    }
}

/// The config file and the canonical project root: the config's directory,
/// canonicalized, with the config itself required to resolve inside it. A
/// config that is a link outside the tree would otherwise let another file's
/// contents be read as this project's config.
fn resolve_project_config(given: Option<PathBuf>) -> anyhow::Result<(PathBuf, PathBuf)> {
    let config_path = resolve_config(given)?;
    let root = config_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let root = canonical_project_root(&root)?;
    let name = config_path
        .file_name()
        .ok_or_else(|| anyhow!("config {} has no file name", config_path.display()))?;
    let path = root.join(name);
    let canonical = contain_path(&root, &path, "the project config")?;
    Ok((canonical, root))
}

/// The contained path of the `main` entry: a regular file, not a link, that
/// resolves inside the canonical root. Validation and every build path share
/// it, so the checked file and the read file resolve the same way.
fn contained_entry(root: &Path, entry: &str) -> anyhow::Result<PathBuf> {
    let root = canonical_project_root(root)?;
    let path = root.join(entry);
    let metadata = std::fs::symlink_metadata(&path)
        .with_context(|| format!("inspect entry point {}", path.display()))?;
    if !metadata.file_type().is_file() {
        bail!("entry point {} is not a regular file", path.display());
    }
    contain_path(&root, &path, "config `main`")
}

fn optional_asset_mode(
    object: &Map<String, Value>,
    key: &str,
    default: &str,
    supported: &[&str],
) -> anyhow::Result<String> {
    let value = match object.get(key) {
        Some(value) => value
            .as_str()
            .ok_or_else(|| anyhow!("config `assets.{key}` must be a string"))?,
        None => default,
    };
    if !supported.contains(&value) {
        bail!("unsupported assets.{key} value: {value:?}");
    }
    Ok(value.to_string())
}

fn valid_binding(binding: &str) -> bool {
    binding.len() <= 128
        && binding
            .chars()
            .next()
            .is_some_and(|value| value == '_' || value == '$' || value.is_ascii_alphabetic())
        && binding
            .chars()
            .skip(1)
            .all(|value| value == '_' || value == '$' || value.is_ascii_alphanumeric())
}

fn validate_worker_first(value: &RunWorkerFirst) -> anyhow::Result<()> {
    let RunWorkerFirst::Routes(routes) = value else {
        return Ok(());
    };
    if routes.is_empty() || routes.len() > 100 {
        bail!("asset worker-first routes must contain between 1 and 100 rules");
    }
    let mut positive = false;
    let mut seen = std::collections::HashSet::new();
    for route in routes {
        if route.is_empty()
            || route.len() > 100
            || route.contains(['\\', '\0'])
            || (!route.starts_with('/') && !route.starts_with("!/"))
            || !seen.insert(route)
        {
            bail!("invalid asset worker-first route: {route:?}");
        }
        positive |= route.starts_with('/');
    }
    if !positive {
        bail!("asset worker-first routes require a positive rule");
    }
    Ok(())
}

fn read_asset_directive(directory: &Path, name: &str) -> anyhow::Result<Option<String>> {
    let path = directory.join(name);
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).with_context(|| format!("inspect {}", path.display())),
    };
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        bail!("asset directive {} is not a regular file", path.display());
    }
    if metadata.len() > MAX_ASSET_DIRECTIVE_BYTES {
        bail!("asset directive {} exceeds 100 KiB", path.display());
    }
    let contents =
        std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
    if contents.len() as u64 > MAX_ASSET_DIRECTIVE_BYTES {
        bail!("asset directive {} exceeds 100 KiB", path.display());
    }
    Ok(Some(contents))
}

fn build_assets(project: &ProjectAssets) -> anyhow::Result<BuiltAssets> {
    let max_file_bytes = crate::env_vars::max_asset_file_bytes()?;
    let mut files = Vec::new();
    collect_asset_files(&project.directory, "", &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    if files.len() > MAX_ASSET_FILES {
        bail!("asset directory exceeds the {MAX_ASSET_FILES}-file limit");
    }

    let mut entries = BTreeMap::new();
    let mut blobs = BTreeMap::new();
    let mut total_bytes = 0_u64;
    for (relative, path) in files {
        let metadata =
            std::fs::metadata(&path).with_context(|| format!("inspect {}", path.display()))?;
        let asset_path = format!("/{relative}");
        crate::assets::validate_asset_file_size(&asset_path, metadata.len(), max_file_bytes)?;
        total_bytes = total_bytes
            .checked_add(metadata.len())
            .context("asset byte count overflow")?;
        if total_bytes > MAX_ASSET_BYTES {
            bail!("asset directory exceeds the 1 GiB deployment limit");
        }
        let body = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
        if body.len() as u64 != metadata.len() {
            bail!("asset changed while being read: {}", path.display());
        }
        let sha256 = format!("{:x}", Sha256::digest(&body));
        blobs.entry(sha256.clone()).or_insert(body);
        entries.insert(
            asset_path,
            AssetEntry {
                sha256,
                bytes: metadata.len(),
                content_type: asset_content_type(&path).map(str::to_string),
            },
        );
    }
    let file_count = u32::try_from(entries.len()).context("asset file count overflow")?;
    let index = serde_json::to_vec(&AssetIndex {
        schema_version: 1,
        entries,
        config: project.config.clone(),
    })?;
    Ok(BuiltAssets {
        index,
        blobs,
        file_count,
        total_bytes,
    })
}

fn collect_asset_files(
    directory: &Path,
    relative: &str,
    files: &mut Vec<(String, PathBuf)>,
) -> anyhow::Result<()> {
    let mut entries = std::fs::read_dir(directory)
        .with_context(|| format!("read asset directory {}", directory.display()))?
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let name = entry.file_name().into_string().map_err(|_| {
            anyhow!(
                "asset path contains a non-UTF-8 name under {}",
                directory.display()
            )
        })?;
        if relative.is_empty() && name == ".assetsignore" {
            bail!(
                "{} is not supported by `celld deploy`; remove it or deploy with Wrangler",
                entry.path().display()
            );
        }
        if relative.is_empty() && (name == "_worker.js" || name.starts_with("_worker.js/")) {
            bail!(
                "refusing to publish reserved Worker source as an asset: {}",
                entry.path().display()
            );
        }
        if relative.is_empty() && matches!(name.as_str(), "_headers" | "_redirects") {
            continue;
        }
        if name.contains(['\\', '\0']) {
            bail!("invalid asset path component: {name:?}");
        }
        let child_relative = if relative.is_empty() {
            name
        } else {
            format!("{relative}/{name}")
        };
        if child_relative.len() + 1 > 1024 {
            bail!("asset path exceeds 1024 bytes: /{child_relative}");
        }
        let asset_path = format!("/{child_relative}");
        if !crate::assets::is_canonical_asset_path(&asset_path) {
            bail!("asset path is not canonical: {asset_path:?}");
        }
        let file_type = entry
            .file_type()
            .with_context(|| format!("inspect asset {}", entry.path().display()))?;
        if file_type.is_symlink() {
            bail!(
                "asset tree contains a symbolic link: {}",
                entry.path().display()
            );
        } else if file_type.is_dir() {
            collect_asset_files(&entry.path(), &child_relative, files)?;
        } else if file_type.is_file() {
            files.push((child_relative, entry.path()));
        } else {
            bail!(
                "asset tree contains a special file: {}",
                entry.path().display()
            );
        }
    }
    Ok(())
}

fn asset_content_type(path: &Path) -> Option<&'static str> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    Some(match extension.as_str() {
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" | "cjs" => "text/javascript; charset=utf-8",
        "json" | "map" => "application/json; charset=utf-8",
        "txt" => "text/plain; charset=utf-8",
        "xml" => "application/xml; charset=utf-8",
        "svg" => "image/svg+xml",
        "webmanifest" => "application/manifest+json; charset=utf-8",
        "wasm" => "application/wasm",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "avif" => "image/avif",
        "ico" => "image/x-icon",
        "pdf" => "application/pdf",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "mp3" => "audio/mpeg",
        "wav" => "audio/wav",
        "ogg" => "audio/ogg",
        "mp4" => "video/mp4",
        "webm" => "video/webm",
        "zip" => "application/zip",
        "gz" => "application/gzip",
        "br" => "application/octet-stream",
        _ => return None,
    })
}

/// The entry module and wasm files, keyed by their deployed module names.
struct BundleOutput {
    bundle: Vec<u8>,
    wasm: Vec<(String, Vec<u8>)>,
    /// The entry's source map, when the build produced one. `None` for
    /// Python and asset-only projects, and for pre-bundled entries without
    /// a sibling map.
    sourcemap: Option<Vec<u8>>,
}

/// Drop a trailing `//# sourceMappingURL=` line from esbuild output, if one
/// is there.
///
/// The map run asks esbuild for an external map (no comment), so this is
/// purely defensive: whatever map mode produced the bytes, the deployed
/// module must equal the bytes of a run without the flag.
fn strip_sourcemap_comment(mut bundle: Vec<u8>) -> Vec<u8> {
    let mut end = bundle.len();
    if bundle.last() == Some(&b'\n') {
        end -= 1;
    }
    let line_start = bundle[..end]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    if bundle[line_start..end].starts_with(b"//# sourceMappingURL=") {
        bundle.truncate(line_start);
    }
    bundle
}

/// Wrangler's default unbundled WASM rule discovers `**/*.wasm` and
/// `**/*.wasm?module` below the entry directory. Discover files rather than
/// parsing JavaScript or running esbuild: the caller's toolchain owns the
/// source and its import spellings.
fn collect_unbundled_wasm(
    directory: &Path,
    relative: &str,
    entry_path: &Path,
    wasm: &mut Vec<(String, Vec<u8>)>,
) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(directory)
        .with_context(|| format!("read wasm directory {}", directory.display()))?
    {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        // The entry already occupies the main-module slot. If it has a WASM
        // suffix, collecting it again creates two modules from one file.
        if path == entry_path {
            continue;
        }
        let is_wasm = path
            .extension()
            .is_some_and(|extension| extension == "wasm")
            || path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".wasm?module"));
        // Never follow directory symlinks or read special files. Unrelated
        // files do not participate in module discovery.
        if !file_type.is_dir() && !is_wasm {
            continue;
        }
        let name = entry.file_name().into_string().map_err(|_| {
            anyhow!(
                "wasm path contains a non-UTF-8 name under {}",
                directory.display()
            )
        })?;
        // Wrangler excludes its state tree. The other exclusions prevent a
        // project-root entry from turning repository or celld state into an
        // application module when celld performs the same recursive scan.
        if file_type.is_dir() && matches!(name.as_str(), ".git" | ".celld" | ".wrangler") {
            continue;
        }
        if name.contains(['\\', '\0']) {
            bail!("invalid wasm path component: {name:?}");
        }
        let name = if relative.is_empty() {
            name
        } else {
            format!("{relative}/{name}")
        };
        if file_type.is_dir() {
            collect_unbundled_wasm(&path, &name, entry_path, wasm)?;
        } else if file_type.is_file() {
            let bytes = std::fs::read(&path)
                .with_context(|| format!("read wasm module {}", path.display()))?;
            wasm.push((name, bytes));
        } else {
            bail!("wasm module {} is not a regular file", path.display());
        }
    }
    Ok(())
}

fn run_esbuild(
    root: &Path,
    bounds: &ReadBounds,
    entry: &Path,
    config: &BundleConfig,
    sourcemap: bool,
) -> anyhow::Result<BundleOutput> {
    // node: builtins stay external. Wrangler polyfills them with unenv; celld
    // implements the workerd `nodejs_compat` subset itself, so the runtime
    // provides them.
    let binary = std::env::var("CELLD_ESBUILD").unwrap_or_else(|_| "esbuild".to_string());
    let outdir = tempfile::tempdir().context("create esbuild output directory")?;
    // The metafile lives apart from the output directory, whose files are
    // scanned for copied modules below: a `.json` copy rule must never pick
    // it up as one.
    let metadir = tempfile::tempdir().context("create esbuild metafile directory")?;
    let metafile = metadir.path().join("meta.json");
    // esbuild leaves an external builtin from a CommonJS dependency behind
    // its synchronous __require helper. Workerd's Wrangler plugin rewrites
    // that call to an ESM import, but the esbuild CLI has no equivalent
    // plugin hook. Give only this generated bundle a lexical bridge to the
    // same builtin table. A runtime global would make require visible to raw
    // ESM Workers, which Workerd does not do.
    let commonjs_builtin_bridge = r#"var require = (id) => {
  const name = String(id);
  const builtin = process.getBuiltinModule(name);
  if (builtin === undefined) {
    const error = new Error(`Cannot find module '${name}'`);
    error.code = "MODULE_NOT_FOUND";
    throw error;
  }
  return builtin;
};"#;
    // esbuild may read only the project, the operator's read roots, and its
    // own two directories: an outside import, input source map, or tsconfig
    // `extends` then fails
    // like a missing file, before any of its bytes can reach the bundle,
    // the map, or esbuild's error output (which quotes source lines).
    let Some(program) = confine::locate_executable(&binary) else {
        bail!(
            "esbuild not found ({binary}).\n\
             `celld deploy` bundles with esbuild; install it and retry,\n\
             or set CELLD_ESBUILD to its path."
        );
    };
    let readable: Vec<&Path> = bounds
        .roots()
        .chain([outdir.path(), metadir.path()])
        .collect();
    let confinement = confine::ReadConfinement::beneath(&readable, &program)?;
    #[cfg(target_os = "linux")]
    if let confine::ReadConfinement::Unavailable(reason) = &confinement {
        note!(
            "warning: esbuild runs unconfined: {reason}. Bundled files are still \
             checked against the project and its read roots, but an outside input source map, \
             tsconfig, or failing file can reach the source map or the error output."
        );
    }
    let mut command = Command::new(&program);
    command
        .current_dir(root)
        .arg(entry)
        .arg("--bundle")
        .arg("--format=esm")
        .arg("--platform=browser")
        .arg("--target=es2024")
        .arg("--conditions=workerd,worker,browser")
        .arg(format!("--banner:js={commonjs_builtin_bridge}"))
        .arg("--external:node:*")
        .arg("--external:cloudflare:*")
        .args(
            crate::js::BARE_NODE_BUILTINS
                .iter()
                .map(|specifier| format!("--external:{specifier}")),
        )
        .args(
            config.loaders.iter().map(|(extension, loader)| {
                format!("--loader:{extension}={}", loader.esbuild_name())
            }),
        )
        .args(
            config
                .define
                .iter()
                .map(|(key, value)| format!("--define:{key}={value}")),
        )
        .arg(format!("--outdir={}", outdir.path().display()))
        .arg(format!("--metafile={}", metafile.display()))
        .arg("--entry-names=index")
        // `external` emits `index.js.map` without appending a
        // `//# sourceMappingURL=` comment, so the bundle equals a run
        // without the flag. The strip below still applies on top.
        .args(sourcemap.then_some("--sourcemap=external"));
    confinement.apply(&mut command);
    let output = command.output().map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            anyhow!(
                "esbuild not found ({binary}).\n\
                     `celld deploy` bundles with esbuild; install it and retry,\n\
                     or set CELLD_ESBUILD to its path."
            )
        } else if error.kind() == std::io::ErrorKind::PermissionDenied && confinement.confines() {
            anyhow!(
                "run esbuild {}: {error}. esbuild runs confined to reads of the \
                 project and its read roots; if this is a wrapper script, it cannot read its \
                 interpreter, so point CELLD_ESBUILD at the self-contained \
                 release binary.",
                program.display()
            )
        } else {
            anyhow!("run esbuild: {error}")
        }
    })?;
    if !output.status.success() {
        bail!(
            "esbuild failed:\n{}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    // Before anything esbuild wrote is used: every file it read must lie in
    // the project or a read root. esbuild follows `..` imports, absolute imports, and
    // directory links wherever they lead, and the deploy may run with more
    // privilege than the project's author, so an import must never pull a
    // file from outside the tree into the deployed bundle or its map.
    // The check stands on its own where the kernel offers no confinement,
    // and backs it up where it does.
    let meta = std::fs::read(&metafile).context("read esbuild metafile")?;
    contain_bundle_inputs(bounds, &meta)?;
    // The strip keeps the deployed module byte-identical to a run without
    // the flag even if an esbuild ever appends the comment despite `external`.
    let bundle = strip_sourcemap_comment(
        std::fs::read(outdir.path().join("index.js")).context("read esbuild output bundle")?,
    );
    let sourcemap = sourcemap
        .then(|| {
            let map = std::fs::read(outdir.path().join("index.js.map"))
                .context("read esbuild source map")?;
            rebase_map_sources(&map, outdir.path(), root)
        })
        .transpose()?;
    // The copied files land beside the bundle; each becomes its own deployed
    // module under the name the rewritten imports use. Only the `copy` loader
    // emits a sibling — `text` and `binary` put the file inside the bundle —
    // so the emitted set follows the same extension map the run used. It must
    // follow that map and not a fixed `.wasm` test: a config that gives
    // `.wasm` a different rule emits no wasm, and a `CompiledWasm` rule on
    // another extension emits one under that extension.
    let copied: BTreeSet<&str> = config
        .loaders
        .iter()
        .filter(|(_, loader)| **loader == Loader::Copy)
        .map(|(extension, _)| extension.as_str())
        .collect();
    let mut wasm = Vec::new();
    for dirent in std::fs::read_dir(outdir.path()).context("read esbuild output directory")? {
        let path = dirent?.path();
        let is_copied = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| {
                // The bundle already holds the main-module slot. A rule that
                // claims `.js` would otherwise deploy it a second time, and
                // one that claims `.map` would ship the `--sourcemap` output.
                name != "index.js"
                    && name != "index.js.map"
                    && copied.iter().any(|extension| name.ends_with(extension))
            });
        if is_copied {
            let name = path
                .file_name()
                .expect("read_dir entries have a file name")
                .to_string_lossy()
                .into_owned();
            let bytes = std::fs::read(&path)
                .with_context(|| format!("read wasm module {}", path.display()))?;
            wasm.push((name, bytes));
        }
    }
    // read_dir order is platform-defined; the deployment version hashes the
    // module list, so keep it stable.
    wasm.sort();
    Ok(BundleOutput {
        bundle,
        wasm,
        sourcemap,
    })
}

/// Refuse a bundle that read any file outside `bounds`. `meta` is the esbuild
/// metafile from a run whose working directory was the project root, so each
/// file input is named relative to it (or absolutely). Every
/// input that names an existing path is checked, whatever it looks like;
/// only one that names nothing and is spelled like another namespace
/// (`(disabled):…`, `<data:…>`, `dataurl:…`) read no file and is skipped.
/// Any other input fails closed.
fn contain_bundle_inputs(bounds: &ReadBounds, meta: &[u8]) -> anyhow::Result<()> {
    let root = bounds.project();
    let meta: Value = serde_json::from_slice(meta).context("parse esbuild metafile")?;
    let inputs = meta
        .get("inputs")
        .and_then(Value::as_object)
        .context("esbuild metafile has no inputs")?;
    for input in inputs.keys() {
        let namespaced = input.starts_with('(')
            || input.starts_with('<')
            || input
                .split_once(':')
                .is_some_and(|(namespace, _)| !namespace.is_empty() && !namespace.contains('/'));
        if namespaced && std::fs::symlink_metadata(root.join(input)).is_err() {
            continue;
        }
        // The error names the input as esbuild keyed it, never the resolved
        // target, so a refusal discloses nothing beyond the project's own
        // spelling.
        let canonical = root
            .join(input)
            .canonicalize()
            .with_context(|| format!("resolve a bundled module {input}"))?;
        if !bounds.contains(&canonical) {
            bail!("a bundled module escapes the project directory: {input}");
        }
    }
    Ok(())
}

/// Rewrite the map's `sources` to project-root-relative paths.
///
/// `base` is the directory relative sources resolve against: esbuild's
/// output directory — a tempdir deleted right after the run, so stored
/// as-is every source would dangle. esbuild, not the caller, writes these
/// strings, so resolving them on the host is safe here; a caller-produced
/// sibling map goes through [`rebase_sibling_map`] instead. A source inside the
/// project root becomes a root-relative slash path, one outside it a
/// `../` path from the root, and one that does not resolve its bare file
/// name. No source names a directory above the common ancestor of the
/// root and the file, so the map discloses nothing of the host's layout
/// beyond what the project imports.
fn rebase_map_sources(map: &[u8], base: &Path, root: &Path) -> anyhow::Result<Vec<u8>> {
    let mut parsed: Value = serde_json::from_slice(map).context("parse source map")?;
    let Some(sources) = parsed.get_mut("sources").and_then(Value::as_array_mut) else {
        return Ok(map.to_vec());
    };
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    for source in sources.iter_mut() {
        let Some(name) = source.as_str() else {
            continue;
        };
        *source = Value::String(rebase_map_source(&root, base, name));
    }
    serde_json::to_vec_pretty(&parsed).context("encode rebased source map")
}

/// Largest caller-produced sibling map `--sourcemap` collects. Worker maps
/// past a few tens of MiB are already pathological; the cap keeps a
/// tenant-planted file from driving the deploy's allocation.
const SIBLING_MAP_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// The sibling `<entry>.map` of a `no_bundle` entry, rebased to
/// project-root-relative sources, or `None`. The map comes from the
/// project tree, which the caller (often a tenant) controls, while the
/// deploy may run with more privilege, so nothing about it can fail the
/// deploy or reach outside the tree: a missing sibling is `None`, and any
/// other problem — the entry's directory resolving outside the root, a
/// symlink, a FIFO or other non-regular file, an oversize file, invalid
/// JSON, or JSON that is not a v3 source map — warns and is `None` too,
/// the same "warns and writes nothing" contract as an unmappable project.
fn sibling_sourcemap(entry: &Path, root: &Path) -> Option<Vec<u8>> {
    match read_sibling_sourcemap(entry, root) {
        Ok(map) => map,
        Err(error) => {
            note!("--sourcemap: skipping the `no_bundle` sibling map: {error:#}");
            None
        }
    }
}

fn read_sibling_sourcemap(entry: &Path, root: &Path) -> anyhow::Result<Option<Vec<u8>>> {
    use std::io::Read;
    use std::os::unix::fs::OpenOptionsExt;
    let root = root.canonicalize().context("resolve the project root")?;
    // The whole resolution stays in the tree: a directory symlink anywhere
    // on the entry's path would otherwise lead the read outside it.
    let dir = entry
        .parent()
        .context("entry has no directory")?
        .canonicalize()
        .context("resolve the entry's directory")?;
    let Ok(relative_dir) = dir.strip_prefix(&root) else {
        bail!("the entry's directory resolves outside the project root");
    };
    let mut name = entry
        .file_name()
        .context("entry has no file name")?
        .to_os_string();
    name.push(".map");
    let path = dir.join(name);
    // The fd is the check: `O_NOFOLLOW` refuses a symlink at open,
    // `O_NONBLOCK` keeps a FIFO from blocking the open, and the fstat
    // below refuses everything that is not a regular file.
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(&path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("open {} (symlinks are not followed)", path.display()))
        }
    };
    let metadata = file
        .metadata()
        .with_context(|| format!("inspect {}", path.display()))?;
    if !metadata.file_type().is_file() {
        bail!("{} is not a regular file", path.display());
    }
    if metadata.len() > SIBLING_MAP_MAX_BYTES {
        bail!(
            "{} is larger than {SIBLING_MAP_MAX_BYTES} bytes",
            path.display()
        );
    }
    let mut map = Vec::new();
    file.take(SIBLING_MAP_MAX_BYTES + 1)
        .read_to_end(&mut map)
        .with_context(|| format!("read {}", path.display()))?;
    if map.len() as u64 > SIBLING_MAP_MAX_BYTES {
        bail!(
            "{} is larger than {SIBLING_MAP_MAX_BYTES} bytes",
            path.display()
        );
    }
    rebase_sibling_map(&map, relative_dir).map(Some)
}

/// Validate a caller-produced map as a v3 source map and rewrite its
/// `sources` (with any `sourceRoot` folded in) to project-root-relative
/// paths. Unlike the esbuild map's rebase, nothing here touches the
/// filesystem: the strings are tenant-written, so resolving them on the
/// host would answer which host paths exist.
fn rebase_sibling_map(map: &[u8], relative_dir: &Path) -> anyhow::Result<Vec<u8>> {
    let mut parsed: Value = serde_json::from_slice(map).context("parse source map")?;
    let object = parsed
        .as_object_mut()
        .context("a source map is a JSON object")?;
    if object.get("version").and_then(Value::as_u64) != Some(3) {
        bail!("not a version 3 source map");
    }
    if !object.get("mappings").is_some_and(Value::is_string) {
        bail!("the source map has no `mappings` string");
    }
    let source_root = match object.remove("sourceRoot") {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(value)) => value,
        Some(_) => bail!("the source map's `sourceRoot` is not a string"),
    };
    let sources = object
        .get_mut("sources")
        .and_then(Value::as_array_mut)
        .context("the source map has no `sources` array")?;
    for source in sources.iter_mut() {
        let name = source
            .as_str()
            .context("a source map `sources` entry is not a string")?;
        // An absolute or URL source ignores `sourceRoot`, as URL resolution
        // would; either way it keeps only its file name below.
        let joined =
            if source_root.is_empty() || name.contains("://") || Path::new(name).is_absolute() {
                name.to_string()
            } else {
                format!("{}/{name}", source_root.trim_end_matches('/'))
            };
        *source = Value::String(lexical_map_source(relative_dir, &joined));
    }
    serde_json::to_vec_pretty(&parsed).context("encode rebased source map")
}

/// `source`, relative to the entry's root-relative directory, as a
/// root-relative slash path, resolved lexically. An absolute path or a
/// URL keeps only its file name: relativizing it would need the root's
/// host location, which the map must not reveal.
fn lexical_map_source(relative_dir: &Path, source: &str) -> String {
    use std::path::Component;
    let candidate = Path::new(source);
    let bare = || {
        candidate
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    };
    if source.contains("://") || candidate.is_absolute() {
        return bare();
    }
    let mut parts: Vec<String> = relative_dir
        .components()
        .map(|part| part.as_os_str().to_string_lossy().into_owned())
        .collect();
    let mut up = 0usize;
    for component in candidate.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            Component::ParentDir => {
                if parts.pop().is_none() {
                    up += 1;
                }
            }
            Component::RootDir | Component::Prefix(_) => return bare(),
        }
    }
    std::iter::repeat_n("..".to_string(), up)
        .chain(parts)
        .collect::<Vec<_>>()
        .join("/")
}

fn rebase_map_source(root: &Path, base: &Path, source: &str) -> String {
    let candidate = Path::new(source);
    let resolved = if candidate.is_absolute() {
        candidate.canonicalize().ok()
    } else {
        base.join(candidate).canonicalize().ok()
    };
    let Some(absolute) = resolved else {
        return candidate
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
    };
    let common = root
        .components()
        .zip(absolute.components())
        .take_while(|(a, b)| a == b)
        .count();
    let up = root.components().count() - common;
    std::iter::repeat_n("..".to_string(), up)
        .chain(
            absolute
                .components()
                .skip(common)
                .map(|part| part.as_os_str().to_string_lossy().into_owned()),
        )
        .collect::<Vec<_>>()
        .join("/")
}

/// Minimal JSONC support: line and block comments, and trailing commas.
/// String contents are preserved verbatim.
fn strip_jsonc(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    let mut chars = source.chars().peekable();
    let mut in_string = false;
    let mut escaped = false;
    while let Some(c) = chars.next() {
        if in_string {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '"' => {
                in_string = true;
                out.push(c);
            }
            '/' if chars.peek() == Some(&'/') => {
                for c in chars.by_ref() {
                    if c == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut previous = '\0';
                for c in chars.by_ref() {
                    if previous == '*' && c == '/' {
                        break;
                    }
                    previous = c;
                }
            }
            '}' | ']' => {
                // The comma this closes is trailing; drop it, keep the layout.
                let trimmed = out.trim_end().len();
                if out[..trimmed].ends_with(',') {
                    out.remove(trimmed - 1);
                }
                out.push(c);
            }
            _ => out.push(c),
        }
    }
    out
}

#[cfg(all(test, celld_internal_tests))]
mod deploy_contract {
    include!(env!("CELLD_INTERNAL_DEPLOY_TESTS"));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fake engine CLI script: builds answer with `built_id`, inspects of
    /// that id answer with `built_content`, inspects of anything else (the
    /// content tag under contention) answer with `tagged_content`, and
    /// every tag fails the way the engine fails a lost tag race. Tests run
    /// it through `sh` rather than executing it: executing a file just
    /// written races any concurrent test's fork, which can still hold the
    /// write descriptor, and fails with ETXTBSY.
    fn fake_engine(
        dir: &Path,
        built_id: &str,
        built_content: &str,
        tagged_content: &str,
    ) -> PathBuf {
        let cli = dir.join("fake-engine");
        let script = r#"#!/bin/sh
if [ "$1" = "build" ]; then
  echo 'CELLD_TEST_BUILT_ID'
elif [ "$1" = "image" ]; then
  if [ "$5" = 'CELLD_TEST_BUILT_ID' ]; then
    echo 'CELLD_TEST_BUILT_CONTENT'
  else
    echo 'CELLD_TEST_TAGGED_CONTENT'
  fi
elif [ "$1" = "tag" ]; then
  echo "failed to create an image $3 after deleting the existing one: AlreadyExists" >&2
  exit 1
else
  echo "unexpected engine call: $*" >&2
  exit 99
fi
"#
        .replace("CELLD_TEST_BUILT_ID", built_id)
        .replace("CELLD_TEST_BUILT_CONTENT", built_content)
        .replace("CELLD_TEST_TAGGED_CONTENT", tagged_content);
        std::fs::write(&cli, script).unwrap();
        cli
    }

    fn dockerfile_project(dir: &Path) {
        std::fs::write(dir.join("Dockerfile"), "FROM scratch\n").unwrap();
    }

    fn bounds(project: &Path) -> ReadBounds {
        ReadBounds::new(project, &[]).unwrap()
    }

    fn sh(script: &Path) -> [&str; 2] {
        ["/bin/sh", script.to_str().unwrap()]
    }

    /// Link `link` to `target`: the suite already assumes a Unix host (the
    /// fake engine runs through `/bin/sh`).
    fn symlink_for_test(target: &Path, link: &Path) {
        std::os::unix::fs::symlink(target, link).unwrap();
    }

    #[test]
    fn dockerfile_outside_the_project_is_refused() {
        let project = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("Dockerfile"), "FROM scratch\n").unwrap();
        symlink_for_test(outside.path(), &project.path().join("sub"));
        let cli = fake_engine(
            project.path(),
            "fake-built-id",
            "[layer]{config}",
            "[layer]{config}",
        );
        let error = resolve_image_cli(
            &sh(&cli),
            &bounds(project.path()),
            "sub/Dockerfile",
            "config `containers[0].image`",
            None,
        )
        .expect_err("a Dockerfile outside the project must be refused, not built");
        let message = format!("{error:?}");
        assert!(
            message.contains("config `containers[0].image`")
                && message.contains("escapes the project"),
            "the refusal must name the config key: {message}"
        );
        // An absolute path is refused without looking at the filesystem,
        // whether or not it names a file: branching on existence would leak
        // host file existence one deploy error at a time.
        for absolute in [
            outside.path().join("Dockerfile"),
            outside.path().join("no-such-dockerfile"),
        ] {
            let error = resolve_image_cli(
                &sh(&cli),
                &bounds(project.path()),
                absolute.to_str().unwrap(),
                "config `containers[0].image`",
                None,
            )
            .expect_err("an absolute image value must be refused, not built");
            let message = format!("{error:?}");
            assert!(
                message.contains("config `containers[0].image`")
                    && message.contains("must be a project-relative path"),
                "the refusal must name the key without probing: {message}"
            );
        }
        // So is a `..` path that climbs out of the project.
        std::fs::write(project.path().join("Dockerfile"), "FROM scratch\n").unwrap();
        std::fs::create_dir(project.path().join("inner")).unwrap();
        let error = resolve_image_cli(
            &sh(&cli),
            &bounds(&project.path().join("inner")),
            "../Dockerfile",
            "config `containers[0].image`",
            None,
        )
        .expect_err("a `..` image value must be refused, not built");
        let message = format!("{error:?}");
        assert!(
            message.contains("must be a project-relative path"),
            "the refusal must say the value is neither: {message}"
        );
    }

    /// An operator's read root admits a Dockerfile outside the project by
    /// absolute or `..` path, and only there: a value outside every root is
    /// still refused without probing, and a missing file or a link inside one
    /// is refused rather than pulled as a reference, with the same message as
    /// a value outside every root.
    #[test]
    fn read_roots_admit_dockerfiles_outside_the_project() {
        let tree = tempfile::tempdir().unwrap();
        let tree_path = tree.path().canonicalize().unwrap();
        let (worktree, outside) = (tree_path.join("worktree"), tree_path.join("outside"));
        let project = worktree.join("dist");
        std::fs::create_dir_all(&project).unwrap();
        std::fs::create_dir_all(worktree.join("images")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(worktree.join("Dockerfile"), "FROM scratch\n").unwrap();
        std::fs::write(worktree.join("images/app.Dockerfile"), "FROM scratch\n").unwrap();
        std::fs::write(outside.join("Dockerfile"), "FROM scratch\n").unwrap();
        symlink_for_test(
            &outside.join("Dockerfile"),
            &worktree.join("linked.Dockerfile"),
        );
        let content = "[layer]{config}";
        let cli = fake_engine(&tree_path, "fake-built-id", content, content);
        let widened = ReadBounds::new(&project, std::slice::from_ref(&worktree)).unwrap();
        let resolve = |bounds: &ReadBounds, image: &str| {
            resolve_image_cli(
                &sh(&cli),
                bounds,
                image,
                "config `containers[0].image`",
                None,
            )
        };
        let absolute = worktree.join("images/app.Dockerfile");
        for image in ["../Dockerfile", absolute.to_str().unwrap()] {
            resolve(&widened, image).unwrap_or_else(|error| panic!("{image}: {error:#}"));
            let error = resolve(&bounds(&project), image).expect_err(image);
            assert!(
                format!("{error:#}").contains("must be a project-relative path"),
                "{image} without the read root: {error:#}"
            );
        }
        let outside_absolute = outside.join("Dockerfile");
        let missing = worktree.join("missing.Dockerfile");
        for (image, want) in [
            (
                "../../outside/Dockerfile",
                "must be a project-relative path",
            ),
            (
                outside_absolute.to_str().unwrap(),
                "must be a project-relative path",
            ),
            (missing.to_str().unwrap(), "must be a project-relative path"),
            ("../linked.Dockerfile", "must be a project-relative path"),
        ] {
            let error = resolve(&widened, image).expect_err(image);
            assert!(format!("{error:#}").contains(want), "{image}: {error:#}");
        }
    }

    /// `.` and `..` resolve by name; `..` stops at `/`; an absolute value
    /// replaces the base; repeated and trailing slashes add nothing.
    #[test]
    fn lexical_join_resolves_by_name_alone() {
        let base = Path::new("/w/dist");
        for (value, want) in [
            ("Dockerfile", "/w/dist/Dockerfile"),
            ("./a/./b", "/w/dist/a/b"),
            ("../Dockerfile", "/w/Dockerfile"),
            ("../../../../etc/x", "/etc/x"),
            ("/abs//x/", "/abs/x"),
            ("/abs/y/../x", "/abs/x"),
        ] {
            assert_eq!(lexical_join(base, value), Path::new(want), "{value}");
        }
    }

    /// A read root must name an existing directory; a link to one counts as
    /// the directory it resolves to.
    #[test]
    fn read_roots_must_be_directories() {
        let tree = tempfile::tempdir().unwrap();
        let (project, root) = (tree.path().join("project"), tree.path().join("root"));
        std::fs::create_dir_all(&project).unwrap();
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(tree.path().join("file"), "").unwrap();
        symlink_for_test(&root, &tree.path().join("linked"));
        for bad in [tree.path().join("file"), tree.path().join("missing")] {
            assert!(
                ReadBounds::new(&project, std::slice::from_ref(&bad)).is_err(),
                "{}",
                bad.display()
            );
        }
        let bounds = ReadBounds::new(&project, &[tree.path().join("linked")]).unwrap();
        assert!(bounds.contains(&root.canonicalize().unwrap().join("x")));
    }

    #[test]
    fn dockerfile_through_an_in_tree_link_still_builds() {
        let project = tempfile::tempdir().unwrap();
        std::fs::create_dir(project.path().join("real")).unwrap();
        std::fs::write(project.path().join("real/Dockerfile"), "FROM scratch\n").unwrap();
        symlink_for_test(Path::new("real"), &project.path().join("sub"));
        let content = "[layer]{config}";
        let cli = fake_engine(project.path(), "fake-built-id", content, content);
        let reference = resolve_image_cli(
            &sh(&cli),
            &bounds(project.path()),
            "sub/Dockerfile",
            "config `containers[0].image`",
            None,
        )
        .expect("a Dockerfile behind an in-tree link still builds");
        assert_eq!(
            reference,
            crate::container::image_reference(&format!("{:x}", Sha256::digest(content)))
        );
    }

    #[test]
    fn tag_race_against_identical_image_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        dockerfile_project(dir.path());
        let content = "[layer-a]{config-a}";
        let cli = fake_engine(dir.path(), "fake-built-id", content, content);
        let reference = resolve_image_cli(
            &sh(&cli),
            &bounds(dir.path()),
            "Dockerfile",
            "config `containers[0].image`",
            None,
        )
        .expect("an identical image under the tag is the lost race's winner");
        assert_eq!(
            reference,
            crate::container::image_reference(&format!("{:x}", Sha256::digest(content)))
        );
    }

    #[test]
    fn tag_race_against_different_image_fails() {
        let dir = tempfile::tempdir().unwrap();
        dockerfile_project(dir.path());
        let cli = fake_engine(
            dir.path(),
            "fake-built-id",
            "[layer-a]{config-a}",
            "[layer-b]{config-b}",
        );
        let error = resolve_image_cli(
            &sh(&cli),
            &bounds(dir.path()),
            "Dockerfile",
            "config `containers[0].image`",
            None,
        )
        .expect_err("a different image under the tag must still fail");
        let message = format!("{error:?}");
        assert!(
            message.contains("AlreadyExists"),
            "the conflict must stay visible: {message}"
        );
    }

    #[test]
    fn deployment_versions_have_the_minted_shape() {
        assert!(crate::protocol::valid_deployment_version(
            "0123456789abcdef"
        ));
        for version in [
            "",
            "abc",
            "current.json",
            "0123456789abcde",
            "0123456789abcdef0",
            "0123456789ABCDEF",
            "zzzzzzzzzzzzzzzz",
            "0123456789abcde/",
        ] {
            assert!(
                !crate::protocol::valid_deployment_version(version),
                "{version:?} is not a minted version"
            );
        }
    }

    #[test]
    fn discard_scope_refuses_keys_outside_the_version_directory() {
        let reference = VersionRef {
            script: "app".to_string(),
            version: "0123456789abcdef".to_string(),
        };
        let prefix = "deploy/app/0123456789abcdef";
        // The version directory itself passes through.
        let inside = [
            format!("{prefix}/manifest.json"),
            format!("{prefix}/index.js"),
        ];
        assert_eq!(
            discard_deletion_scope(prefix, &inside, &reference).unwrap(),
            inside
        );
        // Everything an S3 byte-prefix listing can also return is
        // refused: a sibling version, the script's named pointer, the
        // fleet pointer, and an object at the prefix itself.
        for key in [
            "deploy/app/0123456789abcdef0/manifest.json".to_string(),
            "deploy/app/0123456789abcde/manifest.json".to_string(),
            "deploy/app/current.json".to_string(),
            "deploy/current.json".to_string(),
            prefix.to_string(),
        ] {
            let keys = [inside[0].clone(), key.clone()];
            let error = discard_deletion_scope(prefix, &keys, &reference)
                .expect_err(&format!("{key:?} is outside the version directory"));
            assert!(
                format!("{error:#}").contains("outside the version directory"),
                "unexpected error: {error:#}"
            );
        }
    }
}

#[cfg(test)]
mod sourcemap_tests {
    use super::*;

    /// The marker statement's line (1-based) in the bundle fixture's entry.
    const MARKER_LINE: u32 = 4;

    /// A source outside the root is a `../` path from the root and one that
    /// does not resolve is its file name: the map never carries the host's
    /// absolute layout.
    #[test]
    fn sources_outside_the_root_name_no_host_path() {
        let tree = tempfile::tempdir().unwrap();
        let base = tree.path().canonicalize().unwrap();
        let root = base.join("project");
        let outdir = base.join("out");
        for dir in [root.join("src"), base.join("shared"), outdir.clone()] {
            std::fs::create_dir_all(dir).unwrap();
        }
        std::fs::write(root.join("src/entry.js"), "").unwrap();
        std::fs::write(base.join("shared/util.js"), "").unwrap();
        assert_eq!(
            rebase_map_source(&root, &outdir, "../project/src/entry.js"),
            "src/entry.js"
        );
        assert_eq!(
            rebase_map_source(&root, &outdir, "../shared/util.js"),
            "../shared/util.js"
        );
        let absolute = base.join("shared/util.js");
        assert_eq!(
            rebase_map_source(&root, &outdir, absolute.to_str().unwrap()),
            "../shared/util.js"
        );
        assert_eq!(
            rebase_map_source(&root, &outdir, "../../gone/secret/missing.js"),
            "missing.js"
        );
    }

    fn test_options(config: &Path, sourcemap: Option<PathBuf>) -> Options {
        Options {
            config: Some(config.to_path_buf()),
            bucket: None,
            endpoint: None,
            region: None,
            dry_run: true,
            json: false,
            vars: VarOverrides::None,
            local_images: false,
            sourcemap,
            no_activate: false,
            read_roots: Vec::new(),
        }
    }

    /// Whether the esbuild binary `run_esbuild` would use answers. Tests that
    /// bundle return early without it, so a machine without esbuild still runs
    /// the suite; the Docker test stage installs esbuild, so CI runs them.
    /// Whether this kernel confines esbuild's reads (Landlock).
    fn esbuild_confined() -> bool {
        confine::ReadConfinement::beneath(&[Path::new("/")], Path::new("/bin/sh"))
            .is_ok_and(|confinement| confinement.confines())
    }

    fn esbuild_available() -> bool {
        let binary = std::env::var("CELLD_ESBUILD").unwrap_or_else(|_| "esbuild".to_string());
        Command::new(binary)
            .arg("--version")
            .output()
            .is_ok_and(|output| output.status.success())
    }

    fn write_bundle_fixture(root: &Path) {
        std::fs::write(
            root.join("wrangler.json"),
            r#"{"name": "maptest", "main": "entry.js"}"#,
        )
        .unwrap();
        std::fs::write(
            root.join("entry.js"),
            "import { answer } from \"./dep.js\";\n\
             export default {\n\
             \x20 async fetch() {\n\
             \x20\x20\x20\x20return Response.json({ answer: answer() });\n\
             \x20 },\n\
             };\n",
        )
        .unwrap();
        std::fs::write(
            root.join("dep.js"),
            "export function answer() {\n  return 42;\n}\n",
        )
        .unwrap();
    }

    /// One decoded mapping with an original position.
    struct DecodedSegment {
        source: String,
        /// 0-based, as the v3 format stores it.
        src_line: u32,
    }

    /// A minimal base64-VLQ decoder over a v3 `mappings` string: enough to
    /// check which original line a generated position maps to, without a new
    /// dependency.
    fn decode_mappings(map: &Value) -> Vec<DecodedSegment> {
        let sources: Vec<String> = map
            .get("sources")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|source| source.as_str().map(str::to_string))
            .collect();
        let mappings = map.get("mappings").and_then(Value::as_str).unwrap_or("");
        let mut out = Vec::new();
        let mut src_index = 0i64;
        let mut src_line = 0i64;
        for line in mappings.split(';') {
            for segment in line.split(',') {
                if segment.is_empty() {
                    continue;
                }
                let fields = decode_segment(segment);
                if fields.len() >= 4 {
                    src_index += fields[1];
                    src_line += fields[2];
                    if let Some(source) = sources.get(src_index as usize) {
                        out.push(DecodedSegment {
                            source: source.clone(),
                            src_line: src_line as u32,
                        });
                    }
                }
            }
        }
        out
    }

    fn decode_segment(segment: &str) -> Vec<i64> {
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut values = Vec::new();
        let mut value = 0i64;
        let mut shift = 0u32;
        for byte in segment.bytes() {
            let digit = ALPHABET
                .iter()
                .position(|candidate| *candidate == byte)
                .expect("mappings are base64") as i64;
            let continues = digit & 32 != 0;
            value |= (digit & 31) << shift;
            shift += 5;
            if !continues {
                let decoded = value >> 1;
                values.push(if value & 1 != 0 { -decoded } else { decoded });
                value = 0;
                shift = 0;
            }
        }
        values
    }

    #[test]
    fn sourcemap_flag_parses() {
        let options =
            options_from_arguments(["--sourcemap".to_string(), "out/worker.js.map".to_string()])
                .unwrap()
                .unwrap();
        assert_eq!(options.sourcemap, Some(PathBuf::from("out/worker.js.map")));
        let options = options_from_arguments(Vec::<String>::new())
            .unwrap()
            .unwrap();
        assert_eq!(options.sourcemap, None);
        assert!(options_from_arguments(["--sourcemap".to_string()]).is_err());
    }

    #[test]
    fn strip_sourcemap_comment_only_strips_the_trailing_line() {
        let plain = b"(() => {\n  console.log(1);\n})();\n".to_vec();
        let linked = [
            plain.clone(),
            b"//# sourceMappingURL=index.js.map\n".to_vec(),
        ]
        .concat();
        assert_eq!(strip_sourcemap_comment(linked), plain);
        assert_eq!(strip_sourcemap_comment(plain.clone()), plain);
        // A comment-looking line that is not trailing stays: stripping it
        // would corrupt a bundle that merely mentions the marker.
        let embedded = b"//# sourceMappingURL=index.js.map\nconsole.log(1);\n".to_vec();
        assert_eq!(strip_sourcemap_comment(embedded.clone()), embedded);
    }

    #[test]
    fn sourcemap_flag_leaves_deployed_bytes_identical() {
        if !esbuild_available() {
            eprintln!("skipping: esbuild not on PATH");
            return;
        }
        let root = tempfile::tempdir().unwrap();
        write_bundle_fixture(root.path());
        let config = root.path().join("wrangler.json");
        let plain = build(&test_options(&config, None)).unwrap();
        let mapped = build(&test_options(
            &config,
            Some(root.path().join("worker.js.map")),
        ))
        .unwrap();
        assert_eq!(plain.modules, mapped.modules);
        assert_eq!(plain.version, mapped.version);
        let bundle = &plain.modules[0].1;
        assert!(
            !bundle
                .windows(b"sourceMappingURL".len())
                .any(|window| window == b"sourceMappingURL"),
            "deployed bytes must not reference the map"
        );
        assert!(mapped.sourcemap.is_some());
    }

    #[test]
    fn sourcemap_points_at_the_marker_line() {
        if !esbuild_available() {
            eprintln!("skipping: esbuild not on PATH");
            return;
        }
        let root = tempfile::tempdir().unwrap();
        write_bundle_fixture(root.path());
        let map_path = root.path().join("worker.js.map");
        let built = build(&test_options(
            &root.path().join("wrangler.json"),
            Some(map_path.clone()),
        ))
        .unwrap();
        let map = built.sourcemap.as_ref().expect("esbuild run emits a map");
        write_sourcemap(
            &test_options(&root.path().join("wrangler.json"), Some(map_path.clone())),
            &built,
        )
        .unwrap();
        let stored = std::fs::read(&map_path).unwrap();
        assert_eq!(stored, *map);
        let parsed: Value = serde_json::from_slice(&stored).unwrap();
        assert_eq!(parsed.get("version").and_then(Value::as_u64), Some(3));
        // Every source resolves to a project file: the map must not name the
        // deleted esbuild tempdir.
        let sources = parsed
            .get("sources")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        assert!(!sources.is_empty());
        for source in &sources {
            let name = source.as_str().unwrap();
            assert!(
                root.path().join(name).is_file(),
                "map source {name:?} resolves under the project root"
            );
        }
        // The marker statement maps back to its own line in the entry.
        let segments = decode_mappings(&parsed);
        assert!(
            segments
                .iter()
                .any(|segment| segment.source == "entry.js" && segment.src_line == MARKER_LINE - 1),
            "a mapping names entry.js line {MARKER_LINE}"
        );
        // And the map is not one line repeated: the dependency maps too.
        assert!(
            segments.iter().any(|segment| segment.source == "dep.js"),
            "a mapping names dep.js"
        );
    }

    #[test]
    fn sourcemap_no_bundle_project_warns_and_writes_nothing() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("wrangler.json"),
            r#"{"name": "maptest", "main": "entry.js", "no_bundle": true}"#,
        )
        .unwrap();
        std::fs::write(root.path().join("entry.js"), "export default {};\n").unwrap();
        let map_path = root.path().join("worker.js.map");
        let options = test_options(&root.path().join("wrangler.json"), Some(map_path.clone()));
        let built = build(&options).unwrap();
        assert_eq!(built.modules[0].1, b"export default {};\n".to_vec());
        assert_eq!(built.sourcemap, None);
        write_sourcemap(&options, &built).unwrap();
        assert!(!map_path.exists());
    }

    /// A `no_bundle` entry deploys as written, so its sibling `<entry>.map`
    /// — the caller's toolchain owns both — is collected and rebased like
    /// the bundled map: relative sources resolve against the entry's own
    /// directory.
    #[test]
    fn sourcemap_no_bundle_sibling_map_is_written_rebased() {
        let tree = tempfile::tempdir().unwrap();
        let base = tree.path().canonicalize().unwrap();
        let root = base.join("project");
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(base.join("shared")).unwrap();
        std::fs::write(
            root.join("wrangler.json"),
            r#"{"name": "maptest", "main": "entry.js", "no_bundle": true}"#,
        )
        .unwrap();
        std::fs::write(root.join("entry.js"), "export default {};\n").unwrap();
        std::fs::write(root.join("src/app.ts"), "export {};\n").unwrap();
        std::fs::write(base.join("shared/util.ts"), "export {};\n").unwrap();
        std::fs::write(
            root.join("entry.js.map"),
            r#"{"version":3,"sources":["src/app.ts","../shared/util.ts","missing/gone.ts"],"mappings":"AAAA"}"#,
        )
        .unwrap();
        let map_path = root.join("worker.js.map");
        let options = test_options(&root.join("wrangler.json"), Some(map_path.clone()));
        let built = build(&options).unwrap();
        // The deployed module is the entry as written.
        assert_eq!(built.modules[0].1, b"export default {};\n".to_vec());
        let map = built
            .sourcemap
            .as_ref()
            .expect("a no_bundle sibling map is collected");
        write_sourcemap(&options, &built).unwrap();
        let stored = std::fs::read(&map_path).unwrap();
        assert_eq!(stored, *map);
        let parsed: Value = serde_json::from_slice(&stored).unwrap();
        assert_eq!(parsed.get("version").and_then(Value::as_u64), Some(3));
        assert_eq!(parsed.get("mappings").and_then(Value::as_str), Some("AAAA"));
        let sources = parsed
            .get("sources")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let names: Vec<&str> = sources.iter().filter_map(Value::as_str).collect();
        assert_eq!(
            names,
            ["src/app.ts", "../shared/util.ts", "missing/gone.ts"]
        );
    }

    /// Collecting the sibling map changes nothing about the deployment: the
    /// flag only adds the local file.
    #[test]
    fn sourcemap_no_bundle_leaves_deployed_bytes_identical() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("wrangler.json"),
            r#"{"name": "maptest", "main": "entry.js", "no_bundle": true}"#,
        )
        .unwrap();
        let entry = b"export default {};\n//# sourceMappingURL=entry.js.map\n".to_vec();
        std::fs::write(root.path().join("entry.js"), &entry).unwrap();
        std::fs::write(
            root.path().join("entry.js.map"),
            r#"{"version":3,"sources":["entry.ts"],"mappings":"AAAA"}"#,
        )
        .unwrap();
        let config = root.path().join("wrangler.json");
        let plain = build(&test_options(&config, None)).unwrap();
        let mapped = build(&test_options(
            &config,
            Some(root.path().join("worker.js.map")),
        ))
        .unwrap();
        assert_eq!(plain.modules, mapped.modules);
        assert_eq!(plain.version, mapped.version);
        // The caller's bytes deploy untouched, trailing comment included:
        // celld never rewrites a pre-bundled entry.
        assert_eq!(plain.modules[0].1, entry);
        assert!(mapped.sourcemap.is_some());
    }

    /// A `no_bundle` project whose entry sits at `main`, built with
    /// `--sourcemap`: the deploy must succeed and deploy the entry as
    /// written, whatever the sibling map is. Returns the collected map.
    fn build_no_bundle_with_map(root: &Path, main: &str) -> Option<Vec<u8>> {
        std::fs::write(
            root.join("wrangler.json"),
            format!(r#"{{"name": "maptest", "main": "{main}", "no_bundle": true}}"#),
        )
        .unwrap();
        let options = test_options(&root.join("wrangler.json"), Some(root.join("out.js.map")));
        let built = build(&options).expect("a sibling map never fails the deploy");
        assert_eq!(built.modules[0].1, b"export default {};\n".to_vec());
        built.sourcemap
    }

    /// The sibling map comes from a tree the deploy's caller controls, so
    /// nothing about it may fail the deploy or reach outside the tree: a
    /// symlink (even to a valid map), a FIFO (which must not block the
    /// open), an oversize file, invalid JSON, and JSON that is not a v3
    /// map all warn and collect nothing.
    #[test]
    fn sourcemap_no_bundle_hostile_sibling_is_skipped() {
        const VALID: &str = r#"{"version":3,"sources":["a.ts"],"mappings":"AAAA"}"#;
        /// Plants the sibling at the map path; the second path is a
        /// directory outside the project tree.
        type Plant = fn(&Path, &Path);
        let cases: [(&str, Plant); 7] = [
            ("symlink to a valid map outside the tree", |map, outside| {
                std::fs::write(outside.join("victim.map"), VALID).unwrap();
                std::os::unix::fs::symlink(outside.join("victim.map"), map).unwrap();
            }),
            ("FIFO", |map, _| {
                nix::unistd::mkfifo(map, nix::sys::stat::Mode::S_IRWXU).unwrap();
            }),
            ("directory", |map, _| std::fs::create_dir(map).unwrap()),
            ("oversize", |map, _| {
                let file = std::fs::File::create(map).unwrap();
                file.set_len(SIBLING_MAP_MAX_BYTES + 1).unwrap();
            }),
            ("invalid JSON", |map, _| {
                std::fs::write(map, b"\x00garbage").unwrap()
            }),
            ("JSON that is not a map", |map, _| {
                std::fs::write(map, r#"{"secret":"x","sources":["a.ts"]}"#).unwrap();
            }),
            ("non-string sources", |map, _| {
                std::fs::write(map, r#"{"version":3,"sources":[7],"mappings":""}"#).unwrap();
            }),
        ];
        for (label, plant) in cases {
            let tree = tempfile::tempdir().unwrap();
            let (root, outside) = (tree.path().join("project"), tree.path().join("outside"));
            std::fs::create_dir_all(&root).unwrap();
            std::fs::create_dir_all(&outside).unwrap();
            std::fs::write(root.join("entry.js"), "export default {};\n").unwrap();
            plant(&root.join("entry.js.map"), &outside);
            assert_eq!(
                build_no_bundle_with_map(&root, "entry.js"),
                None,
                "{label}: the sibling must be skipped"
            );
        }
    }

    /// A directory symlink on the entry's path leads outside the project.
    /// The entry itself is refused before the sibling is ever considered;
    /// the sibling reader's own containment is pinned directly below it,
    /// as defense in depth.
    #[test]
    fn sourcemap_no_bundle_sibling_outside_the_root_is_skipped() {
        let tree = tempfile::tempdir().unwrap();
        let (root, outside) = (tree.path().join("project"), tree.path().join("outside"));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("entry.js"), "export default {};\n").unwrap();
        std::fs::write(
            outside.join("entry.js.map"),
            r#"{"version":3,"sources":["a.ts"],"mappings":"AAAA"}"#,
        )
        .unwrap();
        std::os::unix::fs::symlink(&outside, root.join("sub")).unwrap();
        std::fs::write(
            root.join("wrangler.json"),
            r#"{"name": "maptest", "main": "sub/entry.js", "no_bundle": true}"#,
        )
        .unwrap();
        let error = refused(
            build(&test_options(
                &root.join("wrangler.json"),
                Some(root.join("out.js.map")),
            )),
            "an entry outside the project must be refused with its sibling",
        );
        let message = format!("{error:?}");
        assert!(
            message.contains("config `main`") && message.contains("escapes the project"),
            "the refusal must name the config key: {message}"
        );
        assert_eq!(
            sibling_sourcemap(&outside.join("entry.js"), &root),
            None,
            "the sibling reader must skip an entry outside the root on its own"
        );
    }

    /// Sibling sources resolve lexically against the entry's directory,
    /// with `sourceRoot` folded in and dropped; absolute paths and URLs
    /// keep only their file name, whether or not they exist on the host,
    /// so the stored map answers nothing about the host's files.
    #[test]
    fn sibling_map_sources_resolve_without_touching_the_host() {
        let existing = std::env::current_exe().unwrap();
        let map = serde_json::to_vec(&json!({
            "version": 3,
            "sourceRoot": "../src/",
            "sources": [
                "app.ts",
                "./lib/../util.ts",
                "../../../outside.ts",
                existing.to_str().unwrap(),
                "/definitely/missing/gone.ts",
                "webpack://pkg/./mod.ts",
            ],
            "mappings": "AAAA",
        }))
        .unwrap();
        let rebased = rebase_sibling_map(&map, Path::new("dist/worker")).unwrap();
        let parsed: Value = serde_json::from_slice(&rebased).unwrap();
        assert!(
            parsed.get("sourceRoot").is_none(),
            "sourceRoot is folded in"
        );
        let sources: Vec<&str> = parsed["sources"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Value::as_str)
            .collect();
        let exe_name = existing.file_name().unwrap().to_str().unwrap();
        assert_eq!(
            sources,
            [
                "dist/src/app.ts",
                "dist/src/util.ts",
                "../outside.ts",
                exe_name,
                "gone.ts",
                "mod.ts",
            ]
        );
    }

    #[test]
    fn service_binding_with_invalid_target_name_fails_at_deploy() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("wrangler.json"),
            r#"{"name": "svctest", "main": "entry.js", "no_bundle": true, "services": [{"binding": "SB", "service": "__celld-runtime"}]}"#,
        )
        .unwrap();
        std::fs::write(root.path().join("entry.js"), "export default {};\n").unwrap();
        let error = match build(&test_options(&root.path().join("wrangler.json"), None)) {
            Ok(_) => panic!("an undeployable service target must fail the deploy, not the boot"),
            Err(error) => error,
        };
        let message = format!("{error:?}");
        assert!(
            message.contains("__celld-runtime"),
            "the refusal must name the bad target: {message}"
        );
    }

    /// Link `link` to `target`: the suite already assumes a Unix host (the
    /// fake engine runs through `/bin/sh`).
    fn symlink_for_test(target: &Path, link: &Path) {
        std::os::unix::fs::symlink(target, link).unwrap();
    }

    /// The error of a result that must be one, for success types without
    /// `Debug` (`expect_err` needs it).
    fn refused<T>(result: anyhow::Result<T>, message: &str) -> anyhow::Error {
        match result {
            Ok(_) => panic!("{message}"),
            Err(error) => error,
        }
    }

    /// The metafile check: every file input must resolve inside the root.
    /// Namespaced inputs read no file and pass; a `..` path, an absolute
    /// path, and a link out of the tree are refused; an in-tree link passes.
    #[test]
    fn bundle_inputs_outside_the_project_are_refused() {
        let tree = tempfile::tempdir().unwrap();
        let (root, outside) = (tree.path().join("project"), tree.path().join("outside"));
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(root.join("src/app.js"), "").unwrap();
        std::fs::write(outside.join("secret.js"), "").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("out-link")).unwrap();
        std::os::unix::fs::symlink(root.join("src"), root.join("in-link")).unwrap();
        let root = canonical_project_root(&root).unwrap();
        let meta = |inputs: &[&str]| {
            let inputs: serde_json::Map<String, Value> = inputs
                .iter()
                .map(|name| (name.to_string(), json!({})))
                .collect();
            serde_json::to_vec(&json!({ "inputs": inputs })).unwrap()
        };
        let inside = meta(&[
            "src/app.js",
            "in-link/app.js",
            "(disabled):fs",
            "dataurl:data:text/plain,x",
        ]);
        contain_bundle_inputs(&ReadBounds::new(&root, &[]).unwrap(), &inside)
            .expect("in-tree inputs pass");
        let absolute = outside.join("secret.js");
        for escaping in [
            "../outside/secret.js",
            absolute.to_str().unwrap(),
            "out-link/secret.js",
        ] {
            let error = contain_bundle_inputs(
                &ReadBounds::new(&root, &[]).unwrap(),
                &meta(&["src/app.js", escaping]),
            )
            .expect_err(escaping);
            assert!(
                format!("{error:#}").contains("escapes the project directory"),
                "{escaping}: {error:#}"
            );
        }
        assert!(
            contain_bundle_inputs(
                &ReadBounds::new(&root, &[]).unwrap(),
                &meta(&["src/gone.js"])
            )
            .is_err(),
            "an unresolvable file input fails closed"
        );
        // A name spelled like a namespace is skipped only when it names
        // nothing: a real path is checked like any other.
        std::os::unix::fs::symlink(outside.join("secret.js"), root.join("ns:x")).unwrap();
        std::os::unix::fs::symlink(outside.join("secret.js"), root.join("(x).js")).unwrap();
        for disguised in ["ns:x", "(x).js"] {
            let error =
                contain_bundle_inputs(&ReadBounds::new(&root, &[]).unwrap(), &meta(&[disguised]))
                    .expect_err(disguised);
            assert!(
                format!("{error:#}").contains("escapes the project directory"),
                "{disguised}: {error:#}"
            );
        }
    }

    /// End to end through esbuild: an import that climbs out of the
    /// project, one through a directory link out of it, and a JSON import
    /// from outside all fail the build, while in-tree imports (through an
    /// in-tree link too) still bundle.
    #[test]
    fn bundled_imports_outside_the_project_are_refused() {
        if !esbuild_available() {
            eprintln!("skipping: esbuild not on PATH");
            return;
        }
        let tree = tempfile::tempdir().unwrap();
        let (root, outside) = (tree.path().join("project"), tree.path().join("outside"));
        std::fs::create_dir_all(root.join("lib")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(outside.join("secret.js"), "export default 'OUTSIDE';\n").unwrap();
        std::fs::write(outside.join("data.json"), "{\"secret\": \"OUTSIDE\"}\n").unwrap();
        std::fs::write(root.join("lib/ok.js"), "export const ok = 1;\n").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("ext")).unwrap();
        std::os::unix::fs::symlink(root.join("lib"), root.join("alias")).unwrap();
        std::fs::write(
            root.join("wrangler.json"),
            r#"{"name": "contain", "main": "entry.js"}"#,
        )
        .unwrap();
        let config = root.join("wrangler.json");
        let entry = |import: &str| {
            std::fs::write(
                root.join("entry.js"),
                format!(
                    "import x from {import:?};\n\
                     export default {{ fetch() {{ return new Response(JSON.stringify(x)); }} }};\n"
                ),
            )
            .unwrap();
        };
        // Confined, esbuild cannot read the file at all; unconfined, the
        // metafile check refuses what it read.
        let expected = if esbuild_confined() {
            "permission denied"
        } else {
            "escapes the project directory"
        };
        for import in [
            "../outside/secret.js",
            "./ext/secret.js",
            "../outside/data.json",
        ] {
            entry(import);
            let error = format!("{:#}", refused(build(&test_options(&config, None)), import));
            assert!(error.contains(expected), "{import}: {error}");
            assert!(!error.contains("OUTSIDE"), "{import}: {error}");
        }
        std::fs::write(
            root.join("entry.js"),
            "import { ok } from \"./alias/ok.js\";\n\
             export default { fetch() { return new Response(String(ok)); } };\n",
        )
        .unwrap();
        build(&test_options(&config, None)).expect("an in-tree link still bundles");
    }

    /// A monorepo app whose packages are hoisted above its config bundles
    /// once the operator names the repository as a read root, and only then.
    #[test]
    fn read_root_admits_hoisted_node_modules() {
        if !esbuild_available() {
            eprintln!("skipping: esbuild not on PATH");
            return;
        }
        let tree = tempfile::tempdir().unwrap();
        let (repo, outside) = (tree.path().join("repo"), tree.path().join("outside"));
        let app = repo.join("apps/web");
        std::fs::create_dir_all(&app).unwrap();
        std::fs::create_dir_all(repo.join("node_modules/hoisted")).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(
            repo.join("node_modules/hoisted/index.js"),
            "export const greeting = 'HOISTED';\n",
        )
        .unwrap();
        std::fs::write(outside.join("secret.js"), "export default 'OUTSIDE';\n").unwrap();
        std::fs::write(
            app.join("wrangler.json"),
            r#"{"name": "mono", "main": "entry.js"}"#,
        )
        .unwrap();
        let config = app.join("wrangler.json");
        std::fs::write(
            app.join("entry.js"),
            "import { greeting } from \"hoisted\";\n\
             export default { fetch() { return new Response(greeting); } };\n",
        )
        .unwrap();
        let widened = |config: &Path| Options {
            read_roots: vec![repo.clone()],
            ..test_options(config, None)
        };
        refused(
            build(&test_options(&config, None)),
            "hoisted without a read root",
        );
        let built = build(&widened(&config)).expect("the read root admits the hoisted package");
        assert!(built
            .modules
            .iter()
            .any(|(_, bytes)| String::from_utf8_lossy(bytes).contains("HOISTED")));
        // The read root widens nothing beyond itself.
        std::fs::write(
            app.join("entry.js"),
            "import x from \"../../../outside/secret.js\";\n\
             export default { fetch() { return new Response(x); } };\n",
        )
        .unwrap();
        let error = format!(
            "{:#}",
            refused(build(&widened(&config)), "outside the read root")
        );
        assert!(!error.contains("OUTSIDE"), "{error}");
    }

    /// The reads the metafile never lists stay inside too: an input's
    /// `sourceMappingURL`, a `tsconfig.json` `extends` chain, and a linked
    /// file that fails to parse, whose line esbuild's error would quote.
    /// None of the outside markers may reach the bundle, the map, or the
    /// error.
    #[test]
    fn confined_esbuild_reads_nothing_outside_the_project() {
        if !esbuild_available() {
            eprintln!("skipping: esbuild not on PATH");
            return;
        }
        if !esbuild_confined() {
            eprintln!("skipping: this kernel offers no Landlock");
            return;
        }
        let tree = tempfile::tempdir().unwrap();
        let (root, outside) = (tree.path().join("project"), tree.path().join("outside"));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        std::fs::write(
            outside.join("evil.map"),
            r#"{"version":3,"sources":["x.js"],"sourcesContent":["OUTSIDE-MAP"],"mappings":"AAAA"}"#,
        )
        .unwrap();
        std::fs::write(
            outside.join("base.json"),
            r#"{"compilerOptions": {"jsxFactory": "OUTSIDE_FACTORY"}}"#,
        )
        .unwrap();
        std::fs::write(
            outside.join("broken.js"),
            "let x = 1;\nmore OUTSIDE-LINE here\n",
        )
        .unwrap();
        let config = root.join("wrangler.json");
        let leaked = |main: &str, marker: &str| {
            std::fs::write(
                &config,
                format!(r#"{{"name": "confine", "main": {main:?}}}"#),
            )
            .unwrap();
            match build(&test_options(&config, Some(tree.path().join("out.map")))) {
                Ok(built) => {
                    built
                        .modules
                        .iter()
                        .any(|(_, bytes)| String::from_utf8_lossy(bytes).contains(marker))
                        || built
                            .sourcemap
                            .as_ref()
                            .is_some_and(|map| String::from_utf8_lossy(map).contains(marker))
                }
                Err(error) => format!("{error:#}").contains(marker),
            }
        };
        std::fs::write(
            root.join("map.js"),
            "export default { fetch() { return new Response('ok'); } };\n\
             //# sourceMappingURL=../outside/evil.map\n",
        )
        .unwrap();
        assert!(!leaked("map.js", "OUTSIDE-MAP"), "an outside input map");
        std::fs::write(
            root.join("tsconfig.json"),
            r#"{"extends": "../outside/base.json"}"#,
        )
        .unwrap();
        std::fs::write(
            root.join("view.jsx"),
            "export default { fetch() { return new Response(String(<b />)); } };\n",
        )
        .unwrap();
        assert!(
            !leaked("view.jsx", "OUTSIDE_FACTORY"),
            "an outside tsconfig"
        );
        std::fs::remove_file(root.join("tsconfig.json")).unwrap();
        std::os::unix::fs::symlink(outside.join("broken.js"), root.join("broken.js")).unwrap();
        std::fs::write(
            root.join("parse.js"),
            "import \"./broken.js\";\nexport default { fetch() { return new Response('ok'); } };\n",
        )
        .unwrap();
        assert!(
            !leaked("parse.js", "OUTSIDE-LINE"),
            "an outside parse error"
        );
    }

    #[test]
    fn entry_through_a_link_outside_the_project_is_refused() {
        let project = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("entry.js"), "OUTSIDE\n").unwrap();
        symlink_for_test(outside.path(), &project.path().join("sub"));
        std::fs::write(
            project.path().join("wrangler.json"),
            r#"{"name": "linktest", "main": "sub/entry.js", "no_bundle": true}"#,
        )
        .unwrap();
        let error = refused(
            build(&test_options(&project.path().join("wrangler.json"), None)),
            "an entry outside the project must be refused, not deployed",
        );
        let message = format!("{error:?}");
        assert!(
            message.contains("config `main`") && message.contains("escapes the project"),
            "the refusal must name the config key: {message}"
        );
        // The bundled path is refused the same way, before esbuild ever runs.
        std::fs::write(
            project.path().join("bundled.json"),
            r#"{"name": "linktest", "main": "sub/entry.js"}"#,
        )
        .unwrap();
        let error = refused(
            build(&test_options(&project.path().join("bundled.json"), None)),
            "a bundled entry outside the project must be refused without esbuild",
        );
        let message = format!("{error:?}");
        assert!(
            message.contains("config `main`") && message.contains("escapes the project"),
            "the refusal must name the config key: {message}"
        );
    }

    #[test]
    fn entry_through_an_in_tree_link_still_deploys() {
        let project = tempfile::tempdir().unwrap();
        std::fs::create_dir(project.path().join("real")).unwrap();
        std::fs::write(project.path().join("real/entry.js"), "INSIDE\n").unwrap();
        symlink_for_test(Path::new("real"), &project.path().join("sub"));
        std::fs::write(
            project.path().join("wrangler.json"),
            r#"{"name": "linktest", "main": "sub/entry.js", "no_bundle": true}"#,
        )
        .unwrap();
        let built = build(&test_options(&project.path().join("wrangler.json"), None)).unwrap();
        assert_eq!(
            built.modules,
            vec![("index.js".to_string(), b"INSIDE\n".to_vec())]
        );
    }

    #[test]
    fn unbundled_wasm_outside_the_project_is_not_collected() {
        let project = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("entry.js"), "OUTSIDE\n").unwrap();
        std::fs::write(outside.path().join("extra.wasm"), "OUTSIDE-WASM").unwrap();
        symlink_for_test(outside.path(), &project.path().join("sub"));
        std::fs::write(
            project.path().join("wrangler.json"),
            r#"{"name": "linktest", "main": "sub/entry.js", "no_bundle": true}"#,
        )
        .unwrap();
        let error = refused(
            build(&test_options(&project.path().join("wrangler.json"), None)),
            "wasm beside an outside entry must be refused with it",
        );
        let message = format!("{error:?}");
        assert!(
            message.contains("config `main`") && message.contains("escapes the project"),
            "the refusal must name the config key: {message}"
        );
    }

    #[test]
    fn unbundled_wasm_through_an_in_tree_link_is_collected() {
        let project = tempfile::tempdir().unwrap();
        std::fs::create_dir(project.path().join("real")).unwrap();
        std::fs::write(project.path().join("real/entry.js"), "INSIDE\n").unwrap();
        std::fs::write(project.path().join("real/extra.wasm"), "INSIDE-WASM").unwrap();
        symlink_for_test(Path::new("real"), &project.path().join("sub"));
        std::fs::write(
            project.path().join("wrangler.json"),
            r#"{"name": "linktest", "main": "sub/entry.js", "no_bundle": true}"#,
        )
        .unwrap();
        let built = build(&test_options(&project.path().join("wrangler.json"), None)).unwrap();
        assert_eq!(
            built.modules,
            vec![
                ("index.js".to_string(), b"INSIDE\n".to_vec()),
                ("extra.wasm".to_string(), b"INSIDE-WASM".to_vec()),
            ]
        );
    }

    #[test]
    fn asset_directory_through_a_link_outside_the_project_is_refused() {
        let project = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(outside.path().join("assets")).unwrap();
        std::fs::write(outside.path().join("assets/index.html"), "OUTSIDE\n").unwrap();
        symlink_for_test(outside.path(), &project.path().join("sub"));
        std::fs::write(
            project.path().join("wrangler.json"),
            r#"{"name": "linktest", "assets": {"directory": "sub/assets"}}"#,
        )
        .unwrap();
        let error = refused(
            build(&test_options(&project.path().join("wrangler.json"), None)),
            "assets outside the project must be refused, not published",
        );
        let message = format!("{error:?}");
        assert!(
            message.contains("config `assets.directory`")
                && message.contains("escapes the project"),
            "the refusal must name the config key: {message}"
        );
    }

    #[test]
    fn asset_directory_through_an_in_tree_link_still_deploys() {
        let project = tempfile::tempdir().unwrap();
        std::fs::create_dir(project.path().join("real")).unwrap();
        std::fs::create_dir(project.path().join("real/assets")).unwrap();
        std::fs::write(project.path().join("real/assets/index.html"), "INSIDE\n").unwrap();
        symlink_for_test(Path::new("real"), &project.path().join("sub"));
        std::fs::write(
            project.path().join("wrangler.json"),
            r#"{"name": "linktest", "assets": {"directory": "sub/assets"}}"#,
        )
        .unwrap();
        let built = build(&test_options(&project.path().join("wrangler.json"), None)).unwrap();
        let assets = built.assets.expect("an asset project builds assets");
        assert_eq!(assets.file_count, 1);
        assert_eq!(
            assets.blobs.values().collect::<Vec<_>>(),
            vec![&b"INSIDE\n".to_vec()]
        );
    }

    #[test]
    fn d1_migrations_dir_outside_the_project_is_refused() {
        let project = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(outside.path().join("mig")).unwrap();
        std::fs::write(outside.path().join("mig/0001_init.sql"), "OUTSIDE\n").unwrap();
        symlink_for_test(outside.path(), &project.path().join("sub"));
        std::fs::write(
            project.path().join("wrangler.json"),
            r#"{"name": "dbtest", "d1_databases": [{"database_name": "db", "migrations_dir": "sub/mig"}]}"#,
        )
        .unwrap();
        let error = refused(
            read_d1_project(Some(project.path().join("wrangler.json"))),
            "a migrations dir outside the project must be refused",
        );
        let message = format!("{error:?}");
        assert!(
            message.contains("`migrations_dir`") && message.contains("escapes the project"),
            "the refusal must name the config key: {message}"
        );
    }

    #[test]
    fn d1_migrations_dir_through_an_in_tree_link_is_accepted() {
        let project = tempfile::tempdir().unwrap();
        std::fs::create_dir(project.path().join("real")).unwrap();
        std::fs::write(project.path().join("real/0001_init.sql"), "INSIDE\n").unwrap();
        symlink_for_test(Path::new("real"), &project.path().join("sub"));
        std::fs::write(
            project.path().join("wrangler.json"),
            r#"{"name": "dbtest", "d1_databases": [{"database_name": "db", "migrations_dir": "sub"}]}"#,
        )
        .unwrap();
        let parsed = read_d1_project(Some(project.path().join("wrangler.json"))).unwrap();
        assert_eq!(parsed.databases.len(), 1);
        let canonical_root = project.path().canonicalize().unwrap();
        assert!(
            parsed.databases[0]
                .migrations_dir
                .starts_with(&canonical_root),
            "the migrations dir must resolve inside the project: {}",
            parsed.databases[0].migrations_dir.display()
        );
    }

    #[test]
    fn config_through_a_link_outside_the_project_is_refused() {
        let project = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(
            outside.path().join("other.json"),
            r#"{"name": "outside", "main": "entry.js", "no_bundle": true}"#,
        )
        .unwrap();
        std::fs::write(outside.path().join("entry.js"), "OUTSIDE\n").unwrap();
        symlink_for_test(
            &outside.path().join("other.json"),
            &project.path().join("wrangler.json"),
        );
        let error = refused(
            build(&test_options(&project.path().join("wrangler.json"), None)),
            "a config linked outside the project must be refused",
        );
        let message = format!("{error:?}");
        assert!(
            message.contains("project config") && message.contains("escapes the project"),
            "the refusal must name the config: {message}"
        );
        // The directory form finds the same linked file and refuses it too.
        let error = refused(
            build(&test_options(project.path(), None)),
            "a linked config found by directory must be refused",
        );
        let message = format!("{error:?}");
        assert!(
            message.contains("project config") && message.contains("escapes the project"),
            "the refusal must name the config: {message}"
        );
    }

    #[test]
    fn container_image_outside_the_project_is_refused_before_the_engine() {
        // The wired path: a config whose image escapes must fail naming the
        // key, without any engine on PATH (no `CELLD_DOCKER` is set here).
        let project = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("Dockerfile"), "FROM scratch\n").unwrap();
        symlink_for_test(outside.path(), &project.path().join("sub"));
        std::fs::write(
            project.path().join("wrangler.json"),
            r#"{"name": "linktest", "main": "entry.js", "no_bundle": true,
                "durable_objects": {"bindings": [{"name": "C", "class_name": "Worker"}]},
                "migrations": [{"tag": "v1", "new_sqlite_classes": ["Worker"]}],
                "containers": [{"class_name": "Worker", "image": "sub/Dockerfile"}]}"#,
        )
        .unwrap();
        std::fs::write(project.path().join("entry.js"), "INSIDE\n").unwrap();
        let error = refused(
            build(&test_options(&project.path().join("wrangler.json"), None)),
            "a container image outside the project must be refused",
        );
        let message = format!("{error:?}");
        assert!(
            message.contains("config `containers[0].image`")
                && message.contains("escapes the project"),
            "the refusal must name the config key: {message}"
        );
    }

    #[test]
    fn bundled_entry_through_an_in_tree_link_still_bundles() {
        if !esbuild_available() {
            eprintln!("skipping: esbuild not on PATH");
            return;
        }
        let project = tempfile::tempdir().unwrap();
        std::fs::create_dir(project.path().join("real")).unwrap();
        write_bundle_fixture(&project.path().join("real"));
        // The fixture's config names the bare entry; the project config
        // reaches it through the link.
        std::fs::write(
            project.path().join("wrangler.json"),
            r#"{"name": "maptest", "main": "sub/entry.js"}"#,
        )
        .unwrap();
        symlink_for_test(Path::new("real"), &project.path().join("sub"));
        let built = build(&test_options(&project.path().join("wrangler.json"), None)).unwrap();
        assert_eq!(built.modules.len(), 1);
        assert_eq!(built.modules[0].0, "index.js");
        assert!(
            built.modules[0].1.windows(2).any(|w| w == b"42"),
            "the bundle must contain the bundled dependency"
        );
    }

    #[test]
    fn unbundled_wasm_does_not_follow_nested_links() {
        let project = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("evil.wasm"), "OUTSIDE-WASM").unwrap();
        std::fs::write(project.path().join("entry.js"), "INSIDE\n").unwrap();
        symlink_for_test(outside.path(), &project.path().join("nested"));
        std::fs::write(
            project.path().join("wrangler.json"),
            r#"{"name": "linktest", "main": "entry.js", "no_bundle": true}"#,
        )
        .unwrap();
        let built = build(&test_options(&project.path().join("wrangler.json"), None)).unwrap();
        assert_eq!(
            built.modules,
            vec![("index.js".to_string(), b"INSIDE\n".to_vec())]
        );
    }

    #[test]
    fn config_through_an_in_tree_link_still_deploys() {
        let project = tempfile::tempdir().unwrap();
        std::fs::write(
            project.path().join("real.json"),
            r#"{"name": "linktest", "main": "entry.js", "no_bundle": true}"#,
        )
        .unwrap();
        std::fs::write(project.path().join("entry.js"), "INSIDE\n").unwrap();
        symlink_for_test(
            Path::new("real.json"),
            &project.path().join("wrangler.json"),
        );
        let built = build(&test_options(&project.path().join("wrangler.json"), None)).unwrap();
        assert_eq!(built.script_name, "linktest");
        assert_eq!(
            built.modules,
            vec![("index.js".to_string(), b"INSIDE\n".to_vec())]
        );
    }

    #[test]
    fn sourcemap_asset_only_project_writes_nothing() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("wrangler.json"),
            r#"{"name": "maptest", "assets": {"directory": "public"}}"#,
        )
        .unwrap();
        std::fs::create_dir(root.path().join("public")).unwrap();
        std::fs::write(root.path().join("public/index.html"), "<!doctype html>\n").unwrap();
        let map_path = root.path().join("worker.js.map");
        let options = test_options(&root.path().join("wrangler.json"), Some(map_path.clone()));
        let built = build(&options).unwrap();
        assert!(built.modules.is_empty());
        assert_eq!(built.sourcemap, None);
        write_sourcemap(&options, &built).unwrap();
        assert!(!map_path.exists());
    }
}
