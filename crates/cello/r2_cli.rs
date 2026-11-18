// Modified by ryuzcorp (ryuzcorp/celld): r2 list --delimiter, a binding-shape --page, and --cursor continuation; r2 head/get --json answers an absent key with a not_found error object.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The R2 CLI reads operator input and object bodies outside node storage.
#![allow(clippy::disallowed_methods)]

//! `celld r2` — read and write the objects behind an `r2_buckets` binding.
//!
//! A release pipeline publishes a build artifact with
//! `wrangler r2 object put` and the Worker serves it from its `r2_buckets`
//! binding. celld served the binding but had no command, so the only way to
//! seed a bucket was `aws s3 cp` straight into the fleet bucket. That works
//! for the bytes and loses the record: an R2 object carries
//! `customMetadata`, a `cacheExpiry`, and its checksums in a celld-private
//! envelope that a plain copy does not write. The result reads back with no
//! custom metadata, which is a silent difference rather than a failure.
//!
//! So this command does not implement R2. It installs the fleet bucket the
//! same way a node does and calls [`crate::js::r2_ops`] — the functions the
//! binding's ops call. The key scoping, the envelope, the checksum set and
//! the single-request/multipart choice therefore have one implementation,
//! and an object written here is an object the binding could have written.
//!
//! Unlike `celld d1`, `celld kv` and `celld queue`, this command needs no
//! node. A binding reads the fleet bucket directly, so an operator with the
//! bucket credentials can publish into an empty fleet — which is what a
//! release pipeline that runs before a deployment needs.

use crate::cli_options::FLEET_HELP;
use crate::cli_options::LISTING_HELP;
use crate::cli_output::list;
use crate::cli_output::Bounds;
use crate::cli_output::Format;
use crate::cli_output::Output;
use crate::cli_output::Page;
use crate::cli_output::Record;
use crate::cli_output::Resumable;
use crate::cli_output::Resume;
use crate::note;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Context;
use serde_json::json;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;

/// How much of a body the command reads at once. The write buffers what it
/// is given and cuts its own parts, so this only bounds the resident copy
/// of a body that can be many gigabytes.
const READ_CHUNK: usize = 1 << 20;

/// One object in a listing. The row continues past the cursor the
/// store answered the row with: the key itself, wrapped with the
/// listing's bound when the walk is seeded.
struct Object {
    key: String,
    size: u64,
    etag: Option<String>,
    uploaded_ms: i64,
    cursor: String,
}

impl Record for Object {
    fn json(&self) -> Value {
        json!({
            "key": self.key,
            "size": self.size,
            "etag": self.etag,
            "uploaded": self.uploaded_ms,
        })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.key)
    }
}

impl Resumable for Object {
    fn cursor(&self) -> &str {
        &self.cursor
    }
}

/// One prefix a delimiter rolled up, in a text listing. The row
/// continues past the last key that rolled into it: the prefix string
/// itself encodes ahead of its keys, so resuming after it would re-walk
/// the whole folder.
struct Prefix {
    prefix: String,
    cursor: String,
}

impl Record for Prefix {
    fn json(&self) -> Value {
        json!({ "prefix": self.prefix })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.prefix)
    }
}

impl Resumable for Prefix {
    fn cursor(&self) -> &str {
        &self.cursor
    }
}

/// One row of a text listing, in the store's walk order: objects and the
/// prefixes a delimiter rolled up, interleaved by first key. Without a
/// delimiter the page carries no prefixes, so the output is one key per
/// line exactly as before.
enum Row {
    Object(Object),
    Prefix(Prefix),
}

impl Record for Row {
    fn json(&self) -> Value {
        match self {
            Self::Object(object) => object.json(),
            Self::Prefix(prefix) => prefix.json(),
        }
    }

    fn text(&self) -> Cow<'_, str> {
        match self {
            Self::Object(object) => object.text(),
            Self::Prefix(prefix) => prefix.text(),
        }
    }
}

impl Resumable for Row {
    fn cursor(&self) -> &str {
        match self {
            Self::Object(object) => object.cursor(),
            Self::Prefix(prefix) => prefix.cursor(),
        }
    }
}

/// One object's complete record, as `celld r2 head` reports it.
///
/// The JSON carries the binding's object record before the JS conversion.
/// Its `http` and `custom` fields become `httpMetadata` and
/// `customMetadata` in an `R2Object`.
struct Head(Value);

impl Record for Head {
    fn json(&self) -> Value {
        self.0.clone()
    }

    fn text(&self) -> Cow<'_, str> {
        let field = |name: &str| self.0.get(name).cloned().unwrap_or(Value::Null);
        let scalar = |value: &Value| match value {
            Value::Null => "-".to_string(),
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        let mut lines = vec![
            format!("key:      {}", scalar(&field("key"))),
            format!("size:     {}", scalar(&field("size"))),
            format!("etag:     {}", scalar(&field("etag"))),
            format!("uploaded: {}", scalar(&field("uploaded"))),
        ];
        // The two maps print only when they hold something. An object
        // written by a plain `aws s3 cp` has neither, and an empty block
        // for each would hide that difference in a wall of dashes.
        for (label, name) in [("http", "http"), ("custom", "custom")] {
            if let Some(map) = field(name).as_object().filter(|map| !map.is_empty()) {
                for (key, value) in map {
                    lines.push(format!("{label}.{key}: {}", scalar(value)));
                }
            }
        }
        Cow::Owned(lines.join("\n"))
    }
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    let storage = command.fleet.clone().resolve("celld r2")?;
    // The binding's store, installed exactly as a node installs it, so
    // every call below reaches the same keys the Worker reads.
    crate::js::set_r2_store(storage.open().await?);
    let bucket = command.bucket.as_str();

    match command.action {
        Action::Get { key } => {
            let read = crate::js::r2_ops::read(
                bucket,
                &key,
                crate::bucket::BlobRange::Whole,
                &crate::bucket::BlobConditions::default(),
            )
            .await
            .map_err(anyhow::Error::msg)?;
            match read {
                crate::bucket::BlobRead::Hit(blob) => {
                    // Streamed rather than collected: a release artifact is
                    // routinely larger than the memory a CI runner gives
                    // this process.
                    Output::new(Format::Text).stream(blob.body).await?;
                }
                // An unconditional read cannot be refused, so the only
                // other answer is an absent key.
                crate::bucket::BlobRead::Missing | crate::bucket::BlobRead::Unmet(_) => {
                    return absent_key(bucket, &key, command.json);
                }
            }
        }
        Action::Head { key } => {
            let Some(meta) = crate::js::r2_ops::head(bucket, &key)
                .await
                .map_err(anyhow::Error::msg)?
            else {
                return absent_key(bucket, &key, command.json);
            };
            let mut out = Output::new(if command.json {
                Format::Json
            } else {
                Format::Text
            });
            out.row(&Head(crate::js::r2_ops::object_json(&key, &meta, None)))?;
            out.finish()?;
        }
        Action::Put {
            key,
            file,
            http,
            custom,
        } => {
            let request = json!({ "http": http, "custom": custom }).to_string();
            let mut put = crate::js::r2_ops::open_put(bucket.to_string(), key.clone(), &request)
                .map_err(anyhow::Error::msg)?;
            let mut source = Source::open(file.as_deref())?;
            let mut buffer = vec![0u8; READ_CHUNK];
            loop {
                let read = source.read(&mut buffer).context("read the object body")?;
                if read == 0 {
                    break;
                }
                put.push(buffer[..read].to_vec())
                    .await
                    .map_err(anyhow::Error::msg)?;
            }
            let answer = put.finish().await.map_err(anyhow::Error::msg)?;
            let answer: Value = serde_json::from_str(&answer).context("read the write's answer")?;
            let size = answer
                .pointer("/object/size")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            note!("wrote {key:?} ({size} bytes)");
        }
        Action::Delete { keys } => {
            crate::js::r2_ops::delete(bucket, &keys)
                .await
                .map_err(anyhow::Error::msg)?;
            note!("deleted {} object(s)", keys.len());
        }
        Action::List {
            prefix,
            delimiter,
            page,
            cursor,
            bounds,
        } => {
            // `--page` prints one page object — the shape a Worker's
            // `list()` returns — so its cursor is in-band and there is no
            // resume line to print. `--json` keeps its one-record-per-line
            // output, unchanged from upstream.
            if page {
                let page = list_json(
                    bucket,
                    &prefix,
                    delimiter.as_deref(),
                    &bounds,
                    cursor.as_deref(),
                )
                .await?;
                Output::new(Format::Text).line(format_args!("{page}"))?;
                return Ok(());
            }
            let mut out = Output::new(if command.json {
                Format::Json
            } else {
                Format::Text
            });
            let listed = list(&mut out, &bounds, |resume, want| {
                let prefix = prefix.clone();
                let delimiter = delimiter.clone();
                let cursor = cursor.clone();
                async move {
                    fetch_text_page(bucket, &prefix, delimiter.as_deref(), &resume, cursor, want)
                        .await
                }
            })
            .await?;
            out.finish()?;
            // The continuation is the store's cursor past the last printed
            // row, never `--after`: `startAfter` compares decoded keys
            // while the store walks the encoded form, so continuing by key
            // silently drops keys where the two orders disagree.
            listed.report("object", "--cursor");
        }
    }
    Ok(())
}

/// One page of a text listing, through the same request the binding
/// sends. An operator's `--after` bounds the first page and the
/// driver's own continuation — or the operator's `--cursor` — carries
/// the walk; R2 reads the cursor first, so when both are present the
/// cursor wins.
async fn fetch_text_page(
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    resume: &Resume,
    seed: Option<String>,
    want: usize,
) -> anyhow::Result<Page<Row>> {
    let (cursor, after) = match resume {
        Resume::From(after) => (seed, after.clone()),
        Resume::Token(token) => (Some(token.clone()), None),
    };
    let page = crate::js::r2_ops::list_entries(
        bucket,
        &crate::js::r2_ops::list_request(
            prefix,
            cursor.as_deref(),
            after.as_deref(),
            want,
            delimiter,
            false,
        ),
    )
    .await
    .map_err(anyhow::Error::msg)?;
    let rows = page
        .entries
        .into_iter()
        .map(|row| match row {
            crate::bucket::BlobRow::Object { entry, cursor } => Row::Object(Object {
                key: entry.key,
                size: entry.size,
                etag: entry.etag,
                uploaded_ms: entry.uploaded_ms,
                cursor,
            }),
            crate::bucket::BlobRow::Prefix(prefix) => Row::Prefix(Prefix {
                prefix: prefix.prefix,
                cursor: prefix.cursor,
            }),
        })
        .collect();
    // The store's own truncation signal, never a row count: a page
    // filled exactly to its bound is not evidence that another object
    // exists.
    Ok(Page {
        rows,
        next: page.cursor,
    })
}

/// The machine-readable absent-key answer: a `--json` read prints this
/// on stdout, so its caller matches the `error` field instead of
/// substring-matching the English message on stderr.
fn not_found_error(bucket: &str, key: &str) -> Value {
    json!({
        "error": "not_found",
        "bucket": bucket,
        "key": key,
    })
}

/// Fail an absent key. `--json` prints the [`not_found_error`] object on
/// stdout first; the failure itself keeps the human message and the exit
/// code it always had, so only `--json` readers see anything new.
/// `delete` never comes here: removing an absent key succeeds, as on R2.
fn absent_key(bucket: &str, key: &str, json: bool) -> anyhow::Result<()> {
    if json {
        Output::new(Format::Json).line(format_args!("{}", not_found_error(bucket, key)))?;
    }
    bail!("no object {key:?} in r2 bucket {bucket:?}")
}

/// One listed object in the binding's shape: the key, size, etag and
/// upload time the text listing prints, plus the `httpMetadata` and
/// `customMetadata` the `include` half of the request fetched with one
/// head per object.
fn object_json(object: &Value) -> Value {
    json!({
        "key": object.get("key").and_then(Value::as_str).unwrap_or_default(),
        "size": object.get("size").and_then(Value::as_u64).unwrap_or_default(),
        "etag": object.get("etag").and_then(Value::as_str),
        "uploaded": object.get("uploaded").and_then(Value::as_i64).unwrap_or_default(),
        "httpMetadata": object.get("http").cloned().unwrap_or(json!({})),
        "customMetadata": object.get("custom").cloned().unwrap_or(json!({})),
    })
}

/// One listing page in the shape a Worker's `list()` returns, through the
/// same `r2_ops::list` call the binding makes. `--all` walks every page
/// and merges them; otherwise this is one page and its cursor continues
/// it with `--cursor`.
async fn list_json(
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    bounds: &Bounds,
    seed: Option<&str>,
) -> anyhow::Result<Value> {
    let mut objects = Vec::new();
    let mut prefixes = Vec::new();
    // A prefix the walk already reported costs nothing. Keys under a
    // prefix sort contiguously, so one the store produced never answers
    // twice; the set is insurance for keys another tool wrote.
    let mut seen = std::collections::BTreeSet::new();
    let mut cursor: Option<String> = seed.map(str::to_string);
    let mut after = bounds.after.clone();
    loop {
        let page = crate::js::r2_ops::list(
            bucket,
            &crate::js::r2_ops::list_request(
                prefix,
                cursor.as_deref(),
                after.as_deref(),
                match bounds.all {
                    true => Bounds::MAX_PAGE,
                    false => bounds.limit.min(Bounds::MAX_PAGE),
                },
                delimiter,
                true,
            ),
        )
        .await
        .map_err(anyhow::Error::msg)?;
        let listed = page
            .get("objects")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("the listing carried no objects: {page}"))?;
        objects.extend(listed.iter().map(object_json));
        let rolled = page
            .get("prefixes")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("the listing carried no prefixes: {page}"))?;
        for prefix in rolled.iter().filter_map(Value::as_str) {
            // One page never repeats a prefix, so the set only pays on a
            // multi-page walk.
            if bounds.all && !seen.insert(prefix.to_string()) {
                continue;
            }
            prefixes.push(Value::String(prefix.to_string()));
        }
        let truncated = page
            .get("truncated")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let next = page
            .get("cursor")
            .and_then(Value::as_str)
            .map(str::to_string);
        // R2 omits `cursor` entirely when the listing is complete,
        // and callers test `truncated` before reading it.
        if !bounds.all {
            let mut page = json!({
                "objects": objects,
                "delimitedPrefixes": prefixes,
                "truncated": truncated,
            });
            if let (true, Some(next)) = (truncated, next) {
                page["cursor"] = Value::String(next);
            }
            return Ok(page);
        }
        match (truncated, next) {
            (true, Some(next)) => {
                cursor = Some(next);
                after = None;
            }
            _ => {
                return Ok(json!({
                    "objects": objects,
                    "delimitedPrefixes": prefixes,
                    "truncated": false,
                }));
            }
        }
    }
}

/// Where a `put` reads its body. A named file and standard input are both
/// read in bounded pieces, so neither has to fit in memory.
enum Source {
    File(std::io::BufReader<std::fs::File>),
    Stdin(std::io::Stdin),
}

impl Source {
    fn open(path: Option<&std::path::Path>) -> anyhow::Result<Self> {
        match path {
            None => Ok(Self::Stdin(std::io::stdin())),
            Some(path) => {
                let file = std::fs::File::open(path)
                    .with_context(|| format!("open {}", path.display()))?;
                Ok(Self::File(std::io::BufReader::new(file)))
            }
        }
    }

    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(file) => file.read(buffer),
            Self::Stdin(stdin) => stdin.lock().read(buffer),
        }
    }
}

enum Action {
    Get {
        key: String,
    },
    Head {
        key: String,
    },
    Put {
        key: String,
        /// `None` reads the body from standard input.
        file: Option<PathBuf>,
        /// R2's `httpMetadata`, already in the binding's spelling.
        http: BTreeMap<String, Value>,
        /// R2's `customMetadata`.
        custom: BTreeMap<String, String>,
    },
    Delete {
        keys: Vec<String>,
    },
    List {
        prefix: String,
        delimiter: Option<String>,
        /// Print one binding-shaped page object instead of records.
        page: bool,
        /// Resume after the cursor a truncated listing answered with.
        cursor: Option<String>,
        bounds: Bounds,
    },
}

struct Command {
    action: Action,
    bucket: String,
    fleet: crate::cli_options::FleetFlags,
    json: bool,
}

/// The `httpMetadata` field each flag sets, in R2's spelling.
const HTTP_FLAGS: [(&str, &str); 5] = [
    ("--content-type", "contentType"),
    ("--content-language", "contentLanguage"),
    ("--content-disposition", "contentDisposition"),
    ("--content-encoding", "contentEncoding"),
    ("--cache-control", "cacheControl"),
];

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter().peekable();
        let Some(first) = arguments.next() else {
            return Ok(None);
        };
        let verb = match first.as_str() {
            "--help" | "-h" | "help" => return Ok(None),
            verb @ ("get" | "head" | "put" | "delete" | "list") => verb.to_string(),
            other => bail!("unknown `celld r2` subcommand: {other}"),
        };
        // The bucket is the `bucket_name` from `r2_buckets`, not the
        // binding name: the name is what places the keys in the fleet
        // bucket, and it is the one an `env.BUCKET` call resolves to.
        let bucket = arguments
            .next()
            .filter(|value| !value.starts_with('-'))
            .ok_or_else(|| anyhow!("celld r2 needs an r2 bucket name"))?;
        // The same rule the deployment applies to `bucket_name`. Without
        // it a name with a `/` in it would write under another binding's
        // prefix, or outside the reserved `r2/` space entirely.
        if !crate::deploy::valid_resource_name(&bucket) {
            bail!(
                "invalid r2 bucket name {bucket:?}: use letters, digits, `-` and `_` \
                 (not starting with `-`), at most 64 characters"
            );
        }

        let mut positional = Vec::new();
        let mut file = None;
        let mut pipe = false;
        let mut metadata = None;
        let mut prefix = String::new();
        let mut delimiter = None;
        let mut cursor = None;
        let mut http = BTreeMap::new();
        let mut cache_expiry = None;
        let mut fleet = crate::cli_options::FleetFlags::default();
        let mut bounds = Bounds::default();
        let mut json = false;
        let mut page = false;
        while let Some(argument) = arguments.next() {
            let mut value = |flag: &str| {
                arguments
                    .next()
                    .ok_or_else(|| anyhow!("{flag} requires a value"))
            };
            match argument.as_str() {
                "--path" | "--file" => file = Some(PathBuf::from(value("--path")?)),
                "--pipe" => pipe = true,
                "--metadata" => metadata = Some(value("--metadata")?),
                "--cache-expiry" => {
                    let raw = value("--cache-expiry")?;
                    cache_expiry = Some(raw.parse::<i64>().with_context(|| {
                        format!("--cache-expiry takes unix milliseconds, got {raw:?}")
                    })?);
                }
                "--prefix" => prefix = value("--prefix")?,
                "--delimiter" => delimiter = Some(value("--delimiter")?),
                "--cursor" => cursor = Some(value("--cursor")?),
                "--json" => json = true,
                "--page" => page = true,
                // celld has neither concept, and a flag that silently means
                // nothing is the gap the compatibility page forbids.
                flag @ ("--local" | "--remote" | "--jurisdiction") => bail!(
                    "celld r2 does not take {flag}: celld runs neither a local \
                     miniflare store nor a Cloudflare account, and it has no \
                     jurisdiction. An r2 bucket name addresses one key space in \
                     one fleet."
                ),
                "--help" | "-h" => return Ok(None),
                other => {
                    if let Some((_, field)) = HTTP_FLAGS.iter().find(|(flag, _)| *flag == other) {
                        let text = value(other)?;
                        http.insert((*field).to_string(), Value::String(text));
                        continue;
                    }
                    // The shared flags first, so `--bucket`, `--limit` and
                    // their siblings mean here exactly what they mean in
                    // every other command.
                    if fleet.consume(other, &mut value)? || bounds.consume(other, &mut value)? {
                        continue;
                    }
                    if other.starts_with('-') {
                        bail!("unknown option: {other}; run `celld r2 --help` for usage")
                    }
                    positional.push(other.to_string());
                }
            }
        }
        if let Some(expiry) = cache_expiry {
            http.insert("cacheExpiry".to_string(), Value::from(expiry));
        }
        // A flag the verb does not read must be a refusal. Accepting
        // `--content-type` on a `get` would tell an operator they set a
        // header that nothing wrote.
        let refuse = |flag: &str, set: bool| -> anyhow::Result<()> {
            if set {
                bail!("`celld r2 {verb}` takes no {flag}");
            }
            Ok(())
        };
        let write_flags = !http.is_empty() || metadata.is_some();
        let one_key = |what: &str| -> anyhow::Result<String> {
            let [key] = positional.as_slice() else {
                bail!("celld r2 {what} needs exactly one key");
            };
            Ok(key.clone())
        };
        let action = match verb.as_str() {
            "get" | "head" => {
                refuse("--path", file.is_some())?;
                refuse("--pipe", pipe)?;
                refuse("--metadata or a content flag", write_flags)?;
                refuse("--delimiter", delimiter.is_some())?;
                refuse("--cursor", cursor.is_some())?;
                refuse("--page", page)?;
                let key = one_key(&verb)?;
                match verb.as_str() {
                    "get" => Action::Get { key },
                    _ => Action::Head { key },
                }
            }
            "put" => {
                refuse("--delimiter", delimiter.is_some())?;
                refuse("--cursor", cursor.is_some())?;
                refuse("--page", page)?;
                let key = one_key("put")?;
                // Reading a body from a terminal by default would hang
                // with no output, so standard input is opt-in.
                if file.is_some() == pipe {
                    bail!("celld r2 put requires exactly one of --path FILE or --pipe");
                }
                let custom = match &metadata {
                    None => BTreeMap::new(),
                    Some(text) => parse_metadata(text)?,
                };
                Action::Put {
                    key,
                    file,
                    http,
                    custom,
                }
            }
            "delete" => {
                refuse("--path", file.is_some())?;
                refuse("--pipe", pipe)?;
                refuse("--metadata or a content flag", write_flags)?;
                refuse("--delimiter", delimiter.is_some())?;
                refuse("--cursor", cursor.is_some())?;
                refuse("--page", page)?;
                if positional.is_empty() {
                    bail!("celld r2 delete needs at least one key");
                }
                Action::Delete { keys: positional }
            }
            "list" => {
                refuse("--path", file.is_some())?;
                refuse("--pipe", pipe)?;
                refuse("--metadata or a content flag", write_flags)?;
                if !positional.is_empty() {
                    bail!("celld r2 list takes no positional arguments; use --prefix");
                }
                // `--page` already prints one JSON page object, so `--json`
                // beside it is refused rather than silently meaning nothing.
                if page && json {
                    bail!("`celld r2 list --page` prints JSON already; it takes no `--json`");
                }
                Action::List {
                    prefix,
                    delimiter,
                    page,
                    cursor,
                    bounds: bounds.clone(),
                }
            }
            _ => unreachable!("the verb was matched above"),
        };
        bounds.validate()?;
        Ok(Some(Self {
            action,
            bucket,
            fleet: fleet.with_environment(),
            json,
        }))
    }
}

/// `--metadata` as R2's `customMetadata`: a flat object of strings.
///
/// A nested value is refused rather than stringified. R2 stores strings, so
/// accepting an object would write `[object Object]` on one side of the
/// binding and a JSON document on the other.
fn parse_metadata(text: &str) -> anyhow::Result<BTreeMap<String, String>> {
    let parsed: Value = serde_json::from_str(text).context("parse --metadata as JSON")?;
    let object = parsed
        .as_object()
        .ok_or_else(|| anyhow!("--metadata takes a JSON object of strings"))?;
    object
        .iter()
        .map(|(name, value)| match value {
            Value::String(text) => Ok((name.clone(), text.clone())),
            _ => Err(anyhow!(
                "--metadata value for {name:?} must be a string, because R2 \
                 stores customMetadata as strings"
            )),
        })
        .collect()
}

pub fn print_help() {
    // The whole block is laid out here, and the content flags are generated
    // from the table the parser reads, so a flag cannot be accepted and
    // undocumented, or documented and rejected. The column is wide enough
    // for `--content-disposition VALUE`, which is the longest of them.
    let row = |flag: String, text: &str| format!("  {flag:<27} {text}");
    let flags = [
        row("--path FILE".to_string(), "read the body from a file"),
        row("--pipe".to_string(), "read the body from standard input"),
        row(
            "--metadata JSON".to_string(),
            "customMetadata, as a JSON object of strings",
        ),
    ]
    .into_iter()
    .chain(HTTP_FLAGS.iter().map(|(flag, field)| {
        row(
            format!("{flag} VALUE"),
            &format!("set httpMetadata.{field}"),
        )
    }))
    .chain(std::iter::once(row(
        "--cache-expiry MS".to_string(),
        "set httpMetadata.cacheExpiry, in unix milliseconds",
    )))
    .collect::<Vec<_>>()
    .join("\n");
    let text = format!(
        "celld r2 — read and write the objects behind an `r2_buckets` binding

USAGE
  celld r2 get    <bucket-name> <key>    [--json] [fleet options]
  celld r2 head   <bucket-name> <key>    [--json] [fleet options]
  celld r2 put    <bucket-name> <key>    --path FILE | --pipe [put options]
  celld r2 delete <bucket-name> <key>... [fleet options]
  celld r2 list   <bucket-name>          [--prefix P] [--delimiter D] [listing options]

The bucket name is the `bucket_name` from the project's `r2_buckets` entry,
not the binding name. `get` writes the object to stdout as bytes, so an
artifact comes back byte for byte.

A get or head of an absent key fails with `no object ...` on stderr.
With --json it also prints {{\"error\":\"not_found\",...}} on stdout
first, so a caller matches the error field instead of the English
message. `delete` of an absent key succeeds, as on R2.

This command reads the fleet bucket directly and needs no running node, so
a release pipeline can publish an object before it deploys the Worker.

PUT OPTIONS
{flags}

LISTING OPTIONS (list)
  --prefix P          list only the keys under this prefix
  --delimiter D       roll up keys below D into delimitedPrefixes
  --cursor C          continue after the cursor a truncated listing answered
  --page              print one binding-shaped page object (see below)
{LISTING_HELP}

With --page, list prints one page object — objects with key, size,
etag, uploaded, httpMetadata and customMetadata, plus
delimitedPrefixes, truncated and cursor — the fields of a Worker's
list() page other than version, checksums and storageClass. Without it, list prints one key per line (one JSON record
per line with --json), with rolled-up prefixes on their own lines in
walk order.

Continue a truncated listing with --cursor, as the resume line says:
--after bounds the first page past a key, while the cursor continues
the walk past exactly what the listing consumed. When both are given
the cursor wins, as in a Worker's list().

FLEET OPTIONS
{FLEET_HELP}

An object written here carries the same record as one a Worker writes:
customMetadata, cacheExpiry and the checksums travel in a celld envelope
beside the five content headers. A copy made with another tool still reads
through the binding, and its user metadata becomes its customMetadata."
    );
    let _ = crate::cli_output::Output::new(crate::cli_output::Format::Text).help(&text);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(arguments: &[&str]) -> anyhow::Result<Option<Command>> {
        Command::parse(
            arguments
                .iter()
                .map(|argument| argument.to_string())
                .collect(),
        )
    }

    #[test]
    fn list_takes_a_delimiter() {
        let command = parse(&["list", "bucket", "--delimiter", "/"])
            .expect("parse the listing")
            .expect("a verb was given");
        assert!(
            matches!(command.action, Action::List { delimiter: Some(delimiter), .. } if delimiter == "/")
        );
    }

    #[test]
    fn other_verbs_refuse_a_delimiter() {
        for verb in ["get", "head", "put", "delete"] {
            let arguments: Vec<&str> = match verb {
                "put" => vec![verb, "bucket", "key", "--delimiter", "/", "--pipe"],
                "get" | "head" => vec![verb, "bucket", "key", "--delimiter", "/"],
                _ => vec![verb, "bucket", "key", "--delimiter", "/"],
            };
            assert!(parse(&arguments).is_err(), "{verb} accepted --delimiter");
        }
    }

    /// `--json` keeps upstream's one-record-per-line listing; only
    /// `--page` switches to the page object, and only `list` takes it.
    #[test]
    fn page_is_its_own_flag() {
        let json = parse(&["list", "bucket", "--json"])
            .expect("parse the listing")
            .expect("a verb was given");
        assert!(json.json);
        assert!(matches!(json.action, Action::List { page: false, .. }));
        let page = parse(&["list", "bucket", "--page"])
            .expect("parse the listing")
            .expect("a verb was given");
        assert!(matches!(page.action, Action::List { page: true, .. }));
        for arguments in [
            ["get", "bucket", "key", "--page"].as_slice(),
            ["head", "bucket", "key", "--page"].as_slice(),
            ["put", "bucket", "key", "--page", "--pipe"].as_slice(),
            ["delete", "bucket", "key", "--page"].as_slice(),
        ] {
            assert!(parse(arguments).is_err(), "{arguments:?} accepted --page");
        }
    }

    /// The text listing prints one key per line, with rolled-up prefixes
    /// on their own lines; a prefix row resumes after the last key that
    /// rolled into it.
    #[test]
    fn text_rows_still_print_keys() {
        let row = Row::Object(Object {
            key: "photos/sun.png".to_string(),
            size: 3,
            etag: None,
            uploaded_ms: 0,
            cursor: "photos/sun.png".to_string(),
        });
        assert_eq!(row.text(), "photos/sun.png");
        assert_eq!(row.cursor(), "photos/sun.png");
        let row = Row::Prefix(Prefix {
            prefix: "photos/".to_string(),
            cursor: "photos/sun.png".to_string(),
        });
        assert_eq!(row.text(), "photos/");
        assert_eq!(row.cursor(), "photos/sun.png");
    }

    /// `--page` lists one binding-shaped page through the store: decoded
    /// keys with their `httpMetadata`, the rolled-up prefixes, and the
    /// cursor that continues a truncated page.
    #[tokio::test]
    async fn json_lists_one_binding_shaped_page() {
        // The bindings' store, shared with the `r2_store` suite (a second
        // install is ignored); buckets isolate.
        crate::host_channels::set_r2_store(crate::bucket::Bucket::for_test());
        for (key, content_type) in [
            ("a/1", None),
            ("a/2", None),
            ("b/1", None),
            ("top.txt", Some("text/plain")),
        ] {
            let request = serde_json::from_value(serde_json::json!({
                "http": { "contentType": content_type },
            }))
            .expect("parse the put options");
            crate::r2_store::put_once(
                "jsoncli".to_string(),
                key.to_string(),
                b"body".to_vec(),
                request,
            )
            .await
            .expect("put the test object");
        }
        let mut bounds = Bounds::default();
        bounds.limit = 2;
        let page = list_json("jsoncli", "", Some("/"), &bounds, None)
            .await
            .expect("list the page");
        assert_eq!(
            page.get("delimitedPrefixes"),
            Some(&json!(["a/", "b/"])),
            "the page rolls folders up: {page}"
        );
        assert_eq!(page.get("truncated"), Some(&json!(true)));
        let cursor = page
            .get("cursor")
            .and_then(Value::as_str)
            .expect("a truncated page names its cursor");
        bounds.limit = 100;
        let page = list_json("jsoncli", "", Some("/"), &bounds, Some(cursor))
            .await
            .expect("list the next page");
        let objects = page
            .get("objects")
            .and_then(Value::as_array)
            .expect("the page carries objects");
        assert_eq!(objects.len(), 1);
        assert_eq!(
            objects[0]
                .pointer("/httpMetadata/contentType")
                .and_then(Value::as_str),
            Some("text/plain")
        );
        assert_eq!(page.get("truncated"), Some(&json!(false)));
    }

    /// One listed object in the binding's shape: the record's `http`
    /// becomes `httpMetadata` and its `custom` becomes `customMetadata`,
    /// and a missing map is empty rather than absent.
    #[test]
    fn objects_carry_http_metadata() {
        let object = json!({
            "key": "photos/sun.png",
            "size": 3,
            "etag": "\"abc\"",
            "uploaded": 1_791_234_567_000i64,
            "http": { "contentType": "image/png" },
            "custom": { "team": "field" },
        });
        assert_eq!(
            object_json(&object),
            json!({
                "key": "photos/sun.png",
                "size": 3,
                "etag": "\"abc\"",
                "uploaded": 1_791_234_567_000i64,
                "httpMetadata": { "contentType": "image/png" },
                "customMetadata": { "team": "field" },
            })
        );
        let bare = json!({"key": "k", "size": 0, "uploaded": 0});
        assert_eq!(object_json(&bare).get("httpMetadata"), Some(&json!({})));
        assert_eq!(object_json(&bare).get("customMetadata"), Some(&json!({})));
    }

    async fn put_plain(bucket: &str, key: &str) {
        crate::host_channels::set_r2_store(crate::bucket::Bucket::for_test());
        let request = serde_json::from_value(serde_json::json!({ "http": {} }))
            .expect("parse the put options");
        crate::r2_store::put_once(
            bucket.to_string(),
            key.to_string(),
            b"body".to_vec(),
            request,
        )
        .await
        .expect("put the test object");
    }

    async fn list_page(
        bucket: &str,
        prefix: &str,
        cursor: Option<&str>,
        after: Option<&str>,
        limit: usize,
        delimiter: Option<&str>,
    ) -> Value {
        crate::r2_store::list(
            bucket,
            &crate::js::r2_ops::list_request(prefix, cursor, after, limit, delimiter, false),
        )
        .await
        .expect("list the page")
    }

    fn listed_objects(page: &Value) -> Vec<String> {
        page.get("objects")
            .and_then(Value::as_array)
            .expect("the page carries objects")
            .iter()
            .map(|object| {
                object
                    .get("key")
                    .and_then(Value::as_str)
                    .expect("an object carries its key")
                    .to_string()
            })
            .collect()
    }

    fn listed_prefixes(page: &Value) -> Vec<String> {
        page.get("prefixes")
            .and_then(Value::as_array)
            .expect("the page carries prefixes")
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect()
    }

    fn listed_truncated(page: &Value) -> bool {
        page.get("truncated")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }

    /// Keys whose encoded walk order disagrees with decoded UTF-8 byte
    /// order: `~` and non-ASCII bytes sort after plain letters decoded,
    /// before them encoded. `%`, spaces, `?` and `+` round-trip through
    /// the same codec.
    const ADVERSARIAL_FLAT: &[&str] = &[
        "a~", "aa", "m", "z", "~tilde", "éclair", "zebra", "ABC", "abc", "100%", "1000", "a b",
        "a+b", "a?b",
    ];

    /// Nested keys for delimiter walks: a long run under `a/` forces page
    /// boundaries inside one prefix, and escapables appear both in
    /// prefixes and in trailing segments.
    const ADVERSARIAL_NESTED: &[&str] = &[
        "a/1", "a/2", "a/3", "a/4", "a/5", "a~/1", "b/1", "m", "top.txt", "é/x", "x/é", "sp ace/f",
        "a//deep", "trail/", "%/pct", "zz",
    ];

    /// An escapable inside the listed prefix itself, with out-of-scope
    /// keys that must never leak into the walk.
    const ADVERSARIAL_PREFIX: &str = "p~/";
    const ADVERSARIAL_ESCAPED: &[&str] = &[
        "p~/a", "p~/a/1", "p~/a/2", "p~/~", "p~/é/x", "other/1", "p/1",
    ];

    /// The decoded-sorted objects and prefixes a complete walk over `keys`
    /// must report: R2 rolls a key up at the delimiter's first occurrence
    /// past the prefix, and `seed` bounds the walk's first page.
    fn expected_listed(
        keys: &[&str],
        prefix: &str,
        delimiter: Option<&str>,
        seed: Option<&str>,
    ) -> (Vec<String>, Vec<String>) {
        let mut selected: Vec<&str> = keys
            .iter()
            .copied()
            .filter(|key| {
                if !key.starts_with(prefix) {
                    return false;
                }
                match seed {
                    None => true,
                    Some(bound) => *key > bound,
                }
            })
            .collect();
        selected.sort_unstable();
        let mut objects = Vec::new();
        let mut prefixes = std::collections::BTreeSet::new();
        for key in selected {
            let remainder = key
                .strip_prefix(prefix)
                .expect("the filter matched the prefix");
            match delimiter.and_then(|delimiter| remainder.find(delimiter)) {
                Some(at) => {
                    let delimiter = delimiter.expect("the match names it");
                    prefixes.insert(format!("{prefix}{}", &remainder[..at + delimiter.len()]));
                }
                None => objects.push(key.to_string()),
            }
        }
        (objects, prefixes.into_iter().collect())
    }

    /// A Worker's page walk: the first page may carry a `startAfter`
    /// bound, every continuation carries the cursor the store answered
    /// with.
    async fn walk_binding(
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        limit: usize,
        seed: Option<&str>,
    ) -> (Vec<String>, Vec<String>) {
        let mut objects = Vec::new();
        let mut prefixes = Vec::new();
        let mut cursor: Option<String> = None;
        let mut after = seed.map(str::to_string);
        loop {
            let page = list_page(
                bucket,
                prefix,
                cursor.as_deref(),
                after.as_deref(),
                limit,
                delimiter,
            )
            .await;
            objects.extend(listed_objects(&page));
            prefixes.extend(listed_prefixes(&page));
            if !listed_truncated(&page) {
                break;
            }
            cursor = page
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_string);
            assert!(
                cursor.is_some(),
                "a truncated page names its cursor: {page}"
            );
            after = None;
        }
        (objects, prefixes)
    }

    /// The CLI's resume flow: page the text listing's rows in walk order
    /// and continue with `--cursor` past the last printed row, exactly as
    /// the resume line instructs. A stall or a silent omission here is a
    /// stall or a silent omission for every script following the hint.
    async fn walk_hint(
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        limit: usize,
        seed: Option<&str>,
    ) -> (Vec<String>, Vec<String>) {
        let mut objects = Vec::new();
        let mut prefixes = Vec::new();
        let mut cursor: Option<String> = None;
        let mut after = seed.map(str::to_string);
        let mut pages = 0;
        loop {
            pages += 1;
            assert!(
                pages <= 64,
                "the hint walk stalled: page {pages} repeats its own hint"
            );
            let page = fetch_text_page(
                bucket,
                prefix,
                delimiter,
                &Resume::From(after.clone()),
                cursor.clone(),
                limit,
            )
            .await
            .expect("list the page");
            let mut hint = None;
            for row in &page.rows {
                match row {
                    Row::Object(object) => objects.push(object.key.clone()),
                    Row::Prefix(prefix) => prefixes.push(prefix.prefix.clone()),
                }
                hint = Some(row.cursor().to_string());
            }
            let Some(last) = hint else {
                assert!(page.next.is_none(), "an empty page names no continuation");
                break;
            };
            if page.next.is_none() {
                break;
            }
            cursor = Some(last);
            after = None;
        }
        (objects, prefixes)
    }

    /// A `--page` walk: single pages chained by the in-band cursor, as a
    /// script paging `--page --cursor` walks.
    async fn walk_page(
        bucket: &str,
        prefix: &str,
        delimiter: Option<&str>,
        limit: usize,
        seed: Option<&str>,
    ) -> (Vec<String>, Vec<String>) {
        let mut objects = Vec::new();
        let mut prefixes = Vec::new();
        let mut cursor: Option<String> = None;
        let mut after = seed.map(str::to_string);
        loop {
            let mut bounds = Bounds::default();
            bounds.limit = limit;
            bounds.after = after.clone();
            let page = list_json(bucket, prefix, delimiter, &bounds, cursor.as_deref())
                .await
                .expect("list the page");
            objects.extend(
                page.get("objects")
                    .and_then(Value::as_array)
                    .expect("the page carries objects")
                    .iter()
                    .map(|object| {
                        object
                            .get("key")
                            .and_then(Value::as_str)
                            .expect("an object carries its key")
                            .to_string()
                    }),
            );
            prefixes.extend(
                page.get("delimitedPrefixes")
                    .and_then(Value::as_array)
                    .expect("the page carries prefixes")
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string),
            );
            if !page
                .get("truncated")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                break;
            }
            cursor = page
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_string);
            assert!(
                cursor.is_some(),
                "a truncated page names its cursor: {page}"
            );
            after = None;
        }
        (objects, prefixes)
    }

    /// Paging is complete over keys whose encoded and decoded orders
    /// disagree: every walk — binding tokens, CLI hints, and `--page`
    /// cursors, small limits, plain and seeded starts, flat and
    /// rolled-up — reports each key and each folder exactly once. The
    /// walk-order check first proves the corpus is adversarial: without
    /// a disagreement the completeness check below would pass vacuously.
    #[tokio::test]
    async fn paging_is_complete_over_adversarial_keys() {
        for key in ADVERSARIAL_FLAT {
            put_plain("advflat", key).await;
        }
        for key in ADVERSARIAL_NESTED {
            put_plain("advnest", key).await;
        }
        for key in ADVERSARIAL_ESCAPED {
            put_plain("advesc", key).await;
        }
        for (bucket, keys, prefix) in [
            ("advflat", ADVERSARIAL_FLAT, ""),
            ("advnest", ADVERSARIAL_NESTED, ""),
            ("advesc", ADVERSARIAL_ESCAPED, ADVERSARIAL_PREFIX),
        ] {
            // The walk order is the encoded order; it must differ from
            // decoded-sorted order or the corpus proves nothing.
            let (walked, _) = walk_binding(bucket, prefix, None, 1000, None).await;
            let (sorted, _) = expected_listed(keys, prefix, None, None);
            assert_ne!(
                walked, sorted,
                "the {bucket} corpus walks in decoded order: no order disagrees"
            );
            for delimiter in [None, Some("/")] {
                for limit in [1, 2, 3] {
                    for seed in [None, sorted.get(sorted.len() / 2).map(String::as_str)] {
                        let (expected_objects, expected_prefixes) =
                            expected_listed(keys, prefix, delimiter, seed);
                        for (name, walked) in [
                            (
                                "binding",
                                walk_binding(bucket, prefix, delimiter, limit, seed).await,
                            ),
                            (
                                "hint",
                                walk_hint(bucket, prefix, delimiter, limit, seed).await,
                            ),
                            (
                                "page",
                                walk_page(bucket, prefix, delimiter, limit, seed).await,
                            ),
                        ] {
                            let mut objects = walked.0;
                            objects.sort_unstable();
                            assert_eq!(
                                objects, expected_objects,
                                "{name} walk over {bucket} (delimiter {delimiter:?}, limit {limit}, seed {seed:?}): keys skipped or duplicated"
                            );
                            // One folder per rolled-up prefix: keys under a
                            // prefix sort contiguously, so a reported folder
                            // is always fully consumed in its page.
                            let mut prefixes = walked.1;
                            prefixes.sort_unstable();
                            assert_eq!(
                                prefixes, expected_prefixes,
                                "{name} walk over {bucket} (delimiter {delimiter:?}, limit {limit}, seed {seed:?}): prefixes skipped or duplicated"
                            );
                        }
                    }
                }
            }
        }
    }

    /// A delimiter page's trailing prefix resumes past the folder: the
    /// next page lists what follows it, not the folder again. The row's
    /// cursor is the last key that rolled into it, not the prefix string,
    /// which encodes ahead of the keys it stands for.
    #[tokio::test]
    async fn delimiter_hint_passes_its_prefix() {
        for key in ["a/1", "a/2", "b/1", "top.txt"] {
            put_plain("hintprefix", key).await;
        }
        let page = fetch_text_page("hintprefix", "", Some("/"), &Resume::From(None), None, 2)
            .await
            .expect("list the page");
        let rows: Vec<String> = page
            .rows
            .iter()
            .map(|row| row.text().into_owned())
            .collect();
        assert_eq!(rows, ["a/", "b/"]);
        assert!(page.next.is_some());
        let hint = page
            .rows
            .last()
            .map(Resumable::cursor)
            .expect("a row to resume after")
            .to_string();
        assert_eq!(hint, "b/1");
        let resumed = fetch_text_page(
            "hintprefix",
            "",
            Some("/"),
            &Resume::From(None),
            Some(hint),
            100,
        )
        .await
        .expect("list the next page");
        let rows: Vec<String> = resumed
            .rows
            .iter()
            .map(|row| row.text().into_owned())
            .collect();
        assert_eq!(rows, ["top.txt"], "the folder answered twice");
    }

    /// When `--cursor` and `--after` meet, the cursor wins, as in a
    /// Worker's `list()`: the seed continues the walk it came from, and
    /// the bound only starts one.
    #[tokio::test]
    async fn cursor_seed_wins_over_after() {
        for key in ["a", "b", "c"] {
            put_plain("cursorwins", key).await;
        }
        let rows = |page: &Page<Row>| {
            page.rows
                .iter()
                .map(|row| row.text().into_owned())
                .collect::<Vec<_>>()
        };
        let from_cursor = fetch_text_page(
            "cursorwins",
            "",
            None,
            &Resume::From(None),
            Some("a".to_string()),
            100,
        )
        .await
        .expect("list the page");
        assert_eq!(rows(&from_cursor), ["b", "c"]);
        let both = fetch_text_page(
            "cursorwins",
            "",
            None,
            &Resume::From(Some("b".to_string())),
            Some("a".to_string()),
            100,
        )
        .await
        .expect("list the page");
        assert_eq!(rows(&both), ["b", "c"]);
        let bound = fetch_text_page(
            "cursorwins",
            "",
            None,
            &Resume::From(Some("b".to_string())),
            None,
            100,
        )
        .await
        .expect("list the page");
        assert_eq!(rows(&bound), ["c"]);
    }

    /// A complete `--page` omits `cursor`, as a Worker's `list()` does:
    /// callers test `truncated` before reading it.
    #[tokio::test]
    async fn page_omits_cursor_when_complete() {
        for key in ["a", "b"] {
            put_plain("pagecursor", key).await;
        }
        let bounds = Bounds::default();
        let page = list_json("pagecursor", "", None, &bounds, None)
            .await
            .expect("list the page");
        assert_eq!(page.get("truncated"), Some(&json!(false)));
        assert!(
            page.get("cursor").is_none(),
            "a complete page names no cursor: {page}"
        );
    }

    /// A `--page` object carries the `customMetadata` a Worker's `list()`
    /// returns when the caller asked for it, as it carries `httpMetadata`.
    #[tokio::test]
    async fn page_objects_carry_custom_metadata() {
        crate::host_channels::set_r2_store(crate::bucket::Bucket::for_test());
        let request = serde_json::from_value(serde_json::json!({
            "http": {},
            "custom": { "team": "field" },
        }))
        .expect("parse the put options");
        crate::r2_store::put_once(
            "pagecustom".to_string(),
            "report.txt".to_string(),
            b"body".to_vec(),
            request,
        )
        .await
        .expect("put the test object");
        let bounds = Bounds::default();
        let page = list_json("pagecustom", "", None, &bounds, None)
            .await
            .expect("list the page");
        let objects = page
            .get("objects")
            .and_then(Value::as_array)
            .expect("the page carries objects");
        assert_eq!(objects.len(), 1);
        assert_eq!(
            objects[0].get("customMetadata"),
            Some(&json!({ "team": "field" })),
            "the page carries customMetadata: {page}"
        );
    }

    /// `--page` already prints one JSON page object, so `--json` beside
    /// it is refused rather than silently meaning nothing.
    #[test]
    fn page_refuses_json() {
        assert!(parse(&["list", "bucket", "--page", "--json"]).is_err());
    }

    /// A truncated listing continues with the cursor it answered with,
    /// past any order the keys sort in.
    #[test]
    fn list_takes_a_cursor() {
        let command = parse(&["list", "bucket", "--cursor", "a/2"])
            .expect("parse the listing")
            .expect("a verb was given");
        assert!(
            matches!(command.action, Action::List { cursor: Some(cursor), .. } if cursor == "a/2")
        );
    }

    #[test]
    fn other_verbs_refuse_a_cursor() {
        for verb in ["get", "head", "put", "delete"] {
            let arguments: Vec<&str> = match verb {
                "put" => vec![verb, "bucket", "key", "--cursor", "a/2", "--pipe"],
                "get" | "head" => vec![verb, "bucket", "key", "--cursor", "a/2"],
                _ => vec![verb, "bucket", "key", "--cursor", "a/2"],
            };
            assert!(parse(&arguments).is_err(), "{verb} accepted --cursor");
        }
    }

    /// The machine-readable absent-key answer: `--json` readers match the
    /// `error` field instead of substring-matching the English message.
    #[test]
    fn not_found_error_names_the_bucket_and_key() {
        assert_eq!(
            not_found_error("photos", "a/1"),
            json!({
                "error": "not_found",
                "bucket": "photos",
                "key": "a/1",
            })
        );
    }

    /// An absent key fails with the message it always had, with or without
    /// `--json` — the error object is additive signal, not a new failure.
    #[test]
    fn absent_key_keeps_the_human_message() {
        for json in [false, true] {
            let error = match absent_key("photos", "a/1", json) {
                Ok(()) => panic!("an absent key must fail (json: {json})"),
                Err(error) => error,
            };
            assert_eq!(
                format!("{error:#}"),
                "no object \"a/1\" in r2 bucket \"photos\""
            );
        }
    }
}
