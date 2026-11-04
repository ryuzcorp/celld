// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified by ryuzcorp (ryuzcorp/celld): d1 export; d1 execute --batch; deploy reads stay inside the project.

// The D1 CLI reads operator input and migration files outside node storage.
#![allow(clippy::disallowed_methods)]
// Reaching stdout past `Output` is what mixed the result rows and the
// summary onto one stream here in the first place.

//! `celld d1` — run SQL and migrations against a deployed D1 database.
//!
//! A D1 database is a cell, so it is only reachable through the node that owns
//! it. This command finds the fleet the way `celld diagnose` does — it walks
//! the node leases in the bucket, and it reads the same shared secret — and
//! then sends the SQL to a live node's `/runtime/` route, which forwards to the
//! owner. The CLI therefore holds no ownership logic and no SQLite: it reaches
//! the database over the dispatch a Worker's `env.DB` reaches it over.
//!
//! The route is authenticated. A D1 database holds application data and
//! answers arbitrary SQL, and its scope is an HMAC over the database identity
//! rather than a secret, so the unauthenticated `/do/` route
//! refuses a D1 scope and sends the caller here.

use std::path::PathBuf;

use anyhow::{anyhow, bail, Context};
use serde_json::Value;

use crate::cli_options::FleetFlags;
use crate::cli_options::FLEET_HELP;
use crate::cli_output::{align, Format, Output, Record};
use crate::note;
use crate::operator_cell::{Fleet, Reachable, Subject};
use std::borrow::Cow;

/// One row of a `d1 execute` result set.
///
/// The text is rendered when the set is laid out, because a column's width
/// depends on every row in it. The JSON keeps the column names as keys, so
/// `--json` output is self-describing without the header line.
struct Row {
    object: Value,
    text: String,
}

impl Record for Row {
    fn json(&self) -> Value {
        self.object.clone()
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.text)
    }
}

/// One migration, listed rather than applied.
struct Pending {
    name: String,
}

impl Record for Pending {
    fn json(&self) -> Value {
        serde_json::json!({ "migration": self.name })
    }

    fn text(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }
}

/// A SQL value as a table cell. JSON keeps the real type; this is only the
/// human rendering, so a string prints bare and a null prints as nothing.
fn cell(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// A duration as an operator reads it.
fn took(milliseconds: f64) -> String {
    // "0.00 sec" reads as a missing measurement rather than a fast
    // statement, the same way "0.0s" did on a listing summary.
    if milliseconds < 1000.0 {
        format!("{}ms", milliseconds.round() as u64)
    } else {
        format!("{:.2} sec", milliseconds / 1000.0)
    }
}

/// One batch statement: exactly one prepared statement with its binds, as
/// D1's `batch()` takes them.
#[derive(Debug)]
struct BatchStatement {
    sql: String,
    params: Vec<Value>,
}

/// Bytes one batch may carry on stdin. The batch crosses to the owner as
/// one request, and the request body the route accepts reaches far past
/// this; the stdin cap exists so a redirected garbage file fails fast
/// instead of swelling the command.
const BATCH_STDIN_MAX_BYTES: usize = 32 * 1024 * 1024;

fn read_batch_stdin() -> anyhow::Result<String> {
    use std::io::Read as _;
    let mut input = String::new();
    std::io::stdin()
        .lock()
        .take((BATCH_STDIN_MAX_BYTES + 1) as u64)
        .read_to_string(&mut input)
        .context("read the batch from stdin")?;
    if input.len() > BATCH_STDIN_MAX_BYTES {
        bail!("the batch on stdin exceeds {BATCH_STDIN_MAX_BYTES} bytes");
    }
    Ok(input)
}

/// The stdin document as statements: a JSON array of `{"sql", "params"}`.
/// Structural only -- unknown keys are ignored and bind values are the
/// engine's to validate -- but every refusal names its statement, because
/// a bare "invalid batch" against fifty statements helps nobody.
fn parse_batch_input(input: &str) -> anyhow::Result<Vec<BatchStatement>> {
    let value: Value = serde_json::from_str(input).context("parse the batch on stdin as JSON")?;
    let items = value.as_array().ok_or_else(|| {
        anyhow!("the batch on stdin must be a JSON array of {{\"sql\", \"params\"}}")
    })?;
    let mut statements = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        let object = item.as_object().ok_or_else(|| {
            anyhow!(
                "batch statement {index} must be an object with \"sql\" and optional \"params\""
            )
        })?;
        let sql = object
            .get("sql")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("batch statement {index} needs a \"sql\" string"))?;
        let params = match object.get("params") {
            None => Vec::new(),
            Some(params) => params
                .as_array()
                .ok_or_else(|| anyhow!("batch statement {index} needs \"params\" to be an array"))?
                .to_vec(),
        };
        statements.push(BatchStatement {
            sql: sql.to_string(),
            params,
        });
    }
    Ok(statements)
}

/// One result per statement, shaped as the Worker's `batch()` shapes them,
/// so a consumer parses one document one way. Returns the shaped results
/// and the summed per-statement durations for the summary line.
fn shape_batch_results(answers: &[Value]) -> (Vec<Value>, f64) {
    let mut shaped = Vec::with_capacity(answers.len());
    let mut milliseconds = 0.0;
    for answer in answers {
        let columns: Vec<&str> = answer
            .get("columns")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        let rows = answer
            .get("rows")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let results: Vec<Value> = rows
            .into_iter()
            .map(|row| {
                let cells = row.as_array().cloned().unwrap_or_default();
                Value::Object(
                    columns
                        .iter()
                        .cloned()
                        .zip(cells)
                        .map(|(name, value)| (name.to_string(), value))
                        .collect(),
                )
            })
            .collect();
        milliseconds += answer
            .get("meta")
            .and_then(|meta| meta.get("duration"))
            .and_then(Value::as_f64)
            .unwrap_or_default();
        shaped.push(serde_json::json!({
            "success": true,
            "meta": answer.get("meta").cloned().unwrap_or(Value::Null),
            "results": results,
        }));
    }
    (shaped, milliseconds)
}

pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(command) = Command::parse(arguments)? else {
        print_help();
        return Ok(());
    };
    let project = crate::deploy::read_d1_project(command.project.clone())?;
    // The declaration, not just the name: `migrations_dir` belongs to the
    // binding, so the directory has to come from the database the command
    // names or one database's migrations reach another.
    let declaration = project
        .databases
        .iter()
        .find(|database| database.database_name == command.database)
        .ok_or_else(|| {
            anyhow!(
                "the project declares no D1 database named {:?}; it declares {}",
                command.database,
                if project.databases.is_empty() {
                    "none".to_string()
                } else {
                    project
                        .databases
                        .iter()
                        .map(|database| format!("{:?}", database.database_name))
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            )
        })?;
    let scope = crate::js::d1_cell_scope(&declaration.database_identity);
    let storage = command.fleet.clone().resolve("celld d1")?;
    let database = Reachable::open(
        Fleet {
            bucket: &storage.bucket,
            endpoint: storage.endpoint.as_deref(),
            region: &storage.region,
            unsafe_public_advertise: command.unsafe_public_advertise,
        },
        Subject {
            noun: "database",
            source: "d1",
            // A migration on a large table is not a diagnostic ping.
            timeout: std::time::Duration::from_secs(120),
        },
        scope,
        None,
    )
    .await?;

    let mut out = Output::new(if command.json {
        Format::Json
    } else {
        Format::Text
    });
    match command.action {
        Action::Execute { command: sql, file } => {
            let source = match (sql, file) {
                (Some(sql), _) => sql,
                (None, Some(file)) => std::fs::read_to_string(&file)
                    .with_context(|| format!("read {}", file.display()))?,
                (None, None) => unreachable!("parse refuses neither --command nor --file"),
            };
            let result = database.exec(&source).await?;
            // The rows, not only the count: an operator running a SELECT is
            // asking for its result, and `wrangler d1 execute` prints it.
            for (index, set) in result
                .get("results")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .enumerate()
            {
                let columns: Vec<String> = set
                    .get("columns")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect();
                let values: Vec<Vec<Value>> = set
                    .get("rows")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_array)
                    .map(|row| row.to_vec())
                    .collect();
                let cells: Vec<Vec<String>> = values
                    .iter()
                    .map(|row| row.iter().map(cell).collect())
                    .collect();
                let (header, rule, lines) = align(&columns, &cells);
                // A blank line between result sets, so several statements
                // read as several tables rather than one ragged one.
                if index > 0 {
                    out.header("")?;
                }
                out.header(&header)?;
                out.header(&rule)?;
                for (row, text) in values.iter().zip(lines) {
                    let object =
                        Value::Object(columns.iter().cloned().zip(row.iter().cloned()).collect());
                    out.row(&Row { object, text })?;
                }
            }
            // stderr, not stdout. This line used to follow the rows on the
            // same stream, so `celld d1 execute | jq` parsed prose.
            let count = result
                .get("count")
                .and_then(Value::as_u64)
                .unwrap_or_default();
            let milliseconds = result
                .get("duration")
                .and_then(Value::as_f64)
                .unwrap_or_default();
            note!("Executed {count} statement(s) in {}", took(milliseconds));
        }
        Action::Batch => {
            let statements = parse_batch_input(&read_batch_stdin()?)?;
            let answers = database.batch(&statements).await?;
            let answers = answers
                .as_array()
                .ok_or_else(|| anyhow!("the database reply carried no batch results"))?;
            let (shaped, milliseconds) = shape_batch_results(answers);
            note!(
                "Ran {} statement(s) in {}",
                shaped.len(),
                took(milliseconds)
            );
            return out.line(format_args!("{}", Value::Array(shaped)));
        }
        Action::Export {
            output,
            tables,
            no_schema,
            no_data,
        } => {
            let result = database.export(&tables, no_schema, no_data).await?;
            let sql = result
                .get("sql")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("the database reply carried no dump"))?;
            let count = result
                .get("tables")
                .and_then(Value::as_array)
                .map(Vec::len)
                .unwrap_or_default();
            match output {
                Some(path) => {
                    std::fs::write(&path, sql)
                        .with_context(|| format!("write {}", path.display()))?;
                    note!(
                        "Exported {count} table(s), {} bytes, to {}",
                        sql.len(),
                        path.display()
                    );
                }
                // One opaque payload, like every other stdout answer: the
                // summary above stays on stderr so the dump pipes cleanly.
                None => return out.bytes(sql.as_bytes()),
            }
        }
        Action::MigrationsList => {
            let pending = database.pending(declaration).await?;
            if pending.is_empty() {
                note!("No migrations to apply.");
                return out.finish();
            }
            note!("Migrations to be applied:");
            for migration in &pending {
                out.row(&Pending {
                    name: migration.name.clone(),
                })?;
            }
        }
        Action::MigrationsApply => {
            let pending = database.pending(declaration).await?;
            if pending.is_empty() {
                note!("No migrations to apply.");
                return out.finish();
            }
            for migration in pending {
                database
                    .apply(&migration, &declaration.migrations_table)
                    .await?;
                // Applying is an action, not data: its progress belongs on
                // stderr so a redirect of the listing stays parseable.
                note!("Applied {}", migration.name);
            }
        }
    }
    out.finish()
}

struct Migration {
    name: String,
    sql: String,
}

/// One entrance into the fleet: a node's address and the identity a request
/// to it must be signed for.
impl Reachable {
    async fn exec(&self, source: &str) -> anyhow::Result<Value> {
        // `rows: true` is the CLI's difference from the Worker binding's
        // exec(): the operator asked to see the result, not only the count.
        self.call(serde_json::json!({ "exec": { "sql": source, "rows": true } }))
            .await
    }

    /// The database as SQL, read under one transaction on the owner: schema
    /// beside data in creation order, a single snapshot even while the
    /// database takes writes.
    async fn export(
        &self,
        tables: &[String],
        no_schema: bool,
        no_data: bool,
    ) -> anyhow::Result<Value> {
        self.call(serde_json::json!({
            "export": { "tables": tables, "no_schema": no_schema, "no_data": no_data },
        }))
        .await
    }

    /// One transactional batch: the engine runs every prepared statement
    /// inside one transaction and answers one result per statement, as D1's
    /// `batch()` does. A consumer issuing many reads pays one round trip
    /// instead of one subprocess per query.
    async fn batch(&self, statements: &[BatchStatement]) -> anyhow::Result<Value> {
        self.call(serde_json::json!({
            "batch": {
                "statements": statements.iter().map(|statement| {
                    serde_json::json!({ "sql": statement.sql, "params": statement.params })
                }).collect::<Vec<_>>(),
            },
        }))
        .await
    }

    /// Rows for one statement, as `all()` would shape them.
    async fn query(&self, sql: &str) -> anyhow::Result<Vec<Vec<Value>>> {
        let result = self
            .call(serde_json::json!({ "statements": [{ "sql": sql, "params": [] }] }))
            .await?;
        let rows = result
            .get(0)
            .and_then(|first| first.get("rows"))
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("the database reply carried no rows"))?;
        Ok(rows
            .iter()
            .filter_map(|row| row.as_array().cloned())
            .collect())
    }

    /// Every migration file the database has not recorded, in wrangler's
    /// order. The bookkeeping table is the binding's `migrations_table`,
    /// validated at config read, so the name joined into this SQL is a plain
    /// identifier and nothing else.
    async fn pending(
        &self,
        declaration: &crate::deploy::D1Declaration,
    ) -> anyhow::Result<Vec<Migration>> {
        let on_disk = read_migrations(&declaration.migrations_dir)?;
        let table = &declaration.migrations_table;
        self.exec(&format!(
            "CREATE TABLE IF NOT EXISTS \"{table}\" (\
               id INTEGER PRIMARY KEY AUTOINCREMENT, \
               name TEXT UNIQUE, \
               applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP);"
        ))
        .await?;
        let applied = self
            .query(&format!("SELECT name FROM \"{table}\""))
            .await?
            .into_iter()
            .filter_map(|row| row.first().and_then(Value::as_str).map(str::to_string))
            .collect::<std::collections::BTreeSet<_>>();
        Ok(on_disk
            .into_iter()
            .filter(|migration| !applied.contains(&migration.name))
            .collect())
    }

    /// The cell applies the file and records it in one transaction, so a
    /// migration that fails part-way rolls back whole and a re-run starts
    /// clean. The CLI sends the file and the name separately and joins no SQL:
    /// the name reaches the bookkeeping row as a bound parameter, and a file
    /// that ends in a comment or an unterminated statement is refused by the
    /// cell rather than repaired by a guess here.
    async fn apply(&self, migration: &Migration, table: &str) -> anyhow::Result<()> {
        self.call(serde_json::json!({
            "migrate": { "name": migration.name, "sql": migration.sql, "table": table },
        }))
        .await
        .with_context(|| format!("apply {}", migration.name))?;
        Ok(())
    }
}

fn has_sql_extension(path: &std::path::Path) -> bool {
    path.extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|extension| extension.eq_ignore_ascii_case("sql"))
}

fn read_migrations(directory: &std::path::Path) -> anyhow::Result<Vec<Migration>> {
    if !directory.exists() {
        bail!(
            "no migrations directory at {}; create it, or set `migrations_dir` \
             on the d1_databases entry",
            directory.display()
        );
    }
    // The declaration's directory is contained when it is parsed, but the
    // read must not follow a link wherever the path came from.
    if std::fs::symlink_metadata(directory).is_ok_and(|metadata| metadata.file_type().is_symlink())
    {
        bail!(
            "migrations directory {} is a symbolic link; point `migrations_dir` \
             at a regular directory",
            directory.display()
        );
    }
    let mut migrations = Vec::new();
    for entry in
        std::fs::read_dir(directory).with_context(|| format!("read {}", directory.display()))?
    {
        let entry = entry?;
        // Never follow a link out of the migrations directory: the entry is
        // inspected without following, like the asset and wasm walks.
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_symlink() {
            bail!(
                "migration {} is a symbolic link; vendor migrations as regular files",
                path.display()
            );
        }
        // A directory is never a migration, whatever it is named — but a
        // directory HOLDING migrations is a layout this command does not
        // read, and skipping it silently would apply half a schema.
        if file_type.is_dir() {
            let nested = std::fs::read_dir(&path)
                .map(|entries| {
                    entries
                        .flatten()
                        .any(|entry| has_sql_extension(&entry.path()))
                })
                .unwrap_or(false);
            if nested {
                bail!(
                    "{} holds .sql files, and celld reads migrations from a flat \
                     directory only; move them into {} itself",
                    path.display(),
                    directory.display()
                );
            }
            continue;
        }
        if !has_sql_extension(&path) {
            continue;
        }
        // A FIFO or device here would block the command or read nonsense;
        // links are refused above, so only a regular file is left to read.
        if !file_type.is_file() {
            bail!("migration {} is not a regular file", path.display());
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| anyhow!("migration {} has a non-UTF-8 name", path.display()))?
            .to_string();
        let sql =
            std::fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))?;
        migrations.push(Migration { name, sql });
    }
    // Wrangler orders migrations by their numeric prefix, not by the file
    // name as text: `10_` runs after `9_`, where a text sort runs it first
    // and the ALTERs land before the CREATE they depend on. A file without
    // the prefix has no place in that order, so it is refused rather than
    // guessed about.
    let mut ordered = Vec::with_capacity(migrations.len());
    for migration in migrations {
        let digits: String = migration
            .name
            .chars()
            .take_while(char::is_ascii_digit)
            .collect();
        if digits.is_empty() {
            bail!(
                "migration {:?} has no numeric prefix; wrangler names migrations \
                 NNNN_description.sql, and the prefix is the application order",
                migration.name
            );
        }
        // A parse failure past this point is overflow, not absence: 20 or
        // more digits do not fit a u64. Reporting that as "no numeric
        // prefix" sent the operator to rename a file that plainly starts
        // with digits.
        let number: u64 = digits.parse().map_err(|_| {
            anyhow!(
                "migration {:?} has a numeric prefix that does not fit in an \
                 unsigned 64-bit number; use a shorter prefix",
                migration.name
            )
        })?;
        ordered.push((number, migration));
    }
    ordered.sort_by(|left, right| (left.0, &left.1.name).cmp(&(right.0, &right.1.name)));
    Ok(ordered
        .into_iter()
        .map(|(_, migration)| migration)
        .collect())
}

/// Every node with an unexpired lease and a usable address, as (id, address).
/// The id is not decoration: a peer signature binds the node it is addressed
/// to. Any of them will do, because the route forwards to the owner, so the
/// CLI does not resolve ownership and cannot race a cell that moves while it
/// works. They are all returned rather than only the first, because a lease
/// outlives the node that wrote it and a draining node keeps its lease while
/// it refuses work.
enum Action {
    /// Exactly one of command, file, or batch, enforced at parse.
    Execute {
        command: Option<String>,
        file: Option<PathBuf>,
    },
    /// A SQL dump of the live database. `tables` empty means every table;
    /// `output` missing means stdout.
    Export {
        output: Option<PathBuf>,
        tables: Vec<String>,
        no_schema: bool,
        no_data: bool,
    },
    /// A JSON array of statements from stdin, run as one transactional
    /// batch, answered as one JSON array. Always JSON; there is no table
    /// rendering for per-statement results.
    Batch,
    MigrationsList,
    MigrationsApply,
}

struct Command {
    action: Action,
    database: String,
    project: Option<PathBuf>,
    fleet: FleetFlags,
    json: bool,
    unsafe_public_advertise: bool,
}

impl Command {
    fn parse(arguments: Vec<String>) -> anyhow::Result<Option<Self>> {
        let mut arguments = arguments.into_iter().peekable();
        let Some(first) = arguments.next() else {
            return Ok(None);
        };
        let mut is_export = false;
        let action = match first.as_str() {
            "--help" | "-h" | "help" => return Ok(None),
            "execute" => None,
            "export" => {
                is_export = true;
                None
            }
            // An option in the subcommand slot means the subcommand is
            // missing, not that the option names one. Reporting `--bucket` as
            // an unknown subcommand would send an operator looking for a
            // spelling mistake they did not make.
            "migrations" => match arguments.next().as_deref() {
                Some("apply") => Some(Action::MigrationsApply),
                Some("list") => Some(Action::MigrationsList),
                Some(other) if !other.starts_with('-') => {
                    bail!("unknown `celld d1 migrations` subcommand: {other}")
                }
                _ => bail!("`celld d1 migrations` needs `apply` or `list`"),
            },
            other => bail!("unknown `celld d1` subcommand: {other}"),
        };
        let database = arguments
            .next()
            .filter(|value| !value.starts_with('-'))
            .ok_or_else(|| anyhow!("celld d1 needs a database name"))?;

        let mut sql = None;
        let mut file = None;
        let mut output = None;
        let mut tables = Vec::new();
        let mut no_schema = false;
        let mut no_data = false;
        let mut batch = false;
        let mut project = None;
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
                "--command" => sql = Some(value("--command")?),
                "--file" => file = Some(PathBuf::from(value("--file")?)),
                "--output" => output = Some(PathBuf::from(value("--output")?)),
                "--table" => tables.push(value("--table")?),
                "--no-schema" => no_schema = true,
                "--no-data" => no_data = true,
                "--batch" => batch = true,
                "--json" => json = true,
                "--unsafe-public-advertise" => unsafe_public_advertise = true,
                "--help" | "-h" => return Ok(None),
                other => {
                    // The fleet flags are shared, so `--bucket gs://name`
                    // resolves here exactly as it does for every other
                    // command rather than through a second copy of the rule.
                    if fleet.consume(other, &mut value)? {
                        continue;
                    }
                    if other.starts_with('-') {
                        bail!("unknown option: {other}; run `celld d1 --help` for usage")
                    }
                    project = Some(PathBuf::from(other));
                }
            }
        }
        let export_options = output.is_some() || !tables.is_empty() || no_schema || no_data;
        let action = match action {
            Some(action) => {
                // Accepting the flag and ignoring it would tell an operator
                // their SQL ran when nothing read it.
                if sql.is_some() || file.is_some() || batch || export_options {
                    bail!(
                        "`celld d1 migrations` takes no --command, --file, --batch, or export options; \
                         migrations come from the migrations directory"
                    );
                }
                action
            }
            None if is_export => {
                if sql.is_some() || file.is_some() {
                    bail!("`celld d1 export` takes no --command or --file; it dumps the live database");
                }
                if batch {
                    bail!("`celld d1 export` takes no --batch; it dumps the live database");
                }
                if json {
                    bail!("`celld d1 export` writes SQL text, so --json does not apply");
                }
                if no_schema && no_data {
                    bail!("`celld d1 export` with --no-schema and --no-data selects nothing");
                }
                Action::Export {
                    output,
                    tables,
                    no_schema,
                    no_data,
                }
            }
            None => {
                if export_options {
                    bail!(
                        "--output, --table, --no-schema, and --no-data belong to `celld d1 export`"
                    );
                }
                if batch {
                    if sql.is_some() || file.is_some() {
                        bail!(
                            "`celld d1 execute --batch` reads its statements from stdin, so it \
                             takes no --command or --file"
                        );
                    }
                    if !json {
                        bail!(
                            "`celld d1 execute --batch` requires --json: per-statement results \
                             are structured"
                        );
                    }
                    Action::Batch
                } else {
                    if sql.is_some() == file.is_some() {
                        bail!(
                            "celld d1 execute requires exactly one of --command, --file, or --batch"
                        );
                    }
                    Action::Execute { command: sql, file }
                }
            }
        };
        Ok(Some(Self {
            action,
            database,
            project,
            // Resolution is deferred to `run`, so the missing-bucket message
            // is the one every command shares.
            fleet: fleet.with_environment(),
            json,
            unsafe_public_advertise,
        }))
    }
}

pub fn print_help() {
    let text = format!(
        r#"celld d1 — run SQL, migrations, and exports against a deployed D1 database

USAGE:
  celld d1 execute DATABASE (--command SQL | --file PATH | --batch) [PROJECT] --bucket NAME
  celld d1 export DATABASE [--output PATH] [--table NAME]... [PROJECT] --bucket NAME
  celld d1 migrations apply DATABASE [PROJECT] --bucket NAME
  celld d1 migrations list  DATABASE [PROJECT] --bucket NAME

DATABASE is a `database_name` the project declares in `d1_databases`. PROJECT
is the directory holding wrangler.jsonc, and defaults to the working directory.
The `database_id` is the stable database identity when the declaration has one.
Otherwise, celld uses the `database_name` as the identity.

celld d1 needs a running fleet. It finds a node through the bucket's node
leases and sends the SQL to that node, which forwards it to the node that owns
the database. It signs each request with the fleet's shared secret, which it
reads from the bucket, so it needs the same bucket credentials celld deploy
needs.

Migration files are `NNNN_description.sql` in `migrations/`. The `sql`
extension is ASCII case-insensitive. celld applies the files in the order of
their numeric prefixes as wrangler applies them. `migrations_dir` on the
d1_databases entry moves the directory, and `migrations_table` renames the
bookkeeping table. celld records applied migrations in that table
(`d1_migrations` by default), the same table and columns
`wrangler d1 migrations` uses.

`execute` prints a table, and every message goes to stderr, so its stdout
carries only result rows. Pass --json for one JSON object per row.

`execute --batch` reads a JSON array of {{"sql", "params"}} from stdin and
runs it as one transactional batch -- D1's `batch()` semantics, one result
per statement -- printing one JSON array; it requires --json. A caller
issuing many reads pays one round trip instead of one subprocess per query.

`export` writes the database as SQL — schema beside data in creation order —
read under one transaction on the owning node, so the dump is a single
snapshot even while the database takes writes. It prints to stdout, or to
--output. Import a dump with `celld d1 execute DATABASE --file dump.sql`,
which replays it in one transaction, as `wrangler d1 execute --file` does.

OPTIONS:
  --command SQL       SQL to run; several statements are permitted
  --file PATH         Read the SQL from a file instead
  --batch             Read a JSON array of statements from stdin and run it
                      as one transactional batch (requires --json)
  --json              Print one JSON object per row instead of a table
  --output PATH       Write the export to a file instead of stdout
  --table NAME        Export only this table, its indexes, and its triggers;
                      repeat for several; views are left out of a filtered export
  --no-schema         Export data only, no CREATE statements
  --no-data           Export schema only, no INSERTs
{FLEET_HELP}
  --unsafe-public-advertise
                      Permit a node whose advertised address is a public IP
  -h, --help          Show this help"#
    );
    let _ = Output::new(Format::Text).help(&text);
}

#[cfg(test)]
mod d1_export_cli_tests {
    use super::*;

    fn parse(arguments: &[&str]) -> anyhow::Result<Option<Command>> {
        Command::parse(arguments.iter().map(ToString::to_string).collect())
    }

    #[test]
    fn export_parses_with_all_options() {
        let command = parse(&[
            "export",
            "mydb",
            "--output",
            "dump.sql",
            "--table",
            "a",
            "--table",
            "b",
            "--no-schema",
            "--bucket",
            "s3://fleet",
        ])
        .expect("parse")
        .expect("command");
        assert_eq!(command.database, "mydb");
        assert!(!command.json);
        match command.action {
            Action::Export {
                output,
                tables,
                no_schema,
                no_data,
            } => {
                assert_eq!(output, Some(PathBuf::from("dump.sql")));
                assert_eq!(tables, ["a", "b"].map(str::to_string));
                assert!(no_schema);
                assert!(!no_data);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn export_defaults_to_a_full_dump_on_stdout() {
        let command = parse(&["export", "mydb"]).expect("parse").expect("command");
        match command.action {
            Action::Export {
                output,
                tables,
                no_schema,
                no_data,
            } => {
                assert_eq!(output, None);
                assert!(tables.is_empty());
                assert!(!no_schema);
                assert!(!no_data);
            }
            _ => panic!("wrong action"),
        }
    }

    #[test]
    fn export_and_execute_refuse_each_others_options() {
        for (arguments, fragment) in [
            (
                ["export", "db", "--command", "SELECT 1"].as_slice(),
                "takes no --command or --file",
            ),
            (
                ["export", "db", "--file", "q.sql"].as_slice(),
                "takes no --command or --file",
            ),
            (["export", "db", "--batch"].as_slice(), "takes no --batch"),
            (
                ["export", "db", "--json"].as_slice(),
                "--json does not apply",
            ),
            (
                ["export", "db", "--no-schema", "--no-data"].as_slice(),
                "selects nothing",
            ),
            (
                ["execute", "db", "--command", "SELECT 1", "--table", "a"].as_slice(),
                "belong to `celld d1 export`",
            ),
            (
                ["execute", "db", "--command", "SELECT 1", "--no-data"].as_slice(),
                "belong to `celld d1 export`",
            ),
            (
                ["migrations", "apply", "db", "--table", "a"].as_slice(),
                "or export options",
            ),
        ] {
            let error = match parse(arguments) {
                Err(error) => error.to_string(),
                Ok(_) => panic!("accepted {arguments:?}"),
            };
            assert!(error.contains(fragment), "arguments {arguments:?}: {error}");
        }
    }
}

#[cfg(test)]
mod d1_batch_cli_tests {
    use super::*;

    fn parse(arguments: &[&str]) -> anyhow::Result<Option<Command>> {
        Command::parse(arguments.iter().map(ToString::to_string).collect())
    }

    fn refused(arguments: &[&str]) -> String {
        match parse(arguments) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("accepted {arguments:?}"),
        }
    }

    #[test]
    fn batch_parses_with_json() {
        let command = parse(&["execute", "mydb", "--batch", "--json"])
            .expect("parse")
            .expect("command");
        assert_eq!(command.database, "mydb");
        assert!(command.json);
        assert!(matches!(command.action, Action::Batch));
    }

    #[test]
    fn batch_needs_json_and_no_other_source() {
        for (arguments, fragment) in [
            (["execute", "db", "--batch"].as_slice(), "requires --json"),
            (
                [
                    "execute",
                    "db",
                    "--batch",
                    "--json",
                    "--command",
                    "SELECT 1",
                ]
                .as_slice(),
                "takes no --command or --file",
            ),
            (
                ["execute", "db", "--batch", "--json", "--file", "q.sql"].as_slice(),
                "takes no --command or --file",
            ),
            (
                ["migrations", "apply", "db", "--batch", "--json"].as_slice(),
                "--batch",
            ),
            (
                ["execute", "db"].as_slice(),
                "exactly one of --command, --file, or --batch",
            ),
            (
                ["execute", "db", "--command", "a", "--file", "b"].as_slice(),
                "exactly one of --command, --file, or --batch",
            ),
        ] {
            let error = refused(arguments);
            assert!(error.contains(fragment), "arguments {arguments:?}: {error}");
        }
    }

    #[test]
    fn batch_input_parses_statements_with_and_without_params() {
        let statements = parse_batch_input(
            r#"[
              {"sql": "SELECT * FROM t WHERE id = ?", "params": [1]},
              {"sql": "SELECT COUNT(*) FROM t"}
            ]"#,
        )
        .expect("parse");
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0].sql, "SELECT * FROM t WHERE id = ?");
        assert_eq!(statements[0].params, vec![serde_json::json!(1)]);
        assert_eq!(statements[1].sql, "SELECT COUNT(*) FROM t");
        assert!(statements[1].params.is_empty());
        assert!(parse_batch_input("[]").expect("empty").is_empty());
    }

    #[test]
    fn batch_shaping_maps_rows_to_objects_and_sums_durations() {
        // One statement returning rows, one returning none: every shaped
        // result reports success and carries its statement's meta through,
        // positional rows become column-keyed objects, and the summary
        // duration is the per-statement sum.
        let answers = [
            serde_json::json!({
                "columns": ["id", "name"],
                "rows": [[1, "ada"], [2, "bob"]],
                "meta": {"duration": 12.5, "changes": 0, "rows_read": 2},
            }),
            serde_json::json!({
                "columns": [],
                "rows": [],
                "meta": {"duration": 7.5, "changes": 1, "rows_read": 0},
            }),
        ];
        let (shaped, milliseconds) = shape_batch_results(&answers);
        assert_eq!(shaped.len(), 2);
        assert_eq!(shaped[0]["success"], true);
        assert_eq!(shaped[0]["meta"], answers[0]["meta"]);
        assert_eq!(
            shaped[0]["results"],
            serde_json::json!([{"id": 1, "name": "ada"}, {"id": 2, "name": "bob"}])
        );
        assert_eq!(shaped[1]["success"], true);
        assert_eq!(shaped[1]["meta"], answers[1]["meta"]);
        assert_eq!(shaped[1]["results"], serde_json::json!([]));
        assert_eq!(milliseconds, 20.0);
    }

    #[test]
    fn batch_input_refusals_name_the_statement() {
        for (input, fragment) in [
            ("no json", "parse the batch on stdin as JSON"),
            ("{\"sql\": \"SELECT 1\"}", "must be a JSON array"),
            (
                "[{\"sql\": \"SELECT 1\"}, 7]",
                "batch statement 1 must be an object",
            ),
            (
                "[{\"params\": []}]",
                "batch statement 0 needs a \"sql\" string",
            ),
            (
                "[{\"sql\": \"SELECT ?\", \"params\": 1}]",
                "batch statement 0 needs \"params\" to be an array",
            ),
        ] {
            let error = parse_batch_input(input).expect_err("accepted").to_string();
            assert!(error.contains(fragment), "input {input}: {error}");
        }
    }
}

#[cfg(test)]
mod d1_migrations_tests {
    use super::*;

    #[test]
    fn migrations_do_not_follow_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("evil.sql"), "OUTSIDE\n").unwrap();
        let directory = root.path().join("migrations");
        std::fs::create_dir(&directory).unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("evil.sql"),
            directory.join("0001_evil.sql"),
        )
        .unwrap();
        let error = match read_migrations(&directory) {
            Ok(_) => panic!("a symlinked migration must be refused, not read"),
            Err(error) => error,
        };
        let message = format!("{error:?}");
        assert!(
            message.contains("symbolic link"),
            "the refusal must name the link: {message}"
        );
    }

    #[test]
    fn migrations_do_not_follow_a_linked_directory() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("0001_evil.sql"), "OUTSIDE\n").unwrap();
        let linked = root.path().join("migrations");
        std::os::unix::fs::symlink(outside.path(), &linked).unwrap();
        let error = match read_migrations(&linked) {
            Ok(_) => panic!("a symlinked migrations directory must be refused, not read"),
            Err(error) => error,
        };
        let message = format!("{error:?}");
        assert!(
            message.contains("symbolic link"),
            "the refusal must name the link: {message}"
        );
    }

    #[test]
    fn migrations_still_read_regular_files() {
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("migrations");
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(
            directory.join("0001_init.sql"),
            "CREATE TABLE t (id INTEGER);\n",
        )
        .unwrap();
        let migrations = read_migrations(&directory).unwrap();
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].name, "0001_init.sql");
        assert_eq!(migrations[0].sql, "CREATE TABLE t (id INTEGER);\n");
    }
}
