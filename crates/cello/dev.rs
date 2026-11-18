// Modified by ryuzcorp (ryuzcorp/celld): operator token for the internal listener; `cello dev --port 0` assigns an ephemeral port; staged activation; deploy --read-root option; non-dumpable serving process; local state path helper for d1 --local (upstream #272); dev --state with marker-guarded clean (upstream #302).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified by ryuzcorp (ryuzcorp/celld): deploy --sourcemap writes the bundle's external map.

//! The local application stack behind `cello dev`.
//!
//! Development still uses the standalone deployment, ownership, and LTX
//! paths. This module supplies only the infrastructure those paths require:
//! one persisted local object store and one supervised cello node.

use crate::bucket::Bucket;
use crate::{deploy, fleet};
use anyhow::{bail, Context as _};
use glob::{MatchOptions, Pattern};
use notify::{RecursiveMode, Watcher as _};
use nu_ansi_term::{Color, Style};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeMap;
use std::io::IsTerminal as _;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

pub const DEFAULT_PORT: u16 = 9876;
const BUCKET: &str = "cello-dev";

/// The narrow bridge used by the supervised binary process. The public fleet
/// parser never receives this path, so no regular subcommand can select it.
#[doc(hidden)]
pub fn open_local_bucket(database: &Path) -> anyhow::Result<Bucket> {
    Bucket::open_dev(database)
}

#[derive(Debug)]
struct Options {
    project: Option<PathBuf>,
    clean: bool,
    state: Option<PathBuf>,
    stack: StackOptions,
}

/// The local state directory of one project.
///
/// `--clean` deletes this directory and everything below it. The
/// project-relative path is private and the only constructor takes the
/// project directory that `run` resolved, so a command-line value can
/// never become the target of the delete — except through `--state`,
/// which names the directory outright and is therefore guarded by the
/// marker file instead. A newtype carries that rule instead of a comment,
/// because a `PathBuf` field would let a later caller pass any path and
/// lose nothing at compile time.
#[derive(Debug)]
pub(crate) struct DevState {
    path: PathBuf,
    explicit: bool,
}

impl DevState {
    /// The project-relative location of the local state, and also the suffix
    /// that `discard` requires before it deletes a directory it did not
    /// create.
    const DIRECTORY: &'static str = ".cello/dev";

    /// The marker `prepare_state` writes into every state directory it
    /// creates. `--clean` over an explicit `--state` directory deletes
    /// only when the marker proves cello created it: the directory is
    /// arbitrary, so the `.cello/dev` suffix cannot prove that.
    const MARKER: &'static str = ".cello-dev-state";

    fn for_project(project: &Path) -> Self {
        Self {
            path: project.join(Self::DIRECTORY),
            explicit: false,
        }
    }

    /// An explicit state directory, from `--state` or `CELLO_DEV_STATE`.
    /// Unlike `for_project` this names an arbitrary directory, so only
    /// the marker authorizes a delete — even when the path happens to end
    /// in `.cello/dev`.
    fn for_state(dir: PathBuf) -> Self {
        Self {
            path: dir,
            explicit: true,
        }
    }

    /// The state directory for one project: the `--state` flag wins, then
    /// `CELLO_DEV_STATE`, then `PROJECT/.cello/dev`. Operator commands
    /// (`d1 --local`) take the same `--state` flag and resolve through this
    /// too, so they follow the override.
    pub(crate) fn resolve(project: &Path, flag: Option<PathBuf>) -> Self {
        Self::resolve_with(
            project,
            flag,
            std::env::var_os("CELLO_DEV_STATE").map(PathBuf::from),
        )
    }

    fn resolve_with(project: &Path, flag: Option<PathBuf>, env: Option<PathBuf>) -> Self {
        // An empty or whitespace-only flag or variable is unset, not a
        // directory — mirroring the fleet resolver's `trim().is_empty()`
        // check, so `CELLO_DEV_STATE=" "` cannot adopt a directory named
        // `" "`.
        let is_unset = |dir: &PathBuf| dir.to_string_lossy().trim().is_empty();
        match flag
            .filter(|dir| !is_unset(dir))
            .or_else(|| env.filter(|dir| !is_unset(dir)))
        {
            Some(dir) => Self::for_state(dir),
            None => Self::for_project(project),
        }
    }

    /// The SQLite file behind one project's local state, for operator
    /// commands that reach the `cello dev` node through it (`d1 --local`).
    /// Pure path math: the caller decides what a missing file means.
    /// `state` is the command's `--state` flag, so the operator command
    /// resolves exactly like `cello dev`: the flag, then `CELLO_DEV_STATE`,
    /// then `PROJECT/.cello/dev`. `environment` is the lookup
    /// `CELLO_DEV_STATE` resolves through, so tests pass an empty one
    /// instead of inheriting the developer's shell.
    pub(crate) fn database_for_project(
        project: &Path,
        state: Option<PathBuf>,
        environment: impl Fn(&str) -> Option<std::ffi::OsString>,
    ) -> PathBuf {
        Self::resolve_with(
            project,
            state,
            environment("CELLO_DEV_STATE").map(PathBuf::from),
        )
        .path
        .join("objects.sqlite3")
    }

    /// The project directory a Wrangler config belongs to: the parent of the
    /// canonicalized config path, so a symlinked config resolves to the same
    /// project — and the same `.cello/dev` — that `cello dev` runs in. Both
    /// `cello dev` and `d1 --local` resolve through this, so the state
    /// directory never depends on which spelling of the config path named it.
    /// Pure path math beside the canonicalization: the caller decides what a
    /// missing directory means.
    #[allow(clippy::disallowed_methods)] // Project resolution uses the operator's host filesystem.
    pub(crate) fn project_dir(config: &Path) -> anyhow::Result<PathBuf> {
        let config = std::fs::canonicalize(config)
            .with_context(|| format!("resolve Wrangler config {}", config.display()))?;
        config
            .parent()
            .map(Path::to_path_buf)
            .context("the Wrangler config has no project directory")
    }

    fn path(&self) -> &Path {
        &self.path
    }

    /// Only project-relative state proves itself by suffix. An explicit
    /// `--state` path never does, even when it happens to end in
    /// `.cello/dev`: only the marker authorizes a delete or an adoption
    /// there.
    fn is_project_state(&self) -> bool {
        !self.explicit && self.path.ends_with(Self::DIRECTORY)
    }

    /// Delete the local state so the next run starts from nothing. The answer
    /// reports whether a directory was present, because the console line for
    /// an empty project must not claim that data was removed.
    ///
    /// An absent directory is the wanted result of this command, so it is not
    /// an error.
    #[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
    fn discard(&self) -> anyhow::Result<bool> {
        // A project-relative state directory proves itself by suffix. An
        // explicit `--state` directory is arbitrary, so only the marker
        // that `prepare_state` writes proves cello created it: `--clean`
        // must never delete a directory the operator pointed at by
        // mistake. The check stays a refusal rather than a fallback,
        // because a recursive delete must never widen by accident.
        if !self.is_project_state() && !self.marker().exists() {
            if !self.path.exists() {
                return Ok(false);
            }
            anyhow::bail!(
                "refusing to discard {}, which is not a cello dev state directory",
                self.path.display()
            );
        }
        match std::fs::remove_dir_all(&self.path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error)
                .with_context(|| format!("discard local state directory {}", self.path.display())),
        }
    }

    fn marker(&self) -> PathBuf {
        self.path.join(Self::MARKER)
    }
}

#[derive(Debug)]
struct StackOptions {
    listener: SocketAddr,
    logs: bool,
    watch: bool,
    watch_ignores: Vec<Pattern>,
}

struct Store {
    database: PathBuf,
}

#[derive(Clone, Copy)]
struct Console {
    color: bool,
}

impl Console {
    fn new() -> Self {
        let force = std::env::var_os("FORCE_COLOR").is_some_and(|value| value != "0");
        let color =
            std::env::var_os("NO_COLOR").is_none() && (force || std::io::stdout().is_terminal());
        Self { color }
    }

    fn paint(&self, style: Style, value: &str) -> String {
        if self.color {
            style.paint(value).to_string()
        } else {
            value.to_string()
        }
    }

    fn header(&self) {
        crate::note!("{}", self.paint(Color::Cyan.bold(), "cello dev"));
    }

    fn detail(&self, label: &str, value: &str) {
        crate::note!(
            "  {}  {value}",
            self.paint(Style::new().dimmed(), &format!("{label:<7}"))
        );
    }

    fn progress(&self, message: &str) {
        crate::note!("  {} {message}", self.paint(Color::Yellow.normal(), "●"));
    }

    fn ready(&self, origin: &str) {
        crate::note!(
            "  {}  {}",
            self.paint(Color::Green.bold(), "ready"),
            self.paint(Color::Cyan.bold(), origin)
        );
    }

    fn log(&self, line: &str) {
        let style = if line.contains(" ERROR ") {
            Color::Red.normal()
        } else if line.contains(" WARN ") {
            Color::Yellow.normal()
        } else {
            Style::new().dimmed()
        };
        crate::note!("{}", self.paint(style, line));
    }

    fn failure(&self, message: &str) {
        crate::note!("  {} {message}", self.paint(Color::Red.bold(), "error"));
    }
}

struct ProjectWatcher {
    project: PathBuf,
    state: PathBuf,
    /// The lexical spelling of the state directory, made absolute against
    /// the canonical watch root, when it names the same directory as
    /// `state` through a symlink (e.g. `--state /app/dev-state` where that
    /// link points at `/scratch/state`). notify follows directory symlinks
    /// and can report event paths in the traversed spelling, which misses
    /// the canonical prefix — excluding both spellings keeps state writes
    /// from requesting a rebuild either way.
    state_alias: Option<PathBuf>,
    ignored: Vec<Pattern>,
    _watcher: notify::RecommendedWatcher,
    changes: mpsc::UnboundedReceiver<notify::Result<notify::Event>>,
}

impl ProjectWatcher {
    /// Watch `project` recursively, ignoring the resolved state directory
    /// beside the usual build-output directories. `state` is the
    /// canonicalized state directory the run prepared: deployment writes
    /// land there as SQLite WAL writes, and without the exclusion each of
    /// them requests another rebuild, which writes state again — the
    /// supervisor rebuilds in a loop without a source change.
    ///
    /// The watch root is canonicalized first: event paths carry the
    /// spelling of the watched root, so a project reached through a
    /// symlink would otherwise report uncanonicalized event paths that
    /// miss the canonical state prefix and revive the loop. Watching the
    /// resolved root keeps both sides canonical; sources keep reporting
    /// under it either way.
    ///
    /// `state` is the lexical state path: it is canonicalized here, and
    /// its spelling made absolute against the canonical watch root is
    /// kept as an alias when it names the same directory through a
    /// symlink below the project. A spelling that resolves elsewhere (a
    /// relative `--state` run from another directory) is not the watched
    /// state and gains no exclusion, so sources there stay relevant.
    fn new(project: &Path, state: &Path, ignored: Vec<Pattern>) -> anyhow::Result<Self> {
        #[allow(clippy::disallowed_methods)]
        // Project resolution uses the operator's host filesystem.
        let project = std::fs::canonicalize(project)
            .with_context(|| format!("resolve the project directory {}", project.display()))?;
        #[allow(clippy::disallowed_methods)]
        // Local development state is an operator-owned host path.
        let canonical_state = std::fs::canonicalize(state)
            .with_context(|| format!("resolve the local state directory {}", state.display()))?;
        // `Path::join` with an absolute `state` keeps it; a relative one
        // is made absolute against the watch root. Only a spelling that
        // resolves back to the canonical state becomes an alias.
        let lexical_state = project.join(state);
        #[allow(clippy::disallowed_methods)]
        // Local development state is an operator-owned host path.
        let resolved_lexical = std::fs::canonicalize(&lexical_state);
        let state_alias = match resolved_lexical {
            Ok(resolved) if resolved == canonical_state && lexical_state != canonical_state => {
                Some(lexical_state)
            }
            _ => None,
        };
        let (sender, changes) = mpsc::unbounded_channel();
        let mut watcher = notify::recommended_watcher(move |event| {
            let _ = sender.send(event);
        })
        .context("create the project watcher")?;
        watcher
            .watch(&project, RecursiveMode::Recursive)
            .with_context(|| format!("watch the project directory {}", project.display()))?;
        Ok(Self {
            project,
            state: canonical_state,
            state_alias,
            ignored,
            _watcher: watcher,
            changes,
        })
    }

    #[allow(clippy::disallowed_methods)] // Host-side debounce time is outside the Actor domain.
    async fn changed(&mut self) -> anyhow::Result<()> {
        self.next_relevant_change().await?;
        loop {
            // An editor can replace one save through several writes and a
            // rename. Wait for one quiet interval so esbuild never reads the
            // temporary middle of that transaction.
            match tokio::time::timeout(Duration::from_millis(150), self.next_relevant_change())
                .await
            {
                Ok(result) => result?,
                Err(_) => return Ok(()),
            }
        }
    }

    async fn next_relevant_change(&mut self) -> anyhow::Result<()> {
        loop {
            match self
                .changes
                .recv()
                .await
                .context("the project watcher stopped")?
            {
                Ok(event)
                    if relevant_project_event(
                        &self.project,
                        &self.state,
                        self.state_alias.as_deref(),
                        &event,
                        &self.ignored,
                    ) =>
                {
                    return Ok(())
                }
                Ok(_) => {}
                Err(error) => return Err(error).context("watch the project directory"),
            }
        }
    }
}

fn relevant_project_event(
    project: &Path,
    state: &Path,
    state_alias: Option<&Path>,
    event: &notify::Event,
    ignored: &[Pattern],
) -> bool {
    // A read is not a change. The inotify backend reports `Access(Open(Any))`
    // for every file that the bundler and the deploy step read, so a rebuild
    // produces the events that request the next rebuild and the supervisor
    // never settles. The path filters cannot close this loop, because the
    // files that the rebuild reads are the project sources it must watch. A
    // write still arrives as a `Modify` event, and a `Create` or a `Remove`
    // event keeps its own kind, so no real change becomes invisible.
    if matches!(event.kind, notify::EventKind::Access(_)) {
        return false;
    }
    event
        .paths
        .iter()
        .any(|path| !ignored_project_path(project, state, state_alias, path, ignored))
}

fn ignored_project_path(
    project: &Path,
    state: &Path,
    state_alias: Option<&Path>,
    path: &Path,
    ignored: &[Pattern],
) -> bool {
    // The state directory holds the deployment the rebuild writes: SQLite
    // and WAL writes under an in-project `--state` directory would
    // otherwise request another rebuild, which writes state again, so the
    // supervisor loops without a source change. Both the canonicalized
    // directory and its lexical spelling below the project are excluded:
    // notify follows a symlinked state directory and can report either
    // spelling. An external state directory never shares either prefix
    // and keeps working; the default `.cello/dev` stays ignored through
    // the `.cello` rule below too.
    if !state.as_os_str().is_empty() && path.starts_with(state) {
        return true;
    }
    if state_alias.is_some_and(|alias| !alias.as_os_str().is_empty() && path.starts_with(alias)) {
        return true;
    }
    const IGNORED_DIRECTORIES: &[&str] = &[
        ".cache",
        ".cello",
        ".git",
        ".hg",
        ".next",
        ".nuxt",
        ".parcel-cache",
        ".pnpm-store",
        ".svn",
        ".turbo",
        ".wrangler",
        ".yarn",
        "bower_components",
        "coverage",
        "node_modules",
        "target",
    ];
    let relative = path.strip_prefix(project).unwrap_or(path);
    relative
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(name) => Some(name),
            _ => None,
        })
        .any(|name| IGNORED_DIRECTORIES.iter().any(|ignored| name == *ignored))
        || relative.ancestors().any(|candidate| {
            !candidate.as_os_str().is_empty()
                && ignored.iter().any(|pattern| {
                    let options = MatchOptions {
                        case_sensitive: true,
                        require_literal_separator: true,
                        require_literal_leading_dot: true,
                    };
                    pattern.matches_path_with(candidate, options)
                        // A directory creation event arrives before its child
                        // path. Probe one child so `generated/**` also ignores
                        // the event that creates `generated` itself.
                        || pattern.matches_path_with(
                            &candidate.join("__celld_watch_descendant__"),
                            options,
                        )
                })
        })
}

#[cfg(all(test, cello_internal_tests))]
mod dev_contract {
    include!(env!("CELLO_INTERNAL_DEV_TESTS"));
}

struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn install() -> anyhow::Result<Self> {
        #[cfg(unix)]
        {
            Ok(Self {
                interrupt: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::interrupt(),
                )?,
                terminate: tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate(),
                )?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    async fn received(&mut self) {
        #[cfg(unix)]
        crate::asyncrt::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
        }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub fn print_help() -> anyhow::Result<()> {
    crate::cli_output::Output::new(crate::cli_output::Format::Text).help(
        &format!("cello dev — run an application with persistent local storage\n\n\
USAGE:\n  cello dev [PROJECT] [--host IP] [--port PORT] [--logs] [--no-watch] [--clean] [--state DIR]\n\n\
PROJECT is a directory or a Wrangler config. It defaults to the current\n\
directory. cello stores all local state in PROJECT/.cello/dev — or in DIR\n\
when --state (or CELLO_DEV_STATE) names one, keeping the state outside the\n\
source tree — and it keeps that state across a restart. A configuration\n\
change does not migrate the state, so a cell can keep a value that the new\n\
configuration rejects. Use --clean to start from an empty local state;\n\
over an explicit state directory --clean deletes only what Cello created.\n\n\
A .dev.vars file beside the config supplies Worker variables in dotenv form,\n\
as for wrangler dev. Without one, .env and then .env.local supply them, and\n\
an entry of these files that cannot be a binding is skipped with a warning.\n\
The entries override the vars of the config.\n\n\
OPTIONS:\n  --host IP              Worker listener host (default: 127.0.0.1)\n  --port PORT            Worker listener port (default: {DEFAULT_PORT}; 0 assigns an ephemeral port)\n  --clean                Delete the local state before the server starts\n  --state DIR            Keep local state in DIR instead of PROJECT/.cello/dev\n                         (CELLO_DEV_STATE sets the same); --clean removes DIR\n                         only when cello created it\n  --logs                 Show the node warning and information logs\n  --no-watch             Do not rebuild when a project file changes\n  --watch-ignore PATTERN Ignore a project-relative glob; repeat as needed\n  -h, --help             Show this help"
        ),
    )
}

fn options_from_arguments(arguments: Vec<String>) -> anyhow::Result<Option<Options>> {
    let mut project = None;
    let mut host = IpAddr::V4(Ipv4Addr::LOCALHOST);
    let mut port = DEFAULT_PORT;
    let mut clean = false;
    let mut state = None;
    let mut logs = false;
    let mut watch = true;
    let mut watch_ignores = Vec::new();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--help" | "-h" => return Ok(None),
            "--clean" => clean = true,
            "--state" => {
                state = Some(PathBuf::from(
                    arguments.next().context("--state requires a value")?,
                ));
            }
            "--logs" => logs = true,
            "--no-watch" => watch = false,
            "--watch-ignore" => {
                let value = arguments
                    .next()
                    .context("--watch-ignore requires a value")?;
                watch_ignores.push(
                    Pattern::new(&value)
                        .with_context(|| format!("invalid --watch-ignore pattern {value:?}"))?,
                );
            }
            "--host" => {
                let value = arguments.next().context("--host requires a value")?;
                host = value
                    .parse::<IpAddr>()
                    .with_context(|| format!("invalid --host value {value:?}"))?;
            }
            "--port" => {
                let value = arguments.next().context("--port requires a value")?;
                // Zero is an ephemeral port: the node binds it and the
                // supervisor learns the assigned address from the node's
                // announce line, so parallel `cello dev` instances never
                // race on a probed-free port.
                port = value
                    .parse::<u16>()
                    .with_context(|| format!("invalid --port value {value:?}"))?;
            }
            other if other.starts_with('-') => bail!("unknown argument for `cello dev`: {other}"),
            value if project.is_none() => project = Some(PathBuf::from(value)),
            value => bail!("cello dev accepts one PROJECT, but also received {value:?}"),
        }
    }
    if !watch && !watch_ignores.is_empty() {
        bail!("--watch-ignore cannot be used with --no-watch");
    }
    Ok(Some(Options {
        project,
        clean,
        state,
        stack: StackOptions {
            listener: SocketAddr::new(host, port),
            logs,
            watch,
            watch_ignores,
        },
    }))
}

#[allow(clippy::disallowed_methods)] // Project resolution uses the operator's host filesystem.
pub async fn run(arguments: Vec<String>) -> anyhow::Result<()> {
    let Some(options) = options_from_arguments(arguments)? else {
        print_help()?;
        return Ok(());
    };
    let config = deploy::resolve_config(options.project)?;
    let config = std::fs::canonicalize(&config)
        .with_context(|| format!("resolve Wrangler config {}", config.display()))?;
    let project = DevState::project_dir(&config)?;
    let state = DevState::resolve(&project, options.state);
    let console = Console::new();
    console.header();
    console.detail("Project", &config.display().to_string());
    console.detail("State", &state.path().display().to_string());
    // The delete runs before the directory is recreated, and the console names
    // the outcome. A developer who reaches for this flag is already unsure
    // which state the run uses, so a silent delete would answer nothing.
    if options.clean {
        let discarded = state.discard()?;
        console.detail(
            "Clean",
            if discarded {
                "discarded the existing local state"
            } else {
                "no local state to discard"
            },
        );
    }
    prepare_state(&state)?;
    if !options.stack.logs {
        console.detail("Logs", "hidden (use --logs)");
    }
    if !options.stack.watch {
        console.detail("Watch", "disabled");
    }

    let project_hash = format!(
        "{:x}",
        Sha256::digest(project.as_os_str().as_encoded_bytes())
    );
    let store = open_store(state.path(), &console).await?;
    run_stack(
        &config,
        state.path(),
        options.stack,
        &project_hash,
        &store,
        &console,
    )
    .await
}

#[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
fn prepare_state(state: &DevState) -> anyhow::Result<()> {
    // Fail closed on adoption: an explicit `--state` directory is arbitrary,
    // so one that already exists non-empty without the marker is foreign —
    // refusing here keeps a later `--clean` from ever seeing a marker in a
    // directory cello did not create. A missing directory is fresh and
    // proceeds; any other read error propagates. Project-relative state
    // proves itself by suffix and gains the marker on first start, and a
    // new or empty explicit directory is adopted. The check runs before
    // anything is created, so a refusal leaves no marker behind.
    if !state.is_project_state() && !state.marker().exists() {
        match std::fs::read_dir(&state.path) {
            Ok(mut entries) => {
                if entries.next().is_some() {
                    anyhow::bail!(
                        "refusing to adopt {}, which is not a cello dev state directory",
                        state.path.display()
                    );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("read local state directory {}", state.path.display())
                });
            }
        }
    }
    for directory in [&state.path, &state.path.join("runtime")] {
        std::fs::create_dir_all(directory)
            .with_context(|| format!("create local state directory {}", directory.display()))?;
    }
    // The proof of authorship `--clean` requires before it deletes an
    // explicit `--state` directory. Written on every start, so a state
    // directory made by an older Cello gains it on first use.
    std::fs::write(state.marker(), "cello dev state\n")
        .with_context(|| format!("mark local state directory {}", state.path.display()))?;
    Ok(())
}

#[allow(clippy::disallowed_methods)] // Local development state is an operator-owned host path.
async fn open_store(state: &Path, console: &Console) -> anyhow::Result<Store> {
    console.progress("opening the local storage");
    let database = std::fs::canonicalize(state)
        .context("resolve the local state directory")?
        .join("objects.sqlite3");
    let bucket = open_local_bucket(&database)?;
    fleet::validate_bucket(&bucket).await?;
    bucket.probe_cas().await?;
    Ok(Store { database })
}

/// The Worker variables for `cello dev`, from the files that `wrangler dev`
/// reads beside the config (Wrangler getVarsForDev()). A `.dev.vars` file
/// wins outright. Without one, `.env` and then `.env.local` apply, and a later
/// file overrides an earlier one. Each entry overrides the `vars` of the
/// config. Only `cello dev` reads the files, so a local credential cannot
/// reach a fleet.
fn read_dev_vars(config: &Path) -> anyhow::Result<deploy::VarOverrides> {
    // A present `.dev.vars` replaces the dotenv files even when it is empty,
    // as in Wrangler, so an entry that the project keeps in `.env` for
    // another tool does not reach the Worker beside it.
    if let Some(contents) = read_optional(&config.with_file_name(".dev.vars"))? {
        return Ok(deploy::VarOverrides::DevVars(parse_dotenv(&contents)));
    }
    let mut vars = BTreeMap::new();
    for name in [".env", ".env.local"] {
        if let Some(contents) = read_optional(&config.with_file_name(name))? {
            vars.extend(parse_dotenv(&contents));
        }
    }
    Ok(deploy::VarOverrides::DotEnv(vars))
}

/// The contents of `path`, or `None` when it does not exist. One read, not an
/// existence probe and then a read, so a file that vanishes between the two
/// cannot fail the build.
#[allow(clippy::disallowed_methods)] // The project is on the operator's host filesystem.
fn read_optional(path: &Path) -> anyhow::Result<Option<String>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(Some(contents)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {}", path.display())),
    }
}

fn parse_dotenv(contents: &str) -> BTreeMap<String, String> {
    let mut vars = BTreeMap::new();
    let mut lines = contents.lines();
    while let Some(line) = lines.next() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // dotenv accepts the `export NAME=value` form of a shell-sourced
        // file. Without the strip, the name `export NAME` fails binding
        // validation, and a `.env` that loaded before stops the build.
        let line = line.strip_prefix("export ").map_or(line, str::trim_start);
        let Some((name, value)) = line.split_once('=') else {
            continue;
        };
        let (name, value) = (name.trim(), value.trim());
        if name.is_empty() {
            continue;
        }
        let value = match quoted_lines(value, &mut lines) {
            Some(value) => value,
            // One matched pair of quotes comes off; a lone quote is part of
            // the value, as in dotenv, so a secret that ends in one survives.
            None => ['"', '\'']
                .into_iter()
                .find_map(|quote| value.strip_prefix(quote)?.strip_suffix(quote))
                .unwrap_or(value)
                .to_string(),
        };
        vars.insert(name.to_string(), value);
    }
    vars
}

/// A quoted value that continues on the following lines, as dotenv reads a
/// PEM key, joined by newlines without its quotes. Line by line, each base64
/// line of the key became a variable of its own or a name that fails binding
/// validation. `None` leaves `lines` untouched: the value ends on its own
/// line, or no later line closes the quote, so the opening quote is a lone
/// character of a one-line value.
fn quoted_lines(value: &str, lines: &mut std::str::Lines<'_>) -> Option<String> {
    let quote = value
        .chars()
        .next()
        .filter(|quote| matches!(quote, '"' | '\''))?;
    let first = &value[1..];
    if first.ends_with(quote) {
        return None;
    }
    let mut rest = lines.clone();
    let mut joined = first.to_string();
    while let Some(line) = rest.next() {
        joined.push('\n');
        match line.trim_end().strip_suffix(quote) {
            Some(last) => {
                joined.push_str(last);
                *lines = rest;
                return Some(joined);
            }
            None => joined.push_str(line),
        }
    }
    None
}

async fn deploy_project(config: &Path, store: &Store, logs: bool) -> anyhow::Result<()> {
    let bucket = open_local_bucket(&store.database)?;
    let built = deploy::build(&deploy::Options {
        config: Some(config.to_path_buf()),
        bucket: None,
        endpoint: None,
        region: None,
        dry_run: false,
        json: false,
        vars: read_dev_vars(config)?,
        local_images: true,
        // `cello dev` serves locally; deployment source maps are a deploy concern.
        sourcemap: None,
        read_roots: Vec::new(),
        no_activate: false,
    })?;
    if logs {
        built.report();
    }
    crate::wake_format::ensure_ready(&bucket).await?;
    deploy::write(&bucket, &built).await
}

async fn run_stack(
    config: &Path,
    state: &Path,
    options: StackOptions,
    project_hash: &str,
    store: &Store,
    console: &Console,
) -> anyhow::Result<()> {
    // Install both handlers before the child can become ready. A caller can
    // send SIGTERM as soon as the readiness request answers, and installing a
    // handler afterwards leaves the new node orphaned under the default Unix
    // signal action.
    let mut signals = ShutdownSignals::install()?;
    let project = config
        .parent()
        .context("the Wrangler config has no project directory")?;
    // The watcher canonicalizes the watch root and both spellings of the
    // state directory itself; `prepare_state` created it before
    // `run_stack`, so it always exists here. An external directory never
    // shares either prefix and needs no special case.
    let mut watcher = options
        .watch
        .then(|| ProjectWatcher::new(project, state, options.watch_ignores))
        .transpose()?;

    console.progress("building the application");
    deploy_project(config, store, options.logs).await?;
    let mut running = start_node(
        state,
        options.listener,
        options.logs,
        project_hash,
        store,
        console,
    )
    .await?;

    loop {
        match wait_for_node_event(&mut running.child, &mut signals, watcher.as_mut()).await? {
            NodeEvent::Exited(status) => {
                let _ = running.output.await;
                if status.success() {
                    return Ok(());
                }
                bail!("the local cello node exited with {status}");
            }
            NodeEvent::Shutdown => {
                stop_running_node(running).await;
                return Ok(());
            }
            NodeEvent::Reload => {
                console.progress("change detected; rebuilding the application");
                if let Err(error) = deploy_project(config, store, options.logs).await {
                    console.failure(&format!("reload failed: {error:#}"));
                    continue;
                }
                console.progress("restarting the application");
                stop_running_node(running).await;
                running = start_node(
                    state,
                    options.listener,
                    options.logs,
                    project_hash,
                    store,
                    console,
                )
                .await?;
            }
        }
    }
}

struct RunningNode {
    child: Child,
    internal: String,
    output: tokio::task::JoinHandle<()>,
}

enum NodeEvent {
    Exited(std::process::ExitStatus),
    Reload,
    Shutdown,
}

#[allow(clippy::disallowed_methods)] // Child supervision uses real host process and time APIs.
async fn start_node(
    state: &Path,
    listener: SocketAddr,
    logs: bool,
    project_hash: &str,
    store: &Store,
    console: &Console,
) -> anyhow::Result<RunningNode> {
    let executable = std::env::current_exe().context("locate the cello executable")?;
    let listen = listener.to_string();
    let mut command = Command::new(executable);
    // The supervisor owns the child's complete storage and listener topology.
    // Inherited fleet settings can otherwise override that private topology or
    // make its guard reject a valid `cello dev` invocation.
    for variable in [
        "AWS_DEFAULT_REGION",
        "AWS_REGION",
        "CELLO_ADDR",
        "CELLO_ADVERTISE",
        "CELLO_BUCKET",
        "CELLO_CLOUD",
        "CELLO_INTERNAL_ADDR",
        "CELLO_TEST_BUCKET",
        "CELLO_TRUST_FORWARDED_HEADERS",
        "CELLO_UNSAFE_PUBLIC_ADVERTISE",
        "S3_ENDPOINT",
    ] {
        command.env_remove(variable);
    }
    command
        .args([
            "--no-control-plane",
            "--bucket",
            BUCKET,
            "--listen",
            &listen,
            "--internal-listen",
            "127.0.0.1:0",
        ])
        .env("CELLO_INTERNAL_DEV_STORE", &store.database)
        .env("CELLO_WATCH", state.join("runtime"))
        .env("CELLO_NODE", format!("dev-{}", &project_hash[..12]))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    // `run_stack` normally consumes `RunningNode` through the graceful stop
    // path. An error or cancellation can instead drop it, so the child must
    // not outlive the value that proves the supervisor still owns it.
    command.kill_on_drop(true);
    // The child re-execs `cello` with serving arguments, so it makes itself
    // non-dumpable in `main` like any node (`hardening`, patch 0053). The
    // supervisor does the same at its own start; `execve` resets the flag,
    // which is why the child sets it again rather than inheriting it.
    if !logs {
        // The supervisor still forwards ERROR records and the child's stderr.
        // INFO and WARN records stay hidden until the operator asks for them.
        command.env("RUST_LOG", "error");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        // A terminal sends Ctrl-C to its foreground process group. Keep the
        // node in a child group so only this supervisor selects the shutdown
        // mode; otherwise the node can begin a slow fleet handoff before the
        // supervisor asks for the fast same-node preserve path.
        command.as_std_mut().process_group(0);
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt as _;

        let supervisor = unsafe { libc::getpid() };
        // Drop cannot run after SIGKILL or a supervisor crash. Ask the kernel
        // to kill the node in that case. The parent check closes the race in
        // which the supervisor exits between `fork` and `prctl`.
        unsafe {
            command.as_std_mut().pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::getppid() != supervisor {
                    return Err(std::io::Error::from_raw_os_error(libc::ESRCH));
                }
                Ok(())
            });
        }
    }
    let mut child = command.spawn().context("start the local cello node")?;

    let stdout = child
        .stdout
        .take()
        .expect("the local node was configured with piped output");
    let (internal_tx, internal_rx) = oneshot::channel();
    let (public_tx, public_rx) = oneshot::channel();
    let output_console = *console;
    let output = tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();
        let mut internal_tx = Some(internal_tx);
        let mut public_tx = Some(public_tx);
        while let Ok(Some(line)) = lines.next_line().await {
            if let Some(origin) = internal_origin(&line) {
                if let Some(sender) = internal_tx.take() {
                    let _ = sender.send(origin);
                }
            }
            if let Some(address) = public_origin(&line) {
                if let Some(sender) = public_tx.take() {
                    let _ = sender.send(address);
                }
            }
            if logs || line.contains(" ERROR ") {
                output_console.log(&line);
            }
        }
    });
    let internal = match tokio::time::timeout(Duration::from_secs(30), internal_rx).await {
        Ok(Ok(internal)) => internal,
        result => {
            stop_child(&mut child, None).await;
            let _ = output.await;
            match result {
                Err(_) => bail!("the local node did not announce its operator listener"),
                Ok(Err(_)) => {
                    bail!("the local node exited before it announced its operator listener")
                }
                Ok(Ok(_)) => unreachable!(),
            }
        }
    };
    // The node prints its worker announce before its operator announce,
    // so this is already in hand; the timeout only guards a node that
    // never prints it.
    let public = match tokio::time::timeout(Duration::from_secs(30), public_rx).await {
        Ok(Ok(public)) => public,
        result => {
            stop_child(&mut child, Some(&internal)).await;
            let _ = output.await;
            match result {
                Err(_) => bail!("the local node did not announce its worker listener"),
                Ok(Err(_)) => {
                    bail!("the local node exited before it announced its worker listener")
                }
                Ok(Ok(_)) => unreachable!(),
            }
        }
    };
    // A requested port of zero means the node assigned one; readiness
    // and the ready URL use the announced address in that case.
    let listener = if listener.port() == 0 {
        public
    } else {
        listener
    };

    if let Err(error) = wait_for_node(&mut child, listener).await {
        stop_child(&mut child, Some(&internal)).await;
        let _ = output.await;
        return Err(error);
    }
    console.ready(&format!("http://{listener}"));
    Ok(RunningNode {
        child,
        internal,
        output,
    })
}

fn internal_origin(line: &str) -> Option<String> {
    let address = line
        .strip_prefix("cello internal listening on ")?
        .split_once(' ')?
        .0;
    Some(format!("http://{address}"))
}

fn public_origin(line: &str) -> Option<SocketAddr> {
    let address = line.strip_prefix("cello listening on ")?.split_once(' ')?.0;
    address.parse().ok()
}

#[allow(clippy::disallowed_methods)] // Readiness waits on a real child and wall-clock deadline.
async fn wait_for_node(child: &mut Child, listener: SocketAddr) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(200))
        .timeout(Duration::from_secs(1))
        .build()?;
    let readiness_host = match listener.ip() {
        IpAddr::V4(address) if address.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(address) if address.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        address => address,
    };
    let url = format!(
        "http://{}/.well-known/cello/health",
        SocketAddr::new(readiness_host, listener.port())
    );
    let mut last = "the node did not answer".to_string();
    for _ in 0..300 {
        if let Some(status) = child.try_wait().context("read the local node status")? {
            bail!("the local cello node exited during startup with {status}");
        }
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => last = format!("the readiness endpoint returned {}", response.status()),
            Err(error) => last = error.to_string(),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bail!("the local cello node did not become ready: {last}")
}

async fn wait_for_node_event(
    child: &mut Child,
    signals: &mut ShutdownSignals,
    watcher: Option<&mut ProjectWatcher>,
) -> anyhow::Result<NodeEvent> {
    crate::asyncrt::select! {
        status = child.wait() => {
            let status = status.context("wait for the local cello node")?;
            Ok(NodeEvent::Exited(status))
        }
        _ = signals.received() => Ok(NodeEvent::Shutdown),
        changed = wait_for_project_change(watcher) => {
            changed?;
            Ok(NodeEvent::Reload)
        }
    }
}

async fn wait_for_project_change(watcher: Option<&mut ProjectWatcher>) -> anyhow::Result<()> {
    match watcher {
        Some(watcher) => watcher.changed().await,
        None => std::future::pending().await,
    }
}

async fn stop_running_node(mut running: RunningNode) {
    stop_child(&mut running.child, Some(&running.internal)).await;
    let _ = running.output.await;
}

#[allow(clippy::disallowed_methods)] // Shutdown deadlines govern a real child process.
async fn stop_child(child: &mut Child, internal: Option<&str>) {
    let requested = if let Some(internal) = internal {
        match reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
        {
            Ok(client) => {
                // The child inherits this supervisor's environment, so when
                // the operator token is set the child guards /shutdown with
                // it and this request must present it. An unreadable token
                // falls through to the SIGTERM path below, as a refused
                // shutdown does.
                let post = client.post(format!("{internal}/shutdown?handoff=preserve"));
                match crate::operator_token::authenticate(post) {
                    Ok(signed) => signed
                        .send()
                        .await
                        .is_ok_and(|response| response.status().is_success()),
                    Err(_) => false,
                }
            }
            Err(_) => false,
        }
    } else {
        false
    };
    #[cfg(unix)]
    if !requested {
        if let Some(id) = child.id() {
            // The child installs SIGTERM as its graceful durability handoff. The
            // supervisor sends that signal instead of Child::kill, which is a
            // SIGKILL on Unix and can leave the last local transaction unflushed.
            unsafe {
                libc::kill(id as libc::pid_t, libc::SIGTERM);
            }
        }
    }
    let waited = tokio::time::timeout(Duration::from_secs(35), child.wait()).await;
    if waited.is_err() {
        let _ = child.kill().await;
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // Operator-side tests use real tempdirs, not node storage.
mod dev_tests {
    use super::*;

    fn options_of(args: &[&str]) -> Options {
        options_from_arguments(args.iter().map(|arg| arg.to_string()).collect())
            .expect("parse")
            .expect("options")
    }

    #[test]
    fn port_zero_means_ephemeral() {
        let options = options_of(&["--port", "0"]);
        assert_eq!(options.stack.listener.port(), 0);
        let options = options_of(&["--port", "8484"]);
        assert_eq!(options.stack.listener.port(), 8484);
    }

    #[test]
    fn state_flag_reaches_options() {
        let options = options_of(&["--state", "/tmp/elsewhere"]);
        assert_eq!(
            options.state.as_deref(),
            Some(std::path::Path::new("/tmp/elsewhere"))
        );
        assert_eq!(options_of(&[]).state, None);
    }

    #[test]
    fn state_resolution_prefers_flag_then_environment_then_project() {
        let project = std::path::Path::new("/proj");
        let resolve = |flag: Option<&str>, env: Option<&str>| {
            DevState::resolve_with(project, flag.map(PathBuf::from), env.map(PathBuf::from)).path
        };
        assert_eq!(resolve(None, None), project.join(".cello/dev"));
        assert_eq!(
            resolve(None, Some("/env/state")),
            std::path::PathBuf::from("/env/state")
        );
        assert_eq!(
            resolve(Some("/flag/state"), Some("/env/state")),
            std::path::PathBuf::from("/flag/state")
        );
        // An empty or whitespace-only flag or variable is unset, not a
        // directory.
        assert_eq!(
            resolve(Some(""), Some("/env/state")),
            std::path::PathBuf::from("/env/state")
        );
        assert_eq!(resolve(Some(""), None), project.join(".cello/dev"));
        assert_eq!(
            resolve(Some("   "), Some("/env/state")),
            std::path::PathBuf::from("/env/state")
        );
        assert_eq!(resolve(None, Some(" \t ")), project.join(".cello/dev"));
        assert_eq!(resolve(Some("  "), None), project.join(".cello/dev"));
    }

    /// The watcher excludes the resolved state directory: SQLite and WAL
    /// writes under an in-project `--state` directory must not request a
    /// rebuild — each rebuild writes state again, so the supervisor would
    /// loop without a source change — while source files still rebuild,
    /// the default `.cello/dev` stays ignored, and an external state
    /// directory changes nothing.
    #[test]
    fn watcher_excludes_the_resolved_state_directory() {
        use notify::event::{DataChange, ModifyKind};
        let project = tempfile::tempdir().unwrap();
        let state = project.path().join("dev-state");
        std::fs::create_dir_all(&state).unwrap();
        let ignored: Vec<Pattern> = Vec::new();
        let relevant = |state: &Path, path: &Path| {
            relevant_project_event(
                project.path(),
                state,
                None,
                &notify::Event {
                    kind: notify::EventKind::Modify(ModifyKind::Data(DataChange::Any)),
                    paths: vec![path.to_path_buf()],
                    ..Default::default()
                },
                &ignored,
            )
        };
        // State writes never rebuild, including the state directory event
        // itself.
        assert!(!relevant(&state, &state.join("objects.sqlite3")));
        assert!(!relevant(&state, &state.join("objects.sqlite3-wal")));
        assert!(!relevant(&state, &state));
        // Sources beside the state directory still rebuild.
        assert!(relevant(&state, &project.path().join("worker.js")));
        // The default state stays ignored through the `.cello` rule too.
        let default_state = project.path().join(".cello").join("dev");
        assert!(!relevant(
            &default_state,
            &default_state.join("objects.sqlite3")
        ));
        // An external state directory never matches a project event, so it
        // changes nothing: sources still rebuild.
        let elsewhere = tempfile::tempdir().unwrap();
        assert!(relevant(
            elsewhere.path(),
            &project.path().join("worker.js")
        ));
    }

    /// A project reached through a symlinked root still excludes its
    /// state: the watcher canonicalizes the watch root, so event paths
    /// carry the canonical spelling and match the canonical state
    /// prefix. Without the canonicalization the state writes look like
    /// source changes and the supervisor rebuilds in a loop.
    #[test]
    fn watcher_canonicalizes_a_symlinked_watch_root() {
        let real = tempfile::tempdir().unwrap();
        let state = real.path().join("dev-state");
        std::fs::create_dir_all(&state).unwrap();

        let link = real.path().join("linked");
        std::os::unix::fs::symlink(real.path(), &link).unwrap();
        let watcher = ProjectWatcher::new(&link, &state, Vec::new()).expect("watch");
        // The watch root is resolved, so it matches the canonical state
        // directory even though the project was reached through a symlink.
        assert_eq!(watcher.project, std::fs::canonicalize(&link).unwrap(),);
        // Event paths carry the watch root's spelling: a state write under
        // it must be ignored, while a source beside it still rebuilds.
        let ignored: Vec<Pattern> = Vec::new();
        let event = |path: &std::path::Path| notify::Event {
            kind: notify::EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Any,
            )),
            paths: vec![path.to_path_buf()],
            ..Default::default()
        };
        assert!(
            !relevant_project_event(
                &watcher.project,
                &watcher.state,
                watcher.state_alias.as_deref(),
                &event(
                    &watcher
                        .project
                        .join("dev-state")
                        .join("objects.sqlite3-wal")
                ),
                &ignored,
            ),
            "a state write through the symlinked root must be ignored",
        );
        assert!(relevant_project_event(
            &watcher.project,
            &watcher.state,
            watcher.state_alias.as_deref(),
            &event(&watcher.project.join("worker.js")),
            &ignored,
        ));
    }

    /// A symlinked state directory inside the project excludes both
    /// spellings: notify follows the link and can report a WAL event
    /// under the traversed link path, which misses the canonical prefix.
    /// Sources outside the state dir stay relevant either way.
    #[test]
    fn watcher_excludes_a_symlinked_state_directory_spelling() {
        use notify::event::{DataChange, ModifyKind};
        let project = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        let link = project.path().join("dev-state");
        std::os::unix::fs::symlink(target.path(), &link).unwrap();
        let canonical_project = std::fs::canonicalize(project.path()).unwrap();
        let watcher = ProjectWatcher::new(&canonical_project, &link, Vec::new()).expect("watch");
        // The canonical prefix covers the resolved spelling; the alias
        // covers the traversed link spelling.
        assert_eq!(watcher.state, std::fs::canonicalize(&link).unwrap(),);
        assert_eq!(watcher.state_alias.as_deref(), Some(link.as_path()));
        let ignored: Vec<Pattern> = Vec::new();
        let event = |path: &std::path::Path| notify::Event {
            kind: notify::EventKind::Modify(ModifyKind::Data(DataChange::Any)),
            paths: vec![path.to_path_buf()],
            ..Default::default()
        };
        let relevant = |path: &std::path::Path| {
            relevant_project_event(
                &watcher.project,
                &watcher.state,
                watcher.state_alias.as_deref(),
                &event(path),
                &ignored,
            )
        };
        // Either spelling of a state write is ignored ...
        assert!(
            !relevant(&link.join("objects.sqlite3-wal")),
            "a state write under the link spelling must be ignored",
        );
        assert!(
            !relevant(&watcher.state.join("objects.sqlite3-wal")),
            "a state write under the canonical spelling must be ignored",
        );
        // ... while a source beside the state dir still rebuilds.
        assert!(relevant(&watcher.project.join("worker.js")));
    }

    /// `prepare_state` fails closed on adoption and `--clean` deletes only
    /// what Cello created: a marked directory goes away, a non-empty foreign
    /// one is refused by both with its contents intact and no marker left
    /// behind — even when it happens to end in `.cello/dev`, since the
    /// suffix only exempts project-relative state — and a missing one is
    /// already clean.
    #[test]
    fn discard_guards_explicit_state_directories() {
        let marked = tempfile::tempdir().unwrap();
        let state = DevState::for_state(marked.path().to_path_buf());
        prepare_state(&state).unwrap();
        assert!(state.marker().exists());
        assert!(state.discard().unwrap());
        assert!(!marked.path().exists());

        let foreign = tempfile::tempdir().unwrap();
        std::fs::write(foreign.path().join("precious.txt"), "keep\n").unwrap();
        let foreign_state = DevState::for_state(foreign.path().to_path_buf());
        // The start itself must refuse before it writes anything, so a
        // later `--clean` never finds a marker cello planted in a
        // directory it did not create.
        let error = prepare_state(&foreign_state)
            .expect_err("adopting a foreign directory must be refused");
        assert!(
            format!("{error:?}").contains("refusing to adopt"),
            "the refusal must say so: {error:?}"
        );
        assert!(!foreign_state.marker().exists());
        let error = foreign_state
            .discard()
            .expect_err("a foreign directory must be refused");
        assert!(
            format!("{error:?}").contains("refusing to discard"),
            "the refusal must say so: {error:?}"
        );
        assert!(foreign.path().join("precious.txt").exists());
        // A foreign directory that happens to end in `.cello/dev` is still
        // foreign: the suffix only exempts project-relative state, never an
        // explicit `--state` path.
        let shaped = tempfile::tempdir().unwrap();
        let shaped_dir = shaped.path().join(".cello/dev");
        std::fs::create_dir_all(&shaped_dir).unwrap();
        std::fs::write(shaped_dir.join("precious.txt"), "keep\n").unwrap();
        let shaped_state = DevState::for_state(shaped_dir.clone());
        let error = prepare_state(&shaped_state)
            .expect_err("adopting a suffix-shaped foreign directory must be refused");
        assert!(
            format!("{error:?}").contains("refusing to adopt"),
            "the refusal must say so: {error:?}"
        );
        assert!(!shaped_state.marker().exists());
        let error = shaped_state
            .discard()
            .expect_err("a suffix-shaped foreign directory must be refused");
        assert!(
            format!("{error:?}").contains("refusing to discard"),
            "the refusal must say so: {error:?}"
        );
        assert!(shaped_dir.join("precious.txt").exists());

        let missing = DevState::for_state(foreign.path().join("no-such-dir"));
        assert!(!missing.discard().unwrap());
    }

    /// Back-compat: project-relative state is suffix-exempt, so a non-empty
    /// directory from an older Cello gains the marker on first start; a new
    /// or empty explicit directory is adopted the same way.
    #[test]
    fn prepare_state_adopts_project_and_empty_explicit_directories() {
        let project = tempfile::tempdir().unwrap();
        let legacy = project.path().join(".cello/dev");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("objects.sqlite3"), "old\n").unwrap();
        let state = DevState::for_project(project.path());
        prepare_state(&state).unwrap();
        assert!(state.marker().exists());
        assert!(legacy.join("objects.sqlite3").exists());

        let empty = tempfile::tempdir().unwrap();
        let fresh = DevState::for_state(empty.path().join("no-such-dir"));
        prepare_state(&fresh).unwrap();
        assert!(fresh.marker().exists());
        assert!(fresh.discard().unwrap());
    }

    #[test]
    fn announce_parsers_split_worker_and_operator_lines() {
        let worker = "cello listening on 127.0.0.1:40349 (ownership=dev-abc)";
        let operator = "cello internal listening on 127.0.0.1:51234 (advertise=none)";
        assert_eq!(
            public_origin(worker),
            Some("127.0.0.1:40349".parse().expect("addr"))
        );
        assert_eq!(public_origin(operator), None);
        assert_eq!(
            internal_origin(operator).as_deref(),
            Some("http://127.0.0.1:51234")
        );
        assert_eq!(internal_origin(worker), None);
        assert_eq!(
            public_origin("cello listening on bogus (ownership=x)"),
            None
        );
    }
}
