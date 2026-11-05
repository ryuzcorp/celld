// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by ryuzcorp (ryuzcorp/celld): staged activation; deploy --read-root option. Apache-2.0.

// A black-box runner, not node code: it drives the library against a local
// bucket and asserts on the adoption read path, so the execution
// boundary's injected clock/filesystem/async rules do not apply to it.
#![allow(clippy::disallowed_methods)]

//! Staged activation: `deploy --no-activate` uploads a version no adoption
//! can see, `activate` serves a multi-script generation with one pointer
//! write, and `discard` removes a version no pointer names.
//!
//! Every test owns a hermetic local bucket, so tests share nothing and run
//! in any order. Serving assertions go through
//! `DeploymentGraph::load` — the one bucket walk boot and reload share —
//! so "invisible to serving" means invisible to the real adoption path,
//! not to a reimplementation of it. There are no node boots and no
//! sleeps: pointer bytes are the poll watcher's whole input, so byte
//! equality with the pre-stage pointer is exactly "the poll adopts
//! nothing".

use std::path::{Path, PathBuf};

/// One runtime for the whole binary: `asyncrt::set_host_handle` runs
/// exactly once per process, and the dev store's background tasks spawn
/// onto that handle — a per-test runtime would cancel a sibling test's
/// tasks when its test ends. Blocking is serialized so no two tests
/// drive the runtime at once.
static SHARED_RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
static BLOCK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn rt() -> &'static tokio::runtime::Runtime {
    SHARED_RT.get_or_init(|| {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        celld::asyncrt::set_host_handle(rt.handle().clone());
        rt
    })
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let _guard = BLOCK_LOCK.lock().expect("block lock");
    rt().block_on(future)
}

/// Run one future with a `CELLD_TEST_*` fault knob set. The block lock is
/// held across the set, the run, and the unset, so no sibling test in
/// this binary can observe the knob.
fn block_on_with_env<F: std::future::Future>(key: &str, future: F) -> F::Output {
    let _guard = BLOCK_LOCK.lock().expect("block lock");
    std::env::set_var(key, "1");
    let output = rt().block_on(future);
    std::env::remove_var(key);
    output
}

fn fixture_src(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/staged-activation")
        .join(name)
}

/// One hermetic fleet bucket plus writable project copies.
struct Fleet {
    _temp: tempfile::TempDir,
    bucket: celld::bucket::Bucket,
    projects: PathBuf,
}

impl Fleet {
    fn new() -> Self {
        let temp = tempfile::Builder::new()
            .prefix("celld-staged-activation")
            .tempdir()
            .expect("tempdir");
        let state = temp.path().join("state");
        std::fs::create_dir_all(state.join("runtime")).expect("state dir");
        let database = state.join("objects.sqlite3");
        let bucket = celld::dev::open_local_bucket(&database).expect("open dev bucket");
        block_on(async {
            celld::fleet::validate_bucket(&bucket)
                .await
                .expect("validate bucket");
            bucket.probe_cas().await.expect("probe cas");
            celld::wake_format::ensure_ready(&bucket)
                .await
                .expect("ensure ready");
        });
        let projects = temp.path().join("projects");
        std::fs::create_dir_all(&projects).expect("projects dir");
        Self {
            _temp: temp,
            bucket,
            projects,
        }
    }

    /// A writable copy of the named fixture project.
    fn project(&self, name: &str) -> PathBuf {
        let dst = self.projects.join(name);
        std::fs::create_dir_all(&dst).expect("create project dir");
        for file in ["wrangler.json", "worker.js"] {
            std::fs::copy(fixture_src(name).join(file), dst.join(file)).expect("copy fixture");
        }
        dst
    }

    /// A writable copy of the `fixture` project renamed to `script`: a
    /// second script with the same shape, for queue conflicts and
    /// handoffs between two consumers of one queue.
    fn project_renamed(&self, dir: &str, fixture: &str, script: &str) -> PathBuf {
        let dst = self.projects.join(dir);
        std::fs::create_dir_all(&dst).expect("create project dir");
        for file in ["wrangler.json", "worker.js"] {
            std::fs::copy(fixture_src(fixture).join(file), dst.join(file)).expect("copy fixture");
        }
        let path = dst.join("wrangler.json");
        let mut config: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read wrangler"))
                .expect("parse wrangler");
        config["name"] = serde_json::Value::String(script.to_string());
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&config).expect("render wrangler"),
        )
        .expect("write wrangler");
        dst
    }

    /// Declare a queue producer on a project's config, so the next
    /// build is a version that produces to the queue without
    /// consuming it — the standard producer shape.
    fn add_queue_producer(project: &Path, binding: &str, queue: &str) {
        let path = project.join("wrangler.json");
        let mut config: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read wrangler"))
                .expect("parse wrangler");
        config["queues"] = serde_json::json!({
            "producers": [{ "binding": binding, "queue": queue }],
        });
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&config).expect("render wrangler"),
        )
        .expect("write wrangler");
    }

    /// Append a service binding to a project's config, so the next
    /// build reaches one more script.
    fn add_service_binding(project: &Path, binding: &str, service: &str) {
        let path = project.join("wrangler.json");
        let mut config: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read wrangler"))
                .expect("parse wrangler");
        config
            .get_mut("services")
            .and_then(serde_json::Value::as_array_mut)
            .expect("services array")
            .push(serde_json::json!({ "binding": binding, "service": service }));
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&config).expect("render wrangler"),
        )
        .expect("write wrangler");
    }

    /// Drop the queue consumers from a project's config, so the next
    /// build is a version that releases its queues.
    fn drop_queues(project: &Path) {
        let path = project.join("wrangler.json");
        let mut config: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("read wrangler"))
                .expect("parse wrangler");
        config
            .as_object_mut()
            .expect("wrangler object")
            .remove("queues");
        std::fs::write(
            &path,
            serde_json::to_string_pretty(&config).expect("render wrangler"),
        )
        .expect("write wrangler");
    }

    /// Rewrite the worker so the next build hashes a new version.
    fn bump(project: &Path, marker: &str) {
        let path = project.join("worker.js");
        let mut body = std::fs::read_to_string(&path).expect("read worker");
        body.push_str(&format!("\n// {marker}\n"));
        std::fs::write(&path, body).expect("write worker");
    }

    fn build(project: &Path) -> celld::deploy::Built {
        celld::deploy::build(&celld::deploy::Options {
            config: Some(project.to_path_buf()),
            bucket: None,
            endpoint: None,
            region: None,
            dry_run: false,
            json: false,
            vars: celld::deploy::VarOverrides::None,
            local_images: true,
            sourcemap: None,
            no_activate: false,
            read_roots: Vec::new(),
        })
        .expect("build")
    }

    fn block<F: std::future::Future>(&self, future: F) -> F::Output {
        block_on(future)
    }

    /// Every pointer serving reads: the fleet-wide one plus each script's
    /// named one. Byte equality across an operation is exactly "the poll
    /// watcher and `POST /reload` see no change".
    fn pointer_snapshot(&self) -> Vec<(String, Option<Vec<u8>>)> {
        self.block(async {
            let mut snapshot = Vec::new();
            for key in [
                "deploy/current.json",
                "deploy/staged-app/current.json",
                "deploy/staged-helper/current.json",
                "deploy/staged-consumer/current.json",
            ] {
                let body = self
                    .bucket
                    .get(key)
                    .await
                    .expect("get pointer")
                    .map(|(body, _)| body.to_vec());
                snapshot.push((key.to_string(), body));
            }
            snapshot
        })
    }

    fn graph(&self) -> celld::generation::DeploymentGraph {
        self.block(celld::generation::DeploymentGraph::load(
            &self.bucket,
            "test-node".to_string(),
        ))
        .expect("load graph")
    }

    fn version_of(&self, script: &str, version: &str) -> Option<Vec<u8>> {
        self.block(async {
            self.bucket
                .get(&format!("deploy/{script}/{version}/manifest.json"))
                .await
                .expect("get manifest")
                .map(|(body, _)| body.to_vec())
        })
    }

    fn attachment(&self, queue: &str) -> Option<celld::protocol::QueueConsumerDeployment> {
        self.block(celld::fleet::load_queue_consumer_attachment(
            &self.bucket,
            queue,
        ))
        .expect("read attachment")
    }

    fn named_pointer_version(&self, script: &str) -> String {
        let body = self
            .block(self.bucket.get(&format!("deploy/{script}/current.json")))
            .expect("get named pointer")
            .unwrap_or_else(|| panic!("named pointer for {script}"))
            .0;
        let pointer: serde_json::Value = serde_json::from_slice(&body).expect("decode");
        pointer["version"].as_str().expect("version").to_string()
    }

    fn fleet_pointer_body(&self) -> Vec<u8> {
        self.block(self.bucket.get("deploy/current.json"))
            .expect("get fleet pointer")
            .expect("fleet pointer exists")
            .0
            .to_vec()
    }

    /// Overwrite one queue's consumer attachment with the exact bytes a
    /// writer's publish leaves behind: crash residue is a loser's
    /// attachment publish without its commit.
    fn skew_attachment(&self, queue: &str, script: &str, version: &str) {
        self.block(async {
            self.bucket
                .put(
                    &format!("deploy/queues/{queue}/consumer.json"),
                    serde_json::to_vec_pretty(&serde_json::json!({
                        "schema_version": 1,
                        "queue": queue,
                        "consumer": {
                            "script_name": script,
                            "version": version,
                            "prefix": format!("deploy/{script}/{version}"),
                        },
                    }))
                    .expect("render attachment"),
                )
                .await
                .expect("skew attachment");
        });
    }

    fn load_error(&self) -> String {
        let loaded = self.block(celld::generation::DeploymentGraph::load(
            &self.bucket,
            "test-node".to_string(),
        ));
        format!("{:#}", loaded.err().expect("adoption fails"))
    }
}

fn version_ref(script: &str, version: &str) -> celld::deploy::VersionRef {
    celld::deploy::VersionRef {
        script: script.to_string(),
        version: version.to_string(),
    }
}

/// A fleet with plain v1s serving and staged v2s waiting: the app first,
/// the helper beside it.
struct TwoGenerations {
    fleet: Fleet,
    app_v1: String,
    helper_v1: String,
    app_v2: String,
    helper_v2: String,
}

fn two_generations() -> TwoGenerations {
    let fleet = Fleet::new();
    let app = fleet.project("app");
    let helper = fleet.project("helper");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v1))
        .expect("deploy helper v1");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    Fleet::bump(&helper, "helper-v2");
    Fleet::bump(&app, "app-v2");
    let helper_v2 = Fleet::build(&helper);
    let app_v2 = Fleet::build(&app);
    assert_ne!(helper_v1.version, helper_v2.version, "bump changes helper");
    assert_ne!(app_v1.version, app_v2.version, "bump changes app");
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &helper_v2))
        .expect("stage helper v2");
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &app_v2))
        .expect("stage app v2");
    TwoGenerations {
        fleet,
        app_v1: app_v1.version,
        helper_v1: helper_v1.version,
        app_v2: app_v2.version,
        helper_v2: helper_v2.version,
    }
}

#[test]
fn staged_deploy_moves_no_pointer() {
    let fleet = Fleet::new();
    let app = fleet.project("app");
    let helper = fleet.project("helper");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v1))
        .expect("deploy helper v1");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    let before = fleet.pointer_snapshot();

    Fleet::bump(&helper, "helper-v2");
    Fleet::bump(&app, "app-v2");
    let helper_v2 = Fleet::build(&helper);
    let app_v2 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &helper_v2))
        .expect("stage helper v2");
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &app_v2))
        .expect("stage app v2");

    assert_eq!(before, fleet.pointer_snapshot(), "staging moves no pointer");
    // The staged versions are complete on the bucket: activate and
    // discard both find them by script and version.
    for (script, version) in [
        ("staged-helper", &helper_v2.version),
        ("staged-app", &app_v2.version),
    ] {
        let body = fleet
            .version_of(script, version)
            .unwrap_or_else(|| panic!("staged manifest for {script}={version}"));
        let manifest: serde_json::Value =
            serde_json::from_slice(&body).expect("decode staged manifest");
        assert_eq!(manifest["script_name"], script);
        assert_eq!(manifest["version"], *version);
    }
}

#[test]
fn staged_versions_are_invisible_to_serving() {
    let two = two_generations();
    let graph = two.fleet.graph();
    assert_eq!(graph.primary.script_name, "staged-app");
    assert_eq!(
        graph.primary.version, two.app_v1,
        "primary still serves v1 after staging v2"
    );
    let helper = graph
        .cohosted
        .iter()
        .find(|loaded| loaded.script_name == "staged-helper")
        .expect("helper resolves through the service binding");
    assert_eq!(
        helper.version, two.helper_v1,
        "service target still serves v1 after staging v2"
    );
}

#[test]
fn deploy_poll_ignores_staged_versions() {
    let two = two_generations();
    // The watcher's whole input: the pointer, read through its own read
    // function. It still names v1, so the watcher's own predicate
    // reports unchanged and a poll tick adopts nothing — as does
    // `POST /reload`, which reads the same key.
    let pointer = two
        .fleet
        .block(celld::fleet::read_current_pointer(&two.fleet.bucket))
        .expect("read pointer");
    assert_eq!(pointer.version, two.app_v1);
    assert_eq!(pointer.prefix, format!("deploy/staged-app/{}", two.app_v1));
    assert_eq!(pointer.script_name.as_deref(), Some("staged-app"));
    assert!(pointer.components.is_none());
    let serving = celld::generation::ServingIdentity::of_pointer(&pointer);
    assert!(
        serving.names(&pointer),
        "the watcher predicate reports unchanged while staging moves no pointer"
    );
}

#[test]
fn activate_switches_all_scripts_at_once() {
    let two = two_generations();
    let fleet = &two.fleet;
    let activated = fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &two.app_v2),
                version_ref("staged-helper", &two.helper_v2),
            ],
            None,
        ))
        .expect("activate");
    assert_eq!(activated.primary, "staged-app");
    assert!(!activated.unchanged);

    // One pointer names every script's version.
    let pointer = fleet
        .block(celld::fleet::read_current_pointer(&fleet.bucket))
        .expect("read pointer");
    assert_eq!(pointer.version, two.app_v2);
    let generation = pointer.components.expect("activated generation");
    assert_eq!(generation.len(), 2);
    assert_eq!(generation["staged-app"].version, two.app_v2);
    assert_eq!(generation["staged-helper"].version, two.helper_v2);

    // Serving flips for both scripts together.
    let graph = fleet.graph();
    assert_eq!(graph.primary.version, two.app_v2);
    let helper = graph
        .cohosted
        .iter()
        .find(|loaded| loaded.script_name == "staged-helper")
        .expect("helper resolves through the service binding");
    assert_eq!(helper.version, two.helper_v2);

    // The named pointers moved beside the fleet pointer, so a plain
    // `celld deploy` or an older node resolves the same versions.
    for (script, version) in [
        ("staged-app", &two.app_v2),
        ("staged-helper", &two.helper_v2),
    ] {
        let body = fleet
            .block(fleet.bucket.get(&format!("deploy/{script}/current.json")))
            .expect("get named pointer")
            .unwrap_or_else(|| panic!("named pointer for {script}"))
            .0;
        let named: serde_json::Value = serde_json::from_slice(&body).expect("decode");
        assert_eq!(named["version"], *version);
    }

    // Repeating the activation reports unchanged and moves nothing.
    let before = fleet.pointer_snapshot();
    let again = fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &two.app_v2),
                version_ref("staged-helper", &two.helper_v2),
            ],
            None,
        ))
        .expect("re-activate");
    assert!(again.unchanged);
    assert_eq!(before, fleet.pointer_snapshot());
}

#[test]
fn failed_activate_moves_nothing() {
    let two = two_generations();
    let fleet = &two.fleet;
    let before = fleet.pointer_snapshot();
    let error = fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &two.app_v2),
                version_ref("staged-helper", "0bad000000000000"),
            ],
            None,
        ))
        .expect_err("unknown staged version fails");
    assert!(
        format!("{error:#}").contains("no staged version"),
        "unexpected error: {error:#}"
    );
    // No partial generation: validation precedes the single pointer write.
    assert_eq!(before, fleet.pointer_snapshot());
    let graph = fleet.graph();
    assert_eq!(graph.primary.version, two.app_v1);
}

#[test]
fn discard_removes_only_what_no_pointer_names() {
    let fleet = Fleet::new();
    let app = fleet.project("app");
    let helper = fleet.project("helper");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v1))
        .expect("deploy helper v1");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    Fleet::bump(&helper, "helper-v2");
    Fleet::bump(&app, "app-v2");
    let helper_v2 = Fleet::build(&helper);
    let app_v2 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &helper_v2))
        .expect("stage helper v2");
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &app_v2))
        .expect("stage app v2");

    // Serving code is refused: the primary by the fleet pointer, the
    // helper by its named pointer.
    let error = fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[version_ref("staged-app", &app_v1.version)],
        ))
        .expect_err("serving primary is refused");
    assert!(
        format!("{error:#}").contains("serving primary"),
        "unexpected error: {error:#}"
    );
    fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[version_ref("staged-helper", &helper_v1.version)],
        ))
        .expect_err("named version is refused");

    fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &app_v2.version),
                version_ref("staged-helper", &helper_v2.version),
            ],
            None,
        ))
        .expect("activate");

    // Now the generation map refuses v2, and the superseded v1s — staged
    // in the only sense that matters, named by no pointer — discard.
    fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[version_ref("staged-helper", &helper_v2.version)],
        ))
        .expect_err("activated component is refused");
    let discarded = fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[
                version_ref("staged-app", &app_v1.version),
                version_ref("staged-helper", &helper_v1.version),
            ],
        ))
        .expect("discard superseded");
    assert_eq!(discarded.removed.len(), 2);
    assert!(discarded.objects > 0);
    assert!(fleet.version_of("staged-app", &app_v1.version).is_none());
    assert!(fleet
        .version_of("staged-helper", &helper_v1.version)
        .is_none());

    // A discarded version is really gone: activating it fails, and so
    // does discarding what was never there.
    let error = fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-app", &app_v1.version)],
            None,
        ))
        .expect_err("discarded version cannot activate");
    assert!(
        format!("{error:#}").contains("no staged version"),
        "unexpected error: {error:#}"
    );
    fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[version_ref("staged-app", &app_v1.version)],
        ))
        .expect_err("discarding twice fails");
}

#[test]
fn plain_deploy_after_activate_keeps_the_old_shape() {
    let two = two_generations();
    let fleet = &two.fleet;
    fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &two.app_v2),
                version_ref("staged-helper", &two.helper_v2),
            ],
            None,
        ))
        .expect("activate");

    // A plain deploy moves the pointers exactly as before: no components
    // map, top level naming the deployed script.
    let app = fleet.projects.join("app");
    Fleet::bump(&app, "app-v3");
    let app_v3 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v3))
        .expect("deploy app v3");
    let pointer = fleet
        .block(celld::fleet::read_current_pointer(&fleet.bucket))
        .expect("read pointer");
    assert!(pointer.components.is_none(), "plain deploy writes no map");
    assert_eq!(pointer.version, app_v3.version);

    // Serving follows the pointers: the new app with the helper the
    // named pointer still names.
    let graph = fleet.graph();
    assert_eq!(graph.primary.version, app_v3.version);
    let helper = graph
        .cohosted
        .iter()
        .find(|loaded| loaded.script_name == "staged-helper")
        .expect("helper resolves through the named pointer");
    assert_eq!(helper.version, two.helper_v2);
}

#[test]
fn activate_carries_unnamed_scripts() {
    let fleet = Fleet::new();
    let app = fleet.project("app");
    let helper = fleet.project("helper");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v1))
        .expect("deploy helper v1");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    Fleet::bump(&app, "app-v2");
    let app_v2 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &app_v2))
        .expect("stage app v2");

    // Only the app is named; the helper carries over from the legacy
    // named pointer, and the primary stays the app.
    let activated = fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-app", &app_v2.version)],
            None,
        ))
        .expect("activate");
    assert_eq!(activated.primary, "staged-app");
    assert_eq!(activated.components.len(), 2);
    assert_eq!(activated.components["staged-app"].version, app_v2.version);
    assert_eq!(
        activated.components["staged-helper"].version,
        helper_v1.version
    );
    let graph = fleet.graph();
    assert_eq!(graph.primary.version, app_v2.version);
    let helper = graph
        .cohosted
        .iter()
        .find(|loaded| loaded.script_name == "staged-helper")
        .expect("helper resolves");
    assert_eq!(helper.version, helper_v1.version);
    assert_ne!(app_v1.version, app_v2.version);
}

#[test]
fn activate_first_deploy() {
    let fleet = Fleet::new();
    let app = fleet.project("app");
    let helper = fleet.project("helper");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &helper_v1))
        .expect("stage helper");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &app_v1))
        .expect("stage app");

    // No pointer yet and two scripts: the primary is the operator's call.
    let error = fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &app_v1.version),
                version_ref("staged-helper", &helper_v1.version),
            ],
            None,
        ))
        .expect_err("ambiguous primary fails");
    assert!(
        format!("{error:#}").contains("--primary"),
        "unexpected error: {error:#}"
    );
    assert!(
        fleet
            .block(fleet.bucket.get("deploy/current.json"))
            .expect("get")
            .is_none(),
        "failed activation writes no pointer"
    );

    let activated = fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &app_v1.version),
                version_ref("staged-helper", &helper_v1.version),
            ],
            Some("staged-app"),
        ))
        .expect("activate");
    assert_eq!(activated.primary, "staged-app");
    let graph = fleet.graph();
    assert_eq!(graph.primary.script_name, "staged-app");
    assert_eq!(graph.primary.version, app_v1.version);
}

#[test]
fn activate_and_discard_parse_their_arguments() {
    let app_version = "0123456789abcdef";
    let helper_version = "fedcba9876543210";
    let options = celld::deploy::activate_options_from_arguments(
        [
            &format!("staged-app={app_version}"),
            &format!("staged-helper={helper_version}"),
            "--primary",
            "staged-app",
            "--bucket",
            "s3://fleet",
            "--json",
        ]
        .map(str::to_string),
    )
    .expect("parse activate")
    .expect("not help");
    assert_eq!(options.scripts.len(), 2);
    assert_eq!(options.scripts[0].script, "staged-app");
    assert_eq!(options.scripts[0].version, app_version);
    assert_eq!(options.primary.as_deref(), Some("staged-app"));
    assert_eq!(options.bucket.as_deref(), Some("s3://fleet"));
    assert!(options.json);

    let error = celld::deploy::activate_options_from_arguments(
        ["--bucket", "s3://fleet"].map(str::to_string),
    )
    .expect_err("activate needs a version");
    assert!(
        format!("{error:#}").contains("at least one SCRIPT=VERSION"),
        "unexpected error: {error:#}"
    );
    celld::deploy::activate_options_from_arguments(["not-a-ref"].map(str::to_string))
        .expect_err("bare version is refused");
    celld::deploy::activate_options_from_arguments(
        ["Has-Caps=0123456789abcdef"].map(str::to_string),
    )
    .expect_err("bad script name is refused");
    // A version is exactly what `celld deploy` mints: 16 lowercase hex.
    // Anything else — a pointer name, a short prefix, a slash — is a
    // usage error, never a bucket read.
    for version in [
        "current.json",
        "abc",
        "0123456789abcde",
        "0123456789abcdef0",
        "0123456789ABCDEF",
        "zzzzzzzzzzzzzzzz",
        "a/b",
        "",
    ] {
        let error =
            celld::deploy::activate_options_from_arguments([format!("staged-app={version}")])
                .expect_err(&format!("activate refuses version {version:?}"));
        assert!(
            format!("{error:#}").contains("invalid version"),
            "unexpected error for {version:?}: {error:#}"
        );
        let error =
            celld::deploy::discard_options_from_arguments([format!("staged-app={version}")])
                .expect_err(&format!("discard refuses version {version:?}"));
        assert!(
            format!("{error:#}").contains("invalid version"),
            "unexpected error for {version:?}: {error:#}"
        );
    }
    assert!(
        celld::deploy::activate_options_from_arguments(["--help"].map(str::to_string))
            .expect("help parses")
            .is_none()
    );

    let options = celld::deploy::discard_options_from_arguments([
        format!("staged-app={app_version}"),
        "--bucket".to_string(),
        "s3://fleet".to_string(),
    ])
    .expect("parse discard")
    .expect("not help");
    assert_eq!(options.scripts.len(), 1);
    celld::deploy::discard_options_from_arguments(["--bucket", "s3://fleet"].map(str::to_string))
        .expect_err("discard needs a version");
    celld::deploy::discard_options_from_arguments([
        format!("app={app_version}"),
        "--bogus".to_string(),
    ])
    .expect_err("unknown flag is refused");

    // No bucket anywhere is a usage error, not a storage attempt.
    let error = block_on(celld::fleet::run_activate(vec![format!(
        "staged-app={app_version}"
    )]))
    .expect_err("activate needs a bucket");
    assert!(
        format!("{error:#}").contains("--bucket"),
        "unexpected error: {error:#}"
    );
    block_on(celld::fleet::run_discard(vec![format!(
        "staged-app={app_version}"
    )]))
    .expect_err("discard needs a bucket");
}

#[test]
fn queue_attachments_publish_at_activate_time() {
    let fleet = Fleet::new();
    let consumer = fleet.project("consumer");
    let v1 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &v1))
        .expect("deploy consumer v1");
    let attachment = fleet
        .block(celld::fleet::load_queue_consumer_attachment(
            &fleet.bucket,
            "staged-work",
        ))
        .expect("read attachment")
        .expect("v1 attaches its queue");
    assert_eq!(attachment.version, v1.version);

    Fleet::bump(&consumer, "consumer-v2");
    let v2 = Fleet::build(&consumer);
    assert_ne!(v1.version, v2.version);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &v2))
        .expect("stage consumer v2");

    // The attachment still names v1: an adoption in the staging window
    // resolves the serving deployment, never the staged one.
    let attachment = fleet
        .block(celld::fleet::load_queue_consumer_attachment(
            &fleet.bucket,
            "staged-work",
        ))
        .expect("read attachment")
        .expect("v1 still attached");
    assert_eq!(attachment.version, v1.version);
    assert_eq!(fleet.graph().primary.version, v1.version);

    fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-consumer", &v2.version)],
            None,
        ))
        .expect("activate");
    let attachment = fleet
        .block(celld::fleet::load_queue_consumer_attachment(
            &fleet.bucket,
            "staged-work",
        ))
        .expect("read attachment")
        .expect("v2 attached");
    assert_eq!(attachment.version, v2.version);
    assert_eq!(attachment.prefix, v2.prefix);
    assert_eq!(fleet.graph().primary.version, v2.version);
}

#[test]
fn activate_commits_last() {
    // The fleet pointer moves after the attachments converge, and the
    // named pointers move after the commit: aborting between convergence
    // and commit leaves the old pointer serving, the named pointer at
    // its committed version, and the attachments healed back to it — so
    // the serving generation still adopts whole.
    let fleet = Fleet::new();
    let consumer = fleet.project("consumer");
    let v1 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &v1))
        .expect("deploy consumer v1");
    Fleet::bump(&consumer, "consumer-v2");
    let v2 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &v2))
        .expect("stage consumer v2");
    let before = fleet.fleet_pointer_body();

    let error = block_on_with_env(
        "CELLD_TEST_ACTIVATE_ABORT",
        celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-consumer", &v2.version)],
            None,
        ),
    )
    .expect_err("abort fires before the commit");
    assert!(
        format!("{error:#}").contains("injected activate abort"),
        "unexpected error: {error:#}"
    );
    assert_eq!(
        before,
        fleet.fleet_pointer_body(),
        "the fleet pointer still names v1"
    );
    let attachment = fleet
        .attachment("staged-work")
        .expect("attachment healed to the serving version");
    assert_eq!(
        attachment.version, v1.version,
        "the aborted activation heals its attachments before returning"
    );
    assert_eq!(
        fleet.named_pointer_version("staged-consumer"),
        v1.version,
        "named pointers never move before the commit"
    );
    assert_eq!(
        fleet.graph().primary.version,
        v1.version,
        "the serving generation still adopts after the abort"
    );
}

#[test]
fn activate_loses_race_to_concurrent_writer() {
    // A writer that lands between the opening pointer read and the
    // commit wins; the stale activation fails loudly and leaves the
    // rival generation in place instead of overwriting it.
    let two = two_generations();
    let fleet = &two.fleet;
    let error = block_on_with_env(
        "CELLD_TEST_POINTER_RACE",
        celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &two.app_v2),
                version_ref("staged-helper", &two.helper_v2),
            ],
            None,
        ),
    )
    .expect_err("stale commit rejects");
    let error = format!("{error:#}");
    assert!(error.contains("lost a race"), "unexpected error: {error}");
    assert!(
        error.contains("re-run `celld activate`"),
        "unexpected error: {error}"
    );
    let rival: serde_json::Value =
        serde_json::from_slice(&fleet.fleet_pointer_body()).expect("decode rival pointer");
    assert_eq!(rival["celld_test_rival"], true);
    assert_eq!(rival["version"], "ffffffffffffffff");
    // The loser converged nothing away from the winner: named pointers
    // never move before the commit, and the heal pass converges the
    // winner's own named pointer forward.
    assert_eq!(
        fleet.named_pointer_version("staged-app"),
        two.app_v1,
        "the loser leaves the serving named pointers alone"
    );
    assert_eq!(
        fleet.named_pointer_version("staged-helper"),
        two.helper_v1,
        "the loser leaves the serving named pointers alone"
    );
    let rival_named: Option<String> = fleet
        .block(fleet.bucket.get("deploy/staged-rival/current.json"))
        .expect("get rival named pointer")
        .map(|(body, _)| {
            let named: serde_json::Value = serde_json::from_slice(&body).expect("decode");
            named["version"].as_str().expect("version").to_string()
        });
    assert_eq!(
        rival_named.as_deref(),
        Some("ffffffffffffffff"),
        "the loser heals toward the winner before returning"
    );
}

#[test]
fn deploy_loses_race_to_concurrent_writer() {
    // Same for a plain deploy: the fleet-pointer commit CASes on the
    // opening read, so a concurrent writer wins and the loser re-runs.
    let fleet = Fleet::new();
    let app = fleet.project("app");
    let helper = fleet.project("helper");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v1))
        .expect("deploy helper v1");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    Fleet::bump(&app, "app-v2");
    let app_v2 = Fleet::build(&app);

    let error = block_on_with_env(
        "CELLD_TEST_POINTER_RACE",
        celld::deploy::write(&fleet.bucket, &app_v2),
    )
    .expect_err("stale deploy rejects");
    let error = format!("{error:#}");
    assert!(error.contains("lost a race"), "unexpected error: {error}");
    assert!(
        error.contains("re-run `celld deploy`"),
        "unexpected error: {error}"
    );
    let rival: serde_json::Value =
        serde_json::from_slice(&fleet.fleet_pointer_body()).expect("decode rival pointer");
    assert_eq!(rival["celld_test_rival"], true);
}

#[test]
fn component_only_activation_is_visible_to_the_watcher() {
    // Activating the helper alone leaves the primary version and prefix
    // identical — the old watcher predicate compared only those and
    // reported unchanged forever — while the generation map moves. The
    // watcher predicate compares the full generation, and the load path
    // records the identity it read.
    let fleet = Fleet::new();
    let app = fleet.project("app");
    let helper = fleet.project("helper");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v1))
        .expect("deploy helper v1");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    let old = fleet
        .block(celld::fleet::read_current_pointer(&fleet.bucket))
        .expect("read pointer");
    Fleet::bump(&helper, "helper-v2");
    let helper_v2 = Fleet::build(&helper);
    assert_ne!(helper_v1.version, helper_v2.version);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &helper_v2))
        .expect("stage helper v2");
    fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-helper", &helper_v2.version)],
            None,
        ))
        .expect("activate helper only");
    let new = fleet
        .block(celld::fleet::read_current_pointer(&fleet.bucket))
        .expect("read pointer");

    // The premise: primary version and prefix are untouched, so a
    // version-plus-prefix comparison is blind to this activation.
    assert_eq!(old.version, new.version);
    assert_eq!(old.prefix, new.prefix);
    let generation = new.components.as_ref().expect("activated generation");
    assert_eq!(generation["staged-helper"].version, helper_v2.version);

    // The data is present: serving resolves the new helper.
    let graph = fleet.graph();
    let helper = graph
        .cohosted
        .iter()
        .find(|loaded| loaded.script_name == "staged-helper")
        .expect("helper resolves through the service binding");
    assert_eq!(helper.version, helper_v2.version);
    assert_eq!(
        graph.identity,
        celld::generation::ServingIdentity::of_pointer(&new),
        "the load records the identity it read"
    );

    // And the watcher's own predicate adopts it.
    let serving = celld::generation::ServingIdentity::of_pointer(&old);
    assert!(
        !serving.names(&new),
        "a component-only change is a new generation to the watcher"
    );
    assert!(serving.names(&old), "an identical pointer is unchanged");
}

#[test]
fn discard_refuses_byte_prefix_matches_outside_the_version() {
    // S3 listings are byte prefixes: a short version matches every
    // sibling sharing it, and `current.json` matches the script's named
    // pointer. The CLI parser refuses both shapes, but the library
    // entry takes a struct — so the deletion scope refuses them too,
    // and serving code survives either way.
    let two = two_generations();
    let fleet = &two.fleet;
    let short = two.app_v1[..8].to_string();
    assert_ne!(short.len(), two.app_v1.len());

    let error = fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[version_ref("staged-app", &short)],
        ))
        .expect_err("short prefix is refused");
    assert!(
        format!("{error:#}").contains("outside the version directory"),
        "unexpected error: {error:#}"
    );
    assert!(
        fleet.version_of("staged-app", &two.app_v1).is_some(),
        "the serving version survives a short-prefix discard"
    );

    let error = fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[version_ref("staged-app", "current.json")],
        ))
        .expect_err("pointer name is refused");
    assert!(
        format!("{error:#}").contains("outside the version directory"),
        "unexpected error: {error:#}"
    );
    assert_eq!(
        fleet.named_pointer_version("staged-app"),
        two.app_v1,
        "the named pointer survives a pointer-named discard"
    );
}

#[test]
fn discard_refuses_keys_outside_the_version_directory() {
    // Anything a byte-prefix listing returns outside `deploy/<script>/`
    // `<version>/` — a sibling with a longer name, an object at the
    // prefix itself — fails the discard before anything is deleted.
    let two = two_generations();
    let fleet = &two.fleet;
    let sibling = format!("deploy/staged-app/{}junk/evil", two.app_v2);
    let exact = format!("deploy/staged-app/{}", two.app_v2);
    fleet.block(async {
        fleet
            .bucket
            .put(&sibling, vec![0])
            .await
            .expect("plant sibling");
        fleet
            .bucket
            .put(&exact, vec![0])
            .await
            .expect("plant exact key");
    });
    let error = fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[version_ref("staged-app", &two.app_v2)],
        ))
        .expect_err("out-of-scope keys are refused");
    assert!(
        format!("{error:#}").contains("outside the version directory"),
        "unexpected error: {error:#}"
    );
    assert!(
        fleet.version_of("staged-app", &two.app_v2).is_some(),
        "the version survives a refused discard"
    );

    // Control: with the planted keys removed, the same discard succeeds.
    fleet.block(async {
        let gone = fleet.bucket.delete_many(&[sibling, exact]).await;
        assert_eq!(gone.len(), 2);
    });
    fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[version_ref("staged-app", &two.app_v2)],
        ))
        .expect("clean discard succeeds");
    assert!(fleet.version_of("staged-app", &two.app_v2).is_none());
}

#[test]
fn discard_verifies_the_version_manifest() {
    // A version directory whose manifest identifies another script is
    // refused, not deleted.
    let fleet = Fleet::new();
    let version = "deadbeefdeadbeef";
    fleet.block(async {
        fleet
            .bucket
            .put(
                &format!("deploy/staged-app/{version}/manifest.json"),
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "version": version,
                    "script_name": "staged-helper",
                    "main_module": null,
                    "do_classes": [],
                    "sqlite_classes": [],
                    "modules": [],
                    "raw_metadata": {},
                }))
                .expect("render manifest"),
            )
            .await
            .expect("plant manifest");
    });
    let error = fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[version_ref("staged-app", version)],
        ))
        .expect_err("misidentified manifest is refused");
    assert!(
        format!("{error:#}").contains("refusing to discard"),
        "unexpected error: {error:#}"
    );
    assert!(
        fleet.version_of("staged-app", version).is_some(),
        "the refused directory survives"
    );
}

#[test]
fn discard_refuses_a_version_an_attachment_names() {
    // A version no pointer names is still refused while a queue
    // attachment names it: the next adoption through that queue would
    // load missing code.
    let fleet = Fleet::new();
    let consumer = fleet.project("consumer");
    let v1 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &v1))
        .expect("deploy consumer v1");
    Fleet::bump(&consumer, "consumer-v2");
    let v2 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &v2))
        .expect("stage consumer v2");
    let attachment = |version: &str| {
        serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "queue": "staged-work",
            "consumer": {
                "script_name": "staged-consumer",
                "version": version,
                "prefix": format!("deploy/staged-consumer/{version}"),
            },
        }))
        .expect("render attachment")
    };
    // Skew the attachment onto the staged version, as a torn
    // activation would have left it.
    fleet
        .block(fleet.bucket.put(
            "deploy/queues/staged-work/consumer.json",
            attachment(&v2.version),
        ))
        .expect("skew attachment");

    let error = fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[version_ref("staged-consumer", &v2.version)],
        ))
        .expect_err("attached version is refused");
    assert!(
        format!("{error:#}").contains("attached consumer"),
        "unexpected error: {error:#}"
    );
    assert!(
        fleet.version_of("staged-consumer", &v2.version).is_some(),
        "the attached version survives"
    );

    // Control: once the attachment names v1 again, the discard succeeds.
    fleet
        .block(fleet.bucket.put(
            "deploy/queues/staged-work/consumer.json",
            attachment(&v1.version),
        ))
        .expect("restore attachment");
    fleet
        .block(celld::deploy::discard(
            &fleet.bucket,
            &[version_ref("staged-consumer", &v2.version)],
        ))
        .expect("unattached discard succeeds");
}

#[test]
fn failed_activate_with_queue_conflict_moves_nothing() {
    // A queue owned outside the activation fails it before any write:
    // the fleet pointer, the named pointers, and the attachments all
    // still name the old generation.
    let fleet = Fleet::new();
    let rival = fleet.project_renamed("rival", "consumer", "staged-rival");
    let rival_v1 = Fleet::build(&rival);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &rival_v1))
        .expect("deploy rival v1");
    let consumer = fleet.project("consumer");
    Fleet::bump(&consumer, "consumer-v2");
    let v2 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &v2))
        .expect("stage consumer v2");
    let before = fleet.pointer_snapshot();
    let attachment_before = fleet.attachment("staged-work").expect("rival attached");
    assert_eq!(attachment_before.script_name, "staged-rival");

    // The rival serves the queue and is carried unchanged, so the
    // staged consumer cannot take it.
    let error = fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-consumer", &v2.version)],
            None,
        ))
        .expect_err("outside queue owner fails");
    let error = format!("{error:#}");
    assert!(
        error.contains("consumed by serving script"),
        "unexpected error: {error}"
    );
    assert_eq!(
        before,
        fleet.pointer_snapshot(),
        "a failed activation moves no pointer"
    );
    assert_eq!(
        fleet.attachment("staged-work").expect("still attached"),
        attachment_before,
        "a failed activation moves no attachment"
    );
    assert_eq!(fleet.graph().primary.version, rival_v1.version);

    // Same when the owner is not even serving: an orphan attachment
    // still blocks the queue until its owner releases it.
    let fleet = Fleet::new();
    let app = fleet.project("app");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    let consumer = fleet.project("consumer");
    let v1 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &v1))
        .expect("stage consumer v1");
    fleet.block(async {
        fleet
            .bucket
            .put(
                "deploy/queues/staged-work/consumer.json",
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "queue": "staged-work",
                    "consumer": {
                        "script_name": "staged-gone",
                        "version": "0000000000000000",
                        "prefix": "deploy/staged-gone/0000000000000000",
                    },
                }))
                .expect("render attachment"),
            )
            .await
            .expect("plant orphan attachment");
    });
    let before = fleet.pointer_snapshot();
    let error = fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-consumer", &v1.version)],
            None,
        ))
        .expect_err("orphan queue owner fails");
    let error = format!("{error:#}");
    assert!(
        error.contains("already has consumer"),
        "unexpected error: {error}"
    );
    assert_eq!(
        before,
        fleet.pointer_snapshot(),
        "a failed activation moves no pointer"
    );
}

#[test]
fn activate_hands_a_queue_between_staged_scripts_in_any_order() {
    // A queue handed from one staged script to another resolves the
    // same regardless of CLI argument order: the releasing script's
    // new manifest drops the queue, so the handoff validates before
    // anything publishes.
    for order in [false, true] {
        let fleet = Fleet::new();
        let consumer = fleet.project("consumer");
        let v1 = Fleet::build(&consumer);
        fleet
            .block(celld::deploy::write(&fleet.bucket, &v1))
            .expect("deploy consumer v1");
        Fleet::drop_queues(&consumer);
        Fleet::bump(&consumer, "consumer-v2-no-queues");
        let v2 = Fleet::build(&consumer);
        assert_ne!(v1.version, v2.version);
        fleet
            .block(celld::deploy::write_staged(&fleet.bucket, &v2))
            .expect("stage consumer v2");
        let rival = fleet.project_renamed("rival", "consumer", "staged-rival");
        let rival_v1 = Fleet::build(&rival);
        fleet
            .block(celld::deploy::write_staged(&fleet.bucket, &rival_v1))
            .expect("stage rival v1");

        let scripts = [
            version_ref("staged-consumer", &v2.version),
            version_ref("staged-rival", &rival_v1.version),
        ];
        let scripts = if order {
            vec![scripts[1].clone(), scripts[0].clone()]
        } else {
            scripts.to_vec()
        };
        fleet
            .block(celld::deploy::activate(&fleet.bucket, &scripts, None))
            .expect("handoff activates in either order");
        let attachment = fleet
            .attachment("staged-work")
            .expect("queue attached after handoff");
        assert_eq!(attachment.script_name, "staged-rival");
        assert_eq!(attachment.version, rival_v1.version);
        assert_eq!(fleet.named_pointer_version("staged-consumer"), v2.version);
        assert_eq!(
            fleet.named_pointer_version("staged-rival"),
            rival_v1.version
        );
        let pointer = fleet
            .block(celld::fleet::read_current_pointer(&fleet.bucket))
            .expect("read pointer");
        let generation = pointer.components.expect("activated generation");
        assert_eq!(generation["staged-consumer"].version, v2.version);
        assert_eq!(generation["staged-rival"].version, rival_v1.version);
    }
}

#[test]
fn aborted_activate_leaves_nothing_visible_and_rerun_converges() {
    // A crash after convergence but before the commit leaves the old
    // generation serving — nothing visible: the fleet pointer, every
    // named pointer, and the adoption all still name v1 — and re-running
    // the same activation converges and commits.
    let two = two_generations();
    let fleet = &two.fleet;
    let before = fleet.fleet_pointer_body();
    block_on_with_env(
        "CELLD_TEST_ACTIVATE_ABORT",
        celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &two.app_v2),
                version_ref("staged-helper", &two.helper_v2),
            ],
            None,
        ),
    )
    .expect_err("abort fires before the commit");
    assert_eq!(
        before,
        fleet.fleet_pointer_body(),
        "an aborted activation moves no fleet pointer"
    );
    assert_eq!(
        fleet.named_pointer_version("staged-app"),
        two.app_v1,
        "an aborted activation moves no named pointer"
    );
    assert_eq!(
        fleet.named_pointer_version("staged-helper"),
        two.helper_v1,
        "an aborted activation moves no named pointer"
    );
    let graph = fleet.graph();
    assert_eq!(graph.primary.version, two.app_v1);
    let helper = graph
        .cohosted
        .iter()
        .find(|loaded| loaded.script_name == "staged-helper")
        .expect("helper resolves through the service binding");
    assert_eq!(
        helper.version, two.helper_v1,
        "the legacy path never serves the aborted helper v2 under a v1 pointer"
    );

    fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &two.app_v2),
                version_ref("staged-helper", &two.helper_v2),
            ],
            None,
        ))
        .expect("re-run converges");
    let graph = fleet.graph();
    assert_eq!(graph.primary.version, two.app_v2);
    let helper = graph
        .cohosted
        .iter()
        .find(|loaded| loaded.script_name == "staged-helper")
        .expect("helper resolves");
    assert_eq!(helper.version, two.helper_v2);
}

#[test]
fn activate_heals_carried_attachments_and_named_pointers() {
    // Skew from under a committed generation — a named pointer and an
    // attachment naming superseded versions — heals on the next
    // activate, even one that names neither skewed script.
    let fleet = Fleet::new();
    let helper = fleet.project("helper");
    let helper_v0 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v0))
        .expect("deploy helper v0");
    let consumer = fleet.project("consumer");
    let consumer_v0 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &consumer_v0))
        .expect("deploy consumer v0");
    Fleet::bump(&helper, "helper-v1");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v1))
        .expect("deploy helper v1");
    Fleet::bump(&consumer, "consumer-v1");
    let consumer_v1 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &consumer_v1))
        .expect("deploy consumer v1");
    let app = fleet.project("app");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &app_v1.version),
                version_ref("staged-helper", &helper_v1.version),
                version_ref("staged-consumer", &consumer_v1.version),
            ],
            None,
        ))
        .expect("activate v1s");

    // Skew the carried scripts' derivative state back to v0.
    fleet.block(async {
        fleet
            .bucket
            .put(
                "deploy/staged-helper/current.json",
                serde_json::to_vec(&serde_json::json!({
                    "script_name": "staged-helper",
                    "version": helper_v0.version,
                    "prefix": format!("deploy/staged-helper/{}", helper_v0.version),
                    "rollout": { "percent": 100 },
                }))
                .expect("render pointer"),
            )
            .await
            .expect("skew named pointer");
        fleet
            .bucket
            .put(
                "deploy/queues/staged-work/consumer.json",
                serde_json::to_vec(&serde_json::json!({
                    "schema_version": 1,
                    "queue": "staged-work",
                    "consumer": {
                        "script_name": "staged-consumer",
                        "version": consumer_v0.version,
                        "prefix": format!("deploy/staged-consumer/{}", consumer_v0.version),
                    },
                }))
                .expect("render attachment"),
            )
            .await
            .expect("skew attachment");
    });
    assert_eq!(
        fleet.named_pointer_version("staged-helper"),
        helper_v0.version
    );

    Fleet::bump(&app, "app-v2");
    let app_v2 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &app_v2))
        .expect("stage app v2");
    fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-app", &app_v2.version)],
            None,
        ))
        .expect("activate unrelated app v2");
    assert_eq!(
        fleet.named_pointer_version("staged-helper"),
        helper_v1.version,
        "carried named pointer heals"
    );
    let attachment = fleet
        .attachment("staged-work")
        .expect("attached after heal");
    assert_eq!(
        attachment.version, consumer_v1.version,
        "carried attachment heals"
    );
    let pointer = fleet
        .block(celld::fleet::read_current_pointer(&fleet.bucket))
        .expect("read pointer");
    assert_eq!(pointer.version, app_v2.version);
}

#[test]
fn discard_detects_a_pointer_move_during_delete() {
    // A concurrent writer that moves the fleet pointer between the
    // pre-delete check and the re-check fails the discard loudly,
    // even when the deleted version stays unreferenced.
    let two = two_generations();
    let fleet = &two.fleet;
    let error = block_on_with_env(
        "CELLD_TEST_DISCARD_RACE",
        celld::deploy::discard(&fleet.bucket, &[version_ref("staged-app", &two.app_v2)]),
    )
    .expect_err("pointer move is reported");
    let error = format!("{error:#}");
    assert!(
        error.contains("moved the fleet pointer"),
        "unexpected error: {error}"
    );
    assert!(
        error.contains("re-run `celld discard"),
        "unexpected error: {error}"
    );
    assert!(
        fleet.version_of("staged-app", &two.app_v2).is_none(),
        "the unreferenced version was still deleted"
    );
    let rival: serde_json::Value =
        serde_json::from_slice(&fleet.fleet_pointer_body()).expect("decode rival pointer");
    assert_eq!(rival["celld_test_rival"], true);
}

#[test]
fn activate_loser_heals_its_queue_attachments() {
    // A race loser published queue attachments toward a generation that
    // never committed. Before its re-run error returns it heals them
    // toward the committed generation, so the next adoption succeeds
    // instead of wedging on a queue mismatch.
    let fleet = Fleet::new();
    let consumer = fleet.project("consumer");
    let v1 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &v1))
        .expect("deploy consumer v1");
    Fleet::bump(&consumer, "consumer-v2");
    let v2 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &v2))
        .expect("stage consumer v2");

    let error = block_on_with_env(
        "CELLD_TEST_POINTER_RACE",
        celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-consumer", &v2.version)],
            None,
        ),
    )
    .expect_err("stale commit rejects");
    let error = format!("{error:#}");
    assert!(error.contains("lost a race"), "unexpected error: {error}");
    assert!(
        error.contains("re-run `celld activate`"),
        "unexpected error: {error}"
    );
    let rival: serde_json::Value =
        serde_json::from_slice(&fleet.fleet_pointer_body()).expect("decode rival pointer");
    assert_eq!(rival["celld_test_rival"], true);
    let attachment = fleet
        .attachment("staged-work")
        .expect("attachment healed to the serving version");
    assert_eq!(
        attachment.version, v1.version,
        "the loser heals its attachments toward the committed generation"
    );
    assert_eq!(
        fleet.named_pointer_version("staged-consumer"),
        v1.version,
        "the loser moves no named pointer before its failed commit"
    );
}

#[test]
fn deploy_loser_heals_the_serving_generation() {
    // Same for a plain deploy: the loser heals its attachments toward
    // the winner and moves no named pointer before its failed commit.
    let fleet = Fleet::new();
    let consumer = fleet.project("consumer");
    let v1 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &v1))
        .expect("deploy consumer v1");
    Fleet::bump(&consumer, "consumer-v2");
    let v2 = Fleet::build(&consumer);

    let error = block_on_with_env(
        "CELLD_TEST_POINTER_RACE",
        celld::deploy::write(&fleet.bucket, &v2),
    )
    .expect_err("stale deploy rejects");
    let error = format!("{error:#}");
    assert!(error.contains("lost a race"), "unexpected error: {error}");
    assert!(
        error.contains("re-run `celld deploy`"),
        "unexpected error: {error}"
    );
    let rival: serde_json::Value =
        serde_json::from_slice(&fleet.fleet_pointer_body()).expect("decode rival pointer");
    assert_eq!(rival["celld_test_rival"], true);
    let attachment = fleet
        .attachment("staged-work")
        .expect("attachment healed to the serving version");
    assert_eq!(
        attachment.version, v1.version,
        "the loser heals its attachments toward the committed generation"
    );
    assert_eq!(
        fleet.named_pointer_version("staged-consumer"),
        v1.version,
        "the loser moves no named pointer before its failed commit"
    );
}

#[test]
fn crashed_activate_heals_on_the_next_activate() {
    // Crash residue is attachments without a commit — named pointers
    // never move before one. The residue is loud (the serving adoption
    // fails on the mismatch) and the next activate converges the same
    // state toward its own generation, which heals it. This pins the
    // crash contract; the heal itself predates this change.
    let fleet = Fleet::new();
    let consumer = fleet.project("consumer");
    let v1 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &v1))
        .expect("deploy consumer v1");
    Fleet::bump(&consumer, "consumer-v2");
    let v2 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &v2))
        .expect("stage consumer v2");
    fleet.skew_attachment("staged-work", "staged-consumer", &v2.version);

    let error = fleet.load_error();
    assert!(
        error.contains("is already loaded"),
        "residue fails loudly, not silently: {error}"
    );
    fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-consumer", &v2.version)],
            None,
        ))
        .expect("the next activate heals");
    assert_eq!(fleet.graph().primary.version, v2.version);
    let attachment = fleet
        .attachment("staged-work")
        .expect("attached after heal");
    assert_eq!(attachment.version, v2.version);
    assert_eq!(fleet.named_pointer_version("staged-consumer"), v2.version);
}

#[test]
fn crashed_activate_heals_on_plain_deploy() {
    // Same residue, healed by a plain deploy of the skewed script: the
    // deploy converges its attachments and named pointer toward the
    // version it commits.
    let fleet = Fleet::new();
    let consumer = fleet.project("consumer");
    let v1 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &v1))
        .expect("deploy consumer v1");
    Fleet::bump(&consumer, "consumer-v2");
    let v2 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &v2))
        .expect("stage consumer v2");
    fleet.skew_attachment("staged-work", "staged-consumer", &v2.version);
    let error = fleet.load_error();
    assert!(
        error.contains("is already loaded"),
        "residue fails loudly, not silently: {error}"
    );

    Fleet::bump(&consumer, "consumer-v3");
    let v3 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &v3))
        .expect("plain deploy heals");
    assert_eq!(fleet.graph().primary.version, v3.version);
    let attachment = fleet
        .attachment("staged-work")
        .expect("attached after heal");
    assert_eq!(attachment.version, v3.version);
    assert_eq!(fleet.named_pointer_version("staged-consumer"), v3.version);
}

#[test]
fn activate_refuses_a_staged_manifest_that_vanished_mid_activate() {
    // A discard that lands after the opening reads and before the
    // commit: the pre-commit manifest re-check refuses to name deleted
    // code, and the serving generation stays adoptable.
    let two = two_generations();
    let fleet = &two.fleet;
    let before = fleet.fleet_pointer_body();
    let error = block_on_with_env(
        "CELLD_TEST_DISCARD_MID_ACTIVATE",
        celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &two.app_v2),
                version_ref("staged-helper", &two.helper_v2),
            ],
            None,
        ),
    )
    .expect_err("vanished staged manifest refuses the commit");
    let error = format!("{error:#}");
    assert!(
        error.contains("vanished before the commit"),
        "unexpected error: {error}"
    );
    assert!(
        error.contains("re-run `celld deploy --no-activate`"),
        "unexpected error: {error}"
    );
    assert_eq!(
        before,
        fleet.fleet_pointer_body(),
        "no commit names deleted code"
    );
    let graph = fleet.graph();
    assert_eq!(graph.primary.version, two.app_v1);
    let helper = graph
        .cohosted
        .iter()
        .find(|loaded| loaded.script_name == "staged-helper")
        .expect("helper resolves through the service binding");
    assert_eq!(helper.version, two.helper_v1);
}

#[test]
fn activate_refuses_an_unreadable_carried_manifest() {
    // A carried version whose manifest is gone refuses the commit
    // instead of wedging the fleet on code that is not there.
    let fleet = Fleet::new();
    let app = fleet.project("app");
    let helper = fleet.project("helper");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v1))
        .expect("deploy helper v1");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    fleet
        .block(fleet.bucket.delete(&format!(
            "deploy/staged-helper/{}/manifest.json",
            helper_v1.version
        )))
        .expect("delete carried manifest");
    Fleet::bump(&app, "app-v2");
    let app_v2 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &app_v2))
        .expect("stage app v2");

    let before = fleet.fleet_pointer_body();
    let error = fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-app", &app_v2.version)],
            None,
        ))
        .expect_err("unreadable carried manifest refuses the commit");
    assert!(
        format!("{error:#}").contains("has no readable manifest"),
        "unexpected error: {error:#}"
    );
    assert_eq!(
        before,
        fleet.fleet_pointer_body(),
        "no commit names deleted code"
    );
}

#[test]
fn discarded_serving_code_fails_loud_and_redeploys() {
    // No re-check covers a commit landing after it: that interleaving
    // names deleted code. The backstop is loud (adoption refuses a
    // generation whose manifest is missing, never serving wrong code)
    // and recoverable (the same source rebuilds the same version).
    let fleet = Fleet::new();
    let app = fleet.project("app");
    let helper = fleet.project("helper");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v1))
        .expect("deploy helper v1");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    Fleet::bump(&helper, "helper-v2");
    Fleet::bump(&app, "app-v2");
    let helper_v2 = Fleet::build(&helper);
    let app_v2 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &helper_v2))
        .expect("stage helper v2");
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &app_v2))
        .expect("stage app v2");
    fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &app_v2.version),
                version_ref("staged-helper", &helper_v2.version),
            ],
            None,
        ))
        .expect("activate");
    assert_eq!(fleet.graph().primary.version, app_v2.version);

    // The sliver: the version directory deleted after the commit, with
    // exactly the discard's list-and-delete mechanics.
    let keys: Vec<String> = fleet
        .block(
            fleet
                .bucket
                .list(&format!("deploy/staged-app/{}/", app_v2.version)),
        )
        .expect("list version dir")
        .into_iter()
        .map(|object| object.location.to_string())
        .collect();
    assert!(!keys.is_empty(), "the version directory lists its objects");
    let gone = fleet.block(fleet.bucket.delete_many(&keys));
    assert_eq!(gone.len(), keys.len(), "the delete removes the version");

    let error = fleet.load_error();
    assert!(
        error.contains("no such key")
            && error.contains(&format!(
                "deploy/staged-app/{}/manifest.json",
                app_v2.version
            )),
        "adoption names the missing manifest loudly: {error}"
    );

    let rebuilt = Fleet::build(&app);
    assert_eq!(
        rebuilt.version, app_v2.version,
        "the same source reproduces the version"
    );
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &rebuilt))
        .expect("re-stage");
    let again = fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &app_v2.version),
                version_ref("staged-helper", &helper_v2.version),
            ],
            None,
        ))
        .expect("re-activate");
    assert!(
        again.unchanged,
        "the pointer named this generation all along"
    );
    assert_eq!(fleet.graph().primary.version, app_v2.version);
}

/// A fleet serving one activated generation where the app produces to
/// `staged-work` and the consumer is referenced nowhere else: the
/// standard producer shape, in which a skewed attachment used to load
/// silently because the consumer was not otherwise in the graph.
struct ProducerGeneration {
    fleet: Fleet,
    consumer: PathBuf,
    app_v1: String,
    helper_v1: String,
    consumer_v1: String,
}

fn producer_generation() -> ProducerGeneration {
    let fleet = Fleet::new();
    let app = fleet.project("app");
    Fleet::add_queue_producer(&app, "WORK", "staged-work");
    let helper = fleet.project("helper");
    let consumer = fleet.project("consumer");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v1))
        .expect("deploy helper v1");
    let consumer_v1 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &consumer_v1))
        .expect("deploy consumer v1");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[
                version_ref("staged-app", &app_v1.version),
                version_ref("staged-helper", &helper_v1.version),
                version_ref("staged-consumer", &consumer_v1.version),
            ],
            Some("staged-app"),
        ))
        .expect("activate v1 generation");
    ProducerGeneration {
        fleet,
        consumer,
        app_v1: app_v1.version,
        helper_v1: helper_v1.version,
        consumer_v1: consumer_v1.version,
    }
}

#[test]
fn skewed_queue_consumer_fails_loud_in_producer_graph() {
    // The G1 shape: the components map names consumer v1, but crash
    // residue (exact attachment bytes, no commit) names staged
    // consumer v2, and the consumer is referenced nowhere else. The
    // adoption must refuse loudly — naming the generation it was
    // promised against the deployment the queue names — not serve
    // old-app + new-consumer as a silent mix. The next activate of
    // the skewed script heals.
    let producer = producer_generation();
    let fleet = &producer.fleet;
    assert_eq!(
        fleet.named_pointer_version("staged-helper"),
        producer.helper_v1,
        "named pointers converge at activate time"
    );
    Fleet::bump(&producer.consumer, "consumer-v2");
    let consumer_v2 = Fleet::build(&producer.consumer);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &consumer_v2))
        .expect("stage consumer v2");
    fleet.skew_attachment("staged-work", "staged-consumer", &consumer_v2.version);

    let error = fleet.load_error();
    assert!(
        error.contains("activated generation names")
            && error.contains(&producer.consumer_v1)
            && error.contains(&consumer_v2.version),
        "residue fails loudly against the map, not silently: {error}"
    );
    fleet
        .block(celld::deploy::activate(
            &fleet.bucket,
            &[version_ref("staged-consumer", &consumer_v2.version)],
            None,
        ))
        .expect("the next activate heals");
    let graph = fleet.graph();
    assert_eq!(graph.primary.version, producer.app_v1);
    let consumer = graph
        .cohosted
        .iter()
        .find(|loaded| loaded.script_name == "staged-consumer")
        .expect("consumer resolves through the healed attachment");
    assert_eq!(consumer.version, consumer_v2.version);
}

#[test]
fn queue_consumer_missing_from_generation_fails_loud() {
    // The omission half of the queue pin: crash residue names a staged
    // consumer the activated generation never named (an activate that
    // died after publishing its attachments, before its commit). The
    // adoption refuses it exactly like an omitted service target.
    let producer = producer_generation();
    let fleet = &producer.fleet;
    let extra = fleet.project_renamed("extra", "consumer", "staged-extra");
    let staged = Fleet::build(&extra);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &staged))
        .expect("stage the new consumer");
    fleet.skew_attachment("staged-work", "staged-extra", &staged.version);

    let error = fleet.load_error();
    assert!(
        error.contains("is not in the activated generation") && error.contains("staged-extra"),
        "an omitted consumer fails loudly: {error}"
    );
}

#[test]
fn unrelated_plain_deploy_does_not_heal_cross_script_residue() {
    // A plain deploy converges only its own script's attachments, so
    // crash residue on another script's queue survives it as the same
    // loud wedge: recovery is an activate, or a deploy of the skewed
    // script. This pins the scoped crash contract.
    let fleet = Fleet::new();
    let app = fleet.project("app");
    Fleet::add_service_binding(&app, "CONSUMER", "staged-consumer");
    let helper = fleet.project("helper");
    let consumer = fleet.project("consumer");
    let helper_v1 = Fleet::build(&helper);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &helper_v1))
        .expect("deploy helper v1");
    let consumer_v1 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &consumer_v1))
        .expect("deploy consumer v1");
    let app_v1 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v1))
        .expect("deploy app v1");
    Fleet::bump(&consumer, "consumer-v2");
    let consumer_v2 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write_staged(&fleet.bucket, &consumer_v2))
        .expect("stage consumer v2");
    fleet.skew_attachment("staged-work", "staged-consumer", &consumer_v2.version);

    let error = fleet.load_error();
    assert!(
        error.contains("is already loaded"),
        "residue wedges loudly: {error}"
    );
    Fleet::bump(&app, "app-v2");
    let app_v2 = Fleet::build(&app);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &app_v2))
        .expect("unrelated deploy succeeds");
    let error = fleet.load_error();
    assert!(
        error.contains("is already loaded") && error.contains(&consumer_v2.version),
        "the wedge survives an unrelated deploy: {error}"
    );
    Fleet::bump(&consumer, "consumer-v3");
    let consumer_v3 = Fleet::build(&consumer);
    fleet
        .block(celld::deploy::write(&fleet.bucket, &consumer_v3))
        .expect("deploy of the skewed script heals");
    assert_eq!(fleet.graph().primary.version, consumer_v3.version);
    let attachment = fleet
        .attachment("staged-work")
        .expect("attached after heal");
    assert_eq!(attachment.version, consumer_v3.version);
}
