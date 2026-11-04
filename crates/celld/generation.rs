// Modified by ryuzcorp (ryuzcorp/celld): shared runtime cells run in a dedicated runtime isolate pool; runtime compat is the union over declaring scripts; staged activation; deployment cache scope key; staged invoke loads a services-only staged graph.
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! One application deployment as the running process holds it.
//!
//! A [`Generation`] is everything a node derives from a deployment: the
//! compiled Worker configurations, the isolate pools they run in, the
//! Durable Object class registry, the service-binding graph, the asset
//! resolvers, and the cron schedule. A node serves exactly one current
//! generation and reaches it through a snapshot, so a request that started
//! on one generation finishes on it even after the node adopts another.
//!
//! Boot and reload construct a generation through the same two functions,
//! [`DeploymentGraph::load`] and `Generation::build`. Nothing else reads a
//! deployment manifest into runtime state. A value a deployment implies
//! therefore has one place it can be computed, and a reload cannot miss what
//! a boot did, because there is no second path for it to miss.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use anyhow::{anyhow, Context};

use crate::assets::AssetResolver;
use crate::bucket::Bucket;
use crate::fleet::{self, LoadedDeployment};
use crate::js::{Compat, WorkerConfig, WorkerConfigOptions};
use crate::protocol::{ComponentRef, DeployPointer, Rollout};

/// Which generation, within this process. Monotonic from one at boot; never
/// reused, never persisted, and never compared across nodes — the fleet-wide
/// identity of a deployment is its version string.
pub type GenerationId = u64;

/// The generation a node boots on. Later generations count up from it.
pub const FIRST_GENERATION: GenerationId = 1;

/// Ask the node to adopt the deployment `deploy/current.json` names now.
pub struct ReloadRequest {
    /// Rebuild even when the pointer names the current deployment, so a
    /// memoised failure is retried and fresh isolates replace the serving
    /// ones. `POST /reload` sets it; a poll tick and a managed nudge do not.
    pub force: bool,
    /// Where to report the outcome. A poll tick has nobody to tell.
    pub reply: Option<tokio::sync::oneshot::Sender<ReloadOutcome>>,
}

/// What one adoption attempt concluded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReloadOutcome {
    /// A new generation serves. Requests admitted from now on use it.
    Adopted {
        generation: GenerationId,
        version: String,
        prefix: String,
    },
    /// The pointer names the deployment already serving and the request did
    /// not force a rebuild.
    Unchanged {
        generation: GenerationId,
        version: String,
    },
    /// The deployment did not build. The current generation is untouched.
    Failed {
        version: String,
        prefix: String,
        error: String,
    },
}

pub type ReloadSender = tokio::sync::mpsc::UnboundedSender<ReloadRequest>;
pub type ReloadReceiver = tokio::sync::mpsc::UnboundedReceiver<ReloadRequest>;

pub fn reload_channel() -> (ReloadSender, ReloadReceiver) {
    tokio::sync::mpsc::unbounded_channel()
}

/// Ask for a poll now, without waiting for the outcome. A managed
/// `deployment_current` message and a successful apply both end here: the
/// pointer is the authority, and the nudge only shortens the wait for it.
pub fn nudge(reload: &ReloadSender) {
    let _ = reload.send(ReloadRequest {
        force: false,
        reply: None,
    });
}

/// How often a node re-reads the pointer without a nudge. For a standalone
/// node this is the deploy latency; for a managed node it is the backstop
/// behind the push. `CELLD_DEPLOY_POLL_S`, default 30.
pub fn poll_interval() -> std::time::Duration {
    let seconds = crate::env_vars::positive_or("CELLD_DEPLOY_POLL_S", 30u64)
        .expect("validated CELLD_DEPLOY_POLL_S");
    std::time::Duration::from_secs(seconds)
}

/// How long a resident cell may keep running a superseded generation after
/// the node adopts a new one, before its swap is forced: its activity
/// cancelled and its regular WebSockets closed with 1012. Zero forces every
/// resident cell at the flip, which is what a Cloudflare deployment does.
/// `CELLD_DEPLOY_MAX_AGE_S`, default 60.
pub fn max_age() -> std::time::Duration {
    // Not `positive_or`: zero is meaningful here.
    let seconds = crate::env_vars::with_default("CELLD_DEPLOY_MAX_AGE_S", 60u64)
        .expect("validated CELLD_DEPLOY_MAX_AGE_S");
    std::time::Duration::from_secs(seconds)
}

/// The scripts one deployment reaches: the primary script plus every
/// service-binding target and queue consumer it declares, transitively.
///
/// [`DeploymentGraph::load`] is the only bucket walk, so the boot path and
/// the reload path cannot resolve a deployment's dependencies differently.
pub struct DeploymentGraph {
    /// The pointer identity this graph was loaded from. A generation
    /// records it at build, and the pointer watcher compares it, so a
    /// component-only activation — same primary version and prefix, new
    /// map — is adopted instead of mistaken for the serving generation.
    pub identity: ServingIdentity,
    /// The fleet identity this graph was loaded from: the bucket's
    /// scheme, name, and key prefix. Part of the deployment cache scope
    /// key, so two fleets serving deployments with the same versions
    /// never share cache entries, even if the store outlives its
    /// process one day. Local-script mode has no bucket and carries
    /// `local` instead; its identity already names the script path.
    pub fleet: String,
    pub primary: LoadedDeployment,
    pub cohosted: Vec<LoadedDeployment>,
}

/// Everything about a fleet pointer an adoption decision reads: the
/// primary it names and the whole activated generation, not just the
/// primary's version and prefix. Comparing less pins a node to a stale
/// generation forever: the pointer never moves again, so a skipped
/// component-only change is never retried.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServingIdentity {
    pub script_name: Option<String>,
    pub version: String,
    pub prefix: String,
    pub components: Option<BTreeMap<String, ComponentRef>>,
}

impl ServingIdentity {
    pub fn of_pointer(pointer: &DeployPointer) -> Self {
        Self {
            script_name: pointer.script_name.clone(),
            version: pointer.version.clone(),
            prefix: pointer.prefix.clone(),
            components: pointer.components.clone(),
        }
    }

    /// Whether `pointer` names this same serving generation: the same
    /// primary and the same component map. The watcher adopts on any
    /// difference; rebuilding on a pointer that names identical content
    /// is the safe direction, skipping on one that differs is not.
    pub fn names(&self, pointer: &DeployPointer) -> bool {
        *self == Self::of_pointer(pointer)
    }

    /// The deployment cache scope key: a deterministic rendering of the
    /// fleet identity plus this identity, the same for every generation
    /// adopted from them. Every field is length-prefixed and the
    /// components map iterates in `BTreeMap` order, so distinct
    /// (fleet, identity) pairs render distinct keys and no two fields can
    /// blur into each other. Two fleets serving the same versions still
    /// differ in fleet and never share entries. The host stamps the key
    /// into each tenant script's isolate state at build; script input
    /// never names it, so no script can reach another deployment's
    /// entries. A rollout percentage is not part of the identity and
    /// changes no key, matching [`names`](Self::names).
    pub fn cache_scope_key(&self, fleet: &str) -> String {
        fn push_field(key: &mut String, value: &str) {
            key.push_str(&value.len().to_string());
            key.push(':');
            key.push_str(value);
        }
        let mut key = String::from("deployment:");
        push_field(&mut key, fleet);
        match &self.script_name {
            Some(script) => {
                key.push('1');
                push_field(&mut key, script);
            }
            None => key.push('0'),
        }
        push_field(&mut key, &self.version);
        push_field(&mut key, &self.prefix);
        match &self.components {
            Some(components) => {
                key.push('1');
                key.push_str(&components.len().to_string());
                key.push(':');
                for (script, component) in components {
                    push_field(&mut key, script);
                    push_field(&mut key, &component.version);
                    push_field(&mut key, &component.prefix);
                }
            }
            None => key.push('0'),
        }
        key
    }
}

impl DeploymentGraph {
    /// One script and nothing else: the local-script mode a runtime test
    /// starts from a file, which declares no services and has no bucket to
    /// resolve them from.
    pub fn single(primary: LoadedDeployment) -> Self {
        let identity = ServingIdentity {
            script_name: Some(primary.script_name.clone()),
            version: primary.version.clone(),
            prefix: primary.prefix.clone(),
            components: None,
        };
        Self {
            identity,
            fleet: "local".to_string(),
            primary,
            cohosted: Vec::new(),
        }
    }

    /// Resolve the fleet-wide pointer and every script it depends on.
    ///
    /// A service binding names a script, and that script's own pointer names
    /// its deployment — unless the fleet-wide pointer carries an activated
    /// generation (`celld activate`), in which case every script resolves
    /// from that one pointer read and the adoption is atomic across
    /// scripts. A queue dependency names a queue, and the queue's consumer
    /// attachment names an exact deployment. The walk refuses a script that
    /// resolves twice and a queue attached to a deployment other than the
    /// one already loaded, because either would give one script two bodies
    /// in one process. Under an activated generation it also refuses an
    /// attachment the components map does not name, so crash residue (or
    /// the normal pre-commit window) fails the adoption instead of serving
    /// one generation's primary beside another's consumer.
    pub async fn load(bucket: &Bucket, node: String) -> anyhow::Result<Self> {
        let pointer = fleet::read_current_pointer(bucket).await?;
        let primary =
            fleet::load_pinned_worker(bucket, &pointer, fleet::CURRENT_POINTER_KEY, node.clone())
                .await?;
        let primary_script = primary.script_name.clone();
        let components = pointer.components.clone();
        // The map claims to name every script's version. A primary it
        // omits or disagrees with is a corrupt pointer: fail the adoption
        // rather than serve a generation the pointer does not describe.
        if let Some(generation) = &components {
            let pinned = generation.get(&primary_script).with_context(|| {
                format!(
                    "fleet pointer names primary script {primary_script:?} but no version for it"
                )
            })?;
            anyhow::ensure!(
                pinned.version == pointer.version && pinned.prefix == pointer.prefix,
                "fleet pointer primary {} disagrees with its activated generation entry {}",
                pointer.version,
                pinned.version
            );
        }
        let mut loaded_scripts = BTreeMap::from([(primary_script.clone(), primary.prefix.clone())]);
        let mut loaded_consumers =
            BTreeMap::from([(primary_script.clone(), consumed_queues(&primary.options))]);
        let mut visited_queues = BTreeSet::new();
        let mut dependencies = dependencies_of(&primary);
        let mut cohosted = Vec::new();
        while let Some(dependency) = dependencies.pop_front() {
            let loaded = match dependency {
                Dependency::Service(target) => {
                    if target == primary_script || loaded_scripts.contains_key(&target) {
                        continue;
                    }
                    let pinned = components.as_ref().and_then(|generation| {
                        generation.get(&target).map(|component| DeployPointer {
                            script_name: Some(target.clone()),
                            version: component.version.clone(),
                            prefix: component.prefix.clone(),
                            rollout: Rollout { percent: 100 },
                            components: None,
                        })
                    });
                    let loaded = match (pinned, components.is_some()) {
                        (Some(pinned), _) => fleet::load_pinned_worker(
                            bucket,
                            &pinned,
                            &format!("deploy/{target}/current.json"),
                            node.clone(),
                        )
                        .await
                        .with_context(|| format!("load service binding target {target}"))?,
                        (None, true) => anyhow::bail!(
                            "service binding target {target} is not in the activated generation; \
                             activate a generation that names every script"
                        ),
                        (None, false) => fleet::load_named_worker(bucket, &target, node.clone())
                            .await
                            .with_context(|| format!("load service binding target {target}"))?,
                    };
                    if loaded.script_name != target {
                        anyhow::bail!(
                            "service pointer {target} resolved script {}",
                            loaded.script_name
                        );
                    }
                    loaded
                }
                Dependency::Queue(queue) => {
                    if !visited_queues.insert(queue.clone()) {
                        continue;
                    }
                    let declared_by = loaded_consumers
                        .iter()
                        .find_map(|(script, queues)| queues.contains(&queue).then_some(script));
                    let Some(consumer) =
                        fleet::load_queue_consumer_attachment(bucket, &queue).await?
                    else {
                        if let Some(script) = declared_by {
                            anyhow::bail!(
                                "script {script:?} consumes queue {queue:?}, but the queue has no active consumer attachment; re-run `celld activate` (or `celld deploy` for a single script) to converge the queue attachments"
                            );
                        }
                        continue;
                    };
                    if let Some(script) = declared_by {
                        anyhow::ensure!(
                            script == &consumer.script_name,
                            "queue {queue:?} is attached to script {:?}, but loaded script {script:?} also consumes it",
                            consumer.script_name
                        );
                    }
                    if let Some(prefix) = loaded_scripts.get(&consumer.script_name) {
                        anyhow::ensure!(
                            prefix == &consumer.prefix,
                            "queue {queue:?} is attached to deployment {} of script {:?}, but deployment {prefix} is already loaded; re-run `celld activate` (or `celld deploy` for a single script) to converge the queue attachments",
                            consumer.version,
                            consumer.script_name
                        );
                        anyhow::ensure!(
                            loaded_consumers
                                .get(&consumer.script_name)
                                .is_some_and(|queues| queues.contains(&queue)),
                            "queue {queue:?} is attached to script {:?}, but its loaded deployment does not consume that queue",
                            consumer.script_name
                        );
                        continue;
                    }
                    // The components map pins every script's version: an
                    // attachment naming a consumer the map omits or versions
                    // differently is crash residue (or the normal pre-commit
                    // window), and loading it would serve one generation's
                    // primary beside another's consumer. Fail closed exactly
                    // like the service branch above.
                    if let Some(generation) = components.as_ref() {
                        let pinned = generation.get(&consumer.script_name).with_context(|| {
                            format!(
                                "queue {queue:?} is attached to script {:?}, which is not in the activated generation; activate a generation that names every script",
                                consumer.script_name
                            )
                        })?;
                        anyhow::ensure!(
                            pinned.version == consumer.version && pinned.prefix == consumer.prefix,
                            "queue {queue:?} is attached to deployment {} of script {:?}, but the activated generation names {}; re-run `celld activate` to converge the queue attachments",
                            consumer.version,
                            consumer.script_name,
                            pinned.version
                        );
                    }
                    fleet::load_queue_consumer_worker(bucket, &queue, &consumer, node.clone())
                        .await
                        .with_context(|| format!("load consumer for queue {queue:?}"))?
                }
            };
            let target = loaded.script_name.clone();
            anyhow::ensure!(
                loaded_scripts
                    .insert(target.clone(), loaded.prefix.clone())
                    .is_none(),
                "script {target:?} was loaded twice"
            );
            loaded_consumers.insert(target.clone(), consumed_queues(&loaded.options));
            dependencies.extend(dependencies_of(&loaded));
            // A node runs the schedule of the deployment it was given and of
            // no other. The reserved class is one key, so a second script's
            // cron cell would resolve to the first script's config and run
            // the wrong `scheduled` handler. Dropping the schedule is the
            // safe half of that trade and this says so out loud, because a
            // trigger that never fires and says nothing is the failure the
            // whole feature is built to avoid. Deploy the script as a node's
            // own deployment to run its crons.
            if !loaded.crons.is_empty() {
                tracing::warn!(
                    script = %target,
                    crons = %loaded.crons.join(", "),
                    "a service binding target declares cron triggers; a node fires only its own deployment's schedule, so these never run here"
                );
            }
            cohosted.push(loaded);
        }
        Ok(Self {
            identity: ServingIdentity::of_pointer(&pointer),
            // The bucket this graph was read from is the fleet: same
            // versions under another scheme, bucket, or key prefix are
            // another fleet and must never share cache entries.
            fleet: fleet_identity(bucket.scheme(), &bucket.name, &bucket.prefix),
            primary,
            cohosted,
        })
    }
}

impl DeploymentGraph {
    /// Resolve the staged versions a `celld invoke` names: the target
    /// script as primary, plus every service-binding target it reaches,
    /// transitively.
    ///
    /// Each script resolves from the staged pins first and from the live
    /// pointer otherwise — the activated map when one names it, the named
    /// pointer on a legacy fleet — so the graph is the generation an
    /// activation of the same pins would serve. Queue dependencies are
    /// deliberately not loaded: the queue attachments still name the live
    /// consumers until activation converges them, and no broker ever
    /// dispatches a batch to the shadow generation, so loading consumers
    /// could only fail a graph whose queues all work (sends go to the
    /// real queue cells). Pins no loaded script depends on are ignored
    /// with a warning, after the caller has checked they exist: they
    /// cannot affect execution, because a service call resolves only a
    /// binding the calling script declares.
    ///
    /// This is the deliberate second bucket walk beside [`DeploymentGraph::load`]:
    /// boot and reload still share exactly one, and this one is never
    /// adopted — it builds a shadow generation that serves one operator
    /// request and is retired.
    pub async fn load_staged(
        bucket: &Bucket,
        node: String,
        pointer: &DeployPointer,
        target: &crate::deploy::VersionRef,
        pins: &BTreeMap<String, String>,
    ) -> anyhow::Result<Self> {
        let primary = fleet::load_pinned_worker(
            bucket,
            &staged_pointer(&target.script, &target.version),
            &format!("deploy/{}/current.json", target.script),
            node.clone(),
        )
        .await
        .with_context(|| {
            format!(
                "load staged version {} of script {:?}",
                target.version, target.script
            )
        })?;
        let primary_script = primary.script_name.clone();
        let components = pointer.components.clone();
        let mut loaded_scripts = BTreeMap::from([(primary_script.clone(), primary.prefix.clone())]);
        let mut loaded_pins = BTreeSet::from([target.script.clone()]);
        let mut dependencies: VecDeque<String> = staged_services_of(&primary).collect();
        let mut cohosted = Vec::new();
        while let Some(dependency) = dependencies.pop_front() {
            if dependency == primary_script || loaded_scripts.contains_key(&dependency) {
                continue;
            }
            let loaded = match pins.get(&dependency) {
                Some(version) => {
                    loaded_pins.insert(dependency.clone());
                    fleet::load_pinned_worker(
                        bucket,
                        &staged_pointer(&dependency, version),
                        &format!("deploy/{dependency}/current.json"),
                        node.clone(),
                    )
                    .await
                    .with_context(|| {
                        format!("load staged version {version} of script {dependency:?}")
                    })?
                }
                None => match components
                    .as_ref()
                    .and_then(|generation| generation.get(&dependency))
                {
                    // A carried script under an activated generation: the
                    // version the pointer's component map names, exactly as
                    // an activation of these pins would serve it.
                    Some(component) => fleet::load_pinned_worker(
                        bucket,
                        &DeployPointer {
                            script_name: Some(dependency.clone()),
                            version: component.version.clone(),
                            prefix: component.prefix.clone(),
                            rollout: Rollout { percent: 100 },
                            components: None,
                        },
                        &format!("deploy/{dependency}/current.json"),
                        node.clone(),
                    )
                    .await
                    .with_context(|| {
                        format!("load carried script {dependency:?} from the activated generation")
                    })?,
                    // A carried script on a legacy fleet: the named pointer,
                    // exactly as [`DeploymentGraph::load`] resolves it.
                    None => fleet::load_named_worker(bucket, &dependency, node.clone())
                        .await
                        .with_context(|| {
                            format!("load carried script {dependency:?} from its named pointer")
                        })?,
                },
            };
            if loaded.script_name != dependency {
                anyhow::bail!(
                    "service pointer {dependency} resolved script {}",
                    loaded.script_name
                );
            }
            anyhow::ensure!(
                loaded_scripts
                    .insert(dependency.clone(), loaded.prefix.clone())
                    .is_none(),
                "script {dependency:?} was loaded twice"
            );
            dependencies.extend(staged_services_of(&loaded));
            cohosted.push(loaded);
        }
        for pin in pins.keys() {
            if !loaded_pins.contains(pin) {
                tracing::warn!(
                    script = %pin,
                    target = %primary_script,
                    "staged pin is not reachable from the invoke target; ignoring it"
                );
            }
        }
        let staged_map = std::iter::once(&primary)
            .chain(cohosted.iter())
            .map(|loaded| {
                (
                    loaded.script_name.clone(),
                    ComponentRef {
                        version: loaded.version.clone(),
                        prefix: loaded.prefix.clone(),
                    },
                )
            })
            .collect();
        Ok(Self {
            identity: ServingIdentity {
                script_name: Some(primary_script),
                version: primary.version.clone(),
                prefix: primary.prefix.clone(),
                components: Some(staged_map),
            },
            // The same fleet as the live graph; `staged_invoke` salts it
            // with the shadow's reserved id, so the shadow's cache scope is
            // its own even when the staged identity equals the live one.
            fleet: fleet_identity(bucket.scheme(), &bucket.name, &bucket.prefix),
            primary,
            cohosted,
        })
    }
}

/// The pointer for one staged script version: the deterministic version
/// directory `celld deploy --no-activate` uploaded, never a mutable
/// pointer. The caller validates the name and version shapes first, and
/// [`fleet::load_pinned_worker`] re-checks the manifest identity, so a
/// corrupt entry fails the load instead of serving the wrong code.
fn staged_pointer(script: &str, version: &str) -> DeployPointer {
    DeployPointer {
        script_name: Some(script.to_string()),
        version: version.to_string(),
        prefix: format!("deploy/{script}/{version}"),
        rollout: Rollout { percent: 100 },
        components: None,
    }
}

/// The service-binding targets one loaded deployment reaches. Queue
/// dependencies are excluded on purpose: see
/// [`DeploymentGraph::load_staged`].
fn staged_services_of(loaded: &LoadedDeployment) -> impl Iterator<Item = String> + '_ {
    loaded.services.iter().map(|(_, script, _)| script.clone())
}

enum Dependency {
    Service(String),
    Queue(String),
}

fn consumed_queues(options: &WorkerConfigOptions) -> BTreeSet<String> {
    options
        .queue_consumers
        .iter()
        .map(|consumer| consumer.queue.clone())
        .collect()
}

fn dependencies_of(loaded: &LoadedDeployment) -> VecDeque<Dependency> {
    let queues = loaded
        .options
        .queue_bindings
        .iter()
        .map(|binding| binding.queue.clone())
        .chain(loaded.options.queue_consumers.iter().flat_map(|consumer| {
            std::iter::once(consumer.queue.clone()).chain(consumer.dead_letter_queue.clone())
        }))
        .collect::<BTreeSet<_>>();
    loaded
        .services
        .iter()
        .map(|(_, script, _)| Dependency::Service(script.clone()))
        .chain(queues.into_iter().map(Dependency::Queue))
        .collect()
}

/// Node-level inputs `Generation::build` needs beside the deployment itself.
pub struct GenerationOptions {
    pub node: String,
    pub region: String,
}

/// A deployment, built and ready to serve.
///
/// The fields are the four maps `RuntimeManager` once held for the life of
/// the process, plus the asset resolvers and the cron schedule that lived on
/// the application handle. They are private and reached through
/// `RuntimeManager`, which hands out this struct only as a snapshot.
pub struct Generation {
    pub(crate) id: GenerationId,
    pub(crate) version: String,
    pub(crate) prefix: String,
    pub(crate) script_name: String,
    pub(crate) serving: ServingIdentity,
    /// The deployment cache scope key stamped into this generation's
    /// tenant isolates: the fleet's bucket identity plus the serving
    /// deployment's identity. The reaper drops the scope once no live
    /// generation carries it.
    pub(crate) cache_scope: String,
    pub(crate) stateless: crate::runtime::StatelessRuntime,
    pub(crate) services: HashMap<String, crate::runtime::StatelessRuntime>,
    pub(crate) cell_configs: HashMap<String, Arc<WorkerConfig>>,
    /// The isolates a Worker script's cells live in — the same `Pool` the
    /// stateless path admits into, because an isolate is an isolate. Cells
    /// of one script share them, so cells of one class share module scope
    /// exactly when they are colocated, which is what Durable Objects do.
    /// The shared reserved classes live under `RUNTIME_SCRIPT` instead: a
    /// runtime-only pool whose isolates evaluate no tenant bundle.
    pub(crate) cell_isolates: HashMap<String, Arc<crate::pool::Pool>>,
    pub(crate) default_do_class: Option<Arc<str>>,
    pub(crate) assets: HashMap<String, AssetResolver>,
    /// Every container class of every script in the graph.
    pub(crate) containers: Vec<crate::container::ContainerSpec>,
    /// See `Manifest::fence_image`; the first script's, they are all the
    /// same image when one celld deployed them.
    pub(crate) fence_image: Option<String>,
}

impl Generation {
    pub fn id(&self) -> GenerationId {
        self.id
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The primary script: the one ingress serves and whose assets ingress
    /// consults before running the Worker.
    pub fn script_name(&self) -> &str {
        &self.script_name
    }

    /// The pointer identity this generation was adopted from, for the
    /// pointer watcher's unchanged check.
    pub fn serving_identity(&self) -> &ServingIdentity {
        &self.serving
    }

    /// The asset resolver of the named script, if that script deployed
    /// assets.
    pub fn assets(&self, script: &str) -> Option<&AssetResolver> {
        self.assets.get(script)
    }

    /// The primary script's asset resolver, which ingress consults.
    pub fn ingress_assets(&self) -> Option<&AssetResolver> {
        self.assets.get(&self.script_name)
    }

    pub fn has_cell_classes(&self) -> bool {
        !self.cell_configs.is_empty()
    }

    /// The reserved cell carrying this deployment's cron schedule, or `None`
    /// when the deployment declares no `triggers.crons`. Derived from the
    /// registered class rather than plumbed separately, so it cannot
    /// disagree with what `start_cell` will accept.
    pub fn cron_cell(&self) -> Option<String> {
        self.cell_configs
            .get(celld_logic::cron::RESERVED_CLASS)
            .map(|config| celld_logic::cron::reserved_cell(&config.script_name))
    }

    pub(crate) fn cell_config(&self, class: &str) -> Option<Arc<WorkerConfig>> {
        self.cell_configs.get(class).cloned()
    }

    /// The engine's reserved Durable Object classes this generation
    /// registers: cron, queue, workflow, D1, KV. Their cells hold no
    /// application state worth waiting for, so an adoption moves them at
    /// once — and the cron cell must run the new schedule before the
    /// adoption arms it.
    ///
    /// The cron class is named beside `deploy::is_reserved_class` rather
    /// than added to it. That predicate also decides which classes refuse an
    /// unauthenticated operator route, and the cron cell is not one of them,
    /// so widening it to reach this list would widen that refusal too.
    pub fn reserved_classes(&self) -> Vec<String> {
        self.cell_configs
            .keys()
            .filter(|class| {
                crate::deploy::is_reserved_class(class)
                    || class.as_str() == celld_logic::cron::RESERVED_CLASS
            })
            .cloned()
            .collect()
    }

    pub(crate) fn cell_isolates(&self, script: &str) -> Option<Arc<crate::pool::Pool>> {
        self.cell_isolates.get(script).cloned()
    }

    pub(crate) fn service(&self, script: &str) -> Option<crate::runtime::StatelessRuntime> {
        self.services.get(script).cloned()
    }

    pub(crate) fn default_do_class(&self) -> Option<&str> {
        self.default_do_class.as_deref()
    }

    /// Stop every isolate of this generation from taking new work. Stateless
    /// isolates free as their affiliations drop; cell isolates free as their
    /// cells move to a newer generation.
    pub(crate) fn retire(&self) {
        for service in self.services.values() {
            service.isolates.retire_all();
        }
        for pool in self.cell_isolates.values() {
            pool.retire_all();
        }
    }

    /// Whether every isolate of this generation has been freed, so the
    /// generation itself can be dropped.
    pub(crate) fn is_drained(&self) -> bool {
        self.services
            .values()
            .all(|service| service.isolates.is_drained())
            && self.cell_isolates.values().all(|pool| pool.is_drained())
    }

    /// One maintenance pass over the cell pools: retire and free every empty
    /// isolate. An empty cell heap carries no warm request capacity worth
    /// preserving, unlike a stateless one.
    pub(crate) fn reap_cell_pools(&self) {
        for pool in self.cell_isolates.values() {
            pool.reap_empty();
        }
    }

    /// The isolates this generation holds right now, for `/state`.
    pub(crate) fn isolate_census(&self) -> IsolateCensus {
        IsolateCensus {
            stateless: self.stateless.isolates.census(),
            services: self
                .services
                .iter()
                .map(|(name, service)| (name.clone(), service.isolates.census()))
                .collect(),
            cells: self
                .cell_isolates
                .iter()
                .map(|(script, pool)| (script.clone(), pool.census()))
                .collect(),
        }
    }
}

/// The isolates one generation holds, by the pool they belong to, as
/// `/state` reports them.
#[derive(Debug, serde::Serialize)]
pub struct IsolateCensus {
    /// The pool that serves the primary script's stateless handlers.
    pub stateless: crate::pool::PoolCensus,
    /// The pools of the named services, by service name.
    pub services: BTreeMap<String, crate::pool::PoolCensus>,
    /// The pools the cells live in, by Worker script — plus the runtime
    /// pool under its reserved name when the deployment declares a shared
    /// reserved class.
    pub cells: BTreeMap<String, crate::pool::PoolCensus>,
}

/// The generation an isolate was built for, installed as an isolate slot by
/// `Worker::load_config` so a call the isolate makes into the host — a
/// service binding, an assets binding, a queue dispatch — resolves against
/// the graph the caller was built with rather than whichever generation is
/// current when the call lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenerationTag(pub GenerationId);

/// The script name of the runtime isolate pool: the isolates that host the
/// shared reserved classes (`__D1Database`, `__KvNamespace`, `__Queue`).
///
/// Not a deployable script name. `deploy::valid_script_name` accepts only
/// lowercase ASCII, digits, and `-`, so no deployment can claim this name
/// and no tenant pool can collide with the runtime pool in `cell_isolates`.
/// The shared classes address fleet-wide namespace keys, so their cells
/// resolve identically whichever script name hosts them.
pub(crate) const RUNTIME_SCRIPT: &str = "__celld-runtime";

/// The entry module of a runtime isolate. The harness plus this fixed inert
/// module is the whole realm: no tenant bundle is compiled or evaluated, so
/// no tenant's top level shares globals or primordials with a shared cell.
///
/// `load_config` still requires a default export with a `fetch` handler,
/// which this provides. It never runs: the runtime pool serves cells only,
/// and no stateless pool, service binding, or loader targets this script.
const RUNTIME_MODULE: &str = "export default {\n  fetch() {\n    return new Response(\"runtime isolate\", { status: 500 });\n  },\n};\n";

impl Generation {
    /// Build a deployment into a generation that can serve.
    ///
    /// Every script in the graph is compiled and its primary pool warmed, so
    /// a bundle that does not load, or declares a Durable Object class it
    /// does not export, fails here — before the generation can take traffic
    /// — rather than on the first request. The Durable Object classes of
    /// every script form one flat registry, because `start_cell` resolves a
    /// cell by the class its scope names and nothing else.
    ///
    /// Boot and reload both come through here. There is no other function
    /// that turns a deployment into runtime state.
    pub fn build(
        id: GenerationId,
        graph: DeploymentGraph,
        options: GenerationOptions,
    ) -> anyhow::Result<Self> {
        let GenerationOptions { node, region } = options;
        let DeploymentGraph {
            identity,
            fleet,
            primary,
            cohosted,
        } = graph;
        // One deployment cache scope key for the whole generation: the
        // fleet's bucket identity plus the serving deployment's identity.
        // Every tenant script's isolates carry it, so in deployment scope
        // they all share one `caches.default` and one set of named
        // namespaces. The runtime config below is deliberately unstamped —
        // it runs no tenant fetch handlers and is not a script the pointer
        // names, so its isolates stay script-scoped under their reserved
        // name.
        let cache_scope = identity.cache_scope_key(&fleet);
        crate::runtime::init_engine();

        // A Queue class is shared by every co-hosted script, but its broker
        // needs the consumer script rather than whichever script happened to
        // register `__Queue` first. Resolve one deployment-wide catalog before
        // constructing any WorkerConfig, then install the same catalog into
        // every isolate that could host a queue cell.
        let mut queue_catalog = BTreeMap::new();
        for (script, consumers) in
            std::iter::once((&primary.script_name, &primary.options.queue_consumers)).chain(
                cohosted
                    .iter()
                    .map(|target| (&target.script_name, &target.options.queue_consumers)),
            )
        {
            for consumer in consumers {
                let registration = crate::js::QueueConsumerRegistration {
                    script: script.clone(),
                    config: consumer.clone(),
                };
                if let Some(previous) = queue_catalog.insert(consumer.queue.clone(), registration) {
                    return Err(anyhow!(
                        "queue {:?} is consumed by both {} and {}",
                        consumer.queue,
                        previous.script,
                        script
                    ));
                }
            }
        }
        let queue_catalog = queue_catalog.into_values().collect::<Vec<_>>();

        let node: Arc<str> = Arc::from(node);
        let region: Arc<str> = Arc::from(region);
        let primary_script = primary.script_name.clone();
        let version = primary.version.clone();
        let prefix = primary.prefix.clone();
        let mut all_containers = Vec::new();
        let mut fence_image = None;
        // Only classes the user declared can be a bare-id default. Every
        // runtime-supplied class rides in `do_classes` so that its namespace
        // key is minted, and counting one here made adding any D1 binding flip
        // a one-class project past the `len == 1` condition — every
        // `/do/<bare-id>` request then failed with "requires exactly one
        // configured Durable Object class" for a config that still declared
        // exactly one.
        //
        // This asks `deploy::is_reserved_class` rather than naming one class,
        // because the first version of this filter named `__D1Database` alone
        // and `__Workflow` walked straight back into the same bug when it
        // shipped. A fourth reserved class must not be able to do it a third
        // time.
        let user_classes: Vec<&String> = primary
            .options
            .do_classes
            .iter()
            .filter(|class| !crate::deploy::is_reserved_class(class))
            .collect();
        let default_do_class =
            (user_classes.len() == 1).then(|| Arc::from(user_classes[0].as_str()));
        // Two scripts exporting one class name is genuinely ambiguous and is
        // refused — but a *reserved* class is not an export, and the two kinds
        // behave differently:
        //
        // `__D1Database` is deliberately shared. Its namespace is fleet-wide so
        // that several Workers can bind one database and a rename cannot rename
        // it, so two scripts declaring `d1_databases` address the same cells on
        // purpose. Refusing the second made the node exit instead of start.
        //
        // A workflow class is script-scoped (`deploy::workflow_class`), so two
        // scripts produce two distinct names and never reach this branch. If
        // one ever did, it would be a real collision and must still be refused.
        let mut cell_configs: HashMap<String, Arc<WorkerConfig>> = HashMap::new();
        // The shared reserved classes (`__D1Database`, `__KvNamespace`,
        // `__Queue`) run in the runtime isolate pool, not in any tenant
        // script's pool. Their cells serve every script in the fleet, so
        // hosting them beside one script's bundle would let that script's
        // top level tamper with the globals and primordials their gates
        // and data paths use. The runtime config carries no tenant code,
        // bindings, vars, services, loaders, or schedule — only the
        // deployment-wide queue catalog (so shared queue cells dispatch
        // to the declaring consumer), this node's routing identity, the
        // generation tag (so dispatches resolve against this graph), and
        // the union of every declaring script's compatibility switches
        // (`Compat::union` — the one config the shared cells' storage
        // behavior still follows).
        //
        // Pre-registered here, so the per-script loop below keeps this
        // config for every shared class (`register_cell_class` keeps the
        // first). Workflow classes and `.cron` stay in their own script's
        // pool on purpose: a workflow cell executes its script's `run()`
        // and a cron cell its script's `scheduled()`, with that script's
        // bindings, so moving them would move the tenant code they run.
        // The runtime config's compat is the union over every script that
        // declares a shared class — not the primary's alone, which would
        // drop a storage capability (e.g. `sqlite_vec`) that only a
        // cohosted declaring script opts into. A script that declares no
        // shared class contributes nothing: its switches govern cells it
        // never touches.
        let (shared_classes, runtime_compat): (Vec<String>, Option<Compat>) = {
            let mut seen = BTreeSet::new();
            let mut compat: Option<Compat> = None;
            for options in std::iter::once(&primary.options)
                .chain(cohosted.iter().map(|target| &target.options))
            {
                let mut declares = false;
                for class in &options.do_classes {
                    if crate::deploy::is_shared_reserved_class(class) {
                        seen.insert(class.clone());
                        declares = true;
                    }
                }
                if declares {
                    compat = Some(match compat {
                        None => options.compat,
                        Some(acc) => acc.union(options.compat),
                    });
                }
            }
            (seen.into_iter().collect(), compat)
        };
        if !shared_classes.is_empty() {
            let runtime = Arc::new(
                WorkerConfig::new(WorkerConfigOptions {
                    src: RUNTIME_MODULE.to_string(),
                    script_name: RUNTIME_SCRIPT.to_string(),
                    do_classes: shared_classes.clone(),
                    bindings: Vec::new(),
                    r2_bindings: Vec::new(),
                    d1_bindings: Vec::new(),
                    kv_bindings: Vec::new(),
                    queue_bindings: Vec::new(),
                    queue_consumers: Vec::new(),
                    workflow_bindings: Vec::new(),
                    vars: Vec::new(),
                    node: node.to_string(),
                    modules: Vec::new(),
                    compat: runtime_compat.expect("a shared class implies a declaring script"),
                })
                .with_queue_consumers(queue_catalog.clone())
                .with_generation(id),
            );
            for class in &shared_classes {
                cell_configs.insert(class.clone(), runtime.clone());
            }
        }
        let mut service_pools = HashMap::new();
        let mut assets = HashMap::new();
        // Primary first: only this script owns ingress and the node's cron
        // schedule; bindings and runtime construction follow the same path
        // for all scripts.
        for (index, deployment) in std::iter::once(primary).chain(cohosted).enumerate() {
            let is_primary = index == 0;
            let crate::fleet::LoadedDeployment {
                options,
                script_name: script,
                asset_binding,
                loader_bindings,
                assets: resolver,
                services,
                crons,
                containers,
                fence_image: fence,
                ..
            } = deployment;
            all_containers.extend(containers.iter().cloned());
            fence_image = fence_image.or(fence);
            if let Some(resolver) = resolver {
                assets.insert(script.clone(), resolver);
            }
            let classes = options.do_classes.clone();
            let config = Arc::new(
                WorkerConfig::new(options)
                    .with_services(services)
                    .with_asset_binding(asset_binding)
                    .with_loaders(loader_bindings)
                    .with_queue_consumers(queue_catalog.clone())
                    .with_crons(if is_primary { crons } else { Vec::new() })
                    .with_containers(containers)
                    .with_generation(id)
                    .with_cache_scope(cache_scope.clone()),
            );
            let pool = crate::runtime::StatelessRuntime::start(
                config.clone(),
                node.clone(),
                region.clone(),
            )?;
            if service_pools.insert(script.clone(), pool).is_some() {
                return Err(anyhow!("duplicate co-hosted Worker script {script}"));
            }
            for class in classes {
                register_cell_class(&mut cell_configs, class, config.clone(), &|class| {
                    if is_primary {
                        anyhow!("duplicate Durable Object class {class}")
                    } else {
                        anyhow!(
                            "Durable Object class {class} is exported by more than one co-hosted script"
                        )
                    }
                })?;
            }
            // Cron is runtime-supplied, not a manifest class. Its alarm calls
            // the primary script's scheduled handler with that script's bindings.
            if !config.crons.is_empty() {
                cell_configs.insert(celld_logic::cron::RESERVED_CLASS.to_string(), config);
            }
        }
        let stateless = service_pools[&primary_script].clone();
        let cell_isolates: HashMap<String, Arc<crate::pool::Pool>> = cell_configs
            .values()
            .map(|config| {
                (
                    config.script_name.clone(),
                    crate::runtime::cell_pool(config.clone()),
                )
            })
            .collect();
        Ok(Generation {
            id,
            version,
            prefix,
            script_name: primary_script,
            serving: identity,
            cache_scope,
            stateless,
            services: service_pools,
            cell_configs,
            cell_isolates,
            default_do_class,
            assets,
            containers: all_containers,
            fence_image,
        })
    }
}
/// Claim one class name in a deployment's cell registry.
///
/// A duplicate is an error for a user class, because `start_cell` resolves a
/// cell from the class its scope names and two exports of one name are
/// genuinely ambiguous. A duplicate of a *shared* reserved class is not: see
/// `deploy::is_shared_reserved_class`. `Generation::build` pre-registers the
/// shared classes with the runtime config, so this branch is what keeps that
/// config when each tenant script declares them. The caller supplies the
/// error so the message can say whether the collision was inside one script
/// or across two.
fn register_cell_class(
    configs: &mut HashMap<String, Arc<WorkerConfig>>,
    class: String,
    config: Arc<WorkerConfig>,
    duplicate: &dyn Fn(&str) -> anyhow::Error,
) -> anyhow::Result<()> {
    match configs.entry(class) {
        Entry::Vacant(slot) => {
            slot.insert(config);
            Ok(())
        }
        Entry::Occupied(slot) if crate::deploy::is_shared_reserved_class(slot.key()) => Ok(()),
        Entry::Occupied(slot) => Err(duplicate(slot.key())),
    }
}

/// The fleet part of a cache scope key: scheme, bucket name, and key
/// prefix, with `/` between name and prefix. A bucket name holds no `/`
/// (`bucket::split_spec`), so the first `/` after `://` ends it, and
/// `ab` + `c/` never renders like `a` + `bc/`.
fn fleet_identity(scheme: &str, name: &str, prefix: &str) -> String {
    format!("{scheme}://{name}/{prefix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> ServingIdentity {
        ServingIdentity {
            script_name: Some("app".to_string()),
            version: "abc123".to_string(),
            prefix: "deploy/app/abc123".to_string(),
            components: None,
        }
    }

    #[test]
    fn fleet_identity_keeps_name_and_prefix_apart() {
        assert_ne!(
            fleet_identity("s3", "ab", "c/"),
            fleet_identity("s3", "a", "bc/")
        );
        assert_ne!(fleet_identity("s3", "a", ""), fleet_identity("gs", "a", ""));
        assert_eq!(
            fleet_identity("s3", "noite", "fleets/app/"),
            "s3://noite/fleets/app/"
        );
    }

    #[test]
    fn cache_scope_key_is_stable_but_identity_bound() {
        // The same fleet and identity render the same key across
        // generations, so a forced reload of one deployment keeps its
        // deployment scope.
        assert_eq!(
            identity().cache_scope_key("s3://one/fleet-a/"),
            identity().cache_scope_key("s3://one/fleet-a/")
        );
        // Any identity difference renders a different key, so a redeploy
        // starts with an empty scope and never reads the old one.
        let mut version = identity();
        version.version = "def456".to_string();
        assert_ne!(
            identity().cache_scope_key("s3://one/fleet-a/"),
            version.cache_scope_key("s3://one/fleet-a/")
        );
        let mut script = identity();
        script.script_name = Some("other".to_string());
        assert_ne!(
            identity().cache_scope_key("s3://one/fleet-a/"),
            script.cache_scope_key("s3://one/fleet-a/")
        );
        let mut absent = identity();
        absent.script_name = None;
        assert_ne!(
            identity().cache_scope_key("s3://one/fleet-a/"),
            absent.cache_scope_key("s3://one/fleet-a/")
        );
        let mut prefix = identity();
        prefix.prefix = "deploy/app/def456".to_string();
        assert_ne!(
            identity().cache_scope_key("s3://one/fleet-a/"),
            prefix.cache_scope_key("s3://one/fleet-a/")
        );
        // Length-prefixing keeps field boundaries unambiguous: moving one
        // character across the version/prefix boundary changes the key.
        let mut blurred = identity();
        blurred.version = "abc12".to_string();
        blurred.prefix = "3deploy/app/abc123".to_string();
        assert_ne!(
            identity().cache_scope_key("s3://one/fleet-a/"),
            blurred.cache_scope_key("s3://one/fleet-a/")
        );
        // Two fleets serving the same versions still differ in fleet, so
        // they never share entries — not just a deployment name two
        // fleets could share.
        assert_ne!(
            identity().cache_scope_key("s3://one/fleet-a/"),
            identity().cache_scope_key("s3://one/fleet-b/")
        );
        assert_ne!(
            identity().cache_scope_key("s3://one/fleet-a/"),
            identity().cache_scope_key("s3://two/fleet-a/")
        );
    }

    #[test]
    fn cache_scope_key_covers_activated_components() {
        let mut with_components = identity();
        with_components.components = Some(BTreeMap::from([(
            "app".to_string(),
            ComponentRef {
                version: "abc123".to_string(),
                prefix: "deploy/app/abc123".to_string(),
            },
        )]));
        // A component-only activation names a new deployment.
        assert_ne!(
            identity().cache_scope_key("s3://one/fleet-a/"),
            with_components.cache_scope_key("s3://one/fleet-a/")
        );
        // Map order cannot smuggle one generation's components into
        // another's key: iteration is ordered, so insertion order is
        // invisible.
        let mut reordered = with_components.clone();
        reordered.components = Some(BTreeMap::from([(
            "app".to_string(),
            ComponentRef {
                version: "abc123".to_string(),
                prefix: "deploy/app/abc123".to_string(),
            },
        )]));
        assert_eq!(
            with_components.cache_scope_key("s3://one/fleet-a/"),
            reordered.cache_scope_key("s3://one/fleet-a/")
        );
        let mut changed = with_components.clone();
        changed.components = Some(BTreeMap::from([(
            "app".to_string(),
            ComponentRef {
                version: "def456".to_string(),
                prefix: "deploy/app/abc123".to_string(),
            },
        )]));
        assert_ne!(
            with_components.cache_scope_key("s3://one/fleet-a/"),
            changed.cache_scope_key("s3://one/fleet-a/")
        );
    }
}
