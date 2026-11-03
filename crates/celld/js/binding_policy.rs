// Modified by ryuzcorp (ryuzcorp/celld): binding-forgery enforcement. Apache-2.0.

//! Who a host op answers: the calling script's declared bindings.
//!
//! Every binding object a Worker holds resolves to a cell through one of
//! three ops: `__do_id` derives the cell id from a namespace key and a
//! name, and `__do_call` / `__rpc_call` dispatch to the resulting scope.
//! All three used to take those strings from the harness unchecked, and
//! the binding classes take their resource identity as a constructor
//! argument -- so a Worker could reach a resource its config never
//! bound, for D1, KV, queues, workflows, Durable Object namespaces,
//! and (through a storage or container scope) any cell's storage and
//! container.
//!
//! The checks here close that at the host boundary. The calling script's
//! identity and its declared bindings come from the `WorkerConfig` the
//! host installed in this isolate's slot at load -- never from an
//! argument -- and an undeclared name is refused:
//!
//! - a fleet-wide namespace (D1, KV, Queue) answers only for an identity
//!   the script declared, and dispatch recomputes the scope from that
//!   identity so a declared name cannot be paired with a victim id;
//! - a script-scoped namespace answers only for its own script and for a
//!   class the script declared; the whole namespace is the binding, as on
//!   Cloudflare, so any instance id within it is legitimate;
//! - a workflow class answers only for its own script and for a declared
//!   workflow name;
//! - reserved cells no binding names (`.cron`, the bare `__Workflow`)
//!   are never stub-addressable from a Worker;
//! - storage, alarm, container, facet, and socket-registry ops answer
//!   only for the executing cell's own scope.
//!
//! Stubs stay transferable capabilities: a stub received over RPC or read
//! back from storage carries a harness-minted marker, and revive records
//! its scope in this isolate's grant set, which dispatch consults
//! alongside the declarations. Holding a stub object proves nothing on its
//! own -- stub construction is pure JS with no host check -- so the lift
//! mints a marker only for a stub the sender could already dispatch to
//! (the dispatch predicate below, consulted before the marker exists). A
//! forged scope has no grant and no declaration, so it is refused at the
//! lift and at every op it could otherwise reach.
use super::storage_ops::actor_runtime_state;
use super::{BundleFs, WorkerConfig};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// How many received-stub scopes one isolate remembers. A grant is one
/// cell a stub the script legitimately received names; the bound keeps a
/// stub-flooding peer from growing the set without limit, and a script
/// that holds more live stubs than this at once gets a clear error
/// instead of silent memory growth.
pub(super) const DO_GRANT_MAX_SCOPES: usize = 4096;

/// The configuration this isolate was built from: the calling script's
/// identity and declared bindings. `None` for an isolate built outside
/// `load_config`, which every check treats as no declaration at all.
fn caller_config(scope: &mut v8::PinScope) -> Option<Arc<WorkerConfig>> {
    scope
        .get_slot::<Arc<BundleFs>>()
        .map(|bundle| bundle.config.clone())
}

/// Whether `cell` names a resource `config` declared: a Durable Object
/// class of its own script (any instance: the namespace is the binding),
/// a fleet-wide identity (D1, KV, Queue) from its own binding list with
/// the scope recomputed from that identity, or a declared workflow of
/// its own script-scoped workflow class.
fn declared_cell(config: &WorkerConfig, cell: &str, name: Option<&str>) -> Result<(), String> {
    let script = config.script_name.as_str();
    let Some((class, id)) = cell.split_once(':') else {
        return Err(format!("cell scope {cell:?} is malformed"));
    };
    if id.is_empty() {
        return Err(format!("cell scope {cell:?} is malformed"));
    }
    if class == crate::deploy::D1_CLASS {
        return match name {
            Some(identity)
                if config
                    .d1_bindings
                    .iter()
                    .any(|(_, bound)| bound == identity)
                    && cell == crate::engine_api::d1_cell_scope(identity) =>
            {
                Ok(())
            }
            Some(identity) => Err(format!(
                "D1 database {identity:?} is not bound in this Worker"
            )),
            None => Err("an unnamed D1 database cell is not bound in this Worker".to_string()),
        };
    }
    if class == crate::deploy::KV_CLASS {
        return match name {
            Some(cell_name)
                if config.kv_bindings.iter().any(|(_, bound)| {
                    celld_logic::kv::cell_name(bound, 0) == cell_name
                        && cell == crate::engine_api::kv_cell_scope(bound, 0)
                }) =>
            {
                Ok(())
            }
            Some(cell_name) => Err(format!(
                "KV namespace {cell_name:?} is not bound in this Worker"
            )),
            None => Err("an unnamed KV namespace cell is not bound in this Worker".to_string()),
        };
    }
    if class == crate::deploy::QUEUE_CLASS {
        return match name {
            Some(queue)
                if config
                    .queue_bindings
                    .iter()
                    .any(|bound| bound.queue == queue)
                    && cell == crate::engine_api::queue_cell_scope(queue) =>
            {
                Ok(())
            }
            Some(queue) => Err(format!("queue {queue:?} is not bound in this Worker")),
            None => Err("an unnamed queue cell is not bound in this Worker".to_string()),
        };
    }
    if crate::deploy::is_workflow_class(class) {
        let expected = crate::deploy::workflow_class(script);
        if class != expected {
            return Err(format!(
                "workflow class {class:?} is not bound in this Worker"
            ));
        }
        let Some(name) = name else {
            return Err("an unnamed workflow cell is not bound in this Worker".to_string());
        };
        // A cell name is `{workflow}/{instance}`. Match a declared
        // workflow as a prefix so a workflow name carrying a `/` still
        // resolves to the declaration, not to its first segment.
        let declared = config.workflow_bindings.iter().any(|bound| {
            name.len() > bound.workflow.len()
                && name.starts_with(bound.workflow.as_str())
                && name.as_bytes()[bound.workflow.len()] == b'/'
        });
        if !declared {
            let workflow = name.split('/').next().unwrap_or(name);
            return Err(format!("workflow {workflow:?} is not bound in this Worker"));
        }
        let namespace = crate::engine_api::namespace_key(script, class);
        let id = crate::engine_api::durable_object_id_for_name(&namespace, name);
        let expected_scope = format!("{class}:{}", crate::engine_api::durable_object_id_hex(&id));
        if cell != expected_scope {
            return Err(format!(
                "workflow cell {cell:?} is not bound in this Worker"
            ));
        }
        return Ok(());
    }
    // Reserved cells no binding names: the scheduler's cell, the inert
    // bare workflow registration, and any future reserved class. The
    // scheduler drives its cell through host dispatch, never through a
    // Worker stub, so refusing here breaks no legitimate caller.
    if class == celld_logic::cron::RESERVED_CLASS
        || class == crate::deploy::WORKFLOW_CLASS
        || crate::deploy::is_reserved_class(class)
    {
        return Err(format!(
            "reserved cell {cell:?} is not bound in this Worker"
        ));
    }
    if config.do_classes.iter().any(|bound| bound == class) {
        return Ok(());
    }
    Err(format!(
        "Durable Object class {class:?} is not bound in this Worker"
    ))
}

/// Whether `namespace_key` names a namespace the calling script declared,
/// for `operation` (`name`, `unique`, or `validate`) on `input`.
fn declared_namespace(
    config: &WorkerConfig,
    namespace_key: &str,
    operation: &str,
    input: &str,
) -> Result<(), String> {
    let script = config.script_name.as_str();
    if namespace_key == crate::engine_api::D1_NAMESPACE_KEY {
        if operation == "name" && config.d1_bindings.iter().any(|(_, bound)| bound == input) {
            return Ok(());
        }
        return Err(format!("D1 database {input:?} is not bound in this Worker"));
    }
    if namespace_key == crate::engine_api::KV_NAMESPACE_KEY {
        if operation == "name"
            && config
                .kv_bindings
                .iter()
                .any(|(_, bound)| celld_logic::kv::cell_name(bound, 0) == input)
        {
            return Ok(());
        }
        return Err(format!(
            "KV namespace {input:?} is not bound in this Worker"
        ));
    }
    if namespace_key == crate::engine_api::QUEUE_NAMESPACE_KEY {
        if operation == "name"
            && config
                .queue_bindings
                .iter()
                .any(|bound| celld_logic::queue::cell_name(&bound.queue) == input)
        {
            return Ok(());
        }
        return Err(format!("queue {input:?} is not bound in this Worker"));
    }
    // A script-scoped namespace key says which script and class it is;
    // anything else matches no namespace the host minted.
    let Some((key_script, class)) = parse_script_namespace_key(namespace_key) else {
        return Err(
            "a Durable Object namespace this Worker never declared is not bound".to_string(),
        );
    };
    if key_script != script {
        return Err(format!(
            "Durable Object class {class:?} of script {key_script:?} is not bound in this Worker"
        ));
    }
    if crate::deploy::is_workflow_class(class) {
        if class != crate::deploy::workflow_class(script) {
            return Err(format!(
                "workflow class {class:?} is not bound in this Worker"
            ));
        }
        // Dispatch checks the workflow name against the declared
        // bindings; an id alone addresses nothing.
        return Ok(());
    }
    if class == celld_logic::cron::RESERVED_CLASS
        || class == crate::deploy::WORKFLOW_CLASS
        || crate::deploy::is_reserved_class(class)
        || !config.do_classes.iter().any(|bound| bound == class)
    {
        return Err(format!(
            "Durable Object class {class:?} is not bound in this Worker"
        ));
    }
    Ok(())
}

/// Split `cells:v1:{len}:{script}:{class}` into script and class. The
/// length pins the split, so a script name carrying a separator fails
/// closed instead of parsing as another script's namespace.
fn parse_script_namespace_key(key: &str) -> Option<(&str, &str)> {
    let rest = key.strip_prefix("cells:v1:")?;
    let (len, rest) = rest.split_once(':')?;
    let len: usize = len.parse().ok()?;
    let (script, class) = rest.split_once(':')?;
    if script.len() != len || class.is_empty() {
        return None;
    }
    Some((script, class))
}

/// Refuse a `__do_id` derivation the calling script did not declare.
/// A granted scope's name resolves too: the queue cell's DLQ forward
/// grants the dead-letter cell by name before resolving it.
pub(super) fn check_do_namespace(
    scope: &mut v8::PinScope,
    namespace_key: &str,
    operation: &str,
    input: &str,
) -> Result<(), String> {
    if operation == "name" {
        if let Some(cell) = scope_for_name(namespace_key, input) {
            if granted_cell_scope(scope, &cell) {
                return Ok(());
            }
        }
    }
    let Some(config) = caller_config(scope) else {
        return Err("this Worker has no declared bindings".to_string());
    };
    declared_namespace(&config, namespace_key, operation, input)
}

/// The cell scope `(namespace_key, name)` resolves to, if the key names
/// a namespace at all.
fn scope_for_name(namespace_key: &str, name: &str) -> Option<String> {
    let class = if namespace_key == crate::engine_api::D1_NAMESPACE_KEY {
        crate::deploy::D1_CLASS
    } else if namespace_key == crate::engine_api::KV_NAMESPACE_KEY {
        crate::deploy::KV_CLASS
    } else if namespace_key == crate::engine_api::QUEUE_NAMESPACE_KEY {
        crate::deploy::QUEUE_CLASS
    } else if let Some((_, class)) = parse_script_namespace_key(namespace_key) {
        class
    } else {
        return None;
    };
    let id = crate::engine_api::durable_object_id_for_name(namespace_key, name);
    Some(format!(
        "{class}:{}",
        crate::engine_api::durable_object_id_hex(&id)
    ))
}

/// Refuse a `__do_call` / `__rpc_call` dispatch outside the calling
/// script's declared bindings and received-stub grants.
pub(super) fn check_cell_call(
    scope: &mut v8::PinScope,
    cell: &str,
    name: Option<&str>,
) -> Result<(), String> {
    if granted_cell_scope(scope, cell) {
        return Ok(());
    }
    let Some(config) = caller_config(scope) else {
        return Err("this Worker has no declared bindings".to_string());
    };
    declared_cell(&config, cell, name)
}

/// Refuse a stub lift (RPC or storage) the sender could not dispatch:
/// the exact predicate `check_cell_call` answers, consulted before the
/// lift mints a transfer marker. Reads grants, writes none -- only revive
/// records a grant, for a marker a lift-gated sender minted.
pub(super) fn check_lift(
    scope: &mut v8::PinScope,
    cell: &str,
    name: Option<&str>,
) -> Result<(), String> {
    check_cell_call(scope, cell, name)
}

/// Whether `cell` is in this isolate's received-stub grant set: a pinned
/// deployment edge or a scope with at least one live revived stub.
fn granted_cell_scope(scope: &mut v8::PinScope, cell: &str) -> bool {
    actor_runtime_state(scope)
        .do_grants
        .lock()
        .expect("grant lock poisoned")
        .granted(cell)
}

/// Record a received stub's scope, so dispatch honors the transfer. Only
/// the harness calls this, from stub revive (whose markers a lift-gated
/// sender minted: the lift records host-checked scopes in a side table
/// outside the tenant payload, and refuses stubs the sender could not
/// dispatch to). Each revived stub holds one reference; the harness
/// releases it when the stub is collected, so long-lived isolates do not
/// accumulate grants for stubs they no longer hold. A scope stays granted
/// while any live stub needs it.
pub(super) fn grant_cell_scope(scope: &mut v8::PinScope, cell: &str) -> Result<(), String> {
    if !celld_logic::cell::valid_cell_scope(cell) {
        return Err(format!("cell scope {cell:?} is malformed"));
    }
    insert_grant(scope, cell)
}

/// Release one revived-stub reference on `cell`, dropping the grant when
/// the last live stub for the scope is gone. Only the harness calls this,
/// from the grant finalizer. Unknown or malformed scopes are a silent
/// no-op: a finalizer must never throw.
pub(super) fn release_cell_scope(scope: &mut v8::PinScope, cell: &str) {
    if !celld_logic::cell::valid_cell_scope(cell) {
        return;
    }
    actor_runtime_state(scope)
        .do_grants
        .lock()
        .expect("grant lock poisoned")
        .release(cell);
}

/// Record a deployment-declared cell the harness addresses by name: the
/// queue cell's dead-letter forward names a queue this cell's script
/// does not necessarily bind as a producer, so the forward grants the
/// derived scope before resolving it. Only the harness calls this. Pinned
/// grants are not tied to a live stub object, so they are never released.
pub(super) fn grant_cell_name(
    scope: &mut v8::PinScope,
    namespace_key: &str,
    name: &str,
) -> Result<(), String> {
    let Some(cell) = scope_for_name(namespace_key, name) else {
        return Err(
            "a Durable Object namespace this Worker never declared is not bound".to_string(),
        );
    };
    insert_pinned_grant(scope, &cell)
}

fn insert_grant(scope: &mut v8::PinScope, cell: &str) -> Result<(), String> {
    actor_runtime_state(scope)
        .do_grants
        .lock()
        .expect("grant lock poisoned")
        .grant(cell)
}

fn insert_pinned_grant(scope: &mut v8::PinScope, cell: &str) -> Result<(), String> {
    actor_runtime_state(scope)
        .do_grants
        .lock()
        .expect("grant lock poisoned")
        .pin(cell)
}

/// One isolate's received-stub grants: refcounted scopes plus pinned
/// deployment edges, under one cap. `ActorRuntimeState` holds this behind
/// one `Mutex`, so the grant, pin, release, and check paths share a single
/// lock: the count and the pinned set cannot disagree about the budget,
/// and no two-lock ordering can deadlock.
#[derive(Default)]
pub(super) struct GrantSet {
    counts: HashMap<String, usize>,
    pinned: HashSet<String>,
}

impl GrantSet {
    pub(super) fn grant(&mut self, cell: &str) -> Result<(), String> {
        if !self.counts.contains_key(cell)
            && self.counts.len() + self.pinned.len() >= DO_GRANT_MAX_SCOPES
        {
            return Err(format!(
                "this Worker holds more than {DO_GRANT_MAX_SCOPES} received stubs"
            ));
        }
        self.counts
            .entry(cell.to_string())
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
        Ok(())
    }

    pub(super) fn release(&mut self, cell: &str) {
        if let Some(count) = self.counts.get_mut(cell) {
            if *count > 1 {
                *count -= 1;
            } else {
                self.counts.remove(cell);
            }
        }
    }

    pub(super) fn pin(&mut self, cell: &str) -> Result<(), String> {
        if !self.pinned.contains(cell)
            && self.pinned.len() + self.counts.len() >= DO_GRANT_MAX_SCOPES
        {
            return Err(format!(
                "this Worker holds more than {DO_GRANT_MAX_SCOPES} received stubs"
            ));
        }
        self.pinned.insert(cell.to_string());
        Ok(())
    }

    pub(super) fn granted(&self, cell: &str) -> bool {
        self.pinned.contains(cell) || self.counts.contains_key(cell)
    }
}

/// Refuse a cell-scoped op (storage, SQL, alarm, container, facet,
/// socket-registry) addressed at any scope but the executing cell's.
/// A stateless event has no cell, so any scope is refused there. The
/// message names the scope, not the operation; each op adds its own
/// context when it throws.
pub(super) fn require_executing_cell(scope: &mut v8::PinScope, cell: &str) -> Result<(), String> {
    let executing = super::event_context(scope)
        .and_then(|context| super::current_cell_event(&context))
        .map(|frame| frame.storage);
    match executing {
        Some(own) if own == cell => Ok(()),
        _ => Err(format!("scope {cell:?} is not bound to the executing cell")),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_script_namespace_key;

    #[test]
    fn script_namespace_key_round_trip() {
        let key = crate::engine_api::namespace_key("my-script", "MyClass");
        assert_eq!(
            parse_script_namespace_key(&key),
            Some(("my-script", "MyClass"))
        );
    }

    #[test]
    fn script_namespace_key_rejects_garbage() {
        assert_eq!(parse_script_namespace_key("cells:v1:d1:__D1Database"), None);
        assert_eq!(parse_script_namespace_key("foreign"), None);
        assert_eq!(parse_script_namespace_key(""), None);
        assert_eq!(parse_script_namespace_key("cells:v1:99:a:b"), None);
        assert_eq!(parse_script_namespace_key("cells:v1:1:ab:c"), None);
        assert_eq!(parse_script_namespace_key("cells:v1:2:ab:"), None);
    }

    #[test]
    fn grant_set_releases_last_reference() {
        let mut set = super::GrantSet::default();
        set.grant("A:1").expect("grant");
        set.grant("A:1").expect("grant");
        assert!(set.granted("A:1"));
        set.release("A:1");
        assert!(set.granted("A:1"));
        set.release("A:1");
        assert!(!set.granted("A:1"));
        set.release("A:1");
        assert!(!set.granted("A:1"));
    }

    #[test]
    fn grant_set_pinned_survives_release() {
        let mut set = super::GrantSet::default();
        set.pin("Q:dead").expect("pin");
        set.grant("Q:dead").expect("grant");
        set.release("Q:dead");
        assert!(set.granted("Q:dead"));
    }

    #[test]
    fn grant_set_cap_covers_grants_and_pins() {
        let mut set = super::GrantSet::default();
        set.pin("Q:dead").expect("pin under cap");
        for index in 1..super::DO_GRANT_MAX_SCOPES {
            set.grant(&format!("A:{index}")).expect("grant under cap");
        }
        // Grants and pins share one budget: the set is full, so a new
        // scope of either kind is refused.
        assert!(set.grant("B:overflow").is_err());
        assert!(set.pin("B:overflow").is_err());
        assert!(!set.granted("B:overflow"));
        // A scope already held does not grow the set, so re-adding it at
        // the cap still succeeds.
        set.grant("A:1").expect("re-grant at cap");
        set.pin("Q:dead").expect("re-pin at cap");
    }

    #[test]
    fn grant_set_reference_count_saturates() {
        let mut set = super::GrantSet::default();
        set.counts.insert("A:1".to_string(), usize::MAX);
        set.grant("A:1").expect("saturating re-grant");
        assert_eq!(set.counts.get("A:1"), Some(&usize::MAX));
        assert!(set.granted("A:1"));
    }
}
