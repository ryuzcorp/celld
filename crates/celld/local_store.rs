// Modified by ryuzcorp (ryuzcorp/celld): staged activation; dev-store single-writer group commit (upstream #310).
// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The persistent object store for `celld dev`.
//!
//! The development supervisor and its node are separate processes, and both
//! write the fleet store during a reload. SQLite supplies the cross-process
//! transaction that a directory of files cannot: a conditional update checks
//! its ETag and installs the new object in one commit. This backend is local to
//! one development machine. It is not a shared-filesystem production mode.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, Error, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use rusqlite::{params, Connection, OptionalExtension as _, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot;

const STORE: &str = "celld development store";

#[derive(Clone, Debug)]
pub(crate) struct LocalStore {
    database: PathBuf,
    writer: Arc<StoreWriter>,
}

/// The dev store's single writer (upstream #310).
///
/// Every `put` funnels through one thread, one connection, and one queued
/// batch per transaction. Concurrent writers used to pile onto the file with
/// one connection and `Immediate` transaction each, losing the SQLite lock
/// lottery into `busy_timeout` sleeps: throughput collapsed toward one fsync
/// per write and unlucky uploads stalled seconds past the durability budget
/// (`DurabilityUnproven` under fan-out). Draining whatever is already queued
/// costs nothing when idle (a batch of one is today's single PUT) and shares
/// one commit fsync under load.
#[derive(Debug)]
struct StoreWriter {
    queue: mpsc::Sender<PutRequest>,
    /// Batches committed so far; the regression test below reads it.
    batches: Arc<AtomicUsize>,
}

/// One queued PUT for the store's writer thread.
struct PutRequest {
    key: String,
    body: Bytes,
    options: PutOptions,
    modified_ms: i64,
    reply: oneshot::Sender<object_store::Result<PutResult>>,
}

/// Rows per writer transaction. Bounds one commit's hold on the file against
/// concurrent readers and the supervisor's reload writes; a deeper queue
/// simply takes another transaction.
const WRITER_BATCH_ROWS: usize = 1024;

#[derive(Debug)]
struct StoredObject {
    key: String,
    body: Vec<u8>,
    size: u64,
    etag: i64,
    modified_ms: i64,
    attributes: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredAttribute {
    kind: String,
    name: Option<String>,
    value: String,
}

impl LocalStore {
    pub(crate) fn open(database: impl AsRef<FsPath>) -> object_store::Result<Self> {
        let database = database.as_ref().to_path_buf();
        // The writer thread ends when the last store handle drops: its
        // receive loop breaks once every sender is gone.
        let (queue, rx) = mpsc::channel();
        let writer_database = database.clone();
        let writer = Arc::new(StoreWriter {
            queue,
            batches: Arc::new(AtomicUsize::new(0)),
        });
        let writer_thread = writer.clone();
        std::thread::Builder::new()
            .name("celld-dev-store".to_string())
            .spawn(move || write_loop(&writer_database, rx, &writer_thread))
            .map_err(db_error)?;
        let store = Self { database, writer };
        let connection = store.connect()?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(db_error)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS objects (
                   key TEXT PRIMARY KEY,
                   body BLOB NOT NULL,
                   etag INTEGER NOT NULL,
                   modified_ms INTEGER NOT NULL,
                   attributes TEXT NOT NULL
                 );
                 CREATE TABLE IF NOT EXISTS store_sequence (
                   singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                   next_etag INTEGER NOT NULL
                 );
                 INSERT OR IGNORE INTO store_sequence(singleton, next_etag) VALUES (1, 1);",
            )
            .map_err(db_error)?;
        Ok(store)
    }

    fn connect(&self) -> object_store::Result<Connection> {
        let connection = Connection::open(&self.database).map_err(db_error)?;
        configure_connection(&connection)?;
        Ok(connection)
    }

    fn read(&self, key: &str) -> object_store::Result<StoredObject> {
        self.connect()?
            .query_row(
                "SELECT key, body, etag, modified_ms, attributes, length(body)
                 FROM objects WHERE key = ?1",
                [key],
                |row| {
                    Ok(StoredObject {
                        key: row.get(0)?,
                        body: row.get(1)?,
                        size: row.get(5)?,
                        etag: row.get(2)?,
                        modified_ms: row.get(3)?,
                        attributes: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(db_error)?
            .ok_or_else(|| not_found(key))
    }

    fn metadata(&self) -> object_store::Result<Vec<StoredObject>> {
        let connection = self.connect()?;
        let mut statement = connection
            .prepare(
                "SELECT key, CAST(X'' AS BLOB), etag, modified_ms, attributes, length(body)
                 FROM objects ORDER BY key",
            )
            .map_err(db_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok(StoredObject {
                    key: row.get(0)?,
                    body: row.get(1)?,
                    size: row.get(5)?,
                    etag: row.get(2)?,
                    modified_ms: row.get(3)?,
                    attributes: row.get(4)?,
                })
            })
            .map_err(db_error)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db_error)
    }

    fn put_sync(
        &self,
        key: String,
        body: Bytes,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let modified_ms = crate::asyncrt::wall_ms();
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let result = apply_put(&transaction, &key, &body, &options, modified_ms)?;
        transaction.commit().map_err(db_error)?;
        Ok(result)
    }
}

/// One row of a dev-store write, inside the caller's transaction: the
/// conditional check, the ETag draw, and the upsert. Shared by the
/// single-PUT path and the writer thread's batches so both enforce the same
/// conflict rule.
fn apply_put(
    transaction: &Transaction,
    key: &str,
    body: &[u8],
    options: &PutOptions,
    modified_ms: i64,
) -> object_store::Result<PutResult> {
    let attributes = encode_attributes(&options.attributes)?;
    let current = transaction
        .query_row("SELECT etag FROM objects WHERE key = ?1", [key], |row| {
            row.get::<_, i64>(0)
        })
        .optional()
        .map_err(db_error)?;
    match &options.mode {
        PutMode::Overwrite => {}
        PutMode::Create if current.is_some() => return Err(already_exists(key)),
        PutMode::Create => {}
        PutMode::Update(version) => {
            let current = current.map(|etag| etag.to_string());
            if current.as_deref().is_none() || version.e_tag.as_deref() != current.as_deref() {
                return Err(precondition(key));
            }
        }
    }
    let etag = transaction
        .query_row(
            "UPDATE store_sequence SET next_etag = next_etag + 1
             WHERE singleton = 1 RETURNING next_etag - 1",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map_err(db_error)?;
    transaction
        .execute(
            "INSERT INTO objects(key, body, etag, modified_ms, attributes)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(key) DO UPDATE SET
               body = excluded.body,
               etag = excluded.etag,
               modified_ms = excluded.modified_ms,
               attributes = excluded.attributes",
            params![key, body, etag, modified_ms, attributes],
        )
        .map_err(db_error)?;
    Ok(PutResult {
        e_tag: Some(etag.to_string()),
        version: None,
    })
}

/// The writer thread: one connection for the store's lifetime, one
/// transaction per drained batch. A connection that cannot open (or
/// configure) fails every queued PUT instead of hanging it; the loop then
/// keeps failing newcomers until the last handle drops.
fn write_loop(database: &FsPath, rx: mpsc::Receiver<PutRequest>, writer: &StoreWriter) {
    let connection = Connection::open(database)
        .map_err(db_error)
        .and_then(|connection| configure_connection(&connection).map(|()| connection));
    match connection {
        Ok(mut connection) => batch_loop(&mut connection, rx, writer),
        Err(error) => {
            let message = error.to_string();
            for request in rx {
                let _ = request.reply.send(Err(message_error(message.clone())));
            }
        }
    }
}

fn batch_loop(connection: &mut Connection, rx: mpsc::Receiver<PutRequest>, writer: &StoreWriter) {
    loop {
        let first = match rx.recv() {
            Ok(request) => request,
            // Every sender is gone: the last store handle dropped.
            Err(_) => return,
        };
        let mut batch = vec![first];
        while batch.len() < WRITER_BATCH_ROWS {
            match rx.try_recv() {
                Ok(request) => batch.push(request),
                // Empty and disconnected both mean nothing more is
                // available right now; the next `recv` parks or ends.
                Err(_) => break,
            }
        }
        writer.batches.fetch_add(1, Ordering::SeqCst);
        for (request, outcome) in apply_batch(connection, batch) {
            let _ = request.reply.send(outcome);
        }
    }
}

/// One commit for the whole batch. A row that fails its own check fails
/// alone behind a savepoint — as independent transactions do today — while
/// the surviving rows still commit together. A failure to begin or commit
/// the shared transaction fails every row; each caller retries its own
/// upload, and PUTs are idempotent.
fn apply_batch(
    connection: &mut Connection,
    batch: Vec<PutRequest>,
) -> Vec<(PutRequest, object_store::Result<PutResult>)> {
    let transaction = match connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(db_error)
    {
        Ok(transaction) => transaction,
        Err(error) => {
            let message = error.to_string();
            return batch
                .into_iter()
                .map(|request| (request, Err(message_error(message.clone()))))
                .collect();
        }
    };
    let mut outcomes = Vec::with_capacity(batch.len());
    for request in batch {
        let outcome = (|| {
            transaction
                .execute_batch("SAVEPOINT batch_row")
                .map_err(db_error)?;
            let outcome = apply_put(
                &transaction,
                &request.key,
                &request.body,
                &request.options,
                request.modified_ms,
            );
            match &outcome {
                Ok(_) => transaction
                    .execute_batch("RELEASE batch_row")
                    .map_err(db_error)?,
                Err(_) => transaction
                    .execute_batch("ROLLBACK TO batch_row")
                    .map_err(db_error)?,
            }
            outcome
        })();
        outcomes.push((request, outcome));
    }
    if let Err(error) = transaction.commit().map_err(db_error) {
        let message = error.to_string();
        return outcomes
            .into_iter()
            .map(|(request, _)| (request, Err(message_error(message.clone()))))
            .collect();
    }
    outcomes
}

impl LocalStore {
    fn copy_sync(&self, from: &str, to: &str, create: bool) -> object_store::Result<()> {
        let source = self.read(from)?;
        let attributes = decode_attributes(&source.attributes)?;
        self.put_sync(
            to.to_string(),
            source.body.into(),
            PutOptions {
                mode: if create {
                    PutMode::Create
                } else {
                    PutMode::Overwrite
                },
                attributes,
                ..PutOptions::default()
            },
        )?;
        Ok(())
    }
}

fn configure_connection(connection: &Connection) -> object_store::Result<()> {
    // `synchronous` is connection-local. Setting it only while the schema is
    // created leaves later writes at the bundled SQLite default, so a build
    // configuration can silently weaken the development store's durability.
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(db_error)?;
    connection
        .busy_timeout(Duration::from_secs(30))
        .map_err(db_error)
}

impl fmt::Display for LocalStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "LocalStore({})", self.database.display())
    }
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let request = PutRequest {
            key: location.to_string(),
            body: payload.into(),
            options,
            modified_ms: crate::asyncrt::wall_ms(),
            reply: reply_tx,
        };
        // The writer thread owns the only write connection. `send` never
        // blocks, and it fails only after the thread panicked — a bug, never
        // a store state — so fall back to today's direct PUT rather than
        // fail every write from here on.
        if let Err(failed) = self.writer.queue.send(request) {
            let request = failed.0;
            let store = self.clone();
            return crate::asyncrt::blocking(move || {
                store.put_sync(request.key, request.body, request.options)
            })
            .await
            .map_err(db_error)?;
        }
        reply_rx
            .await
            .map_err(|_| message_error("dev store writer stopped"))?
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(LocalUpload {
            store: self.clone(),
            location: location.clone(),
            attributes: options.attributes,
            parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let store = self.clone();
        let key = location.to_string();
        let object = crate::asyncrt::blocking(move || store.read(&key))
            .await
            .map_err(db_error)??;
        let meta = object_meta(&object)?;
        options.check_preconditions(&meta)?;
        let full = 0..object.body.len() as u64;
        let range = match options.range {
            Some(range) => range.as_range(full.end).map_err(db_error)?,
            None => full,
        };
        let body = Bytes::from(object.body).slice(range.start as usize..range.end as usize);
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(body) }).boxed()),
            meta,
            range,
            attributes: decode_attributes(&object.attributes)?,
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let key = location.to_string();
        crate::asyncrt::blocking(move || {
            store
                .connect()?
                .execute("DELETE FROM objects WHERE key = ?1", [key])
                .map_err(db_error)?;
            Ok(())
        })
        .await
        .map_err(db_error)?
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        // S3 byte-prefix semantics, like the fleet backends: the prefix is
        // raw bytes, so a partial segment matches its siblings and an
        // object at the prefix itself is returned. A segment-scoped
        // listing here masked a discard that deleted serving code on S3
        // while every hermetic test stayed green.
        let prefix = prefix.cloned().unwrap_or_default();
        let want: &str = prefix.as_ref();
        let result = self.metadata().and_then(|objects| {
            objects
                .into_iter()
                .map(|object| object_meta(&object))
                .filter(|meta| {
                    meta.as_ref().map_or(true, |meta| {
                        let location: &str = meta.location.as_ref();
                        location.starts_with(want)
                    })
                })
                .collect::<object_store::Result<Vec<_>>>()
        });
        match result {
            Ok(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for object in self.metadata()? {
            let location = stored_location(&object.key)?;
            let Some(mut remainder) = location.prefix_match(&prefix) else {
                continue;
            };
            let Some(child) = remainder.next() else {
                continue;
            };
            if remainder.next().is_some() {
                common_prefixes.insert(prefix.child(child));
            } else {
                objects.push(object_meta(&object)?);
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        crate::asyncrt::blocking(move || store.copy_sync(&from, &to, false))
            .await
            .map_err(db_error)?
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        crate::asyncrt::blocking(move || store.copy_sync(&from, &to, true))
            .await
            .map_err(db_error)?
    }
}

#[async_trait]
impl PaginatedListStore for LocalStore {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        let prefix = prefix.unwrap_or_default();
        let after = options.page_token.as_deref().or(options.offset.as_deref());
        let delimiter = options.delimiter.as_deref();
        let limit = options.max_keys.unwrap_or(usize::MAX);
        let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
        for object in self.metadata()? {
            let Some(remainder) = object.key.strip_prefix(prefix) else {
                continue;
            };
            if after.is_some_and(|after| object.key.as_str() <= after) {
                continue;
            }
            let common = delimiter.and_then(|delimiter| {
                remainder.find(delimiter).map(|index| {
                    stored_location(&format!("{prefix}{}{}", &remainder[..index], delimiter))
                })
            });
            let common = common.transpose()?;
            if let Some(common) = common {
                if let Some((last, Some(previous), _)) = entries.last_mut() {
                    if *previous == common {
                        *last = object.key;
                        continue;
                    }
                }
                entries.push((object.key, Some(common), None));
            } else {
                entries.push((object.key.clone(), None, Some(object_meta(&object)?)));
            }
        }
        let truncated = entries.len() > limit;
        entries.truncate(limit);
        let page_token = truncated
            .then(|| entries.last().map(|(last, _, _)| last.clone()))
            .flatten();
        let mut result = ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
        };
        for (_, common, object) in entries {
            if let Some(common) = common {
                result.common_prefixes.push(common);
            }
            if let Some(object) = object {
                result.objects.push(object);
            }
        }
        Ok(PaginatedListResult { result, page_token })
    }
}

#[derive(Debug)]
struct LocalUpload {
    store: LocalStore,
    location: Path,
    attributes: Attributes,
    parts: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl MultipartUpload for LocalUpload {
    fn put_part(&mut self, data: PutPayload) -> BoxFuture<'static, object_store::Result<()>> {
        self.parts.lock().unwrap().push(data.into());
        async { Ok(()) }.boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let parts = std::mem::take(&mut *self.parts.lock().unwrap());
        let bytes = parts.iter().map(Bytes::len).sum();
        let mut body = Vec::with_capacity(bytes);
        for part in parts {
            body.extend_from_slice(&part);
        }
        self.store
            .put_opts(
                &self.location,
                body.into(),
                PutOptions {
                    attributes: self.attributes.clone(),
                    ..PutOptions::default()
                },
            )
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.parts.lock().unwrap().clear();
        Ok(())
    }
}

fn object_meta(object: &StoredObject) -> object_store::Result<ObjectMeta> {
    let modified = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_millis(object.modified_ms.max(0) as u64))
        .ok_or_else(|| message_error("the object timestamp is outside the system clock range"))?;
    Ok(ObjectMeta {
        location: stored_location(&object.key)?,
        last_modified: modified.into(),
        size: object.size,
        e_tag: Some(object.etag.to_string()),
        version: None,
    })
}

fn encode_attributes(attributes: &Attributes) -> object_store::Result<String> {
    let mut stored = Vec::with_capacity(attributes.len());
    for (attribute, value) in attributes {
        let (kind, name) = match attribute {
            Attribute::ContentDisposition => ("content-disposition", None),
            Attribute::ContentEncoding => ("content-encoding", None),
            Attribute::ContentLanguage => ("content-language", None),
            Attribute::ContentType => ("content-type", None),
            Attribute::CacheControl => ("cache-control", None),
            Attribute::StorageClass => ("storage-class", None),
            Attribute::Metadata(name) => ("metadata", Some(name.as_ref().to_string())),
            _ => {
                return Err(message_error(
                    "the development store does not support this attribute",
                ))
            }
        };
        stored.push(StoredAttribute {
            kind: kind.to_string(),
            name,
            value: value.as_ref().to_string(),
        });
    }
    serde_json::to_string(&stored).map_err(db_error)
}

fn decode_attributes(encoded: &str) -> object_store::Result<Attributes> {
    let stored: Vec<StoredAttribute> = serde_json::from_str(encoded).map_err(db_error)?;
    let mut attributes = Attributes::with_capacity(stored.len());
    for stored in stored {
        let attribute = match stored.kind.as_str() {
            "content-disposition" => Attribute::ContentDisposition,
            "content-encoding" => Attribute::ContentEncoding,
            "content-language" => Attribute::ContentLanguage,
            "content-type" => Attribute::ContentType,
            "cache-control" => Attribute::CacheControl,
            "storage-class" => Attribute::StorageClass,
            "metadata" => Attribute::Metadata(stored.name.unwrap_or_default().into()),
            kind => return Err(message_error(format!("unknown stored attribute {kind:?}"))),
        };
        attributes.insert(attribute, AttributeValue::from(stored.value));
    }
    Ok(attributes)
}

#[cfg(all(test, celld_internal_tests))]
mod internal_tests {
    include!(env!("CELLD_INTERNAL_LOCAL_STORE_TESTS"));
}

/// The [`Path`] a stored key came from. Every key is stored as
/// `location.to_string()`, which is already percent-encoded, so this parses
/// that form back. `Path::from` would encode it a second time, and a listed
/// `přehled.html` would come back as `p%25C5%2599ehled.html`, a key that
/// names no object (denoland/celld#232).
fn stored_location(key: &str) -> object_store::Result<Path> {
    Ok(Path::parse(key)?)
}

fn db_error(error: impl fmt::Display) -> Error {
    message_error(error.to_string())
}

fn message_error(message: impl Into<String>) -> Error {
    Error::Generic {
        store: STORE,
        source: Box::new(std::io::Error::other(message.into())),
    }
}

fn not_found(path: &str) -> Error {
    Error::NotFound {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "the object does not exist",
        )),
    }
}

fn already_exists(path: &str) -> Error {
    Error::AlreadyExists {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "the object already exists",
        )),
    }
}

fn precondition(path: &str) -> Error {
    Error::Precondition {
        path: path.to_string(),
        source: Box::new(std::io::Error::other("the ETag does not match")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::UpdateVersion;

    fn open_store() -> (tempfile::TempDir, LocalStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalStore::open(dir.path().join("objects.sqlite3")).unwrap();
        (dir, store)
    }

    fn put_mode(mode: PutMode) -> PutOptions {
        PutOptions {
            mode,
            ..PutOptions::default()
        }
    }

    async fn body_of(store: &LocalStore, key: &str) -> Vec<u8> {
        let location: Path = key.to_string().into();
        store
            .get(&location)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .to_vec()
    }

    /// Upstream #310: one barrier-synchronized burst of 64 PUTs lands every
    /// row and commits in a handful of batches, not one transaction per PUT.
    /// The bound has 4x margin over the observed 1-4; without the writer the
    /// same burst is 64 separate commits.
    #[test]
    fn writer_batches_one_burst_into_few_commits() {
        let runtime = crate::asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let (_dir, store) = open_store();
            let store = Arc::new(store);
            let barrier = Arc::new(tokio::sync::Barrier::new(64));
            let mut tasks = Vec::new();
            for i in 0u8..64 {
                let (store, barrier) = (store.clone(), barrier.clone());
                tasks.push(tokio::spawn(async move {
                    barrier.wait().await;
                    let location: Path = format!("k{i:03}").into();
                    store
                        .put(&location, PutPayload::from(Bytes::from(vec![i; 64])))
                        .await
                        .unwrap();
                }));
            }
            for task in tasks {
                task.await.unwrap();
            }
            for i in 0u8..64 {
                assert_eq!(
                    body_of(&store, &format!("k{i:03}")).await,
                    vec![i; 64],
                    "every row of the burst lands intact"
                );
            }
            let batches = store.writer.batches.load(Ordering::SeqCst);
            assert!(
                (1..=16).contains(&batches),
                "one burst shares commits: {batches} batches for 64 PUTs"
            );
        });
    }

    /// The shared transaction preserves each row's own conflict rule: a
    /// duplicate Create and a stale Update fail alone while the surviving
    /// rows commit together.
    #[test]
    fn writer_batch_preserves_row_conflicts() {
        let (_dir, store) = open_store();
        let mut connection = store.connect().unwrap();
        let request = |key: &str, body: &[u8], options: PutOptions| PutRequest {
            key: key.to_string(),
            body: Bytes::from(body.to_vec()),
            options,
            modified_ms: 1,
            reply: oneshot::channel().0,
        };
        let outcomes = apply_batch(
            &mut connection,
            vec![
                request("k", b"a", put_mode(PutMode::Create)),
                request("k", b"b", put_mode(PutMode::Create)),
                request(
                    "k",
                    b"c",
                    put_mode(PutMode::Update(UpdateVersion {
                        e_tag: Some("stale".to_string()),
                        version: None,
                    })),
                ),
                request("k", b"d", put_mode(PutMode::Overwrite)),
            ],
        );
        assert!(outcomes[0].1.is_ok());
        assert!(matches!(outcomes[1].1, Err(Error::AlreadyExists { .. })));
        assert!(matches!(outcomes[2].1, Err(Error::Precondition { .. })));
        assert!(outcomes[3].1.is_ok());
        drop(outcomes);
        let location: Path = "k".to_string().into();
        let object = store.get(&location);
        let runtime = crate::asyncrt::shared_test_runtime();
        runtime.block_on(async {
            assert_eq!(object.await.unwrap().bytes().await.unwrap().to_vec(), b"d");
        });
    }

    /// Concurrent duplicate Creates have exactly one winner, and an Update
    /// on a read-back ETag succeeds while a stale one is refused.
    #[test]
    fn concurrent_conditional_puts_stay_correct() {
        let runtime = crate::asyncrt::shared_test_runtime();
        runtime.block_on(async {
            let (_dir, store) = open_store();
            let location: Path = "k".to_string().into();
            let create = || {
                let store = &store;
                let location = &location;
                async move {
                    store
                        .put_opts(
                            location,
                            PutPayload::from(Bytes::from(b"x".to_vec())),
                            put_mode(PutMode::Create),
                        )
                        .await
                }
            };
            let results = futures_util::future::join_all([create(), create(), create()]).await;
            assert_eq!(
                results.iter().filter(|r| r.is_ok()).count(),
                1,
                "exactly one duplicate Create wins"
            );
            assert!(
                results
                    .iter()
                    .all(|r| r.is_ok() || matches!(r, Err(Error::AlreadyExists { .. }))),
                "losers fail as conflicts, never as store errors"
            );
            let head = store.head(&location).await.unwrap();
            let good = store
                .put_opts(
                    &location,
                    PutPayload::from(Bytes::from(b"z".to_vec())),
                    put_mode(PutMode::Update(UpdateVersion {
                        e_tag: head.e_tag,
                        version: None,
                    })),
                )
                .await;
            assert!(good.is_ok(), "an Update on the read-back ETag lands");
            let stale = store
                .put_opts(
                    &location,
                    PutPayload::from(Bytes::from(b"w".to_vec())),
                    put_mode(PutMode::Update(UpdateVersion {
                        e_tag: Some("stale".to_string()),
                        version: None,
                    })),
                )
                .await;
            assert!(
                matches!(stale, Err(Error::Precondition { .. })),
                "a stale ETag is refused"
            );
            assert_eq!(body_of(&store, "k").await, b"z");
        });
    }
}
