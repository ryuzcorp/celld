// Modified by ryuzcorp (ryuzcorp/celld): O(1) hydration count (upstream #270).
//! paged_vfs.rs — a SQLite VFS that faults main-db pages in from the bucket.
//!
//! The fault-in model of paged restore. The main db is a real local file
//! that starts sparse, sized to the pinned cut. On a read of a page the
//! local file does not hold yet, the VFS fetches that page's run from the
//! bucket ([`PageSource`]), writes it into the local file, then lets SQLite
//! read it normally. SQLite otherwise runs as usual over the local file —
//! WAL, page-1 writes, checkpoint — and faulting is hydration.
//!
//! It is a thin wrapper over a base VFS (the host's named VFS, or SQLite's
//! default): every method forwards, except the main db's `xRead`, which
//! faults first, `xWrite` and `xTruncate`, which keep the hydration set
//! honest, and `xOpen`, which sizes the sparse file. The invariants:
//!
//! - One registration serves one database. The hydration set is per
//!   registration, shared by every connection to that file, so a checkpoint
//!   or an earlier fault by one connection is never overwritten by another
//!   connection re-faulting the older cut page.
//! - A page is faulted at most once, and only when the local file does not
//!   hold newer bytes: a page a connection wrote (a checkpoint) is marked
//!   hydrated, and a run that arrives with a fault only fills pages not yet
//!   hydrated.
//! - A page absent from the cut past the commit reads as
//!   the sparse file's zeros, and is marked resolved so it never faults again.
//! - Pages truncated away are forgotten, so a later regrowth faults or
//!   writes them afresh rather than trusting stale marks.
//! - A registration serves one file, the cell's database; any other main
//!   database opened by its name is a plain file, so no second sparse view
//!   can mark the cell's hydration set for bytes the cell never received.
//! - The fault runs on the calling thread and touches no runtime; a failure
//!   surfaces to SQLite as `SQLITE_IOERR_READ` on that read.
//! - Unregistering frees the registration; open files keep their own
//!   references to the reader and the hydration set, and SQLite opens
//!   nothing new through an unregistered VFS.

use crate::ltx::lock_pgno;
use crate::paged::PageSource;
use rusqlite::ffi;
use std::collections::HashSet;
use std::ffi::{c_void, CStr, CString};
use std::os::raw::{c_char, c_int};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

/// Type-erased page reader so one VFS registration can serve any client type.
pub trait PageReader: Send + Sync {
    fn page_size(&self) -> u32;
    fn commit(&self) -> u32;
    /// The cut's bytes for `pgno` and the pages stored right after it that
    /// came along in the same read (first entry is `pgno`); empty if the cut
    /// has no such page.
    fn read_run(&self, pgno: u32) -> crate::error::Result<Vec<(u32, Vec<u8>)>>;
    /// Faults served so far, for the hydration count.
    fn faults(&self) -> u64 {
        0
    }
}

impl PageReader for PageSource {
    fn page_size(&self) -> u32 {
        PageSource::page_size(self)
    }
    fn commit(&self) -> u32 {
        PageSource::commit(self)
    }
    fn read_run(&self, pgno: u32) -> crate::error::Result<Vec<(u32, Vec<u8>)>> {
        self.read_run_blocking(pgno)
    }
    fn faults(&self) -> u64 {
        PageSource::faults(self)
    }
}

/// The hydration set with an exact cut-relative count, so `hydration()` is
/// O(1) (upstream #270): a full walk of the cut per faulted read made a
/// whale's fill quadratic in its size (111 min for 8.9 GB) and stalled
/// foreground reads behind the walk's mutex hold. The count tracks only
/// pages inside the cut (`1..=commit`, never the lock page); the commit is
/// fixed for the registration, so inserts and truncations keep it exact.
#[derive(Default)]
struct HydratedSet {
    pages: HashSet<u32>,
    counted: u32,
}

impl HydratedSet {
    fn contains(&self, pgno: &u32) -> bool {
        self.pages.contains(pgno)
    }

    /// Whether a page counts toward the cut's hydration.
    fn in_cut(pgno: u32, commit: u32, lock: u32) -> bool {
        (1..=commit).contains(&pgno) && pgno != lock
    }

    /// Mark one page resolved, counting it when it is inside the cut.
    /// Past-cut pages (sparse zeros) and the lock page resolve without
    /// counting, exactly as the set membership always did.
    fn insert_counted(&mut self, pgno: u32, commit: u32, lock: u32) {
        if self.pages.insert(pgno) && Self::in_cut(pgno, commit, lock) {
            self.counted += 1;
        }
    }

    /// Mark a written range resolved, counting the pages inside the cut.
    fn mark_range(&mut self, first: u32, last: u32, commit: u32, lock: u32) {
        for pgno in first..=last {
            self.insert_counted(pgno, commit, lock);
        }
    }

    /// Forget pages past a truncation, uncounting the ones inside the cut.
    fn retain_above(&mut self, keep: u32, commit: u32, lock: u32) {
        let mut uncounted = 0;
        self.pages.retain(|pgno| {
            let drop_page = *pgno > keep;
            if drop_page && Self::in_cut(*pgno, commit, lock) {
                uncounted += 1;
            }
            !drop_page
        });
        self.counted -= uncounted;
    }
}

/// Per-registration state, hung off the VFS `pAppData`.
struct VfsAppData {
    base: *mut ffi::sqlite3_vfs,
    /// The cell's database, as the base VFS spells it: the one file the
    /// registration serves.
    path: CString,
    reader: Arc<dyn PageReader>,
    /// Files open through this registration. Unregistering with files still
    /// open used to free the VFS under them: the pager's close deletes the
    /// WAL through `xDelete`, which reads `pAppData` of the freed struct.
    /// The registration is freed by whichever comes last, the unregister or
    /// the final close.
    open_files: std::sync::atomic::AtomicUsize,
    retired: std::sync::atomic::AtomicBool,
    /// Pages whose newest bytes the local main-db file already holds, shared
    /// by every connection opened through this registration. Per-connection
    /// sets would let a later connection re-fault the older cut page over a
    /// page another connection already checkpointed — silent data rollback.
    /// One registration serves one database, so registration scope is
    /// database scope.
    hydrated: Arc<Mutex<HydratedSet>>,
}

/// Our `sqlite3_file` subclass: SQLite reads `methods` (first field); the rest is
/// ours. `base` is the underlying VFS's file; `state` its fault-in bookkeeping.
#[repr(C)]
struct PagedFile {
    methods: *const ffi::sqlite3_io_methods,
    base: *mut ffi::sqlite3_file,
    state: *mut FileState,
}

struct FileState {
    /// The registration, for the open-file count (see [`VfsAppData::open_files`]).
    vfs: *mut ffi::sqlite3_vfs,
    /// `Some` only for the main db; other files (WAL, journal) forward untouched.
    reader: Option<Arc<dyn PageReader>>,
    /// The registration's shared hydration set (see [`VfsAppData::hydrated`]).
    hydrated: Arc<Mutex<HydratedSet>>,
    page_size: u32,
}

unsafe fn base_vfs(vfs: *mut ffi::sqlite3_vfs) -> *mut ffi::sqlite3_vfs {
    (*((*vfs).pAppData as *mut VfsAppData)).base
}

unsafe fn base_methods(file: *mut ffi::sqlite3_file) -> *const ffi::sqlite3_io_methods {
    let paged = file as *mut PagedFile;
    (*(*paged).base).pMethods
}

// ── file methods ────────────────────────────────────────────────────────────

/// A panic here would unwind across `extern "C"` and abort the process; the
/// fault path spawns threads and locks a mutex, both of which can panic.
/// It is an I/O error to the query instead.
unsafe extern "C" fn x_read(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    ofst: ffi::sqlite3_int64,
) -> c_int {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        x_read_inner(file, buf, amt, ofst)
    }))
    .unwrap_or(ffi::SQLITE_IOERR_READ)
}

unsafe fn x_read_inner(
    file: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    ofst: ffi::sqlite3_int64,
) -> c_int {
    let paged = file as *mut PagedFile;
    let state = &*(*paged).state;
    if let Some(reader) = &state.reader {
        let ps = state.page_size.max(1) as i64;
        let first = (ofst / ps) as u32 + 1;
        let last = ((ofst + amt as i64 - 1) / ps) as u32 + 1;
        for pgno in first..=last {
            if state.hydrated.lock().unwrap().contains(&pgno) {
                continue;
            }
            // The fetch runs without the lock, so a fault by one connection
            // never stalls another's read of a hydrated page, and a
            // background hydration step never stalls a foreground fault.
            // Two faults of one run both fetch; the loser writes nothing.
            let run = match reader.read_run(pgno) {
                Ok(run) => run,
                // The fill can complete while this fetch is in flight, and
                // then the owner's epoch GC may delete the objects the fetch
                // reads. The page is in the local file by then; read it there
                // instead of failing the query.
                Err(_) if state.hydrated.lock().unwrap().contains(&pgno) => continue,
                Err(_) => return ffi::SQLITE_IOERR_READ,
            };
            let mut hydrated = state.hydrated.lock().unwrap();
            if hydrated.contains(&pgno) {
                continue;
            }
            // Absent in the cut. Past the commit (or the lock page) the
            // sparse file's zeros stand; mark resolved so we do not re-fault.
            // Inside it the map is broken — a plan starts with a snapshot,
            // and a snapshot carries every page — and zeros would go out as
            // data (a 2 GB opener two thirds holes, fleet 2026-09-02): fail
            // the read instead.
            let commit = reader.commit();
            let lock = lock_pgno(ps as u32);
            if run.is_empty() {
                if pgno <= commit && pgno != lock {
                    return ffi::SQLITE_IOERR_READ;
                }
                hydrated.insert_counted(pgno, commit, lock);
                continue;
            }
            // Hydrate the whole run, except a page that already holds newer
            // bytes (a checkpoint, or an earlier run) — the cut is older.
            for (pgno_, page) in run {
                if pgno_ != pgno && hydrated.contains(&pgno_) {
                    continue;
                }
                let base = (*paged).base;
                let write = (*base_methods(file)).xWrite.unwrap();
                let rc = write(
                    base,
                    page.as_ptr() as *const c_void,
                    page.len() as c_int,
                    (pgno_ as i64 - 1) * ps,
                );
                if rc != ffi::SQLITE_OK {
                    return rc;
                }
                hydrated.insert_counted(pgno_, commit, lock);
            }
        }
    }
    let read = (*base_methods(file)).xRead.unwrap();
    read((*paged).base, buf, amt, ofst)
}

unsafe extern "C" fn x_write(
    file: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    ofst: ffi::sqlite3_int64,
) -> c_int {
    // A write (e.g. a checkpoint) establishes the newest bytes for its pages, so
    // mark them hydrated to keep a later read from faulting an older cut page
    // over them.
    let paged = file as *mut PagedFile;
    let state = &*(*paged).state;
    if let Some(reader) = state.reader.as_ref() {
        let ps = state.page_size.max(1) as i64;
        let first = (ofst / ps) as u32 + 1;
        let last = ((ofst + amt as i64 - 1) / ps) as u32 + 1;
        let commit = reader.commit();
        let lock = lock_pgno(state.page_size.max(1));
        let mut hydrated = state.hydrated.lock().unwrap();
        let write = (*base_methods(file)).xWrite.unwrap();
        let rc = write((*paged).base, buf, amt, ofst);
        // Only bytes that landed count: a mark on a failed write would turn
        // the page into a hole the fault path never fills.
        if rc == ffi::SQLITE_OK {
            hydrated.mark_range(first, last, commit, lock);
        }
        return rc;
    }
    let write = (*base_methods(file)).xWrite.unwrap();
    write((*paged).base, buf, amt, ofst)
}

unsafe extern "C" fn x_close(file: *mut ffi::sqlite3_file) -> c_int {
    let paged = file as *mut PagedFile;
    let rc = match (*base_methods(file)).xClose {
        Some(close) => close((*paged).base),
        None => ffi::SQLITE_OK,
    };
    ffi::sqlite3_free((*paged).base as *mut c_void);
    let state = Box::from_raw((*paged).state);
    let vfs = state.vfs;
    drop(state);
    let app = &*((*vfs).pAppData as *mut VfsAppData);
    let last = app.open_files.fetch_sub(1, Ordering::AcqRel) == 1;
    if last && app.retired.load(Ordering::Acquire) {
        free_vfs(vfs);
    }
    rc
}

/// Frees a registration's allocations: after its unregister, once no file
/// is open through it.
unsafe fn free_vfs(vfs: *mut ffi::sqlite3_vfs) {
    drop(Box::from_raw((*vfs).pAppData as *mut VfsAppData));
    drop(CString::from_raw((*vfs).zName as *mut c_char));
    drop(Box::from_raw(vfs));
}

/// Forward an io-method to the base file verbatim.
macro_rules! forward_io {
    ($name:ident ( $($arg:ident : $ty:ty),* ) $(-> $ret:ty)? => $field:ident) => {
        unsafe extern "C" fn $name(file: *mut ffi::sqlite3_file $(, $arg: $ty)*) $(-> $ret)? {
            let paged = file as *mut PagedFile;
            let m = (*base_methods(file)).$field.unwrap();
            m((*paged).base $(, $arg)*)
        }
    };
}

unsafe extern "C" fn x_truncate(file: *mut ffi::sqlite3_file, size: ffi::sqlite3_int64) -> c_int {
    // Pages past the new size are gone from the local file; drop their marks
    // so a later regrowth faults or writes them afresh instead of reading
    // the base file's zeros as hydrated data.
    let paged = file as *mut PagedFile;
    let state = &*(*paged).state;
    if let Some(reader) = state.reader.as_ref() {
        let ps = state.page_size.max(1) as i64;
        let keep = ((size + ps - 1) / ps).max(0) as u32;
        let commit = reader.commit();
        let lock = lock_pgno(state.page_size.max(1));
        state
            .hydrated
            .lock()
            .unwrap()
            .retain_above(keep, commit, lock);
    }
    let truncate = (*base_methods(file)).xTruncate.unwrap();
    truncate((*paged).base, size)
}
forward_io!(x_sync(flags: c_int) -> c_int => xSync);
forward_io!(x_file_size(size: *mut ffi::sqlite3_int64) -> c_int => xFileSize);
forward_io!(x_lock(lock: c_int) -> c_int => xLock);
forward_io!(x_unlock(lock: c_int) -> c_int => xUnlock);
forward_io!(x_check_reserved_lock(out: *mut c_int) -> c_int => xCheckReservedLock);
forward_io!(x_file_control(op: c_int, arg: *mut c_void) -> c_int => xFileControl);
forward_io!(x_sector_size() -> c_int => xSectorSize);
forward_io!(x_device_characteristics() -> c_int => xDeviceCharacteristics);
forward_io!(x_shm_map(pg: c_int, sz: c_int, ext: c_int, out: *mut *mut c_void) -> c_int => xShmMap);
forward_io!(x_shm_lock(ofst: c_int, n: c_int, flags: c_int) -> c_int => xShmLock);
forward_io!(x_shm_unmap(delete: c_int) -> c_int => xShmUnmap);

unsafe extern "C" fn x_shm_barrier(file: *mut ffi::sqlite3_file) {
    if let Some(barrier) = (*base_methods(file)).xShmBarrier {
        let paged = file as *mut PagedFile;
        barrier((*paged).base);
    }
}

static PAGED_IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 2,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: Some(x_shm_map),
    xShmLock: Some(x_shm_lock),
    xShmBarrier: Some(x_shm_barrier),
    xShmUnmap: Some(x_shm_unmap),
    xFetch: None,
    xUnfetch: None,
};

// ── vfs methods ───────────────────────────────────────────────────────────────

/// The path SQLite hands `xOpen` is the base VFS's full pathname (absolute,
/// symlinks resolved on unix), so the registered path is spelled the same
/// way, by the same VFS, before the comparison.
unsafe fn full_pathname(base: *mut ffi::sqlite3_vfs, path: &Path) -> Option<CString> {
    let raw = CString::new(path.as_os_str().as_encoded_bytes()).ok()?;
    let mut out = vec![0u8; (*base).mxPathname as usize + 1];
    let full = (*base).xFullPathname?;
    let rc = full(
        base,
        raw.as_ptr(),
        out.len() as c_int,
        out.as_mut_ptr().cast(),
    );
    // SQLITE_OK_SYMLINK (OK in the low byte) reports a resolved symlink.
    if rc & 0xff != ffi::SQLITE_OK {
        return None;
    }
    CStr::from_bytes_until_nul(&out).ok().map(CStr::to_owned)
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    name: *const c_char,
    file: *mut ffi::sqlite3_file,
    flags: c_int,
    out_flags: *mut c_int,
) -> c_int {
    let app = &*((*vfs).pAppData as *mut VfsAppData);
    let base = app.base;
    // A separate base file, sized as the base VFS expects.
    let base_file = ffi::sqlite3_malloc((*base).szOsFile) as *mut ffi::sqlite3_file;
    if base_file.is_null() {
        return ffi::SQLITE_NOMEM;
    }
    std::ptr::write_bytes(base_file as *mut u8, 0, (*base).szOsFile as usize);

    let open = (*base).xOpen.unwrap();
    let rc = open(base, name, base_file, flags, out_flags);
    if rc != ffi::SQLITE_OK {
        ffi::sqlite3_free(base_file as *mut c_void);
        return rc;
    }

    // The registration serves one file: the cell's database. Any other
    // main database opened by this VFS name (a backup destination, an
    // inspection copy, a neighbour's file) is a plain file, because a second
    // sparse view of the cut would share the hydration set and mark pages
    // the cell's file never received, so the cell reads holes
    // (CelldPagedVfs.tla, `BreakSecondFileShares`).
    let is_cell = flags & ffi::SQLITE_OPEN_MAIN_DB != 0
        && !name.is_null()
        && CStr::from_ptr(name) == app.path.as_c_str();
    let (reader, page_size) = if is_cell {
        (Some(app.reader.clone()), app.reader.page_size())
    } else {
        (None, 0)
    };
    let state = Box::into_raw(Box::new(FileState {
        vfs,
        reader,
        hydrated: app.hydrated.clone(),
        page_size,
    }));
    app.open_files.fetch_add(1, Ordering::AcqRel);

    let paged = file as *mut PagedFile;
    (*paged).methods = &PAGED_IO_METHODS;
    (*paged).base = base_file;
    (*paged).state = state;

    // Size an EMPTY local main-db file to the cut so its logical size is
    // right and un-faulted pages read as holes we intercept. A later open
    // (the actor's connection, an alarm read) finds a file a checkpoint may
    // have grown past the cut; sizing it again would cut those pages off
    // while their hydration marks survive (CelldPagedVfs.tla,
    // `BreakOpenTruncates`).
    if is_cell {
        let methods = &*base_methods(file);
        let mut size: ffi::sqlite3_int64 = 0;
        let rc = (methods.xFileSize.unwrap())(base_file, &mut size);
        if rc != ffi::SQLITE_OK {
            return rc;
        }
        if size == 0 {
            let bytes = i64::from(app.reader.page_size()) * i64::from(app.reader.commit());
            let rc = (methods.xTruncate.unwrap())(base_file, bytes);
            if rc != ffi::SQLITE_OK {
                return rc;
            }
        }
    }
    ffi::SQLITE_OK
}

macro_rules! forward_vfs {
    ($name:ident ( $($arg:ident : $ty:ty),* ) $(-> $ret:ty)? => $field:ident) => {
        unsafe extern "C" fn $name(vfs: *mut ffi::sqlite3_vfs $(, $arg: $ty)*) $(-> $ret)? {
            let base = base_vfs(vfs);
            let m = (*base).$field.unwrap();
            m(base $(, $arg)*)
        }
    };
}

forward_vfs!(x_delete(name: *const c_char, sync: c_int) -> c_int => xDelete);
forward_vfs!(x_access(name: *const c_char, flags: c_int, out: *mut c_int) -> c_int => xAccess);
forward_vfs!(x_full_pathname(name: *const c_char, n: c_int, out: *mut c_char) -> c_int => xFullPathname);
forward_vfs!(x_randomness(n: c_int, out: *mut c_char) -> c_int => xRandomness);
forward_vfs!(x_sleep(micros: c_int) -> c_int => xSleep);
forward_vfs!(x_current_time(out: *mut f64) -> c_int => xCurrentTime);
forward_vfs!(x_get_last_error(n: c_int, out: *mut c_char) -> c_int => xGetLastError);

/// A process-unique suffix for each registration, so re-activations never
/// collide on a name: SQLite finds a VFS by name, process-wide, and an
/// unregistered name may still have files open through it.
static REGISTRATION_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// The next registration's name, unique in this process.
pub fn next_registration_name() -> String {
    format!(
        "celld-paged-{}",
        REGISTRATION_SEQ.fetch_add(1, Ordering::SeqCst)
    )
}

/// Registers a paged VFS named `name` that faults the main db's pages in from
/// `reader`. celld opens the cell's database through this VFS name. Call
/// [`unregister_paged_vfs`] when the cell closes.
///
/// `base` names the VFS the paged one wraps; `None` is SQLite's default.
/// celld passes its own named VFS when it runs one, so the local file's I/O
/// stays under that host's control — the deterministic simulation in
/// particular, which cannot judge a crash image written past it.
pub fn register_paged_vfs(
    name: &str,
    base: Option<&str>,
    path: &Path,
    reader: Arc<dyn PageReader>,
) -> crate::error::Result<()> {
    let cname = CString::new(name).map_err(|_| crate::error::Error::LTXCorrupted)?;
    let base_name = base
        .map(CString::new)
        .transpose()
        .map_err(|_| crate::error::Error::LTXCorrupted)?;
    unsafe {
        let base =
            ffi::sqlite3_vfs_find(base_name.as_ref().map_or(std::ptr::null(), |n| n.as_ptr()));
        if base.is_null() {
            return Err(crate::error::Error::Other("no base sqlite VFS".into()));
        }
        let Some(path) = full_pathname(base, path) else {
            return Err(crate::error::Error::Other("paged vfs path".into()));
        };
        let app = Box::into_raw(Box::new(VfsAppData {
            base,
            path,
            reader,
            hydrated: Arc::new(Mutex::new(HydratedSet::default())),
            open_files: std::sync::atomic::AtomicUsize::new(0),
            retired: std::sync::atomic::AtomicBool::new(false),
        }));
        let vfs = Box::into_raw(Box::new(ffi::sqlite3_vfs {
            iVersion: 2,
            szOsFile: std::mem::size_of::<PagedFile>() as c_int,
            mxPathname: (*base).mxPathname,
            pNext: std::ptr::null_mut(),
            zName: cname.into_raw(),
            pAppData: app as *mut c_void,
            xOpen: Some(x_open),
            xDelete: Some(x_delete),
            xAccess: Some(x_access),
            xFullPathname: Some(x_full_pathname),
            xDlOpen: (*base).xDlOpen,
            xDlError: (*base).xDlError,
            xDlSym: (*base).xDlSym,
            xDlClose: (*base).xDlClose,
            xRandomness: Some(x_randomness),
            xSleep: Some(x_sleep),
            xCurrentTime: Some(x_current_time),
            xGetLastError: Some(x_get_last_error),
            xCurrentTimeInt64: None,
            xSetSystemCall: None,
            xGetSystemCall: None,
            xNextSystemCall: None,
        }));
        let rc = ffi::sqlite3_vfs_register(vfs, 0);
        if rc != ffi::SQLITE_OK {
            return Err(crate::error::Error::Other(
                format!("register vfs {name}: {rc}").into(),
            ));
        }
    }
    Ok(())
}

/// How much of the cut the cell's file holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hydration {
    /// Pages of the cut the file holds (the lock page never counts).
    pub hydrated: u32,
    /// Pages the cut has, less the lock page.
    pub total: u32,
    /// Faults the registration served so far, foreground and background.
    pub faults: u64,
}

impl Hydration {
    pub fn complete(&self) -> bool {
        self.hydrated >= self.total
    }
}

/// The registration's hydration count, or `None` for an unknown name.
pub fn hydration(name: &str) -> Option<Hydration> {
    let cname = CString::new(name).ok()?;
    unsafe {
        let vfs = ffi::sqlite3_vfs_find(cname.as_ptr());
        if vfs.is_null() {
            return None;
        }
        let app = &*((*vfs).pAppData as *mut VfsAppData);
        let commit = app.reader.commit();
        let lock = lock_pgno(app.reader.page_size());
        let hydrated = app.hydrated.lock().unwrap().counted;
        Some(Hydration {
            hydrated,
            total: commit - u32::from(lock <= commit),
            faults: app.reader.faults(),
        })
    }
}

/// Faults the rest of the cut in, a run at a time, through the VFS's own
/// read path: a connection on the cell's file whose reads are issued below
/// the pager, so every page arrives the way a foreground fault brings it and
/// nothing else changes. The caller paces the steps and stops them; a
/// hydrated cell reads only its local file, and its faults are inert.
pub struct Hydrator {
    conn: rusqlite::Connection,
    name: String,
    cursor: u32,
}

impl Hydrator {
    pub fn open(name: &str, path: &Path) -> crate::error::Result<Self> {
        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            name,
        )
        .map_err(|error| crate::error::Error::Other(format!("hydrator open: {error}").into()))?;
        Ok(Self {
            conn,
            name: name.to_string(),
            cursor: 1,
        })
    }

    /// Faults in the next hole and the holes after it until at least `pages`
    /// pages arrived (a fault brings a run, so one read can bring many) or
    /// the cut is complete; returns the count afterwards.
    pub fn step(&mut self, pages: u32) -> crate::error::Result<Hydration> {
        let cname =
            CString::new(self.name.as_str()).map_err(|_| crate::error::Error::LTXCorrupted)?;
        let (page_size, commit, lock) = unsafe {
            let vfs = ffi::sqlite3_vfs_find(cname.as_ptr());
            if vfs.is_null() {
                return Err(crate::error::Error::Other("hydrator: vfs is gone".into()));
            }
            let app = &*((*vfs).pAppData as *mut VfsAppData);
            let page_size = app.reader.page_size();
            let commit = app.reader.commit();
            (page_size, commit, lock_pgno(page_size))
        };
        let mut file: *mut ffi::sqlite3_file = std::ptr::null_mut();
        // SAFETY: `conn` is open, and FILE_POINTER hands out its main file.
        let rc = unsafe {
            ffi::sqlite3_file_control(
                self.conn.handle(),
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_FILE_POINTER,
                (&mut file as *mut *mut ffi::sqlite3_file).cast(),
            )
        };
        if rc != ffi::SQLITE_OK || file.is_null() {
            return Err(crate::error::Error::Other("hydrator: no main file".into()));
        }
        let mut page = vec![0u8; page_size as usize];
        let gone = || crate::error::Error::Other("hydrator: vfs is gone".into());
        let start = hydration(&self.name).ok_or_else(gone)?.hydrated;
        // The count is exact and O(1): every mutation keeps it, so a
        // faulted read costs one set lookup and a whale's step is linear
        // in its faults, not quadratic in its pages (upstream #270).
        while self.cursor <= commit {
            let pgno = self.cursor;
            self.cursor += 1;
            if pgno == lock {
                continue;
            }
            let held = unsafe {
                let app = &*((*ffi::sqlite3_vfs_find(cname.as_ptr())).pAppData as *mut VfsAppData);
                let hydrated = app.hydrated.lock().unwrap();
                hydrated.contains(&pgno)
            };
            if held {
                continue;
            }
            // SAFETY: `file` is the live main file and `page` holds a page.
            let rc = unsafe {
                let read = (*(*file).pMethods).xRead.expect("xRead");
                read(
                    file,
                    page.as_mut_ptr().cast(),
                    page_size as c_int,
                    (i64::from(pgno) - 1) * i64::from(page_size),
                )
            };
            if rc != ffi::SQLITE_OK {
                return Err(crate::error::Error::Other(
                    format!("hydrate page {pgno}: sqlite rc {rc}").into(),
                ));
            }
            if hydration(&self.name).ok_or_else(gone)?.hydrated - start >= pages {
                break;
            }
        }
        hydration(&self.name).ok_or_else(gone)
    }
}

/// Unregisters and frees a paged VFS registered by [`register_paged_vfs`].
pub fn unregister_paged_vfs(name: &str) -> crate::error::Result<()> {
    let cname = CString::new(name).map_err(|_| crate::error::Error::LTXCorrupted)?;
    unsafe {
        let vfs = ffi::sqlite3_vfs_find(cname.as_ptr());
        if vfs.is_null() {
            return Ok(());
        }
        ffi::sqlite3_vfs_unregister(vfs);
        let app = &*((*vfs).pAppData as *mut VfsAppData);
        app.retired.store(true, Ordering::Release);
        if app.open_files.load(Ordering::Acquire) == 0 {
            free_vfs(vfs);
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // Test tempfiles live outside the injected filesystem.
mod tests {
    use super::*;
    use crate::error::Result;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A synthetic cut: every page holds its own number, served in runs.
    struct TestReader {
        page_size: u32,
        commit: u32,
        run_len: u32,
        faults: AtomicU64,
    }

    impl TestReader {
        fn new(page_size: u32, commit: u32, run_len: u32) -> Self {
            Self {
                page_size,
                commit,
                run_len,
                faults: AtomicU64::new(0),
            }
        }

        fn page(&self, pgno: u32) -> Vec<u8> {
            let mut page = vec![0u8; self.page_size as usize];
            page[..4].copy_from_slice(&pgno.to_le_bytes());
            page
        }
    }

    impl PageReader for TestReader {
        fn page_size(&self) -> u32 {
            self.page_size
        }

        fn commit(&self) -> u32 {
            self.commit
        }

        fn read_run(&self, pgno: u32) -> Result<Vec<(u32, Vec<u8>)>> {
            self.faults.fetch_add(1, Ordering::Relaxed);
            if pgno == lock_pgno(self.page_size) {
                // Served with content like any in-cut page: the write
                // extends the sparse file, and the mark stays uncounted.
                return Ok(vec![(pgno, self.page(pgno))]);
            }
            if pgno > self.commit {
                return Ok(Vec::new());
            }
            Ok((pgno..(pgno + self.run_len).min(self.commit + 1))
                .map(|p| (p, self.page(p)))
                .collect())
        }

        fn faults(&self) -> u64 {
            self.faults.load(Ordering::Relaxed)
        }
    }

    fn register(name: &str, path: &Path, reader: TestReader) -> Arc<TestReader> {
        std::fs::write(path, []).expect("empty sparse file");
        // Grow past the cut the way a checkpoint-grown file would: reads
        // past the commit resolve from these holes instead of failing.
        let grown = 2 * u64::from(reader.commit) * u64::from(reader.page_size);
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .expect("open sparse file")
            .set_len(grown)
            .expect("grow sparse file");
        let reader = Arc::new(reader);
        register_paged_vfs(name, None, path, reader.clone()).expect("register");
        reader
    }

    /// The full walk `hydration()` used to do, straight from the set.
    fn oracle(name: &str) -> u32 {
        let cname = CString::new(name).expect("cname");
        unsafe {
            let vfs = ffi::sqlite3_vfs_find(cname.as_ptr());
            assert!(!vfs.is_null(), "vfs is gone");
            let app = &*((*vfs).pAppData as *mut VfsAppData);
            let commit = app.reader.commit();
            let lock = lock_pgno(app.reader.page_size());
            let set = app.hydrated.lock().unwrap();
            (1..=commit)
                .filter(|p| *p != lock && set.pages.contains(p))
                .count() as u32
        }
    }

    fn counted(name: &str) -> u32 {
        hydration(name).expect("hydration").hydrated
    }

    fn assert_counted(name: &str, context: &str) {
        assert_eq!(
            counted(name),
            oracle(name),
            "count disagrees with the full walk after {context}"
        );
    }

    /// Borrow the live main-db file of a connection opened through the VFS.
    fn main_file(conn: &rusqlite::Connection) -> *mut ffi::sqlite3_file {
        let mut file: *mut ffi::sqlite3_file = std::ptr::null_mut();
        let rc = unsafe {
            ffi::sqlite3_file_control(
                conn.handle(),
                c"main".as_ptr(),
                ffi::SQLITE_FCNTL_FILE_POINTER,
                (&mut file as *mut *mut ffi::sqlite3_file).cast(),
            )
        };
        assert_eq!(rc, ffi::SQLITE_OK, "file pointer");
        assert!(!file.is_null());
        file
    }

    fn raw_read(file: *mut ffi::sqlite3_file, pgno: u32, page_size: u32) -> Vec<u8> {
        let mut page = vec![0u8; page_size as usize];
        let rc = unsafe {
            let read = (*(*file).pMethods).xRead.expect("xRead");
            read(
                file,
                page.as_mut_ptr().cast(),
                page_size as c_int,
                (i64::from(pgno) - 1) * i64::from(page_size),
            )
        };
        assert_eq!(rc, ffi::SQLITE_OK, "raw read of page {pgno}");
        page
    }

    fn raw_write(file: *mut ffi::sqlite3_file, pgno: u32, page: &[u8], page_size: u32) {
        let rc = unsafe {
            let write = (*(*file).pMethods).xWrite.expect("xWrite");
            write(
                file,
                page.as_ptr().cast(),
                page.len() as c_int,
                (i64::from(pgno) - 1) * i64::from(page_size),
            )
        };
        assert_eq!(rc, ffi::SQLITE_OK, "raw write of page {pgno}");
    }

    fn raw_truncate(file: *mut ffi::sqlite3_file, bytes: i64) {
        let rc = unsafe {
            let truncate = (*(*file).pMethods).xTruncate.expect("xTruncate");
            truncate(file, bytes)
        };
        assert_eq!(rc, ffi::SQLITE_OK, "truncate to {bytes}");
    }

    fn open_conn(path: &Path, vfs: &str) -> rusqlite::Connection {
        rusqlite::Connection::open_with_flags_and_vfs(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
            vfs,
        )
        .expect("open through the paged vfs")
    }

    /// Upstream #270: the count stays exact through foreground faults,
    /// background faults, writes, lock-page and past-cut marks, duplicates,
    /// and truncate/regrow — checked against a full walk after each step.
    /// Read back the first four bytes as the page number the test image
    /// stamped there, or fail loudly on short content.
    fn page_no(page: &[u8]) -> u32 {
        u32::from_le_bytes(page[..4].try_into().expect("four content bytes"))
    }

    #[test]
    fn hydration_count_matches_oracle_through_every_mutation() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("oracle.sqlite");
        let name = "test-paged-oracle";
        let (page_size, commit, run_len) = (1024, 64, 4);
        let reader = register(name, &path, TestReader::new(page_size, commit, run_len));
        let lock = lock_pgno(page_size);
        assert!(lock > commit, "the lock page sits past this cut");

        let conn = open_conn(&path, name);
        let file = main_file(&conn);
        // Opening the database faults page 1 for its header, like any
        // first read through the VFS.
        assert_eq!(reader.faults(), 1, "open faults the header run once");
        assert_counted(name, "registration");
        assert_eq!(counted(name), 4, "the header run covers pages 1..=4");

        // Foreground faults, one run at a time; the second read of a held
        // page must not fault or count again.
        let faults_before = reader.faults();
        for pgno in [5u32, 6, 7] {
            let page = raw_read(file, pgno, page_size);
            assert_eq!(page_no(&page), pgno, "filled from the cut");
        }
        assert_eq!(reader.faults(), faults_before + 1, "one run faults once");
        assert_counted(name, "foreground faults");
        assert_eq!(counted(name), 8, "two runs of four faulted");
        raw_read(file, 5, page_size);
        assert_eq!(
            reader.faults(),
            faults_before + 1,
            "a held page never re-faults"
        );
        assert_counted(name, "duplicate fault");

        // The lock page resolves with content but without counting, and a
        // past-cut page resolves from the holes the pre-sized file holds.
        raw_read(file, lock, page_size);
        raw_read(file, commit + 9, page_size);
        assert_counted(name, "lock-page and past-cut marks");
        assert_eq!(counted(name), 8, "neither mark counts");

        // Foreground writes mark their range.
        let mut written = reader.page(20);
        written[4] = 0xA5;
        raw_write(file, 20, &written, page_size);
        raw_write(file, 21, &reader.page(21), page_size);
        assert_counted(name, "foreground writes");
        assert_eq!(&raw_read(file, 20, page_size)[..5], &[20u8, 0, 0, 0, 0xA5]);

        // A fault run skips pages that already hold newer bytes.
        let mut written31 = reader.page(31);
        written31[4] = 0xB7;
        raw_write(file, 31, &written31, page_size);
        raw_read(file, 30, page_size);
        assert_counted(name, "run around a written page");
        assert_eq!(
            raw_read(file, 31, page_size)[4],
            0xB7,
            "writes win over fault runs"
        );

        // Truncation forgets pages past the new size, including counted
        // ones; regrowth faults and writes them afresh.
        raw_truncate(file, 16 * i64::from(page_size));
        assert_counted(name, "truncate");
        assert_eq!(counted(name), 8, "only the two fault runs survive");
        raw_write(file, 40, &reader.page(40), page_size);
        assert_counted(name, "regrow by write");
        raw_read(file, 50, page_size);
        assert_counted(name, "regrow by fault");

        // A background hydrator step faults like a foreground read.
        drop(conn);
        let mut hydrator = Hydrator::open(name, &path).expect("hydrator");
        while !hydration(name).expect("hydration").complete() {
            hydrator.step(8).expect("step");
            assert_counted(name, "background step");
        }
        assert_eq!(counted(name), commit, "the cut fills completely");
        unregister_paged_vfs(name).expect("unregister");
    }

    /// Upstream #270: a 256K-page cut with one-page runs fills to
    /// completion inside a deadline. Before the O(1) count each faulted
    /// read walked the whole cut, which made this shape quadratic (tens of
    /// minutes); it now finishes in seconds.
    #[test]
    fn hydrator_step_completes_linear_cut() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("whale.sqlite");
        let name = "test-paged-whale";
        let (page_size, commit) = (1024, 262_144);
        let reader = register(name, &path, TestReader::new(page_size, commit, 1));
        let total = commit - u32::from(lock_pgno(page_size) <= commit);
        let mut hydrator = Hydrator::open(name, &path).expect("hydrator");
        let started = std::time::Instant::now();
        let deadline = std::time::Duration::from_secs(120);
        loop {
            if hydration(name).expect("hydration").complete() {
                break;
            }
            assert!(
                started.elapsed() < deadline,
                "the fill is not linear: still faulting after {deadline:?}"
            );
            hydrator.step(4096).expect("step");
        }
        let done = hydration(name).expect("hydration");
        assert_eq!(done.hydrated, total, "every cut page is held");
        assert_eq!(done.hydrated, done.total);
        assert_eq!(reader.faults(), total as u64, "each page faulted once");
        unregister_paged_vfs(name).expect("unregister");
    }
}
