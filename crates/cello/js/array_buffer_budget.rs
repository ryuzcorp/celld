// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//
// Modified by Ryuz Corp (Cello): one isolate's ArrayBuffer backing stores are
// charged to a per-isolate budget and refused past it.
// Apache-2.0.

//! A per-isolate ceiling on `ArrayBuffer` backing-store memory.
//!
//! V8's default ArrayBuffer allocator is `malloc`, so `new ArrayBuffer(n)` in
//! a guest was an unbounded allocation inside the shared process:
//! `near_heap_limit` extends the V8 heap and never fails the allocation, and
//! backing stores are external memory, not heap. A loop of
//! `new Uint8Array(8 << 20)` therefore grew the node without limit while the
//! isolate's heap stayed near empty.
//!
//! Each isolate now gets an allocator of its own
//! (`CreateParams::array_buffer_allocator`) that charges every backing store to
//! [`BackingStoreBudget`] and returns null past the cap. V8 turns that null
//! into `RangeError: Array buffer allocation failed` at the allocation site,
//! so the guest sees a catchable failure rather than the process growing, and
//! the refusal condemns the isolate the way the heap guard does. Freeing a
//! store (garbage collection) releases its bytes, so an isolate that churns
//! buffers is never condemned for memory it no longer holds.
//!
//! Two facts about V8 152 shape the coverage:
//!
//! - `v8::ArrayBuffer::New` on the **embedder** API aborts the process when
//!   the allocator returns null (`FatalProcessOutOfMemory:
//!   v8::ArrayBuffer::New`), so the host may not allocate backing stores that
//!   way once a cap exists. [`host_store`] hands V8 a store built from the
//!   host's own `Vec`, charged to the same budget and refused in Rust with a
//!   `RangeError` instead.
//! - Resizable `ArrayBuffer`s and `WebAssembly.Memory` do **not** reach the
//!   embedder allocator: V8 backs them with the platform page allocator
//!   (`BackingStore::TryAllocateAndPartiallyCommitMemory`). Their bytes are
//!   counted in `HeapStatistics::external_memory`, but no hook can refuse
//!   them; `recover_heap` retires an isolate that still holds more than its
//!   whole budget after a collection, and what a single turn can reach is
//!   documented in `docs/reference/limits.md`.
//!
//! The invariants this module keeps:
//!
//! - one isolate's accounting never charges another's: the budget travels
//!   with the allocator V8 holds for that isolate, and every store is freed
//!   by the deleter that charged it;
//! - an allocation past the cap fails closed: the reservation is refused
//!   before memory is taken, and the refusal sets the isolate's condemned
//!   flag;
//! - the process never aborts: every host-created store is charged in Rust
//!   before it exists, and the only null return V8 can see is from a JS
//!   allocation, which V8 handles by throwing.

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

/// Backing-store ceiling as a multiple of the isolate's V8 heap limit, used
/// when `CELLO_V8_EXTERNAL_LIMIT_MB` is unset.
///
/// Four times the heap leaves room for everything one request legitimately
/// holds at once — a 64 MiB request body or `/tmp` read, a 32 MiB WebSocket
/// message or tee skew, a 32 MiB decompression output, and a copy of any of
/// them — and still bounds one isolate to five heap limits in total. The
/// Cloudflare Workers isolate limit is 128 MB for heap and buffers together;
/// see `docs/reference/cloudflare-compat.md`.
pub(super) const EXTERNAL_LIMIT_HEAP_MULTIPLE: usize = 4;

/// The backing-store ceiling for an isolate whose V8 heap limit is
/// `heap_limit` bytes.
///
/// `CELLO_V8_EXTERNAL_LIMIT_MB` overrides the multiple. It is validated at
/// startup like `CELLO_V8_HEAP_LIMIT_MB`, so the multiplication below cannot
/// overflow.
pub(super) fn external_limit_bytes(heap_limit: usize) -> usize {
    crate::env_vars::positive::<usize>("CELLO_V8_EXTERNAL_LIMIT_MB")
        .expect("validated CELLO_V8_EXTERNAL_LIMIT_MB")
        .map(|megabytes| {
            megabytes
                .checked_mul(1024 * 1024)
                .expect("validated CELLO_V8_EXTERNAL_LIMIT_MB range")
        })
        .unwrap_or_else(|| heap_limit.saturating_mul(EXTERNAL_LIMIT_HEAP_MULTIPLE))
}

/// One isolate's backing-store accounting: what it may hold, what it holds,
/// and whether an allocation past the ceiling was refused.
///
/// Shared by the allocator V8 calls and by the host's own stores, so both
/// draw on the same budget. Atomics rather than locks: V8 may free a store
/// from a collection on another thread, and an allocator callback may not
/// block or call back into V8.
pub(super) struct BackingStoreBudget {
    cap: usize,
    used: AtomicUsize,
    /// Latched by the refusal of an allocation past `cap`. It is the
    /// external-memory half of the isolate's condemnation: `recover_heap`
    /// clears it once usage falls back under the recovery share, exactly as
    /// it clears the heap-limit flag.
    exceeded: AtomicBool,
}

impl BackingStoreBudget {
    pub(super) fn new(cap: usize) -> Arc<Self> {
        Arc::new(Self {
            cap,
            used: AtomicUsize::new(0),
            exceeded: AtomicBool::new(false),
        })
    }

    pub(super) fn cap(&self) -> usize {
        self.cap
    }

    pub(super) fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub(super) fn exceeded(&self) -> bool {
        self.exceeded.load(Ordering::Relaxed)
    }

    pub(super) fn clear_exceeded(&self) {
        self.exceeded.store(false, Ordering::Relaxed);
    }

    /// Whether the bytes this allocator handed out are at or above `share` of
    /// the cap. `recover_heap` measures this the way it measures the heap.
    pub(super) fn used_share(&self) -> f64 {
        if self.cap == 0 {
            return 1.0;
        }
        self.used() as f64 / self.cap as f64
    }

    /// Reserve `len` bytes, or refuse and latch the condemnation.
    ///
    /// A compare-exchange loop, not a fetch-add followed by a check: two
    /// concurrent allocations must not both see room for the last bytes.
    fn try_reserve(&self, len: usize) -> bool {
        let mut used = self.used.load(Ordering::Relaxed);
        loop {
            let Some(next) = used.checked_add(len) else {
                self.exceeded.store(true, Ordering::Relaxed);
                return false;
            };
            if next > self.cap {
                self.exceeded.store(true, Ordering::Relaxed);
                return false;
            }
            match self
                .used
                .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(actual) => used = actual,
            }
        }
    }

    fn release(&self, len: usize) {
        let _ = self
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |used| {
                Some(used.saturating_sub(len))
            });
    }
}

/// Backing-store alignment. V8 asks for at least 8-byte alignment (a
/// `Float64Array` view over the store); the system allocator provides more.
const ALIGN: usize = 8;

fn layout(len: usize) -> Option<std::alloc::Layout> {
    std::alloc::Layout::from_size_align(len, ALIGN).ok()
}

/// What a zero-length store's `free` sees. V8 does not call the allocator for
/// an empty `ArrayBuffer` in 152, but the vtable must still answer one, and
/// the pointer must be non-null and freeable: `free` ignores it and `dealloc`
/// is never reached with a zero-size layout.
fn empty_pointer() -> *mut c_void {
    std::ptr::NonNull::<u8>::dangling().as_ptr().cast()
}

fn allocate(budget: &BackingStoreBudget, len: usize, zeroed: bool) -> *mut c_void {
    if len == 0 {
        return empty_pointer();
    }
    if !budget.try_reserve(len) {
        return std::ptr::null_mut();
    }
    let Some(layout) = layout(len) else {
        // Unreachable for a valid `usize` length the budget admitted, but
        // must not abort: a null return is the allocator's only failure.
        budget.release(len);
        return std::ptr::null_mut();
    };
    // SAFETY: `len` is non-zero, so the layout has a non-zero size.
    let pointer = unsafe {
        if zeroed {
            std::alloc::alloc_zeroed(layout)
        } else {
            std::alloc::alloc(layout)
        }
    };
    if pointer.is_null() {
        budget.release(len);
        return std::ptr::null_mut();
    }
    pointer.cast()
}

unsafe extern "C" fn allocate_zeroed(budget: &BackingStoreBudget, len: usize) -> *mut c_void {
    allocate(budget, len, true)
}

unsafe extern "C" fn allocate_uninitialized(
    budget: &BackingStoreBudget,
    len: usize,
) -> *mut c_void {
    allocate(budget, len, false)
}

unsafe extern "C" fn free(budget: &BackingStoreBudget, data: *mut c_void, len: usize) {
    if data.is_null() || len == 0 {
        return;
    }
    // SAFETY: `data` and `len` are exactly what `allocate` returned and
    // charged; `layout` succeeds for anything `allocate` accepted.
    if let Some(layout) = layout(len) {
        unsafe { std::alloc::dealloc(data.cast(), layout) };
        budget.release(len);
    }
}

unsafe extern "C" fn drop_budget(handle: *const BackingStoreBudget) {
    if handle.is_null() {
        return;
    }
    // SAFETY: `allocator` leaked exactly one `Arc::into_raw` of this budget,
    // and V8 calls the vtable's `drop` once, after the isolate is gone.
    unsafe { drop(Arc::from_raw(handle)) };
}

static VTABLE: v8::RustAllocatorVtable<BackingStoreBudget> = v8::RustAllocatorVtable {
    allocate: allocate_zeroed,
    allocate_uninitialized,
    free,
    drop: drop_budget,
};

/// The isolate's allocator: V8 holds a `shared_ptr` to it for the isolate's
/// lifetime, and it holds one `Arc` reference to `budget`.
///
/// # Panics
///
/// Never. V8 requires the allocator at `CreateParams` time; a budget is
/// created for every isolate.
pub(super) fn allocator(budget: &Arc<BackingStoreBudget>) -> v8::UniqueRef<v8::Allocator> {
    // SAFETY: the handle is an `Arc<BackingStoreBudget>` kept alive by the
    // allocator, and `VTABLE` matches that type for all four entries.
    unsafe { v8::new_rust_allocator(Arc::into_raw(budget.clone()), &VTABLE) }
}

/// What the deleter of a host-owned store releases when V8 frees the store.
struct HostCharge {
    budget: Arc<BackingStoreBudget>,
    len: usize,
    /// The store's memory, kept only for its destructor: dropping this `Vec`
    /// after the `Box` is reconstructed frees the buffer V8 pointed at, and
    /// moving the `Vec` here never reallocates it.
    _bytes: Vec<u8>,
}

unsafe extern "C" fn release_host_store(
    _data: *mut c_void,
    _byte_length: usize,
    deleter_data: *mut c_void,
) {
    if deleter_data.is_null() {
        return;
    }
    // SAFETY: `host_store` boxed exactly one `HostCharge` per store and gave
    // V8 its raw pointer as the deleter data; V8 calls this once.
    let charge = unsafe { Box::from_raw(deleter_data.cast::<HostCharge>()) };
    charge.budget.release(charge.len);
    // `charge` drops here, freeing the `Vec` the store presented.
}

/// A backing store for bytes the host already owns, charged to `budget`.
///
/// Answers `None` when the bytes would take the isolate past its cap; the
/// caller throws the `RangeError` the guest would have seen from V8.
///
/// An empty `Vec` takes V8's own path: nothing is charged, and V8's
/// zero-length store needs no deleter bookkeeping.
pub(super) fn host_store(
    budget: &Arc<BackingStoreBudget>,
    mut bytes: Vec<u8>,
) -> Option<v8::UniqueRef<v8::BackingStore>> {
    if bytes.is_empty() {
        return Some(v8::ArrayBuffer::new_backing_store_from_vec(bytes));
    }
    let len = bytes.len();
    if !budget.try_reserve(len) {
        return None;
    }
    let pointer = bytes.as_mut_ptr();
    let charge = Box::new(HostCharge {
        budget: budget.clone(),
        len,
        _bytes: bytes,
    });
    // SAFETY: the pointer is `charge.bytes`'s buffer, which the `Box` keeps
    // alive until V8 calls the deleter, and `release_host_store` takes back
    // exactly the `HostCharge` pointer handed over here.
    Some(unsafe {
        v8::ArrayBuffer::new_backing_store_from_ptr(
            pointer.cast(),
            len,
            release_host_store,
            Box::into_raw(charge).cast(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cap is the hard line: a reservation that would cross it is
    /// refused before any memory is taken, and `used` does not move.
    #[test]
    fn reservation_past_the_cap_is_refused_without_charging() {
        let budget = BackingStoreBudget::new(1024);
        assert!(budget.try_reserve(768));
        assert_eq!(budget.used(), 768);
        assert!(!budget.exceeded());
        assert!(!budget.try_reserve(512));
        assert_eq!(budget.used(), 768, "a refused reservation charged nothing");
        assert!(budget.exceeded(), "the refusal condemns the isolate");
        // The room that was there is still there.
        assert!(budget.try_reserve(256));
        assert_eq!(budget.used(), 1024);
        budget.clear_exceeded();
        assert!(!budget.exceeded());
    }

    /// Releasing hands the bytes back, so churn never walks into the cap.
    #[test]
    fn release_returns_bytes_to_the_budget() {
        let budget = BackingStoreBudget::new(4096);
        for _ in 0..100 {
            assert!(budget.try_reserve(4096));
            budget.release(4096);
        }
        assert_eq!(budget.used(), 0);
        assert!(!budget.exceeded());
        // A release of bytes never reserved cannot underflow the counter.
        budget.release(1 << 20);
        assert_eq!(budget.used(), 0);
    }

    /// Two budgets are two ceilings: filling one leaves the other untouched.
    #[test]
    fn budgets_do_not_share_accounting() {
        let first = BackingStoreBudget::new(1024);
        let second = BackingStoreBudget::new(1024);
        assert!(first.try_reserve(1024));
        assert!(!first.try_reserve(1));
        assert!(
            second.try_reserve(1024),
            "the second isolate is not charged"
        );
        assert!(!second.exceeded());
    }

    /// A real allocation through the vtable is zeroed, charged, and freed
    /// back to the budget.
    #[test]
    fn allocator_round_trip_charges_and_releases() {
        let budget = BackingStoreBudget::new(1 << 20);
        let pointer = allocate(&budget, 4096, true);
        assert!(!pointer.is_null());
        assert_eq!(budget.used(), 4096);
        // SAFETY: `allocate` returned 4096 initialized bytes.
        let bytes = unsafe { std::slice::from_raw_parts(pointer.cast::<u8>(), 4096) };
        assert!(bytes.iter().all(|byte| *byte == 0), "the store is zeroed");
        // SAFETY: the pointer and length are the ones just allocated.
        unsafe { free(&budget, pointer, 4096) };
        assert_eq!(budget.used(), 0);

        let empty = allocate(&budget, 0, false);
        assert!(
            !empty.is_null(),
            "a zero-length store still answers a pointer"
        );
        // SAFETY: a zero-length allocation is not deallocated.
        unsafe { free(&budget, empty, 0) };
        assert_eq!(budget.used(), 0);
    }

    /// A store past the cap answers null, which is the only failure V8 can
    /// handle by throwing.
    #[test]
    fn allocator_refuses_past_the_cap() {
        let budget = BackingStoreBudget::new(4096);
        assert!(!allocate(&budget, 4096, false).is_null());
        assert!(allocate(&budget, 1, false).is_null(), "the cap is exact");
        assert!(budget.exceeded());
        assert_eq!(budget.used(), 4096);
    }
}

/// The budget as a guest sees it: `new ArrayBuffer`/typed arrays are refused
/// with `RangeError` past the ceiling, the isolate is condemned the way the
/// heap guard condemns it, freeing buffers lifts the condemnation, and the
/// process stops growing.
///
/// The ceiling is whatever the isolate has by default (four heap limits, so
/// 512 MiB at the 128 MiB default), deliberately: a test that lowered it
/// through a hook would fail to compile once the wiring is reverted, and the
/// point of these tests is to fail against the old unbounded allocator.
#[cfg(test)]
mod guest_tests {
    use crate::js::{Compat, RequestBody, Worker, WorkerConfig, WorkerConfigOptions};
    use std::sync::Arc;
    use std::time::Instant;

    const MIB: usize = 1024 * 1024;

    const SCRIPT: &str = r#"
const held = [];
globalThis.__held = held;
function n(url, name, fallback) {
  return Number(url.searchParams.get(name) ?? fallback);
}
export default {
  async fetch(request) {
    const url = new URL(request.url);
    switch (url.pathname) {
      // Allocate `step` bytes at a time, filling each view so the pages are
      // really committed, and retain them. The caller keeps `target` below
      // the ceiling: an isolate at its ceiling cannot encode a response body
      // either, which is what `/alloc` proves on its own.
      case "/grow": {
        const step = n(url, "step", 0);
        const target = n(url, "target", 0);
        let grown = 0;
        while (grown + step <= target) {
          const view = new Uint8Array(step);
          view.fill(1);
          held.push(view);
          grown += step;
        }
        return Response.json({ grown });
      }
      // One allocation of `mib` MiB, retained when it succeeds.
      case "/alloc": {
        const bytes = n(url, "mib", 0) * 1024 * 1024;
        try {
          const view = new Uint8Array(bytes);
          view.fill(1);
          held.push(view);
          return Response.json({ ok: true, bytes: view.byteLength });
        } catch (error) {
          return Response.json({ ok: false, name: error.name, message: error.message });
        }
      }
      // Allocate and drop in a loop: nothing is retained, so the budget sees
      // churn, not growth.
      case "/churn": {
        const step = n(url, "step", 0);
        const count = n(url, "count", 0);
        let allocated = 0;
        let refused = null;
        for (let index = 0; index < count; index++) {
          try {
            const view = new Uint8Array(step);
            view.fill(1);
            allocated += step;
          } catch (error) {
            refused = `${error.name}: ${error.message}`;
            break;
          }
        }
        return Response.json({ allocated, refused });
      }
      // Every legitimate per-request payload at once, which the ceiling must
      // leave room for.
      case "/legit": {
        const body = new Uint8Array(64 * 1024 * 1024).fill(1);
        const message = new Uint8Array(32 * 1024 * 1024).fill(2);
        const tee = new Uint8Array(32 * 1024 * 1024).fill(3);
        const cloned = structuredClone(tee);
        held.push(body, message, tee, cloned);
        return Response.json({
          bytes: body.byteLength + message.byteLength + tee.byteLength +
            cloned.byteLength,
        });
      }
      // `structuredClone` copies the buffer through V8's deserializer, which
      // must draw on the same budget.
      case "/clone": {
        const size = n(url, "mib", 0) * 1024 * 1024;
        try {
          const source = new Uint8Array(size);
          const copy = structuredClone(source);
          held.push(source, copy);
          return Response.json({ ok: true, bytes: copy.byteLength });
        } catch (error) {
          return Response.json({ ok: false, name: error.name, message: error.message });
        }
      }
      // `transfer` allocates the target buffer.
      case "/transfer": {
        const from = n(url, "from", 0) * 1024 * 1024;
        const to = n(url, "to", 0) * 1024 * 1024;
        try {
          const moved = new ArrayBuffer(from).transfer(to);
          held.push(moved);
          return Response.json({ ok: true, bytes: moved.byteLength });
        } catch (error) {
          return Response.json({ ok: false, name: error.name });
        }
      }
      case "/free": {
        const freed = held.length;
        held.length = 0;
        return Response.json({ freed });
      }
      // A resizable `ArrayBuffer` is backed by V8's page allocator, not the
      // isolate allocator: this route proves the difference and the backstop.
      case "/resizable": {
        const max = n(url, "max", 0) * 1024 * 1024;
        const to = n(url, "to", 0) * 1024 * 1024;
        try {
          const buffer = new ArrayBuffer(8, { maxByteLength: max });
          buffer.resize(to);
          held.push(buffer);
          return Response.json({ ok: true, bytes: buffer.byteLength });
        } catch (error) {
          return Response.json({ ok: false, name: error.name });
        }
      }
      case "/wasm": {
        const pages = n(url, "pages", 0);
        try {
          const memory = new WebAssembly.Memory({ initial: pages, maximum: pages });
          held.push(memory);
          return Response.json({ ok: true, bytes: memory.buffer.byteLength });
        } catch (error) {
          return Response.json({ ok: false, name: error.name });
        }
      }
    }
    return new Response("not found", { status: 404 });
  },
};
"#;

    /// Runs `test` on a thread created after V8 initialised; see
    /// `runtime::init_v8`.
    fn on_engine_thread(test: impl FnOnce() + Send + 'static) {
        crate::runtime::init_v8();
        crate::asyncrt::shared_test_runtime();
        std::thread::spawn(test).join().expect("engine thread");
    }

    fn load() -> Worker {
        let config = WorkerConfig::new(WorkerConfigOptions {
            src: SCRIPT.to_string(),
            script_name: "array-buffer-budget".to_string(),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            r2_bindings: Vec::new(),
            d1_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: String::new(),
            modules: Vec::new(),
            compat: Compat::default(),
        });
        Worker::load_config(Arc::new(config)).expect("load worker")
    }

    /// One request whose handler settles on promises alone, so its first turn
    /// is the whole request. Answers the JSON body.
    fn fetch(worker: &mut Worker, path: &str) -> serde_json::Value {
        let (reply, mut receive) = tokio::sync::oneshot::channel();
        let job = crate::WorkerJob::Fetch {
            queued_at: Instant::now(),
            entrypoint: None,
            invocation_limits: None,
            url: format!("http://test{path}"),
            method: "GET".to_string(),
            body: RequestBody::Bytes(bytes::Bytes::new()),
            headers: Vec::new(),
            request_id: None,
            tail_report: None,
            reply,
        };
        let (entry, started) = worker.turn_begin(job, None);
        drop(started);
        let mut entry = entry.expect("request starts");
        entry.abandon();
        drop(entry);
        let response = receive
            .try_recv()
            .unwrap_or_else(|error| panic!("{path} did not answer in one turn: {error:?}"))
            .unwrap_or_else(|error| panic!("{path} was rejected: {error}"));
        assert_eq!(response.status, 200, "{path} answered {}", response.status);
        serde_json::from_slice(&response.body).expect("json body")
    }

    /// Bytes the allocator currently charges, and the ceiling it enforces.
    fn budget(worker: &Worker) -> (usize, usize) {
        worker
            .backing_store_budget_for_test()
            .expect("every loaded isolate has a budget")
    }

    /// A full collection, then the foreground tasks it posted, until none is
    /// runnable. The deleter that releases a backing store's bytes runs inside
    /// the collection, so this is what makes "freed" observable.
    fn collect(worker: &mut Worker) {
        worker.settle_heap();
        let inner = worker.inner.as_ref().expect("live worker");
        let (locker, _cells) = inner.lock();
        let platform = v8::V8::get_current_platform();
        for _ in 0..1024 {
            if !v8::Platform::pump_message_loop(&platform, &locker, false) {
                return;
            }
        }
        panic!("foreground tasks kept coming after 1024 runs");
    }

    /// The step-1 failure, now bounded: after 496 MiB of retained views a
    /// further 32 MiB `new Uint8Array` is refused with a `RangeError`, the
    /// isolate is condemned the way the heap guard condemns it, the allocator
    /// stops charging at the ceiling, and a second isolate pays nothing for the
    /// first one's fill.
    ///
    /// The fixture stops 16 MiB under the ceiling on purpose: a response body
    /// is itself a charged host store, and an isolate at the exact ceiling
    /// cannot encode its own answer. That is the guest-visible contract (free
    /// buffers before allocating more), and the conformance fixture reports
    /// through a header for the same reason.
    #[test]
    fn growth_past_the_budget_is_refused_and_bounded() {
        on_engine_thread(|| {
            let mut worker = load();
            let (_, cap) = budget(&worker);
            assert_eq!(cap, 4 * 128 * MIB, "the default ceiling is four heaps");

            let report = fetch(&mut worker, "/grow?step=8388608&target=520093696");
            assert_eq!(report["grown"], 496 * MIB, "{report}");
            let (used_after_grow, _) = budget(&worker);
            assert!(used_after_grow <= cap, "{used_after_grow} of {cap}");

            let report = fetch(&mut worker, "/alloc?mib=32");
            assert_eq!(report["ok"], false, "the growth was not refused: {report}");
            assert_eq!(
                report["name"], "RangeError",
                "the refusal is the RangeError V8 raises: {report}"
            );
            assert!(
                report["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("Array buffer allocation failed")),
                "the message names the failed allocation: {report}"
            );

            let (used, cap_after) = budget(&worker);
            assert_eq!(cap_after, cap);
            assert!(
                used <= cap,
                "the allocator never charges past the ceiling: {used} of {cap}"
            );
            let heap = worker.heap_bytes().expect("live isolate");
            assert!(
                heap.external as usize <= cap + 64 * MIB,
                "V8 external memory stays near the ceiling: {} bytes",
                heap.external
            );
            assert_eq!(
                worker.heap_condemned_for_test(),
                Some(true),
                "the refusal condemns the isolate, as the heap guard does"
            );

            // The condemnation is the refused isolate's. A second isolate
            // starts with an empty budget and serves the same allocation.
            let mut other = load();
            let report = fetch(&mut other, "/alloc?mib=64");
            assert_eq!(
                report["ok"], true,
                "another isolate was charged for the first one's fill: {report}"
            );
            assert_eq!(other.heap_condemned_for_test(), Some(false));
            let (used_other, _) = budget(&other);
            assert!(used_other >= 64 * MIB);
        });
    }

    /// Allocate and free well past the ceiling in total: accounting falls when
    /// the collector frees a store, so churn is never refused and never
    /// condemns the isolate.
    #[test]
    fn churn_past_the_budget_is_never_refused() {
        on_engine_thread(|| {
            let mut worker = load();
            let (_, cap) = budget(&worker);
            // 24 requests x 8 chunks x 8 MiB = 1.5 GiB, three ceilings over.
            for round in 0..24 {
                let report = fetch(&mut worker, "/churn?step=8388608&count=8");
                assert!(
                    report["refused"].is_null(),
                    "churn round {round} was refused: {report}"
                );
                assert_eq!(report["allocated"], 64 * MIB);
                collect(&mut worker);
                let (used, _) = budget(&worker);
                assert!(
                    used < cap / 2,
                    "collected churn left {used} of {cap} charged after round {round}"
                );
                assert_eq!(
                    worker.heap_condemned_for_test(),
                    Some(false),
                    "churn condemned the isolate after round {round}"
                );
            }
        });
    }

    /// Every legitimate payload at once fits: a 64 MiB body or `/tmp` read, a
    /// 32 MiB WebSocket message, a 32 MiB tee budget and a clone of it.
    #[test]
    fn legitimate_payloads_fit_under_the_budget() {
        on_engine_thread(|| {
            let mut worker = load();
            let report = fetch(&mut worker, "/legit");
            assert_eq!(report["bytes"], 160 * MIB, "{report}");
            assert_eq!(worker.heap_condemned_for_test(), Some(false));
            let (used, cap) = budget(&worker);
            // The payloads plus the response body's own store, which is
            // charged like any other.
            assert!(
                (160 * MIB..=160 * MIB + MIB).contains(&used),
                "charged {used} bytes for 160 MiB of payloads"
            );
            assert!(used < cap);

            // And the isolate still takes more afterwards: the ceiling is not
            // a per-request budget.
            let report = fetch(&mut worker, "/churn?step=8388608&count=8");
            assert!(report["refused"].is_null(), "{report}");
            assert_eq!(worker.heap_condemned_for_test(), Some(false));
        });
    }

    /// `structuredClone` and `transfer` allocate through the same budget as
    /// `new ArrayBuffer`.
    ///
    /// Each probe gets its own isolate: a collection releases every store the
    /// guest dropped, but V8 retains one backing store per `structuredClone`
    /// past the first collection (measured: 48 MiB charged after a clone,
    /// 24 MiB after a collection), which would confuse the recovery stage
    /// below. The recovery stage uses only views the test allocated itself,
    /// whose release is deterministic.
    #[test]
    fn clone_and_transfer_use_the_same_budget() {
        on_engine_thread(|| {
            {
                let mut worker = load();
                let report = fetch(&mut worker, "/grow?step=8388608&target=503316480");
                assert_eq!(report["grown"], 480 * MIB, "{report}");
                assert_eq!(worker.heap_condemned_for_test(), Some(false));
                // 32 MiB of headroom; a 24 MiB clone needs 48.
                let report = fetch(&mut worker, "/clone?mib=24");
                assert_eq!(report["ok"], false, "the clone was not refused: {report}");
                assert!(
                    report["message"]
                        .as_str()
                        .is_some_and(|message| message.contains("deserialize")),
                    "the clone failed at V8's deserializer: {report}"
                );
                assert_eq!(
                    worker.heap_condemned_for_test(),
                    Some(true),
                    "the refused clone condemns the isolate"
                );
                let (used, cap) = budget(&worker);
                assert!(
                    used <= cap,
                    "the clone charged past the ceiling: {used}/{cap}"
                );
            }

            {
                let mut worker = load();
                let report = fetch(&mut worker, "/grow?step=8388608&target=503316480");
                assert_eq!(report["grown"], 480 * MIB, "{report}");
                // The transfer target is an allocation too: 8 MiB in, 64 MiB
                // out crosses the ceiling.
                let report = fetch(&mut worker, "/transfer?from=8&to=64");
                assert_eq!(
                    report["ok"], false,
                    "the transfer was not refused: {report}"
                );
                assert_eq!(report["name"], "RangeError", "{report}");
                let (used, cap) = budget(&worker);
                assert!(
                    used <= cap,
                    "the transfer charged past the ceiling: {used}/{cap}"
                );
            }
        });
    }

    /// Freeing what is charged lifts the condemnation: the collector returns
    /// the bytes to the budget, so an isolate that lets go serves again and is
    /// never condemned for memory it no longer holds.
    #[test]
    fn freed_buffers_lift_the_condemnation() {
        on_engine_thread(|| {
            let mut worker = load();
            let (_, cap) = budget(&worker);
            let report = fetch(&mut worker, "/grow?step=8388608&target=503316480");
            assert_eq!(report["grown"], 480 * MIB, "{report}");

            // One allocation past the ceiling condemns the isolate.
            let report = fetch(&mut worker, "/alloc?mib=64");
            assert_eq!(report["ok"], false, "{report}");
            assert_eq!(report["name"], "RangeError", "{report}");
            assert_eq!(worker.heap_condemned_for_test(), Some(true));

            fetch(&mut worker, "/free");
            collect(&mut worker);
            let (used, _) = budget(&worker);
            assert_eq!(used, 0, "the freed stores are released");
            let report = fetch(&mut worker, "/alloc?mib=8");
            assert_eq!(report["ok"], true, "{report}");
            assert_eq!(
                worker.heap_condemned_for_test(),
                Some(false),
                "a drained isolate serves again"
            );
            let (used, _) = budget(&worker);
            assert!(used < cap);
        });
    }

    /// The two paths V8 keeps outside the allocator. A resized `ArrayBuffer` is
    /// charged to no allocator but is counted as external memory, so the next
    /// turn sees it, condemns the isolate and (in a pool) retires it —
    /// `retire_if_over_budget`. Wasm memory is counted nowhere, the documented
    /// remaining gap.
    #[test]
    fn resizable_and_wasm_memory_bypass_the_allocator_but_are_seen() {
        on_engine_thread(|| {
            let mut worker = load();
            let (_, cap) = budget(&worker);
            // A resizable buffer commits its `resize` through the page
            // allocator. Untouched, so this costs address space and the
            // external-memory reading, not resident pages.
            let report = fetch(&mut worker, "/resizable?max=640&to=600");
            assert_eq!(report["ok"], true, "{report}");
            assert_eq!(report["bytes"], 600 * MIB);
            let (used, _) = budget(&worker);
            assert!(
                used < MIB,
                "a resizable buffer charges no allocator, only the answer's \
                 small store: {used}"
            );
            let heap = worker.heap_bytes().expect("live isolate");
            assert!(
                heap.external as usize >= 600 * MIB,
                "V8 counts the resized buffer as external memory: {}",
                heap.external
            );
            assert!(
                heap.external as usize > cap,
                "the resized buffer is past the ceiling, so the isolate retires"
            );

            // The next turn sees it and condemns the isolate. A standalone
            // worker has no pool to retire it to; in a pool the same check
            // retires it, which the conformance suite covers.
            fetch(&mut worker, "/churn?step=8388608&count=1");
            assert_eq!(
                worker.heap_condemned_for_test(),
                Some(true),
                "a store past the ceiling condemns the isolate at the next turn"
            );

            // Wasm memory goes further: the page allocator, and V8 does not
            // even count it in `external_memory` (measured: +212 bytes for a
            // 64 MiB memory). Nothing in this isolate can see it, which is
            // why it is documented as the remaining gap.
            let mut other = load();
            let before = other.heap_bytes().expect("live isolate").external;
            let report = fetch(&mut other, "/wasm?pages=1024");
            assert_eq!(report["ok"], true, "{report}");
            assert_eq!(report["bytes"], 64 * MIB, "{report}");
            let after = other.heap_bytes().expect("live isolate").external;
            assert!(
                after < before + 4 * MIB as u64,
                "wasm memory is invisible to external accounting: {before} -> {after}"
            );
            let (used, _) = budget(&other);
            assert!(used < MIB, "wasm memory charges no allocator: {used}");
        });
    }
}
