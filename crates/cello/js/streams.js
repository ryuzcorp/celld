(function() {
// ReadableStream (WHATWG subset)

// Shadow Object.prototype.then with a non-enumerable
// undefined on every read result so user overrides of
// Object.prototype.then can't intercept `{ value, done }`
// when it flows through user code's await. Non-enumerable
// keeps it invisible to for..in + assert_object_equals.
function _readResult(value, done) {
  const r = { value, done };
  Object.defineProperty(r, 'then', {
    value: undefined,
    enumerable: false,
    writable: true,
    configurable: true,
  });
  return r;
}

// Brand-check sentinel for internal construction.
const _rsc_brand = Symbol("ReadableStreamController");

const RELEASED =
  "This ReadableStream reader has been released.";

// The skew budget one tee applies: bytes its branches may hold queued and
// unconsumed between them before the lagging one fails. It is the host
// tee's budget and message (HTTP_TEE_MAX_SKEW_BYTES and
// HTTP_TEE_SKEW_MESSAGE in http_streams.rs), so a body exposes one bound
// however it was teed; a unit test there pins the pair to this file, since
// a prelude script cannot import a Rust constant.
const _TEE_MAX_SKEW_BYTES = 33554432;
const _TEE_SKEW_MESSAGE =
  "tee branch fell more than 32 MiB behind its sibling; read both branches concurrently";

// What one queued chunk costs the budget. Every chunk is charged, and no
// chunk type is free:
//  * a view (typed array or DataView): its visible `byteLength` -- the
//    window it presents, not the backing buffer it keeps alive;
//  * ArrayBuffer / SharedArrayBuffer: its `byteLength`;
//  * string: 2 bytes per UTF-16 code unit, which is what V8 stores;
//  * anything else (an object, an array, a number, ...): a fixed floor
//    standing in for V8's object header plus the queue slots. Its contents
//    are deliberately not sized: a tee must not run a chunk's getters or
//    proxy traps, and a producer that wants to buffer a payload inside
//    objects needs no tee to do it. The floor still caps a queue of small
//    objects and primitives, and it applies to every chunk type, so empty
//    strings and zero-length buffers are not free.
//
// The budget is a guard against accidental buffering by honest code, not
// an isolate memory limit: this charge can be smaller than what a chunk
// actually retains (a 1 MiB view over a 64 MiB buffer costs 1 MiB -- and a
// 1-byte view the 64-byte floor -- and a payload hidden in an object costs
// the floor).
const _TEE_OBJECT_BYTES = 64;

// The probe must never run chunk code: a chunk may be a Proxy, or an
// object whose prototype is ArrayBuffer.prototype, and a native getter on
// the wrong receiver throws. Capture the intrinsics once, at prelude
// evaluation before any user code runs, and call them with the captured
// Reflect.apply inside try/catch, so the measurement is total and no trap
// or Get runs on the chunk.
const _teeApply = Reflect.apply;
const _teeIsView = ArrayBuffer.isView;
const _teeArrayBufferByteLength = Object.getOwnPropertyDescriptor(
  ArrayBuffer.prototype, 'byteLength').get;
const _teeTypedArrayByteLength = Object.getOwnPropertyDescriptor(
  Object.getPrototypeOf(Uint8Array.prototype), 'byteLength').get;
const _teeDataViewByteLength = Object.getOwnPropertyDescriptor(
  DataView.prototype, 'byteLength').get;
const _teeSharedArrayBufferByteLength =
  typeof SharedArrayBuffer === 'undefined' ? undefined :
    Object.getOwnPropertyDescriptor(
      SharedArrayBuffer.prototype, 'byteLength').get;

// The length a captured getter reports for `value`, or undefined when the
// getter rejects the receiver (a fake ArrayBuffer, a Proxy, a revoked
// Proxy) or reports something that is not a non-negative finite number.
function _teeMeasuredBytes(getter, value) {
  if (getter === undefined) return undefined;
  try {
    const n = _teeApply(getter, value, []);
    if (typeof n === 'number' && Number.isFinite(n) && n >= 0)
      return n;
  } catch (e) {
    // Not an instance of that builtin: fall through to the floor.
  }
  return undefined;
}

function _teeChunkBytes(chunk) {
  let bytes = _TEE_OBJECT_BYTES;
  if (typeof chunk === 'string') {
    bytes = chunk.length * 2;
  } else if (chunk !== null && typeof chunk === 'object') {
    let measured;
    try {
      if (_teeApply(_teeIsView, undefined, [chunk])) {
        // A typed array or DataView. A Proxy of one is not a view, so it
        // never reaches these getters.
        measured = _teeMeasuredBytes(_teeTypedArrayByteLength, chunk);
        if (measured === undefined)
          measured = _teeMeasuredBytes(_teeDataViewByteLength, chunk);
      } else {
        measured = _teeMeasuredBytes(_teeArrayBufferByteLength, chunk);
        if (measured === undefined)
          measured = _teeMeasuredBytes(
            _teeSharedArrayBufferByteLength, chunk);
      }
    } catch (e) {
      measured = undefined;
    }
    if (measured !== undefined) bytes = measured;
  }
  return bytes < _TEE_OBJECT_BYTES ? _TEE_OBJECT_BYTES : bytes;
}

class ReadableStreamDefaultController {
  #stream;
  #closeRequested = false;
  #errored = false;
  constructor(brand, stream) {
    if (brand !== _rsc_brand)
      throw new TypeError(
        "ReadableStreamDefaultController cannot be " +
        "constructed directly");
    this.#stream = stream;
  }
  enqueue(chunk) {
    if (this.#closeRequested || this.#errored ||
        this.#stream._closed)
      throw new TypeError(
        "Cannot enqueue a closed/errored stream");
    // Run size() if the strategy provides one. A throw
    // or invalid return (non-finite, negative) errors
    // the stream and rethrows from enqueue. Size's own
    // controller.error(...) wins if it was called first.
    let size = 1;
    if (this.#stream._sizeFn) {
      try { size = this.#stream._sizeFn(chunk); }
      catch (e) { this.error(e); throw e; }
      // Workerd's message for a non-number size --
      // e.g. an async size() returning a promise.
      if (typeof size !== 'number') {
        const err = new TypeError(
          "The value cannot be converted because " +
          "it is not an integer.");
        this.error(err);
        throw err;
      }
      if (!Number.isFinite(size) || size < 0) {
        const err = new RangeError(
          "invalid strategy size: " + size);
        this.error(err);
        throw err;
      }
    }
    this.#stream._queue.push(chunk);
    this.#stream._queueSizes.push(size);
    this.#stream._queueTotalSize += size;
    this.#stream._resolveRead();
    this.#stream._maybePull();
  }
  close() {
    if (this.#closeRequested || this.#errored ||
        this.#stream._closed)
      throw new TypeError("Already closed");
    this.#closeRequested = true;
    this.#stream._closeRequested = true;
    if (this.#stream._queue.length === 0)
      this.#stream._finishClose();
    else
      this.#stream._resolveRead();
  }
  error(e) {
    if (this.#errored) return;
    this.#errored = true;
    this.#stream._error = e;
    this.#stream._errored = true;
    this.#stream._queue = [];
    this.#stream._queueSizes = [];
    this.#stream._queueTotalSize = 0;
    // Reject closed first (spec order).
    this.#stream._rejectClosed(e);
    // Then reject pending reads.
    for (const r of this.#stream._readRequests)
      r.reject(e);
    this.#stream._readRequests = [];
  }
  get desiredSize() {
    if (this.#errored) return null;
    if (this.#closeRequested) return 0;
    return this.#stream._hwm -
           this.#stream._queueTotalSize;
  }
}

class ReadableStreamDefaultReader {
  #stream;
  #released = false;

  constructor(stream) {
    if (!(stream instanceof ReadableStream))
      throw new TypeError(
        "ReadableStreamDefaultReader requires a " +
        "ReadableStream argument");
    if (stream._locked)
      throw new TypeError("ReadableStream is locked");
    this.#stream = stream;
    stream._locked = true;
    stream._reader = this;
  }

  read() {
    if (this.#released)
      return Promise.reject(
        new TypeError(RELEASED));
    const s = this.#stream;
    s._disturbed = true;
    if (s._errored)
      return Promise.reject(s._error);
    // Check _closed first: a stream that was closed
    // re-entrantly (e.g. close() called from sizeFn) may
    // still have a stuck chunk in its queue which must
    // never be emitted. Only drain the queue while the
    // stream is still readable.
    if (s._closed)
      return Promise.resolve(
        _readResult(undefined, true));
    if (s._queue.length > 0) {
      const chunk = s._queue.shift();
      const sz = s._queueSizes.shift() ?? 0;
      s._queueTotalSize -= sz;
      if (s._queueTotalSize < 0) s._queueTotalSize = 0;
      if (s._queue.length === 0 && s._closeRequested)
        s._finishClose();
      s._maybePull();
      return Promise.resolve(
        _readResult(chunk, false));
    }
    // Queue a read request.
    return new Promise((resolve, reject) => {
      s._readRequests.push({ resolve, reject });
      s._maybePull();
    });
  }

  releaseLock() {
    if (!this.#released) {
      const s = this.#stream;
      // Reject pending read requests per spec.
      for (const r of s._readRequests)
        r.reject(new TypeError(RELEASED));
      s._readRequests = [];
      s._locked = false;
      s._reader = null;
      this.#released = true;
    }
  }

  get closed() {
    if (this.#released)
      return Promise.reject(
        new TypeError(RELEASED));
    return this.#stream._closedPromise;
  }

  cancel(reason) {
    if (this.#released)
      return Promise.reject(
        new TypeError(RELEASED));
    return this.#stream._cancel(reason);
  }
}

class ReadableStream {
  _queue = [];
  // Parallel array of chunk sizes (always 1 when no
  // strategy.size is set) and the running total, for
  // desiredSize accounting per spec.
  _queueSizes = [];
  _queueTotalSize = 0;
  _closed = false;
  _closeRequested = false;
  _errored = false;
  _error = null;
  _locked = false;
  _source = null;
  _controller = null;
  _reader = null;
  _readRequests = [];
  _hwm = 1;
  _pulling = false;
  _pullAgain = false;
  _started = false;

  // Closed promise + resolver.
  _closedPromise;
  _closedResolve;
  _closedReject;

  constructor(underlyingSource, queuingStrategy) {
    this._closedPromise = new Promise((res, rej) => {
      this._closedResolve = res;
      this._closedReject = rej;
    });
    // WHATWG Streams treats [[closedPromise]] as internal
    // state that only surfaces when the user queries
    // reader.closed / stream's _closedPromise. Our
    // Promise is a real JS Promise, so rejecting it when
    // no one has attached a .catch fires V8's unhandled-
    // rejection signal. Attach a no-op sink to mark it
    // handled at the engine level; user `await` on
    // reader.closed still sees the rejection because the
    // promise is shared.
    this._closedPromise.catch(() => {});
    // Spec: extract strategy fields first. Size is read
    // and validated before highWaterMark; an invalid size
    // must beat an invalid highWaterMark. hwm stays
    // undefined when defaulted: byte streams default to 0
    // where default streams use 1.
    let hwm;
    let sizeFn;
    if (queuingStrategy !== undefined) {
      const s = queuingStrategy.size;
      if (s !== undefined && typeof s !== 'function')
        throw new TypeError(
          'strategy.size must be a function');
      sizeFn = s;
      const h = queuingStrategy.highWaterMark;
      if (h !== undefined) {
        const n = Number(h);
        // Workerd throws TypeError here, not the spec's
        // RangeError.
        if (Number.isNaN(n) || n < 0)
          throw new TypeError(
            'highWaterMark must be non-negative');
        hwm = n;
      }
    }
    this._hwm = hwm === undefined ? 1 : hwm;
    this._sizeFn = sizeFn;
    if (underlyingSource === null ||
        (underlyingSource !== undefined &&
         typeof underlyingSource !== 'object'))
      throw new TypeError("Invalid source");
    const src = underlyingSource || {};
    // Workerd extension: a source may declare its total
    // byte length; fetch-style consumers read it for
    // Content-Length (see also FixedLengthStream).
    if (src.expectedLength !== undefined)
      this._expectedLength = Number(src.expectedLength);
    if (src.type !== undefined) {
      const t = String(src.type);
      if (t !== 'bytes')
        throw new TypeError(
          `Invalid type '${t}'`);
      // Byte-stream machinery lives in a lazy prelude;
      // naming the global compiles it on first use. It
      // installs the byte controller plus instance
      // overrides for getReader/_cancel, so nothing
      // below (default controller, pull wiring) runs.
      ReadableByteStreamController
        ._setup(this, src, hwm);
      return;
    }
    // Read each getter once per spec -- extracting
    // pull/start/cancel algorithms happens at
    // construction, not on every invocation.
    const pullFn = src.pull;
    const startFn = src.start;
    const cancelFn = src.cancel;
    if (cancelFn !== undefined &&
        typeof cancelFn !== 'function')
      throw new TypeError(
        "cancel must be a function");
    if (pullFn !== undefined &&
        typeof pullFn !== 'function')
      throw new TypeError(
        "pull must be a function");
    if (startFn !== undefined &&
        typeof startFn !== 'function')
      throw new TypeError(
        "start must be a function");
    this._source = src;
    this._startFn = startFn;
    this._pullFn = pullFn;
    this._cancelFn = cancelFn;
    this._controller =
      new ReadableStreamDefaultController(
        _rsc_brand, this);
    if (!this._startFn) {
      // Workerd starts a source with no start()
      // synchronously (mirrors the byte controller and
      // the writable's sink), so a strategy size() runs
      // before the constructor returns.
      this._started = true;
      this._maybePull();
      return;
    }
    // A sync start() throw must error the stream, not
    // escape the constructor.
    let startResult;
    try {
      startResult =
        this._startFn.call(this._source, this._controller);
    } catch (e) {
      startResult = Promise.reject(e);
    }
    Promise.resolve(startResult).then(
      () => {
        this._started = true;
        this._maybePull();
      },
      (e) => {
        this._controller.error(e);
      }
    );
  }

  // Resolve one pending read request from queue.
  _resolveRead() {
    while (this._readRequests.length > 0 &&
           this._queue.length > 0) {
      const r = this._readRequests.shift();
      const chunk = this._queue.shift();
      const sz = this._queueSizes.shift() ?? 0;
      this._queueTotalSize -= sz;
      if (this._queueTotalSize < 0) this._queueTotalSize = 0;
      r.resolve(_readResult(chunk, false));
    }
    if (this._queue.length === 0 &&
        this._closeRequested)
      this._finishClose();
  }

  _finishClose() {
    if (this._closed) return;
    this._closed = true;
    // Resolve pending reads with done.
    for (const r of this._readRequests)
      r.resolve(_readResult(undefined, true));
    this._readRequests = [];
    if (this._closedResolve) {
      this._closedResolve();
      this._closedResolve = null;
    }
  }

  _rejectClosed(e) {
    if (this._closedReject) {
      this._closedReject(e);
      this._closedReject = null;
      this._closedResolve = null;
    }
  }

  _maybePull() {
    if (!this._started || this._closed ||
        this._errored || this._closeRequested)
      return;
    if (!this._pullFn) return;
    const desiredSize = this._hwm -
      this._queue.length;
    if (desiredSize <= 0 &&
        this._readRequests.length === 0)
      return;
    if (this._pulling) {
      this._pullAgain = true;
      return;
    }
    this._pulling = true;
    let result;
    try {
      result = this._pullFn.call(
        this._source, this._controller);
    } catch (e) {
      this._pulling = false;
      this._controller.error(e);
      return;
    }
    Promise.resolve(result).then(
      () => {
        this._pulling = false;
        if (this._pullAgain) {
          this._pullAgain = false;
          this._maybePull();
        }
      },
      (e) => {
        this._pulling = false;
        this._controller.error(e);
      }
    );
  }

  getReader(options) {
    if (options !== undefined && options !== null) {
      if (options.mode !== undefined)
        throw new TypeError(
          `Invalid reader mode '${options.mode}'`);
    }
    return new ReadableStreamDefaultReader(this);
  }

  get locked() { return this._locked; }

  _cancel(reason) {
    // Spec section 4.2.6: cancel discards queued chunks and
    // transitions to closed. Without this, read() hits
    // the `_queue.length > 0` branch before the `_closed`
    // check and keeps yielding stale values.
    this._queue = [];
    this._queueSizes = [];
    this._queueTotalSize = 0;
    this._disturbed = true;
    // Workerd's internal (native) streams reject a pending
    // read() when the stream is cancelled; spec JS streams
    // resolve it {done: true}. Compression streams opt in
    // to the internal behavior (compression_streams.js).
    if (this._cancelRejectsReads) {
      for (const r of this._readRequests)
        r.reject(reason);
      this._readRequests = [];
    }
    this._finishClose();
    if (this._cancelFn) {
      let result;
      try {
        result = this._cancelFn.call(
          this._source, reason);
      } catch (e) {
        return Promise.reject(e);
      }
      return Promise.resolve(result)
        .then(() => undefined);
    }
    return Promise.resolve(undefined);
  }

  cancel(reason) {
    if (this._locked)
      return Promise.reject(new TypeError(
        "Cannot cancel a locked ReadableStream"));
    return this._cancel(reason);
  }

  tee() {
    // Branches of a byte stream are byte streams, so
    // they support BYOB reads; the source is still read
    // through a default reader (coalesced chunks).
    const isBytes = this._byteCtl !== undefined;
    const reader = this.getReader();
    let closed = false;
    let canceled1 = false, canceled2 = false;
    // A branch the skew budget failed. It is not cancelled: its reads
    // reject with the budget error, the other branch keeps flowing, and
    // the source is not cancelled while that one is live.
    let failed1 = false, failed2 = false;
    let reason1, reason2;
    let reading = false;
    // The charge of the chunk the in-flight pull is enqueuing, computed
    // once per chunk before either enqueue (which may transfer a buffer).
    // A default branch's size strategy reads it back, so the budget and
    // the branch's `_queueTotalSize` agree and a chunk is never measured
    // twice.
    let chunkCharge = _TEE_OBJECT_BYTES;
    const live1 = () => !canceled1 && !failed1;
    const live2 = () => !canceled2 && !failed2;

    // Both branches are gone (cancelled or failed), so nothing can read
    // this tee again: cancel the source with the composite reason instead
    // of holding its reader forever. Workerd's cancel settles a branch
    // immediately rather than waiting for the other, and the source is
    // cancelled only once both are gone -- the same here.
    function release() {
      if (closed || live1() || live2()) return;
      closed = true;
      return reader.cancel([reason1, reason2]);
    }

    const makeBranch = (first) => {
      const src = {
        pull: pullAlgorithm,
        cancel: (reason) => {
          if (first) { canceled1 = true; reason1 = reason; }
          else { canceled2 = true; reason2 = reason; }
          return release();
        },
      };
      if (isBytes) {
        src.type = 'bytes';
        return new ReadableStream(src);
      }
      // Size a default branch's chunks in the budget's own bytes, so its
      // `_queueTotalSize` -- which the reader maintains on every dequeue
      // -- is exactly what the branch holds unconsumed. A bytes strategy
      // may not carry size(), and a byte branch already counts bytes.
      return new ReadableStream(src, { size: () => chunkCharge });
    };
    const branch1 = makeBranch(true);
    const branch2 = makeBranch(false);
    const controllers = [
      branch1._controller,
      branch2._controller,
    ];
    // Bytes a branch holds queued and unconsumed: a byte branch keeps the
    // total on its controller, a default branch on the stream.
    const held = (branch) => branch._byteCtl !== undefined
      ? branch._byteCtl._queueTotalSize
      : branch._queueTotalSize;

    function failBranch(first) {
      if (first) { failed1 = true; reason1 = _TEE_SKEW_MESSAGE; }
      else { failed2 = true; reason2 = _TEE_SKEW_MESSAGE; }
      // The read that fails is the branch nobody drained, not the code
      // that teed, so the message names the fix. Same `Error` and text as
      // the host tee's rejection.
      (first ? controllers[0] : controllers[1])
        .error(new Error(_TEE_SKEW_MESSAGE));
    }

    // Fail a branch only for actual lag: it holds more queued bytes than a
    // live sibling and that difference, measured just after the chunk is
    // queued, is past the budget. The leading branch is never failed and
    // one chunk can fail at most one branch. The chunk size alone fails no
    // branch, but a branch that has not read an oversize chunk yet holds
    // all of it and fails for lag, so with chunks larger than the budget
    // both branches must be read concurrently -- the host tee's total-based
    // check would fail both regardless (see docs/reference/limits.md).
    // Returns whether a branch is still live; with one branch left there is
    // no sibling to be behind, so it keeps everything and the source is not
    // cancelled.
    function failLagging() {
      for (;;) {
        if (!live1() && !live2()) {
          const released = release();
          if (released !== undefined) released.catch(() => {});
          return false;
        }
        if (!live1() || !live2()) return true;
        const firstHeld = held(branch1);
        const secondHeld = held(branch2);
        if (firstHeld > secondHeld &&
            firstHeld - secondHeld > _TEE_MAX_SKEW_BYTES) {
          failBranch(true);
          continue;
        }
        if (secondHeld > firstHeld &&
            secondHeld - firstHeld > _TEE_MAX_SKEW_BYTES) {
          failBranch(false);
          continue;
        }
        return true;
      }
    }

    function pullAlgorithm() {
      if (reading || closed) return Promise.resolve();
      reading = true;
      return reader.read().then(
        ({ value, done }) => {
          reading = false;
          try {
            if (done) {
              if (closed) return;
              // The source has ended: the tee is finished whatever a
              // branch does next, and its `cancel` must not run.
              closed = true;
              // A close can throw for a branch only. A byte branch whose
              // BYOB read left a partial element on an odd-length tail
              // throws from close() (per spec); the sibling must still
              // close, and only the throwing branch is errored.
              try {
                if (live1()) controllers[0].close();
              } catch (e) {
                if (live1()) controllers[0].error(e);
              }
              try {
                if (live2()) controllers[1].close();
              } catch (e) {
                if (live2()) controllers[1].error(e);
              }
              return;
            }
            chunkCharge = _teeChunkBytes(value);
            // Byte enqueue transfers the chunk's buffer,
            // so when both branches are live the first
            // gets a copy.
            if (live1())
              controllers[0].enqueue(isBytes && live2()
                ? new Uint8Array(value) : value);
            if (live2()) controllers[1].enqueue(value);
            // Measure the lag after both enqueues: a branch whose reader
            // was already waiting has drained the chunk synchronously, so
            // the difference between the two totals is what the branches
            // really hold, and an unread branch fails once it is past the
            // budget even when the chunk that got it there is oversize.
            failLagging();
          } catch (e) {
            // No chunk and no stream state may strand a branch: whatever
            // threw (a byte enqueue rejecting a chunk, a controller
            // closed underneath the tee, a failing strategy), both
            // branches get the error, the tee is done, and the source
            // reader is released.
            if (closed) return;
            closed = true;
            if (live1()) controllers[0].error(e);
            if (live2()) controllers[1].error(e);
            const released = reader.cancel([e, e]);
            if (released !== undefined) released.catch(() => {});
          }
        },
        (e) => {
          reading = false;
          if (!closed) {
            closed = true;
            if (live1()) controllers[0].error(e);
            if (live2()) controllers[1].error(e);
          }
        }
      );
    }

    // Error propagation from original stream.
    reader.closed.catch(e => {
      if (!closed) {
        closed = true;
        if (live1()) controllers[0].error(e);
        if (live2()) controllers[1].error(e);
      }
    });

    return [branch1, branch2];
  }

  pipeTo(destination, options) {
    if (!('_queue' in this) ||
        !(this instanceof ReadableStream))
      return Promise.reject(new TypeError(
        "pipeTo called on non-ReadableStream"));
    if (destination === null ||
        typeof destination !== 'object' ||
        !('_sink' in destination) ||
        !(destination instanceof WritableStream))
      return Promise.reject(new TypeError(
        "pipeTo destination must be WritableStream"));
    // Read options eagerly (getters may have
    // side effects).
    let preventClose = false;
    let preventAbort = false;
    let preventCancel = false;
    let signal;
    if (options != null) {
      preventClose = !!options.preventClose;
      preventAbort = !!options.preventAbort;
      preventCancel = !!options.preventCancel;
      signal = options.signal;
    }
    if (this._locked)
      return Promise.reject(new TypeError(
        "ReadableStream is locked"));
    if (destination._locked)
      return Promise.reject(new TypeError(
        "WritableStream is locked"));
    if (signal && signal.aborted) {
      const err = signal.reason || new DOMException(
        "The operation was aborted", "AbortError");
      if (!preventCancel) this._cancel(err);
      if (!preventAbort) destination.abort(err);
      return Promise.reject(err);
    }
    // Per spec, pipeTo immediately marks the source as
    // disturbed so bodyUsed / similar observers flip
    // synchronously even before the first read fires.
    this._disturbed = true;
    const reader = this.getReader();
    const writer = destination.getWriter();
    let shuttingDown = false;
    let currentWrite = Promise.resolve();
    return new Promise((resolve, reject) => {
      let abortListener;
      if (signal) {
        abortListener = () => {
          const err = signal.reason ||
            new DOMException(
              "The operation was aborted",
              "AbortError");
          shutdownWith(err, !preventAbort,
            !preventCancel);
        };
        signal.addEventListener(
          'abort', abortListener);
      }
      function finish(isError, error) {
        reader.releaseLock();
        writer.releaseLock();
        if (signal && abortListener)
          signal.removeEventListener(
            'abort', abortListener);
        if (isError) reject(error);
        else resolve(undefined);
      }
      function shutdownWith(
        err, doAbort, doCancel) {
        if (shuttingDown) return;
        shuttingDown = true;
        // Wait for in-flight write to complete.
        currentWrite.then(doActions, doActions);
        function doActions() {
          const actions = [];
          if (doAbort)
            actions.push(writer.abort(err).then(
              () => {}, ae => { err = ae; }));
          if (doCancel)
            actions.push(reader.cancel(err)
              .catch(() => {}));
          Promise.all(actions)
            .then(() => finish(true, err));
        }
      }
      // Watch for source error.
      reader.closed.catch(re => {
        if (shuttingDown) return;
        shutdownWith(re, !preventAbort, false);
      });
      // Watch for dest error.
      writer.closed.catch(we => {
        if (shuttingDown) return;
        shutdownWith(we, false, !preventCancel);
      });
      function waitForReady() {
        if (shuttingDown) return;
        writer.ready.then(pumpRead, () => {});
      }
      function pumpRead() {
        if (shuttingDown) return;
        reader.read().then(({ value, done }) => {
          if (shuttingDown) return;
          if (done) {
            if (!preventClose) {
              writer.close().then(
                () => finish(false),
                e => finish(true, e));
            } else {
              finish(false);
            }
            return;
          }
          const writeP = writer.write(value)
            .catch(() => {});
          currentWrite = writeP;
          writeP.then(() => {
            if (shuttingDown) return;
            waitForReady();
          });
        }, () => {});
      }
      waitForReady();
    });
  }

  pipeThrough(transform, options) {
    if (!(this instanceof ReadableStream))
      throw new TypeError(
        "pipeThrough called on non-ReadableStream");
    if (typeof transform !== 'object' ||
        transform === null)
      throw new TypeError(
        "pipeThrough argument must be an object");
    if (!(transform.readable instanceof
          ReadableStream))
      throw new TypeError(
        "transform.readable must be ReadableStream");
    if (!(transform.writable instanceof
          WritableStream))
      throw new TypeError(
        "transform.writable must be WritableStream");
    if (this._locked)
      throw new TypeError(
        "ReadableStream is locked");
    if (transform.writable._locked)
      throw new TypeError(
        "WritableStream is locked");
    // Spec: mark the pipeTo promise as handled --
    // pipeTo already mirrors errors between the two
    // streams via the readable's error state, so a
    // rejection here would otherwise surface as an
    // unhandled rejection.
    this.pipeTo(transform.writable, options)
      .catch(() => {});
    return transform.readable;
  }

  async *values(options) {
    const preventCancel =
      !!(options && options.preventCancel);
    const reader = this.getReader();
    let drained = false;
    try {
      while (true) {
        const { value, done } = await reader.read();
        if (done) { drained = true; return; }
        yield value;
      }
    } finally {
      // WHATWG Streams 4.5.3: the default async
      // iterator's return() must cancel the stream
      // before releasing the reader lock -- early
      // termination of the loop (break / return /
      // throw inside `for await`) was leaking the
      // upstream source. Cancel is a no-op once the
      // stream's `done` has been observed, so we
      // skip it on natural drain.
      if (!drained && !preventCancel) {
        try { await reader.cancel(); } catch { /* swallow -- cancel errors don't block release */ }
      }
      reader.releaseLock();
    }
  }
}

// Spec: @@asyncIterator is the same function object as
// values().
ReadableStream.prototype[Symbol.asyncIterator] =
  ReadableStream.prototype.values;

ReadableStream.from = function(asyncIterable) {
  let iterator, nextMethod, isAsync;
  // Check async iterator first, then sync.
  if (asyncIterable != null &&
      typeof asyncIterable[Symbol.asyncIterator]
        === 'function') {
    iterator =
      asyncIterable[Symbol.asyncIterator]();
    if (typeof iterator !== 'object' ||
        iterator === null)
      throw new TypeError(
        "@@asyncIterator must return an object");
    nextMethod = iterator.next;
    isAsync = true;
  } else if (asyncIterable != null &&
             typeof asyncIterable[Symbol.iterator]
               === 'function') {
    iterator = asyncIterable[Symbol.iterator]();
    if (typeof iterator !== 'object' ||
        iterator === null)
      throw new TypeError(
        "@@iterator must return an object");
    nextMethod = iterator.next;
    isAsync = false;
  } else {
    throw new TypeError(
      "ReadableStream.from() requires an iterable");
  }

  let cancelInProgress = false;
  return new ReadableStream({
    async pull(controller) {
      let resultP;
      try {
        resultP = nextMethod.call(iterator);
      } catch (e) {
        controller.error(e);
        throw e;
      }
      const result = isAsync
        ? await resultP : resultP;
      if (typeof result !== 'object' ||
          result === null) {
        controller.error(new TypeError(
          "iterator result must be an object"));
        throw new TypeError(
          "iterator result must be an object");
      }
      if (result.done) {
        controller.close();
        return;
      }
      // Await value even for sync iterators
      // (handles promise-valued sync iterables).
      const v = isAsync
        ? result.value : await result.value;
      controller.enqueue(v);
    },
    cancel(reason) {
      if (cancelInProgress)
        return Promise.resolve();
      cancelInProgress = true;
      if (iterator.return === undefined)
        return Promise.resolve();
      if (typeof iterator.return !== 'function')
        return Promise.reject(new TypeError(
          "iterator.return is not a function"));
      let result;
      try {
        result = iterator.return(reason);
      } catch (e) {
        return Promise.reject(e);
      }
      return Promise.resolve(result).then(r => {
        if (typeof r !== 'object' || r === null)
          throw new TypeError(
            "return() must return an object");
      });
    }
  }, { highWaterMark: 0 });
};

class CountQueuingStrategy {
  constructor({ highWaterMark }) {
    this.highWaterMark = highWaterMark;
  }
  size() { return 1; }
}

class ByteLengthQueuingStrategy {
  constructor({ highWaterMark }) {
    this.highWaterMark = highWaterMark;
  }
  // Optional chain: size(undefined) returns undefined
  // rather than throwing (Workerd behavior).
  size(chunk) { return chunk?.byteLength; }
}

globalThis.ReadableStream = ReadableStream;
globalThis.ReadableStreamDefaultReader =
  ReadableStreamDefaultReader;
globalThis.ReadableStreamDefaultController =
  ReadableStreamDefaultController;
globalThis.CountQueuingStrategy = CountQueuingStrategy;
globalThis.ByteLengthQueuingStrategy =
  ByteLengthQueuingStrategy;
// Web IDL toStringTag -- `Object.prototype.toString
// .call(rs)` returns "[object ReadableStream]"
// instead of "[object Object]".
for (const [cls, name] of [
  [ReadableStream, 'ReadableStream'],
  [ReadableStreamDefaultReader,
    'ReadableStreamDefaultReader'],
  [ReadableStreamDefaultController,
    'ReadableStreamDefaultController'],
  [CountQueuingStrategy, 'CountQueuingStrategy'],
  [ByteLengthQueuingStrategy,
    'ByteLengthQueuingStrategy'],
]) {
  Object.defineProperty(cls.prototype,
    Symbol.toStringTag,
    { value: name, configurable: true });
}
})();
