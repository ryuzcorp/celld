// ArrayBuffer memory guard.
//
// V8 backs a plain ArrayBuffer (and a plain SharedArrayBuffer) with the
// isolate's ArrayBuffer allocator, which the host replaced with one that
// charges a per-isolate budget and refuses past it. Three paths bypass that
// allocator:
//
//  * a resizable ArrayBuffer (`new ArrayBuffer(n, {maxByteLength})`) and
//    `ArrayBuffer.prototype.resize`/`transfer`;
//  * a growable SharedArrayBuffer (`new SharedArrayBuffer(n, {maxByteLength})`)
//    and `SharedArrayBuffer.prototype.grow`;
//  * `WebAssembly.Memory`, both the constructor and `Memory.prototype.grow`.
//
// All three commit pages through V8's platform page allocator, and none of
// them takes part in the isolate allocator's accounting, so this prelude
// charges them to the same budget synchronously, before the pages exist, and
// throws the budget `RangeError` when the reservation is refused.
//
// The wrapper must not be reachable around from tenant code. This runs before
// any user script; it captures every intrinsic it uses, replaces the global
// constructors with `Proxy` objects whose target is not reachable from
// JavaScript, and rewrites `constructor`, `resize`, `transfer`, `grow` and
// `structuredClone` on the genuine prototypes so there is no second copy of
// the native entry point in the realm. A guest can overwrite its own globals
// but cannot name the original constructor: `ArrayBuffer.prototype.constructor`,
// `Reflect.construct` through the proxy, and `structuredClone` all route here.
//
// Accounting: a resizable buffer is charged its current `byteLength` on
// construction and the delta on every `resize`/`grow`; a wasm memory is charged
// its committed size on construction and on every `grow`. Bytes are returned
// when the collector frees the object, through a `FinalizationRegistry`; the
// runtime drives V8's foreground task queue at every turn's settle, so a
// dropped buffer is released by the end of the turn that dropped it. Until
// then its bytes still count, because V8 may still hold the pages.
(function () {
  // ---- captured intrinsics ------------------------------------------------
  // Every one of these is read once, before tenant code can replace it. Calls
  // go through Reflect.apply so a tenant that patches a prototype method (or
  // `WeakMap.prototype.get`) cannot alter the guard's own bookkeeping.
  const _apply = Reflect.apply;
  const _construct = Reflect.construct;
  const _defineProperty = Object.defineProperty;
  const _getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
  const _keys = Object.keys;
  const _proxy = Proxy;
  const _rangeError = RangeError;
  const _typeError = TypeError;
  const _number = Number;
  const _trunc = Math.trunc;
  const _weakMap = WeakMap;
  const _weakMapGet = WeakMap.prototype.get;
  const _weakMapSet = WeakMap.prototype.set;
  const _weakMapDelete = WeakMap.prototype.delete;
  const _set = Set;
  const _setAdd = Set.prototype.add;
  const _setHas = Set.prototype.has;
  const _setForEach = Set.prototype.forEach;
  const _mapForEach = Map.prototype.forEach;
  const _finalizationRegistry = FinalizationRegistry;
  const _finalizeRegister = FinalizationRegistry.prototype.register;
  const _finalizeUnregister = FinalizationRegistry.prototype.unregister;
  const _typedArrayBuffer = _getOwnPropertyDescriptor(
    Object.getPrototypeOf(Uint8Array.prototype), "buffer").get;
  const _dataViewBuffer = _getOwnPropertyDescriptor(DataView.prototype, "buffer").get;

  const _abProto = ArrayBuffer.prototype;
  const _byteLength = _getOwnPropertyDescriptor(_abProto, "byteLength").get;
  const _maxByteLength = _getOwnPropertyDescriptor(_abProto, "maxByteLength").get;
  const _resizable = _getOwnPropertyDescriptor(_abProto, "resizable").get;
  const _resize = _getOwnPropertyDescriptor(_abProto, "resize").value;
  const _transfer = _getOwnPropertyDescriptor(_abProto, "transfer").value;
  const _arrayBuffer = ArrayBuffer;

  const _sharedArrayBuffer =
    typeof SharedArrayBuffer === "function" ? SharedArrayBuffer : undefined;
  let _sabProto = undefined;
  let _sabByteLength = undefined;
  let _sabMaxByteLength = undefined;
  let _sabGrowable = undefined;
  let _sabGrow = undefined;
  if (_sharedArrayBuffer !== undefined) {
    _sabProto = _sharedArrayBuffer.prototype;
    _sabByteLength = _getOwnPropertyDescriptor(_sabProto, "byteLength").get;
    _sabMaxByteLength =
      _getOwnPropertyDescriptor(_sabProto, "maxByteLength").get;
    _sabGrowable = _getOwnPropertyDescriptor(_sabProto, "growable").get;
    _sabGrow = _getOwnPropertyDescriptor(_sabProto, "grow").value;
  }

  const _webAssembly =
    typeof WebAssembly === "object" && WebAssembly !== null
      ? WebAssembly
      : undefined;
  const _memory = _webAssembly === undefined ? undefined : _webAssembly.Memory;
  const _memoryGrow = _memory === undefined
    ? undefined
    : _getOwnPropertyDescriptor(_memory.prototype, "grow").value;
  const _memoryBuffer = _memory === undefined
    ? undefined
    : _getOwnPropertyDescriptor(_memory.prototype, "buffer").get;

  // `globalThis` explicitly: a later `function structuredClone` declaration
  // shadows the bare name. V8 152 has no `structuredClone` builtin, so this is
  // usually undefined here and the graft below captures harness.js's version
  // once the harness has installed it.
  let _innerClone =
    typeof globalThis.structuredClone === "function"
      ? globalThis.structuredClone
      : undefined;

  // ---- the host side ------------------------------------------------------
  // One reservation counter per isolate: the allocator's charges and these
  // wrappers' charges draw on the same bytes.
  const _reserve = $$__array_buffer_reserve;
  const _release = $$__array_buffer_release;
  const _cap = $$__array_buffer_cap;

  const _PAGE = 65536;
  const _MAX = 9007199254740991;
  const _BUDGET = "Array buffer allocation failed: the isolate's ArrayBuffer " +
    "memory budget is exhausted (see CELLO_V8_EXTERNAL_LIMIT_MB); free " +
    "buffers before allocating more";
  const _LENGTH = "Invalid array buffer length";
  const _RESIZE = "Invalid array buffer resize length";

  function _budget() {
    return new _rangeError(_BUDGET);
  }

  // ToInteger plus the non-negative size check the native entry points
  // perform, on the value the guest supplied. The converted number is what
  // reaches the native constructor/method, so a value object's conversion
  // runs once here and not a second time inside V8.
  function _toInteger(value, message) {
    if (typeof value === "bigint") {
      throw new _typeError("Cannot convert a BigInt value to a number");
    }
    const number = _number(value);
    if (number !== number) return 0;
    if (number === 0) return 0;
    const integer = _trunc(number);
    if (integer < 0 || integer > _MAX) throw new _rangeError(message);
    return integer;
  }

  function _isObject(value) {
    return value !== null &&
      (typeof value === "object" || typeof value === "function");
  }

  function _measure(getter, value) {
    if (getter === undefined) return undefined;
    try {
      return _apply(getter, value, []);
    } catch (error) {
      return undefined;
    }
  }

  // ---- bookkeeping --------------------------------------------------------
  // A tracked record holds the bytes reserved for one growable object. The
  // finalizer returns them when the collector frees the object; the runtime
  // pumps V8's foreground task queue at each turn's settle, so that happens by
  // the end of the turn that dropped the object. `done` keeps a record that
  // was unregistered (a transferred buffer) from releasing twice.
  const _records = new _weakMap();

  function _releaseRecord(record) {
    if (record.done === true) return;
    record.done = true;
    const bytes = record.bytes;
    record.bytes = 0;
    if (bytes > 0) _release(bytes);
  }

  const _finalizer = new _finalizationRegistry(function (record) {
    try {
      _releaseRecord(record);
    } catch (error) {
      // A throwing finalizer would be reported as an unhandled error; the
      // accounting it failed on is already lost either way.
    }
  });

  function _track(target, bytes) {
    const record = { bytes, done: false };
    _apply(_weakMapSet, _records, [target, record]);
    _apply(_finalizeRegister, _finalizer, [target, record, record]);
  }

  // Charge `target` up to `bytes` in total. An untracked target is charged
  // its whole size: the guard did not create it, so nothing was reserved.
  function _charge(target, bytes) {
    const record = _apply(_weakMapGet, _records, [target]);
    if (record === undefined) {
      if (bytes > 0 && _reserve(bytes) !== true) throw _budget();
      _track(target, bytes);
      return;
    }
    const delta = bytes - record.bytes;
    if (delta > 0 && _reserve(delta) !== true) throw _budget();
    if (delta < 0) _release(-delta);
    record.bytes = bytes;
  }

  // Move a tracked object's record onto the buffer object that replaced it (a
  // wasm memory's `buffer` changes when it grows), keeping the reservation
  // the caller already made for `total`.
  function _moveRecord(from, to, total) {
    const record = _apply(_weakMapGet, _records, [from]);
    if (record !== undefined) {
      if (from !== to) {
        _apply(_finalizeUnregister, _finalizer, [record]);
        _apply(_weakMapDelete, _records, [from]);
        _apply(_weakMapSet, _records, [to, record]);
        _apply(_finalizeRegister, _finalizer, [to, record, record]);
      }
      record.bytes = total;
      return;
    }
    _track(to, total);
  }

  function _forget(target) {
    const record = _apply(_weakMapGet, _records, [target]);
    if (record === undefined) return;
    _apply(_finalizeUnregister, _finalizer, [record]);
    _apply(_weakMapDelete, _records, [target]);
    _releaseRecord(record);
  }

  // ---- constructors -------------------------------------------------------
  function _maxOption(options) {
    if (!_isObject(options)) return undefined;
    const max = options.maxByteLength;
    if (max === undefined) return undefined;
    return _toInteger(max, _RESIZE);
  }

  const _CONSTRUCTOR = {
    construct(target, args, newTarget) {
      const length = args.length > 0 ? args[0] : undefined;
      const options = args.length > 1 ? args[1] : undefined;
      const max = _maxOption(options);
      if (max === undefined) {
        // Not growable: the isolate allocator charges and refuses it.
        return _construct(target, args, newTarget);
      }
      const byteLength = _toInteger(length, _LENGTH);
      if (max < byteLength) throw new _rangeError(_LENGTH);
      if (max > _cap()) {
        // One buffer may never exceed the whole budget: its address-space
        // reservation would be outside the accounting.
        throw _budget();
      }
      if (_reserve(byteLength) !== true) throw _budget();
      let buffer;
      try {
        buffer = _construct(
          target, [byteLength, { maxByteLength: max }], newTarget);
      } catch (error) {
        if (byteLength > 0) _release(byteLength);
        throw error;
      }
      _track(buffer, byteLength);
      return buffer;
    },
  };

  function _guardConstructor(target) {
    return new _proxy(target, _CONSTRUCTOR);
  }

  function _replace(holder, name, value) {
    const descriptor = _getOwnPropertyDescriptor(holder, name);
    _defineProperty(holder, name, {
      value,
      writable: descriptor === undefined ? true : descriptor.writable === true,
      enumerable:
        descriptor === undefined ? false : descriptor.enumerable === true,
      configurable:
        descriptor === undefined ? true : descriptor.configurable === true,
    });
  }

  // ---- ArrayBuffer.prototype.resize / transfer ---------------------------
  function resize(newLength) {
    // A non-growable buffer takes the native path, which throws the same
    // TypeError an unwrapped realm would.
    if (_measure(_resizable, this) !== true) {
      return _apply(_resize, this, [newLength]);
    }
    const current = _apply(_byteLength, this, []);
    const max = _apply(_maxByteLength, this, []);
    const target = _toInteger(newLength, _RESIZE);
    if (target > max) throw new _rangeError(_RESIZE);
    // Reserve before the pages are committed; a shrink is released after the
    // call succeeds.
    if (target >= current) _charge(this, target);
    let result;
    try {
      result = _apply(_resize, this, [target]);
    } catch (error) {
      _charge(this, current);
      throw error;
    }
    if (target < current) _charge(this, target);
    return result;
  }

  // `transfer` preserves resizability, so a resizable source allocates its
  // replacement through the page allocator: charge the destination before the
  // copy, and release the (now detached) source after it succeeds. Both are
  // resident during the copy, which is what the charge reflects.
  function transfer() {
    if (_measure(_resizable, this) !== true) {
      return _apply(_transfer, this, arguments);
    }
    const current = _apply(_byteLength, this, []);
    const max = _apply(_maxByteLength, this, []);
    const argument = arguments.length > 0 ? arguments[0] : undefined;
    const target = argument === undefined
      ? current
      : _toInteger(argument, _RESIZE);
    if (target > max) throw new _rangeError(_RESIZE);
    if (target > 0 && _reserve(target) !== true) throw _budget();
    let result;
    try {
      result = _apply(_transfer, this, [target]);
    } catch (error) {
      if (target > 0) _release(target);
      throw error;
    }
    _forget(this);
    _track(result, target);
    return result;
  }

  // ---- SharedArrayBuffer.prototype.grow ----------------------------------
  function grow(newLength) {
    if (_apply(_sabGrowable, this, []) !== true) {
      return _apply(_sabGrow, this, [newLength]);
    }
    const current = _apply(_sabByteLength, this, []);
    const max = _apply(_sabMaxByteLength, this, []);
    const target = _toInteger(newLength, _RESIZE);
    if (target < current || target > max) throw new _rangeError(_RESIZE);
    if (target > current) _charge(this, target);
    let result;
    try {
      result = _apply(_sabGrow, this, [target]);
    } catch (error) {
      _charge(this, current);
      throw error;
    }
    return result;
  }

  // ---- WebAssembly.Memory ------------------------------------------------
  function _memoryInitial(descriptor) {
    let initial = descriptor.initial;
    if (initial === undefined) initial = descriptor.minimum;
    if (initial === undefined) {
      throw new _typeError("WebAssembly.Memory: initial is required");
    }
    return _toInteger(initial, _LENGTH);
  }

  const _MEMORY_CONSTRUCTOR = {
    construct(target, args, newTarget) {
      const descriptor = args.length > 0 ? args[0] : undefined;
      if (!_isObject(descriptor)) {
        return _construct(target, args, newTarget);
      }
      const initial = _memoryInitial(descriptor);
      const rawMaximum = descriptor.maximum;
      const maximum = rawMaximum === undefined
        ? undefined
        : _toInteger(rawMaximum, _LENGTH);
      const shared = descriptor.shared ? true : false;
      const address = descriptor.address;
      if (shared === true && maximum === undefined) {
        // The native constructor throws the TypeError for a shared memory
        // without a maximum.
        return _construct(target, args, newTarget);
      }
      if (maximum !== undefined && initial > maximum) {
        throw new _rangeError(_LENGTH);
      }
      const bytes = initial * _PAGE;
      if (bytes > _MAX) throw _budget();
      if (bytes > 0 && _reserve(bytes) !== true) throw _budget();
      const normalized = { initial };
      if (maximum !== undefined) normalized.maximum = maximum;
      if (shared === true) normalized.shared = true;
      if (address !== undefined) normalized.address = address;
      let memory;
      try {
        memory = _construct(target, [normalized], newTarget);
      } catch (error) {
        if (bytes > 0) _release(bytes);
        throw error;
      }
      _track(_apply(_memoryBuffer, memory, []), bytes);
      return memory;
    },
  };

  function memoryGrow(pages) {
    // The buffer getter brand-checks the receiver and throws the native
    // TypeError for anything that is not a Memory.
    const buffer = _apply(_memoryBuffer, this, []);
    const before = _apply(_byteLength, buffer, []);
    const count = _toInteger(pages, _LENGTH);
    const after = before + count * _PAGE;
    // Charge the total the memory will commit (a memory only grows).
    const record = _apply(_weakMapGet, _records, [buffer]);
    const charged = record === undefined ? 0 : record.bytes;
    const extra = after - charged;
    if (extra > 0 && _reserve(extra) !== true) throw _budget();
    let previous;
    try {
      previous = _apply(_memoryGrow, this, [count]);
    } catch (error) {
      if (extra > 0) _release(extra);
      throw error;
    }
    _moveRecord(buffer, _apply(_memoryBuffer, this, []), after);
    return previous;
  }

  // ---- structuredClone ---------------------------------------------------
  // A clone of a resizable ArrayBuffer makes a new page-allocator buffer
  // without going through any wrapper. The clone is complete by the time the
  // result exists, so this is an accounting backstop rather than prevention:
  // the charge is refused when it would cross the budget, and the clone is
  // then dropped to be collected.
  function _chargeClone(root) {
    if (!_isObject(root)) return;
    const seen = new _set();
    const pending = [root];
    while (pending.length > 0) {
      const value = pending[pending.length - 1];
      pending.length = pending.length - 1;
      if (!_isObject(value)) continue;
      if (_apply(_setHas, seen, [value])) continue;
      _apply(_setAdd, seen, [value]);
      if (_measure(_sabByteLength, value) !== undefined) {
        // A cloned SharedArrayBuffer shares the source's block: no new
        // allocation, and its growth goes through the wrapped `grow`.
        continue;
      }
      if (_measure(_byteLength, value) !== undefined) {
        if (_measure(_resizable, value) === true) {
          _charge(value, _apply(_byteLength, value, []));
        }
        continue;
      }
      const view = _measure(_typedArrayBuffer, value);
      if (view !== undefined) {
        pending[pending.length] = view;
        continue;
      }
      const dataView = _measure(_dataViewBuffer, value);
      if (dataView !== undefined) {
        pending[pending.length] = dataView;
        continue;
      }
      let visited = false;
      try {
        _apply(_mapForEach, value, [function (entry, key) {
          pending[pending.length] = key;
          pending[pending.length] = entry;
        }]);
        visited = true;
      } catch (error) {
        visited = false;
      }
      if (!visited) {
        try {
          _apply(_setForEach, value, [function (entry) {
            pending[pending.length] = entry;
          }]);
          visited = true;
        } catch (error) {
          visited = false;
        }
      }
      if (!visited) {
        const names = _keys(value);
        for (let index = 0; index < names.length; index++) {
          pending[pending.length] = value[names[index]];
        }
      }
    }
  }

  function structuredClone(value) {
    if (_innerClone === undefined) {
      throw new _typeError("structuredClone is unavailable");
    }
    const result = _apply(_innerClone, undefined, arguments);
    _chargeClone(result);
    return result;
  }

  // `harness.js` installs `structuredClone` after this prelude runs, so the
  // host calls this from `install_harness`. A prelude-only wrapper would
  // leave the harness's own clone outside the accounting (it goes through the
  // host serializer and V8's deserializer, which allocate a resizable copy
  // through the page allocator).
  function graftStructuredClone() {
    const installed = globalThis.structuredClone;
    if (typeof installed !== "function" || installed === structuredClone) {
      return;
    }
    _innerClone = installed;
    _replace(globalThis, "structuredClone", structuredClone);
  }

  // ---- install -----------------------------------------------------------
  const guardedArrayBuffer = _guardConstructor(_arrayBuffer);
  _replace(globalThis, "ArrayBuffer", guardedArrayBuffer);
  _replace(_abProto, "constructor", guardedArrayBuffer);
  _replace(_abProto, "resize", resize);
  _replace(_abProto, "transfer", transfer);

  if (_sharedArrayBuffer !== undefined) {
    const guardedSharedArrayBuffer = _guardConstructor(_sharedArrayBuffer);
    _replace(globalThis, "SharedArrayBuffer", guardedSharedArrayBuffer);
    _replace(_sabProto, "constructor", guardedSharedArrayBuffer);
    _replace(_sabProto, "grow", grow);
  }

  if (_memory !== undefined) {
    const guardedMemory = new _proxy(_memory, _MEMORY_CONSTRUCTOR);
    _replace(_webAssembly, "Memory", guardedMemory);
    _replace(_memory.prototype, "constructor", guardedMemory);
    _replace(_memory.prototype, "grow", memoryGrow);
    _defineProperty(memoryGrow, "name", { value: "grow", configurable: true });
  }

  if (_innerClone !== undefined) {
    _replace(globalThis, "structuredClone", structuredClone);
  }
  // The host calls this once the harness has installed its own
  // `structuredClone`; see the function above.
  __cello.arrayBufferGuardGraftClone = graftStructuredClone;
})();
