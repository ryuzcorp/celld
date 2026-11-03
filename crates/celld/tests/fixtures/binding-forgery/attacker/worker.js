// Modified by ryuzcorp (ryuzcorp/celld): binding-forgery conformance fixture; WebSocket and TCP socket ownership. Apache-2.0.
import { DurableObject, WorkflowEntrypoint } from 'cloudflare:workers';

const J = (o, st = 200) => Response.json(o, { status: st });
const te = new TextEncoder();

const VICTIM_SCRIPT = 'bf-victim';
const D1_KEY = 'cells:v1:d1:__D1Database';
const KV_KEY = 'cells:v1:kv:__KvNamespace';
const QUEUE_KEY = 'cells:v1:queue:__Queue';
const scriptKey = (script, cls) => `cells:v1:${script.length}:${script}:${cls}`;

const hex = (bytes) =>
  [...new Uint8Array(bytes)].map((b) => b.toString(16).padStart(2, '0')).join('');

async function hmacSha256(keyBytes, msgBytes) {
  const key = await crypto.subtle.importKey(
    'raw', keyBytes, { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'],
  );
  return new Uint8Array(await crypto.subtle.sign('HMAC', key, msgBytes));
}

// The Durable Object id derivation, reimplemented from the public
// namespace-key format: key = SHA-256(namespaceKey), id = HMAC(key,
// name)[0..16] + HMAC(key, id[0..16])[0..16]. A real adversary runs
// this themselves; the fixture does it to prove the dispatch layer,
// not just the id op, refuses forged names.
async function doIdHex(namespaceKey, name) {
  const key = new Uint8Array(await crypto.subtle.digest('SHA-256', te.encode(namespaceKey)));
  const inner = await hmacSha256(key, te.encode(name));
  const first = inner.slice(0, 16);
  const outer = await hmacSha256(key, first);
  return hex(first) + hex(outer.slice(0, 16));
}

const attempt = async (fn) => {
  try {
    return { threw: false, result: await fn() };
  } catch (error) {
    return {
      threw: true,
      name: error?.name ?? 'unknown',
      error: String((error && error.message) || error),
    };
  }
};

const __wsUpgradeHeaders = {
  Upgrade: 'websocket',
  Connection: 'Upgrade',
  'Sec-WebSocket-Key': 'dGhlIHNhbXBsZSBub25jZQ==',
  'Sec-WebSocket-Version': '13',
};

export class AttackerDO extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.env = env;
  }
  // Use a stub handed to this cell over RPC. Legitimate for a stub the
  // sender could already dispatch to; a forged stub is refused at lift,
  // before the call reaches this method.
  async useStub(stub) {
    const r = await (await stub.fetch('https://x/')).json();
    return { secret: r.secret };
  }
  async queryD1(stub) {
    const rows = await stub.__d1Query([
      { sql: 'SELECT v FROM t', params: [], first: false },
    ]);
    return { rows };
  }
  async useWf(stub) {
    const status = await stub.__wfStatus();
    return { status: status.status ?? 'unknown' };
  }
  async fetchVia(stub) {
    const r = await stub.fetch('https://x/');
    return { reached: r.status };
  }
  // Forge a victim-D1 stub from pure-JS ids (no id op: the id op already
  // refuses the undeclared name) and round-trip it through this cell's
  // own storage. The lift refuses the put; without the lift gate the
  // read-back grants and the query leaks.
  async launderStoreD1() {
    const C = Object.getPrototypeOf(this.env.OWN_NS).constructor;
    const IdC = Object.getPrototypeOf(
      this.env.OWN_NS.getByName('probe').id,
    ).constructor;
    const idHex = await doIdHex(D1_KEY, 'bf-victim-db');
    const ns = new C('__D1Database', D1_KEY);
    const stub = ns.get(new IdC('__D1Database', idHex, 'bf-victim-db'));
    await this.ctx.storage.put('launder', stub);
    const back = await this.ctx.storage.get('launder');
    const rows = await back.__d1Query([
      { sql: 'SELECT v FROM t', params: [], first: false },
    ]);
    return { rows };
  }
  // Same shape for an undeclared workflow name in the own class: the id
  // op mints (an id alone addresses nothing), the lift refuses the put.
  async launderStoreWf() {
    const C = Object.getPrototypeOf(this.env.OWN_NS).constructor;
    const cls = '__Workflow.bf-attacker';
    const ns = new C(cls, scriptKey('bf-attacker', cls));
    const stub = ns.getByName('nope-not-declared/xyz');
    await this.ctx.storage.put('launder', stub);
    const back = await this.ctx.storage.get('launder');
    const status = await back.__wfStatus();
    return { status: status.status ?? 'unknown' };
  }
  // Same shape for the victim object namespace via pure-JS ids.
  async launderStoreDo() {
    const C = Object.getPrototypeOf(this.env.OWN_NS).constructor;
    const IdC = Object.getPrototypeOf(
      this.env.OWN_NS.getByName('probe').id,
    ).constructor;
    const idHex = await doIdHex(
      scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name',
    );
    const ns = new C('VictimDO', scriptKey(VICTIM_SCRIPT, 'VictimDO'));
    const stub = ns.get(new IdC('VictimDO', idHex));
    await this.ctx.storage.put('launder', stub);
    const back = await this.ctx.storage.get('launder');
    const r = await (await back.fetch('https://x/')).json();
    return { secret: r.secret };
  }
  // Store a stub for a declared class and use the read-back: the
  // legitimate storage round trip.
  async storeRoundTrip(name) {
    const stub = this.env.OWN_NS.getByName(name);
    await this.ctx.storage.put('pass', stub);
    const back = await this.ctx.storage.get('pass');
    const r = await (await back.fetch('https://x/')).json();
    return { secret: r.secret };
  }
  // Smuggle a forged marker through a class instance beside a legitimate
  // stub. The instance leaves the lift to the clone and arrives as plain
  // data; only the side-table-listed stub revives. Before the structural
  // fix this granted and leaked the victim secret.
  async smuggleStoreInstance(victimHex) {
    class Smuggle {
      constructor() {
        this.__celld$do = 'VictimDO';
        this.v = victimHex;
        this.n = null;
      }
    }
    const own = this.env.OWN_NS.getByName('smuggle-cell');
    await this.ctx.storage.put('smuggle', { s: own, evil: new Smuggle() });
    const back = await this.ctx.storage.get('smuggle');
    const ownOk = typeof back.s?.fetch === 'function';
    const evilFetch = typeof back.evil?.fetch;
    let leaked = null;
    try {
      if (evilFetch === 'function') {
        const r = await (await back.evil.fetch('https://x/')).json();
        leaked = r.secret ?? null;
      }
    } catch {}
    return { ownOk, evilFetch, leaked };
  }
  // A legitimate class instance beside a stub round-trips as data while
  // the stub stays usable.
  async storeInstanceOk() {
    class Legit {
      constructor() {
        this.a = 1;
        this.b = 'x';
      }
    }
    const stub = this.env.OWN_NS.getByName('instance-ok-cell');
    await stub.fetch('https://x/?set=instance-ok-secret');
    await this.ctx.storage.put('instanceOk', { s: stub, obj: new Legit() });
    const back = await this.ctx.storage.get('instanceOk');
    const r = await (await back.s.fetch('https://x/')).json();
    return { secret: r.secret, a: back.obj?.a, b: back.obj?.b };
  }
  // Read back a hand-planted pre-gate envelope: it decodes as plain data
  // and grants nothing.
  async plantV1Inert(victimHex) {
    const scope = this.ctx.storage._scope;
    const tree = { evil: { __celld$do: 'VictimDO', v: victimHex, n: null } };
    globalThis.__celldPlantStoredV1ForTestOnly(scope, 'planted-v1', tree);
    const back = await this.ctx.storage.get('planted-v1');
    const evilFetch = typeof back?.evil?.fetch;
    let leaked = null;
    try {
      if (evilFetch === 'function') {
        const r = await (await back.evil.fetch('https://x/')).json();
        leaked = r.secret ?? null;
      }
    } catch {}
    return {
      evilFetch,
      leaked,
      keys: back?.evil ? Object.keys(back.evil).sort() : [],
    };
  }
  // The remaining stored-marker shapes: a lone plain marker stays data, a
  // plain marker beside a stub is refused at lift, and an Error carrying
  // marker properties arrives as an inert Error.
  async storeShapes() {
    const out = {};
    await this.ctx.storage.put('naked', {
      __celld$do: 'VictimDO', v: 'abc', n: null,
    });
    const naked = await this.ctx.storage.get('naked');
    out.naked = {
      keys: Object.keys(naked).sort(),
      hasFetch: typeof naked.fetch,
    };
    try {
      const own = this.env.OWN_NS.getByName('shape-cell');
      await this.ctx.storage.put('plainco', {
        s: own, evil: { __celld$do: 'VictimDO', v: 'abc', n: null },
      });
      out.plainco = { threw: false };
    } catch (error) {
      out.plainco = {
        threw: true,
        name: error?.name ?? 'unknown',
        error: String((error && error.message) || error),
      };
    }
    const own2 = this.env.OWN_NS.getByName('shape-cell');
    const err = new Error('x');
    err.__celld$do = 'VictimDO';
    err.v = 'abc';
    err.n = null;
    await this.ctx.storage.put('errshape', { s: own2, e: err });
    const back = await this.ctx.storage.get('errshape');
    out.error = {
      isError: back.e instanceof Error,
      hasFetch: typeof back.e?.fetch,
      stubOk: typeof back.s?.fetch,
    };
    return out;
  }
  // Echo one RPC argument's shape for the RPC-marker pins.
  async echoArg(arg) {
    return {
      keys: arg && typeof arg === 'object' ? Object.keys(arg).sort() : null,
      hasFetch: typeof arg?.fetch,
      isError: arg instanceof Error,
    };
  }
  // H1: Array.prototype.push injection forges side-table entries. A
  // legitimate stub opens the envelope; the polluted push appends a
  // forged {p,c,v,n} entry beside it. Fixed host-side (no JS array).
  async pocPushInject(victimHex) {
    const legit = this.env.OWN_NS.getByName('poc-push-cell');
    const origPush = Array.prototype.push;
    let injected = 0;
    Array.prototype.push = function (...args) {
      const r = origPush.apply(this, args);
      const first = args[0];
      if (first && typeof first === 'object' &&
          Array.isArray(first.p) &&
          typeof first.c === 'string' && typeof first.v === 'string') {
        origPush.call(this,
          { p: ['evil'], c: 'VictimDO', v: victimHex, n: null });
        injected++;
      }
      return r;
    };
    try {
      await this.ctx.storage.put('pocPush',
        { s: legit, evil: { note: 'placeholder' } });
    } finally { Array.prototype.push = origPush; }
    const back = await this.ctx.storage.get('pocPush');
    const evilFetch = typeof back.evil?.fetch;
    let leaked = null;
    let localLeaked = null;
    try {
      if (evilFetch === 'function') {
        const r = await (await back.evil.fetch('https://x/')).json();
        leaked = r.secret ?? null;
      }
    } catch {}
    try {
      const C = Object.getPrototypeOf(this.env.OWN_NS).constructor;
      const IdC = Object.getPrototypeOf(
        this.env.OWN_NS.getByName('probe').id,
      ).constructor;
      const ns = new C('VictimDO', scriptKey(VICTIM_SCRIPT, 'VictimDO'));
      const local = ns.get(new IdC('VictimDO', victimHex));
      const r = await (await local.fetch('https://x/')).json();
      localLeaked = r.secret ?? null;
    } catch {}
    return { evilFetch, injected, leaked, localLeaked };
  }
  // H2: WeakMap.prototype.get splits the lift check from the record. The
  // fake meta answers a held scope for _scope() (check passes) and the
  // victim for _className/_value (record forges). Fixed host-side (the
  // host reads one brand value for both).
  async pocWeakmapSplit(victimHex) {
    const legit = this.env.OWN_NS.getByName('poc-wm-cell');
    const marker = this.env.OWN_NS.getByName('poc-wm-marker');
    const fakeDoMeta = { id: {
      _scope: () => legit.id._scope(),
      _className: 'VictimDO', _value: victimHex,
    }, name: null };
    const origGet = WeakMap.prototype.get;
    let calls = 0;
    WeakMap.prototype.get = function (k) {
      if (k === marker) { calls++; return calls >= 2 ? fakeDoMeta : undefined; }
      return origGet.call(this, k);
    };
    try {
      await this.ctx.storage.put('pocWm', { s: legit, m: marker });
    } finally { WeakMap.prototype.get = origGet; }
    const back = await this.ctx.storage.get('pocWm');
    const mFetch = typeof back.m?.fetch;
    let leaked = null;
    try {
      if (mFetch === 'function') {
        const r = await (await back.m.fetch('https://x/')).json();
        leaked = r.secret ?? null;
      }
    } catch {}
    return { calls, mFetch, leaked };
  }
  // H3 storage: Uint8Array.prototype.set flips the envelope tag 1->2,
  // turning the plant hook's inert 0x01 tree into a decoded pair with a
  // forged table. Fixed by host-side tags (0x01/0x02 inert) and gating
  // the plant hook (test-only).
  async pocPlantFlip(victimHex) {
    const scope = this.ctx.storage._scope;
    const tree = [{ s: null }, [{ p: ['s'], c: 'VictimDO', v: victimHex, n: null }]];
    const origSet = Uint8Array.prototype.set;
    let flipped = 0;
    Uint8Array.prototype.set = function (...args) {
      const r = origSet.apply(this, args);
      if (this.length > 0 && this[0] === 1) { this[0] = 2; flipped++; }
      return r;
    };
    try {
      globalThis.__celldPlantStoredV1ForTestOnly(scope, 'pocPlant', tree);
    } finally { Uint8Array.prototype.set = origSet; }
    const back = await this.ctx.storage.get('pocPlant');
    const val = Array.isArray(back) ? back[0] : back;
    const sFetch = typeof val?.s?.fetch;
    let leaked = null;
    try {
      if (sFetch === 'function') {
        const r = await (await val.s.fetch('https://x/')).json();
        leaked = r.secret ?? null;
      }
    } catch {}
    return { flipped, sFetch, leaked };
  }
  // H3 RPC callee half: called as exploitFlip(fakeTree, forgedTable, fn)
  // bare (tag 1, no flip) or as exploitFlip({s}) flipped (tag 3, pair).
  async exploitFlip(...args) {
    let target = null;
    if (args.length === 1 && args[0] && typeof args[0] === 'object' && 's' in args[0]) {
      target = args[0].s;
    } else if (args.length >= 1 && Array.isArray(args[0]) && args[0][0]) {
      target = args[0][0].s;
    }
    const sFetch = typeof target?.fetch;
    let leaked = null;
    try {
      if (sFetch === 'function') {
        const r = await (await target.fetch('https://x/')).json();
        leaked = r.secret ?? null;
      }
    } catch {}
    return { sFetch, leaked, nargs: args.length };
  }
  // I1: a `__proto__` table path must refuse at lift (DataCloneError) and
  // leave Object.prototype clean; a legit nested stub still round-trips.
  async protoAfter() {
    const out = {};
    const nested = this.env.OWN_NS.getByName('proto-nested');
    await nested.fetch('https://x/?set=proto-nested-secret');
    await this.ctx.storage.put('protoNested', { a: { b: { s: nested, n: 7 } } });
    const backNested = await this.ctx.storage.get('protoNested');
    try {
      const r = await (await backNested.a.b.s.fetch('https://x/')).json();
      out.nestedSecret = r.secret ?? null;
      out.nestedN = backNested.a.b.n;
    } catch (error) {
      out.nestedSecret = null;
      out.nestedError = String((error && error.message) || error);
    }
    const legit = this.env.OWN_NS.getByName('proto-cell');
    const outer = {};
    Object.defineProperty(outer, '__proto__', {
      value: { x: legit }, enumerable: true, configurable: true, writable: true,
    });
    try {
      await this.ctx.storage.put('proto', { o: outer });
      out.putThrew = null;
    } catch (error) {
      out.putThrew = String((error && error.message) || error);
      out.putName = error?.name ?? 'unknown';
    }
    if (out.putThrew === null) {
      try {
        await this.ctx.storage.get('proto');
      } catch {}
    }
    out.protoKeys = Object.keys(Object.prototype);
    out.protoHasX = Object.hasOwn(Object.prototype, 'x');
    try {
      delete Object.prototype.x;
    } catch {}
    out.freshProtoOk = Object.getPrototypeOf({}) === Object.prototype;
    const ctorish = { constructor: { prototype: { x: legit } } };
    try {
      await this.ctx.storage.put('protoCtor', ctorish);
      out.ctorThrew = null;
      try {
        await this.ctx.storage.get('protoCtor');
      } catch {}
      out.ctorProtoOk = Object.getPrototypeOf({}) === Object.prototype;
    } catch (error) {
      out.ctorThrew = String((error && error.message) || error);
      out.ctorName = error?.name ?? 'unknown';
      out.ctorProtoOk = Object.getPrototypeOf({}) === Object.prototype;
    }
    return out;
  }
  // I2: the plant hook's tag flips 1->3 under a polluted set, forging a
  // host-table envelope. Fixed host-side (tagged bytes), so the row stays
  // inert: no stub installs and no grant mints for the local stub either.
  async plantFlip03(victimHex) {
    const scope = this.ctx.storage._scope;
    const tree = [{ s: null }, [{ p: ['s'], c: 'VictimDO', v: victimHex, n: null }]];
    const origSet = Uint8Array.prototype.set;
    let flipped = 0;
    Uint8Array.prototype.set = function (...args) {
      const r = origSet.apply(this, args);
      if (this.length > 0 && this[0] === 1) { this[0] = 3; flipped++; }
      return r;
    };
    try {
      globalThis.__celldPlantStoredV1ForTestOnly(scope, 'pocPlant03', tree);
    } finally { Uint8Array.prototype.set = origSet; }
    const back = await this.ctx.storage.get('pocPlant03');
    const val = Array.isArray(back) ? back[0] : back;
    const sFetch = typeof val?.s?.fetch;
    let leaked = null;
    try {
      if (sFetch === 'function') {
        const r = await (await val.s.fetch('https://x/')).json();
        leaked = r.secret ?? null;
      }
    } catch {}
    let localLeaked = null;
    try {
      const C = Object.getPrototypeOf(this.env.OWN_NS).constructor;
      const IdC = Object.getPrototypeOf(
        this.env.OWN_NS.getByName('probe').id,
      ).constructor;
      const ns = new C('VictimDO', scriptKey(VICTIM_SCRIPT, 'VictimDO'));
      const local = ns.get(new IdC('VictimDO', victimHex));
      const r = await (await local.fetch('https://x/')).json();
      localLeaked = r.secret ?? null;
    } catch {}
    return { flipped, sFetch, leaked, localLeaked };
  }
  // I3: plain data still revives as data while Object.prototype carries a
  // smuggled shape marker. The stub co-passenger forces the revive walk;
  // before the own-property guard the plain half revived as a foreign stub.
  async svcProto() {
    const stub = this.env.OWN_NS.getByName('svc-proto-cell');
    await this.ctx.storage.put('svcProto', { s: stub, data: { a: 1, b: 'x' } });
    Object.prototype.__celld$svc = 'default';
    let back = null;
    let threw = null;
    try {
      back = await this.ctx.storage.get('svcProto');
    } catch (error) {
      threw = String((error && error.message) || error);
    } finally {
      delete Object.prototype.__celld$svc;
    }
    const data = back?.data;
    return {
      threw,
      rootType: typeof data,
      a: data && typeof data === 'object' ? data.a ?? null : null,
      b: data && typeof data === 'object' ? data.b ?? null : null,
      keys: data && typeof data === 'object' ? Object.keys(data).sort() : null,
      stubOk: typeof back?.s?.fetch,
    };
  }
  // I4 callee half: throw one error for the polluted-subarray pin.
  async throwBoom() {
    throw new Error('callee-boom');
  }
  // H6: mutable ctx._scope reaches the host-only pending-put drop. After
  // the fix both scopes are non-writable (assignment throws) and the drop
  // derives the executing cell host-side.
  async scopeMutate() {
    const out = {};
    try {
      this.ctx.storage._scope = 'VictimDO:dead';
      out.storageAssignThrew = false;
    } catch {
      out.storageAssignThrew = true;
    }
    try {
      this.ctx._scope = 'VictimDO:dead';
      out.ctxAssignThrew = false;
    } catch {
      out.ctxAssignThrew = true;
    }
    try {
      await this.ctx.storage.put('h6', 'x');
      out.putOk = true;
    } catch (error) {
      out.putOk = false;
      out.putError = String((error && error.message) || error);
    }
    return out;
  }
  // Full prototype-pollution prelude before legit stub passing (stored +
  // RPC) and forged attempts. Legit must still work (primordials + host),
  // forged must still fail (host check). Pollution is restored before
  // every await so async continuations run clean.
  async pollutionRoundTrip(victimHex) {
    const out = {};
    const saved = {
      push: Array.prototype.push,
      mapGet: Map.prototype.get,
      mapSet: Map.prototype.set,
      mapHas: Map.prototype.has,
      setAdd: Set.prototype.add,
      setHas: Set.prototype.has,
      weakGet: WeakMap.prototype.get,
      weakSet: WeakMap.prototype.set,
      weakHas: WeakMap.prototype.has,
      weakSetAdd: WeakSet.prototype.add,
      weakSetHas: WeakSet.prototype.has,
      u8Set: Uint8Array.prototype.set,
      u8Subarray: Uint8Array.prototype.subarray,
      jsonStringify: JSON.stringify,
      jsonParse: JSON.parse,
      arrayIsArray: Array.isArray,
      objectKeys: Object.keys,
      getProto: Object.getPrototypeOf,
    };
    const poison = (name) => {
      throw new Error(`polluted:${name}`);
    };
    const pollute = () => {
      Array.prototype.push = function () { poison('push'); };
      Map.prototype.get = function () { poison('mapGet'); };
      Map.prototype.set = function () { poison('mapSet'); };
      Map.prototype.has = function () { poison('mapHas'); };
      Set.prototype.add = function () { poison('setAdd'); };
      Set.prototype.has = function () { poison('setHas'); };
      WeakMap.prototype.get = function () { poison('weakGet'); };
      WeakMap.prototype.set = function () { poison('weakSet'); };
      WeakMap.prototype.has = function () { poison('weakHas'); };
      WeakSet.prototype.add = function () { poison('weakSetAdd'); };
      WeakSet.prototype.has = function () { poison('weakSetHas'); };
      Uint8Array.prototype.set = function () { poison('u8Set'); };
      Uint8Array.prototype.subarray = function () { poison('u8Subarray'); };
      JSON.stringify = () => { poison('jsonStringify'); };
      JSON.parse = () => { poison('jsonParse'); };
      Array.isArray = () => { poison('arrayIsArray'); };
      Object.keys = () => { poison('objectKeys'); };
      Object.getPrototypeOf = () => { poison('getProto'); };
    };
    const restore = () => {
      Array.prototype.push = saved.push;
      Map.prototype.get = saved.mapGet;
      Map.prototype.set = saved.mapSet;
      Map.prototype.has = saved.mapHas;
      Set.prototype.add = saved.setAdd;
      Set.prototype.has = saved.setHas;
      WeakMap.prototype.get = saved.weakGet;
      WeakMap.prototype.set = saved.weakSet;
      WeakMap.prototype.has = saved.weakHas;
      WeakSet.prototype.add = saved.weakSetAdd;
      WeakSet.prototype.has = saved.weakSetHas;
      Uint8Array.prototype.set = saved.u8Set;
      Uint8Array.prototype.subarray = saved.u8Subarray;
      JSON.stringify = saved.jsonStringify;
      JSON.parse = saved.jsonParse;
      Array.isArray = saved.arrayIsArray;
      Object.keys = saved.objectKeys;
      Object.getPrototypeOf = saved.getProto;
    };
    // Legit stored round trip under pollution.
    pollute();
    let putPromise = null;
    let putThrew = null;
    try {
      const stub = this.env.OWN_NS.getByName('pollution-cell');
      putPromise = this.ctx.storage.put('pollution', { s: stub, n: 1 });
    } catch (error) {
      putThrew = String((error && error.message) || error);
    } finally { restore(); }
    if (putThrew !== null) {
      out.storedLegit = { threw: true, error: putThrew };
    } else {
      await putPromise;
      const back = await this.ctx.storage.get('pollution');
      const r = await (await back.s.fetch('https://x/')).json();
      out.storedLegit = { threw: false, secret: r.secret ?? null, n: back.n };
    }
    // Forged stored (H1-style: legit co-passenger + push injection).
    pollute();
    // Re-pollute push as injector (not thrower) for the forgery half.
    Array.prototype.push = function (...args) {
      const r = saved.push.apply(this, args);
      const first = args[0];
      if (first && typeof first === 'object' &&
          saved.arrayIsArray(first.p) &&
          typeof first.c === 'string' && typeof first.v === 'string') {
        saved.push.call(this,
          { p: ['evil'], c: 'VictimDO', v: victimHex, n: null });
      }
      return r;
    };
    // Un-poison the rest so only the injection runs (the host must still
    // refuse it); legit already proved the throwers above.
    Map.prototype.get = saved.mapGet;
    Map.prototype.set = saved.mapSet;
    WeakMap.prototype.get = saved.weakGet;
    WeakMap.prototype.set = saved.weakSet;
    WeakMap.prototype.has = saved.weakHas;
    Uint8Array.prototype.set = saved.u8Set;
    Array.isArray = saved.arrayIsArray;
    Object.keys = saved.objectKeys;
    Object.getPrototypeOf = saved.getProto;
    JSON.stringify = saved.jsonStringify;
    JSON.parse = saved.jsonParse;
    let forgedPutThrew = null;
    try {
      const legit = this.env.OWN_NS.getByName('pollution-cell');
      const p = this.ctx.storage.put('pollutionForged',
        { s: legit, evil: { note: 'x' } });
      restore();
      await p;
    } catch (error) {
      forgedPutThrew = String((error && error.message) || error);
      restore();
    }
    const backForged = await this.ctx.storage.get('pollutionForged').catch(() => null);
    let forgedLeaked = null;
    let forgedFetch = typeof backForged?.evil?.fetch;
    try {
      if (forgedFetch === 'function') {
        const r = await (await backForged.evil.fetch('https://x/')).json();
        forgedLeaked = r.secret ?? null;
      }
    } catch {}
    out.storedForged = { threw: forgedPutThrew !== null, error: forgedPutThrew, evilFetch: forgedFetch, leaked: forgedLeaked };
    return out;
  }
  // Raw SQL cannot reach the storage tables.
  async sqlCfKv() {
    const out = {};
    try {
      this.ctx.storage.sql.exec(
        "INSERT INTO _cf_KV (scope,k,v) VALUES ('x','y','z')",
      );
      out.insert = { threw: false };
    } catch (error) {
      out.insert = { threw: true, error: String((error && error.message) || error) };
    }
    try {
      const rows = this.ctx.storage.sql.exec('SELECT * FROM _cf_KV').toArray();
      out.select = { threw: false, rows: rows.length };
    } catch (error) {
      out.select = { threw: true, error: String((error && error.message) || error) };
    }
    return out;
  }
  async webSocketMessage(ws, msg) {
    const text = typeof msg === 'string' ? msg : '[binary]';
    const inbox = JSON.parse((await this.ctx.storage.get('ws-inbox')) || '[]');
    inbox.push(text);
    await this.ctx.storage.put('ws-inbox', JSON.stringify(inbox));
    ws.send(`echo:${text}`);
  }
  async webSocketClose(ws, code, reason) {
    await this.ctx.storage.put(
      'ws-closed', JSON.stringify({ code, reason: String(reason ?? '') }),
    );
  }
  async webSocketError(ws, error) {
    await this.ctx.storage.put(
      'ws-error', String((error && error.message) || error),
    );
  }
  async fetch(req) {
    const sub = new URL(req.url).pathname.split('/').pop();
    if (sub === 'own') {
      await this.ctx.storage.put('k', 'own-value');
      return J({ value: await this.ctx.storage.get('k') });
    }
    if (sub === 'ws-accept') {
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1], ['victim-tag']);
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
    if (sub === 'ws-accept-held') {
      // Accept without answering 101: the socket lives in the registry with
      // no transport, so id-ownership probes need no client connection.
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1], ['held-tag']);
      return J({ id: pair[1]._id });
    }
    if (sub === 'ws-list') {
      return J({
        sockets: this.ctx.getWebSockets().map((ws) => ({
          id: ws._id, tags: ws._tags,
        })),
      });
    }
    if (sub === 'ws-attach-read') {
      return J({
        attachments: this.ctx.getWebSockets().map((ws) => {
          try {
            return ws.deserializeAttachment() ?? null;
          } catch {
            return 'unreadable';
          }
        }),
      });
    }
    if (sub === 'ws-accept-known') {
      const id = Number(new URL(req.url).searchParams.get('id'));
      return J(await attempt(async () => {
        this.ctx.acceptWebSocket(new WebSocket(id));
        return { accepted: true };
      }));
    }
    if (sub === 'ws-accept-spray') {
      const n = Number(new URL(req.url).searchParams.get('n') || '256');
      let threw = 0;
      for (let i = 1; i <= n; i++) {
        try {
          this.ctx.acceptWebSocket(new WebSocket(i));
        } catch {
          threw++;
        }
      }
      return J({ sprayed: n, threw });
    }
    if (sub === 'ws-attach-known') {
      const id = Number(new URL(req.url).searchParams.get('id'));
      new WebSocket(id).serializeAttachment({ pwned: 'ws-attach' });
      return J({ set: true });
    }
    if (sub === 'ws-autoresponse-forge') {
      // A forged method receiver: the auto-response methods read only
      // `this._scope`, so a plain object carrying the victim's scope
      // reaches the op, which refuses anything but the executing cell.
      const victim = new URL(req.url).searchParams.get('victim') || 'holder-g';
      const scope = `AttackerDO:${await doIdHex(
        scriptKey('bf-attacker', 'AttackerDO'), victim,
      )}`;
      const set = await attempt(async () => {
        this.ctx.setWebSocketAutoResponse.call(
          { _scope: scope },
          new WebSocketRequestResponsePair('ar-q', 'ar-pwned'),
        );
        return { set: true };
      });
      const get = await attempt(async () => {
        const pair = this.ctx.getWebSocketAutoResponse.call({ _scope: scope });
        return {
          pair: pair === null
            ? null
            : { request: pair.request, response: pair.response },
        };
      });
      return J({ set, get });
    }
    if (sub === 'ws-autoresponse-read') {
      const pair = this.ctx.getWebSocketAutoResponse();
      return J({
        pair: pair === null
          ? null
          : { request: pair.request, response: pair.response },
      });
    }
    if (sub === 'ws-tcp-note-id') {
      await this.ctx.storage.put(
        'tcp-victim-id', new URL(req.url).searchParams.get('id'),
      );
      return J({ ok: true });
    }
    if (sub === 'ws-tcp-read-id') {
      return J({ id: await this.ctx.storage.get('tcp-victim-id') });
    }
    if (sub === 'forge-sql') {
      // Any Durable Object forges the victim D1 cell's SQL scope and
      // selects through it: no D1 binding needed, no stub involved.
      const out = await attempt(async () => {
        const idHex = await doIdHex(D1_KEY, 'bf-victim-db');
        const scope = `__D1Database:${idHex}`;
        const SqlC = Object.getPrototypeOf(this.ctx.storage.sql).constructor;
        const forged = new SqlC(scope, this.ctx, this.ctx.storage);
        const rows = forged.exec('SELECT v FROM t').toArray();
        return { leaked: rows.length > 0 ? rows[0].v : null };
      });
      return J(out);
    }
    if (sub === 'forge-do') {
      // Any Durable Object forges another cell's KV storage scope. The
      // victim D1 cell shares this isolate's thread, so a forged put and
      // get round-trip deterministically; the victim object's own cell
      // usually sits on another thread, where the forged read fails at
      // the connection registry instead of the policy.
      const SC = Object.getPrototypeOf(this.ctx.storage).constructor;
      const d1kv = await attempt(async () => {
        const idHex = await doIdHex(D1_KEY, 'bf-victim-db');
        const forged = new SC(`__D1Database:${idHex}`, this.ctx);
        await forged.put('pwned', 'cross-cell-kv-write');
        return { leaked: await forged.get('pwned') };
      });
      const victimDo = await attempt(async () => {
        const idHex = await doIdHex(scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name');
        const forged = new SC(`VictimDO:${idHex}`, this.ctx);
        return { leaked: await forged.get('secret') };
      });
      return J({ d1kv, victimDo });
    }
    if (sub === 'forge-container') {
      // A container object retargets another cell's container. Unreachable
      // without a containers[] declaration (this fixture has none), so
      // this reports reachability only.
      return J({ hasContainer: this.ctx.container !== undefined });
    }
    // Generic echo cell for the own-namespace round trip.
    const url = new URL(req.url);
    if (url.searchParams.has('set')) {
      await this.ctx.storage.put('secret', url.searchParams.get('set'));
      return J({ ok: true });
    }
    return J({ secret: (await this.ctx.storage.get('secret')) ?? 'unset' });
  }
}

export class AttackerWF extends WorkflowEntrypoint {
  async run() {
    return 'attacker-wf-output';
  }
}

export default {
  async scheduled() {},
  async fetch(req, env) {
    const reqUrl = new URL(req.url);
    const t = reqUrl.pathname.slice(1);
    const q = reqUrl.searchParams;
    const subFetch = async (cell, sub) => await (await env.OWN_NS.getByName(cell).fetch(
      `https://x/sub/${sub}`,
    )).json();
    // Upgrade pass-throughs return the 101 unwrapped, so the test's own TCP
    // becomes the client of the cell's socket. The subrequest carries the
    // Upgrade header workerd demands before a response may carry a socket.
    if (t === 'ws-pass-upgrade') {
      const stub = env.OWN_NS.getByName('spray-holder');
      return stub.fetch('https://x/sub/ws-accept', { headers: __wsUpgradeHeaders });
    }
    if (t === 'ws-victim-proxy-upgrade') {
      return env.SB.fetch('https://victim/ws-up', { headers: __wsUpgradeHeaders });
    }
    const run = async () => {
      switch (t) {
        case 'seed-victim': {
          return await (await env.SB.fetch('https://victim/seed')).json();
        }
        case 'd1-own': {
          await env.OWN_DB.exec('CREATE TABLE IF NOT EXISTS t (v TEXT)');
          await env.OWN_DB.exec("DELETE FROM t");
          await env.OWN_DB.exec("INSERT INTO t (v) VALUES ('own-row')");
          const r = await env.OWN_DB.prepare('SELECT v FROM t').all();
          return { value: r.results[0]?.v ?? null };
        }
        case 'd1-forge': {
          const C = Object.getPrototypeOf(env.OWN_DB).constructor;
          const out = await attempt(async () => {
            const victim = new C('bf-victim-db');
            const r = await victim.prepare('SELECT v FROM t').all();
            return { leaked: r.results[0]?.v ?? null };
          });
          return out;
        }
        case 'd1-rpc-deny': {
          // The binding's own stub answers the query methods but not the
          // operator-only migrate method.
          const query = await env.OWN_DB._stub.__d1Query([
            { sql: 'SELECT 1 AS one', params: [], first: false },
          ]);
          const denied = await attempt(() =>
            env.OWN_DB._stub.__d1Migrate('evil', 'CREATE TABLE evil (x)', 'd1_migrations'));
          return { queryOk: Array.isArray(query), denied };
        }
        case 'kv-own': {
          await env.OWN_KV.put('ownkey', 'own-kv-value');
          return { value: await env.OWN_KV.get('ownkey') };
        }
        case 'kv-forge': {
          const C = Object.getPrototypeOf(env.OWN_KV).constructor;
          const out = await attempt(async () => {
            const victim = new C(env.OWN_KV._namespace, 'bf-victim-kv/0');
            return { leaked: await victim.get('seedkey') };
          });
          return out;
        }
        case 'kv-rpc-deny': {
          const denied = await attempt(() =>
            env.OWN_KV._stub.__kvSweep({ now: Date.now(), limit: 1 }));
          return { denied };
        }
        case 'queue-own': {
          const sent = await env.OWN_Q.send({ hello: 'own' });
          const metrics = await env.OWN_Q.metrics();
          return { sent: !!sent, metricsOk: typeof metrics === 'object' };
        }
        case 'queue-forge': {
          const C = Object.getPrototypeOf(env.OWN_Q).constructor;
          const out = await attempt(async () => {
            const victim = new C('bf-victim-q', 'bf-victim-q', 0);
            await victim.send({ injected: true });
            return { injected: true };
          });
          return out;
        }
        case 'do-own': {
          const stub = env.OWN_NS.getByName('own-name');
          await stub.fetch('https://x/?set=own-do-secret');
          const r = await (await stub.fetch('https://x/')).json();
          return { secret: r.secret };
        }
        case 'do-forge': {
          const C = Object.getPrototypeOf(env.OWN_NS).constructor;
          const out = await attempt(async () => {
            const ns = new C('VictimDO', scriptKey(VICTIM_SCRIPT, 'VictimDO'));
            const stub = ns.getByName('seeded-name');
            const r = await (await stub.fetch('https://x/')).json();
            return { leaked: r.secret };
          });
          return out;
        }
        case 'do-forge-direct': {
          // No id op: the id bytes are derived in pure JS and the
          // stub is built straight from them, so only dispatch-layer
          // enforcement can refuse.
          const out = await attempt(async () => {
            const idHex = await doIdHex(
              scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name',
            );
            const C = Object.getPrototypeOf(env.OWN_NS).constructor;
            const IdC = Object.getPrototypeOf(
              env.OWN_NS.getByName('probe').id,
            ).constructor;
            const ns = new C('VictimDO', scriptKey(VICTIM_SCRIPT, 'VictimDO'));
            const stub = ns.get(new IdC('VictimDO', idHex));
            const r = await (await stub.fetch('https://x/')).json();
            return { leaked: r.secret };
          });
          return out;
        }
        case 'wf-own': {
          const inst = await env.OWN_WF.create({ id: 'own0' });
          const status = await inst.status();
          return { id: inst.id, status: status.status ?? 'unknown' };
        }
        case 'wf-forge': {
          const C = Object.getPrototypeOf(env.OWN_WF).constructor;
          const out = await attempt(async () => {
            const forged = new C('nope-not-declared');
            const inst = await forged.create({ id: 'forged0' });
            return { created: inst.id };
          });
          return out;
        }
        case 'wf-xforge': {
          const C = Object.getPrototypeOf(env.OWN_NS).constructor;
          const out = await attempt(async () => {
            const cls = `__Workflow.${VICTIM_SCRIPT}`;
            const ns = new C(cls, scriptKey(VICTIM_SCRIPT, cls));
            const stub = ns.getByName('bf-victim-wf/seed0');
            const status = await stub.__wfStatus();
            return { leaked: status.status ?? 'unknown' };
          });
          return out;
        }
        case 'cron-forge': {
          // The scheduler cell of this primary deployment. No binding
          // names it, but its scope is the guessable `.cron:{script}`.
          const C = Object.getPrototypeOf(env.OWN_NS).constructor;
          const out = await attempt(async () => {
            const ns = new C('.cron', scriptKey('bf-attacker', '.cron'));
            const IdC = Object.getPrototypeOf(
              env.OWN_NS.getByName('probe').id,
            ).constructor;
            const stub = ns.get(new IdC('.cron', 'bf-attacker'));
            const r = await stub.fetch('https://x/');
            return { reached: r.status };
          });
          return out;
        }
        case 'launder-rpc-do': {
          // A forged victim stub passed to the own cell over RPC: the
          // lift refuses the argument, so the cell never grants it.
          const out = await attempt(async () => {
            const idHex = await doIdHex(
              scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name',
            );
            const C = Object.getPrototypeOf(env.OWN_NS).constructor;
            const IdC = Object.getPrototypeOf(
              env.OWN_NS.getByName('probe').id,
            ).constructor;
            const ns = new C('VictimDO', scriptKey(VICTIM_SCRIPT, 'VictimDO'));
            const forged = ns.get(new IdC('VictimDO', idHex));
            const cell = env.OWN_NS.getByName('launder-cell');
            return await cell.useStub(forged);
          });
          return out;
        }
        case 'launder-rpc-d1': {
          const out = await attempt(async () => {
            const idHex = await doIdHex(D1_KEY, 'bf-victim-db');
            const C = Object.getPrototypeOf(env.OWN_NS).constructor;
            const IdC = Object.getPrototypeOf(
              env.OWN_NS.getByName('probe').id,
            ).constructor;
            const ns = new C('__D1Database', D1_KEY);
            const forged = ns.get(
              new IdC('__D1Database', idHex, 'bf-victim-db'),
            );
            const cell = env.OWN_NS.getByName('launder-cell');
            return await cell.queryD1(forged);
          });
          return out;
        }
        case 'launder-rpc-wf': {
          const out = await attempt(async () => {
            const C = Object.getPrototypeOf(env.OWN_NS).constructor;
            const cls = '__Workflow.bf-attacker';
            const ns = new C(cls, scriptKey('bf-attacker', cls));
            const forged = ns.getByName('nope-not-declared/xyz');
            const cell = env.OWN_NS.getByName('launder-cell');
            return await cell.useWf(forged);
          });
          return out;
        }
        case 'launder-rpc-cron': {
          const out = await attempt(async () => {
            const C = Object.getPrototypeOf(env.OWN_NS).constructor;
            const IdC = Object.getPrototypeOf(
              env.OWN_NS.getByName('probe').id,
            ).constructor;
            const ns = new C('.cron', scriptKey('bf-attacker', '.cron'));
            const forged = ns.get(new IdC('.cron', 'bf-attacker'));
            const cell = env.OWN_NS.getByName('launder-cell');
            return await cell.fetchVia(forged);
          });
          return out;
        }
        case 'launder-store-d1':
        case 'launder-store-wf':
        case 'launder-store-do': {
          // The cell forges locally and round-trips through its own
          // storage, with no RPC at all: the stored lift refuses.
          const method = {
            'launder-store-d1': 'launderStoreD1',
            'launder-store-wf': 'launderStoreWf',
            'launder-store-do': 'launderStoreDo',
          }[t];
          const out = await attempt(async () => {
            const cell = env.OWN_NS.getByName('launder-cell');
            return await cell[method]();
          });
          return out;
        }
        case 'stub-pass-svc': {
          // A stub for a declared class handed to the other script over
          // its service binding: the receiver uses it for a class it
          // never declared.
          const stub = env.OWN_NS.getByName('stub-pass');
          await stub.fetch('https://x/?set=stub-pass-secret');
          return await env.SB.useStub(stub);
        }
        case 'stub-pass-rpc': {
          // The same handoff to the own cell over cell RPC.
          const stub = env.OWN_NS.getByName('rpc-pass');
          await stub.fetch('https://x/?set=rpc-pass-secret');
          const cell = env.OWN_NS.getByName('stub-cell');
          return await cell.useStub(stub);
        }
        case 'stub-pass-stored': {
          // A declared-class stub stored and revived, then used.
          const stub = env.OWN_NS.getByName('stored-name');
          await stub.fetch('https://x/?set=stored-secret');
          const cell = env.OWN_NS.getByName('stub-cell');
          return await cell.storeRoundTrip('stored-name');
        }
        case 'stub-pass-xstore': {
          // The cross-script handoff stored and revived by the
          // receiver: the receiver's cell round-trips a stub for a
          // class its own script never declared.
          const stub = env.OWN_NS.getByName('xstore-name');
          await stub.fetch('https://x/?set=xstore-secret');
          return await env.SB.storeViaCell(stub);
        }
        case 'd1-shared': {
          // Two scripts declaring the same database name share it by
          // design: read the victim-seeded row through the own
          // declared binding for that name.
          const r = await env.SHARED_DB.prepare('SELECT v FROM t').all();
          return { value: r.results[0]?.v ?? null };
        }
        case 'storage-own':
        case 'storage-forge-sql':
        case 'storage-forge-do': {
          const sub = t.slice('storage-'.length);
          const stub = env.OWN_NS.getByName('storage-probe');
          const r = await (await stub.fetch(`https://x/sub/${sub}`)).json();
          return r;
        }
        case 'container-reachable': {
          const stub = env.OWN_NS.getByName('storage-probe');
          return await (await stub.fetch('https://x/sub/forge-container')).json();
        }
        case 'r2-own': {
          await env.OWN_R2.put('ownkey', 'own-r2-value');
          const obj = await env.OWN_R2.get('ownkey');
          return { value: obj === null ? null : await obj.text() };
        }
        case 'r2-noforge': {
          const proto = Object.getPrototypeOf(env.OWN_R2);
          return { ctor: proto.constructor.name };
        }
        case 'svc-own': {
          const r = await (await env.SB.fetch('https://victim/ping')).json();
          return r;
        }
        case 'svc-noforge': {
          const proto = Object.getPrototypeOf(env.SB);
          return { ctor: proto.constructor.name };
        }
        case 'smuggle-store-instance': {
          const idHex = await doIdHex(
            scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name',
          );
          const cell = env.OWN_NS.getByName('smuggle-cell');
          return await cell.smuggleStoreInstance(idHex);
        }
        case 'store-instance-ok': {
          const cell = env.OWN_NS.getByName('stub-cell');
          return await cell.storeInstanceOk();
        }
        case 'plant-v1-inert': {
          const idHex = await doIdHex(
            scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name',
          );
          const cell = env.OWN_NS.getByName('plant-cell');
          return await cell.plantV1Inert(idHex);
        }
        case 'store-shapes': {
          const cell = env.OWN_NS.getByName('shape-cell');
          return await cell.storeShapes();
        }
        case 'rpc-shapes': {
          const cell = env.OWN_NS.getByName('rpc-shape-cell');
          const out = {};
          class RpcSmuggle {
            constructor() {
              this.__celld$do = 'VictimDO';
              this.v = 'abc';
              this.n = null;
            }
          }
          out.instance = await attempt(() => cell.echoArg(new RpcSmuggle()));
          const err = new Error('rpc-err');
          err.__celld$do = 'VictimDO';
          err.v = 'abc';
          out.error = await cell.echoArg(err);
          out.plain = await cell.echoArg({
            __celld$do: 'VictimDO', v: 'abc', n: null,
          });
          return out;
        }
        case 'sql-cfkv': {
          const cell = env.OWN_NS.getByName('sql-cell');
          return await cell.sqlCfKv();
        }
        case 'poc-push-inject': {
          const idHex = await doIdHex(
            scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name',
          );
          const cell = env.OWN_NS.getByName('smuggle-cell');
          return await cell.pocPushInject(idHex);
        }
        case 'poc-weakmap-split': {
          const idHex = await doIdHex(
            scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name',
          );
          const cell = env.OWN_NS.getByName('smuggle-cell');
          return await cell.pocWeakmapSplit(idHex);
        }
        case 'poc-plant-flip': {
          const idHex = await doIdHex(
            scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name',
          );
          const cell = env.OWN_NS.getByName('plant-cell');
          return await cell.pocPlantFlip(idHex);
        }
        case 'poc-rpc-tagflip': {
          const idHex = await doIdHex(
            scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name',
          );
          const cell = env.OWN_NS.getByName('rpc-shape-cell');
          const fakeTree = [{ s: null }];
          const forgedTable = [{ p: [0, 's'], c: 'VictimDO', v: idHex, n: null }];
          const fn = () => 1;
          const origSet = Uint8Array.prototype.set;
          let flipped = 0;
          Uint8Array.prototype.set = function (...args) {
            const r = origSet.apply(this, args);
            if (this.length > 0 && this[0] === 1) { this[0] = 3; flipped++; }
            return r;
          };
          let result;
          try {
            result = cell.exploitFlip(fakeTree, forgedTable, fn);
          } finally { Uint8Array.prototype.set = origSet; }
          return { flipped, result: await result };
        }
        case 'poc-scope-mutate': {
          const cell = env.OWN_NS.getByName('sql-cell');
          return await cell.scopeMutate();
        }
        case 'proto-after': {
          const cell = env.OWN_NS.getByName('proto-cell');
          return await cell.protoAfter();
        }
        case 'plant-flip03': {
          const idHex = await doIdHex(
            scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name',
          );
          const cell = env.OWN_NS.getByName('plant-cell');
          return await cell.plantFlip03(idHex);
        }
        case 'svc-proto': {
          const cell = env.OWN_NS.getByName('svc-proto-cell');
          return await cell.svcProto();
        }
        case 'err-subarray': {
          const cell = env.OWN_NS.getByName('rpc-shape-cell');
          const origSubarray = Uint8Array.prototype.subarray;
          Uint8Array.prototype.subarray = function () {
            throw new Error('polluted:subarray');
          };
          let out;
          try {
            out = await attempt(() => cell.throwBoom());
          } finally {
            Uint8Array.prototype.subarray = origSubarray;
          }
          return out;
        }
        case 'plant-hook-absent': {
          return { plantType: typeof globalThis.__celldPlantStoredV1ForTestOnly };
        }
        case 'pollution-roundtrip': {
          const idHex = await doIdHex(
            scriptKey(VICTIM_SCRIPT, 'VictimDO'), 'seeded-name',
          );
          // Seed the pollution cell first (unpolluted), then run the
          // polluted round trip inside the cell.
          const seed = env.OWN_NS.getByName('pollution-cell');
          await seed.fetch('https://x/?set=pollution-secret');
          const cell = env.OWN_NS.getByName('stub-cell');
          const stored = await cell.pollutionRoundTrip(idHex);
          // Legit RPC under pollution (worker side): seed first, then
          // pollute, hand a declared stub to the own cell, restore
          // before awaiting.
          const rpcSeed = env.OWN_NS.getByName('pollution-rpc');
          await rpcSeed.fetch('https://x/?set=pollution-rpc-secret');
          const savedPush = Array.prototype.push;
          const savedGet = WeakMap.prototype.get;
          const savedSet = Uint8Array.prototype.set;
          const savedIsArray = Array.isArray;
          Array.prototype.push = function () { throw new Error('polluted:push'); };
          WeakMap.prototype.get = function () { throw new Error('polluted:weakGet'); };
          Uint8Array.prototype.set = function () { throw new Error('polluted:u8Set'); };
          Array.isArray = () => { throw new Error('polluted:isArray'); };
          let rpcPromise = null;
          let rpcThrew = null;
          try {
            const stub = env.OWN_NS.getByName('pollution-rpc');
            rpcPromise = cell.useStub(stub);
          } catch (error) {
            rpcThrew = String((error && error.message) || error);
          } finally {
            Array.prototype.push = savedPush;
            WeakMap.prototype.get = savedGet;
            Uint8Array.prototype.set = savedSet;
            Array.isArray = savedIsArray;
          }
          let rpc = null;
          if (rpcThrew !== null) {
            rpc = { threw: true, error: rpcThrew };
          } else {
            rpc = { threw: false, ...(await rpcPromise) };
          }
          // Forged RPC under pollution must still fail.
          const forged = await attempt(async () => {
            const C = Object.getPrototypeOf(env.OWN_NS).constructor;
            const IdC = Object.getPrototypeOf(
              env.OWN_NS.getByName('probe').id,
            ).constructor;
            const ns = new C('VictimDO', scriptKey(VICTIM_SCRIPT, 'VictimDO'));
            const victim = ns.get(new IdC('VictimDO', idHex));
            return await cell.useStub(victim);
          });
          return { stored, rpc, forged };
        }
        case 'ws-id-shape': {
          const pair = new WebSocketPair();
          return { id: pair[0]._id };
        }
        case 'ws-bind-forge': {
          const scope = `AttackerDO:${await doIdHex(
            scriptKey('bf-attacker', 'AttackerDO'), 'holder-a',
          )}`;
          const targets = [
            { id: 1, scope },
            { id: 2 ** 53 - 1, scope },
            { id: 1, scope: 'VictimDO:deadbeef' },
          ];
          const attempts = [];
          for (const target of targets) {
            attempts.push(await attempt(async () => {
              const ws = new WebSocket(undefined, [], target);
              ws.accept();
              return { bound: true };
            }));
          }
          return { attempts };
        }
        case 'ws-holder-setup': {
          return await subFetch(q.get('cell') || 'holder-a', 'ws-accept-held');
        }
        case 'ws-list': {
          return await subFetch(q.get('cell') || 'holder-a', 'ws-list');
        }
        case 'ws-attach-read': {
          return await subFetch(q.get('cell') || 'holder-e', 'ws-attach-read');
        }
        case 'ws-accept-known': {
          const r = await env.OWN_NS.getByName(q.get('cell') || 'holder-b').fetch(
            `https://x/sub/ws-accept-known?id=${q.get('id')}`,
          );
          return await r.json();
        }
        case 'ws-accept-spray': {
          const r = await env.OWN_NS.getByName(q.get('cell') || 'holder-d').fetch(
            `https://x/sub/ws-accept-spray?n=${q.get('n') || '256'}`,
          );
          return await r.json();
        }
        case 'ws-attach-known': {
          const r = await env.OWN_NS.getByName(q.get('cell') || 'holder-f').fetch(
            `https://x/sub/ws-attach-known?id=${q.get('id')}`,
          );
          return await r.json();
        }
        case 'ws-autoresponse-forge': {
          const r = await env.OWN_NS.getByName(q.get('cell') || 'holder-h').fetch(
            `https://x/sub/ws-autoresponse-forge?victim=${q.get('victim') || 'holder-g'}`,
          );
          return await r.json();
        }
        case 'ws-autoresponse-read': {
          return await subFetch(q.get('cell') || 'holder-g', 'ws-autoresponse-read');
        }
        case 'ws-kept-end': {
          // The legitimate join: keep the client end of a subrequest upgrade,
          // exchange one message with the cell, and report the echo.
          const stub = env.OWN_NS.getByName('kept-end');
          const res = await stub.fetch(
            'https://x/sub/ws-accept', { headers: __wsUpgradeHeaders },
          );
          if (res.status !== 101 || !res.webSocket)
            throw new Error(`no upgrade: ${res.status}`);
          const ws = res.webSocket;
          ws.accept();
          ws.send('kept-hello');
          const echo = await new Promise((resolve, reject) => {
            ws.addEventListener('message', (ev) => resolve(ev.data), { once: true });
            ws.addEventListener(
              'close', () => reject(new Error('closed before echo')), { once: true },
            );
            ws.addEventListener(
              'error',
              (ev) => reject(new Error(`socket error: ${ev.message || 'unknown'}`)),
              { once: true },
            );
          });
          return { echo };
        }
        case 'ws-spray': {
          for (let i = 1; i <= 512; i++) new WebSocket(i).send('pwned');
          return { sprayed: 512 };
        }
        case 'ws-spray-close': {
          for (let i = 1; i <= 512; i++) new WebSocket(i).close(4000, 'sprayed');
          return { sprayed: 512 };
        }
        case 'tcp-probe': {
          return { probe: 'tcp-ok' };
        }
        case 'tcp-own': {
          const { connect } = await import('cloudflare:sockets');
          const target = `${reqUrl.hostname}:${reqUrl.port || '80'}`;
          const sock = connect(target);
          await sock.opened;
          const writer = sock.writable.getWriter();
          await writer.write(te.encode(
            `GET /tcp-probe HTTP/1.1\r\nHost: ${target}\r\n\r\n`,
          ));
          writer.releaseLock();
          const reader = sock.readable.getReader();
          let text = '';
          for (;;) {
            const { value, done } = await reader.read();
            if (value) text += new TextDecoder().decode(value);
            if (done || text.includes('tcp-ok')) break;
          }
          reader.releaseLock();
          await sock.close();
          return { reachable: text.includes('tcp-ok') };
        }
        case 'tcp-hold': {
          // The theft victim: connect back to this node, expose our socket
          // id for the known-id probe, send one request, then hold the
          // request — and the connection with its kernel buffers — forever.
          const { connect } = await import('cloudflare:sockets');
          const target = `${reqUrl.hostname}:${reqUrl.port || '80'}`;
          const sock = connect(target);
          await sock.opened;
          const stub = env.OWN_NS.getByName('tcp-coords');
          await stub.fetch(`https://x/sub/ws-tcp-note-id?id=${sock._id}`);
          const writer = sock.writable.getWriter();
          await writer.write(te.encode(
            `GET /tcp-probe HTTP/1.1\r\nHost: ${target}\r\n\r\n`,
          ));
          writer.releaseLock();
          await new Promise(() => {});
          return { unreachable: true };
        }
        case 'tcp-noted': {
          return await subFetch('tcp-coords', 'ws-tcp-read-id');
        }
        case 'tcp-steal-known': {
          const { connect } = await import('cloudflare:sockets');
          const target = `${reqUrl.hostname}:${reqUrl.port || '80'}`;
          const { id } = await subFetch('tcp-coords', 'ws-tcp-read-id');
          const sock = connect(target);
          await sock.opened;
          sock._id = Number(id);
          return await attempt(async () => {
            const reader = sock.readable.getReader();
            const { value } = await reader.read();
            reader.releaseLock();
            const text = new TextDecoder().decode(value || new Uint8Array());
            return {
              bytes: (value && value.byteLength) || 0,
              head: text.slice(0, 60),
            };
          });
        }
        case 'tcp-spray': {
          const { connect } = await import('cloudflare:sockets');
          const target = `${reqUrl.hostname}:${reqUrl.port || '80'}`;
          // One pending read per guessed id; exactly the victim's resolves
          // with bytes. Rejections (failed opens, closed sockets) only count:
          // the probe rejects solely when every id refused, and resolves with
          // the first bytes any id yields. No timers; the victim's reply is
          // the rendezvous. Thirty-two guesses cover the sequential ids a
          // fresh node hands out without bursting a hundred connections.
          const reads = [];
          for (let i = 0; i < 32; i++) {
            const s = connect(target);
            const guess = i + 1;
            // A failed open ends its stream without bytes; that is a refusal
            // like any other, not a result. The id is reassigned after the
            // open settles because the open resolution itself sets `_id`.
            reads.push(s.opened.then(async () => {
              s._id = guess;
              const reader = s.readable.getReader();
              const outcome = await reader.read();
              reader.releaseLock();
              if (outcome.done || !outcome.value || outcome.value.byteLength === 0)
                throw new Error('socket is closed');
              return outcome;
            }, () => { throw new Error('socket is closed'); }));
          }
          return await attempt(async () => {
            const { value } = await new Promise((resolve, reject) => {
              let rejected = 0;
              for (const read of reads) {
                read.then(resolve, () => {
                  if (++rejected === reads.length)
                    reject(new Error('socket is closed'));
                });
              }
            });
            const text = new TextDecoder().decode(value || new Uint8Array());
            return {
              bytes: (value && value.byteLength) || 0,
              head: text.slice(0, 60),
            };
          });
        }
        default:
          throw new Error(`no route ${t}`);
      }
    };
    try {
      return J({ ok: true, ...(await run()) });
    } catch (error) {
      return J({
        ok: false,
        name: error?.name ?? 'unknown',
        error: String((error && error.message) || error),
      });
    }
  },
};
