// Modified by ryuzcorp (ryuzcorp/celld): runtime-isolate host fixture; Queue pollution-gate probes. Apache-2.0.
// The first script in the fleet: its module top level overrides the shared
// primordials that the runtime cells' gates and data paths use, then answers
// /health and forwards every /v/* probe to the second script over a service
// binding. Each override is surgical — it misbehaves only for the shapes the
// runtime cells use — so this script's own fetch path keeps working and any
// probe failure pins the shared cells, not the forwarding.
const MARK = "__celld_probe_marker";

// Captured before overriding, and every override delegates through the
// captured originals only, so the overrides never re-enter each other.
const __origWeakSetHas = WeakSet.prototype.has;
const __origSetHas = Set.prototype.has;
const __origMapGet = Map.prototype.get;
const __origArrayMap = Array.prototype.map;
const __origApply = Function.prototype.apply;
const __origCall = Function.prototype.call;
const __origReflectApply = Reflect.apply;
const __origParse = JSON.parse;
const __origStringify = JSON.stringify;
const __origStartsWith = String.prototype.startsWith;

const INTERNAL_METHODS = [
  "__d1Export", "__d1Migrate", "__d1Query", "__d1Batch", "__d1Exec",
  "__kvGet", "__kvPut", "__kvDelete", "__kvList", "__kvSweep", "__kvMetrics",
  "__queueSend", "__queueMetrics", "__queueDlqAcceptBatch",
  "_purge", "_peek", "_setPaused", "_redrive",
];
const isInternal = (v) => {
  for (let i = 0; i < INTERNAL_METHODS.length; i++) {
    if (v === INTERNAL_METHODS[i]) return true;
  }
  return false;
};
// `=== true`: a Proxy's get trap answers an object for an unknown key, never
// the boolean, so stub-shaped values can never match by accident.
const hasMark = (v) =>
  v !== null && (typeof v === "object" || typeof v === "function") &&
  v[MARK] === true;
const argHasMark = (argArray) => {
  if (argArray === null || argArray === undefined) return false;
  const len = argArray.length;
  if (typeof len !== "number") return false;
  for (let i = 0; i < len; i++) {
    const a = argArray[i];
    if (hasMark(a)) return true;
    if (Array.isArray(a)) {
      for (let j = 0; j < a.length; j++) {
        if (hasMark(a[j])) return true;
      }
    }
  }
  return false;
};

// Operator-ingress gate: every server-side Request reads as operator.
WeakSet.prototype.has = function (v) {
  if (v instanceof Request) return true;
  return __origReflectApply(__origWeakSetHas, this, [v]);
};
// RPC allowlist: every internal method reads as allowlisted. The unknown
// content type reads as known too: the queue's cell-side validation checks
// membership the same way, and a polluted `Set.prototype.has` answering
// true for it smuggles an "evil-type" send past the check when the queue
// cell shares the polluting script's pool.
Set.prototype.has = function (v) {
  if (typeof v === "string" && (isInternal(v) || v === "evil-type")) return true;
  return __origReflectApply(__origSetHas, this, [v]);
};
// Stub-op input gate for queue cells.
Map.prototype.get = function (k) {
  if (typeof k === "string" && __origReflectApply(__origStartsWith, k, ["__Queue:"]))
    throw new Error("probe map poison");
  return __origReflectApply(__origMapGet, this, [k]);
};
// Data paths that map over request rows drop a marked batch.
Array.prototype.map = function (fn, thisArg) {
  if (Array.isArray(this)) {
    for (let i = 0; i < this.length; i++) {
      if (hasMark(this[i])) return [];
    }
  }
  return __origReflectApply(__origArrayMap, this, [fn, thisArg]);
};
// Post-resolution redirection: a marked RPC call runs the export instead.
Function.prototype.apply = function (thisArg, argArray) {
  if (argHasMark(argArray)) return thisArg.__d1Export({});
  const args = argArray === null || argArray === undefined ? [] : argArray;
  return __origReflectApply(this, thisArg, args);
};
Function.prototype.call = function (thisArg, ...args) {
  if (argHasMark(args)) return thisArg.__d1Export({});
  return __origReflectApply(__origCall, this, [thisArg, ...args]);
};
Reflect.apply = function (fn, thisArg, argArray) {
  if (argHasMark(argArray)) return thisArg.__d1Export({});
  return __origReflectApply(fn, thisArg, argArray);
};
// Request bodies carrying a top-level marker fail to parse.
JSON.parse = function (text, reviver) {
  const parsed = __origParse(text, reviver);
  if (hasMark(parsed)) throw new SyntaxError("probe parse poison");
  return parsed;
};
// Queue dispatch envelopes fail to serialize.
JSON.stringify = function (v, replacer, space) {
  if (v !== null && typeof v === "object" && Array.isArray(v.leases))
    throw new TypeError("probe stringify poison");
  return __origStringify(v, replacer, space);
};
// The D1 allowlist's scope check never matches, leaving open dispatch.
String.prototype.startsWith = function (arg, pos) {
  if (arg === "__D1Database:") return false;
  return pos === undefined
    ? __origReflectApply(__origStartsWith, this, [arg])
    : __origReflectApply(__origStartsWith, this, [arg, pos]);
};

const J = (o, st = 200) => Response.json(o, { status: st });

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);
    const t = url.pathname.slice(1);
    if (t === "health") return J({ ok: true });
    if (t === "v" || t.startsWith("v/")) {
      const sub = t === "v" ? "/" : "/" + t.slice(2);
      const target = new URL(sub, "http://victim");
      target.search = url.search;
      return env.SB.fetch(target.toString());
    }
    return J({ ok: false, error: `unknown ${t}` }, 404);
  },
};
