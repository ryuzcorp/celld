// Modified by Ryuz Corp (Cello): process.env probe fixture (upstream #296). Apache-2.0.
import nodeProcess from "node:process";
import { env as namedEnv } from "node:process";

const probe = (read) => {
  const value = read();
  return {
    type: typeof value,
    truthy: Boolean(value),
    value: typeof value === "string" ? value : undefined,
  };
};

// A dynamically loaded child: its strings arrive through the loader env,
// not through deployment vars, and must still reach `process.env` under
// `nodejs_compat` (upstream #296). `CELLO_NODE` rides along to prove the
// host-owned name never crosses into `process.env`.
const CHILD = `const MODULE_SCOPE = (() => {
  const snapshot = (name) => {
    try {
      const v = globalThis.process?.env?.[name];
      return { type: typeof v, value: typeof v === "string" ? v : undefined };
    } catch (e) {
      return { type: "threw", value: String((e && e.message) || e) };
    }
  };
  // Runs while the module evaluates: loader strings must already sit in
  // process.env here, not only once the handler runs.
  return { feature: snapshot("FEATURE"), node: snapshot("CELLO_NODE") };
})();
export default {
  fetch(request, env) {
    const read = (f) => {
      try {
        const v = f();
        return { type: typeof v, value: typeof v === "string" ? v : undefined };
      } catch (e) {
        return { type: "threw", value: String((e && e.message) || e) };
      }
    };
    return Response.json({
      handlerEnv: read(() => env.FEATURE),
      procEnv: read(() => globalThis.process.env.FEATURE),
      moduleScopeFeature: MODULE_SCOPE.feature,
      handlerNode: read(() => env.CELLO_NODE),
      procNode: read(() => globalThis.process.env.CELLO_NODE),
      moduleScopeNode: MODULE_SCOPE.node,
    });
  },
};`;

export default {
  async fetch(request, env) {
    if (new URL(request.url).pathname === "/loader") {
      const child = env.LOADER.get("child", () => ({
        compatibilityDate: "2026-01-01",
        compatibilityFlags: ["nodejs_compat"],
        mainModule: "child.js",
        modules: { "child.js": CHILD },
        env: { FEATURE: "1", CELLO_NODE: "spoofed" },
      }));
      return child.getEntrypoint().fetch(request);
    }
    return Response.json({
      vars: probe(() => env.FROM_VARS),
      devVars: probe(() => env.FROM_DEV_VARS),
      globalVars: probe(() => globalThis.process.env.FROM_VARS),
      globalDevVars: probe(() => globalThis.process.env.FROM_DEV_VARS),
      globalKeys: Object.keys(globalThis.process.env),
      importedVars: probe(() => nodeProcess.env.FROM_VARS),
      importedUnset: probe(() => nodeProcess.env.UNSET_NAME),
      importedString: String(nodeProcess.env.FROM_VARS),
      namedEnvSame: namedEnv === globalThis.process.env,
      sameProcess: nodeProcess === globalThis.process,
    });
  },
};
