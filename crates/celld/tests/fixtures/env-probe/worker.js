// Modified by ryuzcorp (ryuzcorp/celld): process.env probe fixture (upstream #296). Apache-2.0.
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

export default {
  fetch(request, env) {
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
