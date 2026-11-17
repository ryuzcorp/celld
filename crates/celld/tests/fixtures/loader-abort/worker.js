// Modified by ryuzcorp (ryuzcorp/celld): loader abort fixture (upstream #281). Apache-2.0.
const PROVIDER = "http://127.0.0.1:__PROVIDER_PORT__";

const CHILD = `export default {
  fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === "/child") {
      return fetch("${PROVIDER}/hold", { signal: req.signal })
        .then(() => new Response("child-saw-provider-answer"))
        .catch((e) => new Response("child-saw-abort:" + (e?.name || e)));
    }
    return new Response("nope", { status: 404 });
  }
};`;

const loadChild = (env) => env.LOADER.get("c", () => ({
  compatibilityDate: "2026-01-01",
  mainModule: "m.js",
  modules: { "m.js": CHILD },
}));

export default {
  async fetch(req, env) {
    const p = new URL(req.url).pathname;
    if (p === "/health") return new Response("ok");
    if (p === "/direct") {
      const c = new AbortController();
      setTimeout(() => c.abort(), 300);
      try {
        const res = await fetch(PROVIDER + "/hold", { signal: c.signal });
        return new Response("direct-answered:" + res.status);
      } catch (e) {
        return new Response("direct-saw-abort:" + (e?.name || e));
      }
    }
    if (p === "/loaded") {
      const c = new AbortController();
      setTimeout(() => c.abort(), 300);
      const url = new URL(req.url);
      try {
        const w = loadChild(env);
        const res = await w.getEntrypoint().fetch(url.origin + "/child", { signal: c.signal });
        return new Response("loaded-answered:" + (await res.text()));
      } catch (e) {
        return new Response("loaded-saw-abort:" + (e?.name || e));
      }
    }
    return new Response("nope", { status: 404 });
  },
};
