// Staged-invoke fixture: the staged revision of the helper script. The
// tests pin it beside the staged app and assert the staged service
// graph answers, including from `waitUntil` after the reply.
const VERSION = "helper-v2";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === "/version") {
      return new Response(VERSION);
    }
    if (url.pathname === "/write-late" && request.method === "POST") {
      await env.DB.exec(
        "CREATE TABLE IF NOT EXISTS marks (tag TEXT PRIMARY KEY, writer TEXT);",
      );
      await env.DB.prepare(
        "INSERT OR REPLACE INTO marks (tag, writer) VALUES ('late', ?)",
      )
        .bind(VERSION)
        .run();
      return new Response(`late-${VERSION}`);
    }
    return new Response("no such route", { status: 404 });
  },
};
