// Staged-invoke fixture: the live revision of the helper script, reached
// from the app over a service binding and sharing its D1 database.
const VERSION = "helper-v1";

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
