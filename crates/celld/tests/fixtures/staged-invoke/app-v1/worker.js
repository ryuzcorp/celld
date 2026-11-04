// Staged-invoke fixture: the live revision of the app script. The tests
// stage app-v2 beside it and assert the operator path answers the new
// code while the public path still answers this one.
const VERSION = "app-v1";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS marks (tag TEXT PRIMARY KEY, writer TEXT);
`;

async function ensureDb(db) {
  await db.exec(SCHEMA);
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (url.pathname === "/version") {
      return new Response(VERSION);
    }
    if (url.pathname === "/migrate" && request.method === "POST") {
      await ensureDb(env.DB);
      await env.DB.prepare(
        "INSERT OR REPLACE INTO marks (tag, writer) VALUES ('migrated', ?)",
      )
        .bind(VERSION)
        .run();
      return Response.json({ migrated: VERSION });
    }
    if (url.pathname === "/migrate-late" && request.method === "POST") {
      ctx.waitUntil(
        (async () => {
          await env.HELPER.fetch("http://helper/write-late", {
            method: "POST",
          });
        })(),
      );
      return new Response(`late-accepted-${VERSION}`);
    }
    if (url.pathname === "/hang") {
      // Never settles: the invoke must fail at its settle deadline
      // instead of pinning the staged generation forever.
      ctx.waitUntil(new Promise(() => {}));
      return new Response(`hung-${VERSION}`);
    }
    if (url.pathname === "/note" && request.method === "POST") {
      const note = await request.json();
      await ensureDb(env.DB);
      await env.DB.prepare(
        "INSERT OR REPLACE INTO marks (tag, writer) VALUES (?, ?)",
      )
        .bind(note.tag, note.writer)
        .run();
      return Response.json({ noted: note.tag });
    }
    if (url.pathname === "/read") {
      await ensureDb(env.DB);
      const rows = await env.DB.prepare(
        "SELECT tag, writer FROM marks ORDER BY tag",
      ).all();
      return Response.json({ reader: VERSION, rows: rows.results });
    }
    if (url.pathname === "/via-helper") {
      const res = await env.HELPER.fetch("http://helper/version");
      return new Response(`via-${await res.text()}`);
    }
    return new Response("no such route", { status: 404 });
  },
};
