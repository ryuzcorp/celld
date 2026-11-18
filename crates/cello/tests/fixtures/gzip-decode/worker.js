// Modified by ryuzcorp (ryuzcorp/celld): gzip decode fixture (upstream #238). Apache-2.0.
const ORIGIN = "http://127.0.0.1:__GZIP_PORT__";

const probe = async (init, subpath) => {
  const target = ORIGIN + subpath;
  const res = await fetch(target, init);
  const buf = new Uint8Array(await res.arrayBuffer());
  const out = {
    status: res.status,
    contentEncoding: res.headers.get("content-encoding"),
    bytes: buf.length,
    first: buf.length >= 2 ? buf[0] * 256 + buf[1] : -1,
  };
  try {
    out.json = await (await fetch(target, init)).json();
  } catch (e) {
    out.jsonError = e.name;
  }
  try {
    out.text = await (await fetch(target, init)).text();
  } catch (e) {
    out.textError = e.name;
  }
  return out;
};

export default {
  async fetch(req) {
    const p = new URL(req.url).pathname;
    if (p === "/health") return new Response("ok");
    if (p === "/explicit") {
      return Response.json(
        await probe({ headers: { "accept-encoding": "gzip" } }, "/gzip"));
    }
    if (p === "/deflate") {
      return Response.json(await probe(undefined, "/deflate"));
    }
    return Response.json(await probe(undefined, "/gzip"));
  },
};
