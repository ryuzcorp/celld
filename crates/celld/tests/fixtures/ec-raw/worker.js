// Modified by ryuzcorp (ryuzcorp/celld): EC raw fixture (upstream #271). Apache-2.0.
const first = (e) => String(e).split("\n")[0];
async function probe(name) {
  const algo = { name, namedCurve: "P-256" };
  const usages = name === "ECDH" ? ["deriveBits"] : ["sign", "verify"];
  const pair = await crypto.subtle.generateKey(algo, true, usages);
  const raw = new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey));
  const row = {
    algorithm: name + " P-256",
    exportRawLength: raw.length,
    exportRawFirstByte: raw[0],
  };
  // The 65-byte uncompressed point, rebuilt from the JWK export.
  const jwk = await crypto.subtle.exportKey("jwk", pair.publicKey);
  const b = (s) => Uint8Array.from(
    atob(s.replace(/-/g, "+").replace(/_/g, "/")), (c) => c.charCodeAt(0));
  const point = new Uint8Array([4, ...b(jwk.x), ...b(jwk.y)]);
  try {
    await crypto.subtle.importKey(
      "raw", point, algo, true, name === "ECDH" ? [] : ["verify"]);
    row.importRaw = "ok";
  } catch (e) {
    row.importRaw = first(e);
  }
  return row;
}
export default {
  fetch: async () => Response.json([await probe("ECDH"), await probe("ECDSA")]),
};
