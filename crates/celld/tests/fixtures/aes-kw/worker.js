// Modified by ryuzcorp (ryuzcorp/celld): AES-KW fixture (upstream #295). Apache-2.0.
const hex = (s) => Uint8Array.from(s.match(/../g), (b) => parseInt(b, 16));
const toHex = (buf) =>
  [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");

// RFC 3394 section 4.1 (128-bit KEK, 128-bit key data) and 4.6 (256-bit
// KEK, 256-bit key data).
const vectors = {
  "4.1": {
    kek: "000102030405060708090a0b0c0d0e0f",
    key: "00112233445566778899aabbccddeeff",
    wrapped: "1fa68b0a8112b447aef34bd8fb5a7b829d3e862371d2cfe5",
  },
  "4.6": {
    kek: "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    key: "00112233445566778899aabbccddeeff000102030405060708090a0b0c0d0e0f",
    wrapped: "28c9f404c4b810f4cbccb35cfb87f8263f5786e2d80ed326cbc7f0e71a99f43bfb988b9b7a02dd21",
  },
};

async function step(fn) {
  try {
    return await fn();
  } catch (e) {
    return e.name + ": " + e.message;
  }
}

export default {
  async fetch() {
    const s = crypto.subtle;
    const out = {};
    for (const [name, v] of Object.entries(vectors)) {
      out["wrap " + name] = await step(async () => {
        const kek = await s.importKey("raw", hex(v.kek), { name: "AES-KW" }, false, ["wrapKey"]);
        const key = await s.importKey("raw", hex(v.key), { name: "AES-GCM" }, true, ["encrypt"]);
        return toHex(await s.wrapKey("raw", key, kek, { name: "AES-KW" }));
      });
      out["unwrap " + name] = await step(async () => {
        const kek = await s.importKey("raw", hex(v.kek), { name: "AES-KW" }, false, ["unwrapKey"]);
        const key = await s.unwrapKey("raw", hex(v.wrapped), kek, { name: "AES-KW" },
          { name: "AES-GCM" }, true, ["decrypt"]);
        return toHex(await s.exportKey("raw", key));
      });
    }
    // The round trip @cloudflare/workers-oauth-provider 0.8.1 does for
    // every grant.
    out["oauth-provider round trip"] = await step(async () => {
      const hmacKey = await s.importKey("raw", new Uint8Array(32).fill(7),
        { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
      const derived = await s.sign("HMAC", hmacKey, new TextEncoder().encode("token"));
      const kek = await s.importKey("raw", derived, { name: "AES-KW" }, false, ["wrapKey", "unwrapKey"]);
      const grantKey = await s.generateKey({ name: "AES-GCM", length: 256 }, true, ["encrypt", "decrypt"]);
      const wrapped = await s.wrapKey("raw", grantKey, kek, { name: "AES-KW" });
      const unwrapped = await s.unwrapKey("raw", wrapped, kek, { name: "AES-KW" },
        { name: "AES-GCM" }, true, ["encrypt", "decrypt"]);
      const same = toHex(await s.exportKey("raw", unwrapped)) === toHex(await s.exportKey("raw", grantKey));
      return "wrapped " + wrapped.byteLength + " bytes, unwrapped key matches: " + same;
    });
    out["generateKey AES-KW"] = await step(async () => {
      const k = await s.generateKey({ name: "AES-KW", length: 256 }, true, ["wrapKey", "unwrapKey"]);
      return JSON.stringify(await s.exportKey("jwk", k), ["kty", "alg", "key_ops"]);
    });
    out["unwrap with wrong key"] = await step(async () => {
      const kek = await s.importKey("raw", new Uint8Array(16).fill(9),
        { name: "AES-KW" }, false, ["unwrapKey"]);
      await s.unwrapKey("raw", hex(vectors["4.1"].wrapped), kek, { name: "AES-KW" },
        { name: "AES-GCM" }, true, ["decrypt"]);
      return "no error";
    });
    return Response.json(out);
  },
};
