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
    // A key past the engine's spread-argument limit still exports: the JWK
    // `k` builds in chunks, so no `RangeError` escapes (upstream #295).
    // (`generateKey` randomness caps at 64 KiB, so the big key arrives via
    // `importKey`, which takes caller bytes with no cap.)
    out["large HMAC export"] = await step(async () => {
      const key = await s.importKey("raw", new Uint8Array(150 * 1024).fill(7),
        { name: "HMAC", hash: "SHA-256" }, true, ["sign"]);
      const jwk = await s.exportKey("jwk", key);
      return "k length " + jwk.k.length;
    });
    // A JWK `alg` that names another algorithm is not importable as this
    // one: an `A128KW` JWK is not an AES-GCM key that can encrypt. A
    // matching `alg`, or none at all, still imports (upstream #295).
    out["jwk alg mismatch"] = await step(async () => {
      const gcm = await s.generateKey({ name: "AES-GCM", length: 128 }, true, ["encrypt"]);
      const jwk = await s.exportKey("jwk", gcm);
      jwk.alg = "A128KW";
      await s.importKey("jwk", jwk, { name: "AES-GCM" }, true, ["encrypt"]);
      return "no error";
    });
    out["jwk alg match"] = await step(async () => {
      const gcm = await s.generateKey({ name: "AES-GCM", length: 128 }, true, ["encrypt"]);
      const jwk = await s.exportKey("jwk", gcm);
      await s.importKey("jwk", jwk, { name: "AES-GCM" }, true, ["encrypt"]);
      return "imported with " + jwk.alg;
    });
    out["jwk alg absent"] = await step(async () => {
      const gcm = await s.generateKey({ name: "AES-GCM", length: 128 }, true, ["encrypt"]);
      const jwk = await s.exportKey("jwk", gcm);
      delete jwk.alg;
      await s.importKey("jwk", jwk, { name: "AES-GCM" }, true, ["encrypt"]);
      return "imported without alg";
    });
    out["hmac jwk alg mismatch"] = await step(async () => {
      const hmac = await s.importKey("raw", new Uint8Array(32).fill(1),
        { name: "HMAC", hash: "SHA-256" }, true, ["sign"]);
      const jwk = await s.exportKey("jwk", hmac);
      jwk.alg = "HS384";
      await s.importKey("jwk", jwk, { name: "HMAC", hash: "SHA-256" }, true, ["sign"]);
      return "no error";
    });
    // `ext: false` marks a non-extractable key: importing it as
    // extractable must fail, while a non-extractable import stays valid.
    out["jwk ext conflict"] = await step(async () => {
      const gcm = await s.generateKey({ name: "AES-GCM", length: 128 }, true, ["encrypt"]);
      const jwk = await s.exportKey("jwk", gcm);
      jwk.ext = false;
      await s.importKey("jwk", jwk, { name: "AES-GCM" }, true, ["encrypt"]);
      return "no error";
    });
    out["jwk ext compatible"] = await step(async () => {
      const gcm = await s.generateKey({ name: "AES-GCM", length: 128 }, true, ["encrypt"]);
      const jwk = await s.exportKey("jwk", gcm);
      jwk.ext = false;
      const key = await s.importKey("jwk", jwk, { name: "AES-GCM" }, false, ["encrypt"]);
      return "imported extractable=" + key.extractable;
    });
    // AES-KW wraps with an AES-KW key, not any secret key: an AES-GCM key
    // with `wrapKey` usage still cannot stand in (upstream #295).
    out["wrap with GCM key"] = await step(async () => {
      const gcm = await s.importKey("raw", hex(vectors["4.1"].kek),
        { name: "AES-GCM" }, false, ["wrapKey"]);
      const key = await s.importKey("raw", hex(vectors["4.1"].key),
        { name: "AES-GCM" }, true, ["encrypt"]);
      await s.wrapKey("raw", key, gcm, { name: "AES-KW" });
      return "no error";
    });
    out["unwrap with GCM key"] = await step(async () => {
      const gcm = await s.importKey("raw", hex(vectors["4.1"].kek),
        { name: "AES-GCM" }, false, ["unwrapKey"]);
      await s.unwrapKey("raw", hex(vectors["4.1"].wrapped), gcm, { name: "AES-KW" },
        { name: "AES-GCM" }, true, ["decrypt"]);
      return "no error";
    });
    // AES JWK imports need a real AES length: 8 bits (or any non-16/24/32
    // byte length) is a DataError, while a short HMAC JWK still imports.
    out["jwk short aes key"] = await step(async () => {
      await s.importKey("jwk", { kty: "oct", k: "AA" },
        { name: "AES-KW" }, false, ["wrapKey"]);
      return "no error";
    });
    out["jwk bad aes length"] = await step(async () => {
      const gcm = await s.generateKey({ name: "AES-GCM", length: 128 }, true, ["encrypt"]);
      const jwk = await s.exportKey("jwk", gcm);
      jwk.k = "AQIDBAU";
      delete jwk.alg;
      await s.importKey("jwk", jwk, { name: "AES-GCM" }, true, ["encrypt"]);
      return "no error";
    });
    out["jwk short hmac key"] = await step(async () => {
      const key = await s.importKey("jwk", { kty: "oct", k: "AA" },
        { name: "HMAC", hash: "SHA-256" }, true, ["sign"]);
      return "imported " + key.algorithm.length + " bits";
    });
    // SHA-1 HMAC has the JOSE name HS1, for import and export alike.
    out["hmac sha1 jwk alg"] = await step(async () => {
      const hmac = await s.importKey("raw", new Uint8Array(20).fill(2),
        { name: "HMAC", hash: "SHA-1" }, true, ["sign"]);
      const jwk = await s.exportKey("jwk", hmac);
      const back = await s.importKey("jwk", jwk,
        { name: "HMAC", hash: "SHA-1" }, true, ["sign"]);
      return jwk.alg + " reimported=" + (back.algorithm.hash.name === "SHA-1");
    });
    // A present JWK `use` must name encryption for AES keys: `sig` is a
    // DataError, while `enc` or an absent `use` still imports.
    out["jwk aes use mismatch"] = await step(async () => {
      const gcm = await s.generateKey({ name: "AES-GCM", length: 128 }, true, ["encrypt"]);
      const jwk = await s.exportKey("jwk", gcm);
      jwk.use = "sig";
      await s.importKey("jwk", jwk, { name: "AES-GCM" }, true, ["encrypt"]);
      return "no error";
    });
    out["jwk aes use match"] = await step(async () => {
      const gcm = await s.generateKey({ name: "AES-GCM", length: 128 }, true, ["encrypt"]);
      const jwk = await s.exportKey("jwk", gcm);
      jwk.use = "enc";
      await s.importKey("jwk", jwk, { name: "AES-GCM" }, true, ["encrypt"]);
      return "imported with enc";
    });
    // RFC 3394 needs two plaintext semiblocks: wrapping one fails loudly
    // instead of minting a non-standard 16-byte blob (upstream #295).
    out["wrap 8-byte data"] = await step(async () => {
      const tiny = await s.importKey("raw", new Uint8Array(8).fill(3),
        { name: "HMAC", hash: "SHA-256" }, true, ["sign"]);
      const kek = await s.importKey("raw", hex(vectors["4.1"].kek),
        { name: "AES-KW" }, false, ["wrapKey"]);
      const wrapped = await s.wrapKey("raw", tiny, kek, { name: "AES-KW" });
      return "wrapped " + wrapped.byteLength + " bytes";
    });
    return Response.json(out);
  },
};
