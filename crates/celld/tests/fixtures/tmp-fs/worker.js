// Modified by ryuzcorp (ryuzcorp/celld): writable /tmp + node:url fixture (upstream #280). Apache-2.0.
import fs from "node:fs/promises";
import {
  accessSync,
  appendFileSync,
  constants,
  createReadStream,
  createWriteStream,
  mkdirSync,
  readFileSync,
  statSync,
  writeFile as writeFileCb,
  writeFileSync,
} from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";

const codeOf = (fn) => {
  try {
    fn();
    return "ok";
  } catch (error) {
    return error?.code ?? `threw:${error?.message}`;
  }
};

const tmpRoot = async () => {
  await fs.writeFile("/tmp/a.txt", "x");
  writeFileSync("/tmp/c.txt", "sync");
  await new Promise((resolve, reject) => {
    writeFileCb("/tmp/d.txt", "cb", (error) => error ? reject(error) : resolve());
  });
  const w = createWriteStream("/tmp/b.txt");
  await new Promise((resolve, reject) => {
    w.on("error", reject);
    w.on("finish", resolve);
    w.write("hello ");
    w.end("world");
  });
  const readBack = await new Promise((resolve, reject) => {
    let data = "";
    const r = createReadStream("/tmp/b.txt");
    r.on("data", (chunk) => { data += chunk.toString(); });
    r.on("end", () => resolve(data));
    r.on("error", reject);
  });
  return {
    a: await fs.readFile("/tmp/a.txt", "utf8"),
    b: readBack,
    c: readFileSync("/tmp/c.txt", "utf8"),
    d: readFileSync("/tmp/d.txt", "utf8"),
    url: pathToFileURL("/tmp/a.txt").href,
    back: fileURLToPath(pathToFileURL("/tmp/a.txt")),
  };
};

const capWrite = (url) => {
  const bytes = Number(url.searchParams.get("bytes") ?? "0");
  const code = codeOf(() => writeFileSync("/tmp/cap.txt", "x".repeat(bytes)));
  return { code };
};

const capOverwrite = () => {
  // Under a 256-byte cap: two 48-byte overwrites each fit because the
  // replaced bytes are freed, while a 200-byte append on top of 48 fails.
  const first = codeOf(() => writeFileSync("/tmp/re.txt", "a".repeat(48)));
  const second = codeOf(() => writeFileSync("/tmp/re.txt", "b".repeat(48)));
  const append = codeOf(() => appendFileSync("/tmp/re.txt", "c".repeat(200)));
  return { first, second, append };
};

const capStream = (url) => {
  const bytes = Number(url.searchParams.get("bytes") ?? "0");
  const size = Math.ceil(bytes / 4);
  return new Promise((resolve) => {
    const settled = (code) => {
      let length = null;
      if (code === "ok") {
        try {
          length = readFileSync("/tmp/stream.bin").length;
        } catch {}
      }
      resolve({ code, length });
    };
    const s = createWriteStream("/tmp/stream.bin");
    s.on("error", (error) => settled(error?.code ?? `threw:${error?.message}`));
    s.on("finish", () => settled("ok"));
    for (let i = 0; i < 3; i++) s.write(Buffer.alloc(size, "s"));
    s.end(Buffer.alloc(bytes - 3 * size, "s"));
  });
};
const manyEmpty = (url) => {
  const n = Number(url.searchParams.get("n") ?? "0");
  for (let i = 0; i < n; i++) {
    try {
      writeFileSync(`/tmp/many-${i}.txt`, "");
    } catch (error) {
      return { code: error?.code ?? `threw:${error?.message}`, created: i };
    }
  }
  return { code: "ok", created: n };
};
const longPath = (url) => {
  const len = Number(url.searchParams.get("len") ?? "0");
  return { code: codeOf(() => writeFileSync(`/tmp/${"p".repeat(len)}`, "")) };
};
const manyDirs = (url) => {
  const n = Number(url.searchParams.get("n") ?? "0");
  for (let i = 0; i < n; i++) {
    try {
      mkdirSync(`/tmp/manydir-${i}`);
    } catch (error) {
      return { code: error?.code ?? `threw:${error?.message}`, created: i };
    }
  }
  return { code: "ok", created: n };
};
const bigWrite = (url) => {
  const bytes = Number(url.searchParams.get("bytes") ?? "0");
  return { code: codeOf(() => writeFileSync("/tmp/big.bin", "x".repeat(bytes))) };
};
const bigOutside = (url) => {
  const bytes = Number(url.searchParams.get("bytes") ?? "0");
  return { code: codeOf(() => writeFileSync("/bundle/big.bin", "x".repeat(bytes))) };
};
// Unsupported stream ranges fail loudly: serving whole bytes under a range
// request would be silently wrong data (upstream #280 follow-up).
const streamOpts = () => ({
  writeStart: codeOf(() => createWriteStream("/tmp/o.txt", { start: 10 })),
  readStart: codeOf(() => createReadStream("/tmp/a.txt", { start: 1 })),
  readEnd: codeOf(() => createReadStream("/tmp/a.txt", { end: 2 })),
  readEncoding: codeOf(() => createReadStream("/tmp/a.txt", { encoding: "utf8" })),
});
// Directory URLs keep their trailing slash and round-trip (upstream #280).
const dirUrl = () => {
  const href = pathToFileURL("/tmp/d/").href;
  let back;
  try {
    back = fileURLToPath(href);
  } catch (error) {
    back = error?.code ?? `threw:${error?.message}`;
  }
  return { href, back };
};
// Exclusive-create flags fail before mutating: `wx`/`ax` on an existing
// path keep the old bytes, plain `w` still truncates, and a read-mode flag
// is refused loudly instead of silently overwriting (upstream #280).
const flagExclusive = () => {
  writeFileSync("/tmp/excl.txt", "old");
  const wx = codeOf(() => writeFileSync("/tmp/excl.txt", "new", { flag: "wx" }));
  const keptWx = readFileSync("/tmp/excl.txt", "utf8");
  const ax = codeOf(() => appendFileSync("/tmp/excl.txt", "more", { flag: "ax" }));
  const keptAx = readFileSync("/tmp/excl.txt", "utf8");
  const w = codeOf(() => writeFileSync("/tmp/excl.txt", "new", { flag: "w" }));
  const afterW = readFileSync("/tmp/excl.txt", "utf8");
  const readMode = codeOf(() => writeFileSync("/tmp/excl.txt", "x", { flag: "r" }));
  return { wx, keptWx, ax, keptAx, w, afterW, readMode };
};
// `/tmp` files are writable: `W_OK` passes and the stat mode carries
// write bits, while `/bundle` files stay read-only on both counts.
const writableStat = () => {
  writeFileSync("/tmp/stat.txt", "x");
  const file = statSync("/tmp/stat.txt");
  const bundle = statSync("/bundle/worker.js");
  return {
    tmpWOk: codeOf(() => accessSync("/tmp/stat.txt", constants.W_OK)),
    tmpROk: codeOf(() => accessSync("/tmp/stat.txt", constants.R_OK)),
    bundleWOk: codeOf(() => accessSync("/bundle/worker.js", constants.W_OK)),
    bundleROk: codeOf(() => accessSync("/bundle/worker.js", constants.R_OK)),
    fileWriteBits: (file.mode & 0o222) !== 0 ? "writable" : "read-only",
    bundleWriteBits: (bundle.mode & 0o222) !== 0 ? "writable" : "read-only",
    tmpIsFile: file.isFile(),
    bundleIsFile: bundle.isFile(),
  };
};
// A recursive `mkdir` over an existing file is `EEXIST`: the file stays
// a file, and an existing directory still succeeds.
const recursiveOnFile = () => {
  writeFileSync("/tmp/leaf.txt", "x");
  mkdirSync("/tmp/existing-dir", { recursive: true });
  return {
    fileCode: codeOf(() => mkdirSync("/tmp/leaf.txt", { recursive: true })),
    kept: readFileSync("/tmp/leaf.txt", "utf8"),
    dirCode: codeOf(() => mkdirSync("/tmp/existing-dir", { recursive: true })),
  };
};
// An `open` listener that rewrites the file and ends chunkless keeps its
// bytes: `final` must not truncate the file the open already initialized.
const openListenerKeep = () => new Promise((resolve) => {
  const s = createWriteStream("/tmp/keep.txt");
  s.on("open", () => {
    writeFileSync("/tmp/keep.txt", "keep");
    s.end();
  });
  s.on("finish", () => {
    resolve({ data: readFileSync("/tmp/keep.txt", "utf8") });
  });
  s.on("error", (error) => resolve({ data: error?.code ?? `threw:${error?.message}` }));
});
// An exclusive stream errors instead of clobbering: the old bytes survive
// and `open` never fires (upstream #280).
const streamExclusive = () => new Promise((resolve) => {
  writeFileSync("/tmp/sx.txt", "old");
  const s = createWriteStream("/tmp/sx.txt", { flags: "wx" });
  let opened = false;
  s.on("open", () => { opened = true; });
  const done = (code) => resolve({
    code, opened, kept: readFileSync("/tmp/sx.txt", "utf8"),
  });
  s.on("error", (error) => done(error?.code ?? `threw:${error?.message}`));
  s.on("finish", () => done("finished"));
  s.end("new");
});
// A string chunk decodes with the stream's encoding: `"4142"` under `hex`
// is two bytes, and an unknown encoding is refused up front (upstream #280).
const streamEncoding = () => new Promise((resolve) => {
  const s = createWriteStream("/tmp/hex.txt", { encoding: "hex" });
  s.on("error", (error) => resolve({
    code: error?.code ?? `threw:${error?.message}`, hex: null, length: -1,
  }));
  s.on("finish", () => {
    const bytes = readFileSync("/tmp/hex.txt");
    resolve({ code: "ok", hex: bytes.toString("hex"), length: bytes.length });
  });
  s.end("4142");
});
// The file exists before `open` fires: an `open` listener reads the new
// (empty) file, and a truncated one reads empty too, not stale bytes.
// A bad parent reports through `error`, never `open` (upstream #280).
const streamOpenRead = () => new Promise((resolve) => {
  writeFileSync("/tmp/open-old.txt", "stale-bytes");
  const out = {};
  const fresh = createWriteStream("/tmp/open-new.txt");
  const stale = createWriteStream("/tmp/open-old.txt");
  const bad = createWriteStream("/tmp/no/such/dir.txt");
  let pending = 3;
  const next = () => { if (--pending === 0) resolve(out); };
  fresh.on("open", () => {
    try {
      out.created = JSON.stringify(readFileSync("/tmp/open-new.txt", "utf8"));
    } catch (error) {
      out.created = error?.code ?? `threw:${error?.message}`;
    }
    next();
  });
  fresh.on("error", (error) => {
    out.created = error?.code ?? `threw:${error?.message}`;
    next();
  });
  stale.on("open", () => {
    try {
      out.truncated = JSON.stringify(readFileSync("/tmp/open-old.txt", "utf8"));
    } catch (error) {
      out.truncated = error?.code ?? `threw:${error?.message}`;
    }
    next();
  });
  stale.on("error", (error) => {
    out.truncated = error?.code ?? `threw:${error?.message}`;
    next();
  });
  bad.on("open", () => { out.badOpened = true; next(); });
  bad.on("error", (error) => {
    out.badOpened = false;
    out.badCode = error?.code ?? `threw:${error?.message}`;
    next();
  });
  fresh.end("data");
  stale.end("new");
  bad.end("x");
});
// POSIX keeps `%5c`: only an encoded slash is rejected (upstream #280).
const urlBackslash = () => {
  let roundTrip;
  try {
    roundTrip = fileURLToPath(pathToFileURL("/tmp/a\\b"));
  } catch (error) {
    roundTrip = error?.code ?? `threw:${error?.message}`;
  }
  let slash;
  try {
    slash = fileURLToPath("file:///tmp/%2F");
  } catch (error) {
    slash = error?.code ?? `threw:${error?.message}`;
  }
  return { roundTrip, slash };
};

export default {
  async fetch(req) {
    const url = new URL(req.url);
    switch (url.pathname) {
      case "/health":
        // Cheap boot-probe route: the harness polls `/health` for a 200,
        // and every unmapped path otherwise runs the write-heavy root
        // fixture, which a small `CELLD_TMP_MAX_BYTES` would refuse.
        return Response.json({ ok: true });
      case "/cap-write":
        return Response.json(capWrite(url));
      case "/cap-overwrite":
        return Response.json(capOverwrite());
      case "/cap-stream":
        return Response.json(await capStream(url));
      case "/iso-write":
        return Response.json({ code: codeOf(() => writeFileSync("/tmp/iso.txt", "request-a")) });
      case "/iso-read":
        try {
          return Response.json({ code: "ok", data: readFileSync("/tmp/iso.txt", "utf8") });
        } catch (error) {
          return Response.json({ code: error?.code ?? `threw:${error?.message}` });
        }
      case "/outside":
        return Response.json({ code: codeOf(() => writeFileSync("/bundle/evil.txt", "x")) });
      case "/many-empty":
        return Response.json(manyEmpty(url));
      case "/long-path":
        return Response.json(longPath(url));
      case "/many-dirs":
        return Response.json(manyDirs(url));
      case "/big-write":
        return Response.json(bigWrite(url));
      case "/big-outside":
        return Response.json(bigOutside(url));
      case "/stream-opts":
        return Response.json(streamOpts());
      case "/flag-exclusive":
        return Response.json(flagExclusive());
      case "/stream-exclusive":
        return Response.json(await streamExclusive());
      case "/stream-encoding":
        return Response.json(await streamEncoding());
      case "/stream-bad-encoding":
        return Response.json({
          code: codeOf(() => createWriteStream("/tmp/bad.txt", { encoding: "nope" })),
        });
      case "/stream-open-read":
        return Response.json(await streamOpenRead());
      case "/url-backslash":
        return Response.json(urlBackslash());
      case "/dir-url":
        return Response.json(dirUrl());
      case "/writable-stat":
        return Response.json(writableStat());
      case "/recursive-on-file":
        return Response.json(recursiveOnFile());
      case "/open-listener-keep":
        return Response.json(await openListenerKeep());
      default:
        return Response.json(await tmpRoot());
    }
  },
};
