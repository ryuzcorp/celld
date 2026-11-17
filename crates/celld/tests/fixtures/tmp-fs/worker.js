// Modified by ryuzcorp (ryuzcorp/celld): writable /tmp + node:url fixture (upstream #280). Apache-2.0.
import fs from "node:fs/promises";
import {
  createReadStream,
  createWriteStream,
  readFileSync,
  writeFile as writeFileCb,
  writeFileSync,
} from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";

export default {
  async fetch() {
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
    return Response.json({
      a: await fs.readFile("/tmp/a.txt", "utf8"),
      b: readBack,
      c: readFileSync("/tmp/c.txt", "utf8"),
      d: readFileSync("/tmp/d.txt", "utf8"),
      url: pathToFileURL("/tmp/a.txt").href,
      back: fileURLToPath(pathToFileURL("/tmp/a.txt")),
    });
  },
};
