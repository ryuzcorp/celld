//Fails when any docs page has frontmatter that does not start on line 1,
//or has no title source at all (a frontmatter `title:` or an H1).
//A `<!-- Modified by Ryuz Corp ... -->` marker above the frontmatter
//makes Blume ignore the frontmatter, so the comment becomes the title.
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const docs = new URL("../docs/", import.meta.url).pathname;

function* markdown(dir) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) yield* markdown(path);
    else if (entry.name.endsWith(".md")) yield path;
  }
}

//A `---`...`---` block whose lines contain a `title:` key, searched in
//the first 20 lines so body horizontal rules never match.
function titledBlock(lines) {
  const head = lines.slice(0, 20);
  const opens = head
    .map((line, i) => (line.trim() === "---" ? i : -1))
    .filter((i) => i >= 0);
  for (const open of opens) {
    const rest = head.slice(open + 1);
    const close = rest.findIndex((line) => line.trim() === "---");
    if (close === -1) return { open, titled: false };
    if (rest.slice(0, close).some((line) => /^title:\s*\S/.test(line)))
      return { open, titled: true };
  }
  return null;
}

let failures = 0;
for (const file of markdown(docs)) {
  const lines = readFileSync(file, "utf8").split("\n");
  const short = file.split("/docs/")[1];
  if (lines[0].trim() === "---") {
    const block = titledBlock(lines);
    if (!block || block.open !== 0 || !block.titled) {
      console.error(`FAIL ${short}: frontmatter on line 1 has no title`);
      failures++;
    }
    continue;
  }
  const block = titledBlock(lines);
  if (block?.titled) {
    console.error(`FAIL ${short}: frontmatter does not start on line 1`);
    failures++;
    continue;
  }
  if (!lines.some((line) => /^#\s+\S/.test(line))) {
    console.error(`FAIL ${short}: no title source (frontmatter title or H1)`);
    failures++;
  }
}
if (failures > 0) {
  console.error(`${failures} page(s) with unparseable frontmatter`);
  process.exit(1);
}
console.log("docs frontmatter: all pages have a title source on line 1");
