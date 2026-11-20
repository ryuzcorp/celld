//Fails when any docs page has frontmatter that does not start on line 1,
//or has no frontmatter `title` and `description` (the style guide requires
//both on every page). A `<!-- Modified by Ryuz Corp ... -->` marker above
//the frontmatter makes Blume ignore the frontmatter.
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

//A `---`...`---` block on the given lines that sets both a `title:` and a
//`description:` key, searched in the first 20 lines so body horizontal
//rules never match.
function frontmatterBlock(lines) {
  const head = lines.slice(0, 20);
  const opens = head
    .map((line, i) => (line.trim() === "---" ? i : -1))
    .filter((i) => i >= 0);
  for (const open of opens) {
    const rest = head.slice(open + 1);
    const close = rest.findIndex((line) => line.trim() === "---");
    if (close === -1) return { open, complete: false };
    const body = rest.slice(0, close);
    const hasTitle = body.some((line) => /^title:\s*\S/.test(line));
    const hasDescription = body.some((line) => /^description:\s*\S/.test(line));
    if (hasTitle && hasDescription) return { open, complete: true };
    return { open, complete: false };
  }
  return null;
}

let failures = 0;
for (const file of markdown(docs)) {
  const lines = readFileSync(file, "utf8").split("\n");
  const short = file.split("/docs/")[1];
  if (lines[0].trim() === "---") {
    const block = frontmatterBlock(lines);
    if (!block || block.open !== 0 || !block.complete) {
      console.error(`FAIL ${short}: frontmatter on line 1 needs title and description`);
      failures++;
    }
    continue;
  }
  const block = frontmatterBlock(lines);
  if (block?.complete || (block && block.open > 0)) {
    console.error(`FAIL ${short}: frontmatter does not start on line 1`);
    failures++;
    continue;
  }
  console.error(`FAIL ${short}: no frontmatter title and description`);
  failures++;
}
if (failures > 0) {
  console.error(`${failures} page(s) with unparseable frontmatter`);
  process.exit(1);
}
console.log("docs frontmatter: every page sets title and description on line 1");
