#!/usr/bin/env python3
"""Classify an s3bench proxy log into METHOD x key-family counts.

Usage: count.py <logfile>

Key families (anything else prints verbatim under its raw prefix):
  cells/*/ltx          per-cell LTX objects (L0 uploads, snapshots, L1s)
  cells/*/own.json     per-cell ownership records
  log/*/bundle         node log bundle objects
  nodes/               node lease records
  fleet/capacity-v1.json
                       fleet capacity record

A GET with ?list-type=2 (ListObjectsV2) counts as LIST: the SDK issues
listings as GET, but R2 prices them as Class A. A bulk delete
(POST ?delete, DeleteObjects) stays POST: R2 prices it Class A, unlike a
single-key DELETE, which is free.

Summary pricing follows R2 request pricing:
  Class A = PUT, POST, LIST, COPY
  Class B = GET, HEAD
  free    = single-key DELETE only
Authentication failures (401/403) are not billed: they stay in the raw
counts but are excluded from the class totals and shown as AUTH_FAILED.
"""

import json
import sys
from collections import Counter
from urllib.parse import unquote

CLASS_A = {"PUT", "POST", "LIST", "COPY"}
CLASS_B = {"GET", "HEAD"}
FREE = {"DELETE"}


def family(path):
    if path.strip("/") == "":
        return "bucket/"
    parts = path.strip("/").split("/")
    if len(parts) < 2:
        return "/".join(parts) + ("/" if path.endswith("/") else "")
    top, rest = parts[0], parts[1:]
    if top != "cells":
        if top == "log" and len(rest) >= 2 and rest[1] == "bundle":
            return "log/*/bundle"
        if top == "nodes":
            return "nodes/"
        if top == "fleet":
            return "fleet/" + rest[0] if rest else "fleet/"
        return top + "/"
    # cells/<cell>/<...>: skip the cell id.
    rest = rest[1:]
    if not rest:
        return "cells/"
    if rest[0] == "ltx":
        return "cells/*/ltx"
    if rest[0] == "own.json":
        return "cells/*/own.json"
    return "cells/*/" + rest[0]


def main():
    counts = Counter()
    total_lines = 0
    auth_failed = 0
    class_a = class_b = free = 0
    with open(sys.argv[1], encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            total_lines += 1
            method = record["method"]
            status = record.get("status", 0)
            query = record.get("query", "")
            if method == "GET" and "list-type=2" in query:
                method = "LIST"
            key = record["path"].lstrip("/")
            # Strip the bucket prefix: proxy logs path-style /<bucket>/<key>.
            _, _, key = key.partition("/")
            if method == "LIST":
                # A listing addresses a prefix, not a key: the SDK issues
                # GET /<bucket>/?list-type=2&prefix=<prefix>. Classify by
                # the listed prefix so per-cell guard LISTs land in
                # cells/*/ltx instead of bucket/.
                prefix = ""
                for param in query.split("&"):
                    name, _, value = param.partition("=")
                    if name == "prefix":
                        prefix = unquote(value)
                        break
                key = prefix
            counts[(method, family("/" + key))] += 1
            # R2 does not charge authentication failures; every other
            # failure (including conditional-write failures like 412)
            # stays billable.
            if status in (401, 403):
                auth_failed += 1
            elif method in CLASS_A:
                class_a += 1
            elif method in CLASS_B:
                class_b += 1
            elif method in FREE:
                free += 1

    for (method, fam), count in sorted(counts.items()):
        print(f"{method} {fam} {count}")
    print(f"TOTAL {total_lines}")
    print(f"CLASS_A {class_a} (PUT/POST/LIST/COPY)")
    print(f"CLASS_B {class_b} (GET/HEAD)")
    print(f"FREE {free} (DELETE)")
    print(f"AUTH_FAILED {auth_failed} (401/403, not billed)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: count.py <logfile>", file=sys.stderr)
        sys.exit(2)
    main()
