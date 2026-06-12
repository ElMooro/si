#!/usr/bin/env python3
"""Scan repo: engine -> output keys, page (fetch-match OR filename heuristic),
board membership. Prints manifest JSON to stdout."""
import re, glob, io, json, sys

pages = sorted(glob.glob("*.html"))
page_keys = {}
for p in pages:
    s = io.open(p, encoding="utf-8", errors="replace").read()
    page_keys[p] = set(re.findall(r'data/[A-Za-z0-9_\-./]+\.json', s))
key_to_pages = {}
for p, ks in page_keys.items():
    if p == "directory.html":
        continue
    for k in ks:
        key_to_pages.setdefault(k, set()).add(p)
board_src = io.open("aws/lambdas/justhodl-signal-board/source/lambda_function.py",
                     encoding="utf-8").read()
board_keys = set(re.findall(r'"(data/[^"]+\.json)"', board_src))
man = []
for d in sorted(glob.glob("aws/lambdas/*/source/lambda_function.py")):
    name = d.split("/")[2]
    src = io.open(d, encoding="utf-8", errors="replace").read()
    keys = sorted({k for k in re.findall(r'["\'](data/[A-Za-z0-9_\-./]+\.json)["\']', src)
                    if "/_" not in k and "history" not in k and "cache" not in k
                    and "state" not in k})
    if not keys:
        continue
    pset = set()
    for k in keys:
        pset |= key_to_pages.get(k, set())
    short = name.replace("justhodl-", "")
    for cand in (f"{short}.html", f"{short.replace('-agent','')}.html"):
        if cand in pages:
            pset.add(cand)
    man.append({"engine": name, "keys": keys[:3],
                 "page": sorted(pset)[0] if pset else None,
                 "on_board": any(k in board_keys for k in keys)})
out = {"generated_from": "repo scan", "n_engines": len(man),
        "n_no_page": sum(1 for m in man if not m["page"]),
        "n_board_only": sum(1 for m in man if not m["page"] and m["on_board"]),
        "n_invisible": sum(1 for m in man if not m["page"] and not m["on_board"]),
        "engines": man}
json.dump(out, sys.stdout, indent=1)
