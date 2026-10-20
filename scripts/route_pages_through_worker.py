#!/usr/bin/env python3
"""scripts/route_pages_through_worker.py - point the site's hard-coded S3 reads at the Cloudflare Worker.

Khalid, 2026-10-20 ("route the pages through the worker"): hundreds of pages fetched data/*.json straight from the
public bucket (https://justhodl-dashboard-live.s3[.us-east-1].amazonaws.com/data/... or
https://s3.amazonaws.com/justhodl-dashboard-live/data/...).  Every such read is billed S3 egress and skips the edge
cache.  justhodl.ai/data/* is routed to the data proxy Worker (cloudflare/workers/justhodl-data-proxy), which serves
the same objects from the edge (path-keyed cache, revalidated copies of the large publications).

What is rewritten (site files only: root *.html / *.js, page directories, js/, assets/):
  * any absolute S3 URL under /data/            -> https://justhodl.ai/data/...
  * a base constant (const S3 = "https://...amazonaws.com") whose every use appends /data/ -> https://justhodl.ai
  * <link rel="preconnect"> to the bucket       -> https://justhodl.ai
Left alone (the Worker has no route for them): screener/, portfolio/, signals/, sentiment/, reports/, regime/, tools/
and root-level objects; and private-artifacts.js's host allow-list.

Run: python3 scripts/route_pages_through_worker.py [--dry-run]
"""
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
DENY = {"aws", ".github", "scripts", "ci", "config", "cloudflare", "ops", "docs", "supabase", "tools-src",
        "chrome-extension", "node_modules", "_partials", "_site", "tests", "vendor", ".git", "data", "tools"}
HOST = r"https?://(?:justhodl-dashboard-live\.s3(?:\.us-east-1)?\.amazonaws\.com|s3\.amazonaws\.com/justhodl-dashboard-live)"
DATA_URL = re.compile(HOST + r"/data(?=[/\"'`?])")
BASE_ONLY = re.compile(r"""(?P<decl>(?:const|var|let)\s+(?P<name>[A-Za-z_$][\w$]*)\s*=\s*)(?P<q>["'`])""" + HOST + r"""(?P=q)""")
PRECONNECT = re.compile(r'(<link[^>]*rel="preconnect"[^>]*href=")' + HOST + r'(")')
# byte-pinned by page preservation tests (tests/*.test.js hash the whole page): left on S3 until their chains are extended
SKIP_FILES = {"private-artifacts.js", "alpha-scoreboard.html", "bonds.html", "correlation.html", "jh-ticker-research.js",
              "master-rank.html", "opportunities.html", "why.html"}


def site_files():
    for p in sorted(ROOT.glob("*.html")) + sorted(ROOT.glob("*.js")):
        yield p
    for d in sorted(ROOT.iterdir()):
        if not d.is_dir() or d.name in DENY or d.name.startswith("."):
            continue
        if d.name in ("js", "assets") or any(d.rglob("*.html")):
            for p in sorted(d.rglob("*.html")) + sorted(d.rglob("*.js")):
                if "node_modules" not in p.parts:
                    yield p


def rewrite(text):
    """-> (new_text, counts dict, notes)."""
    counts = {"data_urls": 0, "bases": 0, "preconnect": 0}
    notes = []
    text, n = DATA_URL.subn("https://justhodl.ai/data", text)
    counts["data_urls"] = n
    text, n = PRECONNECT.subn(r"\1https://justhodl.ai\2", text)
    counts["preconnect"] = n
    for m in list(BASE_ONLY.finditer(text)):
        name = m.group("name")
        uses = [u for u in re.finditer(r"(?<![\w$])" + re.escape(name) + r"(?![\w$])", text) if u.start() != m.start("name")]
        ok = True
        for u in uses:
            tail = text[u.end():u.end() + 12]
            if not re.match(r"""\}/data/|\s*\+\s*["'`]/data/|/data/""", tail):
                ok = False
                notes.append("%s: base %s used without /data/ -> left on S3" % (name, tail.strip()[:20]))
                break
        if ok and uses:
            text = text[:m.start()] + m.group("decl") + m.group("q") + "https://justhodl.ai" + m.group("q") + text[m.end():]
            counts["bases"] += 1
    return text, counts, notes


def main(argv=None):
    dry = "--dry-run" in (argv or sys.argv[1:])
    total = {"files": 0, "data_urls": 0, "bases": 0, "preconnect": 0}
    left = []
    for p in site_files():
        if p.name in SKIP_FILES:
            continue
        raw = p.read_text(encoding="utf-8", errors="surrogateescape")
        if "amazonaws.com" not in raw:
            continue
        new, counts, notes = rewrite(raw)
        for n in notes:
            left.append("%s: %s" % (p.relative_to(ROOT), n))
        if new != raw:
            total["files"] += 1
            for k in ("data_urls", "bases", "preconnect"):
                total[k] += counts[k]
            if not dry:
                p.write_text(new, encoding="utf-8", errors="surrogateescape")
    print("rewritten: %(files)d files, %(data_urls)d data URLs, %(bases)d base constants, %(preconnect)d preconnects" % total)
    for n in left[:60]:
        print("left:", n)
    remaining = sum(1 for p in site_files() if p.name not in SKIP_FILES and re.search(HOST + "/data/", p.read_text(encoding="utf-8", errors="surrogateescape")))
    print("files still reading /data/ straight from S3:", remaining)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
