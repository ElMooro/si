#!/usr/bin/env python3
"""scripts/build_dependency_map.py -- static dependency map of the JustHodl fleet (QA audit, 2026-09-07).

    engine (aws/lambdas/<fn>) --writes--> data/<key>.json --read by--> engine | page (*.html, *.js)

Everything here is "REFERENCED BY CODE" (a string literal in source). It does NOT prove the artifact exists on S3,
loads successfully, is displayed, or drives a decision -- those are separate states, verified per route by the
read-only probe ops (5216) and the page gates. Outputs:

    docs/audit/dependency-map.json   full edge list + per-engine / per-page records
    docs/audit/dependency-map.md     summary: unused outputs, orphan page references, duplicate writers, cadence gaps

Usage: python3 scripts/build_dependency_map.py [--repo <path>]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path

KEY_RE = re.compile(r"""["'`](data/[A-Za-z0-9_./\-{}$]+?\.json(?:\.gz)?)["'`]""")
FSTR_RE = re.compile(r"""f["'](data/[^"']*?\{[^"']*?\}[^"']*?\.json)["']""")
WRITE_HINTS = ("put_object", "put_s3_json", "write_json", "save_json", "upload", "s3_put", "put_json", "publish")
READ_HINTS = ("get_object", "read_json", "safe_load", "load_json", "get_json", "fetch_json", "_read(", "s3_get", "load(")
SKIP_DIRS = {"node_modules", ".git", "archive", "_partials", "aws", "docs", "tests", "scripts"}


def engine_manifest(repo: Path):
    try:
        m = json.loads((repo / "engine-manifest.json").read_text())
        return {e["engine"]: sorted(set(e.get("keys") or [])) for e in m.get("engines") or []}
    except Exception:
        return {}


def classify_literal(src: str, key: str, own_keys: set) -> str:
    """write | read | both | unknown, by looking at the call context around each occurrence."""
    verdict = set()
    for m in re.finditer(re.escape(key), src):
        ctx = src[max(0, m.start() - 220): m.end() + 60]
        if any(h in ctx for h in WRITE_HINTS) and ("Key=" in ctx or "put" in ctx):
            verdict.add("write")
        if any(h in ctx for h in READ_HINTS):
            verdict.add("read")
    if key in own_keys:
        verdict.add("write")
    if verdict == {"write"}:
        return "write"
    if verdict == {"read"}:
        return "read"
    if verdict:
        return "both"
    return "unknown"


def scan_engines(repo: Path, manifest):
    out = {}
    for d in sorted((repo / "aws" / "lambdas").iterdir()):
        src_dir = d / "source"
        if not src_dir.is_dir():
            continue
        text = ""
        for f in sorted(src_dir.glob("*.py")):
            try:
                text += f.read_text(errors="replace") + "\n"
            except Exception:
                pass
        own = set(manifest.get(d.name, []))
        keys = set(KEY_RE.findall(text))
        templ = set(FSTR_RE.findall(text))
        reads, writes = set(), set(own)
        # Only a bound write argument establishes ownership; nearby mentions never do.
        from gen_engine_manifest import ast_keys
        for source in sorted(src_dir.rglob("*.py")):
            _, actual_reads, _ = ast_keys(source.read_text(errors="replace"))
            reads.update(actual_reads)
        cfg = {}
        try:
            cfg = json.loads((d / "config.json").read_text())
        except Exception:
            pass
        sched = cfg.get("schedule") or cfg.get("eventbridge_scheduler") or cfg.get("schedules")
        out[d.name] = {"reads": sorted(reads), "writes": sorted(writes), "templates": sorted(templ), "schedule": sched, "lines": text.count("\n")}
    return out


def scan_pages(repo: Path):
    from page_sources import scan_pages as scan
    return scan(repo)


def fanout_members(repo: Path):
    try:
        m = json.loads((repo / "config" / "fanout-manifest.json").read_text())
        return {member: tick for tick, members in (m.get("ticks") or {}).items() for member in members}
    except Exception:
        return {}


def build(repo: Path):
    manifest = engine_manifest(repo)
    engines = scan_engines(repo, manifest)
    pages = scan_pages(repo)
    fan = fanout_members(repo)
    writers = defaultdict(list)
    for e, rec in engines.items():
        for k in rec["writes"]:
            writers[k].append(e)
    engine_readers = defaultdict(list)
    for e, rec in engines.items():
        for k in rec["reads"]:
            engine_readers[k].append(e)
    page_readers = defaultdict(list)
    for p, rec in pages.items():
        for k in rec["keys"]:
            page_readers[k].append(p)
    all_keys = set(writers) | set(engine_readers) | set(page_readers)
    unused_outputs = sorted(k for k in writers if not engine_readers.get(k) and not page_readers.get(k))
    orphan_page_refs = sorted(k for k in page_readers if not writers.get(k))
    orphan_engine_refs = sorted(k for k in engine_readers if not writers.get(k))
    dup_writers = {k: v for k, v in writers.items() if len(set(v)) > 1}
    engines_no_consumer = sorted(e for e, rec in engines.items() if rec["writes"] and all(not engine_readers.get(k) and not page_readers.get(k) for k in rec["writes"]))
    engines_no_schedule = sorted(e for e, rec in engines.items() if rec["writes"] and not rec["schedule"] and e not in fan)
    # cycles (engine A reads a key written by B and B reads a key written by A) -- 2-cycles only, cheap
    cycles = []
    for a, ra in engines.items():
        for k in ra["reads"]:
            for b in writers.get(k, []):
                if b == a:
                    continue
                for k2 in engines[b]["reads"]:
                    if a in writers.get(k2, []):
                        cyc = tuple(sorted((a, b)))
                        if cyc not in cycles:
                            cycles.append(cyc)
    return {
        "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "note": "REFERENCED BY CODE only (static). Existence/freshness on S3, successful load, display and decision use are separate states.",
        "counts": {"engines": len(engines), "pages": len(pages), "keys": len(all_keys), "writers": len(writers), "unused_outputs": len(unused_outputs),
                   "orphan_page_refs": len(orphan_page_refs), "orphan_engine_refs": len(orphan_engine_refs), "duplicate_writers": len(dup_writers),
                   "engines_without_consumer": len(engines_no_consumer), "engines_without_schedule": len(engines_no_schedule), "two_cycles": len(cycles)},
        "engines": engines, "pages": pages, "writers": dict(writers), "engine_readers": dict(engine_readers), "page_readers": dict(page_readers),
        "unused_outputs": unused_outputs, "orphan_page_refs": orphan_page_refs, "orphan_engine_refs": orphan_engine_refs,
        "duplicate_writers": dup_writers, "engines_without_consumer": engines_no_consumer, "engines_without_schedule": engines_no_schedule,
        "two_cycles": cycles, "fanout_members": fan,
    }


def render_md(m) -> str:
    c = m["counts"]
    L = ["# Fleet dependency map (static, referenced-by-code)", "", "Generated %s by scripts/build_dependency_map.py. %s" % (m["generated_at"][:19], m["note"]), "",
         "| metric | count |", "|---|---|"]
    for k, v in c.items():
        L.append("| %s | %s |" % (k.replace("_", " "), v))
    L += ["", "## Pages referencing keys no engine writes (orphan page references -- missing or obsolete outputs, or written outside aws/lambdas)", ""]
    for k in m["orphan_page_refs"][:150]:
        L.append("- `%s` <- %s" % (k, ", ".join(m["page_readers"][k][:6])))
    L += ["", "## Engines reading keys no engine writes (orphan engine references)", ""]
    for k in m["orphan_engine_refs"][:150]:
        L.append("- `%s` <- %s" % (k, ", ".join(m["engine_readers"][k][:6])))
    L += ["", "## Keys with more than one writer (duplicate / conflicting definitions)", ""]
    for k, v in sorted(m["duplicate_writers"].items())[:120]:
        L.append("- `%s` <- %s" % (k, ", ".join(sorted(set(v)))))
    L += ["", "## Engines whose every output is unreferenced by any page or engine (%d)" % c["engines_without_consumer"], "",
          ", ".join(m["engines_without_consumer"][:400]), "",
          "## Engines that write outputs but have no schedule in config.json and are not fan-out members (%d)" % c["engines_without_schedule"], "",
          ", ".join(m["engines_without_schedule"][:400]), "",
          "## Two-engine read/write cycles (%d)" % c["two_cycles"], ""]
    for a, b in m["two_cycles"][:60]:
        L.append("- %s <-> %s" % (a, b))
    return "\n".join(L) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=str(Path(__file__).resolve().parents[1]))
    a = ap.parse_args()
    repo = Path(a.repo)
    m = build(repo)
    out = repo / "docs" / "audit"
    out.mkdir(parents=True, exist_ok=True)
    (out / "dependency-map.json").write_text(json.dumps(m, indent=1, sort_keys=True))
    (out / "dependency-map.md").write_text(render_md(m))
    print(json.dumps(m["counts"], indent=1))


if __name__ == "__main__":
    main()
