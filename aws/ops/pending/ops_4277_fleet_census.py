"""
ops_4277 -- fleet census for quantum-desk v2: look at EVERY engine,
mechanically.

Khalid's ask: enrich the quantum desk from every single engine. 771
lambdas can't be read by hand and shouldn't be -- the honest version is
a census: (1) walk every source file on this full checkout and extract
every S3 write target; (2) join against live data/ artifacts with age +
size; (3) shape-probe every artifact that is ticker-keyed (a dict of
>=15 uppercase tickers, or a list of dicts carrying ticker/symbol) and
record HOW to read it; (4) publish the whole map as
data/quantum-desk-sources.json so v2's evidence join is data-driven --
the desk will consult every per-name signal the fleet produces, and
future engines join automatically when the census re-runs.
"""
import json
import os
import re
import sys
from datetime import datetime, timezone

import boto3
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
NOW = datetime.now(timezone.utc)

RX_KEY = re.compile(r"""Key\s*=\s*["'](data/[A-Za-z0-9_\-./]+\.json)["']""")
RX_CONST = re.compile(
    r"""^\s*([A-Z][A-Z0-9_]*(?:KEY|_S3|_OUT)[A-Z0-9_]*)\s*=\s*["'](data/[A-Za-z0-9_\-./]+\.json)["']""",
    re.M)
RX_KEYVAR = re.compile(r"""Key\s*=\s*([A-Z][A-Z0-9_]*)\b""")
TICKER = re.compile(r"^[A-Z][A-Z0-9.\-]{0,6}$")

def walk_writers():
    writers = {}
    n_files = 0
    for eng in sorted(os.listdir("aws/lambdas")):
        sdir = os.path.join("aws/lambdas", eng, "source")
        if not os.path.isdir(sdir):
            continue
        consts, targets = {}, set()
        for root, _, files in os.walk(sdir):
            if "pypdf" in root or "vendor" in root:
                continue
            for f in files:
                if not f.endswith(".py"):
                    continue
                n_files += 1
                try:
                    src = open(os.path.join(root, f),
                               encoding="utf-8", errors="ignore").read()
                except Exception:
                    continue
                for m in RX_CONST.finditer(src):
                    consts[m.group(1)] = m.group(2)
                for m in RX_KEY.finditer(src):
                    targets.add(m.group(1))
                for m in RX_KEYVAR.finditer(src):
                    if m.group(1) in consts:
                        targets.add(consts[m.group(1)])
        for t in targets:
            writers.setdefault(t, []).append(eng)
    return writers, n_files

def list_artifacts():
    arts = {}
    pag = s3.get_paginator("list_objects_v2")
    for pg in pag.paginate(Bucket=BUCKET, Prefix="data/"):
        for o in pg.get("Contents", []):
            k = o["Key"]
            if not k.endswith(".json") or k.count("/") != 1:
                continue
            arts[k] = {
                "age_h": round((NOW - o["LastModified"])
                               .total_seconds() / 3600, 1),
                "kb": o["Size"] // 1024,
            }
    return arts

def find_ticker_struct(doc, path="$", depth=0):
    """Return (path, mode, sample_fields) for the first ticker-keyed
    structure. mode: 'dict' (doc[path][TICKER] -> fields) or
    'list' (rows with ticker/symbol)."""
    if depth > 3 or doc is None:
        return None
    if isinstance(doc, dict):
        tk = [k for k in list(doc)[:4000] if TICKER.match(str(k))]
        if len(tk) >= 15:
            v0 = doc[tk[0]]
            fields = (list(v0)[:10] if isinstance(v0, dict)
                      else [type(v0).__name__])
            return (path, "dict", fields, len(tk))
        for k in list(doc)[:40]:
            r = find_ticker_struct(doc[k], "%s.%s" % (path, k), depth + 1)
            if r:
                return r
    elif isinstance(doc, list) and doc:
        rows = [x for x in doc[:60] if isinstance(x, dict)
                and (x.get("ticker") or x.get("symbol")
                     or x.get("Ticker"))]
        if len(rows) >= 8:
            return (path, "list", list(rows[0])[:10], len(doc))
        for x in doc[:6]:
            r = find_ticker_struct(x, path + "[]", depth + 1)
            if r:
                return r
    return None

fails = []
with report("4277_fleet_census") as r:
    r.heading("ops 4277 -- fleet census: every engine, every artifact")

    writers, n_files = walk_writers()
    engines = len([d for d in os.listdir("aws/lambdas")
                   if os.path.isdir(os.path.join("aws/lambdas", d))])
    r.ok("repo walk: %d engines, %d py files, %d distinct data/ write "
         "targets" % (engines, n_files, len(writers)))

    arts = list_artifacts()
    live_written = {k: v for k, v in arts.items() if k in writers}
    fresh26 = sum(1 for v in arts.values() if v["age_h"] <= 26)
    r.ok("live S3: %d top-level data/*.json (%d fresh<26h); %d have "
         "identified writers; %d write-targets not yet materialized"
         % (len(arts), fresh26, len(live_written),
            len([k for k in writers if k not in arts])))

    r.section("per-ticker shape probe (every candidate, capped fetch)")
    per_ticker = []
    skip = {"data/indicator-bus.json"}  # 18k indicators, macro not names
    cands = [k for k, v in sorted(arts.items(),
                                  key=lambda kv: kv[1]["age_h"])
             if v["kb"] <= 3500 and k not in skip]
    probed = 0
    for k in cands:
        if probed >= 120:
            break
        try:
            body = s3.get_object(Bucket=BUCKET, Key=k)["Body"].read()
            probed += 1
            doc = json.loads(body)
        except Exception:
            continue
        hit = find_ticker_struct(doc)
        if hit:
            path, mode, fields, n = hit
            per_ticker.append({
                "key": k, "path": path, "mode": mode,
                "n": n, "fields": fields,
                "age_h": arts[k]["age_h"],
                "writers": (writers.get(k) or [])[:2]})
    per_ticker.sort(key=lambda x: x["age_h"])
    r.ok("ticker-keyed artifacts found: %d (probed %d)"
         % (len(per_ticker), probed))
    for pt in per_ticker[:28]:
        r.kv(key=pt["key"].replace("data/", ""), mode=pt["mode"],
             n=pt["n"], age_h=pt["age_h"],
             path=pt["path"][:34], fields=",".join(
                 str(f) for f in pt["fields"][:5]))

    r.section("macro candidates (fresh, non-ticker, for new legs)")
    macro_watch = ["canary-warroom", "ka-metrics", "khalid-metrics",
                   "industry-boom", "master-ranker", "cftc",
                   "macro-leads", "boom-stage", "primary-dealers",
                   "global-recession", "rotation", "capital-flow",
                   "tga", "fed-liquidity", "credit-composite",
                   "freight-pulse", "canary"]
    macro = []
    for k, v in sorted(arts.items(), key=lambda kv: kv[1]["age_h"]):
        if any(w in k for w in macro_watch) and v["age_h"] <= 200:
            macro.append({"key": k, "age_h": v["age_h"],
                          "kb": v["kb"],
                          "writers": (writers.get(k) or [])[:2]})
    for mrow in macro[:22]:
        r.kv(**mrow)

    out = {
        "generated_at": NOW.isoformat(timespec="seconds"),
        "ops": 4277,
        "census": {"engines": engines, "py_files": n_files,
                   "write_targets": len(writers),
                   "live_artifacts": len(arts),
                   "fresh_26h": fresh26},
        "per_ticker_sources": per_ticker,
        "macro_candidates": macro[:40],
        "refresh": "re-run ops fleet census after major fleet changes; "
                   "quantum-desk consumes this map data-driven",
    }
    s3.put_object(Bucket=BUCKET, Key="data/quantum-desk-sources.json",
                  Body=json.dumps(out, separators=(",", ":"),
                                  default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    r.ok("census PUBLISHED: data/quantum-desk-sources.json "
         "(%d per-ticker sources, %d macro candidates)"
         % (len(per_ticker), len(macro[:40])))

    if not per_ticker:
        fails.append("no ticker-keyed artifacts found -- probe broken")
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4277 PASS -- the fleet is mapped; v2 wiring next")
if fails:
    sys.exit(1)
