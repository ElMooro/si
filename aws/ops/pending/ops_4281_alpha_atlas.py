"""
ops_4281 -- the Alpha Atlas: every way this platform finds money,
scanned from the system itself.

The 4277 census probed 120 artifacts for ticker shapes; Khalid's ask is
bigger -- ALL opportunity machinery, whatever its shape. This scan
walks EVERY live top-level data/*.json (874), detects actionable
structures (rows with score/conviction/verdict/signal/rank fields, or
verdict maps), classifies each into a mechanism family (how the edge
makes money), pulls its LIVE top signal right now, and publishes
data/alpha-atlas.json. Engines whose write-targets never materialized
are listed as DORMANT -- built alpha not yet switched on. Nothing is
invented: every entry carries its artifact, writer, age, and a real
sampled signal.
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

FAMILIES = [
    ("Insider & Political", ("insider", "congress", "political",
                             "house-ptr", "lobbying", "senate")),
    ("Smart-Money Cloning", ("13f", "clone", "activist", "whale",
                             "guru", "fund-flows", "dealers")),
    ("Squeeze & Short Structure", ("squeeze", "short", "finra",
                                   "borrow", "gamma", "pump")),
    ("Options & Derivatives", ("options", "0dte", "skew", "vol-",
                               "gamma", "flow")),
    ("Momentum & Trend", ("momentum", "trend", "breakout", "leaders",
                          "velocity", "52w", "ma-command", "spx-ma")),
    ("Value & Quality", ("value", "forensic", "quality", "census",
                         "fundamental", "moat", "deep-value",
                         "fortress", "buyback")),
    ("Event & Catalyst", ("deal", "merger", "earnings", "catalyst",
                          "pead", "readthrough", "spinoff", "ipo",
                          "patent", "forward-orders")),
    ("Cross-Signal Convergence", ("convergence", "compound",
                                  "confluence", "best-setups",
                                  "alpha", "master-ranker",
                                  "future-intel", "thesis",
                                  "upside", "quantum-desk")),
    ("Rotation & Industry", ("rotation", "sector", "industry",
                             "theme", "boom", "divergence-pairs",
                             "rrg")),
    ("Macro Timing & Canaries", ("canary", "recession", "macro",
                                 "regime", "cycle", "nowcast",
                                 "liquidity", "tga", "fed", "risk-",
                                 "freight", "import", "grid",
                                 "credit", "warroom", "cftc",
                                 "rainbow", "compass")),
    ("Crypto Cycle & On-Chain", ("crypto", "btc", "eth", "bitcoin",
                                 "mvrv", "stablecoin", "onchain",
                                 "funding-rate", "defi")),
    ("Pairs & Relative Value", ("pairs", "spread", "relative",
                                "ratio", "vs-")),
]
SIGNALY = {"score", "conviction", "verdict", "signal", "rank", "fit",
           "rating", "opportunity", "setup", "alert", "z", "zscore",
           "percentile", "upside", "er", "expected_return", "grade",
           "status", "flag", "pressure", "level", "band"}
NAMEY = ("ticker", "symbol", "name", "label", "asset", "pair", "key",
         "industry", "sector", "class", "id", "title")

def family_for(key):
    k = key.lower()
    for fam, words in FAMILIES:
        if any(w in k for w in words):
            return fam
    return "Uncategorized Signals"

def looks_actionable(rows):
    if not rows or not isinstance(rows[0], dict):
        return None
    f0 = set(rows[0])
    if not (f0 & SIGNALY):
        return None
    namef = next((n for n in NAMEY if n in f0), None)
    sigf = sorted(f0 & SIGNALY)[:3]
    return namef, sigf

def top_signal(doc, depth=0, path="$"):
    """First actionable rows-list anywhere; return (path, n, sample)."""
    if depth > 3:
        return None
    if isinstance(doc, list) and len(doc) >= 3:
        hit = looks_actionable(doc[:5])
        if hit:
            namef, sigf = hit
            row = doc[0]
            samp = {("name" if namef else "row"):
                    str(row.get(namef, ""))[:26] if namef else "-"}
            for sf in sigf:
                v = row.get(sf)
                samp[sf] = (round(v, 3) if isinstance(v, float)
                            else str(v)[:22])
            return path, len(doc), samp
        for i, x in enumerate(doc[:4]):
            r = top_signal(x, depth + 1, "%s[%d]" % (path, i))
            if r:
                return r
    if isinstance(doc, dict):
        for k in list(doc)[:36]:
            r = top_signal(doc[k], depth + 1, "%s.%s" % (path, k))
            if r:
                return r
    return None

# writer map from repo (same walk as 4277, lean)
RX_KEY = re.compile(r"""Key\s*=\s*["'](data/[A-Za-z0-9_\-./]+\.json)["']""")
RX_CONST = re.compile(
    r"""^\s*([A-Z][A-Z0-9_]*KEY[A-Z0-9_]*)\s*=\s*["'](data/[A-Za-z0-9_\-./]+\.json)["']""",
    re.M)
writers = {}
for eng in sorted(os.listdir("aws/lambdas")):
    sp = os.path.join("aws/lambdas", eng, "source", "lambda_function.py")
    if not os.path.exists(sp):
        continue
    try:
        srct = open(sp, encoding="utf-8", errors="ignore").read()
    except Exception:
        continue
    ks = set(RX_KEY.findall(srct)) | {v for _, v in
                                      RX_CONST.findall(srct)}
    for k in ks:
        writers.setdefault(k, []).append(eng)

fails = []
with report("4281_alpha_atlas") as r:
    r.heading("ops 4281 -- Alpha Atlas: every money-making mechanism, "
              "scanned")

    arts = {}
    for pg in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix="data/"):
        for o in pg.get("Contents", []):
            k = o["Key"]
            if k.endswith(".json") and k.count("/") == 1:
                arts[k] = ((NOW - o["LastModified"]).total_seconds()
                           / 3600, o["Size"])
    r.ok("scanning %d live artifacts (writer map: %d targets from "
         "%d engines)" % (len(arts), len(writers),
                          len(os.listdir("aws/lambdas"))))

    atlas, scanned, actionable = {}, 0, 0
    for key, (age_h, size) in sorted(arts.items()):
        if size > 4_000_000 or size < 40:
            continue
        try:
            doc = json.loads(s3.get_object(Bucket=BUCKET,
                                           Key=key)["Body"].read())
        except Exception:
            continue
        scanned += 1
        ts = top_signal(doc)
        if not ts:
            continue
        actionable += 1
        path, n, samp = ts
        fam = family_for(key)
        atlas.setdefault(fam, []).append({
            "artifact": key, "age_h": round(age_h, 1),
            "writers": (writers.get(key) or [])[:2],
            "signal_path": path, "n_signals": n,
            "top_now": samp})
    for fam in atlas:
        atlas[fam].sort(key=lambda x: x["age_h"])

    dormant = sorted(k for k in writers if k not in arts)

    out = {"generated_at": NOW.isoformat(timespec="seconds"),
           "ops": 4281,
           "totals": {"live_artifacts": len(arts),
                      "scanned": scanned,
                      "opportunity_engines": actionable,
                      "families": len(atlas),
                      "dormant_targets": len(dormant)},
           "families": [{"family": fam,
                         "n": len(rows),
                         "engines": rows}
                        for fam, rows in sorted(
                            atlas.items(),
                            key=lambda kv: -len(kv[1]))],
           "dormant": [{"target": k,
                        "writers": (writers.get(k) or [])[:2]}
                       for k in dormant][:60],
           "note": "Every entry sampled live from S3; nothing "
                   "invented. Dormant = engine code writes this key "
                   "but the key does not exist -- built alpha not "
                   "switched on."}
    s3.put_object(Bucket=BUCKET, Key="data/alpha-atlas.json",
                  Body=json.dumps(out, separators=(",", ":"),
                                  default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")

    r.ok("ATLAS: %d opportunity engines across %d families "
         "(scanned %d docs); %d dormant targets"
         % (actionable, len(atlas), scanned, len(dormant)))
    for famrow in out["families"]:
        r.section("%s (%d)" % (famrow["family"], famrow["n"]))
        for e in famrow["engines"][:6]:
            r.kv(artifact=e["artifact"].replace("data/", "")[:34],
                 age_h=e["age_h"], n=e["n_signals"],
                 top=json.dumps(e["top_now"])[:88])
    r.section("dormant (built, never wrote)")
    for d in out["dormant"][:12]:
        r.kv(target=d["target"].replace("data/", "")[:40],
             writers=",".join(d["writers"])[:44])
    if actionable < 100:
        fails.append("only %d actionable engines found -- detector "
                     "too strict for this fleet" % actionable)
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4281 PASS -- the atlas is live at "
             "data/alpha-atlas.json")
if fails:
    sys.exit(1)
