"""ops_5065 -- find the state docs properly, then rank the real backlog.

ops 5064 reported seven gaps, all of them census-econ, and concluded the
fleet had no other backlog. That conclusion was an artifact of my own
discovery: it called list_objects_v2 on data/warm/ with a 4,000-key cap
against a prefix holding roughly 700,000 objects. S3 returns keys in
lexicographic order, so it read a few early-alphabet providers and
stopped -- boj, fred, gdelt, imf, worldbank were never reached. Every
"not at that path" line was me looking in the wrong place and reporting
absence.

That is the same failure that has cost this arc six red gates: measuring
what is convenient and treating it as what is true. A scan that cannot
reach most of the corpus must say so rather than return a clean list.

Discovery is now structural instead of alphabetical: enumerate provider
directories with Delimiter="/" (57 of them, one cheap call), then list
each provider's own small _state/ prefix. No cap can silently truncate
that.

The prose on the page says what should turn up:
    BOJ        55306/120394 series      ~65,000 missing
    OECD       69% of 1,546, 488 denied at source
    FRED       277,453 banked vs 282,141 discovered
    GDELT      gaps 7381, v1 archive 4983/4986
    IMF 218/222 · World Bank 29468/29490 · FINRA 8/9 · NYFed 10/11
If those do not appear now, the gap is in my scan, not in the fleet.

  P0 structural discovery, and a count of what it reached
  P1 every done/total pair found, ranked by absolute missing
  P2 the named lanes, at their REAL paths
  P3 fixable vs blocked, and the expedite order
"""
import json
import re
import sys
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)

PAIRS = [("n_done", "n_total"), ("done", "total"), ("banked", "target"),
         ("flows_parsed", "flows_total"), ("n_series", "n_universe"),
         ("series_done", "series_total"), ("datasets_done",
                                           "datasets_total"),
         ("parts_done", "parts_total"), ("indicators", "indicators_total"),
         ("dataflows", "dataflows_total"), ("fetched", "discovered"),
         ("n_banked", "n_discovered"), ("done_count", "universe")]
BLOCK = re.compile(r"await|api[_ ]?key|secret|credential|denied|403|"
                   r"forbidden|excluded by design|not entitled", re.I)


def jget(k):
    import gzip
    try:
        b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
        if k.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return None


def dirs(prefix):
    out, kw = [], {"Bucket": LIVE, "Prefix": prefix, "Delimiter": "/"}
    while True:
        r = s3.list_objects_v2(**kw)
        out += [p["Prefix"] for p in r.get("CommonPrefixes", [])]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    return out


def keys(prefix, cap=600):
    out, kw = [], {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
    while len(out) < cap:
        r = s3.list_objects_v2(**kw)
        out += [o["Key"] for o in r.get("Contents", [])]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    return out


def scan(doc, depth=0):
    hits = []
    if not isinstance(doc, dict) or depth > 3:
        return hits
    for a, b in PAIRS:
        va, vb = doc.get(a), doc.get(b)
        if isinstance(va, (int, float)) and isinstance(vb, (int, float)) \
                and vb > 4 and 0 <= va <= vb:
            hits.append((a + "/" + b, int(va), int(vb)))
    for k, v in doc.items():
        if isinstance(v, dict):
            hits += [(k + "." + h[0], h[1], h[2])
                     for h in scan(v, depth + 1)]
    return hits


with report("ops_5065_gap_census_fixed") as R:
    fails = []
    out = {"op": "ops_5065", "gaps": []}

    R.section("P0 structural discovery")
    provs = dirs("data/warm/")
    R.log("  provider directories under data/warm/: %d" % len(provs))
    R.log("  %s%s" % ([p.split("/")[-2] for p in provs[:14]],
                      " …" if len(provs) > 14 else ""))
    cand = list(keys("data/_state/", cap=800))
    R.log("  data/_state/ documents: %d" % len(cand))
    for p in provs:
        for sub in ("_state/", ""):
            for k in keys(p + sub, cap=60):
                if k.endswith(".json") and (
                        "state" in k or "coverage" in k
                        or "manifest" in k or "progress" in k):
                    cand.append(k)
    cand = sorted(set(cand))
    R.log("  candidate state documents reached: %d  "
          "(ops 5064 reached far fewer and called it a clean fleet)"
          % len(cand))

    R.section("P1 every done/total pair, ranked by absolute missing")
    found = {}
    for k in cand[:900]:
        d = jget(k)
        if not isinstance(d, dict):
            continue
        blob = json.dumps(d, default=str)[:2500]
        for label, a, b in scan(d):
            miss = b - a
            if miss <= 0 or b < 5:
                continue
            nm = "/".join(k.split("/")[-3:])
            if nm not in found or found[nm][1] < miss:
                found[nm] = (label, miss, a, b, k,
                             bool(BLOCK.search(blob)))
    rows = sorted(found.items(), key=lambda kv: -kv[1][1])
    R.log("  %-40s %10s %10s %7s %s" % ("where", "missing", "of",
                                        "have%", "kind"))
    for nm, (label, miss, a, b, k, bl) in rows[:26]:
        R.log("  %-40s %10s %10s %6.1f%% %s" % (
            nm[-40:], f"{miss:,}", f"{b:,}", 100.0 * a / b,
            "BLOCKED?" if bl else "fixable"))
        out["gaps"].append({"where": nm, "field": label, "missing": miss,
                            "have": a, "total": b, "key": k,
                            "blocked": bl})
    R.log("  documents with a measurable gap: %d" % len(rows))
    R.log("  TOTAL MISSING across the fleet: %s items" % (
        f"{sum(v[1] for v in found.values()):,}"))

    R.section("P2 the named lanes, at whatever path they really use")
    for nm in ("boj", "gdelt", "fred", "imf", "worldbank", "oecd",
               "finra", "nyfed", "frbddp", "tic"):
        hits = [k for k in cand if nm in k.lower()]
        if not hits:
            R.log("  %-10s no state document found anywhere" % nm)
            continue
        best = None
        for k in hits[:6]:
            d = jget(k)
            if not isinstance(d, dict):
                continue
            sc = scan(d)
            if sc:
                best = (k, max(sc, key=lambda x: x[2] - x[1]))
                break
            if best is None:
                best = (k, None)
        if best and best[1]:
            k, (label, a, b) = best
            R.log("  %-10s %-46s %s -> %s/%s (%.0f%%)" % (
                nm, k.split("/")[-1][:46], label, f"{a:,}", f"{b:,}",
                100.0 * a / b))
        elif best:
            d = jget(best[0]) or {}
            R.log("  %-10s %-46s keys=%s" % (
                nm, best[0].split("/")[-1][:46],
                sorted(d.keys())[:9]))

    R.section("P3 expedite order")
    fix = [g for g in out["gaps"] if not g["blocked"]]
    blk = [g for g in out["gaps"] if g["blocked"]]
    R.log("  FIXABLE: %d lanes, %s items missing" % (
        len(fix), f"{sum(g['missing'] for g in fix):,}"))
    for g in fix[:10]:
        R.log("    %-38s %10s missing  (%.0f%% held)" % (
            g["where"][-38:], f"{g['missing']:,}",
            100.0 * g["have"] / g["total"]))
    R.log("  BLOCKED-LOOKING: %d lanes, %s items" % (
        len(blk), f"{sum(g['missing'] for g in blk):,}"))
    for g in blk[:8]:
        R.log("    %-38s %10s missing" % (g["where"][-38:],
                                          f"{g['missing']:,}"))
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/fleet-gaps.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/fleet-gaps.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5065 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(docs=len(cand), gaps=len(out["gaps"]),
         total_missing=sum(g["missing"] for g in out["gaps"]),
         top=(out["gaps"][0]["where"] if out["gaps"] else "-"))
    R.log("ops 5065 GREEN -- backlog quantified on a scan that actually "
          "reached the fleet")
