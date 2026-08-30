"""ops_5064 -- what is actually missing, fleet-wide, ranked.

The provider page states its own gaps in prose and nobody adds them up:

    BOJ        "API universe: 22 dbs · 55306/120394 series"   <- 46%
    OECD       "69% of 1,546 target · 488 denied at source"
    FRED       "COMPLETE_WITH_LEAKS" · banner says 282,141 discovered
               vs 277,453 banked
    GDELT      "gaps 7381 · v1 archive 4983/4986"
    IMF        "218/222 dataflows"
    World Bank "29468/29490 indicators"
    FINRA      "8/9 datasets"        (blocked: no client secret)
    ECOS       "0 series · awaiting ECOS key"   (blocked: credential)
    frbddp     "8 named misses"   BoE "1 named failures"
    TIC        "4 named misses"   census-ts "1 structurally-named ..."
    NY Fed     "hist-v1 full-window: 10/11 families"

Each reads as a footnote; together they are the actual backlog. BOJ
alone is ~65,000 missing series -- an order of magnitude more than
anything the Census econ lane will add.

This op stops trusting the prose and reads the ENGINES' OWN STATE. It
walks every state document, finds the done/total pair each engine keeps
in its own vocabulary (n_done/n_total, banked/target, flows_parsed/
flows_total, done/queue...), and ranks the fleet by what is missing --
absolute first, because a 46% gap on 120k series matters more than a
100% gap on 29.

It also separates the two kinds of gap, because only one is worth
expediting:
    FIXABLE  -- rate limits, wrong access pattern, unfinished crawl
    BLOCKED  -- missing credential, denied at source, excluded by design
Chasing a blocked lane is the waste; finding a fixable one that has sat
at 46% is the win.

Read-only. Nothing is imported here -- the next op expedites whatever
this ranks first.
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

PAIRS = [("n_done", "n_total"), ("done", "total"),
         ("banked", "target"), ("flows_parsed", "flows_total"),
         ("n_series", "n_series_universe"), ("series", "series_total"),
         ("datasets_done", "datasets_total"), ("parts_done", "parts"),
         ("indicators_banked", "indicators_total"),
         ("dataflows_done", "dataflows_total")]
BLOCKED_HINT = re.compile(
    r"await|key|secret|credential|denied|403|forbidden|excluded|"
    r"by design|no data|not available", re.I)


def jget(k):
    import gzip
    try:
        b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
        if k.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return None


def listing(prefix, cap=4000):
    out, kw = [], {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
    while len(out) < cap:
        r = s3.list_objects_v2(**kw)
        out += [(o["Key"], o["Size"], o["LastModified"])
                for o in r.get("Contents", [])]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    return out


def scan(doc, path):
    """Find any done/total pair, whatever the engine calls it."""
    hits = []
    if not isinstance(doc, dict):
        return hits
    for a, b in PAIRS:
        va, vb = doc.get(a), doc.get(b)
        if isinstance(va, (int, float)) and isinstance(vb, (int, float)) \
                and vb > 0 and va <= vb * 1.05:
            hits.append((a + "/" + b, int(va), int(vb)))
    for k, v in doc.items():
        if isinstance(v, dict):
            hits += [(k + "." + h[0], h[1], h[2]) for h in scan(v, path)]
    return hits


with report("ops_5064_fleet_gap_census") as R:
    fails = []
    out = {"op": "ops_5064", "gaps": []}

    R.section("P0 every state document in the fleet")
    docs = [k for k, _, _ in listing("data/_state/")
            if k.endswith(".json") or k.endswith(".json.gz")]
    warm = [k for k, _, _ in listing("data/warm/")
            if "/_state/" in k and k.endswith(".json")]
    R.log("  data/_state/: %d docs   data/warm/*/_state/: %d docs" % (
        len(docs), len(warm)))
    seen = {}
    for k in (docs + warm)[:400]:
        d = jget(k)
        if not isinstance(d, dict):
            continue
        for label, a, b in scan(d, k):
            if b < 5 or a == b:
                continue
            miss = b - a
            if miss <= 0:
                continue
            name = k.split("/")[-1].replace(".json", "")
            prev = seen.get(name)
            if prev and prev[1] >= miss:
                continue
            blob = json.dumps(d, default=str)[:1500]
            seen[name] = (label, miss, a, b, k,
                          bool(BLOCKED_HINT.search(blob)))

    R.section("P1 ranked by what is MISSING, not by percentage")
    rows = sorted(seen.items(), key=lambda kv: -kv[1][1])
    R.log("  %-30s %10s %10s %7s  %s" % ("state doc", "missing",
                                         "of total", "have%", "kind"))
    for name, (label, miss, a, b, k, blocked) in rows[:22]:
        R.log("  %-30s %10s %10s %6.1f%%  %s" % (
            name[:30], f"{miss:,}", f"{b:,}", 100.0 * a / b,
            "BLOCKED?" if blocked else "fixable"))
        out["gaps"].append({"doc": name, "field": label, "missing": miss,
                            "have": a, "total": b, "key": k,
                            "blocked_hint": blocked})
    R.log("  total documents with a measurable gap: %d" % len(rows))

    R.section("P2 the named lanes, checked directly")
    named = {
        "boj": "data/_state/boj-full.json",
        "oecd": "data/_state/sdmx-walk-oecd.json",
        "fred": "data/_state/fred-import.json",
        "gdelt": "data/_state/gdelt-full.json",
        "imf": "data/_state/imf-full.json",
        "worldbank": "data/_state/worldbank-full.json",
        "census-econ-s0": "data/_state/census-econ-s0.json",
    }
    for nm, key in named.items():
        d = jget(key)
        if not isinstance(d, dict):
            R.log("  %-14s %s -> not at that path" % (nm, key))
            continue
        keep = {k: v for k, v in d.items()
                if isinstance(v, (int, float, str))
                and k in ("phase", "n_done", "n_total", "series",
                          "n_series", "universe", "banked", "target",
                          "gaps", "cursor", "updated_at", "queue_left",
                          "rows_total", "failures", "denied")}
        R.log("  %-14s %s" % (nm, json.dumps(keep, default=str)[:150]))

    R.section("P3 fixable vs blocked")
    fixable = [g for g in out["gaps"] if not g["blocked_hint"]]
    blocked = [g for g in out["gaps"] if g["blocked_hint"]]
    R.log("  fixable lanes: %d, missing %s items total" % (
        len(fixable), f"{sum(g['missing'] for g in fixable):,}"))
    for g in fixable[:8]:
        R.log("    %-28s %s missing (%.0f%% held)" % (
            g["doc"][:28], f"{g['missing']:,}",
            100.0 * g["have"] / g["total"]))
    R.log("  looks blocked (credential/denied/by-design): %d" %
          len(blocked))
    for g in blocked[:8]:
        R.log("    %-28s %s missing" % (g["doc"][:28],
                                        f"{g['missing']:,}"))
    R.log("  EXPEDITE ORDER = the fixable list above, largest first.")
    R.log("  Blocked lanes are not slow, they are stopped -- chasing")
    R.log("  them is the waste.")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/fleet-gaps.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/fleet-gaps.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5064 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(gaps=len(out["gaps"]), fixable=len(fixable),
         blocked=len(blocked),
         top=(out["gaps"][0]["doc"] if out["gaps"] else "-"))
    R.log("ops 5064 GREEN -- fleet backlog quantified")
