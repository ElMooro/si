"""ops_5057 -- how much of the Census API do we actually have?

The card reads "full timeseries universe since inception: 55/56
datasets". Both halves are true and together they mislead: it is the
full TIMESERIES universe, and timeseries is one family of the Census
API. The engine says so itself --

    line 226:  if not cd or cd[0] != "timeseries" or len(cd) < 2: continue
    line 18:   "...remains out of timeseries scope by design"

Everything else api.census.gov publishes -- ACS detailed/subject/profile
tables, the decennial census, County Business Patterns, the economic
census, population estimates, CPS, SIPP -- has never been fetched. Not a
failure; a scope that was set once and then quietly became the headline.

Before importing anything this op measures the real universe, because
"import all of it with all the history" over ACS at every geography
level is a multi-terabyte commitment and nobody should start that
blind. api.census.gov/data.json is the Bureau's own machine-readable
catalogue of every dataset-vintage it serves.

  P0 the full catalogue: entries, timeseries vs the rest
  P1 non-timeseries grouped by family, with vintage span -- "all the
     history" means every vintage, so this is the real denominator
  P2 what we hold today, from the walker's own state and S3
  P3 size probe: variable and geography counts for the biggest families,
     turned into an honest call-count and volume estimate
  P4 verdict and a scoped recommendation

Read-only. Nothing is fetched into the warehouse here.
"""
import json
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
DATA_JSON = "https://api.census.gov/data.json"
CSTATE = "data/warm/census-us/_state/state.json"
WARM = "data/warm/census-us/"
UA = "Mozilla/5.0 (compatible; JustHodlAI/1.0) ops-5057"

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)


def http(url, cap=90 * 1024 * 1024):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=110) as r:
            return r.status, r.read(cap)
    except Exception as e:
        return getattr(e, "code", -1), str(e)[:150].encode()


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=k)["Body"].read())
    except Exception:
        return {}


with report("ops_5057_census_universe") as R:
    fails = []
    out = {"op": "ops_5057"}

    R.section("P0 the Bureau's own catalogue")
    st, raw = http(DATA_JSON)
    R.log("  GET data.json -> HTTP %s, %.1f MB" % (st, len(raw) / 1e6))
    if st != 200:
        R.log("  cannot read the catalogue")
        sys.exit(1)
    cat = json.loads(raw)
    items = cat.get("dataset") or []
    R.log("  catalogue entries (dataset x vintage): %s" % f"{len(items):,}")
    ts, other = [], []
    for d in items:
        cd = d.get("c_dataset") or []
        (ts if (cd and cd[0] == "timeseries") else other).append(d)
    R.log("  timeseries entries      : %s" % f"{len(ts):,}")
    R.log("  everything else         : %s  <- never fetched" % (
        f"{len(other):,}"))
    out.update(entries=len(items), timeseries=len(ts), other=len(other))

    R.section("P1 non-timeseries families, with vintage span")
    fam = defaultdict(lambda: {"n": 0, "vintages": set(), "ex": None})
    for d in other:
        cd = d.get("c_dataset") or ["(none)"]
        key = cd[0]
        f = fam[key]
        f["n"] += 1
        v = d.get("c_vintage")
        if v:
            f["vintages"].add(int(v))
        if not f["ex"]:
            f["ex"] = "/".join(str(x) for x in cd)
    rows = sorted(fam.items(), key=lambda kv: -kv[1]["n"])
    R.log("  %-14s %7s  %-11s  %s" % ("family", "entries", "vintages",
                                      "example"))
    for k, v in rows[:22]:
        vs = sorted(v["vintages"])
        R.log("  %-14s %7d  %-11s  %s" % (
            k[:14], v["n"],
            ("%d–%d" % (vs[0], vs[-1])) if vs else "—", v["ex"][:44]))
    R.log("  families outside timeseries: %d" % len(fam))
    out["families"] = {k: {"entries": v["n"],
                           "vintages": len(v["vintages"])}
                       for k, v in rows[:30]}

    R.section("P2 what we hold today")
    cst = jget(CSTATE)
    R.log("  walker: phase=%s n_done=%s/%s rows=%s universe=%s "
          "updated_at=%s" % (
              cst.get("phase"), cst.get("n_done"), cst.get("n_total"),
              f"{cst.get('rows_total') or 0:,}",
              cst.get("n_timeseries_universe"), cst.get("updated_at")))
    n, byts = 0, 0
    kw = {"Bucket": LIVE, "Prefix": WARM, "MaxKeys": 1000}
    while True:
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            n += 1
            byts += o["Size"]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    R.log("  S3: %s objects, %.1f MB under %s" % (f"{n:,}",
                                                  byts / 1e6, WARM))
    held = len(cst.get("done") or cst.get("datasets_done") or [])
    R.log("  coverage of the FULL catalogue: %s of %s entries = %.2f%%"
          % (f"{cst.get('n_done') or held or 0:,}", f"{len(items):,}",
             100.0 * (cst.get("n_done") or held or 0) / max(1, len(items))))
    out["held_rows"] = cst.get("rows_total")
    out["held_mb"] = round(byts / 1e6, 1)

    R.section("P3 size probe on the biggest families")
    probes = [k for k, _ in rows[:6]]
    est_calls = 0
    for key in probes:
        d = next((x for x in other
                  if (x.get("c_dataset") or [None])[0] == key), None)
        if not d:
            continue
        cd = "/".join(str(x) for x in (d.get("c_dataset") or []))
        vint = d.get("c_vintage")
        base = ("https://api.census.gov/data/%s/%s" % (vint, cd)
                if vint else "https://api.census.gov/data/%s" % cd)
        sv, bv = http(base + "/variables.json", cap=40 * 1024 * 1024)
        nv = 0
        if sv == 200:
            try:
                nv = len((json.loads(bv).get("variables") or {}))
            except Exception:
                nv = -1
        sg, bg = http(base + "/geography.json", cap=8 * 1024 * 1024)
        ng = 0
        if sg == 200:
            try:
                ng = len((json.loads(bg).get("fips") or []))
            except Exception:
                ng = -1
        ent = fam[key]["n"]
        calls = ent * max(1, ng) * max(1, (nv // 45) + 1)
        est_calls += calls
        R.log("  %-14s %5d entries · %5s variables · %3s geo levels "
              "-> ~%s calls at 45 vars/call" % (
                  key[:14], ent, nv, ng, f"{calls:,}"))
    R.log("  rough call estimate for these %d families alone: ~%s "
          "requests" % (len(probes), f"{est_calls:,}"))
    R.log("  (Census caps at 500/day without a key; with a key it is "
          "generous but not unlimited -- this is a rate-limit problem "
          "before it is a storage problem)")
    out["est_calls_top6"] = est_calls

    R.section("P4 verdict")
    R.log("  NO -- we do not have all of it. We have the timeseries")
    R.log("  family only: %s of %s catalogue entries (%.2f%%)." % (
        f"{len(ts):,}", f"{len(items):,}",
        100.0 * len(ts) / max(1, len(items))))
    R.log("  Missing entirely: %s entries across %d families, including "
          "ACS, the decennial census, CBP and the economic census." % (
              f"{len(other):,}", len(fam)))
    R.log("  'All the history' for those means every vintage, which the")
    R.log("  vintage spans above make explicit.")
    R.log("  RECOMMENDATION: this is not one import, it is a program.")
    R.log("  Scope it family by family, largest value first, each with")
    R.log("  its own resumable walker on the pattern the eurostat/ecb")
    R.log("  lanes now use. Starting with ACS at every geography level")
    R.log("  would be the most expensive possible first move.")
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/census-universe.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/census-universe.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5057 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(entries=out.get("entries"), timeseries=out.get("timeseries"),
         missing=out.get("other"),
         coverage_pct=round(100.0 * len(ts) / max(1, len(items)), 2))
    R.log("ops 5057 GREEN -- census universe measured")
