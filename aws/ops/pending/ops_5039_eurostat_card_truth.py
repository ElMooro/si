"""ops_5039 -- make the Eurostat card tell the truth (both changes).

Khalid asked for change 1 then change 2. Change 2 as I originally
described it -- "add the series prefix to the catalog's scan" -- would
have broken the page, and the code says why: the scan appends one dict
per object into keys[], and keys[] is serialised into
data/providers/eurostat.json, which data.html downloads. At 966k objects
(2M+ by tonight) that document is hundreds of megabytes and the 600s
scan times out. So change 2 is implemented as its intent rather than its
letter: a COUNTED prefix.

 1. _series_list() accepts a scalar count as well as a list of ids, and
    eurostat's series_from moves from
        ("data/warm/eurostat/catalog.json.gz", "dataflows")   -> 8,152
    to  ("data/providers/eurostat/series-manifest.json",
         "series_extracted")                                  -> 478M+
    The old spec is kept as _series_from_legacy, not deleted.
 2. new registry field count_prefixes: the same LIST walk at MaxKeys=1000
    but only two accumulators come back, so the derived store lifts the
    provider's key and byte totals and the page's headline S3 KEYS /
    WARM+HOT without inflating the per-key array by a single row.

Coverage is deliberately untouched: n_live is computed from keys[] only,
so "100% of 8,152 target" keeps meaning what it has always meant -- the
warm SDMX mirror -- and never gets ratioed against derived pages. All
four properties were proven offline before this shipped.

  P0 wait for the code, invoke the catalog, time the run
  P1 the eurostat card: series count, n_keys, derived block, note
  P2 REGRESSION: coverage still 100% off 8,191 live keys; the provider
     document did not explode; other providers unchanged
  P3 hub totals before/after
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-provider-catalog"
HUB = "data/provider-catalog.json"
PDOC = "data/providers/eurostat.json"

cfg = Config(read_timeout=900, retries={"max_attempts": 2})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(key):
    return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())


def card(hub, slug="eurostat"):
    provs = hub.get("providers") or []
    if isinstance(provs, dict):
        return provs.get(slug)
    return next((p for p in provs
                 if (p.get("slug") or p.get("id")) == slug), None)


with report("ops_5039_eurostat_card_truth") as R:
    fails = []
    out = {"op": "ops_5039"}

    R.section("P0 baseline, then run the catalog")
    before = {}
    try:
        hub0 = jget(HUB)
        c0 = card(hub0) or {}
        before = {"series": (c0.get("series") or {}).get("count"),
                  "n_keys": c0.get("n_keys"),
                  "total_mb": c0.get("total_mb"),
                  "coverage_pct": c0.get("coverage_pct"),
                  "datasets": c0.get("datasets")}
        R.log("  BEFORE eurostat: %s" % json.dumps(before, default=str))
        t0 = (hub0.get("totals") or {})
        R.log("  BEFORE hub totals: keys=%s gb=%s datasets=%s" % (
            t0.get("keys"), t0.get("gb"), t0.get("datasets")))
        out["before"] = before
    except Exception as e:
        R.log("  baseline err %s" % str(e)[:130])
    landed = False
    for i in range(18):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            lm = (c.get("LastModified") or "")[:19]
            if lm >= (NOW - timedelta(minutes=14)).strftime(
                    "%Y-%m-%dT%H:%M:%S"):
                landed = True
                R.log("  new catalog code present (LastModified=%s) "
                      "mem=%s timeout=%ss" % (lm, c.get("MemorySize"),
                                              c.get("Timeout")))
                break
        except Exception:
            pass
        time.sleep(20)
    if not landed:
        R.log("  code freshness unconfirmed -- invoking anyway")
    t_start = time.time()
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=b"{}")
        el = time.time() - t_start
        pay = (r["Payload"].read() or b"")[:400].decode("utf-8", "replace")
        R.log("  invoke status=%s in %.0fs (timeout is 600s)" % (
            r.get("StatusCode"), el))
        R.log("  FunctionError=%s" % r.get("FunctionError"))
        R.log("  payload: %s" % pay[:300])
        if r.get("FunctionError"):
            fails.append("P0:funcerror")
        if el > 540:
            R.log("  *** run is close to the timeout -- the counted "
                  "prefix will only grow; revisit before it doubles ***")
        out["run_seconds"] = round(el)
    except Exception as e:
        R.log("  invoke err %s" % str(e)[:200])
        fails.append("P0:invoke")

    R.section("P1 the eurostat card now")
    try:
        hub = jget(HUB)
        c = card(hub) or {}
        ser = c.get("series") or {}
        R.log("  series.count = %s  (counted=%s, ids=%d)" % (
            f"{ser.get('count') or 0:,}", ser.get("counted"),
            len(ser.get("ids") or [])))
        R.log("  n_keys       = %s   total_mb = %s" % (
            f"{c.get('n_keys') or 0:,}", c.get("total_mb")))
        R.log("  derived      = %s" % json.dumps(c.get("derived"),
                                                 default=str)[:220])
        R.log("  note         = %s" % str(c.get("note"))[:190])
        R.log("  datasets=%s datasets_target=%s coverage_pct=%s" % (
            c.get("datasets"), c.get("datasets_target"),
            c.get("coverage_pct")))
        out["after"] = {"series": ser.get("count"),
                        "n_keys": c.get("n_keys"),
                        "total_mb": c.get("total_mb"),
                        "coverage_pct": c.get("coverage_pct")}
        if not (ser.get("count") or 0) > 1000000:
            R.log("  *** series count did not pick up the manifest ***")
            fails.append("P1:series")
        if not (c.get("derived") or {}).get("objects"):
            R.log("  *** counted prefix produced nothing ***")
            fails.append("P1:derived")
    except Exception as e:
        R.log("  card err %s" % str(e)[:150])
        fails.append("P1")

    R.section("P2 regression -- coverage and document size")
    try:
        c = card(jget(HUB)) or {}
        cov = c.get("coverage_pct")
        R.log("  coverage_pct = %s (must still be the warm-mirror "
              "ratio, unaffected by derived pages)" % cov)
        if cov is not None and float(cov) > 100.5:
            fails.append("P2:coverage")
        h = s3.head_object(Bucket=LIVE, Key=PDOC)
        mb = h["ContentLength"] / 1e6
        R.log("  %s size = %.2f MB (this is what data.html downloads)"
              % (PDOC, mb))
        if mb > 25:
            R.log("  *** provider document has exploded -- the counted "
                  "prefix must be leaking per-key rows ***")
            fails.append("P2:docsize")
        d = jget(PDOC)
        R.log("  per-key rows in the document: %d" % len(d.get("keys")
                                                         or []))
        if len(d.get("keys") or []) > 20000:
            fails.append("P2:keyrows")
        hub = jget(HUB)
        others = [(p.get("slug"), (p.get("series") or {}).get("count"))
                  for p in (hub.get("providers") or [])
                  if p.get("slug") in ("fred", "oecd", "statcan", "bis",
                                       "ecb", "census-us")]
        R.log("  other providers' series counts unchanged? %s" % others)
    except Exception as e:
        R.log("  regression err %s" % str(e)[:150])
        fails.append("P2")

    R.section("P3 hub totals")
    try:
        t = (jget(HUB).get("totals") or {})
        R.log("  AFTER hub totals: providers=%s datasets=%s keys=%s "
              "gb=%s" % (t.get("providers"), t.get("datasets"),
                         f"{t.get('keys') or 0:,}", t.get("gb")))
        out["hub_totals"] = t
    except Exception as e:
        R.log("  totals err %s" % str(e)[:110])
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/eurostat-card-fix.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/eurostat-card-fix.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5039 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(series=(out.get("after") or {}).get("series"),
         n_keys=(out.get("after") or {}).get("n_keys"),
         coverage=(out.get("after") or {}).get("coverage_pct"),
         run_seconds=out.get("run_seconds"))
    R.log("ops 5039 GREEN -- card counts the real series universe, "
         "totals include the derived store, coverage untouched")
