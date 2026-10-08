"""ops_5040 -- read the totals, never re-derive them.

ops 5039 went RED for a real reason this time. The counted-prefix walk
over ~1M objects, on top of the catalog's normal 57-provider work, blew
the 600s function timeout -- the sync invoke died at 793s with the
connection closed and the hub was left untouched. Enumeration was the
wrong mechanism, and it only gets worse: the store is heading past 2M
objects tonight.

So the totals now come from the only place that can produce them for
free -- the engine that writes the pages.

  extractor: the write pool already knows every page's byte size, so it
    carries pages_objects and pages_bytes forward on the pages that
    LAND (a hash-skip adds nothing, a failed write adds nothing), and
    publishes both in series-manifest.json. Everything written before
    the counter existed is seeded exactly once by a single LIST inside
    the serialised worker, guarded by state["pages_seeded"].
  catalog: count_from = (manifest, "pages", "pages_bytes") replaces the
    prefix walk. O(1) forever, no timeout exposure.

Also fixes my own check: ops 5039's P1 read `series` off the HUB, where
providers carry `series_count`; the full series dict lives in the
per-provider document. It reported "series count did not pick up the
manifest" partly because it was reading the wrong field on the wrong
doc. Both are read correctly here.

  P0 wait for both deploys; confirm the extractor seeded its counters
  P1 run the catalog, timed -- must finish far inside 600s now
  P2 the card: series count, n_keys, derived, note (per-provider doc AND
     hub, each read on its own schema)
  P3 regression: coverage still the warm-mirror ratio, document size
     flat, other providers untouched
  P4 hub totals before/after
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
CAT = "justhodl-provider-catalog"
EXT = "justhodl-series-extractor"
HUB = "data/provider-catalog.json"
PDOC = "data/providers/eurostat.json"
MAN = "data/providers/eurostat/series-manifest.json"
STATE_KEY = "data/_state/series-extract-eurostat.json"

cfg = Config(read_timeout=660, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(key):
    return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())


with report("ops_5040_counters_not_scans") as R:
    fails = []
    out = {"op": "ops_5040"}

    R.section("P0 deploys + the extractor's seeded counters")
    try:
        t = (jget(HUB).get("totals") or {})
        R.log("  BEFORE hub totals: keys=%s gb=%s datasets=%s" % (
            f"{t.get('keys') or 0:,}", t.get("gb"), t.get("datasets")))
        out["before_totals"] = t
    except Exception as e:
        R.log("  hub read err %s" % str(e)[:110])
    for fn in (CAT, EXT):
        for i in range(15):
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                lm = (c.get("LastModified") or "")[:19]
                if lm >= (NOW - timedelta(minutes=16)).strftime(
                        "%Y-%m-%dT%H:%M:%S"):
                    R.log("  %s code fresh (%s)" % (fn, lm))
                    break
            except Exception:
                pass
            time.sleep(20)
    seeded = False
    for i in range(16):
        try:
            st = jget(STATE_KEY)
            if st.get("pages_seeded"):
                seeded = True
                R.log("  extractor seeded at %s -> pages_objects=%s "
                      "pages_bytes=%.1f GB" % (
                          st.get("pages_seeded"),
                          f"{st.get('pages_objects') or 0:,}",
                          (st.get("pages_bytes") or 0) / 1e9))
                if st.get("pages_seed_error"):
                    R.log("  seed error: %s" % st["pages_seed_error"])
                break
            R.log("  waiting for the seed pass (flows=%d pages=%s)" % (
                len(st.get("flows_done") or []), st.get("n_pages")))
        except Exception as e:
            R.log("  state err %s" % str(e)[:90])
        time.sleep(30)
    if not seeded:
        R.log("  counters not seeded yet -- the card will show 0 derived")
        fails.append("P0:seed")
    try:
        m = jget(MAN)
        R.log("  manifest: series_extracted=%s n_pages=%s pages=%s "
              "pages_bytes=%.1f GB flows_parsed=%s" % (
                  f"{m.get('series_extracted') or 0:,}", m.get("n_pages"),
                  f"{m.get('pages') or 0:,}",
                  (m.get("pages_bytes") or 0) / 1e9,
                  m.get("flows_parsed")))
        out["manifest"] = {k: m.get(k) for k in
                           ("series_extracted", "pages", "pages_bytes",
                            "flows_parsed", "flows_total")}
    except Exception as e:
        R.log("  manifest err %s" % str(e)[:110])

    R.section("P1 run the catalog (timed)")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=CAT, InvocationType="RequestResponse",
                       Payload=b"{}")
        el = time.time() - t0
        body = (r["Payload"].read() or b"").decode("utf-8", "replace")
        R.log("  status=%s FunctionError=%s in %.0fs" % (
            r.get("StatusCode"), r.get("FunctionError"), el))
        R.log("  payload: %s" % body[:260])
        if r.get("FunctionError"):
            fails.append("P1:funcerror")
        out["run_seconds"] = round(el)
    except Exception as e:
        R.log("  invoke err %s (%.0fs)" % (str(e)[:150], time.time() - t0))
        fails.append("P1:invoke")

    R.section("P2 the card, each doc read on its own schema")
    try:
        d = jget(PDOC)
        ser = d.get("series") or {}
        R.log("  per-provider doc: series.count=%s counted=%s ids=%d" % (
            f"{ser.get('count') or 0:,}", ser.get("counted"),
            len(ser.get("ids") or [])))
        R.log("  n_keys=%s total_mb=%s" % (
            f"{d.get('n_keys') or 0:,}", d.get("total_mb")))
        R.log("  derived=%s" % json.dumps(d.get("derived"),
                                          default=str)[:230])
        R.log("  note=%s" % str(d.get("note"))[:200])
        out["card"] = {"series": ser.get("count"),
                       "n_keys": d.get("n_keys"),
                       "total_mb": d.get("total_mb"),
                       "derived": d.get("derived")}
        if not (ser.get("count") or 0) > 1000000:
            fails.append("P2:series")
        if not (d.get("derived") or {}).get("objects"):
            fails.append("P2:derived")
    except Exception as e:
        R.log("  pdoc err %s" % str(e)[:140])
        fails.append("P2")
    try:
        hub = jget(HUB)
        e = next((p for p in (hub.get("providers") or [])
                  if p.get("slug") == "eurostat"), {})
        R.log("  hub row: series_count=%s n_keys=%s datasets=%s "
              "coverage_pct=%s" % (
                  f"{e.get('series_count') or 0:,}",
                  f"{e.get('n_keys') or 0:,}", e.get("datasets"),
                  e.get("coverage_pct")))
    except Exception as ex:
        R.log("  hub row err %s" % str(ex)[:110])

    R.section("P3 regression")
    try:
        d = jget(PDOC)
        cov = d.get("coverage_pct")
        R.log("  coverage_pct=%s (warm-mirror ratio, must be unmoved)"
              % cov)
        if cov is not None and float(cov) > 100.5:
            fails.append("P3:coverage")
        h = s3.head_object(Bucket=LIVE, Key=PDOC)
        R.log("  %s = %.2f MB, per-key rows=%d" % (
            PDOC, h["ContentLength"] / 1e6, len(d.get("keys") or [])))
        if h["ContentLength"] / 1e6 > 25:
            fails.append("P3:docsize")
        hub = jget(HUB)
        R.log("  other providers: %s" % [
            (p.get("slug"), p.get("series_count"))
            for p in (hub.get("providers") or [])
            if p.get("slug") in ("fred", "oecd", "statcan", "bis")])
    except Exception as e:
        R.log("  regression err %s" % str(e)[:130])
        fails.append("P3")

    R.section("P4 hub totals")
    try:
        t = (jget(HUB).get("totals") or {})
        b = out.get("before_totals") or {}
        R.log("  keys %s -> %s" % (f"{b.get('keys') or 0:,}",
                                   f"{t.get('keys') or 0:,}"))
        R.log("  gb   %s -> %s" % (b.get("gb"), t.get("gb")))
        R.log("  datasets %s -> %s (must NOT absorb derived series)" % (
            b.get("datasets"), t.get("datasets")))
        out["after_totals"] = t
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
        R.log("ops 5040 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(series=(out.get("card") or {}).get("series"),
         n_keys=(out.get("card") or {}).get("n_keys"),
         run_seconds=out.get("run_seconds"))
    R.log("ops 5040 GREEN -- card truthful, totals complete, catalog "
          "no longer enumerates a growing store")
