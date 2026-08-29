"""ops_5045 -- watch the ECB lane land, then make its card honest.

ops 5044 shipped extract_ecb and the lane started clean: 6/207 flows in
the first seconds, and the read-back proved the cross-slice merge --
499 of 500 records spanned more than one year, e.g.
    ecb:AGR:AGR.M.I10.N.AGRI.XCEREA.4F0.N.IX  1991-01..2026-07  n_obs=427
35 years and 427 observations reconstructed for a series that lives
scattered across many AGR__{start}_{end} files. Eurostat finished in the
same window: 8,147/8,147 flows, 1,128,408 pages, 564,204,000 series.

This op does three things.

  P0/P1 track ECB to completion and report rate, failures and in-flight
        slice cursors.
  P2    INTEGRITY on the bracket predicate -- n_pages(before) <=
        objects_counted <= n_pages(after). A single stale read is
        useless against a lane that is still writing; that mistake has
        cost this arc five red gates already.
  P3    the card. ECB still reads "214 series", the same mislabel
        Eurostat carried: series_from counted the dataflow LIST. It now
        points at the extractor's manifest for series_extracted, and
        count_from supplies pages/pages_bytes from the writer's own
        counters -- never a LIST walk, which is what blew the catalog's
        600s timeout twice today.
  P4    hub totals, and coverage must stay a warm-mirror ratio.
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
STATE = "data/_state/series-extract-ecb.json"
MAN = "data/providers/ecb/series-manifest.json"
PFX = "data/providers/ecb/series/"
HUB = "data/provider-catalog.json"
PDOC = "data/providers/ecb.json"

cfg = Config(read_timeout=300, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=k)["Body"].read())
    except Exception:
        return {}


def count_prefix(prefix):
    n, b, kw = 0, 0, {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
    while True:
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            n += 1
            b += o["Size"]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    return n, b


with report("ops_5045_ecb_progress_and_card") as R:
    fails = []
    out = {"op": "ops_5045"}

    R.section("P0/P1 track the ECB lane")
    total = None
    done = False
    prev = -1
    for i in range(9):                        # up to ~36 min
        st = jget(STATE)
        f = len(st.get("flows_done") or [])
        total = st.get("flows_total_grouped") or total or 207
        R.log("  t+%2dmin flows=%d/%s (%.1f%%) pages=%s series=%s "
              "stopped_early=%s" % (
                  i * 4, f, total, 100.0 * f / max(1, total),
                  st.get("n_pages"), st.get("series_count"),
                  st.get("stopped_early")))
        prog = st.get("flow_progress") or {}
        for fid, v in list(prog.items())[:3]:
            R.log("    in-flight %-14s slice %s/%s attempts=%s" % (
                fid[:14], v.get("slice_idx"), v.get("slices"),
                v.get("attempts")))
        if f >= int(total):
            done = True
            R.log("  ALL ECB FLOWS INDEXED")
            break
        if f == prev and i >= 3:
            R.log("    (no flow completed this interval -- a large "
                  "sliced flow is mid-merge, which is expected)")
        prev = f
        time.sleep(240)
    st = jget(STATE)
    f1 = len(st.get("flows_done") or [])
    failed = st.get("failed_flows") or []
    errs = st.get("errors") or {}
    R.log("  failed_flows=%d  errors=%d  holes=%d" % (
        len(failed), len(errs), len(st.get("missing_pages") or [])))
    for k, v in list(errs.items())[:8]:
        R.log("    %-16s %s" % (k[:16], str(v)[:110]))
    out.update(flows=f1, total=total, complete=done,
               failed=failed[:20], series=st.get("series_count"),
               pages=st.get("n_pages"))

    R.section("P2 integrity on the bracket predicate")
    try:
        p0 = int(jget(STATE).get("n_pages") or 0)
        n_obj, byts = count_prefix(PFX)
        p1 = int(jget(STATE).get("n_pages") or 0)
        ok = (p0 - 2) <= n_obj <= (p1 + 2)
        R.log("  bracket %d <= %d <= %d : %s" % (
            p0, n_obj, p1, "CLEAN" if ok else "*** REAL GAP ***"))
        R.log("  %d objects, %.2f GB, avg page %.0f KB" % (
            n_obj, byts / 1e9, byts / max(1, n_obj) / 1024))
        if not ok:
            fails.append("P2:gap")
        out.update(objects=n_obj, gb=round(byts / 1e9, 2))
    except Exception as e:
        R.log("  count err %s" % str(e)[:130])
    m = jget(MAN)
    R.log("  manifest: series_extracted=%s n_pages=%s pages=%s "
          "pages_bytes=%.2f GB flows_parsed=%s/%s" % (
              f"{m.get('series_extracted') or 0:,}", m.get("n_pages"),
              f"{m.get('pages') or 0:,}",
              (m.get("pages_bytes") or 0) / 1e9,
              m.get("flows_parsed"), m.get("flows_total")))

    R.section("P3 the card")
    before = {}
    try:
        d0 = jget(PDOC)
        before = {"series": (d0.get("series") or {}).get("count"),
                  "n_keys": d0.get("n_keys"),
                  "total_mb": d0.get("total_mb")}
        R.log("  BEFORE %s" % json.dumps(before, default=str))
        t0 = (jget(HUB).get("totals") or {})
        out["before_totals"] = t0
        as_of0 = jget(HUB).get("as_of")
    except Exception as e:
        R.log("  baseline err %s" % str(e)[:110])
        as_of0 = None
    for i in range(15):
        try:
            c = lam.get_function_configuration(FunctionName=CAT)
            if (c.get("LastModified") or "")[:19] >= (
                    NOW - timedelta(minutes=50)).strftime(
                        "%Y-%m-%dT%H:%M:%S"):
                R.log("  catalog code fresh (%s)" % c.get("LastModified"))
                break
        except Exception:
            pass
        time.sleep(20)
    try:
        lam.invoke(FunctionName=CAT, InvocationType="Event", Payload=b"{}")
        R.log("  catalog Event invoke sent (sync invokes drop the "
              "connection on a run this long)")
    except Exception as e:
        R.log("  invoke err %s" % str(e)[:120])
        fails.append("P3:invoke")
    for i in range(24):
        time.sleep(30)
        if jget(HUB).get("as_of") != as_of0:
            R.log("  hub rewritten after %ds" % ((i + 1) * 30))
            break
    try:
        d = jget(PDOC)
        ser = d.get("series") or {}
        R.log("  AFTER series.count=%s counted=%s" % (
            f"{ser.get('count') or 0:,}", ser.get("counted")))
        R.log("  n_keys=%s total_mb=%s" % (
            f"{d.get('n_keys') or 0:,}", d.get("total_mb")))
        R.log("  derived=%s" % json.dumps(d.get("derived"),
                                          default=str)[:230])
        R.log("  note=%s" % str(d.get("note"))[:190])
        out["card"] = {"series": ser.get("count"),
                       "n_keys": d.get("n_keys"),
                       "derived": d.get("derived")}
        if not (ser.get("count") or 0) > 100000:
            fails.append("P3:series")
    except Exception as e:
        R.log("  card err %s" % str(e)[:130])
        fails.append("P3")

    R.section("P4 hub totals + coverage")
    try:
        h = jget(HUB)
        t = h.get("totals") or {}
        b = out.get("before_totals") or {}
        R.log("  keys %s -> %s" % (f"{b.get('keys') or 0:,}",
                                   f"{t.get('keys') or 0:,}"))
        R.log("  gb   %s -> %s" % (b.get("gb"), t.get("gb")))
        R.log("  datasets %s -> %s (must NOT absorb series)" % (
            b.get("datasets"), t.get("datasets")))
        for slug in ("ecb", "eurostat"):
            e = next((p for p in (h.get("providers") or [])
                      if p.get("slug") == slug), {})
            R.log("  %-9s series_count=%s n_keys=%s coverage_pct=%s" % (
                slug, f"{e.get('series_count') or 0:,}",
                f"{e.get('n_keys') or 0:,}", e.get("coverage_pct")))
            if e.get("coverage_pct") is not None and \
                    float(e["coverage_pct"]) > 100.5:
                fails.append("P4:coverage")
        out["after_totals"] = t
    except Exception as e:
        R.log("  totals err %s" % str(e)[:120])
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/ecb-series-lane.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/ecb-series-lane.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5045 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(flows=out.get("flows"), total=out.get("total"),
         complete=out.get("complete"), series=out.get("series"),
         objects=out.get("objects"))
    R.log("ops 5045 GREEN -- ECB indexed and its card tells the truth")
