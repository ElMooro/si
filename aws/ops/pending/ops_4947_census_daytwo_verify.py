"""ops_4947 -- census-us v1.1.0 day-two: drive to COMPLETE + verify.

4946 left the walker at 32/56 with the 15-min heartbeat draining the
heavy remainder (bds 1978->, qwi state grids, govs family, saipe/
sahie, pseo, hhpulse/hps, soma). This ops:

  G1  drives/waits the drain to phase=COMPLETE (Event kicks only on
      genuine stall; Scheduler keeps ticking regardless), then checks
      the accounting identity n_done + failures == n_total
  G2  failures ledger: every failure NAMED with a reason; ledger <= 5
      (>=3 grammar-hostile sets queues a fix-op decision, logged)
  G3  inception spot-proof by family: bds reaches the 1970s, qwi
      reaches <=2000, govs* >= 3 sets ok, poverty + healthins ok
  G4  final size census: rows_total floor + S3 byte/key walk of
      data/warm/census-us/ (must exceed the 11.36MB v1.0 footprint)
  G5  "1..1" watchlist (m3/advm3/bfs/mhs2/qtax): LOG ONLY -- verified
      source-read: refresh() re-pulls full artifacts and recomputes
      y0/y1 via fixed year_span, so these heal on tomorrow's refresh;
      forcing today = save-race risk for a cosmetic (no gate)
  G6  sentinel pipeline census-us reports COMPLETE

No lambda source in this push => [skip-deploy]; engine marker was
zip-settled GREEN in 4946. Event-invoke + S3-poll house standard.
"""
import io
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-census-us"
SEN_FN = "justhodl-import-sentinel"
STATE_KEY = "data/warm/census-us/_state/state.json"
WARM_PREFIX = "data/warm/census-us/"
HEALTH_KEY = "data/import-health.json"
V10_ROWS = 2_082_816
V10_MB = 11.36
ONE_ONE = ["m3", "advm3", "bfs", "mhs2", "qtax"]

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def gj(key, default=None):
    import gzip
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        return json.loads(raw)
    except Exception:
        return default


def kick(fn, payload=b"{}"):
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event",
                   Payload=payload)
        return True
    except Exception:
        return False


def age_s(iso):
    try:
        t = datetime.fromisoformat(iso)
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - t).total_seconds()
    except Exception:
        return 1e9


def fam_of(st, slug):
    return ((st.get("catalog") or {}).get(slug) or {}).get("family")


with report("ops_4947_census_daytwo_verify") as R:
    fails = []

    # G1 -- drive to COMPLETE ------------------------------------------
    R.section("G1 drain to COMPLETE (stall-kicks only)")
    st = gj(STATE_KEY) or {}
    if st.get("phase") == "DRAIN" and age_s(st.get("updated_at", "")) > 120:
        kick(FN)
        R.log("  cold start: state %.0fs stale -> async kick"
              % age_s(st.get("updated_at", "")))
    BUDGET, STALL_S = 25 * 60, 300
    t0 = time.time()
    last_fp, last_move, kicks, ok1 = None, time.time(), 0, False
    while time.time() - t0 < BUDGET:
        st = gj(STATE_KEY) or st or {}
        fp = (st.get("n_done"), st.get("rows_total"),
              len(st.get("failures") or {}), st.get("phase"))
        if fp != last_fp:
            last_fp, last_move = fp, time.time()
            R.log("  t+%4ds phase=%s n_done=%s/%s rows=%s q=%s fail=%s"
                  % (time.time() - t0, st.get("phase"),
                     st.get("n_done"), st.get("n_total"),
                     st.get("rows_total"),
                     len(st.get("queue") or []),
                     len(st.get("failures") or {})))
        if st.get("phase") == "COMPLETE":
            ok1 = True
            break
        if time.time() - last_move > STALL_S and kicks < 5:
            kicks += 1
            kick(FN)
            last_move = time.time()
            R.log("  stall %ds -> async kick #%d" % (STALL_S, kicks))
        time.sleep(30)
    st = gj(STATE_KEY) or st or {}
    n_total = st.get("n_total") or 0
    n_done = st.get("n_done") or 0
    n_fail = len(st.get("failures") or {})
    ident = (n_done + n_fail) == n_total
    ok1 = ok1 and ident
    R.log("G1 %s phase=%s identity %s+%s==%s -> %s kicks=%d" % (
        "PASS" if ok1 else "FAIL", st.get("phase"), n_done, n_fail,
        n_total, ident, kicks))
    if not ok1:
        fails.append("G1 completion")

    # G2 -- failures ledger --------------------------------------------
    R.section("G2 failures ledger (named, bounded)")
    fl = st.get("failures") or {}
    for slug, why in sorted(fl.items()):
        R.log("  FAIL %-18s %s" % (slug, str(why)[:90]))
    named = all(isinstance(v, str) and v.strip() for v in fl.values())
    ok2 = len(fl) <= 5 and named
    R.log("G2 %s n_failures=%d all_named=%s%s" % (
        "PASS" if ok2 else "FAIL", len(fl), named,
        " -- >=3 hostile: queue grammar-fix op decision"
        if len(fl) >= 3 else ""))
    if not ok2:
        fails.append("G2 failures ledger")

    # G3 -- inception spot-proof by family -----------------------------
    R.section("G3 inception proof (new families)")
    ds = st.get("datasets") or {}

    def fam_ok(fam):
        return [(sl, d) for sl, d in ds.items()
                if d.get("ok") and fam_of(st, sl) == fam]

    bds = fam_ok("bds")
    bds_ok = any((d.get("y0") or "9999") <= "1980" for _, d in bds)
    qwi = fam_ok("qwi")
    qwi_ok = any((d.get("y0") or "9999") <= "2000" for _, d in qwi)
    govs_n = sum(len(fam_ok(f)) for f in
                 ("govs", "govsemp", "govslocalfin", "govspension",
                  "govsschfin", "govsstatefin", "govsstatetax"))
    pov = fam_ok("poverty")
    hins = fam_ok("healthins")
    for tag, rowsv in (("bds", bds), ("qwi", qwi), ("poverty", pov),
                       ("healthins", hins)):
        for sl, d in rowsv:
            R.log("  %-9s %-24s tp=%-4s mode=%-10s rows=%-8s %s..%s"
                  % (tag, sl, d.get("tp") or "time", d.get("mode"),
                     d.get("rows"), d.get("y0"), d.get("y1")))
    ok3 = bds_ok and qwi_ok and govs_n >= 3 and bool(pov) and bool(hins)
    R.log("G3 %s bds_1970s=%s qwi<=2000=%s govs_ok=%d poverty=%d "
          "healthins=%d" % ("PASS" if ok3 else "FAIL", bds_ok, qwi_ok,
                            govs_n, len(pov), len(hins)))
    if not ok3:
        fails.append("G3 inception")

    # inception table (new families first, like 4946) ------------------
    R.section("inception coverage table")
    tab = sorted(((sl, d) for sl, d in ds.items() if d.get("ok")),
                 key=lambda kv: (fam_of(st, kv[0]) == "eits",
                                 -(kv[1].get("rows") or 0)))
    for sl, d in tab[:50]:
        R.log("  %-11s %-28s %-10s tp=%-4s rows=%-8s %s..%s" % (
            (fam_of(st, sl) or "?")[:11], sl[:28], d.get("mode"),
            d.get("tp") or "time", d.get("rows"),
            d.get("y0"), d.get("y1")))

    # G4 -- final size census ------------------------------------------
    R.section("G4 final rows/bytes census")
    total_b, n_keys, tok = 0, 0, None
    while True:
        kw = dict(Bucket=B, Prefix=WARM_PREFIX, MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        resp = s3.list_objects_v2(**kw)
        for o in resp.get("Contents", []):
            total_b += o["Size"]
            n_keys += 1
        if not resp.get("IsTruncated"):
            break
        tok = resp.get("NextContinuationToken")
    mb = total_b / 1e6
    rows_total = st.get("rows_total") or 0
    ok4 = rows_total > 2_150_000 and mb > V10_MB and n_keys > 44
    R.log("G4 %s rows_total=%s (+%s vs v1.0) store=%.2fMB (v1.0 %.2f) "
          "keys=%d" % ("PASS" if ok4 else "FAIL", rows_total,
                       rows_total - V10_ROWS, mb, V10_MB, n_keys))
    if not ok4:
        fails.append("G4 size")

    # G5 -- "1..1" watchlist (log only, self-heals tomorrow) -----------
    R.section("G5 header-fix watchlist (no gate; heals on refresh)")
    for sl in ONE_ONE:
        d = ds.get(sl) or {}
        R.log("  %-6s y=%s..%s refreshed=%s (expect real span after "
              "next daily refresh)" % (sl, d.get("y0"), d.get("y1"),
                                       d.get("refreshed")))

    # G6 -- sentinel ---------------------------------------------------
    R.section("G6 sentinel pipeline")
    kick(SEN_FN)
    pipe, t0 = None, time.time()
    while time.time() - t0 < 300:
        time.sleep(20)
        h = gj(HEALTH_KEY) or {}
        pipe = next((p for p in h.get("pipelines", [])
                     if p.get("name") == "census-us"), None)
        if pipe and "COMPLETE" in str(pipe.get("status", "")):
            break
    ok6 = bool(pipe and pipe.get("status") == "COMPLETE")
    R.log("G6 %s pipeline=%s" % ("PASS" if ok6 else "FAIL", pipe))
    if not ok6:
        fails.append("G6 sentinel")

    if fails:
        R.log("ops 4947 RED: " + "; ".join(fails))
        sys.exit(1)
    new_done = sum(1 for sl, d in ds.items()
                   if d.get("ok") and fam_of(st, sl) != "eits")
    R.kv(phase=st.get("phase"), n_done=n_done, n_total=n_total,
         failures=n_fail, rows_total=rows_total,
         rows_added_vs_v10=rows_total - V10_ROWS,
         store_mb=round(mb, 2), s3_keys=n_keys,
         new_family_datasets=new_done,
         families=",".join(sorted(set(st.get("families") or []))))
    R.log("ops 4947 GREEN -- full-universe drain COMPLETE and verified; "
          "'1..1' spans heal on tomorrow's scheduled refresh; "
          "remaining day-three check: header-fix confirmation only")
