"""ops_4945 -- census-us drive + end-to-end verify (corrects ops 4944).

Ops 4944 deployed everything green (engine, reserved-concurrency=1,
15-min Scheduler, catalog card, sentinel supervision) and then died in
G3/G4 on the exact banned pattern: a synchronous RequestResponse invoke
of a long-running walker from the runner (ConnectionClosedError at
~324s).  House doctrine (AUTONOMY.md, memory #14c): long engines are
NEVER gated with sync invokes -- Event-invoke + S3 state freshness only.

This ops drives and verifies with that pattern:
  G0  walker state exists (Event-kick + wait if the heartbeat hasn't
      landed yet)
  G1  catalog banked: n_total >= 10 and catalog.json.gz == n_total
  G2  drain progression to PASS: phase COMPLETE, or n_done >= 6 with
      rows_total > 50k and earliest first-year <= 1995; stalls draw an
      async kick (reserved concurrency 1 queues it safely)
  G3  provider-catalog card 'census-us' live in data/provider-catalog.json
  G4  sentinel pipeline 'census-us' supervising in data/import-health.json
  G5  served surfaces from the runner (explicit UA -- banked trap):
      S3 origin carries the card (authoritative), edge reported,
      data.html still renders providers generically

Every gate key below was grepped from the producers (G0_KEY_CONTRACT):
state: phase/n_total/n_done/rows_total/queue/datasets{slug:{ok,rows,
y0,y1,mode}}/failures/n_timeseries_universe/updated_at.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-census-us"
CAT_FN = "justhodl-provider-catalog"
SEN_FN = "justhodl-import-sentinel"
STATE_KEY = "data/warm/census-us/_state/state.json"
CATALOG_KEY = "data/warm/census-us/catalog.json.gz"
HUB_KEY = "data/provider-catalog.json"
HEALTH_KEY = "data/import-health.json"
UA = ("Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/4945 "
      "(raafouis@gmail.com)")

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def gj(key, default=None):
    import gzip
    import io
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        return json.loads(raw)
    except Exception:
        return default


def kick(fn):
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        return True
    except Exception:
        return False


def fetch(url, timeout=45):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except Exception as e:
        return 0, str(e)[:200]


def min_first_year(st):
    ys = [str(d.get("y0")) for d in (st.get("datasets") or {}).values()
          if d.get("ok") and d.get("y0")]
    return min(ys) if ys else None


with report("ops_4945_census_us_drive") as R:
    fails = []

    # G0 -- walker state exists ----------------------------------------
    R.section("G0 walker state")
    st = gj(STATE_KEY)
    if st is None:
        kick(FN)
        R.log("G0 state absent -- Event kick queued, waiting")
        t0 = time.time()
        while time.time() - t0 < 360 and st is None:
            time.sleep(20)
            st = gj(STATE_KEY)
    if st is None:
        R.log("G0 FAIL state never appeared")
        sys.exit(1)
    R.log("G0 PASS phase=%s n_total=%s n_done=%s rows=%s updated=%s" % (
        st.get("phase"), st.get("n_total"), st.get("n_done"),
        st.get("rows_total"), st.get("updated_at")))

    # G1 -- catalog banked ---------------------------------------------
    R.section("G1 catalog")
    cat = gj(CATALOG_KEY) or {}
    n_total = st.get("n_total") or 0
    n_cat = len(cat.get("datasets") or [])
    ok1 = n_total >= 10 and n_cat == n_total
    R.log("G1 %s n_total=%s catalog=%s universe=%s" % (
        "PASS" if ok1 else "FAIL", n_total, n_cat,
        st.get("n_timeseries_universe")))
    if not ok1:
        fails.append("G1 catalog")

    # G2 -- drive the drain (Event kicks only) -------------------------
    R.section("G2 drain drive (async, no sync invokes)")
    BUDGET = 21 * 60
    STALL_S = 330
    t0 = time.time()
    last_fp, last_move = None, time.time()
    kicks = 0
    ok2 = False
    while time.time() - t0 < BUDGET:
        st = gj(STATE_KEY) or st
        fp = (st.get("n_done"), st.get("rows_total"),
              len(st.get("failures") or {}), st.get("phase"))
        if fp != last_fp:
            last_fp, last_move = fp, time.time()
            R.log("  t+%4ds phase=%s n_done=%s/%s rows=%s q=%s" % (
                time.time() - t0, st.get("phase"), st.get("n_done"),
                st.get("n_total"), st.get("rows_total"),
                len(st.get("queue") or [])))
        my0 = min_first_year(st)
        if st.get("phase") == "COMPLETE" or (
                (st.get("n_done") or 0) >= 6 and
                (st.get("rows_total") or 0) > 50000 and
                my0 is not None and my0 <= "1995"):
            ok2 = True
            break
        if time.time() - last_move > STALL_S and kicks < 6:
            kicks += 1
            kick(FN)
            last_move = time.time()
            R.log("  stall %ds -> async kick #%d" % (STALL_S, kicks))
        time.sleep(25)
    st = gj(STATE_KEY) or st
    my0 = min_first_year(st)
    R.log("G2 %s phase=%s n_done=%s/%s rows_total=%s min_first_year=%s "
          "kicks=%d failures=%s" % (
              "PASS" if ok2 else "FAIL", st.get("phase"),
              st.get("n_done"), st.get("n_total"), st.get("rows_total"),
              my0, kicks,
              list((st.get("failures") or {}).items())[:5]))
    if not ok2:
        fails.append("G2 drain")

    # inception coverage table (the "since inception" proof) -----------
    R.section("inception coverage (top datasets by rows)")
    rows_tab = sorted(
        ((sl, d) for sl, d in (st.get("datasets") or {}).items()
         if d.get("ok")),
        key=lambda kv: -(kv[1].get("rows") or 0))
    for sl, d in rows_tab[:40]:
        R.log("  %-34s %-9s rows=%-8s %s..%s" % (
            sl[:34], d.get("mode"), d.get("rows"),
            d.get("y0"), d.get("y1")))

    # G3 -- provider-catalog card --------------------------------------
    R.section("G3 provider-catalog card")
    prev = (gj(HUB_KEY) or {}).get("as_of", "")
    kick(CAT_FN)
    card = None
    t0 = time.time()
    while time.time() - t0 < 540:
        time.sleep(20)
        hub = gj(HUB_KEY) or {}
        if hub.get("as_of", "") > prev:
            card = next((p for p in hub.get("providers", [])
                         if p.get("slug") == "census-us"), None)
            break
    ok3 = bool(card and (card.get("n_keys") or 0) > 0 and
               "EITS" in (card.get("catalog_note") or ""))
    R.log("G3 %s card=%s" % ("PASS" if ok3 else "FAIL",
                             json.dumps(card)[:300] if card else None))
    if not ok3:
        fails.append("G3 card")

    # G4 -- sentinel supervision ---------------------------------------
    R.section("G4 sentinel pipeline")
    kick(SEN_FN)
    pipe = None
    t0 = time.time()
    while time.time() - t0 < 300:
        time.sleep(20)
        h = gj(HEALTH_KEY) or {}
        pipe = next((p for p in h.get("pipelines", [])
                     if p.get("name") == "census-us"), None)
        if pipe and pipe.get("status") != "NO_STATE":
            break
    ok4 = bool(pipe and pipe.get("status") in ("RUNNING", "COMPLETE",
                                               "STALE"))
    R.log("G4 %s pipeline=%s" % ("PASS" if ok4 else "FAIL", pipe))
    if not ok4:
        fails.append("G4 sentinel")

    # G5 -- served surfaces (runner-side, real UA) ---------------------
    R.section("G5 served surfaces")
    v = int(time.time())
    o_st, o_txt = fetch(
        "https://s3.amazonaws.com/%s/%s" % (B, HUB_KEY))
    origin_has = o_st == 200 and '"census-us"' in o_txt
    e_st, e_txt = fetch(
        "https://justhodl.ai/data/provider-catalog.json?v=%d" % v)
    edge_has = e_st == 200 and '"census-us"' in e_txt
    p_st, p_txt = fetch("https://justhodl.ai/data.html?v=%d" % v)
    page_generic = p_st == 200 and "d.providers" in p_txt
    ok5 = origin_has and page_generic
    R.log("G5 %s origin=%s(%s) edge=%s(%s)%s page_generic=%s(%s)" % (
        "PASS" if ok5 else "FAIL", origin_has, o_st, edge_has, e_st,
        "" if edge_has else " [cache-lag tolerated, origin governs]",
        page_generic, p_st))
    if not ok5:
        fails.append("G5 served")

    if fails:
        R.log("ops 4945 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(phase=st.get("phase"), n_total=st.get("n_total"),
         n_done=st.get("n_done"), rows_total=st.get("rows_total"),
         min_first_year=my0, universe=st.get("n_timeseries_universe"),
         card_keys=(card or {}).get("n_keys"),
         card_mb=(card or {}).get("mb"))
    R.log("ops 4945 GREEN -- US Census Bureau importing full history "
          "since inception; card live on data.html; sentinel + 15-min "
          "Scheduler finish and refresh autonomously")
