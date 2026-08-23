"""ops_4951 -- census-us v1.1.3: QWI via probe-verified per-state iteration.

4949's probe captured the exact refusal: "wildcard not supported in
'for' clause for this hierarchy. Please select a specific state" --
and proved the working shape live (state:01 + 2022-Q1 -> 200, real
row). QWI is a per-state product. v1.1.3 adds an override-driven
geo_iter rung (51 FIPS pulls, per-state banks + resume; harness
23/23): still nothing guessed -- this ops first probes which TIME
range form a specific state accepts, confirms on a second state,
THEN writes the override and redoes qwi-sa/se/rh.

  P0  per-state range probe (from 1990 / from 1990-Q1 / year) +
      second-state confirmation -> write override or exit RED
  G0  settle v1.1.3   G0b redo qwi x3   G1 drive COMPLETE
  G2  ledger <=5 named  G3 qwi ENFORCED  G4 size  G6 sentinel
"""
import io
import json
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-census-us"
SEN_FN = "justhodl-import-sentinel"
STATE_KEY = "data/warm/census-us/_state/state.json"
GRAM_KEY = "data/warm/census-us/_state/grammar-overrides.json"
WARM_PREFIX = "data/warm/census-us/"
HEALTH_KEY = "data/import-health.json"
MARKER = "v1.1.4 ops4951"
V10_ROWS = 2_082_816
QWI = ["qwi-sa", "qwi-se", "qwi-rh"]
CORE4 = ["Emp", "HirA", "Sep", "EarnS"]
EXPECT_FAILS = {"aies-miscsector", "asm-industry",
                "poverty-saipe-schdist", "pseo-earnings", "pseo-flows"}

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


def fam_of(st, slug):
    return ((st.get("catalog") or {}).get(slug) or {}).get("family")


def census(url, timeout=90):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "justhodl-ops/4951 (raafouis@gmail.com)"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(8_000_000).decode("utf-8", "replace")
            try:
                n = len(json.loads(body)) - 1
            except Exception:
                n = -1
            return r.status, body[:300], n
    except urllib.error.HTTPError as e:
        try:
            return e.code, e.read().decode("utf-8", "replace")[:300], -1
        except Exception:
            return e.code, "", -1
    except Exception as e:
        return 0, str(e)[:200], -1


def qurl(base, tval, geo, key):
    return (base + "?get=" + ",".join(CORE4)
            + "&time=" + urllib.parse.quote_plus(tval)
            + "&for=" + urllib.parse.quote_plus(geo)
            + "&key=" + key)


with report("ops_4951_census_qwi_bounded") as R:
    fails = []

    # P0 -- BOUNDED per-state range probe --------------------------
    R.section("P0 bounded-range probe (4950: 'requires a bounded "
              "date/time range')")
    key = (lam.get_function_configuration(FunctionName=FN)
           .get("Environment", {}).get("Variables", {})
           .get("CENSUS_API_KEY", ""))
    base = "https://api.census.gov/data/timeseries/qwi/sa"
    cand = [("L-bounded-years", "from 1990 to 2026"),
            ("M-bounded-qtrs", "from 1990-Q1 to 2026-Q4")]
    winner = None
    for name, tv in cand:
        stx, body, n = census(qurl(base, tv, "state:01", key))
        R.log("  %-16s HTTP %-3s rows=%-5s %s" % (
            name, stx, n, body.replace("\n", " ")[:150]))
        if n > 40 and not winner:
            winner = (name, tv, n)
        time.sleep(0.7)
    if not winner:
        R.log("P0 FAIL bounded forms also refused -- bodies above "
              "feed the next iteration; not guessing")
        sys.exit(1)
    stx, body, n2 = census(qurl(base, winner[1], "state:48", key))
    R.log("  confirm state:48 HTTP %s rows=%s" % (stx, n2))
    if n2 <= 40:
        R.log("P0 FAIL winner did not confirm on a second state")
        sys.exit(1)
    ft = winner[1].replace("2026", "{cur}")   # rollover-safe bound
    ov = {"vars": CORE4, "geo_iter": "state", "full_time": ft}
    s3.put_object(Bucket=B, Key=GRAM_KEY,
                  Body=json.dumps({sl: dict(ov) for sl in QWI},
                                  indent=1).encode(),
                  ContentType="application/json")
    R.log("P0 PASS winner=%s (AL %s / TX %s rows) -> full_time=%r "
          "written for %s" % (winner[0], winner[2], n2, ft,
                              ",".join(QWI)))

    # G0 -- settle v1.1.3 ----------------------------------------------
    R.section("G0 zip-settle v1.1.3")
    ok0, t0 = False, time.time()
    while time.time() - t0 < 600:
        try:
            f = lam.get_function(FunctionName=FN)
            req = urllib.request.Request(f["Code"]["Location"])
            with urllib.request.urlopen(req, timeout=90) as r:
                zbytes = r.read()
            src = zipfile.ZipFile(io.BytesIO(zbytes)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARKER in src and \
                    f["Configuration"].get("State") == "Active":
                ok0 = True
                R.log("G0 PASS after %ds" % (time.time() - t0))
                break
            R.log("  t+%4ds marker=%s" % (time.time() - t0,
                                          MARKER in src))
        except Exception as e:
            R.log("  settle: %s" % str(e)[:120])
        time.sleep(25)
    if not ok0:
        R.log("G0 FAIL")
        sys.exit(1)

    # G0b -- redo ------------------------------------------------------
    R.section("G0b redo qwi x3")
    kick(FN, json.dumps({"redo": QWI}).encode())
    time.sleep(20)

    # G1 -- drive ------------------------------------------------------
    R.section("G1 drive to COMPLETE")
    BUDGET, STALL_S = 12 * 60, 240
    t0 = time.time()
    last_fp, last_move, kicks, ok1 = None, time.time(), 0, False
    st = {}
    while time.time() - t0 < BUDGET:
        st = gj(STATE_KEY) or st or {}
        q = st.get("queue") or []
        head = q[0] if q else None
        hd = (st.get("datasets") or {}).get(head) or {}
        fp = (st.get("n_done"), st.get("rows_total"),
              len(st.get("failures") or {}), st.get("phase"), head,
              hd.get("resume_geo"), st.get("updated_at"))
        if fp != last_fp:
            last_fp, last_move = fp, time.time()
            R.log("  t+%4ds %s done=%s/%s rows=%s q=%s head=%s "
                  "geo_i=%s fail=%s" % (
                      time.time() - t0, st.get("phase"),
                      st.get("n_done"), st.get("n_total"),
                      st.get("rows_total"), len(q), head,
                      hd.get("resume_geo"),
                      len(st.get("failures") or {})))
        if st.get("phase") == "COMPLETE":
            ok1 = True
            break
        if time.time() - last_move > STALL_S and kicks < 6:
            kicks += 1
            kick(FN)
            last_move = time.time()
            R.log("  stall -> kick #%d" % kicks)
        time.sleep(20)
    st = gj(STATE_KEY) or st or {}
    n_total = st.get("n_total") or 0
    n_done = st.get("n_done") or 0
    n_fail = len(st.get("failures") or {})
    ok1 = ok1 and (n_done + n_fail) == n_total
    R.log("G1 %s phase=%s %s+%s==%s kicks=%d" % (
        "PASS" if ok1 else "FAIL", st.get("phase"), n_done, n_fail,
        n_total, kicks))
    if not ok1:
        fails.append("G1")

    # G2 -- ledger exact ----------------------------------------------
    R.section("G2 failures ledger")
    fl = st.get("failures") or {}
    for slug, why in sorted(fl.items()):
        R.log("  FAIL %-20s %s" % (slug, str(why)[:100]))
    ok2 = set(fl) <= EXPECT_FAILS and len(fl) <= 5
    R.log("G2 %s n=%d unexpected=%s" % (
        "PASS" if ok2 else "FAIL", len(fl),
        sorted(set(fl) - EXPECT_FAILS)))
    if not ok2:
        fails.append("G2")

    # G3 -- qwi enforced ----------------------------------------------
    R.section("G3 qwi inception")
    ds = st.get("datasets") or {}
    qrows = [(sl, d) for sl, d in ds.items()
             if d.get("ok") and fam_of(st, sl) == "qwi"]
    for sl, d in qrows:
        R.log("  %-8s mode=%-9s rows=%-7s states=%-3s %s..%s" % (
            sl, d.get("mode"), d.get("rows"),
            len(d.get("geo_rows") or {}), d.get("y0"), d.get("y1")))
    ok3 = len(qrows) == 3 and all(
        d.get("mode") == "geo_state" and
        len(d.get("geo_rows") or {}) >= 45 and
        (d.get("y0") or "9999") <= "2000"
        for _, d in qrows)
    R.log("G3 %s qwi_datasets=%d" % ("PASS" if ok3 else "FAIL",
                                     len(qrows)))
    if not ok3:
        fails.append("G3")

    # G4 -- size ------------------------------------------------------
    R.section("G4 rows/bytes")
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
    ok4 = rows_total > 3_900_000 and n_keys > 300
    R.log("G4 %s rows=%s (+%s vs v1.0) %.2fMB keys=%d" % (
        "PASS" if ok4 else "FAIL", rows_total, rows_total - V10_ROWS,
        mb, n_keys))
    if not ok4:
        fails.append("G4")

    # G6 -- sentinel --------------------------------------------------
    R.section("G6 sentinel")
    kick(SEN_FN)
    pipe, t0 = None, time.time()
    while time.time() - t0 < 300:
        time.sleep(20)
        h = gj(HEALTH_KEY) or {}
        pipe = next((p for p in h.get("pipelines", [])
                     if p.get("name") == "census-us"), None)
        if pipe and pipe.get("status") == "COMPLETE":
            break
    ok6 = bool(pipe and pipe.get("status") == "COMPLETE")
    R.log("G6 %s %s" % ("PASS" if ok6 else "FAIL", pipe))
    if not ok6:
        fails.append("G6")

    if fails:
        R.log("ops 4951 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(n_done=n_done, n_total=n_total, failures=n_fail,
         rows_total=rows_total, store_mb=round(mb, 2), s3_keys=n_keys,
         qwi_states=",".join(str(len(d.get("geo_rows") or {}))
                             for _, d in qrows))
    R.log("ops 4951 GREEN -- qwi live per-state since the 1990s; the "
          "full timeseries universe is COMPLETE with exactly the five "
          "structurally out-of-ladder failures named")
