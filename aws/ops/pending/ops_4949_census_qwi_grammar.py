"""ops_4949 -- census-us v1.1.2: probe-verified QWI grammar + clean COMPLETE.

4948 left 8 named failures; qwi-sa/se/rh (state workforce grids, HTTP
400 on every rung) are too valuable to leave. House rule: NEVER guess
grammar -- this ops probes api.census.gov LIVE from the runner, records
the actual 400 bodies, tests candidate shapes (core-4 vars, quarter
time forms), and writes the WINNING shape to _state/grammar-overrides
.json. Engine v1.1.2 (harness 21/21) reads overrides generically in
get_vars/drain_one/refresh -- probe-verified config, not code guesses.
pseo + saipe-schdist get record-only probes (institution/school-
district keyed; expected out-of-ladder -- documented, not fixed).

  P0  probe battery -> decide + write override (or exit RED with
      bodies logged for the next iteration; nothing guessed)
  G0  zip-settle v1.1.2   G0b redo qwi-sa/se/rh
  G1  drive to COMPLETE   G2 ledger <=6 named
  G3  inception incl qwi ENFORCED   G4 rows/MB   G6 sentinel
Engine source in push -> NO skip-deploy; settle gates the race.
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
MARKER = "v1.1.2 ops4949"
V10_ROWS = 2_082_816
QWI = ["qwi-sa", "qwi-se", "qwi-rh"]
CORE4 = ["Emp", "HirA", "Sep", "EarnS"]

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
    """Direct probe from the runner. Returns (status, first 400 chars,
    parsed row count or -1)."""
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "justhodl-ops/4949 (raafouis@gmail.com)"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(6_000_000).decode("utf-8", "replace")
            try:
                n = len(json.loads(body)) - 1
            except Exception:
                n = -1
            return r.status, body[:400], n
    except urllib.error.HTTPError as e:
        try:
            return e.code, e.read().decode("utf-8", "replace")[:400], -1
        except Exception:
            return e.code, "", -1
    except Exception as e:
        return 0, str(e)[:200], -1


def qurl(base, getlist, tval, geo, key):
    parts = ["get=" + ",".join(getlist),
             "time=" + urllib.parse.quote_plus(tval)]
    if geo:
        parts.append("for=" + urllib.parse.quote_plus(geo))
    parts.append("key=" + key)
    return base + "?" + "&".join(parts)


with report("ops_4949_census_qwi_grammar") as R:
    fails = []

    # P0 -- live probe battery -----------------------------------------
    R.section("P0 QWI grammar probe (runner-side, nothing guessed)")
    key = (lam.get_function_configuration(FunctionName=FN)
           .get("Environment", {}).get("Variables", {})
           .get("CENSUS_API_KEY", ""))
    R.log("  census key present: %s" % bool(key))
    base = "https://api.census.gov/data/timeseries/qwi/sa"
    st0 = gj(STATE_KEY) or {}
    old_vars = ((st0.get("datasets") or {}).get("qwi-sa") or {}) \
        .get("vars") or []
    tests = []
    if old_vars:
        tests.append(("A-failing-shape", old_vars[:20], "from 1900",
                      "state:*"))
    tests += [
        ("B-core4-from1900", CORE4, "from 1900", "state:*"),
        ("C-core4-fromQ1",   CORE4, "from 1990-Q1", "state:*"),
        ("D-core4-year",     CORE4, "2022", "state:*"),
        ("G-core4-qtr-window", CORE4, "from 2022-Q1 to 2022-Q4",
         "state:*"),
        ("E-core4-1state-1q", CORE4, "2022-Q1", "state:01"),
    ]
    results = {}
    for name, gv, tv, geo in tests:
        stx, body, n = census(qurl(base, gv, tv, geo, key))
        results[name] = (stx, n)
        R.log("  %-20s HTTP %-3s rows=%-6s %s" % (
            name, stx, n, body.replace("\n", " ")[:180]))
        time.sleep(0.7)
    ov = None
    if results.get("B-core4-from1900", (0, -1))[1] > 1:
        ov = {"vars": CORE4}
        why = "B: core-4 vars unlock full 'from 1900'"
    elif results.get("C-core4-fromQ1", (0, -1))[1] > 1:
        ov = {"vars": CORE4, "full_time": "from 1990-Q1"}
        why = "C: core-4 + quarter-form 'from 1990-Q1'"
    elif results.get("D-core4-year", (0, -1))[1] > 1:
        ov = {"vars": CORE4}
        why = "D: core-4 per-year works; full 400 auto-escalates"
    elif results.get("G-core4-qtr-window", (0, -1))[1] > 1:
        ov = {"vars": CORE4,
              "year_time": "from {y}-Q1 to {y}-Q4"}
        why = "G: per-year quarter window"
    if not ov:
        R.log("P0 FAIL no candidate shape returned rows -- bodies "
              "above are the next iteration's input; NOT guessing")
        sys.exit(1)
    payload = {sl: dict(ov) for sl in QWI}
    s3.put_object(Bucket=B, Key=GRAM_KEY,
                  Body=json.dumps(payload, indent=1).encode(),
                  ContentType="application/json")
    R.log("P0 PASS override written for %s -> %s (%s)" % (
        ",".join(QWI), json.dumps(ov), why))

    # record-only: pseo + saipe-schdist bodies for the ledger ----------
    R.section("P0b record-only probes (expected out-of-ladder)")
    for nm, u in (
        ("pseo-earnings", "https://api.census.gov/data/timeseries/"
         "pseo/earnings?get=Y1_P50_EARNINGS&time=from+2010&key=" + key),
        ("saipe-schdist", "https://api.census.gov/data/timeseries/"
         "poverty/saipe/schdist?get=SAEPOVRAT5_17RV_PT&time=2022"
         "&for=school+district+(unified):*&in=state:01&key=" + key),
    ):
        stx, body, n = census(u)
        R.log("  %-14s HTTP %-3s rows=%-4s %s" % (
            nm, stx, n, body.replace("\n", " ")[:170]))

    # G0 -- zip-settle v1.1.2 ------------------------------------------
    R.section("G0 zip-settle v1.1.2")
    ok0, t0 = False, time.time()
    while time.time() - t0 < 600:
        try:
            f = lam.get_function(FunctionName=FN)
            req = urllib.request.Request(f["Code"]["Location"])
            with urllib.request.urlopen(req, timeout=90) as r:
                zbytes = r.read()
            src = zipfile.ZipFile(io.BytesIO(zbytes)).read(
                "lambda_function.py").decode("utf-8", "replace")
            cfg = f.get("Configuration", {})
            if MARKER in src and cfg.get("State") == "Active":
                ok0 = True
                R.log("G0 PASS marker after %ds" % (time.time() - t0))
                break
            R.log("  t+%4ds marker=%s" % (time.time() - t0,
                                          MARKER in src))
        except Exception as e:
            R.log("  settle: %s" % str(e)[:120])
        time.sleep(25)
    if not ok0:
        R.log("G0 FAIL v1.1.2 never landed")
        sys.exit(1)

    # G0b -- redo the three qwi sets -----------------------------------
    R.section("G0b redo qwi-sa/se/rh")
    kick(FN, json.dumps({"redo": QWI}).encode())
    time.sleep(20)

    # G1 -- drive to COMPLETE ------------------------------------------
    R.section("G1 drive to COMPLETE")
    BUDGET, STALL_S = 15 * 60, 240
    t0 = time.time()
    last_fp, last_move, kicks, ok1 = None, time.time(), 0, False
    st = {}
    while time.time() - t0 < BUDGET:
        st = gj(STATE_KEY) or st or {}
        q = st.get("queue") or []
        head = q[0] if q else None
        hd = (st.get("datasets") or {}).get(head) or {}
        fp = (st.get("n_done"), st.get("rows_total"),
              len(st.get("failures") or {}), st.get("phase"),
              head, hd.get("resume_year"), hd.get("attempts"),
              st.get("updated_at"))
        if fp != last_fp:
            last_fp, last_move = fp, time.time()
            R.log("  t+%4ds %s done=%s/%s rows=%s q=%s head=%s ry=%s "
                  "fail=%s" % (time.time() - t0, st.get("phase"),
                               st.get("n_done"), st.get("n_total"),
                               st.get("rows_total"), len(q), head,
                               hd.get("resume_year"),
                               len(st.get("failures") or {})))
        if st.get("phase") == "COMPLETE":
            ok1 = True
            break
        if time.time() - last_move > STALL_S and kicks < 6:
            kicks += 1
            kick(FN)
            last_move = time.time()
            R.log("  stall -> kick #%d" % kicks)
        time.sleep(25)
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

    # G2 -- ledger -----------------------------------------------------
    R.section("G2 failures ledger")
    fl = st.get("failures") or {}
    for slug, why in sorted(fl.items()):
        R.log("  FAIL %-20s %s" % (slug, str(why)[:100]))
    ok2 = len(fl) <= 6 and all(str(v).strip() for v in fl.values())
    R.log("G2 %s n=%d" % ("PASS" if ok2 else "FAIL", len(fl)))
    if not ok2:
        fails.append("G2")

    # G3 -- inception incl qwi ENFORCED --------------------------------
    R.section("G3 inception (qwi enforced)")
    ds = st.get("datasets") or {}

    def fam_ok(fam):
        return [(sl, d) for sl, d in ds.items()
                if d.get("ok") and fam_of(st, sl) == fam]

    qwi_rows = fam_ok("qwi")
    for sl, d in qwi_rows:
        R.log("  qwi %-8s %-10s tp=%-4s rows=%-8s %s..%s vars=%s" % (
            sl, d.get("mode"), d.get("tp") or "time", d.get("rows"),
            d.get("y0"), d.get("y1"), ",".join(d.get("vars") or [])))
    qwi_ok = len(qwi_rows) >= 2 and any(
        (d.get("y0") or "9999") <= "2000" for _, d in qwi_rows)
    bds_ok = any((d.get("y0") or "9999") <= "1980"
                 for _, d in fam_ok("bds"))
    govs_n = sum(len(fam_ok(f)) for f in
                 ("govs", "govsemp", "govslocalfin", "govspension",
                  "govsschfin", "govsstatefin", "govsstatetax"))
    ok3 = qwi_ok and bds_ok and govs_n >= 3 and \
        bool(fam_ok("poverty")) and bool(fam_ok("healthins"))
    R.log("G3 %s qwi_ok=%s(n=%d) bds=%s govs=%d" % (
        "PASS" if ok3 else "FAIL", qwi_ok, len(qwi_rows), bds_ok,
        govs_n))
    if not ok3:
        fails.append("G3")

    # G4 -- size -------------------------------------------------------
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
    ok4 = rows_total > 3_890_000 and n_keys > 160
    R.log("G4 %s rows=%s (+%s) %.2fMB keys=%d" % (
        "PASS" if ok4 else "FAIL", rows_total, rows_total - V10_ROWS,
        mb, n_keys))
    if not ok4:
        fails.append("G4")

    # G6 -- sentinel ---------------------------------------------------
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
        R.log("ops 4949 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(n_done=n_done, n_total=n_total, failures=n_fail,
         rows_total=rows_total, store_mb=round(mb, 2), s3_keys=n_keys,
         qwi_datasets=len(qwi_rows),
         override=json.dumps(gj(GRAM_KEY) or {})[:200])
    R.log("ops 4949 GREEN -- qwi live via probe-verified grammar; "
          "universe COMPLETE with a clean <=6 named-failure ledger")
