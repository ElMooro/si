"""ops_4948 -- census-us v1.1.1: OOM-guard + BDS grammar repair + COMPLETE.

4947 forensics: the drain crash-looped at head -- county-grained sets
(sahie-class) answer the bare geo variant with multi-hundred-MB dumps;
the unbounded read() OOM-killed the 1GB Lambda BEFORE BIG_TEXT was
consulted, every invoke died without saving, and 12 datasets starved
behind the poison head (updated_at only moved on clean budget-resumes).
Separately bds banked WRONG: it declares BOTH `time` and `YEAR`, and
`time=from 1900` returns only the latest year (2022, 5,516 rows).

v1.1.1 (harness 20/20 GREEN locally):
  * chunked READ_CAP=48MB in http_get -> synthetic 413 the mode ladder
    treats as "wrong variant, next rung" (sahie: "" 413 -> us:* lands)
  * save-first crash quarantine: attempts counter persisted BEFORE the
    pull; 4 dead invokes at one head -> named quarantine, queue moves
  * annual sets answering `time` with a single year redo via YEAR
  * event {"redo":[slug]} = surgical re-import (repairs bds now)

Gates: G0 zip-settle v1.1.1 marker; G0b redo bds accepted; G1 drive to
COMPLETE (fingerprint includes head slug/resume/attempts/updated_at);
G2 failures ledger named & <=6; G3 inception incl bds<=1980 ENFORCED;
G4 rows/MB census; G5 '1..1' watchlist (log; heals on refresh);
G6 sentinel COMPLETE. Engine source in this push -> NO skip-deploy;
zip-settle gates the deploy race. Event-invoke + S3-poll throughout.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
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
MARKER = "v1.1.1 ops4948"
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


def fam_of(st, slug):
    return ((st.get("catalog") or {}).get(slug) or {}).get("family")


with report("ops_4948_census_oom_guard_complete") as R:
    fails = []

    # G0 -- zip-settle v1.1.1 ------------------------------------------
    R.section("G0 zip-settle v1.1.1")
    ok0, t0 = False, time.time()
    while time.time() - t0 < 600:
        try:
            f = lam.get_function(FunctionName=FN)
            loc = f["Code"]["Location"]
            req = urllib.request.Request(loc)
            with urllib.request.urlopen(req, timeout=90) as r:
                zbytes = r.read()
            src = zipfile.ZipFile(io.BytesIO(zbytes)).read(
                "lambda_function.py").decode("utf-8", "replace")
            cfg = f.get("Configuration", {})
            if MARKER in src and cfg.get("State") == "Active" and \
                    cfg.get("LastUpdateStatus") in ("Successful", None):
                ok0 = True
                R.log("G0 PASS marker deployed after %ds (sha %s)" % (
                    time.time() - t0, cfg.get("CodeSha256", "")[:12]))
                break
            R.log("  t+%4ds marker=%s state=%s" % (
                time.time() - t0, MARKER in src, cfg.get("State")))
        except Exception as e:
            R.log("  settle: %s" % str(e)[:120])
        time.sleep(25)
    if not ok0:
        R.log("G0 FAIL v1.1.1 never landed -- aborting")
        sys.exit(1)

    # G0b -- surgical bds repair ---------------------------------------
    R.section("G0b redo bds (wrong-grammar bank)")
    kick(FN, json.dumps({"redo": ["bds"]}).encode())
    ok0b, t0 = False, time.time()
    while time.time() - t0 < 240:
        time.sleep(20)
        st = gj(STATE_KEY) or {}
        b = (st.get("datasets") or {}).get("bds") or {}
        # accepted when the old single-year bank is gone: either reset,
        # mid-rescan, or already re-landed via YEAR
        if b.get("tp") == "YEAR" or not b.get("ok") or \
                "bds" in (st.get("queue") or []):
            ok0b = True
            R.log("G0b PASS bds reset/rescanning (tp=%s ok=%s q=%s)" % (
                b.get("tp"), b.get("ok"), "bds" in (st.get("queue") or [])))
            break
    if not ok0b:
        R.log("G0b FAIL redo not accepted")
        fails.append("G0b redo")

    # G1 -- drive to COMPLETE ------------------------------------------
    R.section("G1 drain to COMPLETE (attempts-aware fingerprint)")
    BUDGET, STALL_S = 20 * 60, 240
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
                  "att=%s fail=%s" % (
                      time.time() - t0, st.get("phase"),
                      st.get("n_done"), st.get("n_total"),
                      st.get("rows_total"), len(q), head,
                      hd.get("resume_year"), hd.get("attempts"),
                      len(st.get("failures") or {})))
        if st.get("phase") == "COMPLETE":
            ok1 = True
            break
        if time.time() - last_move > STALL_S and kicks < 8:
            kicks += 1
            kick(FN)
            last_move = time.time()
            R.log("  stall %ds -> async kick #%d" % (STALL_S, kicks))
        time.sleep(25)
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
    R.section("G2 failures ledger")
    fl = st.get("failures") or {}
    for slug, why in sorted(fl.items()):
        R.log("  FAIL %-18s %s" % (slug, str(why)[:100]))
    named = all(isinstance(v, str) and v.strip() for v in fl.values())
    ok2 = len(fl) <= 6 and named
    R.log("G2 %s n_failures=%d all_named=%s" % (
        "PASS" if ok2 else "FAIL", len(fl), named))
    if not ok2:
        fails.append("G2 failures")

    # G3 -- inception proof, bds ENFORCED ------------------------------
    R.section("G3 inception proof")
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
    pov, hins = fam_ok("poverty"), fam_ok("healthins")
    ok3 = bds_ok and qwi_ok and govs_n >= 3 and bool(pov) and bool(hins)
    R.log("G3 %s bds_1970s=%s qwi<=2000=%s govs=%d poverty=%d "
          "healthins=%d" % ("PASS" if ok3 else "FAIL", bds_ok, qwi_ok,
                            govs_n, len(pov), len(hins)))
    if not ok3:
        fails.append("G3 inception")

    R.section("inception coverage (new families first)")
    tab = sorted(((sl, d) for sl, d in ds.items() if d.get("ok")),
                 key=lambda kv: (fam_of(st, kv[0]) == "eits",
                                 -(kv[1].get("rows") or 0)))
    for sl, d in tab[:50]:
        R.log("  %-11s %-28s %-10s tp=%-4s rows=%-8s %s..%s" % (
            (fam_of(st, sl) or "?")[:11], sl[:28], d.get("mode"),
            d.get("tp") or "time", d.get("rows"),
            d.get("y0"), d.get("y1")))

    # G4 -- final rows/bytes census ------------------------------------
    R.section("G4 final rows/bytes")
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
    ok4 = rows_total > 3_000_000 and mb > V10_MB and n_keys > 90
    R.log("G4 %s rows_total=%s (+%s vs v1.0) store=%.2fMB keys=%d" % (
        "PASS" if ok4 else "FAIL", rows_total, rows_total - V10_ROWS,
        mb, n_keys))
    if not ok4:
        fails.append("G4 size")

    # G5 -- '1..1' watchlist (log only) --------------------------------
    R.section("G5 header-fix watchlist (heals on scheduled refresh)")
    for sl in ONE_ONE:
        d = ds.get(sl) or {}
        R.log("  %-6s y=%s..%s refreshed=%s" % (
            sl, d.get("y0"), d.get("y1"), d.get("refreshed")))

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
    R.log("G6 %s pipeline=%s" % ("PASS" if ok6 else "FAIL", pipe))
    if not ok6:
        fails.append("G6 sentinel")

    if fails:
        R.log("ops 4948 RED: " + "; ".join(fails))
        sys.exit(1)
    new_done = sum(1 for sl, d in ds.items()
                   if d.get("ok") and fam_of(st, sl) != "eits")
    R.kv(phase=st.get("phase"), n_done=n_done, n_total=n_total,
         failures=n_fail, rows_total=rows_total,
         rows_added_vs_v10=rows_total - V10_ROWS,
         store_mb=round(mb, 2), s3_keys=n_keys,
         new_family_datasets=new_done,
         families=",".join(sorted(set(st.get("families") or []))))
    R.log("ops 4948 GREEN -- OOM-guard live, bds repaired via YEAR, "
          "full timeseries universe COMPLETE; '1..1' heals on the next "
          "scheduled refresh (day-three log-check only)")
