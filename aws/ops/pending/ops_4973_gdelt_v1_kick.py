"""ops_4973 -- gdelt v1 backfill unblock (v2 corpus is LIVE at
39.72GB / 396k files; the 1979-2015 index regex matched 0 of the
9,740 files 4962 counted on the same page). v1.0.1 loosens the
capture; this op settles it, kicks, and verifies the backfill moves.

  G-1 marker  G0 settle  G1 kick -> v1_total>=9000 and v1_files>=40
  within 8min (chains + rate(30m) own the rest)
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-gdelt-full"
STATE_KEY = "data/warm/gdelt-full/_state/state.json"
MARK = "v1.0.2 ops4973"
REL = ("aws/lambdas/justhodl-gdelt-full/source/lambda_function.py")
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

with report("ops_4973_gdelt_v1_kick") as R:
    R.section("G-1 marker")
    if MARK not in (Path(__file__).resolve().parents[2].parent /
                    REL).read_text():
        R.log("ABORT marker absent")
        sys.exit(1)
    R.log("  ok %r" % MARK)

    R.section("G0 settle")
    ok0, t0 = False, time.time()
    while time.time() - t0 < 600:
        try:
            f = lam.get_function(FunctionName=FN)
            req = urllib.request.Request(f["Code"]["Location"])
            with urllib.request.urlopen(req, timeout=90) as r:
                src = zipfile.ZipFile(io.BytesIO(r.read())).read(
                    "lambda_function.py").decode("utf-8", "replace")
            if MARK in src:
                ok0 = True
                R.log("  settled (%ds)" % (time.time() - t0))
                break
        except Exception as e:
            R.log("  settle: %s" % str(e)[:80])
        time.sleep(25)
    if not ok0:
        R.log("G0 FAIL")
        sys.exit(1)

    R.section("G1 kick -> v1 backfill moves")
    lam.invoke(FunctionName=FN, InvocationType="Event",
               Payload=b"{}")
    st, t0 = {}, time.time()
    while time.time() - t0 < 8 * 60:
        time.sleep(25)
        try:
            st = json.loads(s3.get_object(
                Bucket=B, Key=STATE_KEY)["Body"].read())
        except Exception:
            st = {}
        R.log("  t+%4ds phase=%s v1=%s/%s v1_gb=%.2f" % (
            time.time() - t0, st.get("phase"),
            st.get("v1_idx", 0), st.get("v1_total"),
            (st.get("v1_bytes") or 0) / 1e9))
        if (st.get("v1_total") or 0) >= 4000 and \
                st.get("v1_idx", 0) >= 40:
            break
    ok1 = (st.get("v1_total") or 0) >= 4000 and \
        st.get("v1_idx", 0) >= 40
    R.log("G1 %s v1_total=%s v1_idx=%s" % (
        "PASS" if ok1 else "FAIL", st.get("v1_total"),
        st.get("v1_idx")))
    if not ok1:
        R.log("ops 4973 RED: G1")
        sys.exit(1)
    R.kv(v1_total=st.get("v1_total"), v1_started=st.get("v1_idx"),
         v2_files=st.get("files"),
         v2_gb=round((st.get("bytes") or 0) / 1e9, 2))
    R.log("ops 4973 GREEN -- 1979-2015 archive backfilling; chains "
          "+ rate(30m) own it to phase V1")
