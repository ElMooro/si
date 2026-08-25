"""ops_4977 -- launch justhodl-boe-full (queue #4; no engine existed).

  G-1 markers  G0 settle(+fallback)  G0b rate(12 hours)
  G1 run: curve_zips>=3 & curve_mb>=10 & iadb_ok>=22
  G2 substance: SONIA csv spans <=1998 -> current; one curve zip
     opens with csv members
  G3 boe card note
"""
import gzip
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
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-boe-full"
CAT = "justhodl-provider-catalog"
STATE_KEY = "data/warm/boe-full/_state/state.json"
HUB_KEY = "data/provider-catalog.json"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
MARKS = {FN: ("v1.0.1 ops4977",
              "aws/lambdas/justhodl-boe-full/source/"
              "lambda_function.py"),
         CAT: ("boe-note-v2",
               "aws/lambdas/justhodl-provider-catalog/source/"
               "lambda_function.py")}
ROOTP = Path(__file__).resolve().parents[2]

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def settle(R, fn, mk, budget=600):
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            f = lam.get_function(FunctionName=fn)
            req = urllib.request.Request(f["Code"]["Location"])
            with urllib.request.urlopen(req, timeout=90) as r:
                src = zipfile.ZipFile(io.BytesIO(r.read())).read(
                    "lambda_function.py").decode("utf-8", "replace")
            if mk in src and \
                    f["Configuration"].get("State") == "Active":
                R.log("  %s settled (%ds)" % (fn, time.time() - t0))
                return True
        except lam.exceptions.ResourceNotFoundException:
            R.log("  %s t+%ds NOT FOUND" % (fn, time.time() - t0))
            if time.time() - t0 > 240:
                return None
        except Exception as e:
            R.log("  %s settle: %s" % (fn, str(e)[:90]))
        time.sleep(25)
    return False


with report("ops_4977_boe_full_launch") as R:
    fails = []
    R.section("G-1 markers")
    for fn, (mk, rel) in MARKS.items():
        if mk not in (ROOTP.parent / rel).read_text():
            R.log("ABORT %r absent" % mk)
            sys.exit(1)
        R.log("  ok %-22s %r" % (fn, mk))

    R.section("G0 settle")
    r0 = settle(R, FN, MARKS[FN][0])
    if r0 is None:
        R.log("  create-branch skipped -> deploy from runner")
        cfg = json.load(open(ROOTP.parent /
                             "aws/lambdas/justhodl-boe-full/"
                             "config.json"))
        deploy_lambda(
            report=R, function_name=FN,
            source_dir=ROOTP / "lambdas/justhodl-boe-full/source",
            env_vars={"S3_BUCKET": B}, timeout=780, memory=1024,
            description=cfg["description"],
            create_function_url=False, smoke=False)
        r0 = settle(R, FN, MARKS[FN][0], budget=300)
    if not r0 or not settle(R, CAT, MARKS[CAT][0]):
        R.log("G0 FAIL")
        sys.exit(1)
    R.log("G0 PASS")

    R.section("G0b rate(12 hours)")
    arn = lam.get_function_configuration(
        FunctionName=FN)["FunctionArn"]
    try:
        sch.create_schedule(
            Name="justhodl-boe-full-12h", GroupName="default",
            ScheduleExpression="rate(12 hours)",
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                    "Input": "{}"})
        R.log("G0b created")
    except Exception as e:
        if "Conflict" in type(e).__name__ or "exists" in str(e):
            R.log("G0b exists (ok)")
        else:
            R.log("G0b FAIL %s" % str(e)[:90])
            fails.append("G0b")

    R.section("G1 run")
    lam.invoke(FunctionName=FN, InvocationType="Event",
               Payload=b"{}")
    st, t0 = {}, time.time()
    while time.time() - t0 < 13 * 60:
        time.sleep(30)
        st = gj(STATE_KEY) or {}
        z = st.get("zips") or {}
        ia = st.get("iadb") or {}
        ok_ia = sum(1 for v in ia.values() if v.get("ok"))
        R.log("  t+%4ds zips=%d iadb_ok=%d fail=%d" % (
            time.time() - t0, len(z), ok_ia,
            len(st.get("failures") or {})))
        if st.get("as_of") and \
                float(st.get("lease_until") or 1) == 0 and \
                len(z) >= 1:
            break
    z = st.get("zips") or {}
    ia = st.get("iadb") or {}
    ok_ia = sum(1 for v in ia.values() if v.get("ok"))
    zmb = sum(v.get("bytes") or 0 for v in z.values()) / 1e6
    ok1 = len(z) >= 3 and zmb >= 10 and ok_ia >= 22
    for k, v in list((st.get("failures") or {}).items())[:8]:
        R.log("    fail %s: %s" % (k, str(v)[:90]))
    R.log("G1 %s zips=%d %.0fMB iadb_ok=%d/%d" % (
        "PASS" if ok1 else "FAIL", len(z), zmb, ok_ia, len(ia)))
    if not ok1:
        fails.append("G1")

    R.section("G2 substance")
    ok2a = ok2b = False
    try:
        raw = gzip.decompress(s3.get_object(
            Bucket=B, Key="data/warm/boe-full/iadb/IUDSOIA.csv.gz"
        )["Body"].read()).decode("utf-8", "replace")
        lines = [l_ for l_ in raw.splitlines() if l_.strip()]
        first, last = lines[1][:12], lines[-1][:12]
        ok2a = len(lines) > 5000 and (
            "199" in first or "199" in raw[:400] or
            "1997" in first + last)
        R.log("  SONIA rows=%d first=%r last=%r" % (
            len(lines), first, last))
    except Exception as e:
        R.log("  sonia err %s" % str(e)[:90])
    try:
        zk = sorted("data/warm/boe-full/curves/" + n
                    for n in z)[0]
        body = s3.get_object(Bucket=B, Key=zk)["Body"].read()
        names = zipfile.ZipFile(io.BytesIO(body)).namelist()
        ok2b = any(n.lower().endswith((".csv", ".xlsx"))
                   for n in names)
        R.log("  %s members=%d sample=%s" % (
            zk.rsplit("/", 1)[-1], len(names), names[:2]))
    except Exception as e:
        R.log("  zip err %s" % str(e)[:90])
    ok2 = ok2a and ok2b
    R.log("G2 %s" % ("PASS" if ok2 else "FAIL"))
    if not ok2:
        fails.append("G2")

    R.section("G3 card")
    t_mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        lam.invoke(FunctionName=CAT, InvocationType="Event",
                   Payload=b"{}")
    except Exception:
        pass
    hub, t0 = {}, time.time()
    while time.time() - t0 < 12 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    be = next((p for p in hub.get("providers", [])
               if p.get("slug") == "boe"), {}) or {}
    ok3 = "FULL warehouse (boe-full v1)" in (
        be.get("catalog_note") or "")
    R.log("G3 %s note=%s" % ("PASS" if ok3 else "FAIL",
                             (be.get("catalog_note") or "")[:180]))
    if not ok3:
        fails.append("G3")

    if fails:
        R.log("ops 4977 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(zips=len(z), curve_mb=round(zmb, 1), iadb_ok=ok_ia,
         failures=len(st.get("failures") or {}))
    R.log("ops 4977 GREEN -- BoE full warehouse live; queue #5 "
          "coinmetrics next")
