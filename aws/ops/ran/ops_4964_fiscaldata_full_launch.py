"""ops_4964 -- launch the FULL FiscalData warehouse (lane #2).

justhodl-fiscaldata-full v1.0.0 (harness 12/12): docs-harvest+seed
discovery, probe-validated universe, full pagination since inception
to JSONL.gz per endpoint, delta refresh + weekly redrain. fd-note-v2
composes the treasury card from the manifest.

  G-1 markers  G0 settle(+new-fn fallback)  G0b schedules: rate(2h)
  delta + rate(7d) redrain  G1 chain-drive 18min (COMPLETE or
  banked>=16 & rows>=120k)  G2 substance: auctions JSONL first
  record 1979-11-15  G3 card
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
FN = "justhodl-fiscaldata-full"
CAT_FN = "justhodl-provider-catalog"
STATE_KEY = "data/warm/fiscaldata-full/_state/state.json"
HUB_KEY = "data/provider-catalog.json"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
MARKS = {FN: ("v1.0.0 ops4964",
              "aws/lambdas/justhodl-fiscaldata-full/source/"
              "lambda_function.py"),
         CAT_FN: ("fd-note-v2",
                  "aws/lambdas/justhodl-provider-catalog/source/"
                  "lambda_function.py")}
ROOTP = Path(__file__).resolve().parents[2]

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)


def gj(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return default


def kick(fn, payload=b"{}"):
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event",
                   Payload=payload)
    except Exception:
        pass


def settle(R, fn, mk, budget=600):
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            f = lam.get_function(FunctionName=fn)
            req = urllib.request.Request(f["Code"]["Location"])
            with urllib.request.urlopen(req, timeout=90) as r:
                zb = r.read()
            src = zipfile.ZipFile(io.BytesIO(zb)).read(
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


def ensure_sched(R, name, expr, arn, inp):
    try:
        sch.create_schedule(
            Name=name, GroupName="default",
            ScheduleExpression=expr,
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                    "Input": inp})
        R.log("  created %s %s" % (name, expr))
        return True
    except Exception as e:
        if "Conflict" in type(e).__name__ or "exists" in str(e):
            R.log("  exists %s (ok)" % name)
            return True
        R.log("  sched FAIL %s: %s" % (name, str(e)[:90]))
        return False


with report("ops_4964_fiscaldata_full_launch") as R:
    fails = []
    R.section("G-1 markers-in-checkout")
    for fn, (mk, rel) in MARKS.items():
        if mk not in (ROOTP.parent / rel).read_text():
            R.log("ABORT: %r absent" % mk)
            sys.exit(1)
        R.log("  ok %-28s %r" % (fn, mk))

    R.section("G0 settle")
    r0 = settle(R, FN, MARKS[FN][0])
    if r0 is None:
        R.log("  create-branch skipped -> deploy from runner")
        cfg = json.load(open(ROOTP.parent /
                             "aws/lambdas/justhodl-fiscaldata-full/"
                             "config.json"))
        deploy_lambda(
            report=R, function_name=FN,
            source_dir=ROOTP /
            "lambdas/justhodl-fiscaldata-full/source",
            env_vars={"S3_BUCKET": B}, timeout=850, memory=1024,
            description=cfg["description"],
            create_function_url=False, smoke=False)
        r0 = settle(R, FN, MARKS[FN][0], budget=300)
    if not r0 or not settle(R, CAT_FN, MARKS[CAT_FN][0]):
        R.log("G0 FAIL")
        sys.exit(1)
    R.log("G0 PASS")

    R.section("G0b schedules")
    arn = lam.get_function_configuration(
        FunctionName=FN)["FunctionArn"]
    ok_b = ensure_sched(R, "justhodl-fiscaldata-full-2h",
                        "rate(2 hours)", arn, "{}")
    ok_b = ensure_sched(R, "justhodl-fiscaldata-full-weekly",
                        "rate(7 days)", arn,
                        json.dumps({"redrain": True})) and ok_b
    if not ok_b:
        fails.append("G0b")

    R.section("G1 chain-drive (18min)")
    kick(FN)
    t0, last_fp, last_move, kicks = time.time(), None, time.time(), 0
    st = {}
    while time.time() - t0 < 18 * 60:
        st = gj(STATE_KEY) or {}
        have = st.get("have") or {}
        rows = sum(v.get("rows") or 0 for v in have.values())
        fp = (st.get("phase"), len(have), rows,
              len(st.get("queue") or []))
        if fp != last_fp:
            last_fp, last_move = fp, time.time()
            R.log("  t+%4ds %s banked=%d rows=%d q=%s "
                  "valid=%s fail=%s" % (
                      time.time() - t0, st.get("phase"), len(have),
                      rows, len(st.get("queue") or []),
                      len(st.get("universe") or {}),
                      len(st.get("failures") or {})))
        if st.get("phase") == "COMPLETE":
            break
        if float(st.get("lease_until") or 0) <= time.time() and \
                time.time() - last_move > 240 and kicks < 5:
            kicks += 1
            kick(FN)
            last_move = time.time()
            R.log("  chain restart kick #%d" % kicks)
        time.sleep(25)
    have = st.get("have") or {}
    rows = sum(v.get("rows") or 0 for v in have.values())
    ok1 = st.get("phase") == "COMPLETE" or \
        (len(have) >= 16 and rows >= 120_000)
    R.log("G1 %s phase=%s banked=%d rows=%d valid=%d" % (
        "PASS" if ok1 else "FAIL", st.get("phase"), len(have),
        rows, len(st.get("universe") or {})))
    if not ok1:
        fails.append("G1")

    R.section("G2 substance: auctions since 1979-11-15")
    ok2 = False
    try:
        raw = gzip.decompress(s3.get_object(
            Bucket=B, Key="data/warm/fiscaldata-full/src/"
            "v1_accounting_od_auctions_query.jsonl.gz"
        )["Body"].read())
        first = json.loads(raw.split(b"\n", 1)[0])
        nrows = raw.count(b"\n")
        ok2 = str(first.get("record_date", ""))[:4] <= "1980" and \
            nrows >= 11000
        R.log("  auctions rows=%d first=%s" % (
            nrows, first.get("record_date")))
    except Exception as e:
        R.log("  substance err: %s" % str(e)[:100])
    R.log("G2 %s" % ("PASS" if ok2 else "FAIL"))
    if not ok2:
        fails.append("G2")

    R.section("G3 card (post-mark)")
    t_mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    kick(CAT_FN)
    hub, t0 = {}, time.time()
    while time.time() - t0 < 12 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    ce = next((p for p in hub.get("providers", [])
               if p.get("slug") == "treasury"), {}) or {}
    note = ce.get("catalog_note") or ""
    ok3 = "FULL FiscalData warehouse" in note
    R.log("G3 %s note=%s" % ("PASS" if ok3 else "FAIL", note[:200]))
    if not ok3:
        fails.append("G3")

    if fails:
        R.log("ops 4964 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(phase=st.get("phase"), banked=len(have), rows=rows,
         valid=len(st.get("universe") or {}),
         invalid_named=len(st.get("invalid") or {}),
         failures=len(st.get("failures") or {}))
    R.log("ops 4964 GREEN -- FiscalData full warehouse live; delta "
          "refresh 2h + weekly redrain own it")
