"""ops_4976 -- launch justhodl-polygon-full (full-market grouped-daily
warehouse; 4975 window evidence). Key injected from the fleet donor
at deploy/config time -- never committed.

  G-1 marker  G0 settle(+new-fn fallback w/ donor key; ensure env
  key set either path)  G0b rate(2 hours)  G1 chain-drive 14min:
  sessions>=250, gb>=0.25, window_start found  G2 substance: one
  banked session parses, resultsCount>=8000  G3 polygon card gains
  pf-note-v2 (rides provider-catalog marker in this push)
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
FN = "justhodl-polygon-full"
CAT = "justhodl-provider-catalog"
STATE_KEY = "data/warm/polygon-full/_state/state.json"
HUB_KEY = "data/provider-catalog.json"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
MARKS = {FN: ("v1.0.0 ops4976",
              "aws/lambdas/justhodl-polygon-full/source/"
              "lambda_function.py"),
         CAT: ("pf-note-v2",
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


def donor_key():
    return lam.get_function_configuration(
        FunctionName="justhodl-equity-research"
    )["Environment"]["Variables"].get("POLYGON_API_KEY") or ""


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


with report("ops_4976_polygon_full_launch") as R:
    fails = []
    R.section("G-1 markers")
    for fn, (mk, rel) in MARKS.items():
        if mk not in (ROOTP.parent / rel).read_text():
            R.log("ABORT %r absent" % mk)
            sys.exit(1)
        R.log("  ok %-24s %r" % (fn, mk))

    R.section("G0 settle + key")
    pk = donor_key()
    R.log("  donor key: %s" % ("present" if pk else "ABSENT"))
    if not pk:
        R.log("G0 FAIL no key")
        sys.exit(1)
    r0 = settle(R, FN, MARKS[FN][0])
    if r0 is None:
        R.log("  create-branch skipped -> deploy from runner")
        cfg = json.load(open(ROOTP.parent /
                             "aws/lambdas/justhodl-polygon-full/"
                             "config.json"))
        deploy_lambda(
            report=R, function_name=FN,
            source_dir=ROOTP /
            "lambdas/justhodl-polygon-full/source",
            env_vars={"S3_BUCKET": B, "POLYGON_API_KEY": pk},
            timeout=780, memory=1024,
            description=cfg["description"],
            create_function_url=False, smoke=False)
        r0 = settle(R, FN, MARKS[FN][0], budget=300)
    if not r0 or not settle(R, CAT, MARKS[CAT][0]):
        R.log("G0 FAIL")
        sys.exit(1)
    env = lam.get_function_configuration(
        FunctionName=FN)["Environment"]["Variables"]
    if not env.get("POLYGON_API_KEY"):
        env["POLYGON_API_KEY"] = pk
        lam.update_function_configuration(
            FunctionName=FN, Environment={"Variables": env})
        R.log("  env key injected post-create")
        time.sleep(20)
    R.log("G0 PASS")

    R.section("G0b rate(2 hours)")
    arn = lam.get_function_configuration(
        FunctionName=FN)["FunctionArn"]
    try:
        sch.create_schedule(
            Name="justhodl-polygon-full-2h", GroupName="default",
            ScheduleExpression="rate(2 hours)",
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

    R.section("G1 chain-drive (14min)")
    lam.invoke(FunctionName=FN, InvocationType="Event",
               Payload=b"{}")
    t0, last_fp, last_move, kicks = time.time(), None, time.time(), 0
    st = {}
    while time.time() - t0 < 14 * 60:
        st = gj(STATE_KEY) or {}
        fp = (st.get("sessions"), st.get("cursor"))
        if fp != last_fp:
            last_fp, last_move = fp, time.time()
            R.log("  t+%4ds %s sessions=%s gb=%.2f cursor=%s "
                  "wstart=%s skips=%s" % (
                      time.time() - t0, st.get("phase"),
                      st.get("sessions") or 0,
                      (st.get("bytes") or 0) / 1e9,
                      st.get("cursor"), st.get("window_start"),
                      st.get("skips")))
        if st.get("phase") == "LIVE":
            break
        if float(st.get("lease_until") or 0) <= time.time() and \
                time.time() - last_move > 200 and kicks < 4:
            kicks += 1
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=b"{}")
            last_move = time.time()
            R.log("  chain restart kick #%d" % kicks)
        time.sleep(25)
    sess = st.get("sessions") or 0
    gb = (st.get("bytes") or 0) / 1e9
    ok1 = sess >= 250 and gb >= 0.25 and \
        bool(st.get("window_start"))
    R.log("G1 %s sessions=%d gb=%.2f wstart=%s phase=%s" % (
        "PASS" if ok1 else "FAIL", sess, gb,
        st.get("window_start"), st.get("phase")))
    if not ok1:
        fails.append("G1")

    R.section("G2 substance")
    ok2 = False
    try:
        ws = st.get("window_start") or "2022-01-03"
        y = ws[:4]
        r_ = s3.list_objects_v2(
            Bucket=B, Prefix="data/warm/polygon-full/grouped/",
            MaxKeys=3)
        k0 = (r_.get("Contents") or [{}])[0].get("Key")
        js = json.loads(gzip.decompress(s3.get_object(
            Bucket=B, Key=k0)["Body"].read()))
        n = js.get("resultsCount", 0)
        r0_ = (js.get("results") or [{}])[0]
        ok2 = n >= 8000 and "T" in r0_ and "c" in r0_
        R.log("  %s -> tickers=%d sample=%s c=%s" % (
            k0.rsplit("/", 1)[-1], n, r0_.get("T"), r0_.get("c")))
    except Exception as e:
        R.log("  substance err %s" % str(e)[:100])
    R.log("G2 %s" % ("PASS" if ok2 else "FAIL"))
    if not ok2:
        fails.append("G2")

    R.section("G3 card (post-mark)")
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
    pg = next((p for p in hub.get("providers", [])
               if p.get("slug") == "polygon"), {}) or {}
    ok3 = "FULL grouped-daily warehouse" in (pg.get("catalog_note")
                                             or "")
    R.log("G3 %s note=%s" % ("PASS" if ok3 else "FAIL",
                             (pg.get("catalog_note") or "")[:180]))
    if not ok3:
        fails.append("G3")

    if fails:
        R.log("ops 4976 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(sessions=sess, gb=round(gb, 2),
         window_start=st.get("window_start"),
         phase=st.get("phase"))
    R.log("ops 4976 GREEN -- full-market sessions banked from the "
          "entitled boundary; rate(2h) holds the edge; banked "
          "dates persist as the window rolls")
