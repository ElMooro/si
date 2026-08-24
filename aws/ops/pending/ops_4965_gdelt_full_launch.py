"""ops_4965 -- launch the FULL GDELT v2 EVENTS warehouse (lane #3)
+ nyfed-hist-note-v2 card fix (board still showed only the PD note).

justhodl-gdelt-full v1.0.0 (harness 14/14): deterministic 15-min
cursor since 2015-02-18, streamed verbatim zips, 404=counted gaps,
self-chain ~38.6GB, v1 1979-2015 phase-2, rate(30m) live edge.

  G-1 markers  G0 settle(+new-fn fallback)  G0b rate(30 minutes)
  G1 chain-drive 15min: cursor advanced, files>=900, gb>=0.20,
     gap-rate<8%  G2 substance: one banked zip opens, CSV row has
     GDELT event shape (numeric GLOBALEVENTID + tab fields)
  G3 cards post-mark: gdelt FULL note + nyfed hist line
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
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-gdelt-full"
CAT_FN = "justhodl-provider-catalog"
STATE_KEY = "data/warm/gdelt-full/_state/state.json"
HUB_KEY = "data/provider-catalog.json"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
MARKS = {FN: ("v1.0.0 ops4965",
              "aws/lambdas/justhodl-gdelt-full/source/"
              "lambda_function.py"),
         CAT_FN: ("gd-note-v2",
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


def kick(fn):
    try:
        lam.invoke(FunctionName=fn, InvocationType="Event",
                   Payload=b"{}")
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


with report("ops_4965_gdelt_full_launch") as R:
    fails = []
    R.section("G-1 markers-in-checkout")
    for fn, (mk, rel) in MARKS.items():
        if mk not in (ROOTP.parent / rel).read_text():
            R.log("ABORT: %r absent" % mk)
            sys.exit(1)
        R.log("  ok %-24s %r" % (fn, mk))
    if "nyfed-hist-note-v2" not in (
            ROOTP.parent / MARKS[CAT_FN][1]).read_text():
        R.log("ABORT: nyfed-hist-note-v2 absent")
        sys.exit(1)
    R.log("  ok nyfed-hist-note-v2")

    R.section("G0 settle")
    r0 = settle(R, FN, MARKS[FN][0])
    if r0 is None:
        R.log("  create-branch skipped -> deploy from runner")
        cfg = json.load(open(ROOTP.parent /
                             "aws/lambdas/justhodl-gdelt-full/"
                             "config.json"))
        deploy_lambda(
            report=R, function_name=FN,
            source_dir=ROOTP / "lambdas/justhodl-gdelt-full/source",
            env_vars={"S3_BUCKET": B}, timeout=850, memory=1024,
            description=cfg["description"],
            create_function_url=False, smoke=False)
        r0 = settle(R, FN, MARKS[FN][0], budget=300)
    if not r0 or not settle(R, CAT_FN, MARKS[CAT_FN][0]):
        R.log("G0 FAIL")
        sys.exit(1)
    R.log("G0 PASS")

    R.section("G0b live-edge schedule rate(30 minutes)")
    arn = lam.get_function_configuration(
        FunctionName=FN)["FunctionArn"]
    try:
        sch.create_schedule(
            Name="justhodl-gdelt-full-30m", GroupName="default",
            ScheduleExpression="rate(30 minutes)",
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                    "Input": "{}"})
        R.log("G0b created")
    except Exception as e:
        if "Conflict" in type(e).__name__ or "exists" in str(e):
            R.log("G0b exists (ok)")
        else:
            R.log("G0b FAIL %s" % str(e)[:100])
            fails.append("G0b")

    R.section("G1 chain-drive (15min)")
    kick(FN)
    t0, last_fp, last_move, kicks = time.time(), None, time.time(), 0
    st = {}
    while time.time() - t0 < 15 * 60:
        st = gj(STATE_KEY) or {}
        fp = (st.get("files"), st.get("gaps"), st.get("cursor"))
        if fp != last_fp:
            last_fp, last_move = fp, time.time()
            R.log("  t+%4ds %s files=%s gb=%.2f gaps=%s "
                  "cursor=%s" % (
                      time.time() - t0, st.get("phase"),
                      st.get("files") or 0,
                      (st.get("bytes") or 0) / 1e9,
                      st.get("gaps") or 0,
                      str(st.get("cursor"))[:10]))
        if float(st.get("lease_until") or 0) <= time.time() and \
                time.time() - last_move > 200 and kicks < 4:
            kicks += 1
            kick(FN)
            last_move = time.time()
            R.log("  chain restart kick #%d" % kicks)
        time.sleep(25)
    files = st.get("files") or 0
    gb = (st.get("bytes") or 0) / 1e9
    gaps = st.get("gaps") or 0
    gap_rate = gaps / max(1, files + gaps)
    ok1 = files >= 900 and gb >= 0.20 and gap_rate < 0.08 and \
        str(st.get("cursor") or "") > "20150218230000"
    R.log("G1 %s files=%d gb=%.2f gaps=%d (%.1f%%) cursor=%s "
          "kicks=%d" % ("PASS" if ok1 else "FAIL", files, gb, gaps,
                        gap_rate * 100, st.get("cursor"), kicks))
    if not ok1:
        fails.append("G1")

    R.section("G2 substance: GDELT event shape")
    ok2 = False
    try:
        pfx = "data/warm/gdelt-full/v2/export/2015/02/"
        r_ = s3.list_objects_v2(Bucket=B, Prefix=pfx, MaxKeys=5)
        k0 = (r_.get("Contents") or [{}])[0].get("Key")
        raw = s3.get_object(Bucket=B, Key=k0)["Body"].read()
        zf = zipfile.ZipFile(io.BytesIO(raw))
        line = zf.read(zf.namelist()[0])[:2000].split(b"\n", 1)[0]
        parts = line.split(b"\t")
        ok2 = len(parts) >= 50 and parts[0].strip().isdigit()
        R.log("  %s -> first row: id=%s cols=%d" % (
            k0.rsplit("/", 1)[-1],
            parts[0][:12].decode("ascii", "replace"), len(parts)))
    except Exception as e:
        R.log("  substance err: %s" % str(e)[:100])
    R.log("G2 %s" % ("PASS" if ok2 else "FAIL"))
    if not ok2:
        fails.append("G2")

    R.section("G3 cards (post-mark)")
    t_mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    kick(CAT_FN)
    hub, t0 = {}, time.time()
    while time.time() - t0 < 12 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    gd = next((p for p in hub.get("providers", [])
               if p.get("slug") == "gdelt"), {}) or {}
    ny = next((p for p in hub.get("providers", [])
               if p.get("slug") == "nyfed"), {}) or {}
    ok3 = "FULL v2 EVENTS warehouse" in (gd.get("catalog_note")
                                         or "") and \
        "hist-v1 full-window" in (ny.get("catalog_note") or "")
    R.log("  gdelt: %s" % (gd.get("catalog_note") or "")[:160])
    R.log("  nyfed: %s" % (ny.get("catalog_note") or "")[:160])
    R.log("G3 %s" % ("PASS" if ok3 else "FAIL"))
    if not ok3:
        fails.append("G3")

    if fails:
        R.log("ops 4965 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(files=files, gb=round(gb, 2), gaps=gaps,
         cursor=st.get("cursor"), phase=st.get("phase"))
    R.log("ops 4965 GREEN -- GDELT full v2-events draining on the "
          "cursor; chains + rate(30m) hold the live edge; v1 "
          "backfill follows automatically")
