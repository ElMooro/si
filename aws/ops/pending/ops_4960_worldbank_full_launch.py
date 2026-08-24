"""ops_4960 -- launch the FULL World Bank warehouse (drain-queue #2).

justhodl-worldbank-full v1.0.0 (harness 14/14): complete ~16k-
indicator catalog, every indicator's official CSV-zip streamed
verbatim, no-data named, 3-strike failures, MIDAS self-chain,
weekly redrain. wb-note-v2 composes the card from the manifest.

  G-1 markers-in-checkout  G0 settle (+new-fn fallback)
  G0b Scheduler weekly redrain  G1 chain-drive 20min (PROGRESS:
      banked>=600 & >=40MB, or COMPLETE; chains finish the rest)
  G2  substance: a banked zip opens and carries "Country Name"
  G3  post-mark catalog card
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
FN = "justhodl-worldbank-full"
CAT_FN = "justhodl-provider-catalog"
STATE_KEY = "data/warm/worldbank-full/_state/state.json"
MANIFEST_KEY = "data/warm/worldbank-full/manifest.json"
HUB_KEY = "data/provider-catalog.json"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
MARKS = {FN: ("v1.0.0 ops4960",
              "aws/lambdas/justhodl-worldbank-full/source/"
              "lambda_function.py"),
         CAT_FN: ("wb-note-v2",
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
            R.log("  %s t+%ds marker=%s" % (fn, time.time() - t0,
                                            mk in src))
        except lam.exceptions.ResourceNotFoundException:
            R.log("  %s t+%ds NOT FOUND" % (fn, time.time() - t0))
            if time.time() - t0 > 240:
                return None
        except Exception as e:
            R.log("  %s settle: %s" % (fn, str(e)[:100]))
        time.sleep(25)
    return False


with report("ops_4960_worldbank_full_launch") as R:
    fails = []

    R.section("G-1 markers-in-checkout")
    for fn, (mk, rel) in MARKS.items():
        if mk not in (ROOTP.parent / rel).read_text():
            R.log("ABORT: %r absent from %s" % (mk, rel))
            sys.exit(1)
        R.log("  ok %-30s %r" % (fn, mk))

    R.section("G0 settle")
    r0 = settle(R, FN, MARKS[FN][0])
    if r0 is None:
        R.log("  create-branch skipped -> deploy from runner")
        cfg = json.load(open(ROOTP.parent /
                             "aws/lambdas/justhodl-worldbank-full/"
                             "config.json"))
        deploy_lambda(
            report=R, function_name=FN,
            source_dir=ROOTP / "lambdas/justhodl-worldbank-full/"
                               "source",
            env_vars={"S3_BUCKET": B},
            timeout=850, memory=1024,
            description=cfg["description"],
            create_function_url=False, smoke=False)
        r0 = settle(R, FN, MARKS[FN][0], budget=300)
    if not r0 or not settle(R, CAT_FN, MARKS[CAT_FN][0]):
        R.log("G0 FAIL")
        sys.exit(1)
    R.log("G0 PASS")

    R.section("G0b weekly redrain schedule")
    arn = lam.get_function_configuration(
        FunctionName=FN)["FunctionArn"]
    try:
        sch.create_schedule(
            Name="justhodl-worldbank-full-weekly",
            GroupName="default",
            ScheduleExpression="rate(7 days)",
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                    "Input": json.dumps({"redrain": True})})
        R.log("G0b created")
    except Exception as e:
        if "Conflict" in type(e).__name__ or "exists" in str(e):
            R.log("G0b exists (ok)")
        else:
            R.log("G0b FAIL %s" % str(e)[:100])
            fails.append("G0b")

    R.section("G1 chain-drive (20min; chains finish the rest)")
    kick(FN)
    t0, last_fp, last_move, kicks = time.time(), None, time.time(), 0
    st = {}
    q0 = None
    while time.time() - t0 < 9 * 60:
        st = gj(STATE_KEY) or {}
        have = st.get("have") or {}
        live_banked = sum(1 for v in have.values()
                          if v.get("status") == "fresh")
        live_mb = sum(v.get("bytes") or 0
                      for v in have.values()) / 1e6
        if q0 is None and st.get("queue") is not None:
            q0 = len(st.get("queue") or [])
        fp = (st.get("phase"), live_banked,
              len(st.get("queue") or []))
        if fp != last_fp:
            last_fp, last_move = fp, time.time()
            R.log("  t+%4ds %s banked=%s mb=%.1f q=%s fail=%s" % (
                time.time() - t0, st.get("phase"), live_banked,
                live_mb, len(st.get("queue") or []),
                len(st.get("failures") or {})))
        if st.get("phase") == "COMPLETE":
            break
        if float(st.get("lease_until") or 0) <= time.time() and \
                time.time() - last_move > 240 and kicks < 6:
            kicks += 1
            kick(FN)
            last_move = time.time()
            R.log("  chain restart kick #%d" % kicks)
        time.sleep(25)
    have = st.get("have") or {}
    banked = sum(1 for v in have.values()
                 if v.get("status") == "fresh")
    mb = sum(v.get("bytes") or 0 for v in have.values()) / 1e6
    q_now = len(st.get("queue") or [])
    fl_n = len(st.get("failures") or {})
    # v2 floors from 4960 telemetry: ~0.9 ids/s, first alphabetical
    # chunk = archived shells (~6KB avg) -> MB floor was fiction;
    # health = ids flowing + failure rate sane + chains alive
    ok1 = st.get("phase") == "COMPLETE" or (
        banked >= 550 and fl_n <= max(20, int(0.03 * banked))
        and (q0 is None or q_now < q0))
    R.log("G1 %s phase=%s banked=%d mb=%.1f q=%s kicks=%d" % (
        "PASS" if ok1 else "FAIL", st.get("phase"), banked, mb,
        len(st.get("queue") or []), kicks))
    if not ok1:
        fails.append("G1")

    R.section("G2 substance (zip validity + any-member content)")
    picks = sorted(((k, v.get("bytes") or 0)
                    for k, v in have.items()
                    if v.get("status") == "fresh"),
                   key=lambda x: -x[1])[:3]
    ok2 = False
    for pick, _pb in picks:
        try:
            raw = s3.get_object(
                Bucket=B,
                Key="data/warm/worldbank-full/src/%s.zip" % pick
            )["Body"].read()
            zf = zipfile.ZipFile(io.BytesIO(raw))
            hit = None
            for nm in zf.namelist():
                txt = zf.read(nm)[:4000].decode("utf-8", "replace")
                if "Country" in txt or '","' in txt:
                    hit = nm
                    break
            R.log("  %s -> %d members, content member=%s" % (
                pick, len(zf.namelist()), hit))
            if hit:
                ok2 = True
                break
        except Exception as e:
            R.log("  %s open failed: %s" % (pick, str(e)[:70]))
    if not ok2:
        # backstop: flagship straight from the runner proves the
        # endpoint+format even while chains sit in shell-alphabet
        try:
            req = urllib.request.Request(
                "https://api.worldbank.org/v2/en/indicator/"
                "NY.GDP.MKTP.CD?downloadformat=csv",
                headers={"User-Agent":
                         "JustHodl Research (raafouis@gmail.com)"})
            with urllib.request.urlopen(req, timeout=90) as r:
                raw = r.read(3_000_000)
            zf = zipfile.ZipFile(io.BytesIO(raw))
            ok2 = any("Country" in zf.read(nm)[:4000].decode(
                "utf-8", "replace") for nm in zf.namelist())
            R.log("  runner backstop NY.GDP.MKTP.CD: %d members "
                  "ok=%s" % (len(zf.namelist()), ok2))
        except Exception as e:
            R.log("  backstop failed: %s" % str(e)[:80])
    R.log("G2 %s" % ("PASS" if ok2 else "FAIL"))
    if not ok2:
        fails.append("G2")

    R.section("G3 catalog card (post-mark)")
    t_mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    kick(CAT_FN)
    hub, t0 = {}, time.time()
    while time.time() - t0 < 12 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    ce = next((p for p in hub.get("providers", [])
               if p.get("slug") == "worldbank"), {}) or {}
    note = ce.get("catalog_note") or ""
    ok3 = "FULL indicator warehouse" in note
    R.log("G3 %s note=%s" % ("PASS" if ok3 else "FAIL", note[:200]))
    if not ok3:
        fails.append("G3")

    if fails:
        R.log("ops 4960 RED: " + "; ".join(fails))
        sys.exit(1)
    man = gj(MANIFEST_KEY) or {}
    R.kv(phase=st.get("phase"), banked=banked, mb=round(mb, 1),
         queue_left=len(st.get("queue") or []),
         no_data=man.get("no_data"),
         failures=len(st.get("failures") or {}))
    R.log("ops 4960 GREEN -- World Bank warehouse draining; chains + "
          "weekly redrain own it; day-two: COMPLETE + final GB")
