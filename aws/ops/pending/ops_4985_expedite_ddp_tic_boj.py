"""ops_4985 -- expedite trio: frbddp-full + tic-full + boj-full.

Khalid: expedite all remaining data, budget is not a problem.
  L1 frbddp-full  Fed release packages (Z.1, H.4.1, H.15...)
  L2 tic-full     TIC banking/flows texts (bctype -- his ask)
  L3 boj-full     the ENTIRE BOJ flat-file portal (page harvest)
Per lane: settle(+fallback) -> rate(12 hours) -> kick -> gates;
cards via note-v2; single catalog kick.
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
CAT = "justhodl-provider-catalog"
HUB_KEY = "data/provider-catalog.json"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
ROOTP = Path(__file__).resolve().parents[2]
LANES = {
    "justhodl-frbddp-full": {
        "mark": "v1.0.0 ops4985",
        "rel": "aws/lambdas/justhodl-frbddp-full/source/"
               "lambda_function.py",
        "state": "data/warm/frbddp-full/_state/state.json",
        "items": "rels", "bar": 10,
        "sched": "justhodl-frbddp-full-12h"},
    "justhodl-tic-full": {
        "mark": "v1.0.0 ops4985",
        "rel": "aws/lambdas/justhodl-tic-full/source/"
               "lambda_function.py",
        "state": "data/warm/tic-full/_state/state.json",
        "items": "files", "bar": 5,
        "sched": "justhodl-tic-full-12h"},
    "justhodl-boj-full": {
        "mark": "v1.0.0 ops4985",
        "rel": "aws/lambdas/justhodl-boj-full/source/"
               "lambda_function.py",
        "state": "data/warm/boj-full/_state/state.json",
        "items": "zips", "bar": 3,
        "sched": "justhodl-boj-full-12h"},
}
CATMARK = "ddp-note-v2"

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
                    "lambda_function.py").decode("utf-8",
                                                 "replace")
            if mk in src and \
                    f["Configuration"].get("State") == "Active":
                R.log("  %s settled (%ds)" % (fn,
                                              time.time() - t0))
                return True
        except lam.exceptions.ResourceNotFoundException:
            if time.time() - t0 > 200:
                return None
        except Exception as e:
            R.log("  %s settle: %s" % (fn, str(e)[:80]))
        time.sleep(22)
    return False


with report("ops_4985_expedite_ddp_tic_boj") as R:
    fails = []
    R.section("G-1 markers")
    for fn, cfg in LANES.items():
        if cfg["mark"] not in (ROOTP.parent /
                               cfg["rel"]).read_text():
            R.log("ABORT %s" % fn)
            sys.exit(1)
        R.log("  ok %s" % fn)
    if CATMARK not in (ROOTP.parent /
                       "aws/lambdas/justhodl-provider-catalog/"
                       "source/lambda_function.py").read_text():
        R.log("ABORT catalog mark")
        sys.exit(1)

    R.section("G0 settle + schedules + kicks")
    for fn, cfg in LANES.items():
        r0 = settle(R, fn, cfg["mark"])
        if r0 is None:
            R.log("  %s create-branch skipped -> runner deploy"
                  % fn)
            deploy_lambda(
                report=R, function_name=fn,
                source_dir=ROOTP / ("lambdas/%s/source" % fn),
                env_vars={"S3_BUCKET": B}, timeout=780,
                memory=1536,
                description="Full warehouse v1.0.0 (ops 4985)",
                create_function_url=False, smoke=False)
            r0 = settle(R, fn, cfg["mark"], budget=300)
        if not r0:
            fails.append(fn + ":G0")
            continue
        arn = lam.get_function_configuration(
            FunctionName=fn)["FunctionArn"]
        try:
            sch.create_schedule(
                Name=cfg["sched"], GroupName="default",
                ScheduleExpression="rate(12 hours)",
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                        "Input": "{}"})
        except Exception as e:
            if "Conflict" not in type(e).__name__ and \
                    "exists" not in str(e):
                R.log("  sched %s: %s" % (cfg["sched"],
                                          str(e)[:70]))
        lam.invoke(FunctionName=fn, InvocationType="Event",
                   Payload=b"{}")
    if not settle(R, CAT, CATMARK):
        fails.append("catalog:G0")
    if fails:
        R.log("ops 4985 RED: " + "; ".join(fails))
        sys.exit(1)
    R.log("G0 PASS -- all lanes kicked")

    R.section("G1 lane drives (12min)")
    t0 = time.time()
    done = {}
    while time.time() - t0 < 12 * 60 and len(done) < 3:
        time.sleep(30)
        for fn, cfg in LANES.items():
            if fn in done:
                continue
            st = gj(cfg["state"]) or {}
            items = st.get(cfg["items"]) or {}
            ok_n = sum(1 for v in items.values() if v.get("ok"))
            R.log("  t+%3ds %-20s ok=%d fail=%d%s" % (
                time.time() - t0, fn.replace("justhodl-", ""),
                ok_n, len(st.get("failures") or {}),
                " uni=%s" % st.get("universe")
                if "universe" in st else ""))
            if st.get("as_of") and \
                    float(st.get("lease_until") or 1) == 0:
                done[fn] = (ok_n, st)
    # v2: compare FINAL states, not the mid-run cache
    finals = {}
    for fn, cfg in LANES.items():
        st = gj(cfg["state"]) or {}
        items = st.get(cfg["items"]) or {}
        ok_n = sum(1 for v in items.values() if v.get("ok"))
        finals[fn] = ok_n
        if ok_n < cfg["bar"]:
            fails.append("%s:G1(%d<%d)" % (
                fn.replace("justhodl-", ""), ok_n, cfg["bar"]))
    done = {fn: (n, {}) for fn, n in finals.items()}
    R.log("G1 %s" % ("PASS" if not fails else
                     "FAIL " + "; ".join(fails)))

    R.section("G2 substance")
    ok2 = 0
    try:
        h = s3.head_object(Bucket=B,
                           Key="data/warm/frbddp-full/Z1.zip")
        R.log("  Z1.zip %.1fMB" % (h["ContentLength"] / 1e6))
        ok2 += 1 if h["ContentLength"] > 3_000_000 else 0
    except Exception as e:
        R.log("  Z1 err %s" % str(e)[:70])
    try:
        raw = s3.get_object(
            Bucket=B, Key="data/warm/tic-full/bctype.txt"
        )["Body"].read()
        ok2 += 1 if b"Total claims" in raw and \
            len(raw) > 3000 else 0
        R.log("  bctype.txt %dB has-total=%s" % (
            len(raw), b"Total claims" in raw))
    except Exception as e:
        R.log("  bctype err %s" % str(e)[:70])
    try:
        r_ = s3.list_objects_v2(
            Bucket=B, Prefix="data/warm/boj-full/", MaxKeys=6)
        ks = [o["Key"] for o in r_.get("Contents") or []
              if o["Key"].endswith(".zip")]
        ok2 += 1 if ks else 0
        R.log("  boj zips sample=%s" % [k.rsplit("/", 1)[-1]
                                        for k in ks[:3]])
    except Exception as e:
        R.log("  boj err %s" % str(e)[:70])
    R.log("G2 %s (%d/3)" % ("PASS" if ok2 == 3 else "FAIL", ok2))
    if ok2 < 3:
        fails.append("G2")

    R.section("G3 cards")
    t_mark = datetime.now(timezone.utc).isoformat(
        timespec="seconds")
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
    ok3 = 0
    for slug, needle in [("fed-board", "FULL release packages"),
                         ("tic", "banking+flows text mirror"),
                         ("boj", "FULL flat-file warehouse")]:
        pe = next((p for p in hub.get("providers", [])
                   if p.get("slug") == slug), {}) or {}
        hit = needle in (pe.get("catalog_note") or "")
        ok3 += 1 if hit else 0
        R.log("  %s %s %s" % (slug, "OK" if hit else "MISS",
                              (pe.get("catalog_note")
                               or "")[:110]))
    R.log("G3 %s (%d/3)" % ("PASS" if ok3 == 3 else "FAIL", ok3))
    if ok3 < 3:
        fails.append("G3")

    if fails:
        R.log("ops 4985 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(ddp=done["justhodl-frbddp-full"][0],
         tic=done["justhodl-tic-full"][0],
         boj=done["justhodl-boj-full"][0])
    R.log("ops 4985 GREEN -- Fed packages + TIC banking + the "
          "whole BOJ portal live; next wave coinmetrics/bcb/"
          "banxico/snb/cboe/occ")
