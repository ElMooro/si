"""ops_4989 v2 (REG in checkout) -- HK + Chile + Korea on data.html (Khalid's ask:
manufacturing, exports, ports, imports, industrial).

  G-1 markers  G0 settle(+fallback) + rate(24 hours)
  G1 run: hk_ok>=8 and cl_ok>=6 (kr reports status, never fails)
  G2 substance: one HK resource gunzips to real CSV rows; one CL
  G3 three new cards render with asia-note-v2
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
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-asia-trade-full"
CAT = "justhodl-provider-catalog"
STATE_KEY = "data/warm/asia-trade/_state/state.json"
HUB_KEY = "data/provider-catalog.json"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
MARKS = {FN: ("v1.0.0 ops4989",
              "aws/lambdas/justhodl-asia-trade-full/source/"
              "lambda_function.py"),
         CAT: ("asia-note-v2",
               "aws/lambdas/justhodl-provider-catalog/source/"
               "lambda_function.py")}
ROOTP = Path(__file__).resolve().parents[2]

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300,
                                 connect_timeout=10,
                                 retries={"max_attempts": 0}))
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


with report("ops_4989_asia_trade_launch") as R:
    fails = []
    R.section("G-1 markers")
    for fn, (mk, rel) in MARKS.items():
        if mk not in (ROOTP.parent / rel).read_text():
            R.log("ABORT %s" % fn)
            sys.exit(1)
        R.log("  ok %s" % fn)

    R.section("G0 settle + schedule")
    r0 = settle(R, FN, MARKS[FN][0])
    if r0 is None:
        R.log("  create-branch skipped -> runner deploy")
        cfg = json.load(open(ROOTP.parent /
                             "aws/lambdas/"
                             "justhodl-asia-trade-full/"
                             "config.json"))
        deploy_lambda(
            report=R, function_name=FN,
            source_dir=ROOTP /
            "lambdas/justhodl-asia-trade-full/source",
            env_vars={"S3_BUCKET": B}, timeout=780, memory=1024,
            description=cfg["description"],
            create_function_url=False, smoke=False)
        r0 = settle(R, FN, MARKS[FN][0], budget=300)
    if not r0 or not settle(R, CAT, MARKS[CAT][0]):
        R.log("G0 FAIL")
        sys.exit(1)
    arn = lam.get_function_configuration(
        FunctionName=FN)["FunctionArn"]
    try:
        sch.create_schedule(
            Name="justhodl-asia-trade-24h", GroupName="default",
            ScheduleExpression="rate(24 hours)",
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED",
            Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                    "Input": "{}"})
        R.log("  schedule created")
    except Exception as e:
        if "Conflict" not in type(e).__name__ and \
                "exists" not in str(e):
            R.log("  sched: %s" % str(e)[:80])

    R.section("G1 run (sync)")
    try:
        resp = lam.invoke(FunctionName=FN,
                          InvocationType="RequestResponse",
                          Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8", "replace")
        R.log("  invoke: err=%s %s" % (resp.get("FunctionError"),
                                       body[:200]))
    except Exception as e:
        R.log("  invoke: %s" % str(e)[:110])
    st = gj(STATE_KEY) or {}

    def okn(cc):
        return sum(1 for v in (st.get(cc, {}).get("res")
                               or {}).values() if v.get("ok"))
    hk, cl, kr = okn("hk"), okn("cl"), okn("kr")
    for k, v in list((st.get("failures") or {}).items())[:10]:
        R.log("    fail %s: %s" % (k, str(v)[:80]))
    R.log("  hk=%d cl=%d kr=%d kr_status=%s" % (
        hk, cl, kr, st.get("kr", {}).get("status", "")[:90]))
    ok1 = hk >= 8 and cl >= 6
    R.log("G1 %s" % ("PASS" if ok1 else "FAIL"))
    if not ok1:
        fails.append("G1")

    R.section("G2 substance")
    ok2 = 0
    for cc in ("hk", "cl"):
        try:
            r_ = s3.list_objects_v2(
                Bucket=B, Prefix="data/warm/asia-trade/%s/" % cc,
                MaxKeys=4)
            k0 = next((o["Key"] for o in r_.get("Contents") or []
                       if o["Key"].endswith(".csv.gz")),
                      (r_.get("Contents") or [{}])[0].get("Key"))
            raw = gzip.decompress(s3.get_object(
                Bucket=B, Key=k0)["Body"].read())
            lines = raw.count(b"\n")
            R.log("  %s %s rows~%d %dB" % (
                cc, k0.rsplit("/", 1)[-1][:50], lines, len(raw)))
            ok2 += 1 if lines >= 5 else 0
        except Exception as e:
            R.log("  %s substance err %s" % (cc, str(e)[:80]))
    R.log("G2 %s (%d/2)" % ("PASS" if ok2 == 2 else "FAIL", ok2))
    if ok2 < 2:
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
    while time.time() - t0 < 10 * 60:
        time.sleep(30)
        hub = gj(HUB_KEY) or {}
        if (hub.get("as_of") or "") >= t_mark:
            break
    ok3 = 0
    for sg in ("hk-data", "cl-datos", "kr-ecos"):
        pe = next((p for p in hub.get("providers", [])
                   if p.get("slug") == sg), {}) or {}
        hit = bool(pe) and (pe.get("catalog_note") or "")
        ok3 += 1 if hit else 0
        R.log("  %s %s %s" % (sg, "OK" if hit else "MISS",
                              (pe.get("catalog_note")
                               or "")[:100]))
    R.log("G3 %s (%d/3)" % ("PASS" if ok3 == 3 else "FAIL", ok3))
    if ok3 < 3:
        fails.append("G3")

    if fails:
        R.log("ops 4989 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(hk=hk, cl=cl, kr_status=st.get("kr", {}
                                        ).get("status", "")[:60])
    R.log("ops 4989 GREEN -- HK + Chile trade/industry live on "
          "data.html; Korea drains the moment an ECOS key lands "
          "in the vault")
