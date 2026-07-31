"""ops_4198 — te-feed create+settle+sweep, vault v3.28.0 te-primary, fire."""
import io
import json
import re
import sys
import time
import urllib.request
import zipfile as zf
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=290,
                                 retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0"}


def fetch(url, timeout=90):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:140]


def settle(rep, name, mark):
    src = (ROOT / "lambdas" / name / "source" /
           "lambda_function.py").read_text()
    if mark not in src:
        rep.fail(f"  {name}: marker missing in checkout")
        return False
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py", src)
        if name == "justhodl-tradingview":
            for sh in sorted((ROOT / "shared").glob("*.py")):
                z.writestr(sh.name, sh.read_text())
    for att in range(5):
        try:
            lam.update_function_code(FunctionName=name,
                                     ZipFile=buf.getvalue(), Publish=True)
            break
        except Exception:
            time.sleep(8)
    for i in range(35):
        try:
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") in (None, "Successful"):
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    lam.get_function(FunctionName=name)["Code"]
                    ["Location"], timeout=60).read())).read(
                    "lambda_function.py").decode()
                if mark in dep:
                    rep.ok(f"  {name} settled at loop {i}")
                    return True
        except Exception:
            pass
        time.sleep(9)
    rep.fail(f"  {name} never settled")
    return False




def main():
    with report("4198_te_primary") as rep:
        rep.heading("ops 4198 — TE paid primary")
        checks = []
        src = (ROOT / "lambdas" / "justhodl-te-feed" / "source" /
               "lambda_function.py").read_text()
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        try:
            lam.get_function_configuration(FunctionName="justhodl-te-feed")
        except Exception:
            donor = lam.get_function_configuration(
                FunctionName="justhodl-families-feed")
            lam.create_function(FunctionName="justhodl-te-feed",
                                Runtime=donor["Runtime"],
                                Role=donor["Role"],
                                Handler="lambda_function.lambda_handler",
                                Code={"ZipFile": buf.getvalue()},
                                Timeout=300, MemorySize=512, Publish=True)
            rep.ok("  te-feed CREATED")
            time.sleep(6)
        checks.append(("te-feed settled",
                       settle(rep, "justhodl-te-feed",
                              "te-feed v1.0 ops4198")))
        for k in (1, 2):
            r = lam.invoke(FunctionName="justhodl-te-feed",
                           InvocationType="RequestResponse", Payload=b"{}")
            rep.kv(**{f"sweep{k}_err": r.get("FunctionError"),
                      f"sweep{k}": r["Payload"].read().decode()[:80]})
        td = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/te-feed.json")["Body"].read())
        p = td.get("prices") or {}
        rep.kv(te_n=td.get("n"), countries=len(
            td.get("countries_done") or []))
        for bare in ("USINTR", "USIRYY", "CNGDPYY", "MXBOT"):
            rep.log(f"  spot {bare}: {json.dumps(p.get(bare))[:110]}")
        checks.append(("te n >= 1200", (td.get("n") or 0) >= 1200))
        checks.append(("USINTR present", "USINTR" in p))

        checks.append(("vault v3.28.0 settled",
                       settle(rep, "justhodl-tradingview",
                              "tradingview-vault v3.28.0 ops4198 "
                              "te-primary")))
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        rep.ok("  vault fired — thaw + 4199 convert")

        try:
            sch = boto3.client("scheduler", region_name="us-east-1")
            arn = lam.get_function_configuration(
                FunctionName="justhodl-te-feed")["FunctionArn"]
            role = sch.get_schedule(
                Name="families-feed-daily")["Target"]["RoleArn"]
            sch.create_schedule(Name="te-feed-daily",
                                ScheduleExpression="cron(0 11 * * ? *)",
                                FlexibleTimeWindow={"Mode": "OFF"},
                                Target={"Arn": arn, "RoleArn": role,
                                        "Input": "{}"})
            rep.ok("  schedule te-feed-daily cron(0 11)")
        except Exception:
            rep.ok("  schedule exists")

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"TE PRIMARY LIVE — n={td.get('n')}")


if __name__ == "__main__":
    main()
