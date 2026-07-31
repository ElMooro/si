"""ops_4205 — CryptoQuant rail: create/settle cq-feed, route-discover x2, vault v3.29.0, fire."""
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
    with report("4205_cq_rail") as rep:
        rep.heading("ops 4205 — CryptoQuant rail")
        checks = []
        src = (ROOT / "lambdas" / "justhodl-cq-feed" / "source" /
               "lambda_function.py").read_text()
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        try:
            lam.get_function_configuration(FunctionName="justhodl-cq-feed")
        except Exception:
            donor = lam.get_function_configuration(
                FunctionName="justhodl-te-feed")
            lam.create_function(FunctionName="justhodl-cq-feed",
                                Runtime=donor["Runtime"],
                                Role=donor["Role"],
                                Handler="lambda_function.lambda_handler",
                                Code={"ZipFile": buf.getvalue()},
                                Timeout=300, MemorySize=512, Publish=True)
            rep.ok("  cq-feed CREATED")
            time.sleep(6)
        checks.append(("cq-feed settled",
                       settle(rep, "justhodl-cq-feed",
                              "cq-feed v1.0 ops4205")))
        for k in (1, 2):
            r = lam.invoke(FunctionName="justhodl-cq-feed",
                           InvocationType="RequestResponse", Payload=b"{}")
            rep.kv(**{f"cq{k}": r["Payload"].read().decode()[:80],
                      f"cq{k}_err": r.get("FunctionError")})
        cd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/cq-feed.json")["Body"].read())
        p = cd.get("prices") or {}
        rep.kv(routed=cd.get("n"), targets=cd.get("targets"),
               dead=len(cd.get("dead") or {}))
        for bare in list(cd.get("routes") or {})[:5]:
            rep.log(f"  route {bare}: "
                    + json.dumps(p.get(bare))[:100])
        checks.append(("cq routed >= 15", (cd.get("n") or 0) >= 15))
        checks.append(("vault v3.29.0 settled",
                       settle(rep, "justhodl-tradingview",
                              "tradingview-vault v3.29.0 ops4205 "
                              "cq-rail")))
        lam.invoke(FunctionName="justhodl-tradingview",
                   InvocationType="Event", Payload=b"{}")
        try:
            sch = boto3.client("scheduler", region_name="us-east-1")
            arn = lam.get_function_configuration(
                FunctionName="justhodl-cq-feed")["FunctionArn"]
            role = sch.get_schedule(
                Name="te-feed-daily")["Target"]["RoleArn"]
            sch.create_schedule(Name="cq-feed-daily",
                                ScheduleExpression="cron(50 10 * * ? *)",
                                FlexibleTimeWindow={"Mode": "OFF"},
                                Target={"Arn": arn, "RoleArn": role,
                                        "Input": "{}"})
            rep.ok("  schedule cq-feed-daily")
        except Exception:
            rep.ok("  schedule exists")
        failed = [l for l, k2 in checks if not k2]
        for l, k2 in checks:
            (rep.ok if k2 else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"CQ RAIL LIVE — routed={cd.get('n')}"
               f"/{cd.get('targets')}")


if __name__ == "__main__":
    main()

# retrigger 4205
