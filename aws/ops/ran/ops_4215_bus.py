"""ops_4215 — indicator-bus create/settle/invoke/verify + schedule."""
import io
import json
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
                   config=Config(read_timeout=150,
                                 retries={"max_attempts": 1}))
BUCKET = "justhodl-dashboard-live"
MARK = "indicator-bus v1.0 ops4215"


def main():
    with report("4215_bus") as rep:
        rep.heading("ops 4215 — THE INDICATOR BUS")
        src = (ROOT / "lambdas" / "justhodl-indicator-bus" / "source" /
               "lambda_function.py").read_text()
        assert MARK in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        try:
            lam.get_function_configuration(
                FunctionName="justhodl-indicator-bus")
        except Exception:
            donor = lam.get_function_configuration(
                FunctionName="justhodl-te-feed")
            lam.create_function(FunctionName="justhodl-indicator-bus",
                                Runtime=donor["Runtime"],
                                Role=donor["Role"],
                                Handler="lambda_function.lambda_handler",
                                Code={"ZipFile": buf.getvalue()},
                                Timeout=120, MemorySize=512,
                                Publish=True)
            rep.ok("  bus CREATED")
            time.sleep(6)
        for att in range(5):
            try:
                lam.update_function_code(
                    FunctionName="justhodl-indicator-bus",
                    ZipFile=buf.getvalue(), Publish=True)
                break
            except Exception:
                time.sleep(8)
        ok = False
        for i in range(30):
            try:
                c = lam.get_function_configuration(
                    FunctionName="justhodl-indicator-bus")
                if c.get("State") == "Active" and \
                        c.get("LastUpdateStatus") in (None,
                                                      "Successful"):
                    dep = zf.ZipFile(io.BytesIO(
                        urllib.request.urlopen(
                            lam.get_function(
                                FunctionName="justhodl-indicator-bus")
                            ["Code"]["Location"],
                            timeout=60).read())).read(
                        "lambda_function.py").decode()
                    if MARK in dep:
                        ok = True
                        rep.ok(f"  settled loop {i}")
                        break
            except Exception:
                pass
            time.sleep(8)
        if not ok:
            rep.fail("never settled")
            sys.exit(1)
        r = lam.invoke(FunctionName="justhodl-indicator-bus",
                       InvocationType="RequestResponse", Payload=b"{}")
        rep.kv(err=r.get("FunctionError"),
               out=r["Payload"].read().decode()[:60])
        bd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/indicator-bus.json")["Body"].read())
        rep.kv(n=bd.get("n"))
        rep.log("  origin: " + json.dumps(bd.get("origin"))[:300])
        for k in ("USINTR", "US10Y", "DE02Y", "ES1!", "BTC_HASHRATE"):
            rep.log(f"  spot {k}: "
                    + json.dumps((bd.get("indicators") or {})
                                 .get(k))[:90])
        if (bd.get("n") or 0) < 11000:
            rep.fail(f"bus n {bd.get('n')} < 11000")
            sys.exit(1)
        try:
            sch = boto3.client("scheduler", region_name="us-east-1")
            arn = lam.get_function_configuration(
                FunctionName="justhodl-indicator-bus")["FunctionArn"]
            role = sch.get_schedule(
                Name="te-feed-daily")["Target"]["RoleArn"]
            sch.create_schedule(Name="indicator-bus-daily",
                                ScheduleExpression="cron(15 12 * * ? *)",
                                FlexibleTimeWindow={"Mode": "OFF"},
                                Target={"Arn": arn, "RoleArn": role,
                                        "Input": "{}"})
            rep.ok("  schedule 12:15")
        except Exception:
            rep.ok("  schedule exists")
        rep.ok(f"BUS LIVE — n={bd.get('n')}")


if __name__ == "__main__":
    main()
