"""ops_4035 — fire the TV workbench: settle-or-create, invoke, schedule."""
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

lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=90, retries={"max_attempts": 1}))
sch = boto3.client("scheduler", region_name="us-east-1")
FN = "justhodl-tv-workbench"
MARK = "tv-workbench v1.1 ops4035 full-fidelity"


def zip_src():
    b = io.BytesIO()
    with zf.ZipFile(b, "w", zf.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py",
                   (ROOT / "lambdas" / FN / "source" /
                    "lambda_function.py").read_text())
    return b.getvalue()


def main():
    with report("4035_fire_wb_v11") as rep:
        rep.heading("ops 4019 — workbench: create/settle + fire + schedule")
        try:
            lam.get_function_configuration(FunctionName=FN)
            rep.log("  function exists")
        except lam.exceptions.ResourceNotFoundException:
            donor = lam.get_function_configuration(
                FunctionName="justhodl-data-census")
            lam.create_function(FunctionName=FN, Runtime=donor["Runtime"],
                                Role=donor["Role"],
                                Handler="lambda_function.lambda_handler",
                                Timeout=300, MemorySize=1024,
                                Code={"ZipFile": zip_src()})
            rep.ok("  created from donor role")
        settled = False
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") != "InProgress":
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    lam.get_function(FunctionName=FN)["Code"]["Location"],
                    timeout=60).read())).read("lambda_function.py").decode()
                if MARK in dep:
                    settled = True
                    break
                try:
                    lam.update_function_code(FunctionName=FN,
                                             ZipFile=zip_src(), Publish=True)
                    rep.log("  pushed v1.0 from runner")
                except lam.exceptions.ResourceConflictException:
                    pass
            time.sleep(8)
        if not settled:
            rep.fail("marker never settled")
            sys.exit(1)
        rep.ok("  v1.0 settled by marker")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b'{"source": "ops4019"}')
        rep.ok("FIRED — workbench assembling")
        try:
            role = sch.get_schedule(Name="data-census-daily")["Target"]["RoleArn"]
            kw = dict(Name="tv-workbench-daily",
                      ScheduleExpression="cron(55 12 * * ? *)",
                      FlexibleTimeWindow={"Mode": "OFF"},
                      Target={"Arn": "arn:aws:lambda:us-east-1:857687956942:"
                                     "function:" + FN,
                              "RoleArn": role,
                              "Input": '{"source": "schedule"}'},
                      State="ENABLED")
            try:
                sch.create_schedule(**kw)
            except sch.exceptions.ConflictException:
                sch.update_schedule(**kw)
            st = sch.get_schedule(Name="tv-workbench-daily")
            rep.ok(f"  schedule {st.get('State')} {st.get('ScheduleExpression')}")
        except Exception as e:
            rep.log(f"  schedule: {type(e).__name__}: {str(e)[:90]}")


if __name__ == "__main__":
    main()
