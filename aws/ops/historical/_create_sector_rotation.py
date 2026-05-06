"""Create justhodl-sector-rotation + 6h schedule + smoke."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-sector-rotation"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-sector-rotation/source"
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def make_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, SOURCE_DIR)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def main():
    with report("create_sector_rotation") as r:
        r.heading("Create justhodl-sector-rotation + 6h schedule")

        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        env_vars = {"POLYGON_KEY": POLYGON_KEY}

        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Timeout=120,
                MemorySize=512,
                Role=ROLE_ARN,
                Environment={"Variables": env_vars},
            )
            r.ok("  ✓ updated existing")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Role=ROLE_ARN,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zb},
                Timeout=120,
                MemorySize=512,
                Architectures=["x86_64"],
                Environment={"Variables": env_vars},
            )
            r.ok("  ✓ created")

        for _ in range(20):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)

        r.heading("EventBridge schedule (every 6 hours)")
        rule_name = f"{LAMBDA_NAME}-6h"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(6 hours)", State="ENABLED")
        fn_arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
        events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            lam.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        r.ok("  ✓ wired")

        r.heading("Smoke test")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:400]}")

        r.heading("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/sector-rotation.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  market_breadth: {d.get('market_breadth')}")
            r.log(f"  desc: {d.get('market_breadth_description')}")
            r.log(f"  spy_close: ${d.get('spy_close')}")
            r.log(f"  📊 Top 5 sectors by 63d RS vs SPY:")
            for s in d.get("sectors", [])[:5]:
                rs = s.get("rs_vs_spy", {})
                rets = s.get("returns", {})
                r.log(f"    {s['ticker']:5s} {s['name']:25s} 63d_ret={rets.get(63,0):>7.2f}%  rs={rs.get(63,0):>+6.2f}%  {s.get('regime')}")
            r.log(f"  📉 Bottom 3:")
            for s in d.get("sectors", [])[-3:]:
                rs = s.get("rs_vs_spy", {})
                rets = s.get("returns", {})
                r.log(f"    {s['ticker']:5s} {s['name']:25s} 63d_ret={rets.get(63,0):>7.2f}%  rs={rs.get(63,0):>+6.2f}%  {s.get('regime')}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
