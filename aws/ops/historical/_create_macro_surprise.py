"""
Create justhodl-macro-surprise Lambda + 6h schedule + smoke test.
"""
import os
import time
import zipfile
import io
import json
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-macro-surprise"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-macro-surprise/source"
FRED_KEY = "2f057499936072679d8843d7fce99989"

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
    with report("create_macro_surprise") as r:
        r.heading("Create justhodl-macro-surprise + smoke test")

        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            r.log(f"  function exists — updating")
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                MemorySize=512,
                Timeout=600,
                Environment={"Variables": {"FRED_KEY": FRED_KEY}},
            )
            r.ok(f"  ✓ updated")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Role=ROLE_ARN,
                Code={"ZipFile": zb},
                MemorySize=512,
                Timeout=600,
                Environment={"Variables": {"FRED_KEY": FRED_KEY}},
                Description="Macro Surprise Index — CESI proxy from FRED",
            )
            r.ok(f"  ✓ created")

        r.section("EventBridge schedule (6h)")
        try:
            rule_name = f"{LAMBDA_NAME}-6h"
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="rate(6 hours)",
                State="ENABLED",
                Description=f"6h trigger for {LAMBDA_NAME}",
            )
            arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
            events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": arn}])
            try:
                lam.add_permission(
                    FunctionName=LAMBDA_NAME,
                    StatementId=f"{rule_name}-perm",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
                )
            except lam.exceptions.ResourceConflictException:
                pass
            r.ok(f"  ✓ wired")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        time.sleep(8)

        r.section("Smoke test")
        try:
            t0 = time.time()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=b"{}")
            payload = inv["Payload"].read().decode()
            r.log(f"  status: {inv['StatusCode']} duration: {time.time()-t0:.1f}s")
            r.log(f"  resp: {payload[:400]}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ {inv['FunctionError']}")
                return
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        r.section("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/macro-surprise.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  composite_z: {data.get('composite_z')}")
            r.log(f"  growth_z: {data.get('growth_z')}")
            r.log(f"  inflation_z: {data.get('inflation_z')}")
            r.log(f"  regime: {data.get('regime')}")
            r.log(f"  desc: {data.get('regime_description')}")
            r.log(f"  n_indicators: {data.get('n_indicators_computed')}/{data.get('n_indicators_tracked')}")

            r.section("📊 By category")
            for cat, v in (data.get("by_category") or {}).items():
                r.log(f"    {cat:14s} avg_z={v['avg_z']:>+5} n_beat={v['n_beat']} n_miss={v['n_miss']} dir={v['direction']}")

            r.section("🟢 Top BEATS (data above trend)")
            for x in (data.get("top_beats") or [])[:5]:
                r.log(f"    {x['series_id']:14s} {x['name'][:38]:38s} z={x['z_score']:>+5} dir={x['direction']}")

            r.section("🔴 Top MISSES (data below trend)")
            for x in (data.get("top_misses") or [])[:5]:
                r.log(f"    {x['series_id']:14s} {x['name'][:38]:38s} z={x['z_score']:>+5} dir={x['direction']}")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
