"""
Create justhodl-etf-flows Lambda + 6h schedule + smoke test.
"""
import os
import time
import zipfile
import io
import json
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-etf-flows"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-etf-flows/source"
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
    with report("create_etf_flows") as r:
        r.heading("Create justhodl-etf-flows + smoke test")

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
                Environment={"Variables": {"POLYGON_KEY": POLYGON_KEY}},
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
                Environment={"Variables": {"POLYGON_KEY": POLYGON_KEY}},
                Description="ETF flow tracker — daily $ volume z-scores",
            )
            r.ok(f"  ✓ created")

        r.section("EventBridge 6h schedule")
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
            r.ok(f"  ✓ schedule wired")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        # Wait for create to finalize
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
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/etf-flows.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {data.get('generated_at')}")
            r.log(f"  duration_s: {data.get('duration_s')}")
            r.log(f"  n_etfs: {data.get('n_etfs_analyzed')}")
            r.log(f"  n_heavy_inflow: {len(data.get('heavy_inflow') or [])}")
            r.log(f"  n_heavy_outflow: {len(data.get('heavy_outflow') or [])}")
            r.log(f"  n_unusual: {len(data.get('unusual_vol') or [])}")

            r.section("📊 By category")
            for cat, v in (data.get("by_category") or {}).items():
                r.log(f"    {cat:22s} n={v['n_etfs']:>2} aum=${v['total_aum_b']:>7,.1f}B today_$vol=${v['total_today_dollar_vol_b']:>5.2f}B avg_z={v['avg_dvol_z']:>+5} avg_r1d={v['avg_return_1d_pct']:>+5}% sig={v['category_signal']}")

            r.section("💰 Heavy inflow")
            for x in data.get("heavy_inflow") or []:
                r.log(f"    {x['ticker']:6s} z={x.get('dvol_z_score'):>+5} r1d={x.get('return_1d_pct'):>+5}% $vol=${x.get('today_dollar_vol_b'):>5.2f}B")

            r.section("💸 Heavy outflow")
            for x in data.get("heavy_outflow") or []:
                r.log(f"    {x['ticker']:6s} z={x.get('dvol_z_score'):>+5} r1d={x.get('return_1d_pct'):>+5}% $vol=${x.get('today_dollar_vol_b'):>5.2f}B")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
