# rerun-marker: 1777918512
"""Create justhodl-wave-signal-logger + 6h schedule + smoke."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-wave-signal-logger"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-wave-signal-logger/source"

POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


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
    with report("create_wave_signal_logger") as r:
        r.heading("Create justhodl-wave-signal-logger + 6h schedule")

        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        env_vars = {"POLYGON_KEY": POLYGON_KEY, "FMP_KEY": FMP_KEY}

        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Timeout=300,
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
                Timeout=300,
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

        # Schedule: 6h offset from main signal-logger by 30min to avoid concurrent DDB writes
        # Main logger runs at 0/6/12/18 — we run at 30min past those
        r.heading("EventBridge schedule (every 6 hours, offset 30min)")
        rule_name = f"{LAMBDA_NAME}-6h"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="cron(30 0,6,12,18 ? * * *)",
            State="ENABLED",
            Description="Wave 1+2 signal logger — 6h offset from main signal-logger",
        )
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

        # Smoke
        r.heading("Smoke test — first run")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:1000]}")

        # DDB verify — count new signal_types
        r.heading("DDB verify (signals just written)")
        try:
            from boto3.dynamodb.conditions import Attr
            from datetime import datetime, timezone, timedelta
            cutoff = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
            tbl = ddb.Table("justhodl-signals")
            resp = tbl.scan(
                Limit=200,
                FilterExpression=Attr("logged_at").gte(cutoff) & Attr("source").eq("wave-signal-logger-v1"),
            )
            items = resp.get("Items", [])
            r.log(f"  signals from this Lambda in last 5 min: {len(items)}")
            from collections import Counter
            type_counts = Counter()
            for it in items:
                type_counts[it.get("signal_type", "?")] += 1
            for t, n in type_counts.most_common():
                r.log(f"    {t:30s} n={n}")
            r.log("\n  Sample signals:")
            for it in items[:5]:
                bp = it.get("baseline_price")
                bp_str = f"${bp}" if bp else "no-price"
                r.log(f"    {it.get('signal_type'):20s} {it.get('measure_against', ''):8s} {it.get('predicted_direction'):8s} conf={it.get('confidence')}  {bp_str}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
