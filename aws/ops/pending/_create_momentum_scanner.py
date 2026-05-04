"""Deploy justhodl-momentum-scanner Lambda + daily 12:30 UTC weekday schedule + smoke test."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-momentum-scanner"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-momentum-scanner/source"
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
    with report("create_momentum_scanner") as r:
        r.heading("Deploy justhodl-momentum-scanner")

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
                Timeout=600,            # 10 min — universe of 503 tickers
                MemorySize=2048,         # heavy parallel processing
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
                Timeout=600,
                MemorySize=2048,
                Architectures=["x86_64"],
                Environment={"Variables": env_vars},
            )
            r.ok("  ✓ created")

        for _ in range(20):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)

        # Schedule: daily 12:30 UTC weekdays
        r.heading("EventBridge schedule (weekdays 12:30 UTC)")
        rule_name = f"{LAMBDA_NAME}-daily"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="cron(30 12 ? * MON-FRI *)",
            State="ENABLED",
            Description="Universe momentum scanner — daily 12:30 UTC weekdays",
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

        # Smoke test
        r.heading("Smoke test (will take 30-90s)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:400]}")

        # S3 verify
        r.heading("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/momentum-scanner.json")
            d = json.loads(obj["Body"].read())
            s_blob = d.get("summary", {})
            for k, v in s_blob.items():
                r.log(f"  {k}: {v}")

            r.log(f"")
            r.log(f"  📊 Top 10 composite momentum:")
            for x in (d.get("rankings") or {}).get("composite_top_50", [])[:10]:
                r.log(f"    {x['ticker']:6s} score={x.get('composite_score',0):.1f}  3m={x.get('ret_3m',0):>+7.2f}%  12m={x.get('ret_12m',0):>+7.2f}%  vol60={x.get('vol_60d',0):.1f}%  sector={x.get('sector','?')[:25]}")

            r.log(f"")
            r.log(f"  📉 Bottom 5 composite (mean reversion candidates):")
            for x in (d.get("rankings") or {}).get("bottom_50_composite", [])[:5]:
                r.log(f"    {x['ticker']:6s} score={x.get('composite_score',0):.1f}  3m={x.get('ret_3m',0):>+7.2f}%")

            r.log(f"")
            r.log(f"  📈 By sector (avg composite):")
            for sec, info in list((d.get("by_sector") or {}).items())[:8]:
                r.log(f"    {sec[:30]:30s} n={info.get('n',0):3d}  avg={info.get('avg_composite',0):.1f}  top={info.get('top_5',[{}])[0].get('ticker','?')}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
