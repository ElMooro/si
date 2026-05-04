"""Create justhodl-momentum-scanner + daily schedule + smoke."""
import io, json, os, time, zipfile, boto3
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
        r.heading("Create justhodl-momentum-scanner + daily schedule")

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
                Timeout=600,
                MemorySize=2048,
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

        r.heading("EventBridge schedule (daily at 12:30 UTC weekdays)")
        rule_name = f"{LAMBDA_NAME}-daily"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="cron(30 12 ? * MON-FRI *)",
            State="ENABLED",
            Description="Universe-wide momentum scan daily 12:30 UTC weekdays",
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

        r.heading("Smoke test (this will take 1-2 minutes)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:600]}")

        r.heading("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/momentum-scanner.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  universe_size: {d.get('universe_size')}")
            r.log(f"  n_with_data: {d.get('n_with_data')}")
            r.log(f"  duration_s: {d.get('duration_s')}")
            r.log(f"  📈 Top 10 by composite score:")
            for s in d.get("top_50_composite", [])[:10]:
                ret_3m = s.get("ret_3m", 0)
                r.log(f"    {s['symbol']:6s} {s.get('name','')[:28]:28s} score={s.get('composite_score')} ret_3m={ret_3m:+.1f}% sector={s.get('sector','')[:20]}")
            r.log(f"  📉 Bottom 5:")
            for s in d.get("bottom_50_composite", [])[-5:]:
                r.log(f"    {s['symbol']:6s} {s.get('name','')[:28]:28s} score={s.get('composite_score')}")
            r.log(f"  🏆 Top sectors by avg composite:")
            for s in d.get("sector_breakdown", [])[:5]:
                r.log(f"    {s['sector']:30s} avg={s['avg_composite']} n={s['n_stocks']} top={s['top_stock']}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
