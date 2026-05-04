"""Create justhodl-calibration-snapshot Lambda + 30min schedule + smoke test."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-calibration-snapshot"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-calibration-snapshot/source"

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
    with report("create_calibration_snapshot") as r:
        r.heading("Create justhodl-calibration-snapshot + 30min schedule")

        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

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
            )
            r.ok("  ✓ created")

        # Wait for active
        for _ in range(20):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)

        # EventBridge 30min schedule
        r.heading("EventBridge schedule (every 30 minutes)")
        rule_name = f"{LAMBDA_NAME}-30min"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(30 minutes)",
            State="ENABLED",
            Description="Refresh calibration snapshot every 30 min",
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
        r.heading("Smoke test")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:400]}")

        # S3 verify
        r.heading("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/calibration-snapshot.json")
            d = json.loads(obj["Body"].read())
            summary = d.get("summary", {})
            for k, v in summary.items():
                r.log(f"  {k}: {v}")
            r.log(f"  signals tracked: {len(d.get('signals', []))}")
            r.log(f"  top 5 by accuracy:")
            for s in d.get("top_accuracy", [])[:5]:
                acc = s.get("overall_accuracy")
                w = s.get("weight")
                n = s.get("overall_n")
                r.log(f"    {s['signal_type']:30s} acc={acc:.4f} weight={w:.4f} n={n}" if acc and w else f"    {s['signal_type']:30s}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
