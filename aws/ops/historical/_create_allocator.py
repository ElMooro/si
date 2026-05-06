"""Create justhodl-allocator + 4h schedule + smoke test."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-allocator"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-allocator/source"

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
    with report("create_allocator") as r:
        r.heading("Create justhodl-allocator + 4h schedule")

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
                Timeout=60,
                MemorySize=256,
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
                Timeout=60,
                MemorySize=256,
                Architectures=["x86_64"],
            )
            r.ok("  ✓ created")

        for _ in range(20):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)

        r.heading("EventBridge schedule (every 4 hours)")
        rule_name = f"{LAMBDA_NAME}-4h"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(4 hours)", State="ENABLED")
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

        r.heading("S3 verify — current allocation")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/allocator.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  regime_headline: {d.get('regime_headline')}")
            r.log(f"  n_rules_applied: {d.get('n_rules_applied')}/{d.get('n_rules_total')}")
            r.log(f"  cash_buffer: {d.get('cash_buffer_pct')}%")
            r.log("")
            r.log("  📊 ASSET SCORES (sorted highest → lowest):")
            for a in d.get("asset_scores", []):
                r.log(f"    {a['emoji']} {a['ticker']:5s} {a['name']:25s} score={a['score']:>+6.1f}  call={a['call']:13s} conviction={a['conviction']:6s} n={a['n_signals']}")
            r.log("")
            r.log("  💼 RECOMMENDED WEIGHTS:")
            for ticker, w in sorted(d.get("recommended_weights_pct", {}).items(), key=lambda x: -x[1]):
                if w > 0:
                    r.log(f"    {ticker:6s} {w}%")
            r.log("")
            r.log("  🟢 OVERWEIGHTS: " + ", ".join(d.get("overweights", []) or ["—"]))
            r.log("  🔴 UNDERWEIGHTS: " + ", ".join(d.get("underweights", []) or ["—"]))
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
