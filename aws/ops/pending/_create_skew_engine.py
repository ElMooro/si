"""Create justhodl-skew-engine + hourly schedule + smoke test."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-skew-engine"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-skew-engine/source"
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
    with report("create_skew_engine") as r:
        r.heading("Create justhodl-skew-engine + hourly schedule")

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
                Timeout=180,
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
                Timeout=180,
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

        # Schedule: hourly weekdays during/around market hours
        r.heading("EventBridge schedule (hourly)")
        rule_name = f"{LAMBDA_NAME}-hourly"
        events.put_rule(Name=rule_name, ScheduleExpression="rate(1 hour)", State="ENABLED",
                        Description="Compute IV skew + term structure every hour")
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
        r.log(f"  resp: {body[:600]}")

        r.heading("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/skew.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  duration_s: {d.get('duration_s')}")
            r.log(f"  summary: {d.get('summary')}")
            for u, det in d.get("underlyings", {}).items():
                if det.get("error"):
                    r.log(f"  {u}: ERROR {det['error']}")
                    continue
                r.log(f"  {u}: contracts={det.get('n_contracts')}  expiries={det.get('n_expiries')}")
                r.log(f"    skew_regime: {det.get('skew_regime')} — {det.get('skew_desc')}")
                r.log(f"    term_structure: {det.get('term_structure')} — {det.get('term_desc')}")
                f = det.get("front", {})
                b = det.get("back", {})
                r.log(f"    front: atm_iv={f.get('atm_iv')}  rr={f.get('risk_reversal')}  skew_25d={f.get('skew_25d')}  butterfly={f.get('butterfly')}")
                r.log(f"    back:  atm_iv={b.get('atm_iv')}  rr={b.get('risk_reversal')}  skew_25d={b.get('skew_25d')}  butterfly={b.get('butterfly')}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
