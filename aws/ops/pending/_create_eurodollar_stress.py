"""Deploy justhodl-eurodollar-stress + hourly EventBridge schedule + smoke test."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-eurodollar-stress"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-eurodollar-stress/source"

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


def fetch_fred_key():
    """Pull FRED key from any existing Lambda's env vars to seed the new one."""
    candidates = [
        "justhodl-yield-curve", "justhodl-macro-surprise",
        "justhodl-financial-secretary", "justhodl-daily-report-v3",
    ]
    for c in candidates:
        try:
            cfg = lam.get_function_configuration(FunctionName=c)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            for k in ["FRED_API_KEY", "FRED_KEY"]:
                if k in env and env[k]:
                    return env[k], f"{c}.{k}"
        except Exception:
            continue
    return None, None


def main():
    with report("create_eurodollar_stress") as r:
        r.heading("1) Create justhodl-eurodollar-stress Lambda")
        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        fred_key, source = fetch_fred_key()
        if fred_key:
            r.ok(f"  ✓ FRED key sourced from {source}, len={len(fred_key)}")
        else:
            r.log("  ⚠ no FRED key found in known Lambdas — using inline fallback")

        env_vars = {}
        if fred_key:
            env_vars["FRED_API_KEY"] = fred_key

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
                Description="8-signal post-LIBOR USD/Eurodollar funding stress monitor",
            )
            r.ok("  ✓ created")

        # Wait for active state
        for _ in range(20):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        r.log(f"  state: {cfg['State']}, last_update: {cfg.get('LastUpdateStatus')}")

        r.heading("2) EventBridge schedule (rate(1 hour))")
        rule_name = f"{LAMBDA_NAME}-1h"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(1 hour)",
            State="ENABLED",
            Description="USD/Eurodollar funding stress monitor — pulls FRED hourly, scores 8 signals",
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

        r.heading("3) Smoke test — first run (will hit FRED 8 times, ~10-30s)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:600]}")

        r.heading("4) S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/eurodollar-stress.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  composite_score: {d.get('composite_score')}")
            r.log(f"  severity: {d.get('severity')}  regime: {d.get('regime')}")
            r.log(f"  n_signals_used: {d.get('n_signals_used')}/{d.get('n_signals_total')}")
            r.log(f"  duration_s: {d.get('duration_s')}")
            r.log("")
            r.log("  Signal breakdown:")
            for s in d.get("signals", []):
                bar = "█" * int(s["score_0_100"] / 5)
                r.log(f"    {s['id']:14s}  value={s['value']:>10}  score={s['score_0_100']:>5.1f}/100  {bar}")
            if d.get("hot_signals"):
                r.log("")
                r.log("  🔴 hot signals (>=70):")
                for s in d["hot_signals"]:
                    r.log(f"    {s['id']:14s}  score={s['score']:.1f} ({s['label']})")
            if d.get("cold_signals"):
                r.log("")
                r.log("  🟢 cold signals (<=30):")
                for s in d["cold_signals"]:
                    r.log(f"    {s['id']:14s}  score={s['score']:.1f} ({s['label']})")
            if d.get("failures"):
                r.log("")
                r.log("  ⚠ failures:")
                for f in d["failures"]:
                    r.log(f"    {f}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
