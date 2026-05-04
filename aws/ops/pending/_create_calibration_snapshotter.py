"""Deploy justhodl-calibration-snapshotter + weekly Sunday 12:00 UTC schedule + seed first snapshot."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-calibration-snapshotter"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-calibration-snapshotter/source"

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
    with report("create_calibration_snapshotter") as r:
        r.heading("1) Create justhodl-calibration-snapshotter Lambda")
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
                Description="Weekly calibration weight + accuracy snapshotter for time-series tracking",
            )
            r.ok("  ✓ created")

        for _ in range(20):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        r.log(f"  state: {cfg['State']}, last update: {cfg.get('LastUpdateStatus')}")

        r.heading("2) EventBridge schedule — Sundays 12:00 UTC")
        # Calibrator runs Sunday 09:00 UTC; snapshot at 12:00 UTC after weights settle
        rule_name = f"{LAMBDA_NAME}-weekly"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="cron(0 12 ? * SUN *)",
            State="ENABLED",
            Description="Weekly calibration snapshot — runs Sundays 12:00 UTC after the calibrator updates SSM at 09:00",
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
        r.ok(f"  ✓ wired ({rule_name} → Sundays 12:00 UTC)")

        r.heading("3) Bootstrap — invoke now to seed first snapshot")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:500]}")

        r.heading("4) Verify outputs")
        for key in ["calibration/history-index.json", "calibration/latest.json"]:
            try:
                obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
                d = json.loads(obj["Body"].read())
                r.log(f"")
                r.log(f"  ✓ {key}")
                if key.endswith("history-index.json"):
                    r.log(f"    n_snapshots: {d.get('n_snapshots')}")
                    for s in d.get("snapshots", []):
                        r.log(f"    • {s.get('iso_week')} ({s.get('week_start')} → {s.get('week_end')})  n_weights={s.get('n_weights')}  n_calibrated≥30={s.get('n_calibrated_n30')}")
                elif key.endswith("latest.json"):
                    summ = d.get("summary") or {}
                    r.log(f"    iso_week: {d.get('iso_week')} ({d.get('week_start')} → {d.get('week_end')})")
                    r.log(f"    n_weights: {summ.get('n_weights_total')}")
                    r.log(f"    n_calibrated_n30: {summ.get('n_signals_calibrated_n30')}")
                    r.log(f"    highest_weight: {summ.get('highest_weight')}")
                    r.log(f"    median_weight: {summ.get('median_weight')}")
                    r.log(f"    weighted_mean_accuracy: {summ.get('weighted_mean_accuracy')}")
                    # Show top 5 weights from snapshot
                    weights = d.get("weights") or {}
                    top5 = sorted(weights.items(), key=lambda x: -float(x[1]))[:8]
                    r.log(f"")
                    r.log(f"    Top 8 weights (preview):")
                    accuracy = d.get("accuracy") or {}
                    for sig, w in top5:
                        acc = accuracy.get(sig)
                        n = (d.get("outcome_counts_60d") or {}).get(sig, 0)
                        acc_str = f"{acc*100:.1f}%" if acc is not None and isinstance(acc, (int, float)) and acc <= 1 else (f"{acc:.1f}%" if acc is not None else "—")
                        r.log(f"      {sig:32s}  w={w:.3f}  acc={acc_str:>7s}  n={n}")
            except Exception as e:
                r.log(f"  ✗ {key}: {e}")


if __name__ == "__main__":
    main()
