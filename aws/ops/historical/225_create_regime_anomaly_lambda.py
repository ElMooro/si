#!/usr/bin/env python3
"""Step 225 — create justhodl-regime-anomaly Lambda. Phase 9.2.

Mirrors step 224's pattern: idempotent create-or-update, test-invoke
with 300s timeout, verify S3 output, EventBridge daily schedule.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
from ops_report import report
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
NEW = "justhodl-regime-anomaly"
RULE = "justhodl-regime-anomaly-refresh"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, connect_timeout=10))
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def build_zip_from_repo():
    src_dir = "aws/lambdas/justhodl-regime-anomaly/source"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in os.listdir(src_dir):
            path = os.path.join(src_dir, fname)
            if os.path.isfile(path):
                zf.write(path, fname)
    buf.seek(0)
    return buf.read()


with report("create_regime_anomaly_lambda") as r:
    r.heading("Phase 9.2 — create justhodl-regime-anomaly Lambda")

    abort = False

    r.section("1. Pre-flight")
    try:
        lam.get_function(FunctionName=NEW)
        already = True
        r.warn(f"  ⚠ {NEW} exists — will update-function-code")
    except ClientError as e:
        if "ResourceNotFoundException" in str(e):
            already = False
            r.log(f"  ✅ {NEW} does not exist — safe to create")
        else:
            r.warn(f"  ✗ unexpected: {e}")
            abort = True

    if not abort:
        r.section("2. Build zip")
        zip_bytes = build_zip_from_repo()
        r.log(f"  zip: {len(zip_bytes)}B")

        if not already:
            r.section(f"3. Create {NEW}")
            lam.create_function(
                FunctionName=NEW,
                Runtime="python3.12",
                Role=ROLE,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zip_bytes},
                Timeout=240,
                MemorySize=1024,
                Architectures=["x86_64"],
                Description="Phase 9.2 — HMM regime detector + anomaly detection",
            )
            time.sleep(3)
            lam.get_waiter("function_active_v2").wait(FunctionName=NEW)
            r.log(f"  ✅ created and Active")
        else:
            r.section(f"3. Update {NEW}")
            lam.update_function_code(FunctionName=NEW, ZipFile=zip_bytes)
            time.sleep(3)
            lam.get_waiter("function_updated_v2").wait(FunctionName=NEW)
            r.log(f"  ✅ updated")

        r.section(f"4. Test-invoke")
        t0 = time.time()
        try:
            inv = lam.invoke(FunctionName=NEW, InvocationType="RequestResponse",
                             Payload=json.dumps({}))
            elapsed = time.time() - t0
            err = inv.get("FunctionError")
            payload = inv["Payload"].read().decode("utf-8", errors="replace")
            if err:
                r.warn(f"  ✗ err={err} ({elapsed:.1f}s)")
                r.warn(f"  payload: {payload[:600]}")
                invoke_ok = False
            else:
                r.log(f"  ✅ OK ({elapsed:.1f}s)")
                r.log(f"  payload: {payload[:600]}")
                invoke_ok = True
        except Exception as e:
            r.warn(f"  ✗ invoke fail: {e}")
            invoke_ok = False

        if invoke_ok:
            time.sleep(5)

            r.section("5. Verify s3://.../data/regime-anomaly.json")
            try:
                obj = s3.get_object(Bucket=BUCKET, Key="data/regime-anomaly.json")
                content = obj["Body"].read()
                age_s = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()
                data = json.loads(content)
                r.log(f"  ✅ written: {len(content)}B  age={age_s:.0f}s")

                hmm = data.get("hmm", {})
                anomaly = data.get("anomaly", {})
                training = data.get("training_window", {})

                r.log(f"  HMM training_n: {hmm.get('training_n')}")
                r.log(f"  HMM is_warming_up: {hmm.get('is_warming_up')}")
                r.log(f"  HMM current state: {hmm.get('state_label')}")
                r.log(f"  HMM probabilities: {hmm.get('state_probabilities')}")
                r.log(f"  Anomaly n_anomalies: {anomaly.get('n_anomalies')}")
                r.log(f"  Anomaly score: {anomaly.get('composite_anomaly_score')}")
                r.log(f"  Training window: {training}")
            except Exception as e:
                r.warn(f"  ✗ S3 read: {e}")

            r.section(f"6. Create EventBridge rule {RULE} (rate(1 day))")
            try:
                events.put_rule(
                    Name=RULE,
                    ScheduleExpression="rate(1 day)",
                    State="ENABLED",
                    Description="Daily HMM refit + anomaly scan",
                )
                r.log(f"  ✅ rule created")

                try:
                    lam.add_permission(
                        FunctionName=NEW,
                        StatementId="EventBridgeInvoke",
                        Action="lambda:InvokeFunction",
                        Principal="events.amazonaws.com",
                        SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE}",
                    )
                    r.log(f"  ✅ EventBridge invoke perm granted")
                except ClientError as e:
                    if "ResourceConflictException" in str(e):
                        r.log(f"  ✅ permission already exists")
                    else:
                        r.warn(f"  ⚠ {e}")

                events.put_targets(
                    Rule=RULE,
                    Targets=[{
                        "Id": "regime-anomaly-target",
                        "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NEW}",
                    }],
                )
                r.log(f"  ✅ rule targets {NEW}")
            except Exception as e:
                r.warn(f"  ⚠ EventBridge: {e}")

    r.section("FINAL")
    r.log(f"  Lambda: {NEW}")
    r.log(f"  S3 output: data/regime-anomaly.json")
    r.log(f"  Schedule: rate(1 day) via {RULE}")
    r.log(f"  Next: build /regime.html frontend")
    r.log("Done")
