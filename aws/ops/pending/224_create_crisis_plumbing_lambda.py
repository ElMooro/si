#!/usr/bin/env python3
"""Step 224 — create justhodl-crisis-plumbing Lambda from repo source.

Phase 9.1 of the system improvement plan.

Procedure:
  1. Pre-flight: confirm new doesn't exist
  2. Build zip from aws/lambdas/justhodl-crisis-plumbing/source/
  3. Create Lambda with FRED_API_KEY env var
  4. Test-invoke
  5. Verify s3://justhodl-dashboard-live/data/crisis-plumbing.json written
  6. Create EventBridge rule for 6h schedule
  7. Grant EventBridge invoke permission
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
NEW = "justhodl-crisis-plumbing"
RULE = "justhodl-crisis-plumbing-refresh"
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, connect_timeout=10))
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def build_zip_from_repo():
    """Build deployment zip from the repo source dir."""
    src_dir = "aws/lambdas/justhodl-crisis-plumbing/source"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in os.listdir(src_dir):
            path = os.path.join(src_dir, fname)
            if os.path.isfile(path):
                zf.write(path, fname)
    buf.seek(0)
    return buf.read()


with report("create_crisis_plumbing_lambda") as r:
    r.heading("Phase 9.1 — create justhodl-crisis-plumbing Lambda")

    abort = False

    # 1. Pre-flight
    r.section("1. Pre-flight check")
    try:
        lam.get_function(FunctionName=NEW)
        r.warn(f"  ⚠ {NEW} already exists — will use update-function-code instead")
        already_exists = True
    except ClientError as e:
        if "ResourceNotFoundException" in str(e):
            r.log(f"  ✅ {NEW} does not exist — safe to create")
            already_exists = False
        else:
            r.warn(f"  ✗ unexpected: {e}")
            abort = True

    if not abort:
        # 2. Build zip
        r.section("2. Build zip from aws/lambdas/justhodl-crisis-plumbing/source/")
        zip_bytes = build_zip_from_repo()
        r.log(f"  zip: {len(zip_bytes)}B")

        if not already_exists:
            # 3. Create Lambda
            r.section(f"3. Create {NEW}")
            lam.create_function(
                FunctionName=NEW,
                Runtime="python3.12",
                Role=ROLE,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zip_bytes},
                Timeout=120,
                MemorySize=512,
                Environment={"Variables": {
                    "FRED_API_KEY": "2f057499936072679d8843d7fce99989",
                }},
                Architectures=["x86_64"],
                Description="Phase 9.1 — official crisis indices + plumbing tier 2 + XCC basis proxy",
            )
            time.sleep(3)
            lam.get_waiter("function_active_v2").wait(FunctionName=NEW)
            r.log(f"  ✅ created and Active")
        else:
            r.section(f"3. Update {NEW} code (already exists)")
            lam.update_function_code(FunctionName=NEW, ZipFile=zip_bytes)
            time.sleep(3)
            lam.get_waiter("function_updated_v2").wait(FunctionName=NEW)
            r.log(f"  ✅ updated")

        # 4. Test-invoke
        r.section(f"4. Test-invoke {NEW}")
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
                r.log(f"  payload: {payload[:500]}")
                invoke_ok = True
        except Exception as e:
            r.warn(f"  ✗ invoke fail: {e}")
            invoke_ok = False

        if invoke_ok:
            time.sleep(5)

            # 5. Verify S3 output
            r.section("5. Verify s3://.../data/crisis-plumbing.json")
            try:
                obj = s3.get_object(Bucket=BUCKET, Key="data/crisis-plumbing.json")
                content = obj["Body"].read()
                age_s = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()
                data = json.loads(content)
                r.log(f"  ✅ written: {len(content)}B  age={age_s:.0f}s")
                composite = data.get("composite", {})
                r.log(f"  composite signal: {composite.get('agreement_signal')}")
                r.log(f"  composite score: {composite.get('composite_stress_score')}")
                r.log(f"  n_indices_available: {composite.get('n_indices_available')}")
                r.log(f"  flagged: {composite.get('flagged_indices')}")
                # Check each crisis index's availability
                for sid, result in data.get("crisis_indices", {}).items():
                    if result.get("available"):
                        r.log(f"    ✅ {sid}: pct={result.get('pct_rank')} val={result.get('latest_value')}")
                    else:
                        r.warn(f"    ✗ {sid}: not available")
            except Exception as e:
                r.warn(f"  ✗ S3 read: {e}")

            # 6. Create EventBridge rule (6h schedule)
            r.section(f"6. Create EventBridge rule {RULE} (rate(6 hours))")
            try:
                events.put_rule(
                    Name=RULE,
                    ScheduleExpression="rate(6 hours)",
                    State="ENABLED",
                    Description="Refresh crisis-plumbing.json every 6h",
                )
                r.log(f"  ✅ rule created")

                # Permission for EB to invoke
                try:
                    lam.add_permission(
                        FunctionName=NEW,
                        StatementId="EventBridgeInvoke",
                        Action="lambda:InvokeFunction",
                        Principal="events.amazonaws.com",
                        SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE}",
                    )
                    r.log(f"  ✅ EventBridge invoke permission granted")
                except ClientError as e:
                    if "ResourceConflictException" in str(e):
                        r.log(f"  ✅ permission already exists")
                    else:
                        r.warn(f"  ⚠ {e}")

                # Add target
                events.put_targets(
                    Rule=RULE,
                    Targets=[{
                        "Id": "crisis-plumbing-target",
                        "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NEW}",
                    }],
                )
                r.log(f"  ✅ rule targets {NEW}")
            except Exception as e:
                r.warn(f"  ⚠ EventBridge: {e}")

    r.section("FINAL")
    r.log(f"  Lambda: {NEW}")
    r.log(f"  S3 output: data/crisis-plumbing.json")
    r.log(f"  Schedule: rate(6 hours) via {RULE}")
    r.log(f"")
    r.log(f"  Next: create /crisis.html on the website to consume this data")
    r.log("Done")
