#!/usr/bin/env python3
"""Step 217 — Phase 4 resume after step 216 partial failure.

Step 216 successfully created justhodl-ka-metrics but failed at
CORS config (OPTIONS is not a valid Lambda Function URL CORS
method — AWS auto-handles preflight). State after step 216:
  - justhodl-ka-metrics created and Active
  - NO Function URL attached
  - Lambda has dual-write code in source (3 ka_+ legacy writes)
  - Old Lambda still has EventBridge rule

This step:
  1. Confirms new Lambda exists, has no URL yet
  2. Creates Function URL with valid CORS (no OPTIONS)
  3. Adds public invoke permission
  4. Test-invokes
  5. Verifies dual-write of 6 S3 keys
  6. Cuts over EventBridge rule target old → new
"""
import json, time
from datetime import datetime, timezone
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
OLD = "justhodl-khalid-metrics"
NEW = "justhodl-ka-metrics"
RULE = "justhodl-khalid-metrics-refresh"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("phase4_resume_after_cors_fail") as r:
    r.heading("Phase 4 resume — finish what step 216 started")

    abort = False

    # 1. Confirm state
    r.section("1. Confirm state from step 216")
    try:
        new_info = lam.get_function(FunctionName=NEW)
        r.log(f"  ✅ {NEW} exists  state={new_info['Configuration']['State']}")
        r.log(f"  CodeSize: {new_info['Configuration']['CodeSize']}B")
    except ClientError as e:
        r.warn(f"  ✗ {NEW} not found — step 216 didn't create it: {e}")
        abort = True

    new_url = None
    if not abort:
        try:
            url_cfg = lam.get_function_url_config(FunctionName=NEW)
            new_url = url_cfg["FunctionUrl"]
            r.log(f"  ✅ Function URL already exists: {new_url}")
        except ClientError as e:
            if "ResourceNotFoundException" in str(e):
                r.log(f"  → no Function URL yet — will create one with valid CORS")
            else:
                r.warn(f"  ✗ unexpected: {e}")
                abort = True

    if not abort and not new_url:
        # 2. Create Function URL with CORS that AWS accepts
        r.section(f"2. Create Function URL for {NEW}")
        url_resp = lam.create_function_url_config(
            FunctionName=NEW,
            AuthType="NONE",
            Cors={
                "AllowOrigins": ["*"],
                "AllowMethods": ["*"],     # wildcard is fine, includes OPTIONS preflight automatically
                "AllowHeaders": ["*"],
                "MaxAge": 86400,
            },
        )
        new_url = url_resp["FunctionUrl"]
        r.log(f"  ✅ {new_url}")

        try:
            lam.add_permission(
                FunctionName=NEW,
                StatementId="FunctionURLAllowPublicAccess",
                Action="lambda:InvokeFunctionUrl",
                Principal="*",
                FunctionUrlAuthType="NONE",
            )
            r.log(f"  ✅ public invoke permission added")
        except ClientError as e:
            if "ResourceConflictException" in str(e):
                r.log(f"  ✅ permission already exists")
            else:
                r.warn(f"  perm: {e}")

    if not abort:
        # 3. Test-invoke
        r.section(f"3. Test-invoke {NEW}")
        t0 = time.time()
        inv = lam.invoke(FunctionName=NEW, InvocationType="RequestResponse",
                         Payload=json.dumps({}))
        elapsed = time.time() - t0
        err = inv.get("FunctionError")
        payload = inv["Payload"].read().decode("utf-8", errors="replace")[:400]
        if err:
            r.warn(f"  ✗ err={err} ({elapsed:.1f}s)")
            r.warn(f"  payload: {payload}")
        else:
            r.log(f"  ✅ OK ({elapsed:.1f}s)")
            r.log(f"  payload: {payload}")

        time.sleep(8)

        # 4. Verify dual-write
        r.section("4. Verify dual-write of 6 S3 keys")
        keys = [
            "data/ka-metrics.json", "data/ka-config.json", "data/ka-analysis.json",
            "data/khalid-metrics.json", "data/khalid-config.json", "data/khalid-analysis.json",
        ]
        now = datetime.now(timezone.utc)
        n_fresh = 0
        for k in keys:
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=k)
                age = (now - obj["LastModified"]).total_seconds()
                mark = "✅ FRESH" if age < 60 else "⏰ stale"
                r.log(f"  {mark} {k:40s}  size={obj['ContentLength']:>10}B  age={age:.0f}s")
                if age < 120: n_fresh += 1
            except ClientError as e:
                if "NoSuchKey" in str(e):
                    r.warn(f"  ✗ MISSING {k}")
                else:
                    r.warn(f"  ✗ {k}: {e}")
        r.log(f"\n  {n_fresh}/{len(keys)} keys fresh (<2 min)")

        # 5. EventBridge cutover
        r.section(f"5. Cut over EventBridge rule {RULE}")
        new_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NEW}"
        try:
            targets = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
            current = [t["Arn"].split(":")[-1] for t in targets]
            r.log(f"  current targets: {current}")

            if NEW in current:
                r.log(f"  ✅ rule already targets {NEW} — nothing to do")
            else:
                # Grant invoke permission first
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
                        r.log(f"  ✅ EventBridge invoke permission already exists")
                    else:
                        r.warn(f"  ⚠ {e}")

                # Update target
                new_targets = [{**t, "Arn": new_arn} for t in targets]
                resp = events.put_targets(Rule=RULE, Targets=new_targets)
                failed = resp.get("FailedEntryCount", 0)
                if failed:
                    r.warn(f"  ⚠ {failed} updates failed: {resp.get('FailedEntries')}")
                else:
                    r.log(f"  ✅ rule now targets {NEW}")

                verify = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
                r.log(f"  verified: {[t['Arn'].split(':')[-1] for t in verify]}")
        except Exception as e:
            r.warn(f"  ⚠ EventBridge: {e}")

        # 6. Summary
        r.section("FINAL")
        r.log(f"  Old Lambda: {OLD} (still alive, no longer triggered by EventBridge)")
        r.log(f"  New Lambda: {NEW}")
        r.log(f"  New Function URL: {new_url}")
        r.log(f"  EventBridge {RULE} → {NEW}")
        r.log("")
        r.log("  Step 218 will:")
        r.log("    a) Update ka/index.html to use new Function URL + data/ka-*.json")
        r.log("  Phase 4b (after 7-day grace): delete old Lambda + Function URL")

    r.log("Done")
