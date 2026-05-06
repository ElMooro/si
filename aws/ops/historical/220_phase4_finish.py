#!/usr/bin/env python3
"""Step 220 — finish phase 4 cutover.

Step 219 verified:
  ✅ New Lambda runs successfully (86s, returns 200, "refreshed+analyzed")
  ✅ 2/3 ka_+ files written fresh: ka-metrics.json + ka-analysis.json
  ❌ ka-config.json missing because it's only written on first-init
    (khalid-config.json is 58 days old too — confirms static)

Action plan:
  1. Copy s3://...data/khalid-config.json → ka-config.json (one-time seed)
  2. Verify all 6 keys now present
  3. Re-cut EventBridge target old → new
  4. Document repointing of ka/index.html for step 221
"""
import json
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


with report("phase4_finish_cutover") as r:
    r.heading("Phase 4 finish — seed ka-config + cut EventBridge")

    # 1. Copy khalid-config.json → ka-config.json
    r.section("1. One-time seed: copy khalid-config.json → ka-config.json")
    try:
        # CopyObject preserves all metadata
        s3.copy_object(
            Bucket=BUCKET,
            Key="data/ka-config.json",
            CopySource={"Bucket": BUCKET, "Key": "data/khalid-config.json"},
            ContentType="application/json",
            MetadataDirective="REPLACE",
        )
        r.log(f"  ✅ copied data/khalid-config.json → data/ka-config.json")
    except Exception as e:
        r.warn(f"  ✗ {e}")

    # 2. Verify all 6 keys
    r.section("2. Verify all 6 S3 keys present")
    keys = [
        "data/ka-metrics.json", "data/ka-config.json", "data/ka-analysis.json",
        "data/khalid-metrics.json", "data/khalid-config.json", "data/khalid-analysis.json",
    ]
    now = datetime.now(timezone.utc)
    n_present = 0
    for k in keys:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            age = (now - obj["LastModified"]).total_seconds()
            r.log(f"  ✅ {k:40s}  size={obj['ContentLength']:>10}B  age={int(age):>10}s")
            n_present += 1
        except ClientError as e:
            if "NoSuchKey" in str(e):
                r.warn(f"  ✗ MISSING {k}")
    r.log(f"\n  {n_present}/{len(keys)} keys present")

    # 3. Cut EventBridge target old → new
    if n_present == len(keys):
        r.section(f"3. Cut EventBridge target → {NEW}")
        new_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{NEW}"
        try:
            targets = events.list_targets_by_rule(Rule=RULE).get("Targets", [])
            current = [t["Arn"].split(":")[-1] for t in targets]
            r.log(f"  current: {current}")

            if NEW in current:
                r.log(f"  ✅ already targets {NEW}")
            else:
                # Ensure invoke perm exists
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
                        r.log(f"  ✅ EventBridge invoke perm already exists")
                    else:
                        r.warn(f"  ⚠ {e}")

                new_targets = [{**t, "Arn": new_arn} for t in targets]
                resp = events.put_targets(Rule=RULE, Targets=new_targets)
                if resp.get("FailedEntryCount", 0) == 0:
                    r.log(f"  ✅ EventBridge → {NEW}")
                else:
                    r.warn(f"  ⚠ failed: {resp.get('FailedEntries')}")

                verify = [t["Arn"].split(":")[-1] for t in events.list_targets_by_rule(Rule=RULE).get("Targets", [])]
                r.log(f"  verified: {verify}")
        except Exception as e:
            r.warn(f"  ⚠ {e}")
    else:
        r.warn(f"  Skipping EventBridge cutover — {n_present}/6 keys present")

    r.section("FINAL")
    r.log(f"  Old: {OLD} (still alive, will delete in Phase 4b after 7-day grace)")
    r.log(f"  New: {NEW}")
    r.log(f"  New URL: https://s6ascg5dntry5w5elqedee77na0fcljz.lambda-url.us-east-1.on.aws/")
    r.log(f"  EventBridge {RULE} → {NEW}")
    r.log(f"")
    r.log(f"  Step 221 (frontend cutover):")
    r.log(f"    a) ka/index.html line 86: replace 3 khalid-*.json keys with ka-*.json")
    r.log(f"    b) ka/index.html line 86: replace OLD Function URL with NEW")
    r.log(f"    c) Verify /ka/ live")
    r.log(f"  Phase 4b (7-day grace): delete {OLD} + its Function URL")
    r.log("Done")
