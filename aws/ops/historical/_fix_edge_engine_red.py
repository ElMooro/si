"""
Step ___ — Re-enable justhodl-edge-6h EB rule + trigger one-off invocation
                   to refresh stale edge-data.json (last RED on dashboard).

Health monitor flagged s3:data/edge-data.json as RED (stale beyond fresh_max
of 25_000s = ~7h). Lambda is healthy (last_modified 2026-03-05, has Function
URL, code structure clean) — same EB-rule-disabled pattern as the 5 rules
re-enabled by step 245.

Actions (idempotent):
  1. Find rules targeting justhodl-edge-engine (config says 'justhodl-edge-6h')
  2. Re-enable any in DISABLED state
  3. Verify lambda:InvokeFunction permission for events.amazonaws.com
  4. Trigger ONE synchronous invocation now to refresh edge-data.json
     immediately (avoids waiting for next scheduled 6h tick)
  5. Verify the S3 object got refreshed (LastModified within last 60s)
"""
from __future__ import annotations
import json
import time
from datetime import datetime, timezone

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
LAMBDA_NAME = "justhodl-edge-engine"
EB_RULE_NAME = "justhodl-edge-6h"   # per config.json
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "edge-data.json"

eb = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("fix_edge_engine_red") as r:
        r.heading("Re-enable justhodl-edge-6h + refresh edge-data.json")

        # 1. Lambda exists and is healthy?
        r.section("1. Lambda health")
        try:
            cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
            r.log(f"  Runtime:     {cfg['Runtime']}")
            r.log(f"  Handler:     {cfg['Handler']}")
            r.log(f"  LastMod:     {cfg['LastModified']}")
            r.log(f"  State:       {cfg.get('State', '?')}")
            r.log(f"  StateReason: {cfg.get('StateReason', '')}")
        except ClientError as e:
            r.fail(f"  Lambda not found: {e}")
            return

        # 2. Current S3 freshness
        r.section("2. Current edge-data.json freshness")
        try:
            head = s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
            age_sec = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds()
            r.log(f"  Size:        {head['ContentLength']} bytes")
            r.log(f"  LastMod:     {head['LastModified']}")
            r.log(f"  Age:         {int(age_sec)}s ({age_sec/3600:.1f}h)")
            stale_before = age_sec > 25_000
        except ClientError as e:
            r.warn(f"  S3 head failed: {e}")
            stale_before = True

        # 3. EB rule state
        r.section("3. EB rule justhodl-edge-6h")
        try:
            rule = eb.describe_rule(Name=EB_RULE_NAME)
            r.log(f"  Schedule:  {rule.get('ScheduleExpression', '?')}")
            r.log(f"  State:     {rule['State']}")
            if rule["State"] == "DISABLED":
                eb.enable_rule(Name=EB_RULE_NAME)
                r.ok(f"  ✓ enabled rule")
            else:
                r.log(f"  rule already enabled")

            # Verify target
            targets = eb.list_targets_by_rule(Rule=EB_RULE_NAME).get("Targets", [])
            target_arns = [t.get("Arn", "") for t in targets]
            expected = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}"
            if expected in target_arns:
                r.ok(f"  ✓ target points at {LAMBDA_NAME}")
            else:
                r.warn(f"  ⚠ targets are {target_arns} — not pointing at {LAMBDA_NAME}!")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                r.warn(f"  Rule '{EB_RULE_NAME}' does not exist — creating it")
                eb.put_rule(
                    Name=EB_RULE_NAME,
                    ScheduleExpression="cron(0 0,6,12,18 * * ? *)",  # 6h UTC
                    State="ENABLED",
                    Description="Run justhodl-edge-engine every 6 hours (auto-restored)",
                )
                expected = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{LAMBDA_NAME}"
                eb.put_targets(
                    Rule=EB_RULE_NAME,
                    Targets=[{"Id": "1", "Arn": expected}],
                )
                r.ok(f"  ✓ created rule + target")
            else:
                r.fail(f"  describe_rule failed: {e}")
                raise

        # 4. Ensure invoke permission
        r.section("4. Lambda invoke permission for EB")
        rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{EB_RULE_NAME}"
        sid = f"AllowEB-{EB_RULE_NAME}-{int(time.time())}"[:64]
        try:
            lam.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId=sid,
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=rule_arn,
            )
            r.ok(f"  ✓ added permission ({sid})")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                r.log(f"  permission already exists")
            else:
                r.warn(f"  add_permission: {e}")

        # 5. Synchronously invoke now to refresh edge-data.json
        r.section("5. One-off invocation to refresh edge-data.json")
        try:
            resp = lam.invoke(
                FunctionName=LAMBDA_NAME,
                InvocationType="RequestResponse",
                Payload=b'{"source":"manual.refresh.from.eb_repair"}',
            )
            status = resp["StatusCode"]
            payload = json.loads(resp["Payload"].read())
            r.log(f"  StatusCode:  {status}")
            r.log(f"  Lambda body status: {payload.get('statusCode', '?')}")
            if "FunctionError" in resp:
                r.fail(f"  ✗ FunctionError: {resp['FunctionError']}")
            else:
                r.ok(f"  ✓ invocation succeeded")
        except ClientError as e:
            r.fail(f"  invoke failed: {e}")

        # 6. Re-check S3 freshness
        r.section("6. Post-invocation freshness check")
        time.sleep(2)
        try:
            head = s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
            age_sec = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds()
            r.log(f"  Size:        {head['ContentLength']} bytes")
            r.log(f"  LastMod:     {head['LastModified']}")
            r.log(f"  Age:         {int(age_sec)}s")
            if age_sec < 60:
                r.ok(f"  ✓ refreshed — health-monitor next tick should flip GREEN")
            elif stale_before and age_sec > 25_000:
                r.fail(f"  ✗ still stale — Lambda invocation didn't write S3")
            else:
                r.warn(f"  ⚠ age {int(age_sec)}s — recently refreshed but not by this run?")
        except ClientError as e:
            r.fail(f"  S3 head failed: {e}")


if __name__ == "__main__":
    main()
