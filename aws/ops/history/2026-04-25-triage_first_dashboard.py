#!/usr/bin/env python3
"""
Step 83.5 — Triage the first dashboard run findings:

  1. Why is edge-data.json red? Check actual age vs threshold.
  2. Why is repo-data.json yellow? Probably timezone / schedule.
  3. Add events:DescribeRule + events:ListTargetsByRule to lambda-execution-role.
  4. Fix known_broken handling so they show as 'info' not 'unknown'.

This is a read-write step — adds IAM perm and updates expectations.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


with report("triage_first_dashboard") as r:
    r.heading("Triage first dashboard run + add IAM perms")

    # ─── 1 + 2. Actual ages of red/yellow files ───
    r.section("1+2. Check actual ages of red/yellow components")
    for key in ["edge-data.json", "repo-data.json", "screener/data.json"]:
        try:
            head = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            age_h = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds() / 3600
            r.log(f"  {key:30} age={age_h:.1f}h size={head['ContentLength']:,}B")
        except Exception as e:
            r.log(f"  {key}: {e}")

    # ─── 3. Add events:Describe perms to lambda-execution-role ───
    r.section("3. Add EventBridge read perms to lambda-execution-role")
    policy_name = "HealthMonitorEventBridgeRead"
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "DescribeAndListEBRules",
            "Effect": "Allow",
            "Action": [
                "events:DescribeRule",
                "events:ListRules",
                "events:ListTargetsByRule",
                "events:ListRuleNamesByTarget",
            ],
            "Resource": "*",
        }]
    }
    try:
        iam.put_role_policy(
            RoleName="lambda-execution-role",
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy_doc),
        )
        r.ok(f"  Attached inline policy {policy_name}")
    except Exception as e:
        r.fail(f"  IAM put_role_policy failed: {e}")

    # ─── 4. Fix known_broken status in expectations.py ───
    r.section("4. Update lambda_function.py — known_broken should show as 'info'")
    src_path = REPO_ROOT / "aws/lambdas/justhodl-health-monitor/source/lambda_function.py"
    src = src_path.read_text(encoding="utf-8")

    # Find the s3 file checker and update the known_broken handling
    old_block = '''        # Combined: worst of the two
        statuses = [out["age_status"], out["size_status"]]
        out["status"] = "red" if "red" in statuses else "yellow" if "yellow" in statuses else "green" if "green" in statuses else "unknown"
        # If known_broken and would be red, downgrade to "info" so it doesn't alarm
        if spec.get("known_broken") and out["status"] == "red":
            out["status"] = "info"'''

    new_block = '''        # Combined: worst of the two
        statuses = [out["age_status"], out["size_status"]]
        out["status"] = "red" if "red" in statuses else "yellow" if "yellow" in statuses else "green" if "green" in statuses else "unknown"
        # If known_broken: never alarm. Force to "info" regardless.
        if spec.get("known_broken"):
            out["status"] = "info"'''

    if old_block in src:
        src = src.replace(old_block, new_block, 1)
        src_path.write_text(src)
        r.ok(f"  Updated known_broken handling (forces 'info' regardless of status)")
    else:
        r.warn(f"  Pattern not found — manual review needed")

    # ─── Re-deploy + re-invoke ───
    r.section("Re-deploy with fixes + re-invoke")
    import io, zipfile

    expectations_src = REPO_ROOT / "aws/ops/health/expectations.py"
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-health-monitor/source"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
        zout.write(expectations_src, "expectations.py")
    zbytes = buf.getvalue()

    lam.update_function_code(FunctionName="justhodl-health-monitor", ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-health-monitor", WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed: {len(zbytes)} bytes")

    # Allow IAM perm propagation a few seconds
    import time
    time.sleep(5)

    resp = lam.invoke(FunctionName="justhodl-health-monitor", InvocationType="RequestResponse")
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()
        r.fail(f"  Invoke error: {payload[:500]}")
    else:
        r.ok(f"  Re-invoke status: {resp.get('StatusCode')}")

    # Read back updated dashboard
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="_health/dashboard.json")
    dash = json.loads(obj["Body"].read())
    r.log(f"\n  System: {dash.get('system_status')}")
    r.log(f"  Counts: {dash.get('counts')}")
    r.log(f"\n  Non-green components after fixes:")
    for c in dash.get("components", []):
        if c.get("status") in ("green", "info"):
            continue
        sid = c.get("id", "?")
        st = c.get("status", "?")
        sev = c.get("severity", "?")
        reason = c.get("reason") or c.get("error") or ""
        age = c.get("age_sec")
        age_h = f"age={age/3600:.1f}h" if age else ""
        r.log(f"    [{st:7}] {sev:12} {sid:50} {age_h:>15}  {reason[:80]}")

    r.kv(
        iam_policy_added="HealthMonitorEventBridgeRead",
        known_broken_logic="now always 'info' regardless",
        next_step="step 84 builds HTML dashboard",
    )
    r.log("Done")
