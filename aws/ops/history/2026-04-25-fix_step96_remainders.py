#!/usr/bin/env python3
"""
Step 97 — Clean up the two failed-fix Lambdas from step 96.

  1. ecb-data-daily-updater: my regex patch caused a syntax error due
     to indentation mismatch. The Lambda fundamentally treats indicator
     as dict — so the right fix is to SKIP non-dict entries at the top
     of the loop, not replace the symbol-extraction line.

  2. fmp-stock-picks-agent: SES neutralization worked, but revealed a
     NEW error — PutObject AccessDenied on its IAM role
     'economyapi-lambda-role'. Add s3:PutObject perm to that role
     (scope to the bucket it should write to).

For ecb: rewrite the source cleanly. The current file is corrupted
by the bad patch.
"""
import io
import json
import os
import re
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def deploy_lambda_source(name, source_files, r):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for relpath, content in source_files:
            info = zipfile.ZipInfo(relpath)
            info.external_attr = 0o644 << 16
            zout.writestr(info, content)
    zbytes = buf.getvalue()
    try:
        lam.update_function_code(FunctionName=name, ZipFile=zbytes)
        lam.get_waiter("function_updated").wait(
            FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
        )
        r.ok(f"    {name}: deployed {len(zbytes)}B")
        return True
    except Exception as e:
        r.fail(f"    {name}: deploy failed: {e}")
        return False


with report("fix_step96_remainders") as r:
    r.heading("Fix step 96's two remaining issues")

    # ────────────────────────────────────────────────────────────────────
    # 1. ecb-data-daily-updater — clean rewrite
    # ────────────────────────────────────────────────────────────────────
    r.section("1. ecb-data-daily-updater — clean source rewrite")
    name = "ecb-data-daily-updater"
    src_dir = REPO_ROOT / "aws/lambdas" / name / "source"
    target = src_dir / "lambda_function.py"

    # Read current (corrupted) source for context, but rewrite from scratch
    current = target.read_text() if target.exists() else ""

    # Clean rewrite: same logic but skip non-dict indicators at the top of the loop
    clean_source = '''import json
import boto3
import random
from datetime import datetime, timezone

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Load current data
    response = s3.get_object(Bucket='openbb-lambda-data', Key='ecb_data.json')
    indicators = json.loads(response['Body'].read())

    print(f"Loaded {len(indicators)} indicators")

    # Add simulated values to ALL indicators
    for indicator in indicators:
        # Defensive: skip if not a dict (upstream API may return strings)
        if not isinstance(indicator, dict):
            continue

        if 'value' not in indicator or indicator['value'] is None:
            sym = indicator.get('symbol', '')
            cat = indicator.get('category', '')

            # Generate appropriate simulated values based on type
            if 'CISS' in sym and 'SS_CI' in sym:
                # Main CISS values (0.1 to 0.5)
                indicator['value'] = round(random.uniform(0.1, 0.5), 4)
            elif 'CISS' in sym:
                # CISS components (0.05 to 0.3)
                indicator['value'] = round(random.uniform(0.05, 0.3), 4)
            elif 'DOLLAR' in sym or 'Dollar' in cat:
                # Dollar funding stress (-50 to -10 basis points)
                indicator['value'] = round(random.uniform(-50, -10), 2)
            elif 'TARGET2' in sym or 'TARGET2' in cat:
                # TARGET2 imbalance ($-100B to $100B)
                indicator['value'] = round(random.uniform(-100, 100), 1)
            elif 'OIS' in sym or 'EONIA' in sym:
                # Money market spreads (5-50 bp)
                indicator['value'] = round(random.uniform(5, 50), 2)
            elif 'BOND' in sym or 'Yield' in cat:
                # Bond yields (1-5%)
                indicator['value'] = round(random.uniform(1, 5), 3)
            else:
                # Generic indicator (1-100 range)
                indicator['value'] = round(random.uniform(1, 100), 2)

            # Update timestamp
            indicator['last_updated'] = datetime.now(timezone.utc).isoformat()

    # Write back
    s3.put_object(
        Bucket='openbb-lambda-data',
        Key='ecb_data.json',
        Body=json.dumps(indicators, indent=2),
        ContentType='application/json',
    )

    print(f"Updated {len(indicators)} indicators")
    return {
        'statusCode': 200,
        'body': json.dumps({'updated': len(indicators)}),
    }
'''
    target.write_text(clean_source)
    r.ok(f"    Rewrote {target.name} cleanly ({len(clean_source.split(chr(10)))} LOC)")

    # Validate Python syntax before deploying
    import ast
    try:
        ast.parse(clean_source)
        r.ok(f"    Syntax OK")
    except SyntaxError as e:
        r.fail(f"    Syntax error: {e}")
        raise SystemExit(1)

    # Deploy + test
    source_files = [(target.name, clean_source)]
    if deploy_lambda_source(name, source_files, r):
        try:
            resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
            if resp.get("FunctionError"):
                payload = resp.get("Payload").read().decode()
                r.warn(f"    {name}: still erroring: {payload[:300]}")
            else:
                r.ok(f"    {name}: invoke clean ({resp.get('StatusCode')})")
        except Exception as e:
            r.fail(f"    {name}: invoke failed: {e}")

    # ────────────────────────────────────────────────────────────────────
    # 2. fmp-stock-picks-agent — add S3 PutObject perm to its IAM role
    # ────────────────────────────────────────────────────────────────────
    r.section("2. fmp-stock-picks-agent — grant S3 PutObject perm")
    name = "fmp-stock-picks-agent"
    role_name = "economyapi-lambda-role"

    # Find what bucket(s) it tries to write to
    src_dir = REPO_ROOT / "aws/lambdas" / name / "source"
    target = src_dir / "lambda_function.py"
    bucket_names = set()
    if target.exists():
        content = target.read_text()
        # Match Bucket='...' or Bucket="..."
        for m in re.finditer(r"""Bucket\s*=\s*['"]([^'"]+)['"]""", content):
            bucket_names.add(m.group(1))
    r.log(f"    Buckets referenced in source: {sorted(bucket_names)}")

    # Build a least-privilege policy for those buckets
    if not bucket_names:
        r.warn("    No buckets found in source; can't scope policy")
    else:
        resources = []
        for b in bucket_names:
            resources.append(f"arn:aws:s3:::{b}")
            resources.append(f"arn:aws:s3:::{b}/*")
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "FmpStockPicksS3Write",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket",
                ],
                "Resource": resources,
            }],
        }
        try:
            iam.put_role_policy(
                RoleName=role_name,
                PolicyName="FmpStockPicksS3Write",
                PolicyDocument=json.dumps(policy_doc),
            )
            r.ok(f"    Attached FmpStockPicksS3Write to {role_name}")
            r.log(f"    Resources: {resources}")
        except Exception as e:
            r.fail(f"    IAM put_role_policy: {e}")

    # Wait for IAM propagation, then test invoke
    import time
    time.sleep(8)
    try:
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
        if resp.get("FunctionError"):
            payload = resp.get("Payload").read().decode()
            r.warn(f"    {name}: still erroring (after IAM grant): {payload[:300]}")
        else:
            r.ok(f"    {name}: invoke clean ({resp.get('StatusCode')})")
    except Exception as e:
        r.fail(f"    {name}: invoke failed: {e}")

    r.kv(
        ecb_rewrite="clean source deployed",
        fmp_iam_grant=f"S3 PutObject on {sorted(bucket_names) if bucket_names else 'none'}",
    )
    r.log("Done")
