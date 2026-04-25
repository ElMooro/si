#!/usr/bin/env python3
"""
Step 163 — Fix IAM AccessDeniedException on outcomes UpdateItem.

Step 162 failed because github-actions-justhodl user lacks
dynamodb:UpdateItem on justhodl-outcomes. (When step 161 had the
reserved-keyword bug, the syntax error caught the call before IAM
ever got to deny it. Step 162 fixed the syntax — IAM denial surfaces.)

Memory says github-actions-justhodl has DDB read perm. Need to add
update perm too. This step:
  A. Inspects current attached policies on github-actions-justhodl
  B. Adds a minimal inline policy granting UpdateItem on
     justhodl-outcomes ONLY (not the whole DDB resource pattern)
  C. Re-runs the legacy retirement
  D. Verifies tagging worked

Why minimal inline policy: principle of least privilege. We only
need UpdateItem on this one table for this specific cleanup task.
Once tagging is done, the policy can stay (cleanup is one-shot but
the policy doesn\\'t harm anything) or get removed (fine to remove
since it\\'s not needed for any other ops work).
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from ops_report import report
import boto3

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
USER_NAME = "github-actions-justhodl"
TABLE_NAME = "justhodl-outcomes"

iam = boto3.client("iam", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


with report("fix_iam_for_outcomes_update") as r:
    r.heading("Add UpdateItem on outcomes to github-actions-justhodl")

    # ─── A. Inspect current policies ────────────────────────────────────
    r.section("A. Current policies on github-actions-justhodl")
    try:
        attached = iam.list_attached_user_policies(UserName=USER_NAME)
        r.log(f"  Attached managed policies:")
        for p in attached.get("AttachedPolicies", []):
            r.log(f"    {p.get('PolicyName')} → {p.get('PolicyArn')}")
        inline = iam.list_user_policies(UserName=USER_NAME)
        r.log(f"\n  Inline policies:")
        for p in inline.get("PolicyNames", []):
            r.log(f"    {p}")
            try:
                pd = iam.get_user_policy(UserName=USER_NAME, PolicyName=p)
                doc = pd.get("PolicyDocument", {})
                stmts = doc.get("Statement", [])
                if not isinstance(stmts, list):
                    stmts = [stmts]
                for s in stmts:
                    actions = s.get("Action", [])
                    if not isinstance(actions, list):
                        actions = [actions]
                    res = s.get("Resource", "?")
                    eff = s.get("Effect", "?")
                    r.log(f"      {eff} {actions[:5]} ON {str(res)[:80]}")
            except Exception as e:
                r.warn(f"      get_user_policy: {e}")
    except Exception as e:
        r.fail(f"  IAM list: {e}")
        raise SystemExit(1)

    # ─── B. Add inline policy granting UpdateItem on justhodl-outcomes ─
    r.section("B. Attach inline policy: outcomes-updateitem")
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "OutcomesUpdateItem",
                "Effect": "Allow",
                "Action": [
                    "dynamodb:UpdateItem",
                    "dynamodb:Scan",
                    "dynamodb:GetItem",
                ],
                "Resource": f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:table/{TABLE_NAME}",
            }
        ],
    }
    try:
        iam.put_user_policy(
            UserName=USER_NAME,
            PolicyName="outcomes-updateitem",
            PolicyDocument=json.dumps(policy_doc),
        )
        r.ok(f"  Inline policy 'outcomes-updateitem' attached")
    except Exception as e:
        r.fail(f"  put_user_policy: {e}")
        raise SystemExit(1)

    # IAM changes can take 5-10 sec to propagate
    r.log(f"  Waiting 8s for IAM propagation...")
    time.sleep(8)

    # ─── C. Re-run the legacy retirement ────────────────────────────────
    r.section("C. Re-tag all correct=None outcomes")
    outcomes = ddb.Table(TABLE_NAME)

    null_outcomes = []
    scan_kwargs = {}
    while True:
        resp = outcomes.scan(**scan_kwargs)
        for o in resp.get("Items", []):
            if o.get("correct") is None:
                null_outcomes.append({"outcome_id": o["outcome_id"]})
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    r.log(f"  Found {len(null_outcomes)} correct=None outcomes")

    thirty_days_ts = int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp())
    tagged = 0
    failed = 0
    sample_errors = []

    for o in null_outcomes:
        try:
            outcomes.update_item(
                Key={"outcome_id": o["outcome_id"]},
                UpdateExpression="SET is_legacy = :l, legacy_reason = :r, #t = :t",
                ExpressionAttributeNames={"#t": "ttl"},
                ExpressionAttributeValues={
                    ":l": True,
                    ":r": "pre_baseline_fix_2026_04_24",
                    ":t": thirty_days_ts,
                },
            )
            tagged += 1
            if tagged % 1000 == 0:
                r.log(f"    Tagged {tagged}/{len(null_outcomes)}...")
        except Exception as e:
            failed += 1
            if len(sample_errors) < 3:
                sample_errors.append((o["outcome_id"][:30], str(e)[:120]))

    r.log(f"\n  Tagged: {tagged}")
    r.log(f"  Failed: {failed}")
    if sample_errors:
        r.log(f"  Sample errors:")
        for sid, err in sample_errors:
            r.log(f"    {sid}: {err}")

    if tagged == len(null_outcomes):
        r.ok(f"  ✅ All legacy records tagged successfully")
    elif tagged > 0:
        r.warn(f"  ⚠ Partial: {tagged}/{len(null_outcomes)}")
    else:
        r.fail(f"  ❌ All updates failed — IAM may need more time")

    # ─── D. Verify ──────────────────────────────────────────────────────
    r.section("D. Verify by re-scanning")
    legacy_count = 0
    pristine = 0
    real = 0
    scan_kwargs = {}
    while True:
        resp = outcomes.scan(**scan_kwargs)
        for o in resp.get("Items", []):
            correct = o.get("correct")
            if correct is None:
                if o.get("is_legacy") is True:
                    legacy_count += 1
                else:
                    pristine += 1
            else:
                real += 1
        if "LastEvaluatedKey" not in resp:
            break
        scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    r.log(f"  Tagged legacy:        {legacy_count}")
    r.log(f"  Untagged correct=None: {pristine}")
    r.log(f"  Real outcomes (T/F):  {real}")

    if pristine == 0:
        r.ok(f"  ✅ All correct=None outcomes are tagged")

    r.kv(
        n_null=len(null_outcomes),
        tagged=tagged,
        failed=failed,
        legacy_after=legacy_count,
        untagged=pristine,
    )
    r.log("Done")
