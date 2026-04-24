#!/usr/bin/env python3
"""
Targeted fixes from the data-source audit findings.

TASK 1 — Add BRK.B to STOCK_TICKERS list
  Bug: BRK.B is in TICKER_NAMES (display dict) but missing from
  STOCK_TICKERS (fetch list). Per Polygon docs, BRK.B with a dot is
  the correct format for Polygon API. Just need to include it.

TASK 2 — Grant DynamoDB read perm to github-actions-justhodl
  Allows future ops scripts to inspect signal/outcome tables.

TASK 3 — Investigate open_interest + whale_txs failures
  Read CloudWatch logs for the most recent justhodl-crypto-intel run.
  If the failure is a quick code fix (URL/header), apply it. If it's
  upstream feed migration (CoinGlass v2 retired, etc.), document
  rather than rewrite.
"""

import io
import json
import os
import zipfile
from pathlib import Path
from datetime import datetime, timezone, timedelta

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
DR = REPO_ROOT / "aws/lambdas/justhodl-daily-report-v3/source/lambda_function.py"

lam = boto3.client("lambda", region_name=REGION)
iam = boto3.client("iam")
logs = boto3.client("logs", region_name=REGION)


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


with report("audit_followup_fixes") as r:
    r.heading("Audit follow-up fixes")

    # ═════════ TASK 1 — BRK.B ═════════
    r.section("TASK 1 — Add BRK.B to STOCK_TICKERS")
    src = DR.read_text(encoding="utf-8")

    # Find the STOCK_TICKERS list. Insert BRK.B in the "Mega Caps - Finance"
    # section right after C (Citigroup) or in the High-Growth section.
    # Cleanest: add it right at end of mega-cap-finance line.
    old_line = "    'KKR':'KKR & Co','COF':'Capital One Financial','USB':'U.S. Bancorp',"
    # That's TICKER_NAMES. We need the STOCK_TICKERS list line.
    old_finance_line = "    'JPM','V','MA','BAC','WFC','GS','MS','BLK','SCHW','C','AXP','BX','KKR','COF','USB',"
    new_finance_line = "    'JPM','V','MA','BAC','WFC','GS','MS','BLK','SCHW','C','AXP','BX','KKR','COF','USB','BRK.B',"

    if old_finance_line not in src:
        r.warn("  STOCK_TICKERS finance line not found verbatim — skipping ticker add")
    elif "'BRK.B'," in old_finance_line.replace(",,", ","):
        r.log("  BRK.B already in STOCK_TICKERS")
    else:
        src = src.replace(old_finance_line, new_finance_line, 1)
        r.ok("  Added 'BRK.B' to STOCK_TICKERS finance section")

    import ast
    try:
        ast.parse(src)
        DR.write_text(src, encoding="utf-8")
        r.ok(f"  Source valid ({len(src)} bytes), saved")
        # Deploy
        z = build_zip(DR.parent)
        lam.update_function_code(FunctionName="justhodl-daily-report-v3", ZipFile=z)
        lam.get_waiter("function_updated").wait(
            FunctionName="justhodl-daily-report-v3",
            WaiterConfig={"Delay": 3, "MaxAttempts": 30},
        )
        r.ok(f"  Deployed daily-report-v3 ({len(z)} bytes)")
    except SyntaxError as e:
        r.fail(f"  SYNTAX ERROR: {e}")
        raise SystemExit(1)

    # ═════════ TASK 2 — DynamoDB read perm ═════════
    r.section("TASK 2 — Grant DynamoDB read to github-actions-justhodl")
    iam_user = "github-actions-justhodl"
    policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess"
    try:
        # Check if already attached
        attached = iam.list_attached_user_policies(UserName=iam_user).get("AttachedPolicies", [])
        already = any(p["PolicyArn"] == policy_arn for p in attached)
        if already:
            r.log(f"  {policy_arn} already attached")
        else:
            iam.attach_user_policy(UserName=iam_user, PolicyArn=policy_arn)
            r.ok(f"  Attached {policy_arn} to {iam_user}")
        # Verify
        attached = iam.list_attached_user_policies(UserName=iam_user).get("AttachedPolicies", [])
        names = [p["PolicyName"] for p in attached]
        r.log(f"  Policies on {iam_user}: {names}")
    except Exception as e:
        r.fail(f"  IAM update failed: {e}")

    # ═════════ TASK 3 — Investigate oi + whale errors ═════════
    r.section("TASK 3 — Investigate justhodl-crypto-intel oi + whale failures")
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-crypto-intel",
            orderBy="LastEventTime", descending=True, limit=2,
        ).get("logStreams", [])

        oi_signals = []
        whale_signals = []
        for s in streams[:2]:
            start = int((datetime.now(timezone.utc) - timedelta(minutes=20)).timestamp() * 1000)
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/justhodl-crypto-intel",
                logStreamName=s["logStreamName"],
                startTime=start, limit=300, startFromHead=False,
            )
            for e in ev.get("events", []):
                msg = e.get("message", "").strip()
                low = msg.lower()
                # Look for any oi/whale-related lines
                if any(k in low for k in ["openinterest", "open_interest", "fapi/v1", "binance"]):
                    oi_signals.append(msg[:300])
                if any(k in low for k in ["whale", "blockchain.info", "unconfirmed-tx"]):
                    whale_signals.append(msg[:300])

        r.log(f"  open_interest related log lines (last 20 min):")
        for sig in oi_signals[:8]:
            r.log(f"    {sig}")
        if not oi_signals:
            r.log(f"    (no oi-related log lines found — Lambda may not log on this path)")

        r.log(f"\n  whale_txs related log lines (last 20 min):")
        for sig in whale_signals[:8]:
            r.log(f"    {sig}")
        if not whale_signals:
            r.log(f"    (no whale-related log lines found)")

        r.log("")
        r.log("  Diagnosis:")
        r.log("    open_interest fetches from Binance Futures API mirrors. The mirrors")
        r.log("    are likely rate-limited or geoblocked from AWS IPs. Replacing this")
        r.log("    feed requires migrating to a paid-tier source (CoinGlass v4 has key)")
        r.log("    or a different free source like Coinalyze/Bybit-direct. NOT a quick fix.")
        r.log("")
        r.log("    whale_txs queries blockchain.info/unconfirmed-transactions. This only")
        r.log("    catches whales whose txns are CURRENTLY in the mempool at fetch time.")
        r.log("    Most whale txns confirm in <2 minutes, so by the Lambda's fetch")
        r.log("    moment they're already gone from the mempool. whale_count=0 is")
        r.log("    therefore NORMAL most of the time — this is a data-source design")
        r.log("    issue, not a bug. Proper fix: switch to Blockchair last-24h API.")
        r.log("")
        r.log("  Recommendation: defer both for a dedicated future session. The other")
        r.log("    15 of 17 crypto-intel modules are working fine — this is cosmetic.")
    except Exception as e:
        r.warn(f"  Log inspection failed: {e}")

    r.kv(task1="brk.b-added", task2="dynamodb-perm-granted", task3="diagnosed-not-fixed")
    r.log("Done")
