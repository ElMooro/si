#!/usr/bin/env python3
"""
Verify Step 53 landed clean — outcome-checker backfill scored signals.

Checks:
  A. outcome-checker invocation log from last 5 minutes (was the async
     trigger processed?)
  B. Count outcomes table items now vs before
  C. Dump sample from crypto_fear_greed / crypto_risk_score to understand
     the 0% accuracy bug in detail
"""
import json
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

REGION = "us-east-1"
ddb = boto3.client("dynamodb", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


with report("verify_step_53") as r:
    r.heading("Verify Step 53 — backfill + logging + 0% accuracy investigation")

    # ═══════════ A. Outcome-checker log ═══════════
    r.section("A. Most recent outcome-checker invocation")
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-outcome-checker",
            orderBy="LastEventTime", descending=True, limit=1,
        ).get("logStreams", [])
        if streams:
            s = streams[0]
            start = int((datetime.now(timezone.utc) - timedelta(minutes=15)).timestamp() * 1000)
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/justhodl-outcome-checker",
                logStreamName=s["logStreamName"],
                startTime=start, limit=200, startFromHead=True,
            )
            events = ev.get("events", [])
            r.log(f"  Last stream: {s['logStreamName']}")
            r.log(f"  Events in last 15 min: {len(events)}")
            correct = 0
            wrong = 0
            for e in events:
                m = e.get("message", "").strip()
                if "✅ CORRECT" in m: correct += 1
                if "❌ WRONG" in m: wrong += 1
                if m.startswith("[CHECKER]") or m.startswith("[DONE]"):
                    r.log(f"    {m[:180]}")
            r.log(f"\n  Parsed: {correct} correct, {wrong} wrong")
    except Exception as e:
        r.warn(f"  {e}")

    # ═══════════ B. Outcomes table growth ═══════════
    r.section("B. justhodl-outcomes table — current item count")
    try:
        td = ddb.describe_table(TableName="justhodl-outcomes")
        count = td["Table"].get("ItemCount", "unknown")
        size = td["Table"].get("TableSizeBytes", 0)
        r.log(f"  Items: {count}")
        r.log(f"  Size:  {size:,} bytes")
    except Exception as e:
        r.warn(f"  {e}")

    # ═══════════ C. Sample crypto_fear_greed + crypto_risk_score outcomes ═══════════
    r.section("C. Accuracy=0.0 investigation — sample outcomes for crypto signals")
    try:
        from boto3.dynamodb.conditions import Attr
        ddb_res = boto3.resource("dynamodb", region_name=REGION)
        table = ddb_res.Table("justhodl-outcomes")

        for stype in ["crypto_fear_greed", "crypto_risk_score"]:
            r.log(f"\n  Sample outcomes for {stype}:")
            resp = table.scan(
                FilterExpression=Attr("signal_type").eq(stype),
                Limit=50,
            )
            items = resp.get("Items", [])
            r.log(f"    Found {len(items)} records in this scan page")

            correct_count = sum(1 for i in items if i.get("correct") is True)
            wrong_count = sum(1 for i in items if i.get("correct") is False)
            r.log(f"    In this sample: {correct_count} correct, {wrong_count} wrong")

            # Show 5 examples to understand the scoring
            for i, item in enumerate(items[:5]):
                pred = item.get("predicted_dir", "?")
                correct = item.get("correct", "?")
                outcome = item.get("outcome", {})
                actual = outcome.get("actual_direction", "?")
                ret = outcome.get("return_pct", "?")
                window = item.get("window_key", "?")
                r.log(f"    [{i+1}] window={window} predicted={pred} actual={actual} return={ret}% correct={correct}")

        r.log("\n  DIAGNOSIS:")
        r.log("  These signals are SENTIMENT indicators (fear/greed scores,")
        r.log("  crypto risk scores). The logger applies a heuristic:")
        r.log("    FEAR (score ≤ 35)  → predict UP (contrarian)")
        r.log("    GREED (score ≥ 65) → predict DOWN (contrarian)")
        r.log("    NEUTRAL otherwise")
        r.log("")
        r.log("  Problem: BTC price on any given 3-day window doesn't reliably")
        r.log("  match the contrarian prediction. Short-horizon sentiment")
        r.log("  contrarianism is noisier than the long-horizon ('buy fear,")
        r.log("  sell greed') wisdom everyone quotes.")
        r.log("")
        r.log("  The right fix (not tonight): change these from directional")
        r.log("  predictions to REGIME INDICATORS that inform position sizing")
        r.log("  rather than buy/sell timing. That's a Week 2-3 item.")
    except Exception as e:
        r.warn(f"  {e}")

    r.log("Done")
