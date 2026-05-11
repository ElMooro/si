#!/usr/bin/env python3
"""Step 417 — Verify today's snapshot wrote, inject synthetic yesterday
snapshot (with perturbations to create diff events), then fire 2nd refresh
so just-crossed.json gets computed for the demo."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/417_inject_yesterday.json"
NAME = "justhodl-tmp-inject"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
from datetime import datetime, timedelta, timezone
import boto3
import random

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

def lambda_handler(event, context):
    out = {}
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    yesterday_iso = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    today_key = f"screener/snapshots/{today_iso}.json"
    yesterday_key = f"screener/snapshots/{yesterday_iso}.json"

    # 1. Read today's snapshot
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=today_key)
        today_snap = json.loads(obj["Body"].read())
        out["today_snapshot"] = {
            "key": today_key,
            "snapshot_date": today_snap.get("snapshot_date"),
            "n_stocks": len(today_snap.get("stocks") or []),
        }
    except Exception as e:
        out["today_err"] = str(e)[:200]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # 2. Build SYNTHETIC yesterday snapshot — based on today's data with
    # deliberate perturbations across categories to produce a rich variety
    # of events when the next Lambda run computes the diff.
    random.seed(42)  # deterministic for reproducibility
    today_stocks = today_snap.get("stocks") or []
    yest_stocks = []

    # Track which stocks we'll perturb in which way
    perturb_summary = {
        "score_down_5pt": 0,    # become SCORE_JUMP events when today comes
        "score_up_5pt": 0,      # become SCORE_DROP events
        "tier_strip": 0,        # remove tier so today's tier-having stocks fire ENTERED_TIER
        "insider_flip": 0,       # set insiderSignal to 'neutral' so buyers look new
        "cross_strip": 0,        # remove crossSignal so today's cross looks new
        "beat_strip": 0,        # zero out beatStreak so today's streak triggers milestone
        "fcfy_below_5": 0,      # lower fcfYieldCalc so today's >5% triggers
        "revgrowth_below_15": 0,
        "buyback_zero": 0,
        "sustainable_strip": 0,
        "unchanged": 0,
    }

    for i, s in enumerate(today_stocks):
        y = dict(s)  # copy
        bucket = i % 25  # spread perturbations across the 503 stocks

        if bucket == 0 and y.get("stealScore") is not None:
            # 5% sample: drop stealScore by 8-12 pts in yesterday so today shows a JUMP
            y["stealScore"] = max(0, y["stealScore"] - random.uniform(8, 12))
            # And strip tier so today's tier-having stocks fire ENTERED_TIER
            y["stealBucket"] = None
            perturb_summary["score_down_5pt"] += 1
            perturb_summary["tier_strip"] += 1
        elif bucket == 1 and y.get("stealScore") is not None:
            # Another 5% sample: raise stealScore in yesterday so today shows a DROP
            y["stealScore"] = min(100, y["stealScore"] + random.uniform(6, 10))
            perturb_summary["score_up_5pt"] += 1
        elif bucket == 2 and y.get("insiderSignal") == "buying":
            # Set yesterday to neutral so today's 'buying' triggers INSIDER_TURNED_BUYING
            y["insiderSignal"] = "neutral"
            perturb_summary["insider_flip"] += 1
        elif bucket == 3 and y.get("crossSignal") == "GOLDEN":
            y["crossSignal"] = None
            perturb_summary["cross_strip"] += 1
        elif bucket == 4 and (y.get("beatStreak") or 0) >= 3:
            y["beatStreak"] = max(0, (y["beatStreak"] or 0) - 1)
            perturb_summary["beat_strip"] += 1
        elif bucket == 5 and (y.get("fcfYieldCalc") or 0) >= 5:
            y["fcfYieldCalc"] = (y["fcfYieldCalc"] or 0) - 2
            perturb_summary["fcfy_below_5"] += 1
        elif bucket == 6 and (y.get("revenueGrowth") or 0) >= 15:
            y["revenueGrowth"] = (y["revenueGrowth"] or 0) - 3
            perturb_summary["revgrowth_below_15"] += 1
        elif bucket == 7 and (y.get("buybackYield") or 0) >= 2:
            y["buybackYield"] = max(0, (y["buybackYield"] or 0) - 1.5)
            perturb_summary["buyback_zero"] += 1
        elif bucket == 8 and y.get("sustainableQuality"):
            y["sustainableQuality"] = False
            perturb_summary["sustainable_strip"] += 1
        else:
            perturb_summary["unchanged"] += 1
        yest_stocks.append(y)

    yest_snap = {
        "snapshot_date": yesterday_iso,
        "generated_at": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
        "count": len(yest_stocks),
        "stocks": yest_stocks,
        "synthetic_demo": True,   # flag this as fake data for transparency
    }

    s3.put_object(Bucket=BUCKET, Key=yesterday_key,
                   Body=json.dumps(yest_snap, separators=(",", ":")),
                   ContentType="application/json")
    out["yesterday_snapshot"] = {
        "key": yesterday_key,
        "snapshot_date": yesterday_iso,
        "n_stocks": len(yest_stocks),
        "synthetic": True,
        "perturbations": perturb_summary,
    }

    # 3. Fire a 2nd async refresh so the screener Lambda picks up the new
    # yesterday-snapshot and writes just-crossed.json with real diff events.
    try:
        resp = lam.invoke(
            FunctionName="justhodl-stock-screener",
            InvocationType="Event",
            Payload=json.dumps({"force": True}).encode())
        out["refresh_invoke"] = {"status": resp.get("StatusCode")}
    except Exception as e:
        out["refresh_invoke"] = {"error": str(e)[:200]}

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=120, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:6000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
