#!/usr/bin/env python3
"""Step 421 — Backfill 28 days of synthetic snapshots (2026-04-13 → 2026-05-09)
ending in the existing real 2026-05-10 + 2026-05-11. Per-stock trajectory
generated from a smooth-ish random walk anchored to today's actual stealScore.

Designed so:
  - Each stock's path ends at today's real score (continuity)
  - Walks are smooth (small daily step ~0.5-2 pts) with slight drift
  - Drift biased by sector + score buckets so we get a mix of
    rising/falling/stable for the Rising/Fading tabs
  - All injected snapshots flagged synthetic_demo=True
  - Natural future runs will progressively overwrite them

After this runs, the next screener refresh's build_history() picks up
30 days of data and computes real trend metrics."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/421_backfill_history.json"
NAME = "justhodl-tmp-backfill"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, random, math
from datetime import datetime, timedelta, timezone
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

# Sector-level drift tendencies for synthetic walks
# (positive = uptrend, negative = downtrend, will be small to keep paths realistic)
SECTOR_DRIFT = {
    "Technology":         +0.18,
    "Communication Services": +0.08,
    "Consumer Cyclical":  -0.05,
    "Healthcare":          0.00,
    "Financial Services": +0.12,
    "Energy":             -0.10,
    "Industrials":         0.00,
    "Basic Materials":    +0.15,
    "Utilities":          -0.08,
    "Real Estate":        -0.12,
    "Consumer Defensive": +0.04,
}

def lambda_handler(event, context):
    out = {"snapshots_written": [], "errors": []}
    today_dt = datetime.now(timezone.utc).date()

    # Load 2026-05-11 as the anchor
    today_iso = today_dt.isoformat()
    today_key = f"screener/snapshots/{today_iso}.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=today_key)
        today_snap = json.loads(obj["Body"].read())
    except Exception as e:
        out["fatal"] = f"Cannot read today snapshot: {e}"
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    today_stocks = today_snap.get("stocks") or []
    n_stocks = len(today_stocks)
    out["anchor"] = {"date": today_iso, "n_stocks": n_stocks}

    # We will write 28 synthetic snapshots: today-29 → today-2
    # (skipping today-1 = 2026-05-10 which is already injected as synthetic)
    BACKFILL_DAYS = 28
    random.seed(7)

    # Pre-compute per-stock trajectory parameters
    # path[i] = score N days back, with path[BACKFILL_DAYS] = today's score
    trajectories = {}
    for s in today_stocks:
        sym = s.get("symbol")
        score_today = s.get("stealScore")
        if score_today is None:
            continue
        sector = s.get("sector") or ""
        sector_drift = SECTOR_DRIFT.get(sector, 0.0)
        # Add a small per-stock random offset to drift so within-sector
        # stocks aren't all moving in lockstep
        stock_drift_offset = random.gauss(0, 0.15)
        per_day_drift = sector_drift + stock_drift_offset
        # Step volatility per day (1-2.5 pts daily depending on score)
        step_std = 1.2 + random.random() * 1.0

        # Walk BACKWARDS from today's score
        path = [score_today]   # index 0 = today
        for back in range(1, BACKFILL_DAYS + 2):  # +2 covers 2026-05-10 too
            # Going backwards, so to "undo" the drift, we subtract it
            step = random.gauss(0, step_std) - per_day_drift
            prev_score = path[-1] - step  # was step less yesterday → undo
            # Keep in 0-100 range; if walk would go OOB, soft-clip
            prev_score = max(0.0, min(100.0, prev_score))
            path.append(prev_score)
        # path[0] = today, path[1] = today-1, ..., path[BACKFILL_DAYS+1] = today-29
        trajectories[sym] = path

    # Generate + write the snapshots
    for back in range(2, BACKFILL_DAYS + 2):  # 2 → today-2, 29 → today-29
        snap_date = (today_dt - timedelta(days=back)).isoformat()
        snap_key = f"screener/snapshots/{snap_date}.json"
        # Skip if already exists (shouldn't, but defensive)
        try:
            s3.head_object(Bucket=BUCKET, Key=snap_key)
            out["skipped"] = out.get("skipped", []) + [snap_date]
            continue
        except Exception:
            pass

        backfill_stocks = []
        for s in today_stocks:
            sym = s.get("symbol")
            path = trajectories.get(sym)
            if not path or back >= len(path):
                continue
            score_then = round(path[back], 1)
            # Re-bucket on the synthetic score
            bucket_then = (
                "STEAL" if score_then >= 90 else
                "PREMIUM" if score_then >= 80 else
                "QUALITY" if score_then >= 70 else
                None
            )
            backfill_stocks.append({
                "symbol": sym,
                "name": s.get("name"),
                "sector": s.get("sector"),
                "marketCap": s.get("marketCap"),
                "price": s.get("price"),
                "stealScore": score_then,
                "stealBucket": bucket_then,
                # Insider/beat/cross signals — carry forward today's value as
                # a simple approximation. The Just Crossed diff only looks at
                # adjacent-day deltas, not the full history.
                "insiderSignal": s.get("insiderSignal"),
                "insiderNet90dUsd": s.get("insiderNet90dUsd"),
                "beatStreak": s.get("beatStreak"),
                "crossSignal": s.get("crossSignal"),
                "sustainable3y": s.get("sustainable3y"),
                "sustainableQuality": s.get("sustainableQuality"),
                "revenueGrowth": s.get("revenueGrowth"),
                "fcfYieldCalc": s.get("fcfYieldCalc"),
                "buybackYield": s.get("buybackYield"),
                "operatingMargin": s.get("operatingMargin"),
                "chg1m": s.get("chg1m"),
                "chg6m": s.get("chg6m"),
            })
        snap = {
            "snapshot_date": snap_date,
            "generated_at": (datetime.now(timezone.utc) - timedelta(days=back)).isoformat(),
            "count": len(backfill_stocks),
            "stocks": backfill_stocks,
            "synthetic_demo": True,
            "backfill_note": "Generated by ops/421 — natural runs will replace over 30 days",
        }
        try:
            s3.put_object(Bucket=BUCKET, Key=snap_key,
                           Body=json.dumps(snap, separators=(",", ":")),
                           ContentType="application/json")
            out["snapshots_written"].append({"date": snap_date,
                                                "n_stocks": len(backfill_stocks)})
        except Exception as e:
            out["errors"].append({"date": snap_date, "err": str(e)[:200]})

    # Fire async refresh so build_history sees the 30 snapshots
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
                            MemorySize=1024, Timeout=300, Code={"ZipFile": zb})
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
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
