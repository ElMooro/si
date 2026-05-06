#!/usr/bin/env python3
"""
Step 247 — Trigger justhodl-backtest-engine immediately after v2.0 deploy.

The engine runs every 6h on EventBridge. To avoid users seeing the
'pending engine recompute' fallback on backtest.html, invoke it once
synchronously now so honest_summary populates in backtest/results.json
and backtest/summary.json before the page is hit.

Reads/writes:
  - Invokes:   justhodl-backtest-engine
  - Side effects: writes backtest/results.json + backtest/summary.json
                  to s3://justhodl-dashboard-live/
  - Verifies:  reads back summary.json and reports honest_summary block
"""
import json
import time
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-backtest-engine"
SUMMARY_KEY = "backtest/summary.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] step 247 — trigger backtest engine")

    # Capture the pre-invoke generated_at so we can confirm we got a fresh run
    try:
        before = json.loads(s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)["Body"].read())
        before_ts = before.get("generated_at")
        before_v = before.get("v")
        print(f"  before: v={before_v}  generated_at={before_ts}")
    except Exception as e:
        print(f"  before: (no existing summary — {e})")
        before_ts = None
        before_v = None

    # Invoke synchronously (RequestResponse) so we wait for completion
    print(f"  invoking {LAMBDA_NAME} (RequestResponse)…")
    started = time.time()
    resp = lam.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=b"{}",
    )
    elapsed = time.time() - started
    status = resp.get("StatusCode")
    func_err = resp.get("FunctionError")
    payload_raw = resp["Payload"].read()
    print(f"  invocation: status={status}  func_err={func_err}  elapsed={elapsed:.1f}s")

    try:
        payload = json.loads(payload_raw)
    except Exception:
        payload = {"raw": payload_raw[:500].decode(errors="replace")}

    if func_err:
        print(f"  ⚠ Lambda raised: {payload}")
        return 1

    # Read back summary to verify v2.0 honest_summary block landed
    time.sleep(2)  # tiny grace period for S3 consistency
    try:
        after = json.loads(s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)["Body"].read())
    except Exception as e:
        print(f"  ⚠ failed to read back summary: {e}")
        return 1

    v = after.get("v")
    gen = after.get("generated_at")
    honest = after.get("honest_summary") or {}
    realistic = after.get("realistic_summary") or {}
    summ = after.get("summary") or {}

    print(f"\n  after: v={v}  generated_at={gen}")
    print(f"  v1.1 sharpe (idealized):       {summ.get('sharpe_proxy')}")
    print(f"  v1.2 sharpe (realistic):       {realistic.get('sharpe_proxy')}")
    print(f"  v2.0 sharpe (honest, point):   {honest.get('sharpe_point')}")
    print(f"  v2.0 sharpe (bootstrap p5):    {honest.get('sharpe_p5')}")
    print(f"  v2.0 sharpe (bootstrap median):{honest.get('sharpe_median')}")
    print(f"  v2.0 sharpe (bootstrap p95):   {honest.get('sharpe_p95')}")
    print(f"  v2.0 ann return:               {honest.get('annualized_return_pct')}%")
    print(f"  v2.0 ann vol:                  {honest.get('annualized_vol_pct')}%")
    print(f"  v2.0 max drawdown:             {honest.get('max_drawdown_pct')}%")
    print(f"  v2.0 trades distributed:       {honest.get('n_trades_distributed')}")
    print(f"  v2.0 business days:            {honest.get('n_business_days')}")
    print(f"  v2.0 daily hit rate:           {honest.get('daily_hit_rate')}")

    if v == "2.0" and honest:
        if honest.get("error"):
            print(f"\n  ⚠ honest_summary has error: {honest['error']}")
            return 1
        print("\n  ✅ v2.0 honest_summary populated — backtest.html will render the new headline section")
        return 0
    else:
        print(f"\n  ⚠ v2.0 honest_summary not populated (v={v})")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
