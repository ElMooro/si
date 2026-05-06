#!/usr/bin/env python3
"""
Step 249 — Re-warm engine after v2.0.1 patch (arithmetic annualization
+ data sufficiency gate) and probe results, persist headline to
aws/ops/reports/249_backtest_v2_0_1_result.json.

Combines step 247 (warmup) + step 248 (probe) into one run since both
must execute against the new code post-deploy.
"""
import json
import os
import time
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-backtest-engine"
SUMMARY_KEY = "backtest/summary.json"
REPORT_PATH = "aws/ops/reports/249_backtest_v2_0_1_result.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    started = time.time()

    # 1. Invoke synchronously
    print(f"[step 249] invoking {LAMBDA_NAME}…")
    resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = time.time() - started
    func_err = resp.get("FunctionError")
    print(f"  status={resp.get('StatusCode')}  func_err={func_err}  elapsed={elapsed:.1f}s")
    payload = json.loads(resp["Payload"].read())
    if func_err:
        print(f"  ⚠ Lambda raised: {payload}")
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w") as f:
            json.dump({"error": "lambda_invoke_failed", "payload": payload}, f, indent=2)
        return 1

    # 2. Read back summary.json
    time.sleep(2)
    body = json.loads(s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)["Body"].read())

    h = body.get("honest_summary") or {}
    real = body.get("realistic_summary") or {}
    summ = body.get("summary") or {}

    # 3. Build report
    now = datetime.now(timezone.utc)
    out = {
        "probed_at": now.isoformat(timespec="seconds"),
        "engine_version": body.get("v"),
        "engine_generated_at": body.get("generated_at"),
        "data_sufficient": h.get("data_sufficient"),
        "data_sufficiency_note": h.get("data_sufficiency_note"),
        "n_business_days": h.get("n_business_days"),
        "n_years": h.get("n_years"),
        "n_trades": h.get("n_trades_distributed"),
        "first_date": summ.get("first_date"),
        "last_date": summ.get("last_date"),
        # Period stats (always reliable)
        "period_total_return_pct": h.get("period_total_return_pct"),
        "period_max_drawdown_pct": h.get("period_max_drawdown_pct"),
        "period_alpha_vs_spy_pct": h.get("period_alpha_vs_spy_pct"),
        "daily_hit_rate": h.get("daily_hit_rate"),
        "n_pos_days": h.get("n_pos_days"),
        "n_neg_days": h.get("n_neg_days"),
        # Annualized (only meaningful if data_sufficient)
        "annualized_return_pct_arith": h.get("annualized_return_pct"),
        "annualized_vol_pct": h.get("annualized_vol_pct"),
        "sharpe_point": h.get("sharpe_point"),
        "sharpe_p5": h.get("sharpe_p5"),
        "sharpe_median": h.get("sharpe_median"),
        "sharpe_p95": h.get("sharpe_p95"),
        # Old method comparison
        "v1_1_sharpe": summ.get("sharpe_proxy"),
        "v1_2_realistic_sharpe": real.get("sharpe_proxy"),
    }

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"\n[step 249] wrote {REPORT_PATH}")
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
