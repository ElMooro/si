#!/usr/bin/env python3
"""
Step 250 — Wait for v2.0.1 engine deploy, re-warm, probe.

Step 249 hit a race condition: deploy-lambdas was still updating the
Lambda code when step 249 ran, so the probe captured OLD v2.0 output.
This script polls the Lambda's last_modified time until the deploy is
fresh, then invokes + probes.
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
REPORT_PATH = "aws/ops/reports/250_v2_0_1_final.json"
EXPECTED_V = "2.0.1"
MAX_WAIT_S = 180


def main():
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    # 1. Wait for code to be the v2.0.1 version. Easiest signal: Lambda's
    #    GetFunctionConfiguration returns LastModified ISO timestamp; we
    #    don't have a previous baseline, so we instead invoke + check S3
    #    output's "v" field until it's 2.0.1.
    started = time.time()
    deadline = started + MAX_WAIT_S
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        print(f"[step 250] attempt {attempt}: invoke + check version…")
        try:
            resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}")
            if resp.get("FunctionError"):
                print(f"  FunctionError: {resp.get('FunctionError')}")
                payload_dbg = resp["Payload"].read()
                print(f"  payload: {payload_dbg[:300]}")
                time.sleep(8)
                continue
            time.sleep(2)
            body = json.loads(s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)["Body"].read())
            v = body.get("v")
            print(f"  current S3 engine_version: {v}")
            if v == EXPECTED_V:
                print(f"  ✓ deploy is fresh ({EXPECTED_V}) after {time.time()-started:.0f}s, {attempt} attempts")
                break
            print(f"  not yet at {EXPECTED_V}, waiting 10s…")
            time.sleep(10)
        except Exception as e:
            print(f"  invoke/read err: {e}")
            time.sleep(8)
    else:
        print(f"[step 250] ⚠ timed out after {MAX_WAIT_S}s waiting for v={EXPECTED_V}")
        out = {
            "error": f"timeout waiting for v={EXPECTED_V}",
            "last_seen_version": v if "v" in locals() else "unknown",
        }
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w") as f:
            json.dump(out, f, indent=2)
        return 1

    # 2. Build the headline report
    h = body.get("honest_summary") or {}
    real = body.get("realistic_summary") or {}
    summ = body.get("summary") or {}

    out = {
        "probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "engine_version": body.get("v"),
        "engine_generated_at": body.get("generated_at"),
        "method": h.get("method"),
        "data_sufficient": h.get("data_sufficient"),
        "data_sufficiency_min_years": h.get("data_sufficiency_min_years"),
        "data_sufficiency_note": h.get("data_sufficiency_note"),
        "n_business_days": h.get("n_business_days"),
        "n_years": h.get("n_years"),
        "n_trades": h.get("n_trades_distributed"),
        "first_date": summ.get("first_date"),
        "last_date": summ.get("last_date"),
        # Period stats — always reliable
        "period_total_return_pct": h.get("period_total_return_pct"),
        "period_max_drawdown_pct": h.get("period_max_drawdown_pct"),
        "period_alpha_vs_spy_pct": h.get("period_alpha_vs_spy_pct"),
        "daily_hit_rate": h.get("daily_hit_rate"),
        "n_pos_days": h.get("n_pos_days"),
        "n_neg_days": h.get("n_neg_days"),
        # Annualized — only meaningful if data_sufficient
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
    print(f"\n[step 250] wrote {REPORT_PATH}")
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
