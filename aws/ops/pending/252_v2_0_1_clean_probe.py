#!/usr/bin/env python3
"""Step 252 — final probe of v2.0.1 after NameError fix."""
import json, os, time, boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-backtest-engine"
SUMMARY_KEY = "backtest/summary.json"
REPORT_PATH = "aws/ops/reports/252_v2_0_1_clean.json"


def main():
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    # Wait up to 90s for the deploy to settle, retry the invoke until
    # honest_summary has no error key
    deadline = time.time() + 120
    body = None
    while time.time() < deadline:
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}")
        if resp.get("FunctionError"):
            print(f"  Lambda raised: {resp.get('FunctionError')}")
            time.sleep(8)
            continue
        time.sleep(2)
        body = json.loads(s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)["Body"].read())
        h = body.get("honest_summary") or {}
        if h.get("error"):
            print(f"  honest_summary still has error: {h['error']} — waiting for redeploy")
            time.sleep(10)
            continue
        if h.get("data_sufficient") is not None or h.get("sharpe_point") is not None:
            print(f"  ✓ honest_summary clean")
            break
        print(f"  honest_summary unexpected shape, waiting…")
        time.sleep(10)

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
        # Annualized — gated by sufficiency
        "annualized_return_pct_arith": h.get("annualized_return_pct"),
        "annualized_vol_pct": h.get("annualized_vol_pct"),
        "sharpe_point": h.get("sharpe_point"),
        "sharpe_p5": h.get("sharpe_p5"),
        "sharpe_median": h.get("sharpe_median"),
        "sharpe_p95": h.get("sharpe_p95"),
        # Comparison
        "v1_1_sharpe": summ.get("sharpe_proxy"),
        "v1_2_realistic_sharpe": real.get("sharpe_proxy"),
    }

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
