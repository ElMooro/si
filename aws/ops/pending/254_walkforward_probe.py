#!/usr/bin/env python3
"""Step 254 — Probe v2.1 walk-forward backtest after deploy."""
import json, os, time, boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-backtest-engine"
SUMMARY_KEY = "backtest/summary.json"
REPORT_PATH = "aws/ops/reports/254_walkforward_probe.json"


def main():
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    # Wait up to 2 min for v2.1 to settle
    deadline = time.time() + 120
    body = None
    while time.time() < deadline:
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}")
        if resp.get("FunctionError"):
            print(f"  Lambda raised: {resp.get('FunctionError')} — waiting")
            time.sleep(8)
            continue
        time.sleep(2)
        body = json.loads(s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)["Body"].read())
        v = body.get("v")
        wf = body.get("walkforward_summary")
        print(f"  engine_version={v}, walkforward_summary present={wf is not None}")
        if v == "2.1":
            break
        time.sleep(8)

    h = body.get("honest_summary") or {}
    wf = body.get("walkforward_summary") or {}
    real = body.get("realistic_summary") or {}
    summ = body.get("summary") or {}

    out = {
        "probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "engine_version": body.get("v"),
        "engine_generated_at": body.get("generated_at"),
        # v2.1 walk-forward
        "v2_1_walkforward": {
            "available": wf.get("available"),
            "data_sufficient": wf.get("data_sufficient"),
            "data_sufficiency_note": wf.get("data_sufficiency_note"),
            "n_snapshots": wf.get("n_snapshots"),
            "snapshots_earliest": wf.get("snapshots_earliest"),
            "snapshots_latest": wf.get("snapshots_latest"),
            "n_trades_total": wf.get("n_trades_total"),
            "n_trades_walkforward_resolved": wf.get("n_trades_walkforward_resolved"),
            "n_trades_predates_snapshots": wf.get("n_trades_predates_snapshots"),
            "n_trades_signal_missing": wf.get("n_trades_signal_missing_in_snapshot"),
            "coverage_pct": wf.get("coverage_pct"),
            "period_total_return_pct": wf.get("period_total_return_pct"),
            "period_alpha_vs_spy_pct": wf.get("period_alpha_vs_spy_pct"),
            "period_max_drawdown_pct": wf.get("period_max_drawdown_pct"),
            "annualized_return_pct": wf.get("annualized_return_pct"),
            "annualized_vol_pct": wf.get("annualized_vol_pct"),
            "sharpe_point": wf.get("sharpe_point"),
            "daily_hit_rate": wf.get("daily_hit_rate"),
            "n_pos_days": wf.get("n_pos_days"),
            "n_neg_days": wf.get("n_neg_days"),
            "reason_unavailable": wf.get("reason"),
            "fetch_errors": wf.get("fetch_errors"),
        },
        # v2.0.1 honest (still useful as comparison)
        "v2_0_1_honest": {
            "data_sufficient": h.get("data_sufficient"),
            "period_total_return_pct": h.get("period_total_return_pct"),
            "period_alpha_vs_spy_pct": h.get("period_alpha_vs_spy_pct"),
            "sharpe_point": h.get("sharpe_point"),
            "sharpe_p5": h.get("sharpe_p5"),
            "sharpe_p95": h.get("sharpe_p95"),
        },
        # legacy comparison
        "v1_1_sharpe": summ.get("sharpe_proxy"),
        "v1_2_sharpe": real.get("sharpe_proxy"),
        "n_business_days": h.get("n_business_days"),
        "n_years": h.get("n_years"),
    }

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
