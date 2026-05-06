#!/usr/bin/env python3
"""
Step 248 — Probe v2.0 backtest result and persist to aws/ops/reports/.

run-ops.yml auto-commits anything in aws/ops/reports/, which means
after this script runs we can `git pull` and read the saved JSON
without needing AWS or api.github.com access.

This is the workaround for: sandbox can't reach S3 or GitHub Actions
log API directly, but it CAN read commits via git over the http remote.
"""
import json
import os
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
SUMMARY_KEY = "backtest/summary.json"
REPORT_PATH = "aws/ops/reports/248_backtest_v2_probe.json"

s3 = boto3.client("s3", region_name=REGION)


def main():
    now = datetime.now(timezone.utc)

    out = {"probed_at": now.isoformat(timespec="seconds"), "source": f"s3://{BUCKET}/{SUMMARY_KEY}"}
    try:
        body = json.loads(s3.get_object(Bucket=BUCKET, Key=SUMMARY_KEY)["Body"].read())
    except Exception as e:
        out["error"] = f"failed to read S3: {e}"
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w") as f:
            json.dump(out, f, indent=2)
        print(json.dumps(out, indent=2))
        return 1

    out["engine_version"] = body.get("v")
    out["engine_generated_at"] = body.get("generated_at")
    out["engine_method"] = (body.get("method") or "")[:200]
    out["v1_1_summary"] = {
        k: body.get("summary", {}).get(k) for k in (
            "n_outcomes", "win_rate", "first_date", "last_date", "n_days",
            "total_return_pct", "max_drawdown_pct", "sharpe_proxy",
            "spy_return_pct", "alpha_vs_spy_pct",
        )
    }
    out["v1_2_realistic_summary"] = body.get("realistic_summary")
    out["v2_0_honest_summary"] = body.get("honest_summary")

    # Headline summary that's easy to read in a `cat` of the file
    h = body.get("honest_summary") or {}
    if h and not h.get("error"):
        out["HEADLINE"] = {
            "sharpe_v1_1_idealized":  body.get("summary", {}).get("sharpe_proxy"),
            "sharpe_v1_2_realistic":  (body.get("realistic_summary") or {}).get("sharpe_proxy"),
            "sharpe_v2_0_honest_pt":  h.get("sharpe_point"),
            "sharpe_v2_0_p5":         h.get("sharpe_p5"),
            "sharpe_v2_0_median":     h.get("sharpe_median"),
            "sharpe_v2_0_p95":        h.get("sharpe_p95"),
            "ann_return_v2_0_pct":    h.get("annualized_return_pct"),
            "ann_vol_v2_0_pct":       h.get("annualized_vol_pct"),
            "max_dd_v2_0_pct":        h.get("max_drawdown_pct"),
            "n_business_days":        h.get("n_business_days"),
            "n_trades":               h.get("n_trades_distributed"),
            "daily_hit_rate":         h.get("daily_hit_rate"),
        }

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)

    print(f"[step 248] wrote {REPORT_PATH}")
    print(json.dumps(out.get("HEADLINE") or out, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
