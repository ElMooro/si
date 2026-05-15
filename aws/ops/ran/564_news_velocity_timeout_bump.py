#!/usr/bin/env python3
"""564 — Bump news-velocity Lambda timeout 600s→900s (retry path was eating
546/600s budget). Also captures NVDA news-velocity SURGE for the record."""
import io, json, os, time as _time
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/564_news_velocity_timeout_bump.json"
NAME = "justhodl-news-velocity"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Update timeout
    try:
        pre = lam.get_function_configuration(FunctionName=NAME)
        out["pre_timeout"] = pre.get("Timeout")
        lam.update_function_configuration(FunctionName=NAME, Timeout=900,
            Description="GDELT news article velocity engine — hourly attention surge detection (v1.2.1)")
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
        post = lam.get_function_configuration(FunctionName=NAME)
        out["post_timeout"] = post.get("Timeout")
        out["update"] = "OK"
    except Exception as e:
        out["update_err"] = str(e)[:200]

    # Capture current sidecar for the record
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/news-velocity.json")
        p = json.loads(obj["Body"].read())
        # Find SURGE / ELEVATED tickers
        surge_elevated = []
        for t, v in (p.get("by_ticker") or {}).items():
            if v.get("velocity_flag") in ("SURGE", "ELEVATED"):
                surge_elevated.append({
                    "ticker": t,
                    "z_score_30d": v.get("z_score_30d"),
                    "velocity_pct": v.get("velocity_pct"),
                    "velocity_flag": v.get("velocity_flag"),
                    "current_volume": v.get("current_volume"),
                    "avg_30d": v.get("avg_30d"),
                    "current_date": v.get("current_date"),
                })
        out["sidecar_now"] = {
            "version": p.get("version"),
            "modified": obj["LastModified"].isoformat()[:19],
            "elapsed_seconds": p.get("elapsed_seconds"),
            "composite_regime": p.get("composite_regime"),
            "composite_signal": p.get("composite_signal"),
            "n_with_data": p.get("n_with_data"),
            "n_with_err": p.get("n_with_err"),
            "n_surge": p.get("n_surge"),
            "n_elevated": p.get("n_elevated"),
            "surge_elevated_tickers": surge_elevated,
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
