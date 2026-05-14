#!/usr/bin/env python3
"""561 — Verify news-velocity v1.2.1 (10s throttle + 1 retry on 429)."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/561_news_velocity_v121_verify.json"
NAME = "justhodl-news-velocity"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Wait for deploy to settle
    for i in range(20):
        try:
            cfg = lam.get_function(FunctionName=NAME)["Configuration"]
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                out["lambda_last_modified"] = cfg.get("LastModified")
                break
        except Exception: pass
        _time.sleep(5)

    # Async invoke (don't wait for response — could take 5+ min)
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="Event", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["invoke_mode"] = "async_event"
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    # Wait for sidecar to refresh — 15 tickers × 10s = 150s + retries
    _time.sleep(360)

    # Read sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/news-velocity.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 2),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "composite_regime": p.get("composite_regime"),
            "composite_signal": p.get("composite_signal"),
            "n_tickers": p.get("n_tickers"),
            "n_with_data": p.get("n_with_data"),
            "n_with_err": p.get("n_with_err"),
            "n_surge": p.get("n_surge"),
            "n_elevated": p.get("n_elevated"),
            "n_subdued": p.get("n_subdued"),
            "elapsed_seconds": p.get("elapsed_seconds"),
        }
        by_t = p.get("by_ticker") or {}
        out["sidecar"]["per_ticker_summary"] = [
            {"ticker": t, "z": v.get("z_score_30d"), "flag": v.get("velocity_flag"),
              "current": v.get("current_volume"), "err": v.get("err"),
              "from_prior_cache": v.get("from_prior_cache")}
            for t, v in by_t.items()
        ]
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
