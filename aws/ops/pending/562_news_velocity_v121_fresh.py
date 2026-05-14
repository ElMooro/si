#!/usr/bin/env python3
"""562 — Force v1.2.1 invoke (async) + wait for fresh sidecar."""
import io, json, os, time as _time
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/562_news_velocity_v121_fresh.json"
NAME = "justhodl-news-velocity"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Confirm v1.2.1 is deployed
    try:
        cfg = lam.get_function(FunctionName=NAME)["Configuration"]
        out["lambda_last_modified"] = cfg.get("LastModified")
    except Exception as e:
        out["lambda_err"] = str(e)[:200]

    # Capture pre-invoke sidecar timestamp
    try:
        obj = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/news-velocity.json")
        out["pre_sidecar_modified"] = obj["LastModified"].isoformat()[:19]
    except Exception: pass

    # Async invoke
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="Event", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    # Wait for sidecar to refresh — worst case 15 × 25s = 375s + slack
    _time.sleep(420)

    # Read final sidecar
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
            "n_recovered_from_cache": p.get("n_recovered_from_cache"),
        }
        by_t = p.get("by_ticker") or {}
        out["sidecar"]["per_ticker"] = [
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
