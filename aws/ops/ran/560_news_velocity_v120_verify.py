#!/usr/bin/env python3
"""560 — Verify news-velocity v1.2.0 fix restores 15/15 tickers with data."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/560_news_velocity_v120_verify.json"
NAME = "justhodl-news-velocity"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Confirm latest deploy
    try:
        cfg = lam.get_function(FunctionName=NAME)["Configuration"]
        out["lambda_last_modified"] = cfg.get("LastModified")
        out["lambda_state"] = cfg.get("State")
    except Exception as e:
        out["lambda_err"] = str(e)[:200]

    # Force invoke — will take ~110s with 15 tickers × 7s
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        out["response_size"] = len(body)
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["raw_response"] = body[:500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-3000:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    _time.sleep(3)

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
        # Sample top_5_velocity
        ranked = (p.get("ranked") or {})
        out["sidecar"]["top_5_velocity"] = (ranked.get("top_5_velocity") or [])[:5]
        out["sidecar"]["top_5_z_30d"] = (ranked.get("top_5_z_30d") or [])[:5]
        # First 3 ticker results to spot patterns
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
