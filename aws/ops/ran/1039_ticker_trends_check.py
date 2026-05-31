#!/usr/bin/env python3
"""1039 — quick lazy check: did ticker-trends complete since 1038 ran?"""
import json, os, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1039_ticker_trends_check.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"checked_at": datetime.now(timezone.utc).isoformat()}
    
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/ticker-trends.json")
        body = obj["Body"].read()
        data = json.loads(body.decode("utf-8"))
        out["file_exists"] = True
        out["size_bytes"]  = len(body)
        out["generated_at"] = data.get("generated_at")
        out["duration_s"]   = data.get("duration_s")
        out["n_processed"]  = data.get("n_processed")
        out["n_ok"]         = data.get("n_ok")
        out["errors"]       = data.get("errors")
        out["config"]       = data.get("config")
        # Top 10 results
        out["top_10"] = []
        for r in (data.get("top_20") or [])[:10]:
            out["top_10"].append({
                "ticker":       r.get("ticker"),
                "score":        r.get("score"),
                "velocity":     r.get("velocity"),
                "level":        r.get("current_level"),
                "prior_level":  r.get("prior_level"),
                "max_in_range": r.get("max_in_range"),
                "interp":       r.get("interp"),
                "price_7d_pct": r.get("price_7d_pct"),
                "stealth":      r.get("stealth"),
                "thesis":       r.get("thesis"),
            })
        out["n_stealth"] = len(data.get("stealth_picks") or [])
        out["stealth_picks"] = [
            {"ticker": r["ticker"], "velocity": r["velocity"],
             "price_7d_pct": r["price_7d_pct"], "thesis": r["thesis"]}
            for r in (data.get("stealth_picks") or [])[:6]
        ]
    except s3.exceptions.NoSuchKey:
        out["file_exists"] = False
    except Exception as e:
        out["err"] = str(e)[:200]
    
    # Also re-trigger future-intelligence to pick up ticker-trends data
    if out.get("file_exists"):
        import boto3
        lam = boto3.client("lambda", region_name=REGION)
        try:
            r = lam.invoke(FunctionName="justhodl-future-intelligence",
                             InvocationType="RequestResponse", Payload=b"{}")
            body = r["Payload"].read().decode("utf-8")
            try:
                p = json.loads(body)
                out["future_intel_rerun"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
            except Exception:
                out["future_intel_rerun_raw"] = body[:300]
        except Exception as e:
            out["future_intel_rerun_err"] = str(e)[:200]
        
        # Read the refreshed composite
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/future-intelligence.json")
            fi = json.loads(obj["Body"].read().decode("utf-8"))
            out["future_intel_with_trends"] = {
                "generated_at":     fi.get("generated_at"),
                "n_scored":         fi.get("n_scored"),
                "feed_freshness":   fi.get("feed_freshness"),
                "n_high_conviction": len(fi.get("highlights", {}).get("high_conviction") or []),
                "n_4_signal":       len(fi.get("highlights", {}).get("4_signal_alignment") or []),
                "n_google_stealth": len(fi.get("highlights", {}).get("google_stealth") or []),
                "top_10":           [
                    {"ticker": r["ticker"], "score": r["future_intel_score"],
                     "subscores": r["subscores"], "n_signals": r["n_independent_signals"]}
                    for r in (fi.get("top_25") or [])[:10]
                ],
                "high_conviction": [
                    {"ticker": r["ticker"], "score": r["future_intel_score"],
                     "thesis": (r.get("thesis") or "")[:140]}
                    for r in (fi.get("highlights", {}).get("high_conviction") or [])[:8]
                ],
            }
        except Exception as e:
            out["future_intel_read_err"] = str(e)[:200]
    
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
