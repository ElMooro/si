#!/usr/bin/env python3
"""1045 — Redeploy ticker-trends with corrected Wikipedia mappings (4 fixes:
QBTS → D-Wave_Systems, OKLO → Oklo_Inc., DJT → literal &, UEC removed),
sync-invoke, verify the previously-404-ing tickers now succeed,
re-invoke future-intelligence to refresh composite."""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1045_wiki_404_fix.json"
REGION = "us-east-1"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=720, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)


def build_zip(name):
    src = pathlib.Path(f"aws/lambdas/{name}/source")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # ── Deploy ticker-trends code
    print("[1045] Deploy ticker-trends with corrected Wikipedia mappings…")
    zb = build_zip("justhodl-ticker-trends")
    out["zip_size"] = len(zb)
    try:
        for attempt in range(4):
            try:
                lam.update_function_code(FunctionName="justhodl-ticker-trends",
                                            ZipFile=zb, Publish=False)
                lam.get_waiter("function_updated").wait(FunctionName="justhodl-ticker-trends")
                break
            except Exception as e:
                if "ResourceConflict" in str(e) and attempt < 3:
                    time.sleep(5 * (attempt + 1)); continue
                raise
        out["redeploy"] = "ok"
    except Exception as e:
        out["redeploy_err"] = str(e)[:300]
        pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
        pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
        return
    
    time.sleep(3)
    
    # ── Sync-invoke
    print("[1045] sync-invoke ticker-trends (~80s on Wikipedia)…")
    long_lam = boto3.client("lambda", region_name=REGION,
                              config=Config(read_timeout=300, connect_timeout=10))
    try:
        r = long_lam.invoke(FunctionName="justhodl-ticker-trends",
                              InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            out["invoke"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["invoke_raw"] = body[:400]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]
    
    time.sleep(2)
    
    # ── Read output, specifically check QBTS / OKLO / DJT are now present
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/ticker-trends.json")
        tt = json.loads(obj["Body"].read().decode("utf-8"))
        all_tickers = {r["ticker"]: r for r in (tt.get("all_results") or [])}
        out["ticker_trends_summary"] = {
            "generated_at": tt.get("generated_at"),
            "duration_s":   tt.get("duration_s"),
            "n_processed":  tt.get("n_processed"),
            "n_ok":         tt.get("n_ok"),
            "errors":       tt.get("errors"),
        }
        # Verify each previously-failing ticker
        out["previously_404"] = {}
        for t in ("QBTS", "OKLO", "DJT"):
            if t in all_tickers:
                r = all_tickers[t]
                out["previously_404"][t] = {
                    "status":       "ok",
                    "score":        r.get("score"),
                    "velocity":     r.get("velocity"),
                    "level":        r.get("current_level"),
                    "wiki_article": r.get("wiki_article"),
                    "sources":      r.get("sources_used"),
                }
            else:
                out["previously_404"][t] = {"status": "still_failing"}
        # Top 10 overall
        out["top_10"] = [
            {"ticker": r["ticker"], "score": r["score"], "velocity": r["velocity"],
             "stealth": r["stealth"], "sources": r.get("sources_used")}
            for r in (tt.get("top_20") or [])[:10]
        ]
    except Exception as e:
        out["s3_err"] = str(e)[:200]
    
    # ── Re-invoke future-intelligence
    print("[1045] re-invoke future-intelligence")
    try:
        r = long_lam.invoke(FunctionName="justhodl-future-intelligence",
                              InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            out["future_intel_invoke"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["future_intel_invoke_raw"] = body[:400]
    except Exception as e:
        out["future_intel_invoke_err"] = str(e)[:200]
    
    time.sleep(2)
    
    # ── Final future-intelligence snapshot
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/future-intelligence.json")
        fi = json.loads(obj["Body"].read().decode("utf-8"))
        out["future_intel_snapshot"] = {
            "generated_at": fi.get("generated_at"),
            "n_scored":     fi.get("n_scored"),
            "feed_freshness": fi.get("feed_freshness"),
            "n_high_conviction": len(fi.get("highlights", {}).get("high_conviction") or []),
            "n_4_signal":   len(fi.get("highlights", {}).get("4_signal_alignment") or []),
            "n_google_stealth": len(fi.get("highlights", {}).get("google_stealth") or []),
            "top_5_with_trends": [
                {"ticker": r["ticker"], "score": r["future_intel_score"],
                 "subscores": r["subscores"]}
                for r in (fi.get("top_25") or [])[:5]
                if (r.get("subscores") or {}).get("ticker_trends", 0) > 0
            ][:5],
        }
    except Exception as e:
        out["fi_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
