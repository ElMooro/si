#!/usr/bin/env python3
"""1043 — Redeploy ticker-trends with em-dash fix, clear MAX_TICKERS=8 cap,
run full 80-ticker universe, validate, re-run future-intelligence to
compose 4-signal scores."""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1043_ticker_trends_v2_final.json"
REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=720, connect_timeout=10))
events_c = boto3.client("events", region_name=REGION)
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
    
    # ── Phase 1: redeploy + clear env vars
    print("[1043] Phase 1: redeploy + env reset to full production config")
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
        
        # Clear all overrides; defaults are MAX_TICKERS=80, SLEEP_BETWEEN_S=1.0
        # Don't pass empty Environment as that breaks; instead pass {} with one
        # benign key so Lambda has env to update.
        lam.update_function_configuration(
            FunctionName="justhodl-ticker-trends",
            Environment={"Variables": {"TRY_GOOGLE": "1"}},
        )
        lam.get_waiter("function_updated").wait(FunctionName="justhodl-ticker-trends")
        out["redeploy"] = "ok"
    except Exception as e:
        out["redeploy_err"] = str(e)[:300]
        pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
        pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
        return
    
    time.sleep(3)
    
    # ── Phase 2: sync-invoke full universe (80 tickers, ~2-3 min)
    print("[1043] Phase 2: sync-invoke full 80-ticker run")
    long_lam = boto3.client("lambda", region_name=REGION,
                              config=Config(read_timeout=720, connect_timeout=10))
    try:
        r = long_lam.invoke(FunctionName="justhodl-ticker-trends",
                              InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out["invoke"] = {"status": r.get("StatusCode"),
                          "fn_err": r.get("FunctionError")}
        try:
            p = json.loads(body)
            out["invoke"]["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["invoke"]["raw"] = body[:400]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]
    
    # ── Phase 3: read final S3 output
    time.sleep(2)
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/ticker-trends.json")
        tt = json.loads(obj["Body"].read().decode("utf-8"))
        out["ticker_trends"] = {
            "generated_at":  tt.get("generated_at"),
            "duration_s":    tt.get("duration_s"),
            "n_processed":   tt.get("n_processed"),
            "n_ok":          tt.get("n_ok"),
            "errors":        tt.get("errors"),
            "sources_used_count": tt.get("sources_used_count"),
            "top_20": [
                {
                    "ticker":       r["ticker"],
                    "score":        r["score"],
                    "velocity":     r["velocity"],
                    "level":        r["current_level"],
                    "prior_level":  r["prior_level"],
                    "interp":       r["interp"],
                    "price_7d_pct": r["price_7d_pct"],
                    "stealth":      r["stealth"],
                    "sources":      r.get("sources_used"),
                    "thesis":       r.get("thesis"),
                }
                for r in (tt.get("top_20") or [])[:20]
            ],
            "stealth_picks": [
                {"ticker": r["ticker"], "velocity": r["velocity"],
                 "price_7d_pct": r["price_7d_pct"], "thesis": r.get("thesis")}
                for r in (tt.get("stealth_picks") or [])[:10]
            ],
        }
    except Exception as e:
        out["ticker_trends_err"] = str(e)[:200]
    
    # ── Phase 4: re-invoke future-intelligence
    print("[1043] Phase 4: re-invoke future-intelligence (4-signal composite)")
    try:
        r = long_lam.invoke(FunctionName="justhodl-future-intelligence",
                              InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out["fi_invoke"] = {"status": r.get("StatusCode"),
                              "fn_err": r.get("FunctionError")}
        try:
            p = json.loads(body)
            out["fi_invoke"]["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["fi_invoke"]["raw"] = body[:400]
    except Exception as e:
        out["fi_invoke_err"] = str(e)[:200]
    
    time.sleep(2)
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/future-intelligence.json")
        fi = json.loads(obj["Body"].read().decode("utf-8"))
        out["future_intel"] = {
            "generated_at":   fi.get("generated_at"),
            "n_scored":       fi.get("n_scored"),
            "feed_freshness": fi.get("feed_freshness"),
            "n_high_conviction":    len(fi.get("highlights", {}).get("high_conviction") or []),
            "n_4_signal_alignment": len(fi.get("highlights", {}).get("4_signal_alignment") or []),
            "n_google_stealth":     len(fi.get("highlights", {}).get("google_stealth") or []),
            "n_multi_signal":       len(fi.get("highlights", {}).get("multi_signal") or []),
            "top_20": [
                {
                    "ticker":    r["ticker"],
                    "score":     r["future_intel_score"],
                    "n_signals": r["n_independent_signals"],
                    "subscores": r["subscores"],
                    "thesis":    (r.get("thesis") or "")[:130],
                }
                for r in (fi.get("top_25") or [])[:20]
            ],
            "high_conviction": [
                {
                    "ticker":    r["ticker"],
                    "score":     r["future_intel_score"],
                    "n_signals": r["n_independent_signals"],
                    "subscores": r["subscores"],
                    "thesis":    (r.get("thesis") or "")[:160],
                }
                for r in (fi.get("highlights", {}).get("high_conviction") or [])[:10]
            ],
            "4_signal_aligned": [
                {"ticker": r["ticker"], "score": r["future_intel_score"],
                 "subscores": r["subscores"], "thesis": (r.get("thesis") or "")[:130]}
                for r in (fi.get("highlights", {}).get("4_signal_alignment") or [])[:10]
            ],
            "google_stealth": [
                {"ticker": r["ticker"], "score": r["future_intel_score"],
                 "subscores": r["subscores"], "thesis": (r.get("thesis") or "")[:130]}
                for r in (fi.get("highlights", {}).get("google_stealth") or [])[:6]
            ],
        }
    except Exception as e:
        out["future_intel_err"] = str(e)[:200]
    
    # ── Phase 5: audit events
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"system-events/audit/{today}.jsonl")
        lines = [l for l in obj["Body"].read().decode().split("\n") if l.strip()]
        entries = [json.loads(l) for l in lines]
        from collections import defaultdict
        by_event = defaultdict(int)
        for e in entries:
            by_event[e.get("event", "?")] += 1
        out["audit_events_today"] = dict(by_event)
    except Exception as e:
        out["audit_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
