#!/usr/bin/env python3
"""1042 — Restore production env vars (clear ops/1040 stale MAX_TICKERS=5),
redeploy v2.1 with diagnostic logging, sync-invoke with small batch,
and pull CloudWatch logs to see what's actually failing.
"""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1042_ticker_trends_diag.json"
REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=720, connect_timeout=10))
logs = boto3.client("logs", region_name=REGION)
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
    
    # ── Phase 1: clear stale env vars + redeploy code
    print("[1042] Phase 1: clear stale env vars + redeploy")
    
    # Get current env
    cfg = lam.get_function_configuration(FunctionName="justhodl-ticker-trends")
    cur_env = (cfg.get("Environment") or {}).get("Variables") or {}
    out["env_before"] = cur_env
    
    # Clear MAX_TICKERS + SLEEP_BETWEEN_S overrides; keep anything else
    new_env = {k: v for k, v in cur_env.items()
                if k not in ("MAX_TICKERS", "SLEEP_BETWEEN_S")}
    # Keep TRY_GOOGLE=1 explicit
    new_env["TRY_GOOGLE"] = "1"
    # Test with smaller batch first to keep diag fast
    new_env["MAX_TICKERS"] = "8"
    out["env_after"] = new_env
    
    # Update code + env in one go
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
        
        lam.update_function_configuration(
            FunctionName="justhodl-ticker-trends",
            Environment={"Variables": new_env},
        )
        lam.get_waiter("function_updated").wait(FunctionName="justhodl-ticker-trends")
        out["redeploy"] = "ok"
    except Exception as e:
        out["redeploy_err"] = str(e)[:300]
        pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
        pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
        return
    
    time.sleep(3)
    
    # ── Phase 2: sync-invoke (small batch, should complete in ~30-60s)
    print("[1042] Phase 2: sync-invoke (8-ticker diag batch)")
    long_lam = boto3.client("lambda", region_name=REGION,
                              config=Config(read_timeout=180, connect_timeout=10))
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
            out["invoke"]["raw"] = body[:500]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]
    
    # ── Phase 3: pull CloudWatch logs (the prints will tell us everything)
    print("[1042] Phase 3: read CloudWatch logs")
    time.sleep(2)
    lg = "/aws/lambda/justhodl-ticker-trends"
    try:
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime", descending=True, limit=1,
        ).get("logStreams", [])
        if streams:
            ev = logs.get_log_events(
                logGroupName=lg, logStreamName=streams[0]["logStreamName"],
                limit=200, startFromHead=False,
            )
            out["log_events"] = [
                {
                    "ts":  datetime.fromtimestamp(e["timestamp"]/1000, tz=timezone.utc).isoformat()[:19],
                    "msg": e.get("message", "").strip()[:400],
                } for e in (ev.get("events") or [])[-80:]
            ]
            # Count error types
            from collections import defaultdict
            err_pat = defaultdict(int)
            for e in out["log_events"]:
                msg = e["msg"]
                if "HTTP " in msg:
                    # Extract HTTP code
                    import re
                    m = re.search(r"HTTP (\d+)", msg)
                    if m:
                        err_pat[f"HTTP_{m.group(1)}"] += 1
                if "wiki[" in msg:
                    err_pat["wiki_attempt"] += 1
                if "gtrends" in msg or "google" in msg.lower():
                    err_pat["gtrends_attempt"] += 1
                if "no_source_data" in msg or "err on" in msg:
                    err_pat["source_failure"] += 1
            out["error_pattern"] = dict(err_pat)
    except Exception as e:
        out["logs_err"] = str(e)[:200]
    
    # ── Phase 4: read final S3 output
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/ticker-trends.json")
        tt = json.loads(obj["Body"].read().decode("utf-8"))
        out["s3_output"] = {
            "generated_at":  tt.get("generated_at"),
            "n_processed":   tt.get("n_processed"),
            "n_ok":          tt.get("n_ok"),
            "errors":        tt.get("errors"),
            "sources_used_count": tt.get("sources_used_count"),
            "top_5":         [
                {"ticker": r["ticker"], "score": r["score"],
                 "velocity": r["velocity"], "sources": r.get("sources_used"),
                 "wiki_article": r.get("wiki_article")}
                for r in (tt.get("top_20") or [])[:5]
            ],
        }
    except Exception as e:
        out["s3_output_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
