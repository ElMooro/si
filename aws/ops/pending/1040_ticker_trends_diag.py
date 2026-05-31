#!/usr/bin/env python3
"""1040 — Why did ticker-trends async fire produce no output?

Possible causes:
  1. Lambda not even invoked (async invoke failed silently)
  2. Crashed on first ticker (import error, etc)
  3. Google Trends 429'd everything
  4. Just slow — still running after 12 minutes

Check:
  a. CloudWatch log group for justhodl-ticker-trends — most recent stream
  b. If logs show errors, capture them
  c. Re-invoke sync this time + capture immediate output
"""
import json, os, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1040_ticker_trends_diag.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=900, connect_timeout=10))
logs = boto3.client("logs", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # 1. List log streams for the function
    lg = "/aws/lambda/justhodl-ticker-trends"
    try:
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime", descending=True, limit=3,
        ).get("logStreams", [])
        out["n_streams"] = len(streams)
        out["streams_summary"] = [
            {
                "name":         s["logStreamName"],
                "first":        s.get("firstEventTimestamp"),
                "last":         s.get("lastEventTimestamp"),
                "stored_bytes": s.get("storedBytes"),
            } for s in streams[:3]
        ]
    except Exception as e:
        out["streams_err"] = str(e)[:200]
        streams = []
    
    # 2. Get last 30 log events from most recent stream
    out["recent_logs"] = []
    if streams:
        try:
            ev = logs.get_log_events(
                logGroupName=lg, logStreamName=streams[0]["logStreamName"],
                limit=80, startFromHead=False,
            )
            for e in ev.get("events") or []:
                msg = e.get("message", "")
                out["recent_logs"].append({
                    "ts":  datetime.fromtimestamp(e["timestamp"]/1000, tz=timezone.utc).isoformat()[:19],
                    "msg": msg.strip()[:300],
                })
            out["recent_logs"] = out["recent_logs"][-30:]
        except Exception as e:
            out["recent_logs_err"] = str(e)[:200]
    
    # 3. Sync-invoke with a TINY universe to diagnose fast
    print("[1040] sync-invoking with overrides MAX_TICKERS=5 SLEEP_BETWEEN=2…")
    
    # Temporarily override env to make this fast
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-ticker-trends")
        existing_env = (cfg.get("Environment") or {}).get("Variables") or {}
        new_env = {**existing_env, "MAX_TICKERS": "5", "SLEEP_BETWEEN_S": "2"}
        lam.update_function_configuration(
            FunctionName="justhodl-ticker-trends",
            Environment={"Variables": new_env},
        )
        lam.get_waiter("function_updated").wait(FunctionName="justhodl-ticker-trends")
        out["env_overridden"] = True
        # Wait for IAM/env propagation
        time.sleep(3)
    except Exception as e:
        out["env_override_err"] = str(e)[:200]
    
    # Now invoke
    try:
        r = lam.invoke(FunctionName="justhodl-ticker-trends",
                        InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out["sync_invoke"] = {
            "status": r.get("StatusCode"),
            "fn_err": r.get("FunctionError"),
            "raw":    body[:400] if r.get("FunctionError") else None,
        }
        try:
            p = json.loads(body)
            out["sync_invoke"]["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            pass
    except Exception as e:
        out["sync_invoke_err"] = str(e)[:200]
    
    # 4. After invoke, dump latest log stream events
    time.sleep(2)
    try:
        streams2 = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime", descending=True, limit=1,
        ).get("logStreams", [])
        if streams2:
            ev = logs.get_log_events(
                logGroupName=lg, logStreamName=streams2[0]["logStreamName"],
                limit=40, startFromHead=False,
            )
            out["post_invoke_logs"] = [
                {
                    "ts":  datetime.fromtimestamp(e["timestamp"]/1000, tz=timezone.utc).isoformat()[:19],
                    "msg": e.get("message", "").strip()[:300],
                } for e in (ev.get("events") or [])[-20:]
            ]
    except Exception as e:
        out["post_invoke_logs_err"] = str(e)[:200]
    
    # 5. Check S3 output now
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/ticker-trends.json")
        data = json.loads(obj["Body"].read().decode("utf-8"))
        out["s3_output"] = {
            "exists":       True,
            "generated_at": data.get("generated_at"),
            "n_ok":         data.get("n_ok"),
            "n_processed":  data.get("n_processed"),
            "errors":       data.get("errors"),
            "top_5_sample": [
                {"ticker": r["ticker"], "velocity": r["velocity"],
                 "level": r["current_level"], "interp": r["interp"]}
                for r in (data.get("top_20") or [])[:5]
            ],
        }
    except s3.exceptions.NoSuchKey:
        out["s3_output"] = {"exists": False}
    except Exception as e:
        out["s3_output_err"] = str(e)[:200]
    
    # 6. Restore prod env
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-ticker-trends")
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        # Remove overrides
        env.pop("MAX_TICKERS", None)
        env.pop("SLEEP_BETWEEN_S", None)
        lam.update_function_configuration(
            FunctionName="justhodl-ticker-trends",
            Environment={"Variables": env} if env else {"Variables": {"_": ""}},
        )
        out["env_restored"] = True
    except Exception as e:
        out["env_restore_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
