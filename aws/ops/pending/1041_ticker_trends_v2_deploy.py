#!/usr/bin/env python3
"""1041 — Deploy ticker-trends v2 (Wikipedia primary) + verify end-to-end.

DEPLOY:
  - justhodl-ticker-trends with rewritten code

VALIDATE:
  - Sync-invoke (now runs in 2-3 min thanks to Wikipedia primary)
  - Read data/ticker-trends.json — check n_ok, sources_used, sample results
  - Sync-invoke future-intelligence to integrate the new feed
  - Read data/future-intelligence.json — check ticker_trends subscores
    populated and n_4_signal_alignment > 0
"""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1041_ticker_trends_v2_deploy.json"
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


def function_exists(name):
    try:
        lam.get_function(FunctionName=name)
        return True
    except lam.exceptions.ResourceNotFoundException:
        return False


def deploy(name):
    cfg = json.loads(pathlib.Path(f"aws/lambdas/{name}/config.json").read_text())
    zb = build_zip(name)
    rec = {"zip_size": len(zb)}
    desc = (cfg.get("description") or "")[:240]
    args = dict(
        Runtime=cfg.get("runtime", "python3.12"),
        Handler=cfg.get("handler", "lambda_function.lambda_handler"),
        Role=cfg.get("role", ROLE_ARN),
        Description=desc,
        Timeout=cfg.get("timeout", 60),
        MemorySize=cfg.get("memory", 256),
    )
    try:
        if function_exists(name):
            for attempt in range(4):
                try:
                    lam.update_function_code(FunctionName=name, ZipFile=zb, Publish=False)
                    lam.get_waiter("function_updated").wait(FunctionName=name)
                    lam.update_function_configuration(FunctionName=name, **args)
                    lam.get_waiter("function_updated").wait(FunctionName=name)
                    rec["action"] = "updated"
                    break
                except Exception as e:
                    if "ResourceConflict" in str(e) and attempt < 3:
                        time.sleep(5 * (attempt + 1))
                        continue
                    raise
        else:
            lam.create_function(FunctionName=name, **args, Code={"ZipFile": zb},
                                  Architectures=["x86_64"], Publish=False)
            lam.get_waiter("function_active_v2").wait(FunctionName=name)
            rec["action"] = "created"
        
        # Schedule
        sched = cfg.get("schedule")
        if sched:
            rule = sched["rule_name"]
            events_c.put_rule(Name=rule, ScheduleExpression=sched["cron"],
                                State="ENABLED",
                                Description=sched.get("description", "")[:240])
            arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{name}"
            events_c.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": arn}])
            sid = f"EventBridge-{rule}"
            try: lam.remove_permission(FunctionName=name, StatementId=sid)
            except Exception: pass
            lam.add_permission(
                FunctionName=name, StatementId=sid,
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule}",
            )
            rec["scheduled"] = True
    except Exception as e:
        rec["err"] = f"{type(e).__name__}: {str(e)[:200]}"
    return rec


def invoke_sync(fn, timeout_s=720):
    try:
        long_lam = boto3.client("lambda", region_name=REGION,
                                  config=Config(read_timeout=timeout_s + 60,
                                                 connect_timeout=10))
        r = long_lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                              Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out = {"status": r.get("StatusCode"), "fn_err": r.get("FunctionError")}
        if r.get("FunctionError"):
            out["err_payload"] = body[:600]
        try:
            p = json.loads(body)
            out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["raw"] = body[:300]
        return out
    except Exception as e:
        return {"fail": f"{type(e).__name__}: {str(e)[:200]}"}


def read_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # ── Phase 1: deploy
    print("[1041] Phase 1: deploy justhodl-ticker-trends v2")
    out["deploy"] = deploy("justhodl-ticker-trends")
    time.sleep(3)
    
    if out["deploy"].get("err"):
        out["aborted"] = "deploy failed"
        pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
        pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
        return
    
    # ── Phase 2: sync-invoke ticker-trends (should complete in ~2-3 min)
    print("[1041] Phase 2: sync-invoke ticker-trends (~2-3 min on Wikipedia)…")
    out["invoke_ticker_trends"] = invoke_sync("justhodl-ticker-trends", timeout_s=720)
    
    # ── Phase 3: read the output
    print("[1041] Phase 3: read data/ticker-trends.json")
    tt = read_s3("data/ticker-trends.json")
    if tt:
        out["ticker_trends_output"] = {
            "generated_at":  tt.get("generated_at"),
            "duration_s":    tt.get("duration_s"),
            "n_processed":   tt.get("n_processed"),
            "n_ok":          tt.get("n_ok"),
            "errors":        tt.get("errors"),
            "sources_used_count": tt.get("sources_used_count"),
            "top_15":        [
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
                for r in (tt.get("top_20") or [])[:15]
            ],
            "stealth_picks": [
                {
                    "ticker":       r["ticker"],
                    "velocity":     r["velocity"],
                    "price_7d_pct": r["price_7d_pct"],
                    "thesis":       r.get("thesis"),
                }
                for r in (tt.get("stealth_picks") or [])[:8]
            ],
        }
    else:
        out["ticker_trends_output"] = {"err": "file not produced"}
    
    # ── Phase 4: re-invoke future-intelligence to consume the new feed
    print("[1041] Phase 4: re-invoke future-intelligence (4-signal composite)")
    out["invoke_future_intel"] = invoke_sync("justhodl-future-intelligence", timeout_s=120)
    
    time.sleep(3)
    
    # ── Phase 5: read future-intelligence with all 4 signals
    fi = read_s3("data/future-intelligence.json")
    if fi:
        out["future_intel_output"] = {
            "generated_at":   fi.get("generated_at"),
            "n_scored":       fi.get("n_scored"),
            "feed_freshness": fi.get("feed_freshness"),
            "n_high_conviction":   len(fi.get("highlights", {}).get("high_conviction") or []),
            "n_4_signal_alignment": len(fi.get("highlights", {}).get("4_signal_alignment") or []),
            "n_google_stealth":    len(fi.get("highlights", {}).get("google_stealth") or []),
            "n_multi_signal":      len(fi.get("highlights", {}).get("multi_signal") or []),
            "top_15": [
                {
                    "ticker":     r["ticker"],
                    "score":      r["future_intel_score"],
                    "n_signals":  r["n_independent_signals"],
                    "subscores":  r["subscores"],
                    "thesis":     (r.get("thesis") or "")[:120],
                }
                for r in (fi.get("top_25") or [])[:15]
            ],
            "high_conviction": [
                {
                    "ticker":     r["ticker"],
                    "score":      r["future_intel_score"],
                    "n_signals":  r["n_independent_signals"],
                    "subscores":  r["subscores"],
                    "thesis":     (r.get("thesis") or "")[:140],
                }
                for r in (fi.get("highlights", {}).get("high_conviction") or [])[:8]
            ],
        }
    
    # ── Phase 6: today's audit events
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
