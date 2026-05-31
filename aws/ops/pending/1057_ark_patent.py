#!/usr/bin/env python3
"""1057 — deploy justhodl-ark-holdings + justhodl-patent-velocity + update event-coordinator.

DEPLOYS
═══════
NEW: justhodl-ark-holdings (daily 6 UTC, 512MB / 180s)
  - 6 ARK ETF holdings via daily CSV downloads from ark-funds.com
  - Day-over-day diff: NEW / ADD / TRIM / CLOSED positions
  - Cross-fund aggregation
  - Emits ark.position_change for material moves

NEW: justhodl-patent-velocity (daily 17 UTC, 512MB / 600s)
  - USPTO PatentsView API (free, no auth)
  - ~80 high-IP companies curated universe
  - Velocity: recent_90d vs trailing baseline
  - NEW CPC category detection
  - Emits patent.velocity_spike for ≥3x + ≥20 patents

UPDATE: justhodl-event-coordinator
  - Adds ark.position_change + patent.velocity_spike routes
  - Adds Telegram formatters with noise suppression filters

VERIFIES
════════
  - Both Lambdas create + schedule cleanly
  - Sync-invoke returns expected metrics
  - S3 outputs valid + populated
"""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1057_ark_patent.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=300))
long_lam = boto3.client("lambda", region_name=REGION,
                          config=Config(read_timeout=700))
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def build_zip(name):
    src = pathlib.Path(f"aws/lambdas/{name}/source")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    return buf.getvalue()


def lambda_exists(name):
    try:
        lam.get_function(FunctionName=name)
        return True
    except lam.exceptions.ResourceNotFoundException:
        return False


def upsert_schedule(rule_name, cron, target_fn, description):
    events.put_rule(Name=rule_name, ScheduleExpression=cron,
                     State="ENABLED", Description=description[:255])
    target_arn = lam.get_function(FunctionName=target_fn)["Configuration"]["FunctionArn"]
    events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": target_arn}])
    try:
        lam.add_permission(
            FunctionName=target_fn,
            StatementId=f"ebridge-{rule_name}"[:64],
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=events.describe_rule(Name=rule_name)["Arn"],
        )
    except lam.exceptions.ResourceConflictException:
        pass


def deploy_lambda(name):
    """Create/update + schedule a Lambda from its config."""
    with open(f"aws/lambdas/{name}/config.json") as f:
        cfg = json.load(f)
    cfg["role"] = ROLE_ARN
    zb = build_zip(name)
    
    if lambda_exists(name):
        for attempt in range(4):
            try:
                lam.update_function_code(FunctionName=name, ZipFile=zb, Publish=False)
                lam.get_waiter("function_updated").wait(FunctionName=name)
                break
            except Exception as e:
                if "ResourceConflict" in str(e) and attempt < 3:
                    time.sleep(5 * (attempt + 1)); continue
                raise
        lam.update_function_configuration(
            FunctionName=name,
            Timeout=cfg.get("timeout", 60),
            MemorySize=cfg.get("memory", 128),
            Description=cfg.get("description", "")[:256],
        )
        lam.get_waiter("function_updated").wait(FunctionName=name)
        action = "update_code"
    else:
        lam.create_function(
            FunctionName=name,
            Runtime=cfg["runtime"],
            Role=cfg["role"],
            Handler=cfg["handler"],
            Code={"ZipFile": zb},
            Description=cfg.get("description", "")[:256],
            Timeout=cfg.get("timeout", 60),
            MemorySize=cfg.get("memory", 128),
            Architectures=cfg.get("architectures", ["x86_64"]),
            Publish=False,
        )
        lam.get_waiter("function_active").wait(FunctionName=name)
        action = "create"
    
    sched = cfg.get("schedule") or {}
    schedule_str = None
    if sched.get("rule_name") and sched.get("cron"):
        upsert_schedule(sched["rule_name"], sched["cron"], name,
                          sched.get("description", ""))
        schedule_str = f"{sched['rule_name']} {sched['cron']}"
    
    return {"action": action, "zip_size": len(zb), "schedule": schedule_str}


def invoke_sync(name):
    r = long_lam.invoke(FunctionName=name,
                         InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        return json.loads(p["body"]) if isinstance(p.get("body"), str) else p
    except Exception:
        return {"_raw": body[:400]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: deploy ARK
    print("[1057] phase 1: deploy justhodl-ark-holdings…")
    try:
        out["ark_deploy"] = deploy_lambda("justhodl-ark-holdings")
    except Exception as e:
        out["ark_deploy_err"] = str(e)[:300]
    time.sleep(3)
    
    # Phase 2: deploy patent-velocity
    print("[1057] phase 2: deploy justhodl-patent-velocity…")
    try:
        out["patent_deploy"] = deploy_lambda("justhodl-patent-velocity")
    except Exception as e:
        out["patent_deploy_err"] = str(e)[:300]
    time.sleep(3)
    
    # Phase 3: update event coordinator
    print("[1057] phase 3: update event-coordinator…")
    try:
        zb = build_zip("justhodl-event-coordinator")
        for attempt in range(4):
            try:
                lam.update_function_code(FunctionName="justhodl-event-coordinator",
                                            ZipFile=zb, Publish=False)
                lam.get_waiter("function_updated").wait(FunctionName="justhodl-event-coordinator")
                break
            except Exception as e:
                if "ResourceConflict" in str(e) and attempt < 3:
                    time.sleep(5 * (attempt + 1)); continue
                raise
        out["coordinator"] = "ok"
    except Exception as e:
        out["coordinator_err"] = str(e)[:300]
    
    time.sleep(3)
    
    # Phase 4: sync-invoke ARK
    print("[1057] phase 4: sync-invoke ark-holdings…")
    t0 = time.time()
    try:
        result = invoke_sync("justhodl-ark-holdings")
        out["ark_invoke"] = {
            "elapsed_s":      round(time.time() - t0, 1),
            "ok":             result.get("ok"),
            "n_funds":        result.get("n_funds"),
            "n_positions":    result.get("n_positions"),
            "n_unique_tickers": result.get("n_unique_tickers"),
            "n_new":          result.get("n_new_positions"),
            "n_adds":         result.get("n_adds"),
            "n_trims":        result.get("n_trims"),
            "n_closed":       result.get("n_closed"),
            "duration_s":     result.get("duration_s"),
            "err":            result.get("_raw"),
        }
    except Exception as e:
        out["ark_invoke_err"] = str(e)[:200]
    
    time.sleep(2)
    
    # Phase 5: sync-invoke patent-velocity
    print("[1057] phase 5: sync-invoke patent-velocity…")
    t0 = time.time()
    try:
        result = invoke_sync("justhodl-patent-velocity")
        out["patent_invoke"] = {
            "elapsed_s":      round(time.time() - t0, 1),
            "ok":             result.get("ok"),
            "n_results":      result.get("n_results"),
            "n_velocity_spikes": result.get("n_velocity_spikes"),
            "n_new_tech_focus": result.get("n_new_tech_focus"),
            "duration_s":     result.get("duration_s"),
            "err":            result.get("_raw"),
        }
    except Exception as e:
        out["patent_invoke_err"] = str(e)[:200]
    
    time.sleep(2)
    
    # Phase 6: verify S3 outputs
    print("[1057] phase 6: verify S3 outputs…")
    for key, label in [
        ("data/ark-holdings.json",  "ark_snapshot"),
        ("data/patent-velocity.json", "patent_snapshot"),
    ]:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"].read()
            d = json.loads(body)
            snapshot = {
                "size_kb":        round(len(body) / 1024, 1),
                "schema":         d.get("schema_version"),
                "method":         d.get("method"),
                "generated_at":   d.get("generated_at"),
                "duration_s":     d.get("duration_s"),
            }
            if label == "ark_snapshot":
                snapshot["n_funds"] = d.get("n_funds_fetched")
                snapshot["n_positions"] = d.get("n_positions_total")
                snapshot["n_unique"] = d.get("n_unique_tickers")
                snapshot["diff"] = d.get("diff_vs_prev", {})
                # Top 5 cross-fund
                snapshot["top_5_cross_fund"] = [
                    {"ticker": r["ticker"], "n_funds": r["n_funds"],
                     "total_value": r["total_value"]}
                    for r in (d.get("cross_fund_top") or [])[:5]
                ]
            elif label == "patent_snapshot":
                snapshot["universe_size"] = d.get("universe_size")
                snapshot["n_results"] = d.get("n_results")
                snapshot["n_velocity_spikes"] = d.get("n_velocity_spikes")
                snapshot["n_new_tech_focus"] = d.get("n_new_tech_focus")
                # Top 5 spikes
                hl = d.get("highlights", {})
                snapshot["top_5_spikes"] = [
                    {"ticker": r["ticker"], "score": r["score"],
                     "velocity": r["velocity_ratio"],
                     "n_recent": r["n_recent_patents"],
                     "n_baseline": r["n_baseline_patents"],
                     "new_cpcs": r.get("new_cpcs") or []}
                    for r in (hl.get("velocity_spikes") or [])[:5]
                ]
            out[label] = snapshot
        except Exception as e:
            out[label + "_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1057] DONE → {REPORT}")


if __name__ == "__main__":
    main()
