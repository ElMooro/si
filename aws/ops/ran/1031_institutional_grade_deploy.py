#!/usr/bin/env python3
"""Step 1031 — Deploy all 5 institutional-grade additions + verify.

Deploys:
  - justhodl-event-flow-monitor   (NEW) hourly health monitor
  - justhodl-event-coordinator    (UPDATED) — 2 new event routes
  - justhodl-signal-board         (UPDATED) — posture.changed emission
  - justhodl-master-ranker        (UPDATED) — convergence.tier_up emission
  - justhodl-miss-detector        (UPDATED) — miss.detected emission

Then invokes:
  - event-flow-monitor (verify it reads audit log + writes health.json)
  - signal-board (might emit posture.changed if posture flipped)
  - master-ranker (might emit convergence.tier_up if ticker crossed)

Verifies:
  - data/event-flow-health.json appears
  - SSM /justhodl/event-flow/pulse contains status
  - Audit log has new events (if conditions met)
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1031_institutional_grade_deploy.json"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def load_config(name):
    return json.loads(pathlib.Path(f"aws/lambdas/{name}/config.json").read_text())


def build_zip(name):
    src_dir = pathlib.Path(f"aws/lambdas/{name}/source")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src_dir.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    return buf.getvalue()


def function_exists(name):
    try:
        lam.get_function(FunctionName=name)
        return True
    except lam.exceptions.ResourceNotFoundException:
        return False


def deploy_lambda(cfg, zip_bytes, retry=4):
    fn = cfg["function_name"]
    desc = (cfg.get("description") or "")[:240]
    args = dict(
        Runtime=cfg.get("runtime", "python3.12"),
        Handler=cfg.get("handler", "lambda_function.lambda_handler"),
        Role=cfg.get("role", ROLE_ARN),
        Description=desc,
        Timeout=cfg.get("timeout", 60),
        MemorySize=cfg.get("memory", 256),
    )
    
    if function_exists(fn):
        for attempt in range(retry):
            try:
                lam.update_function_code(FunctionName=fn, ZipFile=zip_bytes, Publish=False)
                lam.get_waiter("function_updated").wait(FunctionName=fn)
                lam.update_function_configuration(FunctionName=fn, **args)
                lam.get_waiter("function_updated").wait(FunctionName=fn)
                return {"action": "updated"}
            except Exception as e:
                if "ResourceConflict" in str(e) and attempt < retry - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return {"err": f"{type(e).__name__}: {str(e)[:200]}"}
    else:
        try:
            lam.create_function(
                FunctionName=fn, **args,
                Code={"ZipFile": zip_bytes},
                Architectures=cfg.get("architectures", ["x86_64"]),
                Publish=False,
            )
            lam.get_waiter("function_active_v2").wait(FunctionName=fn)
            return {"action": "created"}
        except Exception as e:
            return {"err": f"{type(e).__name__}: {str(e)[:200]}"}


def ensure_schedule(cfg):
    fn = cfg["function_name"]
    sched = cfg.get("schedule")
    if not sched:
        return {"scheduled": False}
    rule = sched["rule_name"]
    events.put_rule(Name=rule, ScheduleExpression=sched["cron"], State="ENABLED",
                     Description=sched.get("description", "")[:240])
    arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fn}"
    events.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": arn}])
    sid = f"EventBridge-{rule}"
    try: lam.remove_permission(FunctionName=fn, StatementId=sid)
    except Exception: pass
    lam.add_permission(
        FunctionName=fn, StatementId=sid,
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule}",
    )
    return {"scheduled": True, "rule": rule, "cron": sched["cron"]}


def invoke_sync(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out = {"status": r.get("StatusCode"), "fn_err": r.get("FunctionError")}
        try:
            p = json.loads(body)
            out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["raw"] = body[:400]
        return out
    except Exception as e:
        return {"fail": f"{type(e).__name__}: {str(e)[:200]}"}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # ─── Phase 1: deploy 5 Lambdas ──────────────────────────────────────
    print("[1031] phase 1: deploy 5 Lambdas…")
    deploy_order = [
        "justhodl-event-coordinator",    # routes new events
        "justhodl-event-flow-monitor",   # NEW - monitors bus health
        "justhodl-signal-board",         # emits posture.changed
        "justhodl-master-ranker",        # emits convergence.tier_up
        "justhodl-miss-detector",        # emits miss.detected
    ]
    out["deploys"] = {}
    for name in deploy_order:
        rec = {}
        try:
            cfg = load_config(name) if (pathlib.Path(f"aws/lambdas/{name}/config.json")).exists() else {"function_name": name}
            zb = build_zip(name)
            rec["zip_size"] = len(zb)
            rec["op"] = deploy_lambda(cfg, zb)
            time.sleep(2)
            if "schedule" in cfg:
                rec["schedule"] = ensure_schedule(cfg)
        except Exception as e:
            rec["error"] = str(e)[:200]
        out["deploys"][name] = rec
    
    # ─── Phase 2: invoke event-flow-monitor (the new infrastructure) ────
    print("[1031] phase 2: invoke event-flow-monitor…")
    time.sleep(3)
    out["event_flow_monitor_invoke"] = invoke_sync("justhodl-event-flow-monitor")
    time.sleep(5)
    
    # ─── Phase 3: invoke signal-board (might emit posture.changed) ──────
    print("[1031] phase 3: invoke signal-board…")
    out["signal_board_invoke"] = invoke_sync("justhodl-signal-board")
    time.sleep(8)
    
    # ─── Phase 4: invoke master-ranker (might emit convergence.tier_up) ─
    print("[1031] phase 4: invoke master-ranker…")
    out["master_ranker_invoke"] = invoke_sync("justhodl-master-ranker")
    time.sleep(8)
    
    # ─── Phase 5: read event-flow-health output ─────────────────────────
    print("[1031] phase 5: read event-flow-health.json…")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/event-flow-health.json")
        h = json.loads(obj["Body"].read().decode())
        out["event_flow_health"] = {
            "pulse":                h.get("pulse"),
            "totals_today":         h.get("totals_today"),
            "n_anomalies":          len(h.get("anomalies", [])),
            "anomalies":            h.get("anomalies", []),
            "coordinator_health":   h.get("coordinator_health"),
            "engine_errors_summary": {
                "n_total_today":    h.get("engine_errors", {}).get("n_total_today"),
                "n_engines":        h.get("engine_errors", {}).get("n_engines_erroring"),
                "by_engine":        h.get("engine_errors", {}).get("by_engine"),
            },
        }
    except Exception as e:
        out["event_flow_health_err"] = str(e)[:200]
    
    # ─── Phase 6: read SSM pulse ────────────────────────────────────────
    try:
        resp = ssm.get_parameter(Name="/justhodl/event-flow/pulse")
        out["ssm_pulse"] = json.loads(resp["Parameter"]["Value"])
    except Exception as e:
        out["ssm_pulse_err"] = str(e)[:120]
    
    # ─── Phase 7: read audit log to see if new events landed ────────────
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"system-events/audit/{today}.jsonl")
        lines = [l for l in obj["Body"].read().decode().split("\n") if l.strip()]
        out["audit_log"] = {
            "n_events": len(lines),
            "last_5_events": [json.loads(l) for l in lines[-5:]],
        }
    except s3.exceptions.NoSuchKey:
        out["audit_log"] = {"missing": True}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()

# run trigger

# trigger 2
