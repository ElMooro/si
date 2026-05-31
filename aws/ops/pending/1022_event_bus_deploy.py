#!/usr/bin/env python3
"""Step 1022 — Event-bus infrastructure + end-to-end coordination test.

DEPLOYS:
  1. EventBridge custom bus:  justhodl-system-events
  2. EventBridge rule:        match source=justhodl.* on the bus
                                target → justhodl-event-coordinator
  3. lambda:InvokeFunction permission for events.amazonaws.com on coordinator
  4. justhodl-event-coordinator (NEW Lambda — receives all events, routes)
  5. justhodl-outcome-checker  (UPDATED — emits outcome.resolved)
  6. justhodl-miss-calibrator  (UPDATED — emits high-conf proposals + extreme near-miss)

THEN TESTS THE END-TO-END FLOW:
  - Invoke outcome-checker → it should call publish(outcome.resolved)
    → bus delivers to coordinator → coordinator invokes calibrator + alpha-calibrator
  - Invoke miss-calibrator → it should publish high-conf proposals
    → coordinator should send Telegram alerts

  - Read the audit log at system-events/audit/{today}.jsonl to verify
    events were received and routed.

This is the FIRST end-to-end event-driven coordination link in the system.
After this, adding more event emissions in other engines is just one
import + one publish() call per engine.
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1022_event_bus_deploy.json"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

EVENT_BUS_NAME = "justhodl-system-events"
RULE_NAME      = "justhodl-events-to-coordinator"
COORDINATOR_FN = "justhodl-event-coordinator"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


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


def ensure_event_bus():
    """Create the custom EventBridge bus if it doesn't exist."""
    try:
        events.describe_event_bus(Name=EVENT_BUS_NAME)
        return {"action": "exists"}
    except events.exceptions.ResourceNotFoundException:
        events.create_event_bus(Name=EVENT_BUS_NAME)
        return {"action": "created"}
    except Exception as e:
        return {"err": f"{type(e).__name__}: {str(e)[:200]}"}


def ensure_event_rule():
    """Create the rule that catches all justhodl.* sources on the bus
    and routes them to the coordinator Lambda."""
    pattern = json.dumps({
        "source": [{"prefix": "justhodl."}]
    })
    
    try:
        events.put_rule(
            Name=RULE_NAME,
            EventBusName=EVENT_BUS_NAME,
            EventPattern=pattern,
            State="ENABLED",
            Description="Routes all justhodl.* events to event-coordinator Lambda"[:240],
        )
        
        coord_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{COORDINATOR_FN}"
        events.put_targets(
            Rule=RULE_NAME,
            EventBusName=EVENT_BUS_NAME,
            Targets=[{
                "Id":  "1",
                "Arn": coord_arn,
            }],
        )
        return {"action": "ensured", "pattern": pattern}
    except Exception as e:
        return {"err": f"{type(e).__name__}: {str(e)[:200]}"}


def ensure_invoke_permission():
    """Grant EventBridge permission to invoke the coordinator Lambda."""
    sid = "EventBridge-from-justhodl-bus"
    try:
        # Remove any stale permission first
        try:
            lam.remove_permission(FunctionName=COORDINATOR_FN, StatementId=sid)
        except Exception:
            pass
        
        rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{EVENT_BUS_NAME}/{RULE_NAME}"
        lam.add_permission(
            FunctionName=COORDINATOR_FN,
            StatementId=sid,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
        return {"action": "added", "rule_arn": rule_arn}
    except Exception as e:
        return {"err": f"{type(e).__name__}: {str(e)[:200]}"}


def invoke_once(fn):
    for attempt in range(4):
        try:
            r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
            body = r["Payload"].read().decode("utf-8", errors="replace")
            out = {"status": r.get("StatusCode"), "fn_err": r.get("FunctionError")}
            try:
                p = json.loads(body)
                out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
            except Exception:
                out["raw"] = body[:500]
            return out
        except Exception as e:
            if "ResourceConflict" in str(e) and attempt < 3:
                time.sleep(5 * (attempt + 1))
                continue
            return {"fail": f"{type(e).__name__}: {str(e)[:300]}"}


def read_audit_log_today():
    """Pull today's coordinator audit log from S3."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"system-events/audit/{today}.jsonl"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        body = obj["Body"].read().decode("utf-8")
        lines = [l for l in body.split("\n") if l.strip()]
        return {
            "key": key,
            "n_events": len(lines),
            "entries": [json.loads(l) for l in lines[-15:]],   # last 15
        }
    except s3.exceptions.NoSuchKey:
        return {"missing": True, "key": key}
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # ─── Phase 1: deploy infrastructure ──────────────────────────────────
    print("[1022] phase 1: deploy 3 Lambdas (coordinator, outcome-checker, miss-calibrator)…")
    deploys = {}
    for name in ("justhodl-event-coordinator",
                  "justhodl-outcome-checker",
                  "justhodl-miss-calibrator"):
        try:
            cfg = load_config(name)
            zb = build_zip(name)
            deploys[name] = {"zip_size": len(zb)}
            deploys[name]["op"] = deploy_lambda(cfg, zb)
            time.sleep(2)
        except Exception as e:
            deploys[name] = {"error": str(e)[:200]}
    out["deploys"] = deploys
    
    # ─── Phase 2: create the event bus + rule + permissions ─────────────
    print("[1022] phase 2: create event bus + rule + permissions…")
    out["bus"]        = ensure_event_bus()
    out["rule"]       = ensure_event_rule()
    out["permission"] = ensure_invoke_permission()
    
    # Wait for EventBridge to propagate the rule
    print("[1022] phase 3: waiting 10s for EventBridge to propagate the rule…")
    time.sleep(10)
    
    # ─── Phase 4: end-to-end test ─────────────────────────────────────────
    print("[1022] phase 4: invoke outcome-checker (should emit outcome.resolved)…")
    out["outcome_checker_invoke"] = invoke_once("justhodl-outcome-checker")
    time.sleep(8)   # allow event to traverse bus → coordinator → downstream
    
    print("[1022] phase 5: invoke miss-calibrator (should emit calibrator proposals)…")
    out["miss_calibrator_invoke"] = invoke_once("justhodl-miss-calibrator")
    time.sleep(8)
    
    # ─── Phase 6: verify audit log ────────────────────────────────────────
    print("[1022] phase 6: read audit log to confirm events flowed end-to-end…")
    out["audit_log"] = read_audit_log_today()
    
    # ─── Phase 7: also test direct event publish to confirm bus is live ──
    print("[1022] phase 7: synthetic event publish — verify bus works directly…")
    try:
        resp = events.put_events(Entries=[{
            "Source":       "justhodl.test",
            "DetailType":   "outcome.resolved",
            "Detail":       json.dumps({
                "n_resolved": 99,
                "source": "ops-1022-synthetic-test",
                "_emitted_at": datetime.now(timezone.utc).isoformat(),
                "_source_engine": "ops-1022",
            }),
            "EventBusName": EVENT_BUS_NAME,
        }])
        out["synthetic_publish"] = {
            "failed_count": resp.get("FailedEntryCount"),
            "entries":      resp.get("Entries"),
        }
        time.sleep(8)
        # Re-read audit to see if synthetic event was routed
        out["audit_log_after_synthetic"] = read_audit_log_today()
    except Exception as e:
        out["synthetic_publish"] = {"err": str(e)[:200]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
