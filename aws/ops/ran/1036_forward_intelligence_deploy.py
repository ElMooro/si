#!/usr/bin/env python3
"""Step 1036 — Deploy 4 new forward-looking engines + integrate into
bagger-engine and master-ranker.

NEW LAMBDAS
═══════════
  1. justhodl-forward-orders     — RPO + contracts + book-to-bill
  2. justhodl-rotation-chain     — value-chain lead-lag detection
  3. justhodl-buzz-velocity      — Reddit + News mention velocity
  4. justhodl-future-intelligence — composite of the above 3

UPDATED LAMBDAS
═══════════════
  - justhodl-event-coordinator (5 new event routes + Telegram formatters)
  - justhodl-master-ranker     (new system: future_intel)
  - justhodl-bagger-engine     (new pillar #8: future_intelligence)

INVOKE ORDER (data-flow respecting)
═══════════════════════════════════
  forward-orders → rotation-chain → buzz-velocity (these 3 are independent)
    → future-intelligence (consumes all 3)
      → master-ranker (consumes future-intelligence)

VERIFICATION
════════════
  Output files exist in S3 + composite has results + event published
"""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1036_forward_intelligence_deploy.json"
REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180, connect_timeout=10))
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


def deploy(cfg, zip_bytes):
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
        for attempt in range(4):
            try:
                lam.update_function_code(FunctionName=fn, ZipFile=zip_bytes, Publish=False)
                lam.get_waiter("function_updated").wait(FunctionName=fn)
                lam.update_function_configuration(FunctionName=fn, **args)
                lam.get_waiter("function_updated").wait(FunctionName=fn)
                return {"action": "updated"}
            except Exception as e:
                if "ResourceConflict" in str(e) and attempt < 3:
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


def invoke_sync(fn, timeout_s=900):
    try:
        # Override client read timeout for long-running engines
        lam_long = boto3.client("lambda", region_name=REGION,
                                  config=Config(read_timeout=timeout_s + 60,
                                                 connect_timeout=10))
        r = lam_long.invoke(FunctionName=fn, InvocationType="RequestResponse",
                             Payload=b"{}")
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


def invoke_async(fn):
    try:
        r = lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
        return {"async": True, "status": r.get("StatusCode")}
    except Exception as e:
        return {"fail": f"{type(e).__name__}: {str(e)[:200]}"}


def s3_object_exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False


def s3_object_summary(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        body = obj["Body"].read()
        data = json.loads(body.decode("utf-8"))
        return {
            "exists":        True,
            "size_kb":       round(len(body) / 1024, 1),
            "generated_at":  data.get("generated_at"),
            "duration_s":    data.get("duration_s"),
            "n_results":     len(data.get("all_results") or
                                  data.get("top_25") or
                                  data.get("top_25_by_score") or
                                  data.get("chains", {}) or []),
            "top_3": (data.get("top_25_by_score") or
                       data.get("top_30") or
                       data.get("top_25") or [])[:3],
        }
    except Exception as e:
        return {"exists": False, "err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # ─── Phase 1: deploy all 7 affected Lambdas ─────────────────────────
    print("[1036] phase 1: deploy 4 new + 3 updated Lambdas…")
    deploy_order = [
        # Updated existing ones first (coordinator routes need to be live before
        # producers fire events; bagger + master-ranker can be done anytime)
        "justhodl-event-coordinator",
        # New engines (must exist before composite can read their outputs)
        "justhodl-forward-orders",
        "justhodl-rotation-chain",
        "justhodl-buzz-velocity",
        "justhodl-future-intelligence",
        # Consumers (integrate the composite)
        "justhodl-master-ranker",
        "justhodl-bagger-engine",
    ]
    out["deploys"] = {}
    for name in deploy_order:
        rec = {}
        try:
            cfg = load_config(name) if pathlib.Path(f"aws/lambdas/{name}/config.json").exists() else {"function_name": name}
            zb = build_zip(name)
            rec["zip_size"] = len(zb)
            rec["op"] = deploy(cfg, zb)
            time.sleep(3)
            if "schedule" in cfg:
                rec["schedule"] = ensure_schedule(cfg)
        except Exception as e:
            rec["error"] = str(e)[:200]
        out["deploys"][name] = rec
    
    # ─── Phase 2: invoke the 3 source engines async (they take 5-15min)─
    # forward-orders touches SEC + NewsAPI for ~250 tickers = SLOW
    # rotation-chain touches FMP for ~70 tickers = MEDIUM
    # buzz-velocity touches Reddit + NewsAPI for ~150 tickers = SLOW
    # We fire all 3 async, sleep, then sync-invoke future-intelligence at the end.
    print("[1036] phase 2: async-invoke the 3 source engines…")
    out["async_invokes"] = {}
    for fn in ("justhodl-forward-orders", "justhodl-rotation-chain", "justhodl-buzz-velocity"):
        out["async_invokes"][fn] = invoke_async(fn)
        time.sleep(2)
    
    # ─── Phase 3: wait for them to complete ─────────────────────────────
    # rotation-chain finishes first (~3-5 min)
    # forward-orders and buzz are 8-12 min each
    # GH Actions has a 6h limit but we want to bound this. Cap at 13min.
    WAIT_MAX_S = 13 * 60
    WAIT_INTERVAL = 30
    print(f"[1036] phase 3: waiting up to {WAIT_MAX_S}s for source engines…")
    
    completion = {
        "data/forward-orders.json":     None,
        "data/rotation-chains.json":    None,
        "data/buzz-velocity.json":      None,
    }
    start_wait = time.time()
    while time.time() - start_wait < WAIT_MAX_S:
        remaining = [k for k, v in completion.items() if v is None]
        if not remaining:
            break
        for key in remaining:
            if s3_object_exists(key):
                completion[key] = time.time() - start_wait
                print(f"[1036]   ✅ {key}  after {completion[key]:.0f}s")
        if all(v is not None for v in completion.values()):
            break
        time.sleep(WAIT_INTERVAL)
    
    out["completion_times_s"] = completion
    
    # ─── Phase 4: now invoke future-intelligence (consumes the 3) ───────
    # Only invoke if at least one source completed
    if any(v is not None for v in completion.values()):
        print("[1036] phase 4: invoke future-intelligence composite…")
        out["future_intel_invoke"] = invoke_sync("justhodl-future-intelligence", timeout_s=120)
    else:
        out["future_intel_invoke"] = {"skipped": "no source data ready"}
    
    # ─── Phase 5: read all 4 output files ───────────────────────────────
    print("[1036] phase 5: read output summaries…")
    out["outputs"] = {}
    for key in ("data/forward-orders.json", "data/rotation-chains.json",
                  "data/buzz-velocity.json", "data/future-intelligence.json"):
        out["outputs"][key] = s3_object_summary(key)
    
    # ─── Phase 6: read audit log for events ─────────────────────────────
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"system-events/audit/{today}.jsonl")
        lines = [l for l in obj["Body"].read().decode().split("\n") if l.strip()]
        entries = [json.loads(l) for l in lines]
        from collections import defaultdict
        by_event = defaultdict(int)
        for e in entries:
            by_event[e.get("event", "?")] += 1
        # Filter to today's new event types
        new_event_types = ["forward_orders.high_conviction", "rotation.next_up",
                            "buzz.spike", "future.signal.high_conviction"]
        out["audit"] = {
            "n_total":             len(entries),
            "by_event":            dict(by_event),
            "new_event_counts":    {k: by_event.get(k, 0) for k in new_event_types},
            "samples":             [{
                "ts": e.get("ts", "")[:19],
                "event": e.get("event"),
                "detail_keys": list((e.get("detail") or {}).keys())[:5],
                "_source": (e.get("detail") or {}).get("_source_engine"),
            } for e in entries[-15:]],
        }
    except Exception as e:
        out["audit_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
