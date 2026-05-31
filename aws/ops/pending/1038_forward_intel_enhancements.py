#!/usr/bin/env python3
"""Step 1038 — Deploy the 4 forward-intelligence enhancements.

═══════════════════════════════════════════════════════════════════════
THE 4 FIXES/FEATURES (user-requested, one-by-one)
═══════════════════════════════════════════════════════════════════════

1. BUZZ UNIVERSE LEAK FIX
   - buzz-velocity + forward-orders both filter foreign listings
     (LMT.BA Buenos Aires etc.) via exchange=NYSE,NASDAQ + dot-filter
   - Validation: re-invoke buzz-velocity, verify top_30 has NO ticker
     with '.' in symbol

2. COMPOSITE THRESHOLD LOWERING
   - future-intelligence HIGH_CONVICTION_THRESHOLD: 75 → 65 (env-tunable)
   - Validation: re-invoke future-intelligence, verify high_conviction
     highlights bucket is non-empty (we know GEV was 70.4 → should now fire)

3. NEW LAMBDA: justhodl-ticker-trends (Google search velocity)
   - Direct API (no pytrends, no pandas)
   - Heavy rate-limited (8s/req, ~80 tickers/run)
   - Outputs data/ticker-trends.json
   - Validation: deploy → invoke async → wait → check output

4. 4 NEW VALUE CHAINS in rotation-chain
   - BIOTECH, COPPER_SILVER, DATACENTER, LITHIUM (4-tier each)
   - Validation: re-invoke rotation-chain, verify chains count went 7→11

DEPLOY ORDER (respects data dependencies):
  coordinator (new routes for ticker_trends.spike)
  → rotation-chain (4 new chains)
  → buzz-velocity (universe fix)
  → forward-orders (universe fix)
  → ticker-trends (NEW — async-invoke since 11min runtime)
  → future-intelligence (new threshold + 4-signal weights)

After deploys, sync-invoke buzz + rotation + future-intel to validate.
ticker-trends fires async and gets validated lazily on next session
(or we can poll for completion).
"""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1038_forward_intel_enhancements.json"
REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180, connect_timeout=10))
events_c = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def load_config(name):
    p = pathlib.Path(f"aws/lambdas/{name}/config.json")
    return json.loads(p.read_text()) if p.exists() else {"function_name": name}


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
    events_c.put_rule(Name=rule, ScheduleExpression=sched["cron"], State="ENABLED",
                       Description=sched.get("description", "")[:240])
    arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fn}"
    events_c.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": arn}])
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


def read_s3(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(),
           "phase_summaries": {}}
    
    # ═══════════════════════════════════════════════════════════════════
    # PHASE A: DEPLOY ALL 6 AFFECTED LAMBDAS
    # ═══════════════════════════════════════════════════════════════════
    print("[1038] Phase A: deploys")
    deploy_order = [
        "justhodl-event-coordinator",
        "justhodl-rotation-chain",
        "justhodl-buzz-velocity",
        "justhodl-forward-orders",
        "justhodl-ticker-trends",
        "justhodl-future-intelligence",
    ]
    out["deploys"] = {}
    for name in deploy_order:
        rec = {}
        try:
            cfg = load_config(name)
            zb = build_zip(name)
            rec["zip_size"] = len(zb)
            rec["op"] = deploy(cfg, zb)
            time.sleep(3)
            if "schedule" in cfg:
                rec["schedule"] = ensure_schedule(cfg)
        except Exception as e:
            rec["error"] = str(e)[:200]
        out["deploys"][name] = rec
    
    # ═══════════════════════════════════════════════════════════════════
    # PHASE B: FIX #1 VALIDATION — buzz-velocity universe filter
    # ═══════════════════════════════════════════════════════════════════
    print("[1038] Phase B: validate buzz-velocity universe US-only filter")
    out["fix_1_buzz_universe"] = {"async_invoke": invoke_async("justhodl-buzz-velocity")}
    # We'll validate later (buzz takes 4-12 min). Will check buzz-velocity.json
    # for dot-suffixed tickers when results come in.
    
    # ═══════════════════════════════════════════════════════════════════
    # PHASE C: FIX #4 VALIDATION — rotation-chain has 11 chains now
    # ═══════════════════════════════════════════════════════════════════
    print("[1038] Phase C: validate 11 chains in rotation-chain")
    rot_invoke = invoke_sync("justhodl-rotation-chain", timeout_s=300)
    out["fix_4_value_chains"] = {"invoke": rot_invoke}
    
    time.sleep(3)
    rot_data = read_s3("data/rotation-chains.json")
    if rot_data:
        out["fix_4_value_chains"]["chain_names"] = list(
            (rot_data.get("chains") or {}).keys())
        out["fix_4_value_chains"]["n_chains"]    = rot_data.get("n_chains")
        # Sanity-check the 4 new ones are there
        out["fix_4_value_chains"]["new_chains_present"] = {
            name: name in (rot_data.get("chains") or {})
            for name in ["BIOTECH", "COPPER_SILVER", "DATACENTER", "LITHIUM"]
        }
        # And their states
        for new in ["BIOTECH", "COPPER_SILVER", "DATACENTER", "LITHIUM"]:
            c = (rot_data.get("chains") or {}).get(new, {})
            out["fix_4_value_chains"][f"{new}_state"] = {
                "leader_tier":      c.get("current_leader_tier"),
                "leader_perf_30d":  c.get("leader_perf_30d_pct"),
                "next_tier_perf":   c.get("next_tier_perf_30d"),
                "state":            c.get("rotation_state"),
                "n_next_up":        len(c.get("next_up_tickers") or []),
                "top_2_next_up":    [
                    {"t": t["ticker"], "lag": t["lag_pct"]}
                    for t in (c.get("next_up_tickers") or [])[:2]
                ],
            }
    
    # ═══════════════════════════════════════════════════════════════════
    # PHASE D: FIX #3 — async-fire ticker-trends (long-running, validate lazy)
    # ═══════════════════════════════════════════════════════════════════
    print("[1038] Phase D: async-invoke ticker-trends (~11 min, validate lazily)")
    out["fix_3_ticker_trends"] = {
        "async_invoke": invoke_async("justhodl-ticker-trends"),
        "note": "Output expected in ~11 min; checked at end of script.",
    }
    
    # ═══════════════════════════════════════════════════════════════════
    # PHASE E: wait for buzz to complete, then validate fix #1
    # ═══════════════════════════════════════════════════════════════════
    print("[1038] Phase E: poll for buzz-velocity completion (max 12 min)…")
    buzz_started = time.time()
    buzz_data = None
    last_check_ts = None
    while time.time() - buzz_started < 720:
        time.sleep(45)
        b = read_s3("data/buzz-velocity.json")
        if b and b.get("generated_at"):
            # Compare to start of session
            gen = b["generated_at"]
            if gen != last_check_ts:
                last_check_ts = gen
                gen_dt = datetime.fromisoformat(gen.replace("Z", "+00:00"))
                if gen_dt > datetime.fromisoformat(out["started"]):
                    buzz_data = b
                    print(f"[1038]   buzz-velocity completed at {gen}")
                    break
    
    if buzz_data:
        all_tickers = [r.get("ticker", "") for r in (buzz_data.get("all_results") or [])]
        dot_tickers = [t for t in all_tickers if "." in t]
        out["fix_1_buzz_universe"]["completed"] = True
        out["fix_1_buzz_universe"]["total_tickers"]      = len(all_tickers)
        out["fix_1_buzz_universe"]["dot_suffix_tickers"] = dot_tickers[:10]
        out["fix_1_buzz_universe"]["passed"]             = (len(dot_tickers) == 0)
    else:
        out["fix_1_buzz_universe"]["completed"] = False
        out["fix_1_buzz_universe"]["note"]      = "Buzz still running; check next session"
    
    # ═══════════════════════════════════════════════════════════════════
    # PHASE F: re-invoke future-intelligence (will pick up new buzz + 65 threshold)
    # ═══════════════════════════════════════════════════════════════════
    print("[1038] Phase F: invoke future-intelligence with new threshold + buzz data")
    fi_invoke = invoke_sync("justhodl-future-intelligence", timeout_s=120)
    out["fix_2_threshold"] = {"invoke": fi_invoke}
    
    time.sleep(3)
    fi_data = read_s3("data/future-intelligence.json")
    if fi_data:
        hc = fi_data.get("highlights", {}).get("high_conviction") or []
        out["fix_2_threshold"]["n_high_conviction"] = len(hc)
        out["fix_2_threshold"]["high_conviction_picks"] = [
            {"ticker": r["ticker"], "score": r["future_intel_score"],
             "n_signals": r.get("n_independent_signals"),
             "thesis": r.get("thesis", "")[:120]}
            for r in hc[:8]
        ]
        out["fix_2_threshold"]["top_10_overall"] = [
            {"ticker": r["ticker"], "score": r["future_intel_score"],
             "subscores": r.get("subscores")}
            for r in (fi_data.get("top_25") or [])[:10]
        ]
        out["fix_2_threshold"]["passed"] = (len(hc) >= 1)
        # Check feed_freshness — did it include ticker_trends?
        ff = fi_data.get("feed_freshness", {})
        out["fix_2_threshold"]["feed_freshness"] = ff
    
    # ═══════════════════════════════════════════════════════════════════
    # PHASE G: check audit log for new events
    # ═══════════════════════════════════════════════════════════════════
    print("[1038] Phase G: read audit log")
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
        # Filter to new event types from this session
        new_types = ["future.signal.high_conviction", "ticker_trends.spike",
                      "rotation.next_up", "forward_orders.high_conviction"]
        out["new_events_count"] = {t: by_event.get(t, 0) for t in new_types}
    except Exception as e:
        out["audit_err"] = str(e)[:200]
    
    # ═══════════════════════════════════════════════════════════════════
    # PHASE H: belated check on ticker-trends
    # ═══════════════════════════════════════════════════════════════════
    print("[1038] Phase H: check if ticker-trends completed")
    tt_data = read_s3("data/ticker-trends.json")
    if tt_data and tt_data.get("generated_at"):
        gen_dt = datetime.fromisoformat(
            tt_data["generated_at"].replace("Z", "+00:00")
            if "Z" in tt_data["generated_at"] else tt_data["generated_at"])
        if gen_dt > datetime.fromisoformat(out["started"]):
            out["fix_3_ticker_trends"]["completed"] = True
            out["fix_3_ticker_trends"]["n_ok"] = tt_data.get("n_ok")
            out["fix_3_ticker_trends"]["errors"] = tt_data.get("errors")
            out["fix_3_ticker_trends"]["top_5"] = [
                {"ticker": r["ticker"], "velocity": r["velocity"],
                 "score": r["score"], "stealth": r["stealth"]}
                for r in (tt_data.get("top_20") or [])[:5]
            ]
        else:
            out["fix_3_ticker_trends"]["completed"] = False
            out["fix_3_ticker_trends"]["note"] = "Old output present; new run not yet complete"
    else:
        out["fix_3_ticker_trends"]["completed"] = False
        out["fix_3_ticker_trends"]["note"] = "No output file yet (still running or failed)"
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
