#!/usr/bin/env python3
"""Step 1012 — Deploy + invoke all 4 components of the next-level build.

  CREATE:  justhodl-engine-signal-map  (new)
  CREATE:  justhodl-miss-calibrator    (new)
  UPDATE:  justhodl-miss-detector      (universe-aware classifier)
  UPDATE:  justhodl-alpha-compass      (uses engine-signal-map)

Then invoke each in dependency order:
  engine-signal-map → magdist already published → miss-detector → 
  alpha-compass → miss-calibrator (reads miss-summary, may have empty
  near_misses_by_signal until miss-detector runs more cycles)
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1012_next_level_deploy.json"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

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


def ensure_function(cfg, zip_bytes):
    fn = cfg["function_name"]
    desc = (cfg.get("description") or "")[:240]
    common = dict(
        Runtime=cfg.get("runtime", "python3.12"),
        Handler=cfg.get("handler", "lambda_function.lambda_handler"),
        Role=cfg.get("role", ROLE_ARN),
        Description=desc,
        Timeout=cfg.get("timeout", 60),
        MemorySize=cfg.get("memory", 256),
    )
    if function_exists(fn):
        lam.update_function_code(FunctionName=fn, ZipFile=zip_bytes, Publish=False)
        lam.get_waiter("function_updated").wait(FunctionName=fn)
        lam.update_function_configuration(FunctionName=fn, **common)
        lam.get_waiter("function_updated").wait(FunctionName=fn)
        return {"action": "updated"}
    else:
        lam.create_function(
            FunctionName=fn, **common,
            Code={"ZipFile": zip_bytes},
            Architectures=cfg.get("architectures", ["x86_64"]),
            Publish=False,
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=fn)
        return {"action": "created"}


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


def invoke_once(fn, retry_on_conflict=3):
    """Invoke with retry on ResourceConflictException (concurrent update)."""
    for attempt in range(retry_on_conflict + 1):
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
            if "ResourceConflictException" in str(e) and attempt < retry_on_conflict:
                time.sleep(5 * (attempt + 1))
                continue
            return {"fail": f"{type(e).__name__}: {str(e)[:300]}"}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": {}}

    # Deploy in dependency order: producer first, then consumers
    deploy_order = [
        "justhodl-engine-signal-map",   # producer for alpha-compass
        "justhodl-miss-detector",        # uses universe — needs latest code
        "justhodl-miss-calibrator",      # reads miss-summary
        "justhodl-alpha-compass",        # consumer of engine-signal-map
    ]
    for name in deploy_order:
        rec = {}
        try:
            cfg = load_config(name)
            zb = build_zip(name)
            rec["zip_size"] = len(zb)
            rec["op"] = ensure_function(cfg, zb)
            time.sleep(2)
            rec["schedule"] = ensure_schedule(cfg)
        except Exception as e:
            rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
        out["lambdas"][name] = rec

    # Now invoke each in dependency order, with the producer first
    # so consumers see fresh outputs
    print("[1012] invoking engine-signal-map…")
    time.sleep(3)
    out["lambdas"]["justhodl-engine-signal-map"]["invoke"] = invoke_once("justhodl-engine-signal-map")
    
    print("[1012] invoking miss-detector (after universe-aware classifier landed)…")
    time.sleep(3)
    out["lambdas"]["justhodl-miss-detector"]["invoke"] = invoke_once("justhodl-miss-detector")
    
    print("[1012] invoking miss-calibrator (reads now-fresh miss-summary)…")
    time.sleep(3)
    out["lambdas"]["justhodl-miss-calibrator"]["invoke"] = invoke_once("justhodl-miss-calibrator")
    
    print("[1012] invoking alpha-compass (consumes engine-signal-map)…")
    time.sleep(3)
    out["lambdas"]["justhodl-alpha-compass"]["invoke"] = invoke_once("justhodl-alpha-compass")

    # S3 verification + content peek
    out["s3"] = {}
    for k in ("data/engine-signal-map.json",
              "data/miss-summary.json",
              "data/miss-calibrator-proposals.json",
              "data/alpha-compass.json"):
        try:
            obj = s3.head_object(Bucket=BUCKET, Key=k)
            out["s3"][k] = {"size": obj["ContentLength"], "modified": str(obj["LastModified"])}
        except Exception as e:
            out["s3"][k] = {"missing": str(e)[:80]}
    
    # Content snapshots
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/engine-signal-map.json")
        m = json.loads(obj["Body"].read().decode())
        out["engine_map_content"] = {
            "totals": m.get("totals"),
            "families": list(m.get("by_family", {}).keys()),
            "n_unknown": len(m.get("unknown_signal_types", [])),
            "sample_family": list(m.get("by_family", {}).items())[:2] if m.get("by_family") else [],
        }
    except Exception as e:
        out["engine_map_err"] = str(e)[:200]
    
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/miss-calibrator-proposals.json")
        p = json.loads(obj["Body"].read().decode())
        out["miss_calibrator_content"] = {
            "totals": p.get("totals"),
            "n_proposals": len(p.get("proposals", [])),
            "n_universe_candidates": len(p.get("universe_candidates", [])),
            "top_3_proposals": p.get("proposals", [])[:3],
        }
    except Exception as e:
        out["miss_calibrator_err"] = str(e)[:200]
    
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/miss-summary.json")
        ms = json.loads(obj["Body"].read().decode())
        out["miss_summary_content"] = {
            "totals": ms.get("totals"),
            "n_near_miss_signals": len(ms.get("near_misses_by_signal", {})),
            "n_recurring_tickers": len(ms.get("top_recurring_tickers", {})),
        }
    except Exception as e:
        out["miss_summary_err"] = str(e)[:200]
    
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/alpha-compass.json")
        c = json.loads(obj["Body"].read().decode())
        # Count cards that now have distribution matches
        with_dist = sum(1 for card in c.get("top_calls", []) + c.get("watchlist", [])
                          if card.get("distribution"))
        out["alpha_compass_content"] = {
            "top_calls":    len(c.get("top_calls", [])),
            "watchlist":    len(c.get("watchlist", [])),
            "cards_with_distribution": with_dist,
            "feeds":        {k: v.get("present") for k, v in (c.get("source_feeds") or {}).items()},
            "first_match_via": (c.get("top_calls", [{}])[0].get("distribution") or {}).get("_match_via"),
        }
    except Exception as e:
        out["alpha_compass_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
