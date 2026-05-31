#!/usr/bin/env python3
"""Step 1014 — Deploy near-miss-monitor + run full chain end-to-end.

CHAIN:
  1. engine-signal-map (CREATE/UPDATE) — picks up 43 new known signal_types
  2. near-miss-monitor (NEW) — extracts near-misses from engine snapshots
  3. miss-detector — folds near-misses into miss-summary
  4. miss-calibrator — generates real proposals (not 0 like before)
  5. alpha-compass — sees enriched data
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1014_near_miss_chain.json"
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


def ensure(cfg, zb):
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
                lam.update_function_code(FunctionName=fn, ZipFile=zb, Publish=False)
                lam.get_waiter("function_updated").wait(FunctionName=fn)
                lam.update_function_configuration(FunctionName=fn, **args)
                lam.get_waiter("function_updated").wait(FunctionName=fn)
                return {"action": "updated"}
            except Exception as e:
                if "ResourceConflictException" in str(e) and attempt < 3:
                    time.sleep(5 * (attempt + 1))
                    continue
                raise
    else:
        lam.create_function(
            FunctionName=fn, **args,
            Code={"ZipFile": zb},
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
                out["raw"] = body[:600]
            return out
        except Exception as e:
            if "ResourceConflictException" in str(e) and attempt < 3:
                time.sleep(5 * (attempt + 1))
                continue
            return {"fail": f"{type(e).__name__}: {str(e)[:300]}"}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": {}}

    # ─── Deploy in dependency order ──────────────────────────────────────
    deploy_order = [
        "justhodl-engine-signal-map",   # has 43 new signals — update
        "justhodl-near-miss-monitor",    # new
        "justhodl-miss-detector",        # update — now reads near-misses
    ]
    for name in deploy_order:
        rec = {}
        try:
            cfg = load_config(name)
            zb = build_zip(name)
            rec["zip_size"] = len(zb)
            rec["op"] = ensure(cfg, zb)
            time.sleep(2)
            rec["schedule"] = ensure_schedule(cfg)
        except Exception as e:
            rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
        out["lambdas"][name] = rec

    # ─── Invoke chain in producer→consumer order ──────────────────────────
    print("[1014] invoke engine-signal-map…")
    time.sleep(3)
    out["lambdas"]["justhodl-engine-signal-map"]["invoke"] = invoke_once("justhodl-engine-signal-map")
    
    print("[1014] invoke near-miss-monitor (the NEW link)…")
    time.sleep(3)
    out["lambdas"]["justhodl-near-miss-monitor"] = {
        **out["lambdas"]["justhodl-near-miss-monitor"],
        "invoke": invoke_once("justhodl-near-miss-monitor"),
    }
    
    print("[1014] invoke miss-detector (folds near-misses)…")
    time.sleep(3)
    out["lambdas"]["justhodl-miss-detector"]["invoke"] = invoke_once("justhodl-miss-detector")
    
    print("[1014] invoke miss-calibrator (now sees populated near_misses_by_signal)…")
    time.sleep(3)
    out["miss_calibrator"] = {"invoke": invoke_once("justhodl-miss-calibrator")}
    
    print("[1014] invoke alpha-compass (richer match coverage)…")
    time.sleep(3)
    out["alpha_compass"] = {"invoke": invoke_once("justhodl-alpha-compass")}

    # ─── S3 + content snapshots ───────────────────────────────────────────
    out["s3"] = {}
    for k in ("data/engine-signal-map.json",
              "data/near-misses-by-signal.json",
              "data/miss-summary.json",
              "data/miss-calibrator-proposals.json",
              "data/alpha-compass.json"):
        try:
            obj = s3.head_object(Bucket=BUCKET, Key=k)
            out["s3"][k] = {"size": obj["ContentLength"], "modified": str(obj["LastModified"])}
        except Exception as e:
            out["s3"][k] = {"missing": str(e)[:80]}
    
    # engine-signal-map: how many unknowns left after expansion?
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/engine-signal-map.json")["Body"].read())
        out["engine_map"] = {
            "totals": d.get("totals"),
            "unknown_remaining": len(d.get("unknown_signal_types", [])),
            "top_unknowns_remaining": d.get("unknown_signal_types", [])[:10],
        }
    except Exception as e:
        out["engine_map_err"] = str(e)[:200]
    
    # near-miss output content
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/near-misses-by-signal.json")["Body"].read())
        out["near_miss_payload"] = {
            "totals": d.get("totals"),
            "near_misses_by_signal": d.get("near_misses_by_signal"),
            "diagnostics_sample": (d.get("diagnostics") or [])[:8],
        }
    except Exception as e:
        out["near_miss_err"] = str(e)[:200]
    
    # miss-summary after near-miss fold
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/miss-summary.json")["Body"].read())
        out["miss_summary_after"] = {
            "totals": d.get("totals"),
            "n_near_miss_signals": len(d.get("near_misses_by_signal", {})),
            "near_misses_by_signal": d.get("near_misses_by_signal"),
            "near_miss_monitor_meta": d.get("near_miss_monitor"),
        }
    except Exception as e:
        out["miss_summary_err"] = str(e)[:200]
    
    # miss-calibrator real proposals
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/miss-calibrator-proposals.json")["Body"].read())
        out["miss_calibrator_after"] = {
            "totals": d.get("totals"),
            "n_proposals": len(d.get("proposals", [])),
            "proposals": d.get("proposals", [])[:5],
        }
    except Exception as e:
        out["miss_calibrator_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
