#!/usr/bin/env python3
"""Step 1005 — Final three-gap close-out.

Runs in the GH Actions runner context, which means boto3 calls use the
deploy-role creds (AWS_ACCESS_KEY_ID secret) — these have CreateFunction
+ PassRole, unlike the lambda-execution-role used by my prior ops attempts.

WHAT THIS DOES
──────────────
1. Snapshot state of all 3 Lambdas (magnitude / miss / compass).
2. For any Lambda that doesn't exist, build a zip from the source/ dir,
   call lambda:CreateFunction with the proper exec role, set up the
   EventBridge schedule + invoke permission, invoke once.
3. For any Lambda that exists but is broken, refresh code + invoke.
4. Verify S3 outputs land for each.
5. Final consolidated report.

This is the deterministic close-out — no more silent-fail loops.
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1005_three_gap_final.json"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def load_config(name: str) -> dict:
    return json.loads(pathlib.Path(f"aws/lambdas/{name}/config.json").read_text())


def build_source_zip(name: str) -> bytes:
    """Zip every .py file in source/ — flat layout, no nested paths."""
    src_dir = pathlib.Path(f"aws/lambdas/{name}/source")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src_dir.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    return buf.getvalue()


def function_exists(name: str) -> dict:
    try:
        meta = lam.get_function(FunctionName=name)
        cfg = meta["Configuration"]
        return {"exists": True, "state": cfg.get("State"),
                "modified": cfg.get("LastModified"),
                "code_size": cfg.get("CodeSize")}
    except lam.exceptions.ResourceNotFoundException:
        return {"exists": False}
    except Exception as e:
        return {"exists": False, "err": str(e)[:200]}


def create_function(cfg: dict, zip_bytes: bytes) -> dict:
    fn = cfg["function_name"]
    desc = (cfg.get("description") or "")[:240]
    args = dict(
        FunctionName = fn,
        Runtime      = cfg.get("runtime", "python3.12"),
        Handler      = cfg.get("handler", "lambda_function.lambda_handler"),
        Role         = cfg.get("role", ROLE_ARN),
        Description  = desc,
        Timeout      = cfg.get("timeout", 60),
        MemorySize   = cfg.get("memory", 256),
        Code         = {"ZipFile": zip_bytes},
        Architectures = cfg.get("architectures", ["x86_64"]),
        Publish      = False,
    )
    lam.create_function(**args)
    lam.get_waiter("function_active_v2").wait(FunctionName=fn)
    return {"action": "created"}


def update_function(cfg: dict, zip_bytes: bytes) -> dict:
    fn = cfg["function_name"]
    desc = (cfg.get("description") or "")[:240]
    lam.update_function_code(FunctionName=fn, ZipFile=zip_bytes, Publish=False)
    lam.get_waiter("function_updated").wait(FunctionName=fn)
    lam.update_function_configuration(
        FunctionName=fn,
        Runtime=cfg.get("runtime", "python3.12"),
        Handler=cfg.get("handler", "lambda_function.lambda_handler"),
        Role=cfg.get("role", ROLE_ARN),
        Description=desc,
        Timeout=cfg.get("timeout", 60),
        MemorySize=cfg.get("memory", 256),
    )
    lam.get_waiter("function_updated").wait(FunctionName=fn)
    return {"action": "updated"}


def ensure_schedule(cfg: dict) -> dict:
    fn = cfg["function_name"]
    sched = cfg.get("schedule")
    if not sched:
        return {"scheduled": False}
    rule = sched["rule_name"]
    events.put_rule(Name=rule, ScheduleExpression=sched["cron"],
                     State="ENABLED",
                     Description=sched.get("description", "")[:240])
    arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fn}"
    events.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": arn}])
    sid = f"EventBridge-{rule}"
    try:
        lam.remove_permission(FunctionName=fn, StatementId=sid)
    except Exception:
        pass
    lam.add_permission(
        FunctionName=fn, StatementId=sid,
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule}",
    )
    return {"scheduled": True, "rule": rule, "cron": sched["cron"]}


def invoke_once(fn: str) -> dict:
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
        return {"fail": str(e)[:300]}


def s3_check(key: str) -> dict:
    try:
        obj = s3.head_object(Bucket=BUCKET, Key=key)
        return {"size": obj["ContentLength"], "modified": str(obj["LastModified"])}
    except Exception as e:
        return {"missing": str(e)[:80]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": {}}
    targets = [
        "justhodl-magnitude-distributions",
        "justhodl-miss-detector",
        "justhodl-alpha-compass",
    ]
    for name in targets:
        rec = {}
        try:
            cfg = load_config(name)
        except Exception as e:
            rec["error"] = f"could not load config: {e}"
            out["lambdas"][name] = rec
            continue
        state = function_exists(name)
        rec["state_before"] = state
        try:
            zb = build_source_zip(name)
            rec["zip_size"] = len(zb)
            if state.get("exists"):
                rec["op"] = update_function(cfg, zb)
            else:
                rec["op"] = create_function(cfg, zb)
            time.sleep(2)
            rec["schedule"] = ensure_schedule(cfg)
            time.sleep(1)
            rec["invoke"] = invoke_once(name)
            rec["state_after"] = function_exists(name)
        except Exception as e:
            rec["error"] = f"{type(e).__name__}: {str(e)[:300]}"
        out["lambdas"][name] = rec

    out["s3"] = {
        "data/magnitude-distributions.json": s3_check("data/magnitude-distributions.json"),
        "data/alpha-compass.json":            s3_check("data/alpha-compass.json"),
        "data/miss-summary.json":             s3_check("data/miss-summary.json"),
    }
    out["finished"] = datetime.now(timezone.utc).isoformat()

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(f"[1005] wrote {REPORT}")


if __name__ == "__main__":
    main()
