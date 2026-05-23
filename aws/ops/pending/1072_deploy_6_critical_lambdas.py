"""
ops 1072 — direct-deploy 6 critical Tier-0/1/2 Lambdas via boto3.

Why direct-deploy instead of deploy-lambdas.yml:
  deploy-lambdas.yml's path filter does git-diff (HEAD^..HEAD) for changed
  Lambdas. Brand-new Lambda dirs created in same commit as the workflow file
  reference are NOT picked up reliably. Pattern established in ops 1063.

For each Lambda:
  1. Read aws/lambdas/<name>/source/lambda_function.py → zip in-memory
  2. Read aws/lambdas/<name>/config.json
  3. For fed-nlp + news-wire: harvest ANTHROPIC_API_KEY from existing
     justhodl-ai-chat env (institutional pattern — don't duplicate secrets).
  4. lambda.create_function (or update if exists)
  5. events.put_rule + put_targets + add_permission

Returns per-Lambda status; report at aws/ops/reports/1072.json.
"""
import json, os, sys, io, zipfile, traceback
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
# Runs from $GITHUB_WORKSPACE (the repo checkout root) on the runner.
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

LAMBDAS_TO_DEPLOY = [
    "justhodl-dr-snapshot",
    "justhodl-cost-anomaly",
    "justhodl-macro-calendar",
    "justhodl-fed-nlp",
    "justhodl-news-wire",
    "justhodl-concentration-liquidity",
]

# Lambdas that need ANTHROPIC_API_KEY harvested from an existing Lambda
NEEDS_ANTHROPIC = {"justhodl-fed-nlp", "justhodl-news-wire"}
ANTHROPIC_SOURCE_LAMBDA = "justhodl-ai-chat"


def zip_source_dir(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src_dir):
            for f in files:
                full = os.path.join(root, f)
                rel = os.path.relpath(full, src_dir)
                z.write(full, rel)
    buf.seek(0)
    return buf.read()


def harvest_anthropic_key(lam):
    try:
        cfg = lam.get_function_configuration(FunctionName=ANTHROPIC_SOURCE_LAMBDA)
        env = cfg.get("Environment", {}).get("Variables", {})
        key = env.get("ANTHROPIC_API_KEY", "")
        if not key or not key.startswith("sk-"):
            return None, f"Source env missing/invalid (len={len(key)})"
        return key, "OK"
    except Exception as e:
        return None, f"ERR harvesting from {ANTHROPIC_SOURCE_LAMBDA}: {e}"


def deploy_one(lam, events, fn_name, anthropic_key):
    out = {"name": fn_name}
    try:
        lambda_dir = os.path.join(REPO_ROOT, "aws", "lambdas", fn_name)
        src_dir = os.path.join(lambda_dir, "source")
        cfg_path = os.path.join(lambda_dir, "config.json")

        if not os.path.isdir(src_dir):
            out["status"] = "ERR"; out["error"] = f"source dir missing: {src_dir}"
            return out
        if not os.path.isfile(cfg_path):
            out["status"] = "ERR"; out["error"] = f"config missing: {cfg_path}"
            return out

        with open(cfg_path) as f:
            cfg = json.load(f)
        env_vars = dict(cfg.get("env", {}))

        # Inject anthropic key if placeholder
        if fn_name in NEEDS_ANTHROPIC:
            if anthropic_key:
                env_vars["ANTHROPIC_API_KEY"] = anthropic_key
            else:
                out["status"] = "ERR"
                out["error"] = "ANTHROPIC_API_KEY harvest failed; refusing to deploy with placeholder"
                return out

        zip_bytes = zip_source_dir(src_dir)
        out["zip_kb"] = round(len(zip_bytes) / 1024, 1)

        # Try create_function first; fall back to update if exists.
        common = dict(
            FunctionName=fn_name,
            Runtime=cfg.get("runtime", "python3.12"),
            Role=cfg["role"],
            Handler=cfg.get("handler", "lambda_function.lambda_handler"),
            Description=cfg.get("description", "")[:255],
            Timeout=int(cfg.get("timeout", 300)),
            MemorySize=int(cfg.get("memory", 512)),
            Environment={"Variables": env_vars},
            Architectures=cfg.get("architectures", ["x86_64"]),
        )
        try:
            r = lam.create_function(Code={"ZipFile": zip_bytes}, **common)
            out["create"] = "CREATED"
            out["arn"] = r["FunctionArn"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                # already exists → update both code + config
                lam.update_function_code(FunctionName=fn_name, ZipFile=zip_bytes)
                # update_function_configuration doesn't accept Code/Architectures
                cfg_update = {k: v for k, v in common.items() if k not in ("Architectures",)}
                lam.update_function_configuration(**cfg_update)
                out["create"] = "UPDATED_EXISTING"
                out["arn"] = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fn_name}"
            else:
                raise

        # Wait until active before scheduling
        waiter = lam.get_waiter("function_active_v2")
        waiter.wait(FunctionName=fn_name, WaiterConfig={"Delay": 2, "MaxAttempts": 30})

        # EventBridge rule + target
        sched = cfg.get("schedule")
        if sched:
            rule_name = sched["rule_name"]
            schedule_expr = sched["cron"]
            events.put_rule(
                Name=rule_name,
                ScheduleExpression=schedule_expr,
                State="ENABLED",
                Description=sched.get("description", "")[:512],
            )
            events.put_targets(
                Rule=rule_name,
                Targets=[{"Id": "1", "Arn": out["arn"]}],
            )
            # Permission for EB to invoke
            try:
                lam.add_permission(
                    FunctionName=fn_name,
                    StatementId=f"AllowEventBridge-{rule_name}",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}",
                )
                out["permission"] = "ADDED"
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceConflictException":
                    out["permission"] = "EXISTS"
                else:
                    raise
            out["schedule"] = f"{rule_name} ({schedule_expr})"

        out["status"] = "OK"
        return out
    except Exception as e:
        out["status"] = "ERR"
        out["error"] = str(e)[:300]
        out["trace"] = traceback.format_exc()[-500:]
        return out


def main():
    started = datetime.now(timezone.utc).isoformat()
    lam = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)

    anthropic_key, anthropic_msg = harvest_anthropic_key(lam)
    report = {
        "started_at": started,
        "anthropic_harvest": anthropic_msg,
        "anthropic_key_found": bool(anthropic_key),
        "deployments": [],
    }

    for name in LAMBDAS_TO_DEPLOY:
        r = deploy_one(lam, events, name, anthropic_key)
        report["deployments"].append(r)
        print(f"[{r.get('status')}] {name}: {r.get('create','-')} | sched={r.get('schedule','-')}")

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    report["summary"] = {
        "ok": sum(1 for d in report["deployments"] if d.get("status") == "OK"),
        "err": sum(1 for d in report["deployments"] if d.get("status") == "ERR"),
    }
    out_path = os.path.join(REPO_ROOT, "aws", "ops", "reports", "1072.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nREPORT: {out_path}")
    print(f"OK={report['summary']['ok']} ERR={report['summary']['err']}")


if __name__ == "__main__":
    main()
