"""
ops 1079 — deploy + verify justhodl-forward-returns (Capital Compass).

Pattern from ops 1074: deploy-lambdas.yml may create the Lambda from
aws/lambdas/justhodl-forward-returns/ on the same push. We:
  1. Wait for the function to exist + be idle
  2. Ensure env vars + memory + timeout match config.json
  3. Wire EventBridge schedule + target + permission
  4. Invoke once to materialize data/forward-returns.json
  5. Read it back and report headline numbers
"""
import json, os, sys, io, zipfile, base64, time, traceback
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-forward-returns"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/forward-returns.json"


def zip_source(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src_dir):
            for f in files:
                full = os.path.join(root, f)
                z.write(full, os.path.relpath(full, src_dir))
    return buf.getvalue()


def wait_for_idle(lam, fn, max_wait=120):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") in ("Successful", None):
                return cfg
            if cfg.get("LastUpdateStatus") == "Failed":
                print(f"  Lambda update FAILED: {cfg.get('LastUpdateStatusReason')}")
                return None
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                pass  # not created yet
            else:
                raise
        time.sleep(3)
    return None


def deploy(lam, events, s3):
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "fn": FN}

    cfg_path = os.path.join(REPO_ROOT, "aws", "lambdas", FN, "config.json")
    src_dir = os.path.join(REPO_ROOT, "aws", "lambdas", FN, "source")

    with open(cfg_path) as f:
        cfg = json.load(f)

    # Phase 1 — wait for deploy-lambdas.yml to have created it, or create it ourselves
    print("Phase 1: ensuring Lambda exists...")
    existing = wait_for_idle(lam, FN, max_wait=60)
    if existing:
        report["initial_state"] = "EXISTS"
        report["initial_code_sha"] = existing.get("CodeSha256", "")[:12]
    else:
        # Need to create
        print(f"  Function {FN} not found — creating fresh.")
        try:
            zip_bytes = zip_source(src_dir)
            common = dict(
                FunctionName=FN,
                Runtime=cfg.get("runtime", "python3.12"),
                Role=cfg["role"],
                Handler=cfg.get("handler", "lambda_function.lambda_handler"),
                Description=cfg.get("description", "")[:255],
                Timeout=int(cfg.get("timeout", 300)),
                MemorySize=int(cfg.get("memory", 1024)),
                Environment={"Variables": cfg.get("env", {})},
                Architectures=cfg.get("architectures", ["x86_64"]),
            )
            r = lam.create_function(Code={"ZipFile": zip_bytes}, **common)
            report["create"] = "CREATED"
            report["arn"] = r["FunctionArn"]
            wait_for_idle(lam, FN, 60)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                # actually exists, race condition — wait
                wait_for_idle(lam, FN, 90)
                report["create"] = "RACED_EXISTS"
            else:
                report["create_err"] = str(e)[:300]
                return report

    # Phase 2 — sync config (env / memory / timeout)
    print("Phase 2: syncing config...")
    cur = lam.get_function_configuration(FunctionName=FN)
    cur_env = cur.get("Environment", {}).get("Variables", {})
    desired_env = cfg.get("env", {})
    needs_update = (
        cur.get("MemorySize") != cfg["memory"]
        or cur.get("Timeout") != cfg["timeout"]
        or set(cur_env.keys()) != set(desired_env.keys())
        or any(cur_env.get(k) != v for k, v in desired_env.items())
    )
    if needs_update:
        lam.update_function_configuration(
            FunctionName=FN,
            Environment={"Variables": desired_env},
            Timeout=int(cfg["timeout"]),
            MemorySize=int(cfg["memory"]),
            Description=cfg.get("description", "")[:255],
        )
        report["config_sync"] = "UPDATED"
        wait_for_idle(lam, FN, 60)
    else:
        report["config_sync"] = "ALREADY_MATCHES"

    # Phase 3 — EventBridge schedule
    print("Phase 3: wiring schedule...")
    sched = cfg.get("schedule", {})
    rule_name = sched.get("rule_name")
    cron = sched.get("cron")
    if rule_name and cron:
        fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{FN}"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression=cron,
            State="ENABLED",
            Description=sched.get("description", "")[:512],
        )
        events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            lam.add_permission(
                FunctionName=FN,
                StatementId=f"AllowEB-{rule_name}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}",
            )
            report["permission"] = "ADDED"
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                report["permission"] = "EXISTS"
            else:
                raise
        report["schedule"] = f"{rule_name} {cron}"

    # Phase 4 — invoke
    print("Phase 4: invoking to materialize output...")
    try:
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail")
        report["invoke_status"] = inv["StatusCode"]
        report["invoke_fn_err"] = inv.get("FunctionError")
        log = base64.b64decode(inv.get("LogResult", "")).decode("utf-8", errors="replace")
        report["log_tail"] = log[-1500:]
    except Exception as e:
        report["invoke_err"] = str(e)[:300]

    # Phase 5 — verify S3 output
    print("Phase 5: verifying S3 output...")
    time.sleep(3)
    try:
        o = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)
        body = o["Body"].read().decode("utf-8")
        data = json.loads(body)
        report["s3_output"] = {
            "size_bytes": len(body),
            "last_modified": o["LastModified"].isoformat(),
            "n_assets": len(data.get("assets", {})),
            "horizon_years": data.get("horizon_years"),
            "headlines": data.get("headlines", [])[:5],
            "top_3_opportunities": data.get("rankings", {}).get("by_opportunity_percentile", [])[:3],
            "top_3_by_er": data.get("rankings", {}).get("by_forward_er", [])[:3],
            "portfolios": {
                k: {"er": p["forward_er_pct"], "ten_k": p["ten_k_10yr"]}
                for k, p in data.get("benchmark_portfolios", {}).items()
            },
        }
        # Spot-check assets
        sample_assets = ["SPY", "IEF", "GLD", "VNQ", "BTC"]
        report["s3_output"]["asset_samples"] = {
            sym: {
                "fwd_er": data["assets"][sym]["forward_er_10y_pct"],
                "percentile": data["assets"][sym]["current_vs_history_percentile"],
                "verdict": data["assets"][sym]["verdict"],
                "ten_k_central": data["assets"][sym]["ten_k_in_10yr_usd"]["central"],
            }
            for sym in sample_assets if sym in data.get("assets", {})
        }
    except Exception as e:
        report["s3_verify_err"] = str(e)[:200]

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    return report


def main():
    lam = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    try:
        report = deploy(lam, events, s3)
    except Exception as e:
        report = {"err": str(e), "trace": traceback.format_exc()[-800:]}

    out = os.path.join(REPO_ROOT, "aws/ops/reports/1079.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("=" * 60)
    print(json.dumps(report, indent=2, default=str)[:3500])


if __name__ == "__main__":
    main()
