"""
ops 1074 — finalize 6 Lambda deployments after deploy-lambdas.yml created them.

Context: ops 1072 raced — deploy-lambdas.yml had already created all 6 from the
prior commit (b4d896d5 PHASE 1B+1C). 1072 hit ResourceConflictException because
update_function_code was mid-flight when our config update fired.

This op:
  1. waits for each function to be Active + LastUpdateStatus Successful
  2. updates env vars (incl. harvesting ANTHROPIC_API_KEY where needed)
  3. verifies EventBridge rule exists + has target + has permission
  4. invokes each Lambda to confirm it runs (sync invoke, capture log tail)
"""
import json, os, sys, time, base64, traceback
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

LAMBDAS = [
    "justhodl-dr-snapshot",
    "justhodl-cost-anomaly",
    "justhodl-macro-calendar",
    "justhodl-fed-nlp",
    "justhodl-news-wire",
    "justhodl-concentration-liquidity",
]

NEEDS_ANTHROPIC = {"justhodl-fed-nlp", "justhodl-news-wire"}
ANTHROPIC_SOURCE = "justhodl-ai-chat"


def wait_for_idle(lam, fn_name, max_wait=120):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn_name)
            state = cfg.get("State")
            last_update = cfg.get("LastUpdateStatus")
            if state == "Active" and last_update in ("Successful", None):
                return True, cfg
            if last_update == "Failed":
                return False, cfg
        except Exception:
            pass
        time.sleep(3)
    return False, None


def main():
    lam = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)

    report = {"started_at": datetime.now(timezone.utc).isoformat(), "results": []}

    # Harvest anthropic key
    anthropic_key = None
    try:
        c = lam.get_function_configuration(FunctionName=ANTHROPIC_SOURCE)
        anthropic_key = c.get("Environment", {}).get("Variables", {}).get("ANTHROPIC_API_KEY")
        report["anthropic_harvest"] = "OK" if anthropic_key and anthropic_key.startswith("sk-") else "BAD_KEY"
    except Exception as e:
        report["anthropic_harvest"] = f"ERR: {e}"

    for fn in LAMBDAS:
        r = {"fn": fn}
        try:
            # 1. wait idle
            ok, cfg = wait_for_idle(lam, fn)
            if not ok:
                r["status"] = "ERR"
                r["error"] = f"Not idle after wait. state={cfg.get('State') if cfg else None} lus={cfg.get('LastUpdateStatus') if cfg else None}"
                report["results"].append(r)
                continue
            r["initial_state"] = "Active"
            r["code_sha"] = cfg.get("CodeSha256", "")[:12]
            r["last_modified"] = cfg.get("LastModified")
            r["mem"] = cfg.get("MemorySize")
            r["timeout"] = cfg.get("Timeout")

            # 2. load config.json from repo
            cfg_path = os.path.join(REPO_ROOT, "aws", "lambdas", fn, "config.json")
            with open(cfg_path) as f:
                desired = json.load(f)
            desired_env = dict(desired.get("env", {}))

            # inject anthropic where needed
            if fn in NEEDS_ANTHROPIC:
                if anthropic_key:
                    desired_env["ANTHROPIC_API_KEY"] = anthropic_key
                else:
                    desired_env.pop("ANTHROPIC_API_KEY", None)

            # 3. update env (only if differs)
            current_env = cfg.get("Environment", {}).get("Variables", {})
            env_diff = (
                set(current_env.keys()) != set(desired_env.keys())
                or any(current_env.get(k) != v for k, v in desired_env.items())
            )
            if env_diff:
                lam.update_function_configuration(
                    FunctionName=fn,
                    Environment={"Variables": desired_env},
                    Timeout=int(desired.get("timeout", cfg.get("Timeout"))),
                    MemorySize=int(desired.get("memory", cfg.get("MemorySize"))),
                    Description=desired.get("description", "")[:255],
                )
                r["env_update"] = f"UPDATED ({len(desired_env)} vars)"
                wait_for_idle(lam, fn, 60)
            else:
                r["env_update"] = "ALREADY_MATCHES"

            # 4. ensure schedule
            sched = desired.get("schedule") or {}
            rule_name = sched.get("rule_name")
            cron_expr = sched.get("cron")
            if rule_name and cron_expr:
                fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{fn}"
                try:
                    events.put_rule(
                        Name=rule_name,
                        ScheduleExpression=cron_expr,
                        State="ENABLED",
                        Description=sched.get("description", "")[:512],
                    )
                    events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
                    try:
                        lam.add_permission(
                            FunctionName=fn,
                            StatementId=f"AllowEB-{rule_name}",
                            Action="lambda:InvokeFunction",
                            Principal="events.amazonaws.com",
                            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}",
                        )
                        r["permission"] = "ADDED"
                    except ClientError as e:
                        if e.response["Error"]["Code"] == "ResourceConflictException":
                            r["permission"] = "EXISTS"
                        else:
                            raise
                    r["schedule"] = f"{rule_name} {cron_expr}"
                except Exception as e:
                    r["schedule_err"] = str(e)[:200]

            # 5. test invoke (manual, dry run)
            # Don't invoke DR-snapshot (could be slow + write to DR bucket; let scheduled run be first)
            # Don't invoke cost-anomaly (Cost Explorer calls cost $0.01 each — schedule will run anyway)
            if fn not in ("justhodl-dr-snapshot", "justhodl-cost-anomaly"):
                try:
                    inv = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", LogType="Tail")
                    r["invoke_status"] = inv.get("StatusCode")
                    r["invoke_err"] = inv.get("FunctionError")
                    log = base64.b64decode(inv.get("LogResult", "")).decode("utf-8", errors="replace")
                    r["log_tail"] = log[-600:]
                except Exception as e:
                    r["invoke_err"] = str(e)[:200]
            else:
                r["invoke_status"] = "SKIPPED (will run on schedule)"

            r["status"] = "OK"
        except Exception as e:
            r["status"] = "ERR"
            r["error"] = str(e)[:300]
            r["trace"] = traceback.format_exc()[-400:]
        report["results"].append(r)
        print(f"[{r.get('status')}] {fn}: env={r.get('env_update','-')} sched={r.get('schedule','-')} invoke={r.get('invoke_status','-')}")

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    report["summary"] = {
        "ok": sum(1 for x in report["results"] if x.get("status") == "OK"),
        "err": sum(1 for x in report["results"] if x.get("status") == "ERR"),
    }

    out = os.path.join(REPO_ROOT, "aws", "ops", "reports", "1074.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nREPORT: {out}")
    print(f"OK={report['summary']['ok']} ERR={report['summary']['err']}")


if __name__ == "__main__":
    main()
