#!/usr/bin/env python3
"""584 — Final verification + polish on all 6 new roadmap Lambdas.
- Confirm TG creds in env
- Confirm EB schedule active
- Force fresh invoke for current data
"""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/584_roadmap_final_verify.json"
REGION = "us-east-1"
ACCOUNT = "857687956942"

LAMBDAS = [
    "justhodl-insider-cluster-scanner",
    "justhodl-khalid-adaptive",
    "justhodl-stress-scenarios",
    "justhodl-political-trades",
    "justhodl-reversal-radar",
    "justhodl-auction-grader",
    "justhodl-repo-lending",
    "justhodl-margin-lending",
]

SIDECARS = {
    "justhodl-insider-cluster-scanner": "data/insider-clusters.json",
    "justhodl-khalid-adaptive": "data/khalid-adaptive.json",
    "justhodl-stress-scenarios": "data/stress-scenarios.json",
    "justhodl-political-trades": "data/political-trades.json",
    "justhodl-reversal-radar": "data/reversal-radar.json",
    "justhodl-auction-grader": "data/auction-grades.json",
    "justhodl-repo-lending": "data/repo-lending.json",
    "justhodl-margin-lending": "data/margin-lending.json",
}

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": {}}

    # Get SSM creds once
    try:
        tg_token = ssm.get_parameter(Name="/justhodl/telegram/token",
                                       WithDecryption=True)["Parameter"]["Value"]
        tg_chat = ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                      WithDecryption=True)["Parameter"]["Value"]
        out["ssm_creds_ok"] = True
    except Exception as e:
        out["ssm_creds_err"] = str(e)[:150]
        tg_token = tg_chat = None

    for name in LAMBDAS:
        info = {}
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
            info["exists"] = True
            info["last_modified"] = cfg.get("LastModified")
            info["state"] = cfg.get("State")
            info["mem"] = cfg.get("MemorySize")
            info["timeout"] = cfg.get("Timeout")
            info["had_TG_token"] = bool(env.get("TELEGRAM_TOKEN"))
            info["had_TG_chat"] = bool(env.get("TELEGRAM_CHAT_ID"))

            # Patch TG creds if missing
            patched = False
            if tg_token and not env.get("TELEGRAM_TOKEN"):
                env["TELEGRAM_TOKEN"] = tg_token
                patched = True
            if tg_chat and not env.get("TELEGRAM_CHAT_ID"):
                env["TELEGRAM_CHAT_ID"] = tg_chat
                patched = True
            if patched:
                lam.update_function_configuration(FunctionName=name,
                                                    Environment={"Variables": env})
                lam.get_waiter("function_updated").wait(FunctionName=name)
                info["env_patched"] = True
        except Exception as e:
            info["err"] = str(e)[:150]
            info["exists"] = False
            out["lambdas"][name] = info
            continue

        # Check EB rule
        try:
            rules = events.list_rule_names_by_target(
                TargetArn=f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{name}")
            info["eb_rules"] = rules.get("RuleNames", [])
            for r in info["eb_rules"][:1]:
                ri = events.describe_rule(Name=r)
                info["eb_schedule"] = ri.get("ScheduleExpression")
                info["eb_state"] = ri.get("State")
        except Exception as e:
            info["eb_err"] = str(e)[:150]

        # Sidecar freshness
        key = SIDECARS.get(name)
        if key:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
                info["sidecar_size_kb"] = round(obj["ContentLength"]/1024, 1)
                info["sidecar_modified"] = obj["LastModified"].isoformat()[:19]
            except Exception as e:
                info["sidecar_err"] = str(e)[:80]

        out["lambdas"][name] = info

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
