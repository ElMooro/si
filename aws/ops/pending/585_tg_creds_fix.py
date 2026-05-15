#!/usr/bin/env python3
"""585 — Find correct Telegram SSM path by inspecting working Lambdas,
then re-patch all 8 + force-deploy margin-lending."""
import io, json, os, time as _time
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/585_tg_creds_fix.json"
REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

LAMBDAS = [
    "justhodl-insider-cluster-scanner",
    "justhodl-khalid-adaptive",
    "justhodl-stress-scenarios",
    "justhodl-political-trades",
    "justhodl-reversal-radar",
    "justhodl-auction-grader",
    "justhodl-repo-lending",
]

# Lambdas known to send Telegram
REFERENCE_LAMBDAS = [
    "justhodl-13f-positions",
    "justhodl-ai-chat",
    "justhodl-morning-intelligence",
    "justhodl-telegram-bot",
    "justhodl-alert-router",
    "justhodl-news-velocity",
    "justhodl-position-monitor",
]


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. Inspect reference Lambdas for actual TG env values
    refs = {}
    for ref in REFERENCE_LAMBDAS:
        try:
            cfg = lam.get_function_configuration(FunctionName=ref)
            env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
            refs[ref] = {
                "has_TG_TOKEN": bool(env.get("TELEGRAM_TOKEN")),
                "has_TG_BOT_TOKEN": bool(env.get("TELEGRAM_BOT_TOKEN")),
                "has_TG_CHAT_ID": bool(env.get("TELEGRAM_CHAT_ID")),
                "tg_env_keys": [k for k in env.keys() if "TELEGRAM" in k.upper() or "TG_" in k.upper()],
            }
        except Exception as e:
            refs[ref] = {"err": str(e)[:80]}
    out["reference_lambdas"] = refs

    # 2. List all SSM parameters with /telegram/ in path
    try:
        paginator = ssm.get_paginator("describe_parameters")
        tg_params = []
        for page in paginator.paginate(ParameterFilters=[
            {"Key": "Name", "Option": "Contains", "Values": ["telegram"]}
        ]):
            for p in page.get("Parameters", []):
                tg_params.append({"name": p["Name"], "type": p.get("Type")})
        out["ssm_telegram_params"] = tg_params
    except Exception as e:
        out["ssm_list_err"] = str(e)[:200]

    # 3. Get actual values from one ref Lambda
    try:
        ref_cfg = lam.get_function_configuration(FunctionName="justhodl-13f-positions")
        ref_env = (ref_cfg.get("Environment") or {}).get("Variables", {}) or {}
        token = ref_env.get("TELEGRAM_TOKEN") or ref_env.get("TELEGRAM_BOT_TOKEN")
        chat = ref_env.get("TELEGRAM_CHAT_ID")
        out["ref_creds_found"] = bool(token and chat)

        # 4. If found, patch all 7 new Lambdas
        if token and chat:
            for name in LAMBDAS:
                try:
                    cfg = lam.get_function_configuration(FunctionName=name)
                    env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
                    env["TELEGRAM_TOKEN"] = token
                    env["TELEGRAM_CHAT_ID"] = chat
                    lam.update_function_configuration(FunctionName=name,
                                                        Environment={"Variables": env})
                    lam.get_waiter("function_updated").wait(FunctionName=name)
                    out.setdefault("patched", []).append(name)
                except Exception as e:
                    out.setdefault("patch_errors", {})[name] = str(e)[:100]
    except Exception as e:
        out["ref_err"] = str(e)[:200]

    # 5. Check margin-lending status (in repo but not deployed)
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-margin-lending")
        out["margin_lending_deployed"] = True
        out["margin_lending_last_modified"] = cfg.get("LastModified")
    except Exception:
        out["margin_lending_deployed"] = False
        out["margin_lending_note"] = "Not in AWS — need deploy via push or direct."

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
