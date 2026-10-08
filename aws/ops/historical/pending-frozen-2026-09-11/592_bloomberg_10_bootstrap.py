#!/usr/bin/env python3
"""592 — Mega bootstrap for all 10 new Bloomberg-roadmap Lambdas.

For each: (1) wait for CI/CD redeploy, (2) patch FRED/FMP/POLYGON/TG creds,
(3) verify EB schedule from config, (4) force invoke, (5) read sidecar."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/592_bloomberg_10_bootstrap.json"
REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# 10 Lambdas to bootstrap with their expected output sidecar + schedule
LAMBDAS = [
    {"name": "justhodl-analyst-consensus",   "sidecar": "data/analyst-consensus.json",
       "schedule": "cron(45 11 ? * MON-FRI *)",
       "memory": 1024, "timeout": 600,
       "env_keys": ["FMP_KEY", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]},
    {"name": "justhodl-vol-surface",         "sidecar": "data/vol-surface.json",
       "schedule": "cron(0 14,16,18,20 ? * MON-FRI *)",
       "memory": 1024, "timeout": 600,
       "env_keys": ["POLYGON_KEY", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]},
    {"name": "justhodl-market-internals",    "sidecar": "data/market-internals.json",
       "schedule": "cron(15 21 ? * MON-FRI *)",
       "memory": 1024, "timeout": 900,
       "env_keys": ["POLYGON_KEY", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]},
    {"name": "justhodl-cds-proxy",           "sidecar": "data/cds-proxy.json",
       "schedule": "cron(0 13,17,21 ? * MON-FRI *)",
       "memory": 256, "timeout": 120,
       "env_keys": ["FRED_API_KEY", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]},
    {"name": "justhodl-esi",                 "sidecar": "data/esi.json",
       "schedule": "cron(30 12 ? * MON-FRI *)",
       "memory": 256, "timeout": 60,
       "env_keys": ["TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]},
    {"name": "justhodl-seasonality",         "sidecar": "data/seasonality.json",
       "schedule": "cron(0 11 ? * MON *)",
       "memory": 1024, "timeout": 600,
       "env_keys": ["POLYGON_KEY"]},
    {"name": "justhodl-liquidity-profile",   "sidecar": "data/liquidity-profile.json",
       "schedule": "cron(0 22 ? * FRI *)",
       "memory": 1024, "timeout": 600,
       "env_keys": ["POLYGON_KEY"]},
    {"name": "justhodl-tic-flows",           "sidecar": "data/tic-flows.json",
       "schedule": "cron(0 22 ? * THU *)",
       "memory": 256, "timeout": 120,
       "env_keys": ["FRED_API_KEY", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]},
    {"name": "justhodl-bond-trace",          "sidecar": "data/bond-trace.json",
       "schedule": "cron(0 21 ? * MON-FRI *)",
       "memory": 256, "timeout": 120,
       "env_keys": ["POLYGON_KEY", "FRED_API_KEY", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]},
    {"name": "justhodl-sellside-views",      "sidecar": "data/sellside-views.json",
       "schedule": "cron(0 14 ? * MON,WED,FRI *)",
       "memory": 256, "timeout": 60,
       "env_keys": ["FMP_KEY", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]},
]


def ssm_get(name):
    try:
        r = ssm.get_parameter(Name=name, WithDecryption=True)
        return r["Parameter"]["Value"]
    except Exception as e:
        print(f"[ssm] {name}: {e}")
        return None


# Pull secrets ONCE
SECRETS = {
    "FRED_API_KEY":     ssm_get("/justhodl/fred/api_key") or "",
    "POLYGON_KEY":      ssm_get("/justhodl/polygon/api_key") or "",
    "FMP_KEY":          ssm_get("/justhodl/fmp/api_key") or "",
    "TELEGRAM_TOKEN":   ssm_get("/justhodl/telegram/bot_token") or "",
    "TELEGRAM_CHAT_ID": ssm_get("/justhodl/telegram/chat_id") or "",
}
# Fallback: try alternate keys
if not SECRETS["FRED_API_KEY"]:
    SECRETS["FRED_API_KEY"] = ssm_get("/justhodl/fred/key") or ssm_get("/justhodl/fred-key") or ""
if not SECRETS["POLYGON_KEY"]:
    SECRETS["POLYGON_KEY"] = ssm_get("/justhodl/polygon/key") or ssm_get("/justhodl/polygon-key") or ""
if not SECRETS["FMP_KEY"]:
    SECRETS["FMP_KEY"] = ssm_get("/justhodl/fmp/key") or ssm_get("/justhodl/fmp-key") or ""

print("SECRETS resolved: " + ", ".join(f"{k}={'YES' if v else 'NO'}" for k, v in SECRETS.items()))


def wait_for_deploy(name, max_wait_s=320):
    """Wait until Lambda exists AND is Active+Successful."""
    t0 = _time.time()
    while _time.time() - t0 < max_wait_s:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                return cfg
        except lam.exceptions.ResourceNotFoundException:
            pass
        except Exception: pass
        _time.sleep(10)
    return None


def patch_env(name, env_keys, sidecar):
    """Patch env vars + memory + timeout in single update."""
    env = {k: SECRETS[k] for k in env_keys if SECRETS.get(k)}
    # All Lambdas need S3 access for output writing — make sure
    try:
        lam.update_function_configuration(
            FunctionName=name,
            Environment={"Variables": env},
        )
        _time.sleep(5)
        return {"patched": True, "env_keys_set": list(env.keys())}
    except Exception as e:
        return {"patched": False, "err": str(e)[:200]}


def ensure_eb_rule(name, schedule):
    """Create or update EB rule pointing at the Lambda function."""
    rule_name = f"{name}-schedule"
    try:
        fn_arn = lam.get_function_configuration(FunctionName=name)["FunctionArn"]
    except Exception as e:
        return {"err": f"get_function_arn: {e}"}
    try:
        events.put_rule(Name=rule_name, ScheduleExpression=schedule, State="ENABLED")
        events.put_targets(Rule=rule_name,
                            Targets=[{"Id": "1", "Arn": fn_arn}])
        # Grant permission to invoke
        try:
            lam.add_permission(FunctionName=name, StatementId="eb-invoke-1",
                                Action="lambda:InvokeFunction",
                                Principal="events.amazonaws.com",
                                SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule_name}")
        except lam.exceptions.ResourceConflictException:
            pass  # already exists
        return {"rule": rule_name, "schedule": schedule, "state": "ENABLED"}
    except Exception as e:
        return {"err": str(e)[:200]}


def force_invoke(name):
    try:
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out = {"status": resp.get("StatusCode"), "fn_error": resp.get("FunctionError")}
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["raw"] = body[:200]
        if resp.get("LogResult"):
            log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            out["log_tail"] = log[-800:]
        return out
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    overall = {"started": datetime.now(timezone.utc).isoformat(), "results": {}}

    for lconf in LAMBDAS:
        name = lconf["name"]
        print(f"\n══ {name} ══")
        r = {}
        # 1. Wait for deploy
        cfg = wait_for_deploy(name)
        r["deploy"] = ("OK" if cfg else "TIMEOUT")
        if not cfg:
            overall["results"][name] = r; continue
        r["last_modified"] = cfg.get("LastModified")

        # 2. Patch env
        r["env"] = patch_env(name, lconf["env_keys"], lconf["sidecar"])

        # 3. EB rule
        r["eb"] = ensure_eb_rule(name, lconf["schedule"])

        # Wait briefly after env update
        _time.sleep(3)

        # 4. Force invoke
        r["invoke"] = force_invoke(name)

        # 5. Read sidecar
        try:
            obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=lconf["sidecar"])
            r["sidecar"] = {
                "exists": True, "size_kb": round(obj["ContentLength"]/1024, 1),
                "modified": obj["LastModified"].isoformat()[:19],
            }
        except Exception as e:
            r["sidecar"] = {"exists": False, "err": str(e)[:80]}

        overall["results"][name] = r
        print(f"   deploy={r['deploy']}  inv={r['invoke'].get('status')} sidecar={r['sidecar']}")

    overall["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(overall, f, indent=2, default=str)


if __name__ == "__main__":
    main()
