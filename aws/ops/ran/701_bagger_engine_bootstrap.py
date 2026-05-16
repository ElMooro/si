"""ops/701 — bootstrap + verify the 100x Bagger Engine.

1. Wait for justhodl-bagger-engine to deploy
2. Patch FMP_KEY + TELEGRAM env vars from SSM
3. Ensure EventBridge weekly schedule
4. Full invoke (boto3 client configured with 900s read timeout)
5. Read data/bagger-engine.json and summarize tiers + top names
"""
import json, os, time, base64
import boto3
from botocore.config import Config
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

# long read timeout — the engine can run up to 720s
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=900, connect_timeout=20, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def get_param(name, default=None):
    try:
        return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return default


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. wait for deploy
    fname = "justhodl-bagger-engine"
    found = False
    for _ in range(20):
        try:
            cfg = lam.get_function_configuration(FunctionName=fname)
            report["deploy"] = {"last_modified": cfg.get("LastModified"),
                                 "memory": cfg.get("MemorySize"),
                                 "timeout": cfg.get("Timeout"),
                                 "state": cfg.get("State")}
            found = True
            break
        except Exception:
            time.sleep(15)
    if not found:
        report["error"] = "bagger-engine not deployed after 5 min"
        _write(report)
        return

    # 2. patch env
    fmp = get_param("/justhodl/fmp-key") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
    tg_token = get_param("/justhodl/telegram/bot_token") or ""
    tg_chat = get_param("/justhodl/telegram/chat_id") or ""
    try:
        cur = lam.get_function_configuration(FunctionName=fname)
        env = (cur.get("Environment") or {}).get("Variables", {}) or {}
        env.update({"FMP_KEY": fmp, "TELEGRAM_TOKEN": tg_token,
                     "TELEGRAM_CHAT_ID": tg_chat, "MAX_WORKERS": "8"})
        lam.update_function_configuration(FunctionName=fname,
                                          Environment={"Variables": env})
        # wait for config update to settle
        for _ in range(20):
            time.sleep(3)
            c = lam.get_function_configuration(FunctionName=fname)
            if c.get("LastUpdateStatus") == "Successful":
                break
        report["env_patch"] = {"keys": sorted(env.keys()), "status": "ok"}
    except Exception as e:
        report["env_patch"] = {"error": str(e)[:200]}

    # 3. ensure EventBridge schedule
    try:
        cfg = lam.get_function_configuration(FunctionName=fname)
        arn = cfg["FunctionArn"]
        rule = "bagger-engine-weekly"
        events.put_rule(Name=rule, ScheduleExpression="cron(0 12 ? * SUN *)", State="ENABLED")
        try:
            lam.add_permission(FunctionName=fname, StatementId=f"{rule}-inv",
                                Action="lambda:InvokeFunction",
                                Principal="events.amazonaws.com",
                                SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule}")
        except Exception:
            pass
        events.put_targets(Rule=rule, Targets=[{"Id": "1", "Arn": arn}])
        report["schedule"] = {"rule": rule, "ok": True}
    except Exception as e:
        report["schedule"] = {"error": str(e)[:200]}

    # 4. full invoke
    print("Invoking bagger-engine (full universe — may take several minutes)...")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=fname, InvocationType="RequestResponse",
                        Payload=b"{}", LogType="Tail")
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", errors="replace") if r.get("LogResult") else ""
        body = r["Payload"].read().decode("utf-8", errors="replace") if r.get("Payload") else ""
        report["invoke"] = {
            "status": r.get("StatusCode"),
            "fn_error": r.get("FunctionError"),
            "elapsed_s": round(time.time() - t0, 1),
            "response": body[:600],
            "log_tail": log[-2500:],
        }
    except Exception as e:
        report["invoke"] = {"error": str(e)[:300], "elapsed_s": round(time.time() - t0, 1)}

    # 5. read sidecar
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/bagger-engine.json")
        sc = json.loads(obj["Body"].read())
        tiers = sc.get("tiers", {})
        report["sidecar"] = {
            "size_kb": round(len(json.dumps(sc)) / 1024, 1),
            "generated_at": sc.get("generated_at"),
            "elapsed_s": sc.get("elapsed_s"),
            "candidates_in_range": sc.get("candidates_in_range"),
            "n_scored": sc.get("n_scored"),
            "n_errors": sc.get("n_errors"),
            "tier_counts": sc.get("tier_counts"),
            "top_15": [
                {"rank": r.get("rank"), "symbol": r.get("symbol"),
                 "name": (r.get("name") or "")[:28], "score": r.get("bagger_score"),
                 "cap_bucket": r.get("cap_bucket"),
                 "classification": r.get("twin_engine", {}).get("classification"),
                 "rev_cagr": r.get("key_stats", {}).get("revenue_cagr_pct"),
                 "roic": r.get("key_stats", {}).get("roic_pct")}
                for r in (sc.get("top_100") or [])[:15]
            ],
            "potential_100x_names": [
                {"symbol": r.get("symbol"), "name": (r.get("name") or "")[:26],
                 "score": r.get("bagger_score"),
                 "yr15_rerated": r.get("twin_engine", {}).get("yr15", {}).get("with_rerating_x")}
                for r in (tiers.get("potential_100x") or [])[:12]
            ],
        }
    except Exception as e:
        report["sidecar"] = {"error": str(e)[:200]}

    _write(report)


def _write(report):
    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/701_bagger_engine_bootstrap.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 701_bagger_engine_bootstrap.json")


if __name__ == "__main__":
    main()
