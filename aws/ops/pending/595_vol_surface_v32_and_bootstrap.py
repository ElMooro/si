"""ops/595 — verify vol-surface v3.2 + bootstrap remaining 9 Bloomberg Lambdas.

After 592 (committed by parallel session but never invoked) deploys the 9 batch
Lambdas, we need to: patch env vars, set EB schedules, force-invoke once, then
read each sidecar from S3.

Lambdas in scope (from batch 1+2+3):
  - justhodl-analyst-consensus (#2)
  - justhodl-market-internals (#7)
  - justhodl-cds-proxy (#5)
  - justhodl-esi (#10)
  - justhodl-seasonality (#13)
  - justhodl-liquidity-profile (#15) -- EXISTING; verify only
  - justhodl-tic-flows (#8)
  - justhodl-bond-trace (#9)
  - justhodl-sellside-views (#14)
"""
import json, os, time, sys
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def get_param(name):
    try:
        return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None


def fetch_sidecar(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {str(e)[:200]}"}


def force_invoke(fname):
    try:
        r = lam.invoke(FunctionName=fname, InvocationType="RequestResponse",
                        Payload=b"{}", LogType="Tail")
        import base64
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", errors="replace") if r.get("LogResult") else ""
        body = r["Payload"].read().decode("utf-8", errors="replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response_preview": body[:500], "log_tail": log[-1500:]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:300]}


def patch_env(fname, extra_env):
    """Merge env vars (don't replace)."""
    try:
        cur = lam.get_function_configuration(FunctionName=fname)
        env = (cur.get("Environment") or {}).get("Variables", {}) or {}
        env.update(extra_env)
        lam.update_function_configuration(FunctionName=fname,
                                          Environment={"Variables": env})
        # wait for config consistency
        time.sleep(2)
        return {"patched": list(extra_env.keys()), "total_env": len(env)}
    except Exception as e:
        return {"err": str(e)[:200]}


def ensure_eb_schedule(rule_name, expr, lambda_name):
    try:
        # Get lambda ARN
        cfg = lam.get_function_configuration(FunctionName=lambda_name)
        arn = cfg["FunctionArn"]
        events.put_rule(Name=rule_name, ScheduleExpression=expr, State="ENABLED")
        # add permission (idempotent — ignore conflict)
        try:
            lam.add_permission(FunctionName=lambda_name,
                                StatementId=f"{rule_name}-inv",
                                Action="lambda:InvokeFunction",
                                Principal="events.amazonaws.com",
                                SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}")
        except Exception:
            pass
        events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": arn}])
        return {"rule": rule_name, "expr": expr, "ok": True}
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # =================================================================
    # SECTION A — VOL-SURFACE v3.2 verification
    # =================================================================
    print("=== A: vol-surface v3.2 verify ===")
    vs = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-vol-surface")
        vs["pre_modified"] = cfg.get("LastModified")
        vs["pre_version"] = cfg.get("Version")
    except Exception as e:
        vs["preflight_err"] = str(e)

    # Wait for CI/CD push to complete redeploy (up to 4 min)
    print("Waiting up to 4 min for CI/CD redeploy of vol-surface...")
    start = time.time()
    while time.time() - start < 240:
        time.sleep(15)
        try:
            cfg = lam.get_function_configuration(FunctionName="justhodl-vol-surface")
            if cfg.get("LastModified") != vs["pre_modified"]:
                vs["new_modified"] = cfg.get("LastModified")
                vs["new_version"] = cfg.get("Version")
                break
        except Exception:
            pass
    vs["redeployed"] = "new_modified" in vs

    vs["invoke"] = force_invoke("justhodl-vol-surface")
    vs["sidecar"] = fetch_sidecar("data/vol-surface.json")
    sc = vs["sidecar"] if "_error" not in vs["sidecar"] else {}
    vs["summary"] = {
        "version": sc.get("version"),
        "regime": sc.get("regime"),
        "score": sc.get("composite_stress_score"),
        "fred_alive": sc.get("fred_alive"),
        "fred_failed": sc.get("fred_failed"),
        "skew_value": (sc.get("skew") or {}).get("value"),
        "skew_source": (sc.get("skew") or {}).get("source"),
        "vvix_value": (sc.get("vvix") or {}).get("value"),
        "vvix_source": (sc.get("vvix") or {}).get("source"),
        "equity_dispersion_regime": (sc.get("equity_dispersion") or {}).get("regime"),
        "n_alerts": len(sc.get("alerts") or []),
        "alerts_preview": (sc.get("alerts") or [])[:6],
        "n_underlyings": len(sc.get("underlyings") or {}),
        "elapsed_s": sc.get("elapsed_s"),
    }
    report["A_vol_surface"] = vs

    # =================================================================
    # SECTION B — bootstrap the 9 batch Lambdas
    # =================================================================
    print("=== B: bootstrap 9 batch Lambdas ===")
    fred = get_param("/justhodl/fred-api-key") or "2f057499936072679d8843d7fce99989"
    fmp = get_param("/justhodl/fmp-key") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
    poly = get_param("/justhodl/polygon-key") or "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
    tg_token = get_param("/justhodl/telegram/bot_token") or ""
    tg_chat = get_param("/justhodl/telegram/chat_id") or ""

    # Map of: (fname, env_vars_to_patch, eb_rule_name, eb_schedule_expr)
    targets = [
        ("justhodl-analyst-consensus",  {"FRED_API_KEY": fred, "FMP_KEY": fmp,
                                           "TELEGRAM_TOKEN": tg_token, "TELEGRAM_CHAT_ID": tg_chat},
         "analyst-consensus-daily", "cron(30 13 ? * MON-FRI *)"),
        ("justhodl-market-internals",   {"FRED_API_KEY": fred, "POLYGON_KEY": poly,
                                           "TELEGRAM_TOKEN": tg_token, "TELEGRAM_CHAT_ID": tg_chat},
         "market-internals-hourly", "cron(35 13-21 ? * MON-FRI *)"),
        ("justhodl-cds-proxy",          {"FRED_API_KEY": fred,
                                           "TELEGRAM_TOKEN": tg_token, "TELEGRAM_CHAT_ID": tg_chat},
         "cds-proxy-daily", "cron(40 13 ? * MON-FRI *)"),
        ("justhodl-esi",                {"FRED_API_KEY": fred,
                                           "TELEGRAM_TOKEN": tg_token, "TELEGRAM_CHAT_ID": tg_chat},
         "esi-daily", "cron(45 13 ? * MON-FRI *)"),
        ("justhodl-seasonality",        {"FRED_API_KEY": fred, "FMP_KEY": fmp},
         "seasonality-daily", "cron(50 13 ? * MON-FRI *)"),
        ("justhodl-liquidity-profile",  {"FRED_API_KEY": fred, "FMP_KEY": fmp},
         "liquidity-profile-daily", "cron(55 13 ? * MON-FRI *)"),
        ("justhodl-tic-flows",          {"FRED_API_KEY": fred,
                                           "TELEGRAM_TOKEN": tg_token, "TELEGRAM_CHAT_ID": tg_chat},
         "tic-flows-daily", "cron(0 14 ? * MON-FRI *)"),
        ("justhodl-bond-trace",         {"FRED_API_KEY": fred,
                                           "TELEGRAM_TOKEN": tg_token, "TELEGRAM_CHAT_ID": tg_chat},
         "bond-trace-daily", "cron(5 14 ? * MON-FRI *)"),
        ("justhodl-sellside-views",     {"FRED_API_KEY": fred, "FMP_KEY": fmp,
                                           "TELEGRAM_TOKEN": tg_token, "TELEGRAM_CHAT_ID": tg_chat},
         "sellside-views-daily", "cron(10 14 ? * MON-FRI *)"),
    ]

    sidecar_map = {
        "justhodl-analyst-consensus":  "data/analyst-consensus.json",
        "justhodl-market-internals":   "data/market-internals.json",
        "justhodl-cds-proxy":          "data/cds-proxy.json",
        "justhodl-esi":                "data/esi.json",
        "justhodl-seasonality":        "data/seasonality.json",
        "justhodl-liquidity-profile":  "data/liquidity-profile.json",
        "justhodl-tic-flows":          "data/tic-flows.json",
        "justhodl-bond-trace":         "data/bond-trace.json",
        "justhodl-sellside-views":     "data/sellside-views.json",
    }

    results = {}
    for fname, env, rule, expr in targets:
        sub = {}
        try:
            cfg = lam.get_function_configuration(FunctionName=fname)
            sub["exists"] = True
            sub["last_modified"] = cfg.get("LastModified")
            sub["memory"] = cfg.get("MemorySize")
            sub["timeout"] = cfg.get("Timeout")
        except Exception as e:
            sub["exists"] = False
            sub["preflight_err"] = str(e)[:200]
            results[fname] = sub
            continue
        sub["env_patch"] = patch_env(fname, env)
        sub["eb"] = ensure_eb_schedule(rule, expr, fname)
        sub["invoke"] = force_invoke(fname)
        sub["sidecar"] = fetch_sidecar(sidecar_map[fname])
        # Brief summary of sidecar — first few top-level keys
        if isinstance(sub["sidecar"], dict) and "_error" not in sub["sidecar"]:
            sub["sidecar_keys"] = list(sub["sidecar"].keys())[:15]
            sub["sidecar_size_bytes"] = len(json.dumps(sub["sidecar"]))
        results[fname] = sub
        print(f"  - {fname}: invoke={sub['invoke'].get('status')} "
              f"fn_err={sub['invoke'].get('fn_error')} "
              f"sidecar_ok={'_error' not in sub.get('sidecar', {})}")
    report["B_bootstrap"] = results

    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/595_vol_surface_v32_and_bootstrap.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nDONE - wrote aws/ops/reports/595_vol_surface_v32_and_bootstrap.json")
    return report


if __name__ == "__main__":
    main()
