"""ops/596 — final verify after schedule-schema fix.

Checks all 9 batch Lambdas now exist, have proper env vars from deploy,
invoke cleanly, and produce sidecars. Then runs invoke on analyst-consensus
to confirm KeyError fix worked.
"""
import json, os, time, base64
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
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
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", errors="replace") if r.get("LogResult") else ""
        body = r["Payload"].read().decode("utf-8", errors="replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response_preview": body[:400], "log_tail": log[-1800:]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:300]}


def patch_env_if_missing(fname, want_keys):
    """Only patch if any of the want_keys are missing. Inherit values from SSM."""
    try:
        cfg = lam.get_function_configuration(FunctionName=fname)
    except Exception as e:
        return {"err": f"get_config: {e}"}
    env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
    missing = [k for k in want_keys if not env.get(k)]
    if not missing:
        return {"already_set": list(env.keys()), "patched": []}
    # Pull from SSM
    src = {
        "FRED_API_KEY":     get_param("/justhodl/fred-api-key") or "2f057499936072679d8843d7fce99989",
        "FMP_KEY":          get_param("/justhodl/fmp-key") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
        "POLYGON_KEY":      get_param("/justhodl/polygon-key") or "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
        "TELEGRAM_TOKEN":   get_param("/justhodl/telegram/bot_token") or "",
        "TELEGRAM_CHAT_ID": get_param("/justhodl/telegram/chat_id") or "",
        "ANTHROPIC_API_KEY": get_param("/justhodl/anthropic-api-key") or "",
    }
    for k in missing:
        if src.get(k): env[k] = src[k]
    lam.update_function_configuration(FunctionName=fname,
                                      Environment={"Variables": env})
    time.sleep(2)
    return {"patched": missing, "total_env": len(env)}


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    targets = [
        # (fname, env_keys_needed, sidecar_key)
        ("justhodl-analyst-consensus",  ["FRED_API_KEY","FMP_KEY","TELEGRAM_TOKEN","TELEGRAM_CHAT_ID"], "data/analyst-consensus.json"),
        ("justhodl-market-internals",   ["FRED_API_KEY","POLYGON_KEY","TELEGRAM_TOKEN","TELEGRAM_CHAT_ID"], "data/market-internals.json"),
        ("justhodl-cds-proxy",          ["FRED_API_KEY","TELEGRAM_TOKEN","TELEGRAM_CHAT_ID"], "data/cds-proxy.json"),
        ("justhodl-esi",                ["FRED_API_KEY","TELEGRAM_TOKEN","TELEGRAM_CHAT_ID"], "data/esi.json"),
        ("justhodl-seasonality",        ["FRED_API_KEY","FMP_KEY"], "data/seasonality.json"),
        ("justhodl-liquidity-profile",  ["FRED_API_KEY","FMP_KEY"], "data/liquidity-profile.json"),
        ("justhodl-tic-flows",          ["FRED_API_KEY","TELEGRAM_TOKEN","TELEGRAM_CHAT_ID"], "data/tic-flows.json"),
        ("justhodl-bond-trace",         ["FRED_API_KEY","TELEGRAM_TOKEN","TELEGRAM_CHAT_ID"], "data/bond-trace.json"),
        ("justhodl-sellside-views",     ["FRED_API_KEY","FMP_KEY","TELEGRAM_TOKEN","TELEGRAM_CHAT_ID"], "data/sellside-views.json"),
    ]

    results = {}
    for fname, env_keys, sidecar_key in targets:
        sub = {}
        try:
            cfg = lam.get_function_configuration(FunctionName=fname)
            sub["exists"] = True
            sub["last_modified"] = cfg.get("LastModified")
            sub["memory"] = cfg.get("MemorySize")
            sub["timeout"] = cfg.get("Timeout")
            sub["runtime"] = cfg.get("Runtime")
            cur_env = (cfg.get("Environment") or {}).get("Variables", {}) or {}
            sub["env_keys_present"] = sorted(cur_env.keys())
        except Exception as e:
            sub["exists"] = False
            sub["err"] = str(e)[:200]
            results[fname] = sub
            continue
        sub["env_check"] = patch_env_if_missing(fname, env_keys)
        sub["invoke"] = force_invoke(fname)
        sub["sidecar"] = fetch_sidecar(sidecar_key)
        sc = sub["sidecar"]
        if isinstance(sc, dict) and "_error" not in sc:
            sub["sidecar_size_bytes"] = len(json.dumps(sc, default=str))
            sub["sidecar_keys"] = list(sc.keys())[:18]
            # Extract a few interesting bits per Lambda
            sub["sidecar_summary"] = {}
            for k in ("regime","method","composite_credit_risk","esi","seasonality","status","score","alert","alerts",
                       "universe_size","n_with_data","n_universe","beat_kings","top_consensus_25",
                       "ad_line","mcclellan","pct_above_50d","pct_above_200d","hyg","lqd","oas",
                       "foreign_holdings","largest_holders","sellside_targets","spx_consensus"):
                if k in sc:
                    v = sc[k]
                    if isinstance(v, list):
                        sub["sidecar_summary"][k] = f"list len={len(v)}"
                    elif isinstance(v, dict):
                        sub["sidecar_summary"][k] = f"dict keys={list(v.keys())[:5]}"
                    else:
                        sub["sidecar_summary"][k] = str(v)[:80]
        results[fname] = sub
        print(f"  {fname}: exists={sub.get('exists')} invoke={sub.get('invoke',{}).get('status')} fn_err={sub.get('invoke',{}).get('fn_error')}")

    report["lambdas"] = results

    # Tally summary
    summary = {
        "n_exist": sum(1 for r in results.values() if r.get("exists")),
        "n_total": len(results),
        "n_invoke_ok": sum(1 for r in results.values() if r.get("invoke",{}).get("status") == 200 and not r.get("invoke",{}).get("fn_error")),
        "n_sidecar_present": sum(1 for r in results.values() if isinstance(r.get("sidecar"), dict) and "_error" not in r.get("sidecar", {})),
        "missing": [n for n, r in results.items() if not r.get("exists")],
        "failed_invoke": [n for n, r in results.items() if r.get("invoke",{}).get("fn_error")],
    }
    report["summary"] = summary

    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/596_post_schedule_fix.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nDONE - {summary['n_exist']}/{summary['n_total']} exist, "
          f"{summary['n_invoke_ok']} invoke-clean, "
          f"{summary['n_sidecar_present']} sidecars present")
    if summary["missing"]:
        print(f"MISSING: {summary['missing']}")
    if summary["failed_invoke"]:
        print(f"FAILED INVOKE: {summary['failed_invoke']}")
    return report


if __name__ == "__main__":
    main()
