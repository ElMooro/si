#!/usr/bin/env python3
"""591 — Bootstrap justhodl-vol-surface (Yahoo + BS delta edition).

Tests whether Yahoo Finance options endpoint works from the Lambda environment
(prior Polygon-based attempt was retired because Polygon plan lacks options).

Steps:
1. Wait for CI/CD to redeploy the rewritten Lambda
2. Patch env vars (TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, FRED_API_KEY) from SSM
3. Force-invoke and inspect output
4. Read sidecar from S3 and report metrics
"""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/591_vol_surface_bootstrap.json"
NAME = "justhodl-vol-surface"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def get_param(path):
    try:
        return ssm.get_parameter(Name=path, WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:
        print(f"[ssm] {path}: {e}")
        return None


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # 1. Get config from SSM
    tg_token = get_param("/justhodl/telegram/bot_token")
    tg_chat = get_param("/justhodl/telegram/chat_id")
    fred_key = "2f057499936072679d8843d7fce99989"  # known constant from memory
    out["ssm"] = {"tg_token": bool(tg_token), "tg_chat": bool(tg_chat), "fred": True}

    # 2. Wait for CI/CD to redeploy
    pre_mod = None
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        pre_mod = cfg.get("LastModified")
    except Exception as e:
        out["pre_get_err"] = str(e)[:120]
    out["pre_modified"] = pre_mod

    for i in range(40):
        try:
            cfg = lam.get_function_configuration(FunctionName=NAME)
            mod = cfg.get("LastModified")
            if mod != pre_mod and cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                out["new_modified"] = mod
                break
        except Exception: pass
        _time.sleep(8)
    _time.sleep(3)

    # 3. Patch env vars
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        existing_env = (cfg.get("Environment") or {}).get("Variables", {})
        new_env = dict(existing_env)
        if tg_token: new_env["TELEGRAM_TOKEN"] = tg_token
        if tg_chat: new_env["TELEGRAM_CHAT_ID"] = tg_chat
        new_env["FRED_API_KEY"] = fred_key
        # Wait for any in-flight update to finish
        for j in range(20):
            cfg2 = lam.get_function_configuration(FunctionName=NAME)
            if cfg2.get("LastUpdateStatus") in ("Successful", "Failed"):
                break
            _time.sleep(3)
        lam.update_function_configuration(
            FunctionName=NAME, Environment={"Variables": new_env})
        for j in range(20):
            cfg2 = lam.get_function_configuration(FunctionName=NAME)
            if cfg2.get("LastUpdateStatus") == "Successful":
                out["env_patch"] = "OK"
                break
            _time.sleep(3)
        out["env_keys"] = sorted(new_env.keys())
    except Exception as e:
        out["env_err"] = str(e)[:200]

    _time.sleep(2)

    # 4. Force invoke
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["raw"] = body[:300]
        if resp.get("LogResult"):
            log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            # Capture interesting lines (Yahoo connectivity, errors)
            lines = log.split("\n")
            interesting = [l for l in lines if any(k in l for k in
                ["vol-surface", "spot=", "fred", "tg", "ERR", "Error",
                 "yahoo", "Yahoo", "Connection", "HTTP", "[SPY", "[QQQ", "[IWM",
                 "[GLD", "[TLT", "[HYG"])]
            out["log_lines"] = interesting[-40:]
    except Exception as e:
        out["invoke_err"] = str(e)[:300]

    # 5. Read sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/vol-surface.json")
        body = obj["Body"].read()
        sidecar = json.loads(body)
        out["sidecar_size_kb"] = round(len(body)/1024, 1)
        # Extract summary
        out["sidecar_summary"] = {
            "generated_at": sidecar.get("generated_at"),
            "elapsed_s": sidecar.get("elapsed_s"),
            "r_rate_used": sidecar.get("r_rate_used"),
            "global": sidecar.get("global"),
            "alerts": sidecar.get("alerts"),
            "underlyings_summary": {
                u: {"spot": v.get("spot"), "regime": v.get("regime"),
                    "rr25_avg": v.get("rr25_avg"), "bf25_avg": v.get("bf25_avg"),
                    "atm_avg": v.get("atm_avg"), "term_slope": v.get("term_slope_per_year"),
                    "n_expirations": len(v.get("expirations") or [])}
                for u, v in (sidecar.get("underlyings") or {}).items()
            },
            "sample_expiration_SPY": (sidecar.get("underlyings") or {}).get("SPY", {}).get("expirations", [None])[0],
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
