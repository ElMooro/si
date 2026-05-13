#!/usr/bin/env python3
"""509 — Scan all justhodl-* Lambdas for FMP_KEY, then update finra-short env + invoke."""
import io, json, os, time as _time, zipfile, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/509_finra_fmp_key.json"
lam = boto3.client("lambda", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Step 1: list all justhodl-* Lambdas
    funcs = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for f in page.get("Functions", []):
            if f["FunctionName"].startswith("justhodl-"):
                funcs.append(f["FunctionName"])
    out["n_lambdas"] = len(funcs)

    # Step 2: find ones with FMP_KEY
    fmp_holders = []
    for fn in funcs:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            env_keys = list((cfg.get("Environment") or {}).get("Variables", {}).keys())
            if "FMP_KEY" in env_keys or "FMP_API_KEY" in env_keys or "FMP_TOKEN" in env_keys:
                fmp_holders.append({"name": fn, "env_keys": env_keys})
        except Exception:
            continue
    out["fmp_holders"] = fmp_holders[:10]

    if not fmp_holders:
        out["error"] = "No Lambda has FMP_KEY env var"
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)
        return

    # Step 3: extract the key value
    src = fmp_holders[0]
    src_cfg = lam.get_function_configuration(FunctionName=src["name"])
    src_env = (src_cfg.get("Environment") or {}).get("Variables", {})
    fmp_key = src_env.get("FMP_KEY") or src_env.get("FMP_API_KEY") or src_env.get("FMP_TOKEN")
    if not fmp_key:
        out["error"] = "FMP_KEY value not retrievable"
        with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)
        return
    out["fmp_key_source"] = src["name"]
    out["fmp_key_prefix"] = fmp_key[:8] + "…"

    # Step 4: update finra-short env (merge existing + FMP_KEY)
    target_cfg = lam.get_function_configuration(FunctionName="justhodl-finra-short")
    target_env = (target_cfg.get("Environment") or {}).get("Variables", {})
    target_env["FMP_KEY"] = fmp_key
    lam.update_function_configuration(
        FunctionName="justhodl-finra-short",
        Environment={"Variables": target_env},
    )
    lam.get_waiter("function_updated").wait(FunctionName="justhodl-finra-short")
    out["finra_env_updated"] = sorted(target_env.keys())

    # Step 5: invoke and capture
    _time.sleep(3)
    resp = lam.invoke(FunctionName="justhodl-finra-short",
                       InvocationType="RequestResponse",
                       LogType="Tail", Payload=b"{}")
    out["invoke_status"] = resp.get("StatusCode")
    out["fn_error"] = resp.get("FunctionError")
    body = resp["Payload"].read().decode("utf-8")
    try:
        p = json.loads(body)
        out["invoke_response"] = json.loads(p["body"]) if p.get("body") else p
    except: out["invoke_raw"] = body[:2000]
    if resp.get("LogResult"):
        out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-3500:]

    # Step 6: read sidecar
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/finra-short.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "version": p.get("version"),
            "data_date": p.get("data_date"),
            "elapsed_seconds": p.get("elapsed_seconds"),
            "market_composite": p.get("market_composite"),
            "n_squeeze_candidates": len(p.get("squeeze_candidates") or []),
            "top_squeeze_10": [
                {k: c.get(k) for k in ["symbol","name","sector","svr_pct",
                                          "z_score","days_to_cover","squeeze_score","squeeze_flags"]}
                for c in (p.get("squeeze_candidates") or [])[:10]
            ],
            "top_svr_10": [
                {k: c.get(k) for k in ["symbol","name","sector","svr_pct","z_score"]}
                for c in (p.get("top_svr") or [])[:10]
            ],
            "top_zscore_10": [
                {k: c.get(k) for k in ["symbol","name","sector","z_score","svr_pct","momentum_pct"]}
                for c in (p.get("top_zscore") or [])[:10]
            ],
            "sectors": p.get("sectors") or {},
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
