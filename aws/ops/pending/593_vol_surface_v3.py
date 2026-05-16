#!/usr/bin/env python3
"""593 — Bootstrap vol-surface v3 (FRED edition) + check 592 status.

This is the 3rd attempt at vol-surface:
  v1 Polygon options → retired (no entitlement)
  v2 Yahoo options   → 429 rate-limit from AWS
  v3 FRED VIX-family + CBOE SKEW → guaranteed to work (FRED endpoint always reachable)
"""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/593_vol_surface_v3.json"
NAME = "justhodl-vol-surface"
REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Wait for CI/CD redeploy
    pre_mod = None
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        pre_mod = cfg.get("LastModified")
    except: pass
    out["pre_modified"] = pre_mod
    for i in range(40):
        try:
            cfg = lam.get_function_configuration(FunctionName=NAME)
            if cfg.get("LastModified") != pre_mod and cfg.get("State")=="Active" and cfg.get("LastUpdateStatus")=="Successful":
                out["new_modified"] = cfg.get("LastModified")
                break
        except: pass
        _time.sleep(8)
    _time.sleep(3)

    # Verify env (FRED + TG should already be patched from ops 591)
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        env_keys = sorted((cfg.get("Environment") or {}).get("Variables", {}).keys())
        out["env_keys_present"] = env_keys
    except Exception as e:
        out["env_err"] = str(e)[:200]

    # Force invoke
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
            out["log_tail"] = log[-1800:]
    except Exception as e:
        out["invoke_err"] = str(e)[:300]

    # Read sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/vol-surface.json")
        body = obj["Body"].read()
        sidecar = json.loads(body)
        out["sidecar_size_kb"] = round(len(body)/1024, 1)
        out["sidecar_summary"] = {
            "data_source": sidecar.get("data_source"),
            "generated_at": sidecar.get("generated_at"),
            "regime": sidecar.get("regime"),
            "composite_stress_score": sidecar.get("composite_stress_score"),
            "stress_components": sidecar.get("stress_components"),
            "skew": sidecar.get("skew"),
            "vvix": sidecar.get("vvix"),
            "term_structure": sidecar.get("term_structure"),
            "cross_asset": {k: v for k, v in (sidecar.get("cross_asset") or {}).items() if k != "spots"},
            "alerts": sidecar.get("alerts"),
            "data_freshness": sidecar.get("data_freshness"),
            "underlyings": sidecar.get("underlyings"),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    # 592 status — has it run? ops/pending should be empty for it if so
    try:
        # We're inside the runner; check if 592's report exists yet
        rpt_path = "aws/ops/reports/592_bloomberg_10_bootstrap.json"
        if os.path.exists(rpt_path):
            with open(rpt_path) as f:
                d592 = json.load(f)
            # Compact summary
            summaries = {}
            for name, info in (d592.get("results") or {}).items():
                summaries[name] = {
                    "invoke": info.get("invoke_status"),
                    "fn_error": info.get("fn_error"),
                    "response": info.get("response"),
                    "sidecar_size_kb": info.get("sidecar_size_kb"),
                    "sidecar_modified": info.get("sidecar_modified"),
                }
            out["592_summary"] = summaries
        else:
            out["592_not_yet_run"] = True
    except Exception as e:
        out["592_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
