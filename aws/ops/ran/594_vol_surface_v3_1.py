#!/usr/bin/env python3
"""594 — Verify vol-surface v3.1 (dead FRED ID fixes). Also check 592 status."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/594_vol_surface_v3_1.json"
NAME = "justhodl-vol-surface"
REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
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
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode"); out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body); out["response"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except: out["raw"] = body[:300]
        if resp.get("LogResult"):
            log = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")
            out["log_tail"] = log[-1800:]
    except Exception as e:
        out["invoke_err"] = str(e)[:300]
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/vol-surface.json")
        body = obj["Body"].read(); sidecar = json.loads(body)
        out["sidecar_size_kb"] = round(len(body)/1024, 1)
        out["sidecar_summary"] = {
            "regime": sidecar.get("regime"),
            "composite_stress_score": sidecar.get("composite_stress_score"),
            "stress_components": sidecar.get("stress_components"),
            "skew": sidecar.get("skew"),
            "vvix": sidecar.get("vvix"),
            "term_structure": sidecar.get("term_structure"),
            "cross_asset": {k: v for k, v in (sidecar.get("cross_asset") or {}).items() if k != "spots"},
            "alerts": sidecar.get("alerts"),
            "underlyings": sidecar.get("underlyings"),
            "data_freshness": sidecar.get("data_freshness"),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    # 592 status
    rpt_path = "aws/ops/reports/592_bloomberg_10_bootstrap.json"
    if os.path.exists(rpt_path):
        try:
            d592 = json.load(open(rpt_path))
            summaries = {}
            for name, info in (d592.get("results") or {}).items():
                summaries[name] = {
                    "invoke": info.get("invoke_status"),
                    "fn_error": info.get("fn_error"),
                    "response": info.get("response"),
                    "sidecar_size_kb": info.get("sidecar_size_kb"),
                }
            out["592_summary"] = summaries
        except Exception as e:
            out["592_err"] = str(e)[:200]
    else:
        out["592_not_yet_run"] = True

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
