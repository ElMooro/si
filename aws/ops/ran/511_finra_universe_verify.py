#!/usr/bin/env python3
"""511 — Trigger FINRA short Lambda after universe fix, verify full data flow."""
import json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/511_finra_universe_verify.json"
NAME = "justhodl-finra-short"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Confirm latest code is deployed
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        out["lambda_last_modified"] = cfg.get("LastModified")
        out["lambda_memory"] = cfg.get("MemorySize")
        out["lambda_timeout"] = cfg.get("Timeout")
    except Exception as e:
        out["cfg_err"] = str(e)[:200]

    # Invoke
    _time.sleep(2)
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["raw"] = body[:1500]
        if resp.get("LogResult"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8","replace")[-3000:]
    except Exception as e:
        out["invoke_err"] = str(e)[:400]

    # Read sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/finra-short.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "data_date": p.get("data_date"),
            "n_universe_analyzed": p.get("n_universe_analyzed"),
            "n_squeeze_candidates": p.get("n_squeeze_candidates"),
            "vw_svr_pct": p.get("vw_svr_pct"),
            "regime": p.get("regime"),
            "top_squeeze": (p.get("top_squeeze_candidates") or p.get("top_squeeze") or [])[:5],
            "top_svr_changes": (p.get("top_svr_changes") or [])[:5],
            "summary": p.get("summary"),
            # First 3 sector aggregates
            "sectors_top3_short": dict(list((p.get("by_sector") or {}).items())[:3]),
        }
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
