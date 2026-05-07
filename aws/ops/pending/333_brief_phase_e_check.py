#!/usr/bin/env python3
"""Step 333 — Investigate morning-brief output to verify Phase E renders."""
import json
import os
import time
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
REPORT = "aws/ops/reports/333_brief_phase_e_check.json"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Check the Lambda was actually updated
    cfg = lam.get_function_configuration(FunctionName="justhodl-morning-brief-tg")
    out["lambda_last_modified"] = cfg.get("LastModified")

    # See what S3 keys morning-brief writes — list common candidates
    candidates = [
        "data/morning-brief-latest.json",
        "data/morning-brief.json",
        "data/morning-tg-brief.json",
        "data/morning-intel.json",
    ]
    out["s3_candidates"] = {}
    for k in candidates:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=k)
            data = json.loads(obj["Body"].read())
            out["s3_candidates"][k] = {
                "size_kb": round(obj["ContentLength"]/1024, 1),
                "age_h": round((datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()/3600, 2),
                "top_keys": list(data.keys())[:15] if isinstance(data, dict) else None,
                "msg_present": "message" in data if isinstance(data, dict) else False,
                "msg_len": len(data.get("message", "")) if isinstance(data, dict) else 0,
                "has_fed_marker": "🏦 Fed Speak" in str(data),
                "has_global_marker": "🌍 Global Macro" in str(data),
            }
        except Exception as e:
            out["s3_candidates"][k] = {"err": str(e)[:80]}

    # Sync invoke morning-brief and capture logs
    print("[333] Sync invoke morning-brief (with LogType='Tail')…")
    resp = lam.invoke(
        FunctionName="justhodl-morning-brief-tg",
        InvocationType="RequestResponse",
        LogType="Tail",
    )
    out["invoke"] = {
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "response_body": resp["Payload"].read().decode("utf-8")[:1000],
    }
    # Decode the inline log tail (last 4KB)
    import base64
    log_b64 = resp.get("LogResult", "")
    if log_b64:
        out["invoke"]["log_tail"] = base64.b64decode(log_b64).decode("utf-8", errors="replace")[-3000:]

    # After invoke, re-check candidates
    time.sleep(3)
    out["s3_after_invoke"] = {}
    for k in candidates:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=k)
            data = json.loads(obj["Body"].read())
            msg = data.get("message", "") if isinstance(data, dict) else ""
            out["s3_after_invoke"][k] = {
                "age_min": round((datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()/60, 1),
                "msg_len": len(msg),
                "has_fed": "🏦 Fed Speak" in msg,
                "has_global": "🌍 Global Macro" in msg,
                "fed_excerpt": "",
                "global_excerpt": "",
            }
            for marker, key in (("🏦 Fed Speak", "fed_excerpt"),
                                  ("🌍 Global Macro", "global_excerpt")):
                idx = msg.find(marker)
                if idx >= 0:
                    out["s3_after_invoke"][k][key] = msg[idx:idx+450]
        except Exception as e:
            out["s3_after_invoke"][k] = {"err": str(e)[:80]}

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:7000])


if __name__ == "__main__":
    main()
