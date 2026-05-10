#!/usr/bin/env python3
"""Step 398 — Final-final verify: 34/34 after CZE fallbacks."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
REPORT = "aws/ops/reports/398_gbc_finalfinal.json"
NAME = "justhodl-tmp-gbc-ff"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    lam.invoke(FunctionName="justhodl-global-business-cycle",
                 InvocationType="RequestResponse", Payload=b"{}")
    time.sleep(3)
    obj = s3.get_object(Bucket="justhodl-dashboard-live",
                          Key="data/global-business-cycle.json")
    d = json.loads(obj["Body"].read())
    by_country = d.get("by_country", {})

    # Build per-country with months_stale and ticker info
    detail = []
    for iso3 in sorted(by_country.keys()):
        c = by_country[iso3]
        detail.append({
            "iso3": iso3,
            "phase": c.get("phase"),
            "cli": c.get("cli_level"),
            "ticker_used": c.get("yahoo_symbol"),
            "ticker_primary": c.get("yahoo_symbol_primary"),
            "ms": c.get("months_stale"),
            "latest": c.get("latest_date"),
        })
    return {"statusCode": 200, "body": json.dumps({
        "fresh_count": d.get("countries_with_fresh_data"),
        "total": d.get("countries_total"),
        "still_unknown": [iso for iso, c in by_country.items()
                            if c.get("phase") == "UNKNOWN"],
        "global_phase": d.get("aggregate", {}).get("global_phase"),
        "global_avg_cli": d.get("aggregate", {}).get("global_avg_cli"),
        "phase_mix": d.get("aggregate", {}).get("global_phase_mix_pct"),
        "decisive": (d.get("interpretation") or {}).get("decisive_call"),
        "detail": detail,
    }, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=300, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:6000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
