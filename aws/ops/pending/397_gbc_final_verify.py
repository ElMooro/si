#!/usr/bin/env python3
"""Step 397 — Final verify: invoke GBC v2.0 (with symbol fallbacks)
and confirm all 34 countries now have fresh ≤3mo data."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/397_gbc_final_verify.json"
NAME = "justhodl-tmp-gbc-final"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, time
import boto3

lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3",     region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    cfg = lam.get_function_configuration(FunctionName="justhodl-global-business-cycle")
    out["lambda_last_modified"] = cfg["LastModified"]

    resp = lam.invoke(FunctionName="justhodl-global-business-cycle",
                       InvocationType="RequestResponse", Payload=b"{}")
    out["invoke_body"] = resp["Payload"].read().decode("utf-8")[:300]

    time.sleep(3)

    obj = s3.get_object(Bucket="justhodl-dashboard-live",
                          Key="data/global-business-cycle.json")
    d = json.loads(obj["Body"].read())
    agg = d.get("aggregate", {})
    by_country = d.get("by_country", {})

    # Focus on the 4 previously-broken
    focus = ["POL","CZE","HUN","CHL"]
    focus_state = {}
    for iso3 in focus:
        c = by_country.get(iso3, {})
        focus_state[iso3] = {
            "phase": c.get("phase"),
            "cli": c.get("cli_level"),
            "ticker_primary": c.get("yahoo_symbol_primary"),
            "ticker_used": c.get("yahoo_symbol"),
            "latest": c.get("latest_date"),
            "months_stale": c.get("months_stale"),
            "ret_12m": c.get("yoy_change"),
            "ret_3m": c.get("three_month_change"),
        }

    # Top-level summary
    out["summary"] = {
        "schema_version": d.get("schema_version"),
        "engine_type": d.get("engine_type"),
        "generated_at": d.get("generated_at"),
        "fresh_count": d.get("countries_with_fresh_data"),
        "total": d.get("countries_total"),
        "global_phase": agg.get("global_phase"),
        "global_avg_cli": agg.get("global_avg_cli"),
        "expansion_breadth": agg.get("expansion_breadth_pct"),
        "phase_mix": agg.get("global_phase_mix_pct"),
        "still_unknown": [iso for iso, c in by_country.items()
                            if c.get("phase") == "UNKNOWN"],
        "focus_4": focus_state,
        "decisive": (d.get("interpretation") or {}).get("decisive_call"),
    }
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
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
