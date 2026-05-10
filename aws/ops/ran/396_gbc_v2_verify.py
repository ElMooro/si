#!/usr/bin/env python3
"""Step 396 — Verify GBC v2.0 (Yahoo Finance synthetic CLI) deployed + ran.
Invoke fresh, read S3 output, show per-country freshness."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/396_gbc_v2_verify.json"
NAME = "justhodl-tmp-gbc-v2-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, time
import boto3

lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3",     region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")

TARGET = "justhodl-global-business-cycle"

def lambda_handler(event, context):
    out = {}

    # Lambda metadata
    cfg = lam.get_function_configuration(FunctionName=TARGET)
    out["lambda_last_modified"] = cfg["LastModified"]
    out["code_size"] = cfg["CodeSize"]

    # Fresh sync invoke
    try:
        resp = lam.invoke(FunctionName=TARGET, InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out["invoke"] = {
            "status": resp.get("StatusCode"),
            "error": resp.get("FunctionError"),
            "body": body[:400],
        }
    except Exception as e:
        out["invoke"] = {"error": str(e)[:200]}

    time.sleep(4)

    # Read fresh S3 output
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/global-business-cycle.json")
        d = json.loads(obj["Body"].read())
        agg = d.get("aggregate", {})
        by_country = d.get("by_country", {})
        n_class = sum(1 for c in by_country.values()
                        if c.get("phase") and c["phase"] != "UNKNOWN")

        per_country = []
        for iso3, c in sorted(by_country.items()):
            per_country.append({
                "iso3": iso3,
                "name": c.get("country_name"),
                "phase": c.get("phase"),
                "cli": c.get("cli_level"),
                "yhsym": c.get("yahoo_symbol"),
                "ret_12m": c.get("yoy_change"),
                "ret_3m": c.get("three_month_change"),
                "ret_1m": c.get("mom_change"),
                "dist_200ma": c.get("dist_200ma_pct"),
                "trend": c.get("trend"),
                "latest": c.get("latest_date"),
                "ms": c.get("months_stale"),
                "n": c.get("history_n"),
            })

        out["s3"] = {
            "schema_version": d.get("schema_version"),
            "engine_type": d.get("engine_type"),
            "generated_at": d.get("generated_at"),
            "elapsed_sec": d.get("elapsed_sec"),
            "fresh_count": d.get("countries_with_fresh_data"),
            "total": d.get("countries_total"),
            "n_classified": n_class,
            "coverage_pct": agg.get("classification_coverage_pct"),
            "global_phase": agg.get("global_phase"),
            "global_avg_cli": agg.get("global_avg_cli"),
            "expansion_breadth": agg.get("expansion_breadth_pct"),
            "contraction_breadth": agg.get("contraction_breadth_pct"),
            "phase_mix": agg.get("global_phase_mix_pct"),
            "still_unknown": [iso for iso, c in by_country.items()
                                if c.get("phase") == "UNKNOWN"],
            "per_country": per_country,
            "decisive": (d.get("interpretation") or {}).get("decisive_call"),
        }
    except Exception as e:
        out["s3"] = {"error": str(e)[:300]}

    # Tail CloudWatch
    try:
        lg = f"/aws/lambda/{TARGET}"
        streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                              descending=True, limit=1)
        if streams.get("logStreams"):
            stream = streams["logStreams"][0]["logStreamName"]
            ev = logs.get_log_events(logGroupName=lg, logStreamName=stream,
                                      startFromHead=False, limit=50)
            lines = [e["message"].strip() for e in ev.get("events", [])]
            failures = [l for l in lines if "EXHAUSTED" in l or "failed" in l]
            out["log_failures"] = failures[-15:]
            out["log_sample"] = [l for l in lines if "[gbc]" in l][-30:]
    except Exception as e:
        out["log_err"] = str(e)[:200]

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
    except Exception as e:
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
