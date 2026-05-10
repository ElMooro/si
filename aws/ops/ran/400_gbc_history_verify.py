#!/usr/bin/env python3
"""Step 400 — trigger GBC v2.0 (now with history) and verify:
  - Both S3 outputs present
  - history shape correct (n_periods, per-country points, aggregate dates)
  - /global-cycle/ page is live and contains the new structure
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/400_gbc_history_verify.json"
NAME = "justhodl-tmp-gbc-history"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, time
import boto3

lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3",     region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
TARGET = "justhodl-global-business-cycle"


def fetch(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent":"JH-verify/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace"), r.status


def lambda_handler(event, context):
    out = {}

    # 1. Lambda metadata + invoke
    cfg = lam.get_function_configuration(FunctionName=TARGET)
    out["lambda"] = {
        "last_modified": cfg["LastModified"],
        "code_size": cfg["CodeSize"],
        "timeout": cfg["Timeout"],
        "memory": cfg["MemorySize"],
    }
    resp = lam.invoke(FunctionName=TARGET, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    out["invoke"] = {
        "status": resp.get("StatusCode"),
        "error": resp.get("FunctionError"),
        "body": body[:400],
    }

    time.sleep(3)

    # 2. Live JSON (unchanged schema)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/global-business-cycle.json")
        d = json.loads(obj["Body"].read())
        agg = d.get("aggregate", {})
        out["live"] = {
            "schema": d.get("schema_version"),
            "fresh_count": d.get("countries_with_fresh_data"),
            "total": d.get("countries_total"),
            "global_phase": agg.get("global_phase"),
            "global_avg_cli": agg.get("global_avg_cli"),
        }
    except Exception as e:
        out["live"] = {"error": str(e)[:200]}

    # 3. History JSON (new output)
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/global-business-cycle-history.json")
        body = obj["Body"].read()
        h = json.loads(body)
        agg_hist = h.get("aggregate", [])
        bc = h.get("by_country", {})

        # Per-country summary
        per = []
        for iso3, info in sorted(bc.items()):
            per.append({
                "iso3": iso3,
                "name": info.get("country_name"),
                "n_points": info.get("n_points"),
                "first": info.get("first_date"),
                "last": info.get("last_date"),
                "last_cli": (info.get("history") or [{}])[-1].get("cli"),
                "last_phase": (info.get("history") or [{}])[-1].get("phase"),
            })

        out["history"] = {
            "schema": h.get("schema_version"),
            "engine_type": h.get("engine_type"),
            "generated_at": h.get("generated_at"),
            "frequency": h.get("frequency"),
            "elapsed_sec": h.get("history_elapsed_sec"),
            "countries_count": h.get("countries_count"),
            "aggregate_n_dates": len(agg_hist),
            "aggregate_first": agg_hist[0] if agg_hist else None,
            "aggregate_last": agg_hist[-1] if agg_hist else None,
            "aggregate_sample_middle": agg_hist[len(agg_hist)//2] if agg_hist else None,
            "size_bytes": len(body),
            "per_country_summary": per,
        }
    except Exception as e:
        out["history"] = {"error": str(e)[:300]}

    # 4. Live page check
    try:
        page, status = fetch("https://justhodl.ai/global-cycle/?cb=" + str(int(time.time())))
        out["history_page"] = {
            "status": status,
            "size": len(page),
            "has_phaseMixChart": "phaseMixChart" in page,
            "has_globalCliChart": "globalCliChart" in page,
            "has_sparkGrid": "sparkGrid" in page,
            "has_compareChart": "compareChart" in page,
            "has_history_json_src": "global-business-cycle-history.json" in page,
            "title_match": "Global Business Cycle" in page and "History" in page,
        }
    except Exception as e:
        out["history_page"] = {"error": str(e)[:300]}

    # 5. CloudWatch tail
    try:
        lg = f"/aws/lambda/{TARGET}"
        streams = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime",
                                              descending=True, limit=1)
        if streams.get("logStreams"):
            stream = streams["logStreams"][0]["logStreamName"]
            ev = logs.get_log_events(logGroupName=lg, logStreamName=stream,
                                      startFromHead=False, limit=60)
            lines = [e["message"].strip() for e in ev.get("events", [])]
            out["log_history_lines"] = [l for l in lines if "[gbc-history]" in l][-25:]
            out["log_failures"] = [l for l in lines if "EXHAUSTED" in l or "ERROR" in l or "failed" in l][-10:]
    except Exception as e:
        out["log_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
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
                            MemorySize=512, Timeout=900, Code={"ZipFile": zb})
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
        out["raw"] = body[:8000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
