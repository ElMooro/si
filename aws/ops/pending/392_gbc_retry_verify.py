#!/usr/bin/env python3
"""Step 392 — Verify retry fix improved classification coverage.
Invoke Lambda fresh, read S3 output, show before/after for the 5 transient
failures (BRA, ESP, GBR, MEX, TUR)."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/392_gbc_retry_verify.json"
NAME = "justhodl-tmp-gbc-retry-verify"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, time
import boto3

lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3",     region_name="us-east-1")
logs = boto3.client("logs",  region_name="us-east-1")

TARGET = "justhodl-global-business-cycle"

def lambda_handler(event, context):
    out = {}

    # 1. Confirm last_modified is recent (post-retry-fix push)
    try:
        cfg = lam.get_function_configuration(FunctionName=TARGET)
        out["lambda_last_modified"] = cfg["LastModified"]
        out["code_size"] = cfg["CodeSize"]
    except Exception as e:
        out["lambda_err"] = str(e)[:200]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # 2. Fresh sync invoke
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

    time.sleep(3)

    # 3. Read fresh S3 output
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/global-business-cycle.json")
        d = json.loads(obj["Body"].read())
        agg = d.get("aggregate", {})
        by_country = d.get("by_country", {})
        n_class = sum(1 for c in by_country.values()
                        if c.get("phase") and c["phase"] != "UNKNOWN")

        # Focus on previously-broken 5
        focus = ["BRA","ESP","GBR","MEX","TUR"]
        focus_state = {}
        for iso3 in focus:
            c = by_country.get(iso3, {})
            focus_state[iso3] = {
                "phase": c.get("phase"),
                "cli": c.get("cli_level"),
                "trend": c.get("trend"),
                "history_n": c.get("history_n"),
            }

        out["s3"] = {
            "generated_at": d.get("generated_at"),
            "elapsed_sec": d.get("elapsed_sec"),
            "global_phase": agg.get("global_phase"),
            "global_avg_cli": agg.get("global_avg_cli"),
            "n_classified": n_class,
            "n_total": len(by_country),
            "coverage_pct": agg.get("classification_coverage_pct"),
            "expansion_breadth": agg.get("expansion_breadth_pct"),
            "contraction_breadth": agg.get("contraction_breadth_pct"),
            "phase_mix": agg.get("global_phase_mix_pct"),
            "still_unknown": [iso for iso, c in by_country.items()
                                if c.get("phase") == "UNKNOWN"],
            "focus_5": focus_state,
        }
    except Exception as e:
        out["s3"] = {"error": str(e)[:300]}

    # 4. Grab last 60 CloudWatch log lines to see retry behavior
    try:
        log_group = f"/aws/lambda/{TARGET}"
        streams = logs.describe_log_streams(
            logGroupName=log_group, orderBy="LastEventTime",
            descending=True, limit=1)
        if streams.get("logStreams"):
            stream_name = streams["logStreams"][0]["logStreamName"]
            events = logs.get_log_events(
                logGroupName=log_group, logStreamName=stream_name,
                startFromHead=False, limit=80)
            lines = [e["message"].strip() for e in events.get("events", [])]
            # Filter for retry attempts
            retry_lines = [l for l in lines if "attempt" in l or "EXHAUSTED" in l]
            out["retry_log_lines"] = retry_lines[-30:]
        else:
            out["retry_log_lines"] = ["no log stream"]
    except Exception as e:
        out["retry_log_lines"] = [f"err: {e}"[:200]]

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
        out["raw"] = body[:6000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
