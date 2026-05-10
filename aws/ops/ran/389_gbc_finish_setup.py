#!/usr/bin/env python3
"""Step 389 — Verify justhodl-global-business-cycle Lambda was created by the
deploy workflow. Then set up EventBridge schedule + trigger initial run +
verify S3 output. Uses only events: and lambda:invoke (no PassRole)."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/389_gbc_finish_setup.json"
NAME = "justhodl-tmp-gbc-finish"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, time
import boto3

lam    = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
s3     = boto3.client("s3",     region_name="us-east-1")

TARGET = "justhodl-global-business-cycle"
RULE   = "justhodl-gbc-daily"
ACC    = "857687956942"
BUCKET = "justhodl-dashboard-live"

def lambda_handler(event, context):
    out = {}

    # 1. Confirm Lambda exists (workflow should have created it)
    try:
        cfg = lam.get_function_configuration(FunctionName=TARGET)
        out["lambda_status"] = {
            "exists": True, "runtime": cfg["Runtime"],
            "timeout": cfg["Timeout"], "memory": cfg["MemorySize"],
            "last_modified": cfg["LastModified"],
            "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
        }
    except Exception as e:
        out["lambda_status"] = {"exists": False, "error": str(e)[:300]}
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # 2. EventBridge schedule (daily 12:00 UTC — CLI is monthly so daily is plenty)
    try:
        events.put_rule(
            Name=RULE,
            ScheduleExpression="cron(0 12 * * ? *)",
            State="ENABLED",
            Description="Daily justhodl-global-business-cycle (OECD CLI across 35 economies)",
        )
        out["rule"] = "created/updated"
    except Exception as e:
        out["rule"] = f"error: {e}"[:200]

    # 3. Grant EventBridge permission to invoke (no PassRole needed for this)
    try:
        lam.add_permission(
            FunctionName=TARGET,
            StatementId="EventBridge-gbc-daily",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{RULE}",
        )
        out["permission"] = "granted"
    except lam.exceptions.ResourceConflictException:
        out["permission"] = "already exists"
    except Exception as e:
        out["permission"] = f"error: {e}"[:200]

    # 4. Attach Lambda as target
    try:
        events.put_targets(
            Rule=RULE,
            Targets=[{"Id": "target1",
                       "Arn": f"arn:aws:lambda:us-east-1:{ACC}:function:{TARGET}"}],
        )
        out["target"] = "attached"
    except Exception as e:
        out["target"] = f"error: {e}"[:200]

    # 5. Initial sync invoke
    try:
        resp = lam.invoke(FunctionName=TARGET,
                            InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out["initial_invoke"] = {
            "status": resp.get("StatusCode"),
            "error": resp.get("FunctionError"),
            "body": body[:500],
        }
    except Exception as e:
        out["initial_invoke"] = {"error": str(e)[:300]}

    time.sleep(3)

    # 6. Verify S3 output
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/global-business-cycle.json")
        d = json.loads(obj["Body"].read())
        agg = d.get("aggregate", {})
        by_country = d.get("by_country", {})
        n_avail = sum(1 for c in by_country.values()
                       if c.get("phase") and c["phase"] != "UNKNOWN")
        out["s3_output"] = {
            "generated_at": d.get("generated_at"),
            "elapsed_sec": d.get("elapsed_sec"),
            "global_phase": agg.get("global_phase"),
            "global_avg_cli": agg.get("global_avg_cli"),
            "expansion_breadth_pct": agg.get("expansion_breadth_pct"),
            "contraction_breadth_pct": agg.get("contraction_breadth_pct"),
            "phase_mix": agg.get("global_phase_mix_pct"),
            "total_weight_covered": agg.get("total_weight_covered"),
            "n_total": len(by_country),
            "n_classified": n_avail,
            "key_countries": {iso: {"phase": (by_country.get(iso) or {}).get("phase"),
                                       "cli": (by_country.get(iso) or {}).get("cli_level"),
                                       "6m": (by_country.get(iso) or {}).get("six_month_change")}
                                for iso in ["USA","CHN","DEU","JPN","IND","GBR","FRA",
                                              "BRA","KOR","CAN","AUS","MEX","IDN","TUR","KOR",
                                              "ESP","ITA","NLD","CHE","POL"]},
            "decisive_call": (d.get("interpretation") or {}).get("decisive_call"),
            "cross_asset_signals": {k: v.get("signal")
                                       for k, v in ((d.get("interpretation") or {}).get("cross_asset") or {}).items()},
        }
    except Exception as e:
        out["s3_output"] = {"error": str(e)[:300]}

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
        out["raw"] = body[:5000]
        out["parse_err"] = str(e)
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
