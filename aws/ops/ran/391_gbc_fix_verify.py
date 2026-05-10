#!/usr/bin/env python3
"""Step 391 — Verify GBC fix landed end-to-end.
- Confirm Lambda code updated (last_modified after fix push 5ba7a97)
- Invoke fresh + read S3 output
- Check avg_cli is now sane (~99.5 not 49.07)
- Check classification coverage improved (was 18/35)
- Check EventBridge schedule was set up by workflow
- Show per-country detail with phase, CLI, trend"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/391_gbc_fix_verify.json"
NAME = "justhodl-tmp-gbc-fix-verify"
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

def lambda_handler(event, context):
    out = {}

    # 1. Lambda metadata
    try:
        cfg = lam.get_function_configuration(FunctionName=TARGET)
        out["lambda"] = {
            "exists": True, "runtime": cfg["Runtime"],
            "timeout": cfg["Timeout"], "memory": cfg["MemorySize"],
            "last_modified": cfg["LastModified"],
            "code_size": cfg["CodeSize"],
        }
    except Exception as e:
        out["lambda"] = {"error": str(e)[:300]}

    # 2. EventBridge rule
    try:
        r = events.describe_rule(Name=RULE)
        out["rule"] = {
            "name": r["Name"],
            "state": r["State"],
            "schedule": r["ScheduleExpression"],
        }
        t = events.list_targets_by_rule(Rule=RULE)
        out["rule_targets"] = [{"id": x["Id"], "arn": x["Arn"]} for x in t.get("Targets", [])]
    except events.exceptions.ResourceNotFoundException:
        out["rule"] = {"error": "RuleNotFound — schedule not set up yet"}
    except Exception as e:
        out["rule"] = {"error": str(e)[:300]}

    # 3. Invoke fresh
    try:
        resp = lam.invoke(FunctionName=TARGET,
                            InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out["invoke"] = {
            "status": resp.get("StatusCode"),
            "error": resp.get("FunctionError"),
            "body": body[:400],
        }
    except Exception as e:
        out["invoke"] = {"error": str(e)[:300]}

    time.sleep(3)

    # 4. Fresh S3 output
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/global-business-cycle.json")
        d = json.loads(obj["Body"].read())
        agg = d.get("aggregate", {})
        by_country = d.get("by_country", {})
        n_classified = sum(1 for c in by_country.values()
                              if c.get("phase") and c["phase"] != "UNKNOWN")
        n_total = len(by_country)

        # Per-country detail
        per_country = []
        for iso3, c in sorted(by_country.items()):
            per_country.append({
                "iso3": iso3,
                "name": c.get("country_name"),
                "phase": c.get("phase"),
                "cli": c.get("cli_level"),
                "trend": c.get("trend"),
                "comp_period": c.get("comp_period"),
                "six_m": c.get("six_month_change"),
                "z_5y": c.get("z_5y"),
                "latest_date": c.get("latest_date"),
                "history_n": c.get("history_n"),
            })

        out["s3"] = {
            "generated_at": d.get("generated_at"),
            "elapsed_sec": d.get("elapsed_sec"),
            "n_classified": n_classified,
            "n_total": n_total,
            "coverage_pct": agg.get("classification_coverage_pct"),
            "global_phase": agg.get("global_phase"),
            "global_avg_cli": agg.get("global_avg_cli"),
            "expansion_breadth_pct": agg.get("expansion_breadth_pct"),
            "contraction_breadth_pct": agg.get("contraction_breadth_pct"),
            "phase_mix_pct": agg.get("global_phase_mix_pct"),
            "decisive_call": (d.get("interpretation") or {}).get("decisive_call"),
            "per_country": per_country,
        }
    except Exception as e:
        out["s3"] = {"error": str(e)[:300]}

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
        out["parse_err"] = str(e)
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
