#!/usr/bin/env python3
"""Step 489 — Verify #15 (debate) + #16 (trade journal) bundle.
Invokes both new Lambdas end-to-end and checks DDB state."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/489_debate_journal_verify.json"
NAME = "justhodl-tmp-489"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3, base64
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.client("dynamodb", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

def lambda_handler(event, context):
    out = {"lambdas": {}, "ddb": {}, "schedules": {}, "sidecars": {}, "invokes": {}}

    # ─── 1. Check 3 new Lambdas deployed ───
    NEW = ["justhodl-debate-engine", "justhodl-trade-logger", "justhodl-trade-evaluator"]
    for name in NEW:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            out["lambdas"][name] = {
                "deployed": True,
                "modified": cfg["LastModified"][:19],
                "memory": cfg["MemorySize"], "timeout": cfg["Timeout"],
                "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
            }
        except Exception as e:
            out["lambdas"][name] = {"deployed": False, "err": str(e)[:200]}

    # ─── 2. Check DDB table ───
    try:
        d = ddb.describe_table(TableName="justhodl-trades")
        out["ddb"]["table"] = {
            "exists": True,
            "status": d["Table"]["TableStatus"],
            "item_count": d["Table"]["ItemCount"],
            "size_bytes": d["Table"]["TableSizeBytes"],
            "gsi": [g["IndexName"] for g in d["Table"].get("GlobalSecondaryIndexes", [])],
        }
    except Exception as e:
        out["ddb"]["table"] = {"exists": False, "err": str(e)[:200]}

    # ─── 3. Check EventBridge rules ───
    for r in ["justhodl-debate-engine-daily", "justhodl-trade-logger-hourly",
              "justhodl-trade-evaluator-daily"]:
        try:
            rule = events.describe_rule(Name=r)
            out["schedules"][r] = {"exists": True,
                                     "schedule": rule.get("ScheduleExpression"),
                                     "state": rule.get("State")}
        except Exception: out["schedules"][r] = {"exists": False}

    # ─── 4. Invoke debate engine ───
    try:
        resp = lam.invoke(FunctionName="justhodl-debate-engine",
                            InvocationType="RequestResponse", LogType="Tail",
                            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        out["invokes"]["debate"] = {
            "status": resp.get("StatusCode"),
            "fn_error": resp.get("FunctionError"),
            "response": json.loads(parsed["body"]) if parsed.get("body") else parsed,
        }
        if resp.get("LogResult"):
            out["invokes"]["debate"]["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-1500:]
    except Exception as e:
        out["invokes"]["debate_err"] = str(e)[:300]

    # ─── 5. Invoke trade-logger ───
    try:
        resp = lam.invoke(FunctionName="justhodl-trade-logger",
                            InvocationType="RequestResponse", LogType="Tail",
                            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        out["invokes"]["trade_logger"] = {
            "status": resp.get("StatusCode"),
            "fn_error": resp.get("FunctionError"),
            "response": json.loads(parsed["body"]) if parsed.get("body") else parsed,
        }
        if resp.get("LogResult"):
            out["invokes"]["trade_logger"]["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-1500:]
    except Exception as e:
        out["invokes"]["trade_logger_err"] = str(e)[:300]

    # ─── 6. Invoke trade-evaluator ───
    try:
        resp = lam.invoke(FunctionName="justhodl-trade-evaluator",
                            InvocationType="RequestResponse", LogType="Tail",
                            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        out["invokes"]["trade_evaluator"] = {
            "status": resp.get("StatusCode"),
            "fn_error": resp.get("FunctionError"),
            "response": json.loads(parsed["body"]) if parsed.get("body") else parsed,
        }
        if resp.get("LogResult"):
            out["invokes"]["trade_evaluator"]["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-1500:]
    except Exception as e:
        out["invokes"]["trade_evaluator_err"] = str(e)[:300]

    # ─── 7. Check sidecars ───
    for key in ["data/debate.json", "data/trade-journal.json"]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            body = obj["Body"].read()
            p = json.loads(body)
            out["sidecars"][key] = {
                "exists": True,
                "size_kb": round(len(body) / 1024, 1),
                "last_modified": obj["LastModified"].isoformat()[:19],
                "top_keys": list(p.keys())[:15],
                "version": p.get("version"),
                "summary": p.get("summary"),
                "generated_at": p.get("generated_at"),
                "n_debates": len(p.get("debates") or []) if "debates" in p else None,
                "n_ledger": len(p.get("ledger") or []) if "ledger" in p else None,
                "n_strategies": len(p.get("strategies") or []) if "strategies" in p else None,
                "first_debate_sample": (p.get("debates") or [None])[0] if "debates" in p else None,
            }
        except Exception as e:
            out["sidecars"][key] = {"exists": False, "err": str(e)[:200]}

    # ─── 8. DDB item count after logger run ───
    try:
        resp = ddb.scan(TableName="justhodl-trades", Select="COUNT", Limit=100)
        out["ddb"]["call_count"] = resp.get("Count", 0)
    except Exception as e:
        out["ddb"]["scan_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 200s for deploys to settle...")
    _time.sleep(200)
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
    _time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
