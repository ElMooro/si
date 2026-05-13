#!/usr/bin/env python3
"""Step 490 — Confirm #15+#16 are still live and show current state."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/490_15_16_status.json"
NAME = "justhodl-tmp-490"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3
s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.client("dynamodb", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    NEW = ["justhodl-debate-engine", "justhodl-trade-logger", "justhodl-trade-evaluator"]
    for name in NEW:
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            out[name] = {"deployed": True, "mod": cfg["LastModified"][:19],
                          "mem": cfg["MemorySize"], "timeout": cfg["Timeout"]}
        except Exception as e:
            out[name] = {"deployed": False, "err": str(e)[:200]}
    # DDB
    try:
        resp = ddb.scan(TableName="justhodl-trades", Select="COUNT")
        out["ddb_call_count"] = resp.get("Count", 0)
    except Exception as e: out["ddb_err"] = str(e)[:200]
    # Sidecars
    out["sidecars"] = {}
    for k in ["data/debate.json", "data/trade-journal.json"]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=k)
            body = obj["Body"].read()
            p = json.loads(body)
            sc = {"exists": True, "size_kb": round(len(body)/1024, 1),
                   "modified": obj["LastModified"].isoformat()[:19],
                   "generated_at": p.get("generated_at"),
                   "version": p.get("version")}
            if "debates" in p:
                sc["n_debates"] = len(p.get("debates") or [])
                sc["summary"] = p.get("summary")
                # Full debates for display
                sc["debates_full"] = p.get("debates")
            if "ledger" in p:
                sc["n_ledger"] = len(p.get("ledger") or [])
                sc["summary"] = p.get("summary")
                sc["strategies"] = p.get("strategies")
                sc["ledger_first_5"] = (p.get("ledger") or [])[:5]
            out["sidecars"][k] = sc
        except Exception as e:
            out["sidecars"][k] = {"exists": False, "err": str(e)[:200]}
    # Daily brief Lambda check
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-alpha-daily-brief")
        out["daily_brief"] = {"deployed": True, "mod": cfg["LastModified"][:19],
                                "env": list((cfg.get("Environment") or {}).get("Variables", {}).keys())}
    except Exception as e:
        out["daily_brief"] = {"deployed": False, "err": str(e)[:200]}
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
                            MemorySize=512, Timeout=60, Code={"ZipFile": zb})
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
