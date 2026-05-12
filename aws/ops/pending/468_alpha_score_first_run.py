#!/usr/bin/env python3
"""Step 468 — Force-invoke alpha-score Lambda + verify output.
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/468_alpha_score_first_run.json"
NAME = "justhodl-tmp-468"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-alpha-score")
        out["lambda"] = {"last_modified": cfg["LastModified"], "code_size": cfg["CodeSize"]}
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # Force-invoke
    resp = lam.invoke(FunctionName="justhodl-alpha-score",
                        InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    parsed = json.loads(body)
    out["invoke"] = json.loads(parsed["body"]) if parsed.get("body") else parsed

    # Read sidecar
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/alpha-score.json")
    body = obj["Body"].read()
    p = json.loads(body)
    out["sidecar"] = {
        "size_kb": round(len(body)/1024, 1),
        "generated_at": p.get("generated_at"),
        "model_version": p.get("model_version"),
        "tier_distribution": p.get("tier_distribution"),
        "weights": p.get("weights"),
        "inputs": p.get("inputs"),
        "elapsed_seconds": p.get("elapsed_seconds"),
        "scored_count": p.get("scored_count"),
    }
    # Top 20 ranked
    stocks = p.get("stocks") or []
    top20 = [s for s in stocks if s.get("rank")][:20]
    out["top20"] = top20
    # Bottom 10 ranked (D-tier)
    bottom = [s for s in stocks if s.get("rank") and s.get("tier") == "D"][:10]
    out["bottom10_D_tier"] = bottom
    # Find some notable picks across tiers
    notable_syms = ["NVDA", "AAPL", "MSFT", "JOE", "TSLA", "GOOGL", "META", "BAC",
                      "SMCI", "WBD", "TTD", "OXY", "AXP", "BRK-B", "VST", "PLTR"]
    out["notable"] = []
    by_sym = {s["symbol"]: s for s in stocks}
    for sym in notable_syms:
        s = by_sym.get(sym)
        if s: out["notable"].append(s)
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 120s for deploy...")
    _time.sleep(120)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=180, Code={"ZipFile": zb})
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
