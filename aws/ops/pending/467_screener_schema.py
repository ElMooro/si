#!/usr/bin/env python3
"""Step 467 — Inspect screener/data.json schema.
Goal: enumerate all fields per stock + sample 3 well-known stocks to see
what data is actually available for Alpha Score Engine input.
"""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/467_screener_schema.json"
NAME = "justhodl-tmp-467"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
        body = obj["Body"].read()
        out["size_kb"] = round(len(body)/1024, 1)
        p = json.loads(body)
        stocks = p.get("stocks") or []
        out["n_stocks"] = len(stocks)
        out["top_keys"] = list(p.keys())
        out["generated_at"] = p.get("generated_at")

        # All unique keys across all stocks
        all_keys = set()
        for s in stocks:
            all_keys.update(s.keys())
        out["all_field_keys"] = sorted(all_keys)
        out["n_unique_fields"] = len(all_keys)

        # Sample 5 well-known stocks
        sample_syms = ["NVDA", "AAPL", "MSFT", "JOE", "TSLA"]
        samples = {}
        for sym in sample_syms:
            s = next((x for x in stocks if x.get("symbol") == sym), None)
            if s: samples[sym] = s
        out["samples"] = samples

        # Field coverage stats — how often is each field populated?
        coverage = {}
        for k in all_keys:
            populated = sum(1 for s in stocks if s.get(k) is not None and s.get(k) != "" and s.get(k) != [])
            coverage[k] = round(populated / len(stocks) * 100, 1) if stocks else 0
        out["field_coverage_pct"] = dict(sorted(coverage.items(), key=lambda kv: -kv[1]))

    except Exception as e:
        out["err"] = str(e)[:300]
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
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
