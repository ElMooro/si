#!/usr/bin/env python3
"""Step 486 — Dump options-flow `all_qualifying` array + `stats` so we know
exact schema to ingest in alpha-score."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/486_options_flow_schema.json"
NAME = "justhodl-tmp-486"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, boto3
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/options-flow.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["top_keys"] = list(p.keys())
        out["stats"] = p.get("stats")
        out["summary"] = p.get("summary")
        aq = p.get("all_qualifying") or []
        out["n_qualifying"] = len(aq)
        out["first_5"] = aq[:5]
        # Look for high-score examples
        scored = [x for x in aq if isinstance(x, dict) and x.get("score", 0) >= 50]
        out["n_score_50_plus"] = len(scored)
        out["sample_tier_A"] = [x for x in aq if x.get("tier") == "TIER_A_BULLISH_FLOW"][:5]
        out["sample_tier_B"] = [x for x in aq if x.get("tier") == "TIER_B_FLOW_BUILDING"][:3]
        # Distribution
        by_tier = {}
        for x in aq:
            t = x.get("tier","?")
            by_tier[t] = by_tier.get(t, 0) + 1
        out["tier_distribution"] = by_tier
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
        out["raw"] = body[:30000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
