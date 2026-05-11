#!/usr/bin/env python3
"""Step 453 — Verify Stage 16.2:
  1. Wait for deploy
  2. Force-invoke smart-money-holdings (now 35 funds + filter)
  3. Check S3 sidecar size, funds successful, symbols covered
  4. Show all funds with their holding counts (which new funds got data)
  5. Show top flagships (should be more diverse now: Icahn/Starboard/Anchorage)
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/453_stage16_2_verify.json"
NAME = "justhodl-tmp-453"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-smart-money-holdings")
        out["lambda"] = {"last_modified": cfg["LastModified"], "code_size": cfg["CodeSize"],
                         "memory": cfg["MemorySize"], "timeout": cfg["Timeout"]}
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # Force-invoke
    try:
        resp = lam.invoke(
            FunctionName="justhodl-smart-money-holdings",
            InvocationType="RequestResponse",
            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        parsed = json.loads(body)
        inner = json.loads(parsed["body"]) if parsed.get("body") else parsed
        out["invoke"] = inner
    except Exception as e:
        out["invoke_err"] = str(e)[:300]

    # Read sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="screener/smart-money-holdings.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["s3_size_kb"] = round(len(body)/1024, 1)
        out["s3_size_mb"] = round(len(body)/1024/1024, 2)
        out["n_symbols"] = p.get("n_symbols")
        out["n_funds_scanned"] = p.get("n_funds_scanned")
        out["n_funds_attempted"] = p.get("n_funds_attempted")
        funds = p.get("funds") or []
        # All funds with holding counts
        out["all_funds"] = [{
            "cik": f.get("cik"),
            "name": f.get("name"),
            "n_holdings": f.get("n_holdings"),
            "total_value_b": round((f.get("total_value") or 0)/1e9, 2),
        } for f in funds]

        # Conviction stats
        holdings = p.get("holdings") or {}
        flagship = sum(1 for e in holdings.values() if isinstance(e, dict) and (e.get("max_pct_of_fund") or 0) >= 10)
        high_conv = sum(1 for e in holdings.values() if isinstance(e, dict) and (e.get("max_pct_of_fund") or 0) >= 5)
        notable = sum(1 for e in holdings.values() if isinstance(e, dict) and (e.get("max_pct_of_fund") or 0) >= 1)
        out["concentration_stats"] = {
            "flagship_10pct_plus": flagship,
            "high_conviction_5pct_plus": high_conv,
            "notable_1pct_plus": notable,
        }
        # Top 25 flagships
        flagships = []
        for sym, e in holdings.items():
            if not isinstance(e, dict): continue
            mp = e.get("max_pct_of_fund")
            if mp is not None and mp >= 10:
                hs = e.get("holders") or []
                hs_sorted = sorted(hs, key=lambda h: -(h.get("pct_of_fund") or 0))
                top = hs_sorted[0] if hs_sorted else None
                flagships.append({
                    "sym": sym, "max_pct": mp,
                    "n_high": e.get("n_high_conviction"),
                    "n_flag": e.get("n_flagship"),
                    "top_holder": (top.get("name") if top else None),
                    "top_pct": (top.get("pct_of_fund") if top else None),
                    "top_value_b": round((top.get("value") or 0)/1e9, 2) if top else None,
                })
        flagships.sort(key=lambda f: -f["max_pct"])
        out["flagships_top25"] = flagships[:25]
    except Exception as e:
        out["s3_err"] = str(e)[:200]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 90s for deploy...")
    time.sleep(90)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=512, Timeout=600, Code={"ZipFile": zb})
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
        out["raw"] = body[:20000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
