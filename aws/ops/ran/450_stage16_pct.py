#!/usr/bin/env python3
"""Step 450 — Verify Stage 16.1:
  1. Wait for new Lambda code to deploy
  2. Force-invoke to rebuild sidecar with pct_of_fund
  3. Show flagship positions (any stock that's >=10% of any fund's portfolio)
  4. Show high-conviction stocks (>=5% of any fund)
  5. Sample 16 well-known stocks → show their max conviction + best holder
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/450_stage16_pct.json"
NAME = "justhodl-tmp-450"
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
        out["lambda"] = {"last_modified": cfg["LastModified"], "code_size": cfg["CodeSize"]}
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # Force-invoke synchronously to rebuild sidecar with pct_of_fund
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

    # Read sidecar and analyze
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="screener/smart-money-holdings.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["s3_size_kb"] = round(len(body)/1024, 1)
        holdings = p.get("holdings") or {}
        out["n_symbols"] = len(holdings)
        out["sample_entry"] = None
        # Schema check: should be {holders, max_pct_of_fund, ...}
        if holdings:
            first_key = next(iter(holdings))
            first_val = holdings[first_key]
            out["first_entry_shape"] = {
                "key": first_key,
                "type": type(first_val).__name__,
                "keys": list(first_val.keys()) if isinstance(first_val, dict) else None,
            }
            if isinstance(first_val, dict):
                out["first_entry_shape"]["holder_keys"] = (
                    list(first_val.get("holders", [{}])[0].keys()) if first_val.get("holders") else None)

        # Find FLAGSHIP positions (any stock that's >=10% of some fund)
        flagships = []
        for sym, e in holdings.items():
            if not isinstance(e, dict): continue
            mp = e.get("max_pct_of_fund")
            if mp is not None and mp >= 10:
                # Find the holder with max pct
                hs = e.get("holders") or []
                hs_sorted = sorted(hs, key=lambda h: -(h.get("pct_of_fund") or 0))
                top = hs_sorted[0] if hs_sorted else None
                flagships.append({
                    "sym": sym, "max_pct": mp,
                    "n_high_conv": e.get("n_high_conviction"),
                    "n_flagship": e.get("n_flagship"),
                    "top_holder": top.get("name") if top else None,
                    "top_pct": top.get("pct_of_fund") if top else None,
                    "top_value_b": round((top.get("value") or 0)/1e9, 2) if top else None,
                })
        flagships.sort(key=lambda f: -f["max_pct"])
        out["flagship_count"] = len(flagships)
        out["flagships_top20"] = flagships[:20]

        # High-conviction count (>=5%)
        hc_count = sum(1 for e in holdings.values()
                          if isinstance(e, dict) and (e.get("max_pct_of_fund") or 0) >= 5)
        out["high_conviction_count"] = hc_count

        # Sample 16 well-known stocks
        watch = ["AAPL","MSFT","NVDA","GOOGL","META","AMZN","TSLA","JPM",
                  "BAC","KO","AXP","OXY","DPZ","CMG","BABA","CRWD"]
        samples = {}
        for sym in watch:
            e = holdings.get(sym)
            if not isinstance(e, dict):
                samples[sym] = None
                continue
            hs = e.get("holders") or []
            hs_sorted = sorted(hs, key=lambda h: -(h.get("pct_of_fund") or 0))
            top = hs_sorted[0] if hs_sorted else None
            samples[sym] = {
                "n_holders": len(hs),
                "max_pct": e.get("max_pct_of_fund"),
                "n_high_conv": e.get("n_high_conviction"),
                "n_flagship": e.get("n_flagship"),
                "top_holder": top.get("name") if top else None,
                "top_holder_pct": top.get("pct_of_fund") if top else None,
                "top_holder_value_b": round((top.get("value") or 0)/1e9, 2) if top else None,
            }
        out["samples"] = samples
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
                            MemorySize=512, Timeout=300, Code={"ZipFile": zb})
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
        out["raw"] = body[:10000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
