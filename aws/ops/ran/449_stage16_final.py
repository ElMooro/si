#!/usr/bin/env python3
"""Step 449 — Final Stage 16 verification:
  1. Confirm S3 sidecar schema matches what screener/index.html expects
  2. Sample-join 10 well-known stocks → confirm enrichment works
  3. Check page reachability via GitHub Pages
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/449_stage16_final.json"
NAME = "justhodl-tmp-449"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
import boto3
s3 = boto3.client("s3", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Fetch sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/smart-money-holdings.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body)/1024, 1),
            "generated_at": p.get("generated_at"),
            "as_of_quarter": p.get("as_of_quarter"),
            "n_symbols": p.get("n_symbols"),
            "n_funds_scanned": p.get("n_funds_scanned"),
            "n_funds_attempted": p.get("n_funds_attempted"),
            "elapsed_seconds": p.get("elapsed_seconds"),
            "top_keys": list(p.keys()),
        }
        holdings = p.get("holdings") or {}
        funds = p.get("funds") or []
        # Schema verification: ensure shape matches page expectations
        out["schema_check"] = {
            "holdings_is_dict": isinstance(holdings, dict),
            "funds_is_list": isinstance(funds, list),
            "holdings_value_is_list": isinstance(next(iter(holdings.values())), list) if holdings else None,
            "holder_keys": list(next(iter(holdings.values()))[0].keys()) if holdings and next(iter(holdings.values())) else None,
            "fund_keys": list(funds[0].keys()) if funds else None,
        }
        # 2. Sample-join — test 12 well-known stocks
        watch = ["AAPL","MSFT","NVDA","GOOGL","META","AMZN","TSLA","JPM","BAC","KO","AXP","OXY","BRK.B","BRKB","SPY","QQQ"]
        joins = {}
        for sym in watch:
            h = holdings.get(sym) or []
            joins[sym] = {
                "smN": len(h),
                "smValue_b": round(sum(x.get("value",0) for x in h) / 1e9, 2),
                "top3": [{"name": x.get("name"), "value_b": round(x.get("value",0)/1e9, 2)} for x in h[:3]],
            }
        out["sample_joins"] = joins
        # 3. Funds that failed (returned 0 holdings)
        attempted_ciks = set()  # we cant know without re-running, just list funds we got
        out["successful_funds"] = [{"name": f.get("name"), "n_holdings": f.get("n_holdings"),
                                       "total_value_b": round((f.get("total_value") or 0)/1e9, 1)}
                                      for f in funds]
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    # 4. Page reachability
    try:
        r = urllib.request.urlopen("https://justhodl.ai/screener/", timeout=15)
        out["screener_page"] = {"status": r.status, "size_kb": round(len(r.read())/1024, 1)}
    except Exception as e:
        out["screener_page_err"] = str(e)[:200]

    # 5. Sidecar public reachability
    try:
        url = "https://justhodl-dashboard-live.s3.amazonaws.com/screener/smart-money-holdings.json"
        r = urllib.request.urlopen(url, timeout=15)
        out["sidecar_public"] = {"status": r.status, "size_kb": round(len(r.read())/1024, 1)}
    except Exception as e:
        out["sidecar_public_err"] = str(e)[:200]

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
                            MemorySize=256, Timeout=120, Code={"ZipFile": zb})
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
