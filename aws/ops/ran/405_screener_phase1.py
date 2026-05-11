#!/usr/bin/env python3
"""Step 405 — Verify Stage 1 of screener overhaul:
  - Trigger justhodl-stock-screener Lambda
  - Confirm new fields land in S3 data.json
  - Confirm page has the 6 new tabs + Fundamentals column group
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/405_screener_phase1.json"
NAME = "justhodl-tmp-screener-p1"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request, time
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3  = boto3.client("s3",     region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
TARGET = "justhodl-stock-screener"

def fetch(url, t=20):
    req = urllib.request.Request(url, headers={"User-Agent":"JH/1.0"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read().decode("utf-8", errors="replace"), r.status

def lambda_handler(event, context):
    out = {}

    # 1. Lambda metadata
    cfg = lam.get_function_configuration(FunctionName=TARGET)
    out["lambda"] = {
        "last_modified": cfg["LastModified"],
        "code_size": cfg["CodeSize"],
        "timeout": cfg["Timeout"],
        "memory": cfg["MemorySize"],
    }

    # 2. Read CURRENT data.json to see if Phase 1 fields are present
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="screener/data.json")
        body = obj["Body"].read()
        data = json.loads(body)
        stocks = data.get("stocks") or []
        out["current_data"] = {
            "generated_at": data.get("generated_at"),
            "n_stocks": len(stocks),
            "size_kb": round(len(body) / 1024, 1),
        }
        # Check for new fields in first stock with revenue data
        sample = next((s for s in stocks if s.get("revenue") is not None), stocks[0] if stocks else {})
        out["current_data"]["sample_symbol"] = sample.get("symbol")
        out["current_data"]["has_new_fields"] = {
            "revenue":          sample.get("revenue"),
            "netIncome":        sample.get("netIncome"),
            "operatingIncome":  sample.get("operatingIncome"),
            "ebitda":           sample.get("ebitda"),
            "freeCashFlow":     sample.get("freeCashFlow"),
            "fcfYieldCalc":     sample.get("fcfYieldCalc"),
            "buybackYield":     sample.get("buybackYield"),
            "rev3yCAGR":        sample.get("rev3yCAGR"),
            "sustainable3y":    sample.get("sustainable3y"),
            "sustainableQuality": sample.get("sustainableQuality"),
        }
    except Exception as e:
        out["current_data"] = {"error": str(e)[:300]}

    # 3. Trigger the Lambda — async invoke (will take a few minutes; we just confirm
    # the new code is deployed and the next scheduled run will pick up new fields)
    try:
        resp = lam.invoke(FunctionName=TARGET, InvocationType="Event",
                           Payload=json.dumps({"force": False}).encode())
        out["invoke"] = {"status": resp.get("StatusCode")}
    except Exception as e:
        out["invoke"] = {"error": str(e)[:200]}

    # 4. Page check
    try:
        page, status = fetch("https://justhodl.ai/screener/?cb=" + str(int(time.time())))
        out["page"] = {
            "status": status,
            "size": len(page),
            "has_money_machines": "MONEY MACHINES" in page,
            "has_sustainable":    "SUSTAINABLE PROFITS" in page,
            "has_margins":        "HIGHEST MARGINS" in page,
            "has_rev_growth":     "REVENUE GROWTH KINGS" in page,
            "has_cash_gen":       "CASH GENERATORS" in page,
            "has_buyback":        "BUYBACK ACTIVE" in page,
            "has_fundamentals":   ">Fundamentals<" in page,
            "has_state_key":      "justhodl.screener.state.v2" in page,
            "has_restoreState":   "function restoreState" in page,
            "has_TAB_DEFAULT_SORT": "TAB_DEFAULT_SORT" in page,
        }
    except Exception as e:
        out["page"] = {"error": str(e)[:200]}

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
        out["raw"] = body[:6000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
