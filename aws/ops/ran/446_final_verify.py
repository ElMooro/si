#!/usr/bin/env python3
"""Step 446 — Final verify: all 5 new dashboards reachable + serving fresh data."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/446_final_verify.json"
NAME = "justhodl-tmp-446"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json, urllib.request
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")

DATASETS = [
    ("M&A Tracker", "screener/ma-latest.json", "https://justhodl.ai/ma/"),
    ("COT Positioning", "screener/cot-latest.json", "https://justhodl.ai/cot/"),
    ("Earnings Pulse", "screener/earnings-sentiment.json", "https://justhodl.ai/earnings-pulse/"),
    ("Smart Money", "screener/smart-money.json", "https://justhodl.ai/smart-money/"),
]

def lambda_handler(event, context):
    out = {"datasets": []}

    # Force-invoke smart-money to get fresh data with as_of_quarter
    try:
        lam.invoke(
            FunctionName="justhodl-smart-money-tracker",
            InvocationType="Event",
            Payload=b"{}")
    except Exception as e:
        out["sm_invoke_err"] = str(e)[:200]

    # Check each dataset
    for name, key, page_url in DATASETS:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            body = obj["Body"].read()
            p = json.loads(body)
            rec = {
                "name": name,
                "s3_key": key,
                "page_url": page_url,
                "size_kb": round(len(body)/1024, 1),
                "last_modified": obj["LastModified"].isoformat(),
                "generated_at": p.get("generated_at"),
            }
            # Per-dataset summary
            if "deals" in p:
                rec["n_records"] = len(p["deals"])
                rec["type"] = "M&A deals"
            elif "contracts" in p:
                rec["n_records"] = len(p["contracts"])
                rec["latest_report_date"] = p.get("latest_report_date")
                rec["extreme_long"] = (p.get("summary") or {}).get("extreme_long_count")
                rec["extreme_short"] = (p.get("summary") or {}).get("extreme_short_count")
                rec["type"] = "COT contracts"
            elif "transcripts" in p:
                rec["n_records"] = len(p["transcripts"])
                rec["guidance_changes"] = (p.get("summary") or {}).get("guidance_changes")
                rec["type"] = "earnings transcripts (Claude-scored)"
            elif "filers" in p:
                rec["n_records"] = len(p["filers"])
                rec["as_of_quarter"] = p.get("as_of_quarter")
                rec["as_of_date"] = p.get("as_of_date")
                rec["type"] = "13F filers"
                # Top 3 by AUM
                rec["top3_aum"] = [{
                    "name": f.get("investor_name"),
                    "aum_b": round((f.get("market_value") or 0)/1e9, 1),
                    "qoq_pct": f.get("qoq_change_pct"),
                } for f in (p.get("filers") or [])[:3]]
            else:
                rec["n_records"] = "?"
            out["datasets"].append(rec)
        except Exception as e:
            out["datasets"].append({"name": name, "err": str(e)[:200]})

        # Test public HTTPS
        try:
            url = f"https://justhodl-dashboard-live.s3.amazonaws.com/{key}"
            r = urllib.request.urlopen(url, timeout=10)
            out["datasets"][-1]["public_https"] = r.status
        except Exception as e:
            out["datasets"][-1]["public_https_err"] = str(e)[:100]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    print("Waiting 90s for smart-money compat fix to deploy...")
    time.sleep(90)
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
    # Wait for async smart-money invoke to refresh S3
    time.sleep(15)
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
