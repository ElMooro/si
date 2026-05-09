#!/usr/bin/env python3
"""Step 377d — Dump raw 30y/2y auctions to find the data anomaly."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/377d_30y_2y_dump.json"
NAME = "justhodl-tmp-30y-dump"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta

FISCAL = ("https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
          "/v1/accounting/od/auctions_query")

def fetch(start, end, page=1):
    params = {
        "filter": f"auction_date:gte:{start},auction_date:lte:{end}",
        "sort": "-auction_date", "format": "json",
        "page[size]": 200, "page[number]": page,
    }
    url = FISCAL + "?" + urllib.parse.urlencode(params, safe=":,")
    req = urllib.request.Request(url, headers={"Accept": "application/json",
                                                  "User-Agent": "verify"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))

def lambda_handler(event, context):
    out = {}
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=365)  # full year for 30y context

    all_records = []
    for page in range(1, 6):
        body = fetch(start.isoformat(), end.isoformat(), page)
        data = body.get("data", []) or []
        all_records.extend(data)
        if len(data) < 200:
            break

    out["n_total"] = len(all_records)

    # Filter and dump 30y bonds
    bonds_30y = [r for r in all_records if (r.get("security_term") or "").strip() == "30-Year"]
    out["bonds_30y"] = []
    for b in bonds_30y[:8]:
        out["bonds_30y"].append({
            "auction_date": b.get("auction_date"),
            "security_term": b.get("security_term"),
            "security_type": b.get("security_type"),
            "high_yield": b.get("high_yield"),
            "high_discnt_rate": b.get("high_discnt_rate"),
            "low_yield": b.get("low_yield"),
            "median_yield": b.get("median_yield"),
            "btc": b.get("bid_to_cover_ratio"),
            "indirect_$$": b.get("indirect_bidder_accepted"),
            "primary_dealer_$$": b.get("primary_dealer_accepted"),
            "total_accepted": b.get("total_accepted"),
            "allocation_pctage": b.get("allocation_pctage"),
            "cusip": b.get("cusip"),
        })

    # Filter and dump 2-year notes
    notes_2y = [r for r in all_records if (r.get("security_term") or "").strip() == "2-Year"]
    out["notes_2y"] = []
    for n in notes_2y[:8]:
        out["notes_2y"].append({
            "auction_date": n.get("auction_date"),
            "security_term": n.get("security_term"),
            "high_yield": n.get("high_yield"),
            "high_discnt_rate": n.get("high_discnt_rate"),
            "btc": n.get("bid_to_cover_ratio"),
            "indirect_$$": n.get("indirect_bidder_accepted"),
            "total_accepted": n.get("total_accepted"),
            "cusip": n.get("cusip"),
        })

    # All 30y/2y/coupon-related security_term values to see what variations exist
    terms = sorted(set((r.get("security_term") or "").strip() for r in all_records))
    out["all_security_terms"] = terms

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG_CODE)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=180, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed else parsed
    except Exception:
        out["raw"] = body[:5000]
    try:
        lam.delete_function(FunctionName=NAME)
    except Exception:
        pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
