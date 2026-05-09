#!/usr/bin/env python3
"""Step 375 — Pull live Treasury auction state for interpretation.

Reads:
  • s3://justhodl-dashboard-live/data/auction-crisis.json  (Khalid's score)
  • api.fiscaldata.treasury.gov last 60d auctions (raw demand stats)

Returns a structured snapshot for interpretation:
  - Current composite crisis score + regime
  - Last 6 indicator readings + which patterns are firing
  - Last 10 actual auctions: tenor, BTC, indirect %, primary dealer takedown,
    tail (high yield - WI), allotted-at-high
  - Comparison versus historical anchors:
      GFC peaks (2008-09-17, 2008-09-23, 2008-10-08)
      COVID crash (2020-03-11, 2020-03-19, 2020-03-26)
      Calm benchmark (2021 bills, 2024 normal notes)
      2024-10-09 AAH=99.31% early-warning
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/375_auction_state.json"
NAME = "justhodl-tmp-auction-state"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG_CODE = '''
import json, urllib.request, urllib.parse
from datetime import datetime, timedelta, timezone
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

# Historical anchor auctions for comparison (from PDFs Khalid uploaded)
ANCHORS = {
    "2008-09-17": {"tenor": "Bill 4-week", "btc": 4.21, "indirect_pct": 11.4,
                   "tail_bps": 0, "context": "GFC: Lehman+1, panic-buy",
                   "regime": "ACUTE_STRESS"},
    "2008-09-23": {"tenor": "Bill 7-day CMB", "btc": None, "indirect_pct": 12.8,
                   "tail_bps": None, "context": "GFC peak: Goldman/Morgan-becomes-bank-week",
                   "regime": "ACUTE_STRESS"},
    "2008-10-08": {"tenor": "TIPS 5-year reopen", "btc": 1.74, "indirect_pct": 32.4,
                   "tail_bps": 4.2, "context": "GFC: TIPS basis trade unwind, PD absorbed",
                   "regime": "ACUTE_STRESS"},
    "2020-03-11": {"tenor": "Bill 4-week", "btc": 2.92, "indirect_pct": 56.0,
                   "tail_bps": None, "context": "COVID: pre-crash flight to bills",
                   "regime": "WATCH"},
    "2020-03-19": {"tenor": "Bill 4-week", "btc": 3.53, "indirect_pct": 28.4,
                   "tail_bps": None, "context": "COVID: cash hoarding peak, indirect collapsed",
                   "regime": "ACUTE_STRESS"},
    "2020-03-26": {"tenor": "Bill 4-week", "btc": 4.09, "indirect_pct": 73.2,
                   "tail_bps": None, "context": "COVID: post-Fed bazooka recovery",
                   "regime": "WATCH"},
    "2021-04-13": {"tenor": "Bill 4-week", "btc": 3.84, "indirect_pct": 56.5,
                   "tail_bps": None, "context": "Crypto-top complacency benchmark",
                   "regime": "CALM"},
    "2024-04-10": {"tenor": "Note 10-year", "btc": 2.34, "indirect_pct": 64.8,
                   "tail_bps": -0.3, "context": "Healthy normal market benchmark",
                   "regime": "CALM"},
    "2024-10-09": {"tenor": "Note 10-year reopen", "btc": 2.32, "indirect_pct": 63.1,
                   "tail_bps": 1.8, "context": "AAH=99.31% early warning before Nov vol",
                   "regime": "WATCH"},
}

def fetch_treasury_auctions(days_back=60):
    """Pull last N days of auctions from fiscaldata.treasury.gov."""
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days_back)
    url = (f"https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
           f"/v1/accounting/od/securities_sales"
           f"?filter=auction_date:gte:{start.isoformat()}"
           f"&fields=auction_date,security_term,security_type,bid_to_cover_ratio,"
           f"high_yield_pct,issue_date,total_accepted,primary_dealer_accepted,"
           f"indirect_accepted,direct_accepted,total_tendered,allotted_pct_at_high"
           f"&page[size]=200&sort=-auction_date")
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/375"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8"))

def lambda_handler(event, context):
    out = {}
    out["as_of"] = datetime.now(timezone.utc).isoformat()
    out["anchors"] = ANCHORS

    # 1. Read Khalid's live auction-crisis score
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/auction-crisis.json")
        body = obj["Body"].read()
        j = json.loads(body)
        out["khalid_score"] = {
            "size_bytes": len(body),
            "last_modified": str(obj.get("LastModified")),
            "data": j,
        }
    except Exception as e:
        out["khalid_score"] = {"error": str(e)}

    # 2. Pull last 60 days of Treasury auctions directly
    try:
        data = fetch_treasury_auctions(60)
        rows = data.get("data", [])
        # Process: sort desc by date, normalize numeric fields
        for r in rows:
            for k in ["bid_to_cover_ratio", "high_yield_pct", "total_accepted",
                       "primary_dealer_accepted", "indirect_accepted",
                       "direct_accepted", "allotted_pct_at_high"]:
                v = r.get(k)
                try:
                    r[k] = float(v) if v not in (None, "", "null") else None
                except (TypeError, ValueError):
                    r[k] = None
            # Compute participation %
            tot = r.get("total_accepted") or 0
            ind = r.get("indirect_accepted") or 0
            pd = r.get("primary_dealer_accepted") or 0
            dr = r.get("direct_accepted") or 0
            r["indirect_pct"] = round(ind / tot * 100, 2) if tot else None
            r["pd_pct"] = round(pd / tot * 100, 2) if tot else None
            r["direct_pct"] = round(dr / tot * 100, 2) if tot else None
        rows.sort(key=lambda r: r.get("auction_date", ""), reverse=True)
        out["treasury_recent"] = {
            "n_total": len(rows),
            "date_range": {
                "newest": rows[0].get("auction_date") if rows else None,
                "oldest": rows[-1].get("auction_date") if rows else None,
            },
            "auctions": rows[:25],  # Top 25 most recent
        }

        # Aggregate by tenor — last 4 vs prior 8 averages
        from collections import defaultdict
        by_tenor = defaultdict(list)
        for r in rows:
            tenor = r.get("security_term", "?")
            by_tenor[tenor].append(r)
        tenor_summary = {}
        for tenor, rs in by_tenor.items():
            recent = rs[:4]   # last 4
            prior = rs[4:12]  # previous 8
            def avg(rss, key):
                vals = [r[key] for r in rss if r.get(key) is not None]
                return round(sum(vals) / len(vals), 3) if vals else None
            tenor_summary[tenor] = {
                "n_total": len(rs),
                "recent_btc": avg(recent, "bid_to_cover_ratio"),
                "prior_btc": avg(prior, "bid_to_cover_ratio"),
                "recent_indirect_pct": avg(recent, "indirect_pct"),
                "prior_indirect_pct": avg(prior, "indirect_pct"),
                "recent_pd_pct": avg(recent, "pd_pct"),
                "prior_pd_pct": avg(prior, "pd_pct"),
                "recent_high_yield": avg(recent, "high_yield_pct"),
                "prior_high_yield": avg(prior, "high_yield_pct"),
                "recent_aah": avg(recent, "allotted_pct_at_high"),
                "prior_aah": avg(prior, "allotted_pct_at_high"),
            }
        out["tenor_trends"] = tenor_summary
    except Exception as e:
        out["treasury_recent"] = {"error": str(e)[:500]}

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
                            MemorySize=512, Timeout=120, Code={"ZipFile": zb})
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
