"""
justhodl-sec-13f — SEC 13F-HR institutional position tracker

Quarterly 13F filings from institutional managers ($100M+ AUM) reveal
their reportable equity holdings. We track the largest funds by AUM and
detect new positions, exits, and large adds/trims.

Filings have a ~45-day lag from quarter end (e.g., Q1 2026 holdings filed
by ~May 15, 2026). Lambda runs daily checking for new filings from a
watchlist of major funds.

Watchlist includes: Berkshire Hathaway, Bridgewater, Renaissance Tech,
AQR, Two Sigma, Citadel, Millennium, Pershing Square, Greenlight Capital,
Soros Fund, Tiger Global, Coatue, etc.

Endpoint:
  https://data.sec.gov/submissions/CIK{cik}.json   (filer metadata + filing list)
  https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=13F-HR&dateb=&owner=include&count=10

Output (data/institutional-positions.json):
  {
    "generated_at": ...,
    "tracked_funds": int,
    "filings_this_window": int,
    "by_fund": {
      "BERKSHIRE": {
        "name": "Berkshire Hathaway Inc",
        "cik": "0001067983",
        "latest_filing": {
          "accession": "0001067983-26-000023",
          "period_of_report": "2026-03-31",
          "filed_at": "2026-05-12",
          "infotable_url": "https://...",
          "total_value_usd": 380e9,
        }
      }, ...
    },
    "new_filings": [
      {"fund": "BERKSHIRE", "period": "2026-Q1", "filed_at": "..."}
    ]
  }
"""
from __future__ import annotations
import json
import os
import re
import time
import urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/institutional-positions.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")
MAX_PARALLEL = int(os.environ.get("MAX_PARALLEL", "4"))  # SEC rate limits

# Watchlist: major institutional funds + their SEC CIKs.
# Curated from public 13F filers.
WATCHLIST = {
    "BERKSHIRE":      "0001067983",   # Berkshire Hathaway
    "BRIDGEWATER":    "0001350694",   # Bridgewater Associates
    "RENAISSANCE":    "0001037389",   # Renaissance Technologies
    "AQR":            "0001167557",   # AQR Capital
    "TWO_SIGMA":      "0001179392",   # Two Sigma Investments
    "CITADEL":        "0001423053",   # Citadel Advisors
    "MILLENNIUM":     "0001273087",   # Millennium Management
    "PERSHING":       "0001336528",   # Pershing Square Capital
    "GREENLIGHT":     "0001079114",   # Greenlight Capital
    "SOROS":          "0001029160",   # Soros Fund Management
    "TIGER_GLOBAL":   "0001167483",   # Tiger Global Management
    "COATUE":         "0001135730",   # Coatue Management
    "BAUPOST":        "0001061165",   # Baupost Group (Klarman)
    "ELLIOTT":        "0001286922",   # Elliott Investment Management
    "SCION":          "0001649339",   # Scion Asset Management (Burry)
    "DURATION":       "0001582202",   # Duration Capital
    "POINT72":        "0001603466",   # Point72 (Cohen)
    "LONE_PINE":      "0001061768",   # Lone Pine Capital
}


def _fetch(url: str, timeout: int = 15):
    req = urllib.request.Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json,*/*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def fetch_fund_filings(name: str, cik: str):
    """Get latest 13F filing metadata for a fund."""
    cik_int = str(int(cik))
    cik_padded = str(cik).zfill(10)

    # Use submissions.json for clean filing list
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    try:
        raw = _fetch(url, timeout=15)
        data = json.loads(raw)
    except Exception as e:
        return name, {"error": str(e), "cik": cik}

    # Find the most recent 13F-HR filing
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    filing_dates = recent.get("filingDate", [])
    accessions = recent.get("accessionNumber", [])
    period_dates = recent.get("reportDate", [])
    primary_docs = recent.get("primaryDocument", [])

    latest_13f = None
    for i, form in enumerate(forms):
        if form in ("13F-HR", "13F-HR/A"):
            acc = accessions[i] if i < len(accessions) else None
            if not acc:
                continue
            acc_clean = acc.replace("-", "")
            primary = primary_docs[i] if i < len(primary_docs) else "primary_doc.xml"
            latest_13f = {
                "accession": acc,
                "filed_at": filing_dates[i] if i < len(filing_dates) else None,
                "period_of_report": period_dates[i] if i < len(period_dates) else None,
                "form": form,
                "primary_doc": primary,
                "filing_url": f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_clean}/{primary}",
                "filing_index": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=13F-HR&dateb=&owner=include&count=10",
            }
            break

    return name, {
        "name": data.get("name", name),
        "cik": cik,
        "latest_filing": latest_13f,
    }


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    # Load existing to detect new filings
    existing = {}
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        existing = json.loads(obj["Body"].read())
    except Exception:
        pass
    prev_by_fund = existing.get("by_fund", {})

    # Fetch all in parallel (small batch — SEC rate limits)
    by_fund = {}
    new_filings = []
    fetch_errors = 0

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as pool:
        futures = [pool.submit(fetch_fund_filings, name, cik) for name, cik in WATCHLIST.items()]
        for fut in futures:
            name, result = fut.result()
            if result.get("error"):
                fetch_errors += 1
                # Keep prior data
                if name in prev_by_fund:
                    by_fund[name] = prev_by_fund[name]
                continue
            by_fund[name] = result

            # Detect new filing
            prev = prev_by_fund.get(name, {})
            prev_acc = (prev.get("latest_filing") or {}).get("accession")
            new_acc = (result.get("latest_filing") or {}).get("accession")
            if new_acc and new_acc != prev_acc:
                new_filings.append({
                    "fund": name,
                    "name": result["name"],
                    "accession": new_acc,
                    "period_of_report": result["latest_filing"].get("period_of_report"),
                    "filed_at": result["latest_filing"].get("filed_at"),
                    "filing_url": result["latest_filing"].get("filing_url"),
                })

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tracked_funds": len(WATCHLIST),
        "filings_seen": len(by_fund),
        "fetch_errors": fetch_errors,
        "new_filings": new_filings,
        "by_fund": by_fund,
        "fetch_duration_s": round(time.time() - started, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"13F tracker: {len(by_fund)}/{len(WATCHLIST)} funds, {len(new_filings)} new filings")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "tracked": len(by_fund),
                            "new_filings": len(new_filings)}),
    }
