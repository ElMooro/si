"""
justhodl-smart-money-tracker — Top Hedge Funds / Institutional Holders Activity

PURPOSE
───────
Tracks what the smartest money is doing with their 13F holdings.
Powered by FMP Ultimate's institutional-ownership/holder-performance-summary
endpoint, which returns:
  - Top filers by portfolio value
  - QoQ counts: securities added, securities removed
  - Current market value vs prior quarter
  - Portfolio size + diversification

This Lambda runs daily, fetches the latest data, ranks the top 50 holders,
and writes to S3 for the smart-money dashboard.

OUTPUT (screener/smart-money.json):
{
  "generated_at": iso8601,
  "as_of_quarter": "Q4 2025",
  "n_filers": 50,
  "filers": [
    {
      "cik": "0001067983",
      "investor_name": "BERKSHIRE HATHAWAY INC",
      "portfolio_size": 42,
      "securities_added": 4,
      "securities_removed": 3,
      "market_value": 274234567890,
      "previous_market_value": 268000000000,
      "qoq_change_pct": 2.32,
      "net_activity": 1,        // added - removed (net new positions)
      "activity_score": 7       // added + removed (gross turnover)
    }, ...
  ],
  "summary": {
    "most_active": [...top 10 by gross activity],
    "biggest_gainers": [...top 10 by qoq value change pct],
    "biggest_increasers": [...top 10 by securities_added],
    "biggest_reducers": [...top 10 by securities_removed]
  }
}
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "screener/smart-money.json"

s3 = boto3.client("s3", region_name="us-east-1")


def fmp(path, params="", retries=2):
    url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-SM/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            print(f"[fmp] {path}: HTTP {e.code}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            print(f"[fmp] {path}: {e}")
            return None
    return None


def get_latest_filed_quarter():
    """Same logic as screener Lambda — pick a quarter that's ≥60 days old."""
    from datetime import date as _date, timedelta
    now = datetime.now(timezone.utc).date()
    quarters = []
    y = now.year
    for offset_y in (0, -1):
        yy = y + offset_y
        for q in (4, 3, 2, 1):
            qend_month = q * 3
            qend_day = 30 if qend_month in (6, 9) else 31
            try:
                qd = _date(yy, qend_month, qend_day)
                if (now - qd).days >= 60:
                    quarters.append((yy, q))
            except ValueError:
                pass
    quarters.sort(key=lambda yq: (-yq[0], -yq[1]))
    return quarters[0] if quarters else (now.year - 1, 4)


def lambda_handler(event, context):
    started = time.time()
    year, quarter = get_latest_filed_quarter()
    print(f"[sm] fetching Q{quarter} {year} holder performance...")

    # holder-performance-summary returns top filers
    # Try without CIK to get aggregate list
    raw = fmp("institutional-ownership/holder-performance-summary",
                 f"&year={year}&quarter={quarter}&page=0")
    if not isinstance(raw, list) or not raw:
        # Try fallback: latest holders list
        raw = fmp("institutional-ownership/latest", "&page=0&limit=100")
        if not isinstance(raw, list):
            return {"statusCode": 500, "body": json.dumps({"error": "no_data"})}

    print(f"[sm] got {len(raw)} filer records")

    # Process each filer
    filers = []
    for r in raw:
        try:
            mv = r.get("marketValue") or 0
            prev_mv = r.get("previousMarketValue") or 0
            added = r.get("securitiesAdded") or 0
            removed = r.get("securitiesRemoved") or 0
            qoq_pct = None
            if prev_mv > 0:
                qoq_pct = round((mv - prev_mv) / prev_mv * 100, 2)
            filers.append({
                "cik": r.get("cik"),
                "investor_name": r.get("investorName") or r.get("name"),
                "date": r.get("date", "")[:10] if r.get("date") else None,
                "portfolio_size": r.get("portfolioSize") or r.get("portfolio_size"),
                "securities_added": added,
                "securities_removed": removed,
                "market_value": mv,
                "previous_market_value": prev_mv,
                "qoq_change_pct": qoq_pct,
                "net_activity": added - removed,
                "activity_score": added + removed,
                "performance": r.get("performance"),
                "performance_pct": r.get("performancePercentage"),
            })
        except Exception:
            continue

    # Sort by market value desc by default
    filers.sort(key=lambda f: -(f.get("market_value") or 0))
    filers = filers[:80]  # cap to top 80 by AUM

    # Build summary sub-rankings
    summary = {
        "n_filers": len(filers),
        "most_active": sorted(
            filers, key=lambda f: -(f.get("activity_score") or 0))[:15],
        "biggest_gainers": sorted(
            [f for f in filers if f.get("qoq_change_pct") is not None],
            key=lambda f: -(f.get("qoq_change_pct") or -999))[:15],
        "biggest_decliners": sorted(
            [f for f in filers if f.get("qoq_change_pct") is not None],
            key=lambda f: (f.get("qoq_change_pct") or 999))[:15],
        "biggest_increasers": sorted(
            filers, key=lambda f: -(f.get("securities_added") or 0))[:15],
        "biggest_reducers": sorted(
            filers, key=lambda f: -(f.get("securities_removed") or 0))[:15],
    }

    # Slim down summary entries
    def slim(f):
        return {
            "cik": f["cik"], "name": f["investor_name"],
            "mv": f["market_value"], "qoq_pct": f["qoq_change_pct"],
            "added": f["securities_added"], "removed": f["securities_removed"],
            "size": f["portfolio_size"],
        }
    for key in ("most_active", "biggest_gainers", "biggest_decliners",
                  "biggest_increasers", "biggest_reducers"):
        summary[key] = [slim(f) for f in summary[key]]

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - started, 1),
        "as_of_quarter": f"Q{quarter} {year}",
        "n_filers": len(filers),
        "filers": filers,
        "summary": summary,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                       Body=json.dumps(payload, default=str),
                       ContentType="application/json",
                       CacheControl="public, max-age=3600")
        print(f"[s3] wrote {len(filers)} filers")
    except Exception as e:
        print(f"[s3] err: {e}")

    return {"statusCode": 200, "body": json.dumps({
        "n_filers": len(filers),
        "as_of": payload["as_of_quarter"],
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
