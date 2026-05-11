"""
justhodl-smart-money-tracker v2 — Top Hedge Funds Activity (per-CIK fetch)

ENDPOINT FIX (v2, 2026-05-11):
Probe 444 found holder-performance-summary requires ?cik=X (per-filer), not
year+quarter. Per-CIK call returns full historical series with QoQ data
(marketValue, previousMarketValue, securitiesAdded/Removed, portfolioSize,
performance, performancePercentage, holdingPeriod, turnover).

PIPELINE:
  1. Curated list of ~25 well-known hedge fund CIKs (Berkshire, BlackRock,
     Vanguard, Bridgewater, Renaissance, Citadel, Tiger, Pershing Square...)
  2. Plus discovery: institutional-ownership/latest pages 0-4 → 50 more CIKs
  3. For each unique CIK (~75 total):
       - GET holder-performance-summary?cik=X
       - Take most recent record (sorted by date desc)
  4. Process: sort by marketValue desc, build summary rankings
  5. Write to S3 at screener/smart-money.json

OUTPUT SCHEMA: same as v1 but with proper QoQ data:
  filers: [{cik, investor_name, market_value, qoq_change_pct,
            securities_added, securities_removed, performance_pct, ...}]
  summary: {biggest_gainers, biggest_decliners, most_active, ...}
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import boto3

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "screener/smart-money.json"

s3 = boto3.client("s3", region_name="us-east-1")

# Curated list of well-known hedge funds / asset managers (CIK numbers).
# Some CIKs may be wrong/missing — invalid ones just return no data + are skipped.
WELL_KNOWN_FUNDS = [
    ("0001067983", "BERKSHIRE HATHAWAY INC"),
    ("0001364742", "BLACKROCK INC"),
    ("0000102909", "VANGUARD GROUP INC"),
    ("0001350694", "BRIDGEWATER ASSOCIATES"),
    ("0001037389", "RENAISSANCE TECHNOLOGIES LLC"),
    ("0001423053", "CITADEL ADVISORS LLC"),
    ("0001179392", "TWO SIGMA INVESTMENTS LP"),
    ("0001009207", "D.E. SHAW & CO INC"),
    ("0001167483", "TIGER GLOBAL MANAGEMENT LLC"),
    ("0001029160", "SOROS FUND MANAGEMENT LLC"),
    ("0001336528", "PERSHING SQUARE CAPITAL"),
    ("0001079114", "GREENLIGHT CAPITAL INC"),
    ("0001040273", "THIRD POINT LLC"),
    ("0001048445", "ELLIOTT INVESTMENT MGMT"),
    ("0001418814", "VALUEACT HOLDINGS LP"),
    ("0001061768", "LONE PINE CAPITAL LLC"),
    ("0001135730", "COATUE MANAGEMENT LLC"),
    ("0001031972", "BAUPOST GROUP LLC"),
    ("0001296958", "POINT72 ASSET MANAGEMENT"),
    ("0001103804", "VIKING GLOBAL INVESTORS LP"),
    ("0001541617", "MILLENNIUM MANAGEMENT LLC"),
    ("0000820027", "TUDOR INVESTMENT CORP ET AL"),
    ("0001167274", "GLENVIEW CAPITAL MGMT LLC"),
    ("0001512699", "SANDS CAPITAL MANAGEMENT"),
    ("0001275148", "WHALE ROCK CAPITAL MGMT"),
    ("0001000275", "ROYAL BANK OF CANADA"),
    ("0000093751", "STATE STREET CORP"),
    ("0000315066", "FIDELITY MANAGEMENT & RESEARCH"),
    ("0001137518", "BRIDGEWATER PURE ALPHA"),
    ("0001112520", "MAVERICK CAPITAL LTD"),
]


def fmp(path, params="", retries=2):
    url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-SM/2.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            if e.code == 400 or e.code == 404:
                return None
            print(f"[fmp] {path}: HTTP {e.code}")
            return None
        except Exception as e:
            if attempt < retries:
                time.sleep(0.5 * (attempt + 1))
                continue
            return None
    return None


def discover_more_ciks(target=50):
    """Pull recent 13F filings to find more CIKs beyond the curated list."""
    discovered = {}
    for page in range(5):  # 5 × 100 = 500 records
        recs = fmp("institutional-ownership/latest", f"&page={page}&limit=100")
        if not isinstance(recs, list) or not recs:
            break
        for r in recs:
            cik = r.get("cik")
            name = r.get("name") or r.get("investorName")
            if cik and cik not in discovered:
                discovered[cik] = name
                if len(discovered) >= target:
                    break
        if len(discovered) >= target:
            break
    return discovered


def fetch_filer(cik_name):
    cik, fallback_name = cik_name
    data = fmp("institutional-ownership/holder-performance-summary",
                 f"&cik={cik}")
    if not isinstance(data, list) or not data:
        return None
    # Sort by date desc, take most recent
    data.sort(key=lambda r: r.get("date", ""), reverse=True)
    latest = data[0]

    mv = latest.get("marketValue") or 0
    prev_mv = latest.get("previousMarketValue") or 0
    if mv <= 0:
        return None

    qoq_pct = latest.get("changeInMarketValuePercentage")
    if qoq_pct is None and prev_mv > 0:
        qoq_pct = round((mv - prev_mv) / prev_mv * 100, 2)

    added = latest.get("securitiesAdded") or 0
    removed = latest.get("securitiesRemoved") or 0

    return {
        "cik": cik,
        "investor_name": latest.get("investorName") or fallback_name,
        "date": (latest.get("date", "") or "")[:10],
        "portfolio_size": latest.get("portfolioSize"),
        "securities_added": added,
        "securities_removed": removed,
        "market_value": mv,
        "previous_market_value": prev_mv,
        "change_in_market_value": latest.get("changeInMarketValue"),
        "qoq_change_pct": qoq_pct,
        "net_activity": added - removed,
        "activity_score": added + removed,
        "performance": latest.get("performance"),
        "performance_pct": latest.get("performancePercentage"),
        "avg_holding_period": latest.get("averageHoldingPeriod"),
        "avg_holding_period_top10": latest.get("averageHoldingPeriodTop10"),
        "turnover": latest.get("turnover"),
    }


def lambda_handler(event, context):
    started = time.time()

    # 1. Discover additional CIKs
    print("[sm] discovering recent 13F filers...")
    discovered = discover_more_ciks(target=80)
    print(f"[sm] discovered {len(discovered)} CIKs from latest filings")

    # 2. Combine curated + discovered (dedupe)
    seen = set()
    all_targets = []
    for cik, name in WELL_KNOWN_FUNDS:
        if cik not in seen:
            seen.add(cik)
            all_targets.append((cik, name))
    for cik, name in discovered.items():
        if cik not in seen:
            seen.add(cik)
            all_targets.append((cik, name))
    print(f"[sm] {len(all_targets)} unique CIKs to fetch")

    # 3. Fetch performance summary for each in parallel
    filers = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for result in ex.map(fetch_filer, all_targets):
            if result:
                filers.append(result)
    print(f"[sm] fetched performance for {len(filers)} filers")

    # 4. Sort by AUM desc
    filers.sort(key=lambda f: -(f.get("market_value") or 0))

    # 5. Build summary
    def slim(f):
        return {
            "cik": f["cik"],
            "name": f["investor_name"],
            "mv": f["market_value"],
            "qoq_pct": f["qoq_change_pct"],
            "added": f["securities_added"],
            "removed": f["securities_removed"],
            "size": f["portfolio_size"],
            "perf_pct": f.get("performance_pct"),
        }

    summary = {
        "n_filers": len(filers),
        "most_active": [slim(f) for f in sorted(
            filers, key=lambda f: -(f.get("activity_score") or 0))[:15]],
        "biggest_gainers": [slim(f) for f in sorted(
            [f for f in filers if f.get("qoq_change_pct") is not None],
            key=lambda f: -(f.get("qoq_change_pct") or -999))[:15]],
        "biggest_decliners": [slim(f) for f in sorted(
            [f for f in filers if f.get("qoq_change_pct") is not None],
            key=lambda f: (f.get("qoq_change_pct") or 999))[:15]],
        "biggest_increasers": [slim(f) for f in sorted(
            filers, key=lambda f: -(f.get("securities_added") or 0))[:15]],
        "biggest_reducers": [slim(f) for f in sorted(
            filers, key=lambda f: -(f.get("securities_removed") or 0))[:15]],
        "best_performers": [slim(f) for f in sorted(
            [f for f in filers if f.get("performance_pct") is not None],
            key=lambda f: -(f.get("performance_pct") or -999))[:15]],
    }

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - started, 1),
        "as_of_date": max((f.get("date") or "" for f in filers), default=""),
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
        "as_of": payload["as_of_date"],
        "elapsed_seconds": payload["elapsed_seconds"],
        "biggest_gainer": (summary["biggest_gainers"] or [None])[0],
        "biggest_decliner": (summary["biggest_decliners"] or [None])[0],
    })}
