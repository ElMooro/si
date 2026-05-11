"""
justhodl-smart-money-holdings — Smart Money × Screener Integration

WHAT IT DOES
────────────
Builds an inverse mapping: for each stock ticker, which top concentrated
hedge funds hold it. The screener page loads this as a sidecar and
displays a 🐳 Smart Money column showing the top 3-5 funds holding each
stock.

WHY CONCENTRATED FUNDS ONLY
────────────────────────────
Vanguard/BlackRock/State Street/Fidelity are passive index trackers
holding 4000-5000+ stocks each. Saying "Vanguard holds AAPL" is true
for nearly every stock — no signal. The 24 funds in the curated list
below are actively managed with concentrated positioning (Berkshire 42
holdings, Pershing Square ~10 positions, Bridgewater ~150). Those are
high-signal "smart money" votes.

PIPELINE
────────
1. For each fund CIK in CONCENTRATED_FUNDS:
   - Discover latest filed quarter (Q4 2025 currently)
   - Paginate /stable/institutional-ownership/extract?cik=X&year=Y&quarter=Q
   - Collect all (symbol, shares, value) records
2. Build inverse map {symbol: [{cik, name, shares, value, rank}, ...]}
   - Sort holders within each symbol by value desc
3. Per-fund summary: {cik, name, n_holdings, top_holdings[10], total_value}
4. Write to S3 at screener/smart-money-holdings.json

EXPECTED METRICS
────────────────
- 24 funds scanned
- ~2000-4000 unique stock symbols covered
- 10-30 KB compressed JSON output
- Runtime: 30-90s (paginated, 8-worker parallel)

EVENTBRIDGE: cron(30 14 * * ? *) — daily 14:30 UTC, 30 min after smart-money-tracker
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, date as _date
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

import boto3

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "screener/smart-money-holdings.json"

s3 = boto3.client("s3", region_name="us-east-1")

# Concentrated active funds — high-signal positioning.
# Curated list excludes pure index trackers (Vanguard, BlackRock, State Street
# FMR/Fidelity — these hold thousands of stocks each = no signal value).
CONCENTRATED_FUNDS = [
    ("0001067983", "Berkshire Hathaway"),       # Buffett — flagship concentrated
    ("0001336528", "Pershing Square Capital"),   # Ackman — ~10 positions
    ("0001079114", "Greenlight Capital"),        # Einhorn
    ("0001040273", "Third Point"),               # Loeb
    ("0001048445", "Elliott Investment Mgmt"),   # Singer — activist
    ("0001418814", "ValueAct Capital"),          # activist
    ("0001061768", "Lone Pine Capital"),         # Mandel — Tiger cub
    ("0001135730", "Coatue Management"),         # Laffont — tech
    ("0001167483", "Tiger Global"),              # Coleman — growth
    ("0001029160", "Soros Fund Management"),     # macro
    ("0001031972", "Baupost Group"),             # Klarman — value
    ("0001296958", "Point72 Asset Management"),  # Cohen
    ("0001103804", "Viking Global Investors"),   # Halvorsen — Tiger cub
    ("0001167274", "Glenview Capital"),          # Robbins — healthcare
    ("0001512699", "Sands Capital Management"),  # growth
    ("0001275148", "Whale Rock Capital"),        # tech growth
    ("0001112520", "Maverick Capital"),          # Tiger cub
    ("0001350694", "Bridgewater Associates"),    # Dalio — macro
    ("0001037389", "Renaissance Technologies"),  # Simons — quant (concentrated by exposure)
    ("0001423053", "Citadel Advisors"),          # Griffin
    ("0001179392", "Two Sigma Investments"),     # quant
    ("0001009207", "D.E. Shaw"),                 # quant
    ("0001541617", "Millennium Management"),     # Englander
    ("0000820027", "Tudor Investment"),          # PTJ — macro
]

# Pagination cap — Renaissance/Citadel can have 1000s of positions
MAX_PAGES_PER_FUND = 30  # 30 × 100 = up to 3000 holdings/fund
# Per-symbol cap (we keep all but limit JSON size when many funds hold)
MAX_HOLDERS_PER_SYMBOL = 24  # all 24 funds


def fmp(path, params="", retries=2):
    url = f"{FMP_BASE}/{path}?apikey={FMP_KEY}{params}"
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-SMH/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503) and attempt < retries:
                time.sleep(0.4 * (attempt + 1))
                continue
            return None
        except Exception:
            if attempt < retries:
                time.sleep(0.4 * (attempt + 1))
                continue
            return None
    return None


def get_latest_filed_quarter():
    """Pick the most-recently fully filed quarter (>=60 days past)."""
    now = datetime.now(timezone.utc).date()
    quarters = []
    for offset_y in (0, -1):
        yy = now.year + offset_y
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


def fetch_fund_holdings(args):
    """Paginate all holdings for one fund."""
    cik, name, year, quarter = args
    all_holdings = []
    seen_cusips = set()
    for page in range(MAX_PAGES_PER_FUND):
        data = fmp("institutional-ownership/extract",
                     f"&cik={cik}&year={year}&quarter={quarter}&page={page}")
        if not isinstance(data, list) or not data:
            break
        # Dedupe by CUSIP+symbol (some classes share CUSIPs)
        new_count = 0
        for h in data:
            cusip = h.get("securityCusip")
            sym = h.get("symbol")
            key = f"{cusip}|{sym}"
            if key in seen_cusips:
                continue
            seen_cusips.add(key)
            # Only keep records with a tradeable symbol (skip CUSIP-only records)
            if not sym or not isinstance(sym, str) or len(sym) > 8:
                continue
            # Skip puts/calls (we want equity ownership only)
            if h.get("putCallShare") and h["putCallShare"] not in ("", "Share", None):
                continue
            all_holdings.append({
                "symbol": sym,
                "shares": h.get("shares") or 0,
                "value": h.get("value") or 0,
                "name": h.get("nameOfIssuer"),
                "title_class": h.get("titleOfClass"),
            })
            new_count += 1
        if new_count == 0 or len(data) < 10:
            break
    # Sort by value desc
    all_holdings.sort(key=lambda h: -(h.get("value") or 0))
    return {"cik": cik, "name": name,
              "n_holdings": len(all_holdings),
              "total_value": sum(h.get("value") or 0 for h in all_holdings),
              "holdings": all_holdings}


def lambda_handler(event, context):
    started = time.time()
    year, quarter = get_latest_filed_quarter()
    print(f"[smh] using Q{quarter} {year}")

    # Fetch holdings for all funds in parallel
    targets = [(cik, name, year, quarter) for cik, name in CONCENTRATED_FUNDS]
    fund_results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for result in ex.map(fetch_fund_holdings, targets):
            if result and result["n_holdings"] > 0:
                fund_results.append(result)
            else:
                print(f"[smh] {result['name'] if result else '?'}: 0 holdings")

    print(f"[smh] fetched {sum(f['n_holdings'] for f in fund_results)} total holdings across {len(fund_results)} funds")

    # Build inverse mapping
    by_symbol = defaultdict(list)
    for fund in fund_results:
        fund_total = fund.get("total_value") or 0
        for h in fund["holdings"]:
            v = h.get("value") or 0
            # CONCENTRATION SIGNAL: what % of this fund's portfolio is this stock?
            # 4%+ is a high-conviction bet. 10%+ is a flagship position.
            # 0.01% is a probe / index-style holding.
            pct_of_fund = round(v / fund_total * 100, 3) if fund_total > 0 else None
            by_symbol[h["symbol"]].append({
                "cik": fund["cik"],
                "name": fund["name"],
                "shares": h["shares"],
                "value": v,
                "pct_of_fund": pct_of_fund,
            })

    # Within each symbol, sort holders by value desc; also compute symbol-level
    # conviction metrics for screener page filtering/sorting.
    holdings_map = {}
    for sym, holders in by_symbol.items():
        holders.sort(key=lambda h: -(h.get("value") or 0))
        # Cap to top N holders by value
        kept = holders[:MAX_HOLDERS_PER_SYMBOL]
        # max_pct_of_fund: highest concentration any single fund has in this stock
        # n_high_conviction: how many funds hold this as >5% of their portfolio
        # n_flagship: how many hold it as >10% (real conviction)
        valid_pcts = [h["pct_of_fund"] for h in kept if h.get("pct_of_fund") is not None]
        max_pct = max(valid_pcts) if valid_pcts else None
        n_high = sum(1 for p in valid_pcts if p >= 5.0)
        n_flagship = sum(1 for p in valid_pcts if p >= 10.0)
        holdings_map[sym] = {
            "holders": kept,
            "max_pct_of_fund": max_pct,
            "n_high_conviction": n_high,
            "n_flagship": n_flagship,
        }

    # Fund summaries (top 10 holdings each, for fund-detail UI)
    fund_summaries = []
    for f in fund_results:
        ft = f.get("total_value") or 0
        # Annotate top holdings with pct_of_fund too (so fund-detail view can show
        # the same conviction column)
        top_with_pct = []
        for h in f["holdings"][:10]:
            v = h.get("value") or 0
            top_with_pct.append({
                **h,
                "pct_of_fund": round(v / ft * 100, 3) if ft > 0 else None,
            })
        fund_summaries.append({
            "cik": f["cik"],
            "name": f["name"],
            "n_holdings": f["n_holdings"],
            "total_value": f["total_value"],
            "top_holdings": top_with_pct,
        })
    fund_summaries.sort(key=lambda f: -f["total_value"])

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(time.time() - started, 1),
        "as_of_quarter": f"Q{quarter} {year}",
        "year": year,
        "quarter": quarter,
        "n_funds_scanned": len(fund_results),
        "n_funds_attempted": len(CONCENTRATED_FUNDS),
        "n_symbols": len(holdings_map),
        "holdings": holdings_map,
        "funds": fund_summaries,
    }

    try:
        body = json.dumps(payload, default=str)
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                       Body=body,
                       ContentType="application/json",
                       CacheControl="public, max-age=3600")
        print(f"[s3] wrote {len(body)/1024:.1f} KB · {len(holdings_map)} symbols")
    except Exception as e:
        print(f"[s3] err: {e}")

    return {"statusCode": 200, "body": json.dumps({
        "n_funds": len(fund_results),
        "n_symbols": len(holdings_map),
        "as_of": payload["as_of_quarter"],
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
