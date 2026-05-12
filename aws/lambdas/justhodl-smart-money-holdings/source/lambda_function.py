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

# Concentrated + active funds — high-signal positioning.
# Expanded from 24 → 35 via probe step 452 (2026-05-11). All CIKs validated
# against /institutional-ownership/holder-performance-summary?cik=X.
# Excludes pure index trackers (Vanguard / BlackRock / State Street / FMR) —
# they hold 4-5k stocks each and saying "Vanguard holds AAPL" is signal-free.
# Sorted roughly by name fame + concentration density (most-concentrated first).
CONCENTRATED_FUNDS = [
    # ── Ultra-concentrated activists & value (≤50 holdings) ──
    ("0001067983", "Berkshire Hathaway"),          # Buffett · 42 · flagship value
    ("0001336528", "Pershing Square Capital"),     # Ackman · 11 · most-concentrated big fund
    ("0000921669", "Icahn Capital Management"),    # Icahn · 13 · NEW · activist legend
    ("0001517137", "Starboard Value"),             # Smith · 22 · NEW · activist
    ("0001345471", "Anchorage Capital"),           # 7 · NEW · ultra-concentrated
    ("0001047644", "Davidson Kempner"),            # 8 · NEW · distressed
    ("0001321655", "Discovery Capital Mgmt"),      # Citrone · 4 · NEW · macro
    ("0001112520", "Akre Capital Management"),    # 18 pos · $9.1B · concentrated value · CORRECTED (was mislabeled "Maverick Capital")
    ("0001541617", "Millennium Management"),       # Englander · 18
    ("0001418814", "ValueAct Capital"),            # Ubben activist · 16
    ("0001061768", "Lone Pine Capital"),           # Mandel · 22
    ("0001167483", "Tiger Global Management"),     # Coleman · 54 · tech growth
    ("0001031972", "Baupost Group"),               # Klarman · 41 · value legend
    ("0001040273", "Third Point"),                 # Loeb · 44 · activist
    ("0001079114", "Greenlight Capital"),          # Einhorn · 40 · NEW · short specialist
    # ── Concentrated active (50-150 holdings) ──
    ("0001135730", "Coatue Management"),           # Laffont · 52 · tech
    ("0001103804", "Viking Global Investors"),     # Halvorsen Tiger cub · 76
    ("0001048445", "Elliott Investment Mgmt"),     # Singer · 56 · NEW · activist
    ("0001346824", "ARK Investment Management"),   # Cathie Wood · 71 · NEW · disruptive innovation
    ("0001020066", "Sands Capital Management"),    # 67 · NEW · correct CIK · growth
    ("0001536411", "Duquesne Family Office"),      # Druckenmiller · 62 · CORRECTED (was mislabeled "Canyon Capital")
    ("0000732905", "Tweedy, Browne"),              # 93 · NEW · legendary value
    ("0001313893", "Maple Capital"),               # 119 · NEW
    ("0001036325", "Davis Selected Advisers"),     # 112 · NEW · value
    ("0001029160", "Soros Fund Management"),       # macro · 244
    # ── Mid-diversified quants/macros (kept — high $ positions still signal) ──
    ("0001350694", "Bridgewater Associates"),      # Dalio · 1040 · macro
    ("0001037389", "Renaissance Technologies"),    # Simons · 3184 · quant
    ("0001179392", "Two Sigma Investments"),       # quant · 4041
    ("0001009207", "D.E. Shaw"),                   # quant · 4558
    ("0000820027", "Tudor Investment"),            # PTJ macro · 4071
    ("0001423053", "Citadel Advisors"),            # Griffin · 12508
    ("0001167557", "AQR Capital Management"),      # Asness quant · 3562 · NEW
    ("0001603466", "Schonfeld Strategic"),         # 3862 · NEW · multi-mgr quant
    ("0000902219", "Wellington Management"),       # 1912 · NEW · classic active mgr
    ("0001374170", "Norges Bank Investment Mgmt"), # Norway sovereign · 1577 · NEW
    # ── Stage 16.3: 17 funds resolved via SEC EDGAR full-text search ──
    # All CIKs are the authoritative SEC values + validated via FMP extract
    ("0001345471", "Trian Fund Management"),       # Peltz · 7 positions · $4.0B · activist legend
    ("0001647251", "TCI Fund Management"),         # Hohn · 9 · $53.6B · ULTRA-concentrated activist
    ("0001656456", "Appaloosa LP"),                # Tepper · 39 · $6.9B · macro/credit legend
    ("0000898382", "Omega Advisors / Cooperman"),  # Cooperman · 40 · $3.0B
    ("0001135778", "Miller Value Partners"),       # Bill Miller · 34 · $0.3B
    ("0001138995", "Glenview Capital Mgmt"),       # Robbins · 57 · $4.9B · healthcare · FIXED CIK
    ("0001387322", "Whale Rock Capital"),          # 32 · $7.8B · tech growth · FIXED CIK
    ("0001747057", "D1 Capital Partners"),         # 42 · $10.7B · growth
    ("0000949509", "Oaktree Capital Mgmt"),        # Howard Marks · 172 · $7.0B · distressed
    ("0000909661", "Farallon Capital Mgmt"),       # 151 · $21.2B
    ("0001325447", "First Eagle Investment Mgmt"), # 422 · $56.8B · global value
    ("0000807249", "GAMCO Investors / Gabelli"),   # Mario Gabelli · 1036 · $10.4B
    ("0001512857", "Brevan Howard Capital Mgmt"),  # Alan Howard · 1714 · $9.2B · macro
    ("0001448574", "Moore Capital Management"),    # Louis Bacon · 665 · $6.9B · macro
    ("0002051323", "Caxton Associates"),           # Bruce Kovner · 752 · $5.1B · macro
    ("0001562230", "Capital Research / Cap Group"), # Capital Group · 454 · $638B (large but kept)
    ("0002054122", "Balyasny / Longaeva Partners"), # Balyasny pod · 119 · $1.2B · multi-mgr
    # ── Stage 16.4: 6 funds added via second-pass SEC EDGAR (probe 461) ──
    # Name-match filter (display_name must contain core token) eliminated
    # the cross-matches that plagued 454b. All extract-validated.
    ("0000905567", "Yacktman Asset Management"),   # Don Yacktman · 76 pos · $7.5B · value legend
    ("0000936753", "Ariel Investments"),           # John Rogers · 108 pos · $9.3B · value
    ("0001353316", "Hound Partners"),              # Tiger cub · 37 pos · $0.7B
    ("0001835549", "Engine No. 1"),                # ExxonMobil activist · 6 pos · $0.1B
    ("0001897612", "T. Rowe Price Inv Mgmt"),      # 898 pos · $158.4B · mutual fund
    ("0001535630", "Element Capital Management"),  # Talpins macro · 55 pos · $0.2B
    # ── Stage 16.5: 10 funds added via probe 462 (Maverick + legendary value) ──
    # The famous-investor backbone is now complete: every major US value/growth
    # legend filing 13F is tracked.
    ("0000934639", "Maverick Capital"),            # Lee Ainslie · 179 pos · $9.3B · Tiger cub · CORRECT CIK
    ("0001720792", "Ruane Cunniff & Goldfarb"),    # Sequoia Fund · 48 pos · $6.4B · LEGENDARY value
    ("0001549575", "Mohnish Pabrai (Dalal Street)"), # 4 pos · $0.4B · ULTRA-concentrated value disciple
    ("0000883965", "Wally Weitz Investment Mgmt"), # 44 pos · $1.7B · concentrated value
    ("0000807985", "Mason Hawkins / Longleaf"),    # Southeastern Asset · 53 pos · $2.2B · value legend
    ("0001056831", "Bruce Berkowitz / Fairholme"), # 15 pos · $1.4B · concentrated value
    ("0001569049", "Light Street Capital"),        # 24 pos · $0.7B · tech-focused
    ("0001510387", "Joel Greenblatt / Gotham"),    # 1719 pos · $27.4B · Magic Formula Investing
    ("0001478735", "Two Sigma Advisers"),          # 2329 pos · $51.4B · sibling to Two Sigma Investments
    ("0001993888", "Pictet Asset Management"),     # 2102 pos · $94.8B · Swiss giant
]

# Min position size to include in inverted index. Keeps file size manageable
# (without filter: diversified funds × thousands of holdings = bloat).
# A $5M / 0.02% position from a $100B fund is noise, not signal.
MIN_POSITION_VALUE = 5_000_000   # $5M absolute floor
MIN_PCT_OF_FUND = 0.02            # 0.02% relative floor — keep if either threshold met

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
    skipped_noise = 0
    for fund in fund_results:
        fund_total = fund.get("total_value") or 0
        for h in fund["holdings"]:
            v = h.get("value") or 0
            # CONCENTRATION SIGNAL: what % of this fund's portfolio is this stock?
            # 4%+ is a high-conviction bet. 10%+ is a flagship position.
            # 0.01% is a probe / index-style holding.
            pct_of_fund = round(v / fund_total * 100, 3) if fund_total > 0 else None
            # SIGNAL FILTER (Stage 16.2): drop noise positions to keep sidecar small.
            # Keep if either the $ value or the % of fund clears the floor.
            # This filters out e.g. Citadel's 0.001% probe positions in obscure tickers
            # without losing meaningful holdings.
            if (pct_of_fund is not None
                and pct_of_fund < MIN_PCT_OF_FUND
                and v < MIN_POSITION_VALUE):
                skipped_noise += 1
                continue
            by_symbol[h["symbol"]].append({
                "cik": fund["cik"],
                "name": fund["name"],
                "shares": h["shares"],
                "value": v,
                "pct_of_fund": pct_of_fund,
            })
    print(f"[smh] inverted index: kept {sum(len(v) for v in by_symbol.values())} entries · skipped {skipped_noise} noise")

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
