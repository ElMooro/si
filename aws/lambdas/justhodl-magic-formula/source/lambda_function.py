"""
justhodl-magic-formula
======================

Greenblatt Magic Formula Screener (Pro Pack v3 #3).

GuruFocus signature feature; from Joel Greenblatt's "The Little Book
That Beats the Market" (2010).

Methodology:
  For each S&P 500 company (ex-financials, ex-utilities):
    1. Earnings Yield = EBIT / Enterprise Value
       (inverse of EV/EBIT; higher = cheaper)
    2. ROIC (Return on Invested Capital):
       Greenblatt formula: EBIT / (Net Working Capital + Net Fixed Assets)
       Simplified: EBIT / (Equity + Total Debt - Cash)
       (Net Operating Assets denominator; higher = better business)

  Rank universe by Earnings Yield (descending) -> rank A
  Rank universe by ROIC (descending) -> rank B
  Combined Magic Formula Rank = rank A + rank B (lowest = best)

  Top 30 = Magic Formula portfolio
  Bottom 30 = worst quality + most expensive

Sector exclusions (per Greenblatt):
  - Financials (banks, insurance) - balance sheets work differently
  - Utilities - regulated returns distort ROIC
  - REITs - non-EBIT accounting

Edge basis:
  Greenblatt (2010) reported 30.8% CAGR over 17 years (1988-2004)
  in backtest vs S&P 500 9.5% CAGR. Long-run academic replications
  show 4-8% alpha (Gray & Carlisle 2013, "Quantitative Value").
  Robust to data-mining critique due to simple 2-factor structure.

Schedule: daily 22:45 UTC (after IPO Pipeline, GF Value).

v1.1.0 (2026-10-26, bank fusion): universe = S&P 500 + the whole US market from the factor book (US common stocks >=
$200M outside the excluded sectors, pre-ranked by the bank's earnings yield + ROIC, 1,200 candidates); prices and
market caps come from the book's bank close (labelled per row), statements from the bank-first /stable path.
"""

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

VERSION = "1.1.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/magic-formula.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN", "") or
                  os.environ.get("TELEGRAM_TOKEN", ""))
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")

# Greenblatt's sector exclusions
EXCLUDED_SECTORS = {"Financial Services", "Financials", "Utilities",
                    "Real Estate"}
# Industries within Financials we double-exclude
EXCLUDED_INDUSTRIES = {"Banks", "Insurance", "REITs",
                       "Capital Markets", "Asset Management"}
# v1.1.0 (bank fusion, 2026-10-26): the universe is the whole US market from the factor book (justhodl-bank-factors),
# not the S&P 500 only - Greenblatt's own universe was every US stock above a size floor.  The book pre-ranks by its
# own earnings yield + ROIC so the exact EBIT/EV pass below fits the budget; prices/market caps come from the book's
# bank close (labelled) so the screen does not depend on 1,700 live /quote calls.
MIN_MCAP_USD = float(os.environ.get("MF_MIN_MCAP_USD", "200000000"))
BOOK_CANDIDATES = int(os.environ.get("MF_BOOK_CANDIDATES", "1200"))
try:
    import bank_enrich as _book
except Exception:  # noqa: BLE001 - without the SDK the engine is the S&P 500 screen it was
    _book = None


def http_get(url, timeout=15, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="ignore")
        except Exception as e:
            last = e
            time.sleep(0.4 * (i + 1))
    raise RuntimeError(f"http_get failed: {last}")


def fmp(path):
    sep = "&" if "?" in path else "?"
    url = f"https://financialmodelingprep.com/stable/{path}{sep}apikey={FMP_KEY}"
    try:
        return json.loads(http_get(url, timeout=20))
    except Exception:
        return None


def get_sp500():
    data = fmp("sp500-constituent")
    if not isinstance(data, list):
        return []
    return [(s.get("symbol"), s.get("sector"), s.get("subSector"))
            for s in data if s.get("symbol")]


def get_book_candidates():
    """Whole-market candidates from the factor book: US common stocks >= MIN_MCAP_USD outside Greenblatt's excluded
    sectors with a positive bank earnings yield and ROIC, pre-ranked by the sum of their ranks (the exact EBIT/EV and
    Greenblatt-ROIC pass re-ranks them).  [] when the book is not there."""
    if _book is None or not _book.available():
        return [], None
    rows = _book.screen(lambda r: (r.get("mcap") or 0) >= MIN_MCAP_USD and r.get("sector") not in EXCLUDED_SECTORS
                        and r.get("industry") not in EXCLUDED_INDUSTRIES and (r.get("earnings_yield") or 0) > 0 and (r.get("roic") or 0) > 0,
                        fields=["earnings_yield", "roic", "sector", "industry", "mcap"])
    if not rows:
        return [], _book.as_of()
    by_ey = sorted(rows, key=lambda r: -r["earnings_yield"])
    by_roic = sorted(rows, key=lambda r: -r["roic"])
    rank = {r["symbol"]: 0 for r in rows}
    for i, r in enumerate(by_ey):
        rank[r["symbol"]] += i
    for i, r in enumerate(by_roic):
        rank[r["symbol"]] += i
    rows.sort(key=lambda r: rank[r["symbol"]])
    return [(r["symbol"], r.get("sector"), r.get("industry")) for r in rows[:BOOK_CANDIDATES]], _book.as_of()


def get_universe():
    """S&P 500 constituents (the original list) + the factor book's whole-market candidates; symbol -> (sector, industry)."""
    uni = {}
    for sym, sec, ind in get_sp500():
        uni[sym] = (sec, ind)
    book, book_as_of = get_book_candidates()
    for sym, sec, ind in book:
        uni.setdefault(sym, (sec, ind))
    return uni, len(book), book_as_of


def book_quote(symbol):
    """quote-shaped row from the factor book (bank close, labelled) or None."""
    if _book is None or not _book.available():
        return None
    try:
        return _book.quote(symbol)
    except Exception:  # noqa: BLE001
        return None


def get_ticker_data(symbol):
    """income, balance, profile (bank-first /stable) + the quote: the factor book's bank close first (a daily screen
    after the close needs the close, and it does not spend 1,700 live calls), the live /quote only when the book lacks
    the name."""
    q = urllib.parse.quote_plus(symbol)
    endpoints = {
        "income":  f"income-statement?symbol={q}&limit=2",
        "balance": f"balance-sheet-statement?symbol={q}&limit=2",
        "profile": f"profile?symbol={q}",
    }
    out = {}
    bq = book_quote(symbol)
    if bq and bq.get("price") and bq.get("marketCap"):
        out["quote"] = [bq]
        out["price_source"] = "factor-book close as of %s" % ((bq.get("as_of") or {}).get("fmp-bank") or "")[:10]
    else:
        endpoints["quote"] = f"quote?symbol={q}"
        out["price_source"] = "live quote"
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fmp, p): k for k, p in endpoints.items()}
        for f in as_completed(futures):
            try:
                out[futures[f]] = f.result()
            except Exception:
                out[futures[f]] = None
    return out


def compute_magic(symbol, sector, industry):
    """Compute earnings yield + ROIC for one ticker per Greenblatt."""
    # Sector/industry filter
    if sector in EXCLUDED_SECTORS or industry in EXCLUDED_INDUSTRIES:
        return None
    try:
        d = get_ticker_data(symbol)
        quote = d.get("quote")
        income = d.get("income")
        balance = d.get("balance")
        profile = d.get("profile")
        if not (isinstance(quote, list) and quote and
                isinstance(income, list) and income and
                isinstance(balance, list) and balance):
            return None
        q0 = quote[0]
        i0 = income[0]
        b0 = balance[0]
        # Profile sector override (more accurate than sp500 constituent list)
        if isinstance(profile, list) and profile:
            p0 = profile[0]
            psect = p0.get("sector") or sector
            pind = p0.get("industry") or industry
            if (psect in EXCLUDED_SECTORS or pind in EXCLUDED_INDUSTRIES):
                return None
            sector = psect
            industry = pind
        # EBIT (TTM operating income proxy)
        ebit = i0.get("operatingIncome") or 0
        if ebit <= 0:
            return None
        # Enterprise Value
        mc = q0.get("marketCap") or 0
        debt = b0.get("totalDebt") or 0
        cash = (b0.get("cashAndShortTermInvestments") or
                b0.get("cashAndCashEquivalents") or 0)
        ev = mc + debt - cash
        if ev <= 0:
            return None
        # Earnings Yield = EBIT / EV
        earnings_yield = ebit / ev
        # ROIC: EBIT / (Equity + Debt - Cash) - approximation of
        # Greenblatt's Net Working Capital + Net Fixed Assets
        equity = (b0.get("totalStockholdersEquity") or
                  b0.get("stockholdersEquity") or 0)
        invested_capital = equity + debt - cash
        if invested_capital <= 0:
            return None
        roic = ebit / invested_capital
        # Sanity bounds
        if not (-2.0 < earnings_yield < 2.0):
            return None
        if not (-2.0 < roic < 5.0):
            return None
        return {
            "ticker": symbol,
            "sector": sector,
            "industry": industry,
            "price": q0.get("price"),
            "price_source": d.get("price_source"),
            "market_cap_usd": mc,
            "ebit_ttm": ebit,
            "enterprise_value": ev,
            "earnings_yield": round(earnings_yield, 4),
            "earnings_yield_pct": round(earnings_yield * 100, 1),
            "evebit": round(ev / ebit, 1) if ebit else None,
            "roic": round(roic, 4),
            "roic_pct": round(roic * 100, 1),
            "invested_capital_usd": invested_capital,
        }
    except Exception:
        return None


def telegram_alert(text):
    if not TELEGRAM_TOKEN:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT, "text": text,
            "parse_mode": "Markdown"}).encode()
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except Exception:
        return False


def lambda_handler(event, context):
    started = datetime.now(timezone.utc).isoformat()
    if not FMP_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": "no_fmp_key"})}
    try:
        universe, n_book, book_as_of = get_universe()
        if not universe:
            return {"statusCode": 500, "body": json.dumps({"ok": False,
                                                           "error": "no_universe"})}
        universe_label = ("S&P 500 + US common stocks >= $%dM from the factor book (%d candidates, bank as of %s), ex-Financials/Utilities/REITs"
                          % (MIN_MCAP_USD / 1e6, n_book, (book_as_of or {}).get("fmp-bank", "?")[:10])) if n_book else "S&P 500 ex-Financials/Utilities/REITs (factor book unavailable)"

        results = []
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(compute_magic, sym, sec, ind):
                       sym for sym, (sec, ind) in universe.items()}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    results.append(r)

        if not results:
            return {"statusCode": 200, "body": json.dumps({
                "ok": False, "error": "no_valid_tickers"})}

        # Greenblatt ranking: rank by Earnings Yield desc + ROIC desc
        # Lower combined rank = better
        by_ey = sorted(results, key=lambda x: -x["earnings_yield"])
        for i, r in enumerate(by_ey):
            r["rank_earnings_yield"] = i + 1
        by_roic = sorted(results, key=lambda x: -x["roic"])
        for i, r in enumerate(by_roic):
            r["rank_roic"] = i + 1
        for r in results:
            r["magic_rank"] = r["rank_earnings_yield"] + r["rank_roic"]

        # Sort by magic rank ascending (best first)
        results.sort(key=lambda x: x["magic_rank"])

        top_30 = results[:30]
        bottom_30 = results[-30:][::-1]  # worst first

        # Sector breakdown of top 30
        sector_counts = {}
        for r in top_30:
            s = r.get("sector") or "Unknown"
            sector_counts[s] = sector_counts.get(s, 0) + 1

        # Universe stats
        median_ey = round(statistics.median(
            [r["earnings_yield_pct"] for r in results]), 2)
        median_roic = round(statistics.median(
            [r["roic_pct"] for r in results]), 2)

        # Regime: how concentrated are the top picks vs market median?
        top10_avg_ey = statistics.mean(
            [r["earnings_yield_pct"] for r in top_30[:10]])
        top10_avg_roic = statistics.mean(
            [r["roic_pct"] for r in top_30[:10]])
        if top10_avg_ey >= 15 and top10_avg_roic >= 30:
            regime = "ABUNDANT_OPPORTUNITY"
        elif top10_avg_ey >= 10 and top10_avg_roic >= 20:
            regime = "GOOD_OPPORTUNITY"
        elif top10_avg_ey >= 7:
            regime = "MODERATE_OPPORTUNITY"
        else:
            regime = "SCARCE_OPPORTUNITY"

        payload = {
            "version": VERSION,
            "generated_at": started,
            "universe": universe_label,
            "n_universe": len(universe),
            "n_book_candidates": n_book,
            "book_as_of": book_as_of,
            "regime": regime,
            "n_universe_eligible": len(results),
            "median_earnings_yield_pct": median_ey,
            "median_roic_pct": median_roic,
            "top_10_avg_earnings_yield_pct": round(top10_avg_ey, 2),
            "top_10_avg_roic_pct": round(top10_avg_roic, 2),
            "top_30": top_30,
            "bottom_30": bottom_30,
            "sector_breakdown_top_30": sector_counts,
            "all_eligible": results,
            "methodology": {
                "earnings_yield_formula": "EBIT / Enterprise Value",
                "roic_formula": ("EBIT / (Total Equity + Total Debt - Cash) "
                                 "[Greenblatt-simplified Net Operating Assets]"),
                "ranking": "ascending(rank_ey + rank_roic) - lower is better",
                "excluded_sectors": sorted(EXCLUDED_SECTORS),
                "excluded_industries": sorted(EXCLUDED_INDUSTRIES),
                "universe": universe_label,
                "price_basis": "factor-book bank close when the book holds the name (labelled per row in price_source), else live /quote",
                "regime_bands": {
                    "ABUNDANT_OPPORTUNITY": ("top10_avg_ey >= 15% AND "
                                             "top10_avg_roic >= 30%"),
                    "GOOD_OPPORTUNITY": ("top10_avg_ey >= 10% AND "
                                         "top10_avg_roic >= 20%"),
                    "MODERATE_OPPORTUNITY": "top10_avg_ey >= 7%",
                    "SCARCE_OPPORTUNITY": "below all of above",
                },
            },
            "edge_basis": ("Greenblatt (2010) reported 30.8% CAGR 1988-2004 "
                           "vs S&P 9.5%. Academic replications (Gray & "
                           "Carlisle 2013) show 4-8% alpha. Two-factor "
                           "structure resists data-mining."),
            "sources": ["factor book data/warm/factor-book (justhodl-bank-factors; aws/shared/bank_enrich)",
                        "FMP /stable/quote (only when the book lacks the name)", "FMP /stable/income-statement",
                        "FMP /stable/balance-sheet-statement",
                        "FMP /stable/profile",
                        "FMP /stable/sp500-constituent"],
        }
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                      Body=json.dumps(payload, indent=2,
                                      default=str).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=900")

        # Telegram alert on ABUNDANT_OPPORTUNITY regime
        if regime == "ABUNDANT_OPPORTUNITY":
            top5 = top_30[:5]
            lines = "\n".join(
                f"- {x['ticker']} EY {x['earnings_yield_pct']:.1f}% "
                f"ROIC {x['roic_pct']:.1f}%" for x in top5)
            telegram_alert(f"*Magic Formula: ABUNDANT regime*\n"
                           f"Top 10 avg EY: {top10_avg_ey:.1f}%, "
                           f"ROIC: {top10_avg_roic:.1f}%\nTop 5:\n{lines}")

        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "regime": regime,
            "n_eligible": len(results),
            "top_30_top_pick": top_30[0]["ticker"] if top_30 else None,
            "median_ey_pct": median_ey,
            "median_roic_pct": median_roic})}

    except Exception as e:
        err = {"version": VERSION, "generated_at": started,
               "state": "ERROR", "error": str(e)[:500]}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode(),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": str(e)})}
