"""
Pro Pack v3 #7 - GuruFocus Predictability Rank
================================================

GuruFocus's flagship moat-identification metric: 1-5 star rating based on
10-year revenue + EPS growth stability. Methodology synthesis from published
GuruFocus research:

  - High predictability (5★) = smooth historical trajectory, low growth-rate
    variance, consistent year-over-year compounding -> moat candidates that
    justify premium PE multiples and serve as "coffee can" portfolio holds
  - Low predictability (1★) = lumpy/cyclical earnings, high variance, often
    deserving discounted multiples regardless of headline growth

Backtest justification: GuruFocus published research shows 5★ predictability
stocks with PE < 20 returned 11.4% annualized 2000-2020 vs S&P 500 at 6.2%.

Methodology (using FMP /stable/income-statement, 10y depth):

For each ticker, compute:
  - REVENUE: 10y annual values
    * Revenue R^2: linear regression goodness-of-fit (1.0 = perfectly linear)
    * Revenue 10y CAGR
    * Revenue YoY growth rate coefficient-of-variation (CoV)
  - EPS (diluted): 10y annual values
    * EPS R^2
    * EPS 10y CAGR  
    * EPS YoY CoV

Predictability Star Rating (1-5 stars):
  5*: Rev R^2 >= 0.95 AND EPS R^2 >= 0.85  (elite moat)
  4*: Rev R^2 >= 0.90 AND EPS R^2 >= 0.75  (strong predictable)
  3*: Rev R^2 >= 0.80 AND EPS R^2 >= 0.60  (moderate predictable)
  2*: Rev R^2 >= 0.65 OR  EPS R^2 >= 0.50  (some predictability)
  1*: anything below                       (cyclical/unpredictable)

Valuation Overlay (cross-section with current PE):
  Cheap: PE_ttm < 20
  Fair:  20 <= PE_ttm < 30
  Rich:  PE_ttm >= 30

Sweet Spot = 5* predictability + Cheap valuation (GuruFocus high-conviction
"undervalued moat" signal).

Universe: STATIC_TOP50_SPX (deterministic, FMP-quota friendly).
Schedule: daily 23:50 UTC (after StarMine, before midnight close).
"""

import os
import json
import time
import math
import statistics
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone

import boto3

# ---------- Constants ----------
VERSION = "1.0.1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/predictability.json"
FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

FMP_SLEEP_SEC = 0.5   # 3 calls/ticker -> ~150 calls + sleeps = ~3 min
HTTP_TIMEOUT = 25
HISTORY_YEARS = 10

# Same universe as StarMine for cross-engine consistency
STATIC_TOP50_SPX = [
    {"symbol": "AAPL",  "sector": "Technology"},
    {"symbol": "MSFT",  "sector": "Technology"},
    {"symbol": "NVDA",  "sector": "Technology"},
    {"symbol": "GOOGL", "sector": "Communication Services"},
    {"symbol": "GOOG",  "sector": "Communication Services"},
    {"symbol": "AMZN",  "sector": "Consumer Cyclical"},
    {"symbol": "META",  "sector": "Communication Services"},
    {"symbol": "TSLA",  "sector": "Consumer Cyclical"},
    {"symbol": "BRK-B", "sector": "Financial Services"},
    {"symbol": "JPM",   "sector": "Financial Services"},
    {"symbol": "LLY",   "sector": "Healthcare"},
    {"symbol": "V",     "sector": "Financial Services"},
    {"symbol": "XOM",   "sector": "Energy"},
    {"symbol": "UNH",   "sector": "Healthcare"},
    {"symbol": "JNJ",   "sector": "Healthcare"},
    {"symbol": "MA",    "sector": "Financial Services"},
    {"symbol": "WMT",   "sector": "Consumer Defensive"},
    {"symbol": "PG",    "sector": "Consumer Defensive"},
    {"symbol": "AVGO",  "sector": "Technology"},
    {"symbol": "HD",    "sector": "Consumer Cyclical"},
    {"symbol": "ORCL",  "sector": "Technology"},
    {"symbol": "MRK",   "sector": "Healthcare"},
    {"symbol": "COST",  "sector": "Consumer Defensive"},
    {"symbol": "ABBV",  "sector": "Healthcare"},
    {"symbol": "BAC",   "sector": "Financial Services"},
    {"symbol": "CVX",   "sector": "Energy"},
    {"symbol": "ADBE",  "sector": "Technology"},
    {"symbol": "KO",    "sector": "Consumer Defensive"},
    {"symbol": "CRM",   "sector": "Technology"},
    {"symbol": "PEP",   "sector": "Consumer Defensive"},
    {"symbol": "AMD",   "sector": "Technology"},
    {"symbol": "ACN",   "sector": "Technology"},
    {"symbol": "TMO",   "sector": "Healthcare"},
    {"symbol": "MCD",   "sector": "Consumer Cyclical"},
    {"symbol": "CSCO",  "sector": "Technology"},
    {"symbol": "WFC",   "sector": "Financial Services"},
    {"symbol": "ABT",   "sector": "Healthcare"},
    {"symbol": "LIN",   "sector": "Basic Materials"},
    {"symbol": "DHR",   "sector": "Healthcare"},
    {"symbol": "DIS",   "sector": "Communication Services"},
    {"symbol": "TXN",   "sector": "Technology"},
    {"symbol": "NFLX",  "sector": "Communication Services"},
    {"symbol": "GE",    "sector": "Industrials"},
    {"symbol": "IBM",   "sector": "Technology"},
    {"symbol": "INTU",  "sector": "Technology"},
    {"symbol": "AMGN",  "sector": "Healthcare"},
    {"symbol": "VZ",    "sector": "Communication Services"},
    {"symbol": "PFE",   "sector": "Healthcare"},
    {"symbol": "QCOM",  "sector": "Technology"},
    {"symbol": "CMCSA", "sector": "Communication Services"},
]


# ---------- HTTP helper ----------
def http_json(url, retries=4):
    backoffs = [5, 15, 30, 60]
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8", errors="ignore"))
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < retries:
                time.sleep(backoffs[min(attempt, len(backoffs) - 1)])
                continue
            return {"_error": f"HTTP {e.code}"}
        except Exception as e:
            if attempt < retries:
                time.sleep(2)
                continue
            return {"_error": str(e)[:100]}
    return {"_error": "exhausted retries"}


def fmp_income_statement(symbol, years=HISTORY_YEARS):
    url = (f"{FMP_BASE}/income-statement?symbol={symbol}"
           f"&period=annual&limit={years}&apikey={FMP_KEY}")
    d = http_json(url)
    return d if isinstance(d, list) else []


def fmp_quote(symbol):
    url = f"{FMP_BASE}/quote?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    if isinstance(d, list) and d:
        return d[0]
    return {}


def fmp_ratios_ttm(symbol):
    """/stable/ratios-ttm has priceToEarningsRatioTTM (PE) etc.
    The /stable/quote endpoint does NOT include PE."""
    url = f"{FMP_BASE}/ratios-ttm?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    if isinstance(d, list) and d:
        return d[0]
    return {}


# ---------- Statistics ----------
def r_squared(values):
    """R^2 of linear regression y ~ x where x = [0, 1, ..., n-1].
    Returns float in [0, 1] or None if insufficient data / zero variance."""
    if not values or len(values) < 4:
        return None
    n = len(values)
    xs = list(range(n))
    y_mean = statistics.mean(values)
    x_mean = statistics.mean(xs)
    num = sum((xs[i] - x_mean) * (values[i] - y_mean) for i in range(n))
    den_x = sum((x - x_mean) ** 2 for x in xs)
    den_y = sum((y - y_mean) ** 2 for y in values)
    if den_x == 0 or den_y == 0:
        return None
    # Pearson correlation^2 = R^2 for simple linear regression
    r = num / math.sqrt(den_x * den_y)
    return round(r * r, 4)


def cagr(values):
    """Compound annual growth rate from values[0] (oldest) to values[-1] (newest).
    Years = len(values) - 1. Returns pct float or None if invalid."""
    if not values or len(values) < 2:
        return None
    start = values[0]
    end = values[-1]
    if start <= 0 or end <= 0:
        return None
    years = len(values) - 1
    try:
        rate = (end / start) ** (1 / years) - 1
        return round(rate * 100, 2)
    except (ValueError, ZeroDivisionError):
        return None


def yoy_growth_cov(values):
    """Coefficient of variation (stdev/mean) of YoY growth rates.
    Lower = more consistent growth. Returns float or None."""
    if not values or len(values) < 3:
        return None
    growths = []
    for i in range(1, len(values)):
        if values[i - 1] == 0:
            continue
        try:
            g = (values[i] - values[i - 1]) / abs(values[i - 1])
            growths.append(g)
        except (ZeroDivisionError, ValueError):
            continue
    if len(growths) < 2:
        return None
    try:
        m = statistics.mean(growths)
        sd = statistics.stdev(growths)
        if abs(m) < 0.001:
            return None
        return round(abs(sd / m), 3)
    except statistics.StatisticsError:
        return None


# ---------- Predictability classification ----------
def predictability_stars(rev_r2, eps_r2):
    if rev_r2 is None or eps_r2 is None:
        return None
    if rev_r2 >= 0.95 and eps_r2 >= 0.85:
        return 5
    if rev_r2 >= 0.90 and eps_r2 >= 0.75:
        return 4
    if rev_r2 >= 0.80 and eps_r2 >= 0.60:
        return 3
    if rev_r2 >= 0.65 or eps_r2 >= 0.50:
        return 2
    return 1


def valuation_bucket(pe_ttm):
    if pe_ttm is None or pe_ttm <= 0:
        return "UNKNOWN"
    if pe_ttm < 20:
        return "CHEAP"
    if pe_ttm < 30:
        return "FAIR"
    return "RICH"


# ---------- Per-ticker analysis ----------
def analyze_ticker(symbol, sector):
    inc = fmp_income_statement(symbol)
    time.sleep(FMP_SLEEP_SEC)
    q = fmp_quote(symbol)
    time.sleep(FMP_SLEEP_SEC)
    ratios = fmp_ratios_ttm(symbol)
    time.sleep(FMP_SLEEP_SEC)

    if not inc or len(inc) < 4:
        return {"ticker": symbol, "sector": sector, "ok": False,
                "error": f"insufficient income history ({len(inc)} years)"}

    # FMP returns most-recent year first; reverse to chronological order
    inc_sorted = sorted(inc, key=lambda x: x.get("date", "") or "")
    revs = []
    eps_vals = []
    fiscal_years = []
    for row in inc_sorted:
        r = row.get("revenue")
        e = (row.get("epsDiluted") or row.get("eps") or
             row.get("epsdiluted"))
        if r is not None and e is not None:
            try:
                revs.append(float(r))
                eps_vals.append(float(e))
                fiscal_years.append(row.get("date", "")[:4])
            except (ValueError, TypeError):
                continue

    if len(revs) < 4:
        return {"ticker": symbol, "sector": sector, "ok": False,
                "error": f"insufficient clean data ({len(revs)} years)"}

    rev_r2 = r_squared(revs)
    eps_r2 = r_squared(eps_vals)
    rev_cagr = cagr(revs)
    eps_cagr = cagr(eps_vals)
    rev_cov = yoy_growth_cov(revs)
    eps_cov = yoy_growth_cov(eps_vals)
    stars = predictability_stars(rev_r2, eps_r2)

    price = q.get("price")
    pe_ttm = (ratios.get("priceToEarningsRatioTTM") or
              ratios.get("priceToEarningsRatio"))
    if pe_ttm is not None:
        try:
            pe_ttm = float(pe_ttm)
        except (ValueError, TypeError):
            pe_ttm = None
    market_cap = q.get("marketCap")
    val_bucket = valuation_bucket(pe_ttm)

    return {
        "ticker": symbol,
        "sector": sector,
        "ok": True,
        "n_years": len(revs),
        "fiscal_years": fiscal_years,
        "revenue_r2": rev_r2,
        "revenue_cagr_pct": rev_cagr,
        "revenue_yoy_cov": rev_cov,
        "revenue_latest_usd": revs[-1] if revs else None,
        "revenue_oldest_usd": revs[0] if revs else None,
        "eps_r2": eps_r2,
        "eps_cagr_pct": eps_cagr,
        "eps_yoy_cov": eps_cov,
        "eps_latest": eps_vals[-1] if eps_vals else None,
        "eps_oldest": eps_vals[0] if eps_vals else None,
        "predictability_stars": stars,
        "valuation_bucket": val_bucket,
        "pe_ttm": pe_ttm,
        "price": price,
        "market_cap_usd": market_cap,
        "sweet_spot": (stars == 5 and val_bucket == "CHEAP"),
        "premium_warranted": (stars >= 4 and val_bucket in ("FAIR", "RICH")),
    }


def telegram_notify(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown",
        }).encode("utf-8")
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10)
    except Exception:
        pass


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)

    if not FMP_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": "FMP_KEY not set"})}

    # 1. Universe + per-ticker analysis
    per_ticker = []
    for t in STATIC_TOP50_SPX:
        try:
            row = analyze_ticker(t["symbol"], t["sector"])
            per_ticker.append(row)
        except Exception as e:
            per_ticker.append({"ticker": t["symbol"], "sector": t["sector"],
                                "ok": False, "error": str(e)[:100]})

    ok_rows = [r for r in per_ticker if r.get("ok")]
    n_ok = len(ok_rows)

    # 2. Aggregate stats
    star_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, None: 0}
    bucket_counts = {"CHEAP": 0, "FAIR": 0, "RICH": 0, "UNKNOWN": 0}
    for r in ok_rows:
        s = r.get("predictability_stars")
        star_counts[s if s in (1, 2, 3, 4, 5) else None] += 1
        bucket_counts[r.get("valuation_bucket", "UNKNOWN")] += 1

    # 3. Sweet spot detection
    sweet_spot_picks = sorted(
        [r for r in ok_rows if r.get("sweet_spot")],
        key=lambda x: (-(x.get("predictability_stars") or 0),
                       x.get("pe_ttm") or 999))

    # 4. Top 5* moat list (all 5-star regardless of valuation)
    elite_moats = sorted(
        [r for r in ok_rows if r.get("predictability_stars") == 5],
        key=lambda x: -(x.get("revenue_r2") or 0))

    # 5. Most predictable overall (rank by composite R^2)
    most_predictable = sorted(
        [r for r in ok_rows if r.get("revenue_r2") is not None and
         r.get("eps_r2") is not None],
        key=lambda x: -((x.get("revenue_r2") or 0) +
                        (x.get("eps_r2") or 0)))[:15]

    # 6. Least predictable (cyclical / lumpy)
    least_predictable = sorted(
        [r for r in ok_rows if r.get("predictability_stars") in (1, 2)],
        key=lambda x: (x.get("predictability_stars") or 5,
                       (x.get("revenue_r2") or 1) + (x.get("eps_r2") or 1)))[:10]

    # 7. Sector breakdown
    sector_stars = {}
    for r in ok_rows:
        sec = r.get("sector", "Unknown")
        sec_data = sector_stars.setdefault(sec, {"n": 0, "stars_sum": 0,
                                                  "n_5star": 0})
        sec_data["n"] += 1
        sec_data["stars_sum"] += (r.get("predictability_stars") or 0)
        if r.get("predictability_stars") == 5:
            sec_data["n_5star"] += 1
    sector_breakdown = {
        sec: {"n": d["n"],
              "avg_stars": round(d["stars_sum"] / d["n"], 2) if d["n"] else 0,
              "n_5star": d["n_5star"]}
        for sec, d in sector_stars.items()}

    # 8. Universe-state classification
    pct_4plus = ((star_counts[4] + star_counts[5]) / max(1, n_ok)) * 100
    if pct_4plus >= 50:
        universe_state = "HIGH_MOAT_CONCENTRATION"
    elif pct_4plus >= 30:
        universe_state = "BALANCED_PREDICTABILITY"
    else:
        universe_state = "LOW_MOAT_CONCENTRATION"

    # 9. Build output
    out = {
        "ok": True,
        "version": VERSION,
        "generated_at": started.isoformat(),
        "universe_state": universe_state,
        "n_universe": len(STATIC_TOP50_SPX),
        "n_analyzed": n_ok,
        "n_failed": len(per_ticker) - n_ok,
        "history_years": HISTORY_YEARS,
        "star_distribution": {
            "5_star": star_counts[5],
            "4_star": star_counts[4],
            "3_star": star_counts[3],
            "2_star": star_counts[2],
            "1_star": star_counts[1],
            "unrated": star_counts[None],
        },
        "valuation_distribution": bucket_counts,
        "n_sweet_spot": len(sweet_spot_picks),
        "n_elite_moats": len(elite_moats),
        "sector_breakdown": sector_breakdown,
        "sweet_spot_picks": [
            {"ticker": r["ticker"], "sector": r["sector"],
             "stars": r["predictability_stars"],
             "valuation": r["valuation_bucket"],
             "pe_ttm": r["pe_ttm"], "price": r["price"],
             "rev_r2": r["revenue_r2"], "eps_r2": r["eps_r2"],
             "rev_cagr_pct": r["revenue_cagr_pct"],
             "eps_cagr_pct": r["eps_cagr_pct"],
             "n_years": r["n_years"],
             "market_cap_usd": r["market_cap_usd"]}
            for r in sweet_spot_picks
        ],
        "elite_moats": [
            {"ticker": r["ticker"], "sector": r["sector"],
             "stars": r["predictability_stars"],
             "valuation": r["valuation_bucket"],
             "pe_ttm": r["pe_ttm"], "price": r["price"],
             "rev_r2": r["revenue_r2"], "eps_r2": r["eps_r2"],
             "rev_cagr_pct": r["revenue_cagr_pct"],
             "eps_cagr_pct": r["eps_cagr_pct"],
             "rev_cov": r["revenue_yoy_cov"],
             "eps_cov": r["eps_yoy_cov"],
             "n_years": r["n_years"],
             "market_cap_usd": r["market_cap_usd"]}
            for r in elite_moats
        ],
        "most_predictable_top_15": [
            {"ticker": r["ticker"], "sector": r["sector"],
             "stars": r["predictability_stars"],
             "rev_r2": r["revenue_r2"], "eps_r2": r["eps_r2"],
             "composite_r2": round((r.get("revenue_r2", 0) +
                                    r.get("eps_r2", 0)) / 2, 4),
             "rev_cagr_pct": r["revenue_cagr_pct"],
             "eps_cagr_pct": r["eps_cagr_pct"],
             "pe_ttm": r["pe_ttm"],
             "valuation": r["valuation_bucket"]}
            for r in most_predictable
        ],
        "least_predictable": [
            {"ticker": r["ticker"], "sector": r["sector"],
             "stars": r["predictability_stars"],
             "rev_r2": r["revenue_r2"], "eps_r2": r["eps_r2"],
             "rev_cov": r["revenue_yoy_cov"],
             "eps_cov": r["eps_yoy_cov"],
             "pe_ttm": r["pe_ttm"],
             "valuation": r["valuation_bucket"]}
            for r in least_predictable
        ],
        "all_tickers": [
            {"ticker": r["ticker"], "sector": r["sector"],
             "stars": r.get("predictability_stars"),
             "rev_r2": r.get("revenue_r2"),
             "eps_r2": r.get("eps_r2"),
             "rev_cagr_pct": r.get("revenue_cagr_pct"),
             "eps_cagr_pct": r.get("eps_cagr_pct"),
             "pe_ttm": r.get("pe_ttm"),
             "valuation": r.get("valuation_bucket"),
             "n_years": r.get("n_years"),
             "ok": r.get("ok"), "error": r.get("error")}
            for r in per_ticker
        ],
        "methodology": {
            "data_source": "FMP /stable/income-statement, 10y annual depth",
            "rev_metric": ("R^2 of linear regression on revenue vs year index "
                           "(1.0 = perfectly smooth growth)"),
            "eps_metric": ("R^2 of linear regression on diluted EPS vs year "
                           "index (1.0 = perfectly smooth)"),
            "star_thresholds": {
                "5_star": "Rev R^2 >= 0.95 AND EPS R^2 >= 0.85 (elite moat)",
                "4_star": "Rev R^2 >= 0.90 AND EPS R^2 >= 0.75 (strong)",
                "3_star": "Rev R^2 >= 0.80 AND EPS R^2 >= 0.60 (moderate)",
                "2_star": "Rev R^2 >= 0.65 OR EPS R^2 >= 0.50 (some)",
                "1_star": "anything below (cyclical/unpredictable)",
            },
            "valuation_buckets": {
                "CHEAP": "PE_ttm < 20",
                "FAIR":  "20 <= PE_ttm < 30",
                "RICH":  "PE_ttm >= 30",
            },
            "sweet_spot": "5* predictability + CHEAP valuation",
            "universe": "STATIC_TOP50_SPX (deterministic, FMP-quota friendly)",
        },
        "edge_basis": ("GuruFocus published research: 5* predictability stocks "
                       "with PE < 20 returned 11.4% annualized 2000-2020 vs "
                       "S&P 500 at 6.2%. Smooth historical growth is a strong "
                       "moat proxy and justifies premium multiples. The market "
                       "systematically under-prices boring predictability."),
        "universe_state_meaning": {
            "HIGH_MOAT_CONCENTRATION":  ">=50% of names are 4* or 5*",
            "BALANCED_PREDICTABILITY":   "30-50% of names are 4* or 5*",
            "LOW_MOAT_CONCENTRATION":   "<30% of names are 4* or 5*",
        },
        "sources": {
            "income_statement": "FMP /stable/income-statement",
            "quote": "FMP /stable/quote",
            "ratios_ttm": "FMP /stable/ratios-ttm (PE for valuation overlay)",
            "universe": "STATIC_TOP50_SPX (shared with StarMine #4)",
        },
    }

    # 10. Persist
    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=300",
        )
    except Exception as e:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False,
                                    "error": f"s3 put failed: {str(e)[:200]}"})}

    # 11. Telegram alert on sweet-spot detection
    if sweet_spot_picks:
        names = ", ".join(f"{p['ticker']}(PE {p['pe_ttm']:.0f})"
                           for p in sweet_spot_picks[:5])
        telegram_notify(
            f"⭐⭐⭐⭐⭐ *Predictability Sweet Spot*\n"
            f"{len(sweet_spot_picks)} elite-moat names at cheap valuations:\n"
            f"{names}\n"
            f"justhodl.ai/predictability.html"
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "version": VERSION,
            "universe_state": universe_state,
            "n_analyzed": n_ok,
            "n_sweet_spot": len(sweet_spot_picks),
            "n_elite_moats": len(elite_moats),
            "star_distribution": out["star_distribution"],
            "valuation_distribution": bucket_counts,
        }),
    }


if __name__ == "__main__":
    r = lambda_handler({}, None)
    print(json.dumps(r, indent=2))
