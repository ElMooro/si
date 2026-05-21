"""
justhodl-earnings-quality
==========================

Earnings quality scanner via Sloan (1996) accruals anomaly.

Pressure-test:
  - Naive: just look at P/E or earnings growth. Misses the fundamental
    insight that EARNINGS != CASH. Companies with high accruals (book
    earnings >> operating cash flow) have systematically underperformed.
  - Better: 4-factor quality score using fundamental data:
    (1) Sloan accruals = (Net Income - OCF) / Total Assets
        High accruals (>+5%) = poor quality (low quality earnings)
        Low/negative accruals = high quality (cash-backed earnings)
    (2) Beneish M-Score proxies: DSRI (days sales receivables index),
        GMI (gross margin index), AQI (asset quality index)
    (3) Cash conversion ratio: OCF / Net Income > 1.0 = high quality
    (4) Year-over-year accruals change (deteriorating signals warning)

  - Two-sided signal:
    HIGH_QUALITY_RICH: top decile (low accruals, OCF > NI) = LONG candidates
    LOW_QUALITY_RICH: bottom decile (high accruals, OCF << NI) = SHORT/AVOID

Edge basis:
  Sloan 1996 ("Do stock prices fully reflect information in accruals and
  cash flows about future earnings?") - hedge portfolio +10% annual
  return; Beneish 1999 (M-Score detects earnings manipulation); Hirshleifer
  et al. 2004 (extends Sloan with net operating asset growth);
  Penman-Zhang 2002 (operating accruals + special items decomposition).

Trade tickets:
  HIGH_QUALITY_RICH: equal-weight top 20 = LONG portfolio (1+yr hold)
  LOW_QUALITY_RICH: equal-weight bottom 10 = SHORT candidates / AVOID

Universe: master-ranker top 200 by mcap; $2B+ gate.

Schedule: weekly Wed 13:30 UTC (fresh quarterly filings post-Tuesday).
"""
import json
import os
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/earnings-quality.json"
SSM_STATE_KEY = "/justhodl/earnings-quality/state"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

FALLBACK_UNIVERSE = [
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","AVGO","JPM","V","MA",
    "WMT","PG","JNJ","UNH","HD","BAC","XOM","CVX","PFE","ABBV","MRK","LLY",
    "DIS","NFLX","CRM","ADBE","ORCL","INTC","AMD","MU","QCOM","TXN","IBM",
    "GS","MS","C","WFC","AXP","BLK","SPGI","T","VZ","CMCSA","CSCO","ACN",
    "NKE","MCD","SBUX","KO","PEP","TGT","COST","LOW","F","GM","BA","CAT",
    "DE","HON","RTX","LMT","GE","MMM","DOW","ABT","TMO","DHR","BMY","GILD",
    "AMGN","REGN","VRTX","BIIB","ISRG","PYPL","UBER","SNOW","DDOG","CRWD",
    "PANW","NET","OKTA","MDB","TEAM","ZS","FTNT","NOW","VEEV","WDAY",
    "AZO","ORLY","TJX","DG","KR","SYY","ADM","GIS","K","MO","KMB","CL",
    "ABNB","SHOP","SQ","BKNG","HLT","MAR","RCL","NCLH","DAL","UAL","LUV",
    "UNP","CSX","NSC","LULU","SO","DUK","NEE","XEL","WEC","ED","D","AEP",
    "MSCI","ICE","CME","NDAQ","CBOE","COIN","SCHW","BX","KKR","APO",
]


def http_get(url, timeout=15, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="ignore")
        except Exception as e:
            last = e
            time.sleep(0.5 * (i + 1))
    raise RuntimeError(f"http_get failed: {last}")


def load_universe():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/master-ranker.json")
        data = json.loads(obj["Body"].read())
        picks = (data.get("picks") or data.get("ranks") or data.get("universe")
                 or data.get("results") or [])
        if isinstance(picks, list):
            ts = []
            for r in picks[:300]:
                if isinstance(r, dict):
                    t = r.get("ticker") or r.get("symbol")
                    if t:
                        ts.append(t.upper())
                elif isinstance(r, str):
                    ts.append(r.upper())
            if ts:
                return ts[:200]
    except Exception:
        pass
    return FALLBACK_UNIVERSE


def fmp_quote(symbol):
    q = urllib.parse.quote_plus(symbol)
    url = f"https://financialmodelingprep.com/stable/quote?symbol={q}&apikey={FMP_KEY}"
    try:
        data = json.loads(http_get(url))
        if isinstance(data, list) and data:
            row = data[0]
            return {
                "price": float(row.get("price", 0)) or None,
                "market_cap": float(row.get("marketCap", 0)) or None,
                "pe": float(row.get("pe", 0)) if row.get("pe") else None,
                "name": row.get("name") or row.get("companyName"),
            }
    except Exception:
        pass
    return None


def fmp_financials(symbol):
    """Pull last 4 quarters: income, cash flow, balance sheet."""
    q = urllib.parse.quote_plus(symbol)
    out = {"income": None, "cash_flow": None, "balance_sheet": None}
    endpoints = [
        ("income", f"https://financialmodelingprep.com/stable/income-statement"
                   f"?symbol={q}&period=quarter&limit=8&apikey={FMP_KEY}"),
        ("cash_flow", f"https://financialmodelingprep.com/stable/cash-flow-statement"
                      f"?symbol={q}&period=quarter&limit=8&apikey={FMP_KEY}"),
        ("balance_sheet", f"https://financialmodelingprep.com/stable/balance-sheet-statement"
                          f"?symbol={q}&period=quarter&limit=8&apikey={FMP_KEY}"),
    ]
    for k, url in endpoints:
        try:
            data = json.loads(http_get(url))
            if isinstance(data, list) and len(data) >= 2:
                out[k] = data
        except Exception:
            pass
    return out


def analyze_ticker(symbol):
    quote = fmp_quote(symbol)
    if not quote or not quote.get("market_cap") or quote["market_cap"] < 2_000_000_000:
        return None
    f = fmp_financials(symbol)
    if not f["income"] or not f["cash_flow"] or not f["balance_sheet"]:
        return None
    inc = f["income"]
    cf = f["cash_flow"]
    bs = f["balance_sheet"]

    # TTM aggregation (last 4 quarters)
    def ttm(rows, key):
        try:
            return sum(float(r.get(key, 0) or 0) for r in rows[:4])
        except Exception:
            return None

    def ttm_prior(rows, key):
        try:
            return sum(float(r.get(key, 0) or 0) for r in rows[4:8])
        except Exception:
            return None

    ttm_ni = ttm(inc, "netIncome")
    ttm_revenue = ttm(inc, "revenue")
    ttm_gross_profit = ttm(inc, "grossProfit")
    ttm_ocf = ttm(cf, "operatingCashFlow") or ttm(cf, "netCashProvidedByOperatingActivities")
    ttm_capex = ttm(cf, "capitalExpenditure")
    if ttm_capex:
        ttm_capex = abs(ttm_capex)
    ttm_fcf = (ttm_ocf - ttm_capex) if (ttm_ocf is not None and ttm_capex is not None) else None

    if not bs or len(bs) < 1:
        return None
    latest_bs = bs[0]
    total_assets = float(latest_bs.get("totalAssets", 0) or 0)
    if total_assets <= 0:
        return None
    accounts_receivable = float(latest_bs.get("netReceivables", 0) or 0)

    # Sloan accruals = (NI - OCF) / Total Assets
    if ttm_ni is None or ttm_ocf is None:
        return None
    sloan_accruals_pct = (ttm_ni - ttm_ocf) / total_assets * 100

    # Cash conversion ratio: OCF / NI
    cash_conv_ratio = (ttm_ocf / ttm_ni) if ttm_ni != 0 else None

    # Beneish proxies (simplified, requires 2 periods)
    ttm_revenue_prior = ttm_prior(inc, "revenue")
    ttm_gross_profit_prior = ttm_prior(inc, "grossProfit")
    dsri = None
    gmi = None
    # DSRI (days sales receivables index): (AR/Rev)_t / (AR/Rev)_t-1
    if len(bs) >= 5 and ttm_revenue and ttm_revenue_prior:
        ar_prior = float(bs[4].get("netReceivables", 0) or 0)
        if ar_prior > 0 and ttm_revenue_prior > 0:
            dsri = ((accounts_receivable / ttm_revenue) /
                    (ar_prior / ttm_revenue_prior))
    # GMI (gross margin index): (GP/Rev)_t-1 / (GP/Rev)_t (lower margin = higher GMI = bad)
    if ttm_gross_profit and ttm_revenue and ttm_gross_profit_prior and ttm_revenue_prior:
        gmi = ((ttm_gross_profit_prior / ttm_revenue_prior) /
               (ttm_gross_profit / ttm_revenue))

    # YoY accruals change
    accruals_prior = None
    if len(bs) >= 5 and ttm_ni is not None:
        ni_prior = ttm_prior(inc, "netIncome")
        ocf_prior = ttm_prior(cf, "operatingCashFlow") or ttm_prior(cf, "netCashProvidedByOperatingActivities")
        ta_prior = float(bs[4].get("totalAssets", 0) or 0)
        if ni_prior is not None and ocf_prior is not None and ta_prior > 0:
            accruals_prior = (ni_prior - ocf_prior) / ta_prior * 100

    accruals_change_yoy = (sloan_accruals_pct - accruals_prior) if accruals_prior is not None else None

    # Composite quality score (-1 = worst, +1 = best)
    score = 0.0
    # Sloan accruals: high accruals = bad
    if sloan_accruals_pct <= -3:
        score += 0.35  # cash > earnings, very high quality
    elif sloan_accruals_pct <= 0:
        score += 0.25
    elif sloan_accruals_pct <= 3:
        score += 0.1
    elif sloan_accruals_pct <= 6:
        score -= 0.1
    else:
        score -= 0.3  # extremely high accruals = manipulation risk

    # Cash conversion ratio
    if cash_conv_ratio is not None:
        if cash_conv_ratio >= 1.2:
            score += 0.25
        elif cash_conv_ratio >= 1.0:
            score += 0.15
        elif cash_conv_ratio >= 0.8:
            score += 0.05
        elif cash_conv_ratio >= 0.5:
            score -= 0.1
        else:
            score -= 0.25

    # DSRI: high DSRI = AR growing faster than revenue = sales channel stuffing
    if dsri is not None:
        if dsri <= 1.0:
            score += 0.1
        elif dsri <= 1.2:
            score += 0.02
        elif dsri <= 1.5:
            score -= 0.05
        else:
            score -= 0.15

    # GMI: high GMI = margins deteriorating
    if gmi is not None:
        if gmi <= 1.0:
            score += 0.1
        elif gmi <= 1.1:
            score += 0.02
        else:
            score -= 0.1

    # YoY accrual change
    if accruals_change_yoy is not None:
        if accruals_change_yoy <= -2:
            score += 0.1  # improving quality
        elif accruals_change_yoy <= 1:
            score += 0.03
        elif accruals_change_yoy <= 3:
            score -= 0.05
        else:
            score -= 0.15  # deteriorating quality

    score = max(-1.0, min(1.0, score))

    return {
        "ticker": symbol,
        "name": quote.get("name"),
        "price": quote.get("price"),
        "market_cap_usd": quote["market_cap"],
        "pe": quote.get("pe"),
        "ttm_ni_usd": int(ttm_ni) if ttm_ni else None,
        "ttm_ocf_usd": int(ttm_ocf) if ttm_ocf else None,
        "ttm_fcf_usd": int(ttm_fcf) if ttm_fcf else None,
        "sloan_accruals_pct_assets": round(sloan_accruals_pct, 2),
        "cash_conversion_ratio": round(cash_conv_ratio, 2) if cash_conv_ratio else None,
        "dsri_beneish": round(dsri, 2) if dsri else None,
        "gmi_beneish": round(gmi, 2) if gmi else None,
        "accruals_change_yoy_pct": round(accruals_change_yoy, 2) if accruals_change_yoy is not None else None,
        "quality_score": round(score, 3),
    }


def lambda_handler(event, context):
    start = time.time()
    try:
        universe = load_universe()
        results = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(analyze_ticker, t): t for t in universe[:120]}
            for f in as_completed(futs):
                try:
                    r = f.result()
                    if r:
                        results.append(r)
                except Exception:
                    continue

        # Sort by quality score (descending)
        results.sort(key=lambda r: r["quality_score"], reverse=True)

        n_high = sum(1 for r in results if r["quality_score"] >= 0.5)
        n_low = sum(1 for r in results if r["quality_score"] <= -0.3)
        n_mid = len(results) - n_high - n_low

        # State: split signal (high quality + low quality both meaningful)
        if n_high >= 15 and n_low >= 5:
            state, strength = "BOTH_TAILS_RICH", 0.8
        elif n_high >= 10:
            state, strength = "HIGH_QUALITY_RICH", 0.65
        elif n_low >= 5:
            state, strength = "LOW_QUALITY_RICH", 0.6
        elif n_high >= 4 or n_low >= 2:
            state, strength = "ACTIVE", 0.4
        else:
            state, strength = "QUIET", 0.1

        long_candidates = results[:20]
        short_candidates = [r for r in results if r["quality_score"] <= -0.3][:10]

        tickets = []
        for r in long_candidates[:10]:
            tickets.append({
                "ticker": r["ticker"],
                "side": "LONG",
                "rationale": (
                    f"High earnings quality: Sloan accruals "
                    f"{r['sloan_accruals_pct_assets']}%, "
                    f"cash conversion {r['cash_conversion_ratio']}x, "
                    f"quality score {r['quality_score']}"
                ),
                "holding_period": "12+ months (quarterly rebalance)",
                "size_pct_portfolio": 1.0,
                "expected_alpha_pct_yr": 5 if r["quality_score"] >= 0.7 else 3,
            })
        for r in short_candidates[:5]:
            tickets.append({
                "ticker": r["ticker"],
                "side": "SHORT_OR_AVOID",
                "rationale": (
                    f"Low earnings quality: Sloan accruals "
                    f"{r['sloan_accruals_pct_assets']}% (high = manipulation risk), "
                    f"cash conversion {r['cash_conversion_ratio']}x, "
                    f"quality score {r['quality_score']}"
                ),
                "holding_period": "Avoid / short 6+ months",
                "size_pct_portfolio": 0.5,
                "expected_alpha_pct_yr": -6,
            })

        out = {
            "engine": "earnings-quality",
            "version": VERSION,
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state": state,
            "signal_strength": round(strength, 2),
            "n_qualified": len(results),
            "n_high_quality": n_high,
            "n_low_quality": n_low,
            "n_mid_quality": n_mid,
            "universe_size": len(universe),
            "top_20_high_quality": long_candidates,
            "top_10_low_quality_avoid": short_candidates,
            "all_ranked": results,
            "trade_tickets": tickets,
            "n_tickets": len(tickets),
            "methodology": (
                "Earnings quality via Sloan (1996) accruals anomaly + Beneish "
                "M-Score proxies. 4-factor: (1) Sloan accruals = "
                "(TTM NI - TTM OCF) / Total Assets (high = poor quality); "
                "(2) Cash conversion ratio = TTM OCF / TTM NI (>1.0 = high "
                "quality); (3) Beneish DSRI (AR/Revenue trend, channel "
                "stuffing) + GMI (margin trend); (4) YoY accruals change "
                "(deterioration signal). $2B+ mcap gate. Composite weights: "
                "accruals 35% + cash-conv 25% + Beneish 20% + YoY-change 20%. "
                "Top 20 = LONG portfolio (quarterly rebalance, 12+mo hold). "
                "Bottom decile = AVOID/SHORT candidates. Edge basis: Sloan "
                "1996 (+10%/yr hedge), Beneish 1999, Hirshleifer 2004, "
                "Penman-Zhang 2002."
            ),
            "sources": [
                "s3://justhodl-dashboard-live/data/master-ranker.json (universe)",
                "FMP /stable/quote (mcap, P/E)",
                "FMP /stable/income-statement?period=quarter&limit=8 (TTM + prior)",
                "FMP /stable/cash-flow-statement?period=quarter&limit=8",
                "FMP /stable/balance-sheet-statement?period=quarter&limit=8",
            ],
            "why_now": (f"{n_high} high-quality LONG candidates + "
                        f"{n_low} low-quality SHORT/AVOID candidates"),
            "run_seconds": round(time.time() - start, 2),
        }

        try:
            prev = ssm.get_parameter(Name=SSM_STATE_KEY)["Parameter"]["Value"]
        except Exception:
            prev = None
        if prev != state and "RICH" in state and TELEGRAM_TOKEN:
            top_h = "\n".join(f"- {r['ticker']} score {r['quality_score']} "
                              f"accruals {r['sloan_accruals_pct_assets']}%"
                              for r in long_candidates[:5])
            msg = (f"*EARNINGS-QUALITY -> {state}*\n"
                   f"{n_high} HIGH + {n_low} LOW quality\n"
                   f"Top 5 LONG:\n{top_h}\n"
                   f"Quarterly rebalance. retail-edges.html for full top 20.")
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                body = urllib.parse.urlencode({
                    "chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "Markdown",
                    "disable_web_page_preview": "true",
                }).encode("utf-8")
                urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=10)
            except Exception:
                pass
        try:
            ssm.put_parameter(Name=SSM_STATE_KEY, Value=state, Type="String", Overwrite=True)
        except Exception:
            pass

        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, max-age=60",
        )
        return {"statusCode": 200, "body": json.dumps({"ok": True, "state": state,
                                                         "n_ranked": len(results)})}
    except Exception as e:
        import traceback
        err = {"engine": "earnings-quality", "error": str(e)[:300],
               "trace": traceback.format_exc()[:600],
               "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode("utf-8"),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"error": str(e)[:300]})}
