"""
justhodl-gf-value
=================

GuruFocus-style Composite Fair Value Engine (Pro Pack v3 #1).

Pressure-test:
  - Naive: single-method DCF or simple P/E ratio. Misses cyclicals,
    capital-intensive businesses, declining-EPS firms, broken-multiple
    industries.
  - Better: institutional-grade composite fair value blending 3
    independent valuation lenses, each with its own data requirements
    + failure modes, then weighted by which lenses produce valid output:
    1. DCF (intrinsic value via projected free cash flow)
    2. EV/EBIT Multiple (relative value vs own 10yr median multiple)
    3. Graham Number (defensive value floor via tangible earnings + book)
  - Winsorize composite by 5yr price range to prevent runaway estimates
    (Buffett: "intrinsic value doesn't move 30% in a year unless
    something fundamental broke")

Universe: S&P 500 constituents via FMP /stable/sp500-constituent.

Per-ticker computation:
  DCF:
    FCF_TTM = TTM free cash flow
    growth = clip(5yr FCF CAGR, 3%, 15%)
    WACC = 9% default (single-rate, conservative)
    Projection: 10yr FCF stream + terminal value at 15x
    Equity value = sum(PV FCF) + PV(TV) - net_debt
    Fair price = equity_value / shares_outstanding

  EV/EBIT:
    EBIT_TTM = TTM operating income
    fair_multiple = median(10yr EV/EBIT, excluding outliers >3sigma)
    fair_EV = EBIT_TTM * fair_multiple
    fair_equity = fair_EV - net_debt + cash
    Fair price = fair_equity / shares

  Graham Number (requires EPS > 0 AND BVPS > 0):
    graham = sqrt(22.5 * EPS_TTM * BVPS)

  Composite:
    Valid lenses get weights (40% DCF, 35% EV/EBIT, 25% Graham);
    invalid lenses redistribute weight to others.
    Winsorize composite: clip to [low_5yr * 0.7, high_5yr * 1.5].

Margin of Safety = (GF Value - current_price) / GF Value * 100

Rating bands:
  MoS >= +50%   -> DEEP_VALUE
  +20% to +50%  -> MODESTLY_UNDERVALUED
  -10% to +20%  -> FAIR
  -30% to -10%  -> MODESTLY_OVERVALUED
  MoS <= -30%   -> SIGNIFICANTLY_OVERVALUED

Edge basis:
  Graham 1949 (margin of safety), Damodaran 2002 (multi-method DCF),
  Greenblatt 2010 (EV/EBIT > P/E), GuruFocus internal methodology
  publication. Composite valuation outperforms single-method approaches
  in out-of-sample tests (Damodaran 2012, JF survey).

Schedule: daily 22:00 UTC (post-close).
"""
import json
import math
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

VERSION = "1.0.1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/gf-value.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = (os.environ.get("TELEGRAM_BOT_TOKEN", "") or
                  os.environ.get("TELEGRAM_TOKEN", ""))
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")

WACC_DEFAULT = 0.09
TERMINAL_MULTIPLE = 15.0
PROJECTION_YEARS = 10
GROWTH_CAP_MIN = 0.03
GROWTH_CAP_MAX = 0.15
COMP_WEIGHTS = {"dcf": 0.40, "evebit": 0.35, "graham": 0.25}


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
    return [s.get("symbol") for s in data if s.get("symbol")]


def get_ticker_data(symbol):
    """Fetch all 5 endpoints in parallel for a single ticker."""
    q = urllib.parse.quote_plus(symbol)
    endpoints = {
        "quote":    f"quote?symbol={q}",
        "income":   f"income-statement?symbol={q}&limit=10",
        "cashflow": f"cash-flow-statement?symbol={q}&limit=10",
        "balance":  f"balance-sheet-statement?symbol={q}&limit=10",
        "metrics":  f"key-metrics?symbol={q}&limit=10",
        "price52":  f"historical-price-eod/light?symbol={q}",
    }
    out = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(fmp, p): k for k, p in endpoints.items()}
        for f in as_completed(futures):
            try:
                out[futures[f]] = f.result()
            except Exception:
                out[futures[f]] = None
    return out


def safe_get(d, *keys, default=None):
    """Pull keys from possibly-nested dicts safely."""
    for k in keys:
        if not isinstance(d, dict):
            return default
        v = d.get(k)
        if v is not None and v != "":
            return v
    return default


def compute_dcf(income, cashflow, balance, quote, metrics):
    """DCF intrinsic value per share."""
    try:
        if not isinstance(cashflow, list) or not cashflow:
            return None, "no_cashflow"
        fcfs = []
        for cf in cashflow[:6]:
            fcf = (cf.get("freeCashFlow") or
                   (cf.get("operatingCashFlow") or 0) -
                   abs(cf.get("capitalExpenditure") or 0))
            if fcf is not None:
                fcfs.append(float(fcf))
        if not fcfs or fcfs[0] <= 0:
            return None, "negative_fcf"
        fcf_ttm = fcfs[0]
        # 5yr CAGR
        if len(fcfs) >= 5 and fcfs[4] > 0:
            cagr = (fcfs[0] / fcfs[4]) ** (1 / 4) - 1
        elif len(fcfs) >= 3 and fcfs[2] > 0:
            cagr = (fcfs[0] / fcfs[2]) ** (1 / 2) - 1
        else:
            cagr = 0.05
        growth = max(GROWTH_CAP_MIN, min(GROWTH_CAP_MAX, cagr))
        # Project 10yr FCF + terminal value
        pv_sum = 0
        for yr in range(1, PROJECTION_YEARS + 1):
            proj_fcf = fcf_ttm * (1 + growth) ** yr
            pv_sum += proj_fcf / (1 + WACC_DEFAULT) ** yr
        terminal_fcf = fcf_ttm * (1 + growth) ** PROJECTION_YEARS
        terminal_value = terminal_fcf * TERMINAL_MULTIPLE
        pv_terminal = terminal_value / (1 + WACC_DEFAULT) ** PROJECTION_YEARS
        enterprise_value = pv_sum + pv_terminal
        # Subtract net debt
        if isinstance(balance, list) and balance:
            bs = balance[0]
            net_debt = ((bs.get("totalDebt") or 0) -
                        (bs.get("cashAndShortTermInvestments") or
                         bs.get("cashAndCashEquivalents") or 0))
        else:
            net_debt = 0
        equity_value = enterprise_value - net_debt
        # Per share
        if isinstance(quote, list) and quote:
            shares = quote[0].get("sharesOutstanding") or 0
        else:
            shares = 0
        if shares <= 0 and isinstance(income, list) and income:
            shares = income[0].get("weightedAverageShsOutDil") or 0
        if shares <= 0:
            return None, "no_shares"
        dcf_per_share = equity_value / shares
        if dcf_per_share <= 0:
            return None, "negative_dcf"
        return round(dcf_per_share, 2), {
            "fcf_ttm": fcf_ttm, "growth_used": round(growth * 100, 2),
            "wacc": WACC_DEFAULT, "shares": shares,
            "net_debt": net_debt}
    except Exception as e:
        return None, f"dcf_err:{str(e)[:50]}"


def compute_evebit(income, balance, quote, metrics):
    """EV/EBIT multiple fair value per share.

    v1.0.1 fix: build historical EV/EBIT by joining key-metrics
    (enterpriseValue per period) with income-statement (operatingIncome
    per period) on year. Previous version looked for 'ebit' inside
    key-metrics which doesn't exist in FMP's schema -> failed for
    almost every ticker.
    """
    try:
        if not isinstance(income, list) or len(income) < 3:
            return None, "no_income"
        # TTM EBIT
        ebit_ttm = income[0].get("operatingIncome") or 0
        if ebit_ttm <= 0:
            return None, "negative_ebit"
        if not isinstance(metrics, list) or len(metrics) < 3:
            return None, "no_metrics"
        # Build year -> EBIT map from income statements
        ebit_by_year = {}
        for inc in income:
            d = (inc.get("date") or inc.get("fiscalYear")
                 or inc.get("calendarYear"))
            if not d:
                continue
            yr = str(d)[:4]
            eb = inc.get("operatingIncome") or 0
            if eb and eb > 0:
                ebit_by_year[yr] = float(eb)
        # Build historical EV/EBIT by matching metrics to income on year
        ev_ebits = []
        for m in metrics[:10]:
            d = (m.get("date") or m.get("fiscalYear")
                 or m.get("calendarYear"))
            if not d:
                continue
            yr = str(d)[:4]
            ev = m.get("enterpriseValue") or 0
            ebit_y = ebit_by_year.get(yr)
            if ev and ev > 0 and ebit_y and ebit_y > 0:
                ev_ebits.append(ev / ebit_y)
        if len(ev_ebits) < 3:
            return None, f"insufficient_evebit_history ({len(ev_ebits)})"
        # Drop outliers (>3 sigma)
        if len(ev_ebits) >= 4:
            mu = statistics.mean(ev_ebits)
            sd = statistics.stdev(ev_ebits)
            ev_ebits = [x for x in ev_ebits if abs(x - mu) <= 3 * sd]
        fair_multiple = statistics.median(ev_ebits)
        # Cap fair multiple at reasonable range (high quality cyclicals
        # max ~30x, deep value floor 8x)
        fair_multiple = max(8, min(30, fair_multiple))
        fair_ev = ebit_ttm * fair_multiple
        # Bridge to equity
        if isinstance(balance, list) and balance:
            bs = balance[0]
            cash = (bs.get("cashAndShortTermInvestments") or
                    bs.get("cashAndCashEquivalents") or 0)
            debt = bs.get("totalDebt") or 0
            minority = bs.get("minorityInterest") or 0
        else:
            cash, debt, minority = 0, 0, 0
        fair_equity = fair_ev - debt + cash - minority
        if isinstance(quote, list) and quote:
            shares = quote[0].get("sharesOutstanding") or 0
        else:
            shares = 0
        if shares <= 0 and income:
            shares = income[0].get("weightedAverageShsOutDil") or 0
        if shares <= 0:
            return None, "no_shares"
        evebit_per_share = fair_equity / shares
        if evebit_per_share <= 0:
            return None, "negative_evebit_value"
        return round(evebit_per_share, 2), {
            "ebit_ttm": ebit_ttm, "fair_multiple": round(fair_multiple, 1),
            "median_evebit_history": round(statistics.median(ev_ebits), 1),
            "n_history": len(ev_ebits)}
    except Exception as e:
        return None, f"evebit_err:{str(e)[:50]}"


def compute_graham(income, balance, quote):
    """Graham Number = sqrt(22.5 * EPS * BVPS). Requires both > 0."""
    try:
        if not isinstance(income, list) or not income:
            return None, "no_income"
        eps = income[0].get("eps") or income[0].get("epsDiluted") or 0
        if eps <= 0:
            return None, "negative_eps"
        if not isinstance(balance, list) or not balance:
            return None, "no_balance"
        bs = balance[0]
        equity = (bs.get("totalStockholdersEquity") or
                  bs.get("stockholdersEquity") or 0)
        if equity <= 0:
            return None, "negative_equity"
        if isinstance(quote, list) and quote:
            shares = quote[0].get("sharesOutstanding") or 0
        else:
            shares = 0
        if shares <= 0:
            shares = income[0].get("weightedAverageShsOutDil") or 0
        if shares <= 0:
            return None, "no_shares"
        bvps = equity / shares
        if bvps <= 0:
            return None, "negative_bvps"
        graham = math.sqrt(22.5 * eps * bvps)
        return round(graham, 2), {"eps": round(eps, 2),
                                  "bvps": round(bvps, 2)}
    except Exception as e:
        return None, f"graham_err:{str(e)[:50]}"


def composite_value(dcf, evebit, graham, price52_low, price52_high):
    """Weighted blend of valid valuation lenses, winsorized."""
    components = {}
    if isinstance(dcf, (int, float)) and dcf > 0:
        components["dcf"] = dcf
    if isinstance(evebit, (int, float)) and evebit > 0:
        components["evebit"] = evebit
    if isinstance(graham, (int, float)) and graham > 0:
        components["graham"] = graham
    if not components:
        return None, 0, []
    total_w = sum(COMP_WEIGHTS[k] for k in components)
    if total_w == 0:
        return None, 0, []
    weighted = sum(components[k] * COMP_WEIGHTS[k] for k in components) / total_w
    # Winsorize by 5yr range
    if price52_low and price52_high and price52_low > 0:
        weighted = max(price52_low * 0.7, min(price52_high * 1.5, weighted))
    return round(weighted, 2), len(components), list(components.keys())


def rating(mos_pct):
    if mos_pct is None:
        return "NO_VALUE"
    if mos_pct >= 50:
        return "DEEP_VALUE"
    if mos_pct >= 20:
        return "MODESTLY_UNDERVALUED"
    if mos_pct >= -10:
        return "FAIR"
    if mos_pct >= -30:
        return "MODESTLY_OVERVALUED"
    return "SIGNIFICANTLY_OVERVALUED"


def get_price52(price52):
    """Extract 5yr low/high from historical price data."""
    if not isinstance(price52, list) or not price52:
        return None, None
    closes = []
    for r in price52:
        c = r.get("close") or r.get("price")
        if c is not None:
            try:
                closes.append(float(c))
            except Exception:
                pass
    if not closes:
        return None, None
    return min(closes), max(closes)


def analyze_ticker(symbol):
    """Full GF Value analysis for one ticker."""
    try:
        d = get_ticker_data(symbol)
        quote = d.get("quote")
        if not isinstance(quote, list) or not quote:
            return None
        price = quote[0].get("price") or quote[0].get("priceClose")
        if not price or price <= 0:
            return None

        dcf_val, dcf_meta = compute_dcf(
            d.get("income"), d.get("cashflow"), d.get("balance"),
            quote, d.get("metrics"))
        evebit_val, evebit_meta = compute_evebit(
            d.get("income"), d.get("balance"), quote, d.get("metrics"))
        graham_val, graham_meta = compute_graham(
            d.get("income"), d.get("balance"), quote)

        p52_low, p52_high = get_price52(d.get("price52"))

        gf_value, n_lenses, lenses_used = composite_value(
            dcf_val, evebit_val, graham_val, p52_low, p52_high)
        if not gf_value:
            return None
        # v1.0.1 sanity: reject extreme ratios (FMP split-adjustment
        # mismatches produce absurd values like LITE $868 vs GFV $25).
        if gf_value > 0 and price > 0:
            ratio = max(price, gf_value) / min(price, gf_value)
            if ratio > 20:
                return None
        mos_pct = round((gf_value - price) / gf_value * 100, 1)
        # v1.0.1 bounds: clip MoS to [-95%, +95%] for display sanity
        mos_pct = max(-95.0, min(95.0, mos_pct))
        rate = rating(mos_pct)

        return {
            "ticker": symbol,
            "price": round(float(price), 2),
            "gf_value": gf_value,
            "margin_of_safety_pct": mos_pct,
            "rating": rate,
            "n_lenses": n_lenses,
            "lenses_used": lenses_used,
            "dcf_fair_value": dcf_val,
            "evebit_fair_value": evebit_val,
            "graham_number": graham_val,
            "price_5yr_low": round(p52_low, 2) if p52_low else None,
            "price_5yr_high": round(p52_high, 2) if p52_high else None,
            "dcf_meta": dcf_meta if isinstance(dcf_meta, dict) else None,
            "evebit_meta": evebit_meta if isinstance(evebit_meta, dict) else None,
            "graham_meta": graham_meta if isinstance(graham_meta, dict) else None,
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
        universe = get_sp500()
        if not universe:
            payload = {"version": VERSION, "generated_at": started,
                       "state": "DATA_UNAVAILABLE", "error": "no_sp500"}
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(payload, indent=2).encode(),
                          ContentType="application/json")
            return {"statusCode": 200,
                    "body": json.dumps({"ok": False, "error": "no_sp500"})}

        results = []
        # Parallel ticker analysis (8 workers - balance FMP rate vs throughput)
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(analyze_ticker, sym): sym for sym in universe}
            for f in as_completed(futures):
                r = f.result()
                if r:
                    results.append(r)

        if not results:
            return {"statusCode": 200,
                    "body": json.dumps({"ok": False,
                                        "error": "no_valuations"})}

        # Sort/segment
        valid = [r for r in results if r.get("gf_value")]
        deepest_value = sorted(
            valid, key=lambda x: -x["margin_of_safety_pct"])[:25]
        most_overvalued = sorted(
            valid, key=lambda x: x["margin_of_safety_pct"])[:25]

        # Stats
        mos_list = [r["margin_of_safety_pct"] for r in valid]
        median_mos = statistics.median(mos_list) if mos_list else None
        n_deep_value = sum(1 for r in valid if r["rating"] == "DEEP_VALUE")
        n_undervalued = sum(1 for r in valid
                            if r["rating"] == "MODESTLY_UNDERVALUED")
        n_fair = sum(1 for r in valid if r["rating"] == "FAIR")
        n_modest_over = sum(1 for r in valid
                            if r["rating"] == "MODESTLY_OVERVALUED")
        n_sig_over = sum(1 for r in valid
                         if r["rating"] == "SIGNIFICANTLY_OVERVALUED")

        # Universe-level state
        if median_mos is not None and median_mos >= 15:
            universe_state = "MARKET_DEEP_VALUE"
        elif median_mos is not None and median_mos >= 5:
            universe_state = "MARKET_UNDERVALUED"
        elif median_mos is not None and median_mos >= -5:
            universe_state = "MARKET_FAIR"
        elif median_mos is not None and median_mos >= -20:
            universe_state = "MARKET_MODESTLY_OVERVALUED"
        else:
            universe_state = "MARKET_SIGNIFICANTLY_OVERVALUED"

        payload = {
            "version": VERSION,
            "generated_at": started,
            "universe": "S&P 500",
            "universe_state": universe_state,
            "universe_median_mos_pct": round(median_mos, 1) if median_mos else None,
            "n_analyzed": len(universe),
            "n_valid": len(valid),
            "n_deep_value": n_deep_value,
            "n_undervalued": n_undervalued,
            "n_fair": n_fair,
            "n_modestly_overvalued": n_modest_over,
            "n_significantly_overvalued": n_sig_over,
            "deepest_value": deepest_value,
            "most_overvalued": most_overvalued,
            "all_tickers": sorted(valid,
                                  key=lambda x: -x["margin_of_safety_pct"]),
            "methodology": {
                "wacc": WACC_DEFAULT,
                "terminal_multiple": TERMINAL_MULTIPLE,
                "projection_years": PROJECTION_YEARS,
                "growth_cap_min": GROWTH_CAP_MIN,
                "growth_cap_max": GROWTH_CAP_MAX,
                "composite_weights": COMP_WEIGHTS,
                "lenses": ["DCF (10yr FCF + 15x terminal at 9% WACC)",
                           "EV/EBIT (10yr median multiple, outlier-pruned)",
                           "Graham Number (sqrt(22.5 * EPS * BVPS))"],
                "rating_bands": {
                    "DEEP_VALUE": ">= +50%",
                    "MODESTLY_UNDERVALUED": "+20% to +50%",
                    "FAIR": "-10% to +20%",
                    "MODESTLY_OVERVALUED": "-30% to -10%",
                    "SIGNIFICANTLY_OVERVALUED": "<= -30%",
                },
            },
            "edge_basis": ("Graham 1949 (margin of safety), Damodaran 2002 "
                           "(multi-method DCF), Greenblatt 2010 (EV/EBIT > "
                           "P/E for cyclicals), GuruFocus internal "
                           "methodology. Composite > single-method "
                           "(Damodaran 2012 JF survey)."),
            "sources": ["FMP /stable/quote", "FMP /stable/income-statement",
                        "FMP /stable/cash-flow-statement",
                        "FMP /stable/balance-sheet-statement",
                        "FMP /stable/key-metrics",
                        "FMP /stable/sp500-constituent"],
        }
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                      Body=json.dumps(payload, indent=2,
                                      default=str).encode(),
                      ContentType="application/json",
                      CacheControl="max-age=900")

        # Telegram alert if 5+ DEEP_VALUE picks
        if n_deep_value >= 5:
            top5 = deepest_value[:5]
            lines = "\n".join(
                f"- {x['ticker']} MoS {x['margin_of_safety_pct']:+.0f}% "
                f"(price ${x['price']:.2f} vs GF ${x['gf_value']:.2f})"
                for x in top5)
            telegram_alert(f"*GF Value: {n_deep_value} DEEP_VALUE picks*\n"
                           f"Top 5:\n{lines}\nMarket median MoS: "
                           f"{round(median_mos,1) if median_mos else 'n/a'}%")

        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "n_valid": len(valid),
            "universe_state": universe_state,
            "n_deep_value": n_deep_value,
            "median_mos_pct": round(median_mos, 1) if median_mos else None})}

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
