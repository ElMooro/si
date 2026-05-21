"""
justhodl-eva-spread -- Pro Pack v3 #10 - Economic Value Added (EVA) Spread

═══════════════════════════════════════════════════════════════════════════════
INSTITUTIONAL ENGINE
────────────────────
Stern Stewart's flagship metric, the way hedge funds use it (Bennett Stewart
2002 enhancements). Identifies true economic value creators vs. accounting-
profitable but economically-destructive companies.

Per Bennett Stewart's research, EVA correlates with stock returns ~50% better
than EPS. This is the single most important compounder identifier in the
institutional toolkit.

CORE FORMULA
────────────
   EVA            = NOPAT - (WACC * Invested Capital)
   EVA Spread     = ROIC - WACC                          (cross-sectional rank)
   EVA Margin     = EVA / Revenue                        (scale-normalized)
   EVA Momentum   = (EVA_t - EVA_t-1) / IC_t-1           (Bennett Stewart preferred)
   MVA            = MarketCap - InvestedCapital          (cumulative market verdict)

WACC COMPUTATION (CAPM + IG spread)
────────────────────────────────────
   Ke (cost of equity)  = Rf + Beta * ERP
                          Rf  = FRED DGS10 (US 10Y Treasury)
                          ERP = 5.50% (Damodaran 2026 implied ERP)
   Kd (cost of debt)    = Rf + IG_credit_spread
                          IG  = FRED BAMLC0A4CBBB (BBB IG OAS)
   E/V, D/V             = MarketCap / (MarketCap + Total Debt)
   T (tax shield)       = effectiveTaxRateTTM (FMP)
   WACC                 = E/V * Ke + D/V * Kd * (1 - T)

ROIC SOURCE
───────────
   Use FMP /stable/key-metrics-ttm returnOnInvestedCapitalTTM (validated in #8)

UNIVERSE
────────
   STATIC_TOP50_SPX (consistency with #4 StarMine / #7 Predictability / #8 Smart Beta).
   Deterministic, FMP-quota-friendly, allows cross-engine compounder fusion.

REGIME CLASSIFICATION
─────────────────────
   CREATING_VALUE_BROAD     : >=70% of universe has EVA Spread > 0
   MIXED                    : 40-70% positive EVA Spread
   DESTROYING_VALUE_BROAD   : <40% positive EVA Spread

SUPER COMPOUNDER FILTER (cross-engine — flagged in output for fusion w/ #7 #8)
────────────────────────────────────────────────────────────────────────────────
   EVA Spread > 5%  AND  ROIC > 20%  AND  EVA Momentum > 0  =  flag

OUTPUT
──────
   s3://justhodl-dashboard-live/data/eva-spread.json
   Schedule: daily 00:45 UTC (after Smart Beta 00:15)

═══════════════════════════════════════════════════════════════════════════════
"""
import json
import os
import statistics
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"

FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_KEY = os.environ.get("FMP_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
FRED_KEY = os.environ.get("FRED_KEY", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/eva-spread.json"

FMP_SLEEP_SEC = 0.4
HTTP_TIMEOUT = 25
HISTORY_YEARS = 3   # for EVA Momentum
ERP_PCT = 5.50      # Damodaran 2026 implied equity risk premium
DEFAULT_IG_SPREAD_PCT = 1.50  # fallback if FRED BAMLC0A4CBBB call fails

# Same universe as starmine + predictability + smart-beta for cross-engine fusion.
STATIC_TOP50_SPX = [
    ("AAPL", "Technology"), ("MSFT", "Technology"), ("NVDA", "Technology"),
    ("GOOGL", "Communication Services"), ("GOOG", "Communication Services"),
    ("AMZN", "Consumer Cyclical"), ("META", "Communication Services"),
    ("BRK-B", "Financial Services"), ("LLY", "Healthcare"),
    ("AVGO", "Technology"), ("TSLA", "Consumer Cyclical"),
    ("JPM", "Financial Services"), ("WMT", "Consumer Defensive"),
    ("V", "Financial Services"), ("UNH", "Healthcare"),
    ("XOM", "Energy"), ("MA", "Financial Services"),
    ("ORCL", "Technology"), ("COST", "Consumer Defensive"),
    ("PG", "Consumer Defensive"), ("JNJ", "Healthcare"),
    ("HD", "Consumer Cyclical"), ("NFLX", "Communication Services"),
    ("BAC", "Financial Services"), ("CVX", "Energy"),
    ("ABBV", "Healthcare"), ("CRM", "Technology"),
    ("KO", "Consumer Defensive"), ("AMD", "Technology"),
    ("WFC", "Financial Services"), ("MRK", "Healthcare"),
    ("CSCO", "Technology"), ("ADBE", "Technology"),
    ("PEP", "Consumer Defensive"), ("LIN", "Basic Materials"),
    ("TMO", "Healthcare"), ("ACN", "Technology"),
    ("MCD", "Consumer Cyclical"), ("ABT", "Healthcare"),
    ("CMCSA", "Communication Services"), ("INTU", "Technology"),
    ("IBM", "Technology"), ("DHR", "Healthcare"),
    ("TXN", "Technology"), ("PM", "Consumer Defensive"),
    ("DIS", "Communication Services"), ("CAT", "Industrials"),
    ("VZ", "Communication Services"), ("PFE", "Healthcare"),
    ("QCOM", "Technology"),
]


# ---------- HTTP ----------
def http_json(url, retries=4):
    backoffs = [5, 15, 30, 60]
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "JustHodl-EVA/1.0"})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504) and i < retries:
                time.sleep(backoffs[min(i, len(backoffs) - 1)])
                continue
            print(f"http_json err {e.code}: {url[:90]}")
            return None
        except Exception as e:
            if i < retries:
                time.sleep(backoffs[min(i, len(backoffs) - 1)])
                continue
            print(f"http_json fail: {e}")
            return None
    return None


# ---------- FRED ----------
def fred_latest(series_id):
    """Most recent observation for a FRED series."""
    if not FRED_KEY:
        return None
    url = (f"{FRED_BASE}?series_id={series_id}&api_key={FRED_KEY}"
           f"&file_type=json&sort_order=desc&limit=10")
    d = http_json(url)
    if not d:
        return None
    for obs in d.get("observations", []):
        if obs.get("value") not in (".", None, ""):
            try:
                return float(obs["value"])
            except (ValueError, TypeError):
                continue
    return None


# ---------- FMP ----------
def fmp_quote(symbol):
    url = f"{FMP_BASE}/quote?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    return d[0] if isinstance(d, list) and d else {}


def fmp_profile(symbol):
    url = f"{FMP_BASE}/profile?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    return d[0] if isinstance(d, list) and d else {}


def fmp_key_metrics_ttm(symbol):
    url = f"{FMP_BASE}/key-metrics-ttm?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    return d[0] if isinstance(d, list) and d else {}


def fmp_ratios_ttm(symbol):
    url = f"{FMP_BASE}/ratios-ttm?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    return d[0] if isinstance(d, list) and d else {}


def fmp_income_statement(symbol, years=HISTORY_YEARS):
    url = (f"{FMP_BASE}/income-statement?symbol={symbol}"
           f"&period=annual&limit={years}&apikey={FMP_KEY}")
    d = http_json(url)
    return d if isinstance(d, list) else []


def fmp_key_metrics_annual(symbol, years=HISTORY_YEARS):
    url = (f"{FMP_BASE}/key-metrics?symbol={symbol}"
           f"&period=annual&limit={years}&apikey={FMP_KEY}")
    d = http_json(url)
    return d if isinstance(d, list) else []


# ---------- Stats ----------
def safe_float(x):
    try:
        if x is None or x == "":
            return None
        return float(x)
    except (ValueError, TypeError):
        return None


def percentile_rank(values, sample):
    """Percentile of sample within values (0..100)."""
    if sample is None or not values:
        return None
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    n_below = sum(1 for v in cleaned if v < sample)
    n_equal = sum(1 for v in cleaned if v == sample)
    return round(100.0 * (n_below + 0.5 * n_equal) / len(cleaned), 1)


# ---------- WACC ----------
def compute_wacc(market_cap, total_debt, beta, eff_tax,
                  rf_pct, ig_spread_pct, erp_pct=ERP_PCT):
    """Returns (wacc_pct, components_dict) or (None, components)."""
    components = {
        "risk_free_pct": rf_pct,
        "erp_pct": erp_pct,
        "ig_spread_pct": ig_spread_pct,
        "beta": beta,
        "effective_tax_rate": eff_tax,
        "market_cap_usd": market_cap,
        "total_debt_usd": total_debt,
    }
    if not market_cap or market_cap <= 0:
        return None, components
    if beta is None:
        beta = 1.0  # market beta default
    if eff_tax is None or eff_tax < 0 or eff_tax > 1:
        eff_tax = 0.21  # statutory US corporate
    # Cost of equity (CAPM)
    ke = rf_pct + beta * erp_pct
    # Cost of debt (Rf + IG spread)
    kd = rf_pct + ig_spread_pct
    # Weights
    if total_debt is None or total_debt < 0:
        total_debt = 0.0
    v = market_cap + total_debt
    we = market_cap / v
    wd = total_debt / v
    wacc = we * ke + wd * kd * (1 - eff_tax)
    components.update({
        "cost_of_equity_pct": round(ke, 3),
        "cost_of_debt_pct": round(kd, 3),
        "weight_equity": round(we, 4),
        "weight_debt": round(wd, 4),
    })
    return round(wacc, 3), components


# ---------- EVA ----------
def compute_eva_history(symbol, wacc_pct):
    """Pull 3y annual income + key metrics, compute EVA per year, return trend."""
    inc = fmp_income_statement(symbol)
    time.sleep(FMP_SLEEP_SEC)
    km_hist = fmp_key_metrics_annual(symbol)
    time.sleep(FMP_SLEEP_SEC)
    if not inc or not km_hist:
        return None
    # Index both by year
    inc_by_year = {}
    for row in inc:
        y = (row.get("date") or "")[:4]
        if y:
            inc_by_year[y] = row
    km_by_year = {}
    for row in km_hist:
        y = (row.get("date") or "")[:4]
        if y:
            km_by_year[y] = row
    years = sorted(set(inc_by_year) & set(km_by_year), reverse=False)
    if len(years) < 2:
        return None
    series = []
    for y in years:
        r_inc = inc_by_year[y]
        r_km = km_by_year[y]
        op_income = safe_float(r_inc.get("operatingIncome"))
        tax_rate = safe_float(r_inc.get("effectiveTaxRate")) or 0.21
        ic = safe_float(r_km.get("investedCapital"))
        revenue = safe_float(r_inc.get("revenue"))
        if op_income is None or ic is None or ic <= 0:
            continue
        nopat = op_income * (1 - tax_rate)
        eva = nopat - (wacc_pct / 100.0) * ic
        series.append({
            "year": y,
            "revenue": revenue,
            "nopat": round(nopat, 0),
            "invested_capital": round(ic, 0),
            "eva": round(eva, 0),
        })
    return series if len(series) >= 2 else None


def compute_eva_momentum(series):
    """EVA Momentum = (EVA_t - EVA_t-1) / IC_t-1, expressed as percent."""
    if not series or len(series) < 2:
        return None
    latest = series[-1]
    prior = series[-2]
    if not prior.get("invested_capital") or prior["invested_capital"] <= 0:
        return None
    delta = latest["eva"] - prior["eva"]
    return round(100.0 * delta / prior["invested_capital"], 2)


# ---------- Per-ticker analysis ----------
def analyze_ticker(symbol, sector, rf_pct, ig_spread_pct):
    q = fmp_quote(symbol)
    time.sleep(FMP_SLEEP_SEC)
    prof = fmp_profile(symbol)
    time.sleep(FMP_SLEEP_SEC)
    km = fmp_key_metrics_ttm(symbol)
    time.sleep(FMP_SLEEP_SEC)
    ratios = fmp_ratios_ttm(symbol)
    time.sleep(FMP_SLEEP_SEC)

    market_cap = safe_float(q.get("marketCap"))
    price = safe_float(q.get("price"))
    beta = safe_float(prof.get("beta"))
    eff_tax = safe_float(ratios.get("effectiveTaxRateTTM"))
    roic_ttm = safe_float(km.get("returnOnInvestedCapitalTTM"))  # decimal
    invested_capital = safe_float(km.get("investedCapitalTTM"))
    total_debt = safe_float(km.get("netDebtToEBITDATTM"))  # placeholder check
    # Better: get total debt from key-metrics-ttm enterpriseValue - market_cap + cash
    ev = safe_float(km.get("enterpriseValueTTM"))
    if ev is not None and market_cap is not None:
        total_debt = max(0.0, ev - market_cap)
    revenue_per_share = safe_float(ratios.get("revenuePerShareTTM"))

    if (roic_ttm is None or invested_capital is None or
            invested_capital <= 0 or market_cap is None):
        return {"ticker": symbol, "sector": sector, "ok": False,
                "reason": "missing core fields (roic/ic/mcap)"}

    wacc_pct, wacc_comp = compute_wacc(
        market_cap, total_debt, beta, eff_tax, rf_pct, ig_spread_pct)
    if wacc_pct is None:
        return {"ticker": symbol, "sector": sector, "ok": False,
                "reason": "wacc calc failed"}

    roic_pct = roic_ttm * 100.0
    eva_spread_pct = round(roic_pct - wacc_pct, 3)
    eva_usd = round((eva_spread_pct / 100.0) * invested_capital, 0)
    mva = round(market_cap - invested_capital, 0)

    # EVA history & momentum
    eva_series = compute_eva_history(symbol, wacc_pct)
    eva_momentum_pct = compute_eva_momentum(eva_series) if eva_series else None

    # EVA Margin
    latest_revenue = (eva_series[-1].get("revenue")
                      if eva_series else None)
    eva_margin_pct = None
    if latest_revenue and latest_revenue > 0:
        eva_margin_pct = round(100.0 * eva_usd / latest_revenue, 2)

    return {
        "ticker": symbol,
        "sector": sector,
        "ok": True,
        "price_usd": price,
        "market_cap_usd": market_cap,
        "invested_capital_usd": invested_capital,
        "total_debt_usd": total_debt,
        "roic_ttm_pct": round(roic_pct, 3),
        "wacc_pct": wacc_pct,
        "eva_spread_pct": eva_spread_pct,
        "eva_usd": eva_usd,
        "eva_margin_pct": eva_margin_pct,
        "eva_momentum_pct": eva_momentum_pct,
        "mva_usd": mva,
        "mva_multiple": (round(market_cap / invested_capital, 2)
                          if invested_capital else None),
        "wacc_components": wacc_comp,
        "eva_history": eva_series,
        "value_creator": eva_spread_pct > 0,
        "super_compounder": (eva_spread_pct > 5 and roic_pct > 20 and
                             (eva_momentum_pct or 0) > 0),
    }


# ---------- Aggregation ----------
def classify_universe(results):
    valid = [r for r in results if r.get("ok")]
    if not valid:
        return "NO_DATA"
    n_pos = sum(1 for r in valid if r.get("value_creator"))
    pct = n_pos / len(valid)
    if pct >= 0.70:
        return "CREATING_VALUE_BROAD"
    if pct >= 0.40:
        return "MIXED"
    return "DESTROYING_VALUE_BROAD"


def sector_breakdown(results):
    by_sec = {}
    for r in results:
        if not r.get("ok"):
            continue
        s = r.get("sector") or "Unknown"
        d = by_sec.setdefault(s, {"n": 0, "n_creators": 0,
                                   "eva_spreads": [], "roics": [],
                                   "waccs": []})
        d["n"] += 1
        if r.get("value_creator"):
            d["n_creators"] += 1
        d["eva_spreads"].append(r["eva_spread_pct"])
        d["roics"].append(r["roic_ttm_pct"])
        d["waccs"].append(r["wacc_pct"])
    out = {}
    for s, d in by_sec.items():
        out[s] = {
            "n": d["n"],
            "n_creators": d["n_creators"],
            "pct_creators": round(100.0 * d["n_creators"] / d["n"], 1),
            "median_eva_spread_pct": round(statistics.median(
                d["eva_spreads"]), 2),
            "median_roic_pct": round(statistics.median(d["roics"]), 2),
            "median_wacc_pct": round(statistics.median(d["waccs"]), 2),
        }
    return out


def lambda_handler(event, context):
    started = time.time()
    if not FMP_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False,
                                     "error": "FMP_KEY not set"})}

    # Pull macro inputs (Rf + IG spread)
    rf_pct = fred_latest("DGS10")
    ig_oas_pct = fred_latest("BAMLC0A4CBBB")
    if rf_pct is None:
        rf_pct = 4.41   # last-known fallback
    if ig_oas_pct is None:
        ig_oas_pct = DEFAULT_IG_SPREAD_PCT

    results = []
    for sym, sector in STATIC_TOP50_SPX:
        try:
            r = analyze_ticker(sym, sector, rf_pct, ig_oas_pct)
        except Exception as e:
            r = {"ticker": sym, "sector": sector, "ok": False,
                  "reason": f"exception: {str(e)[:120]}"}
        results.append(r)
        # progress log every 10
        if len(results) % 10 == 0:
            print(f"  progress: {len(results)}/{len(STATIC_TOP50_SPX)}"
                   f" — last={sym}")

    valid = [r for r in results if r.get("ok")]
    n_valid = len(valid)

    # Universe state
    regime = classify_universe(results)
    n_creators = sum(1 for r in valid if r.get("value_creator"))
    n_destroyers = n_valid - n_creators
    n_super = sum(1 for r in valid if r.get("super_compounder"))

    # Add cross-sectional percentile ranks
    all_spreads = [r["eva_spread_pct"] for r in valid]
    all_momentum = [r["eva_momentum_pct"] for r in valid
                    if r.get("eva_momentum_pct") is not None]
    all_mva = [r["mva_usd"] for r in valid]
    for r in valid:
        r["eva_spread_pct_pctile"] = percentile_rank(
            all_spreads, r["eva_spread_pct"])
        if r.get("eva_momentum_pct") is not None:
            r["eva_momentum_pctile"] = percentile_rank(
                all_momentum, r["eva_momentum_pct"])
        r["mva_pctile"] = percentile_rank(all_mva, r["mva_usd"])

    # Sort + tops
    top_spread = sorted(valid, key=lambda x: x["eva_spread_pct"],
                         reverse=True)[:10]
    top_momentum = sorted(
        [r for r in valid if r.get("eva_momentum_pct") is not None],
        key=lambda x: x["eva_momentum_pct"], reverse=True)[:10]
    top_mva = sorted(valid, key=lambda x: x["mva_usd"],
                      reverse=True)[:10]
    bottom_spread = sorted(valid, key=lambda x: x["eva_spread_pct"])[:5]
    super_compounders = [r for r in valid if r.get("super_compounder")]

    output = {
        "engine": "eva-spread",
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe": "STATIC_TOP50_SPX (cross-engine fusion with #4/#7/#8)",
        "universe_state": regime,
        "n_analyzed": len(results),
        "n_valid": n_valid,
        "n_value_creators": n_creators,
        "n_value_destroyers": n_destroyers,
        "n_super_compounders": n_super,
        "wacc_inputs": {
            "risk_free_10y_pct": rf_pct,
            "equity_risk_premium_pct": ERP_PCT,
            "ig_credit_spread_pct": ig_oas_pct,
            "source_rf": "FRED DGS10",
            "source_ig_spread": "FRED BAMLC0A4CBBB",
            "source_erp": "Damodaran 2026 implied ERP (hardcoded)",
        },
        "universe_medians": {
            "eva_spread_pct": round(statistics.median(all_spreads), 2)
                if all_spreads else None,
            "roic_pct": round(statistics.median(
                [r["roic_ttm_pct"] for r in valid]), 2) if valid else None,
            "wacc_pct": round(statistics.median(
                [r["wacc_pct"] for r in valid]), 2) if valid else None,
            "eva_momentum_pct": round(statistics.median(all_momentum), 2)
                if all_momentum else None,
        },
        "top_10_eva_spread": [
            {k: r[k] for k in
             ("ticker", "sector", "roic_ttm_pct", "wacc_pct",
              "eva_spread_pct", "eva_spread_pct_pctile", "eva_usd",
              "eva_margin_pct", "eva_momentum_pct", "mva_usd",
              "super_compounder")} for r in top_spread],
        "top_10_eva_momentum": [
            {k: r[k] for k in
             ("ticker", "sector", "eva_momentum_pct",
              "eva_spread_pct", "roic_ttm_pct", "wacc_pct",
              "eva_usd", "super_compounder")} for r in top_momentum],
        "top_10_mva": [
            {k: r[k] for k in
             ("ticker", "sector", "mva_usd", "market_cap_usd",
              "invested_capital_usd", "mva_multiple",
              "eva_spread_pct")} for r in top_mva],
        "bottom_5_destroyers": [
            {k: r[k] for k in
             ("ticker", "sector", "eva_spread_pct", "roic_ttm_pct",
              "wacc_pct", "eva_usd", "eva_momentum_pct")}
            for r in bottom_spread],
        "super_compounders": [
            {k: r[k] for k in
             ("ticker", "sector", "eva_spread_pct", "roic_ttm_pct",
              "wacc_pct", "eva_momentum_pct", "eva_margin_pct",
              "mva_multiple")} for r in super_compounders],
        "sector_breakdown": sector_breakdown(results),
        "all_tickers": [
            {"ticker": r["ticker"], "ok": r.get("ok"),
             "sector": r.get("sector"),
             "eva_spread_pct": r.get("eva_spread_pct"),
             "roic_ttm_pct": r.get("roic_ttm_pct"),
             "wacc_pct": r.get("wacc_pct"),
             "eva_momentum_pct": r.get("eva_momentum_pct"),
             "mva_usd": r.get("mva_usd"),
             "value_creator": r.get("value_creator"),
             "super_compounder": r.get("super_compounder"),
             "reason": r.get("reason")} for r in results],
        "methodology": {
            "framework": "Stern Stewart EVA + Bennett Stewart (2002) EVA Momentum",
            "formula": "EVA = NOPAT - (WACC * Invested Capital)",
            "spread": "EVA Spread = ROIC - WACC (cross-sectional rank)",
            "momentum": "EVA Momentum = (EVA_t - EVA_t-1) / IC_t-1 (per BS 2002)",
            "mva": "MVA = MarketCap - InvestedCapital (cumulative market verdict)",
            "wacc": (
                "Ke = Rf + Beta*ERP (CAPM); "
                "Kd = Rf + IG_spread; "
                "WACC = E/V*Ke + D/V*Kd*(1-T). "
                f"Rf from FRED DGS10={rf_pct}, ERP={ERP_PCT}% (Damodaran), "
                f"IG spread from FRED BAMLC0A4CBBB={ig_oas_pct}."
            ),
            "super_compounder_filter": (
                "EVA Spread > 5pp AND ROIC > 20% AND EVA Momentum > 0. "
                "Fuse with #7 Predictability 5-star + #8 Smart Beta quality "
                "leader for the institutional compound-and-hold screen."
            ),
            "regime_thresholds": {
                "CREATING_VALUE_BROAD": ">=70% of universe EVA Spread > 0",
                "MIXED": "40-70% positive EVA Spread",
                "DESTROYING_VALUE_BROAD": "<40% positive EVA Spread",
            },
        },
        "data_sources": {
            "fmp_quote": "/stable/quote (market cap, price)",
            "fmp_profile": "/stable/profile (beta)",
            "fmp_key_metrics_ttm": "/stable/key-metrics-ttm (ROIC, IC, EV)",
            "fmp_ratios_ttm": "/stable/ratios-ttm (effective tax)",
            "fmp_income_statement": "/stable/income-statement (3y, EVA series)",
            "fmp_key_metrics_annual": "/stable/key-metrics (3y, IC series)",
            "fred_dgs10": "10Y Treasury (risk-free)",
            "fred_bamlc0a4cbbb": "BBB IG OAS (cost of debt spread)",
        },
        "duration_seconds": round(time.time() - started, 1),
    }

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_KEY,
        Body=json.dumps(output, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=900")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "version": VERSION,
            "universe_state": regime,
            "n_valid": n_valid,
            "n_value_creators": n_creators,
            "n_value_destroyers": n_destroyers,
            "n_super_compounders": n_super,
            "wacc_inputs": output["wacc_inputs"],
            "universe_medians": output["universe_medians"],
            "top_3_eva_spread": [
                {"t": r["ticker"], "spread": r["eva_spread_pct"],
                 "roic": r["roic_ttm_pct"], "wacc": r["wacc_pct"]}
                for r in top_spread[:3]],
            "super_compounders": [r["ticker"]
                                   for r in super_compounders],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2))
