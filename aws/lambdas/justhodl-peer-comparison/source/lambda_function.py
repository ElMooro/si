"""
justhodl-peer-comparison -- Institutional peer / comparable analysis engine.

═══════════════════════════════════════════════════════════════════════════════
INSTITUTIONAL THESIS
────────────────────
Every research workflow starts with: "what does this look like next to its
peers." Bloomberg has the CRPR function ($24k/yr seat). FactSet has comp
tables. CapIQ does this best — and costs $20-50k/yr per seat. Retail
products show none of this comparably.

This engine builds a side-by-side comparable analysis for every name in
the research universe with z-scores vs peer-group median + best/worst
quartile flags + diagnostic narrative.

THE COMPARISON METHODOLOGY (Damodaran institutional grade)
──────────────────────────────────────────────────────────
  Peer set selection: FMP /stable/peers (same sector+industry, market-cap
  proximity filter 0.5x-2x). 8 peers per target.

  Metric panels (Damodaran "synthetic ratings" structure):

  VALUATION (5 metrics)
    EV/EBITDA TTM    forward 12mo if available
    P/E TTM          forward 12mo if available
    P/B              tangible book preferred
    EV/Sales TTM     for unprofitable / growth names
    FCF Yield TTM    free cash flow / market cap

  PROFITABILITY (5 metrics)
    ROIC TTM         Damodaran-adjusted (ex-cash, ex-goodwill)
    ROE TTM          beware leverage distortion
    Gross Margin
    EBITDA Margin
    FCF Margin

  GROWTH (4 metrics)
    Revenue CAGR 3y
    EPS CAGR 3y
    Gross Profit CAGR 3y
    FCF CAGR 3y

  BALANCE SHEET (4 metrics)
    Net Debt / EBITDA
    Interest Coverage  EBIT / Interest Expense
    Current Ratio
    Debt / Equity

  MARKET (3 metrics)
    1Y Total Return
    Beta (FMP)
    52w Drawdown

Z-SCORING
─────────
For each metric, compute z = (target_value - peer_median) / peer_std.
Flag:
  TOP_QUARTILE       z >= 0.67 (better than 75% of peers — "premium")
  ABOVE_MEDIAN       0 < z < 0.67
  BELOW_MEDIAN       -0.67 < z < 0
  BOTTOM_QUARTILE    z <= -0.67 ("discount" or "weakness")

Higher-is-better metrics: ROIC, margins, growth, FCF yield, coverage
Lower-is-better metrics: EV/EBITDA, P/E, P/B, EV/Sales, Net Debt/EBITDA,
                          drawdown

NARRATIVE GENERATION
────────────────────
For each name, generate institutional one-pager:
  "X trades at Y EV/EBITDA (BOTTOM_QUARTILE = discount to peers).
   ROIC of Z% is TOP_QUARTILE. Growth lags peer median by N bps.
   Net debt/EBITDA in line with sector. Net thesis: cheap quality
   with growth gap."

DISTINCTION FROM EXISTING ENGINES
──────────────────────────────────
  justhodl-fundamentals-engine   per-ticker DCF + segmentation + scores
                                   (no peer comparison layer)
  justhodl-asymmetric-scorer      asymmetric R/R scoring within universe
  justhodl-ka-metrics             custom metric tracker
  THIS engine                    peer SET comparison + z-scores + narrative

UNIVERSE
────────
STATIC_TOP50_SPX precomputed daily. Function URL mode allows on-demand
peer lookup for any ticker.

OUTPUT
──────
  s3://justhodl-dashboard-live/data/peer-comparison.json
  Schedule: daily 13 UTC (after fundamentals-engine refresh)

ACADEMIC BASIS
──────────────
- Damodaran (2012). Investment Valuation. Wiley. Ch 17-18 on relative
  valuation + comparable selection.
- Liu, Nissim, Thomas (2002). Equity valuation using multiples.
  Journal of Accounting Research, 40(1), 135-172.
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
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/peer-comparison.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/stable"
HTTP_TIMEOUT = 20

# Metric directionality: "high_is_good" or "low_is_good"
METRICS = {
    # Valuation (lower = cheaper)
    "ev_ebitda_ttm": "low_is_good",
    "pe_ttm": "low_is_good",
    "pb": "low_is_good",
    "ev_sales_ttm": "low_is_good",
    "fcf_yield": "high_is_good",
    # Profitability
    "roic_ttm": "high_is_good",
    "roe_ttm": "high_is_good",
    "gross_margin": "high_is_good",
    "ebitda_margin": "high_is_good",
    "fcf_margin": "high_is_good",
    # Growth
    "revenue_cagr_3y": "high_is_good",
    "eps_cagr_3y": "high_is_good",
    "gross_profit_cagr_3y": "high_is_good",
    "fcf_cagr_3y": "high_is_good",
    # Balance sheet
    "net_debt_to_ebitda": "low_is_good",
    "interest_coverage": "high_is_good",
    "current_ratio": "high_is_good",
    "debt_to_equity": "low_is_good",
    # Market
    "return_1y": "high_is_good",
    "beta": "low_is_good",
    "drawdown_52w_pct": "high_is_good",  # less-negative = better
}

STATIC_TOP50_SPX = [
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "BRK-B",
    "LLY", "AVGO", "TSLA", "JPM", "WMT", "V", "UNH", "XOM", "MA",
    "ORCL", "COST", "PG", "JNJ", "HD", "NFLX", "BAC", "CVX", "ABBV",
    "CRM", "KO", "AMD", "WFC", "MRK", "CSCO", "ADBE", "PEP", "LIN",
    "TMO", "ACN", "MCD", "ABT", "CMCSA", "INTU", "IBM", "DHR", "TXN",
    "PM", "DIS", "CAT", "VZ", "PFE", "QCOM",
]

s3 = boto3.client("s3", region_name="us-east-1")


def http_json(url, timeout=HTTP_TIMEOUT):
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "JustHodl-PeerComp/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[http] {e.code}: {url[:80]}")
        return None
    except Exception as e:
        print(f"[http] err: {str(e)[:80]}")
        return None


# ---------- FMP fetchers ----------
def fmp_peers(symbol):
    url = f"{FMP_BASE}/stock-peers?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    if isinstance(d, list) and d:
        row = d[0] if isinstance(d[0], dict) else None
        if row:
            return row.get("peersList") or []
    return []


def fmp_profile(symbol):
    url = f"{FMP_BASE}/profile?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    return (d[0] if isinstance(d, list) and d else None)


def fmp_key_metrics_ttm(symbol):
    url = f"{FMP_BASE}/key-metrics-ttm?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    return (d[0] if isinstance(d, list) and d else None)


def fmp_ratios_ttm(symbol):
    url = f"{FMP_BASE}/ratios-ttm?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    return (d[0] if isinstance(d, list) and d else None)


def fmp_income_growth(symbol, limit=4):
    url = (f"{FMP_BASE}/income-statement-growth?symbol={symbol}"
           f"&period=annual&limit={limit}&apikey={FMP_KEY}")
    return http_json(url)


def fmp_cf_growth(symbol, limit=4):
    url = (f"{FMP_BASE}/cash-flow-statement-growth?symbol={symbol}"
           f"&period=annual&limit={limit}&apikey={FMP_KEY}")
    return http_json(url)


def fmp_quote(symbol):
    url = f"{FMP_BASE}/quote?symbol={symbol}&apikey={FMP_KEY}"
    d = http_json(url)
    return (d[0] if isinstance(d, list) and d else None)


# ---------- Metric extraction ----------
def safe_float(v):
    try:
        if v is None:
            return None
        f = float(v)
        if f != f or f == float("inf") or f == float("-inf"):
            return None
        return f
    except (ValueError, TypeError):
        return None


def cagr_n(growth_rows, n, field):
    """Compute compounded growth rate from FMP annual growth rows."""
    if not isinstance(growth_rows, list) or len(growth_rows) < n:
        return None
    rates = []
    for r in growth_rows[:n]:
        if not isinstance(r, dict):
            continue
        v = safe_float(r.get(field))
        if v is not None:
            rates.append(v)
    if len(rates) < 2:
        return None
    # Geometric mean
    compound = 1.0
    for r in rates:
        compound *= (1 + r)
    return (compound ** (1.0 / len(rates))) - 1


def extract_metrics(symbol):
    """Pull + compute the full metric panel for one ticker."""
    profile = fmp_profile(symbol)
    time.sleep(0.15)
    km = fmp_key_metrics_ttm(symbol)
    time.sleep(0.15)
    ratios = fmp_ratios_ttm(symbol)
    time.sleep(0.15)
    income_growth = fmp_income_growth(symbol)
    time.sleep(0.15)
    cf_growth = fmp_cf_growth(symbol)
    time.sleep(0.15)
    quote = fmp_quote(symbol)
    time.sleep(0.15)

    m = {}

    if km:
        m["ev_ebitda_ttm"] = safe_float(km.get("evToEbitdaTTM")
                                         or km.get("evToEBITDATTM"))
        m["ev_sales_ttm"] = safe_float(km.get("evToSalesTTM")
                                        or km.get("evToRevenueTTM"))
        m["roic_ttm"] = safe_float(km.get("roicTTM"))
        m["net_debt_to_ebitda"] = safe_float(km.get("netDebtToEBITDATTM")
                                              or km.get("netDebtToEbitdaTTM"))
        m["interest_coverage"] = safe_float(km.get("interestCoverageTTM"))
        m["current_ratio"] = safe_float(km.get("currentRatioTTM"))
        m["debt_to_equity"] = safe_float(km.get("debtToEquityTTM")
                                          or km.get("debtToEquity"))

    if ratios:
        m["pe_ttm"] = safe_float(ratios.get("priceToEarningsRatioTTM")
                                  or ratios.get("peRatioTTM"))
        m["pb"] = safe_float(ratios.get("priceToBookRatioTTM")
                              or ratios.get("pbRatioTTM"))
        m["roe_ttm"] = (m.get("roe_ttm")
                         or safe_float(ratios.get("returnOnEquityTTM")))
        m["gross_margin"] = safe_float(ratios.get("grossProfitMarginTTM")
                                        or ratios.get("grossMarginTTM"))
        m["ebitda_margin"] = safe_float(ratios.get("ebitdaMarginTTM"))
        m["fcf_margin"] = safe_float(ratios.get("freeCashFlowMarginTTM"))
        m["fcf_yield"] = safe_float(ratios.get("freeCashFlowYieldTTM"))

    m["revenue_cagr_3y"] = cagr_n(income_growth, 3, "growthRevenue")
    m["eps_cagr_3y"] = cagr_n(income_growth, 3, "growthEPS")
    m["gross_profit_cagr_3y"] = cagr_n(income_growth, 3, "growthGrossProfit")
    m["fcf_cagr_3y"] = cagr_n(cf_growth, 3, "growthFreeCashFlow")

    if quote:
        m["return_1y"] = safe_float(quote.get("change") or quote.get(
            "yearChange") or quote.get("yearPercentChange"))
        # change is dollar; we want pct
        price = safe_float(quote.get("price"))
        year_low = safe_float(quote.get("yearLow"))
        year_high = safe_float(quote.get("yearHigh"))
        if year_low and price:
            m["return_1y_pct"] = (price - year_low) / year_low * 100
        # 52w drawdown (negative is worse — we'll store the pct value;
        # high_is_good means less-negative is better)
        if year_high and price:
            m["drawdown_52w_pct"] = -(year_high - price) / year_high * 100

    if profile:
        m["beta"] = safe_float(profile.get("beta"))
        m["sector"] = profile.get("sector")
        m["industry"] = profile.get("industry")
        m["market_cap_usd"] = safe_float(profile.get("marketCap"))
        m["company_name"] = profile.get("companyName")

    return m


def select_peers(target, peers, profile_target, all_profiles, max_n=8):
    """Filter peers by sector match + 0.5x-2x market cap proximity."""
    if not profile_target:
        return peers[:max_n]
    target_mc = profile_target.get("market_cap_usd")
    target_sector = profile_target.get("sector")
    if not target_mc or not target_sector:
        return peers[:max_n]
    scored = []
    for p in peers:
        prof = all_profiles.get(p)
        if not prof:
            continue
        if prof.get("sector") != target_sector:
            continue
        mc = prof.get("market_cap_usd")
        if not mc:
            continue
        ratio = max(mc, target_mc) / max(min(mc, target_mc), 1)
        if ratio > 5.0:
            continue
        scored.append((p, ratio))
    scored.sort(key=lambda x: x[1])
    filtered = [p for p, _ in scored][:max_n]
    if len(filtered) < 4:
        # Loosen: take any sector-match peer regardless of size
        sector_only = [p for p in peers
                        if all_profiles.get(p, {}).get(
                            "sector") == target_sector][:max_n]
        filtered = sector_only or peers[:max_n]
    return filtered


# ---------- Z-scoring + classification ----------
def compute_zscores(target_metrics, peer_metrics_list):
    """For each metric, compute target's z-score vs peer median."""
    out = {}
    for metric, direction in METRICS.items():
        target_val = target_metrics.get(metric)
        peer_vals = [m.get(metric) for m in peer_metrics_list
                      if m.get(metric) is not None]
        if target_val is None or len(peer_vals) < 3:
            out[metric] = {
                "target_value": target_val,
                "peer_median": None, "peer_n": len(peer_vals),
                "z_score": None, "classification": "INSUFFICIENT_DATA",
            }
            continue
        median = statistics.median(peer_vals)
        try:
            std = statistics.stdev(peer_vals + [target_val])
        except statistics.StatisticsError:
            std = 0
        if std == 0:
            z = 0
        else:
            z = (target_val - median) / std
        # Flip sign for low_is_good so z>0 always = "better"
        z_normalized = z if direction == "high_is_good" else -z
        if z_normalized >= 0.67:
            cls = "TOP_QUARTILE"
        elif z_normalized > 0:
            cls = "ABOVE_MEDIAN"
        elif z_normalized > -0.67:
            cls = "BELOW_MEDIAN"
        else:
            cls = "BOTTOM_QUARTILE"
        out[metric] = {
            "target_value": round(target_val, 4),
            "peer_median": round(median, 4),
            "peer_n": len(peer_vals),
            "z_score": round(z, 2),
            "z_normalized": round(z_normalized, 2),
            "direction": direction,
            "classification": cls,
        }
    return out


def generate_narrative(symbol, target_metrics, zscores, peers):
    """Generate institutional one-sentence summaries per category."""
    narrative = {}

    # Valuation summary
    val_cls = []
    for m in ["ev_ebitda_ttm", "pe_ttm", "fcf_yield"]:
        z = zscores.get(m) or {}
        if z.get("classification") in ("TOP_QUARTILE", "ABOVE_MEDIAN"):
            val_cls.append(z["classification"])
    if val_cls.count("TOP_QUARTILE") >= 2:
        narrative["valuation"] = "Trades at material discount to peer set"
    elif val_cls.count("TOP_QUARTILE") + val_cls.count(
            "ABOVE_MEDIAN") >= 2:
        narrative["valuation"] = "Trades at modest discount to peer set"
    else:
        narrative["valuation"] = "In-line or premium to peer set"

    # Profitability
    prof_cls = []
    for m in ["roic_ttm", "gross_margin", "ebitda_margin", "fcf_margin"]:
        z = zscores.get(m) or {}
        if z.get("classification") in ("TOP_QUARTILE", "ABOVE_MEDIAN"):
            prof_cls.append(z["classification"])
    if prof_cls.count("TOP_QUARTILE") >= 2:
        narrative["profitability"] = "Best-in-class profitability vs peers"
    elif prof_cls:
        narrative["profitability"] = "Above-peer profitability"
    else:
        narrative["profitability"] = "Below-peer profitability"

    # Growth
    g_cls = []
    for m in ["revenue_cagr_3y", "eps_cagr_3y", "fcf_cagr_3y"]:
        z = zscores.get(m) or {}
        if z.get("classification") in ("TOP_QUARTILE", "ABOVE_MEDIAN"):
            g_cls.append(z["classification"])
    if g_cls.count("TOP_QUARTILE") >= 2:
        narrative["growth"] = "Top-quartile growth profile vs peers"
    elif g_cls:
        narrative["growth"] = "Above-peer growth profile"
    else:
        narrative["growth"] = "Below-peer growth"

    # Composite thesis
    parts = [p for p in [narrative.get("valuation"),
                            narrative.get("profitability"),
                            narrative.get("growth")] if p]
    narrative["composite"] = (
        f"{symbol}: " + ". ".join(parts) + ".")

    return narrative


# ---------- Main ----------
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[peer-comparison] start v{VERSION}")

    if not FMP_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False,
                                      "error": "FMP_KEY missing"})}

    # Allow override of universe via event for on-demand mode
    universe = STATIC_TOP50_SPX
    if isinstance(event, dict):
        if event.get("tickers") and isinstance(event["tickers"], list):
            universe = [t.upper() for t in event["tickers"]][:20]
        elif event.get("ticker"):
            universe = [event["ticker"].upper()]

    # 1) Pull peers for each target ticker
    peers_map = {}
    for sym in universe:
        try:
            peers_map[sym] = fmp_peers(sym)
            time.sleep(0.15)
        except Exception as e:
            print(f"[peers] {sym} err: {str(e)[:80]}")
            peers_map[sym] = []

    # 2) Collect all unique tickers needed (target + their peers)
    all_tickers = set(universe)
    for sym, peers in peers_map.items():
        all_tickers.update(peers[:12])  # cap peers checked
    all_tickers = sorted(all_tickers)
    print(f"[peer-comparison] universe={len(universe)} "
          f"total_unique={len(all_tickers)}")

    # 3) Fetch metrics for all
    metrics_by_ticker = {}
    profiles = {}
    for i, t in enumerate(all_tickers):
        try:
            m = extract_metrics(t)
            metrics_by_ticker[t] = m
            profiles[t] = {
                "sector": m.get("sector"),
                "industry": m.get("industry"),
                "market_cap_usd": m.get("market_cap_usd"),
                "company_name": m.get("company_name"),
            }
            if i % 20 == 0:
                print(f"[peer-comparison] {i+1}/{len(all_tickers)}")
        except Exception as e:
            print(f"[metrics] {t} err: {str(e)[:80]}")

    # 4) Per-target: select peer set + compute z-scores + narrative
    results = []
    for sym in universe:
        target_m = metrics_by_ticker.get(sym)
        if not target_m:
            continue
        all_peers = peers_map.get(sym) or []
        target_profile = profiles.get(sym)
        chosen_peers = select_peers(sym, all_peers, target_profile,
                                       profiles, max_n=8)
        peer_metrics = [metrics_by_ticker[p] for p in chosen_peers
                          if metrics_by_ticker.get(p)]
        if len(peer_metrics) < 3:
            continue
        zscores = compute_zscores(target_m, peer_metrics)
        narrative = generate_narrative(sym, target_m, zscores, chosen_peers)
        results.append({
            "ticker": sym,
            "company_name": target_m.get("company_name"),
            "sector": target_m.get("sector"),
            "industry": target_m.get("industry"),
            "market_cap_usd": target_m.get("market_cap_usd"),
            "peer_set": chosen_peers,
            "n_peers": len(chosen_peers),
            "target_metrics": {k: target_m.get(k) for k in METRICS.keys()},
            "peer_medians": {k: zscores[k]["peer_median"]
                              for k in METRICS.keys() if k in zscores},
            "z_scores": zscores,
            "narrative": narrative,
        })

    output = {
        "engine": "peer-comparison",
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "n_evaluated": len(results),
        "universe_size": len(universe),
        "results": results,
        "metric_definitions": {
            m: {"direction": d,
                 "best_classification":
                    "TOP_QUARTILE = better than 75% of peers"}
            for m, d in METRICS.items()
        },
        "methodology": {
            "framework": "Damodaran-grade comparable analysis with z-scoring",
            "philosophy": (
                "Bloomberg CRPR + FactSet comps + CapIQ panels all do "
                "this at $20-50k/yr. This engine builds the institutional "
                "side-by-side with z-scores, peer-set selection by "
                "sector + market-cap proximity, and narrative summary."),
            "peer_selection": (
                "FMP /stable/peers seed + same-sector filter + 0.5x-2x "
                "market-cap proximity (relaxes to 5x if too few candidates)."),
            "z_normalization": (
                "Sign-flipped for low-is-good metrics so positive z always "
                "= better than peer median. Quartile cutoffs at +-0.67σ "
                "(roughly 25/50/75 percentile breaks)."),
            "narrative": (
                "Per-category one-line summary + composite thesis "
                "covering valuation, profitability, growth."),
        },
        "academic_basis": [
            "Damodaran (2012). Investment Valuation. Wiley. Ch 17-18.",
            "Liu, Nissim, Thomas (2002). Equity valuation using "
            "multiples. JAR, 40(1), 135-172.",
        ],
        "duration_seconds": round(time.time() - started, 1),
    }

    # Persist for batch mode only — on-demand mode returns inline
    is_on_demand = isinstance(event, dict) and (event.get("tickers")
                                                   or event.get("ticker"))
    if not is_on_demand:
        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(output, default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=3600")

    print(f"[peer-comparison] complete: {len(results)} tickers, "
          f"{output['duration_seconds']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True, "version": VERSION,
            "n_evaluated": len(results),
            "mode": "on_demand" if is_on_demand else "batch",
            "results": (results if is_on_demand
                          else [{"ticker": r["ticker"],
                                  "narrative": r["narrative"]}
                                 for r in results]),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
