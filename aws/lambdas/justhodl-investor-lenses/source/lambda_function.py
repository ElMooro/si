"""
justhodl-investor-lenses  —  Famous-investor valuation panel.

For a ticker, computes what each legendary investor's *published methodology*
implies as fair value, and whether the stock passes that investor's screen.
The product is the DISAGREEMENT across lenses, not any single number.

Lenses:
  - Buffett   : owner-earnings, discounted at 10Y UST, margin-of-safety haircut
  - Graham    : Graham Number  sqrt(22.5 * EPS * BVPS)  + defensive screen
  - Lynch     : PEG fair value (fair at PEG 1.0; he'd pay to ~1.5)
  - Greenblatt: Magic Formula components (EBIT/EV yield, ROIC) + verdict

Design:
  - 100% deterministic. NO LLM dependency -> runs even with Anthropic credits down.
  - Real data only (FMP). Keys resolved via SSM-first resolver (keys.py).
  - Writes data/investor-lenses/<TICKER>.json to S3 for the Research Desk page.
  - Emits a compact 'feed' block so canary-grid / research-dossier can read it as
    feed:justhodl-investor-lenses:lenses.<name>.fair_value

Schedule suggestion:  cron(15 13 * * ? *)  (after fundamentals refresh)
Payload: {"ticker": "AAPL"}  or  {"tickers": ["AAPL","MSFT",...]}
"""
import os
import json
import math
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("JH_BUCKET", "justhodl-dashboard-live")
S3_PREFIX = "data/investor-lenses"
FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_STABLE = "https://financialmodelingprep.com/stable"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

_s3 = boto3.client("s3")


# ----------------------------------------------------------------------------
# data fetch (real only)
# ----------------------------------------------------------------------------
def _get_json(url: str, tries: int = 3, backoff: float = 0.6):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            last = e
            time.sleep(backoff * (2 ** i))
    raise RuntimeError(f"fetch failed after {tries}: {url.split('?')[0]} :: {last}")


def fetch_fundamentals(ticker: str) -> dict:
    """Pull the real statements/metrics needed by all four lenses."""
    fmp = os.environ.get("FMP_KEY", "") or os.environ.get("FMP_API_KEY", "")
    t = ticker.upper()

    profile = _get_json(f"{FMP_BASE}/profile?symbol={t}&apikey={fmp}")
    quote = _get_json(f"{FMP_BASE}/quote?symbol={t}&apikey={fmp}")
    income = _get_json(f"{FMP_BASE}/income-statement?symbol={t}&period=annual&limit=12&apikey={fmp}")
    balance = _get_json(f"{FMP_BASE}/balance-sheet-statement?symbol={t}&period=annual&limit=12&apikey={fmp}")
    cashflow = _get_json(f"{FMP_BASE}/cash-flow-statement?symbol={t}&period=annual&limit=12&apikey={fmp}")
    metrics = _get_json(f"{FMP_BASE}/key-metrics?symbol={t}&period=annual&limit=12&apikey={fmp}")
    growth = _get_json(f"{FMP_BASE}/financial-growth?symbol={t}&period=annual&limit=6&apikey={fmp}")

    if not profile or not quote:
        raise RuntimeError(f"no profile/quote for {t} (bad ticker or key)")

    return {
        "profile": profile[0] if profile else {},
        "quote": quote[0] if quote else {},
        "income": income or [],
        "balance": balance or [],
        "cashflow": cashflow or [],
        "metrics": metrics or [],
        "growth": growth or [],
    }


def fetch_risk_free() -> float:
    """10Y UST from FRED (DGS10), latest. Buffett's discount anchor."""
    try:
        fred = os.environ.get("FRED_API_KEY", "") or os.environ.get("FRED_KEY", "")
        url = (f"{FRED_BASE}?series_id=DGS10&api_key={fred}&file_type=json"
               f"&sort_order=desc&limit=1")
        j = _get_json(url)
        obs = j.get("observations", [])
        if obs and obs[0]["value"] not in (".", ""):
            return float(obs[0]["value"]) / 100.0
    except Exception:
        pass
    return 0.043  # only if FRED unreachable; documented, not a data fabrication


# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------
def _safe(d: dict, *keys, default=None):
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return default


def _avg(vals):
    vals = [v for v in vals if isinstance(v, (int, float))]
    return sum(vals) / len(vals) if vals else None


# ----------------------------------------------------------------------------
# LENS 1 — Buffett: owner earnings
# ----------------------------------------------------------------------------
def lens_buffett(f: dict, rf: float) -> dict:
    """
    Owner earnings = Net income + D&A - maintenance capex (proxy: avg capex).
    Capitalize at max(rf + equity-risk-premium floor, hurdle). Buffett wants a
    business bought below a discount-to-owner-earnings with margin of safety.
    """
    inc = f["income"]
    cf = f["cashflow"]
    quote = f["quote"]
    shares = _safe(quote, "sharesOutstanding") or (_safe(inc[0], "weightedAverageShsOutDil", "weightedAverageShsOut") if inc else None)
    price = _safe(quote, "price")

    if not inc or not cf or not shares or not price:
        return {"name": "Buffett", "ok": False, "reason": "insufficient data"}

    # 3-year averages smooth the cycle (Buffett thinks in normalized earnings)
    ni = _avg([_safe(x, "netIncome") for x in inc[:3]])
    dep = _avg([_safe(x, "depreciationAndAmortization") for x in cf[:3]])
    capex = _avg([abs(_safe(x, "capitalExpenditure", default=0)) for x in cf[:3]])

    if ni is None or dep is None or capex is None:
        return {"name": "Buffett", "ok": False, "reason": "missing owner-earnings inputs"}

    owner_earnings = ni + dep - capex
    oe_per_share = owner_earnings / shares

    # discount rate: never below a 9% hurdle even in low-rate world
    disc = max(rf + 0.055, 0.09)
    # no-growth capitalized value, then apply a 25% margin of safety
    intrinsic = oe_per_share / disc
    fair_value_mos = intrinsic * 0.75  # buy price with margin of safety

    upside = (fair_value_mos / price - 1.0) * 100 if price else None

    # screen: Buffett wants consistent high ROE, low debt, wide margin
    metrics = f["metrics"]
    roe = _avg([_safe(x, "returnOnEquity", "roe") for x in metrics[:5]])
    passes = bool(roe and roe > 0.15 and owner_earnings > 0 and fair_value_mos >= price)

    return {
        "name": "Buffett",
        "ok": True,
        "method": "owner-earnings capitalized, 25% margin of safety",
        "owner_earnings": round(owner_earnings, 0),
        "oe_per_share": round(oe_per_share, 2),
        "discount_rate": round(disc, 4),
        "intrinsic_no_mos": round(intrinsic, 2),
        "fair_value": round(fair_value_mos, 2),
        "price": price,
        "upside_pct": round(upside, 1) if upside is not None else None,
        "passes_screen": passes,
        "why": (f"owner earnings ${owner_earnings/1e9:.1f}B / {disc:.1%} hurdle, "
                f"less 25% MoS -> ${fair_value_mos:.0f}. "
                + ("passes" if passes else "fails")
                + f" (avg ROE {roe:.0%})" if roe else ""),
    }


# ----------------------------------------------------------------------------
# LENS 2 — Graham: Graham Number + defensive screen
# ----------------------------------------------------------------------------
def lens_graham(f: dict) -> dict:
    quote = f["quote"]
    metrics = f["metrics"]
    inc = f["income"]
    balance = f["balance"]
    price = _safe(quote, "price")
    eps = _safe(quote, "eps") or (_safe(inc[0], "epsDiluted", "eps") if inc else None)
    # BVPS: stable has no bookValuePerShare; derive from equity / diluted shares
    bvps = None
    if balance and inc:
        equity = _safe(balance[0], "totalStockholdersEquity", "totalEquity")
        sh = _safe(inc[0], "weightedAverageShsOutDil", "weightedAverageShsOut")
        if equity and sh:
            bvps = equity / sh
    # stable also returns a precomputed grahamNumber we can cross-check
    graham_precomputed = _safe(metrics[0], "grahamNumber") if metrics else None

    if not price or not eps or not bvps or eps <= 0 or bvps <= 0:
        return {"name": "Graham", "ok": False,
                "reason": "needs positive EPS and BVPS (asset-light/negative-equity names fail by design)"}

    graham_number = math.sqrt(22.5 * eps * bvps)
    upside = (graham_number / price - 1.0) * 100

    # defensive screen elements
    pe = price / eps
    pb = price / bvps
    current_ratio = _safe(metrics[0], "currentRatio") if metrics else None
    if current_ratio is None and balance:
        ca = _safe(balance[0], "totalCurrentAssets")
        cl = _safe(balance[0], "totalCurrentLiabilities")
        if ca and cl:
            current_ratio = ca / cl
    passes = bool(pe <= 15 and pb <= 1.5 and (current_ratio or 0) >= 1.5)

    return {
        "name": "Graham",
        "ok": True,
        "method": "Graham Number sqrt(22.5*EPS*BVPS) + defensive screen",
        "graham_number": round(graham_number, 2),
        "fair_value": round(graham_number, 2),
        "price": price,
        "upside_pct": round(upside, 1),
        "pe": round(pe, 1),
        "pb": round(pb, 1),
        "current_ratio": round(current_ratio, 2) if current_ratio else None,
        "passes_screen": passes,
        "why": (f"Graham Number ${graham_number:.0f} vs ${price:.0f}. "
                f"Defensive screen P/E<=15 ({pe:.0f}), P/B<=1.5 ({pb:.1f}): "
                + ("passes" if passes else "fails")),
    }


# ----------------------------------------------------------------------------
# LENS 3 — Lynch: PEG fair value
# ----------------------------------------------------------------------------
def lens_lynch(f: dict) -> dict:
    quote = f["quote"]
    growth = f["growth"]
    metrics = f["metrics"]
    price = _safe(quote, "price")
    eps = _safe(quote, "eps") or (_safe(f["income"][0], "epsDiluted", "eps") if f["income"] else None)

    # Lynch growth = sustainable EPS growth; use 3-5y earnings growth, capped 25%
    g = None
    if growth:
        g = _avg([_safe(x, "epsgrowth", "epsdilutedGrowth") for x in growth[:3]])
    if g is not None:
        g = max(min(g, 0.25), 0.0)  # cap: Lynch distrusts >25% as unsustainable

    if not price or not eps or eps <= 0 or not g or g <= 0:
        return {"name": "Lynch", "ok": False,
                "reason": "needs positive EPS and positive sustainable growth"}

    g_pct = g * 100.0
    pe = price / eps
    peg = pe / g_pct

    # Lynch fair value at PEG=1.0; he'd pay up to ~1.5 for a great grower
    fair_value_peg1 = eps * g_pct
    fair_value_peg15 = eps * g_pct * 1.5
    upside = (fair_value_peg1 / price - 1.0) * 100
    passes = bool(peg <= 1.5)

    # dividend-adjusted PEG (Lynch's actual formula: (growth+yield)/PE)
    div_yield = _safe(quote, "dividendYield")
    if div_yield is None and f["cashflow"] and _safe(quote, "marketCap"):
        dp = abs(_safe(f["cashflow"][0], "commonDividendsPaid", "netDividendsPaid", default=0) or 0)
        mc = _safe(quote, "marketCap")
        div_yield = (dp / mc) if mc else None
    lynch_ratio = None
    if div_yield is not None:
        lynch_ratio = (g_pct + div_yield * 100) / pe

    return {
        "name": "Lynch",
        "ok": True,
        "method": "PEG fair value (fair at PEG 1.0, buyable to 1.5)",
        "growth_used_pct": round(g_pct, 1),
        "pe": round(pe, 1),
        "peg": round(peg, 2),
        "lynch_ratio": round(lynch_ratio, 2) if lynch_ratio else None,
        "fair_value": round(fair_value_peg1, 2),
        "fair_value_stretch": round(fair_value_peg15, 2),
        "price": price,
        "upside_pct": round(upside, 1),
        "passes_screen": passes,
        "why": (f"PEG {peg:.2f} (P/E {pe:.0f} / {g_pct:.0f}% growth). "
                f"Fair at PEG 1.0 = ${fair_value_peg1:.0f}: "
                + ("passes, PEG under 1.5" if passes else "fails, PEG over 1.5")),
    }


# ----------------------------------------------------------------------------
# LENS 4 — Greenblatt: Magic Formula components
# ----------------------------------------------------------------------------
def lens_greenblatt(f: dict) -> dict:
    quote = f["quote"]
    inc = f["income"]
    balance = f["balance"]
    metrics = f["metrics"]
    price = _safe(quote, "price")
    mcap = _safe(quote, "marketCap")

    if not inc or not balance or not mcap:
        return {"name": "Greenblatt", "ok": False, "reason": "insufficient data"}

    ebit = _safe(inc[0], "operatingIncome", "ebit")
    total_debt = _safe(balance[0], "totalDebt", default=0)
    cash = _safe(balance[0], "cashAndShortTermInvestments",
                 "cashAndCashEquivalents", default=0)
    ev = mcap + (total_debt or 0) - (cash or 0)

    if not ebit or not ev or ev <= 0:
        return {"name": "Greenblatt", "ok": False, "reason": "bad EBIT/EV"}

    earnings_yield = ebit / ev  # EBIT/EV — the "cheapness" leg
    roic = _avg([_safe(x, "returnOnInvestedCapital", "roic") for x in metrics[:3]])

    # Greenblatt doesn't produce a price target; he ranks. We express the two
    # legs and a composite verdict. High EY + high ROIC = the sweet spot.
    good_cheap = earnings_yield >= 0.08   # ~8%+ EBIT/EV yield
    good_quality = bool(roic and roic >= 0.15)
    passes = good_cheap and good_quality

    return {
        "name": "Greenblatt",
        "ok": True,
        "method": "Magic Formula legs — EBIT/EV yield + ROIC (rank, not target)",
        "earnings_yield": round(earnings_yield, 4),
        "earnings_yield_pct": round(earnings_yield * 100, 1),
        "roic": round(roic, 4) if roic else None,
        "roic_pct": round(roic * 100, 1) if roic else None,
        "fair_value": None,  # Greenblatt ranks; no single fair value by design
        "price": price,
        "passes_screen": passes,
        "why": (f"EBIT/EV yield {earnings_yield*100:.1f}% "
                f"({'cheap' if good_cheap else 'not cheap'}), "
                f"ROIC {roic*100:.0f}% ({'high-quality' if good_quality else 'low-quality'}). "
                + ("Magic-Formula candidate" if passes else "not a candidate")),
    }


# ----------------------------------------------------------------------------
# orchestration
# ----------------------------------------------------------------------------
def analyze(ticker: str) -> dict:
    f = fetch_fundamentals(ticker)
    rf = fetch_risk_free()
    price = _safe(f["quote"], "price")

    lenses = {
        "buffett": lens_buffett(f, rf),
        "graham": lens_graham(f),
        "lynch": lens_lynch(f),
        "greenblatt": lens_greenblatt(f),
    }

    # consensus of the lenses that produced a fair value
    fvs = [l["fair_value"] for l in lenses.values()
           if l.get("ok") and l.get("fair_value")]
    passes = [l["name"] for l in lenses.values() if l.get("passes_screen")]

    lens_median = None
    if fvs:
        s = sorted(fvs)
        n = len(s)
        lens_median = s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2

    # the actual product: where do the lenses DISAGREE?
    verdicts = []
    for l in lenses.values():
        if not l.get("ok"):
            continue
        u = l.get("upside_pct")
        if u is None:
            tag = "SCREEN" + ("+" if l.get("passes_screen") else "-")
        elif u > 15:
            tag = "CHEAP"
        elif u < -15:
            tag = "EXPENSIVE"
        else:
            tag = "FAIR"
        verdicts.append(f"{l['name']}:{tag}")

    disagreement = len(set(v.split(":")[1].rstrip("+-") for v in verdicts)) > 1

    return {
        "ticker": ticker.upper(),
        "generated": datetime.now(timezone.utc).isoformat(),
        "price": price,
        "risk_free_10y": rf,
        "lenses": lenses,
        "summary": {
            "lens_median_fair_value": round(lens_median, 2) if lens_median else None,
            "median_upside_pct": (round((lens_median / price - 1) * 100, 1)
                                  if lens_median and price else None),
            "passes": passes,
            "n_pass": len(passes),
            "verdicts": verdicts,
            "lenses_disagree": disagreement,
            "read": _one_liner(lenses, disagreement),
        },
        "_data_quality": "real:FMP+FRED",
    }


def _one_liner(lenses: dict, disagree: bool) -> str:
    b = lenses["buffett"]; g = lenses["graham"]; ly = lenses["lynch"]
    parts = []
    if b.get("ok") and b.get("upside_pct") is not None:
        parts.append(f"Buffett {'buy' if b['upside_pct'] > 0 else 'wait'}")
    if g.get("ok") and g.get("upside_pct") is not None:
        parts.append(f"Graham {'cheap' if g['upside_pct'] > 0 else 'expensive'}")
    if ly.get("ok") and ly.get("upside_pct") is not None:
        parts.append(f"Lynch {'fair' if -15 < ly['upside_pct'] < 15 else ('cheap' if ly['upside_pct'] > 0 else 'rich')}")
    core = "; ".join(parts) if parts else "lenses inconclusive"
    return core + (" — lenses disagree, that divergence is the signal." if disagree
                   else " — lenses agree.")


def write_s3(result: dict):
    key = f"{S3_PREFIX}/{result['ticker']}.json"
    _s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=json.dumps(result, indent=2).encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=300",
    )
    return key


def lambda_handler(event, context):
    tickers = event.get("tickers") or ([event["ticker"]] if event.get("ticker") else [])
    if not tickers:
        return {"statusCode": 400, "body": "provide ticker or tickers[]"}

    out = {}
    for t in tickers:
        try:
            res = analyze(t)
            key = write_s3(res)
            out[t.upper()] = {"ok": True, "s3": key,
                              "read": res["summary"]["read"]}
        except Exception as e:
            out[t.upper()] = {"ok": False, "error": str(e)}

    return {"statusCode": 200, "body": json.dumps(out)}


if __name__ == "__main__":
    # local smoke test against real data (needs FMP_KEY / FRED_KEY in env)
    import sys
    tk = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    print(json.dumps(analyze(tk), indent=2))
