"""
justhodl-equity-research (v1.0.1)
════════════════════════
Institutional-grade equity research desk. Given a ticker, produces a
full research paper with the same coverage a hedge fund analyst would
generate as homework before committing capital.

INVOKED via Lambda URL: ?ticker=ORCL  (or POST with {"ticker":"ORCL"})
Cache: results stored in S3 at equity-research/<TICKER>.json for 24h.

DATA FETCHED (in parallel)
══════════════════════════
FMP /stable/ endpoints:
  - profile                      → company description, sector, mkt cap
  - quote                        → current price, day range, volume
  - income-statement (20y annual + 8q quarterly)
  - balance-sheet-statement (20y annual)
  - cash-flow-statement (20y annual)
  - ratios (15y annual)          → P/E, P/B, ROE, ROA, ROIC, margins
  - ratios-ttm                   → current TTM ratios
  - key-metrics (15y annual)     → market cap, EV, FCF yield, debt/equity
  - key-metrics-ttm              → current
  - financial-growth (10y)       → revenue, EPS, FCF growth history
  - analyst-estimates            → forward EPS/revenue projections
  - price-target-consensus       → analyst consensus PT
  - dcf                          → FMP's DCF estimate
  - financial-scores             → Piotroski, Altman Z
  - peers                        → peer tickers
  - historical-price-eod (10y)   → returns, volatility, max drawdown

DERIVED ANALYSIS
════════════════
  - 20-year revenue CAGR, EPS CAGR, FCF CAGR
  - Margin trend (gross/operating/net over time)
  - Balance sheet quality (debt/equity trend, current ratio, working capital)
  - Cash flow quality (CFO vs net income, FCF conversion)
  - Earnings consistency (quarters of consecutive growth, beat rate)
  - Industry P/E comparison (peer average vs ticker)
  - Drawdown history (max DD, avg DD recovery time)
  - Buyback + dividend history (capital return)

CLAUDE SYNTHESIS
════════════════
After all data is gathered + summarized, Claude produces:
  - Executive summary (3-4 sentences institutional voice)
  - Bull case (investment thesis with 4-5 specific drivers)
  - Bear case (risks with 4-5 specific concerns)
  - Valuation assessment (DCF gap, multiples vs peers vs history)
  - Financial health (5-pillar score: profitability/growth/leverage/
                                 liquidity/quality)
  - Final verdict (BUY/HOLD/SELL with conviction grade + 12-month PT)
  - Key catalysts (next 12 months)
  - Invalidation triggers (what would change the thesis)

OUTPUT (JSON)
═════════════
{
  "ticker", "generated_at", "from_cache",
  "company": {name, sector, industry, country, exchange, ceo, employees,
              description, market_cap, ipo_date},
  "quote": {price, change_pct, volume, day_range, year_range},
  "verdict": {rating: BUY|HOLD|SELL, conviction_grade: A|B|C|D,
              price_target_12m, upside_pct, confidence_pct},
  "executive_summary": "...",
  "thesis": {bull_case: {...}, bear_case: {...}},
  "valuation": {pe_ratio, pe_industry, pe_5yr_avg, peg, dcf_estimate,
                dcf_upside_pct, ev_ebitda, p_b, fcf_yield, peer_table},
  "financial_health": {pillars, overall_score, altman_z, piotroski},
  "growth": {revenue_5yr_cagr, revenue_10yr_cagr, eps_5yr_cagr,
             eps_10yr_cagr, fcf_5yr_cagr, recent_quarters},
  "statements": {income_annual[20], balance_annual[20],
                 cashflow_annual[20], income_quarterly[8]},
  "margins": {gross_trend[], operating_trend[], net_trend[]},
  "returns": {ytd, 1yr, 3yr_cagr, 5yr_cagr, 10yr_cagr, max_drawdown_pct},
  "analyst": {pt_consensus, n_analysts, estimate_eps_fwd},
  "catalysts_12m": [...],
  "invalidation_triggers": [...],
  "metadata": {data_freshness, sources, elapsed_sec, claude_model}
}
"""

import json
import math
import os
import re
import sys
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3

# ═════════════════════════════════════════════════════════════════════
# Config
# ═════════════════════════════════════════════════════════════════════

FMP_KEY      = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE     = "https://financialmodelingprep.com/stable"
POLYGON_KEY  = os.environ.get("POLYGON_API_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
POLYGON_BASE = "https://api.polygon.io"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ANTHROPIC_KEY", "")
MODEL        = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
S3_BUCKET    = "justhodl-dashboard-live"
CACHE_PREFIX = "equity-research/"
CACHE_TTL    = 24 * 3600   # 24h cache (statements don't change daily)
FETCH_TIMEOUT = 20         # FMP per-call timeout
CLAUDE_TIMEOUT = 150        # was 90s, but bigger schema + transcript pushes to ~85s
FALLBACK_BUDGET_S = 70      # hard cap on the GLM/Sonnet fallback so a slow LLM never
                            # runs the Lambda into its 300s timeout before the doc writes
                            # (worst case: 150s Anthropic read-timeout + 70s fallback = 220s)

s3 = boto3.client("s3", region_name="us-east-1")


# ═════════════════════════════════════════════════════════════════════
# HTTP helpers
# ═════════════════════════════════════════════════════════════════════

def fmp_get(endpoint: str, **params) -> Optional[Any]:
    """Call FMP /stable/{endpoint} with apikey + params."""
    q = dict(params)
    q["apikey"] = FMP_KEY
    qs = urllib.parse.urlencode(q)
    url = f"{FMP_BASE}/{endpoint}?{qs}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodlEquityResearch/1.0"})
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            print(f"[fmp_get] {endpoint} → {e.code} (entitlement)")
        else:
            print(f"[fmp_get] {endpoint} → HTTP {e.code}")
    except Exception as e:
        print(f"[fmp_get] {endpoint} → {type(e).__name__}: {str(e)[:120]}")
    return None


def polygon_get(path: str, **params) -> Optional[Any]:
    """Call api.polygon.io/{path} with apiKey + params. Used for options data (OMON)."""
    q = dict(params)
    q["apiKey"] = POLYGON_KEY
    url = f"{POLYGON_BASE}/{path}?{urllib.parse.urlencode(q)}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodlEquityResearch/1.0"})
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        print(f"[polygon_get] {path} → HTTP {e.code}")
    except Exception as e:
        print(f"[polygon_get] {path} → {type(e).__name__}: {str(e)[:120]}")
    return None


def build_options_expectations(ticker: str, spot, next_earnings) -> Optional[dict]:
    """Bloomberg-OMON-style forward market expectations from Polygon options.
    Our tier exposes implied_volatility, greeks and open_interest per contract (no live
    bid/ask quotes, no IV history) — so we use the IV-based expected move rather than a
    straddle price, and omit IV-rank (no history). All values are real, market-derived.

      implied_move% = ATM_IV × √(days_to_expiry / 365)
    where the target expiry is the first listed expiry on/after the next earnings date."""
    import math, datetime
    if not spot or spot <= 0:
        return None
    lo, hi = round(spot * 0.80), round(spot * 1.20)
    params = {"strike_price.gte": lo, "strike_price.lte": hi, "limit": 250}
    if next_earnings:
        params["expiration_date.gte"] = next_earnings
    snap = polygon_get(f"v3/snapshot/options/{ticker}", **params)
    res = snap.get("results") if isinstance(snap, dict) else None
    if not res:
        return None

    def strike(c): return (c.get("details") or {}).get("strike_price")
    def ctype(c):  return (c.get("details") or {}).get("contract_type")

    exps: Dict[str, list] = {}
    for c in res:
        e = (c.get("details") or {}).get("expiration_date")
        if e and c.get("implied_volatility"):
            exps.setdefault(e, []).append(c)
    if not exps:
        return None

    today = datetime.date.today()
    target = sorted(exps.keys())[0]            # first expiry on/after earnings
    chain = exps[target]
    strikes = {strike(c) for c in chain if strike(c)}
    if not strikes:
        return None
    atm_k = min(strikes, key=lambda k: abs(k - spot))
    atm_ivs = [c["implied_volatility"] for c in chain if strike(c) == atm_k]
    atm_iv = sum(atm_ivs) / len(atm_ivs)
    try:
        days = max((datetime.date.fromisoformat(target) - today).days, 1)
    except Exception:
        days = 30
    move_pct = atm_iv * math.sqrt(days / 365.0) * 100

    # 25-delta-ish skew proxy: ~0.95×spot put IV minus ~1.05×spot call IV
    puts  = [c for c in chain if ctype(c) == "put"]
    calls = [c for c in chain if ctype(c) == "call"]
    pskew = min(puts,  key=lambda c: abs(strike(c) - spot * 0.95))["implied_volatility"] if puts else None
    cskew = min(calls, key=lambda c: abs(strike(c) - spot * 1.05))["implied_volatility"] if calls else None
    skew = (pskew - cskew) if (pskew is not None and cskew is not None) else None

    poi = sum((c.get("open_interest") or 0) for c in res if ctype(c) == "put")
    coi = sum((c.get("open_interest") or 0) for c in res if ctype(c) == "call")

    # vol smile: ATM-relative IV by strike for the target expiry (for a mini-chart)
    smile = []
    for k in sorted(strikes):
        ivs = [c["implied_volatility"] for c in chain if strike(c) == k]
        if ivs:
            smile.append({"strike": k, "iv_pct": round(sum(ivs) / len(ivs) * 100, 1),
                          "moneyness": round(k / spot, 3)})

    return {
        "spot":             round(spot, 2),
        "next_earnings":    next_earnings,
        "expiry":           target,
        "days_to_expiry":   days,
        "atm_iv_pct":       round(atm_iv * 100, 1),
        "implied_move_pct": round(move_pct, 1),
        "expected_low":     round(spot * (1 - move_pct / 100), 2),
        "expected_high":    round(spot * (1 + move_pct / 100), 2),
        "put_skew_pts":     round(skew * 100, 1) if skew is not None else None,
        "pc_oi_ratio":      round(poi / coi, 2) if coi else None,
        "put_oi":           poi,
        "call_oi":          coi,
        "n_contracts":      len(res),
        "smile":            smile[:40],
    }


def claude_call(system, user: str, max_tokens: int = 6000, use_cache: bool = True) -> str:
    """Anthropic Haiku primary; GLM-5.1 (Z.ai, via the shared llm_router) fallback when the
    direct Anthropic call fails — e.g. credits exhausted returns HTTP 400. The fallback is
    time-bounded: if neither model returns within the budget, we raise so the handler still
    writes the full quantitative report (valuation, business mix, price history, peers…) with
    the AI prose marked unavailable, rather than letting a slow LLM chain run the Lambda into
    its timeout and write nothing at all."""
    try:
        return _anthropic_call(system, user, max_tokens, use_cache)
    except Exception as e:
        print(f"[claude_call] Anthropic failed ({str(e)[:120]}); routing via llm_router (GLM-5.1)")
        try:
            from llm_router import complete
        except Exception as ie:
            print(f"[claude_call] llm_router import failed: {ie}")
            raise e
        sys_text = system if isinstance(system, str) else \
            "\n".join(b.get("text", "") for b in (system or []) if isinstance(b, dict))
        import concurrent.futures as _cf
        _ex = _cf.ThreadPoolExecutor(max_workers=1)
        try:
            fut = _ex.submit(complete, user, tier="reason",
                             max_tokens=max(max_tokens, 2000), system=(sys_text or None))
            try:
                return fut.result(timeout=FALLBACK_BUDGET_S)
            except _cf.TimeoutError:
                print(f"[claude_call] llm_router fallback exceeded {FALLBACK_BUDGET_S}s; "
                      f"giving up so the doc still writes with quant data")
                raise RuntimeError("LLM fallback timed out — quant report still produced")
        finally:
            # wait=False is critical: do NOT block on the still-hung LLM thread, or
            # the handler can't reach the S3 write before the Lambda times out.
            _ex.shutdown(wait=False)


def _anthropic_call(system, user: str, max_tokens: int = 6000, use_cache: bool = True) -> str:
    """Single-message call to Anthropic API with prompt caching.

    system: either a plain str (legacy) or a list of typed content blocks
            (when use_cache=True we convert str → list and mark for caching).
    use_cache: when True, marks the system block with cache_control ttl=1h.
               Falls back gracefully if total system tokens < cache threshold
               (4096 for Haiku 4.5) — Anthropic just returns no-cache metadata.
    """
    if not ANTHROPIC_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    # Build system block — either pass-through string, or structured for caching
    if use_cache:
        # Wrap system text in a content block with cache_control breakpoint.
        # ttl=1h fits the nightly prewarmer pattern (52 tickers in ~13 min wall time,
        # all within one hour). The 2.0x write surcharge is paid once per hour,
        # then all subsequent calls within the TTL pay only 0.10x = 90% off.
        if isinstance(system, str):
            system_blocks = [{
                "type": "text",
                "text": system,
                "cache_control": {"type": "ephemeral", "ttl": "1h"},
            }]
        else:
            system_blocks = system  # caller already provided structured blocks
    else:
        system_blocks = system  # legacy str passthrough

    payload = json.dumps({
        "model": MODEL,
        "max_tokens": max_tokens,
        "system": system_blocks,
        "messages": [{"role": "user", "content": user}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages", data=payload,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "extended-cache-ttl-2025-04-11",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=CLAUDE_TIMEOUT) as r:
        data = json.loads(r.read())
    if not data.get("content"):
        raise RuntimeError(f"Empty Claude response: {data}")

    # Log cache telemetry — useful for verifying savings
    usage = data.get("usage", {}) or {}
    cache_create = usage.get("cache_creation_input_tokens", 0)
    cache_read   = usage.get("cache_read_input_tokens", 0)
    input_tokens = usage.get("input_tokens", 0)
    output_tokens = usage.get("output_tokens", 0)
    print(f"[claude] usage: input={input_tokens} cache_create={cache_create} "
          f"cache_read={cache_read} output={output_tokens}")

    text = "".join(b.get("text", "") for b in data["content"] if b.get("type") == "text").strip()
    # Stash usage in a sidecar attribute for the caller to read (Python doesn't
    # allow attaching to str literals; we'll return text and let caller call again
    # for the usage if needed. For now, we just log it.)
    return text


# ═════════════════════════════════════════════════════════════════════
# v2.0 INSTITUTIONAL MODULES — technicals (full TA), liquidity & solvency,
# growth-vs-market-cap, quantitative risk, backlog/RPO join. Pure-python,
# every block guarded: any failure degrades to {"available": False}.
# ═════════════════════════════════════════════════════════════════════

def _n2(o, k):
    try:
        v = (o or {}).get(k)
        return float(v) if v is not None and v == v else None
    except Exception:
        return None


def _sma(vals, n):
    out = [None] * len(vals)
    s = 0.0
    for i, v in enumerate(vals):
        s += v
        if i >= n:
            s -= vals[i - n]
        if i >= n - 1:
            out[i] = s / n
    return out


def _ema(vals, n):
    out = [None] * len(vals)
    if len(vals) < n:
        return out
    k = 2.0 / (n + 1)
    e = sum(vals[:n]) / n
    out[n - 1] = e
    for i in range(n, len(vals)):
        e = vals[i] * k + e * (1 - k)
        out[i] = e
    return out


def _rsi14(closes, n=14):
    out = [None] * len(closes)
    if len(closes) <= n:
        return out
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0); losses += max(-d, 0)
    ag, al = gains / n, losses / n
    out[n] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0)) / n
        al = (al * (n - 1) + max(-d, 0)) / n
        out[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    return out


def _macd(closes, f=12, s=26, sig=9):
    ef, es = _ema(closes, f), _ema(closes, s)
    line = [None if (a is None or b is None) else a - b for a, b in zip(ef, es)]
    vals = [v for v in line if v is not None]
    signal = [None] * len(line)
    if len(vals) >= sig:
        off = len(line) - len(vals)
        sg = _ema(vals, sig)
        for i, v in enumerate(sg):
            signal[off + i] = v
    hist = [None if (a is None or b is None) else a - b for a, b in zip(line, signal)]
    return line, signal, hist


def _bollinger(closes, n=20, k=2.0):
    mid = _sma(closes, n)
    up, lo = [None] * len(closes), [None] * len(closes)
    for i in range(n - 1, len(closes)):
        w = closes[i - n + 1:i + 1]
        m = mid[i]
        var = sum((x - m) ** 2 for x in w) / n
        sd = var ** 0.5
        up[i], lo[i] = m + k * sd, m - k * sd
    return mid, up, lo


def _px_rows(raw_obj):
    """Normalize FMP historical shapes -> ascending [{date,o,h,l,c,v}]."""
    rows = raw_obj
    if isinstance(rows, dict):
        rows = rows.get("historical") or rows.get("results") or []
    if not isinstance(rows, list):
        return []
    out = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        c = _n2(r, "close") if r.get("close") is not None else _n2(r, "price")
        if c is None or not r.get("date"):
            continue
        out.append({"date": str(r["date"])[:10], "o": _n2(r, "open"), "h": _n2(r, "high"),
                    "l": _n2(r, "low"), "c": c, "v": _n2(r, "volume") or 0.0})
    out.sort(key=lambda x: x["date"])
    return out


def build_technicals(raw):
    rows = _px_rows(raw.get("prices_full"))
    if len(rows) < 60:
        return {"available": False, "reason": "insufficient price history"}
    rows = rows[-620:]
    dates = [r["date"] for r in rows]
    closes = [r["c"] for r in rows]
    highs = [r["h"] if r["h"] is not None else r["c"] for r in rows]
    lows = [r["l"] if r["l"] is not None else r["c"] for r in rows]
    vols = [r["v"] for r in rows]
    sma20, sma50 = _sma(closes, 20), _sma(closes, 50)
    sma100, sma200 = _sma(closes, 100), _sma(closes, 200)
    bb_m, bb_u, bb_l = _bollinger(closes, 20, 2.0)
    rsi = _rsi14(closes)
    m_line, m_sig, m_hist = _macd(closes)
    # beta / corr vs SPY (2y daily)
    beta = corr = None
    try:
        spy = {r["date"]: r["c"] for r in _px_rows(raw.get("spy_light"))}
        pairs = [(closes[i] / closes[i - 1] - 1, spy[dates[i]] / spy[dates[i - 1]] - 1)
                 for i in range(1, len(dates)) if dates[i] in spy and dates[i - 1] in spy and spy[dates[i - 1]]]
        if len(pairs) > 120:
            xs = [p[1] for p in pairs]; ys = [p[0] for p in pairs]
            mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
            cov = sum((x - mx) * (y - my) for x, y in pairs) / len(pairs)
            vx = sum((x - mx) ** 2 for x in xs) / len(xs)
            vy = sum((y - my) ** 2 for y in ys) / len(ys)
            if vx > 0:
                beta = round(cov / vx, 2)
            if vx > 0 and vy > 0:
                corr = round(cov / ((vx * vy) ** 0.5), 2)
    except Exception:
        pass
    last = closes[-1]
    hi52 = max(closes[-252:]) if len(closes) >= 252 else max(closes)
    lo52 = min(closes[-252:]) if len(closes) >= 252 else min(closes)
    rets30 = [closes[i] / closes[i - 1] - 1 for i in range(len(closes) - 29, len(closes))] if len(closes) > 30 else []
    rvol30 = None
    if rets30:
        mu = sum(rets30) / len(rets30)
        rvol30 = round(((sum((r - mu) ** 2 for r in rets30) / len(rets30)) ** 0.5) * (252 ** 0.5) * 100, 1)
    adv63 = round(sum(closes[i] * vols[i] for i in range(len(closes) - 63, len(closes))) / 63 / 1e6, 1) if len(closes) >= 63 else None

    def chg(nd):
        return round((last / closes[-nd - 1] - 1) * 100, 1) if len(closes) > nd else None
    def _r(a, d=2):
        return [None if x is None else round(x, d) for x in a]
    N = 504
    series = {"dates": dates[-N:], "close": _r(closes[-N:]), "volume": [round(v) for v in vols[-N:]],
              "sma20": _r(sma20[-N:]), "sma50": _r(sma50[-N:]), "sma100": _r(sma100[-N:]),
              "sma200": _r(sma200[-N:]), "bb_upper": _r(bb_u[-N:]), "bb_lower": _r(bb_l[-N:]),
              "rsi": _r(rsi[-N:], 1), "macd": _r(m_line[-N:], 3), "macd_signal": _r(m_sig[-N:], 3),
              "macd_hist": _r(m_hist[-N:], 3)}
    ml, ms = m_line[-1], m_sig[-1]
    stats = {"last": round(last, 2), "chg_1m_pct": chg(21), "chg_3m_pct": chg(63),
             "chg_6m_pct": chg(126), "chg_1y_pct": chg(252),
             "high_52w": round(hi52, 2), "low_52w": round(lo52, 2),
             "off_high_pct": round((last / hi52 - 1) * 100, 1),
             "above_200sma": bool(sma200[-1] and last > sma200[-1]),
             "above_50sma": bool(sma50[-1] and last > sma50[-1]),
             "golden_cross": bool(sma50[-1] and sma200[-1] and sma50[-1] > sma200[-1]),
             "rsi_last": series["rsi"][-1],
             "macd_state": ("bullish" if (ml is not None and ms is not None and ml > ms) else
                            "bearish" if (ml is not None and ms is not None) else None),
             "realized_vol_30d_pct": rvol30, "beta_2y": beta, "spy_corr_2y": corr,
             "adv_dollar_3m_musd": adv63}
    return {"available": True, "series": series, "stats": stats}


def build_liquidity(income_annual, balance_annual, cashflow_annual, ratios_ttm, technicals):
    bs = balance_annual[0] if balance_annual else {}
    inc = income_annual[0] if income_annual else {}
    cf = cashflow_annual[0] if cashflow_annual else {}
    rt = (ratios_ttm[0] if isinstance(ratios_ttm, list) and ratios_ttm else ratios_ttm) or {}
    cash = (_n2(bs, "cashAndCashEquivalents") or 0) + (_n2(bs, "shortTermInvestments") or 0)
    ca, cl = _n2(bs, "totalCurrentAssets"), _n2(bs, "totalCurrentLiabilities")
    inv = _n2(bs, "inventory") or 0
    st_debt, lt_debt = _n2(bs, "shortTermDebt") or 0, _n2(bs, "longTermDebt") or 0
    tdebt = _n2(bs, "totalDebt") or (st_debt + lt_debt)
    op_inc = _n2(inc, "operatingIncome")
    int_exp = abs(_n2(inc, "interestExpense") or 0)
    dep = _n2(cf, "depreciationAndAmortization") or 0
    ebitda = (op_inc + dep) if op_inc is not None else None
    ocf = _n2(cf, "operatingCashFlow") or _n2(cf, "netCashProvidedByOperatingActivities")
    capex = abs(_n2(cf, "capitalExpenditure") or 0)
    fcf = (ocf - capex) if ocf is not None else None
    net_debt = tdebt - cash
    out = {"available": True,
           "cash_and_sti_b": round(cash / 1e9, 2),
           "total_debt_b": round(tdebt / 1e9, 2),
           "net_debt_b": round(net_debt / 1e9, 2),
           "st_debt_b": round(st_debt / 1e9, 2), "lt_debt_b": round(lt_debt / 1e9, 2),
           "current_ratio": round(ca / cl, 2) if ca and cl else _n2(rt, "currentRatioTTM"),
           "quick_ratio": round((ca - inv) / cl, 2) if ca and cl else _n2(rt, "quickRatioTTM"),
           "cash_ratio": round(cash / cl, 2) if cl else None,
           "net_debt_to_ebitda": round(net_debt / ebitda, 2) if ebitda and ebitda > 0 else None,
           "interest_coverage": round(op_inc / int_exp, 1) if op_inc is not None and int_exp > 0 else None,
           "working_capital_b": round((ca - cl) / 1e9, 2) if ca is not None and cl is not None else None,
           "fcf_annual_b": round(fcf / 1e9, 2) if fcf is not None else None,
           "cash_runway_quarters": (round(cash / (abs(fcf) / 4), 1)
                                    if (fcf is not None and fcf < 0 and cash > 0) else None),
           "adv_dollar_3m_musd": (technicals.get("stats", {}) or {}).get("adv_dollar_3m_musd")
                                  if isinstance(technicals, dict) else None}
    cr = out["current_ratio"]; nde = out["net_debt_to_ebitda"]
    if fcf is not None and fcf < 0 and out["cash_runway_quarters"] and out["cash_runway_quarters"] < 6:
        read = "TIGHT — cash-burner with under 6 quarters of runway; financing risk is live."
    elif nde is not None and nde > 3.5:
        read = "LEVERED — net debt above 3.5x EBITDA; refinancing and rate sensitivity matter."
    elif net_debt < 0:
        read = "FORTRESS — net cash balance sheet; liquidity is a strategic weapon here."
    elif cr is not None and cr < 1:
        read = "WATCH — current liabilities exceed current assets."
    else:
        read = "ADEQUATE — no near-term liquidity constraint visible in the filings."
    out["read"] = read
    return out


def build_growth_vs_mcap(raw, income_annual, cashflow_annual):
    prof = raw.get("profile"); prof = prof[0] if isinstance(prof, list) and prof else (prof or {})
    q = raw.get("quote"); q = q[0] if isinstance(q, list) and q else (q or {})
    rt = raw.get("ratios_ttm"); rt = rt[0] if isinstance(rt, list) and rt else (rt or {})
    km = raw.get("key_metrics_ttm"); km = km[0] if isinstance(km, list) and km else (km or {})
    est = raw.get("estimates") if isinstance(raw.get("estimates"), list) else []
    mcap = _n2(prof, "marketCap") or _n2(q, "marketCap")
    revs = [_n2(r, "revenue") for r in income_annual[:6] if _n2(r, "revenue")]
    fcfs = []
    for r in cashflow_annual[:6]:
        o = _n2(r, "operatingCashFlow") or _n2(r, "netCashProvidedByOperatingActivities")
        c = abs(_n2(r, "capitalExpenditure") or 0)
        if o is not None:
            fcfs.append(o - c)

    def cagr(series, yrs):
        if len(series) > yrs and series[yrs] and series[yrs] > 0 and series[0] and series[0] > 0:
            return round(((series[0] / series[yrs]) ** (1.0 / yrs) - 1) * 100, 1)
        return None
    rev_c3, rev_c5 = cagr(revs, 3), cagr(revs, 5)
    rev_yoy = round((revs[0] / revs[1] - 1) * 100, 1) if len(revs) > 1 and revs[1] else None
    fcf_c3 = cagr([f for f in fcfs], 3) if len(fcfs) > 3 and all(f and f > 0 for f in fcfs[:4]) else None
    fcf_margin = round(fcfs[0] / revs[0] * 100, 1) if fcfs and revs and revs[0] else None
    # forward growth from analyst estimates (next FY vs latest actual)
    fwd_rev_g = fwd_eps_g = None
    try:
        e0 = est[0] if est else {}
        er = _n2(e0, "revenueAvg") or _n2(e0, "estimatedRevenueAvg")
        if er and revs:
            fwd_rev_g = round((er / revs[0] - 1) * 100, 1)
        ee = _n2(e0, "epsAvg") or _n2(e0, "estimatedEpsAvg")
        eps_now = _n2(income_annual[0] if income_annual else {}, "epsdiluted") or _n2(income_annual[0] if income_annual else {}, "eps")
        if ee and eps_now and eps_now > 0:
            fwd_eps_g = round((ee / eps_now - 1) * 100, 1)
    except Exception:
        pass
    pe = _n2(rt, "priceToEarningsRatioTTM") or _n2(rt, "peRatioTTM") or _n2(q, "pe")
    ev_s = _n2(km, "evToSalesTTM") or _n2(rt, "evToSalesTTM") or _n2(rt, "priceToSalesRatioTTM")
    fcf_yield = round(fcfs[0] / mcap * 100, 2) if fcfs and mcap else _n2(km, "freeCashFlowYieldTTM")
    g_for_peg = fwd_eps_g if fwd_eps_g and fwd_eps_g > 0 else rev_c3
    peg = round(pe / g_for_peg, 2) if pe and g_for_peg and g_for_peg > 0 else None
    garp = round(ev_s / rev_c3, 2) if ev_s and rev_c3 and rev_c3 > 0 else None
    rule40 = round((rev_yoy or 0) + (fcf_margin or 0), 1) if rev_yoy is not None or fcf_margin is not None else None
    out = {"available": True, "market_cap_b": round(mcap / 1e9, 1) if mcap else None,
           "rev_yoy_pct": rev_yoy, "rev_cagr_3y_pct": rev_c3, "rev_cagr_5y_pct": rev_c5,
           "fcf_cagr_3y_pct": fcf_c3, "fcf_margin_pct": fcf_margin,
           "fwd_rev_growth_pct": fwd_rev_g, "fwd_eps_growth_pct": fwd_eps_g,
           "pe_ttm": round(pe, 1) if pe else None, "ev_to_sales": round(ev_s, 1) if ev_s else None,
           "peg": peg, "ev_s_per_growth": garp, "fcf_yield_pct": round(fcf_yield, 2) if fcf_yield else None,
           "rule_of_40": rule40}
    if peg is not None and peg < 1:
        read = "GROWTH AT A DISCOUNT — you are paying less than 1x P/E per point of growth."
    elif garp is not None and garp > 1.2:
        read = "EXPENSIVE VS GROWTH — the sales multiple outruns the growth rate; the market is paying for a future that must show up."
    elif rule40 is not None and rule40 >= 40:
        read = "RULE-OF-40 PASS — growth plus FCF margin clears the institutional efficiency bar."
    else:
        read = "IN LINE — valuation and growth are roughly matched; the edge must come from estimate revisions."
    out["read"] = read
    return out


def build_quant_risk(raw, income_annual, cashflow_annual):
    sc = raw.get("scores"); sc = sc[0] if isinstance(sc, list) and sc else (sc or {})
    altman = _n2(sc, "altmanZScore")
    piotroski = _n2(sc, "piotroskiScore")
    shares = [_n2(r, "weightedAverageShsOutDil") or _n2(r, "weightedAverageShsOut") for r in income_annual[:5]]
    shares = [s for s in shares if s]
    dil_1y = round((shares[0] / shares[1] - 1) * 100, 1) if len(shares) > 1 and shares[1] else None
    dil_3y = round(((shares[0] / shares[3]) ** (1 / 3) - 1) * 100, 1) if len(shares) > 3 and shares[3] else None
    cf = cashflow_annual[0] if cashflow_annual else {}
    inc = income_annual[0] if income_annual else {}
    sbc = abs(_n2(cf, "stockBasedCompensation") or 0)
    rev = _n2(inc, "revenue")
    sbc_pct = round(sbc / rev * 100, 1) if rev else None
    gms = [_n2(r, "grossProfitRatio") for r in income_annual[:4]]
    gm_trend = round((gms[0] - gms[3]) * 100, 1) if len(gms) > 3 and gms[0] is not None and gms[3] is not None else None
    flags = []
    if altman is not None and altman < 1.8:
        flags.append(f"Altman Z {altman:.1f} — distress zone")
    if piotroski is not None and piotroski <= 3:
        flags.append(f"Piotroski {int(piotroski)}/9 — weak fundamental quality")
    if dil_1y is not None and dil_1y > 5:
        flags.append(f"Dilution {dil_1y:+.1f}% shares YoY")
    if sbc_pct is not None and sbc_pct > 10:
        flags.append(f"SBC {sbc_pct:.0f}% of revenue")
    if gm_trend is not None and gm_trend < -3:
        flags.append(f"Gross margin down {gm_trend:.1f}pp over 3y")
    return {"available": True, "altman_z": altman, "piotroski": piotroski,
            "dilution_1y_pct": dil_1y, "dilution_3y_cagr_pct": dil_3y,
            "sbc_pct_of_rev": sbc_pct, "gross_margin_3y_delta_pp": gm_trend,
            "flags": flags,
            "read": ("CLEAN — no quantitative distress or quality flags." if not flags
                     else f"{len(flags)} flag(s): " + "; ".join(flags[:3]))}


def read_backlog_join(ticker):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/backlog.json")
        d = json.loads(obj["Body"].read())
        rows = d.get("names") or d.get("rows") or d.get("tickers") or d
        ent = None
        if isinstance(rows, dict):
            ent = rows.get(ticker) or rows.get(ticker.upper())
        elif isinstance(rows, list):
            ent = next((r for r in rows if isinstance(r, dict)
                        and str(r.get("ticker") or r.get("symbol") or "").upper() == ticker), None)
        if not ent:
            return {"available": False, "reason": "no backlog/RPO coverage for this name"}
        def g(*keys):
            for k in keys:
                v = _n2(ent, k)
                if v is not None:
                    return v
            return None
        return {"available": True,
                "rpo_b": g("rpo_b", "rpo_billion", "rpo") ,
                "rpo_yoy_pct": g("rpo_yoy_pct", "rpo_yoy", "rpo_growth_yoy"),
                "deferred_rev_yoy_pct": g("deferred_yoy_pct", "deferred_rev_yoy", "deferred_growth"),
                "book_to_bill": g("book_to_bill", "btb"),
                "note": ent.get("note") or ent.get("read") or "Contracted demand from the platform's RPO/backlog extractor.",
                "as_of": d.get("generated_at")}
    except Exception as e:
        return {"available": False, "reason": str(e)[:80]}


# ═════════════════════════════════════════════════════════════════════
# v2.1 INDUSTRY COMPASS — stock vs its official Finviz industry
#   (1) stock perf windows vs industry perf windows (gap quantified)
#   (2) stock-level Grinold-Kroner 12m ER, every component published
#   (3) laggard-catchup screen: industry pumped + stock lagged + growth
#       intact + cheaper than industry  → asymmetric catch-up candidate
#   (4) rate sensitivity vs the market-implied next-12m rate path
# Zero LLM cost. Consumes data/finviz-groups.json + data/asset-compass.json.
# ═════════════════════════════════════════════════════════════════════

_IND_DROP = {"and", "the", "of", "industry", "industries", "other", "misc"}


def _norm_ind(s):
    s = (s or "").lower().replace("&", " and ").replace("—", " ").replace("–", " ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return [w for w in s.split() if w not in _IND_DROP]


def _match_industry(fmp_industry, rows):
    """Exact normalized match first, else best token-Jaccard ≥ 0.5.
    Returns (row, confidence) or (None, None)."""
    toks = set(_norm_ind(fmp_industry))
    if not toks or not rows:
        return None, None
    best, best_j = None, 0.0
    for r in rows:
        rt = set(_norm_ind(r.get("name")))
        if not rt:
            continue
        if rt == toks:
            return r, 1.0
        j = len(toks & rt) / len(toks | rt)
        if j > best_j:
            best, best_j = r, j
    if best is not None and best_j >= 0.5:
        return best, round(best_j, 2)
    return None, None


def _stock_perf_windows(prices_eod):
    """perf_m/q/h/y % from FMP EOD (descending by date), mirroring
    Finviz group windows (21/63/126/252 trading days)."""
    if not isinstance(prices_eod, list) or len(prices_eod) < 30:
        return {}
    prices = sorted(prices_eod, key=lambda p: p.get("date") or "")
    closes = [(_safe_num(p, "price") or _safe_num(p, "close")) for p in prices]
    closes = [c for c in closes if c and c > 0]
    if len(closes) < 30:
        return {}

    def perf(n):
        if len(closes) <= n or not closes[-1 - n]:
            return None
        return round((closes[-1] / closes[-1 - n] - 1.0) * 100.0, 1)
    return {"perf_m": perf(21), "perf_q": perf(63),
            "perf_h": perf(126), "perf_y": perf(252)}


def _median(vals):
    v = sorted(vals)
    n = len(v)
    if not n:
        return None
    return v[n // 2] if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2.0


def build_industry_compass(raw, income_annual, cashflow_annual):
    prof = _first(raw.get("profile")) or {}
    quote = _first(raw.get("quote")) or {}
    rt = _first(raw.get("ratios_ttm")) or {}
    px = _safe_num(quote, "price")
    fmp_ind = prof.get("industry")

    # ── shared macro anchors (from the live asset-compass engine) ──
    infl, rf_dir, infl_src = None, None, None
    try:
        mac = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                       Key="data/asset-compass.json")["Body"].read())
        mf = mac.get("macro_forward") or {}
        infl = _safe_num(mf, "infl_1y_expected_pct")
        rf_dir = mf.get("rf_direction_next_year")
        infl_src = mf.get("infl_source")
    except Exception:
        pass
    infl_flag = None
    if infl is None:
        infl, infl_flag = 2.5, "FALLBACK: asset-compass macro unavailable; 2.5% assumed"

    # ── industry row (official Finviz group aggregates) ──
    ind_row, conf = None, None
    try:
        fg = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                      Key="data/finviz-groups.json")["Body"].read())
        ind_row, conf = _match_industry(fmp_ind, fg.get("industries") or [])
    except Exception:
        pass

    sp = _stock_perf_windows(raw.get("prices_eod"))
    gaps, ind_perf = {}, {}
    if ind_row:
        for w in ("perf_m", "perf_q", "perf_h", "perf_y"):
            iv = _safe_num(ind_row, w)
            ind_perf[w] = iv
            if iv is not None and sp.get(w) is not None:
                gaps[w] = round(sp[w] - iv, 1)

    # ── stock-level Grinold-Kroner (12m), every component real + published ──
    pe_now = (_safe_num(rt, "priceToEarningsRatioTTM")
              or _safe_num(rt, "peRatioTTM") or _safe_num(quote, "pe"))
    dy = bby = None
    inc0 = income_annual[0] if income_annual else {}
    shs0 = (_safe_num(inc0, "weightedAverageShsOutDil")
            or _safe_num(inc0, "weightedAverageShsOut"))
    if cashflow_annual and px and shs0:
        cf0 = cashflow_annual[0]
        div_paid = abs(_safe_num(cf0, "commonDividendsPaid")
                       or _safe_num(cf0, "netDividendsPaid")
                       or _safe_num(cf0, "dividendsPaid") or 0.0)
        if div_paid:
            dy = round(div_paid / (px * shs0) * 100.0, 2)
    if len(income_annual) >= 2 and shs0:
        shs1 = (_safe_num(income_annual[1], "weightedAverageShsOutDil")
                or _safe_num(income_annual[1], "weightedAverageShsOut"))
        if shs1:
            bby = round((shs1 - shs0) / shs1 * 100.0, 2)  # + = net shrink

    # forward nominal EPS growth: nearest future analyst FY vs latest FY EPS
    eps0 = _safe_num(inc0, "epsdiluted") or _safe_num(inc0, "epsDiluted") \
        or _safe_num(inc0, "eps")
    g_fwd = None
    est = raw.get("estimates") if isinstance(raw.get("estimates"), list) else []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    fut = sorted([e for e in est if (e.get("date") or "") >= today],
                 key=lambda e: e.get("date") or "")
    if fut and eps0 and eps0 > 0:
        e_fwd = _safe_num(fut[0], "epsAvg") or _safe_num(fut[0], "estimatedEpsAvg")
        if e_fwd:
            g_fwd = round((e_fwd / eps0 - 1.0) * 100.0, 1)
    # historical 5y EPS CAGR as the sanity anchor
    g_hist = None
    if len(income_annual) >= 6 and eps0 and eps0 > 0:
        eps5 = _safe_num(income_annual[5], "epsdiluted") \
            or _safe_num(income_annual[5], "epsDiluted") \
            or _safe_num(income_annual[5], "eps")
        if eps5 and eps5 > 0:
            g_hist = round(((eps0 / eps5) ** 0.2 - 1.0) * 100.0, 1)
    g_used = None
    if g_fwd is not None and g_hist is not None:
        g_used = min(g_fwd, max(g_hist, 0.0) + 10.0)   # fwd, capped near history
    elif g_fwd is not None:
        g_used = g_fwd
    elif g_hist is not None:
        g_used = g_hist
    g_real = (round(max(min(g_used - infl, 12.0), -5.0), 1)
              if g_used is not None else None)

    # partial P/E reversion toward the stock's own 10y median
    ratios_annual = raw.get("ratios_annual") \
        if isinstance(raw.get("ratios_annual"), list) else []
    pes_hist = [(_safe_num(r, "priceToEarningsRatio")
                 or _safe_num(r, "priceEarningsRatio")) for r in ratios_annual[:10]]
    pes_hist = [p for p in pes_hist if p and 0 < p < 200]
    pe_med = round(_median(pes_hist), 1) if len(pes_hist) >= 5 else None
    rev = None
    if pe_med and pe_now and pe_now > 0:
        rev = round(max(min(25.0 * math.log(pe_med / pe_now), 8.0), -8.0), 1)

    er = comp = None
    if g_real is not None and pe_now:
        parts = [dy or 0.0, bby or 0.0, infl, g_real, rev or 0.0]
        er = round(sum(parts), 1)
        comp = {"div_yield_pct": dy, "net_buyback_yield_pct": bby,
                "inflation_pct": round(infl, 2), "infl_source": infl_src,
                "real_eps_growth_pct": g_real,
                "eps_growth_fwd_nominal_pct": g_fwd,
                "eps_cagr_5y_hist_pct": g_hist,
                "pe_reversion_pct": rev, "pe_now": pe_now,
                "pe_median_10y": pe_med,
                "model": "Grinold-Kroner: DY + net buyback + inflation + "
                         "real EPS growth + 25% P/E reversion to own 10y "
                         "median (capped ±8)"}
        if infl_flag:
            comp["flag"] = infl_flag

    # ── laggard-catchup screen (asymmetry theory at stock level) ──
    screen = {"verdict": "N/A", "why": []}
    if ind_row and gaps:
        pumped = ((ind_perf.get("perf_h") or 0) >= 15.0
                  or (ind_perf.get("perf_q") or 0) >= 10.0)
        lagged = ((gaps.get("perf_h") is not None and gaps["perf_h"] <= -10.0)
                  or (gaps.get("perf_q") is not None and gaps["perf_q"] <= -8.0))
        ind_g = _safe_num(ind_row, "eps_g_n5y")
        growth_ok = (g_fwd is not None
                     and g_fwd >= max(10.0, ind_g if ind_g is not None else 0.0))
        ind_pe = _safe_num(ind_row, "pe")
        value_ok = (pe_now is not None and ind_pe is not None
                    and pe_now <= ind_pe)
        w = screen["why"]
        w.append(f"industry 6m {ind_perf.get('perf_h')}% / 3m "
                 f"{ind_perf.get('perf_q')}% ({'pumped' if pumped else 'not pumped'})")
        w.append(f"stock gap vs industry: 6m {gaps.get('perf_h')}pp / "
                 f"3m {gaps.get('perf_q')}pp ({'lagging' if lagged else 'in line'})")
        w.append(f"fwd EPS growth {g_fwd}% vs industry next-5y {ind_g}% "
                 f"({'intact' if growth_ok else 'not superior'})")
        w.append(f"P/E {pe_now} vs industry {ind_pe} "
                 f"({'cheaper' if value_ok else 'not cheaper'})")
        if pumped and lagged and growth_ok and value_ok:
            screen["verdict"] = "LAGGARD_CATCHUP"
        elif pumped and lagged and (growth_ok or value_ok):
            screen["verdict"] = "PARTIAL"
        else:
            screen["verdict"] = "NONE"
        screen["thresholds"] = {"industry_pumped": "6m≥+15% or 3m≥+10%",
                                "stock_lagged": "gap 6m≤−10pp or 3m≤−8pp",
                                "growth": "fwd EPS ≥ max(10%, industry n5y)",
                                "value": "P/E ≤ industry P/E"}

    # ── rate sensitivity vs the market-implied next-12m path ──
    dur = None
    if pe_now:
        dur = ("LONG" if pe_now > 30 else "MID" if pe_now >= 15 else "SHORT")
    rate = {"duration_bucket": dur, "earnings_yield_pct":
            (round(100.0 / pe_now, 1) if pe_now else None),
            "rate_env_next_12m": rf_dir,
            "read": None}
    if dur and rf_dir:
        if rf_dir == "HIGHER" and dur == "LONG":
            rate["read"] = ("Headwind: market prices rates higher over 12m "
                            "and this is a long-duration multiple.")
        elif rf_dir == "LOWER" and dur == "LONG":
            rate["read"] = ("Tailwind: market prices rates lower over 12m; "
                            "long-duration multiples benefit most.")
        else:
            rate["read"] = ("Modest sensitivity: multiple duration is "
                            f"{dur.lower()} against a {rf_dir.lower()} "
                            "implied rate path.")

    return {"available": True,
            "fmp_industry": fmp_ind,
            "finviz_industry": (ind_row or {}).get("name"),
            "match_confidence": conf,
            "stock_perf": sp or None,
            "industry_perf": ind_perf or None,
            "perf_gap_pp": gaps or None,
            "industry_valuation": ({k: _safe_num(ind_row, k) for k in
                                    ("pe", "fwd_pe", "peg", "ps",
                                     "eps_g_n5y", "sales_g_5y")}
                                   if ind_row else None),
            "expected_return_1y": {"er_1y_pct": er, "components": comp},
            "laggard_catchup": screen,
            "rate_sensitivity": rate,
            "note": ("Industry aggregates are official Finviz group exports; "
                     "macro anchors are market-implied from the live "
                     "asset-compass engine. No LLM used in this module.")}


def build_v2_institutional(ticker, raw, income_annual, income_quarterly, balance_annual, cashflow_annual):
    out = {}
    for name, fn in (("technicals", lambda: build_technicals(raw)),
                     ("liquidity", lambda: build_liquidity(income_annual, balance_annual,
                                                            cashflow_annual, raw.get("ratios_ttm"), out.get("technicals") or {})),
                     ("growth_vs_mcap", lambda: build_growth_vs_mcap(raw, income_annual, cashflow_annual)),
                     ("quant_risk", lambda: build_quant_risk(raw, income_annual, cashflow_annual)),
                     ("industry_compass", lambda: build_industry_compass(raw, income_annual, cashflow_annual)),
                     ("backlog", lambda: read_backlog_join(ticker))):
        try:
            out[name] = fn()
        except Exception as e:
            out[name] = {"available": False, "error": str(e)[:100]}
    return out


# ═════════════════════════════════════════════════════════════════════
# Data fetching — all FMP endpoints in parallel
# ═════════════════════════════════════════════════════════════════════

def fetch_all(ticker: str) -> Dict[str, Any]:
    """Pull all FMP data points in parallel."""
    fetches = {
        "profile":          ("profile", {"symbol": ticker}),
        "quote":            ("quote",   {"symbol": ticker}),
        "income_annual":    ("income-statement", {"symbol": ticker, "period": "annual", "limit": 20}),
        "income_quarterly": ("income-statement", {"symbol": ticker, "period": "quarter", "limit": 8}),
        "balance_annual":   ("balance-sheet-statement", {"symbol": ticker, "period": "annual", "limit": 20}),
        "cashflow_annual":  ("cash-flow-statement", {"symbol": ticker, "period": "annual", "limit": 20}),
        "ratios_annual":    ("ratios", {"symbol": ticker, "period": "annual", "limit": 15}),
        "ratios_ttm":       ("ratios-ttm", {"symbol": ticker}),
        "key_metrics":      ("key-metrics", {"symbol": ticker, "period": "annual", "limit": 15}),
        "key_metrics_ttm":  ("key-metrics-ttm", {"symbol": ticker}),
        "growth":           ("financial-growth", {"symbol": ticker, "period": "annual", "limit": 10}),
        "estimates":        ("analyst-estimates", {"symbol": ticker, "period": "annual", "limit": 5}),
        "rev_product_seg":  ("revenue-product-segmentation", {"symbol": ticker, "period": "annual"}),
        "rev_geo_seg":      ("revenue-geographic-segmentation", {"symbol": ticker, "period": "annual"}),
        "grades_consensus": ("grades-consensus", {"symbol": ticker}),
        "pt_summary":       ("price-target-summary", {"symbol": ticker}),
        "grades_actions":   ("grades", {"symbol": ticker, "limit": 12}),
        "grades_hist":      ("grades-historical", {"symbol": ticker, "limit": 14}),
        "earnings_cal":     ("earnings", {"symbol": ticker, "limit": 12}),
        "dividends_cal":    ("dividends", {"symbol": ticker, "limit": 8}),
        "pt_consensus":     ("price-target-consensus", {"symbol": ticker}),
        "dcf":              ("discounted-cash-flow", {"symbol": ticker}),
        "scores":           ("financial-scores", {"symbol": ticker}),
        "peers":            ("stock-peers", {"symbol": ticker}),
        "earnings":         ("earnings", {"symbol": ticker, "limit": 12}),
        "ownership":        ("acquisition-of-beneficial-ownership", {"symbol": ticker}),
        "transcript_dates": ("earning-call-transcript-dates", {"symbol": ticker}),
        "prices_eod":       ("historical-price-eod/light",
                              {"symbol": ticker, "from": _date_n_years_ago(10)}),
        "prices_full":      ("historical-price-eod/full",
                              {"symbol": ticker, "from": _date_n_years_ago(2)}),
        "spy_light":        ("historical-price-eod/light",
                              {"symbol": "SPY", "from": _date_n_years_ago(2)}),
        "dividends":        ("dividends", {"symbol": ticker, "limit": 20}),
    }

    results: Dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fmp_get, ep, **params): name
                   for name, (ep, params) in fetches.items()}
        for fut in as_completed(futures):
            name = futures[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                print(f"[fetch_all] {name} crashed: {e}")
                results[name] = None
    return results


def _date_n_years_ago(n: int) -> str:
    from datetime import timedelta
    return (datetime.now(timezone.utc) - timedelta(days=365 * n)).strftime("%Y-%m-%d")


# ═════════════════════════════════════════════════════════════════════
# Derived analytics
# ═════════════════════════════════════════════════════════════════════

def _safe_num(d: dict, key: str, default=None):
    """Get a numeric field, returning default if missing/None/zero-string."""
    if not isinstance(d, dict):
        return default
    v = d.get(key)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _first(maybe_list):
    """FMP often returns a list with one item; unwrap or return None."""
    if isinstance(maybe_list, list) and maybe_list:
        return maybe_list[0]
    if isinstance(maybe_list, dict):
        return maybe_list
    return None


def cagr(end_val: float, start_val: float, n_years: int) -> Optional[float]:
    if start_val is None or end_val is None or start_val <= 0 or n_years <= 0:
        return None
    try:
        return (pow(end_val / start_val, 1 / n_years) - 1) * 100
    except (ValueError, ZeroDivisionError):
        return None


def compute_growth(income_annual: list) -> dict:
    """Compute multi-period CAGRs from annual income statements."""
    if not isinstance(income_annual, list) or len(income_annual) < 2:
        return {}
    # FMP returns most-recent first
    rev_field = "revenue"
    eps_field = "epsDiluted"
    ni_field = "netIncome"

    def cagr_for(field, n):
        if len(income_annual) < n + 1:
            return None
        end = _safe_num(income_annual[0], field)
        start = _safe_num(income_annual[n], field)
        return cagr(end, start, n)

    return {
        "revenue_3yr_cagr":  cagr_for(rev_field, 3),
        "revenue_5yr_cagr":  cagr_for(rev_field, 5),
        "revenue_10yr_cagr": cagr_for(rev_field, 10),
        "eps_3yr_cagr":      cagr_for(eps_field, 3),
        "eps_5yr_cagr":      cagr_for(eps_field, 5),
        "eps_10yr_cagr":     cagr_for(eps_field, 10),
        "ni_5yr_cagr":       cagr_for(ni_field, 5),
        "ni_10yr_cagr":      cagr_for(ni_field, 10),
    }


def compute_fcf_cagr(cf_annual: list) -> dict:
    if not isinstance(cf_annual, list) or len(cf_annual) < 2:
        return {}
    def cagr_for_fcf(n):
        if len(cf_annual) < n + 1:
            return None
        end = _safe_num(cf_annual[0], "freeCashFlow")
        start = _safe_num(cf_annual[n], "freeCashFlow")
        return cagr(end, start, n)
    return {
        "fcf_3yr_cagr":  cagr_for_fcf(3),
        "fcf_5yr_cagr":  cagr_for_fcf(5),
        "fcf_10yr_cagr": cagr_for_fcf(10),
    }


def compute_margin_trend(income_annual: list, n: int = 10) -> dict:
    """Margin time-series for the last n years. /stable/ income statements don't carry
    the *Ratio fields, so compute each margin from raw revenue + profit lines."""
    if not isinstance(income_annual, list):
        return {"gross_trend": [], "operating_trend": [], "net_trend": []}
    rows = income_annual[:n]

    def margin(r, num_key):
        rev = _safe_num(r, "revenue")
        num = _safe_num(r, num_key)
        if rev and num is not None and rev != 0:
            return round(num / rev * 100, 2)
        return None

    return {
        "gross_trend":     [{"date": r.get("date"), "value": margin(r, "grossProfit")}
                              for r in rows if r.get("date")],
        "operating_trend": [{"date": r.get("date"), "value": margin(r, "operatingIncome")}
                              for r in rows if r.get("date")],
        "net_trend":       [{"date": r.get("date"), "value": margin(r, "netIncome")}
                              for r in rows if r.get("date")],
    }


def compact_price_series(prices_eod: list, max_points: int = 160) -> list:
    """Downsample the 10y EOD series to ~max_points (date, close) for the price chart."""
    if not isinstance(prices_eod, list) or not prices_eod:
        return []
    pts = []
    for p in sorted(prices_eod, key=lambda x: x.get("date") or ""):
        c = _safe_num(p, "price")
        if c is None:
            c = _safe_num(p, "close")
        d = p.get("date")
        if d and c is not None:
            pts.append({"date": d, "close": round(c, 2)})
    if len(pts) <= max_points:
        return pts
    step = len(pts) / float(max_points)
    sampled = [pts[int(i * step)] for i in range(max_points)]
    if sampled[-1]["date"] != pts[-1]["date"]:
        sampled.append(pts[-1])      # always include the latest close
    return sampled


def build_business_mix(prod_seg: list, geo_seg: list, latest_revenue) -> dict:
    """Revenue composition — by product/operating segment and by geography — plus a
    multi-year segment trend. Real FMP segmentation data (the honest 'business mix')."""
    def latest(seg):
        if not isinstance(seg, list) or not seg:
            return {}, None
        rows = sorted([s for s in seg if s.get("date")], key=lambda s: s.get("date"))
        return (rows[-1].get("data") or {}), rows[-1].get("date")

    prod_data, prod_date = latest(prod_seg)
    geo_data, geo_date = latest(geo_seg)

    seg = {k: v for k, v in prod_data.items() if isinstance(v, (int, float)) and v}
    geo = {k: v for k, v in geo_data.items() if isinstance(v, (int, float)) and v}

    # If geography only discloses a non-US figure, derive domestic from total revenue.
    if geo and latest_revenue:
        non_us = sum(v for k, v in geo.items()
                     if any(t in k.lower() for t in ("non-us", "non us", "international", "foreign", "rest of world")))
        has_domestic = any(t in k.lower() for k in geo for t in ("united states", "u.s.", "domestic", "us "))
        if non_us and not has_domestic and latest_revenue > non_us:
            geo = {"United States": round(latest_revenue - non_us), **geo}

    trend = []
    if isinstance(prod_seg, list):
        for s in sorted([x for x in prod_seg if x.get("date")], key=lambda x: x.get("date"))[-6:]:
            dd = {k: v for k, v in (s.get("data") or {}).items() if isinstance(v, (int, float)) and v}
            if dd:
                trend.append({"date": s.get("date"), "data": dd})

    return {
        "segments":         seg,
        "segments_as_of":   prod_date,
        "geography":        geo,
        "geography_as_of":  geo_date,
        "segment_trend":    trend,
    }


def build_analyst_ratings(consensus, pt_summary, actions, hist) -> dict:
    """Bloomberg-ANR-style analyst layer: live buy/hold/sell consensus, ratings trend,
    recent upgrade/downgrade actions, and price-target momentum. All from FMP."""
    c = _first(consensus) or {}
    pt = _first(pt_summary) or {}

    def n(d, k): 
        v = _safe_num(d, k)
        return int(v) if v is not None else 0

    dist = {
        "strong_buy":  n(c, "strongBuy"),
        "buy":         n(c, "buy"),
        "hold":        n(c, "hold"),
        "sell":        n(c, "sell"),
        "strong_sell": n(c, "strongSell"),
        "consensus":   c.get("consensus"),
    }
    dist["total"] = dist["strong_buy"] + dist["buy"] + dist["hold"] + dist["sell"] + dist["strong_sell"]

    pt_block = {
        "last_month_avg":   _safe_num(pt, "lastMonthAvgPriceTarget"),
        "last_month_n":     _safe_num(pt, "lastMonthCount"),
        "last_quarter_avg": _safe_num(pt, "lastQuarterAvgPriceTarget"),
        "last_quarter_n":   _safe_num(pt, "lastQuarterCount"),
        "last_year_avg":    _safe_num(pt, "lastYearAvgPriceTarget"),
        "last_year_n":      _safe_num(pt, "lastYearCount"),
        "all_time_avg":     _safe_num(pt, "allTimeAvgPriceTarget"),
    }
    # momentum direction: last-month vs last-year average target
    lm, ly = pt_block["last_month_avg"], pt_block["last_year_avg"]
    if lm and ly and ly > 0:
        pt_block["momentum_pct"] = round((lm / ly - 1) * 100, 1)

    recent = []
    if isinstance(actions, list):
        for a in actions[:10]:
            recent.append({
                "date":   a.get("date"),
                "firm":   a.get("gradingCompany"),
                "from":   a.get("previousGrade"),
                "to":     a.get("newGrade"),
                "action": a.get("action"),
            })

    trend = []
    if isinstance(hist, list):
        for h in sorted([x for x in hist if x.get("date")], key=lambda x: x.get("date"))[-12:]:
            trend.append({
                "date":        h.get("date"),
                "strong_buy":  n(h, "analystRatingsStrongBuy"),
                "buy":         n(h, "analystRatingsBuy"),
                "hold":        n(h, "analystRatingsHold"),
                "sell":        n(h, "analystRatingsSell"),
                "strong_sell": n(h, "analystRatingsStrongSell"),
            })

    return {
        "distribution":   dist,
        "pt_momentum":    pt_block,
        "recent_actions": recent,
        "ratings_trend":  trend,
    }


def _pct(x):
    """Convert FMP's ratio (0.45 = 45%) to percent."""
    if x is None: return None
    return round(x * 100, 2)


def compute_quarterly_consistency(quarterly: list) -> dict:
    """Count consecutive YoY-growth quarters + recent revenue surprises."""
    if not isinstance(quarterly, list) or len(quarterly) < 5:
        return {"consecutive_yoy_growth": 0, "recent_quarters": []}
    # Sort newest first (FMP default) — compute YoY by stepping 4 quarters
    consec = 0
    rows = quarterly[:8]
    recent = []
    for i, q in enumerate(rows):
        rev = _safe_num(q, "revenue")
        eps = _safe_num(q, "epsDiluted")
        date = q.get("date")
        yoy = None
        if len(quarterly) > i + 4:
            prev_rev = _safe_num(quarterly[i + 4], "revenue")
            if prev_rev and rev:
                yoy = round((rev / prev_rev - 1) * 100, 2)
        recent.append({"date": date, "revenue": rev, "eps_diluted": eps, "yoy_rev_growth": yoy})
        if yoy is not None and yoy > 0 and i == consec:
            consec += 1
    return {"consecutive_yoy_growth": consec, "recent_quarters": recent}


def compute_returns(prices_eod: list, current_price: Optional[float]) -> dict:
    """From EOD prices, compute YTD/1y/3y/5y returns + max drawdown."""
    if not isinstance(prices_eod, list) or len(prices_eod) < 60 or current_price is None:
        return {}
    # FMP returns descending by date. Compute returns from oldest to newest.
    prices = sorted(prices_eod, key=lambda p: p.get("date") or "")
    if not prices:
        return {}

    def price_n_days_ago(n: int) -> Optional[float]:
        if n >= len(prices):
            return _safe_num(prices[0], "price") or _safe_num(prices[0], "close")
        p = prices[-1 - n]
        return _safe_num(p, "price") or _safe_num(p, "close")

    def ret(prev: Optional[float]) -> Optional[float]:
        if prev is None or prev <= 0: return None
        return round((current_price / prev - 1) * 100, 2)

    # YTD: find first price of current year
    now_year = datetime.now(timezone.utc).year
    ytd_start = None
    for p in prices:
        d = p.get("date") or ""
        if d.startswith(str(now_year)):
            ytd_start = _safe_num(p, "price") or _safe_num(p, "close")
            break

    # Max drawdown over the 10y window
    closes = [(_safe_num(p, "price") or _safe_num(p, "close")) for p in prices]
    closes = [c for c in closes if c is not None and c > 0]
    max_dd = 0.0
    peak = 0
    if closes:
        peak = closes[0]
        for c in closes:
            if c > peak: peak = c
            dd = (peak - c) / peak * 100 if peak > 0 else 0
            if dd > max_dd: max_dd = dd

    return {
        "ytd_pct":        ret(ytd_start) if ytd_start else None,
        "1yr_pct":        ret(price_n_days_ago(252)),
        "3yr_cagr_pct":   _to_cagr(current_price, price_n_days_ago(252 * 3), 3),
        "5yr_cagr_pct":   _to_cagr(current_price, price_n_days_ago(252 * 5), 5),
        "10yr_cagr_pct":  _to_cagr(current_price, price_n_days_ago(min(252 * 10, len(prices) - 1)), 10),
        "max_drawdown_pct": round(max_dd, 2),
    }


def _to_cagr(end, start, n):
    if not start or start <= 0 or not end or end <= 0:
        return None
    try:
        return round((pow(end / start, 1 / n) - 1) * 100, 2)
    except (ValueError, ZeroDivisionError):
        return None


def compute_balance_quality(balance_annual: list) -> dict:
    """Working capital, current ratio, debt/equity trend."""
    if not isinstance(balance_annual, list) or not balance_annual:
        return {}
    latest = balance_annual[0]
    debt = _safe_num(latest, "totalDebt", 0) or 0
    eq   = _safe_num(latest, "totalEquity", 0) or _safe_num(latest, "totalStockholdersEquity", 0) or 0
    ca   = _safe_num(latest, "totalCurrentAssets")
    cl   = _safe_num(latest, "totalCurrentLiabilities")
    return {
        "total_debt":            debt,
        "total_equity":          eq,
        "debt_to_equity":        round(debt / eq, 2) if eq else None,
        "current_ratio":         round(ca / cl, 2) if (ca and cl) else None,
        "working_capital":       round((ca or 0) - (cl or 0), 0) if (ca and cl) else None,
        "cash_and_st_inv":       _safe_num(latest, "cashAndShortTermInvestments"),
    }


def compute_cf_quality(income_annual: list, cf_annual: list) -> dict:
    """CFO/NI ratio (cash quality) and FCF conversion."""
    if not (isinstance(income_annual, list) and isinstance(cf_annual, list)
            and income_annual and cf_annual):
        return {}
    latest_ni  = _safe_num(income_annual[0], "netIncome")
    latest_cfo = _safe_num(cf_annual[0], "operatingCashFlow") or _safe_num(cf_annual[0], "netCashProvidedByOperatingActivities")
    latest_fcf = _safe_num(cf_annual[0], "freeCashFlow")
    return {
        "cfo_to_ni":            round(latest_cfo / latest_ni, 2) if (latest_cfo and latest_ni) else None,
        "fcf_conversion_pct":   round(latest_fcf / latest_ni * 100, 1) if (latest_fcf and latest_ni) else None,
        "latest_cfo":           latest_cfo,
        "latest_fcf":           latest_fcf,
    }


def compute_valuation(profile: dict, ratios_ttm: dict, key_ttm: dict,
                       ratios_annual: list, dcf: dict, pt_consensus: dict,
                       quote: dict) -> dict:
    """Pull all the valuation metrics into one section."""
    # FMP /stable/ratios-ttm field names (verified by ops 1139):
    #   priceToEarningsRatioTTM, priceToBookRatioTTM, priceToSalesRatioTTM,
    #   priceToFreeCashFlowRatioTTM (NO 's'), enterpriseValueMultipleTTM,
    #   operatingProfitMarginTTM. priceEarningsToGrowthRatioTTM doesn't exist —
    #   it's priceToEarningsGrowthRatioTTM.
    # ROE/ROIC live in /stable/key-metrics-ttm not ratios-ttm.
    pe_ttm     = (_safe_num(ratios_ttm, "priceToEarningsRatioTTM")
                  or _safe_num(ratios_ttm, "peRatioTTM"))
    pb_ttm     = _safe_num(ratios_ttm, "priceToBookRatioTTM")
    ps_ttm     = _safe_num(ratios_ttm, "priceToSalesRatioTTM")
    pfcf_ttm   = (_safe_num(ratios_ttm, "priceToFreeCashFlowRatioTTM")
                  or _safe_num(ratios_ttm, "priceToFreeCashFlowsRatioTTM"))
    ev_ebitda  = (_safe_num(ratios_ttm, "enterpriseValueMultipleTTM")
                  or _safe_num(key_ttm,  "evToEBITDATTM")
                  or _safe_num(key_ttm,  "enterpriseValueOverEBITDATTM"))
    fcf_yield  = _safe_num(key_ttm, "freeCashFlowYieldTTM")
    div_yield  = (_safe_num(ratios_ttm, "dividendYieldTTM")
                  or _safe_num(ratios_ttm, "dividendYieldPercentageTTM")
                  or _safe_num(key_ttm, "dividendYieldTTM"))
    peg        = (_safe_num(ratios_ttm, "priceToEarningsGrowthRatioTTM")
                  or _safe_num(ratios_ttm, "priceEarningsToGrowthRatioTTM"))
    roe        = _safe_num(key_ttm, "returnOnEquityTTM")
    roic       = _safe_num(key_ttm, "returnOnInvestedCapitalTTM")

    # 5yr avg PE from annual ratios
    pe_5yr = None
    if isinstance(ratios_annual, list) and ratios_annual:
        pes = [_safe_num(r, "priceToEarningsRatio") or _safe_num(r, "priceEarningsRatio")
               for r in ratios_annual[:5]]
        pes = [p for p in pes if p is not None and 0 < p < 200]
        if pes:
            pe_5yr = round(sum(pes) / len(pes), 1)

    # DCF
    dcf_obj = _first(dcf) or {}
    dcf_val = _safe_num(dcf_obj, "dcf") or _safe_num(dcf_obj, "Dcf")
    current_px = _safe_num(quote, "price") or _safe_num(profile, "price")
    dcf_upside = None
    if dcf_val and current_px and current_px > 0:
        dcf_upside = round((dcf_val / current_px - 1) * 100, 1)

    # Analyst PT
    pt_obj = _first(pt_consensus) or {}
    pt_median = _safe_num(pt_obj, "targetMedian") or _safe_num(pt_obj, "targetConsensus")
    pt_high   = _safe_num(pt_obj, "targetHigh")
    pt_low    = _safe_num(pt_obj, "targetLow")
    pt_upside = None
    if pt_median and current_px and current_px > 0:
        pt_upside = round((pt_median / current_px - 1) * 100, 1)

    return {
        "pe_ttm":            round(pe_ttm, 2) if pe_ttm else None,
        "pe_5yr_avg":        pe_5yr,
        "pb_ttm":            round(pb_ttm, 2) if pb_ttm else None,
        "ps_ttm":            round(ps_ttm, 2) if ps_ttm else None,
        "pfcf_ttm":          round(pfcf_ttm, 2) if pfcf_ttm else None,
        "ev_ebitda":         round(ev_ebitda, 2) if ev_ebitda else None,
        "peg_ratio":         round(peg, 2) if peg else None,
        "fcf_yield_pct":     round(fcf_yield * 100, 2) if fcf_yield else None,
        "div_yield_pct":     round(div_yield * 100, 2) if div_yield else None,
        "roe_ttm_pct":       _pct(roe),
        "roic_ttm_pct":      _pct(roic),
        "dcf_estimate":      round(dcf_val, 2) if dcf_val else None,
        "dcf_upside_pct":    dcf_upside,
        "analyst_pt_median": pt_median,
        "analyst_pt_high":   pt_high,
        "analyst_pt_low":    pt_low,
        "analyst_pt_upside_pct": pt_upside,
    }


def fetch_industry_benchmarks(industry, sector):
    """Real industry & sector P/E from FMP's daily snapshot — the true 'vs industry'
    benchmark (averaged across exchanges), not just a 5-peer median. Walks back up to a
    week to land on a trading day that has data."""
    import datetime
    out = {"industry": industry, "sector": sector, "industry_pe": None,
           "sector_pe": None, "as_of": None}

    def _avg_pe(rows, field, name):
        if not isinstance(rows, list) or not name:
            return None
        pes = [_safe_num(r, "pe") for r in rows
               if str(r.get(field, "")).strip().lower() == str(name).strip().lower()]
        pes = [p for p in pes if p and 0 < p < 250]      # drop nulls + absurd outliers
        return round(sum(pes) / len(pes), 1) if pes else None

    for back in range(0, 7):
        d = (datetime.date.today() - datetime.timedelta(days=back)).isoformat()
        ind = fmp_get("industry-pe-snapshot", date=d)
        if isinstance(ind, list) and ind:
            out["industry_pe"] = _avg_pe(ind, "industry", industry)
            out["sector_pe"] = _avg_pe(fmp_get("sector-pe-snapshot", date=d), "sector", sector)
            out["as_of"] = d
            break
    return out


def compute_financial_health(scores: list, ratios_ttm: dict, key_ttm: dict,
                              balance_qual: dict, cf_qual: dict,
                              growth: dict) -> dict:
    """5-pillar health score: profitability/growth/leverage/liquidity/quality."""
    scores_obj = _first(scores) or {}

    altman_z   = _safe_num(scores_obj, "altmanZScore")
    piotroski  = _safe_num(scores_obj, "piotroskiScore")

    # Pillar grades (each 0-100)
    pillars: Dict[str, Any] = {}

    # 1. Profitability — ROE > 15, margin > 10
    # ROE lives in key-metrics-ttm not ratios-ttm (FMP).
    roe = _safe_num(key_ttm, "returnOnEquityTTM") or _safe_num(ratios_ttm, "returnOnEquityTTM") or 0
    op_margin = _safe_num(ratios_ttm, "operatingProfitMarginTTM") or 0
    prof_score = min(100, (roe * 100 * 3 + op_margin * 100 * 3) / 2)  # cap at 100
    pillars["profitability"] = {
        "score": round(max(0, min(100, prof_score)), 0),
        "roe_pct":        _pct(roe),
        "op_margin_pct":  _pct(op_margin),
    }

    # 2. Growth — 5yr revenue + EPS CAGR. Score = (rev_cagr + eps_cagr) * 4
    rev5 = growth.get("revenue_5yr_cagr") or 0
    eps5 = growth.get("eps_5yr_cagr") or 0
    growth_score = (rev5 + eps5) * 3
    pillars["growth"] = {
        "score": round(max(0, min(100, growth_score)), 0),
        "rev_5y_cagr_pct": rev5,
        "eps_5y_cagr_pct": eps5,
    }

    # 3. Leverage — debt/equity. Score = 100 - 30 * d/e (1.0 = 70, 2.0 = 40)
    de = balance_qual.get("debt_to_equity")
    leverage_score = 100 - 30 * (de or 0) if de is not None else 50
    pillars["leverage"] = {
        "score": round(max(0, min(100, leverage_score)), 0),
        "debt_to_equity": de,
        "altman_z_score": altman_z,
    }

    # 4. Liquidity — current ratio
    cr = balance_qual.get("current_ratio")
    liquidity_score = 50 + (cr or 1) * 25 if cr is not None else 50
    pillars["liquidity"] = {
        "score": round(max(0, min(100, liquidity_score)), 0),
        "current_ratio": cr,
        "working_capital": balance_qual.get("working_capital"),
    }

    # 5. Quality — CFO/NI, Piotroski. Score = piotroski * 10 + 20 if cfo/ni > 1
    cfo_ni = cf_qual.get("cfo_to_ni")
    quality_score = ((piotroski or 5) * 10) + (20 if (cfo_ni or 0) > 1 else 0)
    pillars["quality"] = {
        "score": round(max(0, min(100, quality_score)), 0),
        "piotroski_score": piotroski,
        "cfo_to_ni":  cfo_ni,
    }

    overall = sum(p["score"] for p in pillars.values()) / 5
    return {
        "pillars":     pillars,
        "overall_score": round(overall, 0),
        "altman_z":    altman_z,
        "piotroski":   piotroski,
    }


def fetch_latest_transcript(ticker: str, transcript_dates: list) -> Optional[dict]:
    """Fetch the most recent earnings call transcript.

    /stable/earning-call-transcript-dates gives us a list of (date,
    fiscalYear, quarter) tuples. We pick the most recent and fetch
    /stable/earning-call-transcript?symbol=X&year=Y&quarter=Q to get
    the actual call content.

    Returns: {date, year, quarter, content_truncated, content_full_chars}
             or None if no transcripts.
    """
    if not isinstance(transcript_dates, list) or not transcript_dates:
        return None
    # Most-recent first by date string
    sorted_dates = sorted(transcript_dates,
                            key=lambda d: d.get("date") or "",
                            reverse=True)
    latest_meta = sorted_dates[0]
    year = latest_meta.get("fiscalYear")
    quarter = latest_meta.get("quarter")
    if not year or not quarter:
        return None

    # Fetch the actual call
    r = fmp_get("earning-call-transcript", symbol=ticker,
                  year=year, quarter=quarter)
    transcript = _first(r) or {}
    content = transcript.get("content") or ""
    if not content:
        return None

    # Transcripts can be 50K-150K chars. We need to truncate intelligently
    # for the Claude payload. Hedge fund analysts care most about:
    #   - The intro / prepared remarks (CEO + CFO outlook)
    #   - The Q&A (where analysts probe weaknesses)
    # Strategy: take the first 8000 chars (prepared remarks) + last
    # 8000 chars (final Q&A often has the most pointed exchanges).
    full_len = len(content)
    if full_len <= 16000:
        truncated = content
    else:
        truncated = (content[:8000] +
                       "\n\n…[middle of call omitted for brevity]…\n\n" +
                       content[-8000:])

    return {
        "date":               latest_meta.get("date"),
        "fiscal_year":        year,
        "quarter":            quarter,
        "full_chars":         full_len,
        "truncated_chars":    len(truncated),
        "content_truncated":  truncated,
    }


def compute_institutional_activity(ownership_filings: list) -> dict:
    """SEC 13D/13G beneficial ownership filings analysis.

    13D = activist filing (intent to influence). 13G = passive >5% holder.
    The pattern matters more than the count: a cluster of recent filings
    from blue-chip institutions (Vanguard, Blackrock, Berkshire,
    Wellington) suggests crossing-the-threshold accumulation.

    NB: This is NOT insider trading (Form 4) — FMP doesn't expose Form 4
    on the current plan. 13D/13G data is the closest proxy: institutional
    'smart money' position changes that cross 5% reporting threshold.
    """
    if not isinstance(ownership_filings, list) or not ownership_filings:
        return {}

    # FMP returns recent filings; some may be 5+ years old. Filter to last 24 months.
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(days=730)).strftime("%Y-%m-%d")
    recent = [f for f in ownership_filings if (f.get("filingDate") or "") >= cutoff]
    # Sort newest first
    recent.sort(key=lambda f: f.get("filingDate") or "", reverse=True)

    # Top 8 most-recent filings for display
    recent_top = recent[:8]
    filings_display = []
    for f in recent_top:
        filings_display.append({
            "filing_date":          f.get("filingDate"),
            "filer":                f.get("nameOfReportingPerson"),
            "shares_owned":         _safe_num(f, "amountBeneficiallyOwned"),
            "pct_of_class":         _safe_num(f, "percentOfClass"),
            "filer_jurisdiction":   f.get("citizenshipOrPlaceOfOrganization"),
            "filer_type":           f.get("typeOfReportingPerson"),
            "url":                  f.get("url"),
        })

    # Aggregate
    unique_filers = set(f.get("nameOfReportingPerson") for f in recent if f.get("nameOfReportingPerson"))
    total_pct = sum(_safe_num(f, "percentOfClass") or 0 for f in recent)

    return {
        "n_filings_total":      len(ownership_filings),
        "n_filings_recent_24m": len(recent),
        "n_unique_filers_24m":  len(unique_filers),
        "filings_display":      filings_display,
        "summary_note":         "13D/13G filings show institutional positions crossing the 5% reporting threshold. Not insider Form 4 (which FMP doesn't expose on current plan). This is institutional 'smart money' accumulation/divestment.",
    }


def compute_capital_allocation(cf_annual: list, income_annual: list,
                                 quote: dict) -> dict:
    """Institutional capital allocation analysis.

    Hedge funds care about:
      - Total capital returned to shareholders (divs + buybacks) over time
      - Payout ratio (capital returned / net income) — sustainability check
      - Shareholder yield (capital returned / market cap) — total return floor
      - Buyback yield vs dividend yield (cash distribution mix)
      - Capex / revenue trend (capital intensity, business model signal)
      - 'Cash-cow vs capex-heavy' framing
    """
    if not (isinstance(cf_annual, list) and isinstance(income_annual, list)
            and cf_annual and income_annual):
        return {}
    market_cap = _safe_num(quote, "marketCap") or 0

    # Per-year detail (last 10y)
    timeline = []
    for i, cf in enumerate(cf_annual[:10]):
        date = cf.get("date") or ""
        ni_row = None
        # Match income statement by date
        for inc in income_annual:
            if inc.get("date") == date:
                ni_row = inc
                break
        net_income = _safe_num(ni_row, "netIncome") if ni_row else None

        # FMP stores dividendsPaid and commonStockRepurchased as NEGATIVE
        # (cash outflows). Convert to positive for reader clarity.
        # FMP /stable/cash-flow-statement field naming (verified ops 1143):
        #   commonDividendsPaid   ← real dividend cash outflow (preferred)
        #   netDividendsPaid      ← same value, alternate field
        #   dividendsPaid         ← LEGACY name, not in current /stable/ response
        # Try them in order; legacy as final fallback if FMP ever flips back.
        divs_raw = (_safe_num(cf, "commonDividendsPaid")
                    or _safe_num(cf, "netDividendsPaid")
                    or _safe_num(cf, "dividendsPaid"))
        bb_raw   = _safe_num(cf, "commonStockRepurchased")
        capex_raw = _safe_num(cf, "capitalExpenditure")

        divs_paid    = abs(divs_raw) if divs_raw is not None else None
        buybacks     = abs(bb_raw)   if bb_raw is not None else None
        capex        = abs(capex_raw) if capex_raw is not None else None
        capital_returned = (divs_paid or 0) + (buybacks or 0)

        payout_ratio = None
        if net_income and net_income > 0 and capital_returned > 0:
            payout_ratio = round(capital_returned / net_income * 100, 1)

        fcf = _safe_num(cf, "freeCashFlow")
        fcf_payout = None
        if fcf and fcf > 0 and capital_returned > 0:
            fcf_payout = round(capital_returned / fcf * 100, 1)

        # Get revenue for capex/revenue trend
        revenue = _safe_num(ni_row, "revenue") if ni_row else None
        capex_to_rev = round(capex / revenue * 100, 2) if (capex and revenue and revenue > 0) else None

        timeline.append({
            "year":              date[:4] if date else None,
            "net_income":        net_income,
            "free_cash_flow":    fcf,
            "dividends_paid":    divs_paid,
            "buybacks":          buybacks,
            "capex":             capex,
            "revenue":           revenue,
            "capital_returned":  capital_returned if capital_returned > 0 else None,
            "payout_ratio_pct":  payout_ratio,
            "fcf_payout_pct":    fcf_payout,
            "capex_to_revenue_pct": capex_to_rev,
        })

    # ── Aggregates (rolling 10y where available)
    def sum_field(name, n=10):
        vals = [t.get(name) for t in timeline[:n]]
        clean = [v for v in vals if isinstance(v, (int, float))]
        return sum(clean) if clean else None

    total_divs_10y     = sum_field("dividends_paid")
    total_buybacks_10y = sum_field("buybacks")
    total_capex_10y    = sum_field("capex")
    total_returned_10y = (total_divs_10y or 0) + (total_buybacks_10y or 0)

    # ── Shareholder yield = recent annualized capital return / mkt cap
    latest_return = timeline[0].get("capital_returned") if timeline else None
    shareholder_yield_pct = None
    if latest_return and market_cap > 0:
        shareholder_yield_pct = round(latest_return / market_cap * 100, 2)

    # Distribution mix (latest year): what fraction is buybacks vs divs
    buyback_share_pct = None
    if timeline and timeline[0].get("capital_returned"):
        latest = timeline[0]
        if latest.get("buybacks") is not None:
            buyback_share_pct = round((latest["buybacks"] or 0) / latest["capital_returned"] * 100, 1)

    # Capital intensity trend (capex/rev)
    capex_trend = [t.get("capex_to_revenue_pct") for t in timeline if t.get("capex_to_revenue_pct") is not None]
    capex_recent_avg = round(sum(capex_trend[:3]) / len(capex_trend[:3]), 2) if capex_trend[:3] else None
    capex_older_avg  = round(sum(capex_trend[5:8]) / len(capex_trend[5:8]), 2) if len(capex_trend) >= 8 else None

    capex_intensity_trend = None
    if capex_recent_avg is not None and capex_older_avg is not None and capex_older_avg > 0:
        change = (capex_recent_avg - capex_older_avg) / capex_older_avg
        if change > 0.2:    capex_intensity_trend = "rising"
        elif change < -0.2: capex_intensity_trend = "falling"
        else:               capex_intensity_trend = "stable"

    return {
        "timeline":               timeline,
        "total_dividends_10y":    total_divs_10y,
        "total_buybacks_10y":     total_buybacks_10y,
        "total_capex_10y":        total_capex_10y,
        "total_returned_10y":     total_returned_10y if total_returned_10y > 0 else None,
        "shareholder_yield_pct":  shareholder_yield_pct,
        "buyback_share_of_return_pct": buyback_share_pct,
        "latest_payout_ratio_pct": timeline[0].get("payout_ratio_pct") if timeline else None,
        "latest_fcf_payout_pct":  timeline[0].get("fcf_payout_pct") if timeline else None,
        "capex_to_revenue_recent_avg": capex_recent_avg,
        "capex_to_revenue_older_avg":  capex_older_avg,
        "capex_intensity_trend":  capex_intensity_trend,
    }


def build_earnings_vol_edge(prices_eod, earnings_rows, options_block, next_earnings_date):
    """#6 EARNINGS-VOL EDGE (delta on top of live implied-move block).
    Realized earnings reaction (bracket max of BMO/AMC gap) over last <=8 prints vs
    options-implied move -> RICH/CHEAP/FAIR vol read + PEAD drift T+1/T+5/T+20 by beat/miss.
    All real market data: FMP EOD closes x FMP /stable/earnings dates; nothing fabricated."""
    import datetime as _dt, bisect
    try:
        prices = prices_eod.get("historical") if isinstance(prices_eod, dict) else prices_eod
        rows = []
        for p in (prices or []):
            d = p.get("date"); c = _safe_num(p, "price") or _safe_num(p, "close")
            if d and c and c > 0:
                rows.append((d[:10], c))
        rows.sort()
        dates = [r[0] for r in rows]; closes = [r[1] for r in rows]
        if len(rows) < 60 or not isinstance(earnings_rows, list):
            return {"status": "insufficient_history"}
        today = _dt.date.today().isoformat()
        def _sp(er):
            a = _safe_num(er, "epsActual"); e = _safe_num(er, "epsEstimated")
            if a is None or e is None or abs(e) < 1e-9: return None
            return round((a - e) / abs(e) * 100.0, 1)
        past = sorted([er for er in earnings_rows if er.get("date") and er["date"][:10] < today
                       and _safe_num(er, "epsActual") is not None],
                      key=lambda er: er["date"], reverse=True)[:8]
        prints = []
        for er in past:
            ed = er["date"][:10]
            j = bisect.bisect_left(dates, ed)
            i_pre = j - 1
            i_ed  = j if j < len(dates) and dates[j] == ed else None
            i_post = (i_ed + 1) if i_ed is not None else j
            if i_pre < 0 or i_post >= len(dates): continue
            cands = []
            if i_ed is not None:
                cands.append((abs(closes[i_ed] / closes[i_pre] - 1.0), i_ed))
                cands.append((abs(closes[i_post] / closes[i_ed] - 1.0), i_post))
            else:
                cands.append((abs(closes[i_post] / closes[i_pre] - 1.0), i_post))
            mv, t0 = max(cands)
            def _dr(k):
                return round((closes[t0 + k] / closes[t0] - 1.0) * 100.0, 1) if t0 + k < len(closes) else None
            sp = _sp(er)
            bucket = "beat" if (sp is not None and sp > 0.5) else ("miss" if (sp is not None and sp < -0.5) else "inline")
            prints.append({"date": ed, "eps_surprise_pct": sp, "bucket": bucket,
                           "reaction_move_pct": round(mv * 100.0, 1),
                           "t1_pct": _dr(1), "t5_pct": _dr(5), "t20_pct": _dr(20)})
        if len(prints) < 4:
            return {"status": "insufficient_history", "prints_used": len(prints)}
        moves = sorted(p["reaction_move_pct"] for p in prints)
        n = len(moves)
        med = round((moves[n // 2] if n % 2 else (moves[n // 2 - 1] + moves[n // 2]) / 2.0), 1)
        implied = options_block.get("implied_move_pct") if isinstance(options_block, dict) else None
        verdict = ratio = None
        if implied is not None and med and med > 0:
            ratio = round(float(implied) / med, 2)
            verdict = "RICH" if ratio >= 1.25 else ("CHEAP" if ratio <= 0.80 else "FAIR")
        pead = {}
        for b in ("beat", "miss", "inline"):
            bp = [p for p in prints if p["bucket"] == b]
            def _avg(k):
                v = [p[k] for p in bp if p[k] is not None]
                return round(sum(v) / len(v), 1) if v else None
            pead[b] = {"n": len(bp), "t1_pct": _avg("t1_pct"), "t5_pct": _avg("t5_pct"), "t20_pct": _avg("t20_pct")}
        d2e = None
        if next_earnings_date:
            try: d2e = (_dt.date.fromisoformat(next_earnings_date[:10]) - _dt.date.today()).days
            except Exception: d2e = None
        return {"status": "ok", "next_earnings": next_earnings_date, "days_to_earnings": d2e,
                "implied_move_pct": implied, "median_realized_move_pct": med,
                "implied_vs_realized_ratio": ratio, "vol_verdict": verdict,
                "prints_used": len(prints), "prints": prints, "pead": pead,
                "method": "reaction = max(BMO gap, AMC gap) around FMP report date; PEAD drift from reaction close; beat/miss = +/-0.5% EPS surprise"}
    except Exception as _e:
        print(f"[evx] build failed: {type(_e).__name__}: {str(_e)[:120]}")
        return {"status": "error"}


def compute_earnings_track_record(earnings_rows: list) -> dict:
    """Institutional-style earnings beat/miss analysis.

    Hedge fund framing: a stock that beats consensus 7 of 8 quarters is a
    'high-quality compounder'; one that beats by SHRINKING magnitude is
    showing deteriorating fundamentals even if the beats continue.

    Returns:
      - eps_beats / eps_misses / eps_inline counts
      - eps_beat_rate (% of quarters with epsActual >= epsEstimated)
      - eps_avg_beat_pct (mean (actual-est)/est across beat quarters)
      - eps_avg_miss_pct (mean across miss quarters, negative number)
      - eps_current_streak  (consecutive beats, negative = miss streak)
      - eps_magnitude_trend ('expanding' | 'stable' | 'shrinking' | None)
      - revenue_beat_rate, revenue_avg_surprise_pct (same for revenue)
      - quarters: detailed list of past quarters
    """
    if not isinstance(earnings_rows, list):
        return {}
    # Filter to past quarters only (actual numbers reported, not forward)
    past = [r for r in earnings_rows
              if isinstance(r, dict)
              and r.get("epsActual") is not None
              and r.get("epsEstimated") is not None]
    if not past:
        return {}
    # Newest first (FMP default), confirm by sorting
    past = sorted(past, key=lambda r: r.get("date") or "", reverse=True)[:12]

    def surprise_pct(actual, est):
        try:
            if est is None or est == 0: return None
            return round((float(actual) - float(est)) / abs(float(est)) * 100, 2)
        except (TypeError, ValueError):
            return None

    # Detailed quarter list
    quarters = []
    for r in past:
        eps_act, eps_est = r.get("epsActual"), r.get("epsEstimated")
        rev_act, rev_est = r.get("revenueActual"), r.get("revenueEstimated")
        quarters.append({
            "date":              r.get("date"),
            "eps_estimated":     eps_est,
            "eps_actual":        eps_act,
            "eps_surprise_pct":  surprise_pct(eps_act, eps_est),
            "revenue_estimated": rev_est,
            "revenue_actual":    rev_act,
            "revenue_surprise_pct": surprise_pct(rev_act, rev_est),
        })

    # EPS beat/miss aggregates
    eps_surprises = [q["eps_surprise_pct"] for q in quarters if q["eps_surprise_pct"] is not None]
    eps_beats = [s for s in eps_surprises if s > 0.5]   # >0.5% counts as a beat
    eps_misses = [s for s in eps_surprises if s < -0.5]
    eps_inline = [s for s in eps_surprises if -0.5 <= s <= 0.5]

    # Current streak: count from most recent quarter, sign = direction
    streak = 0
    if quarters and quarters[0]["eps_surprise_pct"] is not None:
        direction = 1 if quarters[0]["eps_surprise_pct"] > 0.5 else (-1 if quarters[0]["eps_surprise_pct"] < -0.5 else 0)
        if direction != 0:
            for q in quarters:
                s = q["eps_surprise_pct"]
                if s is None: break
                if direction > 0 and s > 0.5: streak += 1
                elif direction < 0 and s < -0.5: streak += 1
                else: break
            streak *= direction

    # Magnitude trend: compare first half avg beat vs second half
    magnitude_trend = None
    if len(eps_beats) >= 4 and len(quarters) >= 6:
        recent_beats = [q["eps_surprise_pct"] for q in quarters[:4]
                         if q["eps_surprise_pct"] is not None and q["eps_surprise_pct"] > 0.5]
        older_beats = [q["eps_surprise_pct"] for q in quarters[4:8]
                        if q["eps_surprise_pct"] is not None and q["eps_surprise_pct"] > 0.5]
        if recent_beats and older_beats:
            recent_avg = sum(recent_beats) / len(recent_beats)
            older_avg = sum(older_beats) / len(older_beats)
            if recent_avg > older_avg * 1.2:   magnitude_trend = "expanding"
            elif recent_avg < older_avg * 0.7: magnitude_trend = "shrinking"
            else: magnitude_trend = "stable"

    # Revenue surprise aggregates
    rev_surprises = [q["revenue_surprise_pct"] for q in quarters
                       if q["revenue_surprise_pct"] is not None]
    rev_beats = [s for s in rev_surprises if s > 0.5]
    rev_misses = [s for s in rev_surprises if s < -0.5]

    def avg(lst): return round(sum(lst) / len(lst), 2) if lst else None

    return {
        "n_quarters":              len(quarters),
        "eps_beats":               len(eps_beats),
        "eps_misses":              len(eps_misses),
        "eps_inline":              len(eps_inline),
        "eps_beat_rate_pct":       round(len(eps_beats) / max(1, len(eps_surprises)) * 100, 1),
        "eps_avg_beat_pct":        avg(eps_beats),
        "eps_avg_miss_pct":        avg(eps_misses),
        "eps_current_streak":      streak,
        "eps_magnitude_trend":     magnitude_trend,
        "revenue_beats":           len(rev_beats),
        "revenue_misses":          len(rev_misses),
        "revenue_beat_rate_pct":   round(len(rev_beats) / max(1, len(rev_surprises)) * 100, 1)
                                       if rev_surprises else None,
        "revenue_avg_surprise_pct": avg(rev_surprises),
        "quarters":                quarters,
    }


def build_peer_comparison(subject_ticker: str, subject_ratios_ttm: dict,
                            subject_key_ttm: dict, subject_quote: dict,
                            subject_company: dict, peers_list: list,
                            peer_details: dict) -> dict:
    """Build a side-by-side comparison table: subject + peers with key valuation
    metrics, plus peer-median summary stats. The peer median functions as the
    'industry P/E' benchmark hedge fund analysts compare against."""
    import statistics as _stats

    def make_row(sym, name, price, mkt_cap, ratios, key_metrics, is_subject=False):
        # ratios = /stable/ratios-ttm response.  Provides PE/PB/PS/EV multiple / op margin.
        # key_metrics = /stable/key-metrics-ttm response.  Provides ROE / ROIC / EV/EBITDA.
        # FMP field naming verified by ops 1139.
        return {
            "symbol":      sym,
            "name":        name,
            "price":       price,
            "market_cap":  mkt_cap,
            "pe":          (_safe_num(ratios, "priceToEarningsRatioTTM")
                              or _safe_num(ratios, "peRatioTTM")),
            "pb":          _safe_num(ratios, "priceToBookRatioTTM"),
            "ps":          _safe_num(ratios, "priceToSalesRatioTTM"),
            "ev_ebitda":   (_safe_num(ratios, "enterpriseValueMultipleTTM")
                              or _safe_num(key_metrics, "evToEBITDATTM")
                              or _safe_num(key_metrics, "enterpriseValueOverEBITDATTM")),
            "roe_pct":     _pct(_safe_num(key_metrics, "returnOnEquityTTM")),
            "op_margin_pct": _pct(_safe_num(ratios, "operatingProfitMarginTTM")),
            "is_subject":  is_subject,
        }

    rows = [
        make_row(subject_ticker, subject_company.get("name") or subject_ticker,
                  _safe_num(subject_quote, "price"),
                  _safe_num(subject_quote, "marketCap") or subject_company.get("market_cap"),
                  subject_ratios_ttm, subject_key_ttm, is_subject=True)
    ]
    for peer in peers_list[:5]:
        sym = peer.get("symbol")
        if not sym: continue
        detail = peer_details.get(sym, {})
        rows.append(make_row(
            sym, peer.get("companyName") or sym,
            peer.get("price"), peer.get("mktCap"),
            detail.get("ratios", {}), detail.get("key_metrics", {}),
        ))

    def median(vals):
        clean = [v for v in vals if isinstance(v, (int, float)) and v == v]  # filter NaN
        # Also filter ridiculous outliers (P/E > 500 = junk)
        clean = [v for v in clean if -500 < v < 500]
        return round(_stats.median(clean), 2) if clean else None

    peer_rows = rows[1:]
    summary = {
        "n_peers":           len(peer_rows),
        "median_pe":         median([r["pe"] for r in peer_rows]),
        "median_pb":         median([r["pb"] for r in peer_rows]),
        "median_ps":         median([r["ps"] for r in peer_rows]),
        "median_ev_ebitda":  median([r["ev_ebitda"] for r in peer_rows]),
        "median_roe_pct":    median([r["roe_pct"] for r in peer_rows]),
        "median_op_margin_pct": median([r["op_margin_pct"] for r in peer_rows]),
    }

    # Subject's premium / discount vs peer median (negative = trading at discount)
    subject = rows[0]
    relative: Dict[str, Any] = {}
    for key in ("pe", "pb", "ps", "ev_ebitda"):
        sub = subject.get(key)
        med = summary.get(f"median_{key}")
        if isinstance(sub, (int, float)) and isinstance(med, (int, float)) and med > 0:
            relative[f"premium_pct_{key}"] = round((sub / med - 1) * 100, 1)

    return {
        "sector":     subject_company.get("sector"),
        "industry":   subject_company.get("industry"),
        "rows":       rows,
        "summary":    summary,
        "relative":   relative,
    }


# ═════════════════════════════════════════════════════════════════════
# Claude synthesis
# ═════════════════════════════════════════════════════════════════════

CLAUDE_SYSTEM = """You are a senior equity research analyst at a top hedge fund.
You write the kind of research memo a portfolio manager would read before making a multi-million dollar
position decision. Your output is structured, opinionated, and rooted ENTIRELY in the data provided.

NEVER invent numbers. If a metric is missing, state that. Use specific data points.
Be DIRECTIONAL — equivocation is for losers. Tell the PM whether to buy, hold, or sell.

OUTPUT JSON ONLY, no markdown, no preamble.

═══════════════════════════════════════════════════════════════════════════
ANALYTICAL FRAMEWORK
═══════════════════════════════════════════════════════════════════════════

You analyze through five lenses, weighted by what the data supports. Never assert
without evidence — every claim cites a number.

1. GROWTH QUALITY (not just speed)
   - Revenue 5y CAGR matters less than its consistency and accelerating/decelerating trend
   - Watch the spread between revenue growth and FCF growth — if FCF lags revenue by 5+ppts
     over multiple years, growth is being bought, not earned
   - Decelerating growth into margin expansion = mature franchise (good for steady eddies)
   - Accelerating growth at the expense of margins = land grab (good only if TAM is real)
   - 3-yr trailing rev CAGR > 5-yr → accelerating. Below 50% of 5-yr → sharp deceleration

2. CAPITAL EFFICIENCY (the metric that separates compounders from melt-ups)
   - ROIC > WACC by 500+ bps = real value creation; spread <200 bps = mediocre
   - ROIC trajectory matters as much as level — rising ROIC in a mature business is rare and prized
   - High ROIC + low reinvestment opportunity = capital return story (think CL, KO, PEP)
   - High ROIC + ample reinvestment = compounder (think MSFT 2014-2024, V, MA)
   - Falling ROIC is a yellow flag even if levels look fine — it precedes multiple compression

3. BALANCE SHEET POSTURE
   - Net debt / EBITDA matters in context of revenue volatility: a cyclical at 2.5x is risky;
     a SaaS subscription model at 2.5x is fine
   - Interest coverage ratio (EBIT / Interest) < 5x = real risk if rates stay elevated
   - Cash conversion (FCF/Net Income) below 80% over multiple years = earnings quality issue
   - Watch for stock-comp-as-percent-of-revenue creep — common at growth-tech, dilutes shareholders silently

4. VALUATION RELATIVE TO TRAJECTORY
   - P/E vs 5-yr historical avg matters more than absolute level
   - Quality compounders DESERVE premium multiples — paying 35x for a 20% grower with 30% ROIC
     can be cheaper than paying 12x for a 5% grower with 10% ROIC
   - DCF gap is informative but not decisive — large gaps often reflect modeling assumption
     differences, not real mispricing
   - FCF yield is the truth-teller for mature companies — 4%+ FCF yield with single-digit growth
     is a defensible holding

5. EARNINGS DURABILITY + SHAREHOLDER RETURN
   - Beat-rate above 75% over 8+ quarters = high-quality management or strong franchise (both good)
   - Beat-rate with SHRINKING magnitude trend = quality deteriorating, even if absolute beats continue
   - Buyback-driven EPS growth is real but lower quality than organic — distinguish them
   - Dividend cuts are the most powerful bearish signal that exists for an income-style holding

═══════════════════════════════════════════════════════════════════════════
SCENARIO CONSTRUCTION (this is where most analysts get lazy — don't be lazy)
═══════════════════════════════════════════════════════════════════════════

The 'scenarios' block is critical — it's what PMs actually use to size positions.
Probabilities MUST sum to 100. Be honest about the ranges:
  - Bull case = optimistic path (multiple expansion, growth acceleration,
    catalysts hit). Typically 20-35% probability.
  - Base case = current trajectory continues, no surprises. Typically 40-60%
    probability — the highest weight.
  - Bear case = thesis breaks (multiple compression, growth deceleration,
    catalyst misses). Typically 15-30% probability.

The price targets between the three should span a meaningful range — if your
bull and bear are within 10% of base, the scenarios aren't doing real work.
A well-constructed scenario shows a 30-60% spread top-to-bottom on the targets.

Each scenario's drivers must be:
  - SPECIFIC: "EPS hits $7.50 from $6.20 via 12% rev growth + 50bps op margin"
              NOT "earnings grow nicely"
  - INDEPENDENT: drivers in the bull case should be different paths, not all
                 contingent on one thing (a 4-driver scenario where everything
                 depends on China lifting export restrictions is really a 1-driver scenario)
  - FALSIFIABLE: a PM should be able to track each driver quarterly and know
                 whether the scenario is on track

═══════════════════════════════════════════════════════════════════════════
VERDICT + POSITION SIZING DISCIPLINE
═══════════════════════════════════════════════════════════════════════════

Position size (1-15% range, concentrated book) should reflect:
  - Conviction (A+ → 10-15%, A → 7-10%, B → 4-7%, C → 2-4%, D → 1-2%)
  - Volatility (smaller for high-beta names, larger for defensive)
  - Asymmetry (bull case 3x bear case loss = larger; symmetric = smaller)

Rating scale:
  - STRONG_BUY: high conviction + 25%+ upside in base case + favorable asymmetry
                (use sparingly — should be 5-10% of all calls)
  - BUY: positive expected value + thesis intact + reasonable entry
  - HOLD: fair value + no edge in either direction + acceptable to maintain existing position
          but not adding new capital
  - SELL: negative expected value OR thesis broken OR superior alternative exists
  - STRONG_SELL: severe overvaluation OR fundamental deterioration OR balance sheet stress

Conviction grade rubric:
  - A+ : exceptional thesis, clear catalysts, fortress balance sheet, sub-fair valuation
  - A  : strong thesis, multiple catalysts visible, healthy financials, fair-to-attractive value
  - A- : strong thesis with one notable risk; otherwise pristine
  - B+ : sound thesis but valuation is full OR a meaningful concern exists
  - B  : decent setup with multiple offsetting positives and negatives
  - B- : marginal thesis or expensive valuation requires several things to work
  - C+/C/C- : thesis requires multiple catalysts to land OR significant deterioration
  - D : avoid; multiple structural issues

A PM reads your verdict_rationale FIRST and your full memo second. Make the
rationale earn that read — tie thesis + key risk + valuation + size in 30-50 words.

═══════════════════════════════════════════════════════════════════════════
DATA HANDLING RULES
═══════════════════════════════════════════════════════════════════════════

- Missing data: say "not available" explicitly. Never extrapolate from limited samples.
- Stale data: if metrics are from prior year and quarterlies tell a different story,
  weight the quarterlies. Note the discrepancy in 'risk_factors'.
- Peer comparison: cite at least one specific peer ticker with a specific multiple,
  not "compared to peers".
- Earnings call: if transcript provided, attribute quotes to specific speakers
  (CEO/CFO by name when given). Don't paraphrase as if it's your analysis.
- Institutional / Form 4 data: when provided, use signal labels (CLUSTER_BUY,
  ACCELERATING_SELL, etc) directly. Don't override the quantitative signal with
  vibes.

═══════════════════════════════════════════════════════════════════════════
CONTRARIAN PATTERN RECOGNITION
═══════════════════════════════════════════════════════════════════════════

These are signals the average analyst misses. When you see one, weight it heavily
in your verdict — they predict turns better than headline metrics.

1. DIVERGENCES (one of the strongest signals in equity analysis):
   - Revenue accelerating while gross margin compressing → pricing power eroding
     even though top line looks fine. Common at peak cycles before margin reversion.
   - Net income growing faster than FCF for 3+ years → working capital build,
     receivables aging, or capex deferral. Earnings quality issue.
   - Stock-comp expense rising faster than revenue → dilution accelerating, often
     hidden by buybacks. Watch FD share count, not basic.
   - Insider selling clustered AFTER a positive guidance raise → leadership doesn't
     fully believe their own guidance. Strongest insider signal that exists.

2. MEAN REVERSION SETUPS:
   - Trailing P/E in top decile of 10-year range + decelerating revenue growth →
     classic multiple-compression setup. Even great companies face this when expectations
     overshoot reality.
   - ROIC peaking at 3-year highs + capex intensity rising → ROIC will likely revert
     down as new investments earn lower marginal returns.
   - Margin at all-time highs + competitive intensity rising (new entrants, price wars)
     → margin reversion typically precedes EPS estimate cuts by 2-4 quarters.

3. ASYMMETRY-FAVORING SETUPS:
   - Trailing P/E in bottom decile + accelerating revenue growth + improving margins
     = the textbook compounder buy setup. Rare; act decisively when found.
   - Cluster insider buying + recent margin expansion + clean balance sheet
     = high-probability setup. Cluster buys especially valuable because insiders
     rarely buy with own money (most comp is RSUs).
   - Activist 13D filing + underperforming valuation + management track record of
     responsiveness = setup-rich situation.

4. STRUCTURAL DETERIORATION (often missed early):
   - Days sales outstanding (DSO) climbing for 3+ quarters → customers paying slower,
     often a sign of customer financial stress or aggressive revenue recognition.
   - Inventory days climbing faster than revenue → demand softening; expect future
     gross margin pressure from inventory markdowns.
   - Customer concentration risk: if top 10 customers = >30% of revenue and growing
     → buyer power increasing, future pricing flexibility decreasing.

═══════════════════════════════════════════════════════════════════════════
SECTOR-AWARE INTERPRETATION
═══════════════════════════════════════════════════════════════════════════

Same metric means different things in different sectors. Calibrate:

- SOFTWARE / SAAS: focus on net retention (>110% = healthy), rule-of-40 (growth +
  FCF margin), gross margin (>75% = good), and stock-comp-as-percent-of-revenue
  (<10% = healthy, >15% = problematic). P/E often less useful than EV/sales × growth.

- CONSUMER STAPLES: shareholder yield + dividend coverage are paramount. Growth is
  typically 3-5%, so look for share-of-wallet trends, geographic expansion, premiumization.
  Margin stability through cycles is the moat. KO, PEP, PG framework.

- BANKS / FINANCIALS: book value matters more than earnings; ROTCE > 15% = excellent;
  NIM trends, credit quality (provisions, net charge-offs, NPL ratio), and capital ratios
  (CET1, leverage ratio) are the analytical core. P/E means little; P/B and P/TBV matter.

- ENERGY / CYCLICALS: focus on free cash flow at mid-cycle prices, balance sheet
  strength through downturn, cash return discipline. Mid-cycle FCF yield > 10% on a
  fortress balance sheet = compelling. Avoid extrapolating peak-cycle earnings.

- BIG-CAP TECH: dominance/moat metrics — market share trends, competitive position
  in core franchise (does ChatGPT threaten Google search? Does Apple's services
  growth offset iPhone deceleration?). Capital allocation discipline matters because
  TAMs are huge — wasted capex destroys real value.

═══════════════════════════════════════════════════════════════════════════
OUTPUT JSON SCHEMA
═══════════════════════════════════════════════════════════════════════════

Schema:
{
  "executive_summary": "3-4 sentence top-line. Lead with the recommendation and key thesis.",
  "investment_thesis": {
    "title":             "Punchy 5-8 word headline",
    "thesis_paragraph":  "150-200 word case for owning the stock, with specific numbers from the data",
    "key_drivers": [
      {"driver": "...", "supporting_data": "specific metric e.g. 'rev 5y CAGR 14.2%'"},
      ... (4-5 drivers)
    ]
  },
  "risk_factors": {
    "title": "Punchy risk-headline",
    "risk_paragraph": "150-200 words on what could go wrong, with specific numbers",
    "key_risks": [
      {"risk": "...", "evidence": "specific data e.g. 'debt/equity 2.4x vs sector 0.8x'"},
      ... (4-5 risks)
    ]
  },
  "devils_advocate": {
    "title": "The short-seller's pitch (punchy, 5-9 words)",
    "short_thesis": "120-180 words making the SINGLE most compelling bear/short case — the argument a skeptical PM or an activist short-seller would actually present at a partners' meeting. Steelman it hard: turn the report's own data against the stock (stretched valuation, decelerating or low-quality growth, margin/accounting red flags, leverage, competitive erosion, capital-allocation that flatters EPS, beat-magnitude shrinking, insider/institutional exits). Do NOT hedge, do NOT 'on the other hand' — this section's entire job is to argue the other side as forcefully as the data honestly allows.",
    "kill_points": [
      {"point": "the sharpest, most specific argument against owning this here", "evidence": "the exact number that supports it"},
      ... (3-4 points, each falsifiable and tied to a figure)
    ],
    "what_bulls_underestimate": "40-60 words on the single risk the bull case is most likely waving away, and why it could matter more than consensus thinks."
  },
  "valuation_assessment": "150 words on whether the stock is cheap/fair/expensive given P/E vs 5yr avg, DCF gap, peer multiples, and FCF yield. Be specific.",
  "peer_comparison_assessment": "100 words on how the subject's valuation multiples compare to the peer-median (which functions as the industry P/E benchmark). Reference SPECIFIC peer ticker(s) where helpful. Frame as: 'trading at X% premium/discount to peer median P/E of Y'.",
  "industry_comparison_assessment": "90 words using the REAL industry_comparison block (FMP industry & sector P/E snapshot). State the company P/E vs the industry P/E and sector P/E explicitly with the percentages, then judge whether that premium/discount is JUSTIFIED by the company's fundamentals (growth, margins, ROE, leverage) or whether it signals mispricing. A discount is only cheap if the business isn't structurally inferior; a premium is only warranted by superior growth/returns. Be decisive.",
  "business_mix_assessment": "90 words on the revenue composition in the business_mix block: which segment(s) and geograph(ies) drive revenue and how concentrated that is, whether the mix is shifting over the trend, and — importantly for customer concentration — who the key customers are IF that is evident from the company description, segments, or earnings-call excerpt (e.g. a single dominant government/enterprise customer). If customer detail isn't in the provided data, say 'specific customer concentration not disclosed in the provided data' rather than guessing. Frame the concentration as a risk or a strength.",
  "relationships": {
    "_INSTRUCTIONS_": "Build a grounded customer/partner/supplier map. HARD RULES: (1) Include ONLY entities EXPLICITLY named in the provided company.description, business_mix segments, or earnings_call_excerpt. (2) DO NOT use any outside or trained knowledge — if you are recalling a relationship rather than reading it in the provided text, OMIT it. Fabricated edges are a critical failure. (3) If a customer/partner is referenced but not named (e.g. 'a large customer paused projects'), you MAY include it as name 'Undisclosed customer' with the detail given. (4) Suppliers are rarely disclosed — include only if a supplier/vendor is explicitly named; otherwise return []. (5) Every entry MUST carry a 'source' field naming where it came from.",
    "summary": "30-60 words describing the customer/partner picture, grounded only in the provided text.",
    "customers": [
      {"name": "exact name/label as it appears in the provided text", "detail": "10-15 words on the relationship", "concentration": "stated % or qualitative ('primary customer','majority of revenue') if given, else null", "source": "description | earnings_call | segments"}
    ],
    "partners": [
      {"name": "...", "detail": "...", "source": "description | earnings_call"}
    ],
    "suppliers": [
      {"name": "...", "detail": "...", "source": "description | earnings_call"}
    ]
  },
  "earnings_track_record_assessment": "80 words on the company's earnings consistency. Cite the EPS beat rate, current streak, magnitude trend, and revenue surprise. Hedge fund framing: 'beats 7 of 8 quarters but with shrinking magnitude = deteriorating quality' is more useful than just 'beats consensus regularly'.",
  "capital_allocation_assessment": "80 words on management's capital allocation. Cite total capital returned 10y, shareholder yield, dividend vs buyback mix, payout ratio sustainability, and capex intensity trend. Frame as 'cash-cow returning $X to shareholders' vs 'reinvesting heavily into capex' — both can be good, depends on ROIC.",
  "institutional_activity_assessment": "60 words on recent SEC 13D/13G beneficial-ownership filings (institutional positions crossing 5% threshold). If filings are stale (>24mo old) or absent, say so plainly. If recent clustering by notable institutions (Berkshire, Vanguard, Blackrock, Wellington), call it out as 'smart money accumulation'.",
  "earnings_call_sentiment": {
    "available": true|false (set to false if no transcript was provided),
    "overall_tone": "BULLISH | CONFIDENT | NEUTRAL | CAUTIOUS | DEFENSIVE",
    "tone_summary": "100 words describing the management's tone across the prepared remarks and Q&A. Cite specific phrases ('several large customers paused projects', 'we expect double-digit growth to accelerate') — direct attribution to CEO or CFO when possible. Distinguish the prepared-remarks tone from the Q&A tone — Q&A often reveals more.",
    "key_topics": ["3-5 topic clusters management spent the most time on, e.g. 'AI infrastructure capex', 'China demand softness', 'pricing power in enterprise'"],
    "guidance_change": "RAISED | MAINTAINED | LOWERED | NOT_PROVIDED",
    "guidance_summary": "50 words on what management said about forward guidance and how it changed from prior quarter (if mentioned).",
    "notable_quotes": [
      {"speaker": "name + title", "quote": "verbatim short quote", "significance": "why this matters to a PM"},
      ... (2-3 quotes that contain the most information)
    ]
  },
  "financial_health_summary": "100 words on the 5-pillar score, calling out the strongest and weakest pillars with the actual numbers.",
  "competitive_position": "100 words on the company's moat and industry position based on margins, growth durability, and ROIC vs peers.",
  "catalysts_12m": [
    {"event": "...", "timeframe": "Q2 2026 | H1 2027 | etc", "potential_impact": "..."},
    ... (3-5 catalysts)
  ],
  "invalidation_triggers": [
    {"trigger": "what would change the thesis", "monitor": "what to watch"},
    ... (3-5 triggers)
  ],
  "scenarios": {
    "bull_case": {
      "price_target_12m":  <number, USD>,
      "probability_pct":   <0-100, your subjective probability this scenario plays out>,
      "thesis_1liner":     "10-15 word summary of what has to be true",
      "drivers":           ["3-4 concrete things that get us here, e.g. 'revenue accelerates to 18%', 'multiple re-rates to 30x as AI moat clarifies'"]
    },
    "base_case": {
      "price_target_12m":  <number, USD>,
      "probability_pct":   <0-100, typically the highest weight 40-60%>,
      "thesis_1liner":     "current trajectory, no surprises",
      "drivers":           ["..."]
    },
    "bear_case": {
      "price_target_12m":  <number, USD>,
      "probability_pct":   <0-100>,
      "thesis_1liner":     "...",
      "drivers":           ["..."]
    }
  },
  "forward_model": {
    "summary": "120-160 words walking through your forward model: the revenue trajectory you assume and WHY (anchor to analyst_estimates consensus where present, then adjust up/down with your own view of the growth drivers and risks), the margin path, the resulting EPS, and the multiple you apply to reach a fair value. State plainly whether the model implies upside or downside vs the current price.",
    "revenue_projections": [
      {"year": "FY2026", "revenue_est": <number USD>, "growth_pct": <number>, "basis": "analyst consensus | your adjustment + why"},
      {"year": "FY2027", "revenue_est": <number USD>, "growth_pct": <number>, "basis": "..."},
      {"year": "FY2028", "revenue_est": <number USD>, "growth_pct": <number>, "basis": "..."}
    ],
    "margin_path": {"metric": "operating | net margin", "current_pct": <number>, "projected_pct": <number>, "rationale": "20-40 words"},
    "eps_projections": [
      {"year": "FY2026", "eps_est": <number>, "growth_pct": <number>},
      {"year": "FY2027", "eps_est": <number>, "growth_pct": <number>},
      {"year": "FY2028", "eps_est": <number>, "growth_pct": <number>}
    ],
    "price_model": {
      "method": "forward P/E × projected EPS, cross-checked against the company's own 5yr-avg multiple, the industry P/E, and a DCF view",
      "forward_pe_applied": <number>,
      "forward_pe_rationale": "20-40 words on why this multiple — vs the company's 5yr avg and the industry P/E from industry_comparison",
      "target_eps_year": "FY2027",
      "fair_value_base": <number USD — projected EPS for target year × forward_pe_applied>,
      "fair_value_bull": <number USD>,
      "fair_value_bear": <number USD>,
      "upside_to_base_pct": <number — vs current price>,
      "dcf_cross_check": "1-2 sentences: does a DCF (cite the dcf_estimate in valuation) corroborate or contradict the multiple-based value?"
    },
    "key_assumptions": ["3-5 falsifiable assumptions the model rests on, each with the number"],
    "confidence": "HIGH | MEDIUM | LOW — with a 10-word reason tied to analyst coverage + visibility"
  },
  "verdict": {
    "rating":              "STRONG_BUY | BUY | HOLD | SELL | STRONG_SELL",
    "conviction_grade":    "A+ | A | A- | B+ | B | B- | C+ | C | C- | D",
    "price_target_12m":    <number — the 12-month PT in USD>,
    "upside_pct":          <number — implied upside vs current>,
    "confidence_pct":      <0-100 reflecting probability the thesis plays out>,
    "position_size_pct":   <recommended position size 1-15% for a concentrated book>,
    "time_horizon_months": <how long to hold, typically 6-24>,
    "verdict_rationale":   "30-50 word reasoning tying together thesis + risks + valuation"
  }
}"""


def fetch_etf_flow_context(payload: dict) -> str:
    """Fetch sector ETF flow context for this ticker from etf-flows/per-ticker-context.json
    PLUS Phase 2 multi-asset macro regime tag.

    Returns a 2-3 sentence snippet ready for prompt injection — or empty
    string if data unavailable. We use the ticker's GICS sector to look up
    its sector SPDR ETF (XLK for Technology, XLF for Financials, etc.) and
    fetch that ETF's flow signal + the broader market regime + the macro
    multi-asset regime tag (the foundational signal).

    Pattern: research-aware analyst writes flow-conscious + regime-conscious
    theses. If the macro regime is CREDIT_STRESS or FLIGHT_TO_QUALITY,
    the analyst should temper bullish theses; if REFLATION/RISK_ON, allow
    full conviction on appropriate sector exposures.
    """
    try:
        sector = (payload.get("company") or {}).get("sector")
        snippets = []

        # ── Phase 2 multi-asset macro regime ──────────────────────────────
        try:
            macro_obj = s3.get_object(Bucket=S3_BUCKET, Key="macro/regime.json")
            macro = json.loads(macro_obj["Body"].read())
            tl = macro.get("top_level_regime", {}) or {}
            subs = macro.get("sub_regimes", {}) or {}
            if tl.get("regime"):
                sub_summary = ", ".join(
                    f"{k.replace('_regime','')}={v.get('label', '—')}"
                    for k, v in subs.items() if v.get("label") and v.get("label") != "INSUFFICIENT_DATA"
                )
                snippets.append(
                    f"[MULTI-ASSET MACRO REGIME] Top-level: {tl['regime']} ({tl.get('confidence','—')} confidence). "
                    f"Reasoning: {tl.get('reasoning', '—')}. Sub-regimes: {sub_summary}."
                )
        except Exception:
            pass

        # ── Per-sector ETF flow context ──────────────────────────────────
        if sector:
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key="etf-flows/per-ticker-context.json")
                ctx = json.loads(obj["Body"].read())
                by_sector = (ctx.get("context") or {}).get("by_sector") or {}
                sector_ctx = by_sector.get(sector)
                if sector_ctx and sector_ctx.get("prompt_snippet"):
                    snippets.append(f"[SECTOR ETF FLOWS] {sector_ctx['prompt_snippet']}")
                else:
                    global_regime = (ctx.get("context") or {}).get("global_regime", "UNKNOWN")
                    smart_dumb = (ctx.get("context") or {}).get("smart_vs_dumb_label", "MIXED")
                    risk_label = (ctx.get("context") or {}).get("risk_on_off_label", "MIXED")
                    snippets.append(
                        f"[FLOW REGIME] Market regime: {global_regime}. "
                        f"Smart money: {smart_dumb}. Risk posture: {risk_label}."
                    )
            except Exception:
                pass

        # ── Constituent pull-through pressure (institutional alpha edge) ─
        # NEW: comprehensive per-stock ETF exposure across ALL 84 tracked ETFs,
        # not just high-z ETFs. So even tickers without extreme sector flow
        # get the cross-ETF positioning context.
        ticker = payload.get("ticker")
        if ticker:
            try:
                obj = s3.get_object(Bucket=S3_BUCKET, Key="etf-flows/stock-exposure-lookup.json")
                lookup = json.loads(obj["Body"].read())
                exposure = lookup.get(ticker)
                if exposure:
                    n_etfs = exposure.get("n_etfs_holding", 0)
                    cum_wt = exposure.get("cumulative_weight_pct", 0)
                    agg_5d = exposure.get("total_aggregate_flow_5d_usd") or 0
                    agg_21d = exposure.get("total_aggregate_flow_21d_usd") or 0
                    top_etfs = (exposure.get("top_etfs") or [])[:5]
                    direction = ("BUYING" if agg_5d > 1e6 else "SELLING" if agg_5d < -1e6 else "NEUTRAL")
                    etf_breakdown = "; ".join(
                        f"{e['etf']} (wt {e.get('weight_pct', 0):.1f}%, z={e.get('etf_zscore'):.2f}σ, "
                        f"flow ${(e.get('etf_flow_5d_usd') or 0)/1e6:+.0f}M)"
                        for e in top_etfs
                    )
                    snippets.append(
                        f"[CROSS-ETF FLOW EXPOSURE on {ticker}] {direction} bias — "
                        f"this stock is held by {n_etfs} of the tracked ETFs "
                        f"(cumulative weight exposure {cum_wt:.1f}%). "
                        f"Aggregated implied flow: 5d=${agg_5d/1e6:+,.1f}M, "
                        f"21d=${agg_21d/1e6:+,.1f}M across all holding ETFs. "
                        f"Top contributing ETFs: {etf_breakdown}. "
                        f"This is real institutional positioning via index/ETF channels — "
                        f"reconcile your fundamental thesis with how money is actually flowing "
                        f"through these vehicles."
                    )
            except Exception as e:
                print(f"[exposure-lookup] unavailable for {ticker}: {e}")

        if not snippets:
            return ""
        return "\n\n" + "\n".join(snippets) + "\n"
    except Exception as e:
        print(f"[etf-flow-context] unavailable: {e}")
        return ""


def build_claude_prompt(payload: dict) -> str:
    """Compose user prompt — JSON dump of every meaningful field, plus
    sector ETF flow context if available (institutional positioning signal)."""
    flow_context = fetch_etf_flow_context(payload)
    return (
        f"Produce institutional equity research for "
        f"{payload['company'].get('name','?')} ({payload['ticker']}).\n\n"
        "All data follows. Synthesize per the schema in the system prompt. "
        "When you see [ETF FLOW CONTEXT] at the bottom, factor it into your "
        "conviction grade and thesis — institutional sector positioning often "
        "leads price action by days to weeks (Ben-David 2017, BIS 2018).\n\n"
        "```json\n" + json.dumps(payload, indent=2, default=str)[:60000] + "\n```"
        + flow_context
    )


def _repair_truncated_json(text: str) -> dict:
    """Salvage a JSON object that was cut off when the model hit max_tokens.
    Walk to the last clean boundary (a completed value: closing brace/bracket or
    comma, outside any string), drop a trailing comma, then close the still-open
    containers. Recovers every complete field before the truncation point."""
    i = text.find("{")
    if i < 0:
        raise ValueError("no JSON object found")
    s = text[i:]
    stack = []
    in_str = False
    esc = False
    clean_idx = -1
    clean_stack = None
    for idx, ch in enumerate(s):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
            continue
        if ch in "{[":
            stack.append("}" if ch == "{" else "]")
        elif ch in "}]":
            if stack:
                stack.pop()
            clean_idx, clean_stack = idx, list(stack)
        elif ch == ",":
            clean_idx, clean_stack = idx, list(stack)
    if clean_idx < 0:
        raise ValueError("cannot repair truncated JSON")
    frag = s[:clean_idx + 1].rstrip().rstrip(",")
    frag += "".join(reversed(clean_stack or []))
    return json.loads(frag)


def parse_claude(text: str) -> dict:
    """Strip ```json fences and parse; salvage if the response was truncated."""
    import re
    text = re.sub(r"^```(?:json)?\s*", "", text.strip(), flags=re.MULTILINE)
    text = re.sub(r"\s*```\s*$", "", text.strip(), flags=re.MULTILINE)
    # Find first balanced JSON object
    depth, start = 0, -1
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0: start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                try:
                    return json.loads(text[start:i+1])
                except json.JSONDecodeError:
                    continue
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Model hit the token cap mid-object — salvage the complete fields.
        return _repair_truncated_json(text)


# ═════════════════════════════════════════════════════════════════════
# Main handler
# ═════════════════════════════════════════════════════════════════════

def load_macro_regime_snapshot() -> dict:
    """Read macro/regime.json and return a compact snapshot to STAMP on every
    research output as regime_at_generation. The point: when track-record
    backtests this call later, it knows EXACTLY which regime was active at
    generation time without needing historical lookups. Institutional
    attribution by regime hinges on this stamp.

    Returns dict with: regime, confidence, reasoning, sub_regimes_summary,
    generated_at — or empty dict if macro engine output unavailable.
    """
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="macro/regime.json")
        doc = json.loads(obj["Body"].read())
        tl = doc.get("top_level_regime", {}) or {}
        subs = doc.get("sub_regimes", {}) or {}
        return {
            "regime":         tl.get("regime"),
            "confidence":     tl.get("confidence"),
            "reasoning":      tl.get("reasoning"),
            "sub_regimes":    {
                k: {"label": v.get("label"), "score": v.get("score")}
                for k, v in subs.items()
            },
            "macro_generated_at": doc.get("generated_at"),
        }
    except Exception as e:
        print(f"[regime-stamp] unavailable: {e}")
        return {}


def lambda_handler(event, context):
    t0 = time.time()

    # ── Async-invocation mode
    # When invoked via boto3 .invoke(InvocationType='Event'), the event is a
    # plain dict (no API Gateway wrapping). We look for _internal=1 to
    # distinguish background work from a user-facing HTTP request.
    # The async path returns nothing meaningful (Lambda 'Event' invocations
    # discard the return value); it just runs the work + writes to S3.
    is_internal_async = (isinstance(event, dict)
                          and event.get("_internal") == "1"
                          and not event.get("queryStringParameters"))

    # ── Extract ticker from query string OR POST body OR direct async event
    ticker = None
    qs = {}
    try:
        if isinstance(event, dict):
            if is_internal_async:
                ticker = event.get("ticker")
            else:
                qs = event.get("queryStringParameters") or {}
                ticker = qs.get("ticker")
                if not ticker and event.get("body"):
                    body = event["body"]
                    if isinstance(body, str):
                        try: body = json.loads(body)
                        except Exception: body = {}
                    if isinstance(body, dict): ticker = body.get("ticker")
    except Exception as e:
        return _http_error(400, f"Could not parse request: {e}")

    if not ticker:
        return _http_error(400, "Missing 'ticker' query parameter")
    ticker = ticker.strip().upper()
    # Tickers can contain letters, digits, and class-share separators (- or .)
    # e.g. AAPL, MSFT, BRK-B, BRK.B, RDS-A
    import re as _re
    if not _re.fullmatch(r"[A-Z0-9.\-]{1,10}", ticker):
        return _http_error(400, f"Invalid ticker: {ticker}")

    # ── Flags
    force_refresh = qs.get("refresh") in ("1", "true", "yes") if not is_internal_async else event.get("force_refresh", False)
    kickoff_mode  = qs.get("kickoff") in ("1", "true", "yes") and not is_internal_async
    cache_key = f"{CACHE_PREFIX}{ticker}.json"

    # ── Cache check (skipped if force_refresh)
    if not force_refresh:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=cache_key)
            cached = json.loads(obj["Body"].read())
            cached["from_cache"] = True
            cached["cache_age_seconds"] = int(time.time() - _iso_to_epoch(cached.get("generated_at")))
            if cached["cache_age_seconds"] < CACHE_TTL and "earnings_vol_edge" in cached:
                print(f"[cache] HIT {ticker} age={cached['cache_age_seconds']}s")
                if is_internal_async:
                    return {"ok": True, "from_cache": True, "ticker": ticker}
                return _http_ok(cached)
        except s3.exceptions.NoSuchKey:
            pass
        except Exception as e:
            print(f"[cache] read error: {e}")

    # ── KICKOFF MODE — fire async self-invoke + return immediately
    # This is the fix for 'Failed to fetch'. Generating a fresh report takes
    # 90-120s (Claude synthesis + 19 FMP fetches). Browsers / proxies kill
    # long fetches around 30-60s. Instead of holding the HTTP connection
    # open, we kick off async work and let the frontend poll S3 via the
    # CDN edge cache for the result.
    if kickoff_mode:
        try:
            _lam = boto3.client("lambda", region_name="us-east-1")
            _lam.invoke(
                FunctionName=context.function_name if context else "justhodl-equity-research",
                InvocationType="Event",
                Payload=json.dumps({
                    "_internal":     "1",
                    "ticker":        ticker,
                    "force_refresh": True,
                }).encode("utf-8"),
            )
            print(f"[kickoff] queued async generation for {ticker}")
        except Exception as e:
            print(f"[kickoff] failed to invoke async: {e}")
            return _http_error(500, f"Could not queue research: {e}")
        # Return a 202 Accepted with poll info
        return {
            "statusCode": 202,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-store",
            },
            "body": json.dumps({
                "status":         "processing",
                "ticker":         ticker,
                "eta_seconds":    100,
                "message":        f"Research for {ticker} is being generated in the background.",
                # Frontend polls this URL via CDN — files appear here when done.
                "poll_s3_url":    f"https://justhodl-data-proxy.raafouis.workers.dev/{cache_key}",
                "poll_direct_url":f"https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/{cache_key}",
            }),
        }

    # ── Fetch all FMP endpoints in parallel
    print(f"[research] fetching {ticker}")
    raw = fetch_all(ticker)
    n_ok = sum(1 for v in raw.values() if v)
    print(f"[research] {n_ok}/{len(raw)} endpoints returned data")

    profile_obj = _first(raw.get("profile")) or {}
    quote_obj   = _first(raw.get("quote"))   or {}

    if not profile_obj and not quote_obj:
        return _http_error(404, f"No data for ticker {ticker}")

    income_annual    = raw.get("income_annual") if isinstance(raw.get("income_annual"), list) else []
    income_quarterly = raw.get("income_quarterly") if isinstance(raw.get("income_quarterly"), list) else []
    balance_annual   = raw.get("balance_annual") if isinstance(raw.get("balance_annual"), list) else []
    cashflow_annual  = raw.get("cashflow_annual") if isinstance(raw.get("cashflow_annual"), list) else []
    # ── v2.0 institutional modules (technicals / liquidity / growth-vs-mcap / quant-risk / backlog)
    v2 = build_v2_institutional(ticker, raw, income_annual, income_quarterly,
                                balance_annual, cashflow_annual)
    ratios_annual    = raw.get("ratios_annual") if isinstance(raw.get("ratios_annual"), list) else []
    ratios_ttm       = _first(raw.get("ratios_ttm")) or {}
    key_metrics      = raw.get("key_metrics") if isinstance(raw.get("key_metrics"), list) else []
    key_ttm          = _first(raw.get("key_metrics_ttm")) or {}
    growth_series    = raw.get("growth") if isinstance(raw.get("growth"), list) else []
    estimates        = raw.get("estimates") if isinstance(raw.get("estimates"), list) else []
    pt_consensus     = raw.get("pt_consensus") or {}
    dcf              = raw.get("dcf") or {}
    scores           = raw.get("scores") or {}
    peers_obj        = _first(raw.get("peers")) or {}
    prices_eod       = raw.get("prices_eod") if isinstance(raw.get("prices_eod"), list) else []
    dividends        = raw.get("dividends") if isinstance(raw.get("dividends"), list) else []
    earnings         = raw.get("earnings") if isinstance(raw.get("earnings"), list) else []
    ownership_data   = raw.get("ownership") if isinstance(raw.get("ownership"), list) else []
    transcript_dates_data = raw.get("transcript_dates") if isinstance(raw.get("transcript_dates"), list) else []

    # The peers endpoint returns a LIST of peer objects directly (not wrapped).
    # Each has symbol, companyName, price, mktCap.
    peers_list = raw.get("peers") if isinstance(raw.get("peers"), list) else []
    peer_symbols = [p.get("symbol") for p in peers_list if p.get("symbol")][:5]

    # ── Second-round parallel fetch: peer ratios + key metrics for comparison
    peer_details: Dict[str, Dict[str, Any]] = {}
    if peer_symbols:
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {}
            for s in peer_symbols:
                futures[ex.submit(fmp_get, "ratios-ttm", symbol=s)] = (s, "ratios")
                futures[ex.submit(fmp_get, "key-metrics-ttm", symbol=s)] = (s, "key_metrics")
            for fut in as_completed(futures):
                sym, kind = futures[fut]
                try:
                    peer_details.setdefault(sym, {})[kind] = _first(fut.result()) or {}
                except Exception:
                    peer_details.setdefault(sym, {})[kind] = {}
        print(f"[peers] fetched detail for {len(peer_details)} peers")

    # ── Derive analytics
    growth_metrics    = compute_growth(income_annual)
    fcf_metrics       = compute_fcf_cagr(cashflow_annual)
    margin_trend      = compute_margin_trend(income_annual, n=12)
    qty_consistency   = compute_quarterly_consistency(income_quarterly)
    current_price     = _safe_num(quote_obj, "price") or _safe_num(profile_obj, "price")
    returns           = compute_returns(prices_eod, current_price)
    balance_qual      = compute_balance_quality(balance_annual)
    cf_qual           = compute_cf_quality(income_annual, cashflow_annual)
    valuation         = compute_valuation(profile_obj, ratios_ttm, key_ttm,
                                            ratios_annual, dcf, pt_consensus, quote_obj)
    health            = compute_financial_health(scores, ratios_ttm, key_ttm,
                                                   balance_qual, cf_qual,
                                                   {**growth_metrics, **fcf_metrics})

    # Company block needs to be built BEFORE peer comparison since we pass it in.
    _company_block_for_peers = {
        "name":         profile_obj.get("companyName") or profile_obj.get("name"),
        "sector":       profile_obj.get("sector"),
        "industry":     profile_obj.get("industry"),
        "market_cap":   _safe_num(profile_obj, "mktCap") or _safe_num(profile_obj, "marketCap"),
    }
    peer_comparison = build_peer_comparison(
        ticker, ratios_ttm, key_ttm, quote_obj,
        _company_block_for_peers, peers_list, peer_details,
    )

    # ── Earnings beat/miss track record
    earnings_track_record = compute_earnings_track_record(earnings)

    # ── Capital allocation timeline
    capital_allocation = compute_capital_allocation(cashflow_annual, income_annual, quote_obj)

    # ── Institutional activity (13D/13G filings — smart money tracking)
    institutional_activity = compute_institutional_activity(ownership_data)

    # ── Earnings call transcript (most recent quarter)
    earnings_call = fetch_latest_transcript(ticker, transcript_dates_data)
    if earnings_call:
        print(f"[transcript] {ticker} Q{earnings_call['quarter']} {earnings_call['fiscal_year']} "
              f"({earnings_call['date']}) — {earnings_call['full_chars']} chars full, "
              f"{earnings_call['truncated_chars']} sent to Claude")

    # ── Compact statements (every year, just essential fields)
    def compact_income(rows):
        keys = ("date","revenue","grossProfit","operatingIncome","netIncome",
                "epsDiluted","weightedAverageShsOutDil","grossProfitRatio",
                "operatingIncomeRatio","netIncomeRatio")
        return [{k: r.get(k) for k in keys if k in r} for r in rows]

    def compact_balance(rows):
        keys = ("date","totalAssets","totalLiabilities","totalEquity",
                "totalStockholdersEquity","totalCurrentAssets",
                "totalCurrentLiabilities","cashAndShortTermInvestments",
                "totalDebt","longTermDebt","shortTermDebt","goodwill")
        return [{k: r.get(k) for k in keys if k in r} for r in rows]

    def compact_cf(rows):
        # FMP /stable/cash-flow-statement uses (verified ops 1143):
        #   commonDividendsPaid (real)        — replaces legacy 'dividendsPaid'
        #   netCashProvidedByInvestingActivities — replaces legacy 'netCashUsedForInvestingActivities'
        #   netCashProvidedByFinancingActivities — replaces legacy 'netCashUsedProvidedByFinancingActivities'
        # We emit both the new and legacy field names so older frontends
        # and downstream consumers continue working.
        keys = ("date","operatingCashFlow","netCashProvidedByOperatingActivities",
                "capitalExpenditure","freeCashFlow",
                "commonDividendsPaid","netDividendsPaid","dividendsPaid",
                "commonStockRepurchased",
                "netCashProvidedByInvestingActivities", "netCashUsedForInvestingActivities",
                "netCashProvidedByFinancingActivities", "netCashUsedProvidedByFinancingActivities",
                "netIncome")
        out_rows = []
        for r in rows:
            row = {k: r.get(k) for k in keys if k in r}
            # Forward-fill legacy field names from new ones so older
            # consumers keep working without code changes
            if row.get("dividendsPaid") is None:
                row["dividendsPaid"] = (row.get("commonDividendsPaid")
                                          or row.get("netDividendsPaid"))
            if row.get("netCashUsedForInvestingActivities") is None:
                row["netCashUsedForInvestingActivities"] = row.get("netCashProvidedByInvestingActivities")
            if row.get("netCashUsedProvidedByFinancingActivities") is None:
                row["netCashUsedProvidedByFinancingActivities"] = row.get("netCashProvidedByFinancingActivities")
            out_rows.append(row)
        return out_rows

    # ── Build payload for Claude
    company_block = {
        "name":         profile_obj.get("companyName") or profile_obj.get("name"),
        "sector":       profile_obj.get("sector"),
        "industry":     profile_obj.get("industry"),
        "country":      profile_obj.get("country"),
        "exchange":     profile_obj.get("exchange"),
        "ceo":          profile_obj.get("ceo"),
        "employees":    profile_obj.get("fullTimeEmployees"),
        "ipo_date":     profile_obj.get("ipoDate"),
        "market_cap":   _safe_num(profile_obj, "mktCap") or _safe_num(profile_obj, "marketCap"),
        "description":  (profile_obj.get("description") or "")[:1200],
        "website":      profile_obj.get("website"),
        "beta":         _safe_num(profile_obj, "beta"),
    }

    quote_block = {
        "price":            current_price,
        "change_pct":       _safe_num(quote_obj, "changesPercentage") or _safe_num(quote_obj, "changePercentage"),
        "volume":           _safe_num(quote_obj, "volume"),
        "avg_volume":       _safe_num(quote_obj, "avgVolume"),
        "day_low":          _safe_num(quote_obj, "dayLow"),
        "day_high":         _safe_num(quote_obj, "dayHigh"),
        "year_low":         _safe_num(quote_obj, "yearLow"),
        "year_high":        _safe_num(quote_obj, "yearHigh"),
    }

    # Forward estimates (next 2 years)
    est_block = []
    if isinstance(estimates, list):
        # /stable/ returns newest-first with v-stable field names (revenueAvg, epsAvg…).
        # Sort ascending so the nearest forward years lead — the forward model needs them.
        _rows = sorted([e for e in estimates if e.get("date")], key=lambda e: e.get("date"))
        for e in _rows[:5]:
            est_block.append({
                "date":            e.get("date"),
                "revenue_avg":     _safe_num(e, "revenueAvg"),
                "revenue_low":     _safe_num(e, "revenueLow"),
                "revenue_high":    _safe_num(e, "revenueHigh"),
                "eps_avg":         _safe_num(e, "epsAvg"),
                "eps_low":         _safe_num(e, "epsLow"),
                "eps_high":        _safe_num(e, "epsHigh"),
                "ebitda_avg":      _safe_num(e, "ebitdaAvg"),
                "net_income_avg":  _safe_num(e, "netIncomeAvg"),
                "num_analysts_rev":_safe_num(e, "numAnalystsRevenue"),
                "num_analysts_eps":_safe_num(e, "numAnalystsEps"),
            })

    # ── Industry / sector P/E benchmark (real, from FMP snapshot) + company-vs-industry
    ind_bench = fetch_industry_benchmarks(company_block.get("industry"), company_block.get("sector"))

    def _rel_pct(co, bench):
        return round((co / bench - 1) * 100, 1) if (co and bench and bench > 0) else None

    industry_comparison = {
        "industry":     company_block.get("industry"),
        "sector":       company_block.get("sector"),
        "as_of":        ind_bench.get("as_of"),
        "industry_pe":  ind_bench.get("industry_pe"),
        "sector_pe":    ind_bench.get("sector_pe"),
        "company": {
            "pe":        valuation.get("pe_ttm"),
            "ps":        valuation.get("ps_ttm"),
            "pb":        valuation.get("pb_ttm"),
            "ev_ebitda": valuation.get("ev_ebitda"),
            "peg":       valuation.get("peg_ratio"),
            "roe_pct":   valuation.get("roe_ttm_pct"),
            "pe_5yr_avg": valuation.get("pe_5yr_avg"),
        },
        "pe_vs_industry_pct": _rel_pct(valuation.get("pe_ttm"), ind_bench.get("industry_pe")),
        "pe_vs_sector_pct":   _rel_pct(valuation.get("pe_ttm"), ind_bench.get("sector_pe")),
    }

    # ── Business mix (segment + geographic revenue) and price history (for the chart)
    _latest_rev = _safe_num(income_annual[0], "revenue") if income_annual else None
    business_mix = build_business_mix(raw.get("rev_product_seg"), raw.get("rev_geo_seg"), _latest_rev)
    price_history = compact_price_series(prices_eod)
    analyst_ratings = build_analyst_ratings(raw.get("grades_consensus"), raw.get("pt_summary"),
                                            raw.get("grades_actions"), raw.get("grades_hist"))

    # ── Next earnings date (drives options expiry selection + events calendar)
    import datetime as _dt
    _today_str = _dt.date.today().isoformat()
    _ecal = raw.get("earnings_cal") or []
    _future_earn = sorted([e for e in _ecal if e.get("date") and e["date"] >= _today_str],
                          key=lambda e: e["date"]) if isinstance(_ecal, list) else []
    next_earnings_date = _future_earn[0]["date"] if _future_earn else None

    # ── Options-implied expectations (OMON): implied move into earnings, IV, skew, P/C OI
    _spot = quote_block.get("price")
    try:
        options_expectations = build_options_expectations(ticker, _spot, next_earnings_date)
    except Exception as _e:
        print(f"[options] build failed: {type(_e).__name__}: {str(_e)[:120]}")
        options_expectations = None

    try:
        earnings_vol_edge = build_earnings_vol_edge(prices_eod, earnings, options_expectations, next_earnings_date)
    except Exception as _e:
        print(f"[evx] wrap failed: {_e}")
        earnings_vol_edge = {"status": "error"}

    payload = {
        "ticker":          ticker,
        "company":         company_block,
        "quote":           quote_block,
        "valuation":       valuation,
        "industry_comparison": industry_comparison,
        "business_mix":    business_mix,
        "analyst_ratings": analyst_ratings,
        "options_expectations": options_expectations,
        "earnings_vol_edge": earnings_vol_edge,
        "growth":          {**growth_metrics, **fcf_metrics, **qty_consistency},
        "margins":         margin_trend,
        "balance_quality": balance_qual,
        "cashflow_quality": cf_qual,
        "financial_health": health,
        "returns":         returns,
        "analyst_estimates": est_block,
        "peer_comparison": peer_comparison,
        "earnings_track_record": earnings_track_record,
        "capital_allocation": capital_allocation,
        "institutional_activity": institutional_activity,
        "earnings_call_excerpt": earnings_call,
        "statements_preview": {
            "income_top_5y":      compact_income(income_annual[:5]),
            "balance_top_5y":     compact_balance(balance_annual[:5]),
            "cashflow_top_5y":    compact_cf(cashflow_annual[:5]),
        },
    }

    # ── Call Claude for synthesis
    claude_synthesis = {}
    claude_elapsed = None
    claude_diag = {"raw_chars": 0, "parsed_keys": [], "parse_error": None,
                    "raw_head": "", "raw_tail": ""}
    try:
        t_claude = time.time()
        user_prompt = build_claude_prompt(payload)
        response_text = claude_call(CLAUDE_SYSTEM, user_prompt, max_tokens=6000)
        claude_elapsed = round(time.time() - t_claude, 2)
        claude_diag["raw_chars"] = len(response_text)
        claude_diag["raw_head"] = response_text[:400]
        claude_diag["raw_tail"] = response_text[-400:] if len(response_text) > 800 else ""
        try:
            claude_synthesis = parse_claude(response_text)
            claude_diag["parsed_keys"] = sorted((claude_synthesis or {}).keys())
        except Exception as parse_e:
            claude_diag["parse_error"] = str(parse_e)[:200]
            print(f"[claude] PARSE ERROR: {parse_e}")
            # Fall through with empty synthesis — page still renders the data
        print(f"[claude] {len(response_text)} chars in {claude_elapsed}s · "
              f"parsed {len(claude_diag['parsed_keys'])} top-level keys")
    except Exception as e:
        print(f"[claude] ERROR: {e}\n{traceback.format_exc()[:600]}")
        claude_synthesis = {
            "executive_summary": f"AI synthesis failed: {str(e)[:200]}. Underlying data is available below.",
            "verdict": {"rating": "HOLD", "conviction_grade": "C",
                         "verdict_rationale": "Manual review required — AI synthesis unavailable."},
        }

    # ── Assemble final document
    document = {
        "schema_version": "2.2",  # v2.2: + earnings_vol_edge (#6 realized-vs-implied + PEAD); v2.1: + industry_compass (Finviz industry join, stock GK ER, laggard-catchup, rate sensitivity)
        "technicals":         v2.get("technicals"),
        "liquidity_solvency": v2.get("liquidity"),
        "growth_vs_mcap":     v2.get("growth_vs_mcap"),
        "quant_risk":         v2.get("quant_risk"),
        "industry_compass":   v2.get("industry_compass"),
        "backlog":            v2.get("backlog"),
        "ticker":         ticker,
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "from_cache":     False,
        "regime_at_generation": load_macro_regime_snapshot(),  # Phase 2 attribution stamp
        "company":        company_block,
        "quote":          quote_block,
        "verdict":        claude_synthesis.get("verdict") or {},
        "executive_summary":   claude_synthesis.get("executive_summary"),
        "investment_thesis":   claude_synthesis.get("investment_thesis"),
        "risk_factors":        claude_synthesis.get("risk_factors"),
        "devils_advocate":     claude_synthesis.get("devils_advocate"),
        "valuation_assessment":claude_synthesis.get("valuation_assessment"),
        "peer_comparison_assessment": claude_synthesis.get("peer_comparison_assessment"),
        "industry_comparison_assessment": claude_synthesis.get("industry_comparison_assessment"),
        "business_mix_assessment": claude_synthesis.get("business_mix_assessment"),
        "relationships":       claude_synthesis.get("relationships"),
        "earnings_track_record_assessment": claude_synthesis.get("earnings_track_record_assessment"),
        "capital_allocation_assessment": claude_synthesis.get("capital_allocation_assessment"),
        "institutional_activity_assessment": claude_synthesis.get("institutional_activity_assessment"),
        "earnings_call_sentiment": claude_synthesis.get("earnings_call_sentiment"),
        "financial_health_summary": claude_synthesis.get("financial_health_summary"),
        "competitive_position":claude_synthesis.get("competitive_position"),
        "catalysts_12m":       claude_synthesis.get("catalysts_12m") or [],
        "invalidation_triggers": claude_synthesis.get("invalidation_triggers") or [],
        "forward_model":       claude_synthesis.get("forward_model"),
        "industry_comparison": industry_comparison,
        "business_mix":        business_mix,
        "analyst_ratings":     analyst_ratings,
        "options_expectations": options_expectations,
        "earnings_vol_edge": earnings_vol_edge,
        "price_history":       price_history,
        "scenarios":           _enrich_scenarios(
            claude_synthesis.get("scenarios"), current_price
        ),
        "valuation":           valuation,
        "growth":              {**growth_metrics, **fcf_metrics, **qty_consistency},
        "margins":             margin_trend,
        "balance_quality":     balance_qual,
        "cashflow_quality":    cf_qual,
        "financial_health":    health,
        "returns":             returns,
        "analyst_estimates":   est_block,
        "peer_comparison":     peer_comparison,
        "earnings_track_record": earnings_track_record,
        "capital_allocation":  capital_allocation,
        "institutional_activity": institutional_activity,
        "earnings_call": {
            "date":              earnings_call.get("date") if earnings_call else None,
            "fiscal_year":       earnings_call.get("fiscal_year") if earnings_call else None,
            "quarter":           earnings_call.get("quarter") if earnings_call else None,
            "full_chars":        earnings_call.get("full_chars") if earnings_call else None,
        } if earnings_call else None,
        "short_interest": {
            # FMP /stable/ doesn't expose short interest on the current plan
            # (verified ops 1141). Real data requires either:
            #   1. FINRA Gateway registration (in KHALID_ACTIONS.md pending list)
            #   2. FMP plan upgrade
            #   3. NYSE/Nasdaq direct feed
            # Until one of those, this field is a placeholder so the
            # frontend can render a transparent 'data gap' card rather
            # than silently omitting an important institutional signal.
            "available":          False,
            "reason":             "FMP /stable/short-interest is not exposed on the current plan tier. FINRA Gateway registration is the standard institutional source for short interest (% of float, days to cover, trend) — registration is pending in the operator's action backlog. Once enabled, this section will surface short interest % of float, days-to-cover, and historical trend.",
            "alternate_sources":  ["FINRA Gateway", "NYSE Short Interest XML feed", "Nasdaq Short Interest Reports"],
        },
        "statements": {
            "income_annual":     compact_income(income_annual),
            "balance_annual":    compact_balance(balance_annual),
            "cashflow_annual":   compact_cf(cashflow_annual),
            "income_quarterly":  compact_income(income_quarterly),
        },
        "metadata": {
            "data_sources_loaded":  n_ok,
            "data_sources_total":   len(raw),
            "fmp_endpoints":        list(raw.keys()),
            "fmp_endpoints_failed": [k for k, v in raw.items() if not v],
            "claude_model":         MODEL,
            "claude_elapsed_sec":   claude_elapsed,
            "claude_raw_chars":     claude_diag.get("raw_chars"),
            "claude_parsed_keys":   claude_diag.get("parsed_keys"),
            "claude_parse_error":   claude_diag.get("parse_error"),
            "claude_raw_head":      claude_diag.get("raw_head"),
            "claude_raw_tail":      claude_diag.get("raw_tail"),
            "total_elapsed_sec":    round(time.time() - t0, 2),
        },
    }

    # ── Cache to S3 — this is what async pollers wait for.
    # The bucket has ACLs disabled (BucketOwnerEnforced). Public access for
    # the equity-research/* prefix is granted via bucket policy
    # (PublicReadEquityResearch statement, see ops 1151), not per-object ACL.
    try:
        # /stable/quote lacks v3 avgVolume -- derive 50d avg from the doc's own
        # technicals volume series (real data, zero extra API). ops 3132.
        try:
            if (document.get("quote") or {}).get("avg_volume") is None:
                _vv = (((document.get("technicals") or {}).get("series") or {}).get("volume") or [])
                _vv = [x for x in _vv[-50:] if x]
                if len(_vv) >= 20:
                    document["quote"]["avg_volume"] = round(sum(_vv) / len(_vv))
        except Exception:
            pass

        body_bytes = json.dumps(document, default=str).encode("utf-8")

        s3.put_object(
            Bucket=S3_BUCKET, Key=cache_key,
            Body=body_bytes,
            ContentType="application/json",
            CacheControl=f"public, max-age={CACHE_TTL}",
        )
        print(f"[cache] WROTE {cache_key}")
        # ── History snapshot: also write a date-stamped copy.
        # Lets the research-backtest Lambda see meaningful day-over-day
        # returns instead of always 0% (the latest snapshot is always
        # "current" so entry == current price). New file per day; same-day
        # re-runs overwrite. Path: equity-research-history/YYYY-MM-DD/{TICKER}.json
        try:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            history_key = f"equity-research-history/{today}/{ticker.upper()}.json"
            s3.put_object(
                Bucket=S3_BUCKET, Key=history_key,
                Body=body_bytes,
                ContentType="application/json",
                CacheControl="public, max-age=86400",
            )
            print(f"[history] WROTE {history_key}")
        except Exception as e:
            # Don't fail the whole call if history write fails
            print(f"[history] write failed: {e}")
    except Exception as e:
        print(f"[cache] write failed: {e}")

    # Async internal invocations just confirm completion; the real payload
    # lives in S3 where the polling client will pick it up.
    if is_internal_async:
        return {
            "ok":      True,
            "ticker":  ticker,
            "wrote":   cache_key,
            "elapsed": round(time.time() - t0, 1),
        }
    return _http_ok(document)


def _enrich_scenarios(scenarios: Optional[dict], current_price: Optional[float]) -> Optional[dict]:
    """Augment Claude's scenarios block with derived metrics.

    Adds per-scenario upside_pct and a probability-weighted expected value
    + risk/reward ratio.

    Probability-weighted expected value (EV) is the formula a hedge fund
    PM uses to compare positions: a 35% bull case at +40% beats a 20% bull
    case at +60% even though the bull number looks lower. EV makes the
    comparison apples-to-apples.

    Risk/reward = (bull target - current) / (current - bear target). A
    well-structured trade has R/R >= 2 (you make 2x your worst-case loss
    if the bull plays out).
    """
    if not scenarios or not isinstance(scenarios, dict):
        return None
    if not current_price or current_price <= 0:
        return scenarios

    enriched = dict(scenarios)

    # Compute per-scenario upside %
    for key in ("bull_case", "base_case", "bear_case"):
        case = enriched.get(key)
        if not isinstance(case, dict):
            continue
        target = case.get("price_target_12m")
        try:
            if target is not None:
                target_f = float(target)
                case["upside_pct"] = round((target_f / current_price - 1) * 100, 1)
        except (TypeError, ValueError):
            pass

    # Probability-weighted expected value
    try:
        ev = 0.0
        total_prob = 0.0
        for key in ("bull_case", "base_case", "bear_case"):
            case = enriched.get(key) or {}
            target = case.get("price_target_12m")
            prob = case.get("probability_pct")
            if target is not None and prob is not None:
                ev += float(target) * (float(prob) / 100.0)
                total_prob += float(prob)
        # If probabilities don't sum cleanly to 100, normalize
        if total_prob > 0 and abs(total_prob - 100) > 1:
            ev = ev * (100.0 / total_prob)
        enriched["expected_value_12m"] = round(ev, 2) if ev > 0 else None
        if enriched.get("expected_value_12m"):
            enriched["expected_value_upside_pct"] = round(
                (enriched["expected_value_12m"] / current_price - 1) * 100, 1
            )
        enriched["probability_sum"] = round(total_prob, 1)
    except Exception:
        pass

    # Risk/reward — purely a function of bull and bear targets
    try:
        bull = (enriched.get("bull_case") or {}).get("price_target_12m")
        bear = (enriched.get("bear_case") or {}).get("price_target_12m")
        if bull and bear and bear < current_price < bull:
            reward = float(bull) - current_price
            risk = current_price - float(bear)
            if risk > 0:
                enriched["risk_reward_ratio"] = round(reward / risk, 2)
    except Exception:
        pass

    return enriched


def _iso_to_epoch(iso_str: Optional[str]) -> float:
    if not iso_str: return 0
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0


def _http_ok(body: dict) -> dict:
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=3600",
        },
        "body": json.dumps(body, default=str),
    }


def _http_error(status: int, msg: str) -> dict:
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps({"error": msg}),
    }
