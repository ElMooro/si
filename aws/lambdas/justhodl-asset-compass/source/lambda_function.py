"""justhodl-asset-compass — forward-looking cross-asset Expected-Return +
Asymmetry engine. The capital-allocation layer of the fleet.

Every sibling engine answers "what is happening?" (regime, flows, stress,
dislocations). None answers the allocator's question: "over the NEXT 12
months, what is each investable asset class expected to return, and where
is the upside/downside skew most favorable?" This engine does, with every
number decomposed and traceable — Grinold-Kroner style, the way GMO /
Research Affiliates publish capital-market assumptions, plus a formalized
asymmetry model ("huge upside, small downside") with an explicit
survival gate so deep-drawdown value traps are never auto-blessed.

BLOCKS
══════
1. MACRO FORWARD (FRED, all market-implied — not opinions):
   • rf_now            = 1y Treasury (DGS1)
   • rf_1y_forward     = implied 1y rate 1y ahead from the curve:
                         (1+DGS2)^2/(1+DGS1) − 1   (pure expectations)
   • infl_1y_expected  = Cleveland Fed 1-yr expected inflation (EXPINF1YR,
                         fallback T10YIE)
   • real_1y_forward   = rf_1y_forward − infl_1y_expected
   • growth pulse      = payrolls 3m momentum z + retail sales 3m z −
                         unemployment 3m rise (each z vs its own 10y
                         history) → real-growth proxy 0–3% for equity g.
2. PER-ASSET 1Y EXPECTED RETURN (er_components published for every asset):
   • Cash: DGS1.  • UST via IEF/TLT: y10 − D·Δy10_fwd (curve-implied Δ).
   • TIPS: DFII10 + expected inflation − D·Δreal.
   • Gold: expected-inflation anchor + β(gold, real-rate)·Δreal_fwd —
     β is OLS-estimated from raw daily data (must rediscover the proven
     negative gold↔DFII10 relation or the ops verify fails) + trend tilt.
   • Silver: gold ER + gold/silver-ratio mean-reversion (10y z).
   • Miners: β(GDX,GLD)·gold ER − cost drag.
   • REITs: TTM distribution yield + inflation + escalator (assumption,
     flagged) — rate-sensitivity noted.
   • Equity ETFs (SPY/QQQ/IWM/EFA/EEM): TTM dividend yield + nominal
     growth (infl + growth-pulse real g) + 200-dma stretch reversion.
   • Commodities (DBC/USO/CPER), BTC/ETH: NO fabricated ER — no
     yield/earnings anchor exists, so er_1y=None with an honest note;
     ranked on asymmetry/trend/breakout instead. BTC/ETH consume
     data/crypto-cycle-risk.json for cycle context; n_cycles≈4 flagged.
3. ASYMMETRY (all assets): upside = recovery-to-3y-high; downside =
   distance to the asset's own 95th-percentile historical drawdown depth;
   raw ratio + drawdown-percentile + trend confirmation → asym_score
   0-100. SURVIVAL GATE: structural assets (broad indices, gold, USTs)
   are gated only by trend; narrow/cyclical assets (GDX, USO, SLV, BTC,
   ETH, CPER) must show trend confirmation (px>50dma or rising 50dma)
   before status=ACTIONABLE, else WATCH — "down 75%" alone is upside on
   things that cannot die, and a value trap on things that can.
4. BREAKOUT SCAN (incl. the requested gold/silver charts): 20d Bollinger
   bandwidth percentile (squeeze), 52w range position, 60d-high breaks
   with volume thrust → SQUEEZE / COILED / BREAKOUT / EXTENDED / TRENDING
   / NONE.
5. SIBLING FUSION (warn-only, defensive): risk-regime label,
   cross-asset-regime RORO, cross-asset-rv dislocations count attached
   for context.

DATA: FRED (key via env/SSM), Polygon daily aggs + dividends, CoinGecko
(keyless) for BTC/ETH, sibling S3 JSONs. Real, auto-updating, no demo
data. OUTPUT: data/asset-compass.json  Schedule: daily 22:15 UTC.
STATUS: PROVISIONAL per Edge-Accuracy standard — enters the alpha-grading
loop; no edge is claimed until excess-vs-SPY stats prove it.
"""
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

BUCKET   = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY  = "data/asset-compass.json"
S3       = boto3.client("s3", region_name="us-east-1")
SSM      = boto3.client("ssm", region_name="us-east-1")
POLY_KEY = os.environ.get("POLYGON_API_KEY") or os.environ.get("POLYGON_KEY", "")
FRED_URL = "https://api.stlouisfed.org/fred/series/observations"

UNIVERSE = [
    # ticker, class, label, duration, structural?, er_model
    ("CASH", "cash",      "Cash (1y T-bill)",      0.0,  True,  "cash"),
    ("IEF",  "bonds",     "US Treasuries 7-10y",   7.6,  True,  "ust"),
    ("TLT",  "bonds",     "US Treasuries 20y+",   16.5,  True,  "ust"),
    ("TIP",  "bonds",     "US TIPS",               6.9,  True,  "tips"),
    ("HYG",  "credit",    "US High Yield (junk)",  3.6,  True,  "credit"),
    ("LQD",  "credit",    "US IG Corporates",      8.4,  True,  "credit"),
    ("EMB",  "credit",    "EM Sovereign Debt",     7.2,  True,  "credit"),
    ("MUB",  "credit",    "US Munis",              6.4,  True,  "credit"),
    ("GLD",  "metals",    "Gold",                  0.0,  True,  "gold"),
    ("SLV",  "metals",    "Silver",                0.0,  False, "silver"),
    ("GDX",  "metals",    "Gold Miners",           0.0,  False, "miners"),
    ("PPLT", "metals",    "Platinum",              0.0,  False, "none"),
    ("VNQ",  "reits",     "US REITs",              0.0,  True,  "reit"),
    ("AMLP", "reits",     "Midstream MLPs",        0.0,  True,  "mlp"),
    ("SPY",  "equities",  "US Large Cap",          0.0,  True,  "equity"),
    ("QQQ",  "equities",  "US Growth/Tech",        0.0,  True,  "equity"),
    ("IWM",  "equities",  "US Small Cap",          0.0,  True,  "equity"),
    ("EFA",  "equities",  "Intl Developed",        0.0,  True,  "equity"),
    ("EEM",  "equities",  "Emerging Markets",      0.0,  True,  "equity"),
    ("EWJ",  "equities",  "Japan",                 0.0,  True,  "equity_intl"),
    ("FXI",  "equities",  "China Large Cap",       0.0,  False, "equity_intl"),
    ("INDA", "equities",  "India",                 0.0,  True,  "equity_intl"),
    ("DBC",  "commodities","Broad Commodities",    0.0,  True,  "none"),
    ("DBA",  "commodities","Agriculture",          0.0,  True,  "none"),
    ("USO",  "commodities","Crude Oil",            0.0,  False, "none"),
    ("UNG",  "commodities","Natural Gas",          0.0,  False, "none"),
    ("CPER", "commodities","Copper",               0.0,  False, "none"),
    ("URA",  "commodities","Uranium Miners",       0.0,  False, "none"),
    ("BTC",  "crypto",    "Bitcoin",               0.0,  False, "none"),
    ("ETH",  "crypto",    "Ethereum",              0.0,  False, "none"),
    ("SOL",  "crypto",    "Solana",                0.0,  False, "none"),
]

# credit ER inputs: long-run annual credit-loss assumptions (published; the
# spread pays you for these) — HY ~2.3%/yr, IG ~0.15%, EM sov ~0.60%,
# munis ~0.10% (Moody's/S&P long-run default×LGD ballparks, flagged as
# ASSUMPTION in components), plus which FRED OAS series gives live context.
CREDIT_META = {
    "HYG": {"loss_pct": 2.30, "oas_fred": "BAMLH0A0HYM2", "oas_label": "HY OAS"},
    "LQD": {"loss_pct": 0.15, "oas_fred": "BAMLC0A0CM",  "oas_label": "IG OAS"},
    "EMB": {"loss_pct": 0.60, "oas_fred": None,           "oas_label": None},
    "MUB": {"loss_pct": 0.10, "oas_fred": None,           "oas_label": None},
}

# structurally decaying futures products: the ETF can bleed even when spot
# goes sideways (contango roll). UNG is the canonical case — barred from
# ACTIONABLE outright; USO carries the milder tactical-only flag.
DECAY = {
    "UNG": ("STRUCTURAL_DECAY: contango roll drag — trade in days-weeks, "
            "never buy-and-hold", True),   # True = hard-bar from ACTIONABLE
    "USO": ("ROLL_DRAG: futures roll cost — tactical exposure only", False),
}

# investment-horizon map: (suggested hold, why that horizon) — the basis is
# the point; a horizon without a mechanism is just a guess.
HORIZON = {
    "CASH": ("any", "parking; no path risk"),
    "IEF":  ("~8y", "hold to duration immunizes the rate path"),
    "TLT":  ("~16y or tactical", "duration immunization is impractical at 16y — "
             "most holders are making a tactical rate bet"),
    "TIP":  ("~7y", "hold to duration; inflation accrual does the work"),
    "HYG":  ("2-4y", "a full credit-spread cycle; carry needs time to beat defaults"),
    "LQD":  ("3-5y", "spread cycle + duration; carry compounds slowly"),
    "EMB":  ("2-4y", "EM spread cycle; expect drawdowns around dollar spikes"),
    "MUB":  ("3-5y", "tax-adjusted carry compounds; low turnover asset"),
    "GLD":  ("regime asset", "hold while real rates fall or as portfolio "
             "insurance — no fixed horizon"),
    "SLV":  ("6-18m tactical", "high-beta gold proxy; ride the metal regime, "
             "exit when it turns"),
    "GDX":  ("6-18m tactical", "leveraged gold expression via miner equity"),
    "PPLT": ("6-18m tactical", "thin industrial market; cyclical demand"),
    "VNQ":  ("3-5y", "income + escalator compounding; rate-sensitive entry matters"),
    "AMLP": ("3-5y", "distribution compounding; volume-linked toll-road cash flows"),
    "SPY":  ("3-5y minimum", "the equity premium needs time to realize"),
    "QQQ":  ("3-5y minimum", "same premium, longer duration — bigger drawdowns"),
    "IWM":  ("3-5y minimum", "small-cap premium is lumpy; patience required"),
    "EFA":  ("3-5y minimum", "equity premium + currency noise"),
    "EEM":  ("3-5y minimum", "EM premium is real but violent"),
    "EWJ":  ("2-4y", "governance/BOJ normalization theme horizon"),
    "FXI":  ("1-3y tactical", "policy-cycle driven; thesis-dependent"),
    "INDA": ("3-5y+", "structural growth compounding story"),
    "DBC":  ("1-6m tactical", "futures roll — not a buy-and-hold vehicle"),
    "DBA":  ("1-6m tactical", "futures roll; weather/season driven"),
    "USO":  ("weeks-months", "tactical only — roll drag taxes long holds"),
    "UNG":  ("days-weeks TRADE ONLY", "structural contango decay"),
    "CPER": ("1-6m tactical", "futures roll; electrification theme via COPX "
             "is the long-hold version"),
    "URA":  ("1-3y thematic", "miner equity — fuel-cycle repricing takes quarters"),
    "BTC":  ("full cycle ~4y", "halving cycle; size for -70% interim drawdowns"),
    "ETH":  ("full cycle ~4y", "same cycle, higher beta"),
    "SOL":  ("full cycle ~4y", "youngest history of the three — thinnest evidence"),
}

# ───────────────────────── http / data plumbing ─────────────────────────

def _http(url, timeout=25, tries=2):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "justhodl-asset-compass/1.0",
                "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", "replace"))
        except Exception:
            if i == tries - 1:
                return None
            time.sleep(1.2)


def get_fred_key():
    for k in ("FRED_API_KEY", "FRED_KEY", "FRED_TOKEN"):
        if os.environ.get(k):
            return os.environ[k]
    try:
        return SSM.get_parameter(Name="/justhodl/fred/api-key",
                                 WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return ""


def fred_series(series_id, start, key):
    """→ list of (date_str, float) ascending; '.' rows skipped."""
    q = urllib.parse.urlencode({
        "series_id": series_id, "api_key": key, "file_type": "json",
        "observation_start": start, "sort_order": "asc", "limit": 100000})
    d = _http(f"{FRED_URL}?{q}") or {}
    out = []
    for o in d.get("observations") or []:
        v = o.get("value")
        if v not in (None, "", "."):
            try:
                out.append((o["date"], float(v)))
            except Exception:
                pass
    return out


def polygon_daily(ticker, years):
    frm = (datetime.now(timezone.utc) - timedelta(days=int(years * 365.25))
           ).strftime("%Y-%m-%d")
    to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{frm}/{to}?adjusted=true&sort=asc&limit=50000&apiKey={POLY_KEY}")
    d = _http(url) or {}
    rows = d.get("results") or []
    return [{"t": r["t"], "c": float(r["c"]), "h": float(r.get("h", r["c"])),
             "v": float(r.get("v", 0.0))} for r in rows
            if isinstance(r.get("c"), (int, float))]


def polygon_ttm_div(ticker, price):
    url = (f"https://api.polygon.io/v3/reference/dividends?ticker={ticker}"
           f"&limit=20&order=desc&sort=ex_dividend_date&apiKey={POLY_KEY}")
    d = _http(url) or {}
    cutoff = (datetime.now(timezone.utc) - timedelta(days=370)
              ).strftime("%Y-%m-%d")
    ttm = sum(float(r.get("cash_amount") or 0.0)
              for r in (d.get("results") or [])
              if str(r.get("ex_dividend_date", "")) >= cutoff)
    if price and ttm > 0:
        return round(ttm / price * 100.0, 2)
    return None


def crypto_daily(coin_id, poly_ticker):
    """CoinGecko (keyless, backoff) -> Polygon X: pair fallback. Real data
    on either path; [] only if both fail (row then flags NO_DATA)."""
    for attempt in range(3):
        d = _http(f"https://api.coingecko.com/api/v3/coins/{coin_id}"
                  f"/market_chart?vs_currency=usd&days=1095&interval=daily",
                  timeout=30, tries=1)
        if d and d.get("prices"):
            return [{"t": int(p[0]), "c": float(p[1]), "h": float(p[1]),
                     "v": 0.0} for p in d["prices"] if p and p[1]]
        time.sleep(2.0 + attempt * 2.0)
    return polygon_daily(poly_ticker, 3)


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}

# ─────────────────────────── pure math core ───────────────────────────
# (module-level pure functions: unit-testable, no I/O, no magic numbers
#  beyond published weights)

def implied_1y1y(y1_pct, y2_pct):
    """Curve-implied 1y rate 1y forward, in %, from 1y and 2y spot."""
    y1, y2 = y1_pct / 100.0, y2_pct / 100.0
    return round((((1.0 + y2) ** 2) / (1.0 + y1) - 1.0) * 100.0, 3)


def implied_fwd_10y(y1_pct, y10_pct):
    """Approx expected 10y yield in 1y: solve 10·y10 = 1·y1 + 9·f."""
    return round((10.0 * y10_pct - y1_pct) / 9.0, 3)


def bond_er(y_now, duration, dy_expected_pp):
    """1y total-return est: carry − duration × expected yield change."""
    return round(y_now - duration * dy_expected_pp, 2)


def ols_beta(xs, ys):
    """Slope of y on x. Pure python, returns (beta, n)."""
    n = min(len(xs), len(ys))
    if n < 60:
        return None, n
    xs, ys = xs[-n:], ys[-n:]
    mx, my = sum(xs) / n, sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    var = sum((x - mx) ** 2 for x in xs)
    if var <= 0:
        return None, n
    return cov / var, n


def zscore_latest(series, lookback=None):
    s = series[-lookback:] if lookback else series
    n = len(s)
    if n < 30:
        return None
    m = sum(s) / n
    sd = math.sqrt(sum((v - m) ** 2 for v in s) / n)
    if sd <= 0:
        return None
    return round((s[-1] - m) / sd, 2)


def drawdown_series(closes):
    dds, peak = [], -1e18
    for c in closes:
        peak = max(peak, c)
        dds.append(c / peak - 1.0)
    return dds


def percentile_of(value, population):
    if not population:
        return None
    below = sum(1 for v in population if v <= value)
    return round(below / len(population) * 100.0, 1)


def sma(vals, n):
    if len(vals) < n:
        return None
    return sum(vals[-n:]) / n


def asymmetry(price, high_3y, dd_now, dd_hist):
    """upside %, downside %, raw ratio. downside = fall to the asset's own
    95th-pctile historical drawdown depth measured from the SAME peak."""
    upside = min((high_3y / price - 1.0) * 100.0, 200.0) if price else None
    deep = sorted(dd_hist)
    if not deep or price is None:
        return upside, None, None
    p95_depth = deep[max(0, int(0.05 * len(deep)) - 1)]  # e.g. −0.42
    p95_depth = min(p95_depth, -0.05)
    downside = max(((1.0 + p95_depth) / (1.0 + dd_now) - 1.0) * 100.0, -90.0)
    if downside >= -0.5:
        ratio = 25.0
    else:
        ratio = min(round((upside or 0.0) / abs(downside), 2), 25.0)
    return (round(upside, 1) if upside is not None else None,
            round(downside, 1), ratio)


def breakout_state(closes, highs, vols):
    """SQUEEZE / COILED / BREAKOUT / EXTENDED / TRENDING / NONE + metrics."""
    if len(closes) < 260:
        return {"state": "NONE", "note": "insufficient history"}
    px = closes[-1]
    # 20d Bollinger bandwidth series over last year
    bw = []
    for i in range(len(closes) - 252, len(closes)):
        w = closes[i - 19:i + 1]
        if len(w) < 20:
            continue
        m = sum(w) / 20.0
        sd = math.sqrt(sum((v - m) ** 2 for v in w) / 20.0)
        bw.append((4.0 * sd / m) if m else 0.0)
    bw_pct = percentile_of(bw[-1], bw) if bw else None
    hi52, lo52 = max(closes[-252:]), min(closes[-252:])
    rng_pos = round((px - lo52) / (hi52 - lo52) * 100.0, 1) if hi52 > lo52 else None
    hi60_prior = max(highs[-61:-1])
    v20 = sma(vols, 20) or 0.0
    v_prior = (sum(vols[-120:-20]) / 100.0) if len(vols) >= 120 else 0.0
    vol_ratio = round(v20 / v_prior, 2) if v_prior > 0 else None
    s50 = sma(closes, 50)
    ext_pct = round((px / s50 - 1.0) * 100.0, 1) if s50 else None
    state = "NONE"
    if px > hi60_prior and (vol_ratio is None or vol_ratio >= 1.15):
        state = "BREAKOUT"
    elif ext_pct is not None and ext_pct > 15.0:
        state = "EXTENDED"
    elif bw_pct is not None and bw_pct <= 10.0:
        state = "SQUEEZE"
    elif bw_pct is not None and bw_pct <= 25.0 and px >= 0.95 * hi60_prior:
        state = "COILED"
    elif s50 and px > s50 and rng_pos is not None and rng_pos > 60.0:
        state = "TRENDING"
    return {"state": state, "bb_width_pctile_1y": bw_pct,
            "range_52w_pos_pct": rng_pos,
            "dist_to_52w_high_pct": round((hi52 / px - 1.0) * 100.0, 1),
            "vol_20d_ratio": vol_ratio, "ext_vs_50dma_pct": ext_pct}


def trend_of(closes):
    s50, s200 = sma(closes, 50), sma(closes, 200)
    px = closes[-1] if closes else None
    if not (px and s50 and s200):
        return {"label": "UNKNOWN", "ok": False}
    s50_prev = sma(closes[:-10], 50)
    rising50 = bool(s50_prev and s50 > s50_prev)
    if px > s50 > s200:
        lab = "UPTREND"
    elif px > s200:
        lab = "RECOVERING"
    elif px > s50:
        lab = "BASING"
    else:
        lab = "DOWNTREND"
    return {"label": lab, "ok": bool(px > s50 or rising50),
            "px_vs_50dma_pct": round((px / s50 - 1.0) * 100.0, 1),
            "px_vs_200dma_pct": round((px / s200 - 1.0) * 100.0, 1),
            "sma50_rising": rising50}


def growth_pulse(pay_z, ret_z, un_chg):
    """Real-growth proxy for equity g, 0–3%. Weights published here."""
    raw = 1.5 + 0.5 * (pay_z or 0.0) + 0.3 * (ret_z or 0.0) \
        - 0.6 * max(un_chg or 0.0, 0.0) * 10.0
    return round(min(max(raw, 0.0), 3.0), 2)


def asym_score(ratio, dd_pctile, trend_ok, bo_state):
    """0-100: 40% ratio (cap 5×), 30% drawdown depth pctile (deeper=more),
    15% trend confirmation, 15% coiled/breakout bonus."""
    if ratio is None:
        return None
    r = min(ratio, 5.0) / 5.0 * 40.0
    d = ((100.0 - dd_pctile) / 100.0 * 30.0) if dd_pctile is not None else 15.0
    t = 15.0 if trend_ok else 0.0
    b = 15.0 if bo_state in ("COILED", "BREAKOUT", "SQUEEZE") else 0.0
    return round(r + d + t + b, 1)


def corr_returns(a_closes, b_closes, n=90):
    """Pearson correlation of daily % changes over the last n overlapping
    observations. Aligns from the tail; None if too thin."""
    if not a_closes or not b_closes:
        return None
    m = min(len(a_closes), len(b_closes), n + 1)
    if m < 40:
        return None
    ra = [a_closes[-m + i + 1] / a_closes[-m + i] - 1.0 for i in range(m - 1)
          if a_closes[-m + i]]
    rb = [b_closes[-m + i + 1] / b_closes[-m + i] - 1.0 for i in range(m - 1)
          if b_closes[-m + i]]
    k = min(len(ra), len(rb))
    if k < 40:
        return None
    ra, rb = ra[-k:], rb[-k:]
    ma, mb = sum(ra) / k, sum(rb) / k
    cov = sum((x - ma) * (y - mb) for x, y in zip(ra, rb))
    va = sum((x - ma) ** 2 for x in ra)
    vb = sum((y - mb) ** 2 for y in rb)
    if va <= 0 or vb <= 0:
        return None
    return round(cov / math.sqrt(va * vb), 2)


def credit_er(ttm_yield, loss_pct, dur, dy10):
    """Credit 12m ER: carry − long-run credit-loss assumption − duration ×
    expected parallel shift (dy10 as the shift proxy — flagged assumption)."""
    if ttm_yield is None:
        return None, {}
    dur_fx = round(-dur * (dy10 or 0.0), 2)
    er = round(ttm_yield - loss_pct + dur_fx, 2)
    return er, {"carry_ttm_yield_pct": ttm_yield,
                "credit_loss_assumption_pct": -loss_pct,
                "duration_effect_pct": dur_fx, "duration": dur,
                "assumption": "long-run default×LGD loss rate; parallel "
                              "curve shift (dy10) applied to duration"}


def build_read(row, rf, rf_dir, dreal, oas_pctile=None):
    """Deterministic bull/bear case per asset from the engine's OWN numbers.
    Zero LLM. Every line is traceable to a published field."""
    bull, bear = [], []
    er = row.get("er_1y_pct")
    s = row.get("asym") or {}
    tr = row.get("trend") or {}
    bo = (row.get("breakout") or {}).get("state")
    flags = row.get("flags") or []
    corr = row.get("corr_spy_90d")
    dur = row.get("_dur") or 0.0
    tkr = row.get("ticker")

    # hurdle vs cash — the first institutional question
    if er is not None and rf is not None:
        ex = round(er - rf, 1)
        row["excess_vs_cash_pp"] = ex
        dn = s.get("downside_pct")
        if dn is not None and dn < -1:
            row["premium_per_unit_downside"] = round(ex / abs(dn), 2)
        if ex >= 2.0:
            bull.append("Pays +%.1fpp over cash for the risk" % ex)
        elif ex <= 0.0:
            bear.append("Fails the cash hurdle: ER %.1f%% vs %.1f%% "
                        "risk-free" % (er, rf))
    if er is None:
        bear.append("No yield/earnings anchor — ranked on asymmetry and "
                    "trend only")

    # asymmetry + gate
    ratio = s.get("ratio")
    if ratio is not None and ratio >= 3 and s.get("status") == "ACTIONABLE":
        bull.append("Asymmetric setup: %.0fx upside/downside with trend "
                    "confirmed" % ratio)
    if ratio is not None and ratio >= 3 and s.get("status") == "WATCH":
        bear.append("Max asymmetry but still in a downtrend — survival "
                    "gate holding it at WATCH")
    if tr.get("label") == "DOWNTREND":
        bear.append("Price below falling 50dma — no trend confirmation")
    if tr.get("label") == "UPTREND":
        bull.append("Confirmed uptrend (price > 50dma > 200dma)")
    if bo in ("SQUEEZE", "COILED"):
        bull.append("%s: volatility compressed near the top of the range — "
                    "energy stored" % bo)
    if bo == "BREAKOUT":
        bull.append("Volume-confirmed breakout through the 60d ceiling")
    if bo == "EXTENDED":
        bear.append("Extended >15%% above the 50dma — chase risk")

    # rates
    if dur >= 5 and rf_dir == "HIGHER":
        bear.append("Long duration (%.0fy) against a market pricing rates "
                    "HIGHER over 12m" % dur)
    if dur >= 5 and rf_dir == "LOWER":
        bull.append("Long duration (%.0fy) with the market pricing rates "
                    "LOWER — convexity tailwind" % dur)

    # gold/real-rate mechanism
    if tkr in ("GLD", "SLV", "GDX") and dreal is not None:
        if dreal < -0.05:
            bull.append("Market-implied real rates FALLING (%.2fpp) — the "
                        "proven gold driver is a tailwind" % dreal)
        elif dreal > 0.05:
            bear.append("Market-implied real rates RISING (%.2fpp) — the "
                        "proven gold headwind" % dreal)

    # credit spread cushion
    if oas_pctile is not None:
        if oas_pctile >= 70:
            bull.append("Spreads at the %.0fth pctile of 10y — wide "
                        "cushion against losses" % oas_pctile)
        elif oas_pctile <= 25:
            bear.append("Spreads at the %.0fth pctile of 10y — thin "
                        "cushion, priced for perfection" % oas_pctile)

    # diversification
    if corr is not None and er is not None:
        if corr <= 0.30:
            bull.append("Low correlation to SPY (%.2f) — genuine "
                        "diversifier right now" % corr)
        elif corr >= 0.85 and tkr != "SPY":
            bear.append("Correlation %.2f to SPY — adds no diversification"
                        % corr)

    # structural flags
    for f in flags:
        if f.startswith("STRUCTURAL_DECAY"):
            bear.append("Bleeds on contango roll even in flat spot — "
                        "buy-and-hold is structurally negative")
        elif f.startswith("ROLL_DRAG"):
            bear.append("Futures roll cost taxes long holds — tactical "
                        "vehicle only")
        elif f.startswith("LOW_N"):
            bear.append("Only ~4 cycles of history — asymmetry stats are "
                        "low-confidence")
        elif f.startswith("ASSUMPTION"):
            bear.append("ER leans on a modeled assumption (%s)"
                        % f.split(":", 1)[-1].strip())

    st = s.get("status")
    net = ("Setup live: %s, hold %s." % (st, (row.get("horizon") or {})
           .get("hold", "—")) if st == "ACTIONABLE" else
           "Asymmetry present, trigger absent — wait for trend."
           if st == "WATCH" else
           "Carry it for the premium, not the timing."
           if (er is not None and rf is not None and er - rf >= 2
               and st != "ACTIONABLE") else
           "Nothing compelling at today's prices.")
    return {"bull": bull[:4], "bear": bear[:4], "net": net}

# ─────────────────────────────── engine ───────────────────────────────

def _mom_z(series_vals, step=3):
    """z of the latest 3-period momentum vs its own 10y momentum history."""
    if len(series_vals) < step + 40:
        return None, None
    moms = [(series_vals[i] / series_vals[i - step] - 1.0) * 100.0
            for i in range(step, len(series_vals)) if series_vals[i - step]]
    return zscore_latest(moms), round(moms[-1], 2)


def build_macro_forward(fkey):
    start10 = (datetime.now(timezone.utc) - timedelta(days=3660)
               ).strftime("%Y-%m-%d")
    s = {sid: fred_series(sid, start10, fkey)
         for sid in ("DGS1", "DGS2", "DGS10", "DFII10", "T10YIE",
                     "EXPINF1YR", "PAYEMS", "RSAFS", "UNRATE")}
    last = {k: (v[-1][1] if v else None) for k, v in s.items()}
    y1, y2, y10 = last.get("DGS1"), last.get("DGS2"), last.get("DGS10")
    infl = last.get("EXPINF1YR") or last.get("T10YIE")
    infl_src = "EXPINF1YR (Cleveland Fed 1y)" if last.get("EXPINF1YR") \
        else "T10YIE (10y breakeven fallback)"
    rf_fwd = implied_1y1y(y1, y2) if (y1 is not None and y2 is not None) else None
    y10_fwd = implied_fwd_10y(y1, y10) if (y1 is not None and y10 is not None) else None
    pay_z, pay_mom = _mom_z([v for _, v in s.get("PAYEMS") or []])
    ret_z, ret_mom = _mom_z([v for _, v in s.get("RSAFS") or []])
    un = [v for _, v in s.get("UNRATE") or []]
    un_chg = round(un[-1] - un[-4], 2) if len(un) >= 4 else None
    g = growth_pulse(pay_z, ret_z, un_chg)
    real_fwd = round(rf_fwd - infl, 2) if (rf_fwd is not None and infl is not None) else None
    dfii = last.get("DFII10")
    return {
        "rf_now_pct": y1, "rf_1y_forward_pct": rf_fwd,
        "rf_direction_next_year": (None if (rf_fwd is None or y1 is None) else
                                   ("LOWER" if rf_fwd < y1 - 0.10 else
                                    "HIGHER" if rf_fwd > y1 + 0.10 else "FLAT")),
        "y10_now_pct": y10, "y10_1y_forward_pct": y10_fwd,
        "infl_1y_expected_pct": infl, "infl_source": infl_src,
        "real_1y_forward_pct": real_fwd, "real_10y_now_pct": dfii,
        "delta_real_expected_pp": (round(real_fwd - (y1 - infl), 2)
                                   if None not in (real_fwd, y1, infl) else None),
        "growth": {"payrolls_3m_mom_pct": pay_mom, "payrolls_z": pay_z,
                   "retail_3m_mom_pct": ret_mom, "retail_z": ret_z,
                   "unrate_3m_chg_pp": un_chg,
                   "real_growth_proxy_pct": g},
        "note": ("All forward values are market-implied (Treasury curve, "
                 "Cleveland Fed expectations) — the market's next-12m "
                 "pricing, not a forecast by this engine."),
        "_fred_last": last,
    }


# ══════════════════════ v1.2: ledger · matrix · scenarios ══════════════════════
LEDGER_KEY = "data/compass-forecast-ledger.json"


def _bars_date_map(bl):
    out = {}
    for b in bl or []:
        try:
            out[datetime.fromtimestamp(b["t"] / 1000, tz=timezone.utc)
                .strftime("%Y-%m-%d")] = float(b["c"])
        except Exception:
            continue
    return out


def factor_betas(bars, closes, fkey):
    """Empirical per-asset sensitivities from 3y of REAL daily data:
    rate_beta  = % move per +100bp Δ 10y nominal (DGS10)
    bei_beta   = % move per +100bp Δ 10y breakeven (T10YIE)
    spy_beta   = beta to SPY daily returns
    All via the same OLS used by the gold acid test. None when <120 obs."""
    start = (datetime.now(timezone.utc) - timedelta(days=1150)
             ).strftime("%Y-%m-%d")
    y10 = dict(fred_series("DGS10", start, fkey))
    bei = dict(fred_series("T10YIE", start, fkey))
    fdates = sorted(set(y10) & set(bei))
    spy_ret = None
    if closes.get("SPY") and len(closes["SPY"]) > 130:
        c = closes["SPY"]
        spy_ret = [c[i + 1] / c[i] - 1.0 for i in range(len(c) - 1) if c[i]]
    per = {}
    for tkr, bl in bars.items():
        dmap = _bars_date_map(bl)
        rets, dys, dbs = [], [], []
        prev = None
        for d in fdates:
            if d not in dmap:
                continue
            if prev is not None:
                p0, p1 = dmap[prev], dmap[d]
                if p0:
                    rets.append(p1 / p0 - 1.0)
                    dys.append(y10[d] - y10[prev])
                    dbs.append(bei[d] - bei[prev])
            prev = d
        rate_b = bei_b = None
        if len(rets) >= 120:
            rb, _ = ols_beta(dys, rets)
            bb, _ = ols_beta(dbs, rets)
            rate_b = round(rb * 100, 2) if rb is not None else None
            bei_b = round(bb * 100, 2) if bb is not None else None
        spy_b = None
        c = closes.get(tkr)
        if spy_ret and c and len(c) > 130:
            r = [c[i + 1] / c[i] - 1.0 for i in range(len(c) - 1) if c[i]]
            k = min(len(r), len(spy_ret))
            b, _ = ols_beta(spy_ret[-k:], r[-k:])
            spy_b = round(b, 2) if b is not None else None
        per[tkr] = {"rate_beta_pct_per_100bp": rate_b,
                    "bei_beta_pct_per_100bp": bei_b,
                    "spy_beta": spy_b,
                    "obs": len(rets)}
    return per


def corr_matrix_31(closes):
    tks = sorted(t for t, c in closes.items() if c and len(c) > 95)
    n = len(tks)
    M = [[1.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            c = corr_returns(closes[tks[i]], closes[tks[j]], 90)
            v = round(c, 3) if c is not None else None
            M[i][j] = M[j][i] = v
    # greedy clusters at 0.6
    used, clusters = set(), []
    for i in range(n):
        if i in used:
            continue
        grp = [i]
        used.add(i)
        for j in range(n):
            if j in used:
                continue
            if all(isinstance(M[k][j], float) and M[k][j] >= 0.6
                   for k in grp):
                grp.append(j)
                used.add(j)
        vals = [M[a][b] for a in grp for b in grp if a < b
                and isinstance(M[a][b], float)]
        clusters.append({"tickers": [tks[k] for k in grp],
                         "avg_internal_corr": round(
                             sum(vals) / len(vals), 2) if vals else None})
    pairs = [(M[i][j], tks[i], tks[j]) for i in range(n)
             for j in range(i + 1, n) if isinstance(M[i][j], float)]
    pairs.sort(key=lambda x: x[0])
    return {"tickers": tks, "window_days": 90, "matrix": M,
            "clusters": sorted(clusters,
                               key=lambda c: -len(c["tickers"])),
            "most_diversifying_pairs": [
                {"a": a, "b": b, "corr": round(v, 3)}
                for v, a, b in pairs[:8]]}


SCENARIOS_NOTE = ("Linear first-order from empirical 3y betas. "
                  "recession = SPY -25% + rates -150bp; "
                  "inflation_shock = breakeven +100bp + nominal +75bp. "
                  "Approximations, not guarantees; convexity ignored.")


def scenario_table(betas):
    rows = {}
    for tkr, b in betas.items():
        rb, sb, bb = (b.get("rate_beta_pct_per_100bp"),
                      b.get("spy_beta"),
                      b.get("bei_beta_pct_per_100bp"))
        if rb is None and sb is None:
            continue

        def cap(x):
            return None if x is None else round(max(-80.0, min(80.0, x)), 1)
        rec = None
        if sb is not None:
            rec = sb * -25.0 + (rb or 0.0) * -1.5
        infl = None
        if bb is not None or rb is not None:
            infl = (bb or 0.0) * 1.0 + (rb or 0.0) * 0.75
        rows[tkr] = {"plus_100bp_pct": cap(rb),
                     "minus_100bp_pct": cap(-rb if rb is not None
                                            else None),
                     "recession_pct": cap(rec),
                     "inflation_shock_pct": cap(infl)}
    return {"note": SCENARIOS_NOTE, "assets": rows}


def ledger_update(assets, y1):
    """Monthly ER snapshots; edge-accuracy-style grading at 12m:
    realized 12m PRICE return per asset vs forecast ER, sign-hit on
    excess-vs-cash. Price-only (dividends excluded), stated."""
    led = s3_json(LEDGER_KEY) or {}
    entries = led.get("entries") or []
    graded = led.get("graded") or []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def days(a, b):
        return (datetime.strptime(a, "%Y-%m-%d")
                - datetime.strptime(b, "%Y-%m-%d")).days
    px_now = {a["ticker"]: a.get("price") for a in assets}
    if not entries or days(today, entries[-1]["date"]) >= 28:
        snap = {a["ticker"]: {"er_1y_pct": a["er_1y_pct"],
                              "price": a.get("price")}
                for a in assets
                if a.get("er_1y_pct") is not None and a.get("price")}
        if len(snap) >= 15:
            entries.append({"date": today, "cash_rf_pct": y1,
                            "n": len(snap), "assets": snap})
            entries = entries[-120:]
    done = {g["date"] for g in graded}
    for e in entries:
        if e["date"] in done or days(today, e["date"]) < 360:
            continue
        rows, hits, errs = [], 0, []
        for t, sn in (e.get("assets") or {}).items():
            p0, p1 = sn.get("price"), px_now.get(t)
            if not (p0 and p1):
                continue
            realized = round((p1 / p0 - 1.0) * 100, 2)
            err = round(realized - sn["er_1y_pct"], 2)
            rf = e.get("cash_rf_pct") or 0.0
            hit = (sn["er_1y_pct"] > rf) == (realized > rf)
            hits += 1 if hit else 0
            errs.append(abs(err))
            rows.append({"ticker": t, "forecast_er_pct": sn["er_1y_pct"],
                         "realized_12m_pct": realized, "error_pp": err,
                         "hit_excess_vs_cash": hit})
        if rows:
            graded.append({"date": e["date"], "graded_at": today,
                           "n": len(rows),
                           "mae_pp": round(sum(errs) / len(errs), 2),
                           "hit_rate_excess_vs_cash": round(
                               100.0 * hits / len(rows), 1),
                           "assets": rows})
    led = {"method": "edge-accuracy style: monthly ER vectors graded "
                     "at 12m; realized = price-only total (dividends "
                     "excluded, stated); hit = sign agreement on "
                     "excess-vs-cash", "entries": entries,
           "graded": graded[-60:]}
    S3.put_object(Bucket=BUCKET, Key=LEDGER_KEY,
                  Body=json.dumps(led, separators=(",", ":")).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    first = entries[0]["date"] if entries else today
    eta = (datetime.strptime(first, "%Y-%m-%d")
           + timedelta(days=360)).strftime("%Y-%m-%d")
    summary = {"entries_n": len(entries), "since": first,
               "graded_n": len(graded)}
    if graded:
        g = graded[-1]
        summary["latest_grade"] = {
            "vintage": g["date"], "n": g["n"], "mae_pp": g["mae_pp"],
            "hit_rate_excess_vs_cash": g["hit_rate_excess_vs_cash"]}
    else:
        summary["grading"] = "WARMING_UP"
        summary["first_grade_eta"] = eta
    return summary
# ═══════════════════════════ end v1.2 block ═══════════════════════════



def lambda_handler(event, context):
    t0 = time.time()
    warns = []
    fkey = get_fred_key()
    macro = build_macro_forward(fkey)
    y1 = macro.get("rf_now_pct")
    y10, y10f = macro.get("y10_now_pct"), macro.get("y10_1y_forward_pct")
    infl = macro.get("infl_1y_expected_pct")
    dfii = macro.get("real_10y_now_pct")
    g = (macro.get("growth") or {}).get("real_growth_proxy_pct") or 1.5
    dy10 = round(y10f - y10, 3) if None not in (y10f, y10) else 0.0
    dreal = macro.get("delta_real_expected_pp") or 0.0

    # ── price history ──
    bars, closes = {}, {}
    for tkr, *_ in UNIVERSE:
        if tkr in ("CASH",):
            continue
        if tkr == "BTC":
            bars[tkr] = crypto_daily("bitcoin", "X:BTCUSD")
        elif tkr == "ETH":
            bars[tkr] = crypto_daily("ethereum", "X:ETHUSD")
        elif tkr == "SOL":
            bars[tkr] = crypto_daily("solana", "X:SOLUSD")
        else:
            bars[tkr] = polygon_daily(tkr, 3)
            time.sleep(0.15)
        closes[tkr] = [b["c"] for b in bars[tkr]]
        if len(closes[tkr]) < 200:
            warns.append(f"{tkr}: only {len(closes[tkr])} bars")
    gld10 = polygon_daily("GLD", 10)
    slv10 = polygon_daily("SLV", 10)

    # ── credit-spread context: current OAS + its 10y percentile ──
    start10y = (datetime.now(timezone.utc)
                - timedelta(days=3660)).strftime("%Y-%m-%d")
    oas_ctx = {}
    for _tk, cm in CREDIT_META.items():
        sid = cm.get("oas_fred")
        if not sid or sid in oas_ctx:
            continue
        ser = fred_series(sid, start10y, fkey) or []
        vals = [v for _, v in ser if v is not None]
        if len(vals) > 500:
            oas_ctx[sid] = {"now_pct": round(vals[-1], 2),
                            "pctile_10y": percentile_of(vals[-1], vals),
                            "label": cm.get("oas_label")}

    # ── data-derived betas (must rediscover reality, not assume it) ──
    beta_gold_real, beta_gold_n = None, 0
    fred_dfii = fred_series("DFII10", (datetime.now(timezone.utc)
                            - timedelta(days=1200)).strftime("%Y-%m-%d"), fkey)
    if fred_dfii and closes.get("GLD"):
        dmap = {d: v for d, v in fred_dfii}
        gmap = {datetime.fromtimestamp(b["t"] / 1000, tz=timezone.utc)
                .strftime("%Y-%m-%d"): b["c"] for b in bars["GLD"]}
        days = sorted(set(dmap) & set(gmap))
        xs, ys = [], []
        for a, b in zip(days, days[1:]):
            dx = (dmap[b] - dmap[a]) * 100.0            # bps change in real yld
            dy = (gmap[b] / gmap[a] - 1.0) * 100.0      # % change in gold
            if abs(dx) < 60:
                xs.append(dx); ys.append(dy)
        slope, beta_gold_n = ols_beta(xs, ys)
        if slope is not None:
            beta_gold_real = round(slope * 100.0, 2)    # % per +100bp real
    beta_gdx_gold = None
    if closes.get("GDX") and closes.get("GLD"):
        n = min(len(closes["GDX"]), len(closes["GLD"]), 500)
        gx = [closes["GDX"][-n + i + 1] / closes["GDX"][-n + i] - 1.0
              for i in range(n - 1)]
        gl = [closes["GLD"][-n + i + 1] / closes["GLD"][-n + i] - 1.0
              for i in range(n - 1)]
        b, _ = ols_beta(gl, gx)
        beta_gdx_gold = round(b, 2) if b is not None else None

    gsr_z, gsr_now = None, None
    if gld10 and slv10:
        smap = {b["t"]: b["c"] for b in slv10}
        ratio = [b["c"] / smap[b["t"]] for b in gld10
                 if b["t"] in smap and smap[b["t"]]]
        if len(ratio) > 500:
            gsr_now = round(ratio[-1], 2)
            gsr_z = zscore_latest(ratio)

    cyc = s3_json("data/crypto-cycle-risk.json")
    rr = s3_json("data/risk-regime.json")
    xar = s3_json("data/cross-asset-regime.json")
    xrv = s3_json("data/cross-asset-rv.json")

    # ── per-asset assembly ──
    assets = []
    for tkr, klass, label, dur, structural, model in UNIVERSE:
        _cl = closes.get(tkr) or []
        px = (_cl[-1] if _cl else None) if tkr != "CASH" else 1.0
        row = {"ticker": tkr, "class": klass, "label": label, "price": px,
               "structural": structural, "er_1y_pct": None,
               "er_components": {}, "flags": []}
        if tkr == "CASH":
            row["er_1y_pct"] = y1
            row["er_components"] = {"carry_pct": y1, "model": "1y T-bill"}
            row["asym"] = {"status": "N/A"}
            row["trend"] = {"label": "N/A", "ok": True}
            row["breakout"] = {"state": "NONE"}
            assets.append(row); continue
        cl = closes.get(tkr) or []
        if not cl:
            row["flags"].append("NO_DATA"); assets.append(row); continue
        hs = [b["h"] for b in bars[tkr]]
        vs = [b["v"] for b in bars[tkr]]
        tr = trend_of(cl)
        bo = breakout_state(cl, hs, vs)
        dds = drawdown_series(cl)
        dd_now = dds[-1]
        # long history for the drawdown distribution where we have it
        dd_hist = drawdown_series([b["c"] for b in (
            gld10 if tkr == "GLD" else slv10 if tkr == "SLV" else bars[tkr])])
        up, dn, ratio = asymmetry(px, max(cl), dd_now, dd_hist)
        dd_pct = percentile_of(dd_now, dd_hist)
        gate_ok = bool(tr.get("ok")) if not structural else True
        status = ("ACTIONABLE" if (ratio or 0) >= 1.5 and gate_ok and
                  tr.get("label") != "DOWNTREND"
                  else "WATCH" if (ratio or 0) >= 1.5
                  else "NEUTRAL")
        if not structural and not tr.get("ok"):
            row["flags"].append("SURVIVAL_GATE: needs trend confirmation "
                                "(can-die asset in downtrend)")
        row["asym"] = {"upside_pct": up, "downside_pct": dn,
                       "ratio": ratio, "dd_now_pct": round(dd_now * 100, 1),
                       "dd_depth_pctile_hist": dd_pct, "status": status,
                       "score": asym_score(ratio, dd_pct, tr.get("ok"),
                                           bo.get("state"))}
        row["trend"], row["breakout"] = tr, bo

        ec = row["er_components"]
        if model == "ust" and None not in (y10, y10f):
            row["er_1y_pct"] = bond_er(y10, dur, dy10)
            ec.update(carry_pct=y10, duration=dur,
                      expected_dy10_pp=dy10, model="curve-implied fwd 10y")
        elif model == "tips" and None not in (dfii, infl):
            row["er_1y_pct"] = round(bond_er(dfii, dur, dreal) + infl, 2)
            ec.update(real_yield_pct=dfii, infl_accrual_pct=infl,
                      duration=dur, expected_dreal_pp=dreal)
        elif model == "gold":
            tilt = 1.0 if tr["label"] in ("UPTREND", "RECOVERING") else -1.0
            if infl is not None:
                b = beta_gold_real if beta_gold_real is not None else -25.0
                row["er_1y_pct"] = round(infl + b * dreal + tilt, 2)
                ec.update(infl_anchor_pct=infl,
                          beta_pct_per_100bp_real=beta_gold_real,
                          beta_obs=beta_gold_n, expected_dreal_pp=dreal,
                          trend_tilt_pct=tilt,
                          model="inflation anchor + data-fit real-rate beta")
        elif model == "silver":
            g_er = next((a["er_1y_pct"] for a in assets
                         if a["ticker"] == "GLD"), None)
            if g_er is not None:
                rv = round(min(max(1.5 * (gsr_z or 0.0), -6.0), 6.0), 2)
                row["er_1y_pct"] = round(g_er + rv, 2)
                ec.update(gold_er_pct=g_er, gsr_now=gsr_now, gsr_z_10y=gsr_z,
                          gsr_reversion_pct=rv,
                          model="gold ER + gold/silver-ratio 10y reversion")
        elif model == "miners":
            g_er = next((a["er_1y_pct"] for a in assets
                         if a["ticker"] == "GLD"), None)
            if g_er is not None and beta_gdx_gold:
                row["er_1y_pct"] = round(beta_gdx_gold * g_er - 1.0, 2)
                ec.update(gold_er_pct=g_er, beta_gdx_gold=beta_gdx_gold,
                          cost_drag_pct=-1.0)
        elif model == "credit":
            yld = polygon_ttm_div(tkr, px)
            cm = CREDIT_META.get(tkr, {})
            er_c, comp_c = credit_er(yld, cm.get("loss_pct", 0.5),
                                     dur, dy10)
            if er_c is not None:
                row["er_1y_pct"] = er_c
                ec.update(comp_c)
                sid = cm.get("oas_fred")
                if sid and sid in oas_ctx:
                    ec.update(oas_now_pct=oas_ctx[sid]["now_pct"],
                              oas_pctile_10y=oas_ctx[sid]["pctile_10y"],
                              oas_series=oas_ctx[sid]["label"])
                row["flags"].append("ASSUMPTION: %.2f%%/yr long-run "
                                    "credit-loss rate" % cm.get("loss_pct",
                                                                0.5))
        elif model == "mlp":
            yld = polygon_ttm_div(tkr, px)
            if yld and infl is not None:
                row["er_1y_pct"] = round(yld + infl, 2)
                ec.update(ttm_dist_yield_pct=yld, infl_pct=infl,
                          model="distribution carry + inflation passthrough",
                          assumption="distribution sustainability not "
                                     "modeled; volume-linked cash flows")
                row["flags"].append("ASSUMPTION: distributions sustained")
        elif model == "reit":
            yld = polygon_ttm_div(tkr, px)
            if yld and infl is not None:
                row["er_1y_pct"] = round(yld + infl + 0.75, 2)
                ec.update(ttm_dist_yield_pct=yld, infl_pct=infl,
                          real_escalator_pct=0.75,
                          spread_vs_10y_pp=(round(yld - y10, 2)
                                            if y10 is not None else None),
                          assumption="0.75% real NOI escalator",
                          rate_sensitivity="negative to rising y10")
                row["flags"].append("ASSUMPTION: real escalator 0.75%")
        elif model in ("equity", "equity_intl"):
            yld = polygon_ttm_div(tkr, px)
            s200 = sma(cl, 200)
            stretch = ((px / s200 - 1.0) * 100.0) if s200 else 0.0
            val = round(min(max(-0.35 * stretch, -4.0), 4.0), 2)
            g_use = round(g * 0.5, 2) if model == "equity_intl" else g
            if yld is not None and infl is not None:
                row["er_1y_pct"] = round(yld + infl + g_use + val, 2)
                ec.update(ttm_div_yield_pct=yld, infl_pct=infl,
                          real_growth_proxy_pct=g_use,
                          stretch_vs_200dma_pct=round(stretch, 1),
                          valuation_reversion_pct=val,
                          model="Grinold-Kroner lite: yield+growth+reversion",
                          omitted="buyback yield (needs holdings data; "
                                  "stock-level GK lands in equity-research)")
                if model == "equity_intl":
                    ec["assumption"] = ("US growth proxy halved as the "
                                        "global-cycle stand-in — no local "
                                        "nowcast wired yet")
                    row["flags"].append("ASSUMPTION: US-cycle growth proxy")
        else:
            row["er_1y_pct"] = None
            ec.update(model="no yield/earnings anchor — ranked on "
                            "asymmetry/trend/breakout only")
            if klass == "crypto":
                row["flags"].append("LOW_N: ~4 halving cycles of history; "
                                    "asymmetry stats are low-confidence"
                                    if tkr != "SOL" else
                                    "LOW_N: youngest major chain — thinnest "
                                    "cycle evidence of the three")
                if tkr == "BTC" and isinstance(cyc, dict) and cyc:
                    row["cycle_context"] = {
                        "crypto_cycle_risk": cyc.get("composite_score")
                        or cyc.get("score"),
                        "phase": cyc.get("phase") or cyc.get("cycle_phase")}
        # structural-decay products: flag, and hard-bar UNG from ACTIONABLE
        if tkr in DECAY:
            note, hard_bar = DECAY[tkr]
            row["flags"].append(note)
            if hard_bar and (row.get("asym") or {}).get("status") \
                    == "ACTIONABLE":
                row["asym"]["status"] = "WATCH"
                row["flags"].append("BARRED from ACTIONABLE: decay product")
        assets.append(row)

    # ── v1.1 enrichment: correlation, horizon, deterministic reads ──
    dur_map = {t[0]: t[3] for t in UNIVERSE}
    dreal = macro.get("delta_real_expected_pp")
    spy_cl = closes.get("SPY") or []
    for row in assets:
        tkr = row["ticker"]
        if tkr != "CASH" and tkr != "SPY":
            row["corr_spy_90d"] = corr_returns(closes.get(tkr) or [],
                                               spy_cl, 90)
        elif tkr == "SPY":
            row["corr_spy_90d"] = 1.0
        hz = HORIZON.get(tkr)
        if hz:
            row["horizon"] = {"hold": hz[0], "basis": hz[1]}
            if (row.get("breakout") or {}).get("state") in ("BREAKOUT",
                                                            "SQUEEZE",
                                                            "COILED"):
                row["horizon"]["note"] = "tactical window open"
        row["_dur"] = dur_map.get(tkr, 0.0)
        oas_p = None
        cm = CREDIT_META.get(tkr)
        if cm and cm.get("oas_fred") in oas_ctx:
            oas_p = oas_ctx[cm["oas_fred"]]["pctile_10y"]
        row["read"] = build_read(row, y1, macro.get(
            "rf_direction_next_year"), dreal, oas_p)
        row.pop("_dur", None)

    er_rank = sorted([a for a in assets if a["er_1y_pct"] is not None],
                     key=lambda a: a["er_1y_pct"], reverse=True)
    as_rank = sorted([a for a in assets if (a.get("asym") or {}).get("score")],
                     key=lambda a: a["asym"]["score"], reverse=True)
    bo_watch = [a["ticker"] for a in assets if (a.get("breakout") or {})
                .get("state") in ("BREAKOUT", "COILED", "SQUEEZE")]
    diversifiers = sorted(
        [a for a in assets if a.get("corr_spy_90d") is not None
         and a["corr_spy_90d"] <= 0.35 and a["er_1y_pct"] is not None
         and y1 is not None and a["er_1y_pct"] > y1],
        key=lambda a: a["corr_spy_90d"])


    # ── v1.2: correlations, factor betas, scenarios, forecast ledger ──
    v12 = {}
    try:
        fb = factor_betas(bars, closes, fkey)
        v12["factor_betas"] = fb
        v12["correlations"] = corr_matrix_31(closes)
        v12["scenarios"] = scenario_table(fb)
    except Exception as e:
        warns.append("v12 analytics: %s" % str(e)[:120])
    try:
        v12["forecast_ledger"] = ledger_update(assets, y1)
    except Exception as e:
        warns.append("v12 ledger: %s" % str(e)[:120])

    out = {
        "schema_version": "1.2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "justhodl-asset-compass", "status": "PROVISIONAL",
        "macro_forward": {k: v for k, v in macro.items()
                          if k != "_fred_last"},
        "hurdle": {"cash_rf_pct": y1,
                   "note": "every ER should be judged as excess over this"},
        "credit_context": oas_ctx or None,
        "betas": {"gold_vs_real_rate_pct_per_100bp": beta_gold_real,
                  "gold_beta_obs": beta_gold_n,
                  "gdx_vs_gold": beta_gdx_gold,
                  "gold_silver_ratio": gsr_now, "gsr_z_10y": gsr_z},
        "assets": assets,
        "correlations": v12.get("correlations"),
        "factor_betas": v12.get("factor_betas"),
        "scenarios": v12.get("scenarios"),
        "forecast_ledger": v12.get("forecast_ledger"),
        "boards": {
            "er_ranking": [{"ticker": a["ticker"], "label": a["label"],
                            "er_1y_pct": a["er_1y_pct"],
                            "excess_vs_cash_pp": a.get("excess_vs_cash_pp")}
                           for a in er_rank],
            "diversifiers": [{"ticker": a["ticker"], "label": a["label"],
                              "corr_spy_90d": a["corr_spy_90d"],
                              "er_1y_pct": a["er_1y_pct"]}
                             for a in diversifiers[:8]],
            "asymmetry_ranking": [{"ticker": a["ticker"],
                                   "label": a["label"],
                                   "score": a["asym"]["score"],
                                   "ratio": a["asym"].get("ratio"),
                                   "status": a["asym"].get("status")}
                                  for a in as_rank[:10]],
            "breakout_watch": bo_watch},
        "context": {"risk_regime": (rr or {}).get("regime")
                    or (rr or {}).get("label"),
                    "cross_asset_roro": (xar or {}).get("risk_score")
                    or (xar or {}).get("roro_score"),
                    "rv_dislocations": len([r for r in
                                            ((xrv or {}).get("relationships")
                                             or []) if str(r.get("state", ""))
                                            .upper() == "DISLOCATED"])},
        "methodology": {
            "er": "Grinold-Kroner style decomposition per class; every "
                  "component published in er_components; assets without a "
                  "yield/earnings anchor carry er=None (never fabricated).",
            "credit": "carry (TTM distribution) minus a published long-run "
                      "credit-loss assumption minus duration x expected "
                      "curve shift; OAS level + 10y percentile published "
                      "for cushion context.",
            "reads": "bull/bear cases are deterministic — every line is "
                     "generated from the engine's own published numbers "
                     "(hurdle vs cash, asymmetry, trend, rates, spreads, "
                     "correlation, structural flags). No LLM.",
            "horizon": "per-asset suggested hold with its mechanism "
                       "(duration immunization, premium realization, "
                       "spread cycle, halving cycle, roll decay).",
            "correlation": "90d Pearson of daily returns vs SPY; "
                           "diversifiers board = corr <= 0.35 AND ER "
                           "above cash.",
            "asymmetry": "upside = recovery to 3y high (cap 200%); downside "
                         "= move to own 95th-pctile historical drawdown "
                         "depth; survival gate on can-die assets.",
            "forward_macro": "market-implied only (curve + Cleveland Fed).",
            "horizon": "12 months"},
        "warns": warns,
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=300")
    return {"statusCode": 200,
            "body": json.dumps({"ok": True, "assets": len(assets),
                                "er_modeled": len(er_rank),
                                "warns": len(warns)})}
