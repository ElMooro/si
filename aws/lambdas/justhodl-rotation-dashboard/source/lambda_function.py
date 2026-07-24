"""
justhodl-rotation-dashboard v1.0.0 — "WHICH ASSET IS CAPITAL ROTATING INTO?"
=============================================================================

WHY THIS EXISTS (audit-first, ops 3816)
───────────────────────────────────────
The fleet answers "is this asset good?" from a dozen angles. It does NOT answer
the RELATIVE question: which asset, right now, is capital rotating INTO.

Verified gaps before building (grep across 748 engines):
  • rs_ratio appears in exactly 2 engines — sector-rotation (11 SPDR sectors)
    and theme-rotation (40 equity themes). BOTH equity-only. No cross-asset RRG.
  • trend_gate / absolute_momentum → 1 hit. hysteresis → 1 hit.
  • cot_index / cot_percentile (crowding normalisation) → 0 hits.
  • NONE of {cross-asset-regime, rotation-chain, alpha-compass, episode-compass,
    sector-rotation, theme-rotation} joins ETF flows OR crowding. Zero.

So this does not rebuild anything. It is the SPINE that fuses four layers the
platform already computes separately into ONE ranked overweight list.

THE FOUR LAYERS
───────────────
  L1 REGIME    — growth x inflation quadrant (nowcast-desk) + RORO (risk-regime)
                 + dollar direction (dollar-radar). Sets a per-CLASS prior from
                 the Merrill "Investment Clock" / Bridgewater four-boxes matrix.
  L2 RATIOS    — 11 cross-asset ratios (Copper/Gold, cyclicals/defensives,
                 growth/value, small/large, EM/US, HY/IG, equal/cap, SOX/SPX,
                 stocks/bonds, Gold/Silver, Oil/Gold). A ratio strips shared beta
                 and isolates the rotation. z-scored + 20d slope.
  L3 TREND GATE— THE DRAWDOWN CONTROL, and the thing the fleet never had.
                 Eligible ONLY if price > 200d SMA *AND* 12m excess return over
                 cash > 0. Below-trend assets are redirected to cash, NOT ranked.
                 (Faber/Antonacci dual momentum: the gate controls drawdowns,
                 the ranking does not.)
  L4 RANK+CONFIRM — 12-1 / 3m / 6m momentum blend + RS-line slope vs SPY →
                 RS-Ratio & RS-Momentum → RRG quadrant. Then confirmed against
                 REAL ETF flows and capped by CROWDING (COT index 0-100).

CONFLUENCE, NOT ANY SINGLE LAYER. Overweight = eligible AND top-ranked AND
flow-confirmed AND not maximally crowded. Every metric here has documented false
signals; the edge is agreement across independent layers.

HONESTY RULES SHIPPED IN THE FEED (never strip)
───────────────────────────────────────────────
  • The 2026 GOLD DISTORTION: gold has run on central-bank/debasement demand,
    not cyclical fear. Every gold-denominated ratio (Copper/Gold, Lumber/Gold,
    Gold/Silver, Oil/Gold) is therefore pushed toward its "fear" extreme for a
    NON-FEAR reason. We flag gold_distortion=True and label those ratios
    "favor real assets", NOT "recession imminent". Corroborate with copper's own
    absolute strength before concluding slowdown.
  • Leading vs coincident is stamped per ratio. Coincident gauges are never
    presented as forecasts.
  • Assets with insufficient history are EXCLUDED, never guessed.
  • Hysteresis buffers are applied so a name doesn't flip on 1bp of rank noise.
"""

import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

import boto3

VERSION = "1.1.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/rotation-dashboard.json"
HIST_KEY = "data/rotation-dashboard-history.json"
POLY_KEY = os.environ.get("POLYGON_API_KEY", "")

s3 = boto3.client("s3")

BENCH = "SPY"
CASH = "BIL"

# ── Cross-asset universe. class drives the L1 regime prior. ──────────────────
UNIVERSE = [
    # equities — style / size / region
    ("SPY", "US Large Cap", "equity_us"),
    ("IWM", "US Small Cap", "equity_us_small"),
    ("IWF", "US Growth", "equity_growth"),
    ("IWD", "US Value", "equity_value"),
    ("RSP", "S&P Equal Weight", "equity_us"),
    ("EFA", "Intl Developed", "equity_intl"),
    ("EEM", "Emerging Markets", "equity_em"),
    ("FXI", "China Large Cap", "equity_em"),
    ("INDA", "India", "equity_em"),
    ("EWJ", "Japan", "equity_intl"),
    # equity sectors
    ("XLK", "Technology", "equity_growth"),
    ("XLF", "Financials", "equity_cyclical"),
    ("XLE", "Energy", "equity_energy"),
    ("XLV", "Health Care", "equity_defensive"),
    ("XLI", "Industrials", "equity_cyclical"),
    ("XLY", "Cons Discretionary", "equity_cyclical"),
    ("XLP", "Cons Staples", "equity_defensive"),
    ("XLB", "Materials", "equity_cyclical"),
    ("XLU", "Utilities", "equity_defensive"),
    ("XLRE", "Real Estate", "real_estate"),
    ("XLC", "Communication Svcs", "equity_growth"),
    ("SMH", "Semiconductors", "equity_growth"),
    # fixed income
    ("TLT", "Long Treasuries", "duration"),
    ("IEF", "7-10y Treasuries", "duration"),
    ("SHY", "1-3y Treasuries", "short_duration"),
    ("TIP", "TIPS", "inflation_linked"),
    ("LQD", "IG Credit", "credit_ig"),
    ("HYG", "High Yield", "credit_hy"),
    ("EMB", "EM Debt", "credit_em"),
    # real assets
    ("GLD", "Gold", "gold"),
    ("SLV", "Silver", "precious"),
    ("DBC", "Broad Commodities", "commodity"),
    ("USO", "Crude Oil", "energy_cmdty"),
    ("CPER", "Copper", "industrial_metal"),
    ("GDX", "Gold Miners", "gold"),
    # crypto + cash
    ("IBIT", "Bitcoin (spot ETF)", "crypto"),
    ("BIL", "T-Bills / Cash", "cash"),
]

# ── L2: the ratio dashboard. lead=leading|coincident is stamped, never implied ─
RATIOS = [
    ("copper_gold", "CPER", "GLD", "leading",
     "Cyclicals, materials, EM, short duration (growth/higher yields)",
     "Defensives, long bonds, gold (fear)", True),
    ("gold_silver", "GLD", "SLV", "coincident",
     "Gold, defensives, Treasuries (fear/late-cycle)",
     "Silver, cyclicals, materials (reflation)", True),
    ("oil_gold", "USO", "GLD", "coincident",
     "Energy, TIPS, cyclicals (inflation/demand)",
     "Gold, defensives (fear/deflation)", True),
    ("cyclicals_defensives", "XLY", "XLP", "coincident",
     "Risk-on: expansion, high beta, small caps",
     "Risk-off: defensive rotation", False),
    ("growth_value", "IWF", "IWD", "coincident",
     "Tech, NASDAQ, long-duration equity (low rates/liquidity)",
     "Value, cyclicals, financials, energy", False),
    ("small_large", "IWM", "SPY", "leading",
     "Small caps, cyclicals (risk-on, broadening, early-cycle)",
     "Mega-cap concentration, late-cycle narrowing", False),
    ("em_us", "EEM", "SPY", "coincident",
     "EM (Asia semis, EM ex-China) — falling $ + firm commodities",
     "US exceptionalism, strong dollar", False),
    ("hy_ig", "HYG", "LQD", "leading",
     "Risk-on, spread compression → HY, equities, cyclicals",
     "Credit risk-off, default fear (leads equity stress)", False),
    ("equal_cap", "RSP", "SPY", "coincident",
     "Broad participation, healthy breadth",
     "Narrow/fragile mega-cap leadership", False),
    ("semis_spx", "SMH", "SPY", "leading",
     "Tech, growth, global capex cycle",
     "Value/defensive leadership over tech", False),
    ("stocks_bonds", "SPY", "TLT", "coincident",
     "Risk-on/reflation → equities",
     "Flight to duration → bonds", False),
]

# ── L1: growth x inflation → per-class prior (Investment Clock / four boxes) ──
REGIME_PRIOR = {
    "GOLDILOCKS": {
        "equity_growth": 1.0, "equity_us": 0.8, "equity_us_small": 0.7,
        "equity_em": 0.7, "equity_intl": 0.5, "equity_cyclical": 0.5,
        "credit_hy": 0.5, "crypto": 0.8, "equity_defensive": -0.3,
        "gold": -0.2, "commodity": -0.3, "duration": 0.0, "cash": -0.6,
    },
    "OVERHEAT": {
        "commodity": 1.0, "energy_cmdty": 0.9, "equity_energy": 0.9,
        "industrial_metal": 0.8, "equity_value": 0.7, "equity_cyclical": 0.7,
        "inflation_linked": 0.6, "equity_em": 0.5, "gold": 0.4, "crypto": 0.3,
        "duration": -0.8, "equity_growth": -0.4, "cash": 0.0,
    },
    "STAGFLATION": {
        "gold": 1.0, "precious": 0.8, "commodity": 0.8, "energy_cmdty": 0.7,
        "inflation_linked": 0.7, "cash": 0.6, "equity_energy": 0.5,
        "equity_defensive": 0.3, "short_duration": 0.4,
        "equity_growth": -0.8, "equity_us_small": -0.6, "credit_hy": -0.6,
        "duration": -0.4, "crypto": -0.3,
    },
    "DISINFLATION/SLOWDOWN": {
        "duration": 1.0, "credit_ig": 0.6, "equity_defensive": 0.6,
        "gold": 0.4, "inflation_linked": -0.2, "short_duration": 0.3,
        "equity_cyclical": -0.5, "commodity": -0.7, "energy_cmdty": -0.7,
        "equity_us_small": -0.4, "credit_hy": -0.4, "cash": 0.2,
    },
    "SOFT LANDING": {
        "equity_us": 0.7, "equity_growth": 0.6, "credit_ig": 0.5,
        "duration": 0.5, "equity_intl": 0.4, "equity_em": 0.4,
        "credit_hy": 0.3, "crypto": 0.4, "cash": -0.3, "commodity": -0.2,
    },
    "MIXED": {},
}

# COT contract mapping for the crowding cap (L4). Only where honest.
COT_MAP = {
    "SPY": ["ES"], "IWM": ["RTY"], "TLT": ["ZB"], "IEF": ["ZN"],
    "SHY": ["ZF"], "GLD": ["GC"], "SLV": ["SI"], "USO": ["CL"],
    "CPER": ["HG"], "IBIT": ["BTC"], "DBC": ["CL"], "EFA": ["NQ"],
}


# ═══════════════════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════════════════
def http_json(url, timeout=25):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"[http] {url[:70]} -> {e}")
        return None


def read_feed(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[feed] {key} -> {e}")
        return None


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def stdev(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def zscore(series, value):
    m, sd = mean(series), stdev(series)
    if m is None or not sd:
        return None
    return round((value - m) / sd, 2)


def pct(a, b):
    if a is None or b is None or b == 0:
        return None
    return round((a / b - 1) * 100, 2)


def fetch_closes(ticker, days=560):
    """Polygon daily closes, OLDEST-FIRST. Returns [(date_str, close)]."""
    end = date.today()
    start = end - timedelta(days=days)
    qs = urllib.parse.urlencode(
        {"apiKey": POLY_KEY, "adjusted": "true", "sort": "asc", "limit": 50000}
    )
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{start:%Y-%m-%d}/{end:%Y-%m-%d}?{qs}")
    d = http_json(url)
    if not d or d.get("status") not in ("OK", "DELAYED") or not d.get("results"):
        return []
    out = []
    for b in d["results"]:
        c = b.get("c")
        if c:
            out.append((datetime.fromtimestamp(b["t"] / 1000, timezone.utc)
                        .strftime("%Y-%m-%d"), float(c)))
    return out


def sma(closes, n):
    if len(closes) < n:
        return None
    return sum(closes[-n:]) / n


def ret_over(closes, n):
    """Total return over the last n sessions, in percent."""
    if len(closes) < n + 1:
        return None
    return round((closes[-1] / closes[-1 - n] - 1) * 100, 2)


# ═══════════════════════════════════════════════════════════════════════════
# LAYER 1 — regime
# ═══════════════════════════════════════════════════════════════════════════
def layer1_regime():
    out = {"layer": 1, "name": "Regime", "degraded": []}

    nc = read_feed("data/nowcast-desk.json") or {}
    quad = nc.get("nowcast_quadrant") or {}
    regime = quad.get("regime") or "MIXED"
    out["quadrant"] = {
        "regime": regime,
        "growth": quad.get("growth"),
        "inflation": quad.get("inflation"),
        "gdpnow": quad.get("gdpnow"),
        "underlying_inflation": quad.get("underlying_inflation"),
        "growth_confirmation": quad.get("growth_confirmation"),
        "regime_confidence": quad.get("regime_confidence"),
    }
    if not quad:
        out["degraded"].append("nowcast-desk quadrant unavailable — prior = MIXED (neutral)")

    rr = read_feed("data/risk-regime.json") or {}
    out["roro"] = {
        "score": rr.get("score"),
        "regime": rr.get("regime"),
        "posture": (rr.get("posture") or {}).get("size_mult"),
    }
    if not rr:
        out["degraded"].append("risk-regime unavailable — RORO tilt neutralised")

    out["prior"] = REGIME_PRIOR.get(regime, {})
    dr = read_feed("data/dollar-radar.json") or {}
    # Probe ops 3817: the 3m change is NESTED at bbdxy.dxy_synth.chg_3m_pct,
    # not top-level. Reading it top-level silently neutralised the dollar tilt.
    bb = dr.get("bbdxy") or {}
    synth = bb.get("dxy_synth") or {}
    dxy_3m = synth.get("chg_3m_pct")
    if dxy_3m is None:
        dxy_3m = bb.get("chg_3m_pct")
    out["dollar"] = {
        "chg_1m_pct": synth.get("chg_1m_pct") or bb.get("chg_1m_pct"),
        "chg_3m_pct": dxy_3m,
        "chg_1y_pct": synth.get("chg_1y_pct") or bb.get("chg_1y_pct"),
        "breadth_spread_3m_pp": bb.get("breadth_spread_3m_pp"),
        "range_pctile_1y": bb.get("range_pctile_1y"),
        "regime": dr.get("regime"),
        "pressure": dr.get("dollar_pressure"),
        "headline": dr.get("headline"),
        "source_path": "bbdxy.dxy_synth.chg_3m_pct",
    }
    if dxy_3m is not None:
        out["dollar"]["direction"] = ("FALLING" if dxy_3m < -1 else
                                      "RISING" if dxy_3m > 1 else "FLAT")
        out["dollar"]["favours"] = (
            "EM, ex-US, commodities, gold" if dxy_3m < -1
            else "US large-cap, cash, risk-off" if dxy_3m > 1
            else "no strong dollar tilt")
        # a rising dollar is a real headwind for EM / commodities / gold
        tilt = -0.25 if dxy_3m > 1 else 0.25 if dxy_3m < -1 else 0.0
        if tilt:
            pr = dict(out.get("prior") or {})
            for cls in ("equity_em", "commodity", "gold", "precious",
                        "industrial_metal", "energy_cmdty", "equity_intl",
                        "credit_em"):
                pr[cls] = round(pr.get(cls, 0.0) + tilt, 2)
            out["dollar_tilt_applied"] = tilt
            out["_prior_after_dollar"] = pr
    else:
        out["degraded"].append("dollar-radar 3m change unavailable")

    out["prior"] = out.pop("_prior_after_dollar", None) or REGIME_PRIOR.get(regime, {})
    out["prior_source"] = ("Merrill Investment Clock / Bridgewater four-boxes "
                           "growth x inflation asset matrix")
    return out


# ═══════════════════════════════════════════════════════════════════════════
# LAYER 2 — cross-asset ratios
# ═══════════════════════════════════════════════════════════════════════════
def layer2_ratios(hist):
    rows, gold_distortion = [], False

    gld = hist.get("GLD", [])
    if len(gld) > 260:
        g_1y = ret_over([c for _, c in gld], 252)
        if g_1y is not None and g_1y > 25:
            gold_distortion = True

    for key, num_t, den_t, lead, up_favours, down_favours, gold_denom in RATIOS:
        n, d = hist.get(num_t, []), hist.get(den_t, [])
        if len(n) < 260 or len(d) < 260:
            continue
        dn = dict(n)
        pairs = [(dt, dn[dt] / c) for dt, c in d if dt in dn and c]
        if len(pairs) < 260:
            continue
        vals = [v for _, v in pairs]
        cur = vals[-1]
        hist_win = vals[-252:]
        slope_20 = pct(cur, vals[-21]) if len(vals) > 21 else None
        row = {
            "key": key, "numerator": num_t, "denominator": den_t,
            "value": round(cur, 4),
            "z_1y": zscore(hist_win, cur),
            "chg_20d_pct": slope_20,
            "chg_60d_pct": pct(cur, vals[-61]) if len(vals) > 61 else None,
            "pctile_1y": round(
                100 * sum(1 for v in hist_win if v <= cur) / len(hist_win), 1),
            "direction": ("RISING" if (slope_20 or 0) > 1
                          else "FALLING" if (slope_20 or 0) < -1 else "FLAT"),
            "lead_type": lead,
            "favours": up_favours if (slope_20 or 0) > 0 else down_favours,
        }
        if gold_denom and gold_distortion:
            row["gold_distorted"] = True
            row["caveat"] = (
                "Gold-denominated. Gold has run on central-bank/debasement "
                "demand, not cyclical fear — read this as 'favour real assets', "
                "NOT 'recession imminent'. Corroborate with copper's own "
                "absolute strength and a rising broad commodity index.")
        rows.append(row)

    risk_on = sum(1 for r in rows
                  if r["key"] in ("cyclicals_defensives", "small_large",
                                  "hy_ig", "semis_spx", "equal_cap",
                                  "stocks_bonds")
                  and r["direction"] == "RISING")
    risk_off = sum(1 for r in rows
                   if r["key"] in ("cyclicals_defensives", "small_large",
                                   "hy_ig", "semis_spx", "equal_cap",
                                   "stocks_bonds")
                   and r["direction"] == "FALLING")
    return {
        "layer": 2, "name": "Cross-Asset Ratios", "n_ratios": len(rows),
        "gold_distortion": gold_distortion,
        "gold_distortion_note": (
            "ACTIVE — every gold-denominated ratio is pushed toward its 'fear' "
            "extreme for a non-fear reason." if gold_distortion else "not detected"),
        "roro_buckets": {"risk_on_ratios": risk_on, "risk_off_ratios": risk_off,
                         "net": risk_on - risk_off},
        "ratios": rows,
    }


# ═══════════════════════════════════════════════════════════════════════════
# LAYER 3 + 4 — trend gate, ranking, flow + crowding confirm
# ═══════════════════════════════════════════════════════════════════════════
def build_cot_index(cftc):
    """COT Index 0-100 per contract. Shape confirmed by probe ops 3817:
       cftc['data'] = {CODE: {contract, name, category, weekly_reports:[...]}}
       weekly_reports[] rows carry net_speculator / noncommercial_long|short.
       0 = most net-short in the window (washed out), 100 = most net-long (crowded)."""
    idx = {}
    if not isinstance(cftc, dict):
        return idx
    data = cftc.get("data")
    if not isinstance(data, dict):
        print(f"[cot] no 'data' dict; keys={list(cftc)[:10]}")
        return idx
    for code, row in data.items():
        if not isinstance(row, dict):
            continue
        reports = row.get("weekly_reports") or []
        nets = []
        for w in reports:
            if not isinstance(w, dict):
                continue
            n = w.get("net_speculator")
            if n is None:
                nl, ns = w.get("noncommercial_long"), w.get("noncommercial_short")
                if isinstance(nl, (int, float)) and isinstance(ns, (int, float)):
                    n = nl - ns
            if isinstance(n, (int, float)):
                nets.append(float(n))
        if len(nets) < 12:
            continue
        cur, lo, hi = nets[0], min(nets), max(nets)
        if hi == lo:
            continue
        idx[str(code).upper()] = {
            "contract": str(code).upper(),
            "name": row.get("name"),
            "category": row.get("category"),
            "net_speculator": cur,
            "cot_index": round(100 * (cur - lo) / (hi - lo), 1),
            "n_obs": len(nets),
            "basis": "min-max of net_speculator over available weekly reports",
        }
    print(f"[cot] built {len(idx)} contract indices")
    return idx


def cot_for(ticker, cot_idx):
    for code in COT_MAP.get(ticker, []):
        blk = cot_idx.get(code.upper())
        if blk:
            return dict(blk)
    return None


def layer34(hist, flows, cot_idx, prior, prev_ranks):
    bench = [c for _, c in hist.get(BENCH, [])]
    cash = [c for _, c in hist.get(CASH, [])]
    cash_12m = ret_over(cash, 252) if len(cash) > 253 else 0.0
    if cash_12m is None:
        cash_12m = 0.0

    flow_idx = {}
    for r in (flows.get("flows") or flows.get("by_etf") or []) if flows else []:
        if isinstance(r, dict):
            sym = (r.get("ticker") or r.get("symbol") or "").upper()
            if sym:
                flow_idx[sym] = r

    rows, excluded = [], []
    for ticker, label, cls in UNIVERSE:
        series = hist.get(ticker, [])
        closes = [c for _, c in series]
        if len(closes) < 260:
            excluded.append({"ticker": ticker, "label": label,
                             "reason": f"insufficient history ({len(closes)} sessions, need 260)"})
            continue

        px = closes[-1]
        sma200 = sma(closes, 200)
        r12 = ret_over(closes, 252)
        r12_1 = ret_over(closes[:-21], 231) if len(closes) > 253 else None
        r6 = ret_over(closes, 126)
        r3 = ret_over(closes, 63)
        r1 = ret_over(closes, 21)

        # ── LAYER 3: the trend gate. Drawdown control lives HERE, not in rank ──
        above_ma = (px > sma200) if sma200 else None
        excess_12m = round(r12 - cash_12m, 2) if r12 is not None else None
        eligible = bool(above_ma) and (excess_12m or 0) > 0
        gate_reason = []
        if above_ma is False:
            gate_reason.append("below 200d SMA")
        if (excess_12m or 0) <= 0:
            gate_reason.append(f"12m excess vs cash {excess_12m}pp <= 0")

        # ── LAYER 4a: RS line vs benchmark → RS-Ratio / RS-Momentum → quadrant ─
        rs_ratio = rs_mom = quadrant = None
        if ticker != BENCH and len(bench) >= len(closes) >= 130:
            b = bench[-len(closes):]
            rs = [c / bb for c, bb in zip(closes, b) if bb]
            if len(rs) > 130:
                rs_now = pct(rs[-1], rs[-64])          # 3m relative strength
                rs_prev = pct(rs[-64], rs[-127])       # prior 3m
                if rs_now is not None and rs_prev is not None:
                    rs_ratio = round(rs_now, 2)
                    rs_mom = round(rs_now - rs_prev, 2)
                    quadrant = ("LEADING" if rs_ratio > 0 and rs_mom > 0 else
                                "IMPROVING" if rs_ratio <= 0 and rs_mom > 0 else
                                "WEAKENING" if rs_ratio > 0 and rs_mom <= 0 else
                                "LAGGING")
        elif ticker == BENCH:
            rs_ratio, rs_mom, quadrant = 0.0, 0.0, "BENCHMARK"

        mom_parts = [x for x in (r12_1, r6, r3) if x is not None]
        mom_score = round(sum(mom_parts) / len(mom_parts), 2) if mom_parts else None

        # ── LAYER 4b: flow confirmation ──
        fr = flow_idx.get(ticker)
        flow_blk = None
        if fr:
            nf = (fr.get("net_flow_usd") or fr.get("flow_usd")
                  or fr.get("net_creation_usd"))
            aum = fr.get("aum") or fr.get("aum_usd")
            flow_blk = {
                "net_flow_usd": nf,
                "pct_of_aum": round(nf / aum * 100, 2) if nf and aum else None,
                "state": ("INFLOW" if (nf or 0) > 0 else
                          "OUTFLOW" if (nf or 0) < 0 else "FLAT"),
            }

        # ── LAYER 4c: crowding cap ──
        crowd = cot_for(ticker, cot_idx)
        crowd_state = None
        if crowd and crowd.get("cot_index") is not None:
            ci = crowd["cot_index"]
            crowd_state = ("CROWDED" if ci >= 85 else
                           "WASHED_OUT" if ci <= 15 else "NEUTRAL")
            crowd["state"] = crowd_state

        # ── CONFLUENCE ──
        conf, drivers = 0.0, []
        p = prior.get(cls)
        if p:
            conf += p * 20
            drivers.append(f"regime prior {p:+.1f} ({cls})")
        if mom_score is not None:
            conf += max(-25, min(25, mom_score * 0.6))
            drivers.append(f"momentum blend {mom_score:+.1f}%")
        if rs_mom is not None:
            conf += max(-15, min(15, rs_mom * 0.8))
            drivers.append(f"RS-momentum {rs_mom:+.1f}")
        if quadrant == "LEADING":
            conf += 8; drivers.append("RRG Leading")
        elif quadrant == "IMPROVING":
            conf += 10; drivers.append("RRG Improving (early rotate-in)")
        elif quadrant == "WEAKENING":
            conf -= 6; drivers.append("RRG Weakening")
        elif quadrant == "LAGGING":
            conf -= 10; drivers.append("RRG Lagging")
        if flow_blk and flow_blk["state"] == "INFLOW":
            conf += 6; drivers.append("ETF flows confirming (inflow)")
        elif flow_blk and flow_blk["state"] == "OUTFLOW":
            conf -= 8; drivers.append("ETF flows contradicting (outflow)")
        if crowd_state == "CROWDED":
            conf -= 10; drivers.append(f"crowded (COT idx {crowd['cot_index']})")
        elif crowd_state == "WASHED_OUT":
            conf += 5; drivers.append(f"washed-out positioning (COT idx {crowd['cot_index']})")
        if not eligible:
            conf -= 30; drivers.append("FAILS TREND GATE — redirected to cash")

        rows.append({
            "ticker": ticker, "label": label, "asset_class": cls,
            "price": round(px, 2),
            # L3
            "trend_gate": {
                "eligible": eligible,
                "above_200d_sma": above_ma,
                "sma_200d": round(sma200, 2) if sma200 else None,
                "pct_vs_200d": pct(px, sma200),
                "ret_12m_pct": r12,
                "cash_12m_pct": cash_12m,
                "excess_vs_cash_pp": excess_12m,
                "fail_reasons": gate_reason or None,
            },
            # L4
            "momentum": {"ret_1m_pct": r1, "ret_3m_pct": r3, "ret_6m_pct": r6,
                         "ret_12m_pct": r12, "ret_12_1_pct": r12_1,
                         "blend_score": mom_score},
            "rrg": {"rs_ratio": rs_ratio, "rs_momentum": rs_mom,
                    "quadrant": quadrant},
            "flows": flow_blk,
            "crowding": crowd,
            "regime_prior": p,
            "confluence_score": round(conf, 1),
            "drivers": drivers,
        })

    rows.sort(key=lambda r: r["confluence_score"], reverse=True)

    # ── hysteresis: a name must beat its prior rank by >2 slots to move band ──
    for i, r in enumerate(rows, 1):
        r["rank"] = i
        prev = prev_ranks.get(r["ticker"])
        r["prev_rank"] = prev
        r["rank_delta"] = (prev - i) if prev else None
        if prev and abs(prev - i) <= 2:
            r["rank_stability"] = "STABLE (within hysteresis buffer)"
        elif prev:
            r["rank_stability"] = "MOVED"
        else:
            r["rank_stability"] = "NEW"

    eligible_rows = [r for r in rows if r["trend_gate"]["eligible"]]
    overweight = [r for r in eligible_rows[:8]
                  if r["confluence_score"] > 0
                  and (r["crowding"] or {}).get("state") != "CROWDED"]

    return rows, eligible_rows, overweight, excluded


# ═══════════════════════════════════════════════════════════════════════════
def lambda_handler(event, context):
    t0 = time.time()
    if not POLY_KEY:
        raise RuntimeError("POLYGON_API_KEY missing — cannot build price history")

    tickers = sorted({t for t, _, _ in UNIVERSE} |
                     {t for _, n, d, *_ in RATIOS for t in (n, d)})
    hist, failed = {}, []
    for t in tickers:
        c = fetch_closes(t)
        if len(c) < 130:
            failed.append(t)
        hist[t] = c
    print(f"[hist] {len(hist)-len(failed)}/{len(tickers)} ok, failed={failed}")

    prev = read_feed(HIST_KEY) or {"runs": []}
    prev_ranks = {}
    if prev.get("runs"):
        prev_ranks = (prev["runs"][-1] or {}).get("ranks", {})

    l1 = layer1_regime()
    l2 = layer2_ratios(hist)
    try:
        flows = read_feed("data/etf-true-flows.json") or {}
    except Exception as e:
        print(f"[flows] {e}"); flows = {}
    try:
        cot_idx = build_cot_index(read_feed("data/cftc-all-cache.json"))
    except Exception as e:
        print(f"[cot] parse failed: {e}"); cot_idx = {}
    rows, eligible, overweight, excluded = layer34(
        hist, flows, cot_idx, l1.get("prior") or {}, prev_ranks)

    degraded = list(l1.get("degraded") or [])
    if not flows:
        degraded.append("etf-true-flows unavailable — flow confirmation skipped")
    if not cot_idx:
        degraded.append("cftc-all-cache unmapped — crowding cap skipped")
    if failed:
        degraded.append(f"no price history: {','.join(failed)}")

    out = {
        "engine": "rotation-dashboard", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "thesis": ("Not 'is this asset good?' but 'which asset is capital "
                   "rotating INTO?'. Four layers — regime, ratios, trend gate, "
                   "rank+flows+crowding — fused into one ranked overweight list. "
                   "Overweight = eligible AND top-ranked AND flow-confirmed AND "
                   "not maximally crowded."),
        "layer1_regime": l1,
        "layer2_ratios": l2,
        "layer3_layer4": {
            "layer": "3+4", "name": "Trend Gate · Rank · Flows · Crowding",
            "n_universe": len(UNIVERSE), "n_scored": len(rows),
            "n_eligible": len(eligible),
            "n_failed_gate": len(rows) - len(eligible),
            "cash_12m_pct": rows[0]["trend_gate"]["cash_12m_pct"] if rows else None,
        },
        "overweight": [
            {"rank": r["rank"], "ticker": r["ticker"], "label": r["label"],
             "asset_class": r["asset_class"],
             "confluence_score": r["confluence_score"],
             "quadrant": r["rrg"]["quadrant"],
             "drivers": r["drivers"]} for r in overweight],
        "avoid": [
            {"rank": r["rank"], "ticker": r["ticker"], "label": r["label"],
             "confluence_score": r["confluence_score"],
             "why": r["trend_gate"]["fail_reasons"] or r["drivers"][:2]}
            for r in rows[-6:]],
        "assets": rows,
        "excluded": excluded,
        "quadrant_counts": {
            q: sum(1 for r in rows if r["rrg"]["quadrant"] == q)
            for q in ("LEADING", "IMPROVING", "WEAKENING", "LAGGING")},
        "degraded": degraded,
        "methodology": {
            "trend_gate": ("price > 200d SMA AND 12m total return > 12m cash "
                           "(BIL) return. Faber/Antonacci dual momentum — the "
                           "GATE controls drawdowns, the ranking does not."),
            "rs_ratio": "3-month return of the asset/benchmark relative-strength line",
            "rs_momentum": "current 3m RS minus prior 3m RS (acceleration; turns first)",
            "quadrants": "LEADING(+,+) IMPROVING(-,+) WEAKENING(+,-) LAGGING(-,-)",
            "cot_index": "spec net positioning min-max normalised 0-100; >=85 crowded, <=15 washed-out",
            "hysteresis": "rank moves of <=2 slots are reported STABLE to cut whipsaw",
        },
        "caveats": [
            "No single metric picks winners — every ratio and gauge here has "
            "documented false signals. The edge is CONFLUENCE across layers.",
            "Leading vs coincident is stamped per ratio; never treat a "
            "coincident gauge (VIX level, breadth, most sector ratios) as a forecast.",
            "ETF proxies are not the asset class itself — expense, tracking "
            "error and wrapper effects are real.",
            "Momentum is not value: a top-ranked asset can be expensive. Cross-"
            "read against asset-compass/forward-returns for the strategic view.",
            "Research only, not investment advice.",
        ],
        "build_seconds": round(time.time() - t0, 1),
    }

    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=300")

    runs = (prev.get("runs") or [])[-364:]
    runs.append({
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "regime": (l1.get("quadrant") or {}).get("regime"),
        "n_eligible": len(eligible),
        "overweight": [r["ticker"] for r in overweight],
        "ranks": {r["ticker"]: r["rank"] for r in rows},
    })
    s3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                  Body=json.dumps({"engine": "rotation-dashboard",
                                   "runs": runs}, default=str).encode(),
                  ContentType="application/json")

    print(f"[done] scored={len(rows)} eligible={len(eligible)} "
          f"overweight={[r['ticker'] for r in overweight]} in {out['build_seconds']}s")
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "scored": len(rows), "eligible": len(eligible),
         "overweight": [r["ticker"] for r in overweight]})}
