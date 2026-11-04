"""justhodl-strength - relative strength vs the S&P 500 and the long-accumulation / tight-spread detector, computed
daily from the Polygon EOD bank into data/strength.json (Khalid, 2026-11-04: "Strength vs S&P 500 ... most matched
fund flows is missing; also improve the strength and add which one has been in long accumulation with very very tight
price spread").

A READER. Nothing is fetched from a provider: daily bars come from the Polygon EOD bank (data/warm/polygon-full/eod,
justhodl-polygon-eod, 03:10 UTC), fund flows from the native ETF desk (data/etf-desk-research.json, 23:05 UTC) with the
canonical 300-fund packet as the fallback, the smart-money read from the 13F book's per-ticker table
(data/13f-tickers.json, 16:37 UTC), names / sectors from FMP's banked profile bulk, the S&P 500 list from the FMP walker.
Every figure is derived from those dated artifacts and stamped with their dates; a figure that cannot be computed is
null, never guessed.

UNIVERSE  the strength desk (the page's four sleeves: ETF / STOCK / METAL / CRYPTO, 52 names), the configured ETF desk
(116 funds), the S&P 500 constituents, and the 13F cohort's most widely held stocks - deduplicated, each row carrying
the scopes it belongs to. Bars: the last 520 sessions.

STRENGTH  excess return vs SPY (pp) on 1 / 5 / 21 / 63 / 126 / 252 sessions; the composite is 10 / 20 / 30 / 25 / 15 on
1d / 5d / 21d / 63d / 126d (the page's old 35 / 30 / 20 / 15 on 1d / 5d / 21d / 63d was day-heavy); `strength` is the
universe percentile (0-100) of the same weights applied to cross-sectional z-scores, so horizons with different
dispersion count equally; RS trend = annualized OLS slope of the log RS line over 63 sessions; RS consistency = share
of the last 63 sessions whose rolling 21-session excess return was positive; information ratio = 63-session excess
return over its own tracking volatility; Mansfield RS = RS line vs its 252-session mean; 52-week position; SMA 50/200.

COIL  the longest trailing run of sessions whose closes all fit inside 6% / 10% / 15% of the last close (lengths L6,
L10, L15); the chosen base is the (length, range) pair scoring best; score 0-100 = base length (up to 35) + tightness
(up to 30) + Bollinger-width squeeze percentile (up to 10) + volume dry-up (up to 8) + accumulation inside the base
(Chaikin money flow and up-volume / down-volume, up to 12) + relative strength holding through the base (up to 5).
Tiers: VERY TIGHT (closes inside 6% for >= 40 sessions), TIGHT (inside 10% for >= 40), LONG BASE (inside 15% for >= 120),
FORMING (inside 10% for >= 20) - each only when the base is a contraction (at most 60% of the instrument's own
252-session close range), so cash-like funds never coil. Fund flows and 13F stances are shown beside the coil as
corroboration; they never enter the price score.

FLOWS  for a configured fund: the fund's OWN latest 1 / 5 / 21 reporting-observation windows (USD and % of reported
assets) and whether the SPY-aligned windows matched; for a stock: the 13F cohort's holders now / prior, bull / bear
manager votes, net dollars at reported prices and the stance. A reported position is not a trade.
"""
from __future__ import annotations

import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import gzip

import boto3

import chain_guard          # the fleet-wide trigger cap: every engine start is admitted, counted and stamped
import polygon_bankfirst    # aws/shared: the Polygon EOD bank's layout (ROOT, MANIFEST, FIELDS); rows are read here, thread-safely
import fmp_bank             # aws/shared: the FMP warehouse reader (profiles, walker lists) - S3 only
try:
    import etf_desk_catalog  # aws/shared: the configured ETF desk (116 funds)
except Exception:  # noqa: BLE001
    etf_desk_catalog = None

VERSION = "1.0.3"
CONTRACT = "strength.v1"
ENGINE = "justhodl-strength"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = os.environ.get("OUT_KEY", "data/strength.json")
DESK_KEY = os.environ.get("ETF_DESK_KEY", "data/etf-desk-research.json")
CANON_KEY = os.environ.get("FLOW_CANON_KEY", "data/provider-fund-flow-research.json")
TICKERS_KEY = os.environ.get("TICKERS_13F_KEY", "data/13f-tickers.json")
ACTIONS_INDEX_KEY = "data/corporate-actions-index.json"            # justhodl-corporate-actions: Polygon splits / dividends per ticker
ACTIONS_PREFIX = "data/corporate-actions/"
COMMON_RATIOS = (2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0, 25.0, 50.0, 1.5, 0.5, 1 / 3, 0.25, 0.2, 0.125, 0.1, 2 / 3, 0.05, 0.04, 0.02)
BENCH = "SPY"
LOOKBACK = int(os.environ.get("LOOKBACK_SESSIONS", "520"))
MAX_UNIVERSE = int(os.environ.get("MAX_UNIVERSE", "900"))
F13_TOP = int(os.environ.get("F13_TOP", "300"))            # most widely held 13F stocks admitted to the universe
BOARD = int(os.environ.get("BOARD_ROWS", "40"))
SPARK = 60
MIN_SESSIONS = 70                                          # below this a row carries bars but no strength
THREADS = 12
RESERVE_S = 45.0                                           # seconds kept for the rows, the boards and the put
WEIGHTS = (("d", 0.10), ("w", 0.20), ("m", 0.30), ("q", 0.25), ("h", 0.15))
HORIZONS = (("d", 1), ("w", 5), ("m", 21), ("q", 63), ("h", 126), ("y", 252))
BASE_TIERS = ((6.0, 40), (10.0, 40), (15.0, 120))       # (close range %, sessions) -> VERY TIGHT / TIGHT / LONG BASE; FORMING = 10% over 20
BASE_MAX = 260

SLEEVES = {
    "ETF": [("QQQ", "Nasdaq 100"), ("IWM", "Russell 2000"), ("DIA", "Dow 30"), ("XLK", "Technology"), ("SMH", "Semis"), ("XLF", "Financials"),
            ("XLE", "Energy"), ("XLU", "Utilities"), ("XLP", "Staples"), ("XLV", "Health Care"), ("XLI", "Industrials"), ("XLY", "Discretionary"),
            ("GLD", "Gold"), ("TLT", "Long Treasury"), ("HYG", "High Yield"), ("EEM", "Emerging Mkts"), ("FXI", "China"), ("EWJ", "Japan"),
            ("KRE", "Regional Banks"), ("XBI", "Biotech")],
    "STOCK": [("AAPL", "Apple"), ("MSFT", "Microsoft"), ("NVDA", "NVIDIA"), ("AMZN", "Amazon"), ("META", "Meta"), ("GOOGL", "Alphabet"),
              ("TSLA", "Tesla"), ("AVGO", "Broadcom"), ("JPM", "JPMorgan"), ("LLY", "Eli Lilly"), ("XOM", "Exxon"), ("UNH", "UnitedHealth"),
              ("V", "Visa"), ("MA", "Mastercard"), ("HD", "Home Depot"), ("COST", "Costco"), ("NFLX", "Netflix"), ("AMD", "AMD"),
              ("ORCL", "Oracle"), ("JNJ", "J&J")],
    "METAL": [("GLD", "Gold"), ("SLV", "Silver"), ("GDX", "Gold Miners"), ("GDXJ", "Jr Gold Miners"), ("PPLT", "Platinum"), ("CPER", "Copper"),
              ("IAU", "Gold (IAU)"), ("SIVR", "Silver (SIVR)")],
    "CRYPTO": [("IBIT", "Bitcoin ETF"), ("ETHA", "Ether ETF"), ("FBTC", "Bitcoin (Fidel.)"), ("BITO", "BTC Futures")],
}
F13_COLUMNS = ["n_holders", "n_prior_holders", "holder_delta", "value_usd", "value_prior_usd", "inflow_usd", "outflow_usd", "net_usd",
               "bull", "bear", "stance", "kind", "bucket", "sector", "industry", "n_identities", "conc_n_holders", "conc_holder_delta"]
SOURCES = [
    {"engine": "justhodl-polygon-eod", "key": "data/warm/polygon-full/eod/manifest.json", "role": "daily bars (grouped daily folded per ticker)", "schedule": "daily 03:10 UTC", "expected_h": 24},
    {"engine": "justhodl-etf-global-desk", "key": DESK_KEY, "role": "native ETF desk: reported fund flows, profiles, holdings", "schedule": "daily 23:05 UTC", "expected_h": 24},
    {"engine": "justhodl-etf-fund-flows", "key": CANON_KEY, "role": "canonical 300-fund flow packet (ETF Global via Massive/Polygon)", "schedule": "daily 22:00 UTC", "expected_h": 24},
    {"engine": "justhodl-13f-book", "key": TICKERS_KEY, "role": "13F cohort read per ticker", "schedule": "daily 16:37 UTC + hand-off", "expected_h": 24},
]


# ----------------------------------------------------------------------------------------------------------------- math
def f(v, d=2):
    try:
        if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
            return None
        return round(float(v), d)
    except (TypeError, ValueError):
        return None


def pct_change(a, b):
    return None if a in (None, 0) or b is None else (b / a - 1.0) * 100.0


def ret_n(c, n):
    return pct_change(c[-1 - n], c[-1]) if len(c) > n else None


def horizons(c):
    return {k: f(ret_n(c, n)) for k, n in HORIZONS}


def sma(c, n):
    return sum(c[-n:]) / n if len(c) >= n else None


def stdev(xs):
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def slope(ys):
    """OLS slope per step of ys against 0..n-1."""
    n = len(ys)
    if n < 3:
        return None
    xm = (n - 1) / 2.0
    ym = sum(ys) / n
    sxx = sum((i - xm) ** 2 for i in range(n))
    return sum((i - xm) * (y - ym) for i, y in enumerate(ys)) / sxx if sxx else None


def percentile_of(value, population):
    """Share of the population strictly below value, 0-100."""
    if value is None or not population:
        return None
    below = sum(1 for p in population if p < value)
    ties = sum(1 for p in population if p == value)
    return 100.0 * (below + 0.5 * ties) / len(population)


def align(bars, bench):
    """Closes of a ticker and the benchmark on their common sessions, oldest first."""
    bd = {b["date"]: b for b in bench}
    dates, c, s = [], [], []
    for b in bars:
        x = bd.get(b["date"])
        if x is None:
            continue
        dates.append(b["date"])
        c.append(float(b["c"]))
        s.append(float(x["c"]))
    return dates, c, s


def rs_metrics(c, s):
    """RS line metrics on aligned closes: trend (annualized % per year of the log RS line over 63 sessions), consistency
    (share of the last 63 sessions whose 21-session excess return was positive), Mansfield RS (RS line vs its 252-session
    mean, %), the RS line's 52-week position and whether it sits at a 52-week high, information ratio over 63."""
    n = len(c)
    out = {"rs_trend_63": None, "rs_consistency_63": None, "rs_vs_52w": None, "rs_52w_pos": None, "rs_new_high": None, "ir_63": None, "vol_63": None, "excess_vol_63": None}
    if n < 22 or any(x <= 0 for x in c[-260:]) or any(x <= 0 for x in s[-260:]):
        return out
    rs = [c[i] / s[i] for i in range(n)]
    if n >= 63:
        sl = slope([math.log(x) for x in rs[-63:]])
        out["rs_trend_63"] = f(sl * 252 * 100) if sl is not None else None
        pos = 0
        for i in range(n - 63, n):
            if i - 21 >= 0 and rs[i] > rs[i - 21]:
                pos += 1
        out["rs_consistency_63"] = f(100.0 * pos / 63, 1)
        e = [math.log(c[i] / c[i - 1]) - math.log(s[i] / s[i - 1]) for i in range(n - 63, n) if i >= 1]
        sd = stdev(e)
        r = [math.log(c[i] / c[i - 1]) for i in range(n - 63, n) if i >= 1]
        rsd = stdev(r)
        out["vol_63"] = f(rsd * math.sqrt(252) * 100, 1) if rsd else None
        out["excess_vol_63"] = f(sd * math.sqrt(252) * 100, 1) if sd else None
        if sd and sd > 1e-9:
            out["ir_63"] = f(sum(e) / (sd * math.sqrt(len(e))), 2)
    if n >= 252:
        win = rs[-252:]
        mean = sum(win) / 252
        hi, lo = max(win), min(win)
        out["rs_vs_52w"] = f((rs[-1] / mean - 1) * 100) if mean else None
        out["rs_52w_pos"] = f(100.0 * (rs[-1] - lo) / (hi - lo), 1) if hi > lo else None
        out["rs_new_high"] = bool(rs[-1] >= hi * 0.995)
    return out


def context_52w(bars):
    c = [float(b["c"]) for b in bars]
    out = {"hi_252": None, "lo_252": None, "pos_52w": None, "off_high_pct": None, "sma50": None, "sma200": None, "above_50": None, "above_200": None, "sma50_over_200": None}
    if not c:
        return out
    n = min(252, len(c))
    win = bars[-n:]
    hi = max(float(b.get("h", b["c"])) for b in win)
    lo = min(float(b.get("l", b["c"])) for b in win)
    if len(c) >= 130:
        out.update(hi_252=f(hi), lo_252=f(lo), pos_52w=f(100.0 * (c[-1] - lo) / (hi - lo), 1) if hi > lo else None, off_high_pct=f((c[-1] / hi - 1) * 100) if hi else None)
    s50, s200 = sma(c, 50), sma(c, 200)
    out.update(sma50=f(s50), sma200=f(s200), above_50=(c[-1] > s50) if s50 else None, above_200=(c[-1] > s200) if s200 else None,
               sma50_over_200=(s50 > s200) if (s50 and s200) else None)
    return out


def atr_pct(bars, n=14):
    if len(bars) < n + 1:
        return None
    trs = []
    for i in range(len(bars) - n, len(bars)):
        h, l, pc = float(bars[i].get("h", bars[i]["c"])), float(bars[i].get("l", bars[i]["c"])), float(bars[i - 1]["c"])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    c = float(bars[-1]["c"])
    return f(100.0 * (sum(trs) / n) / c) if c else None


def bb_width(c, n=20, k=2.0, end=None):
    """Bollinger bandwidth (%) of the n closes ending at `end` (exclusive; the last close by default)."""
    end = len(c) if end is None else end
    if end < n:
        return None
    win = c[end - n:end]
    m = sum(win) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in win) / n)
    return 100.0 * (2 * k * sd) / m if m else None


def bb_width_series(c, n=20, k=2.0, back=252):
    out = []
    for end in range(max(n, len(c) - back + 1), len(c) + 1):
        w = bb_width(c, n, k, end)
        if w is not None:
            out.append(w)
    return out


def cmf(bars):
    num = den = 0.0
    for b in bars:
        h, l, c, v = float(b.get("h", b["c"])), float(b.get("l", b["c"])), float(b["c"]), float(b.get("v") or 0)
        if h == l or v <= 0:
            continue
        num += (((c - l) - (h - c)) / (h - l)) * v
        den += v
    return (num / den) if den else None


def updown_volume(bars):
    up = dn = 0.0
    for i in range(1, len(bars)):
        v = float(bars[i].get("v") or 0)
        if bars[i]["c"] > bars[i - 1]["c"]:
            up += v
        elif bars[i]["c"] < bars[i - 1]["c"]:
            dn += v
    return (up / dn) if dn > 0 else (None if up == 0 else 9.99)


def obv_slope(bars):
    """OBV change over the window as a fraction of the window's total volume (-1..1)."""
    obv, tot = 0.0, 0.0
    for i in range(1, len(bars)):
        v = float(bars[i].get("v") or 0)
        tot += v
        if bars[i]["c"] > bars[i - 1]["c"]:
            obv += v
        elif bars[i]["c"] < bars[i - 1]["c"]:
            obv -= v
    return (obv / tot) if tot > 0 else None


def base_lengths(c, tiers=BASE_TIERS, n_max=BASE_MAX):
    """For each tier threshold, the longest trailing run of sessions whose closes all sit within that % of the last close.
    The close range is non-decreasing in the length, so one outward scan answers every tier."""
    last = c[-1]
    if not last or last <= 0:
        return {t: 0 for t, _ in tiers}, {}
    found = {t: 0 for t, _ in tiers}
    ranges = {}
    mx = mn = last
    limit = min(n_max, len(c))
    open_t = [t for t, _ in tiers]
    for L in range(1, limit + 1):
        x = c[-L]
        if x > mx:
            mx = x
        if x < mn:
            mn = x
        rng = 100.0 * (mx - mn) / last
        still = []
        for t in open_t:
            if rng <= t:
                found[t] = L
                ranges[t] = rng
            else:
                continue
            still.append(t)
        open_t = still
        if not open_t:
            break
    return found, ranges


CONTRACTION_MAX = 0.6                                      # a base must be at most 60% of the instrument's own 252-session close range
DEAL_ATR_PCT, DEAL_RANGE_PCT = 0.6, 3.5                    # a stock this still inside this thin a base is pinned by a pending deal, not accumulating


def tier_of(found, ranges=None, range_252=None):
    """The base's label from the three trailing lengths, tightest first: VERY TIGHT (closes inside 6% for >= 40 sessions),
    TIGHT (inside 10% for >= 40), LONG BASE (inside 15% for >= 120), FORMING (inside 10% for >= 20); else none. A tier
    counts only when the base is a CONTRACTION of the instrument's own movement - its range at most 60% of the
    252-session close range - so a T-bill or ultra-short bond fund that never moves is not a coil. Returns
    (label, threshold) - the base reported and scored is the labelled threshold's own run. Pure."""
    def contracted(t):
        if range_252 is None or ranges is None or t not in ranges:
            return True
        return range_252 > 0 and ranges[t] / range_252 <= CONTRACTION_MAX
    if found.get(6.0, 0) >= 40 and contracted(6.0):
        return "VERY TIGHT", 6.0
    if found.get(10.0, 0) >= 40 and contracted(10.0):
        return "TIGHT", 10.0
    if found.get(15.0, 0) >= 120 and contracted(15.0):
        return "LONG BASE", 15.0
    if found.get(10.0, 0) >= 20 and contracted(10.0):
        return "FORMING", 10.0
    return None, None


def length_points(L):
    return 0.0 if L < 20 else min(35.0, 10.0 + 25.0 * math.log(L / 20.0) / math.log(12.5))


def tight_points(rng):
    for lim, pts in ((4.0, 30), (6.0, 25), (8.0, 19), (10.0, 14), (15.0, 7)):
        if rng <= lim:
            return float(pts)
    return 0.0


def coil(bars, c_al, s_al, rs_trend):
    """The long-accumulation / tight-spread read for one ticker. Pure."""
    c = [float(b["c"]) for b in bars]
    out = {"tier": None, "base_len": 0, "base_range_pct": None, "wick_range_pct": None, "len_6": 0, "len_10": 0, "len_15": 0, "atr14_pct": None, "range_252_pct": None, "contraction": None,
           "bb_width_pct": None, "bb_width_pctile_252": None, "vol_dry": None, "cmf_base": None, "updown_vol": None, "obv_slope_base": None,
           "rs_hold_base": None, "pos_in_base": None, "score": None, "points": None}
    if len(c) < 20:
        return out
    found, ranges = base_lengths(c)
    year = c[-252:]
    range_252 = 100.0 * (max(year) - min(year)) / c[-1] if len(year) >= 120 and c[-1] else None
    out.update(len_6=found[6.0], len_10=found[10.0], len_15=found[15.0], atr14_pct=atr_pct(bars), range_252_pct=f(range_252))
    bw = bb_width(c)
    out["bb_width_pct"] = f(bw)
    series = bb_width_series(c)
    if bw is not None and len(series) >= 60:
        out["bb_width_pctile_252"] = f(percentile_of(bw, series), 1)
    if len(c) >= 140:
        recent = [float(b.get("v") or 0) for b in bars[-20:]]
        prior = [float(b.get("v") or 0) for b in bars[-140:-20]]
        pa = sum(prior) / len(prior)
        out["vol_dry"] = f((sum(recent) / 20) / pa, 2) if pa > 0 else None
    tier, thr = tier_of(found, ranges, range_252)
    if tier is None:
        return out
    L, rng = found[thr], ranges[thr]
    win = bars[-L:]
    out.update(tier=tier, base_len=L, base_range_pct=f(rng), contraction=f(rng / range_252, 2) if range_252 else None)
    hi = max(float(b.get("h", b["c"])) for b in win)
    lo = min(float(b.get("l", b["c"])) for b in win)
    out["wick_range_pct"] = f(100.0 * (hi - lo) / c[-1])
    cmx, cmn = max(c[-L:]), min(c[-L:])
    out["pos_in_base"] = f(100.0 * (c[-1] - cmn) / (cmx - cmn), 1) if cmx > cmn else None
    m = cmf(win)
    ud = updown_volume(win)
    ob = obv_slope(win)
    out.update(cmf_base=f(m, 3), updown_vol=f(ud), obv_slope_base=f(ob, 3))
    if len(c_al) > L and len(s_al) > L:
        out["rs_hold_base"] = f(pct_change(c_al[-1 - L], c_al[-1]) - pct_change(s_al[-1 - L], s_al[-1]))
    squeeze = out["bb_width_pctile_252"]
    sq = 10.0 if squeeze is not None and squeeze <= 10 else 6.0 if squeeze is not None and squeeze <= 25 else 2.0 if squeeze is not None and squeeze <= 50 else 0.0
    vd = out["vol_dry"]
    dry = 8.0 if vd is not None and vd <= 0.6 else 5.0 if vd is not None and vd <= 0.8 else 2.0 if vd is not None and vd <= 0.95 else 0.0
    acc = (7.0 if m is not None and m > 0.10 else 4.0 if m is not None and m > 0.03 else 0.0) + (5.0 if ud is not None and ud >= 1.25 else 2.0 if ud is not None and ud >= 1.05 else 0.0)
    held = out["rs_hold_base"] is not None and out["rs_hold_base"] >= 0
    trend = rs_trend is not None and rs_trend >= 0
    rs_pts = 5.0 if (held and trend) else 2.0 if (held or trend) else 0.0
    out["points"] = {"length": f(length_points(L), 1), "tightness": tight_points(rng), "squeeze": sq, "dry_up": dry, "accumulation": acc, "rs_hold": rs_pts}
    out["score"] = f(min(100.0, length_points(L) + tight_points(rng) + sq + dry + acc + rs_pts), 1)
    return out


def posture(d, sd):
    """The page's day posture codes, from close-to-close moves (the page overlays the live quote). Pure."""
    if d is None or sd is None:
        return {"code": "NA", "label": "-", "rs": None}
    rs = d - sd
    if sd <= -0.8 and d > 0:
        return {"code": "BID", "label": "UP WHILE SPX DUMPS", "rs": f(rs)}
    if sd <= -0.8 and abs(d) < 0.25:
        return {"code": "FLAT", "label": "FLAT WHILE SPX DUMPS", "rs": f(rs)}
    if sd <= -0.8 and d < 0 and d > sd * 0.45:
        return {"code": "HOLD", "label": "DUMPING LESS THAN SPX", "rs": f(rs)}
    if sd < 0 and d > 0:
        return {"code": "BID", "label": "BID VS SPX", "rs": f(rs)}
    if sd < 0 and d > sd:
        return {"code": "HOLD", "label": "HOLDING UP", "rs": f(rs)}
    if rs >= 0.4:
        return {"code": "LEAD", "label": "LEADING", "rs": f(rs)}
    if rs <= -0.4:
        return {"code": "LAG", "label": "LAGGING", "rs": f(rs)}
    return {"code": "INLINE", "label": "IN LINE", "rs": f(rs)}


def pattern(bars):
    """The desk's 40-session pattern read (COIL / GOLDEN CROSS / BREAKOUT / BREAKDOWN / DOUBLE BOTTOM / UPTREND / DOWNTREND / RANGE). Pure."""
    if len(bars) < 40:
        return None
    c = [float(b["c"]) for b in bars]
    last = c[-1]
    win = bars[-40:]
    hi20 = max(float(b.get("h", b["c"])) for b in win[-20:])
    lo20 = min(float(b.get("l", b["c"])) for b in win[-20:])
    lows = sorted(float(b.get("l", b["c"])) for b in win)
    ma10, ma40 = sma(c, 10), sma(c, 40)
    rng = (hi20 - lo20) / last if last else 0
    bounce = (last - lows[0]) / lows[0] if lows and lows[0] > 0 else 0
    if rng < 0.035:
        return "COIL"
    if ma10 > ma40 and c[-15] <= ma40:
        return "GOLDEN CROSS"
    if last >= hi20 * 0.999 and last >= c[-6]:
        return "BREAKOUT"
    if last <= lo20 * 1.001 and last <= c[-6]:
        return "BREAKDOWN"
    if bounce >= 0.04 and len(lows) >= 2 and (lows[1] - lows[0]) / last < 0.015:
        return "DOUBLE BOTTOM"
    if ma10 > ma40:
        return "UPTREND"
    if ma10 < ma40:
        return "DOWNTREND"
    return "RANGE"


def composite(vs):
    """Weighted excess return in pp (10 / 20 / 30 / 25 / 15 on 1d / 5d / 21d / 63d / 126d). Pure."""
    s = w = 0.0
    for k, wt in WEIGHTS:
        if vs.get(k) is not None:
            s += vs[k] * wt
            w += wt
    return f(s / w) if w else None


def rank_rows(rows):
    """Cross-sectional z-scores per horizon (winsorized at +-3) -> weighted z composite -> universe percentile 0-100. Pure."""
    stats = {}
    for k, _ in WEIGHTS:
        xs = [r["vs"][k] for r in rows if r.get("vs") and r["vs"].get(k) is not None]
        sd = stdev(xs)
        stats[k] = (sum(xs) / len(xs), sd) if xs and sd else None
    zs = []
    for r in rows:
        if not r.get("vs"):
            r["z"] = None
            continue
        s = w = 0.0
        for k, wt in WEIGHTS:
            st = stats.get(k)
            v = r["vs"].get(k)
            if st and v is not None:
                z = max(-3.0, min(3.0, (v - st[0]) / st[1]))
                s += z * wt
                w += wt
        r["z"] = f(s / w, 3) if w else None
        if r["z"] is not None:
            zs.append(r["z"])
    for r in rows:
        r["strength"] = f(percentile_of(r["z"], zs), 1) if r.get("z") is not None else None
    return {k: {"mean": f(v[0]), "sd": f(v[1])} if v else None for k, v in stats.items()}


# -------------------------------------------------------------------------------------------------------------- flows
def window_of(w):
    if not w or w.get("status") != "matched_reporting_window" or w.get("flow_usd_decimal") is None:
        return None
    try:
        usd = float(w["flow_usd_decimal"])
    except (TypeError, ValueError):
        return None
    pct = w.get("flow_to_end_assets_pct_decimal")
    return {"usd": f(usd, 0), "pct_aum": f(float(pct), 3) if pct not in (None, "") else None, "start": w.get("start_date"), "end": w.get("end_date")}


def fund_flow(fl, eligible, now_iso, source, reference_end):
    """One fund's flow read from a flow measurement block (desk: funds[t].flows; canonical: funds[t]). Pure."""
    valid = str(fl.get("source_valid_until") or "")
    current = bool(eligible) and valid > now_iso
    own = fl.get("latest_windows") or {}
    al = fl.get("aligned_windows") or {}
    latest = fl.get("latest_observation") or {}
    try:
        aum = float(latest.get("reported_assets_usd_decimal")) if latest.get("reported_assets_usd_decimal") not in (None, "") else None
    except (TypeError, ValueError):
        aum = None
    return {"source": source, "current": current, "valid_until": valid or None, "effective": fl.get("latest_effective_date"), "processed": fl.get("latest_processed_date"),
            "reference_end": reference_end, "aum_usd": f(aum, 0),
            "w1": window_of(own.get("1")) if current else None, "w5": window_of(own.get("5")) if current else None, "w21": window_of(own.get("21")) if current else None,
            "aligned": {k: bool((al.get(k) or {}).get("status") == "matched_reporting_window") for k in ("1", "5", "21")},
            "status": (fl.get("quality") or {}).get("status")}


def flows_from_desk(desk, now_iso):
    funds = (desk or {}).get("funds") or {}
    ref = ((desk or {}).get("reference") or {}).get("end_date")
    out, names = {}, {}
    for t, fd in funds.items():
        fl = fd.get("flows") or {}
        out[t] = fund_flow(fl, (fd.get("quality") or {}).get("flow_current_eligible"), now_iso, "etf-desk", ref)
        try:
            names[t] = ((fd.get("profiles") or {}).get("current") or {}).get("summary", {}).get("text", {}).get("description", {}).get("value")
        except Exception:  # noqa: BLE001
            pass
    return out, names


def flows_from_canonical(canon, now_iso):
    funds = (canon or {}).get("funds") or {}
    ref = ((canon or {}).get("reference") or {}).get("end_date")
    return {t: fund_flow(fl, (fl.get("quality") or {}).get("status") == "complete_acquired_history", now_iso, "provider-flow-research", ref) for t, fl in funds.items()}


def f13_rows(tbl):
    cols = (tbl or {}).get("columns") or F13_COLUMNS
    out = {}
    for t, row in ((tbl or {}).get("by_ticker") or {}).items():
        d = dict(zip(cols, row))
        out[t] = {k: d.get(k) for k in ("n_holders", "n_prior_holders", "holder_delta", "value_usd", "net_usd", "bull", "bear", "stance", "kind", "bucket", "sector", "industry", "conc_n_holders", "conc_holder_delta")}
    return out


# ------------------------------------------------------------------------------------------------------------ universe
def universe(etf_desk, sp500, f13, desk_names):
    """Ticker -> {name, sleeve, scopes}. The strength desk first (its names and sleeves win), then the ETF desk, the
    S&P 500 and the 13F most-held stocks, capped. Pure."""
    u = {}

    def add(t, name, scope, sleeve=None):
        t = str(t or "").upper().strip()
        if not t or "/" in t or " " in t or len(t) > 10:
            return
        if len(u) >= MAX_UNIVERSE and t not in u:
            return
        r = u.setdefault(t, {"name": name, "sleeve": sleeve, "scopes": []})
        if scope not in r["scopes"]:
            r["scopes"].append(scope)
        if not r["name"] and name:
            r["name"] = name
        if not r["sleeve"] and sleeve:
            r["sleeve"] = sleeve
    for sl, items in SLEEVES.items():
        for t, n in items:
            add(t, n, "desk", sl)
    for t in etf_desk or []:
        add(t, desk_names.get(t), "etf-desk", "ETF")
    for row in sp500 or []:
        add(row.get("symbol"), row.get("name"), "sp500", "STOCK")
    top = sorted(((t, v) for t, v in (f13 or {}).items() if v.get("kind") == "stock" and (v.get("n_holders") or 0) >= 2), key=lambda kv: -(kv[1]["n_holders"] or 0))[:F13_TOP]
    for t, v in top:
        add(t, None, "13f", "STOCK")
    return u


def profile_lookup(symbols):
    """Name / sector / industry / isEtf for the universe from FMP's banked profile bulk (one dated copy); {} on a miss."""
    try:
        rows = fmp_bank.snapshot("profile")
    except Exception:  # noqa: BLE001
        rows = None
    out = {}
    want = set(symbols)
    for r in rows or []:
        s = str(r.get("symbol") or "").upper()
        if s in want and s not in out:
            out[s] = {"name": r.get("companyName"), "sector": r.get("sector") or None, "industry": r.get("industry") or None,
                      "is_etf": str(r.get("isEtf")).lower() in ("true", "1"), "exchange": r.get("exchangeShortName") or r.get("exchange")}
    return out


# -------------------------------------------------------------------------------------------------------------- build
def _s3():
    return boto3.client("s3", region_name="us-east-1")


def bank_key(ticker):
    return polygon_bankfirst.ROOT + "".join(ch if (ch.isalnum() or ch in "._-^") else "_" for ch in str(ticker).upper())[:80] + ".json.gz"


def read_bars(s3, ticker, sessions=LOOKBACK):
    """The last `sessions` daily bars of one ticker from the Polygon EOD bank, oldest first:
    {date, o, h, l, c, v}; [] when unbanked. Thread-safe (no shared cache)."""
    try:
        raw = s3.get_object(Bucket=S3_BUCKET, Key=bank_key(ticker))["Body"].read()
        doc = json.loads(gzip.decompress(raw))
    except Exception as exc:  # noqa: BLE001
        code = str(((getattr(exc, "response", None) or {}).get("Error") or {}).get("Code") or "")
        if type(exc).__name__ == "NoSuchKey" or code in ("NoSuchKey", "404", "NotFound") or "NoSuchKey" in str(exc):
            return []                                        # unbanked: a row without bars, never a guess
        raise                                                # throttling, access, network: the caller records it
    rows = [r for r in (doc or {}).get("rows") or [] if isinstance(r, list) and len(r) >= 5 and r[0] is not None]
    out = []
    for r in rows[-sessions:]:
        d = {k: v for k, v in zip(polygon_bankfirst.FIELDS, r) if v is not None}
        if d.get("c") is None:
            continue
        d["date"] = polygon_bankfirst._row_date(r[0]).isoformat()
        out.append(d)
    return out


def get_json(s3, key):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(obj["Body"].read()), obj["LastModified"]


def optional(s3, key):
    try:
        doc, mod = get_json(s3, key)
        return doc, mod.isoformat(timespec="seconds"), None
    except Exception as exc:  # noqa: BLE001
        return None, None, "%s: %s" % (type(exc).__name__, str(exc)[:120])


def head(s3, key, now, expected_h):
    try:
        h = s3.head_object(Bucket=S3_BUCKET, Key=key)
        age = (now - h["LastModified"]).total_seconds() / 3600
        return {"key": key, "bytes": h["ContentLength"], "last_modified": h["LastModified"].isoformat(timespec="seconds"), "age_h": round(age, 1),
                "tier": "fresh" if age <= max(30, expected_h * 1.25) else "stale" if age <= expected_h * 7 else "dead"}
    except Exception as exc:  # noqa: BLE001
        return {"key": key, "bytes": None, "last_modified": None, "age_h": None, "tier": "missing", "error": type(exc).__name__}


def apply_splits(bars, events):
    """Backward split adjustment of unadjusted bank bars: every bar BEFORE a split date is multiplied by the split's
    price factor (to / from: a 2-for-1 split -> 0.5, a 1-for-3 reverse split -> 3.0) and its volume divided by it;
    the bar on the split date itself already trades post-split. Returns new bars; the input is never mutated. Pure."""
    ev = sorted([(str(e["date"]), float(e["factor"])) for e in events or [] if e.get("date") and e.get("factor")], key=lambda x: x[0])
    if not ev:
        return bars
    out = []
    for b in bars:
        f = 1.0
        for d, x in ev:
            if b["date"] < d:
                f *= x
        if f == 1.0:
            out.append(b)
            continue
        nb = dict(b)
        for k in ("o", "h", "l", "c", "vw"):
            if nb.get(k) is not None:
                nb[k] = float(nb[k]) * f
        if nb.get("v") is not None:
            nb["v"] = float(nb["v"]) / f
        out.append(nb)
    return out


def infer_splits(bars):
    """Unrecorded splits in unadjusted bars: a close-to-close ratio >= 1.8 or <= 0.56 whose volume moves the opposite
    way by a matching amount (5 sessions each side, within x1.6) - a reverse split prints a 3x price with ~1/3 the
    shares traded; a crash or a squeeze does not. The ratio snaps to the nearest common split ratio within 4%, else it is
    kept as measured. Returns [{date, factor, ratio, source: 'inferred'}] oldest first. Pure."""
    found = []
    n = len(bars)
    for i in range(1, n):
        pc, c = float(bars[i - 1]["c"]), float(bars[i]["c"])
        if pc <= 0 or c <= 0:
            continue
        r = c / pc
        if 0.56 < r < 1.8:
            continue
        before = [float(b.get("v") or 0) for b in bars[max(0, i - 5):i]]
        after = [float(b.get("v") or 0) for b in bars[i:i + 5]]
        if len(before) < 3 or len(after) < 3 or not sum(before) or not sum(after):
            continue
        vr = (sum(before) / len(before)) / (sum(after) / len(after))
        if abs(math.log(vr) - math.log(r)) > math.log(1.6):
            continue
        snap = min(COMMON_RATIOS, key=lambda x: abs(math.log(x) - math.log(r)))
        ratio = snap if abs(math.log(snap) - math.log(r)) <= math.log(1.04) else r
        found.append({"date": bars[i]["date"], "factor": f(ratio, 6), "ratio": f(r, 4), "source": "inferred"})
    return found


def recorded_splits(s3, index, tickers):
    """Split events for the tickers the corporate-actions producer covers with at least one split: {ticker: [events]}."""
    out = {}
    tk = (index or {}).get("tickers") or {}
    for t in tickers:
        ent = tk.get(t)
        if not isinstance(ent, dict) or not ent.get("n_splits"):
            continue
        doc, _, _ = optional(s3, ACTIONS_PREFIX + t + ".json")
        ev = [{"date": e.get("date"), "factor": e.get("factor"), "ratio_from": e.get("ratio_from"), "ratio_to": e.get("ratio_to"), "source": "corporate-actions"}
              for e in ((doc or {}).get("events") or []) if isinstance(e, dict) and e.get("type") == "split" and e.get("date") and e.get("factor")]
        if ev:
            out[t] = ev
    return out


def row_for(t, meta, bars, bench, bank_last):
    c = [float(b["c"]) for b in bars]
    h = horizons(c) if len(c) >= 2 else {k: None for k, _ in HORIZONS}
    row = {"ticker": t, "name": meta.get("name"), "sleeve": meta.get("sleeve"), "scopes": meta.get("scopes") or [], "kind": meta.get("kind"),
           "sector": meta.get("sector"), "industry": meta.get("industry"), "px": f(c[-1]) if c else None, "as_of": bars[-1]["date"] if bars else None,
           "sessions": len(bars), "stale": bool(bars and bank_last and bars[-1]["date"] < bank_last), "h": h, "vs": None, "composite_pp": None, "z": None,
           "strength": None, "posture": None, "pattern": pattern(bars), "coil": None, "flow": None, "f13": None}
    row.update({k: None for k in ("rs_trend_63", "rs_consistency_63", "rs_vs_52w", "rs_52w_pos", "rs_new_high", "ir_63", "vol_63", "excess_vol_63")})
    row.update(context_52w(bars))
    if len(bars) < MIN_SESSIONS or not bench:
        return row
    _, c_al, s_al = align(bars, bench)
    if len(c_al) < MIN_SESSIONS:
        return row
    sh = horizons(s_al)
    hh = horizons(c_al)
    row["vs"] = {k: f(hh[k] - sh[k]) if (hh.get(k) is not None and sh.get(k) is not None) else None for k, _ in HORIZONS}
    row["composite_pp"] = composite(row["vs"])
    row.update(rs_metrics(c_al, s_al))
    row["posture"] = posture(hh["d"], sh["d"])
    row["coil"] = coil(bars, c_al, s_al, row["rs_trend_63"])
    cl = row["coil"]
    if cl.get("tier") and row.get("kind") != "fund" and cl.get("atr14_pct") is not None and cl["atr14_pct"] <= DEAL_ATR_PCT and (cl.get("base_range_pct") or 99) <= DEAL_RANGE_PCT:
        cl["flag"] = "PINNED"            # a stock printing almost no daily range inside a sliver of a base trades like a pending deal, not an accumulation
        cl["flag_note"] = "deal-like: ATR %.2f%% <= %.1f%% and base range %.2f%% <= %.1f%%; kept off the coil board" % (cl["atr14_pct"], DEAL_ATR_PCT, cl["base_range_pct"], DEAL_RANGE_PCT)
    return row


def boards_from(rows):
    live = [r for r in rows if r.get("strength") is not None and not r.get("stale")]
    by_strength = sorted(live, key=lambda r: -r["strength"])
    coiled = sorted([r for r in live if (r.get("coil") or {}).get("tier") and not (r["coil"] or {}).get("flag")], key=lambda r: (-(r["coil"]["score"] or 0), -(r["coil"]["base_len"] or 0)))
    desk = [r for r in live if "desk" in r["scopes"]]

    def slim(r):
        return {"ticker": r["ticker"], "name": r["name"], "sleeve": r["sleeve"], "scopes": r["scopes"], "strength": r["strength"], "composite_pp": r["composite_pp"],
                "vs": r["vs"], "rs_trend_63": r["rs_trend_63"], "rs_consistency_63": r["rs_consistency_63"], "ir_63": r["ir_63"], "pos_52w": r["pos_52w"],
                "coil": {k: (r["coil"] or {}).get(k) for k in ("tier", "base_len", "base_range_pct", "score", "flag")} if r.get("coil") else None,
                "f13_stance": (r.get("f13") or {}).get("stance"), "flow_w21_pct": ((r.get("flow") or {}).get("w21") or {}).get("pct_aum")}

    def breadth(group):
        out = {"n": len(group)}
        for k in ("w", "m", "q"):
            n = sum(1 for r in group if r["vs"] and r["vs"].get(k) is not None and r["vs"][k] > 0)
            d = sum(1 for r in group if r["vs"] and r["vs"].get(k) is not None)
            out["above_spy_" + k] = n
            out["pct_" + k] = f(100.0 * n / d, 1) if d else None
        out["rs_new_highs"] = sum(1 for r in group if r.get("rs_new_high"))
        out["coiled"] = sum(1 for r in group if (r.get("coil") or {}).get("tier") and not (r.get("coil") or {}).get("flag"))
        out["pinned"] = sum(1 for r in group if (r.get("coil") or {}).get("flag"))
        return out
    return {"leaders": [slim(r) for r in by_strength[:BOARD]], "laggards": [slim(r) for r in by_strength[-BOARD:][::-1]],
            "coil": [slim(r) for r in coiled[:BOARD * 2]], "rs_new_highs": [slim(r) for r in by_strength if r.get("rs_new_high")][:BOARD],
            "breadth": {"universe": breadth(live), "desk": breadth(desk)}, "n_ranked": len(live)}


def build(s3, now, log=print, remaining_s=300.0):
    started = time.time()
    now_iso = now.isoformat()
    errors = []
    # 1. inputs
    man, _, man_err = optional(s3, polygon_bankfirst.MANIFEST)
    man = man or {}
    bank_last = man.get("last_session")
    desk, desk_mod, desk_err = optional(s3, DESK_KEY)
    flows, desk_names = flows_from_desk(desk, now_iso) if desk else ({}, {})
    canon, canon_mod, canon_err = optional(s3, CANON_KEY)
    if canon:
        for t, v in flows_from_canonical(canon, now_iso).items():
            flows.setdefault(t, v)
    tbl, tbl_mod, tbl_err = optional(s3, TICKERS_KEY)
    f13 = f13_rows(tbl) if tbl else {}
    for k, e in (("polygon-eod-manifest", man_err), ("etf-desk", desk_err), ("canonical-flows", canon_err), ("13f-tickers", tbl_err)):
        if e:
            errors.append({"input": k, "error": e})
    try:
        sp500 = fmp_bank.walker("sp500-constituent") or []
        if not isinstance(sp500, list):
            sp500 = []
    except Exception as exc:  # noqa: BLE001
        sp500, _ = [], errors.append({"input": "sp500-constituent", "error": "%s: %s" % (type(exc).__name__, str(exc)[:120])})
    etf_desk = list(etf_desk_catalog.DESK) if etf_desk_catalog else []
    u = universe(etf_desk, sp500, f13, desk_names)
    log("strength: universe %d (desk %d, etf-desk %d, sp500 %d, 13f %d), bank last session %s" % (
        len(u), sum(1 for v in u.values() if "desk" in v["scopes"]), sum(1 for v in u.values() if "etf-desk" in v["scopes"]),
        sum(1 for v in u.values() if "sp500" in v["scopes"]), sum(1 for v in u.values() if "13f" in v["scopes"]), bank_last))
    prof = profile_lookup(list(u.keys()))
    for t, meta in u.items():
        p = prof.get(t) or {}
        f13r = f13.get(t) or {}
        meta["name"] = meta.get("name") or p.get("name") or t
        meta["sector"] = p.get("sector") or f13r.get("sector")
        meta["industry"] = p.get("industry") or f13r.get("industry")
        meta["kind"] = "fund" if (t in flows or p.get("is_etf") or f13r.get("kind") == "fund") else "stock" if (p or f13r) else None
    # 2. bars (bank only)
    bench = read_bars(s3, BENCH)
    if not bench:
        raise RuntimeError("the Polygon EOD bank has no %s bars" % BENCH)
    deadline = started + max(0.0, remaining_s - RESERVE_S)   # bars stop at the deadline; rows, boards and the put need the reserve
    tickers = [t for t in u if t != BENCH]
    bars_by = {}

    def fetch(t):
        if time.time() > deadline:
            return t, None
        try:
            return t, read_bars(s3, t)
        except Exception as exc:  # noqa: BLE001
            errors.append({"ticker": t, "error": "%s: %s" % (type(exc).__name__, str(exc)[:80])})
            return t, []
    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        for t, b in pool.map(fetch, tickers):
            bars_by[t] = b
    unbanked = sorted(t for t, b in bars_by.items() if b is not None and not b)
    skipped = sorted(t for t, b in bars_by.items() if b is None)
    # 2b. splits: the bank's grouped-daily rows are unadjusted; recorded splits first, then volume-confirmed inferred ones
    actions_index, actions_mod, actions_err = optional(s3, ACTIONS_INDEX_KEY)
    if actions_err:
        errors.append({"input": "corporate-actions-index", "error": actions_err})
    recorded = recorded_splits(s3, actions_index, [t for t, b in bars_by.items() if b] + [BENCH])
    splits_by = {}
    for t in list(bars_by.keys()) + [BENCH]:
        b = bench if t == BENCH else bars_by.get(t)
        if not b:
            continue
        ev = list(recorded.get(t) or [])
        adjusted = apply_splits(b, ev) if ev else b
        inferred = [x for x in infer_splits(adjusted) if not any(abs((datetime.fromisoformat(x["date"]) - datetime.fromisoformat(r["date"])).days) <= 3 for r in ev)]
        if inferred:
            adjusted = apply_splits(adjusted, inferred)
            ev = ev + inferred
        if ev:
            splits_by[t] = ev
        if t == BENCH:
            bench = adjusted
        else:
            bars_by[t] = adjusted
    # 3. rows
    rows = []
    for t in tickers:
        b = bars_by.get(t)
        if not b:
            continue
        r = row_for(t, u[t], b, bench, bank_last)
        r["splits"] = splits_by.get(t)
        r["flow"] = flows.get(t)
        r["f13"] = f13.get(t)
        rows.append(r)
    stats = rank_rows(rows)
    rows.sort(key=lambda r: (-(r["strength"] if r["strength"] is not None else -1), r["ticker"]))
    boards = boards_from(rows)
    on_board = {x["ticker"] for key in ("leaders", "laggards", "coil", "rs_new_highs") for x in boards[key]}
    for r in rows:
        if "desk" in r["scopes"] or r["ticker"] in on_board:
            r["spark"] = [f(float(b["c"]), 4) for b in bars_by[r["ticker"]][-SPARK:]]
    bench_h = horizons([float(b["c"]) for b in bench])
    bench_ctx = context_52w(bench)
    flows_summary = {"desk_generated_at": (desk or {}).get("generated_at"), "desk_last_modified": desk_mod, "reference_end": ((desk or {}).get("reference") or {}).get("end_date"),
                     "n_configured": len(((desk or {}).get("funds") or {})), "n_current_now": sum(1 for v in flows.values() if v["current"]),
                     "n_in_universe": sum(1 for r in rows if r["flow"]), "n_universe_current": sum(1 for r in rows if r["flow"] and r["flow"]["current"]),
                     "n_universe_w5": sum(1 for r in rows if r["flow"] and r["flow"].get("w5")), "canonical_generated_at": (canon or {}).get("generated_at"),
                     "canonical_funds": len(((canon or {}).get("funds") or {}))}
    f13_summary = {"period": (tbl or {}).get("period"), "generated_at": (tbl or {}).get("generated_at"), "n_tickers": (tbl or {}).get("n_tickers"), "last_modified": tbl_mod,
                   "n_in_universe": sum(1 for r in rows if r["f13"])}
    sources = [{**s, **head(s3, s["key"], now, s["expected_h"])} for s in SOURCES]
    out = {
        "engine": ENGINE, "version": VERSION, "contract": CONTRACT, "generated_at": now.isoformat(timespec="seconds"),
        "as_of": bank_last, "bank": {"last_session": bank_last, "updated_at": man.get("updated_at"), "tickers": man.get("tickers"), "first_session": man.get("first_session"),
                                     "lookback_sessions": LOOKBACK},
        "benchmark": {"ticker": BENCH, "px": f(float(bench[-1]["c"])), "as_of": bench[-1]["date"], "h": bench_h, "sessions": len(bench), **bench_ctx},
        "splits": {"index_generated_at": (actions_index or {}).get("generated_at"), "index_covered": (actions_index or {}).get("covered"), "index_last_modified": actions_mod,
                   "n_rows_adjusted": sum(1 for r in rows if r.get("splits")), "recorded": sum(1 for r in rows for e in (r.get("splits") or []) if e.get("source") == "corporate-actions"),
                   "inferred": sum(1 for r in rows for e in (r.get("splits") or []) if e.get("source") == "inferred"),
                   "note": "bank bars are unadjusted grouped-daily rows; bars before a split are rebased by the split factor (recorded by justhodl-corporate-actions, or inferred from a volume-confirmed price jump)"},
        "universe": {"n": len(u), "n_rows": len(rows), "n_ranked": boards["n_ranked"], "scopes": {s: sum(1 for v in u.values() if s in v["scopes"]) for s in ("desk", "etf-desk", "sp500", "13f")},
                     "unbanked": unbanked, "skipped_for_time": skipped, "sp500_rows": len(sp500), "etf_desk_rows": len(etf_desk), "f13_rows": len(f13), "max": MAX_UNIVERSE},
        "sleeves": {sl: [{"ticker": t, "name": n} for t, n in items] for sl, items in SLEEVES.items()},
        "weights": dict(WEIGHTS), "z_stats": stats,
        "rows": rows, "boards": boards, "flows": flows_summary, "f13": f13_summary, "sources": sources, "errors": errors[:50], "n_errors": len(errors),
        "definitions": {
            "strength": "universe percentile (0-100) of the weighted cross-sectional z-score of excess return vs SPY on 1d / 5d / 21d / 63d / 126d (10 / 20 / 30 / 25 / 15)",
            "composite_pp": "the same weights on raw excess returns, in percentage points",
            "rs_trend_63": "annualized slope (% per year) of the log RS line (close / SPY) over 63 sessions",
            "rs_consistency_63": "share of the last 63 sessions whose rolling 21-session excess return vs SPY was positive",
            "ir_63": "63-session excess log return over its tracking volatility (information ratio, per quarter)",
            "rs_vs_52w": "RS line vs its 252-session mean, % (Mansfield relative strength)",
            "coil": "longest trailing run of closes inside 6 / 10 / 15% of the last close; score = length (35) + tightness (30) + Bollinger-width squeeze (10) + volume dry-up (8) + accumulation inside the base: CMF and up/down volume (12) + RS holding (5)",
            "tiers": "VERY TIGHT: closes inside 6% for >= 40 sessions; TIGHT: inside 10% for >= 40; LONG BASE: inside 15% for >= 120; FORMING: inside 10% for >= 20 - and the base must be a contraction: at most 60% of the instrument's own 252-session close range (a T-bill fund that never moves is not a coil)",
            "flow": "a configured fund's own latest 1 / 5 / 21 reporting-observation windows from the native ETF desk (USD, % of reported assets) while its source is current; aligned = the SPY-grid windows matched",
            "f13": "the 13F cohort's read of the ticker across two dated disclosures: holders, bull / bear manager votes, net dollars at reported prices. Newly disclosed is not bought; absent is not sold.",
            "posture": "day posture from close-to-close moves; the page overlays the live quote",
            "pinned": "a stock whose 14-session ATR is <= 0.6% of price inside a base of <= 3.5% trades like a pending deal (merger arbitrage), not an accumulation: flagged PINNED and kept off the coil board",
            "splits": "bank bars are unadjusted; bars before a recorded split (corporate-actions) or an inferred one (close ratio >= 1.8 or <= 0.56 with volume moving the opposite way, 5 sessions each side) are rebased by the split factor",
        },
        "note": "Relative strength is a price fact, not a forecast. Fund flows are reported creation / redemption observations; 13F stances count managers. No trade, call or sizing is implied.",
        "run_seconds": round(time.time() - started, 1),
    }
    return out


def put_json(s3, key, doc, cache="max-age=300"):
    body = json.dumps(doc, separators=(",", ":"), default=str).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json", CacheControl=cache)
    return len(body)


def lambda_handler(event, context):
    chain_guard.begin(event, context)   # trigger cap: a root run (the schedule) or whatever chain started it
    now = datetime.now(timezone.utc)
    s3 = _s3()
    try:
        remaining = context.get_remaining_time_in_millis() / 1000.0
    except Exception:  # noqa: BLE001
        remaining = 300.0
    out = build(s3, now, remaining_s=remaining)
    trig = event.get("trigger") if isinstance(event, dict) else None
    out["trigger"] = {"kind": trig or "scheduled"}
    n = put_json(s3, OUT_KEY, out)
    b = out["boards"]
    print("strength: %d rows (%d ranked), as of %s, leaders %s, coil %s, flows current %d/%d, 13f %d, %d bytes, %.1fs, errors %d" % (
        len(out["rows"]), b["n_ranked"], out["as_of"], [x["ticker"] for x in b["leaders"][:5]], [x["ticker"] for x in b["coil"][:5]],
        out["flows"]["n_universe_current"], out["flows"]["n_in_universe"], out["f13"]["n_in_universe"], n, out["run_seconds"], out["n_errors"]))
    return {"statusCode": 200, "key": OUT_KEY, "bytes": n, "rows": len(out["rows"]), "ranked": b["n_ranked"], "as_of": out["as_of"], "run_seconds": out["run_seconds"], "errors": out["n_errors"]}
