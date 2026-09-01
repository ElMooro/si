"""justhodl-fortress v1.0.0 -- FORTRESS COIL: dump-resilient accumulation radar.

Khalid's spec (2026-08-31), verbatim intent:
  stocks AND etfs that barely dipped / did not move / even rose during big
  market dumps (= being accumulated), that sit UNDER the 250 EMA, carry a
  LOW valuation, whose INDUSTRY ETF is seeing major INFLOWS, whose stock and
  industry show (preferably major) GROWTH and are gaining MOMENTUM, sit in a
  very TIGHT Bollinger band (accumulation, about to break out), preferably
  with a MAJOR BACKLOG and NEW CONTRACTS that are meaningful vs market cap --
  plus any other metric that makes the downside safe and the upside convex.

Built the way a fund's internal multi-factor screen is built:

  1. SIGNAL LIBRARY computed from the house bar warehouse
     (data/warm/polygon-full/grouped/*, full-market adjusted OHLCV, ~12.5k
     tickers/session): SPY drawdown episodes >= 4% and the stock's CAPTURE
     inside each one; behaviour on SPY's worst-5% days (excess bps, green
     rate); down/up capture and downside/upside beta; true EMA250/EMA200
     (SMA-seeded); Bollinger(20,2) bandwidth percentile vs its own 252
     sessions, Keltner containment (TTM squeeze) and days coiled; ATR and
     volume contraction; RS vs SPY and vs the name's own industry ETF;
     52w position; realized vol; max drawdown.  Zero external API calls.
  2. FUSION of fleet feeds, each bound by the ops-5080 census probe:
     finviz-universe (valuation, growth, safety, analyst PT, earnings date,
     ETF flags + ETF net flows), fundamental-census-matrix (Altman,
     Piotroski, Beneish, buyback/dilution), industry-boom (industry growth
     + 20d momentum), industry-rotation (leadership, RRG, dump-day excess),
     etf-flows/daily (Polygon creations/redemptions z-score + quadrant),
     etf-true-flows (dShares x NAV), backlog (XBRL RPO), backlog-mined
     (10-Q text), deal-history (contract wins vs market cap), catalyst,
     floor-audit (NLAV liquid floor + committed-revenue coverage),
     resilience (2-factor abnormal returns on adverse days), 13F flows,
     short-interest, insider clusters, estimate revisions, S&P 500 roster.
  3. GATES (the spec) -> tiers: FORTRESS_COIL (6/6) > COILED (5) >
     ACCUMULATING (4) > WATCH (3) > SCREENED.  Knife guard caps names whose
     drawdown is a collapse, not accumulation.
  4. CROSS-SECTIONAL SCORING: nine pillars, published weights, industry-
     neutral valuation percentiles, composite 0-100; ASYMMETRY = empirical
     upside room / empirical dump downside (capture applied to a -10% SPY
     dump, cushioned by the liquid floor).
  5. ACCOUNTABILITY: `top_picks` is auto-harvested by justhodl-signal-
     harvester (eng:fortress) for forward grading; daily snapshots under
     data/fortress/history/ let the engine compute its OWN 21-session base
     rates from the bar warehouse once they mature.  Honest None where a
     metric cannot be computed; never a default that masquerades as data.

OUTPUT  data/fortress.json  (+ data/fortress/history/{session}.json.gz)
SCHEDULE 03:30 UTC Tue-Sat (after polygon-full lands the prior session).

v2.0.0 -- evidence-grade rebuild (Khalid: "improve it exponentially"):
  * THREE YEARS of bars (760 sessions) so the dump statistics rest on every
    SPY drawdown since 2023, weighted by depth AND recency (half-life one
    year); capture measured two ways (peak-to-trough and on the market's
    down days inside each dump) and the two must agree; t-statistics and
    an evidence-confidence score shrink thin evidence toward neutral.
  * ACCUMULATION pillar built from volume structure, the way Wyckoff /
    Chaikin / O'Neil read it: OBV and A/D divergence vs price, up/down
    volume ratio, absorption (closing-location and volume on the market's
    worst days), FINRA dark-pool share and acceleration, 13F net, insider
    clusters, Congress purchases, ETF constituent flow pressure.
  * STRUCTURE pillar: higher-lows count, Minervini volatility-contraction
    sequence, O'Neil RS-line-at-new-high-before-price, base age and depth,
    Carter squeeze momentum (which way the coil is leaning).
  * Tail-risk safety: CVaR(5%), downside deviation, Ulcer index, gap risk,
    Amihud illiquidity, rate beta (TLT) and dollar beta (UUP).
  * Options overlay where the fleet covers the name: IV rank, variance
    risk premium, skew, put/call, net premium.
  * Rank-normalised composite (absolute anchors blended with cross-
    sectional percentiles), conviction (geometric mean of the six gate
    pillars) and evidence confidence.
  * Decision support: trade plan (pivot / stop / target / R:R), risk-
    parity sizing for the picks, what changed since the last session,
    the single trigger that would flip each COILED name to FORTRESS.
  * VALIDATION: event {"mode":"backtest"} walks forward through ~4 years
    of the bar warehouse every 15 sessions, scores the price-structure
    legs point-in-time, and reports 21/63-session excess returns vs SPY
    by gates passed and by capture decile (data/fortress-backtest.json,
    weekly). Fundamentals/flows are not backtested -- no look-ahead.
"""
import array
import bisect
import gzip
import json
import math
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "2.0.0"
ENGINE = "justhodl-fortress"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/fortress.json"
HIST_PREFIX = "data/fortress/history/"
BARS_ROOT = "data/warm/polygon-full/grouped/"

# ── parameters (published in the payload) ────────────────────────────────
P = {
    "sessions_loaded": 760,      # ~3 years: every SPY drawdown since 2023 feeds the capture statistics
    "lookback": 252,
    "episode_half_life": 252,    # recency weight on dump episodes (sessions)
    "backtest_sessions": 1050,
    "backtest_step": 15,
    "dump_min_dd_pct": -4.0,     # SPY peak-to-trough that counts as a dump
    "big_dump_dd_pct": -8.0,
    "worst_day_quantile": 0.05,
    "worst_day_min_n": 12,
    "capture_barely_dipped": 0.35,   # size-weighted capture <= 35% of the SPY dumps
    "capture_worst_max": 1.0,        # and never fell MORE than the market in any dump
    "bb_squeeze_pctile": 20.0,       # bandwidth in the bottom 20% of own year
    "bb_tight_pctile": 10.0,
    "keltner_atr_mult": 1.5,
    "min_price": 2.0,
    "min_adv_usd": 1_000_000,
    "min_sessions": 200,
    "knife_ret_3m_pct": -40.0,
    "knife_below_ema_pct": -35.0,
    "growth_major_pct": 20.0,
    "inflow_score_min": 55.0,
    "valuation_score_min": 55.0,
}
WEIGHTS = {  # composite pillars, sum 100
    "resilience": 20, "accumulation": 12, "coil": 11, "structure": 5,
    "location": 4, "valuation": 11, "growth": 9, "flows": 9, "momentum": 5,
    "backlog_contracts": 5, "safety": 9,
}
GATE_PILLARS = ["resilience", "coil", "location", "valuation", "growth", "flows"]
BACKTEST_KEY = "data/fortress-backtest.json"
GATE_NAMES = ["under_ema250", "dump_resilient", "coiled", "low_valuation",
              "growth", "industry_inflows"]
TIER_BY_GATES = {6: "FORTRESS_COIL", 5: "COILED", 4: "ACCUMULATING",
                 3: "WATCH"}

SECTOR_ETF = {
    "Technology": "XLK", "Communication Services": "XLC",
    "Healthcare": "XLV", "Financial": "XLF", "Industrials": "XLI",
    "Energy": "XLE", "Basic Materials": "XLB",
    "Consumer Cyclical": "XLY", "Consumer Defensive": "XLP",
    "Utilities": "XLU", "Real Estate": "XLRE",
}
# finviz industry -> industry ETF (ETFs that carry flow data in the fleet)
IND_ETF = {
    "Semiconductors": "SMH", "Semiconductor Equipment & Materials": "SMH",
    "Software - Application": "IGV", "Software - Infrastructure": "IGV",
    "Information Technology Services": "IGV",
    "Internet Content & Information": "FDN", "Internet Retail": "FDN",
    "Biotechnology": "XBI",
    "Residential Construction": "ITB",
    "Building Products & Equipment": "XHB", "Building Materials": "XHB",
    "Furnishings, Fixtures & Appliances": "XHB",
    "Banks - Regional": "KRE", "Banks - Diversified": "KBE",
    "Oil & Gas E&P": "XOP", "Oil & Gas Equipment & Services": "OIH",
    "Oil & Gas Drilling": "OIH",
    "Gold": "GDX", "Silver": "GDX", "Other Precious Metals & Mining": "GDX",
    "Copper": "COPX",
    "Steel": "XME", "Aluminum": "XME", "Other Industrial Metals & Mining": "XME",
    "Coking Coal": "XME", "Thermal Coal": "XME",
    "Uranium": "URA",
    "Specialty Retail": "XRT", "Apparel Retail": "XRT",
    "Department Stores": "XRT", "Home Improvement Retail": "XRT",
    "Discount Stores": "XRT",
    "Railroads": "IYT", "Trucking": "IYT",
    "Integrated Freight & Logistics": "IYT", "Marine Shipping": "IYT",
    "Airlines": "JETS", "Airports & Air Services": "JETS",
    "Aerospace & Defense": "ITA",
    "Solar": "TAN", "Utilities - Renewable": "ICLN",
    "Engineering & Construction": "PAVE", "Infrastructure Operations": "PAVE",
    "Farm & Heavy Construction Machinery": "PAVE",
    "Specialty Industrial Machinery": "PAVE",
    "Electrical Equipment & Parts": "GRID",
    "Auto Manufacturers": "KARS", "Auto Parts": "KARS",
    "Medical Devices": "IHI", "Medical Instruments & Supplies": "IHI",
    "Insurance - Property & Casualty": "KIE", "Insurance - Life": "KIE",
    "Insurance - Diversified": "KIE", "Insurance - Specialty": "KIE",
    "Insurance Brokers": "KIE", "Insurance - Reinsurance": "KIE",
}
IND_ETF_NAME = {
    "SMH": "Semiconductors", "IGV": "Software", "FDN": "Internet",
    "CIBR": "Cybersecurity", "XBI": "Biotech", "IBB": "Biotech (large)",
    "ITB": "Homebuilders", "XHB": "Homebuilders / Building",
    "KRE": "Regional Banks", "KBE": "Banks", "XOP": "Oil & Gas E&P",
    "OIH": "Oil Services", "GDX": "Gold Miners", "COPX": "Copper Miners",
    "XME": "Metals & Mining", "URA": "Uranium", "XRT": "Retail",
    "IYT": "Transports", "JETS": "Airlines", "ITA": "Aerospace & Defense",
    "TAN": "Solar", "ICLN": "Clean Energy", "PAVE": "Infrastructure",
    "GRID": "Grid / Electrification", "KARS": "Autos & EV",
    "IHI": "Medical Devices", "KIE": "Insurance", "LIT": "Lithium & Battery",
    "WCLD": "Cloud Software",
    "XLK": "Technology", "XLC": "Communication Services",
    "XLV": "Health Care", "XLF": "Financials", "XLI": "Industrials",
    "XLE": "Energy", "XLB": "Materials", "XLY": "Consumer Discretionary",
    "XLP": "Consumer Staples", "XLU": "Utilities", "XLRE": "Real Estate",
}
LEV_RX = re.compile(r"\b(2x|3x|-1x|ultra|bull|bear|inverse|short|leveraged|"
                    r"daily 2|daily 3)\b", re.I)
# products whose "resilience" is an option overlay or a volatility bet, not accumulation
OVERLAY_RX = re.compile(r"\bvix\b|volatility|market neutral|anti-beta|buffer|defined outcome|"
                        r"hedged|covered call|premium income|high income|enhanced (dividend|income)|"
                        r"option income|overlay|floor etf|managed futures|long/short|tail risk|"
                        r"target \d+|select income|advantage .*income|buy-?write|yieldmax|"
                        r"option (strategy|premium)|derivative income|income (strategy|plus)", re.I)
NON_OPERATING_RX = re.compile(r"^(closed-end fund|exchange traded fund|shell companies)", re.I)
TICKER_OK = re.compile(r"^[A-Z]{1,5}(\.[AB])?$")

s3 = boto3.client("s3", region_name="us-east-1")
LOG = []
T0 = time.time()


def log(msg):
    LOG.append("%6.1fs %s" % (time.time() - T0, msg))
    print("[fortress] " + LOG[-1])


# ── S3 helpers ───────────────────────────────────────────────────────────
def s3_json(key, default=None):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body)
        return json.loads(body)
    except Exception as e:  # noqa: BLE001
        log("read miss %s: %s" % (key, str(e)[:80]))
        return default


def s3_put_json(key, obj, gz=False):
    body = json.dumps(obj, default=str, separators=(",", ":")).encode()
    if gz:
        s3.put_object(Bucket=BUCKET, Key=key, Body=gzip.compress(body),
                      ContentType="application/gzip")
    else:
        s3.put_object(Bucket=BUCKET, Key=key, Body=body,
                      ContentType="application/json",
                      CacheControl="public, max-age=600")


def list_keys(prefix):
    out = []
    for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET,
                                                          Prefix=prefix):
        for o in pg.get("Contents", []):
            out.append(o["Key"])
    return out


# ── numeric helpers ──────────────────────────────────────────────────────
def fnum(x):
    if isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        return float(x) if math.isfinite(x) else None
    if isinstance(x, str):
        s = x.strip().replace(",", "").replace("%", "")
        if not s or s == "-":
            return None
        mult = 1.0
        if s[-1] in "KMBT":
            mult = {"K": 1e3, "M": 1e6, "B": 1e9, "T": 1e12}[s[-1]]
            s = s[:-1]
        try:
            return float(s) * mult
        except ValueError:
            return None
    return None


def clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def rnd(v, n=2):
    return None if v is None else round(v, n)


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else 0.5 * (xs[n // 2 - 1] + xs[n // 2])


def std(xs):
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def ols_beta(ys, xs):
    """slope of y on x; needs >= 20 pairs."""
    n = len(xs)
    if n < 20:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    vx = sum((x - mx) ** 2 for x in xs)
    if vx <= 0:
        return None
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / vx


def lin_map(v, x0, y0, x1, y1):
    """piecewise-linear map with clamping to [min(y0,y1), max(y0,y1)]."""
    if v is None:
        return None
    if x1 == x0:
        return y1
    t = (v - x0) / (x1 - x0)
    t = max(0.0, min(1.0, t))
    return y0 + t * (y1 - y0)


def pct_rank(values):
    """values: dict key->float. returns key->percentile 0..100 (higher value
    = higher percentile). Ties get the mean rank."""
    items = sorted((v, k) for k, v in values.items() if v is not None)
    n = len(items)
    out = {}
    i = 0
    while i < n:
        j = i
        while j + 1 < n and items[j + 1][0] == items[i][0]:
            j += 1
        r = (i + j) / 2.0
        p = 100.0 * (r + 0.5) / n if n > 1 else 50.0
        for k in range(i, j + 1):
            out[items[k][1]] = p
        i = j + 1
    return out


# ── technical library ────────────────────────────────────────────────────
def ema_last(vals, n):
    if len(vals) < n:
        return None
    k = 2.0 / (n + 1)
    e = sum(vals[:n]) / n
    for v in vals[n:]:
        e = v * k + e * (1 - k)
    return e


def ema_series(vals, n):
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


def sma(vals, n):
    return sum(vals[-n:]) / n if len(vals) >= n else None


def bb_series(c, n=20, k=2.0):
    """rolling Bollinger: returns (mid, upper, lower, width) lists."""
    m = len(c)
    mid = [None] * m
    up = [None] * m
    lo = [None] * m
    bw = [None] * m
    s = 0.0
    s2 = 0.0
    for i, v in enumerate(c):
        s += v
        s2 += v * v
        if i >= n:
            o = c[i - n]
            s -= o
            s2 -= o * o
        if i >= n - 1:
            mu = s / n
            var = max(0.0, s2 / n - mu * mu)
            sd = var ** 0.5
            mid[i] = mu
            up[i] = mu + k * sd
            lo[i] = mu - k * sd
            bw[i] = (2 * k * sd / mu) if mu > 0 else None
    return mid, up, lo, bw


def atr_series(h, lo, c, n=20):
    """Wilder ATR (RMA)."""
    m = len(c)
    out = [None] * m
    if m < n + 1:
        return out
    trs = [None] * m
    for i in range(1, m):
        pc = c[i - 1]
        trs[i] = max(h[i] - lo[i], abs(h[i] - pc), abs(lo[i] - pc))
    a = sum(trs[1:n + 1]) / n
    out[n] = a
    for i in range(n + 1, m):
        a = (a * (n - 1) + trs[i]) / n
        out[i] = a
    return out


def max_drawdown(c):
    peak = c[0]
    mdd = 0.0
    for v in c:
        if v > peak:
            peak = v
        dd = v / peak - 1
        if dd < mdd:
            mdd = dd
    return mdd * 100


def dump_episodes(c, min_dd):
    """SPY peak-to-trough drawdowns reaching <= min_dd (%) inside `c`.
    Returns [(peak_i, trough_i, dd_pct, closed)]; an open drawdown at the
    end is reported with closed=False."""
    eps = []
    peak = c[0]
    peak_i = 0
    trough = c[0]
    trough_i = 0
    in_dd = False
    maxdd = 0.0
    for i, v in enumerate(c):
        if v >= peak:
            if in_dd and maxdd <= min_dd:
                eps.append((peak_i, trough_i, maxdd, True))
            peak, peak_i, trough, trough_i = v, i, v, i
            in_dd, maxdd = False, 0.0
        else:
            in_dd = True
            if v < trough:
                trough, trough_i = v, i
            dd = (v / peak - 1) * 100
            if dd < maxdd:
                maxdd = dd
    if in_dd and maxdd <= min_dd:
        eps.append((peak_i, trough_i, maxdd, False))
    return eps


# ── bar warehouse ────────────────────────────────────────────────────────
class Bars:
    """Per-ticker aligned arrays: d = session index, o/c/h/l/v floats."""
    __slots__ = ("d", "c", "h", "l", "v", "o")

    def __init__(self):
        self.d = array.array("i")
        self.c = array.array("d")
        self.h = array.array("d")
        self.l = array.array("d")
        self.v = array.array("d")
        self.o = array.array("d")

    def pos_at_or_before(self, idx):
        p = bisect.bisect_right(self.d, idx) - 1
        return p if p >= 0 else None

    def pos_exact(self, idx):
        p = bisect.bisect_left(self.d, idx)
        return p if p < len(self.d) and self.d[p] == idx else None


def session_keys(n):
    now = datetime.now(timezone.utc)
    keys = []
    for yr in range(now.year - 6, now.year + 1):
        keys.extend(k for k in list_keys(BARS_ROOT + "%d/" % yr)
                    if k.endswith(".json.gz"))
    keys.sort()
    return keys[-n:]


def load_session(key, keep):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    j = json.loads(gzip.decompress(body))
    out = []
    for r in j.get("results") or []:
        t = r.get("T")
        c = r.get("c")
        if not t or c is None or c <= 0:
            continue
        if keep is not None:
            if t not in keep:
                continue
        elif not TICKER_OK.match(t):
            continue
        out.append((t, float(c), float(r.get("h") or c),
                    float(r.get("l") or c), float(r.get("v") or 0.0),
                    float(r.get("o") or c)))
    return out


def load_bars(keys, keep, workers=12):
    bars = {}
    dates = [k.rsplit("/", 1)[1][:10] for k in keys]
    chunk = 24
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for start in range(0, len(keys), chunk):
            sub = keys[start:start + chunk]
            for off, rows in enumerate(ex.map(lambda k: load_session(k, keep),
                                              sub)):
                idx = start + off
                for t, c, h, lo, v, o in rows:
                    b = bars.get(t)
                    if b is None:
                        b = Bars()
                        bars[t] = b
                    b.d.append(idx)
                    b.c.append(c)
                    b.h.append(h)
                    b.l.append(lo)
                    b.v.append(v)
                    b.o.append(o)
            log("bars %d/%d sessions, %d tickers" % (
                min(start + chunk, len(keys)), len(keys), len(bars)))
    return dates, bars


# ── market context (SPY) ─────────────────────────────────────────────────
def market_context(spy, dates, n_sessions, asof_pos=None, window=None):
    """SPY context. Episodes are detected over the FULL loaded window (so
    every drawdown of the last ~3 years feeds the capture statistics) and
    carry a recency weight; the worst-day set is the trailing 252 sessions
    (the current tape's behaviour). asof_pos restricts everything to
    positions <= asof_pos (backtest mode: point-in-time, no look-ahead)."""
    lb = P["lookback"]
    c = list(spy.c)
    d = list(spy.d)
    if asof_pos is not None:
        c = c[:asof_pos + 1]
        d = d[:asof_pos + 1]
    if window:
        c = c[-window:]
        d = d[-window:]
    last_idx = d[-1]
    eps = []
    hl = P["episode_half_life"]
    for pk, tr, dd, closed in dump_episodes(c, P["dump_min_dd_pct"]):
        age = last_idx - d[tr]
        eps.append({"peak_idx": d[pk], "trough_idx": d[tr],
                    "peak_date": dates[d[pk]], "trough_date": dates[d[tr]],
                    "spy_dd_pct": round(dd, 2), "closed": closed,
                    "sessions": d[tr] - d[pk], "age_sessions": age,
                    "big": dd <= P["big_dump_dd_pct"],
                    "recent": age <= lb,
                    "weight": round(abs(dd) * (0.5 ** (age / hl)), 3)})
    rets = {}
    for i in range(1, len(c)):
        if d[i] - d[i - 1] == 1:
            rets[d[i]] = c[i] / c[i - 1] - 1
    start = max(0, len(c) - lb)
    dl = d[start:]
    cl = c[start:]
    lb_rets = {k: v for k, v in rets.items() if k >= dl[0]}
    k = max(P["worst_day_min_n"], int(len(lb_rets) * P["worst_day_quantile"]))
    worst = sorted(lb_rets.items(), key=lambda kv: kv[1])[:k]
    worst_idx = {i for i, _ in worst}
    # long worst-day set over the whole window (robustness statistics)
    k_long = max(P["worst_day_min_n"], int(len(rets) * P["worst_day_quantile"]))
    worst_long = sorted(rets.items(), key=lambda kv: kv[1])[:k_long]
    ema250 = ema_last(c, 250)
    ema200 = ema_last(c, 200)
    close_by_idx = {d[i]: c[i] for i in range(len(c))}
    return {
        "episodes": eps, "rets": rets, "lb_rets": lb_rets,
        "worst_idx": worst_idx, "worst_idx_long": {i for i, _ in worst_long},
        "close_by_idx": close_by_idx, "last_idx": last_idx,
        "worst_days": [{"date": dates[i], "spy_ret_pct": round(r * 100, 2)}
                       for i, r in worst],
        "n_episodes": len(eps), "n_episodes_recent": sum(1 for e in eps if e["recent"]),
        "n_big_dumps": sum(1 for e in eps if e["big"]),
        "spy_close": c[-1], "spy_ema250": ema250, "spy_ema200": ema200,
        "spy_vs_ema250_pct": (rnd((c[-1] / ema250 - 1) * 100)
                              if ema250 else None),
        "spy_ret_1m_pct": rnd((c[-1] / c[-22] - 1) * 100) if len(c) > 22 else None,
        "spy_ret_3m_pct": rnd((c[-1] / c[-64] - 1) * 100) if len(c) > 64 else None,
        "spy_dd_from_high_pct": rnd((c[-1] / max(cl) - 1) * 100),
        "lookback_start": dates[dl[0]], "lookback_sessions": len(cl),
        "window_start": dates[d[0]], "window_sessions": len(c),
        "n_sessions_loaded": n_sessions,
    }


# ── per-ticker price signals ─────────────────────────────────────────────
def linreg_last(vals):
    """value of the least-squares line at the last point (Carter-style momentum)."""
    n = len(vals)
    if n < 3:
        return None
    xs = range(n)
    mx = (n - 1) / 2.0
    my = sum(vals) / n
    vx = sum((x - mx) ** 2 for x in xs)
    if vx <= 0:
        return None
    sl = sum((x - mx) * (y - my) for x, y in zip(xs, vals)) / vx
    return my + sl * (n - 1 - mx)


def swing_points(c, w=5):
    """indexes of local minima / maxima of closes (window +-w)."""
    lows, highs = [], []
    n = len(c)
    for i in range(w, n - w):
        seg = c[i - w:i + w + 1]
        if c[i] == min(seg) and seg.count(c[i]) == 1:
            lows.append(i)
        elif c[i] == max(seg) and seg.count(c[i]) == 1:
            highs.append(i)
    return lows, highs


def price_signals(b, mkt, etf_bars, dates, aux=None):
    n = len(b.c)
    c = list(b.c)
    h = list(b.h)
    lo = list(b.l)
    v = list(b.v)
    o = list(b.o)
    d = b.d
    out = {"n_sessions": n}
    last = c[-1]
    out["close"] = last
    out["last_session"] = dates[d[-1]]
    adv20 = mean([c[i] * v[i] for i in range(max(0, n - 20), n)])
    out["adv_usd_20d"] = adv20
    # ---- moving averages / location
    ema250 = ema_last(c, 250)
    ema200 = ema_last(c, 200)
    out["ema250"] = ema250
    out["ema200"] = ema200
    out["ema250_available"] = ema250 is not None
    out["vs_ema250_pct"] = (last / ema250 - 1) * 100 if ema250 else None
    out["vs_ema200_pct"] = (last / ema200 - 1) * 100 if ema200 else None
    s250 = sma(c, 250)
    s200 = sma(c, 200)
    s50 = sma(c, 50)
    out["vs_sma250_pct"] = (last / s250 - 1) * 100 if s250 else None
    out["vs_sma200_pct"] = (last / s200 - 1) * 100 if s200 else None
    out["vs_sma50_pct"] = (last / s50 - 1) * 100 if s50 else None
    # ---- returns / momentum
    def ret(k):
        return (last / c[-k - 1] - 1) * 100 if n > k else None
    out["ret_1m_pct"] = ret(21)
    out["ret_3m_pct"] = ret(63)
    out["ret_6m_pct"] = ret(126)
    out["ret_12m_pct"] = ret(252)
    lb = c[-P["lookback"]:]
    hi52 = max(lb)
    lo52 = min(lb)
    out["pct_from_52w_high"] = (last / hi52 - 1) * 100
    out["pct_above_52w_low"] = (last / lo52 - 1) * 100 if lo52 > 0 else None
    out["max_dd_1y_pct"] = max_drawdown(lb)
    hi_pos = len(lb) - 1 - lb[::-1].index(hi52)
    out["weeks_since_52w_high"] = round((len(lb) - 1 - hi_pos) / 5.0, 1)
    # ---- realized vol / tail
    lr = [math.log(c[i] / c[i - 1]) for i in range(1, n) if c[i - 1] > 0]
    v20 = std(lr[-20:]) if len(lr) >= 20 else None
    v100 = std(lr[-100:]) if len(lr) >= 100 else None
    out["vol_20d_pct"] = v20 * math.sqrt(252) * 100 if v20 else None
    out["vol_100d_pct"] = v100 * math.sqrt(252) * 100 if v100 else None
    out["vol_contraction"] = (v20 / v100) if (v20 and v100) else None
    r1y = [c[i] / c[i - 1] - 1 for i in range(max(1, n - 252), n)]
    if len(r1y) >= 60:
        srt = sorted(r1y)
        k5 = max(3, int(len(srt) * 0.05))
        out["cvar5_pct"] = mean(srt[:k5]) * 100
        out["downside_dev_pct"] = (mean([min(x, 0.0) ** 2 for x in r1y]) ** 0.5) * math.sqrt(252) * 100
        peak = lb[0]
        dds = []
        for x in lb:
            peak = max(peak, x)
            dds.append((x / peak - 1) * 100)
        out["ulcer_index"] = (mean([x * x for x in dds]) ** 0.5)
    else:
        out["cvar5_pct"] = None
        out["downside_dev_pct"] = None
        out["ulcer_index"] = None
    gaps = [abs(o[i] / c[i - 1] - 1) for i in range(max(1, n - 252), n) if c[i - 1] > 0 and o[i] > 0]
    out["gaps_5pct_1y"] = sum(1 for g in gaps if g >= 0.05) if gaps else None
    out["max_gap_pct_1y"] = (max(gaps) * 100) if gaps else None
    ami = [abs(c[i] / c[i - 1] - 1) / (c[i] * v[i]) for i in range(max(1, n - 63), n) if c[i] * v[i] > 0]
    out["amihud_illiq"] = (mean(ami) * 1e6) if ami else None
    # ---- Bollinger / Keltner coil
    W = P["lookback"]
    mid, up, lob, bw = bb_series(c, 20, 2.0)
    cur_bw = bw[-1]
    hist_bw = [x for x in bw[-W:] if x is not None]
    if cur_bw is not None and len(hist_bw) >= 60:
        out["bb_width_pct"] = cur_bw * 100
        out["bb_width_pctile"] = 100.0 * sum(1 for x in hist_bw if x <= cur_bw) / len(hist_bw)
        out["bb_width_6m_low"] = cur_bw <= min(x for x in bw[-126:] if x is not None)
        rng = (up[-1] - lob[-1])
        out["bb_pct_b"] = ((last - lob[-1]) / rng) if rng > 0 else None
        out["bb_upper"] = up[-1]
        out["bb_lower"] = lob[-1]
    else:
        out["bb_width_pct"] = None
        out["bb_width_pctile"] = None
        out["bb_width_6m_low"] = None
        out["bb_pct_b"] = None
    atr = atr_series(h, lo, c, 20)
    atr100 = atr_series(h, lo, c, 100) if n > 101 else [None] * n
    ema20 = ema_series(c, 20)
    sq = []
    km = P["keltner_atr_mult"]
    for i in range(max(0, n - 60), n):
        if up[i] is None or atr[i] is None or ema20[i] is None:
            sq.append(None)
            continue
        ku = ema20[i] + km * atr[i]
        kl = ema20[i] - km * atr[i]
        sq.append(up[i] < ku and lob[i] > kl)
    out["ttm_squeeze_on"] = sq[-1] if sq else None
    days = 0
    for x in reversed(sq):
        if x:
            days += 1
        else:
            break
    out["squeeze_days"] = days
    out["atr20_pct"] = (atr[-1] / last * 100) if atr[-1] else None
    out["atr_contraction"] = (atr[-1] / atr100[-1]) if (atr[-1] and atr100[-1]) else None
    adv100 = mean(v[-100:]) if n >= 100 else None
    a20 = mean(v[-20:])
    out["volume_dryup"] = (a20 / adv100) if (a20 and adv100) else None
    out["rel_volume_last"] = (v[-1] / a20) if a20 else None
    # Carter squeeze momentum: close vs mid of (Donchian mid, SMA20), linreg over 20
    if n >= 40 and mid[-1] is not None:
        vals = []
        for i in range(n - 20, n):
            hh = max(h[i - 19:i + 1])
            ll = min(lo[i - 19:i + 1])
            vals.append(c[i] - ((hh + ll) / 2.0 + mid[i]) / 2.0)
        mom = linreg_last(vals)
        out["squeeze_momentum_atr"] = (mom / atr[-1]) if (mom is not None and atr[-1]) else None
        out["squeeze_momentum_rising"] = (vals[-1] > vals[-2]) if len(vals) >= 2 else None
    else:
        out["squeeze_momentum_atr"] = None
        out["squeeze_momentum_rising"] = None
    top60 = max(c[-60:])
    out["range_low_60"] = min(c[-60:])
    out["breakout_level"] = top60
    out["dist_to_breakout_pct"] = (top60 / last - 1) * 100
    if up[-1] is not None and last > up[-1] and (out["rel_volume_last"] or 0) >= 1.5 \
            and last >= top60 * 0.995:
        out["coil_state"] = "IGNITED"
    elif out.get("bb_pct_b") is not None and out["bb_pct_b"] >= 0.7 \
            and out["dist_to_breakout_pct"] <= 3.0:
        out["coil_state"] = "PRE_BREAKOUT"
    elif (out.get("bb_width_pctile") is not None
          and out["bb_width_pctile"] <= P["bb_squeeze_pctile"]) or out["ttm_squeeze_on"]:
        out["coil_state"] = "COILING"
    else:
        out["coil_state"] = "LOOSE"
    # ---- structure: higher lows / VCP / base
    seg = c[-120:]
    lows_i, highs_i = swing_points(seg, 5)
    hl_count = 0
    if len(lows_i) >= 2:
        for j in range(len(lows_i) - 1, 0, -1):
            if seg[lows_i[j]] > seg[lows_i[j - 1]]:
                hl_count += 1
            else:
                break
    out["higher_lows"] = hl_count if lows_i else None
    contractions = []
    if highs_i and lows_i:
        pts = sorted([(i, "H") for i in highs_i] + [(i, "L") for i in lows_i])
        last_h = None
        for i, kind in pts:
            if kind == "H":
                last_h = seg[i]
            elif last_h:
                contractions.append(round((1 - seg[i] / last_h) * 100, 1))
                last_h = None
    out["vcp_contractions"] = contractions[-4:]
    if len(contractions) >= 2:
        out["vcp_ok"] = bool(contractions[-1] < contractions[-2] and contractions[-1] <= 12.0)
        out["vcp_strict"] = bool(len(contractions) >= 3 and contractions[-1] < contractions[-2] < contractions[-3]
                                 and contractions[-1] <= 10.0)
    else:
        out["vcp_ok"] = None
        out["vcp_strict"] = None
    # ---- volume structure: OBV / A-D / up-down volume / absorption
    if n >= 66:   # obv_series has n-1 entries; the 63-session slope needs index -64
        obv = 0.0
        ad = 0.0
        obv_series = []
        ad_series = []
        upv = 0.0
        dnv = 0.0
        for i in range(1, n):
            chg = c[i] - c[i - 1]
            obv += v[i] if chg > 0 else (-v[i] if chg < 0 else 0.0)
            rng = h[i] - lo[i]
            clv = ((c[i] - lo[i]) - (h[i] - c[i])) / rng if rng > 0 else 0.0
            ad += clv * v[i]
            obv_series.append(obv)
            ad_series.append(ad)
            if i >= n - 50:
                if chg > 0:
                    upv += v[i]
                elif chg < 0:
                    dnv += v[i]
        av63 = mean(v[-63:]) or 1.0
        out["obv_slope_63"] = (obv_series[-1] - obv_series[-64]) / (av63 * 63)   # -1..1 units of ADV/day
        out["ad_slope_63"] = (ad_series[-1] - ad_series[-64]) / (av63 * 63)
        out["updown_volume_ratio_50"] = (upv / dnv) if dnv > 0 else None
        r63 = out.get("ret_3m_pct")
        out["obv_divergence"] = bool(out["obv_slope_63"] > 0.05 and r63 is not None and r63 <= 2.0)
        out["ad_divergence"] = bool(out["ad_slope_63"] > 0.05 and r63 is not None and r63 <= 2.0)
    else:
        for k in ("obv_slope_63", "ad_slope_63", "updown_volume_ratio_50"):
            out[k] = None
        out["obv_divergence"] = None
        out["ad_divergence"] = None
    # ---- dump resilience vs SPY
    lb_rets = mkt["lb_rets"]
    all_rets = mkt["rets"]
    pos_by_idx = {}
    for p_ in range(n):
        pos_by_idx[d[p_]] = p_
    pairs_s = []
    pairs_m = []
    worst_rows = []
    worst_long = []
    for idx, mr in all_rets.items():
        p_ = pos_by_idx.get(idx)
        if p_ is None or p_ == 0 or d[p_ - 1] != idx - 1:
            continue
        sr = c[p_] / c[p_ - 1] - 1
        if idx in lb_rets:
            pairs_s.append(sr)
            pairs_m.append(mr)
        if idx in mkt["worst_idx"]:
            rng = h[p_] - lo[p_]
            clv = ((c[p_] - lo[p_]) / rng) if rng > 0 else 0.5
            a20_ = mean(v[max(0, p_ - 20):p_]) or None
            worst_rows.append((idx, sr, mr, clv, (v[p_] / a20_) if a20_ else None))
        if idx in mkt["worst_idx_long"]:
            worst_long.append((sr - mr) * 1e4)
    out["n_days_matched"] = len(pairs_s)
    if len(pairs_s) >= 60:
        out["beta_1y"] = ols_beta(pairs_s, pairs_m)
        dn = [(s_, m_) for s_, m_ in zip(pairs_s, pairs_m) if m_ < 0]
        upp = [(s_, m_) for s_, m_ in zip(pairs_s, pairs_m) if m_ > 0]
        out["down_beta"] = ols_beta([x[0] for x in dn], [x[1] for x in dn]) if len(dn) >= 20 else None
        out["up_beta"] = ols_beta([x[0] for x in upp], [x[1] for x in upp]) if len(upp) >= 20 else None
        out["beta_asymmetry"] = ((out["up_beta"] - out["down_beta"])
                                 if (out["up_beta"] is not None and out["down_beta"] is not None) else None)
        ms = mean([m_ for _, m_ in dn])
        out["down_capture_pct"] = (mean([s_ for s_, _ in dn]) / ms * 100) if (dn and ms) else None
        mu = mean([m_ for _, m_ in upp])
        out["up_capture_pct"] = (mean([s_ for s_, _ in upp]) / mu * 100) if (upp and mu) else None
        out["corr_spy"] = None
        ss, sm = std(pairs_s), std(pairs_m)
        if ss and sm:
            msx = sum(pairs_s) / len(pairs_s)
            mmx = sum(pairs_m) / len(pairs_m)
            cov = sum((a - msx) * (bb - mmx) for a, bb in zip(pairs_s, pairs_m)) / (len(pairs_s) - 1)
            out["corr_spy"] = cov / (ss * sm)
    else:
        for k in ("beta_1y", "down_beta", "up_beta", "beta_asymmetry",
                  "down_capture_pct", "up_capture_pct", "corr_spy"):
            out[k] = None
    if len(worst_rows) >= max(6, P["worst_day_min_n"] // 2):
        ex = [(s_ - m_) * 1e4 for _, s_, m_, _, _ in worst_rows]
        out["worst_days_n"] = len(worst_rows)
        out["worst_days_excess_bps"] = mean(ex)
        sdx = std(ex)
        out["worst_days_tstat"] = (mean(ex) / (sdx / math.sqrt(len(ex)))) if sdx else None
        out["worst_days_green_rate"] = sum(1 for _, s_, _, _, _ in worst_rows if s_ >= 0) / len(worst_rows)
        out["worst_days_held_rate"] = sum(1 for _, s_, m_, _, _ in worst_rows if s_ >= 0.25 * m_) / len(worst_rows)
        out["worst_days_mean_ret_pct"] = mean([s_ for _, s_, _, _, _ in worst_rows]) * 100
        out["absorption_clv"] = mean([clv for _, _, _, clv, _ in worst_rows])
        rv = [rv_ for _, _, _, _, rv_ in worst_rows if rv_ is not None]
        out["worst_days_rel_volume"] = mean(rv) if rv else None
    else:
        out["worst_days_n"] = len(worst_rows)
        for k in ("worst_days_excess_bps", "worst_days_tstat", "worst_days_green_rate",
                  "worst_days_held_rate", "worst_days_mean_ret_pct", "absorption_clv",
                  "worst_days_rel_volume"):
            out[k] = None
    out["worst_days_long_n"] = len(worst_long)
    out["worst_days_long_excess_bps"] = mean(worst_long) if len(worst_long) >= 12 else None
    # episodes (three years, depth- and recency-weighted; two capture reads)
    ep_rows = []
    for e in mkt["episodes"]:
        p0 = b.pos_at_or_before(e["peak_idx"])
        p1 = b.pos_at_or_before(e["trough_idx"])
        if p0 is None or p1 is None or p1 <= p0:
            continue
        if e["peak_idx"] - d[p0] > 5 or e["trough_idx"] - d[p1] > 5:
            continue  # stock was not trading through the window
        sr = (c[p1] / c[p0] - 1) * 100
        cap = sr / e["spy_dd_pct"] if e["spy_dd_pct"] else None
        # in-episode down-day capture: stock vs SPY on the market's red days only
        sd_ = 0.0
        md_ = 0.0
        for pp in range(p0 + 1, p1 + 1):
            idx = d[pp]
            mr = all_rets.get(idx)
            if mr is None or mr >= 0 or d[pp - 1] != idx - 1:
                continue
            sd_ += c[pp] / c[pp - 1] - 1
            md_ += mr
        dcap = (sd_ / md_) if md_ < 0 else None
        ep_rows.append({"peak_date": e["peak_date"], "trough_date": e["trough_date"],
                        "spy_pct": e["spy_dd_pct"], "stock_pct": round(sr, 2),
                        "capture": (round(cap, 2) if cap is not None else None),
                        "day_capture": (round(dcap, 2) if dcap is not None else None),
                        "big": e["big"], "closed": e["closed"], "recent": e["recent"],
                        "weight": e["weight"]})
    out["episodes"] = ep_rows
    out["n_episodes"] = len(ep_rows)
    out["n_episodes_recent"] = sum(1 for x in ep_rows if x["recent"])
    caps = [x["capture"] for x in ep_rows if x["capture"] is not None]
    out["capture_median"] = median(caps)
    out["capture_mean"] = mean(caps)
    out["capture_worst"] = max(caps) if caps else None
    wsum = sum(x["weight"] for x in ep_rows if x["capture"] is not None)
    out["capture_weighted"] = (sum(x["capture"] * x["weight"] for x in ep_rows if x["capture"] is not None) / wsum
                               if wsum else None)
    dsum = sum(x["weight"] for x in ep_rows if x["day_capture"] is not None)
    out["capture_days_weighted"] = (sum(x["day_capture"] * x["weight"] for x in ep_rows if x["day_capture"] is not None) / dsum
                                    if dsum else None)
    rc = [x for x in ep_rows if x["recent"] and x["capture"] is not None]
    rw = sum(x["weight"] for x in rc)
    out["capture_recent_1y"] = (sum(x["capture"] * x["weight"] for x in rc) / rw) if rw else None
    if caps:
        wx = max(ep_rows, key=lambda x: (x["capture"] if x["capture"] is not None else -9))
        out["worst_episode"] = "%s..%s SPY %.1f%% stock %+.1f%%" % (wx["peak_date"], wx["trough_date"], wx["spy_pct"], wx["stock_pct"])
    else:
        out["worst_episode"] = None
    out["flat_or_up_share"] = (sum(1 for x in ep_rows if x["stock_pct"] >= 0) / len(ep_rows)) if ep_rows else None
    out["barely_dipped_share"] = (sum(1 for x in caps if x <= P["capture_barely_dipped"]) / len(caps)) if caps else None
    big = [x["capture"] for x in ep_rows if x["big"] and x["capture"] is not None]
    out["capture_big_dumps"] = median(big)
    if caps:
        out["dump_capture"] = out["capture_weighted"]
        out["dump_capture_basis"] = "episodes"
    elif out.get("down_capture_pct") is not None:
        out["dump_capture"] = out["down_capture_pct"] / 100.0
        out["dump_capture_basis"] = "down_days"
    else:
        out["dump_capture"] = None
        out["dump_capture_basis"] = None
    # evidence confidence for the resilience read (0..1)
    n_ep = len(caps)
    wn = out["worst_days_n"] or 0
    out["resilience_confidence"] = round(0.5 * min(1.0, n_ep / 4.0) + 0.5 * min(1.0, wn / 12.0), 2)
    # ---- RS line vs SPY (O'Neil): ratio at/near a 52-week high while price is not
    cbi = mkt.get("close_by_idx") or {}
    rs_line = []
    for p_ in range(max(0, n - 252), n):
        sc = cbi.get(d[p_])
        if sc:
            rs_line.append(c[p_] / sc)
    if len(rs_line) >= 120:
        rs_hi = max(rs_line)
        out["rs_line_vs_high_pct"] = (rs_line[-1] / rs_hi - 1) * 100
        out["rs_line_new_high"] = out["rs_line_vs_high_pct"] >= -2.0
        out["rs_leading"] = bool(out["rs_line_new_high"] and out["pct_from_52w_high"] <= -10.0)
    else:
        out["rs_line_vs_high_pct"] = None
        out["rs_line_new_high"] = None
        out["rs_leading"] = None
    # ---- macro betas (rates / dollar) when the hedges are in the tape
    if aux:
        for key, pack in aux.items():
            ab, apos = pack if pack else (None, None)
            if ab is None or len(pairs_s) < 60:
                out[key] = None
                continue
            xs, ys = [], []
            for idx in lb_rets:
                p_ = pos_by_idx.get(idx)
                q_ = apos.get(idx)
                if p_ is None or q_ is None or p_ == 0 or q_ == 0 or d[p_ - 1] != idx - 1 or ab.d[q_ - 1] != idx - 1:
                    continue
                ys.append(c[p_] / c[p_ - 1] - 1)
                xs.append(ab.c[q_] / ab.c[q_ - 1] - 1)
            out[key] = ols_beta(ys, xs) if len(xs) >= 60 else None
    return out


def rs_vs(b, eb, k=63):
    if eb is None or len(b.c) <= k or len(eb.c) <= k:
        return None
    return ((b.c[-1] / b.c[-k - 1] - 1) - (eb.c[-1] / eb.c[-k - 1] - 1)) * 100


# ── feed loaders ─────────────────────────────────────────────────────────
def load_feeds():
    F = {}
    t = time.time()
    fv = s3_json("data/finviz-universe.json", {}) or {}
    F["finviz"] = fv.get("by_ticker") or {}
    F["finviz_asof"] = fv.get("generated_at")
    cm = s3_json("data/fundamental-census-matrix.json", {}) or {}
    cols = cm.get("cols") or {}
    tks = cm.get("tickers") or []
    census = {}
    want = ["altman_z", "piotroski_f", "beneish_m", "roic_pct", "fcf_yield_pct",
            "net_buyback_yield_pct", "share_count_yoy_pct", "netdebt_to_ebitda_ttm",
            "interest_coverage_ttm", "revenue_yoy_pct", "eps_yoy_pct",
            "revenue_cagr_3y_pct", "earnings_in_days", "upside_pct", "downside_pct",
            "peg_ttm", "ev_ebitda_ttm", "pe_ttm", "ps_ttm", "fcf_ev_yield_pct",
            "cash", "totalDebt", "netDebt", "mcap", "sloan_accruals_pct",
            "inst_net_usd_m", "whale_net_usd_m"]
    if isinstance(tks, list) and isinstance(cols, dict):
        for i, tk in enumerate(tks):
            row = {}
            for w in want:
                col = cols.get(w)
                if isinstance(col, list) and i < len(col):
                    row[w] = fnum(col[i])
            census[str(tk).upper()] = row
    F["census"] = census
    F["census_asof"] = cm.get("generated_at")
    ib = s3_json("data/industry-boom.json", {}) or {}
    F["boom"] = {str(r.get("industry")): r for r in (ib.get("league") or []) if isinstance(r, dict)}
    F["boom_asof"] = ib.get("generated_at")
    ir = s3_json("data/industry-rotation.json", {}) or {}
    F["rotation"] = {r.get("etf"): r for r in (ir.get("ladder") or []) if isinstance(r, dict) and r.get("etf")}
    F["rrg"] = ir.get("rrg") if isinstance(ir.get("rrg"), dict) else {}
    F["rotation_asof"] = ir.get("generated_at")
    ef = s3_json("etf-flows/daily.json", {}) or {}
    F["flows_poly"] = {m.get("ticker"): m for m in (ef.get("metrics") or [])
                       if isinstance(m, dict) and m.get("ticker") and not m.get("error")}
    F["flows_poly_asof"] = ef.get("generated_at") or ef.get("as_of")
    tf = s3_json("data/etf-true-flows.json", {}) or {}
    F["flows_true"] = tf.get("by_etf") or {}
    F["flows_true_asof"] = tf.get("generated_at") or tf.get("as_of")
    bk = s3_json("data/backlog.json", {}) or {}
    F["backlog"] = bk.get("by_ticker") or {}
    F["backlog_asof"] = bk.get("generated_at")
    bm = s3_json("data/backlog-mined.json", {}) or {}
    F["backlog_mined"] = bm.get("by_ticker") or {}
    F["backlog_mined_asof"] = bm.get("as_of")
    dh = s3_json("data/deal-history.json", {}) or {}
    contracts = {}
    cut = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
    for e in (dh.get("entries") or {}).values():
        if not isinstance(e, dict):
            continue
        et = str(e.get("event_type") or "")
        if et not in ("contract_win", "govt_contract", "licensing_supply"):
            continue
        if (e.get("announce") or "") < cut:
            continue
        sym = str(e.get("sym") or "").upper()
        val = fnum(e.get("val"))
        if not sym or not val:
            continue
        c = contracts.setdefault(sym, {"n": 0, "usd": 0.0, "vs_mcap_pct": 0.0, "last": None, "types": set()})
        c["n"] += 1
        c["usd"] += val
        c["vs_mcap_pct"] += fnum(e.get("vs_mc")) or 0.0
        c["types"].add(et)
        if not c["last"] or e["announce"] > c["last"]:
            c["last"] = e["announce"]
    for c in contracts.values():
        c["types"] = sorted(c["types"])
    F["contracts"] = contracts
    F["deal_hist_asof"] = dh.get("generated_at")
    ct = s3_json("data/catalyst.json", {}) or {}
    F["catalyst"] = ct.get("by_ticker") or {}
    F["catalyst_asof"] = ct.get("as_of")
    fa = s3_json("data/floor-audit.json", {}) or {}
    F["floor_deep"] = fa.get("tickers") or {}
    F["floor_screen"] = {str(r.get("ticker")).upper(): r for r in (fa.get("screen") or []) if isinstance(r, dict)}
    F["floor_asof"] = fa.get("as_of")
    rs = s3_json("data/resilience.json", {}) or {}
    res = {}
    for k in ("all_resilient", "about_to_boom"):
        for r in rs.get(k) or []:
            if isinstance(r, dict) and r.get("ticker"):
                res.setdefault(r["ticker"], r)
    F["resilience"] = res
    F["resilience_asof"] = rs.get("generated_at")
    f13 = s3_json("data/13f-flows-by-ticker.json", {}) or {}
    F["f13"] = f13.get("t") or {}
    F["f13_asof"] = f13.get("as_of")
    si = s3_json("data/short-interest.json", {}) or {}
    F["short"] = si.get("by_ticker") or {}
    F["short_asof"] = si.get("generated_at")
    ins = s3_json("data/insider-radar.json", {}) or {}
    ib_ = {}
    for r in ins.get("latest_buys") or []:
        if isinstance(r, dict) and r.get("ticker"):
            e = ib_.setdefault(r["ticker"], {"n_buys": 0, "usd": 0.0, "last": None, "cluster": False})
            e["n_buys"] += 1
            e["usd"] += fnum(r.get("value")) or 0.0
            if not e["last"] or str(r.get("date")) > e["last"]:
                e["last"] = str(r.get("date"))
    for r in ins.get("clusters") or []:
        if isinstance(r, dict) and r.get("ticker"):
            e = ib_.setdefault(r["ticker"], {"n_buys": 0, "usd": 0.0, "last": None, "cluster": False})
            e["cluster"] = True
            e["n_insiders"] = r.get("n_insiders")
    F["insider"] = ib_
    F["insider_asof"] = ins.get("generated_at")
    er = s3_json("data/estimate-revisions.json", {}) or {}
    rev = {}
    for k in ("upward_revisions", "downward_revisions"):
        for r in er.get(k) or []:
            if isinstance(r, dict) and r.get("ticker"):
                rev[r["ticker"]] = {"eps_rev_pct": fnum(r.get("eps_rev_pct")),
                                    "direction": r.get("direction")}
    F["revisions"] = rev
    F["revisions_asof"] = er.get("generated_at")
    sp = s3_json("data/sp500.json", {}) or {}
    spx = set()
    mf = sp.get("member_fields") or []
    for m in sp.get("members") or []:
        if isinstance(m, dict):
            t_ = m.get("ticker") or m.get("symbol")
        elif isinstance(m, list) and m:
            t_ = m[0]
        else:
            t_ = None
        if t_:
            spx.add(str(t_).upper())
    F["sp500"] = spx
    F["sp500_asof"] = sp.get("as_of")
    # ---- v2 feeds
    se = s3_json("etf-flows/stock-exposure-lookup.json", {}) or {}
    F["stock_exposure"] = se if isinstance(se, dict) else {}
    dp = s3_json("data/dark-pool.json", {}) or {}
    F["dark"] = dp.get("xray_map") if isinstance(dp.get("xray_map"), dict) else {}
    F["dark_asof"] = dp.get("generated_at") or dp.get("as_of")
    F["dark_week"] = dp.get("latest_week") or dp.get("week")
    pt = s3_json("data/political-trades.json", {}) or {}
    cong = {}
    cut60 = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%d")
    for t_ in pt.get("trades_recent_50") or []:
        if not isinstance(t_, dict) or not t_.get("ticker"):
            continue
        if str(t_.get("transaction_date") or "")[:10] < cut60:
            continue
        e = cong.setdefault(str(t_["ticker"]).upper(), {"buys": 0, "sells": 0, "buy_usd_max": 0.0, "last": None, "cluster": None})
        if str(t_.get("transaction_type") or "").lower() in ("purchase", "buy"):
            e["buys"] += 1
            e["buy_usd_max"] += fnum(t_.get("amount_max_usd")) or 0.0
        else:
            e["sells"] += 1
        dt_ = str(t_.get("transaction_date") or "")[:10]
        if not e["last"] or dt_ > e["last"]:
            e["last"] = dt_
    for c_ in pt.get("clusters_top_10") or []:
        if isinstance(c_, dict) and c_.get("ticker"):
            e = cong.setdefault(str(c_["ticker"]).upper(), {"buys": 0, "sells": 0, "buy_usd_max": 0.0, "last": None, "cluster": None})
            e["cluster"] = {"direction": c_.get("direction"), "n_members": c_.get("n_members")}
    F["congress"] = cong
    F["congress_asof"] = pt.get("generated_at")
    oa = s3_json("data/options-analytics.json", {}) or {}
    F["options"] = {r.get("ticker"): r for r in (oa.get("board") or []) if isinstance(r, dict) and r.get("ticker")}
    F["options_asof"] = oa.get("generated_at")
    rc = s3_json("data/regime-composite.json", {}) or {}
    vr = s3_json("data/vol-regime.json", {}) or {}
    F["regime"] = {"meta_regime": rc.get("meta_regime"), "meta_class": rc.get("meta_class"),
                   "composite_score": rc.get("composite_score"), "as_of": rc.get("as_of") or rc.get("generated_at"),
                   "vol_regime": vr.get("composite_regime"), "vol_score": vr.get("composite_score"),
                   "vol_as_of": vr.get("as_of")}
    yc = s3_json("data/yield-curve.json", {}) or {}
    ten = ((yc.get("nominal_yields") or {}).get("10Y") or {})
    F["y10"] = fnum(ten.get("value")) if isinstance(ten, dict) else None
    F["yc_asof"] = yc.get("as_of") or yc.get("generated_at")
    ic = s3_json("data/insider-clusters.json", {}) or {}
    F["insider_clusters"] = {str(r.get("ticker")).upper(): r for r in (ic.get("clusters") or []) if isinstance(r, dict) and r.get("ticker")}
    F["backtest"] = s3_json(BACKTEST_KEY, None)
    log("v2 feeds: stock_exposure=%d dark=%d congress=%d options=%d insider_clusters=%d y10=%s regime=%s/%s backtest=%s" % (
        len(F["stock_exposure"]), len(F["dark"]), len(cong), len(F["options"]), len(F["insider_clusters"]),
        F["y10"], F["regime"].get("meta_regime"), F["regime"].get("vol_regime"),
        (F["backtest"] or {}).get("as_of") if F["backtest"] else None))
    log("feeds loaded in %.1fs: finviz=%d census=%d boom=%d rotation=%d flows_poly=%d "
        "flows_true=%d backlog=%d mined=%d contracts=%d catalyst=%d floor=%d/%d "
        "resilience=%d f13=%d short=%d insider=%d rev=%d spx=%d" % (
            time.time() - t, len(F["finviz"]), len(census), len(F["boom"]),
            len(F["rotation"]), len(F["flows_poly"]), len(F["flows_true"]),
            len(F["backlog"]), len(F["backlog_mined"]), len(contracts),
            len(F["catalyst"]), len(F["floor_deep"]), len(F["floor_screen"]),
            len(res), len(F["f13"]), len(F["short"]), len(ib_), len(rev), len(spx)))
    return F


def to_poly(t):  # finviz "BRK-B" -> polygon "BRK.B"
    return t.replace("-", ".")


def to_fv(t):
    return t.replace(".", "-")


def is_etf_row(fv):
    if not fv:
        return False
    if fv.get("etf_type") or str(fv.get("asset_type") or "").lower().startswith("etf"):
        return True
    return "exchange traded fund" in str(fv.get("industry") or "").lower()


def parse_earnings_days(s, today):
    """finviz 'Sep 04 AMC' / 'Nov 05' -> days until (>=0), None if unknown."""
    if not s or not isinstance(s, str):
        return None
    m = re.match(r"([A-Z][a-z]{2}) (\d{1,2})", s.strip())
    if not m:
        return None
    try:
        dt = datetime.strptime("%s %s %d" % (m.group(1), m.group(2), today.year), "%b %d %Y").date()
    except ValueError:
        return None
    delta = (dt - today).days
    if delta < -45:
        delta += 365
    return delta


# ── industry ETF flow consensus ──────────────────────────────────────────
def flow_legs(etf, F):
    legs = {}
    m = F["flows_poly"].get(etf)
    if m:
        legs["z90"] = fnum(m.get("flow_zscore_90d"))
        legs["pct_aum_21d"] = fnum(m.get("pct_aum_21d"))
        legs["pct_aum_5d"] = fnum(m.get("pct_aum_5d"))
        legs["flow_21d_usd"] = fnum(m.get("flow_21d_usd"))
        legs["quadrant"] = m.get("quadrant")
        legs["persistence_days"] = m.get("persistence_days")
        legs["signal"] = m.get("signal_label")
    t = F["flows_true"].get(etf)
    if t:
        nf20 = fnum(t.get("net_flow_20d_usd"))
        aum = fnum(t.get("aum_est_b"))
        legs["true_flow_20d_usd"] = nf20
        if nf20 is not None and aum:
            legs["true_pct_aum_20d"] = nf20 / (aum * 1e9) * 100
    fv = F["finviz"].get(to_fv(etf))
    if fv:
        legs["fv_flows_1m_pct"] = fnum(fv.get("flows_1m_pct"))
        legs["fv_flows_1m_usd"] = fnum(fv.get("flows_1m"))
        legs["fv_flows_3m_usd"] = fnum(fv.get("flows_3m"))
        legs["fv_aum_usd"] = fnum(fv.get("aum"))
        f1 = legs["fv_flows_1m_usd"]
        a = legs["fv_aum_usd"]
        if legs.get("fv_flows_1m_pct") is None and f1 is not None and a:
            legs["fv_flows_1m_pct"] = f1 / a * 100
    parts = []
    if legs.get("z90") is not None:
        parts.append(clamp(50 + 25 * legs["z90"]))
    if legs.get("pct_aum_21d") is not None:
        parts.append(clamp(50 + 12.5 * legs["pct_aum_21d"]))
    if legs.get("true_pct_aum_20d") is not None:
        parts.append(clamp(50 + 12.5 * legs["true_pct_aum_20d"]))
    if legs.get("fv_flows_1m_pct") is not None:
        parts.append(clamp(50 + 12.5 * legs["fv_flows_1m_pct"]))
    legs["score"] = mean(parts)
    legs["n_sources"] = len(parts)
    pos = [x for x in (legs.get("pct_aum_21d"), legs.get("true_pct_aum_20d"),
                       legs.get("fv_flows_1m_pct"), legs.get("z90")) if x is not None]
    legs["inflow"] = (legs["score"] is not None and legs["score"] >= P["inflow_score_min"]
                      and any(x > 0 for x in pos))
    legs["major"] = bool((legs.get("z90") or 0) >= 1.0 or (legs.get("pct_aum_21d") or 0) >= 2.0
                         or (legs.get("true_pct_aum_20d") or 0) >= 2.0
                         or (legs.get("fv_flows_1m_pct") or 0) >= 2.0)
    return legs


# ── scoring ──────────────────────────────────────────────────────────────
def growth_score(fv, cs, boom):
    """absolute-anchored growth pillar 0-100 + facts."""
    s_yoy = fnum(fv.get("sales_yoy_ttm"))
    s_qoq = fnum(fv.get("sales_growth_qoq"))
    e_yoy = fnum(fv.get("eps_yoy_ttm"))
    e_qoq = fnum(fv.get("eps_growth_qoq"))
    e_ny = fnum(fv.get("eps_growth_ny"))
    s_3y = fnum(fv.get("sales_g_3y"))
    if s_yoy is None:
        s_yoy = cs.get("revenue_yoy_pct")
    if e_yoy is None:
        e_yoy = cs.get("eps_yoy_pct")
    ind_g = fnum((boom or {}).get("comp", {}).get("rev_mean")) if boom else None
    ind_b = fnum((boom or {}).get("comp", {}).get("rev_breadth")) if boom else None
    parts = []
    if s_yoy is not None:
        parts.append((2.0, lin_map(s_yoy, -20, 0, 40, 100)))
    if s_qoq is not None:
        parts.append((1.0, lin_map(s_qoq, -20, 0, 40, 100)))
    if e_yoy is not None:
        parts.append((1.0, lin_map(e_yoy, -30, 0, 60, 100)))
    if e_qoq is not None:
        parts.append((0.5, lin_map(e_qoq, -30, 0, 60, 100)))
    if e_ny is not None:
        parts.append((1.0, lin_map(e_ny, -20, 0, 40, 100)))
    if s_3y is not None:
        parts.append((0.5, lin_map(s_3y, -10, 0, 30, 100)))
    if ind_g is not None:
        parts.append((1.5, lin_map(ind_g, -10, 0, 30, 100)))
    if ind_b is not None:
        parts.append((0.5, lin_map(ind_b, 20, 0, 90, 100)))
    e_nq = fnum(fv.get("eps_next_q"))
    if e_nq is not None:
        parts.append((0.5, lin_map(e_nq, -30, 0, 60, 100)))
    e_sur = fnum(fv.get("eps_surprise"))
    if e_sur is not None:
        parts.append((0.5, lin_map(e_sur, -20, 10, 20, 100)))
    r_sur = fnum(fv.get("rev_surprise"))
    if r_sur is not None:
        parts.append((0.5, lin_map(r_sur, -10, 10, 10, 100)))
    s_5y = fnum(fv.get("sales_growth_5y")) or fnum(fv.get("sales_g_5y"))
    if s_5y is not None:
        parts.append((0.5, lin_map(s_5y, -5, 0, 25, 100)))
    sc = (sum(w * v for w, v in parts) / sum(w for w, _ in parts)) if parts else None
    stock_growth = max([x for x in (s_yoy, s_qoq, e_yoy) if x is not None], default=None)
    return {
        "growth_score": sc, "sales_yoy_pct": s_yoy, "sales_qoq_pct": s_qoq,
        "eps_yoy_pct": e_yoy, "eps_qoq_pct": e_qoq, "eps_next_y_pct": e_ny,
        "sales_3y_pct": s_3y, "industry_rev_growth_pct": ind_g,
        "industry_rev_breadth_pct": ind_b,
        "stock_growing": (stock_growth is not None and stock_growth >= 5.0),
        "industry_growing": (ind_g is not None and ind_g >= 5.0),
        "major_growth": ((s_yoy is not None and s_yoy >= P["growth_major_pct"])
                         or (ind_g is not None and ind_g >= P["growth_major_pct"])),
    }


def backlog_block(sym, F, mcap, rev_ttm):
    bk = F["backlog"].get(sym) or {}
    bm = F["backlog_mined"].get(sym) or {}
    ct = F["contracts"].get(sym)
    cat = F["catalyst"].get(sym) or {}
    fd = F["floor_deep"].get(sym) or {}
    fs = F["floor_screen"].get(sym) or {}
    rpo = fnum(bk.get("rpo"))
    mined = fnum(bm.get("backlog_usd")) if bm.get("status") == "MINED" else None
    backlog = rpo if rpo else mined
    src = "xbrl_rpo" if rpo else ("10q_text" if mined else None)
    out = {"backlog_usd": backlog, "backlog_source": src,
           "backlog_asof": bk.get("rpo_asof") if rpo else bm.get("asof"),
           "rpo_qoq_pct": fnum(bk.get("rpo_qoq")), "rpo_yoy_pct": fnum(bk.get("rpo_yoy")),
           "backlog_qoq_pct": fnum(bm.get("backlog_qoq_pct")),
           "demand_accelerating": bk.get("demand_accelerating"),
           "ev_to_rpo": fnum(bk.get("ev_to_rpo"))}
    out["backlog_to_mcap"] = (backlog / mcap) if (backlog and mcap) else None
    out["backlog_to_revenue"] = (backlog / rev_ttm) if (backlog and rev_ttm) else None
    cc = fnum(fd.get("committed_rev_coverage"))
    if cc is None:
        cc = fnum(fs.get("committed_coverage"))
    out["committed_coverage"] = cc
    if ct:
        out["contracts_90d_n"] = ct["n"]
        out["contracts_90d_usd"] = ct["usd"]
        out["contracts_90d_vs_mcap_pct"] = (ct["usd"] / mcap * 100) if mcap else None
        out["contracts_last"] = ct["last"]
        out["contract_types"] = ct["types"]
    else:
        out["contracts_90d_n"] = 0
        out["contracts_90d_usd"] = None
        out["contracts_90d_vs_mcap_pct"] = None
    cls = [c.get("class") for c in (cat.get("catalysts") or []) if isinstance(c, dict)]
    out["catalyst_classes"] = cls[:8]
    out["catalyst_score"] = fnum(cat.get("score"))
    out["has_contract_catalyst"] = any(c in ("MAJOR_CONTRACT", "MAJOR_CUSTOMER", "MAJOR_BACKLOG", "BACKLOG_ACCEL")
                                      for c in cls)
    parts = []
    b2m = out["backlog_to_mcap"]
    if b2m is not None:
        parts.append((2.0, lin_map(b2m, 0.0, 20, 1.0, 100)))
    g = out["rpo_yoy_pct"] if out["rpo_yoy_pct"] is not None else out["backlog_qoq_pct"]
    if g is not None:
        parts.append((1.0, lin_map(g, -20, 10, 40, 100)))
    if out["demand_accelerating"] is not None:
        parts.append((0.5, 85.0 if out["demand_accelerating"] else 35.0))
    vm = out["contracts_90d_vs_mcap_pct"]
    if vm is not None:
        parts.append((1.5, lin_map(vm, 0.0, 30, 15.0, 100)))
    if out["has_contract_catalyst"]:
        parts.append((0.5, 80.0))
    out["backlog_contracts_score"] = (sum(w * v for w, v in parts) / sum(w for w, _ in parts)) if parts else None
    out["major_backlog"] = bool((b2m is not None and b2m >= 0.5) or
                                (out["backlog_to_revenue"] is not None and out["backlog_to_revenue"] >= 1.0 and (g or 0) > 0))
    out["meaningful_contracts"] = bool(vm is not None and vm >= 3.0)
    return out


def safety_block(sym, fv, cs, ps, F, mcap, today):
    fd = F["floor_deep"].get(sym) or {}
    fs = F["floor_screen"].get(sym) or {}
    cov = fnum(fd.get("coverage"))
    if cov is None:
        cov = fnum(fs.get("approx_coverage"))
    altman = cs.get("altman_z")
    pio = cs.get("piotroski_f")
    ben = cs.get("beneish_m")
    icov = cs.get("interest_coverage_ttm")
    nd_e = cs.get("netdebt_to_ebitda_ttm")
    debt_eq = fnum(fv.get("debt_eq"))
    cur = fnum(fv.get("current_ratio"))
    pfcf = fnum(fv.get("p_fcf"))
    dil = cs.get("share_count_yoy_pct")
    if dil is None:
        dil = fnum(fd.get("dilution_yoy"))
        dil = dil * 100 if dil is not None else None
    bb_y = cs.get("net_buyback_yield_pct")
    sf = fnum(fv.get("short_float_pct"))
    ed = cs.get("earnings_in_days")
    if ed is None:
        ed = parse_earnings_days(fv.get("earnings_date"), today)
    beta = ps.get("beta_1y")
    mdd = ps.get("max_dd_1y_pct")
    adv = ps.get("adv_usd_20d")
    parts = []
    if cov is not None:
        parts.append((2.0, lin_map(cov, 0.0, 30, 1.0, 100)))
    if altman is not None:
        parts.append((1.5, lin_map(altman, 1.0, 10, 4.0, 100)))
    if pio is not None:
        parts.append((1.0, lin_map(pio, 2, 10, 8, 100)))
    if ben is not None:
        parts.append((0.5, 85.0 if ben < -1.78 else 30.0))
    if icov is not None:
        parts.append((1.0, lin_map(icov, 0.0, 10, 8.0, 100)))
    elif nd_e is not None:
        parts.append((1.0, lin_map(nd_e, 5.0, 10, 0.0, 100)))
    elif debt_eq is not None:
        parts.append((1.0, lin_map(debt_eq, 2.0, 10, 0.0, 100)))
    if cur is not None:
        parts.append((0.5, lin_map(cur, 0.8, 20, 2.5, 100)))
    if pfcf is not None:
        parts.append((0.5, 80.0 if pfcf > 0 else 25.0))
    if dil is not None:
        parts.append((1.5, lin_map(dil, 15.0, 0, -2.0, 100)))
    if beta is not None:
        parts.append((1.0, lin_map(beta, 1.8, 20, 0.5, 100)))
    if mdd is not None:
        parts.append((1.0, lin_map(mdd, -70.0, 10, -15.0, 100)))
    if adv is not None:
        parts.append((0.5, lin_map(math.log10(max(adv, 1.0)), 6.0, 20, 8.0, 100)))
    if sf is not None:
        parts.append((0.5, 90.0 if sf < 10 else (60.0 if sf < 20 else 25.0)))
    cvar = ps.get("cvar5_pct")
    if cvar is not None:
        parts.append((1.0, lin_map(cvar, -8.0, 0, -2.0, 100)))
    gaps = ps.get("gaps_5pct_1y")
    if gaps is not None:
        parts.append((1.0, lin_map(gaps, 12, 0, 0, 100)))
    ulc = ps.get("ulcer_index")
    if ulc is not None:
        parts.append((0.5, lin_map(ulc, 30.0, 0, 5.0, 100)))
    ami = ps.get("amihud_illiq")
    if ami is not None:
        parts.append((0.5, lin_map(math.log10(max(ami, 1e-6)), 1.0, 0, -2.0, 100)))
    sc = (sum(w * v for w, v in parts) / sum(w for w, _ in parts)) if parts else None
    risks = []
    if gaps is not None and gaps >= 6:
        risks.append("%d overnight gaps of 5%%+ in the year (max %.0f%%) -- event-driven tape" % (
            gaps, ps.get("max_gap_pct_1y") or 0))
    if cvar is not None and cvar <= -6:
        risks.append("CVaR(5%%) %.1f%%: the worst twentieth of days average that loss" % cvar)
    if ed is not None and 0 <= ed <= 7:
        risks.append("earnings in %d day(s) -- binary event inside the coil" % ed)
        sc = (sc - 12) if sc is not None else None
    if dil is not None and dil > 10:
        risks.append("share count +%.0f%% y/y -- dilution" % dil)
    if altman is not None and altman < 1.8:
        risks.append("Altman Z %.2f -- distress zone" % altman)
    if ben is not None and ben > -1.78:
        risks.append("Beneish M %.2f -- earnings-manipulation flag" % ben)
    if sf is not None and sf >= 25:
        risks.append("short float %.0f%% -- crowded short, violent both ways" % sf)
    if mdd is not None and mdd <= -60:
        risks.append("max drawdown %.0f%% over the year" % mdd)
    if cov is not None and cov < 0:
        risks.append("net liquid assets negative (debt exceeds liquid assets)")
    if adv is not None and adv < 5e6:
        risks.append("thin liquidity: $%.1fM average daily dollar volume" % (adv / 1e6))
    return {"safety_score": (clamp(sc) if sc is not None else None), "nlav_coverage": cov,
            "runway_months": fnum(fd.get("runway_months")),
            "altman_z": altman, "piotroski_f": pio, "beneish_m": ben,
            "interest_coverage": icov, "netdebt_to_ebitda": nd_e, "debt_to_equity": debt_eq,
            "current_ratio": cur, "p_fcf": pfcf, "dilution_yoy_pct": dil,
            "net_buyback_yield_pct": bb_y, "short_float_pct": sf, "earnings_in_days": ed,
            "risks": risks}


def build_stock_rows(bars, dates, mkt, F, etf_bars):
    today = datetime.now(timezone.utc).date()
    AUX = {}
    for key, tk in (("rate_beta", "TLT"), ("dollar_beta", "UUP")):
        ab = bars.get(tk)
        AUX[key] = (ab, {ab.d[i]: i for i in range(len(ab.d))}) if ab else None
    rows = []
    n_fv = 0
    flow_cache = {}
    for sym, b in bars.items():
        fv = F["finviz"].get(to_fv(sym))
        if not fv or is_etf_row(fv):
            continue
        if NON_OPERATING_RX.match(str(fv.get("industry") or "")):
            continue  # closed-end funds / shells are not operating companies
        n_fv += 1
        n = len(b.c)
        if n < 60:
            continue
        ps = price_signals(b, mkt, etf_bars, dates, aux=AUX)
        cs = F["census"].get(sym) or {}
        mcap = fnum(fv.get("market_cap"))
        if mcap is not None and 0 < mcap < 1e8:
            mcap *= 1e6  # finviz export denominates in $MM
        if mcap is None:
            mcap = cs.get("mcap")
        sector = fv.get("sector") or ""
        industry = fv.get("industry") or ""
        ind_etf = IND_ETF.get(industry)
        sec_etf = SECTOR_ETF.get(sector)
        etf = ind_etf or sec_etf
        if etf not in flow_cache:
            flow_cache[etf] = flow_legs(etf, F) if etf else {}
        if sec_etf and sec_etf not in flow_cache:
            flow_cache[sec_etf] = flow_legs(sec_etf, F)
        fl = flow_cache.get(etf) or {}
        fls = flow_cache.get(sec_etf) or {}
        boom = F["boom"].get(industry)
        if boom is None and industry:
            il = industry.lower()
            for k, v in F["boom"].items():
                kl = k.lower()
                if kl and (kl in il or il in kl):
                    boom = v
                    break
        rot = F["rotation"].get(etf) or {}
        rrg = (F["rrg"].get(etf) or {}) if isinstance(F["rrg"], dict) else {}
        r = {"ticker": sym, "name": fv.get("company"), "sector": sector, "industry": industry,
             "country": fv.get("country"), "market_cap": mcap,
             "cap_bucket": ("mega" if (mcap or 0) >= 2e11 else "large" if (mcap or 0) >= 1e10
                            else "mid" if (mcap or 0) >= 2e9 else "small" if (mcap or 0) >= 3e8
                            else "micro" if (mcap or 0) >= 5e7 else "nano" if mcap else None),
             "sp500": sym in F["sp500"], "industry_etf": etf,
             "industry_etf_kind": ("industry" if ind_etf else "sector"), "sector_etf": sec_etf,
             "industry_etf_name": IND_ETF_NAME.get(etf)}
        r.update({k: ps.get(k) for k in ps if k != "episodes"})
        r["episodes"] = ps["episodes"]
        r["rs_vs_industry_3m_pp"] = rs_vs(b, etf_bars.get(etf), 63)
        r["rs_vs_spy_3m_pp"] = rs_vs(b, bars.get("SPY"), 63)
        r["rs_vs_spy_6m_pp"] = rs_vs(b, bars.get("SPY"), 126)
        # valuation raw
        r["pe"] = fnum(fv.get("pe"))
        r["fwd_pe"] = fnum(fv.get("fwd_pe"))
        r["peg"] = fnum(fv.get("peg"))
        if r["peg"] is None:
            r["peg"] = cs.get("peg_ttm")
        r["ps"] = fnum(fv.get("ps"))
        r["pb"] = fnum(fv.get("pb"))
        r["ev_ebitda"] = fnum(fv.get("ev_ebitda"))
        if r["ev_ebitda"] is None:
            r["ev_ebitda"] = cs.get("ev_ebitda_ttm")
        r["ev_sales"] = fnum(fv.get("ev_sales"))
        r["p_fcf"] = fnum(fv.get("p_fcf"))
        r["fcf_yield_pct"] = cs.get("fcf_yield_pct")
        if r["fcf_yield_pct"] is None and r["p_fcf"]:
            r["fcf_yield_pct"] = 100.0 / r["p_fcf"]
        r["roic_pct"] = fnum(fv.get("roic"))
        if r["roic_pct"] is None:
            r["roic_pct"] = cs.get("roic_pct")
        r["gross_margin_pct"] = fnum(fv.get("gross_margin"))
        r["oper_margin_pct"] = fnum(fv.get("oper_margin"))
        r["target_price"] = fnum(fv.get("target_price"))
        r["pt_upside_pct"] = ((r["target_price"] / ps["close"] - 1) * 100
                              if (r["target_price"] and ps["close"]) else None)
        r["analyst_recom"] = fnum(fv.get("analyst_recom"))
        rev_ttm = fnum(fv.get("sales"))
        if rev_ttm is not None and 0 < rev_ttm < 1e8:
            rev_ttm *= 1e6  # finviz $MM
        r["revenue_ttm"] = rev_ttm
        r.update(growth_score(fv, cs, boom))
        r["industry_boom_score"] = fnum((boom or {}).get("boom_score"))
        r["industry_boom_delta_20d"] = fnum((boom or {}).get("score_delta_20d"))
        r["industry_deal_wins_30d"] = (boom or {}).get("comp", {}).get("deal_wins_30d") if boom else None
        r["industry_backlog_accel_share"] = (boom or {}).get("comp", {}).get("backlog_accel_share") if boom else None
        r["industry_leadership"] = fnum(rot.get("leadership_score"))
        r["industry_tag"] = rot.get("tag")
        r["industry_rrg"] = rrg.get("quadrant") if isinstance(rrg, dict) else None
        r["industry_dump_excess_bps"] = fnum(rot.get("dump_day_excess_bps"))
        r["industry_rel_mom_3m_pp"] = fnum(rot.get("rel_mom_3m_pp"))
        r["flows"] = {k: (rnd(v, 3) if isinstance(v, float) else v) for k, v in fl.items()}
        r["sector_flows"] = {k: (rnd(v, 3) if isinstance(v, float) else v) for k, v in fls.items()}
        r["flow_score"] = fl.get("score")
        r["industry_inflow"] = bool(fl.get("inflow"))
        r["industry_inflow_major"] = bool(fl.get("major"))
        r.update(backlog_block(sym, F, mcap, rev_ttm))
        r.update(safety_block(sym, fv, cs, ps, F, mcap, today))
        res = F["resilience"].get(sym) or {}
        r["resilience_engine"] = ({"score": res.get("resilience"), "stage": res.get("stage"),
                                   "flow_confirmed": res.get("flow_confirmed"),
                                   "adverse_hit_rate_pct": res.get("adverse_hit_rate_pct")}
                                  if res else None)
        f13 = F["f13"].get(sym) or {}
        r["inst_net_usd"] = fnum(f13.get("n"))
        r["whale_net_usd"] = fnum(f13.get("wn"))
        r["inst_funds_holding"] = f13.get("nf")
        sh = F["short"].get(sym) or {}
        r["days_to_cover"] = fnum(sh.get("days_to_cover"))
        r["si_change_pct"] = fnum(sh.get("si_change_pct"))
        r["short_signal"] = sh.get("signal")
        ins = F["insider"].get(sym)
        r["insider_buys_30d"] = ins["n_buys"] if ins else 0
        r["insider_buy_usd_30d"] = ins["usd"] if ins else None
        r["insider_cluster"] = bool(ins and ins.get("cluster"))
        rv = F["revisions"].get(sym) or {}
        r["eps_rev_pct"] = rv.get("eps_rev_pct")
        r["insider_trans_pct"] = fnum(fv.get("insider_trans_pct"))
        r["inst_trans_pct"] = fnum(fv.get("inst_trans_pct"))
        r["inst_own_pct"] = fnum(fv.get("inst_own_pct"))
        # ---- v2: growth extras, rule of 40, ERP
        r["eps_next_q_pct"] = fnum(fv.get("eps_next_q"))
        r["eps_surprise_pct"] = fnum(fv.get("eps_surprise"))
        r["rev_surprise_pct"] = fnum(fv.get("rev_surprise"))
        r["sales_5y_pct"] = fnum(fv.get("sales_growth_5y")) or fnum(fv.get("sales_g_5y"))
        fcfm = None
        if r.get("fcf_yield_pct") is not None and r.get("ps"):
            fcfm = r["fcf_yield_pct"] * r["ps"]          # FCF/mcap x mcap/sales = FCF margin (%)
        r["fcf_margin_pct"] = fcfm
        r["rule_of_40"] = (r["sales_yoy_pct"] + fcfm) if (fcfm is not None and r.get("sales_yoy_pct") is not None) else None
        ey = (100.0 / r["fwd_pe"]) if (r.get("fwd_pe") and r["fwd_pe"] > 0) else ((100.0 / r["pe"]) if (r.get("pe") and r["pe"] > 0) else None)
        r["earnings_yield_pct"] = ey
        r["erp_vs_10y_pct"] = (ey - F["y10"]) if (ey is not None and F.get("y10") is not None) else None
        # ---- v2: ETF constituent pressure (mechanical demand from ETF creations)
        se = F["stock_exposure"].get(sym) or F["stock_exposure"].get(to_fv(sym)) or {}
        p21 = fnum(se.get("total_aggregate_flow_21d_usd"))
        r["etf_pressure_21d_usd"] = p21
        r["etf_pressure_5d_usd"] = fnum(se.get("total_aggregate_flow_5d_usd"))
        r["etf_pressure_pct_mcap"] = (p21 / mcap * 100) if (p21 is not None and mcap) else None
        r["etf_pressure_days_adv"] = (p21 / ps["adv_usd_20d"]) if (p21 is not None and ps.get("adv_usd_20d")) else None
        r["n_etfs_holding"] = se.get("n_etfs_holding")
        r["etf_quadrant"] = se.get("quadrant")
        # ---- v2: dark pool (FINRA ATS share and acceleration)
        dk = F["dark"].get(sym) or {}
        r["dark_pool_pct"] = fnum(dk.get("dp"))
        r["dark_pool_accel"] = fnum(dk.get("acc"))
        r["dark_pool_state"] = dk.get("st")
        r["dark_short_z"] = fnum(dk.get("sz"))
        r["dark_conviction"] = dk.get("cv")
        # ---- v2: Congress, insider clusters, options
        cg = F["congress"].get(sym)
        r["congress_buys_60d"] = cg["buys"] if cg else 0
        r["congress_sells_60d"] = cg["sells"] if cg else 0
        r["congress_buy_usd_max"] = cg["buy_usd_max"] if cg else None
        r["congress_cluster"] = (cg or {}).get("cluster")
        icl = F["insider_clusters"].get(sym)
        r["insider_cluster_n"] = icl.get("n_insiders") if icl else None
        r["insider_cluster_usd"] = fnum(icl.get("total_value")) if icl else None
        r["insider_cluster_role"] = icl.get("highest_role") if icl else None
        if icl and not r.get("insider_cluster"):
            r["insider_cluster"] = True
        op = F["options"].get(sym)
        if op:
            r["options"] = {"iv_rank": fnum(op.get("iv_rank")), "vrp": fnum(op.get("vrp")),
                            "skew_25d": fnum(op.get("skew_25d")), "pcr_vol": fnum(op.get("pcr_vol")),
                            "net_premium_usd": fnum(op.get("net_premium_usd")),
                            "gamma_regime": op.get("gamma_regime"), "signal": op.get("signal"),
                            "n_unusual": op.get("n_unusual")}
        else:
            r["options"] = None
        rows.append(r)
    log("stock rows built: %d (finviz-matched %d)" % (len(rows), n_fv))
    return rows


def valuation_percentiles(rows):
    """industry-neutral cheapness: percentile of PEG / EV-EBITDA / P-S /
    P-FCF / forward P-E within the name's industry (>= 8 names with data,
    else its sector); score = 100 - mean percentile (lower multiple =
    lower percentile = cheaper)."""
    metrics = ["peg", "ev_ebitda", "ps", "p_fcf", "fwd_pe"]
    ind_n = {}
    for r in rows:
        if any(r.get(m) is not None and r[m] > 0 for m in metrics):
            ind_n[r["industry"] or "?"] = ind_n.get(r["industry"] or "?", 0) + 1
    groups = {}
    for r in rows:
        ind = r["industry"] or "?"
        if ind_n.get(ind, 0) >= 8:
            key = ("industry", ind)
        else:
            key = ("sector", r["sector"] or "?")
        r["_vgrp"] = key
        groups.setdefault(key, []).append(r)
    vp = {}
    for key, members in groups.items():
        for m in metrics:
            vals = {r["ticker"]: r[m] for r in members if r.get(m) is not None and r[m] > 0}
            if len(vals) < 5:
                continue
            for tk, p in pct_rank(vals).items():
                vp.setdefault(tk, {})[m] = p
    for r in rows:
        key = r.pop("_vgrp")
        pcts = vp.get(r["ticker"], {})
        sc = (100.0 - mean(list(pcts.values()))) if pcts else None
        if sc is not None and r.get("peg") is not None and 0 < r["peg"] <= 1.0:
            sc = clamp(sc + 10)
        if sc is not None and r.get("fcf_yield_pct") is not None and r["fcf_yield_pct"] >= 6:
            sc = clamp(sc + 5)
        if sc is not None and r.get("erp_vs_10y_pct") is not None and r["erp_vs_10y_pct"] >= 2.0:
            sc = clamp(sc + 5)   # earnings yield beats the 10-year by 200 bps+
        if sc is not None and r.get("rule_of_40") is not None and r["rule_of_40"] >= 40 \
                and str(r.get("industry") or "").startswith("Software"):
            sc = clamp(sc + 5)
        r["valuation_score"] = sc
        r["valuation_group"] = key[0]
        r["valuation_group_name"] = key[1]
        r["valuation_pctiles"] = {k: round(v, 1) for k, v in pcts.items()}
        r["low_valuation"] = bool(sc is not None and sc >= P["valuation_score_min"])
        r["cheap_absolute"] = bool((r.get("peg") is not None and 0 < r["peg"] <= 1.2)
                                  or (r.get("ev_ebitda") is not None and 0 < r["ev_ebitda"] <= 10)
                                  or (r.get("fcf_yield_pct") is not None and r["fcf_yield_pct"] >= 6))


def resilience_score(r):
    parts = []
    cap = r.get("dump_capture")
    if cap is not None:
        parts.append((3.0, lin_map(cap, 1.0, 0, -0.25, 100)))
    worst = r.get("capture_worst")
    if worst is not None:
        parts.append((1.5, lin_map(worst, 2.0, 0, 0.0, 100)))
    ex = r.get("worst_days_excess_bps")
    if ex is not None:
        parts.append((2.0, lin_map(ex, -150, 0, 150, 100)))
    gr = r.get("worst_days_green_rate")
    if gr is not None:
        parts.append((1.0, lin_map(gr, 0.1, 0, 0.7, 100)))
    dc = r.get("down_capture_pct")
    if dc is not None:
        parts.append((1.5, lin_map(dc, 130, 0, 0, 100)))
    ba = r.get("beta_asymmetry")
    if ba is not None:
        parts.append((1.0, lin_map(ba, -0.5, 20, 1.0, 100)))
    fu = r.get("flat_or_up_share")
    if fu is not None:
        parts.append((1.0, lin_map(fu, 0.0, 20, 1.0, 100)))
    re_ = r.get("resilience_engine")
    if re_ and re_.get("score") is not None:
        parts.append((0.5, clamp(re_["score"])))
    dcap = r.get("capture_days_weighted")
    if dcap is not None:
        parts.append((1.5, lin_map(dcap, 1.0, 0, -0.25, 100)))
    ts = r.get("worst_days_tstat")
    if ts is not None:
        parts.append((1.0, lin_map(ts, -2.0, 0, 3.0, 100)))
    lx = r.get("worst_days_long_excess_bps")
    if lx is not None:
        parts.append((1.0, lin_map(lx, -150, 0, 150, 100)))
    if not parts:
        return None
    sc = sum(w * v for w, v in parts) / sum(w for w, _ in parts)
    conf = r.get("resilience_confidence")
    if conf is not None:
        sc = 50.0 + (sc - 50.0) * (0.4 + 0.6 * conf)   # thin evidence shrinks toward neutral
    return sc


def accumulation_score(r):
    """Volume-structure and smart-money evidence that the dips are being bought."""
    parts = []
    ob = r.get("obv_slope_63")
    if ob is not None:
        parts.append((2.0, lin_map(ob, -0.3, 0, 0.3, 100)))
    ad = r.get("ad_slope_63")
    if ad is not None:
        parts.append((1.5, lin_map(ad, -0.2, 0, 0.2, 100)))
    ud = r.get("updown_volume_ratio_50")
    if ud is not None:
        parts.append((1.5, lin_map(ud, 0.6, 0, 1.6, 100)))
    clv = r.get("absorption_clv")
    if clv is not None:
        parts.append((2.0, lin_map(clv, 0.2, 0, 0.8, 100)))
    if r.get("obv_divergence") or r.get("ad_divergence"):
        parts.append((1.0, 90.0))
    st = r.get("dark_pool_state")
    if st:
        parts.append((1.5, {"ACCUMULATION": 90.0, "DISTRIBUTION": 15.0}.get(str(st).upper(), 50.0)))
    da = r.get("dark_pool_accel")
    if da is not None:
        parts.append((0.5, lin_map(da, -0.5, 20, 0.6, 100)))
    ins = r.get("inst_net_usd")
    mc = r.get("market_cap")
    if ins is not None and mc:
        parts.append((1.0, lin_map(ins / mc * 100, -2.0, 0, 2.0, 100)))
    if r.get("insider_cluster"):
        parts.append((1.0, 90.0))
    elif r.get("insider_buys_30d"):
        parts.append((1.0, 70.0))
    if r.get("congress_buys_60d"):
        parts.append((0.5, 85.0 if r["congress_buys_60d"] >= 2 else 70.0))
    it = r.get("inst_trans_pct")
    if it is not None:
        parts.append((0.5, lin_map(it, -5.0, 0, 5.0, 100)))
    bb_ = r.get("net_buyback_yield_pct")
    if bb_ is not None:
        parts.append((1.0, lin_map(bb_, -3.0, 0, 5.0, 100)))
    ep = r.get("etf_pressure_pct_mcap")
    if ep is not None:
        parts.append((1.0, lin_map(ep, -0.5, 0, 0.5, 100)))
    return (sum(w * v for w, v in parts) / sum(w for w, _ in parts)) if parts else None


def structure_score(r):
    parts = []
    hl = r.get("higher_lows")
    if hl is not None:
        parts.append((1.5, lin_map(hl, 0, 25, 3, 100)))
    if r.get("vcp_ok") is not None:
        parts.append((1.5, 95.0 if r.get("vcp_strict") else (80.0 if r["vcp_ok"] else 30.0)))
    if r.get("rs_leading") is not None:
        parts.append((1.5, 95.0 if r["rs_leading"] else (70.0 if r.get("rs_line_new_high") else 35.0)))
    w = r.get("weeks_since_52w_high")
    if w is not None:
        parts.append((0.5, 85.0 if 7 <= w <= 40 else (55.0 if w < 7 else 40.0)))
    sm = r.get("squeeze_momentum_atr")
    if sm is not None:
        parts.append((1.0, lin_map(sm, -1.0, 15, 1.0, 100) + (5.0 if r.get("squeeze_momentum_rising") else 0.0)))
    return (clamp(sum(w * v for w, v in parts) / sum(w for w, _ in parts)) if parts else None)


def flows_pillar(r):
    parts = []
    fs = r.get("flow_score")
    if fs is not None:
        parts.append((2.5, fs))
    ss = (r.get("sector_flows") or {}).get("score")
    if ss is not None:
        parts.append((0.5, ss))
    ep = r.get("etf_pressure_pct_mcap")
    if ep is not None:
        parts.append((1.5, lin_map(ep, -1.0, 0, 1.0, 100)))
    pers = (r.get("flows") or {}).get("persistence_days")
    if pers is not None:
        parts.append((0.5, lin_map(float(pers), 0, 40, 10, 100)))
    return (sum(w * v for w, v in parts) / sum(w for w, _ in parts)) if parts else None


def coil_score(r):
    parts = []
    bp = r.get("bb_width_pctile")
    if bp is not None:
        parts.append((3.0, 100 - bp))
    if r.get("ttm_squeeze_on") is not None:
        parts.append((1.5, 85.0 if r["ttm_squeeze_on"] else 30.0))
    sd = r.get("squeeze_days")
    if sd is not None:
        parts.append((0.5, lin_map(sd, 0, 20, 30, 100)))
    ac = r.get("atr_contraction")
    if ac is not None:
        parts.append((1.0, lin_map(ac, 1.3, 10, 0.5, 100)))
    vd = r.get("volume_dryup")
    if vd is not None:
        parts.append((0.5, lin_map(vd, 1.5, 20, 0.5, 100)))
    pb = r.get("bb_pct_b")
    if pb is not None:
        parts.append((0.5, 100 - abs(pb - 0.6) * 150))
    st = r.get("coil_state")
    if st == "IGNITED":
        parts.append((2.0, 20.0))  # already released -- not a pre-breakout coil
    op = r.get("options") or {}
    if op.get("iv_rank") is not None:
        parts.append((1.0, lin_map(op["iv_rank"], 80, 10, 10, 100)))   # cheap options = coil confirmed by the vol market
    sm = r.get("squeeze_momentum_atr")
    if sm is not None:
        parts.append((0.5, lin_map(sm, -1.0, 25, 1.0, 100)))
    return (clamp(sum(w * v for w, v in parts) / sum(w for w, _ in parts)) if parts else None)


def location_score(r):
    v = r.get("vs_ema250_pct")
    if v is None:
        return None
    if v > 3:
        return 0.0
    if v > 0:
        return 20.0
    if v >= -10:
        return lin_map(v, 0, 70, -10, 100)
    if v >= -25:
        return lin_map(v, -10, 100, -25, 60)
    return lin_map(v, -25, 60, -50, 15)


def momentum_score(r):
    parts = []
    rs3 = r.get("rs_vs_spy_3m_pp")
    if rs3 is not None:
        parts.append((1.5, lin_map(rs3, -20, 0, 20, 100)))
    ri = r.get("rs_vs_industry_3m_pp")
    if ri is not None:
        parts.append((1.0, lin_map(ri, -20, 0, 20, 100)))
    r1 = r.get("ret_1m_pct")
    if r1 is not None:
        parts.append((0.5, lin_map(r1, -15, 0, 15, 100)))
    ld = r.get("industry_leadership")
    if ld is not None:
        parts.append((1.5, clamp(ld)))
    bd = r.get("industry_boom_delta_20d")
    if bd is not None:
        parts.append((1.0, lin_map(bd, -10, 0, 10, 100)))
    q = r.get("industry_rrg")
    if q:
        parts.append((1.0, {"LEADING": 90, "IMPROVING": 75, "WEAKENING": 40, "LAGGING": 15}.get(str(q).upper(), 50)))
    er = r.get("eps_rev_pct")
    if er is not None:
        parts.append((1.0, lin_map(er, -10, 0, 10, 100)))
    return (sum(w * v for w, v in parts) / sum(w for w, _ in parts)) if parts else None


def dump_gate(r):
    """Episode capture and worst-day behaviour must not contradict each other:
    a name that rose through the drawdowns but bleeds on the single worst
    days is not a fortress."""
    cap = r.get("dump_capture")
    ex = r.get("worst_days_excess_bps")
    gr = r.get("worst_days_green_rate") or 0.0
    worst = r.get("capture_worst")
    if cap is None and ex is None:
        return None
    dcap = r.get("capture_days_weighted")
    ep_ok = (cap is not None and cap <= P["capture_barely_dipped"]
             and (worst is None or worst <= P["capture_worst_max"])
             and (dcap is None or dcap <= 0.6)
             and (ex is None or ex >= -75))
    day_ok = (ex is not None and ex >= 25 and gr >= 0.40 and (cap is None or cap <= 0.7)
              and (worst is None or worst <= 1.5))
    return bool(ep_ok or day_ok)


def coil_gate(r):
    bp = r.get("bb_width_pctile")
    sq = r.get("ttm_squeeze_on")
    if bp is None and sq is None:
        return None
    return bool((bp is not None and bp <= P["bb_squeeze_pctile"])
                or (sq and (r.get("squeeze_days") or 0) >= 3))


def gates_and_tier(r):
    g = {
        "under_ema250": (r["vs_ema250_pct"] < 0) if r.get("vs_ema250_pct") is not None else None,
        "dump_resilient": None,
        "coiled": None,
        "low_valuation": r.get("low_valuation") if r.get("valuation_score") is not None else (
            True if r.get("cheap_absolute") else None),
        "growth": (bool(r.get("stock_growing") or r.get("industry_growing"))
                   if (r.get("growth_score") is not None) else None),
        "industry_inflows": (bool(r.get("industry_inflow")) if r.get("flow_score") is not None else None),
    }
    g["dump_resilient"] = dump_gate(r)
    g["coiled"] = coil_gate(r)
    passed = sum(1 for v in g.values() if v)
    tier = TIER_BY_GATES.get(passed, "SCREENED")
    caps = []
    hyg = (r.get("close") or 0) >= P["min_price"] and (r.get("adv_usd_20d") or 0) >= P["min_adv_usd"] \
        and (r.get("n_sessions") or 0) >= P["min_sessions"]
    if not hyg and tier != "SCREENED":
        caps.append("hygiene: price/liquidity/history below floor")
        tier = "SCREENED"
    knife = ((r.get("ret_3m_pct") is not None and r["ret_3m_pct"] <= P["knife_ret_3m_pct"])
             or (r.get("vs_ema250_pct") is not None and r["vs_ema250_pct"] <= P["knife_below_ema_pct"]))
    if knife and tier in ("FORTRESS_COIL", "COILED", "ACCUMULATING"):
        caps.append("knife guard: -%s%% in 3m / %s%% below EMA250 is a collapse, not accumulation" % (
            rnd(abs(r.get("ret_3m_pct") or 0), 0), rnd(r.get("vs_ema250_pct"), 0)))
        tier = "WATCH"
    if r.get("coil_state") == "IGNITED" and tier in ("FORTRESS_COIL", "COILED"):
        caps.append("already ignited: breakout printed on volume -- chase risk, not a coil")
        tier = "ACCUMULATING"
    r["gates"] = g
    r["gates_passed"] = passed
    r["tier"] = tier
    r["tier_caps"] = caps


def composite_and_asymmetry(r, ranks=None):
    pillars = {
        "resilience": resilience_score(r), "accumulation": accumulation_score(r),
        "coil": coil_score(r), "structure": structure_score(r), "location": location_score(r),
        "valuation": r.get("valuation_score"), "growth": r.get("growth_score"),
        "flows": flows_pillar(r), "momentum": momentum_score(r),
        "backlog_contracts": r.get("backlog_contracts_score"), "safety": r.get("safety_score"),
    }
    r["pillars"] = {k: rnd(v, 1) for k, v in pillars.items()}
    r["_pillars_raw"] = pillars
    num = 0.0
    den = 0.0
    for k, w in WEIGHTS.items():
        v = pillars.get(k)
        if v is not None:
            num += w * clamp(v)
            den += w
    comp_abs = (num / den) if den >= 60 else None
    cov = round(den / sum(WEIGHTS.values()), 2)
    comp = comp_abs
    if ranks is not None and comp_abs is not None:
        rn = 0.0
        rd = 0.0
        pr = {}
        for k, w in WEIGHTS.items():
            pv = (ranks.get(k) or {}).get(r["ticker"])
            if pv is not None:
                rn += w * pv
                rd += w
                pr[k] = round(pv, 1)
        r["pillar_ranks"] = pr
        if rd >= 60:
            comp = 0.5 * comp_abs + 0.5 * (rn / rd)
    r["pillar_coverage"] = cov
    r["composite_abs"] = rnd(comp_abs, 1)
    # evidence confidence: resilience sample + data coverage
    conf = 0.6 * (r.get("resilience_confidence") or 0.0) + 0.4 * cov
    r["confidence"] = round(conf, 2)
    r["composite"] = rnd(50.0 + (comp - 50.0) * (0.5 + 0.5 * conf), 1) if comp is not None else None
    # conviction: geometric mean of the six gate pillars (a fortress is strong everywhere)
    gp = [clamp(pillars[k]) / 100.0 for k in GATE_PILLARS if pillars.get(k) is not None]
    r["conviction"] = rnd(100.0 * math.exp(sum(math.log(max(x, 0.02)) for x in gp) / len(gp)), 1) if len(gp) >= 4 else None
    # asymmetry: empirical upside room / empirical dump downside
    ups = [x for x in (r.get("pt_upside_pct"), -r.get("pct_from_52w_high") if r.get("pct_from_52w_high") is not None else None)
           if x is not None and x > 0]
    upside = median(ups) if ups else None
    cap = r.get("dump_capture")
    vol = r.get("vol_100d_pct") or r.get("vol_20d_pct")
    sig_m = (vol / math.sqrt(12)) if vol else None  # one-month 1-sigma move
    if cap is not None:
        downside = 10.0 * min(2.5, cap)                # empirical: capture x a -10% SPY dump
        downside = max(downside, 0.5 * sig_m if sig_m else 2.0)  # idiosyncratic floor
        cv = r.get("cvar5_pct")
        if cv is not None:
            downside = max(downside, -cv * 1.5)         # tail floor: 1.5x the average bad-day loss
        cov_ = r.get("nlav_coverage")
        if cov_ is not None and cov_ >= 0.5:
            downside *= 0.7
    else:
        downside = None
    r["upside_room_pct"] = rnd(upside, 1)
    r["dump_downside_pct"] = rnd(downside, 1)
    r["asymmetry"] = rnd(min(25.0, upside / downside), 2) if (upside is not None and downside) else None


def pillar_ranks(rows):
    """cross-sectional percentile of every pillar across the scored universe."""
    out = {}
    for k in WEIGHTS:
        vals = {r["ticker"]: r["_pillars_raw"].get(k) for r in rows if r.get("_pillars_raw", {}).get(k) is not None}
        out[k] = pct_rank(vals)
    return out


def reasons_for(r):
    R = []
    cap = r.get("dump_capture")
    if r.get("n_episodes"):
        fu = ("%.0f%%" % (r["flat_or_up_share"] * 100)) if r.get("flat_or_up_share") is not None else "n/a"
        if cap is None:
            R.append("%d SPY drawdown episode(s) overlapped; capture n/a" % r["n_episodes"])
        elif cap < 0:
            R.append("ROSE while SPY dumped: capture %.2f across %d drawdown episode(s), flat-or-up in %s of them" % (
                cap, r["n_episodes"], fu))
        else:
            R.append("captured only %.0f%% of the SPY dumps (size-weighted) across %d episode(s), flat-or-up in %s of them" % (
                cap * 100, r["n_episodes"], fu))
        if r.get("capture_worst") is not None and r["capture_worst"] > 1.0:
            R.append("but fell MORE than the market in its worst episode (%s)" % r.get("worst_episode"))
    if r.get("worst_days_excess_bps") is not None:
        R.append("on SPY's %d worst days: %+.0f bps vs SPY, green %.0f%% of the time" % (
            r["worst_days_n"], r["worst_days_excess_bps"], (r.get("worst_days_green_rate") or 0) * 100))
    if r.get("vs_ema250_pct") is not None:
        R.append("%.1f%% %s the 250-day EMA" % (abs(r["vs_ema250_pct"]), "below" if r["vs_ema250_pct"] < 0 else "above"))
    if r.get("bb_width_pctile") is not None:
        R.append("Bollinger bandwidth in the %.0fth percentile of its own year%s%s" % (
            r["bb_width_pctile"], " -- TTM squeeze ON" if r.get("ttm_squeeze_on") else "",
            (" for %d sessions" % r["squeeze_days"]) if r.get("squeeze_days") else ""))
    if r.get("valuation_score") is not None:
        R.append("valuation cheaper than %.0f%% of its %s peers%s" % (
            r["valuation_score"], r.get("valuation_group") or "sector",
            (" (PEG %.2f)" % r["peg"]) if r.get("peg") else ""))
    if r.get("sales_yoy_pct") is not None:
        R.append("revenue %+.0f%% y/y%s%s" % (
            r["sales_yoy_pct"], (", %+.0f%% q/q" % r["sales_qoq_pct"]) if r.get("sales_qoq_pct") is not None else "",
            (", industry %+.0f%%" % r["industry_rev_growth_pct"]) if r.get("industry_rev_growth_pct") is not None else ""))
    fl = r.get("flows") or {}
    if r.get("flow_score") is not None:
        bits = []
        if fl.get("z90") is not None:
            bits.append("flow z %.1f" % fl["z90"])
        if fl.get("pct_aum_21d") is not None:
            bits.append("%+.1f%% AUM/21d" % fl["pct_aum_21d"])
        if fl.get("fv_flows_1m_pct") is not None:
            bits.append("%+.1f%% AUM/1m (Finviz)" % fl["fv_flows_1m_pct"])
        R.append("%s (%s) flows: %s%s" % (r.get("industry_etf"), r.get("industry_etf_name") or "",
                                          ", ".join(bits) or "no flow legs",
                                          " -- MAJOR inflows" if r.get("industry_inflow_major") else ""))
    if r.get("backlog_to_mcap") is not None:
        R.append("backlog %.1fx market cap (%s%s)" % (
            r["backlog_to_mcap"], r.get("backlog_source"),
            (", RPO %+.0f%% y/y" % r["rpo_yoy_pct"]) if r.get("rpo_yoy_pct") is not None else ""))
    if r.get("contracts_90d_vs_mcap_pct"):
        R.append("$%.0fM of contract wins in 90d = %.1f%% of market cap" % (
            (r.get("contracts_90d_usd") or 0) / 1e6, r["contracts_90d_vs_mcap_pct"]))
    if r.get("nlav_coverage") is not None and r["nlav_coverage"] >= 0.3:
        R.append("net liquid assets cover %.0f%% of market cap" % (r["nlav_coverage"] * 100))
    if r.get("insider_buys_30d"):
        R.append("%d insider buy(s) in 30d%s" % (r["insider_buys_30d"], " (cluster)" if r.get("insider_cluster") else ""))
    if r.get("inst_net_usd") and r["inst_net_usd"] > 0:
        R.append("13F net institutional buying $%.0fM" % (r["inst_net_usd"] / 1e6))
    if r.get("obv_divergence") or r.get("ad_divergence"):
        R.append("volume divergence: %s rising %+.2f ADV/day while price is flat (%+.1f%% 3m) -- dips are being bought" % (
            "OBV" if r.get("obv_divergence") else "A/D line",
            (r.get("obv_slope_63") if r.get("obv_divergence") else r.get("ad_slope_63")) or 0, r.get("ret_3m_pct") or 0))
    if r.get("absorption_clv") is not None and r["absorption_clv"] >= 0.6:
        R.append("absorption: closed in the top %.0f%% of its range on the market's worst days%s" % (
            r["absorption_clv"] * 100, (" on %.1fx volume" % r["worst_days_rel_volume"]) if r.get("worst_days_rel_volume") else ""))
    if str(r.get("dark_pool_state") or "").upper() == "ACCUMULATION":
        R.append("FINRA dark-pool share %.0f%% in ACCUMULATION state (accel %+.2f)" % (
            r.get("dark_pool_pct") or 0, r.get("dark_pool_accel") or 0))
    if r.get("rs_leading"):
        R.append("RS line vs SPY at a 52-week high while price sits %.0f%% under its own high (O'Neil lead)" % abs(r.get("pct_from_52w_high") or 0))
    if r.get("vcp_ok"):
        R.append("volatility contraction: pullbacks %s%% -- supply drying up" % "% > ".join(str(x) for x in (r.get("vcp_contractions") or [])[-3:]))
    if r.get("etf_pressure_pct_mcap") is not None and r["etf_pressure_pct_mcap"] >= 0.3:
        R.append("ETF creations imply $%.0fM of mechanical demand (21d) = %.2f%% of market cap" % (
            (r.get("etf_pressure_21d_usd") or 0) / 1e6, r["etf_pressure_pct_mcap"]))
    if r.get("congress_buys_60d"):
        R.append("%d Congressional purchase(s) disclosed in 60d" % r["congress_buys_60d"])
    op = r.get("options") or {}
    if op.get("iv_rank") is not None and op["iv_rank"] <= 25:
        R.append("options market agrees: IV rank %.0f (cheap convexity)%s" % (
            op["iv_rank"], (", VRP %.2f" % op["vrp"]) if op.get("vrp") is not None else ""))
    return R[:12]


def invalidation_for(r):
    parts = []
    rl = r.get("range_low_60")
    if rl and r.get("close"):
        parts.append("close below the 60-session range low %.2f (%.1f%% away) on rising volume -- supply won" % (
            rl, (rl / r["close"] - 1) * 100))
    if r.get("dump_capture") is not None:
        parts.append("a market dump in which the name falls more than half of SPY's move (capture > 0.5)")
    if r.get("flow_score") is not None:
        parts.append("industry flow score falling below 45 (net outflows)")
    if r.get("sales_yoy_pct") is not None:
        parts.append("revenue growth turning negative y/y")
    return "; ".join(parts) if parts else None


def trade_plan(r):
    """Pivot / stop / target / reward-to-risk from the coil geometry. Research
    shorthand for a PM's ticket, not advice."""
    c = r.get("close")
    atr = (r.get("atr20_pct") or 0) / 100.0 * c if c else None
    if not c or not atr:
        return None
    pivot = r.get("breakout_level") or c
    stop = max((r.get("range_low_60") or 0) * 0.99, c - 2.5 * atr)
    if stop >= c:
        stop = c - 2.0 * atr
    hi52 = c * (1 - (r.get("pct_from_52w_high") or 0) / 100.0)
    tgt_cands = [x for x in (r.get("target_price"), hi52) if x and x > c * 1.02]
    target = min(tgt_cands) if tgt_cands else c * (1 + 2 * (r.get("atr20_pct") or 3) * math.sqrt(20) / 100.0)
    target2 = max(tgt_cands) if tgt_cands else None
    risk = (c - stop) / c * 100
    reward = (target / c - 1) * 100
    return {"pivot": rnd(pivot, 2), "pivot_dist_pct": rnd((pivot / c - 1) * 100, 1),
            "stop": rnd(stop, 2), "risk_pct": rnd(risk, 1),
            "target": rnd(target, 2), "reward_pct": rnd(reward, 1), "target_2": rnd(target2, 2),
            "reward_to_risk": rnd(min(20.0, reward / risk), 2) if risk > 0 else None,
            "note": "enter on the pivot break with volume or scale inside the coil; stop under the range low / 2.5 ATR"}


def risk_parity_sizing(picks):
    """equal expected dump-loss weights across the picks (capped 15%)."""
    inv = {}
    for p_ in picks:
        dd = p_.get("dump_downside_pct")
        if dd and dd > 0:
            inv[p_["ticker"]] = 1.0 / dd
    if not inv:
        return {}
    w = {k: v / sum(inv.values()) for k, v in inv.items()}
    cap = max(0.15, 1.0 / len(w))
    for _ in range(8):
        over = {k: v for k, v in w.items() if v > cap + 1e-9}
        if not over:
            break
        excess = sum(v - cap for v in over.values())
        for k in over:
            w[k] = cap
        rest = [k for k in w if k not in over]
        tot = sum(w[k] for k in rest)
        for k in rest:
            w[k] += excess * (w[k] / tot) if tot else 0
    return {k: round(v * 100, 1) for k, v in w.items()}


def watch_trigger(r):
    """For a 5/6 name: the one gate that fails and what would flip it."""
    g = r.get("gates") or {}
    fails = [k for k, v in g.items() if v is False]
    if len(fails) != 1:
        return None
    k = fails[0]
    if k == "under_ema250":
        return "above EMA250 by %.1f%% -- needs a pullback under %.2f" % (r.get("vs_ema250_pct") or 0, r.get("ema250") or 0)
    if k == "dump_resilient":
        return "dump capture %.2f (worst %.2f) -- needs the next SPY dump held (capture <= 0.35, never worse than the market)" % (
            r.get("dump_capture") or 0, r.get("capture_worst") or 0)
    if k == "coiled":
        return "bandwidth pctile %.0f -- needs <= 20 or a 3-session Keltner squeeze" % (r.get("bb_width_pctile") or 0)
    if k == "low_valuation":
        return "valuation score %.0f vs %s peers -- needs >= 55 (cheaper than most peers) or PEG <= 1.2 / EV-EBITDA <= 10 / FCF yield >= 6%%" % (
            r.get("valuation_score") or 0, r.get("valuation_group") or "sector")
    if k == "growth":
        return "revenue %s%% y/y, industry %s%% -- needs >= 5%% on either" % (rnd(r.get("sales_yoy_pct"), 0), rnd(r.get("industry_rev_growth_pct"), 0))
    if k == "industry_inflows":
        return "%s flow score %s -- needs >= 55 with a positive leg" % (r.get("industry_etf"), rnd(r.get("flow_score"), 0))
    return k


def session_changes(stock_rows, session):
    """what moved since the previous snapshot (tier upgrades / downgrades / entries / exits)."""
    keys = sorted(k for k in list_keys(HIST_PREFIX) if k.endswith(".json.gz") and k[len(HIST_PREFIX):-8] < session)
    if not keys:
        return {"status": "no prior snapshot", "prev_session": None}
    prev = s3_json(keys[-1]) or {}
    prev_tier = {p_["t"]: p_.get("tier") for p_ in prev.get("picks") or []}
    rank = {"FORTRESS_COIL": 0, "COILED": 1, "ACCUMULATING": 2, "WATCH": 3, "SCREENED": 4}
    cur = {r["ticker"]: r["tier"] for r in stock_rows if r["tier"] in ("FORTRESS_COIL", "COILED", "ACCUMULATING")}
    ups, downs, new, gone = [], [], [], []
    for t, tier in cur.items():
        pt_ = prev_tier.get(t)
        if pt_ is None:
            new.append({"ticker": t, "tier": tier})
        elif rank[tier] < rank.get(pt_, 4):
            ups.append({"ticker": t, "from": pt_, "to": tier})
        elif rank[tier] > rank.get(pt_, 4):
            downs.append({"ticker": t, "from": pt_, "to": tier})
    for t, pt_ in prev_tier.items():
        if t not in cur:
            gone.append({"ticker": t, "was": pt_})
    order = lambda x: rank.get(x.get("to") or x.get("tier") or "", 9)  # noqa: E731
    return {"status": "ok", "prev_session": prev.get("session"),
            "new_fortress": [x["ticker"] for x in ups + new if (x.get("to") or x.get("tier")) == "FORTRESS_COIL"],
            "upgrades": sorted(ups, key=order)[:40], "downgrades": sorted(downs, key=order)[:40],
            "new_entries": sorted(new, key=order)[:40], "exits": gone[:40],
            "n_prev": len(prev_tier), "n_now": len(cur)}


# ── ETF rows ─────────────────────────────────────────────────────────────
def build_etf_rows(bars, dates, mkt, F):
    rows = []
    for sym, b in bars.items():
        fv = F["finviz"].get(to_fv(sym)) or {}
        known_etf = is_etf_row(fv) or sym in F["flows_poly"] or sym in F["flows_true"] or sym in F["rotation"]
        if not known_etf or sym == "SPY" or len(b.c) < 60:
            continue
        name = fv.get("company") or ""
        lev = bool(LEV_RX.search(name)) or bool((F["flows_poly"].get(sym) or {}).get("leveraged"))
        if lev or OVERLAY_RX.search(name):
            continue
        et = str(fv.get("etf_type") or "").lower()
        if "equit" not in et and sym not in IND_ETF.values() and sym not in SECTOR_ETF.values():
            continue  # bond / commodity / currency wrappers are not the brief (equity accumulation)
        ps = price_signals(b, mkt, {}, dates)
        if (ps.get("vol_100d_pct") or 0) < 8.0:
            continue  # cash-like vol: not an equity accumulation candidate
        fl = flow_legs(sym, F)
        rot = F["rotation"].get(sym) or {}
        aum = fnum(fv.get("aum"))
        if aum is not None and 0 < aum < 1e8:
            aum *= 1e6  # finviz $MM
        if aum is not None and aum > 2e12:
            aum = None  # bogus feed value (VXZ carried 4e13)
        r = {"ticker": sym, "name": name, "aum_usd": aum,
             "etf_type": fv.get("etf_type"), "kind": rot.get("kind"),
             "industry_name": IND_ETF_NAME.get(sym) or rot.get("name")}
        r.update({k: ps.get(k) for k in ps if k != "episodes"})
        r["episodes"] = ps["episodes"]
        r["rs_vs_spy_3m_pp"] = rs_vs(b, bars.get("SPY"), 63)
        r["flows"] = {k: (rnd(v, 3) if isinstance(v, float) else v) for k, v in fl.items()}
        r["flow_score"] = fl.get("score")
        r["inflow"] = bool(fl.get("inflow"))
        r["inflow_major"] = bool(fl.get("major"))
        r["leadership"] = fnum(rot.get("leadership_score"))
        r["rrg"] = ((F["rrg"].get(sym) or {}).get("quadrant") if isinstance(F["rrg"], dict) else None)
        g = {
            "under_ema250": (r["vs_ema250_pct"] < 0) if r.get("vs_ema250_pct") is not None else None,
            "dump_resilient": None, "coiled": None,
            "inflows": (bool(fl.get("inflow")) if fl.get("score") is not None else None),
        }
        g["dump_resilient"] = dump_gate(r)
        g["coiled"] = coil_gate(r)
        passed = sum(1 for v in g.values() if v)
        r["gates"] = g
        r["gates_passed"] = passed
        r["tier"] = {4: "ETF_FORTRESS", 3: "ETF_COILED", 2: "ETF_WATCH"}.get(passed, "SCREENED")
        if r["tier"] != "SCREENED" and ((r.get("adv_usd_20d") or 0) < P["min_adv_usd"]
                                        or (aum is not None and aum < 5e7)):
            r["tier"] = "SCREENED"
        pil = {"resilience": resilience_score(r), "coil": coil_score(r),
               "location": location_score(r), "flows": fl.get("score"),
               "momentum": (lin_map(r["rs_vs_spy_3m_pp"], -20, 0, 20, 100)
                            if r.get("rs_vs_spy_3m_pp") is not None else None)}
        w = {"resilience": 35, "coil": 20, "location": 10, "flows": 25, "momentum": 10}
        num = sum(w[k] * clamp(v) for k, v in pil.items() if v is not None)
        den = sum(w[k] for k, v in pil.items() if v is not None)
        r["pillars"] = {k: rnd(v, 1) for k, v in pil.items()}
        r["composite"] = rnd(num / den, 1) if den >= 50 else None
        rows.append(r)
    rows.sort(key=lambda x: (-(x["gates_passed"]), -(x["composite"] or 0)))
    log("etf rows built: %d" % len(rows))
    return rows


# ── industry board ───────────────────────────────────────────────────────
def build_industry_board(stock_rows, etf_rows, F):
    by_etf = {}
    for r in stock_rows:
        e = r.get("industry_etf")
        if not e:
            continue
        b = by_etf.setdefault(e, {"etf": e, "name": IND_ETF_NAME.get(e) or e, "n": 0,
                                   "tiers": {}, "top": [], "industries": set()})
        b["n"] += 1
        b["tiers"][r["tier"]] = b["tiers"].get(r["tier"], 0) + 1
        b["industries"].add(r.get("industry"))
        if r["tier"] != "SCREENED":
            b["top"].append((r.get("composite") or 0, r["ticker"], r["tier"]))
    erow = {r["ticker"]: r for r in etf_rows}
    out = []
    for e, b in by_etf.items():
        fl = flow_legs(e, F)
        er = erow.get(e) or {}
        rot = F["rotation"].get(e) or {}
        boom_rows = [F["boom"].get(i) for i in b["industries"] if F["boom"].get(i)]
        b["top"].sort(reverse=True)
        out.append({
            "etf": e, "name": b["name"], "n_stocks": b["n"], "tiers": b["tiers"],
            "top_names": [{"ticker": t, "tier": ti, "composite": c} for c, t, ti in b["top"][:6]],
            "flow_score": rnd(fl.get("score"), 1), "inflow": bool(fl.get("inflow")),
            "inflow_major": bool(fl.get("major")),
            "flow_z90": rnd(fl.get("z90"), 2), "pct_aum_21d": rnd(fl.get("pct_aum_21d"), 2),
            "fv_flows_1m_pct": rnd(fl.get("fv_flows_1m_pct"), 2),
            "quadrant": fl.get("quadrant"),
            "leadership": fnum(rot.get("leadership_score")), "tag": rot.get("tag"),
            "rrg": ((F["rrg"].get(e) or {}).get("quadrant") if isinstance(F["rrg"], dict) else None),
            "etf_vs_ema250_pct": rnd(er.get("vs_ema250_pct"), 2),
            "etf_dump_capture": rnd(er.get("dump_capture"), 2),
            "etf_bb_width_pctile": rnd(er.get("bb_width_pctile"), 1),
            "etf_tier": er.get("tier"),
            "industry_rev_growth_pct": rnd(mean([fnum((x.get("comp") or {}).get("rev_mean")) for x in boom_rows]), 1),
            "boom_score": rnd(mean([fnum(x.get("boom_score")) for x in boom_rows]), 1),
            "boom_delta_20d": rnd(mean([fnum(x.get("score_delta_20d")) for x in boom_rows]), 1),
        })
    out.sort(key=lambda x: (-(x["tiers"].get("FORTRESS_COIL", 0) * 4 + x["tiers"].get("COILED", 0) * 2
                              + x["tiers"].get("ACCUMULATING", 0)), -(x["flow_score"] or 0)))
    return out


# ── history + own base rates ─────────────────────────────────────────────
def snapshot_and_base_rates(stock_rows, bars, dates, session):
    picks = [{"t": r["ticker"], "tier": r["tier"], "score": r.get("composite"), "c": rnd(r.get("close"), 4)}
             for r in stock_rows if r["tier"] in ("FORTRESS_COIL", "COILED", "ACCUMULATING")][:400]
    try:
        s3_put_json(HIST_PREFIX + session + ".json.gz",
                    {"session": session, "as_of": datetime.now(timezone.utc).isoformat(), "picks": picks}, gz=True)
    except Exception as e:  # noqa: BLE001
        log("history write failed: %s" % str(e)[:100])
    keys = sorted(k for k in list_keys(HIST_PREFIX) if k.endswith(".json.gz"))[-60:]
    date_idx = {d: i for i, d in enumerate(dates)}
    spy = bars.get("SPY")
    agg = {}
    n_snap = 0
    H = 21
    for k in keys:
        snap = s3_json(k)
        if not snap:
            continue
        s = snap.get("session")
        if s not in date_idx:
            continue
        i0 = date_idx[s]
        if i0 + H >= len(dates):
            continue
        n_snap += 1
        p_spy0 = spy.pos_exact(i0) if spy else None
        p_spy1 = spy.pos_at_or_before(i0 + H) if spy else None
        spy_ret = (spy.c[p_spy1] / spy.c[p_spy0] - 1) if (p_spy0 is not None and p_spy1 is not None) else None
        for p in snap.get("picks") or []:
            b = bars.get(p.get("t"))
            if not b:
                continue
            q0 = b.pos_exact(i0)
            q1 = b.pos_at_or_before(i0 + H)
            if q0 is None or q1 is None or q1 <= q0:
                continue
            rr = b.c[q1] / b.c[q0] - 1
            a = agg.setdefault(p.get("tier"), {"n": 0, "rets": [], "ex": []})
            a["n"] += 1
            a["rets"].append(rr * 100)
            if spy_ret is not None:
                a["ex"].append((rr - spy_ret) * 100)
    out = {}
    for tier, a in agg.items():
        out[tier] = {"n": a["n"], "median_ret_21d_pct": rnd(median(a["rets"]), 2),
                     "median_excess_vs_spy_pct": rnd(median(a["ex"]), 2),
                     "hit_rate_vs_spy_pct": (rnd(100.0 * sum(1 for x in a["ex"] if x > 0) / len(a["ex"]), 1)
                                             if a["ex"] else None)}
    return {"status": "accruing" if not out else "measured", "horizon_sessions": H,
            "snapshots_graded": n_snap, "snapshots_total": len(keys), "by_tier": out,
            "note": ("forward returns computed by THIS engine from the bar warehouse on its own "
                     "past snapshots; also harvested fleet-wide as eng:fortress by justhodl-signal-harvester")}


# ── walk-forward validation of the price-structure legs ─────────────────
def lite_signals(b, pos, mkt_t):
    """point-in-time price gates for one name as of position `pos` (inclusive)."""
    start = max(0, pos - (P["sessions_loaded"] - 1))
    c = b.c[start:pos + 1]
    n = len(c)
    if n < 300:
        return None
    c = list(c)
    last = c[-1]
    v = b.v[pos - 19:pos + 1]
    adv = sum(c[-20 + i] * v[i] for i in range(len(v))) / max(1, len(v))
    if last < P["min_price"] or adv < P["min_adv_usd"]:
        return None
    ema250 = ema_last(c, 250)
    if ema250 is None:
        return None
    under = last < ema250
    _, up, lob, bw = bb_series(c[-300:], 20, 2.0)
    hist = [x for x in bw[-252:] if x is not None]
    cur = bw[-1]
    if cur is None or len(hist) < 100:
        return None
    bbp = 100.0 * sum(1 for x in hist if x <= cur) / len(hist)
    h = list(b.h[pos - 99:pos + 1])
    lo = list(b.l[pos - 99:pos + 1])
    c100 = c[-100:]
    atr = atr_series(h, lo, c100, 20)
    e20 = ema_series(c100, 20)
    sq_days = 0
    km = P["keltner_atr_mult"]
    for k in range(99, 39, -1):          # last 60 sessions; bb arrays are aligned to c[-300:]
        j = 200 + k
        if up[j] is None or atr[k] is None or e20[k] is None:
            break
        if up[j] < e20[k] + km * atr[k] and lob[j] > e20[k] - km * atr[k]:
            sq_days += 1
        else:
            break
    coiled = bbp <= P["bb_squeeze_pctile"] or sq_days >= 3
    # episodes as of t
    d = b.d
    caps = []
    worst = None
    wsum = 0.0
    wcap = 0.0
    dsum = 0.0
    dcap_ = 0.0
    rets = mkt_t["rets"]
    for e in mkt_t["episodes"]:
        p0 = b.pos_at_or_before(e["peak_idx"])
        p1 = b.pos_at_or_before(e["trough_idx"])
        if p0 is None or p1 is None or p1 <= p0 or p0 < start:
            continue
        if e["peak_idx"] - d[p0] > 5 or e["trough_idx"] - d[p1] > 5:
            continue
        sr = (b.c[p1] / b.c[p0] - 1) * 100
        cap = sr / e["spy_dd_pct"]
        caps.append(cap)
        worst = cap if worst is None else max(worst, cap)
        wsum += e["weight"]
        wcap += cap * e["weight"]
        sd_ = 0.0
        md_ = 0.0
        for pp in range(p0 + 1, p1 + 1):
            mr = rets.get(d[pp])
            if mr is None or mr >= 0 or d[pp - 1] != d[pp] - 1:
                continue
            sd_ += b.c[pp] / b.c[pp - 1] - 1
            md_ += mr
        if md_ < 0:
            dsum += e["weight"]
            dcap_ += (sd_ / md_) * e["weight"]
    capw = (wcap / wsum) if wsum else None
    dcapw = (dcap_ / dsum) if dsum else None
    ex = []
    for idx in mkt_t["worst_idx"]:
        q = b.pos_exact(idx)
        if q is None or q == 0 or d[q - 1] != idx - 1:
            continue
        ex.append((b.c[q] / b.c[q - 1] - 1 - rets[idx]) * 1e4)
    exm = mean(ex) if len(ex) >= 6 else None
    resilient = None
    if capw is not None or exm is not None:
        ep_ok = (capw is not None and capw <= P["capture_barely_dipped"]
                 and (worst is None or worst <= P["capture_worst_max"])
                 and (dcapw is None or dcapw <= 0.6) and (exm is None or exm >= -75))
        day_ok = (exm is not None and exm >= 25 and (capw is None or capw <= 0.7)
                  and (worst is None or worst <= 1.5))
        resilient = bool(ep_ok or day_ok)
    knife = (n > 63 and last / c[-64] - 1 <= P["knife_ret_3m_pct"] / 100.0) or (last / ema250 - 1 <= P["knife_below_ema_pct"] / 100.0)
    return {"under": under, "coiled": coiled, "resilient": resilient, "bbp": bbp,
            "capture": capw, "worst_ex": exm, "knife": knife,
            "n_gates": int(under) + int(bool(coiled)) + int(bool(resilient))}


def run_backtest(event):
    F = load_feeds()
    etfs = {to_poly(str(t).upper()) for t, fv in F["finviz"].items() if is_etf_row(fv)}
    etfs.update(F["flows_poly"].keys())
    keys = session_keys(int(event.get("sessions") or P["backtest_sessions"]))
    dates, bars = load_bars(keys, None)   # every clean ticker in the tape (delisted names included)
    spy = bars.get("SPY")
    if not spy or len(spy.c) < 900:
        raise RuntimeError("SPY history too short for a walk-forward test")
    W = P["sessions_loaded"]
    step = int(event.get("step") or P["backtest_step"])
    H1, H2 = 21, 63
    spy_pos = {spy.d[i]: i for i in range(len(spy.d))}
    t_list = list(range(W, len(dates) - H2, step))
    log("backtest: %d sessions, %d test dates (%s..%s), universe %d" % (
        len(dates), len(t_list), dates[t_list[0]] if t_list else None, dates[t_list[-1]] if t_list else None, len(bars)))
    obs = []   # (t, sym, n_gates, under, coiled, resilient, bbp, capture, ex21, ex63, r21)
    per_date = []
    for t in t_list:
        sp = spy_pos.get(t)
        if sp is None:
            continue
        mkt_t = market_context(spy, dates, len(dates), asof_pos=sp, window=W)
        sp21 = spy.pos_at_or_before(t + H1)
        sp63 = spy.pos_at_or_before(t + H2)
        spy_r21 = spy.c[sp21] / spy.c[sp] - 1
        spy_r63 = spy.c[sp63] / spy.c[sp] - 1
        n_t = 0
        coh = []
        for sym, b in bars.items():
            if sym in etfs or sym == "SPY":
                continue
            pos = b.pos_exact(t)
            if pos is None:
                continue
            sig = lite_signals(b, pos, mkt_t)
            if sig is None:
                continue
            p21 = b.pos_at_or_before(t + H1)
            p63 = b.pos_at_or_before(t + H2)
            if p21 is None or p63 is None or b.d[p21] < t + H1 - 3 or b.d[p63] < t + H2 - 5:
                continue
            r21 = b.c[p21] / b.c[pos] - 1
            r63 = b.c[p63] / b.c[pos] - 1
            r21 = max(-0.95, min(3.0, r21))
            r63 = max(-0.95, min(5.0, r63))
            n_t += 1
            ex21 = (r21 - spy_r21) * 100
            ex63 = (r63 - spy_r63) * 100
            obs.append((t, sig["n_gates"], sig["under"], sig["coiled"], sig["resilient"], sig["bbp"],
                        sig["capture"], sig["knife"], ex21, ex63))
            if sig["n_gates"] == 3 and not sig["knife"]:
                coh.append(ex21)
        per_date.append({"date": dates[t], "n_scored": n_t, "spy_fwd21_pct": round(spy_r21 * 100, 2),
                         "spy_fwd63_pct": round(spy_r63 * 100, 2), "n_fortress3": len(coh),
                         "fortress3_median_ex21_pct": rnd(median(coh), 2),
                         "fortress3_hit21_pct": rnd(100.0 * sum(1 for x in coh if x > 0) / len(coh), 1) if coh else None,
                         "n_dump_episodes_asof": mkt_t["n_episodes"]})
        log("backtest %s: scored %d, 3/3 cohort %d" % (dates[t], n_t, len(coh)))

    def stats(rows):
        if not rows:
            return {"n": 0}
        e21 = [x[8] for x in rows]
        e63 = [x[9] for x in rows]
        return {"n": len(rows), "median_ex21_pct": rnd(median(e21), 2), "mean_ex21_pct": rnd(mean(e21), 2),
                "hit21_pct": rnd(100.0 * sum(1 for x in e21 if x > 0) / len(e21), 1),
                "median_ex63_pct": rnd(median(e63), 2), "mean_ex63_pct": rnd(mean(e63), 2),
                "hit63_pct": rnd(100.0 * sum(1 for x in e63 if x > 0) / len(e63), 1)}
    clean = [x for x in obs if not x[7]]
    by_gates = {str(k): stats([x for x in clean if x[1] == k]) for k in range(4)}
    fortress_tight = stats([x for x in clean if x[1] == 3 and x[5] <= 10])
    resilient = stats([x for x in clean if x[4] is True])
    not_resilient = stats([x for x in clean if x[4] is False])
    under_only = stats([x for x in clean if x[2] and not x[3] and not x[4]])
    knives = stats([x for x in obs if x[7]])
    withcap = sorted([x for x in clean if x[6] is not None], key=lambda x: x[6])
    deciles = {}
    if len(withcap) >= 100:
        k = len(withcap) // 10
        for i in range(10):
            seg = withcap[i * k:(i + 1) * k if i < 9 else len(withcap)]
            deciles["D%d" % (i + 1)] = dict(stats(seg), capture_lo=rnd(seg[0][6], 2), capture_hi=rnd(seg[-1][6], 2))
    spy_ex_note = "excess = name forward return minus SPY over the same sessions, percentage points; returns clipped to [-95%, +300%/+500%]"
    out = {
        "engine": ENGINE, "version": VERSION, "mode": "backtest",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window_sessions": W, "sessions_loaded": len(dates), "first_session": dates[0], "last_session": dates[-1],
        "test_dates": [d_["date"] for d_ in per_date], "step_sessions": step, "n_observations": len(obs),
        "n_universe_scored_mean": rnd(mean([d_["n_scored"] for d_ in per_date]), 0),
        "horizons": {"h1": H1, "h2": H2},
        "by_price_gates": by_gates,
        "fortress3": by_gates.get("3"), "fortress3_tight_bbw10": fortress_tight,
        "resilient_vs_not": {"resilient": resilient, "not_resilient": not_resilient},
        "under_ema250_only": under_only, "knife_guard_cohort": knives,
        "by_capture_decile": deciles,
        "per_date": per_date,
        "method": ("Every %d sessions from %s, the three price-structure gates (under EMA250, dump-resilient, coiled) "
                   "are scored exactly as the live engine scores them, using only bars up to that date (SPY episodes "
                   "and worst days as of that date). Forward returns are read %d and %d sessions later." % (step, dates[W] if len(dates) > W else "?", H1, H2)),
        "caveats": [
            "Price-structure legs only: valuation, growth, flows, backlog and safety feeds have no point-in-time history and are NOT backtested.",
            "Universe = every clean ticker with bars on the test date and $1M+ ADV (delisted names included, so survivorship bias is limited; today's ETF list is excluded).",
            "Adjusted bars from the polygon-full warehouse; corporate actions as Polygon adjusts them.",
            spy_ex_note,
            "A walk-forward sample of ~%d dates is a check on direction and monotonicity, not a guarantee." % len(per_date),
        ],
        "diagnostics": {"log": LOG[-30:], "elapsed_s": round(time.time() - T0, 1)},
    }
    s3_put_json(BACKTEST_KEY, out)
    log("backtest written: obs=%d 3/3=%s tight=%s %.0fs" % (len(obs), by_gates.get("3"), fortress_tight, time.time() - T0))
    return {"ok": True, "mode": "backtest", "n_observations": len(obs), "fortress3": by_gates.get("3"),
            "elapsed_s": round(time.time() - T0, 1)}


DEFINITIONS = {
    "dump_capture": "Size-weighted mean of (stock return / SPY return) across every SPY peak-to-trough drawdown of at least 4% in the trailing year, weighted by the depth of each dump so the biggest dump counts most. 0.20 = the stock fell only a fifth of the market's dumps; negative = it rose while the market dumped. capture_worst is the single worst episode; a fortress may never have fallen more than the market (worst <= 1.0). Falls back to the down-day capture ratio when no episode overlaps the name's history.",
    "worst_days_excess_bps": "Average of (stock return minus SPY return) on SPY's worst 5% of days in the trailing year, in basis points. Positive = held up better than the market on its ugliest days.",
    "worst_days_green_rate": "Share of SPY's worst days on which the stock still closed UP.",
    "down_capture_pct": "Mean stock return on all SPY down-days divided by the mean SPY return on those days, x100. 40 = the stock falls 40% as much as SPY on red days; 0 or negative = it does not fall (or rises) when the market does.",
    "beta_asymmetry": "Upside beta (regressed on SPY up-days) minus downside beta (SPY down-days). Positive = more sensitive to rallies than to dumps: a convex profile.",
    "vs_ema250_pct": "Distance from the true 250-session exponential moving average (SMA-seeded). Negative = below the long-term average = the accumulation zone this radar hunts. Below -35% is treated as a collapse, not accumulation (knife guard).",
    "bb_width_pctile": "Where today's Bollinger(20,2) bandwidth sits versus every day of the trailing year. 5 = tighter than 95% of its own history. Bottom 20% = coiled; bottom 10% = very tight.",
    "ttm_squeeze_on": "Bollinger bands entirely inside the Keltner channel (EMA20 +/- 1.5 ATR20). A classic pre-expansion signature; squeeze_days counts how long it has held. The coiled gate needs a bandwidth percentile <= 20 or a squeeze that has held >= 3 sessions.",
    "bb_pct_b": "Position inside the bands: 0 = lower band, 1 = upper band. 0.6-0.9 with a tight band = coiled near the top of its range.",
    "coil_state": "COILING = tight band and no release; PRE_BREAKOUT = pressing the upper band within 3% of the 60-session high; IGNITED = closed above the upper band on 1.5x volume (already released, chase risk); LOOSE = bands not compressed.",
    "atr_contraction": "ATR(20) / ATR(100). Below 1 = daily ranges are shrinking: volatility compression.",
    "volume_dryup": "20-day average volume / 100-day average volume. Below 1 = supply has dried up while the name held its range.",
    "valuation_score": "100 minus the average percentile of PEG, EV/EBITDA, P/S, P/FCF and forward P/E within the name's own industry (or sector when the industry has fewer than 8 names). 70 = cheaper than 70% of peers. +10 if PEG <= 1, +5 if FCF yield >= 6%.",
    "growth_score": "Absolute-anchored blend of revenue y/y and q/q, EPS y/y, q/q and next-year, 3-year sales CAGR, plus the industry's mean revenue growth and breadth from the boom league. 'Major growth' = revenue or industry growth >= 20%.",
    "flow_score": "Consensus of independent flow reads for the name's industry ETF: Polygon creation/redemption z-score (90d) and %-of-AUM (21d), true dShares x NAV flow (%-of-AUM, 20d), and Finviz 1-month net flow %-of-AUM. 50 = flat; >= 55 with a positive leg = inflows; z >= 1 or >= 2% of AUM = MAJOR.",
    "industry_etf": "The ETF that best represents the name's Finviz industry (e.g. Semiconductors -> SMH, Regional Banks -> KRE). Falls back to the sector SPDR when no industry ETF exists; 'industry_etf_kind' says which.",
    "momentum_score": "3-month relative strength vs SPY and vs the industry ETF, 1-month return, industry leadership score and RRG quadrant from the rotation engine, boom-score 20-day delta, and EPS estimate-revision direction.",
    "backlog_to_mcap": "Order book (XBRL remaining performance obligations, else 10-Q text-mined backlog) divided by market cap. 1.0 = a year of committed revenue equal to the whole company. 'Major backlog' = >= 0.5x market cap, or >= 1x revenue and growing.",
    "contracts_90d_vs_mcap_pct": "Sum of announced contract / government / supply wins in the last 90 days as a percent of market cap (deal-scanner ledger). >= 3% = meaningful.",
    "safety_score": "Downside cushion: net-liquid-asset coverage of market cap (floor auditor), Altman Z, Piotroski F, Beneish M, interest coverage or net debt/EBITDA, current ratio, positive FCF, dilution, beta, 1-year max drawdown, dollar liquidity, short-float extremes. Earnings inside 7 days subtracts 12 (binary event inside the coil).",
    "composite": "Weighted average of the nine pillars (weights published in the payload) computed only when at least 60% of the weight is covered by real data. Not a rating -- a ranking of confluence.",
    "asymmetry": "Upside room (median of analyst-PT upside and distance to the 52-week high) divided by the expected loss in a -10% SPY dump (10 x dump capture, floored at half of the name's own one-month sigma so a negative capture never reads as zero risk, cut by 30% when net liquid assets cover >= 50% of market cap; capped at 25). 3.0 = three points of room per point of empirical dump risk.",
    "tier": "FORTRESS_COIL passes all six spec gates (under EMA250, dump-resilient, coiled, low valuation, growth, industry inflows); COILED five; ACCUMULATING four; WATCH three; SCREENED otherwise. Dump-resilient means size-weighted episode capture <= 0.35, never worse than the market in any single dump (worst capture <= 1.0), with worst-day excess not worse than -75 bps; or worst-day excess >= +25 bps with a >= 40% green rate, capture <= 0.7 and worst <= 1.5 -- the two reads may not contradict. The knife guard, hygiene floor and an already-ignited breakout cap the tier.",
    "nlav_coverage": "Net liquid asset value (cash + investments + live-marked crypto x0.85 receivables - debt) divided by market cap, from the asset-floor auditor. 0.6 = 60% of the price is already cash-like.",
    "rs_vs_industry_3m_pp": "Stock's 63-session return minus its industry ETF's, in percentage points. The O'Neil relative-strength leg: is the name leading its own group?",
    "base_rates": "Forward 21-session returns of this engine's OWN past picks, recomputed from the bar warehouse each run. Accrues from the first snapshot; nothing is claimed until measured.",
    "capture_days_weighted": "Second, more robust read of the same dumps: inside each SPY drawdown episode, the stock's return summed over the market's DOWN days only, divided by SPY's -- idiosyncratic up-gaps on green days cannot flatter it. Weighted like dump_capture. The dump gate requires this read <= 0.6 so the two cannot contradict.",
    "capture_recent_1y": "The size/recency-weighted capture restricted to episodes of the trailing year (episodes from the full three-year window drive dump_capture).",
    "worst_days_tstat": "t-statistic of the worst-day excess return (mean / standard error). |t| > 2 = the edge over SPY on bad days is unlikely to be noise; the resilience pillar is shrunk toward neutral when the sample is thin (resilience_confidence).",
    "worst_days_long_excess_bps": "Same worst-day excess, measured on SPY's worst 5% of days across the whole three-year window (a larger sample than the trailing-year set).",
    "resilience_confidence": "0-1 evidence weight for the resilience read: half from the number of dump episodes that overlap the name (4+ = full), half from the number of matched worst days (12+ = full). Multiplies the deviation of the resilience pillar from 50.",
    "absorption_clv": "Average closing location on SPY's worst days: (close - low) / (high - low). 0.8 = the stock closed near its high of the day while the market was being sold -- buyers absorbed the supply. worst_days_rel_volume shows whether that happened on above-average volume.",
    "obv_slope_63": "On-Balance Volume change over 63 sessions in units of average daily volume per day (-1..+1). Positive while price is flat (ret 3m <= +2%) is an OBV divergence: cumulative volume is flowing in without the price paying for it -- the accumulation signature.",
    "ad_slope_63": "Chaikin Accumulation/Distribution line slope, same units as obv_slope_63 but weighted by where each day closed in its range.",
    "updown_volume_ratio_50": "Volume on up days divided by volume on down days over 50 sessions. > 1.3 = buyers are the aggressive side.",
    "dark_pool_state": "FINRA ATS (dark-pool) share of consolidated volume, its acceleration and the state the dark-pool engine assigns (ACCUMULATION / DISTRIBUTION). Rising off-exchange share on a flat tape is institutional positioning; lags ~2-3 weeks by construction.",
    "etf_pressure_21d_usd": "Mechanical demand from ETF creations: sum over every ETF that holds the name of (that ETF's 21-day net flow x the name's weight in it), from the constituent-pressure engine. Shown as % of market cap and as days of average dollar volume.",
    "congress_buys_60d": "Congressional purchase disclosures naming the ticker in the last 60 days (STOCK Act filings), with the maximum disclosed dollar range. A cluster of members buying is a positioning read, not a fundamental one.",
    "insider_cluster_n": "Number of distinct insiders buying in the open market inside the cluster window (insider-cluster scanner), total dollars and the highest role involved.",
    "higher_lows": "Count of consecutive rising swing lows (5-session pivots) over the last 120 sessions. 3+ = an ascending base: sellers are being met at higher prices each time.",
    "vcp_contractions": "Minervini volatility-contraction pattern: depth (%) of successive pullbacks from swing high to swing low. vcp_ok = the latest pullback is shallower than the one before and <= 12%; vcp_strict = three shrinking pullbacks ending <= 10%.",
    "rs_leading": "O'Neil's relative-strength-line lead: the stock/SPY ratio is within 2% of its 52-week high while the price itself is 10%+ below its own 52-week high. Relative strength leads price.",
    "weeks_since_52w_high": "Age of the base. O'Neil's proper bases run 7+ weeks; very old bases (40+ weeks) lose sponsorship.",
    "squeeze_momentum_atr": "Carter-style squeeze momentum: the linear-regression value of (close minus the mid of the 20-day Donchian mid and SMA20) over 20 sessions, in ATRs. Positive and rising = the coil is leaning up before it releases.",
    "cvar5_pct": "Conditional value-at-risk: the average daily return on the worst 5% of days in the trailing year. -4 = a bad day averages -4%. Also floors the dump-downside used in the asymmetry ratio (1.5x CVaR).",
    "downside_dev_pct": "Annualised downside deviation (only negative daily returns count) -- the Sortino denominator.",
    "ulcer_index": "Root-mean-square percentage drawdown from the running peak over the trailing year. Measures how deep and how long the name sat under water.",
    "gaps_5pct_1y": "Number of overnight gaps of 5%+ (open vs prior close) in the trailing year, and the largest. Event-driven tapes (biotech binaries) gap; a fortress mostly does not.",
    "amihud_illiq": "Amihud illiquidity: average |daily return| per $1M traded over 63 sessions. Higher = price moves more per dollar = thinner, more expensive to trade.",
    "rate_beta": "Regression beta of the stock's daily returns on TLT (20+ year Treasuries) over the trailing year. Near 0 = indifferent to rate shocks.",
    "dollar_beta": "Regression beta on UUP (US dollar index). Near 0 = indifferent to dollar shocks.",
    "erp_vs_10y_pct": "Earnings yield (forward, else trailing) minus the 10-year Treasury yield, in percentage points. >= 2 = the equity risk premium on this name is real; +5 valuation bonus.",
    "rule_of_40": "Revenue growth % + FCF margin % (FCF margin = FCF yield x P/S). Software names at 40+ earn a valuation bonus; shown for every name that has both inputs.",
    "options": "Where the fleet's options engine covers the name: IV rank (100 = richest of the year; <= 25 = the vol market also sees a coil), variance risk premium, 25-delta skew, put/call volume ratio, net options premium, dealer gamma regime.",
    "accumulation_score": "Pillar (weight 12): OBV and A/D slopes and divergences, up/down volume ratio, absorption on worst days, dark-pool state and acceleration, 13F net buying vs market cap, insider cluster / buys, Congressional purchases, institutional-transaction change, net buyback yield, ETF constituent pressure.",
    "structure_score": "Pillar (weight 5): higher-lows count, VCP, RS-line lead, base age, squeeze momentum direction.",
    "flows_pillar": "Pillar (weight 9): industry-ETF flow consensus (x2.5), sector-ETF consensus (x0.5), ETF constituent pressure as % of market cap (x1.5), flow persistence days (x0.5).",
    "pillar_ranks": "Each pillar's cross-sectional percentile across every name scored today. The composite is half absolute-anchored pillars, half these ranks, so it is comparable across regimes and still reads in absolute terms.",
    "confidence": "0-1 evidence confidence: 60% resilience_confidence + 40% pillar coverage. The composite is shrunk toward 50 by (0.5 + 0.5 x confidence) so thin evidence cannot top the board.",
    "conviction": "Geometric mean of the six gate pillars (resilience, coil, location, valuation, growth, flows). Unlike a weighted average it punishes any single weak leg -- a fortress must be strong everywhere.",
    "trade_plan": "Pivot = 60-session high; stop = max(range low x 0.99, close - 2.5 ATR); target = the NEARER of the analyst PT and the 52-week high (the first ceiling), target_2 the farther; a 2-ATR x sqrt(20) move if neither is above the price; reward_to_risk in R, capped at 20. Research shorthand for a ticket, not advice.",
    "sizing": "Risk-parity weights across the picks: proportional to 1 / dump_downside so every position carries the same expected loss in a -10% SPY dump, capped at 15%, renormalised.",
    "changes": "Tier moves since the previous snapshot: upgrades, downgrades, new entries into the top three tiers, exits, and the names that just became FORTRESS_COIL.",
    "watch_trigger": "For a 5/6 name, the one gate that fails and the level that would flip it.",
    "validation": "The weekly walk-forward backtest of the price-structure legs (data/fortress-backtest.json): 21- and 63-session excess returns vs SPY by number of price gates passed, by dump-capture decile, and per test date. Fundamentals and flows are not backtested (no point-in-time history) -- read the caveats.",
    "regime": "Market backdrop from the fleet regime engines (regime-composite meta regime, vol regime) plus SPY vs EMA250 and breadth. A fortress screen is most useful entering or inside a dump; when nothing has dumped for a year the capture legs rest on the three-year window and the down-day read.",
}


# ── handler ──────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    global T0
    T0 = time.time()
    LOG.clear()
    event = event or {}
    if str(event.get("mode") or "").lower() == "backtest":
        return run_backtest(event)
    F = load_feeds()
    keep = set()
    for t in F["finviz"]:
        keep.add(to_poly(str(t).upper()))
    keep.update(F["flows_poly"].keys())
    keep.update(F["flows_true"].keys())
    keep.update(F["rotation"].keys())
    keep.update(SECTOR_ETF.values())
    keep.update(IND_ETF.values())
    keep.add("SPY")
    keep = {t for t in keep if TICKER_OK.match(t)}
    keys = session_keys(int(event.get("sessions") or P["sessions_loaded"]))
    if not keys:
        raise RuntimeError("no polygon-full grouped sessions found under " + BARS_ROOT)
    dates, bars = load_bars(keys, keep)
    if "SPY" not in bars or len(bars["SPY"].c) < 260:
        raise RuntimeError("SPY bars missing/short: %s" % len(bars.get("SPY", Bars()).c))
    session = dates[-1]
    mkt = market_context(bars["SPY"], dates, len(dates))
    log("market: %d dump episodes (%d big), %d worst days, SPY vs EMA250 %s%%" % (
        len(mkt["episodes"]), sum(1 for e in mkt["episodes"] if e["big"]),
        len(mkt["worst_days"]), mkt["spy_vs_ema250_pct"]))
    etf_bars = {e: bars[e] for e in set(list(SECTOR_ETF.values()) + list(IND_ETF.values())) if e in bars}
    stock_rows = build_stock_rows(bars, dates, mkt, F, etf_bars)
    valuation_percentiles(stock_rows)
    # industry-relative resilience percentile (lower capture = higher percentile)
    by_ind = {}
    for r in stock_rows:
        if r.get("dump_capture") is not None:
            by_ind.setdefault(r.get("industry") or "?", {})[r["ticker"]] = r["dump_capture"]
    ind_rank = {}
    for ind, vals in by_ind.items():
        if len(vals) >= 5:
            for tk, pr in pct_rank(vals).items():
                ind_rank[tk] = round(100.0 - pr, 1)
    for r in stock_rows:
        r["industry_resilience_pctile"] = ind_rank.get(r["ticker"])
    for r in stock_rows:
        gates_and_tier(r)
        composite_and_asymmetry(r)          # pass 1: absolute pillars
    ranks = pillar_ranks(stock_rows)
    for r in stock_rows:
        composite_and_asymmetry(r, ranks)   # pass 2: rank-blended composite
        r.pop("_pillars_raw", None)
        r["trade_plan"] = trade_plan(r)
        r["watch_trigger"] = watch_trigger(r) if r["gates_passed"] == 5 else None
    for r in stock_rows:
        r["reasons"] = reasons_for(r)
        r["invalidation"] = invalidation_for(r)
        r["risks"] = r.get("risks") or []
        r["flags"] = [f for f, ok in (("MAJOR_GROWTH", r.get("major_growth")),
                                      ("MAJOR_INFLOWS", r.get("industry_inflow_major")),
                                      ("MAJOR_BACKLOG", r.get("major_backlog")),
                                      ("CONTRACTS", r.get("meaningful_contracts")),
                                      ("TIGHT_COIL", (r.get("bb_width_pctile") is not None
                                                      and r["bb_width_pctile"] <= P["bb_tight_pctile"])),
                                      ("PRE_BREAKOUT", r.get("coil_state") == "PRE_BREAKOUT"),
                                      ("IGNITED", r.get("coil_state") == "IGNITED"),
                                      ("BIG_DUMP_PROOF", (r.get("capture_big_dumps") is not None
                                                          and r["capture_big_dumps"] <= P["capture_barely_dipped"])),
                                      ("INSIDER_BUYING", bool(r.get("insider_buys_30d") or r.get("insider_cluster"))),
                                      ("OBV_DIVERGENCE", bool(r.get("obv_divergence") or r.get("ad_divergence"))),
                                      ("ABSORPTION", (r.get("absorption_clv") is not None and r["absorption_clv"] >= 0.6)),
                                      ("DARK_POOL_ACCUM", str(r.get("dark_pool_state") or "").upper() == "ACCUMULATION"),
                                      ("RS_LEADING", bool(r.get("rs_leading"))),
                                      ("VCP", bool(r.get("vcp_ok"))),
                                      ("HIGHER_LOWS", (r.get("higher_lows") or 0) >= 3),
                                      ("ETF_PRESSURE", (r.get("etf_pressure_pct_mcap") is not None and r["etf_pressure_pct_mcap"] >= 0.3)),
                                      ("CONGRESS_BUYING", bool(r.get("congress_buys_60d"))),
                                      ("SP500", r.get("sp500"))) if ok]
    tier_rank = {"FORTRESS_COIL": 0, "COILED": 1, "ACCUMULATING": 2, "WATCH": 3, "SCREENED": 4}
    stock_rows.sort(key=lambda r: (tier_rank[r["tier"]], -(r.get("composite") or 0)))
    tiers = {}
    for r in stock_rows:
        tiers[r["tier"]] = tiers.get(r["tier"], 0) + 1
    funnel = {g: sum(1 for r in stock_rows if r["gates"].get(g)) for g in GATE_NAMES}
    funnel["hygiene_ok"] = sum(1 for r in stock_rows if (r.get("close") or 0) >= P["min_price"]
                               and (r.get("adv_usd_20d") or 0) >= P["min_adv_usd"]
                               and (r.get("n_sessions") or 0) >= P["min_sessions"])
    funnel["ema250_available"] = sum(1 for r in stock_rows if r.get("ema250_available"))
    funnel["scored"] = len(stock_rows)
    etf_rows = build_etf_rows(bars, dates, mkt, F)
    industries = build_industry_board(stock_rows, etf_rows, F)
    base_rates = snapshot_and_base_rates(stock_rows, bars, dates, session)

    def slim(r, keep_episodes=True):
        o = {}
        for k, v in r.items():
            if k == "episodes" and not keep_episodes:
                continue
            if isinstance(v, float):
                v = round(v, 4)
            o[k] = v
        return o

    board = [slim(r) for r in stock_rows if r["tier"] != "SCREENED"][:800]
    LEDGER_KEYS = ["ticker", "name", "sector", "industry", "cap_bucket", "market_cap", "tier", "gates_passed",
                   "composite", "asymmetry", "close", "vs_ema250_pct", "dump_capture", "worst_days_excess_bps",
                   "bb_width_pctile", "ttm_squeeze_on", "coil_state", "valuation_score", "growth_score",
                   "flow_score", "industry_etf", "backlog_to_mcap", "contracts_90d_vs_mcap_pct",
                   "safety_score", "ret_3m_pct", "flags", "conviction", "confidence", "capture_worst",
                   "obv_slope_63", "absorption_clv", "dark_pool_state", "higher_lows", "rs_leading"]
    ledger = [{k: (round(r[k], 3) if isinstance(r.get(k), float) else r.get(k)) for k in LEDGER_KEYS}
              for r in stock_rows if r["gates_passed"] >= 2 and r["tier"] == "SCREENED"][:2500]
    top_picks = [{"ticker": r["ticker"], "score": r.get("composite"), "tier": r["tier"],
                  "conviction": r.get("conviction"), "confidence": r.get("confidence"),
                  "asymmetry": r.get("asymmetry"), "dump_capture": rnd(r.get("dump_capture"), 2),
                  "dump_downside_pct": r.get("dump_downside_pct"),
                  "vs_ema250_pct": rnd(r.get("vs_ema250_pct"), 1),
                  "bb_width_pctile": rnd(r.get("bb_width_pctile"), 0),
                  "industry_etf": r.get("industry_etf"), "trade_plan": r.get("trade_plan"),
                  "reasons": r["reasons"][:4]}
                 for r in stock_rows if r["tier"] in ("FORTRESS_COIL", "COILED") and r.get("composite") is not None][:60]
    sizing = risk_parity_sizing(top_picks[:25])
    for p_ in top_picks:
        p_["risk_parity_weight_pct"] = sizing.get(p_["ticker"])
    changes = session_changes(stock_rows, session)
    bt = F.get("backtest") or {}
    validation = ({"status": "measured", "as_of": bt.get("as_of"), "n_observations": bt.get("n_observations"),
                   "test_dates": (bt.get("test_dates") or [])[:1] + (bt.get("test_dates") or [])[-1:],
                   "n_test_dates": len(bt.get("test_dates") or []),
                   "by_price_gates": bt.get("by_price_gates"), "fortress3_tight_bbw10": bt.get("fortress3_tight_bbw10"),
                   "resilient_vs_not": bt.get("resilient_vs_not"), "under_ema250_only": bt.get("under_ema250_only"),
                   "knife_guard_cohort": bt.get("knife_guard_cohort"),
                   "by_capture_decile": bt.get("by_capture_decile"), "per_date": (bt.get("per_date") or [])[-30:],
                   "method": bt.get("method"), "caveats": bt.get("caveats")}
                  if bt else {"status": "pending", "note": "weekly walk-forward backtest not yet written (justhodl-fortress-backtest-weekly, Sundays 09:00 UTC)"})
    regime = dict(F.get("regime") or {})
    regime.update({"spy_vs_ema250_pct": mkt.get("spy_vs_ema250_pct"), "spy_dd_from_high_pct": mkt.get("spy_dd_from_high_pct"),
                   "n_dump_episodes_3y": mkt.get("n_episodes"), "n_dump_episodes_1y": mkt.get("n_episodes_recent"),
                   "n_big_dumps_3y": mkt.get("n_big_dumps"), "y10_pct": F.get("y10"),
                   "read": ("no SPY drawdown of 4%+ in the trailing year -- capture rests on the three-year window and the down-day read"
                            if not mkt.get("n_episodes_recent") else
                            "%d dump episode(s) in the trailing year, %d in the three-year window (%d of 8%%+)" % (
                                mkt.get("n_episodes_recent"), mkt.get("n_episodes"), mkt.get("n_big_dumps")))})
    etf_board = [slim(r) for r in etf_rows if r["tier"] != "SCREENED"][:150]
    breadth = {
        "pct_under_ema250": rnd(100.0 * sum(1 for r in stock_rows if (r.get("vs_ema250_pct") or 0) < 0
                                            and r.get("ema250_available")) / max(1, funnel["ema250_available"]), 1),
        "pct_coiled": rnd(100.0 * funnel["coiled"] / max(1, len(stock_rows)), 1),
        "pct_dump_resilient": rnd(100.0 * funnel["dump_resilient"] / max(1, len(stock_rows)), 1),
    }
    out = {
        "engine": ENGINE, "version": VERSION, "ok": True,
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "session": session, "sessions_loaded": len(dates), "bars_first": dates[0],
        "doctrine": ("Buy what the market could not push down: names that captured little or none of the "
                     "SPY dumps, sit under their 250-EMA in a Bollinger squeeze, are cheap versus their "
                     "industry, growing, backed by industry inflows and an order book -- and whose volume "
                     "structure shows the dips being bought. Every leg is real data or an honest None; the "
                     "price legs are walked forward through years of tape every week. Research shorthand, not advice."),
        "params": P, "weights": WEIGHTS, "gate_names": GATE_NAMES,
        "market": {k: v for k, v in mkt.items() if k not in ("rets", "lb_rets", "worst_idx", "worst_idx_long", "close_by_idx")},
        "breadth": breadth,
        "funnel": funnel, "tiers": tiers, "n_scored": len(stock_rows),
        "n_universe_bars": len(bars),
        "top_picks": top_picks, "sizing": sizing, "changes": changes, "validation": validation, "regime": regime,
        "board": board, "ledger": ledger,
        "etfs": etf_board, "etf_tiers": {t: sum(1 for r in etf_rows if r["tier"] == t)
                                          for t in ("ETF_FORTRESS", "ETF_COILED", "ETF_WATCH", "SCREENED")},
        "industries": industries,
        "base_rates": base_rates,
        "definitions": DEFINITIONS,
        "inputs": {
            "bars": "data/warm/polygon-full/grouped/ (%s..%s, %d sessions)" % (dates[0], session, len(dates)),
            "finviz_universe": F.get("finviz_asof"), "fundamental_census_matrix": F.get("census_asof"),
            "industry_boom": F.get("boom_asof"), "industry_rotation": F.get("rotation_asof"),
            "etf_flows_polygon": F.get("flows_poly_asof"), "etf_true_flows": F.get("flows_true_asof"),
            "backlog": F.get("backlog_asof"), "backlog_mined": F.get("backlog_mined_asof"),
            "deal_history": F.get("deal_hist_asof"), "catalyst": F.get("catalyst_asof"),
            "floor_audit": F.get("floor_asof"), "resilience": F.get("resilience_asof"),
            "f13_flows": F.get("f13_asof"), "short_interest": F.get("short_asof"),
            "insider_radar": F.get("insider_asof"), "estimate_revisions": F.get("revisions_asof"),
            "sp500": F.get("sp500_asof"),
            "stock_exposure": "etf-flows/stock-exposure-lookup.json (%d names)" % len(F.get("stock_exposure") or {}),
            "dark_pool": "%s (week %s, %d names)" % (F.get("dark_asof"), F.get("dark_week"), len(F.get("dark") or {})),
            "political_trades": F.get("congress_asof"), "options_analytics": F.get("options_asof"),
            "regime_composite": (F.get("regime") or {}).get("as_of"), "vol_regime": (F.get("regime") or {}).get("vol_as_of"),
            "yield_curve": F.get("yc_asof"), "backtest": (F.get("backtest") or {}).get("as_of") if F.get("backtest") else None,
        },
        "diagnostics": {"log": LOG[-40:], "elapsed_s": round(time.time() - T0, 1)},
    }
    out["diagnostics"]["elapsed_s"] = round(time.time() - T0, 1)
    s3_put_json(OUT_KEY, out)
    log("wrote %s: scored=%d tiers=%s picks=%d etfs=%d industries=%d %.1fs" % (
        OUT_KEY, len(stock_rows), tiers, len(top_picks), len(etf_board), len(industries),
        time.time() - T0))
    return {"ok": True, "session": session, "n_scored": len(stock_rows), "tiers": tiers,
            "top_picks": len(top_picks), "elapsed_s": round(time.time() - T0, 1)}
