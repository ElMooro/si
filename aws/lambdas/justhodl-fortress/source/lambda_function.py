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

VERSION = "1.0.3"
ENGINE = "justhodl-fortress"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/fortress.json"
HIST_PREFIX = "data/fortress/history/"
BARS_ROOT = "data/warm/polygon-full/grouped/"

# ── parameters (published in the payload) ────────────────────────────────
P = {
    "sessions_loaded": 330,      # >= 271 for BB-width pctile over 252 + warm-up
    "lookback": 252,
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
    "resilience": 24, "coil": 14, "location": 5, "valuation": 12,
    "growth": 12, "flows": 10, "momentum": 8, "backlog_contracts": 7,
    "safety": 8,
}
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
                        r"option income|overlay|floor etf|managed futures|long/short|tail risk", re.I)
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
    """Per-ticker aligned arrays: d = session index, c/h/l/v floats."""
    __slots__ = ("d", "c", "h", "l", "v")

    def __init__(self):
        self.d = array.array("i")
        self.c = array.array("d")
        self.h = array.array("d")
        self.l = array.array("d")
        self.v = array.array("d")

    def pos_at_or_before(self, idx):
        p = bisect.bisect_right(self.d, idx) - 1
        return p if p >= 0 else None

    def pos_exact(self, idx):
        p = bisect.bisect_left(self.d, idx)
        return p if p < len(self.d) and self.d[p] == idx else None


def session_keys(n):
    now = datetime.now(timezone.utc)
    keys = []
    for yr in (now.year - 2, now.year - 1, now.year):
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
        if not t or c is None or c <= 0 or t not in keep:
            continue
        out.append((t, float(c), float(r.get("h") or c),
                    float(r.get("l") or c), float(r.get("v") or 0.0)))
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
                for t, c, h, lo, v in rows:
                    b = bars.get(t)
                    if b is None:
                        b = Bars()
                        bars[t] = b
                    b.d.append(idx)
                    b.c.append(c)
                    b.h.append(h)
                    b.l.append(lo)
                    b.v.append(v)
            log("bars %d/%d sessions, %d tickers" % (
                min(start + chunk, len(keys)), len(keys), len(bars)))
    return dates, bars


# ── market context (SPY) ─────────────────────────────────────────────────
def market_context(spy, dates, n_sessions):
    lb = P["lookback"]
    c = list(spy.c)
    d = list(spy.d)
    start = max(0, len(c) - lb)
    cl = c[start:]
    dl = d[start:]
    eps = []
    for pk, tr, dd, closed in dump_episodes(cl, P["dump_min_dd_pct"]):
        eps.append({"peak_idx": dl[pk], "trough_idx": dl[tr],
                    "peak_date": dates[dl[pk]], "trough_date": dates[dl[tr]],
                    "spy_dd_pct": round(dd, 2), "closed": closed,
                    "sessions": dl[tr] - dl[pk],
                    "big": dd <= P["big_dump_dd_pct"]})
    # daily returns keyed by session idx (only consecutive sessions)
    rets = {}
    for i in range(1, len(c)):
        if d[i] - d[i - 1] == 1:
            rets[d[i]] = c[i] / c[i - 1] - 1
    lb_rets = {k: v for k, v in rets.items() if k >= dl[0]}
    k = max(P["worst_day_min_n"], int(len(lb_rets) * P["worst_day_quantile"]))
    worst = sorted(lb_rets.items(), key=lambda kv: kv[1])[:k]
    worst_idx = {i for i, _ in worst}
    ema250 = ema_last(c, 250)
    ema200 = ema_last(c, 200)
    return {
        "episodes": eps, "rets": rets, "lb_rets": lb_rets,
        "worst_idx": worst_idx,
        "worst_days": [{"date": dates[i], "spy_ret_pct": round(r * 100, 2)}
                       for i, r in worst],
        "spy_close": c[-1], "spy_ema250": ema250, "spy_ema200": ema200,
        "spy_vs_ema250_pct": (rnd((c[-1] / ema250 - 1) * 100)
                              if ema250 else None),
        "spy_ret_1m_pct": rnd((c[-1] / c[-22] - 1) * 100) if len(c) > 22 else None,
        "spy_ret_3m_pct": rnd((c[-1] / c[-64] - 1) * 100) if len(c) > 64 else None,
        "spy_dd_from_high_pct": rnd((c[-1] / max(cl) - 1) * 100),
        "lookback_start": dates[dl[0]], "lookback_sessions": len(cl),
        "n_sessions_loaded": n_sessions,
    }


# ── per-ticker price signals ─────────────────────────────────────────────
def price_signals(b, mkt, etf_bars, dates):
    n = len(b.c)
    c = list(b.c)
    h = list(b.h)
    lo = list(b.l)
    v = list(b.v)
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
    # ---- realized vol
    lr = [math.log(c[i] / c[i - 1]) for i in range(1, n) if c[i - 1] > 0]
    v20 = std(lr[-20:]) if len(lr) >= 20 else None
    v100 = std(lr[-100:]) if len(lr) >= 100 else None
    out["vol_20d_pct"] = v20 * math.sqrt(252) * 100 if v20 else None
    out["vol_100d_pct"] = v100 * math.sqrt(252) * 100 if v100 else None
    out["vol_contraction"] = (v20 / v100) if (v20 and v100) else None
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
    # breakout status
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
    # ---- dump resilience vs SPY
    rets = mkt["rets"]
    lb_rets = mkt["lb_rets"]
    pos_by_idx = {}
    for p in range(n):
        pos_by_idx[d[p]] = p
    pairs_s = []
    pairs_m = []
    worst_rows = []
    for idx, mr in lb_rets.items():
        p = pos_by_idx.get(idx)
        if p is None or p == 0 or d[p - 1] != idx - 1:
            continue
        sr = c[p] / c[p - 1] - 1
        pairs_s.append(sr)
        pairs_m.append(mr)
        if idx in mkt["worst_idx"]:
            worst_rows.append((idx, sr, mr))
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
        if len(pairs_s) >= 20:
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
        ex = [(s_ - m_) * 1e4 for _, s_, m_ in worst_rows]
        out["worst_days_n"] = len(worst_rows)
        out["worst_days_excess_bps"] = mean(ex)
        out["worst_days_green_rate"] = sum(1 for _, s_, _ in worst_rows if s_ >= 0) / len(worst_rows)
        out["worst_days_held_rate"] = sum(1 for _, s_, m_ in worst_rows if s_ >= 0.25 * m_) / len(worst_rows)
        out["worst_days_mean_ret_pct"] = mean([s_ for _, s_, _ in worst_rows]) * 100
    else:
        out["worst_days_n"] = len(worst_rows)
        for k in ("worst_days_excess_bps", "worst_days_green_rate",
                  "worst_days_held_rate", "worst_days_mean_ret_pct"):
            out[k] = None
    # episodes
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
        ep_rows.append({"peak_date": e["peak_date"], "trough_date": e["trough_date"],
                        "spy_pct": e["spy_dd_pct"], "stock_pct": round(sr, 2),
                        "capture": (round(cap, 2) if cap is not None else None),
                        "big": e["big"], "closed": e["closed"]})
    out["episodes"] = ep_rows
    out["n_episodes"] = len(ep_rows)
    caps = [x["capture"] for x in ep_rows if x["capture"] is not None]
    out["capture_median"] = median(caps)
    out["capture_mean"] = mean(caps)
    out["capture_worst"] = max(caps) if caps else None
    wsum = sum(abs(x["spy_pct"]) for x in ep_rows if x["capture"] is not None)
    out["capture_weighted"] = (sum(x["capture"] * abs(x["spy_pct"]) for x in ep_rows if x["capture"] is not None) / wsum
                               if wsum else None)
    if caps:
        wx = max(ep_rows, key=lambda x: (x["capture"] if x["capture"] is not None else -9))
        out["worst_episode"] = "%s..%s SPY %.1f%% stock %+.1f%%" % (wx["peak_date"], wx["trough_date"], wx["spy_pct"], wx["stock_pct"])
    else:
        out["worst_episode"] = None
    out["flat_or_up_share"] = (sum(1 for x in ep_rows if x["stock_pct"] >= 0) / len(ep_rows)) if ep_rows else None
    out["barely_dipped_share"] = (sum(1 for x in caps if x <= P["capture_barely_dipped"]) / len(caps)) if caps else None
    big = [x["capture"] for x in ep_rows if x["big"] and x["capture"] is not None]
    out["capture_big_dumps"] = median(big)
    # dump_capture: the one number -- episode-based when available, else day-based
    if caps:
        out["dump_capture"] = out["capture_weighted"]
        out["dump_capture_basis"] = "episodes"
    elif out.get("down_capture_pct") is not None:
        out["dump_capture"] = out["down_capture_pct"] / 100.0
        out["dump_capture_basis"] = "down_days"
    else:
        out["dump_capture"] = None
        out["dump_capture_basis"] = None
    # ---- RS vs industry ETF (63d) is attached by caller (needs ETF bars)
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
    sc = (sum(w * v for w, v in parts) / sum(w for w, _ in parts)) if parts else None
    risks = []
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
            "altman_z": altman, "piotroski_f": pio, "beneish_m": ben,
            "interest_coverage": icov, "netdebt_to_ebitda": nd_e, "debt_to_equity": debt_eq,
            "current_ratio": cur, "p_fcf": pfcf, "dilution_yoy_pct": dil,
            "net_buyback_yield_pct": bb_y, "short_float_pct": sf, "earnings_in_days": ed,
            "risks": risks}


def build_stock_rows(bars, dates, mkt, F, etf_bars):
    today = datetime.now(timezone.utc).date()
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
        ps = price_signals(b, mkt, etf_bars, dates)
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
    ep_ok = (cap is not None and cap <= P["capture_barely_dipped"]
             and (worst is None or worst <= P["capture_worst_max"])
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


def composite_and_asymmetry(r):
    pillars = {
        "resilience": resilience_score(r), "coil": coil_score(r), "location": location_score(r),
        "valuation": r.get("valuation_score"), "growth": r.get("growth_score"),
        "flows": r.get("flow_score"), "momentum": momentum_score(r),
        "backlog_contracts": r.get("backlog_contracts_score"), "safety": r.get("safety_score"),
    }
    num = 0.0
    den = 0.0
    for k, w in WEIGHTS.items():
        v = pillars.get(k)
        if v is not None:
            num += w * clamp(v)
            den += w
    comp = (num / den) if den >= 60 else None
    cov = round(den / sum(WEIGHTS.values()), 2)
    r["pillars"] = {k: rnd(v, 1) for k, v in pillars.items()}
    r["pillar_coverage"] = cov
    r["composite"] = rnd(comp, 1)
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
        cov_ = r.get("nlav_coverage")
        if cov_ is not None and cov_ >= 0.5:
            downside *= 0.7
    else:
        downside = None
    r["upside_room_pct"] = rnd(upside, 1)
    r["dump_downside_pct"] = rnd(downside, 1)
    r["asymmetry"] = rnd(min(25.0, upside / downside), 2) if (upside is not None and downside) else None


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
    return R[:9]


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
}


# ── handler ──────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    global T0
    T0 = time.time()
    LOG.clear()
    event = event or {}
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
    for r in stock_rows:
        gates_and_tier(r)
        composite_and_asymmetry(r)
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
                                      ("INSIDER_BUYING", bool(r.get("insider_buys_30d"))),
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
                   "safety_score", "ret_3m_pct", "flags"]
    ledger = [{k: (round(r[k], 3) if isinstance(r.get(k), float) else r.get(k)) for k in LEDGER_KEYS}
              for r in stock_rows if r["gates_passed"] >= 2 and r["tier"] == "SCREENED"][:2500]
    top_picks = [{"ticker": r["ticker"], "score": r.get("composite"), "tier": r["tier"],
                  "asymmetry": r.get("asymmetry"), "dump_capture": rnd(r.get("dump_capture"), 2),
                  "vs_ema250_pct": rnd(r.get("vs_ema250_pct"), 1),
                  "bb_width_pctile": rnd(r.get("bb_width_pctile"), 0),
                  "industry_etf": r.get("industry_etf"), "reasons": r["reasons"][:4]}
                 for r in stock_rows if r["tier"] in ("FORTRESS_COIL", "COILED") and r.get("composite") is not None][:60]
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
                     "industry, growing, backed by industry inflows and an order book. Every leg is real "
                     "data or an honest None. Research shorthand, not advice."),
        "params": P, "weights": WEIGHTS, "gate_names": GATE_NAMES,
        "market": {k: v for k, v in mkt.items() if k not in ("rets", "lb_rets", "worst_idx")},
        "breadth": breadth,
        "funnel": funnel, "tiers": tiers, "n_scored": len(stock_rows),
        "n_universe_bars": len(bars),
        "top_picks": top_picks, "board": board, "ledger": ledger,
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
