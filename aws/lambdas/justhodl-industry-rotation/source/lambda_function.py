"""justhodl-industry-rotation -- Market regime -> industry-ETF leadership
-> strongest stocks inside the strongest ETFs.

Doctrine (Khalid, 2026-07-07): industry momentum subsumes most individual
stock momentum (Moskowitz-Grinblatt 1999; AQR industry-momentum work).
Find the strongest army first, then the strongest soldiers. The premium
signal is DIVERGENCE UNDER WEAKNESS: SPY below its short MAs while an
industry ETF holds its 50/100/200 ladder with a RISING ETF/SPY ratio --
that is institutional absorption, not luck.

Layers (all real data, zero LLM):
  1. Market regime: SPY vs SMA20/50 -> STRONG / NEUTRAL / WEAK, enriched
     with risk-regime score (failure-isolated).
  2. 33-ETF ladder (11 SPDR sectors + 22 industries): SMA50/100/200
     state, ETF/SPY ratio vs its own 50d MA, 20d ratio slope, 3m
     relative-momentum percentile -> leadership_score 0-100.
  3. Tags: ABSORPTION (weak tape, strong ladder, rising ratio),
     BREAKDOWN (strong tape, broken ladder, falling ratio).
  4. Self-accruing score history -> rank_delta_20d (honest WARMING
     until 21 sessions accrue).
  5. Soldiers: top-5 leader ETFs get holdings via FMP /stable/etf-holder
     (graceful skip) + resilient-name join from data/resilience.json.
  6. by_sector_name map so best-setups can join on harvested sector
     strings.

Out: data/industry-rotation.json (CacheControl 900s).
"""
import json
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

S3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/industry-rotation.json"

import os
POLY = (os.environ.get("POLYGON_API_KEY")
        or os.environ.get("POLYGON_KEY") or "")
FMP = (os.environ.get("FMP_API_KEY")
       or os.environ.get("FMP_KEY") or "")

SECTORS = {
    "XLK": "Technology", "XLF": "Financials", "XLE": "Energy",
    "XLV": "Health Care", "XLI": "Industrials",
    "XLY": "Consumer Discretionary", "XLP": "Consumer Staples",
    "XLU": "Utilities", "XLB": "Materials",
    "XLC": "Communication Services", "XLRE": "Real Estate"}
INDUSTRIES = {
    "SMH": ("Semiconductors", "Technology"),
    "IGV": ("Software", "Technology"),
    "FDN": ("Internet", "Communication Services"),
    "CIBR": ("Cybersecurity", "Technology"),
    "XBI": ("Biotech", "Health Care"),
    "IBB": ("Biotech Large", "Health Care"),
    "ITB": ("Homebuilders", "Consumer Discretionary"),
    "KRE": ("Regional Banks", "Financials"),
    "KBE": ("Banks", "Financials"),
    "XOP": ("Oil E&P", "Energy"),
    "OIH": ("Oil Services", "Energy"),
    "GDX": ("Gold Miners", "Materials"),
    "XME": ("Metals & Mining", "Materials"),
    "XRT": ("Retail", "Consumer Discretionary"),
    "IYT": ("Transports", "Industrials"),
    "ITA": ("Aerospace & Defense", "Industrials"),
    "JETS": ("Airlines", "Industrials"),
    "TAN": ("Solar", "Energy"),
    "URA": ("Uranium", "Energy"),
    "KWEB": ("China Internet", "Communication Services"),
    "LIT": ("Lithium & Battery", "Materials"),
    "PAVE": ("Infrastructure", "Industrials"),
    "XES": ("Oil Equipment & Svcs", "Energy"),
    "COPX": ("Copper Miners", "Materials"),
    "XAR": ("Aerospace & Defense Eq", "Industrials"),
    "GRID": ("Smart Grid / Electrification", "Utilities"),
    "WCLD": ("Cloud Software", "Technology"),
    "XTN": ("Transportation Eq", "Industrials"),
    "XHB": ("Homebuilders Broad", "Consumer Discretionary")}
UNIVERSE = list(SECTORS) + list(INDUSTRIES)
# v3.2: Invesco equal-weight twins (post-2023 tickers) for
# narrowness detection -- cap-weight rising while equal-weight lags
# = generals without soldiers.
EW_TWINS = {"XLK": "RSPT", "XLF": "RSPF", "XLV": "RSPH",
            "XLY": "RSPD", "XLP": "RSPS", "XLE": "RSPG",
            "XLI": "RSPN", "XLB": "RSPM", "XLRE": "RSPR",
            "XLU": "RSPU", "XLC": "RSPC"}


def _http(url, timeout=25, tries=2):
    for a in range(tries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-industry-rotation"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except Exception:
            time.sleep(1.0 + a)
    return None


def polygon_daily(tkr, days=520):
    to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    frm = (datetime.now(timezone.utc) - timedelta(days=days)
           ).strftime("%Y-%m-%d")
    d = _http("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/"
              "%s/%s?adjusted=true&sort=asc&limit=5000&apiKey=%s"
              % (tkr, frm, to, POLY))
    return [float(b["c"]) for b in (d or {}).get("results") or []]


def s3_json(key):
    try:
        return json.loads(
            S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def sma(v, n):
    return sum(v[-n:]) / n if len(v) >= n else None


def _roll_z(series, i, n):
    w = series[max(0, i - n + 1):i + 1]
    if len(w) < 20:
        return 0.0
    m = sum(w) / len(w)
    sd = (sum((v - m) ** 2 for v in w) / len(w)) ** 0.5 or 1e-9
    return max(-3.5, min(3.5, (series[i] - m) / sd))


def rrg_of(closes, spy, trail_pts=12, step=5):
    """JdK-style RRG approximation (open formulations per StockCharts/
    TradingView community implementations): RS-Ratio = 100 + z of the
    smoothed ETF/SPY ratio vs its 63d base (trend of relative
    performance); RS-Momentum = 100 + z of the 10d ROC of RS-Ratio
    (leading turn detector). Trail sampled every `step` sessions."""
    n = min(len(closes), len(spy))
    if n < 160:
        return None
    ratio = [closes[-n + i] / spy[-n + i] for i in range(n)
             if spy[-n + i]]
    m = len(ratio)
    sm = [sum(ratio[max(0, i - 9):i + 1]) / min(10, i + 1)
          for i in range(m)]
    base = [sum(sm[max(0, i - 62):i + 1]) / min(63, i + 1)
            for i in range(m)]
    rs = [100.0 * sm[i] / base[i] if base[i] else 100.0
          for i in range(m)]
    rsr = [100.0 + 1.5 * _roll_z(rs, i, 126) for i in range(m)]
    roc = [rsr[i] - rsr[i - 10] if i >= 10 else 0.0 for i in range(m)]
    rsm = [100.0 + 1.5 * _roll_z(roc, i, 126) for i in range(m)]

    def quad(x, y):
        if x >= 100 and y >= 100:
            return "LEADING"
        if x >= 100:
            return "WEAKENING"
        if y >= 100:
            return "IMPROVING"
        return "LAGGING"
    trail = [[round(rsr[i], 2), round(rsm[i], 2)]
             for i in range(m - 1 - step * (trail_pts - 1), m, step)
             if 0 <= i < m]
    x, y = round(rsr[-1], 2), round(rsm[-1], 2)
    return {"x": x, "y": y, "quadrant": quad(x, y), "trail": trail}


def pct_rank(x, pop):
    if x is None or not pop:
        return None
    return round(100.0 * sum(1 for p in pop if p <= x) / len(pop), 1)


def beta_to(etf, spy, n=252):
    m = min(len(etf), len(spy), n + 1)
    if m < 130:
        return None
    re = [etf[-m + i + 1] / etf[-m + i] - 1 for i in range(m - 1)
          if etf[-m + i]]
    rs = [spy[-m + i + 1] / spy[-m + i] - 1 for i in range(m - 1)
          if spy[-m + i]]
    k = min(len(re), len(rs))
    re, rs = re[-k:], rs[-k:]
    mu_e, mu_s = sum(re) / k, sum(rs) / k
    var = sum((x - mu_s) ** 2 for x in rs)
    if var <= 0:
        return None
    return sum((a - mu_e) * (b - mu_s) for a, b in zip(re, rs)) / var


def _ret(c, a, b):
    """% return from index -a to index -b (a>b), None if short."""
    if len(c) < a or not c[-a]:
        return None
    return (c[-b] / c[-a] - 1.0) * 100


def alpha_horizons(etf, spy, b):
    """Jegadeesh-Titman skip-month + beta-adjusted (BAB separation):
    alpha_h = ETF_h - beta*SPY_h, horizons 3m raw, 6m and 12m each
    SKIPPING the most recent month (reversal noise)."""
    if b is None:
        b = 1.0
    out = {}
    for key, a, e in (("3m", 64, 1), ("6m_skip", 148, 22),
                      ("12m1_skip", 274, 22)):
        re_, rs_ = _ret(etf, a, e), _ret(spy, a, e)
        out[key] = (round(re_ - b * rs_, 2)
                    if None not in (re_, rs_) else None)
    w = {"3m": 0.5, "6m_skip": 0.3, "12m1_skip": 0.2}
    parts = [(w[k], v) for k, v in out.items() if v is not None]
    out["blend"] = (round(sum(wt * v for wt, v in parts)
                          / sum(wt for wt, _ in parts), 2)
                    if parts else None)
    return out


def dump_rebound(etf, spy, look=126):
    """The email's weakness + rebound tests, QUANTIFIED: mean excess
    return (bps/day) on SPY's worst-decile and best-decile days in the
    trailing window. Positive dump excess = refuses to break = hidden
    demand."""
    m = min(len(etf), len(spy), look + 1)
    if m < 60:
        return None, None
    re = [etf[-m + i + 1] / etf[-m + i] - 1 for i in range(m - 1)]
    rs = [spy[-m + i + 1] / spy[-m + i] - 1 for i in range(m - 1)]
    srt = sorted(rs)
    k = max(6, len(rs) // 10)
    lo, hi = srt[k - 1], srt[-k]
    dumps = [(a - b) for a, b in zip(re, rs) if b <= lo]
    pops = [(a - b) for a, b in zip(re, rs) if b >= hi]
    d = round(sum(dumps) / len(dumps) * 10000) if len(dumps) >= 6 else None
    r = round(sum(pops) / len(pops) * 10000) if len(pops) >= 6 else None
    return d, r


def crowded_flag(etf, spy):
    """De Bondt-Thaler long-horizon caution: 2y relative-strength line
    at its 95th+ percentile of its own range AND 20d ratio slope now
    negative -> extended + decelerating = CROWDED."""
    m = min(len(etf), len(spy))
    if m < 300:
        return False
    r = [etf[-m + i] / spy[-m + i] for i in range(m) if spy[-m + i]]
    cur = r[-1]
    peak = max(r[-21:])
    cur_p = 100.0 * sum(1 for x in r if x <= cur) / len(r)
    peak_p = 100.0 * sum(1 for x in r if x <= peak) / len(r)
    slope20 = (r[-1] / r[-21] - 1.0) if len(r) >= 21 and r[-21] else 0
    # peak at extreme highs within the month, still elevated now,
    # rolling over -- but NOT already broken (that's BREAKDOWN's job)
    return peak_p >= 95.0 and cur_p >= 85.0 and slope20 < 0


def regime_of(spy):
    px = spy[-1]
    s20, s50 = sma(spy, 20), sma(spy, 50)
    if s20 and s50:
        if px > s20 and px > s50:
            return "STRONG"
        if px < s20 and px < s50:
            return "WEAK"
    return "NEUTRAL"


def ladder_row(tkr, closes, spy, regime, blend_pop):
    px = closes[-1]
    s50, s100, s200 = sma(closes, 50), sma(closes, 100), sma(closes, 200)
    ab50 = bool(s50 and px > s50)
    ab100 = bool(s100 and px > s100)
    ab200 = bool(s200 and px > s200)
    # v4.1: MA distances + SMA50xSMA200 golden/death cross (why.html
    # industry join -- Khalid: show where the industry stands vs its
    # moving averages and whether a golden or death cross occurred).
    pct50 = round((px / s50 - 1) * 100, 2) if s50 else None
    pct200 = round((px / s200 - 1) * 100, 2) if s200 else None
    gx = dx = None
    if len(closes) >= 261:
        def _sma_at(i, n_):
            return sum(closes[i - n_ + 1:i + 1]) / n_
        last = len(closes) - 1
        for back in range(1, 61):
            p50, p200 = _sma_at(last - back, 50), _sma_at(last - back, 200)
            c50, c200 = _sma_at(last - back + 1, 50), _sma_at(last - back + 1, 200)
            if p50 <= p200 and c50 > c200:
                gx = back
                break
            if p50 >= p200 and c50 < c200:
                dx = back
                break
    # v3.7 technicals for the Cross Board
    def _ema_series(vals, n_):
        k = 2.0 / (n_ + 1)
        e = vals[0]
        out_e = [e]
        for v_ in vals[1:]:
            e = v_ * k + e * (1 - k)
            out_e.append(e)
        return out_e

    rsi14 = None
    if len(closes) >= 30:
        g_, l_ = 0.0, 0.0
        for i in range(-14, 0):
            d_ = closes[i] - closes[i - 1]
            g_ += max(d_, 0)
            l_ += max(-d_, 0)
        ag, al = g_ / 14, l_ / 14
        for i in range(-14, 0):
            pass
        rsi14 = round(100 - 100 / (1 + (ag / al if al else 99)), 1)
    macd_x = None
    if len(closes) >= 60:
        e12 = _ema_series(closes[-120:], 12)
        e26 = _ema_series(closes[-120:], 26)
        line = [a - b for a, b in zip(e12, e26)]
        sig = _ema_series(line, 9)
        hist_ = [a - b for a, b in zip(line, sig)]
        cross, cd = None, None
        for back in range(1, 6):
            if hist_[-back - 1] <= 0 < hist_[-back]:
                cross, cd = "UP", back
                break
            if hist_[-back - 1] >= 0 > hist_[-back]:
                cross, cd = "DOWN", back
                break
        macd_x = {"hist": round(hist_[-1], 3), "cross": cross,
                  "cross_days": cd}
    bb = None
    if len(closes) >= 150:
        widths = []
        for i in range(len(closes) - 130, len(closes)):
            w_ = closes[i - 19:i + 1]
            m_ = sum(w_) / 20
            sd_ = (sum((x - m_) ** 2 for x in w_) / 20) ** 0.5
            widths.append(4 * sd_ / m_ if m_ else 0)
        w20 = closes[-20:]
        mid = sum(w20) / 20
        sd = (sum((x - mid) ** 2 for x in w20) / 20) ** 0.5
        up_, lo_ = mid + 2 * sd, mid - 2 * sd
        pct_b = round((px - lo_) / (up_ - lo_), 2) if up_ > lo_ \
            else None
        wsort = sorted(widths)
        squeeze = widths[-1] <= wsort[max(0, len(wsort) // 5 - 1)]
        bb = {"pct_b": pct_b,
              "width_pct": round(widths[-1] * 100, 2),
              "squeeze": bool(squeeze),
              "at": ("UPPER" if pct_b is not None and pct_b >= 0.95
                     else "LOWER" if pct_b is not None
                     and pct_b <= 0.05 else "MID")}
    extremes = []
    for n_, tag_hi, tag_lo in ((5, "W-HI", "W-LO"),
                               (21, "M-HI", "M-LO"),
                               (252, "52W-HI", "52W-LO")):
        if len(closes) >= n_ + 1:
            hi_, lo_ = max(closes[-n_:]), min(closes[-n_:])
            if px >= hi_ * 0.9925:
                extremes.append(tag_hi)
            elif px <= lo_ * 1.0075:
                extremes.append(tag_lo)

    n = min(len(closes), len(spy))
    ratio = [closes[-n + i] / spy[-n + i] for i in range(n)
             if spy[-n + i]]
    r50 = sma(ratio, 50)
    ratio_above = bool(r50 and ratio[-1] > r50)
    slope = (round((ratio[-1] / ratio[-21] - 1.0) * 100, 2)
             if len(ratio) >= 21 and ratio[-21] else None)
    mom3 = (round((closes[-1] / closes[-64] - 1.0)
                  * 100, 2) if len(closes) >= 64 else None)
    rel3 = (round(mom3 - ((spy[-1] / spy[-64] - 1.0) * 100), 2)
            if mom3 is not None and len(spy) >= 64 else None)
    b = beta_to(closes, spy)
    ah = alpha_horizons(closes, spy, b)
    raw = alpha_horizons(closes, spy, 0.0)   # beta=0 => raw returns
    dump_x, reb_x = dump_rebound(closes, spy)
    crowded = crowded_flag(closes, spy)
    score = 0
    score += 20 if ab50 else 0
    score += 15 if ab100 else 0
    score += 15 if ab200 else 0
    score += 15 if ratio_above else 0
    score += 15 if (slope or 0) > 0 else 0
    pr = pct_rank(ah.get("blend"), blend_pop)
    score += round((pr or 0) / 5.0)          # 0-20
    score = min(100, score)
    ratio_3m_high = bool(len(ratio) >= 63
                         and ratio[-1] >= max(ratio[-63:]))
    if dump_x is not None and reb_x is not None:
        res_read = ("RESILIENT_LEADER" if dump_x > 0 and reb_x > 0
                    else "DEFENSIVE_ONLY" if dump_x > 0
                    else "HIGH_BETA_PROFILE" if reb_x > 0
                    else "WEAK_BOTH_WAYS")
    else:
        res_read = None
    tag = None
    if regime == "WEAK" and ab50 and (slope or 0) > 0 and score >= 65:
        tag = "ABSORPTION"
    elif regime != "WEAK" and not ab50 and (slope or 0) < 0 \
            and score <= 25:
        tag = "BREAKDOWN"
    name, parent = (SECTORS.get(tkr, ""), None) if tkr in SECTORS \
        else INDUSTRIES[tkr]
    return {"etf": tkr, "name": name or SECTORS.get(tkr),
            "kind": "SECTOR" if tkr in SECTORS else "INDUSTRY",
            "parent_sector": parent or SECTORS.get(tkr),
            "price": round(px, 2),
            "above_sma50": ab50, "above_sma100": ab100,
            "above_sma200": ab200,
            "above_sma20": bool(sma(closes, 20)
                                and px > sma(closes, 20)),
            "pct_vs_sma50": pct50, "pct_vs_sma200": pct200,
            "golden_cross_sessions_ago": gx,
            "death_cross_sessions_ago": dx,
            "rsi14": rsi14, "macd": macd_x, "bb": bb,
            "extremes": extremes,
            "ratio_above_50d": ratio_above,
            "ratio_slope_20d_pct": slope,
            "rel_mom_3m_pp": rel3, "rel_mom_pctile": pr,
            "beta_spy_1y": round(b, 2) if b is not None else None,
            "alpha_mom": ah, "raw_mom": raw,
            "dump_day_excess_bps": dump_x,
            "rebound_excess_bps": reb_x,
            "crowded": crowded,
            "ratio_3m_high": ratio_3m_high,
            "resilience_read": res_read,
            "leadership_score": score, "tag": tag}


def fmp_holdings(tkr):
    """Real ETF constituents (what the fund HOLDS), not etf-holder
    (legacy: who owns ETF SHARES -- backwards). /stable/etf/holdings is
    the current FMP endpoint; returns (holdings, reason) so failures
    are diagnosable, never silent."""
    if not FMP:
        return None, "no FMP key in env"
    d = _http("https://financialmodelingprep.com/stable/etf/holdings"
              "?symbol=%s&apikey=%s" % (tkr, FMP))
    if not isinstance(d, list) or not d:
        return None, ("etf/holdings empty/non-list for %s (plan-gated "
                      "or wrong symbol) -- resp head: %s"
                      % (tkr, json.dumps(d)[:120]))
    rows = sorted(
        (r for r in d if (r.get("asset") or r.get("symbol"))),
        key=lambda r: -(r.get("weightPercentage")
                        or r.get("weightPercent") or 0))[:12]
    out = [{"ticker": (r.get("asset") or r.get("symbol")),
           "weight_pct": round(r.get("weightPercentage")
                               or r.get("weightPercent") or 0, 2)}
          for r in rows]
    return (out, None) if out else (None, "zero usable rows for %s" % tkr)


def resilience_index(doc):
    """Ground-truth schema (read from justhodl-resilience source, not
    guessed): rows live under about_to_boom / all_resilient / top_picks,
    keyed by ticker, with fields resilience (0-100) + stage
    (ABSORBING/COILED/IGNITING). No sector field exists anywhere in the
    row -- so joining happens on TICKER MEMBERSHIP in real ETF holdings,
    never on a sector-name string match."""
    idx = {}
    if not doc:
        return idx
    for k in ("all_resilient", "top_picks", "about_to_boom"):
        for r in (doc.get(k) or []):
            t = r.get("ticker")
            if t and t not in idx:
                idx[t] = {"ticker": t, "stage": r.get("stage"),
                          "resilience": r.get("resilience")}
    return idx


def soldiers_of(holdings, res_idx):
    """Holdings of the leading ETF that ALSO show up on the resilience
    engine's watchlist -- genuinely idiosyncratically strong, not just
    riding the ETF's own beta."""
    if not holdings:
        return []
    hits = [dict(res_idx[h["ticker"]], weight_pct=h["weight_pct"])
            for h in holdings if h["ticker"] in res_idx]
    return hits[:8]


def polygon_daily_cv(tkr, days=560):
    """Closes + volumes (same aggs call carries both -- zero extra
    API cost for the Raschke volume layer)."""
    to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    frm = (datetime.now(timezone.utc) - timedelta(days=days)
           ).strftime("%Y-%m-%d")
    d = _http("https://api.polygon.io/v2/aggs/ticker/%s/range/1/day/"
              "%s/%s?adjusted=true&sort=asc&limit=5000&apiKey=%s"
              % (tkr, frm, to, POLY))
    bars = (d or {}).get("results") or []
    return ([float(b["c"]) for b in bars],
            [float(b.get("v") or 0) for b in bars],
            [datetime.fromtimestamp(b["t"] / 1000, tz=timezone.utc
                                    ).strftime("%Y-%m-%d")
             for b in bars])


def volume_metrics(closes, vols):
    """Email volume layer: up-vol vs down-vol (demand), recent volume
    vs 50d avg (breakout fuel), pullback volume dry-up (selling
    pressure weakening). Confirmation, not the main signal."""
    n = min(len(closes), len(vols))
    if n < 60:
        return None
    c, v = closes[-n:], vols[-n:]
    up = dn = 0.0
    for i in range(n - 20, n):
        if c[i] > c[i - 1]:
            up += v[i]
        elif c[i] < c[i - 1]:
            dn += v[i]
    updown = round(up / dn, 2) if dn > 0 else None
    v50 = sum(v[-50:]) / 50.0
    v5 = sum(v[-5:]) / 5.0
    vol_vs50 = round(100.0 * v5 / v50) if v50 > 0 else None
    dn_days = [v[i] for i in range(n - 15, n) if c[i] < c[i - 1]]
    dryup = (bool(sum(dn_days) / len(dn_days) < 0.9 * v50)
             if dn_days and v50 > 0 else None)
    return {"updown_vol_ratio_20d": updown,
            "vol_5d_vs_50d_pct": vol_vs50,
            "pullback_vol_dryup": dryup}


FIN_CACHE = {}


def fmp_scores(tkr):
    """Full /stable/financial-scores payload (Altman Z + Piotroski
    F-Score both live here) -- cached, one call per name per run."""
    if tkr in FIN_CACHE:
        return FIN_CACHE[tkr]
    if not FMP:
        return None
    d = _http("https://financialmodelingprep.com/stable/"
              "financial-scores?symbol=%s&apikey=%s" % (tkr, FMP))
    if isinstance(d, list) and d:
        d = d[0]
    if not isinstance(d, dict):
        FIN_CACHE[tkr] = None
        return None
    def _f(k):
        try:
            v = d.get(k)
            return float(v) if v is not None else None
        except Exception:
            return None
    out = {"z": _f("altmanZScore"),
           "piotroski": (int(_f("piotroskiScore"))
                         if _f("piotroskiScore") is not None
                         else None)}
    FIN_CACHE[tkr] = out
    return out


def fmp_growth(tkr):
    """Latest-FY growth legs for Khalid's confirm list: revenue,
    net income (margin direction), share count (dilution)."""
    d = _http("https://financialmodelingprep.com/stable/"
              "financial-growth?symbol=%s&limit=1&apikey=%s"
              % (tkr, FMP))
    if isinstance(d, list) and d:
        d = d[0]
    if not isinstance(d, dict):
        return None
    def _p(*keys):
        for k in keys:
            v = d.get(k)
            if v is not None:
                try:
                    return round(float(v) * 100, 1)
                except Exception:
                    pass
        return None
    return {"rev_g": _p("revenueGrowth"),
            "ni_g": _p("netIncomeGrowth"),
            "sh_g": _p("weightedAverageSharesGrowth",
                       "weightedAverageSharesDilutedGrowth")}


def fin_grade(sc, gr):
    """Financial-statement grade: Piotroski 45% + Altman zone 25% +
    revenue growth 10% + margin direction 10% + dilution 10%.
    Letter bands A+..F. Returns None when the statements are silent."""
    if not sc or sc.get("piotroski") is None:
        return None
    pts, mx = sc["piotroski"] * 5.0, 45.0
    z = sc.get("z")
    if z is not None:
        mx += 25
        pts += 25 if z > 2.99 else (12 if z >= 1.81 else 0)
    if gr:
        if gr.get("rev_g") is not None:
            mx += 10
            rg = gr["rev_g"]
            pts += 10 if rg > 15 else (7 if rg > 5 else
                                       (4 if rg > 0 else 0))
        if gr.get("ni_g") is not None and gr.get("rev_g") is not None:
            mx += 10
            pts += 10 if gr["ni_g"] >= gr["rev_g"] else \
                (5 if gr["ni_g"] > 0 else 0)
        if gr.get("sh_g") is not None:
            mx += 10
            sh = gr["sh_g"]
            pts += 10 if sh <= 0 else (6 if sh <= 2 else 0)
    score = round(pts * 100.0 / mx)
    for cut, letter in ((85, "A+"), (78, "A"), (72, "A-"),
                        (65, "B+"), (58, "B"), (52, "B-"),
                        (45, "C+"), (38, "C"), (30, "C-"),
                        (20, "D")):
        if score >= cut:
            return {"grade": letter, "score": score,
                    "f_score": sc["piotroski"], "z": z,
                    "z_zone": (None if z is None else
                               "SAFE" if z > 2.99 else
                               "GREY" if z >= 1.81 else "DISTRESS"),
                    **{k: (gr or {}).get(k)
                       for k in ("rev_g", "ni_g", "sh_g")}}
    return {"grade": "F", "score": score,
            "f_score": sc["piotroski"], "z": z,
            "z_zone": (None if z is None else
                       "SAFE" if z > 2.99 else
                       "GREY" if z >= 1.81 else "DISTRESS"),
            **{k: (gr or {}).get(k)
               for k in ("rev_g", "ni_g", "sh_g")}}


def industry_credit(etfs, holdings_by_etf, warns):
    """Khalid's CDS doctrine with honest free data: real single-name
    CDS is paywalled ($20K+/yr, per justhodl-cds-proxy), so the
    per-company credit proxy is Altman Z (the standard distress score
    already used fleet-wide). An industry is IN DANGER when many of
    its own companies screen distressed -- exactly the many-high-CDS
    rule, on the balance-sheet proxy. Z<1.8 distress, 1.8-3 grey.
    Financials excluded per-name (Altman is invalid for banks --
    standard practice; sector-level OAS from cds-proxy covers them).
    """
    out = {}
    fin_skip = {"XLF", "KRE", "KBE"}
    for etf in etfs:
        holds = holdings_by_etf.get(etf) or []
        if not holds:
            continue
        if etf in fin_skip:
            out[etf] = {"read": "N/A_FINANCIALS",
                        "note": "Altman invalid for banks; see "
                                "cds-proxy sector OAS"}
            continue
        zs = []
        for h in holds[:8]:
            try:
                _sc = fmp_scores(h["ticker"])
                z = _sc.get("z") if _sc else None
            except Exception:
                z = None
            if z is not None and -10 < z < 100:
                zs.append(z)
            time.sleep(0.1)
        if len(zs) < 4:
            out[etf] = {"read": "INSUFFICIENT",
                        "n_scored": len(zs)}
            continue
        zs.sort()
        med = zs[len(zs) // 2]
        distress = round(100.0 * sum(1 for z in zs if z < 1.8)
                         / len(zs))
        grey = round(100.0 * sum(1 for z in zs if 1.8 <= z < 3.0)
                     / len(zs))
        read = ("DANGER" if distress >= 30 or med < 1.8 else
                "WATCH" if distress >= 15 or med < 2.5 else "OK")
        out[etf] = {"n_scored": len(zs), "median_z": round(med, 2),
                    "distress_pct": distress, "grey_pct": grey,
                    "read": read}
    return out


def lambda_handler(event=None, context=None):
    warns = []
    spy = polygon_daily("SPY")
    if len(spy) < 210:
        raise RuntimeError("SPY history short: %d (poly_key=%s)"
                           % (len(spy), bool(POLY)))
    regime = regime_of(spy)
    rr = s3_json("data/risk-regime.json") or {}

    closes, volumes, bar_dates = {}, {}, {}
    for t in UNIVERSE:
        closes[t], volumes[t], bar_dates[t] = \
            polygon_daily_cv(t, days=1300)
        if len(closes[t]) < 210:
            warns.append("%s: %d bars" % (t, len(closes[t])))
        time.sleep(0.14)

    blend_pop = []
    for t, c in closes.items():
        if len(c) >= 148:
            v = alpha_horizons(c, spy, beta_to(c, spy)).get("blend")
            if v is not None:
                blend_pop.append(v)

    rows = [ladder_row(t, c, spy, regime, blend_pop)
            for t, c in closes.items() if len(c) >= 210]
    closes66 = {t: [round(x, 2) for x in c[-66:]]
                for t, c in closes.items() if len(c) >= 66}
    for r in rows:
        r["volume"] = volume_metrics(closes.get(r["etf"], []),
                                     volumes.get(r["etf"], []))

    # ── Raschke 100-pt scorecard (the email's exact weights):
    # abs-mom 25 / rel-RS 25 / MA 15 / resilience 15 / breadth 10 /
    # volume 5 / fundamental 5. breadth+fundamental need holdings
    # (measured in the contenders pass below); until then
    # basis=core85, honestly rescaled and labeled. ──
    raw_pop = [x["raw_mom"]["blend"] for x in rows
               if x["raw_mom"].get("blend") is not None]
    dump_pop = [x["dump_day_excess_bps"] for x in rows
                if x["dump_day_excess_bps"] is not None]
    reb_pop = [x["rebound_excess_bps"] for x in rows
               if x["rebound_excess_bps"] is not None]

    def _band(sc):
        return ("LEADERSHIP" if sc >= 80 else
                "STRONG_WATCH" if sc >= 65 else
                "NEUTRAL" if sc >= 50 else
                "WEAK" if sc >= 35 else "AVOID")

    for r in rows:
        abs_c = (pct_rank(r["raw_mom"].get("blend"), raw_pop)
                 or 50) / 100.0 * 25
        rel_c = ((r["rel_mom_pctile"] or 50) / 100.0 * 15
                 + (5 if r["ratio_above_50d"] else 0)
                 + (5 if r["ratio_3m_high"] else 0))
        ma_c = ((6 if r["above_sma50"] else 0)
                + (4 if r["above_sma100"] else 0)
                + (5 if r["above_sma200"] else 0))
        res_c = ((pct_rank(r["dump_day_excess_bps"], dump_pop)
                  or 50) / 100.0 * 8
                 + (pct_rank(r["rebound_excess_bps"], reb_pop)
                    or 50) / 100.0 * 7)
        vm = r.get("volume") or {}
        ud = vm.get("updown_vol_ratio_20d")
        vol_c = (5 if (ud or 0) >= 1.3 else
                 3 if (ud or 0) >= 1.1 else
                 1.5 if (ud or 0) >= 0.9 else
                 0 if ud is not None else None)
        core = abs_c + rel_c + ma_c + res_c + (vol_c or 0)
        basis = 85 if vol_c is not None else 80
        r["scorecard_components"] = {
            "abs_mom_25": round(abs_c, 1), "rel_rs_25": round(rel_c, 1),
            "ma_trend_15": ma_c, "resilience_15": round(res_c, 1),
            "volume_5": vol_c, "breadth_10": None,
            "fundamental_5": None}
        r["scorecard_100"] = round(core * 100.0 / basis, 1)
        r["scorecard_basis"] = "core%d" % basis
        r["scorecard_band"] = _band(r["scorecard_100"])
    rows.sort(key=lambda r: -(r.get("scorecard_100") or 0))
    rows.sort(key=lambda r: -r["leadership_score"])

    # self-accruing history -> 20d rank delta
    prev = s3_json(OUT_KEY) or {}
    hist = prev.get("score_history") or {}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist[today] = {r["etf"]: r["leadership_score"] for r in rows}
    hist = dict(sorted(hist.items())[-45:])
    dates = sorted(hist)
    rank_note = None
    if len(dates) >= 3:
        back = min(21, len(dates))
        old = hist[dates[-back]]
        old_rank = {e: i for i, (e, _) in enumerate(
            sorted(old.items(), key=lambda kv: -kv[1]))}
        for i, r in enumerate(rows):
            if r["etf"] in old_rank:
                r["rank_delta_20d"] = old_rank[r["etf"]] - i
                r["rank_delta_days"] = back - 1
        if back < 21:
            rank_note = ("ADAPTIVE: rank delta measured over the %d "
                         "sessions accrued so far; converges to 20d "
                         "at 21 sessions (%d/21)"
                         % (back - 1, len(dates)))
    else:
        rank_note = ("WARMING_UP: rank delta needs 3+ sessions "
                     "(%d/21 accrued)" % len(dates))

    def fmp_estimates(tkr):
        """Analyst estimates -> forward EPS (next FY) + 3y EPS CAGR.
        Basis for Khalid's forward-PEG framework."""
        d_ = _http("https://financialmodelingprep.com/stable/"
                   "analyst-estimates?symbol=%s&period=annual"
                   "&limit=4&apikey=%s" % (tkr, FMP))
        rows_ = [r_ for r_ in (d_ or [])
                 if r_.get("epsAvg") or r_.get("estimatedEpsAvg")]
        if not rows_:
            return None
        rows_.sort(key=lambda r_: r_.get("date") or "")
        today_s = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        fut = [r_ for r_ in rows_
               if (r_.get("date") or "") > today_s] or rows_
        eps = [(r_.get("epsAvg") or r_.get("estimatedEpsAvg"))
               for r_ in fut]
        fwd = eps[0]
        cagr = None
        if len(eps) >= 2 and eps[0] and eps[0] > 0 and eps[-1] \
                and eps[-1] > 0:
            yrs = len(eps) - 1
            cagr = ((eps[-1] / eps[0]) ** (1.0 / yrs) - 1) * 100
        return {"fwd_eps": fwd, "eps_cagr_pct":
                (round(cagr, 1) if cagr is not None else None)}

    _soldier_pe = {}

    def fmp_pe_ttm(tkr):
        d_ = _http("https://financialmodelingprep.com/stable/"
                   "ratios-ttm?symbol=%s&apikey=%s" % (tkr, FMP))
        row_ = (d_ or [{}])[0] if isinstance(d_, list) else (d_ or {})
        for k_ in ("priceToEarningsRatioTTM", "peRatioTTM",
                   "priceEarningsRatioTTM"):
            v_ = row_.get(k_)
            if v_ and 0 < v_ < 500:
                return round(v_, 1)
        return None

    _soldier_est = {}

    def fmp_quotes(tickers):
        """/stable/quote is single-symbol only (fleet precedent:
        13f-price-divergence, activist-13d -- the comma batch returns
        empty, 3085 lesson). Serial singles: price, priceAvg50/200,
        yearHigh/Low -- the raw legs for the soldier risk:reward."""
        out_q = {}
        for tk in tickers:
            d_ = _http("https://financialmodelingprep.com/stable/"
                       "quote?symbol=%s&apikey=%s" % (tk, FMP))
            if isinstance(d_, list) and d_ and d_[0].get("symbol"):
                out_q[d_[0]["symbol"]] = d_[0]
            time.sleep(0.04)
        return out_q

    # soldiers for the top-5 leaders
    _accj = s3_json("data/accumulation-radar.json") or {}
    _acc_set = {x.get("ticker") for x in
                ((_accj.get("accumulating") or {}).get("stocks")
                 or [])}
    _dist_set = {x.get("ticker") for x in
                 ((_accj.get("distributing") or {}).get("stocks")
                  or [])}
    _dpmap = {r0.get("ticker"): r0.get("state") for r0 in
              ((s3_json("data/dark-pool.json") or {}).get("board")
               or []) if r0.get("state") not in (None, "NEUTRAL")}
    _soldier_q = {}
    _prev_fin = {}
    for _l0 in (prev.get("leaders") or []):
        for _h0 in (_l0.get("holdings_top") or []):
            if _h0.get("ticker") and _h0.get("fin"):
                _prev_fin[_h0["ticker"]] = _h0["fin"]

    res_doc = s3_json("data/resilience.json")
    res_idx = resilience_index(res_doc)
    warns.append("resilience_index: %d tickers loaded" % len(res_idx))
    pead_pos = set()
    try:
        et = s3_json("data/earnings-tracker.json") or {}
        for r_ in (et.get("pead_signals") or []):
            if (r_.get("eps_surprise_pct") or 0) > 0 and r_.get("ticker"):
                pead_pos.add(r_["ticker"].upper())
    except Exception:
        pass
    hold_cache = {}

    def breadth_of(holds):
        """Hong-Stein diffusion / email step 5: % of the army's own
        soldiers above their 50d. BROAD>=60, NARROW<40 (one-megacap
        warning)."""
        if not holds:
            return None
        above = tot = 0
        for h in holds:
            t = h["ticker"]
            if t not in hold_cache:
                hold_cache[t] = polygon_daily(t, 220)
                time.sleep(0.12)
            c = hold_cache[t]
            s50 = sma(c, 50)
            if s50 and c:
                tot += 1
                above += 1 if c[-1] > s50 else 0
        if not tot:
            return None
        pct = round(100.0 * above / tot)
        return {"pct_above_50d": pct, "n_priced": tot,
                "read": ("BROAD" if pct >= 60
                         else "NARROW" if pct < 40 else "MIXED")}

    rev_pos = set(pead_pos)
    try:
        er = s3_json("data/estimate-revisions.json") or {}
        for r_ in (er.get("top_picks") or []):
            t_ = r_.get("ticker")
            if t_ and (r_.get("eps_rev_pct") or r_.get("score")
                       or 0) > 0:
                rev_pos.add(t_.upper())
    except Exception:
        pass

    def fundamental_confirm(holds):
        if not holds:
            return None
        n = min(8, len(holds))
        hits = sum(1 for h in holds[:n]
                   if h["ticker"].upper() in rev_pos)
        return round(100.0 * hits / n)

    leaders = []
    for r in rows[:5]:
        holds, reason = None, "not attempted"
        try:
            holds, reason = fmp_holdings(r["etf"])
        except Exception as e:
            reason = str(e)[:100]
        if holds is None:
            warns.append("holdings skip %s: %s" % (r["etf"], reason))
        sold = soldiers_of(holds, res_idx)
        etf_3m = _ret(closes.get(r["etf"], []), 64, 1)
        spy_3m = _ret(spy, 64, 1)
        for x in sold:
            x["pead"] = x["ticker"].upper() in pead_pos
            t = x["ticker"]
            if t not in hold_cache:
                hold_cache[t] = polygon_daily(t, 220)
                time.sleep(0.12)
            s3m = _ret(hold_cache[t], 64, 1)
            if None not in (s3m, etf_3m, spy_3m):
                x["vs_etf_3m_pp"] = round(s3m - etf_3m, 1)
                x["vs_spy_3m_pp"] = round(s3m - spy_3m, 1)
                x["chain_intact"] = bool(s3m > etf_3m > spy_3m)
        br = breadth_of(holds)
        fc = fundamental_confirm(holds)
        if br is not None and r.get("scorecard_100") is not None:
            comp = r["scorecard_components"]
            comp["breadth_10"] = round(br["pct_above_50d"] / 10.0, 1)
            comp["fundamental_5"] = (round(min(5.0, fc / 10.0), 1)
                                     if fc is not None else None)
            core = (comp["abs_mom_25"] + comp["rel_rs_25"]
                    + comp["ma_trend_15"] + comp["resilience_15"]
                    + (comp["volume_5"] or 0) + comp["breadth_10"]
                    + (comp["fundamental_5"] or 0))
            basis = (85 if comp["volume_5"] is not None else 80) \
                + 10 + (5 if comp["fundamental_5"] is not None else 0)
            r["scorecard_100"] = round(core * 100.0 / basis, 1)
            r["scorecard_basis"] = "full%d" % basis
            r["scorecard_band"] = _band(r["scorecard_100"])
        r["fundamental_confirm_pct"] = fc
        leaders.append(dict(
            r, holdings_top=holds,
            holdings_reason=(None if holds else reason),
            breadth=br, fundamental_confirm_pct=fc,
            resilient_names=sold))

    # ── industry credit-danger: leaders + breakdown cluster ──
    holdings_by_etf = {l["etf"]: l.get("holdings_top") for l in leaders}
    # v3.1: holdings for the WHOLE universe -- internal breadth needs
    # every army's soldier list, not just leaders + breakdowns.
    for r_ in rows:
        if r_["etf"] not in holdings_by_etf:
            try:
                h_, why_ = fmp_holdings(r_["etf"])
            except Exception as e:
                h_, why_ = None, str(e)[:80]
            if h_ is None:
                warns.append("holdings skip %s: %s"
                             % (r_["etf"], why_[:60]))
            holdings_by_etf[r_["etf"]] = h_
            time.sleep(0.12)

    # ── v3.1 INTERNAL BREADTH: % of top-25 holdings above their own
    # 50/200-DMA, from accumulation-radar's ma_state (per-name MA flags
    # across its 487-name universe; prior close). Practitioner
    # threshold: >=70% above 50d = healthy trend. ──
    _ma = dict((s3_json("data/accumulation-radar.json") or {}
                ).get("ma_state") or {})
    # gap-fill (3054 finding: 18/40 ETFs hold small-caps outside the
    # radar's 487-name universe): compute above-50/200 directly from
    # Polygon for uncovered holdings, capped at 12 fills per ETF.
    _fill_budget = 220
    for r_ in rows:
        if _fill_budget <= 0:
            break
        holds = (holdings_by_etf.get(r_["etf"]) or [])[:25]
        missing = [h["ticker"] for h in holds
                   if h.get("ticker") and h["ticker"] not in _ma][:12]
        need = 5 - sum(1 for h in holds
                       if h.get("ticker") in _ma)
        if need <= 0:
            continue
        for tk_ in missing:
            if _fill_budget <= 0:
                break
            try:
                c_ = polygon_daily(tk_, days=320)
                _fill_budget -= 1
                if len(c_) >= 200:
                    s50_, s200_ = sma(c_, 50), sma(c_, 200)
                    _ma[tk_] = [1 if s50_ and c_[-1] > s50_ else 0,
                                1 if s200_ and c_[-1] > s200_ else 0]
                time.sleep(0.12)
            except Exception:
                _fill_budget -= 1
    for r_ in rows:
        holds = (holdings_by_etf.get(r_["etf"]) or [])[:25]
        cov = [(h["ticker"], _ma[h["ticker"]]) for h in holds
               if h.get("ticker") in _ma]
        if len(cov) >= 5:
            r_["internal_breadth"] = {
                "pct_above_50d": round(100.0 * sum(v[0] for _, v in cov)
                                       / len(cov)),
                "pct_above_200d": round(100.0 * sum(v[1] for _, v in cov)
                                        / len(cov)),
                "n_covered": len(cov), "n_holdings": len(holds),
                "read": ("HEALTHY" if sum(v[0] for _, v in cov)
                         / len(cov) >= 0.70 else
                         "NARROW" if sum(v[0] for _, v in cov)
                         / len(cov) < 0.40 else "MIXED")}
        else:
            r_["internal_breadth"] = None

    # ── v3.1 RRG: quadrant map + trails + dated transitions ──
    rrg = {}
    for t, c in closes.items():
        if len(c) >= 210:
            g = rrg_of(c, spy)
            if g:
                rrg[t] = g
    prev_rrg = prev.get("rrg") or {}
    prev_trans = prev.get("rrg_transitions") or []
    today_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    new_trans = []
    for t, g in rrg.items():
        was = (prev_rrg.get(t) or {}).get("quadrant")
        if was and was != g["quadrant"]:
            new_trans.append({"etf": t, "date": today_iso,
                              "from": was, "to": g["quadrant"],
                              "x": g["x"], "y": g["y"],
                              "bullish": (was, g["quadrant"]) in
                              (("IMPROVING", "LEADING"),
                               ("LAGGING", "IMPROVING"),
                               ("WEAKENING", "LEADING"))})
    rrg_transitions = (new_trans + [t_ for t_ in prev_trans
                                    if t_.get("date") != today_iso]
                       )[:60]
    # closed loop: IMPROVING->LEADING is the canonical RRG buy signal;
    # graded forward vs SPY like every other engine.
    try:
        from decimal import Decimal
        nowt = datetime.now(timezone.utc)
        tbl = boto3.resource("dynamodb", "us-east-1").Table(
            "justhodl-signals")
        for tr in new_trans:
            if not (tr["from"] == "IMPROVING"
                    and tr["to"] == "LEADING"):
                continue
            px_ = closes.get(tr["etf"], [None])[-1]
            if not px_:
                continue
            tbl.put_item(Item={
                "signal_id": "irrrg-UP#%s#%s" % (tr["etf"], today_iso),
                "signal_type": "ir_rrg_improving_to_leading",
                "predicted_direction": "UP",
                "signal_value": str(tr["x"]),
                "confidence": Decimal("0.55"),
                "measure_against": "ticker_vs_benchmark",
                "baseline_price": str(round(px_, 2)),
                "benchmark": "SPY",
                "check_windows": ["day_5", "day_21", "day_63"],
                "outcomes": {}, "accuracy_scores": {},
                "status": "pending", "logged_at": nowt.isoformat(),
                "logged_epoch": int(nowt.timestamp()),
                "horizon_days_primary": 21, "schema_version": "2",
                "ttl": int(nowt.timestamp()) + 120 * 86400,
                "metadata": {"engine": "industry-rotation",
                             "v": "3.1", "transition": "IMP->LEAD"}})
    except Exception as e:
        warns.append("rrg loop-log: %s" % str(e)[:70])

    # ── v3.2 EQUAL-WEIGHT vs CAP-WEIGHT divergence (item 4) ──
    ew_cw = {}
    for cw, ew in EW_TWINS.items():
        try:
            ec = polygon_daily(ew, days=420)
            time.sleep(0.12)
        except Exception:
            ec = []
        cwc = closes.get(cw) or []
        n_ = min(len(ec), len(cwc))
        if n_ < 140:
            continue
        ratio_ = [ec[-n_ + i] / cwc[-n_ + i] for i in range(n_)
                  if cwc[-n_ + i]]
        base_ = sma(ratio_, 63)
        sl20 = (round((ratio_[-1] / ratio_[-21] - 1.0) * 100, 2)
                if len(ratio_) >= 21 and ratio_[-21] else None)
        read_ = ("BROAD" if base_ and ratio_[-1] >= base_ else
                 "NARROW" if sl20 is not None and sl20 < -1.0
                 else "THINNING")
        ew_cw[cw] = {"ew": ew, "ew_cw_20d_pct": sl20,
                     "above_63d_base": bool(base_
                                            and ratio_[-1] >= base_),
                     "read": read_}
    for r_ in rows:
        if r_["etf"] in ew_cw:
            r_["ew_cw"] = ew_cw[r_["etf"]]

    # ── v3.2 DATED MA-CROSS EVENTS (item 5) ──
    prev_ma_ev = prev.get("ma_events") or []
    new_ev = []
    for t, c in closes.items():
        if len(c) < 210:
            continue
        for n_, lbl in ((20, "20D"), (50, "50D"), (100, "100D"),
                        (200, "200D")):
            s_now = sma(c, n_)
            s_prev = sma(c[:-1], n_)
            if not (s_now and s_prev):
                continue
            if c[-2] <= s_prev and c[-1] > s_now:
                new_ev.append({"etf": t, "date": today_iso,
                               "event": "CROSS_ABOVE_%s" % lbl,
                               "bullish": True})
            elif c[-2] >= s_prev and c[-1] < s_now:
                new_ev.append({"etf": t, "date": today_iso,
                               "event": "CROSS_BELOW_%s" % lbl,
                               "bullish": False})
        g50n, g200n = sma(c, 50), sma(c, 200)
        g50p, g200p = sma(c[:-3], 50), sma(c[:-3], 200)
        if all((g50n, g200n, g50p, g200p)):
            if g50p <= g200p and g50n > g200n:
                new_ev.append({"etf": t, "date": today_iso,
                               "event": "GOLDEN_CROSS",
                               "bullish": True})
            elif g50p >= g200p and g50n < g200n:
                new_ev.append({"etf": t, "date": today_iso,
                               "event": "DEATH_CROSS",
                               "bullish": False})
    ma_events = (new_ev + [e for e in prev_ma_ev
                           if e.get("date") != today_iso])[:80]
    try:
        from decimal import Decimal
        nowt2 = datetime.now(timezone.utc)
        tbl2 = boto3.resource("dynamodb", "us-east-1").Table(
            "justhodl-signals")
        for ev in new_ev:
            if ev["event"] != "GOLDEN_CROSS":
                continue
            px2 = closes.get(ev["etf"], [None])[-1]
            if not px2:
                continue
            tbl2.put_item(Item={
                "signal_id": "irgc-UP#%s#%s" % (ev["etf"], today_iso),
                "signal_type": "ir_golden_cross",
                "predicted_direction": "UP",
                "signal_value": "1", "confidence": Decimal("0.55"),
                "measure_against": "ticker_vs_benchmark",
                "baseline_price": str(round(px2, 2)),
                "benchmark": "SPY",
                "check_windows": ["day_5", "day_21", "day_63"],
                "outcomes": {}, "accuracy_scores": {},
                "status": "pending", "logged_at": nowt2.isoformat(),
                "logged_epoch": int(nowt2.timestamp()),
                "horizon_days_primary": 63, "schema_version": "2",
                "ttl": int(nowt2.timestamp()) + 150 * 86400,
                "metadata": {"engine": "industry-rotation",
                             "v": "3.2", "event": "GOLDEN_CROSS"}})
    except Exception as e:
        warns.append("ma-event loop-log: %s" % str(e)[:70])

    # ══ v3.3 FLEET JOINS (items 6-11 of Khalid's 17) ══
    def _g(o, *ks, default=None):
        for k in ks:
            if not isinstance(o, dict):
                return default
            o = o.get(k)
        return o if o is not None else default

    # (8) Wyckoff dated phase per ETF (phase-detector top-400 map)
    _pdj = s3_json("data/phase-detector.json") or {}
    _ph = {t: {"phase": v.get("p"), "begin": v.get("b")}
           for t, v in (_pdj.get("phases_all") or {}).items()}
    if not _ph:
        _ph = _pdj.get("tickers") or {}
    # (7) smart-money: whales $, dark-pool state, capital-flow,
    #     options-confluence posture
    _wh = (s3_json("data/whales.json") or {}).get("stocks") or {}
    _dp = {r0.get("ticker"): r0.get("state") for r0 in
           ((s3_json("data/dark-pool.json") or {}).get("board") or [])
           if r0.get("ticker")}
    _cf = s3_json("data/capital-flow.json") or {}
    _cft = {}
    for side in ("accumulating", "distributing"):
        for r0 in (_cf.get(side) or []):
            t0 = r0.get("ticker") or r0.get("etf")
            if t0:
                _cft[t0] = side.upper()
    _oc = {r0.get("ticker"): r0.get("posture") or r0.get("state")
           for r0 in ((s3_json("data/options-confluence.json") or {}
                       ).get("multi_engine_confluence") or [])
           if r0.get("ticker")}
    # (10) revision-breadth positive set
    _er = {r0.get("ticker") for r0 in
           ((s3_json("data/estimate-revisions.json") or {}
             ).get("top_picks") or []) if r0.get("ticker")}
    join_hits = {"wyckoff": 0, "whales": 0, "dark_pool": 0,
                 "capital_flow": 0, "options": 0, "rev_hits": 0}
    for r_ in rows:
        t = r_["etf"]
        sm = {}
        if t in _ph and _ph[t].get("phase") not in (None, "NEUTRAL"):
            r_["wyckoff"] = {"phase": _ph[t]["phase"],
                             "begin": _ph[t].get("begin")}
            join_hits["wyckoff"] += 1
        if t in _wh and _wh[t].get("conviction_flow_usd"):
            sm["whale_usd"] = _wh[t]["conviction_flow_usd"]
            join_hits["whales"] += 1
        if _dp.get(t) and _dp[t] != "NEUTRAL":
            sm["dark_pool"] = _dp[t]
            join_hits["dark_pool"] += 1
        if t in _cft:
            sm["capital_flow"] = _cft[t]
            join_hits["capital_flow"] += 1
        if _oc.get(t):
            sm["options"] = _oc[t]
            join_hits["options"] += 1
        if sm:
            r_["smart_money"] = sm
        holds = (holdings_by_etf.get(t) or [])[:25]
        _soldier_q.update(fmp_quotes(
            [h0["ticker"] for h0 in holds if h0.get("ticker")
             and h0["ticker"] not in _soldier_q]))
        hits = [h["ticker"] for h in holds
                if h.get("ticker") in _er]
        # v3.5: fleet chips on every soldier (phase/whale/ER+)
        for h in holds:
            ht = h.get("ticker")
            if not ht:
                continue
            php = _ph.get(ht) or {}
            if php.get("phase") and php["phase"] != "NEUTRAL":
                h["phase"] = php["phase"]
                h["phase_begin"] = php.get("begin")
            whu = (_wh.get(ht) or {}).get("conviction_flow_usd")
            if whu and abs(whu) >= 25e6:
                h["whale_musd"] = round(whu / 1e6)
            if ht in _er:
                h["er_plus"] = True
            if ht in _acc_set:
                h["acc_state"] = "ACCUMULATION"
            elif ht in _dist_set:
                h["acc_state"] = "DISTRIBUTION"
            if _dpmap.get(ht):
                h["dp"] = _dpmap[ht]
            q_ = _soldier_q.get(ht)
            if q_ and q_.get("price"):
                px = q_["price"]
                up = (q_.get("yearHigh") or 0) / px - 1
                stop = None
                for cand in (q_.get("priceAvg50"),
                             q_.get("priceAvg200"),
                             q_.get("yearLow")):
                    if cand and cand < px:
                        stop = cand
                        break
                if ht not in _soldier_pe:
                    _soldier_pe[ht] = fmp_pe_ttm(ht)
                pe_t = _soldier_pe.get(ht)
                t_eps = (px / pe_t) if pe_t else None
                if pe_t:
                    h["pe"] = pe_t
                if ht not in _soldier_est:
                    _soldier_est[ht] = fmp_estimates(ht)
                est = _soldier_est.get(ht)
                if est and est.get("fwd_eps") and est["fwd_eps"] > 0:
                    fwd_eps = est["fwd_eps"]
                    basis = "estimates"
                    g = est.get("eps_cagr_pct")
                    # ADR currency guard (TSM lesson, ops 3090):
                    # FMP estimates for foreign filers come back in
                    # LOCAL currency (TSM ~368 TWD vs $11.5 USD ADR
                    # EPS -> fPE 0.9). Level is unusable, but the
                    # GROWTH RATIO is currency-invariant -- so
                    # normalize: fwd = trailing_usd * (1 + growth).
                    if t_eps and t_eps > 0 and \
                            not (0.2 <= fwd_eps / t_eps <= 5.0):
                        if g and g > -60:
                            fwd_eps = t_eps * (1 + g / 100.0)
                            basis = "normalized"
                        else:
                            fwd_eps = None
                    if fwd_eps and fwd_eps > 0:
                        fpe = px / fwd_eps
                        if 3 <= fpe <= 150:
                            h["fwd_pe"] = round(fpe, 1)
                            h["fwd_pe_basis"] = basis
                            if g and g > 0:
                                h["eps_cagr_pct"] = g
                                peg = fpe / g
                                if 0.1 <= peg <= 10:
                                    h["peg_fwd"] = round(peg, 2)
                if stop and up > 0:
                    dn = px / stop - 1
                    dn = max(dn, 0.01)
                    h["rr"] = {"up_pct": round(up * 100, 1),
                               "down_pct": round(dn * 100, 1),
                               "ratio": round(up / dn, 1),
                               "stop_basis": ("50DMA" if stop ==
                                              q_.get("priceAvg50")
                                              else "200DMA" if stop ==
                                              q_.get("priceAvg200")
                                              else "52w low")}
            # v4.0 financial-statement grade (7-day cache via prev)
            pf = _prev_fin.get(ht)
            fresh_ok = False
            if pf and pf.get("as_of"):
                try:
                    _age = (datetime.now(timezone.utc)
                            - datetime.fromisoformat(
                                pf["as_of"] + "T00:00:00+00:00")
                            ).days
                    fresh_ok = _age <= 7
                except Exception:
                    pass
            if fresh_ok:
                h["fin"] = pf
            else:
                fg = fin_grade(fmp_scores(ht), fmp_growth(ht))
                if fg:
                    fg["as_of"] = datetime.now(
                        timezone.utc).strftime("%Y-%m-%d")
                    h["fin"] = fg
        if holds:
            r_["rev_plus_hits"] = {"n": len(hits),
                                   "of": len(holds),
                                   "names": hits[:5]}
            join_hits["rev_hits"] += 1 if hits else 0

    # (6) Stovall cycle conditioning via cycle-clock
    _cc = s3_json("data/cycle-clock.json") or {}
    phase_txt = str(_g(_cc, "phase") or _g(_cc, "cycle",
                   "phase") or _g(_cc, "verdict")
                    or "").upper()
    stovall = {"EARLY": ["XLY", "XLF", "XLI", "XLB", "XRT", "ITB",
                         "XHB", "KRE"],
               "MID": ["XLK", "XLC", "XLI", "SMH", "IGV", "FDN"],
               "LATE": ["XLE", "XLB", "XLP", "XLV", "XLU", "XES",
                        "OIH", "XOP", "XME"],
               "RECESSION": ["XLP", "XLV", "XLU"]}
    key = ("RECESSION" if "RECESS" in phase_txt or "CONTRACT"
           in phase_txt else "LATE" if "LATE" in phase_txt else
           "EARLY" if "EARLY" in phase_txt else
           "MID" if "MID" in phase_txt else None)
    cycle_context = None
    if key:
        exp = stovall[key]
        top8 = [r_["etf"] for r_ in sorted(
            rows, key=lambda x: -x["leadership_score"])[:8]]
        cycle_context = {
            "phase_raw": phase_txt[:80], "phase_bucket": key,
            "expected_leaders": exp,
            "actual_top8": top8,
            "aligned": [t for t in top8 if t in exp],
            "anomalies": [t for t in top8 if t not in exp
                          and t not in ("XLV", "XLP", "XLU")]}

    # (9) XLY/XLP risk-appetite strip (+ factor-regime z)
    risk_appetite = None
    xly, xlp = closes.get("XLY") or [], closes.get("XLP") or []
    n_ = min(len(xly), len(xlp))
    if n_ >= 140:
        ra = [xly[-n_ + i] / xlp[-n_ + i] for i in range(n_)
              if xlp[-n_ + i]]
        ma126 = sma(ra, 126)
        fr_z = _g(s3_json("data/factor-regime.json") or {},
                  "risk_appetite", "z")
        risk_appetite = {
            "xly_xlp": round(ra[-1], 4),
            "vs_126d_ma_pct": (round((ra[-1] / ma126 - 1) * 100, 2)
                               if ma126 else None),
            "slope_20d_pct": (round((ra[-1] / ra[-21] - 1) * 100, 2)
                              if len(ra) >= 21 else None),
            "read": ("RISK_ON" if ma126 and ra[-1] > ma126 else
                     "RISK_OFF"),
            "factor_regime_z": fr_z,
            "spark": [round(v, 4) for v in ra[-126::5]]}

    # (11) APAC lead-lag chips: read apac-leadlag's proven pairs
    # directly (3057 lesson: heuristic walker missed the schema --
    # pairs[].best{horizon,r,n} with names like "Taiwan semis -> SMH")
    _ap = s3_json("data/apac-leadlag.json") or \
        s3_json("data/apac.json") or {}
    apac_chips = {}
    for pr in (_ap.get("pairs") or []):
        best = pr.get("best") or {}
        nm = pr.get("name") or ""
        r_val = best.get("r")
        if r_val is None:
            continue
        for t in ("SMH", "SOXX"):
            if nm.endswith(t) or (" %s" % t) in nm:
                apac_chips.setdefault(t, []).append(
                    {"src": nm, "r": r_val,
                     "lead_days": best.get("horizon"),
                     "note": "follow-through" if r_val > 0
                     else "contrarian"})
    for r_ in rows:
        if r_["etf"] in apac_chips:
            r_["apac"] = apac_chips[r_["etf"]][:2]

    # ══ v3.4 (items 12-17) ══
    from datetime import date as _date
    cur_month = datetime.now(timezone.utc).month
    ranked_now = sorted(rows, key=lambda x: -x["leadership_score"])
    rank_now = {r_["etf"]: i for i, r_ in enumerate(ranked_now)}
    q_cut = max(1, len(rows) // 5)
    for r_ in rows:
        t = r_["etf"]
        c = closes.get(t) or []
        # (12) ratio sparkline vs SPY, 126d sampled every 5
        n_ = min(len(c), len(spy))
        if n_ >= 130:
            ra_ = [c[-n_ + i] / spy[-n_ + i] for i in range(n_)
                   if spy[-n_ + i]]
            r_["ratio_spark"] = [round(v / ra_[-126], 4)
                                 for v in ra_[-126::5]]
        # (13) vol-adjusted momentum (63d return / 63d vol)
        if len(c) >= 65:
            rets = [c[i] / c[i - 1] - 1 for i in
                    range(len(c) - 63, len(c))]
            mu = sum(rets) / len(rets)
            sd = (sum((x - mu) ** 2 for x in rets)
                  / len(rets)) ** 0.5 or 1e-9
            r_["sharpe_mom_63d"] = round(mu / sd * (252 ** 0.5), 2)
        # (15) seasonality: this month's hit rate over available years
        dts = bar_dates.get(t) or []
        if len(dts) == len(c) and len(c) > 300:
            by_ym = {}
            for i in range(len(c)):
                ym = dts[i][:7]
                by_ym.setdefault(ym, []).append(c[i])
            mrets = []
            yms = sorted(by_ym)
            for j in range(1, len(yms)):
                if int(yms[j][5:7]) == cur_month:
                    p0 = by_ym[yms[j - 1]][-1]
                    p1 = by_ym[yms[j]][-1]
                    if p0:
                        mrets.append(p1 / p0 - 1)
            if len(mrets) >= 3:
                r_["seasonality"] = {
                    "month": cur_month,
                    "hit_pct": round(100.0 * sum(1 for x in mrets
                                                 if x > 0)
                                     / len(mrets)),
                    "avg_pct": round(100.0 * sum(mrets)
                                     / len(mrets), 2),
                    "n_years": len(mrets)}
        # (16)+(17) from score_history
        hh = hist.get(t) or []
        if len(hh) >= 6:
            old5 = {e2["etf"]: i2 for i2, e2 in enumerate(sorted(
                [{"etf": k2, "s": (v2[-6]["s"] if len(v2) >= 6
                                   else None)}
                 for k2, v2 in hist.items()
                 if len(v2) >= 6],
                key=lambda x: -(x["s"] or -1)))}
            if t in old5:
                r_["rank_delta_5d"] = old5[t] - rank_now[t]
        streak = 0
        ranks_hist = []
        for back in range(1, min(len(hh), 40) + 1):
            snap = sorted(
                [(k2, v2[-back]["s"]) for k2, v2 in hist.items()
                 if len(v2) >= back],
                key=lambda x: -x[1])
            pos = next((i2 for i2, (k2, _s) in enumerate(snap)
                        if k2 == t), None)
            if pos is not None and pos < q_cut:
                streak += 1
            else:
                break
        if rank_now[t] < q_cut:
            r_["leader_streak"] = streak + 1
    # (14) pair-spread board: top-3 vs bottom-3 by scorecard
    sc_rows = [r_ for r_ in rows if r_.get("scorecard_100") is not None]
    sc_rows.sort(key=lambda x: -x["scorecard_100"])
    pair_board = []
    for i2 in range(min(3, len(sc_rows) // 2)):
        lo, sh = sc_rows[i2], sc_rows[-(i2 + 1)]
        cl, cs = closes.get(lo["etf"]) or [], closes.get(sh["etf"]) \
            or []
        n2 = min(len(cl), len(cs))
        spread_spark = None
        if n2 >= 130:
            sp_ = [cl[-n2 + k] / cs[-n2 + k] for k in range(n2)
                   if cs[-n2 + k]]
            spread_spark = [round(v / sp_[-126], 4)
                            for v in sp_[-126::5]]
        pair_board.append({
            "long": lo["etf"], "short": sh["etf"],
            "long_score": lo["scorecard_100"],
            "short_score": sh["scorecard_100"],
            "spread_63d_pct": (round(((cl[-1] / cl[-64])
                                      / (cs[-1] / cs[-64]) - 1)
                                     * 100, 2)
                               if len(cl) >= 64 and len(cs) >= 64
                               else None),
            "spread_spark": spread_spark,
            "note": "research spread, not advice; beta-unadjusted"})
    credit = {}
    try:
        credit = industry_credit(list(holdings_by_etf),
                                 holdings_by_etf, warns)
    except Exception as e:
        warns.append("industry_credit: %s" % str(e)[:100])
    # sector-level OAS enrichment from the existing cds-proxy engine
    try:
        cp = s3_json("data/cds-proxy.json") or {}
        for etf, sec in (("XLF", "financials"), ("XLE", "energy"),
                         ("XLK", "tech"), ("XLRE", "reits")):
            row_ = (cp.get("sectors") or {}).get(sec)
            if row_ and etf in credit:
                credit[etf]["sector_oas"] = row_
            elif row_:
                credit[etf] = {"read": "SECTOR_OAS_ONLY",
                               "sector_oas": row_}
    except Exception:
        pass
    for r_ in rows:
        c = credit.get(r_["etf"])
        if c:
            r_["credit_read"] = c.get("read")
            if r_["tag"] == "BREAKDOWN" and c.get("read") == "DANGER":
                r_["tag"] = "CONFIRMED_DETERIORATION"

    # Fund-flows: the writer produces etf-flows/daily.json with a
    # per-ticker "metrics" map (verified in its put_object calls --
    # there are NO per-ticker files). One read, defensive to
    # dict-keyed or list-of-rows shapes. Display-only.
    flows_hit = 0
    try:
        fd = s3_json("etf-flows/daily.json") or {}
        met = fd.get("metrics") or {}
        fmap = {}
        if isinstance(met, dict):
            for k, v in met.items():
                if isinstance(v, dict):
                    t = (v.get("ticker") or k or "").upper()
                    if "flow_21d_usd" in v or "flow_5d_usd" in v:
                        fmap[t] = v
        elif isinstance(met, list):
            for v in met:
                if isinstance(v, dict) and v.get("ticker"):
                    fmap[v["ticker"].upper()] = v
        for r in rows:
            v = fmap.get(r["etf"])
            if v:
                r["fund_flows"] = {
                    "flow_21d_usd": v.get("flow_21d_usd"),
                    "flow_label": v.get("label")}
                flows_hit += 1
    except Exception as e:
        warns.append("flows join err: %s" % str(e)[:80])
    warns.append("fund_flows joined: %d/%d" % (flows_hit, len(rows)))

    fv = s3_json("data/finviz-groups.json") or {}
    by_sector = {}
    for r in rows:
        if r["kind"] == "SECTOR":
            by_sector[r["name"]] = {
                "etf": r["etf"],
                "leadership_score": r["leadership_score"],
                "tag": r["tag"]}

    out = {
        "engine": "justhodl-industry-rotation", "version": "4.3",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "doctrine": "regime -> industry leadership -> strongest "
                    "soldiers; divergence under weakness = absorption "
                    "(industry momentum subsumes stock momentum: "
                    "Moskowitz-Grinblatt 1999, AQR)",
        "market_regime": {
            "state": regime,
            "spy": round(spy[-1], 2),
            "spy_above_sma20": spy[-1] > (sma(spy, 20) or 9e9),
            "spy_above_sma50": spy[-1] > (sma(spy, 50) or 9e9),
            "risk_regime_score": rr.get("risk_regime_score"),
            "risk_regime": rr.get("risk_regime")},
        "method": {
            "leadership_score": "SMA50 +20, SMA100 +15, SMA200 +15, "
                                "ratio>50dMA +15, ratio 20d slope>0 "
                                "+15, alpha-momentum blend percentile "
                                "/5 (0-20)",
            "alpha_momentum": "beta-adjusted (1y OLS) excess returns; "
                              "blend = 0.5*3m + 0.3*6m-skip-month + "
                              "0.2*12m-minus-1m (Jegadeesh-Titman skip "
                              "+ BAB separation of RS from beta)",
            "dump_rebound": "mean excess bps/day on SPY worst-decile / "
                            "best-decile days, trailing 126d -- the "
                            "weakness and rebound tests quantified",
            "scorecard_100": "Raschke weights abs25/relRS25/MA15/resilience15/breadth10/vol5/fund5; partial data honestly rescaled with scorecard_basis; bands 80+ LEADERSHIP / 65 STRONG_WATCH / 50 NEUTRAL / 35 WEAK / <35 AVOID",
            "industry_credit": "per-name Altman-Z aggregation (Z<1.8 distress); DANGER when >=30% distressed or median<1.8 -- the many-high-CDS rule on the honest free proxy; banks excluded (Altman invalid), covered by cds-proxy sector OAS",
            "crowded": "2y RS line >=95th own-percentile + 20d ratio "
                       "slope negative (long-horizon reversal caution)",
            "breadth": "leaders: % of holdings above own 50d; "
                       "BROAD>=60 / NARROW<40",
            "absorption": "regime WEAK + above SMA50 + rising ratio + "
                          "score>=65", "universe_n": len(rows)},
        "closes66": closes66, "ladder": rows,
        "leaders": leaders,
        "absorption_watch": [r["etf"] for r in rows
                             if r["tag"] == "ABSORPTION"],
        "breakdown_watch": [r["etf"] for r in rows
                            if r["tag"] == "BREAKDOWN"],
        "by_sector_name": by_sector,
        "industry_credit": credit,
        "finviz_sector_perf_attached": bool(fv),
        "score_history": hist,
        "rrg": rrg, "rrg_transitions": rrg_transitions,
        "ew_cw": ew_cw, "ma_events": ma_events,
        "cycle_context": cycle_context,
        "risk_appetite": risk_appetite,
        "join_hits": join_hits, "pair_board": pair_board,
        "rr_debug": {"n_quotes": len(_soldier_q),
                     "fmp_key_set": bool(FMP),
                     "first": (lambda kv: {"t": kv[0],
                                           "price": kv[1].get("price"),
                                           "pa50": kv[1].get("priceAvg50")}
                               if kv else None)(
                                   next(iter(_soldier_q.items()),
                                        None))},
        "rank_note": rank_note,
        "warns": warns[:20]}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    return {"statusCode": 200,
            "body": json.dumps({"ok": True, "regime": regime,
                                "top": rows[0]["etf"] if rows else None,
                                "absorption": len(out["absorption_watch"]),
                                "warns": len(warns)})}
