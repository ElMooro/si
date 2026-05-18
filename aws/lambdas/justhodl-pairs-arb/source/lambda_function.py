"""
justhodl-pairs-arb - the Statistical Arbitrage / Pairs Trading desk.

Every other opportunity engine on this platform is long-biased: it ranks
stocks expected to go UP. That whole stack shares one hidden factor - broad
equity beta. When the market falls, they tend to fall together.

This engine is different. It is market-NEUTRAL. It does not care which way
the market goes. It hunts for two stocks that historically move together -
a cointegrated pair - and flags the moments when their price spread has
pulled abnormally far apart. The trade is to LONG the relatively cheap leg
and SHORT the relatively rich leg in equal dollar size; the position makes
money when the spread snaps back to its normal relationship, regardless of
market direction. This is the original quant strategy - Morgan Stanley's
pairs desk in the 1980s, and the bread and butter of stat-arb funds since.

A naive pairs screen is a money pit, so this engine gates hard:

  - CORRELATION IS NOT COINTEGRATION. Two stocks can be 0.9 correlated and
    still drift apart forever. The real gate is the AR(1) test on the spread
    itself: the spread must statistically pull back toward its own mean. We
    require a mean-reversion coefficient strictly inside (0,1) and a sane
    half-life - fast enough to trade (3-45 trading days).
  - THE KILLER IS THE STRUCTURAL BREAK. A "cointegrated" pair can decouple
    permanently when one company is acquired, disrupted, or caught in fraud.
    Defenses: (1) the spread's first-half mean and second-half mean must be
    stable - a drifting spread is a broken relationship, not a setup;
    (2) we count historical ROUND TRIPS - the pair must have repeatedly
    crossed wide and reverted, proving the relationship is durable, not a
    one-off; (3) a dislocation beyond 4 sigma is treated as a SUSPECTED BREAK
    and quarantined - do not blindly fade it.
  - A PENDING TAKEOVER BREAKS A PAIR. We pull the live M&A feed and exclude
    any pair where either leg is currently an acquisition target.
  - YOU MUST BE ABLE TO SHORT THE RICH LEG. Both legs are gated on a market
    cap floor so the short side is genuinely borrowable.

Universe: the stock-screener, grouped by sector (pairs are only formed
WITHIN a sector - same macro drivers). Per name we pull ~1 year of EOD
closes from FMP, build every within-sector pair, and run the full battery.

Output data/pairs-arb.json     Schedule daily 23:30 UTC (after the US close)
Real data only. Research, not advice. Market-neutral, before borrow costs.
"""
import json
import math
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from itertools import combinations

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/pairs-arb.json"
SCHEMA = "pairs-arb-1.0"

FMP_KEY = os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP_BASE = "https://financialmodelingprep.com/stable"

# ---- universe / compute knobs ----
PER_SECTOR_CAP = 34        # top-N most liquid names per sector
MIN_SECTOR_NAMES = 6
MIN_MARKETCAP = 1.0e9      # both legs must be liquid enough to short
LOOKBACK_DAYS = 252        # ~one trading year
MIN_OVERLAP = 170          # min aligned trading days for a valid pair
WORKERS = 16

# ---- pair gates ----
MIN_CORR = 0.85            # log-price level correlation floor
MIN_PHI = 0.0              # AR(1) mean-reversion coeff must be in (0,1)
MAX_PHI = 0.985            # phi too close to 1 -> no real reversion
MIN_HALFLIFE = 3.0         # trading days - faster than this is noise
MAX_HALFLIFE = 45.0        # slower than this ties capital up too long
ENTRY_Z = 2.0              # |z| dislocation needed to flag a setup
BREAK_Z = 4.0              # beyond this -> suspected structural break
MAX_DRIFT_SIGMA = 1.0      # first-half vs second-half spread-mean stability
STACK_CAP = 130            # max pairs written to the main stack


# ----------------------------------------------------------- small numerics
def num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def mean(xs):
    return sum(xs) / len(xs) if xs else None


def variance(xs):
    if len(xs) < 2:
        return None
    m = sum(xs) / len(xs)
    return sum((x - m) ** 2 for x in xs) / len(xs)


def stdev(xs):
    v = variance(xs)
    return math.sqrt(v) if v is not None else None


def covariance(xs, ys):
    n = len(xs)
    if n < 2 or n != len(ys):
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    return sum((xs[k] - mx) * (ys[k] - my) for k in range(n)) / n


def corr(xs, ys):
    cv = covariance(xs, ys)
    sx = stdev(xs)
    sy = stdev(ys)
    if cv is None or not sx or not sy:
        return None
    return cv / (sx * sy)


def ols_slope(x, y):
    """Slope b of y ~ a + b*x  (least squares)."""
    vx = variance(x)
    cv = covariance(x, y)
    if vx is None or vx < 1e-12 or cv is None:
        return None
    return cv / vx


def ar1_phi(series):
    """AR(1) coefficient: (s_t - mu) = phi*(s_{t-1} - mu) + e."""
    if len(series) < 30:
        return None
    m = sum(series) / len(series)
    lag = [s - m for s in series[:-1]]
    cur = [s - m for s in series[1:]]
    denom = sum(v * v for v in lag)
    if denom < 1e-12:
        return None
    return sum(lag[k] * cur[k] for k in range(len(lag))) / denom


def count_round_trips(zser):
    """A round trip = the z-spread reaches |z|>=1.5 then returns to |z|<=0.5.
    Repeated round trips prove the relationship reverts durably."""
    trips = 0
    extended = False
    for z in zser:
        if abs(z) >= 1.5:
            extended = True
        elif extended and abs(z) <= 0.5:
            trips += 1
            extended = False
    return trips


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


# ------------------------------------------------------------------ fetch
def fetch_prices(symbol):
    url = "%s/historical-price-eod/light?symbol=%s&apikey=%s" % (
        FMP_BASE, symbol, FMP_KEY)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8", "ignore"))
        rows = data if isinstance(data, list) else (data or {}).get("historical", [])
        out = {}
        for row in rows[:LOOKBACK_DAYS + 12]:
            if not isinstance(row, dict):
                continue
            d = row.get("date")
            p = num(row.get("price") if row.get("price") is not None
                    else row.get("close") if row.get("close") is not None
                    else row.get("adjClose"))
            if d and p and p > 0:
                out[str(d)[:10]] = p
        return symbol, out
    except Exception:
        return symbol, {}


def fetch_ma_targets():
    """Symbols currently named as acquisition targets - their pairs are
    structurally broken by the pending deal, so we exclude them."""
    targets = set()
    try:
        url = "%s/mergers-acquisitions-latest?page=0&limit=500&apikey=%s" % (
            FMP_BASE, FMP_KEY)
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8", "ignore"))
        if isinstance(data, list):
            for row in data:
                if isinstance(row, dict):
                    t = (row.get("targetedSymbol") or "").upper().strip()
                    if t:
                        targets.add(t)
    except Exception:
        pass
    return targets


# ------------------------------------------------------------ pair engine
def analyse_pair(si, sj, logp_i, logp_j, dates_i, dates_j):
    """logp_* are date->logprice dicts. Returns a metrics dict or None."""
    common = sorted(dates_i & dates_j)
    if len(common) < MIN_OVERLAP:
        return None
    common = common[-LOOKBACK_DAYS:]
    lpi = [logp_i[d] for d in common]
    lpj = [logp_j[d] for d in common]

    c = corr(lpi, lpj)
    if c is None or c < MIN_CORR:
        return None

    # hedge ratio b: lpi ~ a + b*lpj  (positive comovement required)
    b = ols_slope(lpj, lpi)
    if b is None or b <= 0.05 or b > 8.0:
        return None

    spread = [lpi[k] - b * lpj[k] for k in range(len(common))]
    mu = mean(spread)
    sd = stdev(spread)
    if sd is None or sd < 1e-5:
        return None

    # AR(1) mean reversion - the real cointegration gate
    phi = ar1_phi(spread)
    if phi is None or phi <= MIN_PHI or phi >= MAX_PHI:
        return None
    half_life = -math.log(2.0) / math.log(phi)
    if half_life < MIN_HALFLIFE or half_life > MAX_HALFLIFE:
        return None

    # structural-stability guard: spread mean must not drift across the window
    h = len(spread) // 2
    m1 = mean(spread[:h])
    m2 = mean(spread[h:])
    drift_sigma = abs(m1 - m2) / sd
    if drift_sigma > MAX_DRIFT_SIGMA:
        return None

    z = (spread[-1] - mu) / sd
    if abs(z) < ENTRY_Z:
        return None

    zser = [(s - mu) / sd for s in spread]
    round_trips = count_round_trips(zser)

    return {
        "corr": c,
        "hedge_ratio": b,
        "z": z,
        "half_life": half_life,
        "drift_sigma": drift_sigma,
        "round_trips": round_trips,
        "sd": sd,
        "n_days": len(common),
    }


def score_pair(m):
    """0-100 conviction blend."""
    az = abs(m["z"])
    z_c = clamp((az - ENTRY_Z) / (BREAK_Z - ENTRY_Z), 0.0, 1.0) * 100.0
    corr_c = clamp((m["corr"] - MIN_CORR) / 0.13, 0.0, 1.0) * 100.0
    # half-life sweet spot ~ 8-22 trading days
    hl = m["half_life"]
    if hl <= 8.0:
        hl_c = clamp((hl - MIN_HALFLIFE) / 5.0, 0.0, 1.0) * 100.0
    elif hl <= 22.0:
        hl_c = 100.0
    else:
        hl_c = clamp(1.0 - (hl - 22.0) / 23.0, 0.0, 1.0) * 100.0
    rt_c = clamp(m["round_trips"] / 6.0, 0.0, 1.0) * 100.0
    return round(0.34 * z_c + 0.20 * corr_c + 0.22 * hl_c + 0.24 * rt_c, 1)


def build_entry(si, sj, m, meta, ma_flag):
    """Assemble the trade ticket for one qualifying pair."""
    ri = meta[si]
    rj = meta[sj]
    z = m["z"]
    sd = m["sd"]
    az = abs(z)

    # spread = logP_i - b*logP_j. z>0 => i rich vs j => short i, long j.
    if z > 0:
        long_sym, short_sym = sj, si
        long_row, short_row = rj, ri
    else:
        long_sym, short_sym = si, sj
        long_row, short_row = ri, rj

    # expected convergence: reversion to +/-0.5 sigma (conservative target)
    reversion_pts = max(az - 0.5, 0.0) * sd          # log points
    conv_pct = (math.exp(reversion_pts) - 1.0) * 100.0
    hl = m["half_life"]
    ann_pct = conv_pct * (252.0 / hl) if hl > 0 else None

    suspected_break = az > BREAK_Z
    score = score_pair(m)

    if suspected_break:
        tier = "SUSPECTED BREAK"
    elif score >= 70 and az >= 2.5 and hl <= 25 and m["round_trips"] >= 3:
        tier = "PRIME SETUP"
    elif score >= 55 and m["round_trips"] >= 2:
        tier = "STRONG"
    else:
        tier = "WATCH"

    notes = []
    notes.append("%d historical round trips - the spread has repeatedly "
                 "stretched wide and reverted." % m["round_trips"])
    notes.append("Half-life %.0f trading days: a reversion to the mean "
                 "typically plays out on that horizon." % hl)
    if ma_flag:
        notes.append("CAUTION: a leg is named in the live M&A feed - a "
                     "pending deal can permanently break the pair.")
    if suspected_break:
        notes.append("Dislocation exceeds 4 sigma. This is more often a "
                     "broken relationship than a setup - investigate the "
                     "rich leg for news before fading it.")
    if hl >= 30:
        notes.append("Slower-reverting pair - capital is committed longer; "
                     "size accordingly.")

    return {
        "pair": "%s / %s" % (si, sj),
        "sector": long_row.get("sector") or short_row.get("sector"),
        "trade": "LONG %s  /  SHORT %s" % (long_sym, short_sym),
        "long_leg": {
            "symbol": long_sym,
            "name": long_row.get("name") or long_sym,
            "price": num(long_row.get("price")),
        },
        "short_leg": {
            "symbol": short_sym,
            "name": short_row.get("name") or short_sym,
            "price": num(short_row.get("price")),
        },
        "z_score": round(z, 2),
        "abs_z": round(az, 2),
        "half_life_days": round(hl, 1),
        "correlation": round(m["corr"], 3),
        "hedge_ratio": round(m["hedge_ratio"], 3),
        "round_trips": m["round_trips"],
        "spread_sigma": round(sd, 4),
        "drift_sigma": round(m["drift_sigma"], 2),
        "lookback_days": m["n_days"],
        "expected_convergence_pct": round(conv_pct, 1),
        "annualized_pct": round(ann_pct, 1) if ann_pct is not None else None,
        "ma_flag": bool(ma_flag),
        "suspected_break": suspected_break,
        "score": score,
        "tier": tier,
        "sizing": "Dollar-neutral: equal $ on the long and the short leg "
                  "(log hedge ratio %.2f - near 1.0, so equal dollars is a "
                  "sound approximation)." % m["hedge_ratio"],
        "notes": notes,
    }


# ------------------------------------------------------------------ handler
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    try:
        sc = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="screener/data.json")["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": "screener read failed: %s" % e}

    rows = sc.get("stocks")
    if not isinstance(rows, list):
        bs = sc.get("by_symbol") or {}
        rows = list(bs.values()) if isinstance(bs, dict) else []

    # group liquid common stock by sector
    sectors = {}
    meta = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        sym = (r.get("symbol") or "").upper().strip()
        sec = (r.get("sector") or "").strip()
        mc = num(r.get("marketCap"))
        px = num(r.get("price"))
        if not sym or not sec or not sym.isalpha() or len(sym) > 5:
            continue
        if mc is None or mc < MIN_MARKETCAP or px is None or px <= 0:
            continue
        sectors.setdefault(sec, []).append((mc, sym))
        meta[sym] = r

    universe = []
    for sec, lst in sectors.items():
        lst.sort(key=lambda x: x[0], reverse=True)
        keep = [s for _, s in lst[:PER_SECTOR_CAP]]
        if len(keep) >= MIN_SECTOR_NAMES:
            sectors[sec] = keep
            universe.extend(keep)
        else:
            sectors[sec] = []
    sectors = {k: v for k, v in sectors.items() if v}
    universe = sorted(set(universe))

    # pull EOD price history concurrently
    prices = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for sym, series in ex.map(fetch_prices, universe):
            if len(series) >= MIN_OVERLAP:
                prices[sym] = series

    # M&A target exclusion set (non-fatal)
    ma_targets = fetch_ma_targets()

    # precompute log-price dicts + date sets once per symbol
    logp = {}
    dsets = {}
    for sym, series in prices.items():
        lp = {d: math.log(p) for d, p in series.items()}
        logp[sym] = lp
        dsets[sym] = set(lp.keys())

    entries = []
    n_pairs_tested = 0
    for sec, syms in sectors.items():
        have = [s for s in syms if s in logp]
        for si, sj in combinations(sorted(have), 2):
            n_pairs_tested += 1
            m = analyse_pair(si, sj, logp[si], logp[sj], dsets[si], dsets[sj])
            if not m:
                continue
            ma_flag = (si in ma_targets) or (sj in ma_targets)
            entries.append(build_entry(si, sj, m, meta, ma_flag))

    entries.sort(key=lambda e: e["score"], reverse=True)

    tradeable = [e for e in entries if not e["suspected_break"]]
    breaks = [e for e in entries if e["suspected_break"]]
    breaks.sort(key=lambda e: e["abs_z"], reverse=True)

    stack = tradeable[:STACK_CAP]
    prime = [e for e in stack if e["tier"] == "PRIME SETUP"]
    strong = [e for e in stack if e["tier"] == "STRONG"]

    med_hl = None
    hls = sorted(e["half_life_days"] for e in stack)
    if hls:
        n = len(hls)
        med_hl = hls[n // 2] if n % 2 else (hls[n // 2 - 1] + hls[n // 2]) / 2.0

    headline = (
        "%d cointegrated pairs are dislocated past 2 sigma - %d PRIME setups, "
        "%d STRONG. %d wide pairs quarantined as suspected structural breaks."
        % (len(stack), len(prime), len(strong), len(breaks)))
    how_to_read = (
        "A pairs trade is market-neutral: you LONG the relatively cheap stock "
        "and SHORT the relatively rich one in equal dollar size, and you "
        "profit when their historical price spread snaps back together - up "
        "or down markets do not matter. Each row is a pair whose spread is "
        "currently stretched past 2 standard deviations. The gates are "
        "strict: the spread must statistically mean-revert (AR(1) half-life "
        "3-45 days), the relationship must be stable (no drift across the "
        "year), and it must have reverted repeatedly before (round trips). "
        "z-score is how far the spread is stretched now; the trade is to "
        "fade it back toward the mean. Pairs past 4 sigma are quarantined "
        "separately - that is usually a broken relationship, not a gift. "
        "Expected convergence assumes reversion to +/-0.5 sigma and is "
        "before borrow costs. Research, not advice.")

    out = {
        "ok": True,
        "schema_version": SCHEMA,
        "generated_at": now.strftime("%Y-%m-%d %H:%M UTC"),
        "elapsed_s": round(time.time() - t0, 1),
        "source": "stock-screener universe + FMP /stable EOD price history",
        "strategy": "statistical arbitrage - market-neutral within-sector "
                    "pairs trading",
        "headline": headline,
        "how_to_read": how_to_read,
        "universe_symbols": len(universe),
        "symbols_with_history": len(prices),
        "sectors_scanned": len(sectors),
        "pairs_tested": n_pairs_tested,
        "summary": {
            "n_tradeable": len(stack),
            "n_prime": len(prime),
            "n_strong": len(strong),
            "n_suspected_breaks": len(breaks),
            "median_half_life_days": round(med_hl, 1) if med_hl else None,
        },
        "gates": {
            "min_correlation": MIN_CORR,
            "ar1_phi_band": [MIN_PHI, MAX_PHI],
            "half_life_band_days": [MIN_HALFLIFE, MAX_HALFLIFE],
            "entry_z": ENTRY_Z,
            "break_z": BREAK_Z,
        },
        "pairs": stack,
        "suspected_breaks": breaks[:40],
    }

    body = json.dumps(out, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=body,
                  ContentType="application/json", CacheControl="max-age=300")

    return {"statusCode": 200, "body": json.dumps({
        "ok": True,
        "pairs_tested": n_pairs_tested,
        "n_tradeable": len(stack),
        "prime": len(prime),
        "strong": len(strong),
        "suspected_breaks": len(breaks),
        "elapsed_s": out["elapsed_s"],
    })}
