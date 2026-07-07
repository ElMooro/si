"""
justhodl-dollar-radar -- the Dollar Radar.

One engine that answers a single question: is the dollar set up to
PUMP or to DUMP, and why.

Three layers:

  1. DOLLAR INDICES -- the FRED trade-weighted family (Broad, Advanced
     Foreign Economies, Emerging Market Economies, plus the real broad
     index) and ~10 bilateral crosses, each with level, 1m/3m change
     and a one-year series for charting.

  2. PUMP / DUMP CANARIES -- a ten-signal weighted composite. The
     dollar is driven by relative liquidity, relative monetary policy,
     real-rate differentials, safe-haven demand and offshore funding
     stress; each canary votes on a -2..+2 scale (negative = DUMP
     pressure, positive = PUMP pressure) and the weighted blend gives
     a Dollar Pressure score in -100..+100.

     Liquidity is handled correctly: net liquidity = WALCL - RRP - TGA.
     Fed balance-sheet expansion, RRP draining and TGA drawdowns all
     ADD liquidity (dollar DUMP); QT, RRP building and TGA rebuilds
     DRAIN it (dollar PUMP).

  3. TECHNICALS -- the dollar index against its 50- and 200-day moving
     averages, its one-year range percentile, and double top / double
     bottom detection (a top is bearish for the dollar, a bottom
     bullish).

Output: data/dollar-radar.json. All data is FRED. Honest framing: this
shifts the odds on the dollar's next move from a cluster of leading
signals -- it is not a day-trading oracle.
"""
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass

SCHEMA = "3.0"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/dollar-radar.json"
EURODOLLAR_KEY = "data/eurodollar-stress.json"
CB_STANCE_KEY = "data/cb-stance.json"
CHINA_KEY = "data/china-liquidity.json"
CFTC_KEY = "data/cftc-all-cache.json"
HIST_KEY = "data/dollar-radar-history.json"

s3 = boto3.client("s3")
SSM = boto3.client("ssm")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


def send_telegram(msg):
    """Best-effort Telegram push; never raises into the engine."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[tg] no creds")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                           "parse_mode": "HTML",
                           "disable_web_page_preview": True}).encode("utf-8")
        req = urllib.request.Request(
            "https://api.telegram.org/bot%s/sendMessage" % TELEGRAM_TOKEN,
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        print("[tg] err: %s" % e)


def read_prev_output():
    """Last run's output -- the previous JSON IS the no-spam state."""
    try:
        return json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=OUT_KEY)["Body"].read())
    except Exception:
        return {}

# ---- the FRED dollar family ------------------------------------------------
# headline trade-weighted indices
INDICES = [
    ("broad", "DTWEXBGS", "U.S. Dollar -- Broad", "daily",
     "The Fed's nominal broad trade-weighted dollar index -- the "
     "institutional benchmark, broader and better-weighted than ICE DXY."),
    ("afe", "DTWEXAFEGS", "Dollar vs Advanced Economies", "daily",
     "Nominal dollar index against advanced foreign economies "
     "(euro, yen, sterling, loonie and peers)."),
    ("eme", "DTWEXEMEGS", "Dollar vs Emerging Markets", "daily",
     "Nominal dollar index against emerging-market economies -- the "
     "cleanest read on EM funding pressure."),
    ("real_broad", "RTWEXBGS", "Real Broad Dollar", "monthly",
     "Inflation-adjusted broad dollar index -- the dollar's true "
     "competitiveness, monthly."),
]
# bilateral crosses -- normalised so UP = stronger dollar
BILATERALS = [
    ("eur", "DEXUSEU", "EUR", True),    # USD per EUR -> invert
    ("gbp", "DEXUSUK", "GBP", True),    # USD per GBP -> invert
    ("jpy", "DEXJPUS", "JPY", False),   # JPY per USD -> as-is
    ("cny", "DEXCHUS", "CNY", False),
    ("cad", "DEXCAUS", "CAD", False),
    ("mxn", "DEXMXUS", "MXN", False),
    ("krw", "DEXKOUS", "KRW", False),
    ("chf", "DEXSZUS", "CHF", False),
    ("inr", "DEXINUS", "INR", False),
    ("brl", "DEXBZUS", "BRL", False),
    ("aud", "DEXUSAL", "AUD", True),   # USD per AUD -> invert
    ("sgd", "DEXSIUS", "SGD", False),
    ("twd", "DEXTAUS", "TWD", False),
]

# ---- Bloomberg Dollar Spot Index (BBDXY) replica ----------------------------
# Back-engineered from the published Bloomberg Currency Index methodology:
# a fixed-weight geometric basket over the 12 constituent currencies, weights
# set 50% Fed trade shares / 50% BIS FX-turnover liquidity, CNH capped at 7%,
# sub-2% currencies dropped, rebalanced annually on the last business day of
# June. The spot index maintains target weights daily, so a fixed-weight
# geometric construction replicates it exactly up to the CNH/CNY proxy basis.
BBDXY_WEIGHTS_EFFECTIVE = "2025-07-01"
BBDXY_WEIGHTS_SOURCE = (
    "Bloomberg 2025 BBDXY rebalance announcement -- target weights effective "
    "after the close of 2025-06-30. Annual rebalance is the last business "
    "day of June; swap in the 2026 table when Bloomberg publishes it.")
BBDXY_WEIGHTS = {
    "EUR": 29.47, "JPY": 12.38, "CAD": 11.65, "GBP": 10.27, "MXN": 9.62,
    "CNH": 7.00, "CHF": 4.47, "AUD": 4.39, "KRW": 3.16, "INR": 2.83,
    "SGD": 2.61, "TWD": 2.15}
# FRED H.10 series per constituent: (series_id, invert). invert=True means
# the series is quoted USD-per-unit and must be flipped to FX-per-USD so that
# UP always means a stronger dollar. FRED carries no offshore CNH fix, so the
# onshore CNY (DEXCHUS) stands in -- the CNH-CNY basis is normally well under
# half a percent and is disclosed in the output.
BBDXY_FRED = {
    "EUR": ("DEXUSEU", True), "JPY": ("DEXJPUS", False),
    "CAD": ("DEXCAUS", False), "GBP": ("DEXUSUK", True),
    "MXN": ("DEXMXUS", False), "CNH": ("DEXCHUS", False),
    "CHF": ("DEXSZUS", False), "AUD": ("DEXUSAL", True),
    "KRW": ("DEXKOUS", False), "INR": ("DEXINUS", False),
    "SGD": ("DEXSIUS", False), "TWD": ("DEXTAUS", False)}
# ICE DXY, frozen 1973 weights -- computed on the same engine so the
# BBDXY-minus-DXY spread isolates the EM/Asia leg of any dollar move.
DXY_WEIGHTS = {"EUR": 57.6, "JPY": 13.6, "GBP": 11.9, "CAD": 9.1,
               "SEK": 4.2, "CHF": 3.6}
DXY_FRED = {"EUR": ("DEXUSEU", True), "JPY": ("DEXJPUS", False),
            "GBP": ("DEXUSUK", True), "CAD": ("DEXCAUS", False),
            "SEK": ("DEXSDUS", False), "CHF": ("DEXSZUS", False)}


def _norm_fx_per_usd(series, invert):
    """FRED tuples -> FX-per-USD tuples (UP = stronger dollar)."""
    out = []
    for d, v in series:
        if v and v > 0:
            out.append((d, (1.0 / v) if invert else v))
    return out


def _geometric_basket(fx_raw, weights, fred_map):
    """Fixed-weight geometric USD basket from FRED dailies.

    Returns (levels [(date, level)] rebased to 100 at the first date on
    which every present constituent has a seed value, renormalised weights
    actually used, and the list of missing constituents). Local-holiday
    gaps are forward-filled, standard index practice."""
    per_ccy = {}
    for ccy in weights:
        sid, invert = fred_map[ccy]
        m = {}
        for d, v in _norm_fx_per_usd(fx_raw.get(sid) or [], invert):
            m[d] = v
        if len(m) >= 60:
            per_ccy[ccy] = m
    missing = [c for c in weights if c not in per_ccy]
    covered_w = sum(weights[c] for c in per_ccy)
    total_w = sum(weights.values())
    # weight-aware integrity gate: publish only while the live constituents
    # still carry >=85 percent of the official basket weight (losing TWD is
    # tolerable and renormalised; losing EUR is not) and at least half the
    # constituent count remains.
    if covered_w < 0.85 * total_w or len(per_ccy) < max(3, len(weights) // 2):
        return [], {}, missing
    tot = sum(weights[c] for c in per_ccy)
    wr = {c: weights[c] / tot for c in per_ccy}
    dates = sorted(set().union(*[set(m) for m in per_ccy.values()]))
    last, levels, prev = {}, [], None
    lvl = 100.0
    for d in dates:
        cur, full = {}, True
        for c, m in per_ccy.items():
            if d in m:
                last[c] = m[d]
            if c not in last:
                full = False
                break
            cur[c] = last[c]
        if not full:
            continue
        if prev is not None:
            lr = 0.0
            for c, wgt in wr.items():
                lr += wgt * math.log(cur[c] / prev[c])
            lvl *= math.exp(lr)
        levels.append((d, lvl))
        prev = cur
    return levels, wr, missing


def build_bbdxy(fx_raw, broad_series):
    """The full back-engineered BBDXY block for the output JSON."""
    levels, wr, missing = _geometric_basket(fx_raw, BBDXY_WEIGHTS, BBDXY_FRED)
    dxy_levels, _, _ = _geometric_basket(fx_raw, DXY_WEIGHTS, DXY_FRED)
    if len(levels) < 120:
        return {"available": False, "missing": missing,
                "note": "insufficient FRED FX coverage for the replica"}

    def _chgs(s):
        return {h: (round(chg_pct(s, dd), 2)
                    if chg_pct(s, dd) is not None else None)
                for h, dd in [("chg_1d_pct", 1), ("chg_1w_pct", 7),
                              ("chg_1m_pct", 30), ("chg_3m_pct", 91),
                              ("chg_6m_pct", 182), ("chg_1y_pct", 365)]}

    rep = _chgs(levels)
    dxy = _chgs(dxy_levels) if len(dxy_levels) >= 120 else {}

    constituents = []
    for ccy in sorted(BBDXY_WEIGHTS, key=lambda c: -BBDXY_WEIGHTS[c]):
        sid, invert = BBDXY_FRED[ccy]
        norm = _norm_fx_per_usd(fx_raw.get(sid) or [], invert)
        row = {"currency": ccy, "fred_id": sid,
               "target_weight_pct": BBDXY_WEIGHTS[ccy],
               "present": ccy in wr}
        if ccy == "CNH":
            row["proxy"] = "onshore CNY (DEXCHUS) stands in for offshore CNH"
        if norm and ccy in wr:
            row["fx_per_usd"] = round(norm[-1][1], 4)
            row["as_of"] = norm[-1][0]
            for h, dd in [("usd_chg_1m_pct", 30), ("usd_chg_1y_pct", 365)]:
                v = chg_pct(norm, dd)
                row[h] = round(v, 2) if v is not None else None
            for h, dd in [("contrib_1m_pp", 30), ("contrib_1y_pp", 365)]:
                then = value_days_ago(norm, dd)
                if then:
                    row[h] = round(
                        wr[ccy] * math.log(norm[-1][1] / then) * 100.0, 3)
                else:
                    row[h] = None
        constituents.append(row)

    breadth_1m = None
    if rep.get("chg_1m_pct") is not None and dxy.get("chg_1m_pct") is not None:
        breadth_1m = round(rep["chg_1m_pct"] - dxy["chg_1m_pct"], 2)
    breadth_3m = None
    if rep.get("chg_3m_pct") is not None and dxy.get("chg_3m_pct") is not None:
        breadth_3m = round(rep["chg_3m_pct"] - dxy["chg_3m_pct"], 2)
    if breadth_1m is None:
        bverdict = "breadth unavailable"
    elif breadth_1m >= 0.35:
        bverdict = ("BROAD-LED -- the EM and Asia legs are outrunning the "
                    "G10-heavy DXY, the classic offshore dollar-funding "
                    "stress fingerprint")
    elif breadth_1m <= -0.35:
        bverdict = ("G10-LED -- the dollar move is concentrated in the "
                    "majors; EM currencies are holding up better than DXY "
                    "implies")
    else:
        bverdict = "IN LINE -- broad basket and DXY are moving together"

    vs_broad_1m = None
    if broad_series and rep.get("chg_1m_pct") is not None:
        bc = chg_pct(broad_series, 30)
        if bc is not None:
            vs_broad_1m = round(rep["chg_1m_pct"] - bc, 2)

    out = {"available": True,
           "name": "BBDXY Replica -- Bloomberg Dollar Spot, back-engineered",
           "basis": ("Fixed-weight geometric basket over the 12 official "
                     "constituents, FRED H.10 noon-NY fixes, forward-filled "
                     "over local holidays, rebased to 100 at the window "
                     "start. Tracks Bloomberg BBDXY returns up to the "
                     "CNH/CNY proxy basis."),
           "weights_effective": BBDXY_WEIGHTS_EFFECTIVE,
           "weights_source": BBDXY_WEIGHTS_SOURCE,
           "level": round(levels[-1][1], 3), "as_of": levels[-1][0],
           "range_pctile_1y": (round(pctile_1y(levels), 1)
                               if pctile_1y(levels) is not None else None),
           "missing": missing,
           "constituents": constituents,
           "breadth_spread_1m_pp": breadth_1m,
           "breadth_spread_3m_pp": breadth_3m,
           "breadth_verdict": bverdict,
           "vs_fed_broad_1m_pp": vs_broad_1m,
           "series": [[d, round(v, 3)] for d, v in levels[-260:]],
           "dxy_synth": {"weights": DXY_WEIGHTS,
                         "level": (round(dxy_levels[-1][1], 3)
                                   if dxy_levels else None),
                         "series": [[d, round(v, 3)]
                                    for d, v in dxy_levels[-260:]],
                         **dxy}}
    out.update(rep)
    return out


# canary inputs
CANARY_SERIES = ["WALCL", "WRESBAL", "RRPONTSYD", "WTREGEN", "DFII10",
                 "DGS10", "DGS2", "IRLTLT01DEM156N", "VIXCLS",
                 "BAMLH0A0HYM2", "DCOILWTICO", "SWPT", "NFCI", "T10YIE"]

# pattern parameters (mirrors justhodl-chart-patterns)
SWING_W = 5
PEAK_TOL = 0.04
VALLEY_MIN = 0.05
SEP_MIN = 12
SEP_MAX = 130
PATTERN_RECENT = 70


# ---- FRED ------------------------------------------------------------------
def get_fred_key():
    for k in ["FRED_API_KEY", "FRED_KEY", "FRED_TOKEN"]:
        v = os.environ.get(k)
        if v:
            return v
    try:
        return SSM.get_parameter(Name="/justhodl/fred/api-key",
                                 WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return "2f057499936072679d8843d7fce99989"


def fred_series(series_id, start_date, key):
    qs = urllib.parse.urlencode({
        "series_id": series_id, "api_key": key, "file_type": "json",
        "observation_start": start_date})
    req = urllib.request.Request("%s?%s" % (FRED_BASE, qs),
                                 headers={"User-Agent": "justhodl-dollar/1.0"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                d = json.loads(resp.read().decode())
            break
        except Exception:
            if attempt == 2:
                return []
            time.sleep(1 + attempt)
    out = []
    for obs in d.get("observations", []):
        v = obs.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append((obs["date"], float(v)))
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda x: x[0])
    return out


def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return {}


# ---- series maths ----------------------------------------------------------
def _dt(s):
    return datetime.strptime(s[:10], "%Y-%m-%d")


def value_days_ago(series, days):
    """The observation value closest to `days` calendar days before the
    last point."""
    if not series:
        return None
    target = _dt(series[-1][0]) - timedelta(days=days)
    best, bd = None, None
    for d, v in series:
        gap = abs((_dt(d) - target).days)
        if bd is None or gap < bd:
            bd, best = gap, v
    return best


def chg_pct(series, days):
    if len(series) < 3:
        return None
    now = series[-1][1]
    then = value_days_ago(series, days)
    if then is None or then == 0:
        return None
    return (now - then) / abs(then) * 100.0


def chg_abs(series, days):
    if len(series) < 3:
        return None
    then = value_days_ago(series, days)
    if then is None:
        return None
    return series[-1][1] - then


def sma(values, n):
    if len(values) < n:
        return None
    return sum(values[-n:]) / float(n)


def pctile_1y(series):
    """Where the last value sits in its trailing 1y range, 0-100."""
    vals = [v for _, v in series][-260:]
    if len(vals) < 30:
        return None
    lo, hi = min(vals), max(vals)
    if hi == lo:
        return 50.0
    return (vals[-1] - lo) / (hi - lo) * 100.0


# ---- double top / bottom (ported from justhodl-chart-patterns) -------------
def swing_points(closes):
    highs, lows = [], []
    n, w = len(closes), SWING_W
    for i in range(w, n - w):
        win = closes[i - w:i + w + 1]
        c = closes[i]
        if c == max(win) and (i - w) + win.index(c) == i:
            highs.append(i)
        elif c == min(win) and (i - w) + win.index(c) == i:
            lows.append(i)
    return highs, lows


def detect_double(dates, closes, want_top):
    highs, lows = swing_points(closes)
    n = len(closes)
    pts = highs if want_top else lows
    best = None
    for a in range(len(pts)):
        for b in range(a + 1, len(pts)):
            i1, i2 = pts[a], pts[b]
            sep = i2 - i1
            if sep < SEP_MIN or sep > SEP_MAX or i2 < n - PATTERN_RECENT:
                continue
            p1, p2 = closes[i1], closes[i2]
            if abs(p2 - p1) / p1 > PEAK_TOL:
                continue
            if want_top:
                mids = [j for j in lows if i1 < j < i2]
                if not mids:
                    continue
                im = min(mids, key=lambda j: closes[j])
                depth = (min(p1, p2) - closes[im]) / min(p1, p2)
            else:
                mids = [j for j in highs if i1 < j < i2]
                if not mids:
                    continue
                im = max(mids, key=lambda j: closes[j])
                depth = (closes[im] - max(p1, p2)) / closes[im]
            if depth < VALLEY_MIN:
                continue
            if want_top:
                status = "CONFIRMED" if closes[-1] < closes[im] else "FORMING"
            else:
                status = "CONFIRMED" if closes[-1] > closes[im] else "FORMING"
            sym = max(0.0, 1.0 - abs(p2 - p1) / p1 / PEAK_TOL)
            q = round(100.0 * (0.55 * sym + 0.45 * min(1.0, depth / 0.15)))
            if best is None or q > best["quality"]:
                best = {
                    "pattern": "double_top" if want_top else "double_bottom",
                    "status": status, "quality": q,
                    "neckline": round(closes[im], 3),
                    "extreme_1": {"date": dates[i1], "level": round(p1, 3)},
                    "extreme_2": {"date": dates[i2], "level": round(p2, 3)},
                    "pivot": {"date": dates[im], "level": round(closes[im], 3)},
                    "depth_pct": round(depth * 100, 1),
                }
    return best


# ---- canary scoring --------------------------------------------------------
def _lean(value, thresholds, signs):
    """Map a value to a -2..+2 lean given ascending thresholds and the
    lean to apply in each band (len(signs) == len(thresholds)+1)."""
    for i, t in enumerate(thresholds):
        if value < t:
            return signs[i]
    return signs[-1]


def build_canaries(fred, sib=None):
    """Returns (canary_list, composite_pressure)."""
    can = []
    sib = sib or {}

    def add(label, value_txt, lean, weight, detail):
        can.append({
            "label": label, "reading": value_txt,
            "signal": ("PUMP" if lean > 0 else
                       ("DUMP" if lean < 0 else "NEUTRAL")),
            "lean": lean, "weight": weight, "detail": detail})

    walcl = fred.get("WALCL") or []
    rrp = fred.get("RRPONTSYD") or []
    tga = fred.get("WTREGEN") or []

    # 1) NET LIQUIDITY  (WALCL/1000 - RRP - TGA, all $bn)  ----------------
    if walcl and rrp and tga:
        def netliq_at(days):
            w = value_days_ago(walcl, days)
            r = value_days_ago(rrp, days)
            t = value_days_ago(tga, days)
            if None in (w, r, t):
                return None
            return w / 1000.0 - r - t / 1000.0
        nl_now = walcl[-1][1] / 1000.0 - rrp[-1][1] - tga[-1][1] / 1000.0
        nl_then = netliq_at(91)
        if nl_then:
            d = nl_now - nl_then
            # rising net liquidity -> more dollars -> DUMP pressure
            lean = _lean(d, [-150, -40, 40, 150], [2, 1, 0, -1, -2])
            add("Fed net liquidity (13w change)",
                "%+.0f $bn" % d, lean, 2.0,
                "Net liquidity = balance sheet minus RRP minus TGA. Rising "
                "liquidity floods the system with dollars (DUMP); draining "
                "it starves the system (PUMP).")

    # 2) FED BALANCE SHEET  (QE vs QT) -----------------------------------
    if walcl:
        d = chg_pct(walcl, 91)
        if d is not None:
            lean = _lean(d, [-1.2, -0.3, 0.3, 1.2], [2, 1, 0, -1, -2])
            add("Fed balance sheet trend (QE/QT)",
                "%+.2f%% / 13w" % d, lean, 1.5,
                "Balance-sheet expansion (QE) creates dollars and weighs on "
                "the dollar; contraction (QT) is dollar-supportive.")

    # 3) RRP DRAIN  (repo plumbing) --------------------------------------
    if rrp:
        d = chg_abs(rrp, 91)
        if d is not None:
            # RRP falling -> cash released into the system -> DUMP;
            # RRP building -> cash locked at the Fed, drained -> PUMP
            lean = _lean(d, [-200, -50, 50, 200], [-2, -1, 0, 1, 2])
            add("Reverse repo (RRP) drain",
                "%+.0f $bn / 13w" % d, lean, 1.0,
                "Cash leaving the Fed's reverse-repo facility re-enters the "
                "system as liquidity (DUMP). A rising RRP drains it (PUMP).")

    # 4) TGA  (Treasury cash) --------------------------------------------
    if tga:
        d = chg_abs(tga, 91)
        if d is not None:
            d = d / 1000.0  # WTREGEN is $mn; report in $bn
            # TGA rising -> Treasury drains reserves -> PUMP
            lean = _lean(d, [-150, -40, 40, 150], [-2, -1, 0, 1, 2])
            add("Treasury General Account",
                "%+.0f $bn / 13w" % d, lean, 1.0,
                "A TGA rebuild (bill issuance) drains bank reserves and "
                "supports the dollar; a TGA drawdown (spending) adds "
                "liquidity and weighs on it.")

    # 5) REAL YIELD  (DFII10) --------------------------------------------
    dfii = fred.get("DFII10") or []
    if dfii:
        d = chg_abs(dfii, 91)
        if d is not None:
            lean = _lean(d, [-0.35, -0.1, 0.1, 0.35], [-2, -1, 0, 1, 2])
            add("US 10y real yield trend",
                "%+.2f pp / 13w" % d, lean, 1.5,
                "Rising US real yields raise the reward for holding dollars "
                "and pull capital in (PUMP); falling real yields push it "
                "out (DUMP).")

    # 6) US-GERMANY 10y SPREAD  (policy differential) --------------------
    dgs10 = fred.get("DGS10") or []
    de10 = fred.get("IRLTLT01DEM156N") or []
    if dgs10 and de10:
        sp_now = dgs10[-1][1] - de10[-1][1]
        d = None
        de_then = value_days_ago(de10, 100)
        us_then = value_days_ago(dgs10, 100)
        if de_then is not None and us_then is not None:
            d = sp_now - (us_then - de_then)
        if d is not None:
            lean = _lean(d, [-0.3, -0.08, 0.08, 0.3], [-2, -1, 0, 1, 2])
            add("US-Germany 10y spread",
                "%.2f pp (%+.2f / 13w)" % (sp_now, d), lean, 1.0,
                "A widening US yield advantage over the euro area pulls "
                "capital into dollars (PUMP); a narrowing one releases it "
                "(DUMP).")

    # 7) VIX  (safe-haven bid) -------------------------------------------
    vix = fred.get("VIXCLS") or []
    if vix:
        lvl = vix[-1][1]
        d = chg_abs(vix, 21)
        # high or rising VIX -> flight into the dollar -> PUMP
        lean = _lean(lvl, [16, 20, 26, 32], [-1, 0, 1, 2, 2])
        if d is not None and d > 4 and lean < 2:
            lean += 1
        add("Equity volatility (VIX safe-haven)",
            "%.1f" % lvl, lean, 1.0,
            "The dollar is the deep-crisis safe haven. A VIX spike pulls "
            "global capital into dollars (PUMP); calm markets let it leave "
            "(DUMP).")

    # 8) HY CREDIT SPREADS  (risk-off) -----------------------------------
    hy = fred.get("BAMLH0A0HYM2") or []
    if hy:
        d = chg_abs(hy, 63)
        if d is not None:
            lean = _lean(d, [-0.6, -0.15, 0.15, 0.6], [-1, 0, 0, 1, 2])
            add("High-yield credit spreads",
                "%.2f%% (%+.2f / 13w)" % (hy[-1][1], d), lean, 1.0,
                "Widening credit spreads signal risk-off and a scramble "
                "into dollars (PUMP); tightening spreads release the bid "
                "(DUMP).")

    # 9) DOLLAR MOMENTUM  (trend-following) ------------------------------
    broad = fred.get("DTWEXBGS") or []
    if len(broad) > 210:
        vals = [v for _, v in broad]
        ma50, ma200 = sma(vals, 50), sma(vals, 200)
        roc = chg_pct(broad, 63)
        score = 0
        if ma50 and ma200:
            if vals[-1] > ma50 > ma200:
                score = 2
            elif vals[-1] > ma50:
                score = 1
            elif vals[-1] < ma50 < ma200:
                score = -2
            elif vals[-1] < ma50:
                score = -1
        add("Dollar index momentum",
            "%.2f vs 50d/200d" % vals[-1], score, 1.5,
            "Pure trend: the broad dollar above a rising 50-/200-day "
            "stack is in a PUMP trend; below a falling stack is a DUMP "
            "trend.")

    # 10) OFFSHORE DOLLAR FUNDING STRESS (eurodollar engine) -------------
    ed = read_json(EURODOLLAR_KEY)
    ed_score = None
    for k in ("stress_score", "composite", "composite_score", "score",
              "stress", "stress_index", "eurodollar_stress", "level"):
        if isinstance(ed.get(k), (int, float)):
            ed_score = ed.get(k)
            break
    if ed_score is not None:
        # high eurodollar stress -> global dollar scramble -> PUMP
        lean = _lean(ed_score, [25, 45, 60, 75], [-1, 0, 1, 2, 2])
        add("Offshore dollar funding stress",
            "%.0f/100" % ed_score, lean, 1.5,
            "When offshore borrowers scramble for scarce dollars the "
            "currency is bid hard (PUMP). Easy funding lets it drift "
            "lower (DUMP).")


    # 11) US 10Y NOMINAL YIELD TREND  (the risk-asset co-pilot) ----------
    if dgs10:
        d = chg_abs(dgs10, 63)
        if d is not None:
            lean = _lean(d, [-0.35, -0.1, 0.1, 0.35], [-2, -1, 0, 1, 2])
            add("US 10y nominal yield trend",
                "%.2f%% (%+.2f pp / 13w)" % (dgs10[-1][1], d), lean, 1.25,
                "Rising 10y yields raise the dollar's carry and pull "
                "capital in (PUMP); falling yields release it (DUMP). "
                "With DXY, this is the primary risk-asset transmission "
                "channel.")

    # 12) POLICY REPRICING  (2y front end) -------------------------------
    dgs2 = fred.get("DGS2") or []
    if dgs2:
        d = chg_abs(dgs2, 63)
        if d is not None:
            lean = _lean(d, [-0.4, -0.12, 0.12, 0.4], [-2, -1, 0, 1, 2])
            add("Fed path repricing (2y yield)",
                "%.2f%% (%+.2f pp / 13w)" % (dgs2[-1][1], d), lean, 1.0,
                "The 2y is the market's Fed dot. Front-end repricing "
                "higher = hawkish surprise (PUMP); lower = dovish "
                "surprise (DUMP).")

    # 13) FED SWAP LINES  (eurodollar shortage relief valve) -------------
    swpt = fred.get("SWPT") or []
    if swpt:
        bn = swpt[-1][1] / 1000.0
        lean = _lean(bn, [1, 10, 60, 150], [0, 1, 1, 2, 2])
        add("Fed FX swap lines outstanding",
            "%.1f $bn" % bn, lean, 0.75,
            "Foreign central banks tap Fed swap lines only when offshore "
            "dollars are scarce -- usage confirms a eurodollar shortage "
            "(PUMP), even as the facility caps how violent the squeeze "
            "can get.")

    # 14) CB POLICY STANCE GAP  (Fed vs ECB/BoJ, from cb-stance engine) --
    try:
        cbj = sib.get("cb") or {}
        def _stance_num(node):
            r = str(((node or {}).get("regime")) or "").upper()
            if "HAWK" in r:
                return 1.0
            if "DOVISH" in r or "EASING" in r:
                return -1.0
            return 0.0 if r else None
        low = {str(k).lower(): v for k, v in cbj.items()
               if isinstance(v, dict)}
        fed_n = _stance_num(low.get("fed"))
        peers = [x for x in (_stance_num(low.get("ecb")),
                             _stance_num(low.get("boj")),
                             _stance_num(low.get("boe")))
                 if x is not None]
        if fed_n is not None and peers:
            gap = fed_n - sum(peers) / len(peers)
            lean = max(-2, min(2, int(round(gap))))
            add("CB stance gap (Fed vs ECB/BoJ)",
                "Fed %s vs peers" % (str((cbj.get("fed") or {})
                                         .get("regime", "?")).upper()),
                lean, 1.0,
                "A Fed more hawkish than the ECB and BoJ widens the "
                "policy gap in the dollar's favour (PUMP); a Fed easing "
                "into hawkish peers narrows it (DUMP).")
    except Exception as e:
        print("[canary cb-stance] %s" % e)

    # 15) CHINA CREDIT IMPULSE  (global reflation channel) ---------------
    try:
        cn = sib.get("cn") or {}
        for nest in ("latest", "current", "summary", "china"):
            if isinstance(cn.get(nest), dict):
                cn = {**cn.get(nest), **{k: v for k, v in cn.items()
                                         if not isinstance(v, dict)}}
                break
        ci = None
        for k in ("credit_impulse_pp", "credit_impulse", "impulse_pp",
                  "credit_impulse_yoy"):
            v = cn.get(k)
            if isinstance(v, (int, float)):
                ci = float(v)
                break
            if isinstance(v, dict):
                for kk in ("pp", "impulse_pp", "value", "latest", "yoy",
                           "yoy_pp", "current", "level", "impulse"):
                    if isinstance(v.get(kk), (int, float)):
                        ci = float(v[kk])
                        break
                if ci is None:
                    nums = [x for x in v.values()
                            if isinstance(x, (int, float))]
                    if nums:
                        ci = float(nums[0])
                if ci is not None:
                    break
        if ci is not None and not (-15 < ci < 15):
            ci = None  # unit sanity: credit impulse lives in single digits
        if ci is not None:
            lean = _lean(ci, [-2, -0.5, 0.5, 2], [2, 1, 0, -1, -2])
            add("China credit impulse",
                "%+.1f pp" % ci, lean, 0.75,
                "Accelerating Chinese credit reflates global trade and "
                "commodity demand, pulling capital out of dollars "
                "(DUMP); a credit contraction starves it and supports "
                "the dollar (PUMP).")
    except Exception as e:
        print("[canary china] %s" % e)

    # 16) SPECULATIVE USD POSITIONING  (contrarian at extremes) ----------
    try:
        cf = sib.get("cftc") or {}
        rows = (cf.get("contracts") or cf.get("data") or cf.get("all") or
                cf.get("extremes") or cf.get("rows") or
                (cf if isinstance(cf, list) else []))
        if isinstance(rows, dict):
            rows = list(rows.values())
        pct = None
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            blob = " ".join(str(v) for v in r.values()
                            if isinstance(v, str)).upper()
            if ("DOLLAR" in blob and "INDEX" in blob) or " DX " in blob:
                for k, v in r.items():
                    lk = str(k).lower()
                    if (isinstance(v, (int, float)) and
                            ("pctile" in lk or "percentile" in lk or
                             "pct_rank" in lk)):
                        pct = float(v)
                        break
                if pct is None:
                    for k, v in r.items():
                        lk = str(k).lower()
                        if (isinstance(v, (int, float)) and
                                (lk == "z" or "zscore" in lk or
                                 lk == "z_score")):
                            # map z to an effective percentile band
                            pct = 50.0 + max(-50.0, min(50.0, v * 20.0))
                            break
                break
        if pct is not None:
            lean = 0
            if pct >= 88:
                lean = -1
            elif pct <= 12:
                lean = 1
            add("Speculative USD positioning (CFTC)",
                "%.0f%%ile net long" % pct, lean, 0.5,
                "Crowded speculative dollar longs are fuel for a squeeze "
                "lower (contrarian DUMP risk); a washed-out short base "
                "is fuel for a rip higher (contrarian PUMP).")
    except Exception as e:
        print("[canary cftc] %s" % e)

    # ---- composite ------------------------------------------------------
    if not can:
        return [], None
    wsum = sum(c["weight"] for c in can)
    raw = sum(c["lean"] * c["weight"] for c in can) / wsum  # -2..+2
    pressure = round(raw / 2.0 * 100.0)                     # -100..+100
    return can, pressure


def build_risk_transmission(fred, sib=None):
    """The RISK-ASSET TRANSMISSION dial -- the direct macro drivers of risk
    assets pumping or dumping, anchored on the two most-watched dials:
    DXY and US10Y (46% of the weight, per house doctrine).

    Every component is oriented risk-positive: +2 = pump fuel, -2 = dump
    pressure. Score runs -100 (hard DUMP pressure) to +100 (hard PUMP fuel).
    This is the DRIVER side (leading macro inputs); the RORO risk-regime
    engine is the cross-asset CONFIRMATION side -- read them together.
    """
    sib = sib or {}
    broad = fred.get("DTWEXBGS") or []
    dgs10 = fred.get("DGS10") or []
    dfii = fred.get("DFII10") or []
    comps = []

    def comp(label, reading, lean, weight, note):
        comps.append({"label": label, "reading": reading,
                      "lean": max(-2, min(2, lean)), "weight": weight,
                      "note": note})

    # 1) DXY -- the global tightening dial (w .28)
    dx = chg_pct(broad, 21) if broad else None
    if dx is not None:
        lean = _lean(dx, [-1.5, -0.4, 0.4, 1.5], [2, 1, 0, -1, -2])
        comp("DXY (broad) 1m", "%.2f (%+.2f%%)" % (broad[-1][1], dx),
             lean, 0.28,
             "Dollar down = global easing and a tailwind for EM, "
             "commodities and crypto; dollar up = tightening headwind.")

    # 2) US10Y -- with the breakeven quality test (w .18)
    y10 = chg_abs(dgs10, 21) if dgs10 else None
    hy = fred.get("BAMLH0A0HYM2") or []
    hyd = chg_abs(hy, 21) if hy else None
    be = chg_abs(fred.get("T10YIE") or [], 21)
    if y10 is not None:
        lean = _lean(y10, [-0.25, -0.08, 0.08, 0.25], [2, 1, 0, -1, -2])
        why = ""
        if y10 > 0.08 and be is not None:
            if be >= 0.6 * y10:
                lean += 1
                why = " Reflationary rise (breakevens carrying it) -- "
                why += "less hostile for risk."
            elif be <= 0.15 * y10:
                why = (" Real-rate tightening (breakevens flat) -- the "
                       "hostile kind of yield rise.")
        elif y10 < -0.08 and hyd is not None and hyd > 0.15:
            lean -= 1
            why = (" Yields falling WITH credit widening -- a growth "
                   "scare, not pump fuel.")
        comp("US10Y 1m", "%.2f%% (%+.2f pp)" % (dgs10[-1][1], y10),
             lean, 0.18,
             "Rising long yields raise every hurdle rate." + why)

    # 3) 10y real yield -- the discount rate (w .10)
    ry = chg_abs(dfii, 21) if dfii else None
    if ry is not None:
        lean = _lean(ry, [-0.2, -0.06, 0.06, 0.2], [2, 1, 0, -1, -2])
        comp("US 10y real yield 1m", "%+.2f pp" % ry, lean, 0.10,
             "The cleanest discount rate -- rising real yields hit "
             "long-duration risk (tech, crypto) hardest.")

    # 4) Fed net liquidity -- the tide (w .12)
    walcl = fred.get("WALCL") or []
    rrp = fred.get("RRPONTSYD") or []
    tga = fred.get("WTREGEN") or []
    if walcl and rrp and tga:
        try:
            w0 = value_days_ago(walcl, 91)
            r0 = value_days_ago(rrp, 91)
            t0_ = value_days_ago(tga, 91)
            if None not in (w0, r0, t0_):
                nl_now = (walcl[-1][1] / 1000.0 - rrp[-1][1]
                          - tga[-1][1] / 1000.0)
                d = nl_now - (w0 / 1000.0 - r0 - t0_ / 1000.0)
                lean = _lean(d, [-150, -40, 40, 150], [-2, -1, 0, 1, 2])
                comp("Fed net liquidity 13w", "%+.0f $bn" % d, lean, 0.12,
                     "Risk assets ride the liquidity tide: rising net "
                     "liquidity (QE / RRP drain / TGA spend) is pump "
                     "fuel; draining it is dump pressure.")
        except Exception as e:
            print("[rt netliq] %s" % e)

    # 5) VIX -- risk appetite (w .10)
    vix = fred.get("VIXCLS") or []
    if vix:
        lvl = vix[-1][1]
        lean = _lean(lvl, [14, 18, 24, 30], [2, 1, 0, -1, -2])
        d21 = chg_abs(vix, 21)
        if d21 is not None and d21 > 4:
            lean -= 1
        comp("VIX", "%.1f" % lvl, lean, 0.10,
             "Calm vol regimes let risk premia compress (pump); vol "
             "spikes force de-risking (dump).")

    # 6) HY credit spreads -- the equity canary (w .07)
    if hy:
        d = chg_abs(hy, 63)
        if d is not None:
            lean = _lean(d, [-0.5, -0.12, 0.12, 0.5], [2, 1, 0, -1, -2])
            comp("HY credit spreads 13w",
                 "%.2f%% (%+.2f)" % (hy[-1][1], d), lean, 0.07,
                 "Credit leads equities: tightening spreads are pump "
                 "confirmation; widening is the first dump tell.")

    # 7) Bond volatility -- the collateral channel (w .05)
    try:
        bv = sib.get("bv") or {}
        p = None
        for k in ("composite_percentile", "composite_z_score",
                  "composite_z"):
            if isinstance(bv.get(k), (int, float)):
                p = float(bv[k])
                if "z" in k:  # map z to percentile-ish band
                    p = 50.0 + max(-50.0, min(50.0, p * 20.0))
                break
        if p is not None:
            lean = _lean(p, [25, 45, 60, 80], [2, 1, 0, -1, -2])
            comp("Bond volatility (pctile)", "%.0f%%ile" % p, lean, 0.05,
                 "Rate vol drives collateral haircuts and VaR limits -- "
                 "calm bond vol is pump-permissive; a MOVE-style spike "
                 "forces global de-grossing.")
    except Exception as e:
        print("[rt bondvol] %s" % e)

    # 8) Financial conditions (NFCI) -- the official dial (w .05)
    nfci = fred.get("NFCI") or []
    if nfci:
        d = chg_abs(nfci, 91)
        if d is not None:
            lean = _lean(d, [-0.1, -0.03, 0.03, 0.1], [2, 1, 0, -1, -2])
            comp("Financial conditions (NFCI) 13w", "%+.2f" % d, lean,
                 0.05,
                 "The Chicago Fed's conditions index: easing conditions "
                 "are pump fuel; tightening conditions choke risk.")

    # 9) Oil shock -- the stagflation channel (w .05)
    oil = fred.get("DCOILWTICO") or []
    co = chg_pct(oil, 21) if oil else None
    if co is not None:
        lean = 0
        note = "No energy shock in play."
        if co > 30:
            lean, note = -2, ("An oil spike is a tax on the world and a "
                              "stagflation impulse -- hard dump pressure.")
        elif co > 15:
            lean, note = -1, "A fast oil rise pressures margins and CPI."
        elif co < -25:
            lean, note = -1, ("An oil crash of this speed usually means "
                              "a demand scare, not a windfall.")
        comp("Oil (WTI) 1m", "%.0f (%+.1f%%)" % (oil[-1][1], co), lean,
             0.05, note)

    if not comps:
        return {"score": None, "verdict": "UNKNOWN", "components": [],
                "note": "Insufficient data."}
    wsum = sum(c["weight"] for c in comps)
    raw = sum(c["lean"] * c["weight"] for c in comps) / wsum
    score = int(round(raw / 2.0 * 100.0))
    if score >= 45:
        verdict, note = "RISK PUMP", ("The direct drivers are aligned "
                                      "easy -- the mix that fuels "
                                      "risk-asset pumps.")
    elif score >= 15:
        verdict, note = "LEAN PUMP", ("The driver mix tilts supportive "
                                      "for risk assets.")
    elif score > -15:
        verdict, note = "NEUTRAL", ("The direct drivers are not imposing "
                                    "a direction on risk assets.")
    elif score > -45:
        verdict, note = "LEAN DUMP", ("The driver mix tilts against risk "
                                      "assets.")
    else:
        verdict, note = "RISK DUMP", ("The direct drivers are aligned "
                                      "tight -- the mix that dumps risk "
                                      "assets.")
    return {"score": score, "verdict": verdict, "note": note,
            "components": comps,
            "anchor": "DXY + US10Y carry 46% of the weight by design",
            "how_to_read": (
                "The direct macro drivers of risk assets, each oriented "
                "+2 pump fuel to -2 dump pressure: DXY (28%), US10Y with "
                "a breakeven quality test (18%), the 10y real yield "
                "(10%), Fed net liquidity (12%), VIX (10%), HY credit "
                "(7%), bond volatility (5%), Chicago Fed financial "
                "conditions (5%) and oil shocks (5%). -100 = hard dump "
                "pressure, +100 = hard pump fuel. This is the leading "
                "DRIVER side; the RORO risk-regime engine is the "
                "cross-asset CONFIRMATION side.")}


def regime_of(pressure):
    if pressure is None:
        return "UNKNOWN", "Insufficient data to score the dollar."
    if pressure >= 45:
        return "DOLLAR PUMP", ("The canaries cluster hard on the strong-"
                               "dollar side -- liquidity, rates and "
                               "safe-haven flow are aligned higher.")
    if pressure >= 15:
        return "LEAN PUMP", ("A modest tilt toward a firmer dollar -- the "
                             "signal cluster leans up but is not decisive.")
    if pressure > -15:
        return "NEUTRAL", ("The pump and dump forces roughly offset -- the "
                           "dollar lacks a dominant driver here.")
    if pressure > -45:
        return "LEAN DUMP", ("A modest tilt toward a softer dollar -- "
                             "liquidity and risk appetite lean against it.")
    return "DOLLAR DUMP", ("The canaries cluster hard on the weak-dollar "
                           "side -- ample liquidity and risk-on flow are "
                           "draining the dollar bid.")


# ---- handler ---------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    if isinstance(event, dict) and event.get("test_telegram"):
        send_telegram("\u2705 <b>Dollar Radar</b> -- Telegram tripwire armed "
                      "and reachable. You will get a push on any flip into "
                      "DOLLAR PUMP or DOLLAR DUMP.")
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "test_telegram": "sent"})}
    key = get_fred_key()
    start = (now - timedelta(days=900)).strftime("%Y-%m-%d")

    # fetch dollar indices
    indices_out = []
    series_cache = {}
    for sid_key, sid, name, freq, desc in INDICES:
        s = fred_series(sid, start, key)
        series_cache[sid] = s
        if not s:
            continue
        indices_out.append({
            "key": sid_key, "fred_id": sid, "name": name,
            "frequency": freq, "description": desc,
            "level": round(s[-1][1], 3), "as_of": s[-1][0],
            "chg_1m_pct": (round(chg_pct(s, 30), 2)
                           if chg_pct(s, 30) is not None else None),
            "chg_3m_pct": (round(chg_pct(s, 91), 2)
                           if chg_pct(s, 91) is not None else None),
            "chg_1y_pct": (round(chg_pct(s, 365), 2)
                           if chg_pct(s, 365) is not None else None),
            "range_pctile_1y": (round(pctile_1y(s), 1)
                                if pctile_1y(s) is not None else None),
            "series": [[d, round(v, 3)] for d, v in s[-260:]],
        })

    # bilateral crosses -- normalised so UP = stronger dollar
    bilat_out = []
    bilat_raw = {}
    for bkey, sid, ccy, invert in BILATERALS:
        s = fred_series(sid, start, key)
        bilat_raw[sid] = s
        if not s:
            continue
        norm = [(d, (1.0 / v if (invert and v) else v)) for d, v in s]
        bilat_out.append({
            "key": bkey, "fred_id": sid, "currency": ccy,
            "quote": "USD strength vs %s" % ccy,
            "chg_1m_pct": (round(chg_pct(norm, 30), 2)
                           if chg_pct(norm, 30) is not None else None),
            "chg_3m_pct": (round(chg_pct(norm, 91), 2)
                           if chg_pct(norm, 91) is not None else None),
            "chg_1y_pct": (round(chg_pct(norm, 365), 2)
                           if chg_pct(norm, 365) is not None else None),
        })

    # ---- BBDXY replica (Bloomberg Dollar Spot, back-engineered) --------
    fx_raw = dict(bilat_raw)
    for _pair in list(BBDXY_FRED.values()) + list(DXY_FRED.values()):
        _sid = _pair[0]
        if not fx_raw.get(_sid):
            fx_raw[_sid] = fred_series(_sid, start, key)
    bbdxy = build_bbdxy(fx_raw, series_cache.get("DTWEXBGS") or [])

    # canary inputs
    fred = {}
    for sid in CANARY_SERIES:
        fred[sid] = fred_series(sid, start, key)
    fred["DTWEXBGS"] = series_cache.get("DTWEXBGS") or fred_series(
        "DTWEXBGS", start, key)

    siblings = {"cb": read_json(CB_STANCE_KEY),
                "cn": read_json(CHINA_KEY),
                "cftc": read_json(CFTC_KEY),
                "bv": read_json("data/bond-vol.json")}
    canaries, pressure = build_canaries(fred, siblings)
    risk_tx = build_risk_transmission(fred, siblings)
    regime, regime_note = regime_of(pressure)
    n_pump = sum(1 for c in canaries if c["lean"] > 0)
    n_dump = sum(1 for c in canaries if c["lean"] < 0)
    _prev = read_prev_output()
    prev_regime = _prev.get("regime")
    prev_risk = (_prev.get("risk_transmission") or {}).get("verdict")

    # technicals + patterns on the broad dollar
    broad = fred.get("DTWEXBGS") or []
    technicals = {}
    if len(broad) > 210:
        dates = [d for d, _ in broad]
        closes = [v for _, v in broad]
        vals = closes
        ma50, ma200 = sma(vals, 50), sma(vals, 200)
        technicals = {
            "index": "DTWEXBGS (Broad Trade-Weighted Dollar)",
            "level": round(vals[-1], 3), "as_of": dates[-1],
            "sma50": round(ma50, 3) if ma50 else None,
            "sma200": round(ma200, 3) if ma200 else None,
            "pct_vs_sma50": (round((vals[-1] / ma50 - 1) * 100, 2)
                             if ma50 else None),
            "pct_vs_sma200": (round((vals[-1] / ma200 - 1) * 100, 2)
                              if ma200 else None),
            "range_pctile_1y": (round(pctile_1y(broad), 1)
                                if pctile_1y(broad) is not None else None),
            "double_top": detect_double(dates, closes, True),
            "double_bottom": detect_double(dates, closes, False),
            "series": [[d, round(v, 3)] for d, v in broad[-320:]],
        }

    # headline
    pat = ""
    if technicals.get("double_top"):
        pat = (" A double top is %s on the dollar index -- a bearish "
               "reversal risk." % technicals["double_top"]["status"].lower())
    elif technicals.get("double_bottom"):
        pat = (" A double bottom is %s on the dollar index -- a bullish "
               "reversal risk." % technicals["double_bottom"]["status"]
               .lower())
    headline = ("Dollar Pressure %+d/100 -- %s. %d canaries lean pump, "
                "%d lean dump.%s" % (pressure if pressure is not None else 0,
                                     regime, n_pump, n_dump, pat))
    if risk_tx.get("score") is not None:
        headline += (" Risk-asset transmission: %s (%+d)."
                     % (risk_tx["verdict"], risk_tx["score"]))

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-dollar-radar",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 1),
        "dollar_pressure": pressure,
        "regime": regime,
        "regime_note": regime_note,
        "headline": headline,
        "canaries_pump": n_pump,
        "canaries_dump": n_dump,
        "canaries": canaries,
        "indices": indices_out,
        "bilaterals": bilat_out,
        "bbdxy": bbdxy,
        "technicals": technicals,
        "risk_transmission": risk_tx,
        "history_key": HIST_KEY,
        "how_to_read": (
            "Dollar Pressure runs -100 (hard DUMP) to +100 (hard PUMP). It "
            "is a weighted vote of ten leading canaries -- net liquidity, "
            "QE/QT, the RRP and TGA plumbing, real yields, the US-Germany "
            "rate gap, the VIX safe-haven bid, credit spreads, dollar "
            "momentum and offshore funding stress. Liquidity in (QE, RRP "
            "drain, TGA drawdown) weighs the dollar DOWN; liquidity out "
            "(QT, RRP build, TGA rebuild) holds it UP. v2 adds the US10Y "
            "trend, 2y Fed-path repricing, Fed swap-line usage (the "
            "eurodollar relief valve), the Fed-vs-ECB/BoJ stance gap, "
            "China's credit impulse and CFTC positioning extremes -- "
            "plus the Risk-Asset Transmission dial that reads DXY x "
            "US10Y for what they mean for risk assets. v3 adds the BBDXY "
            "Replica -- the Bloomberg Dollar Spot Index back-engineered as "
            "a fixed-weight geometric basket over its 12 official "
            "constituents (official target weights, CNH capped at 7pct), "
            "with per-currency contribution attribution and a broad-vs-DXY "
            "breadth spread that isolates the EM and Asia leg of any "
            "dollar move."),
        "disclaimer": (
            "A probabilistic dollar radar built from leading macro "
            "signals. It shifts the odds on the dollar's next move; it "
            "does not time the day and is not investment advice."),
    }
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out, default=str).encode("utf-8"),
                      ContentType="application/json")
    except Exception as e:
        # audit P2.5: emit EMF metric for silent put_object failure
        print(__import__('json').dumps({"_aws":{"Timestamp":int(__import__('time').time()*1000),"CloudWatchMetrics":[{"Namespace":"JustHodl/Reliability","Dimensions":[["Lambda"]],"Metrics":[{"Name":"S3PutFailure","Unit":"Count"}]}]},"Lambda":__import__('os').environ.get("AWS_LAMBDA_FUNCTION_NAME","?"),"S3PutFailure":1,"error":str(e)[:200] if 'e' in dir() else "unknown"}))
        print("S3 write fail: %s" % e)
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": str(e)})}

    # --- daily history ledger (append-only, capped) --------------------
    try:
        hist = read_json(HIST_KEY)
        rows = hist.get("rows") if isinstance(hist, dict) else hist
        rows = rows if isinstance(rows, list) else []
        today = now.strftime("%Y-%m-%d")
        rows = [r for r in rows if r.get("date") != today]
        rows.append({"date": today, "ts": now.isoformat(),
                     "dollar_pressure": pressure, "regime": regime,
                     "risk_score": risk_tx.get("score"),
                     "risk_verdict": risk_tx.get("verdict")})
        rows = rows[-800:]
        s3.put_object(Bucket=S3_BUCKET, Key=HIST_KEY,
                      Body=json.dumps({"rows": rows,
                                       "updated": now.isoformat()},
                                      default=str).encode("utf-8"),
                      ContentType="application/json")
    except Exception as e:
        print("[history] %s" % e)

    # --- regime-flip tripwire -- fires only on a flip INTO a hard regime,
    # comparing to the previous run, so it never spams while a regime holds.
    alerted = None
    try:
        p = "%+d" % int(round(pressure)) if pressure is not None else "?"
        if regime == "DOLLAR PUMP" and prev_regime != "DOLLAR PUMP":
            send_telegram(
                "\U0001F6A8 <b>DOLLAR RADAR -- regime flip</b>\n\n"
                "Dollar Pressure %s -> <b>DOLLAR PUMP</b>\n"
                "(was %s)\n\n"
                "The pump/dump canaries have clustered hard on the strong-"
                "dollar side -- tighter global USD liquidity. A dollar "
                "squeeze is a classic risk-off / funding-stress signal.\n\n"
                "justhodl.ai/dollar.html" % (p, prev_regime or "n/a"))
            alerted = "DOLLAR PUMP"
        elif regime == "DOLLAR DUMP" and prev_regime != "DOLLAR DUMP":
            send_telegram(
                "\U0001F7E2 <b>DOLLAR RADAR -- regime flip</b>\n\n"
                "Dollar Pressure %s -> <b>DOLLAR DUMP</b>\n"
                "(was %s)\n\n"
                "The canaries have clustered hard on the weak-dollar side "
                "-- Fed liquidity (QE / RRP drain / TGA drawdown) is "
                "flooding the system with dollars. Typically risk-on.\n\n"
                "justhodl.ai/dollar.html" % (p, prev_regime or "n/a"))
            alerted = "DOLLAR DUMP"
        rv, prv = risk_tx.get("verdict"), prev_risk
        if rv == "RISK DUMP" and prv != "RISK DUMP":
            send_telegram(
                "\U0001F534 <b>RISK-ASSET TRANSMISSION -- flip</b>\n\n"
                "DXY x US10Y -> <b>RISK DUMP</b> (%+d)\n(was %s)\n\n"
                "Dollar and yields are tightening together -- the classic "
                "mix that dumps risk assets.\n\njusthodl.ai/dollar.html"
                % (risk_tx.get("score") or 0, prv or "n/a"))
            alerted = (alerted or "") + "+RISK DUMP"
        elif rv == "RISK PUMP" and prv != "RISK PUMP":
            send_telegram(
                "\U0001F7E2 <b>RISK-ASSET TRANSMISSION -- flip</b>\n\n"
                "DXY x US10Y -> <b>RISK PUMP</b> (%+d)\n(was %s)\n\n"
                "Dollar and yields are easing together -- the liquidity "
                "mix that fuels risk-asset pumps.\n\n"
                "justhodl.ai/dollar.html"
                % (risk_tx.get("score") or 0, prv or "n/a"))
            alerted = (alerted or "") + "+RISK PUMP"
    except Exception as e:
        print("[tg] flip alert err: %s" % e)

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "dollar_pressure": pressure, "regime": regime,
        "canaries": len(canaries), "indices": len(indices_out),
        "bilaterals": len(bilat_out),
        "double_top": bool(technicals.get("double_top")),
        "double_bottom": bool(technicals.get("double_bottom")),
        "telegram_alert": alerted,
        "build_seconds": out["build_seconds"]})}
