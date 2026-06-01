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
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

SCHEMA = "1.0"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/dollar-radar.json"
EURODOLLAR_KEY = "data/eurodollar-stress.json"

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
]
# canary inputs
CANARY_SERIES = ["WALCL", "WRESBAL", "RRPONTSYD", "WTREGEN", "DFII10",
                 "DGS10", "DGS2", "IRLTLT01DEM156N", "VIXCLS",
                 "BAMLH0A0HYM2", "DCOILWTICO"]

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


def build_canaries(fred):
    """Returns (canary_list, composite_pressure)."""
    can = []

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
            return w / 1000.0 - r - t
        nl_now = walcl[-1][1] / 1000.0 - rrp[-1][1] - tga[-1][1]
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
    for k in ("stress_score", "composite", "score", "stress"):
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

    # ---- composite ------------------------------------------------------
    if not can:
        return [], None
    wsum = sum(c["weight"] for c in can)
    raw = sum(c["lean"] * c["weight"] for c in can) / wsum  # -2..+2
    pressure = round(raw / 2.0 * 100.0)                     # -100..+100
    return can, pressure


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
    for bkey, sid, ccy, invert in BILATERALS:
        s = fred_series(sid, start, key)
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

    # canary inputs
    fred = {}
    for sid in CANARY_SERIES:
        fred[sid] = fred_series(sid, start, key)
    fred["DTWEXBGS"] = series_cache.get("DTWEXBGS") or fred_series(
        "DTWEXBGS", start, key)

    canaries, pressure = build_canaries(fred)
    regime, regime_note = regime_of(pressure)
    n_pump = sum(1 for c in canaries if c["lean"] > 0)
    n_dump = sum(1 for c in canaries if c["lean"] < 0)
    prev_regime = read_prev_output().get("regime")

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
        "technicals": technicals,
        "how_to_read": (
            "Dollar Pressure runs -100 (hard DUMP) to +100 (hard PUMP). It "
            "is a weighted vote of ten leading canaries -- net liquidity, "
            "QE/QT, the RRP and TGA plumbing, real yields, the US-Germany "
            "rate gap, the VIX safe-haven bid, credit spreads, dollar "
            "momentum and offshore funding stress. Liquidity in (QE, RRP "
            "drain, TGA drawdown) weighs the dollar DOWN; liquidity out "
            "(QT, RRP build, TGA rebuild) holds it UP."),
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
