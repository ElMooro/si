"""
justhodl-cycle-clock  ·  v1.0
─────────────────────────────────────────────────────────────────────────────
ONE honest verdict on WHERE WE ARE IN THE CYCLE and HOW MUCH LIQUIDITY-SQUEEZE
RISK is building — synthesized from the engines that already exist, with the
DIVERGENCES surfaced rather than averaged away.

This is a META engine: it reads other engines' published JSON (no raw re-fetch)
and fuses them into:
  · CYCLE POSITION  — investment-clock phase (EARLY/MID/LATE/DOWNTURN) anchored on
    the macro-regime quadrant, confirmed/contradicted by US-cycle, global business
    cycle, and the growth nowcast. Late-stage froth overlay (valuation/leverage).
  · LIQUIDITY-SQUEEZE RISK  — 0-100 gauge from the purpose-built stress stack
    (plumbing health, funding stress, crisis composite, canaries, global &
    systemic stress) modified by the liquidity-flow direction.
  · DIVERGENCES  — explicit conflicts (e.g. global-expansion vs US-stagflation;
    aggregate-liquidity-flat vs credit-engine-draining; equity-on vs crypto-off).
  · VERDICT  — a single honest sentence a PM can act on.

Honesty: this fuses MODEL OUTPUTS, not ground truth. Every input read is shown
with its own staleness; a missing/stale feed degrades gracefully and is flagged.
"""
import json, time
from datetime import datetime, timezone

VERSION = "3.1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/cycle-clock.json"

import boto3
s3 = boto3.client("s3", "us-east-1")

import concurrent.futures as _cf
try:
    from llm_router import complete as _llm
except Exception:
    _llm = None


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None

def _age_days(iso):
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        return round((datetime.now(timezone.utc) - dt).total_seconds() / 86400, 1)
    except Exception:
        return None

def _get(d, *path, default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(p)
    return cur if cur is not None else default


# macro-regime quadrant → investment-clock phase
import urllib.request, urllib.parse
from statistics import mean, pstdev

FRED_KEY = "2f057499936072679d8843d7fce99989"


def _fred(series, start="2006-01-01"):
    try:
        u = "https://api.stlouisfed.org/fred/series/observations?" + urllib.parse.urlencode(
            {"series_id": series, "api_key": FRED_KEY, "file_type": "json", "observation_start": start})
        j = json.loads(urllib.request.urlopen(u, timeout=12).read())
        return [(o["date"], float(o["value"])) for o in j.get("observations", [])
                if o.get("value") not in (None, ".", "")]
    except Exception:
        return []


def _yoy(pts):
    d = dict(pts); ks = sorted(d); out = []
    for i, k in enumerate(ks):
        if i >= 12 and d[ks[i - 12]]:
            out.append((k, d[k] / d[ks[i - 12]] - 1.0))
    return out


def _z_series(pts, lb=120):
    vals = [v for _, v in pts]; dates = [d for d, _ in pts]; out = []
    for i in range(len(vals)):
        if i < 24:
            continue
        w = vals[max(0, i - lb + 1):i + 1]
        m = mean(w); sd = pstdev(w) if len(w) > 1 else 0
        out.append((dates[i], round((vals[i] - m) / sd, 2) if sd else 0.0))
    return out


def _coord_series():
    """Monthly growth_z & inflation_z from FRED momentum (industrial production + payrolls for
    growth; headline CPI + core PCE for inflation), aligned → last ~13 months for the trail + now."""
    def blend(a, b):
        da, db = dict(a), dict(b); ks = sorted(set(da) & set(db))
        return {k: round((da[k] + db[k]) / 2, 2) for k in ks}
    g = blend(_z_series(_yoy(_fred("INDPRO"))), _z_series(_yoy(_fred("PAYEMS"))))
    f = blend(_z_series(_yoy(_fred("CPIAUCSL"))), _z_series(_yoy(_fred("PCEPILFE"))))
    ks = sorted(set(g) & set(f))[-13:]
    return [{"m": k[:7], "g": g[k], "i": f[k]} for k in ks]


def _quad2d(g, i):
    if g is None or i is None:
        return None
    if g >= 0 and i < 0:  return "GOLDILOCKS"
    if g >= 0 and i >= 0: return "OVERHEAT"
    if g < 0 and i >= 0:  return "STAGFLATION"
    return "DOWNTURN"


# historical asset leadership by clock quadrant (Merrill investment-clock canon)
QUAD_ASSETS = {
    "GOLDILOCKS":  {"clock": "Recovery",    "lead": ["Equities — cyclicals & tech", "High-yield credit", "EM equities"],
                    "lag": ["Cash", "Long-duration Treasuries"]},
    "OVERHEAT":    {"clock": "Overheat",     "lead": ["Commodities — energy & materials", "Value / cyclical equities", "TIPS / inflation hedges"],
                    "lag": ["Long Treasuries", "Long-duration growth"]},
    "STAGFLATION": {"clock": "Stagflation",  "lead": ["Cash / T-bills", "Gold", "Energy & commodities", "Defensives — staples, utilities, healthcare"],
                    "lag": ["Long-duration equities", "High-yield credit"]},
    "DOWNTURN":    {"clock": "Reflation",    "lead": ["Long Treasuries — duration", "Gold", "Quality / defensive equities"],
                    "lag": ["Cyclicals", "High-yield credit", "Commodities"]},
}


COORD_PHASE = {"GOLDILOCKS": ("MID CYCLE", 2), "OVERHEAT": ("LATE-MID CYCLE", 2.5),
               "STAGFLATION": ("LATE CYCLE", 3), "DOWNTURN": ("DOWNTURN", 4)}

# what observable change would neutralise each synthesis driver — makes the read falsifiable
FLIP = {
    "Liquidity-pulse draining": "the liquidity pulse turns from draining to neutral/expanding",
    "Bank-reserve scarcity": "bank reserves climb back above ~11% of GDP",
    "Recession-prob composite": "the recession-prob composite falls back below 25%",
    "Capital-cycle scarcity building": "industry capex turns back up / capacity utilization rolls over",
    "Capacity flooding (late cycle)": "industry capex growth rolls back below replacement",
    "Commodity cure-for-low-prices": "commodity prices recover back above their trend",
    "Leading labor weakening": "leading labor stabilises or re-accelerates",
    "Retail euphoria (contrarian)": "AAII bulls reset out of the euphoric extreme",
    "Smart money distributing": "COT smart-money flips from distributing to accumulating",
    "Credit complacency": "credit spreads widen out of rich territory",
    "Bond-vol complacency": "MOVE lifts off its suppressed lows",
    "Tail risk elevated": "the option-implied tail gauge falls back below ~40",
    "Fed turning hawkish": "Fed drift neutralises and futures stop pricing hikes",
    "Yen-carry unwind risk": "yen-carry unwind pressure eases",
    "Tape contradicts regime": "the tape starts confirming the quadrant prescription",
    "Global cycle expanding": "the global cycle rolls from expansion to contraction",
    "Activity nowcast expanding": "the activity nowcast turns to contraction",
    "Credit spreads benign": "credit spreads turn stressed",
    "Fed net liquidity rising": "Fed net liquidity rolls over",
    "Cross-asset RORO risk-on": "cross-asset RORO flips back to risk-off",
    "Macro regime constructive": "the macro regime turns defensive",
    "Breadth thrust firing": "the breadth thrust fades",
    "Crypto implied vol elevated": "crypto DVOL falls back to normal",
    "Crypto funding crowded long": "crypto perp funding normalises",
    "Crypto dry-powder loaded (contrarian)": "stablecoin dry powder gets deployed (SSR rises)",
    "Crypto options hedging (put skew + backwardation)": "crypto skew normalises / vol term re-contangos",
    "Crypto miners deep value (Puell)": "Puell rises back above 1",
    "Crypto hash-ribbon recovery": "hash ribbon rolls back into capitulation",
    "Crypto carry backwardation (deleveraging)": "futures basis re-contangos",
    "Crypto heavy exchange inflow (distribution)": "exchange netflow normalises",
    "Crypto below realized price (deep value)": "price reclaims aggregate cost basis",
    "Crypto NUPL euphoria (late-cycle)": "NUPL cools out of euphoria",
    "Crypto stablecoin depeg (tail risk)": "stablecoins re-peg to $1",
    "Crypto spot-ETF strong inflow (marginal bid)": "ETF creations slow / reverse",
    "Crypto spot-ETF strong outflow (marginal sell)": "ETF redemptions abate / turn to inflows",
    "Crypto perp leverage buildup (fragile)": "open interest unwinds / funding normalises",
    "Crypto perp liquidation cascade (acute stress)": "OI stabilises after forced selling clears",
    "Crypto dealer negative gamma (vol-expansion)": "spot reclaims the gamma-flip level",
}


def _net_liquidity():
    """Fed net liquidity = balance sheet − reverse repo − Treasury General Account, with the 13-week
    change, a 26-week sparkline, and the percentile of the current level since 2018. WALCL is $millions,
    RRP/TGA are $billions/$millions — normalize all to $trillions."""
    walcl = _fred("WALCL", start="2018-01-01")
    rrp = _fred("RRPONTSYD", start="2018-01-01")
    tga = _fred("WTREGEN", start="2018-01-01")
    if not walcl:
        return None
    rrp_d, tga_d = dict(rrp), dict(tga)
    rrp_ks, tga_ks = sorted(rrp_d), sorted(tga_d)
    def nearest(ks, d, date):
        import bisect
        i = bisect.bisect_right(ks, date) - 1
        return d[ks[i]] if i >= 0 else (d[ks[0]] if ks else 0.0)
    series = []
    for date, wv in walcl:
        net = wv / 1e6 - nearest(rrp_ks, rrp_d, date) / 1e3 - nearest(tga_ks, tga_d, date) / 1e6
        series.append((date, round(net, 3)))
    cur = series[-1][1]
    net13 = series[-14][1] if len(series) > 13 else series[0][1]
    vals = [v for _, v in series]
    pct = round(sum(1 for v in vals if v <= cur) / len(vals) * 100)
    last = series[-1][0]
    return {"walcl_tn": round(walcl[-1][1] / 1e6, 3), "rrp_tn": round(nearest(rrp_ks, rrp_d, last) / 1e3, 3),
            "tga_tn": round(nearest(tga_ks, tga_d, last) / 1e6, 3), "net_tn": cur,
            "net_13w_delta_bn": round((cur - net13) * 1000, 1), "as_of": last,
            "percentile_since_2018": pct, "series": [{"d": d[:7], "v": v} for d, v in series[-26:]]}


def _sahm():
    """Sahm recession trigger: 3-mo avg unemployment minus its trailing-12-mo low. ≥0.50 = recession."""
    u = _fred("UNRATE", start="2017-01-01")
    if len(u) < 15:
        return None
    vals = [v for _, v in u]
    sahm = round(mean(vals[-3:]) - min(vals[-12:]), 2)
    return {"value": sahm, "triggered": sahm >= 0.5, "as_of": u[-1][0]}


def _ai_synthesis(state):
    """Heavy AI read: feed the full structured state to GLM (tier=reason, GLM-5.1; Claude fallback)
    and get back a buy-side strategist's synthesis as JSON."""
    if _llm is None:
        return None
    SYSTEM = ("You are a senior buy-side macro strategist writing the daily Cycle & Liquidity read for a "
              "portfolio manager. You receive a structured state assembled from quantitative engines. Write a "
              "sharp, honest, specific synthesis: cite the actual numbers, name the key tension explicitly, "
              "and never invent data that is not in the state. Acknowledge uncertainty and any unprecedentedness "
              "caveat (reduce conviction when the configuration is historically unusual). No price targets, no "
              "hype. Return ONLY valid minified JSON, no markdown, no commentary outside the JSON.")
    prompt = ("STATE (JSON):\n" + json.dumps(state, default=str) +
              "\n\nReturn ONLY this JSON schema, filled from the state:\n"
              '{"executive_read":"4-5 sentence synthesis: where we are in the cycle, the single most important '
              'tension, what the liquidity backdrop adds, and what it means for risk",'
              '"regime_call":"short decisive label + one clause",'
              '"bull_case":["2-3 specific points grounded in the numbers"],'
              '"bear_case":["2-3 specific points grounded in the numbers"],'
              '"positioning":{"own":["assets/sectors to favour, tied to the quadrant and confirmed by the data"],'
              '"reduce":["assets to trim or avoid"],"sizing":"one line on conviction and sizing given the '
              'unprecedentedness/divergences"},'
              '"watch":["3 specific, falsifiable triggers that would change the read"],'
              '"divergence_reads":["one plain-English sentence per key divergence explaining why it matters"],'
              '"liquidity_read":"1-2 sentences on net liquidity direction + any draining flickers",'
              '"bottom_line":"one decisive sentence a PM can act on today"}')
    try:
        raw = ""
        for _attempt in range(2):
            try:
                raw = _llm(prompt, tier="reason", max_tokens=1500, system=SYSTEM) or ""
                if raw.strip():
                    break
            except Exception as _e:
                if _attempt == 0:
                    time.sleep(3); continue
                raise
        txt = raw.strip()
        if txt.startswith("```"):
            txt = txt.split("```", 2)[1]
            txt = txt[4:] if txt.startswith("json") else txt
        i, j = txt.find("{"), txt.rfind("}")
        if i >= 0 and j > i:
            return json.loads(txt[i:j + 1])
    except Exception as e:
        print(f"[cycle-clock] AI synthesis failed: {str(e)[:140]}")
    return None


POLY_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

CROSS_ASSETS = [
    ("SPY", "US equities", "equity"), ("QQQ", "US tech", "equity"), ("IWM", "Small caps", "equity"),
    ("IWD", "Value", "factor"), ("IWF", "Growth", "factor"),
    ("DBC", "Commodities", "real"), ("GLD", "Gold", "real"), ("XLE", "Energy", "real"),
    ("TLT", "Long Treasuries", "rates"), ("IEF", "7-10y Treasuries", "rates"),
    ("HYG", "High yield", "credit"), ("LQD", "IG credit", "credit"),
    ("UUP", "US dollar", "fx"), ("XLU", "Utilities", "defensive"),
]
# which cross-asset proxies each quadrant historically expects to lead
EXPECT_LEAD = {
    "OVERHEAT": ["DBC", "GLD", "XLE", "IWD"],
    "GOLDILOCKS": ["SPY", "QQQ", "IWM", "HYG"],
    "STAGFLATION": ["GLD", "DBC", "XLU", "UUP"],
    "DOWNTURN": ["TLT", "IEF", "GLD"],
}
SCENARIO_ASSETS = {
    "GOLDILOCKS": ["Equities (cyclicals, tech)", "HY credit", "EM equities"],
    "REFLATION": ["Equities", "Commodities", "TIPS"],
    "OVERHEAT": ["Commodities", "Value / cyclicals", "TIPS"],
    "STAGFLATION": ["Cash / T-bills", "Gold", "Energy", "Defensives"],
    "DEFLATION-BUST": ["Long Treasuries", "Gold", "Cash", "Quality"],
    "DOWNTURN": ["Long Treasuries", "Gold", "Quality"],
}


def _poly_ret(t):
    import datetime as _dt
    end = _dt.date.today(); start = end - _dt.timedelta(days=130)
    u = (f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{start}/{end}"
         f"?adjusted=true&sort=asc&limit=200&apiKey={POLY_KEY}")
    try:
        r = json.loads(urllib.request.urlopen(u, timeout=10).read()).get("results") or []
        if len(r) < 63:
            return None
        c = [x["c"] for x in r]
        return {"ret_1m": round((c[-1] / c[-22] - 1) * 100, 1), "ret_3m": round((c[-1] / c[-63] - 1) * 100, 1)}
    except Exception:
        return None


def _cross_asset():
    out = []
    with _cf.ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(_poly_ret, t): (t, lab, cls) for t, lab, cls in CROSS_ASSETS}
        for f in futs:
            t, lab, cls = futs[f]
            try:
                r = f.result(timeout=14)
            except Exception:
                r = None
            if r:
                out.append({"ticker": t, "label": lab, "class": cls, **r})
    out.sort(key=lambda x: -x["ret_1m"])
    return out


def _cb_impulse():
    """G3 central-bank balance-sheet 13-week % change (currency-neutral liquidity impulse)."""
    def imp(series, n):
        p = _fred(series, start="2022-01-01")
        return round((p[-1][1] / p[-(n + 1)][1] - 1) * 100, 1) if len(p) > n else None
    return {"fed_13w_pct": imp("WALCL", 13), "ecb_13w_pct": imp("ECBASSETSW", 13),
            "boj_13w_pct": imp("JPNASSETS", 3)}


CFTC_URL = "https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/signals"
HISTORY_KEY = "data/cycle-clock-history.json"


def _update_history(snapshot):
    """Append today's snapshot to the persisted self-history, dedup by date, cap length."""
    try:
        hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HISTORY_KEY)["Body"].read())
        if not isinstance(hist, list):
            hist = []
    except Exception:
        hist = []
    hist = [h for h in hist if h.get("date") != snapshot["date"]]
    hist.append(snapshot)
    hist = hist[-300:]
    try:
        s3.put_object(Bucket=BUCKET, Key=HISTORY_KEY, Body=json.dumps(hist, default=str).encode(),
                      ContentType="application/json", CacheControl="no-cache, max-age=0")
    except Exception as e:
        print(f"[cycle-clock] history write failed: {str(e)[:80]}")
    return hist


def _coord_series_long(n=84):
    """Like _coord_series but returns the last n months (default 7y) for backtesting."""
    def blend(a, b):
        da, db = dict(a), dict(b); ks = sorted(set(da) & set(db))
        return {k: round((da[k] + db[k]) / 2, 2) for k in ks}
    g = blend(_z_series(_yoy(_fred("INDPRO"))), _z_series(_yoy(_fred("PAYEMS"))))
    f = blend(_z_series(_yoy(_fred("CPIAUCSL"))), _z_series(_yoy(_fred("PCEPILFE"))))
    ks = sorted(set(g) & set(f))[-n:]
    return [{"m": k[:7], "g": g[k], "i": f[k]} for k in ks]


def _spy_monthly():
    """Month-end S&P 500 closes from FRED SP500 (10y daily) — deeper history than the
    Polygon key exposes, so the quadrant backtest covers multiple regimes."""
    pts = _fred("SP500", start="2015-01-01")
    bymo = {}
    for d, v in pts:
        bymo[d[:7]] = v  # pts are sorted ascending → last write per month = month-end close
    return sorted(bymo.items())


def _spy_daily(days=430):
    import datetime as _dt
    end = _dt.date.today(); start = end - _dt.timedelta(days=days)
    u = (f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/day/{start}/{end}"
         f"?adjusted=true&sort=asc&limit=1000&apiKey={POLY_KEY}")
    res = json.loads(urllib.request.urlopen(u, timeout=12).read()).get("results") or []
    return [(_dt.datetime.utcfromtimestamp(x["t"] / 1000).date().isoformat(), x["c"]) for x in res]


def _quadrant_backtest():
    """Real backtest: label each of the last ~7y of growth×inflation months by quadrant and
    measure what SPY actually did over the next 1m / 3m. Grades the clock's core macro call."""
    coords = _coord_series_long()
    months = _spy_monthly()
    if not coords or not months:
        return None
    idx = {m: i for i, (m, _) in enumerate(months)}
    agg = {}
    for c in coords:
        q = _quad2d(c["g"], c["i"])
        if q is None or c["m"] not in idx:
            continue
        i = idx[c["m"]]
        if i + 1 >= len(months):
            continue
        c0 = months[i][1]; f1 = (months[i + 1][1] / c0 - 1) * 100
        f3 = (months[i + 3][1] / c0 - 1) * 100 if i + 3 < len(months) else None
        agg.setdefault(q, []).append((f1, f3))
    out = {}
    for q, lst in agg.items():
        f1s = [a for a, _ in lst]; f3s = [b for _, b in lst if b is not None]
        out[q] = {"n": len(lst),
                  "avg_fwd_1m": round(sum(f1s) / len(f1s), 2) if f1s else None,
                  "avg_fwd_3m": round(sum(f3s) / len(f3s), 2) if f3s else None,
                  "pct_pos_1m": round(100 * sum(1 for a in f1s if a > 0) / len(f1s)) if f1s else None}
    return {"by_quadrant": out, "n_months": len(coords), "lookback_years": round(len(coords) / 12, 1)}


def _grade_posture_log(hist, spy):
    """Forward-grade each matured posture snapshot against SPY's 21-session forward move."""
    import bisect
    if not spy:
        return {"n_graded": 0, "status": "no_price_data"}
    dates = [d for d, _ in spy]; closes = [c for _, c in spy]
    graded = []
    for snap in hist:
        sc = snap.get("posture_score"); d = snap.get("date")
        if sc is None or abs(sc) < 15 or not d:
            continue
        j = bisect.bisect_left(dates, d)
        if j >= len(dates) or j + 21 >= len(closes):
            continue
        f = (closes[j + 21] / closes[j] - 1) * 100
        correct = (sc < 0 and f < 0) or (sc > 0 and f > 0)
        graded.append({"date": d, "score": sc, "fwd_spy_21d": round(f, 2), "correct": bool(correct)})
    n = len(graded)
    roff = [g["fwd_spy_21d"] for g in graded if g["score"] < 0]
    ron = [g["fwd_spy_21d"] for g in graded if g["score"] > 0]
    return {"n_graded": n, "horizon_sessions": 21,
            "status": "accumulating" if n < 5 else "live",
            "hit_rate": round(100 * sum(1 for g in graded if g["correct"]) / n) if n else None,
            "avg_fwd_when_riskoff": round(sum(roff) / len(roff), 2) if roff else None,
            "avg_fwd_when_riskon": round(sum(ron) / len(ron), 2) if ron else None,
            "recent": graded[-12:]}


def _cftc_signals():
    """Live COT/CFTC futures-positioning read from the positioning agent (Lambda URL)."""
    try:
        return json.loads(urllib.request.urlopen(CFTC_URL, timeout=12).read())
    except Exception:
        return None


QUAD_PHASE = {
    "REFLATION":      ("EARLY CYCLE",   1, "growth recovering, inflation rising off lows"),
    "GOLDILOCKS":     ("MID CYCLE",     2, "growth firm, inflation contained — the sweet spot"),
    "OVERHEAT":       ("LATE-MID CYCLE", 2.5, "growth strong but inflation accelerating"),
    "STAGFLATION":    ("LATE CYCLE",    3, "growth flat/slowing while inflation runs — classic late cycle"),
    "DEFLATION-BUST": ("DOWNTURN",      4, "growth and inflation both falling — contraction/recession risk"),
}


def lambda_handler(event, context):
    t0 = time.time()
    avail, stale = {}, []

    def load(key, label):
        d = _read(key)
        if d is None:
            avail[label] = "MISSING"
            return {}
        age = _age_days(d.get("generated_at") or d.get("as_of") or d.get("asof"))
        avail[label] = f"ok ({age}d)" if age is not None else "ok"
        if age is not None and age > 3.5:
            stale.append(f"{label} {age}d stale")
        return d

    uscy = load("data/us-cycle.json", "us_cycle")
    gbc = load("data/global-business-cycle.json", "global_cycle")
    now = load("data/macro-nowcast.json", "nowcast")
    reg = load("data/regime.json", "macro_regime")
    play = load("data/regime-playbook.json", "regime_playbook")
    gliq = load("data/global-liquidity.json", "global_liquidity")
    lflow = load("data/liquidity-flow.json", "liquidity_flow")
    lce = load("data/liquidity-credit-engine.json", "liquidity_credit")
    plumb = load("data/eurodollar-plumbing.json", "plumbing")
    tnoise = load("data/treasury-noise.json", "treasury_noise")
    crisis = load("data/crisis-composite.json", "crisis_composite")
    canary = load("data/crisis-canaries.json", "crisis_canaries")
    gstress = load("data/global-stress.json", "global_stress")
    sysstress = load("data/systemic-stress.json", "systemic_stress")
    rmap = load("data/regime-map.json", "risk_map")
    rrisk = load("data/risk-regime.json", "risk_regime")
    msurp = load("data/macro-surprise.json", "macro_surprise")
    ycurve = load("data/yield-curve.json", "yield_curve")
    credit = load("data/credit-stress.json", "credit_stress")
    secrot = load("data/sector-rotation.json", "sector_rotation")
    rcomp = load("data/regime-composite.json", "regime_composite")
    fomc = load("data/fomc-reaction.json", "fomc_reaction")
    sovf = load("data/sovereign-fiscal.json", "sovereign_fiscal")
    sfails = load("data/settlement-fails.json", "settlement_fails")
    analogs_d = load("data/historical-analogs.json", "historical_analogs")
    vixc = load("data/vix-curve.json", "vix_curve")
    dollar = load("data/dollar-radar.json", "dollar_radar")
    epsrev = load("data/eps-revision-velocity.json", "eps_revisions")
    # hard-data recession cluster (income-side GDP, freight, business investment)
    bea_hd = load("data/bea-economic.json", "bea_hard_data")
    mleads_hd = load("data/macro-leads.json", "macro_leads_hd")
    census_hd = load("data/census-economic.json", "census_hard_data")
    bls_hd = load("data/bls-labor.json", "bls_labor_hd")
    # Phase 6 — broaden the synthesis across the fleet
    fedspeak = load("data/fed-speak.json", "fed_speak")
    fednlp = load("data/fed-nlp.json", "fed_nlp")
    movex = load("data/move-index.json", "move_index")
    bondvol = load("data/bond-vol.json", "bond_vol")
    volreg = load("data/vol-regime.json", "vol_regime")
    vvix = load("data/vvix-vov-regime.json", "vvix")
    skewtail = load("data/skew-tail-hedging.json", "skew_tail")
    aaii = load("data/aaii-sentiment.json", "aaii")
    retail = load("data/retail-sentiment.json", "retail_sentiment")
    crediteq = load("data/credit-equity-divergence.json", "credit_equity")
    breadth = load("data/breadth-thrust.json", "breadth_thrust")
    goldeq = load("data/gold-equity-rotation.json", "gold_equity")
    china = load("data/china-liquidity.json", "china_liquidity")
    liqpulse = load("data/liquidity-pulse.json", "liquidity_pulse")
    liqinfl = load("data/liquidity-inflection.json", "liquidity_inflection")
    yencarry = load("data/yen-carry.json", "yen_carry")
    xaregime = load("data/cross-asset-regime.json", "cross_asset_regime")
    ticflows = load("data/tic-flows.json", "tic_flows")
    laborlead = load("data/labor-leading.json", "labor_leading")
    actnow = load("data/activity-nowcast.json", "activity_nowcast")
    conspulse = load("data/consumer-pulse.json", "consumer_pulse")
    bankstress = load("data/bank-stress.json", "bank_stress")
    commcurves = load("data/commodity-curves.json", "commodity_curves")
    # Phase 7 — Fed path, COT positioning, stress scenarios, tail risk
    fedwatch = load("data/fedwatch.json", "fedwatch")
    stressscen = load("data/stress-scenarios.json", "stress_scenarios")
    tailrisk = load("data/tail-risk.json", "tail_risk")
    cissstress = load("data/ciss-stress.json", "ciss")
    corrbreaks = load("data/correlation-breaks.json", "correlation_breaks")
    firmstress = load("data/firm-stress.json", "firm_stress")
    cdvol = load("data/crypto-dvol.json", "crypto_dvol")
    cfund = load("data/crypto-funding.json", "crypto_funding")
    cliq = load("data/crypto-liquidity.json", "crypto_liquidity")
    cropts = load("data/crypto-options-surface.json", "crypto_options")
    cminers = load("data/crypto-miners.json", "crypto_miners")
    cbasis = load("data/crypto-basis.json", "crypto_basis")
    cxflow = load("data/crypto-exchange-flows.json", "crypto_exchange_flows")
    ccot = load("data/crypto-cot.json", "crypto_cot")
    conchain = load("data/onchain-ratios.json", "crypto_onchain")
    cspeg = load("data/crypto-stablecoin-peg.json", "crypto_stablecoin_peg")
    cprem = load("data/coinbase-premium.json", "crypto_coinbase_premium")
    cetf = load("data/crypto-etf-flows.json", "crypto_etf_flows")
    chl = load("data/hyperliquid-perps.json", "hyperliquid_perps")
    cgex = load("data/crypto-gex.json", "crypto_gex")
    bneck = load("data/bottleneck-boom.json", "bottleneck")
    _bphases = (_get(bneck, "capital_cycle_phase_counts") or {})
    _bflood = (_get(bneck, "capacity_flood_warnings") or [])
    _bcure = (_get(bneck, "cure_for_low_prices") or [])
    _bearly = (_get(bneck, "early_bottleneck_calls") or [])
    cftc = _cftc_signals()

    # ───────────────────────── CYCLE POSITION ─────────────────────────
    quad = _get(reg, "current", "quadrant")
    phase_label, phase_n, phase_desc = QUAD_PHASE.get(quad, ("UNKNOWN", 0, "macro regime unavailable"))
    growth_mom = _get(reg, "current", "growth_6m_momentum")
    infl_mom = _get(reg, "current", "inflation_6m_momentum")
    liq_state_macro = _get(reg, "current", "liquidity_state")
    next3 = _get(reg, "current", "next_3m_probabilities", default={})

    nowcast_regime = _get(now, "regime")
    nowcast_score = _get(now, "normalized_score")
    us_score = _get(uscy, "cycle_score", "score_0_100")
    us_level = _get(uscy, "cycle_score", "level")
    gbc_phase = _get(gbc, "aggregate", "global_phase")
    gbc_cli = _get(gbc, "aggregate", "global_avg_cli")
    gbc_expansion_breadth = _get(gbc, "aggregate", "expansion_breadth_pct")

    # late-stage froth overlay from US-cycle valuation/leverage z-scores
    froth = []
    for comp in _get(uscy, "cycle_score", "components", default=[]):
        if comp.get("id") in ("buffett", "margin", "real_10y", "term_premium") and (comp.get("z") or 0) >= 0.85:
            froth.append(f"{comp['id']} z+{comp['z']}")
    growth_dir = ("slowing" if (nowcast_regime == "SLOWING" or (growth_mom or 0) < 0.1)
                  else "accelerating" if (growth_mom or 0) > 0.3 else "flat")
    infl_dir = ("rising" if (infl_mom or 0) > 0.3 else "falling" if (infl_mom or 0) < -0.3 else "stable")

    # confidence = agreement among US-cycle WATCH/late, nowcast slowing, stagflation, froth
    late_votes = sum([quad in ("STAGFLATION", "DEFLATION-BUST", "OVERHEAT"),
                      us_level in ("WATCH", "CAUTION", "ELEVATED"),
                      nowcast_regime in ("SLOWING", "CONTRACTION RISK"),
                      len(froth) >= 2])
    cycle_conf = "high" if late_votes >= 3 else "moderate" if late_votes == 2 else "low"

    # ── 2D growth×inflation coordinates + 12-month trail (FRED momentum z) ──
    trail = _coord_series()
    coord = trail[-1] if trail else None
    g_now = coord["g"] if coord else None
    i_now = coord["i"] if coord else None
    quad2d = _quad2d(g_now, i_now)
    # data-surprise tilt (macro-surprise engine) — complementary near-term read
    surprise = {"growth_z": _get(msurp, "growth_z"), "inflation_z": _get(msurp, "inflation_z"),
                "composite_z": _get(msurp, "composite_z"), "regime": _get(msurp, "regime")}
    # asset leadership for the current quadrant (canonical clock) + system playbook leaders
    assets = None
    if quad2d in QUAD_ASSETS:
        a = QUAD_ASSETS[quad2d]
        leaders = _get(play, "proven_signal_leaders", default=[])
        assets = {"clock_phase": a["clock"], "lead": a["lead"], "lag": a["lag"],
                  "system_leaders": leaders[:5] if isinstance(leaders, list) else []}
    # recession-probability composite (yield curve + credit + nowcast + sector rotation)
    rp_votes, rp_max = 0.0, 0.0
    yc_regime = _get(ycurve, "regime"); yc_inv = _get(ycurve, "inversion_flags", default={})
    if yc_regime is not None:
        rp_max += 35
        if "INVERT" in str(yc_regime).upper() or (isinstance(yc_inv, dict) and any(yc_inv.values())):
            rp_votes += 35
        elif "BULL" in str(yc_regime).upper() and "STEEP" in str(yc_regime).upper():
            rp_votes += 20
    cr_regime = _get(credit, "composite_regime")
    if cr_regime is not None:
        rp_max += 25
        if str(cr_regime).upper() in ("STRESS", "WIDENING", "ELEVATED", "RISK-OFF"):
            rp_votes += 25
    if nowcast_regime is not None:
        rp_max += 25
        if nowcast_regime in ("SLOWING", "CONTRACTION RISK"):
            rp_votes += (25 if nowcast_regime == "CONTRACTION RISK" else 12)
    sr_app = _get(secrot, "risk_appetite")
    if sr_app is not None:
        rp_max += 15
        if str(sr_app).upper() in ("RISK-OFF", "DEFENSIVE", "DEFENSIVE LEADERSHIP"):
            rp_votes += 15
    # ── hard-data recession cluster: GDP-GDI gap (income vs output) + freight (goods
    # economy) + core capex (business investment). Orthogonal to the yield-curve/credit/
    # nowcast votes above; each adds its own max so the composite stays balanced —
    # strong capex correctly contributes non-recessionary evidence, not just votes. ──
    hard_data_recession = {}
    _gg = _get(bea_hd, "gdp_gdi", default={}) or {}
    _gdi_gap = _gg.get("gap_pct")
    if _gdi_gap is not None:
        hard_data_recession["gdp_gdi_gap_pct"] = _gdi_gap
        hard_data_recession["real_gdi_pct"] = _gg.get("real_gdi_pct")
        rp_max += 12
        if _gdi_gap > 1.0:
            rp_votes += 12       # GDI materially weaker than GDP — income-side recession tell
        elif _gdi_gap > 0.5:
            rp_votes += 6
    _fr = _get(mleads_hd, "freight_activity", "composite", default={}) or {}
    _fr_read = _fr.get("read")
    if _fr_read:
        hard_data_recession["freight_read"] = _fr_read
        hard_data_recession["freight_yoy_pct"] = _fr.get("avg_yoy_pct")
        rp_max += 10
        if "CONTRACT" in _fr_read:
            rp_votes += 10       # freight recession — goods economy contracting
        elif "SOFT" in _fr_read or "FLAT" in _fr_read:
            rp_votes += 4
    _cx = _get(census_hd, "manufacturing_orders", "core_capex_orders", default={}) or {}
    _cx_yoy = _cx.get("yoy_pct")
    if _cx_yoy is not None:
        hard_data_recession["core_capex_yoy_pct"] = _cx_yoy
        rp_max += 10
        if _cx_yoy < 0:
            rp_votes += 10       # capex contracting — investment-cycle recession signal
        elif _cx_yoy < 2:
            rp_votes += 4
        # strong capex (>2% YoY) adds max but zero votes = active non-recessionary evidence
    if hard_data_recession:
        _weak = sum(1 for c in [
            (_gdi_gap is not None and _gdi_gap > 1.0),
            (_fr_read and "CONTRACT" in _fr_read),
            (_cx_yoy is not None and _cx_yoy < 0)] if c)
        hard_data_recession["recession_signals"] = _weak
        hard_data_recession["read"] = ("HARD DATA CONFIRMS RECESSION RISK" if _weak >= 2
                                       else "HARD DATA MIXED — one soft read" if _weak == 1
                                       else "HARD DATA NOT RECESSIONARY")
    # ── manufacturing cycle LEAD: order backlog-to-shipments + demand-vs-supply
    # spread (from bottleneck-boom's decades-deep Census M3 pressure) + Fed G.17
    # capacity utilization. These turn BEFORE production and GDP — the earliest
    # boom / slowdown / bust tell. Rising backlog + widening demand-supply spread +
    # tightening capacity = boom building; backlog rolling over + slack = recession lead. ──
    mfg_lead = {}
    _tmfg = _get(bneck, "industry_pressure", "TOTAL_MFG", default={}) or {}
    if _tmfg:
        for k in ("backlog_to_shipments", "backlog_ratio_z", "backlog_yoy_pct",
                  "new_orders_yoy_pct", "demand_supply_spread_pp", "spread_state",
                  "bottleneck_forming", "direction"):
            mfg_lead[k] = _tmfg.get(k)
    _cu = _fred("MCUMFN")
    _cu_chg = None
    if _cu and len(_cu) >= 13:
        _cu_now = _cu[-1][1]; _cu_chg = round(_cu_now - _cu[-13][1], 1)
        mfg_lead["capacity_utilization_pct"] = round(_cu_now, 1)
        mfg_lead["capacity_util_yoy_chg_pp"] = _cu_chg
        mfg_lead["capacity_read"] = ("TIGHT — late-cycle / inflationary" if _cu_now >= 79
                                     else "SLACK — spare capacity / slowdown" if _cu_now < 76 else "NORMAL")
    if mfg_lead:
        _boom = _bust = 0
        if _tmfg.get("bottleneck_forming"):
            _boom += 1
        if _tmfg.get("spread_state") == "CLOSING":
            _bust += 1
        _noy = _tmfg.get("new_orders_yoy_pct")
        if _noy is not None:
            _boom += (1 if _noy > 4 else 0); _bust += (1 if _noy < 0 else 0)
        _brz = _tmfg.get("backlog_ratio_z")
        if _brz is not None:
            _boom += (1 if _brz > 0.5 else 0); _bust += (1 if _brz < -0.5 else 0)
        if _cu_chg is not None:
            _boom += (1 if _cu_chg > 1.5 else 0); _bust += (1 if _cu_chg < -1.5 else 0)
        mfg_lead["lead_verdict"] = (
            "BOOM BUILDING — orders & backlog outrunning supply" if (_boom >= 2 and _boom > _bust)
            else "CONTRACTING — orders & backlog rolling over (recession lead)" if (_bust >= 2 and _bust > _boom)
            else "SLOWING" if _bust > _boom else "EXPANDING" if _boom > _bust else "NEUTRAL")
        mfg_lead["note"] = ("manufacturing backlog-to-shipments + demand-supply spread + Fed G.17 capacity "
                            "utilization — these lead production/GDP; the earliest boom/slowdown/bust tell")
        # feed the LEADING verdict into the recession composite (balanced — a boom adds max + 0 votes)
        rp_max += 12
        if "CONTRACTING" in mfg_lead["lead_verdict"]:
            rp_votes += 12
        elif mfg_lead["lead_verdict"] == "SLOWING":
            rp_votes += 5
    recession_prob = round(rp_votes / rp_max * 100) if rp_max else None
    # ── profit / margin cycle: corporate profits (BEA) vs unit labor costs (BLS).
    # Classic investment-clock signal — late cycle is defined by margin compression
    # (labor costs outrunning profits). No dedicated aggregate-margin engine exists,
    # so the macro clock is its natural home. Read-only, informational (not a
    # recession vote — margin compression already shows up via nowcast/sector). ──
    profit_margin_cycle = None
    _cp_yoy = _get(bea_hd, "corporate_profits", "yoy_pct")
    _ulc_qoq = _get(bls_hd, "summary", "unit_labor_costs_qoq_pct")
    _prod_qoq = _get(bls_hd, "summary", "productivity_qoq_pct")
    if _cp_yoy is not None or _ulc_qoq is not None:
        if _cp_yoy is not None and _ulc_qoq is not None:
            # profits growing well while labor costs contained = margin tailwind
            if _cp_yoy >= 6 and _ulc_qoq <= 2.5:
                mr = "EXPANDING — profits outrunning labor costs (margin tailwind)"
            elif _cp_yoy < 0 or _ulc_qoq >= 4:
                mr = "COMPRESSING — labor costs eating margins (late-cycle tell)"
            else:
                mr = "STABLE"
        else:
            mr = "PARTIAL"
        profit_margin_cycle = {
            "corp_profits_yoy_pct": _cp_yoy, "unit_labor_costs_qoq_pct": _ulc_qoq,
            "productivity_qoq_pct": _prod_qoq, "read": mr,
            "note": "aggregate corporate profits vs unit labor costs — margins compress late-cycle; "
                    "expanding margins argue against imminent earnings recession",
        }
    sahm = _sahm()
    yc_decomp = {"real_10y_pct": _get(ycurve, "real_yields", "10Y_REAL", "value_pct"),
                 "breakeven_10y_pct": _get(ycurve, "inflation_expectations", "10Y_BREAKEVEN", "value_pct"),
                 "term_premium_bps": _get(ycurve, "term_premium_proxy_bps"),
                 "spreads_bps": _get(ycurve, "spreads_bps")}
    yield_curve = {
        "regime": yc_regime, "regime_desc": _get(ycurve, "regime_description"),
        "as_of": _get(ycurve, "as_of_date"),
        "curve_points": [{"tenor": p.get("tenor"), "years": p.get("years"), "yield_pct": p.get("yield_pct"),
                          "chg_20d_bps": p.get("chg_20d_bps")}
                         for p in (_get(ycurve, "curve_points", default=[]) or [])],
        "spreads_bps": _get(ycurve, "spreads_bps"), "inversion_flags": yc_inv,
        "term_premium_bps": _get(ycurve, "term_premium_proxy_bps"),
        "real_10y_pct": yc_decomp["real_10y_pct"], "breakeven_10y_pct": yc_decomp["breakeven_10y_pct"],
    }
    vol_regime = _get(vixc, "composite_regime")
    dollar_regime = _get(dollar, "regime") or _get(dollar, "regime_note")
    _eps_top = _get(epsrev, "summary", "top_25_overall", default=[]) or []
    eps_breadth = ({"high_velocity_count": len(_eps_top),
                    "top_names": [x.get("symbol") for x in _eps_top[:8] if isinstance(x, dict)]}
                   if _eps_top else None)

    # ── cross-asset confirmation: is the quadrant's prescription actually working? ──
    cross = _cross_asset()
    ca_confirm = None
    if cross and quad2d:
        exp = EXPECT_LEAD.get(quad2d, [])
        spy = next((x["ret_1m"] for x in cross if x["ticker"] == "SPY"), 0.0)
        leaders = [x for x in cross if x["ticker"] in exp]
        if leaders:
            beat = sum(1 for x in leaders if x["ret_1m"] > spy)
            frac = beat / len(leaders)
            status = "CONFIRMED" if frac >= 0.6 else "CONTRADICTED" if frac <= 0.34 else "MIXED"
            lead_labels = [x["label"] for x in cross if x["ticker"] in exp]
            ca_confirm = {"status": status, "expected": lead_labels, "beating_spy": f"{beat}/{len(leaders)}",
                          "note": (f"{quad2d.title()} historically favours {', '.join(lead_labels[:3])}; "
                                   f"{beat} of {len(leaders)} are beating SPY over the last month — "
                                   f"{'tape confirms the regime read' if status == 'CONFIRMED' else 'tape contradicts the regime read (prescription is the worst-performing book right now)' if status == 'CONTRADICTED' else 'mixed confirmation'}.")}
    # ── global central-bank liquidity (G3 impulse + global-liquidity engine) ──
    cb_imp = _cb_impulse()
    global_liq = {"regime": _get(gliq, "regime"), "global_liquidity_index": _get(gliq, "global_liquidity_index"),
                  "global_impulse_13w_pct": _get(gliq, "global_impulse_13w_pct"), "cb_impulse_13w": cb_imp}
    # ── scenario playbook: next-3m quadrant odds → asset plan per scenario ──
    scenario_playbook = ([{"scenario": k, "odds_pct": v, "assets": SCENARIO_ASSETS.get(k, [])}
                          for k, v in sorted((next3 or {}).items(), key=lambda x: -x[1])] if next3 else [])

    # ── Phase 6: rates / Fed / volatility complex ──
    rates_fed_vol = {
        "fed_tone": _get(fedspeak, "aggregate", "interpretation"),
        "fed_sentiment": _get(fedspeak, "aggregate", "avg_sentiment"),
        "fed_drift": _get(fednlp, "drift", "classification"), "fed_drift_z": _get(fednlp, "drift", "drift_z"),
        "move_level": _get(movex, "level"), "move_pctile": _get(movex, "percentile"), "move_regime": _get(movex, "regime"),
        "bond_vol_regime": _get(bondvol, "regime"), "bond_vol_posture": _get(bondvol, "risk_posture"),
        "bond_vol_signal": _get(bondvol, "term_structure", "signal"),
        "equity_vol_regime": _get(volreg, "composite_regime"), "equity_vol_score": _get(volreg, "composite_score"),
        "vov_state": _get(vvix, "state"), "tail_state": _get(skewtail, "state"),
    }
    # ── positioning / sentiment / internals ──
    aaii_l = _get(aaii, "latest", default={}) or {}; aaii_x = _get(aaii, "extremes", default={}) or {}
    positioning = {
        "aaii_bull_pct": round((aaii_l.get("bullish") or 0) * 100) if aaii_l.get("bullish") is not None else None,
        "aaii_bear_pct": round((aaii_l.get("bearish") or 0) * 100) if aaii_l.get("bearish") is not None else None,
        "aaii_spread_z": _get(aaii, "z_scores", "spread"),
        "aaii_extreme": ("EXTREME_BULL" if aaii_x.get("is_bullish_extreme") else
                         "EXTREME_BEAR" if aaii_x.get("is_bearish_extreme") else "normal"),
        "aaii_note": _get(aaii, "interpretation"),
        "retail_regime": _get(retail, "market_regime"), "retail_note": _get(retail, "market_regime_signal"),
        "credit_equity_state": _get(crediteq, "state"), "credit_equity_note": _get(crediteq, "regime_explanation"),
        "breadth_thrust_state": _get(breadth, "state"), "breadth_thrust_strength": _get(breadth, "signal_strength"),
        "gold_equity_state": _get(goldeq, "state"), "gold_equity_note": _get(goldeq, "regime_explanation"),
    }
    # ── growth / recession depth ──
    growth_depth = {
        "labor_regime": _get(laborlead, "regime"), "labor_note": _get(laborlead, "interpretation"),
        "activity_index": _get(actnow, "activity_index"), "activity_regime": _get(actnow, "regime"),
        "activity_momentum": _get(actnow, "momentum"),
        "consumer_index": _get(conspulse, "pulse_index"), "consumer_regime": _get(conspulse, "regime"),
        "bank_stress_score": _get(bankstress, "bank_stress_score"), "bank_stress_regime": _get(bankstress, "regime"),
        "reserves_to_gdp_pct": _get(bankstress, "reserve_adequacy", "reserves_to_gdp_pct"),
        "reserves_read": _get(bankstress, "reserve_adequacy", "read"),
    }
    # ── global liquidity → G4 (China) + pulse + inflection ──
    global_liq["china"] = {"regime": _get(china, "regime"), "credit_impulse_pp": _get(china, "credit_impulse", "value_pp"),
                           "m2_yoy_pct": _get(china, "money", "m2_yoy_pct")}
    global_liq["pulse"] = {"liquidity_regime": _get(liqpulse, "composites", "liquidity_regime"),
                           "liquidity_score": _get(liqpulse, "composites", "liquidity_score"),
                           "summary": _get(liqpulse, "summary")}
    global_liq["inflection"] = {"usd_state": _get(liqinfl, "usd", "state"), "usd_impulse_z": _get(liqinfl, "usd", "impulse_z"),
                                "last_flip": _get(liqinfl, "usd", "last_flip")}
    # ── cross-asset extensions: yen carry, correlation regime, foreign demand, commodity curve ──
    _xa_alerts = _get(xaregime, "alerts", default=[]) or []
    _xa_breaks = _get(xaregime, "correlation_breaks", default=[]) or []
    cross_asset_risk = {
        "yen_carry": {"unwind_score": _get(yencarry, "unwind_risk_score"), "unwind_label": _get(yencarry, "unwind_risk_label"),
                      "regime": _get(yencarry, "carry_regime"), "headline": _get(yencarry, "headline"),
                      "trigger": (_get(yencarry, "triggers", default=[]) or [None])[0]},
        "correlation_regime": {"r20d": _get(xaregime, "regime_20d", "regime"), "r60d": _get(xaregime, "regime_60d", "regime"),
                               "top_alert": (_xa_alerts[0].get("msg") if _xa_alerts else None),
                               "breaks": [b.get("pair") for b in _xa_breaks[:3]]},
        "tic": {"stress": _get(ticflows, "composite_tic_stress"), "regime": _get(ticflows, "regime"),
                "note": _get(ticflows, "interpretation")},
        "commodity_curve": {"regime": _get(commcurves, "composite_regime"), "signal": _get(commcurves, "composite_signal")},
        "correlation_break": {"signal": _get(corrbreaks, "signal"), "z": _get(corrbreaks, "frobenius_z_score_1y")},
    }
    # ── Phase 7: implied Fed path (fedwatch) + tail risk into the rates/vol block ──
    _nm = _get(fedwatch, "next_meeting", default={}) or {}
    rates_fed_vol["fed_path"] = {
        "current_midpoint": _get(fedwatch, "current_fed_funds_range", "midpoint"),
        "next_date": _nm.get("date"), "next_days": _nm.get("days_until"),
        "implied_move_bps": _nm.get("implied_move_bps"), "post_rate_pct": _nm.get("implied_post_meeting_rate_pct"),
        "summary_6mo": _get(fedwatch, "next_6mo_summary"),
    }
    rates_fed_vol["tail_gauge"] = _get(tailrisk, "system_tail_gauge")
    rates_fed_vol["tail_regime"] = _get(tailrisk, "tail_regime")
    rates_fed_vol["tail_valuation"] = _get(tailrisk, "tail_valuation")
    # crypto implied vol (DVOL) into the volatility complex
    rates_fed_vol["crypto_dvol"] = _get(cdvol, "btc", "dvol")
    rates_fed_vol["crypto_dvol_regime"] = _get(cdvol, "btc", "regime")
    rates_fed_vol["crypto_dvol_pctile"] = _get(cdvol, "btc", "pctile_1y")
    rates_fed_vol["crypto_dvol_trend"] = _get(cdvol, "btc", "trend")
    # ── consolidated crypto block: liquidity tide + perp leverage + implied vol ──
    crypto = {
        "dvol_btc": _get(cdvol, "btc", "dvol"), "dvol_btc_pctile": _get(cdvol, "btc", "pctile_1y"),
        "dvol_btc_regime": _get(cdvol, "btc", "regime"), "dvol_btc_trend": _get(cdvol, "btc", "trend"),
        "dvol_eth": _get(cdvol, "eth", "dvol"), "vol_regime": _get(cdvol, "crypto_vol_regime"),
        "funding_regime": _get(cfund, "composite_regime"), "funding_signal": _get(cfund, "composite_signal"),
        "funding_composite": _get(cfund, "market_composite"),
        "squeeze_candidates": [{"coin": c.get("coin"), "z": c.get("z_score"), "apr": c.get("annualized_pct"),
                                "regime": c.get("regime")}
                               for c in (_get(cfund, "squeeze_candidates", default=[]) or [])[:5]],
        "liquidity_regime": _get(cliq, "regime"), "ssr": _get(cliq, "ssr", "value"),
        "ssr_pctile": _get(cliq, "ssr", "percentile_2y"), "ssr_read": _get(cliq, "ssr", "interpretation"),
        "fear_greed": _get(cliq, "fear_greed", "value"), "fear_greed_class": _get(cliq, "fear_greed", "classification"),
        "directional_read": _get(cliq, "directional_read"),
        # options surface (skew / 25d risk reversal / vol term structure)
        "rr_25d": _get(cropts, "btc", "headline_30d", "rr_25d"),
        "skew_read": _get(cropts, "btc", "interpretation"),
        "vol_term_regime": _get(cropts, "btc", "term_structure", "regime"),
        "eth_rr_25d": _get(cropts, "eth", "headline_30d", "rr_25d"),
        # miner economics (hash ribbons + Puell)
        "hash_ribbon": _get(cminers, "hash_ribbons", "state"),
        "days_in_capitulation": _get(cminers, "hash_ribbons", "days_in_capitulation"),
        "puell": _get(cminers, "puell", "value"), "puell_zone": _get(cminers, "puell", "zone"),
        "miners_read": _get(cminers, "interpretation"),
        # futures basis / cash-and-carry
        "cash_carry_3m": _get(cbasis, "btc", "cash_and_carry_yield_3m_pct"),
        "carry_regime": _get(cbasis, "btc", "regime"),
        "eth_cash_carry_3m": _get(cbasis, "eth", "cash_and_carry_yield_3m_pct"),
        "eth_funding_ann": _get(cbasis, "eth", "funding_annualized_pct"),
        # exchange netflows (accumulation/distribution) — note: "outflow=bullish" measured INVERTED ~3.5y
        "exchange_flow_regime": _get(cxflow, "btc", "regime"),
        "exchange_flow_30d_pctile": _get(cxflow, "btc", "cum_30d_pctile"),
        "exchange_flow_study": _get(cxflow, "btc", "event_study", "verdict"),
        # CME institutional COT (asset managers vs leveraged funds)
        "cot_asset_mgr": _get(ccot, "btc", "asset_mgr", "read"),
        "cot_asset_mgr_pctile": _get(ccot, "btc", "asset_mgr", "net_pctile_3y"),
        "cot_lev_funds": _get(ccot, "btc", "lev_funds", "read"),
        "cot_divergence": _get(ccot, "btc", "divergence"),
        # on-chain valuation: realized price (aggregate cost basis) + NUPL
        "realized_price": _get(conchain, "btc", "realized_price") or _get(conchain, "realized_price"),
        "price_vs_realized_pct": _get(conchain, "btc", "price_vs_realized_pct") or _get(conchain, "price_vs_realized_pct"),
        "nupl": _get(conchain, "btc", "nupl") or _get(conchain, "nupl"),
        "nupl_zone": _get(conchain, "btc", "nupl_zone") or _get(conchain, "nupl_zone"),
        # stablecoin peg (depeg tail risk) + coinbase premium (US spot demand)
        "stablecoin_peg_status": _get(cspeg, "status"),
        "stablecoin_peg_gauge": _get(cspeg, "gauge"),
        "stablecoin_worst_depeg": _get(cspeg, "worst_depeg_pct"),
        "coinbase_premium_pct": _get(cprem, "btc", "premium_pct"),
        # spot ETF net flows — the marginal buyer (event-study CONFIRMED predictive)
        "etf_flow_btc_regime": _get(cetf, "btc_etf", "regime"),
        "etf_flow_btc_30d_usd": _get(cetf, "btc_etf", "cum_30d_usd"),
        "etf_flow_btc_pctile": _get(cetf, "btc_etf", "cum_30d_pctile"),
        "etf_flow_eth_regime": _get(cetf, "eth_etf", "regime"),
        "etf_flow_eth_30d_usd": _get(cetf, "eth_etf", "cum_30d_usd"),
        # Hyperliquid perp leverage gauge
        "hl_total_oi_usd": _get(chl, "total_oi_usd"),
        "hl_btc_funding_ann_pct": _get(chl, "btc", "funding_ann_pct"),
        "hl_leverage_regime": _get(chl, "leverage_regime"),
        "hl_liq_pressure": _get(chl, "liq_pressure_proxy"),
        # dealer gamma (GEX) positioning-by-strike
        "gex_btc_regime": _get(cgex, "btc", "regime"),
        "gex_btc_net_usd": _get(cgex, "btc", "net_gex_usd"),
        "gex_btc_flip": _get(cgex, "btc", "gamma_flip"),
        "gex_btc_spot_vs_flip": _get(cgex, "btc", "spot_vs_flip"),
        "gex_btc_call_wall": _get(cgex, "btc", "call_wall"),
        "gex_btc_put_wall": _get(cgex, "btc", "put_wall"),
        "gex_btc_max_pain": _get(cgex, "btc", "max_pain"),
    }
    # ── COT / CFTC futures positioning into the positioning block ──
    positioning["cot"] = {"score": _get(cftc, "positioning_score"), "risk_appetite": _get(cftc, "risk_appetite"),
                          "smart_money": _get(cftc, "smart_money"), "summary": _get(cftc, "summary")}
    # ── stress scenarios + tail + euro-area systemic + correlation regime ──
    def _names(lst):
        out = []
        for x in (lst or [])[:4]:
            out.append(x.get("ticker") or x.get("name") or x.get("symbol") if isinstance(x, dict) else x)
        return [o for o in out if o]
    stress_scenarios = {
        "top": _get(stressscen, "top_scenario"),
        "scenarios": [{"key": s.get("key"), "name": s.get("name"), "prob_pct": s.get("probability_pct"),
                       "winners": _names(s.get("winners")), "losers": _names(s.get("losers"))}
                      for s in (_get(stressscen, "scenarios", default=[]) or [])[:6]],
        "asset_impact_winners": [{"ticker": w.get("ticker"), "expected_return_pct": w.get("expected_return_pct")}
                                 for w in (_get(stressscen, "asset_impact", "top_5_winners", default=[]) or [])[:5]],
        "tail_gauge": _get(tailrisk, "system_tail_gauge"), "tail_regime": _get(tailrisk, "tail_regime"),
        "tail_valuation": _get(tailrisk, "tail_valuation"),
        "ea_ciss_regime": _get(cissstress, "ea_regime"), "ea_ciss": _get(cissstress, "ea_composite"),
        "correlation_signal": _get(corrbreaks, "signal"), "correlation_z": _get(corrbreaks, "frobenius_z_score_1y"),
    }
    # ── firm book reverse-stress: what breaks the book first ──
    _fsum = _get(firmstress, "summary", default={}) or {}
    _fscen = sorted([s for s in (_get(firmstress, "scenarios", default=[]) or []) if s.get("book_pnl_pct") is not None],
                    key=lambda x: x.get("book_pnl_pct", 0))[:5]
    _rs = _get(firmstress, "reverse_stress", default={}) or {}
    stress_scenarios["firm_book"] = {
        "posture": _get(firmstress, "posture"), "headline": _get(firmstress, "headline"),
        "soft_pct": _get(firmstress, "loss_limits", "soft_pct"), "hard_pct": _get(firmstress, "loss_limits", "hard_pct"),
        "net_pct": _fsum.get("firm_net_pct"), "gross_pct": _fsum.get("firm_gross_pct"),
        "annual_vol_pct": _fsum.get("annual_vol_pct"), "var_99_1d_pct": _fsum.get("var_99_1d_pct"),
        "n_names": _fsum.get("n_names_modelled"),
        "worst": [{"scenario": s.get("scenario"), "pnl_pct": s.get("book_pnl_pct")} for s in _fscen],
        "reverse": {
            "to_soft_mult": _get(_rs, "to_minus_15pct", "multiplier"), "to_soft_reachable": _get(_rs, "to_minus_15pct", "reachable"),
            "to_soft_interp": _get(_rs, "to_minus_15pct", "interpretation"),
            "to_hard_mult": _get(_rs, "to_minus_25pct", "multiplier"), "to_hard_reachable": _get(_rs, "to_minus_25pct", "reachable"),
        },
    }

    cycle = {
        "phase": phase_label, "phase_n": phase_n, "quadrant": quad, "description": phase_desc,
        "growth_direction": growth_dir, "inflation_direction": infl_dir,
        "froth_markers": froth, "confidence": cycle_conf,
        "us_cycle_score": us_score, "us_cycle_level": us_level,
        "global_phase": gbc_phase, "global_avg_cli": gbc_cli, "global_expansion_breadth_pct": gbc_expansion_breadth,
        "nowcast_regime": nowcast_regime, "macro_liquidity_state": liq_state_macro,
        "next_3m_quadrant_odds": next3,
        "coordinates": coord, "trail": trail, "quadrant_2d": quad2d,
        "headline_quadrant": quad2d or quad,
        "headline_phase": (COORD_PHASE.get(quad2d, (phase_label, phase_n))[0] if quad2d else phase_label),
        "macro_regime_quadrant": quad,
        "surprise_tilt": surprise, "asset_leadership": assets,
        "recession_prob_pct": recession_prob,
        "hard_data_recession": hard_data_recession or None,
        "manufacturing_cycle_lead": mfg_lead or None,
        "profit_margin_cycle": profit_margin_cycle,
        "sahm": sahm,
        "yield_curve_regime": yc_regime, "yield_curve_decomp": yc_decomp,
        "vol_regime": vol_regime, "dollar_regime": dollar_regime, "eps_revision_breadth": eps_breadth,
        "credit_regime": cr_regime,
        "sector_risk_appetite": sr_app, "regime_composite": _get(rcomp, "meta_regime"),
        "scenario_playbook": scenario_playbook,
    }

    # ──────────────────── LIQUIDITY-SQUEEZE RISK ────────────────────
    def num(v):
        return float(v) if isinstance(v, (int, float)) else None
    plumb_health = num(_get(plumb, "plumbing_health"))
    plumb_stress = (100 - plumb_health) if plumb_health is not None else None
    fund_stress = num(_get(tnoise, "treasury_stress")) or num(_get(tnoise, "funding_stress"))
    crisis_score = num(_get(crisis, "master_crisis_score"))
    canary_score = num(_get(canary, "composite_score"))
    gstress_idx = num(_get(gstress, "global_stress_index"))
    sys_score = num(_get(sysstress, "composite", "score_0_100"))

    comps = {"global_stress": (gstress_idx, 0.25), "crisis_composite": (crisis_score, 0.20),
             "plumbing": (plumb_stress, 0.20), "funding": (fund_stress, 0.15),
             "systemic": (sys_score, 0.10), "canaries": (canary_score, 0.10)}
    num_, den_ = 0.0, 0.0
    for v, w in comps.values():
        if v is not None:
            num_ += v * w; den_ += w
    base_squeeze = (num_ / den_) if den_ else None

    # liquidity-DIRECTION modifier: draining adds, easing subtracts
    impulse = num(_get(gliq, "global_impulse_13w_pct"))
    lce_liq_state = _get(lce, "interpretation", "pillars", "liquidity", "state")
    flow_regime = _get(lflow, "regime")
    net_30d = _get(lflow, "deltas", "1m", "net")
    plumb_fingerprint = _get(play, "current_fingerprint", "plumbing")
    direction_mod = 0.0
    flickers = []
    if impulse is not None and impulse < -0.5:
        direction_mod += min(10, abs(impulse) * 4); flickers.append(f"global liquidity impulse {impulse}% (draining)")
    if lce_liq_state and str(lce_liq_state).upper() in ("DRAINING", "TIGHTENING"):
        direction_mod += 4; flickers.append(f"credit-engine liquidity pillar {lce_liq_state}")
    if plumb_fingerprint and str(plumb_fingerprint).upper() == "TIGHTENING":
        direction_mod += 4; flickers.append("plumbing fingerprint TIGHTENING")
    if isinstance(net_30d, (int, float)) and net_30d < -30:
        direction_mod += 3; flickers.append(f"net liquidity {net_30d}B/30d (draining)")
    gstress_pctile = _get(gstress, "stress_momentum", "percentile")
    if isinstance(gstress_pctile, (int, float)) and gstress_pctile >= 60:
        flickers.append(f"global stress {gstress_pctile}th pctile (elevated)")
    # Fed net-liquidity decomposition + settlement-fails collateral flicker (Phase 2)
    netliq = _net_liquidity()
    if netliq and isinstance(netliq.get("net_13w_delta_bn"), (int, float)) and netliq["net_13w_delta_bn"] < -40:
        flickers.append(f"Fed net liquidity {netliq['net_13w_delta_bn']}B/13w (draining: TGA/RRP)")
    sf_sig = _get(sfails, "signal")
    if sf_sig and str(sf_sig).upper() in ("ELEVATED", "HIGH", "ACUTE", "STRESS", "STRAINED"):
        flickers.append(f"settlement fails {sf_sig} — {_get(sfails, 'headline', 'collateral scarcity')}")

    squeeze = round(min(100, (base_squeeze or 0) + direction_mod), 1) if base_squeeze is not None else None
    def squeeze_level(s):
        if s is None:
            return "UNKNOWN"
        if s < 20: return "LOW"
        if s < 35: return "LOW · WATCH"
        if s < 50: return "RISING"
        if s < 70: return "ELEVATED"
        if s < 85: return "HIGH"
        return "ACUTE"
    sq_level = squeeze_level(squeeze)

    liquidity = {
        "squeeze_risk_score": squeeze, "level": sq_level,
        "aggregate_liquidity_regime": _get(gliq, "regime"),
        "aggregate_liquidity_read": _get(gliq, "regime_read"),
        "impulse_13w_pct": impulse, "flow_regime": flow_regime, "net_30d_bn": net_30d,
        "net_liquidity": netliq,
        "flickers": flickers,
        "components": {"global_stress": gstress_idx, "crisis_composite": crisis_score,
                       "plumbing_health": plumb_health, "funding_stress": fund_stress,
                       "systemic_stress": sys_score, "crisis_canaries": canary_score,
                       "crisis_defcon": _get(crisis, "defcon_level"),
                       "global_stress_level": _get(gstress, "global_stress_level")},
    }

    # ──────────────────── CROSS-ASSET RISK (RORO) ────────────────────
    roro = num(_get(rrisk, "risk_regime_score"))
    risk = {
        "roro_score": roro,
        "regime": _get(rrisk, "risk_regime"),
        "posture": _get(rrisk, "posture"),
        "components": _get(rrisk, "components"),
        "tells": _get(rrisk, "tells"),
        "read": ("risk-on" if (roro or 0) > 15 else "risk-off" if (roro or 0) < -15 else "neutral"),
        "fomc_context": _get(fomc, "regime_context"),
    }

    # ───────────────────────── DIVERGENCES ─────────────────────────
    divergences = []
    if gbc_phase and "EXPANSION" in str(gbc_phase) and quad in ("STAGFLATION", "DEFLATION-BUST"):
        divergences.append(f"GROWTH SPLIT: global business cycle reads {gbc_phase} "
                           f"(CLI {gbc_cli}, {gbc_expansion_breadth}% breadth) while the US macro regime is "
                           f"{quad} — global growth engines bullish, US/inflation engines cautious.")
    if _get(gliq, "regime") in ("NEUTRAL", "EASING", "LOOSE") and lce_liq_state in ("DRAINING", "TIGHTENING"):
        divergences.append(f"LIQUIDITY SPLIT: aggregate central-bank liquidity is {_get(gliq, 'regime')}/flat, "
                           f"but the credit-engine liquidity pillar is {lce_liq_state} — a draining flicker under a calm surface.")
    rlabel = _get(rmap, "regime", "label")
    if rlabel == "BIFURCATED":
        divergences.append("CROSS-ASSET SPLIT: Risk Map BIFURCATED — equities broadly risk-on while crypto is risk-off.")
    _cotdiv = _get(ccot, "btc", "divergence")
    if _cotdiv and "cash-and-carry" in str(_cotdiv).lower():
        divergences.append("CRYPTO COT SPLIT: CME asset managers net long while leveraged funds net short — "
                           "ETF-era cash-and-carry basis trade (institutions accumulating, hedge funds harvesting basis), "
                           "not directional bearishness.")
    if nowcast_regime == "SLOWING":
        spy3 = _get(now, "regime_spy_performance", "SLOWING", "horizons", "3m", "median_pct")
        if spy3 is not None:
            divergences.append(f"COUNTERINTUITIVE: growth nowcast is SLOWING, but historically SLOWING preceded "
                               f"SPY +{spy3}% median over 3m — slowing has not meant down for equities.")
    if roro is not None and roro > 15 and recession_prob is not None and recession_prob >= 50:
        divergences.append(f"RISK-vs-MACRO SPLIT: cross-asset RORO is risk-on ({roro:+.0f}) while the recession-"
                           f"probability composite is {recession_prob}% — markets price benign, the curve/credit/cycle stack does not.")
    if roro is not None and roro < -15 and quad2d == "GOLDILOCKS":
        divergences.append(f"RISK-vs-CYCLE SPLIT: the cycle reads goldilocks but cross-asset RORO is risk-off "
                           f"({roro:+.0f}) — price action disagrees with the macro read.")
    if ca_confirm and ca_confirm.get("status") == "CONTRADICTED":
        divergences.append("PRESCRIPTION-vs-TAPE: " + ca_confirm["note"])
    if positioning.get("aaii_extreme") == "EXTREME_BULL":
        divergences.append(f"EUPHORIA: retail sentiment at a bullish extreme ({positioning.get('aaii_bull_pct')}% bulls, "
                           f"spread z {positioning.get('aaii_spread_z')}) — historically a contrarian sell tell.")
    _mp = rates_fed_vol.get("move_pctile")
    if isinstance(_mp, (int, float)) and _mp < 15:
        divergences.append(f"COMPLACENCY: bond vol (MOVE) at the {_mp:.0f}th percentile and credit "
                           f"{positioning.get('credit_equity_state', '')} while liquidity drains — vol/credit markets "
                           f"price calm into a tightening backdrop.")
    _rg = growth_depth.get("reserves_to_gdp_pct")
    if isinstance(_rg, (int, float)) and _rg < 11:
        divergences.append(f"RESERVE SCARCITY: bank reserves {_rg:.1f}% of GDP (below the ~11% scarcity threshold) — "
                           f"the structural pre-condition for repo/funding stress, even with squeeze risk still low today.")
    if rates_fed_vol.get("fed_drift") == "HAWKISH_SHIFT" and str(growth_depth.get("labor_regime", "")).lower() == "weakening":
        divergences.append(f"POLICY-ERROR RISK: Fed communication is shifting hawkish (drift z {rates_fed_vol.get('fed_drift_z')}) "
                           f"while leading labor is weakening — tightening into a slowdown.")
    if _get(cftc, "smart_money") == "DISTRIBUTING":
        divergences.append("SMART MONEY DISTRIBUTING: large-spec/commercial futures positioning is net distributing — "
                           "institutional futures flows are reducing risk even as retail sits at a bullish extreme.")
    if str(rates_fed_vol.get("tail_regime", "")).upper() == "ELEVATED":
        divergences.append(f"TAIL RISK ELEVATED: the option-implied crash gauge is {rates_fed_vol.get('tail_gauge')}/100 "
                           f"(ELEVATED, protection {str(rates_fed_vol.get('tail_valuation','')).lower()}) — the left tail of the "
                           f"return distribution is fattening beneath the calm surface.")
    if _get(cdvol, "btc", "trend") == "RISING" and _get(cdvol, "btc", "regime") in ("LOW", "NORMAL"):
        divergences.append(f"CRYPTO VOL RISING: BTC DVOL {_get(cdvol, 'btc', 'dvol')} ({_get(cdvol, 'btc', 'pctile_1y')}th pctile) "
                           f"is climbing off low levels — crypto hedging demand is picking up beneath a still-normal surface.")
    _btc_cc = _get(cbasis, "btc", "cash_and_carry_yield_3m_pct")
    _eth_fund = _get(cbasis, "eth", "funding_annualized_pct")
    if (isinstance(_btc_cc, (int, float)) and isinstance(_eth_fund, (int, float))
            and _btc_cc > 1 and _eth_fund < -2):
        divergences.append(f"CRYPTO BTC/ETH SPLIT: BTC futures carry positive ({_btc_cc}% 3m contango) while ETH perp "
                           f"funding is negative ({_eth_fund}%/yr) — ETH-specific deleveraging beneath a calm BTC surface.")
    _mr = _get(cminers, "hash_ribbons", "state")
    _pz = _get(cminers, "puell", "zone")
    if _mr == "CAPITULATION" and isinstance(_get(cminers, "puell", "value"), (int, float)) and _get(cminers, "puell", "value") < 1:
        divergences.append(f"CRYPTO MINER CAPITULATION: hash ribbons in capitulation "
                           f"({_get(cminers, 'hash_ribbons', 'days_in_capitulation')}d) with Puell {_get(cminers, 'puell', 'value')} ({_pz}) — "
                           f"miner stress that has historically marked bottoming *setups* (the timing signal itself is unproven live).")

    # ───────── DETERMINISTIC SYNTHESIS ("the read") — rules-based, works without the LLM ─────────
    contribs = []
    def _add(label, side, weight, note=""):
        contribs.append({"label": label, "side": side, "weight": weight, "note": note})
    # risk-on contributors
    if roro is not None and roro > 15: _add("Cross-asset RORO risk-on", 1, 8, f"RORO {roro:+.0f}")
    if gbc_phase and "EXPANSION" in str(gbc_phase).upper(): _add("Global cycle expanding", 1, 7, str(gbc_phase))
    if isinstance(netliq, dict) and (netliq.get("net_13w_delta_bn") or 0) > 0: _add("Fed net liquidity rising", 1, 4)
    if growth_depth.get("activity_regime") == "EXPANDING": _add("Activity nowcast expanding", 1, 5)
    if cr_regime and "BENIGN" in str(cr_regime).upper(): _add("Credit spreads benign", 1, 5)
    if positioning.get("breadth_thrust_state") not in (None, "NULL", "none"): _add("Breadth thrust firing", 1, 6)
    if str(_get(reg, "current", "quadrant") or "").upper() in ("GOLDILOCKS", "REFLATION"): _add("Macro regime constructive", 1, 5)
    # risk-off contributors
    if recession_prob is not None and recession_prob >= 30: _add("Recession-prob composite", -1, min(10, recession_prob / 5), f"{recession_prob}%")
    if sq_level in ("ELEVATED", "HIGH", "ACUTE", "RISING"): _add("Liquidity-squeeze rising", -1, 8, sq_level)
    if str(_get(liqpulse, "composites", "liquidity_regime") or "").upper() in ("ACUTE_DRAIN", "DRAINING"): _add("Liquidity-pulse draining", -1, 9, "ACUTE_DRAIN")
    _rg = growth_depth.get("reserves_to_gdp_pct")
    if isinstance(_rg, (int, float)) and _rg < 11: _add("Bank-reserve scarcity", -1, 7, f"{_rg:.1f}% GDP")
    if str(growth_depth.get("labor_regime", "")).lower() == "weakening": _add("Leading labor weakening", -1, 6)
    if positioning.get("aaii_extreme") == "EXTREME_BULL": _add("Retail euphoria (contrarian)", -1, 6)
    if positioning.get("cot", {}).get("smart_money") == "DISTRIBUTING": _add("Smart money distributing", -1, 6)
    if str(positioning.get("credit_equity_state", "")).upper().find("BULL_RICH") >= 0: _add("Credit complacency", -1, 3)
    _mp = rates_fed_vol.get("move_pctile")
    if isinstance(_mp, (int, float)) and _mp < 15: _add("Bond-vol complacency", -1, 3, f"MOVE {_mp:.0f}th")
    if str(rates_fed_vol.get("tail_regime", "")).upper() in ("ELEVATED", "HIGH"): _add("Tail risk elevated", -1, 6)
    if rates_fed_vol.get("fed_drift") == "HAWKISH_SHIFT": _add("Fed turning hawkish", -1, 5)
    _yc = _get(cross_asset_risk, "yen_carry", "unwind_score")
    if isinstance(_yc, (int, float)) and _yc >= 50: _add("Yen-carry unwind risk", -1, 5)
    if ca_confirm and ca_confirm.get("status") == "CONTRADICTED": _add("Tape contradicts regime", -1, 5)
    # capital-cycle contributors (bottleneck-boom supply side; structural / 18-24mo horizon, modest weight)
    if _bphases.get("SCARCITY_BUILDING", 0) >= 1:
        _add("Capital-cycle scarcity building", 1, 4,
             "supply exiting money-losing cyclicals — 18-24mo tightening setup")
    if _bflood:
        _add("Capacity flooding (late cycle)", -1, 4,
             f"{len(_bflood)} boom names' industries now adding capacity")
    if _bcure:
        _add("Commodity cure-for-low-prices", 1, 3, ",".join(_bcure[:3]))
    # crypto contributors (one input among many; modest weight)
    if _get(cdvol, "btc", "regime") in ("ELEVATED", "HIGH"):
        _add("Crypto implied vol elevated", -1, 3, f"DVOL {_get(cdvol, 'btc', 'dvol')}")
    if ("LONG" in str(_get(cfund, "composite_regime") or "").upper()
            and "SQUEEZE" in str(_get(cfund, "composite_signal") or "").upper()):
        _add("Crypto funding crowded long", -1, 3)
    if _get(cliq, "regime") == "DRY-POWDER LOADED":
        _add("Crypto dry-powder loaded (contrarian)", 1, 2, "SSR low · sidelined stablecoin cash")
    # options surface: extreme put skew + vol backwardation = near-term hedging / defensive
    _rr = _get(cropts, "btc", "headline_30d", "rr_25d")
    _vt = str(_get(cropts, "btc", "term_structure", "regime") or "")
    if isinstance(_rr, (int, float)) and _rr <= -6 and "BACKWARD" in _vt.upper():
        _add("Crypto options hedging (put skew + backwardation)", -1, 2, f"RR {_rr}")
    # miners: deep-value Puell / hash-ribbon recovery = contrarian risk-on
    _pu = _get(cminers, "puell", "value")
    if isinstance(_pu, (int, float)) and _pu < 0.6:
        _add("Crypto miners deep value (Puell)", 1, 2, f"Puell {_pu}")
    if _get(cminers, "hash_ribbons", "state") == "RECOVERY/BUY":
        _add("Crypto hash-ribbon recovery", 1, 2)
    # basis: carry backwardation = deleveraging / funding stress
    _cc = _get(cbasis, "btc", "cash_and_carry_yield_3m_pct")
    if isinstance(_cc, (int, float)) and _cc <= -2:
        _add("Crypto carry backwardation (deleveraging)", -1, 2, f"3m carry {_cc}%")
    # exchange netflows: heavy INFLOW (distribution) is the cleaner caution signal — the naive
    # "outflow=bullish" relationship measured INVERTED over ~3.5y, so only the inflow side, low weight.
    _xfp = _get(cxflow, "btc", "cum_30d_pctile")
    if isinstance(_xfp, (int, float)) and _xfp >= 80:
        _add("Crypto heavy exchange inflow (distribution)", -1, 1, f"netflow {_xfp}th pctile")
    # realized price: below aggregate cost basis = capitulation value (contrarian risk-on)
    _pvr = _get(conchain, "btc", "price_vs_realized_pct") or _get(conchain, "price_vs_realized_pct")
    if isinstance(_pvr, (int, float)) and _pvr < 0:
        _add("Crypto below realized price (deep value)", 1, 2, f"{_pvr}% vs cost basis")
    # NUPL euphoria = late-cycle caution
    _nupl = _get(conchain, "btc", "nupl") or _get(conchain, "nupl")
    if isinstance(_nupl, (int, float)) and _nupl >= 0.75:
        _add("Crypto NUPL euphoria (late-cycle)", -1, 2, f"NUPL {_nupl}")
    # stablecoin depeg = crypto tail-risk / funding stress (fires only on a real depeg, not minor drift)
    if _get(cspeg, "gauge") == "red":
        _add("Crypto stablecoin depeg (tail risk)", -1, 3, f"{_get(cspeg, 'worst_coin')} {_get(cspeg, 'worst_depeg_pct')}%")
    # spot ETF flows — the marginal buyer; event-study CONFIRMED predictive, so meaningful weight
    _etfp = _get(cetf, "btc_etf", "cum_30d_pctile")
    _etf30 = _get(cetf, "btc_etf", "cum_30d_usd")
    if isinstance(_etfp, (int, float)):
        if _etfp >= 80:
            _add("Crypto spot-ETF strong inflow (marginal bid)", 1, 3,
                 f"BTC ETF +${(_etf30 or 0)/1e9:.1f}B/30d, {_etfp}th")
        elif _etfp <= 20:
            _add("Crypto spot-ETF strong outflow (marginal sell)", -1, 3,
                 f"BTC ETF ${(_etf30 or 0)/1e9:.1f}B/30d, {_etfp}th")
    # Hyperliquid perp leverage: buildup = fragile (risk-off); cascade = acute deleveraging stress
    _hlr = str(_get(chl, "leverage_regime") or "")
    if "BUILDUP" in _hlr.upper():
        _add("Crypto perp leverage buildup (fragile)", -1, 2,
             f"HL OI ${(_get(chl, 'total_oi_usd') or 0)/1e9:.1f}B, {_get(chl, 'total_oi_chg_24h_pct')}%/24h")
    elif "CASCADE" in _hlr.upper():
        _add("Crypto perp liquidation cascade (acute stress)", -1, 1, _hlr)
    # dealer gamma: NEGATIVE gamma (spot below flip) = dealers amplify moves = vol-expansion / unstable
    _gxr = str(_get(cgex, "btc", "regime") or "")
    _svf = _get(cgex, "btc", "spot_vs_flip")
    if "NEGATIVE" in _gxr.upper() and isinstance(_svf, (int, float)) and _svf <= -0.5:
        _add("Crypto dealer negative gamma (vol-expansion)", -1, 1,
             f"spot {_svf}% below flip ${_get(cgex, 'btc', 'gamma_flip')}")
    on = sum(c["weight"] for c in contribs if c["side"] > 0)
    off = sum(c["weight"] for c in contribs if c["side"] < 0)
    score = max(-100, min(100, round((on - off) / max(on + off, 1) * 100)))
    posture = ("STRONG RISK-OFF" if score <= -40 else "RISK-OFF" if score <= -15 else
               "NEUTRAL" if score < 15 else "RISK-ON" if score < 40 else "STRONG RISK-ON")
    conviction = ("HIGH" if abs(score) >= 45 else "MODERATE" if abs(score) >= 20 else "LOW")
    bullish = sorted([c for c in contribs if c["side"] > 0], key=lambda c: -c["weight"])[:4]
    bearish = sorted([c for c in contribs if c["side"] < 0], key=lambda c: -c["weight"])[:5]
    own = [x["label"] for x in (cross or [])[:3]]
    reduce = [x["label"] for x in (cross or [])[-3:]][::-1]
    _sev = ["RESERVE SCARCITY", "TAIL RISK", "POLICY-ERROR", "SMART MONEY", "LIQUIDITY SPLIT", "COMPLACENCY", "EUPHORIA", "PRESCRIPTION"]
    key_risk = next((dv for s in _sev for dv in divergences if dv.startswith(s)), (divergences[0] if divergences else None))
    bl = (f"Rules-based read: {posture} ({score:+d}, {conviction.lower()} conviction). "
          f"{str(cycle.get('headline_phase') or '').title()} with the tape leading {', '.join(own[:2]).lower()} while the textbook "
          f"{quad2d.lower() if quad2d else ''} book lags. "
          f"{len([c for c in contribs if c['side']<0])} risk-off vs {len([c for c in contribs if c['side']>0])} risk-on signals active.")
    synthesis = {
        "posture": posture, "score": score, "conviction": conviction,
        "bullish_drivers": [{"label": c["label"], "note": c["note"]} for c in bullish],
        "bearish_drivers": [{"label": c["label"], "note": c["note"]} for c in bearish],
        "own_whats_leading": own, "reduce_whats_lagging": reduce,
        "key_risk": key_risk, "bottom_line": bl,
        "n_risk_off": len([c for c in contribs if c["side"] < 0]),
        "n_risk_on": len([c for c in contribs if c["side"] > 0]),
    }
    _dominant = bearish if score < 0 else bullish
    _flips = [FLIP[c["label"]] for c in _dominant[:5] if c["label"] in FLIP]
    synthesis["what_flips_it"] = {
        "direction": "toward risk-on" if score < 0 else "toward risk-off",
        "conditions": _flips,
        "note": (f"This {posture.lower()} read neutralises toward NEUTRAL if roughly half of these reverse; "
                 f"a full flip needs the net tally to cross {'+15' if score < 0 else '−15'} (now {score:+d}).")
        if _flips else None,
    }

    # ───────────────────────── VERDICT ─────────────────────────
    sq_phrase = {"LOW": "liquidity ample", "LOW · WATCH": "liquidity flat/easy with mild draining flickers",
                 "RISING": "liquidity tightening", "ELEVATED": "liquidity stress building",
                 "HIGH": "liquidity squeeze underway", "ACUTE": "acute liquidity squeeze",
                 "UNKNOWN": "liquidity read unavailable"}.get(sq_level, "")
    fav = (" Historically favours " + ", ".join(assets["lead"][:2]).lower() + ".") if (assets and quad2d) else ""
    roro_clause = f", RORO {risk['read']}" if roro is not None else ""
    head_ph = (COORD_PHASE.get(quad2d, (phase_label, phase_n))[0] if quad2d else phase_label)
    reg_note = (f" (macro-regime model still reads {quad.lower()})" if (quad2d and quad and quad.upper() != quad2d) else "")
    rec_note = (f", recession-prob {recession_prob}%" if recession_prob is not None else "")
    verdict = (f"{head_ph} — coordinates in {quad2d.title() if quad2d else 'n/a'} "
               f"(growth z {g_now if g_now is not None else '—'}, inflation z {i_now if i_now is not None else '—'})"
               f"{reg_note}. {sq_phrase}, squeeze risk {sq_level} ({squeeze}){roro_clause}{rec_note}."
               + (" Key divergence: " + divergences[0].split(':')[0] + "." if divergences else "")
               + fav)

    falsifier = ("Cycle call flips if the macro-regime quadrant rotates out of STAGFLATION (→ GOLDILOCKS/REFLATION = "
                 "re-acceleration, or → DEFLATION-BUST = downturn). Squeeze-risk escalates if plumbing health breaks "
                 "below ~70, funding stress > 40, or crisis composite leaves DEFCON 4 — none of which is true now.")

    # ───────────────── HEAVY AI SYNTHESIS (GLM via llm_router, bounded) ─────────────────
    near3 = (_get(analogs_d, "analogs", default=[]) or [])[:3]
    ai_state = {
        "as_of": datetime.now(timezone.utc).date().isoformat(),
        "cycle": {
            "headline_phase": cycle.get("headline_phase"), "quadrant": cycle.get("quadrant_2d"),
            "growth_z": g_now, "inflation_z": i_now, "trail_recent": (trail[-4:] if trail else []),
            "macro_regime_model": cycle.get("macro_regime_quadrant"),
            "us_cycle": f"{us_score} {us_level}", "global_phase": cycle.get("global_phase"),
            "global_cli": cycle.get("global_avg_cli"), "global_breadth_pct": cycle.get("global_expansion_breadth_pct"),
            "nowcast": cycle.get("nowcast_regime"), "froth_markers": cycle.get("froth_markers"),
            "recession_prob_pct": recession_prob, "sahm": sahm,
            "yield_curve": yc_regime, "yield_curve_decomp": yc_decomp, "credit": cr_regime,
            "vol_regime": vol_regime, "dollar_regime": dollar_regime, "eps_revision_breadth": eps_breadth,
            "surprise_tilt": surprise, "next_3m_quadrant_odds": next3,
        },
        "asset_leadership": assets,
        "risk": {"roro_score": roro, "read": risk.get("read"), "posture": risk.get("posture"),
                 "tells": risk.get("tells"), "fomc_context": risk.get("fomc_context")},
        "liquidity": {"squeeze_score": squeeze, "level": sq_level,
                      "aggregate_regime": liquidity.get("aggregate_liquidity_regime"),
                      "net_liquidity": netliq, "flickers": flickers,
                      "stress_stack": liquidity.get("components")},
        "analogs": {"nearest": [{"date": a.get("date"), "similarity": a.get("similarity"),
                                 "fwd_63d_pct": a.get("forward_63d_pct")} for a in near3],
                    "directional_call": _get(analogs_d, "directional_call"),
                    "forward_distribution": _get(analogs_d, "forward_distribution"),
                    "unprecedentedness": _get(analogs_d, "unprecedentedness")},
        "cross_asset_confirmation": ca_confirm,
        "cross_asset_returns_1m": {x["label"]: x["ret_1m"] for x in (cross or [])},
        "global_liquidity": global_liq,
        "rates_fed_vol": rates_fed_vol,
        "positioning_sentiment": positioning,
        "growth_depth": growth_depth,
        "cross_asset_risk": cross_asset_risk,
        "fed_path": rates_fed_vol.get("fed_path"),
        "cot_positioning": positioning.get("cot"),
        "stress_scenarios": {"top": stress_scenarios.get("top"), "tail_regime": stress_scenarios.get("tail_regime"),
                             "tail_gauge": stress_scenarios.get("tail_gauge")},
        "scenario_playbook": scenario_playbook,
        "crypto": {"vol_regime": crypto.get("dvol_btc_regime"), "funding": crypto.get("funding_regime"),
                   "liquidity": crypto.get("liquidity_regime"), "fear_greed": crypto.get("fear_greed_class"),
                   "skew": crypto.get("skew_read"), "vol_term": crypto.get("vol_term_regime"),
                   "hash_ribbon": crypto.get("hash_ribbon"), "puell": crypto.get("puell"),
                   "carry_3m": crypto.get("cash_carry_3m"), "carry_regime": crypto.get("carry_regime")},
        "rules_based_read": {"posture": synthesis["posture"], "score": synthesis["score"],
                             "key_risk": synthesis["key_risk"]},
        "divergences": divergences,
    }
    ai = None
    if _llm is not None:
        _ex = _cf.ThreadPoolExecutor(max_workers=1)
        try:
            ai = _ex.submit(_ai_synthesis, ai_state).result(timeout=75)
        except Exception as e:
            print(f"[cycle-clock] AI bounded-call failed: {str(e)[:100]}")
        finally:
            _ex.shutdown(wait=False)
    print(f"[cycle-clock] AI synthesis: {'OK' if ai else 'unavailable'}")

    # ───────── SELF-HISTORY: log today's snapshot, derive trajectory ─────────
    _today = datetime.now(timezone.utc).date().isoformat()
    snap = {"date": _today, "quadrant": quad2d, "g": g_now, "i": i_now,
            "recession_prob": recession_prob, "squeeze": squeeze, "roro": roro,
            "net_liq_tn": (netliq or {}).get("net_tn"), "tail_gauge": _get(tailrisk, "system_tail_gauge"),
            "posture_score": synthesis["score"], "n_divergences": len(divergences)}
    hist = _update_history(snap)
    def _delta(field, back):
        if len(hist) > back:
            prev = hist[-(back + 1)].get(field)
            if prev is not None and snap.get(field) is not None:
                return round(snap[field] - prev, 2)
        return None
    trajectory = {
        "n_days_logged": len(hist),
        "posture_score_5d": _delta("posture_score", 5), "posture_score_21d": _delta("posture_score", 21),
        "recession_prob_21d": _delta("recession_prob", 21), "squeeze_21d": _delta("squeeze", 21),
        "net_liq_21d": _delta("net_liq_tn", 21), "tail_21d": _delta("tail_gauge", 21),
        "series": hist[-90:],
    }
    # ── SELF-GRADING: quadrant backtest (immediate) + posture-log forward grade (accumulates) ──
    track_record = None
    try:
        _spy = _spy_daily()
        track_record = {"quadrant_backtest": _quadrant_backtest(),
                        "posture_grade": _grade_posture_log(hist, _spy),
                        "current_quadrant": quad2d}
    except Exception as e:
        print(f"[cycle-clock] track_record failed: {str(e)[:100]}")

    # ── data integrity: how trustworthy is this synthesis given input freshness ──
    _missing = [k for k, v in avail.items() if v == "MISSING"]
    _stale_lbls = {s.split(" ")[0] for s in stale}
    _total = len(avail)
    _fresh = sum(1 for k, v in avail.items() if v != "MISSING" and k not in _stale_lbls)
    data_integrity = {
        "n_sources": _total, "n_fresh": _fresh, "n_stale": len(stale), "n_missing": len(_missing),
        "integrity_pct": round(100 * _fresh / _total) if _total else None,
        "stale": stale[:14], "missing": _missing[:14],
    }

    out = {
        "engine": "cycle-clock", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "wl_research": __import__("wl_fusion").block(('GROWTH', 'INFLATION')),
        "duration_s": round(time.time() - t0, 1),
        "verdict": verdict,
        "synthesis": synthesis,
        "trajectory": trajectory,
        "track_record": track_record,
        "data_integrity": data_integrity,
        "ai": ai,
        "cycle": cycle,
        "risk": risk,
        "analogs": {
            "nearest": (_get(analogs_d, "analogs", default=[]) or [])[:3],
            "forward_distribution": _get(analogs_d, "forward_distribution"),
            "directional_call": _get(analogs_d, "directional_call"),
            "directional_description": _get(analogs_d, "directional_description"),
            "unprecedentedness": _get(analogs_d, "unprecedentedness"),
        },
        "liquidity": liquidity,
        "cross_asset": {"returns": cross, "confirmation": ca_confirm},
        "global_liquidity": global_liq,
        "rates_fed_vol": rates_fed_vol,
        "yield_curve": yield_curve,
        "positioning": positioning,
        "growth_depth": growth_depth,
        "cross_asset_risk": cross_asset_risk,
        "stress_scenarios": stress_scenarios,
        "crypto": crypto,
        "capital_cycle": {
            "phases": _bphases,
            "scarcity_building": _bphases.get("SCARCITY_BUILDING", 0),
            "flooding": _bphases.get("CAPACITY_FLOODING", 0),
            "capacity_flood_names": _bflood[:6],
            "commodity_cure_setups": _bcure,
            "early_calls": [c.get("ticker") for c in _bearly[:5]],
        },
        "divergences": divergences,
        "falsifier": falsifier,
        "availability": avail, "stale": stale,
        "methodology": (
            "Meta-synthesis of published engine outputs. Cycle phase anchors on the macro-regime quadrant "
            "(Investment-Clock mapping: REFLATION→early, GOLDILOCKS→mid, STAGFLATION→late, DEFLATION-BUST→downturn), "
            "confirmed/contradicted by US-cycle level, global business-cycle phase, and the growth nowcast, with a "
            "late-stage froth overlay (US-cycle valuation/leverage z-scores ≥0.85). Liquidity-squeeze risk is a "
            "weighted blend of the purpose-built stress stack — global stress 0.25, crisis composite 0.20, plumbing "
            "0.20, funding 0.15, systemic 0.10, canaries 0.10 — plus a liquidity-direction modifier (draining adds). "
            "Divergences are surfaced, not averaged. Fuses model outputs, not ground truth; stale inputs are flagged. "
            "Research, not investment advice."),
    }

    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache, max-age=0")
    print(f"[cycle-clock] {phase_label} ({quad}) conf={cycle_conf} | squeeze {squeeze} {sq_level} | "
          f"{len(divergences)} divergences | {len(flickers)} flickers")
    return {"statusCode": 200, "body": json.dumps({"phase": phase_label, "quadrant": quad,
                                                    "squeeze_risk": squeeze, "level": sq_level})}
