"""
justhodl-global-stress -- the Global Stress Matrix.

Tracks stock-market stress and bond-market stress across the world in
one ranked matrix. The market under the most stress is meant to flash
red on the page; this engine produces the score and the level label
that drives that.

  EQUITY MARKETS (6): United States, Euro Area, United Kingdom, Japan,
  China, Emerging Markets -- each via a liquid, FMP-covered market ETF.

  BOND MARKETS (4): US Treasuries, US Credit (high-yield), Intl
  Developed Government, EM Sovereign.

For every market a 0-100 stress score is built from three components
that need no forecasting -- they are mechanical readings of the tape:

  - DRAWDOWN     -- how far the market sits below its 52-week high;
  - VOLATILITY   -- where 20-day realised volatility sits in its own
                    trailing one-year range (a percentile);
  - TREND        -- how far price sits below its 200-day average.

Equity stress weights drawdown/vol/trend 0.45/0.35/0.20; bond stress
weights drawdown/vol 0.55/0.45 (a bond selloff IS the stress). Scores
roll up into a global equity-stress and bond-stress reading and an
overall Global Stress Index, with the worst market flagged.

Data: FMP /stable/historical-price-eod/light. Output:
data/global-stress.json. Honest framing: this measures stress that is
already in the tape -- it is a thermometer, not a forecast.
"""
import concurrent.futures as cf
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

SCHEMA = "1.1"
BASE = "https://financialmodelingprep.com/stable"
FMP = os.environ.get("FMP_KEY", "")
FRED_KEY = os.environ.get("FRED_KEY", "")
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/global-stress.json"
HIST_KEY = "data/global-stress-history.json"

HIST_BARS = 300
SERIES_BARS = 130

s3 = boto3.client("s3")

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


def read_prev_history():
    try:
        return json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=HIST_KEY)["Body"].read())
    except Exception:
        return {"snapshots": []}


def read_sidecar(key):
    """Read another engine's published JSON; {} if absent."""
    try:
        return json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def fred_series(series_id, days=460):
    """FRED observations as [(date, value)] oldest-first; [] on failure."""
    if not FRED_KEY:
        return []
    start = (datetime.now(timezone.utc)
             - timedelta(days=days)).strftime("%Y-%m-%d")
    url = ("%s?series_id=%s&api_key=%s&file_type=json&observation_start=%s"
           % (FRED_BASE, series_id, FRED_KEY, start))
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "JustHodl-GlobalStress/1.0"})
            r = urllib.request.urlopen(req, timeout=25)
            obs = json.loads(r.read().decode("utf-8")).get("observations", [])
            out = []
            for o in obs:
                v = o.get("value")
                if v in (None, ".", ""):
                    continue
                try:
                    out.append((o.get("date"), float(v)))
                except (TypeError, ValueError):
                    continue
            return out
        except Exception:
            time.sleep(0.6 + attempt)
    return []


def pearson(a, b):
    """Pearson correlation of two equal-tail return series."""
    n = min(len(a), len(b))
    if n < 5:
        return None
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    va = sum((x - ma) ** 2 for x in a)
    vb = sum((x - mb) ** 2 for x in b)
    if va <= 0 or vb <= 0:
        return None
    return cov / (va * vb) ** 0.5

# market -> (key, ETF, display name, what it tracks)
EQUITY = [
    ("us", "SPY", "United States", "S&P 500"),
    ("euro", "FEZ", "Euro Area", "Euro Stoxx 50"),
    ("uk", "EWU", "United Kingdom", "MSCI United Kingdom"),
    ("japan", "EWJ", "Japan", "MSCI Japan"),
    ("china", "MCHI", "China", "MSCI China"),
    ("india", "INDA", "India", "MSCI India"),
    ("korea", "EWY", "South Korea", "MSCI South Korea"),
    ("em", "EEM", "Emerging Markets", "MSCI Emerging Markets"),
]
BONDS = [
    ("ust", "IEF", "US Treasuries", "7-10y US Treasuries"),
    ("usig", "LQD", "US IG Credit", "US investment-grade corporates"),
    ("uscredit", "HYG", "US High-Yield", "US high-yield corporates"),
    ("intl", "BWX", "Intl Developed Govt", "ex-US developed sovereigns"),
    ("emdebt", "EMB", "EM Sovereign", "USD emerging-market sovereigns"),
]


# ---- data ------------------------------------------------------------------
def fmp(path, params="", max_retries=3):
    url = "%s/%s?apikey=%s%s" % (BASE, path, FMP, params)
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "JustHodl-GlobalStress/1.0"})
            r = urllib.request.urlopen(req, timeout=25)
            return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(1 + attempt * 2 + attempt ** 2)
                continue
            return None
        except Exception:
            time.sleep(0.5 + attempt)
    return None


def get_closes(symbol):
    data = fmp("historical-price-eod/light", "&symbol=%s" % symbol)
    if not isinstance(data, list) or len(data) < 220:
        return None
    rows = []
    for r in data:
        d, p = (r or {}).get("date"), (r or {}).get("price")
        if d is None or p is None:
            continue
        try:
            rows.append((str(d)[:10], float(p)))
        except (TypeError, ValueError):
            continue
    rows.sort(key=lambda x: x[0])
    return rows[-HIST_BARS:]


# ---- stress maths ----------------------------------------------------------
def realized_vol(closes, window):
    """Annualised realised vol over `window` daily returns."""
    if len(closes) < window + 1:
        return None
    rets = [closes[i] / closes[i - 1] - 1.0
            for i in range(len(closes) - window, len(closes))]
    m = sum(rets) / len(rets)
    var = sum((x - m) ** 2 for x in rets) / len(rets)
    return (var ** 0.5) * (252 ** 0.5) * 100.0


def vol_series(closes, window):
    out = []
    for t in range(window, len(closes)):
        seg = closes[t - window:t + 1]
        rets = [seg[i] / seg[i - 1] - 1.0 for i in range(1, len(seg))]
        m = sum(rets) / len(rets)
        var = sum((x - m) ** 2 for x in rets) / len(rets)
        out.append((var ** 0.5) * (252 ** 0.5) * 100.0)
    return out


def pctile(value, sample):
    if not sample:
        return None
    below = sum(1 for x in sample if x <= value)
    return below / float(len(sample)) * 100.0


def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def stress_for(closes, kind):
    """0-100 stress score + components for one market's price series."""
    n = len(closes)
    if n < 220:
        return None
    last = closes[-1]
    hi_52w = max(closes[-252:]) if n >= 252 else max(closes)
    dd = (hi_52w - last) / hi_52w * 100.0 if hi_52w else 0.0

    # drawdown -> 0-100 (equities fall further than bonds)
    dd_full = 22.0 if kind == "equity" else 13.0
    dd_score = clamp(dd / dd_full * 100.0)

    # volatility percentile within trailing 1y
    vs = vol_series(closes, 20)
    rv = realized_vol(closes, 20)
    vp = pctile(rv, vs[-252:]) if (rv is not None and vs) else None
    vol_score = vp if vp is not None else 50.0

    # trend -- distance below the 200-day average
    sma200 = sum(closes[-200:]) / 200.0 if n >= 200 else None
    if sma200:
        gap = (last - sma200) / sma200 * 100.0
        trend_score = clamp(-gap / 12.0 * 100.0) if gap < 0 else 0.0
    else:
        trend_score = 0.0

    if kind == "equity":
        total = 0.45 * dd_score + 0.35 * vol_score + 0.20 * trend_score
    else:
        total = 0.55 * dd_score + 0.45 * vol_score
    total = round(clamp(total))

    return {
        "stress": total,
        "level": stress_level(total),
        "drawdown_pct": round(dd, 1),
        "drawdown_score": round(dd_score),
        "realized_vol": round(rv, 1) if rv is not None else None,
        "vol_percentile": round(vp) if vp is not None else None,
        "below_sma200_pct": (round((last / sma200 - 1) * 100, 1)
                             if sma200 else None),
        "last": round(last, 2),
        "series": [round(c, 2) for c in closes[-SERIES_BARS:]],
    }


def stress_level(s):
    if s >= 75:
        return "ACUTE"
    if s >= 55:
        return "STRESSED"
    if s >= 32:
        return "ELEVATED"
    return "CALM"


# ---- additional stress metrics ---------------------------------------------
def implied_vol_panel():
    """Forward-looking equity fear -- the VIX. Level + 1y percentile."""
    obs = fred_series("VIXCLS", days=460)
    vals = [v for _, v in obs]
    if len(vals) < 60:
        return None
    last = vals[-1]
    sample = vals[-252:]
    pc = pctile(last, sample)
    # VIX ~12 = calm, ~40 = acute
    lvl_score = clamp((last - 12.0) / (40.0 - 12.0) * 100.0)
    score = round(0.5 * lvl_score + 0.5 * (pc if pc is not None else 50.0))
    chg_1m = round(last - vals[-22], 1) if len(vals) > 21 else None
    return {
        "vix": round(last, 2),
        "percentile_1y": round(pc) if pc is not None else None,
        "change_1m": chg_1m,
        "stress_score": score,
        "level": stress_level(score),
        "series": [round(v, 2) for v in vals[-SERIES_BARS:]],
    }


def credit_spread_panel():
    """The credit-stress dimension -- the full ICE BofA option-adjusted
    spread ladder. OAS is the junk-vs-Treasury spread: the cleanest read
    of how much extra yield the market demands to carry credit risk. It
    spans the US high-yield rating ladder (BB -> B -> CCC & Lower),
    investment grade, and emerging-market corporate credit, and derives
    the CCC-vs-BB dispersion -- how hard the market is punishing the
    worst junk relative to the best."""
    specs = [
        ("hy",      "BAMLH0A0HYM2",      "US High-Yield (Master)",       3.0,  9.0),
        ("hy_bb",   "BAMLH0A1HYBB",      "US HY -- BB (top junk)",       1.8,  5.5),
        ("hy_b",    "BAMLH0A2HYB",       "US HY -- B (mid junk)",        3.0,  9.0),
        ("hy_ccc",  "BAMLH0A3HYC",       "US HY -- CCC & Lower (worst)", 7.0, 20.0),
        ("ig",      "BAMLC0A0CM",        "US Investment-Grade",          0.8,  2.5),
        ("em_corp", "BAMLEMCBPIOAS",     "EM Corporate",                 1.8,  6.5),
        ("em_hy",   "BAMLEMHBHYCRPIOAS", "EM High-Yield Corporate",      4.0, 13.0),
    ]
    rows, scores, last_by = [], [], {}
    for key, sid, name, calm, acute in specs:
        obs = fred_series(sid, days=460)
        vals = [v for _, v in obs]
        if len(vals) < 60:
            continue
        last = vals[-1]
        last_by[key] = last
        pc = pctile(last, vals[-252:])
        lvl_score = clamp((last - calm) / (acute - calm) * 100.0)
        score = round(0.5 * lvl_score
                      + 0.5 * (pc if pc is not None else 50.0))
        chg_1m = round((last - vals[-22]) * 100) if len(vals) > 21 else None
        scores.append(score)
        rows.append({"key": key, "name": name, "oas_pct": round(last, 2),
                     "percentile_1y": round(pc) if pc is not None else None,
                     "change_1m_bps": chg_1m,
                     "stress_score": score, "level": stress_level(score),
                     "series": [round(v, 2) for v in vals[-SERIES_BARS:]]})
    if not rows:
        return None
    composite = round(sum(scores) / len(scores))
    # CCC-vs-BB dispersion -- the worst junk punished harder than the
    # best is distress concentrating in the weakest credits.
    dispersion = None
    if "hy_ccc" in last_by and "hy_bb" in last_by:
        gap = last_by["hy_ccc"] - last_by["hy_bb"]
        d_score = round(clamp((gap - 5.0) / (13.0 - 5.0) * 100.0))
        dispersion = {"ccc_minus_bb_pct": round(gap, 2),
                      "stress_score": d_score,
                      "level": stress_level(d_score)}
    worst = max(rows, key=lambda r: r["stress_score"])
    return {"composite_score": composite,
            "level": stress_level(composite),
            "tier_dispersion": dispersion,
            "worst_tier": {"name": worst["name"],
                           "oas_pct": worst["oas_pct"],
                           "stress_score": worst["stress_score"],
                           "level": worst["level"]},
            "spreads": rows}


def contagion_index(rows):
    """Average pairwise correlation of 30-day daily returns across every
    market. High = a synchronised move = systemic contagion."""
    series = []
    for r in rows:
        s = r.get("series") or []
        if len(s) >= 31:
            series.append([s[i] / s[i - 1] - 1.0
                           for i in range(len(s) - 30, len(s))])
    if len(series) < 3:
        return None
    cors = []
    for i in range(len(series)):
        for j in range(i + 1, len(series)):
            c = pearson(series[i], series[j])
            if c is not None:
                cors.append(c)
    if not cors:
        return None
    avg = sum(cors) / len(cors)
    # 0.2 = healthy dispersion .. 0.85 = everything moving as one
    score = round(clamp((avg - 0.2) / (0.85 - 0.2) * 100.0))
    return {"avg_pairwise_correlation": round(avg, 2),
            "pairs": len(cors),
            "stress_score": score, "level": stress_level(score)}


def stress_breadth(rows):
    """How widespread the stress is across the market matrix."""
    n = len(rows)
    if not n:
        return None
    elevated = sum(1 for r in rows if r["stress"] >= 32)
    stressed = sum(1 for r in rows if r["stress"] >= 55)
    acute = sum(1 for r in rows if r["stress"] >= 75)
    return {"markets": n,
            "elevated_plus": elevated,
            "elevated_plus_pct": round(elevated / n * 100),
            "stressed_plus": stressed,
            "acute": acute,
            "breadth_score": round(elevated / n * 100)}


def rates_panel():
    """The bond market's fear gauge -- realised volatility of the 10-year
    Treasury yield (a MOVE-style rate-vol read), plus the 2s10s curve as
    term-structure context. Rate volatility is what flips the bond market
    from orderly to disorderly; the curve is read alongside, not scored."""
    obs = fred_series("DGS10", days=560)
    yields = [v for _, v in obs]
    if len(yields) < 80:
        return None
    # daily yield changes in basis points
    chg = [(yields[i] - yields[i - 1]) * 100.0
           for i in range(1, len(yields))]
    if len(chg) < 60:
        return None
    # rolling 20-day realised vol of daily bp moves, annualised
    vser = []
    for end in range(20, len(chg) + 1):
        w = chg[end - 20:end]
        mean = sum(w) / 20.0
        var = sum((x - mean) ** 2 for x in w) / 19.0
        vser.append((var ** 0.5) * (252 ** 0.5))
    if not vser:
        return None
    rv = vser[-1]
    pc = pctile(rv, vser[-252:])
    # ~60bp annualised = calm, ~160bp = acute (a MOVE-like band)
    lvl_score = clamp((rv - 60.0) / (160.0 - 60.0) * 100.0)
    score = round(0.5 * lvl_score + 0.5 * (pc if pc is not None else 50.0))
    chg_1m = (round(rv - vser[-22]) if len(vser) > 21 else None)
    # 2s10s curve shape -- context only, not folded into the score
    curve, inverted = None, None
    cobs = fred_series("T10Y2Y", days=120)
    if cobs:
        curve = round(cobs[-1][1], 2)
        inverted = curve < 0
    return {
        "rate_vol_bp": round(rv),
        "percentile_1y": round(pc) if pc is not None else None,
        "change_1m_bp": chg_1m,
        "curve_2s10s": curve,
        "curve_inverted": inverted,
        "stress_score": score,
        "level": stress_level(score),
        "series": [round(v) for v in vser[-SERIES_BARS:]],
    }


def safe_haven_panel():
    """Flight-to-safety confirmation -- active demand for gold."""
    closes = get_closes("GLD")
    if not closes:
        return None
    vals = [c for _, c in closes]
    if len(vals) < 70:
        return None
    last = vals[-1]
    m1 = round((last / vals[-22] - 1) * 100, 1) if len(vals) > 21 else None
    m3 = round((last / vals[-64] - 1) * 100, 1) if len(vals) > 63 else None
    hi = max(vals[-252:]) if len(vals) >= 252 else max(vals)
    near_high = round((last / hi - 1) * 100, 1)
    demand = round(clamp(50.0 + (m1 if m1 is not None else 0.0) * 6.0))
    return {"gold": round(last, 2), "gold_1m_pct": m1, "gold_3m_pct": m3,
            "gold_vs_52w_high_pct": near_high,
            "haven_demand_score": demand,
            "level": stress_level(demand),
            "series": [round(v, 2) for v in vals[-SERIES_BARS:]]}


def sovereign_panel():
    """Sovereign-default stress -- the layer beneath corporate credit.

      Euro-area periphery: the BTP-Bund / Bonos-Bund spreads, read from
      the dedicated euro-fragmentation engine (the proper spread vs the
      German Bund benchmark, with a 0-100 fragmentation score).

      EM sovereign: USD emerging-market sovereign debt (EMB) measured
      against US Treasuries (IEF). A falling EMB/IEF ratio is the
      EMBI-style sovereign spread widening -- expressed as relative
      performance, it strips out the common duration move and isolates
      the sovereign credit component."""
    out_euro, out_em = None, None

    # --- euro-area periphery (from the fragmentation engine) ---
    frag = read_sidecar("data/euro-fragmentation.json")
    fr = frag.get("fragmentation") or {}
    cvp = frag.get("core_vs_periphery") or {}
    if isinstance(fr.get("score_0_100"), (int, float)):
        sc = round(clamp(fr["score_0_100"]))
        out_euro = {"stress_score": sc, "level": stress_level(sc),
                    "regime": fr.get("regime"),
                    "periphery_avg_spread_bp": cvp.get(
                        "periphery_avg_spread_bp"),
                    "widest_periphery": cvp.get("widest_periphery"),
                    "widest_spread_bp": fr.get("widest_spread_bp")}

    # --- EM sovereign: EMB vs US Treasuries (IEF) ---
    emb = get_closes("EMB")
    ief = get_closes("IEF")
    if emb and ief and len(emb) > 70 and len(ief) > 70:
        n = min(len(emb), len(ief))
        e = [c for _, c in emb][-n:]
        t = [c for _, c in ief][-n:]
        ratio = [e[i] / t[i] for i in range(n) if t[i]]
        if len(ratio) > 70:
            last = ratio[-1]
            hi = max(ratio[-252:]) if len(ratio) >= 252 else max(ratio)
            dd = (hi - last) / hi * 100.0 if hi else 0.0
            m1 = ((last / ratio[-22] - 1) * 100
                  if len(ratio) > 21 else None)
            # ratio drawdown 0% = calm, ~15% = acute spread blowout
            sc = round(clamp(dd / 15.0 * 100.0))
            out_em = {"stress_score": sc, "level": stress_level(sc),
                      "emb_vs_ust_drawdown_pct": round(dd, 1),
                      "ratio_change_1m_pct": (round(m1, 1)
                                              if m1 is not None else None),
                      "series": [round(r, 4) for r in ratio[-SERIES_BARS:]]}

    scores = [d["stress_score"] for d in (out_euro, out_em) if d]
    if not scores:
        return None
    composite = round(sum(scores) / len(scores))
    return {"stress_score": composite, "level": stress_level(composite),
            "euro_periphery": out_euro, "em_sovereign": out_em}


def funding_panel():
    """Money-market / funding stress -- a cross-reference into the
    dedicated eurodollar-stress engine. That engine scores eight funding
    signals hourly (the broad dollar, EM FX, T-bill demand, the
    SOFR-IORB spread, cross-currency basis, oil, the 30Y-10Y curve
    inversion, bond vol) and publishes a composite 0-100 plus the
    hottest stressors. Surfaced here as a context panel only -- funding
    does NOT feed the GSI blend, because the eurodollar signal is
    already its own component of the firm-wide crisis composite. The
    intent is visibility, not double-counting."""
    fs = read_sidecar("data/eurodollar-stress.json")
    if not isinstance(fs.get("composite_score"), (int, float)):
        return None
    sc = round(clamp(fs["composite_score"]))
    hot = fs.get("hot_signals") or []
    return {
        "stress_score": sc,
        "level": stress_level(sc),
        "severity": fs.get("severity"),
        "regime": fs.get("regime"),
        "n_signals": fs.get("n_signals_used"),
        "n_signals_used": fs.get("n_signals_used"),
        "n_signals_total": fs.get("n_signals_total"),
        "n_failures": fs.get("n_failures"),
        "hot_signals": [{"id": h.get("id"), "label": h.get("label"),
                         "score": h.get("score")}
                        for h in hot[:3]],
        "as_of": fs.get("as_of"),
        "source": "eurodollar-stress engine (hourly)",
    }


def update_history(gsi, market, eq, bd):
    """Append this run to the rolling history and return the snapshots."""
    hist = read_prev_history()
    snaps = hist.get("snapshots", [])
    snaps.append({"ts": datetime.now(timezone.utc).isoformat(),
                  "gsi": gsi, "market": market, "equity": eq, "bond": bd})
    snaps = snaps[-480:]
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=HIST_KEY,
                      Body=json.dumps({"snapshots": snaps},
                                      default=str).encode("utf-8"),
                      ContentType="application/json")
    except Exception as e:
        print("history write fail: %s" % e)
    return snaps


def stress_momentum(snaps, gsi):
    """Direction and rate of change of the Global Stress Index, plus where
    the current reading sits in its own accumulated history."""
    prior = [s.get("gsi") for s in snaps[:-1]
             if isinstance(s.get("gsi"), (int, float))]
    allg = [s.get("gsi") for s in snaps
            if isinstance(s.get("gsi"), (int, float))]
    pct = pctile(gsi, allg) if len(allg) >= 12 else None
    if not prior:
        return {"direction": "n/a", "change_5_runs": None,
                "change_20_runs": None, "percentile": None,
                "runs_tracked": len(snaps)}
    ref5 = prior[-5] if len(prior) >= 5 else prior[0]
    ref20 = prior[-20] if len(prior) >= 20 else prior[0]
    d5 = gsi - ref5
    direction = ("RISING" if d5 >= 4 else
                 "FALLING" if d5 <= -4 else "STABLE")
    return {"direction": direction,
            "change_5_runs": round(d5),
            "change_20_runs": round(gsi - ref20),
            "percentile": round(pct) if pct is not None else None,
            "runs_tracked": len(snaps)}


# ---- handler ---------------------------------------------------------------
def scan(spec, kind):
    key, sym, name, tracks = spec
    closes = get_closes(sym)
    if not closes:
        return None
    st = stress_for([c for _, c in closes], kind)
    if not st:
        return None
    return {"key": key, "symbol": sym, "market": name,
            "tracks": tracks, "asset_class": kind,
            "as_of": closes[-1][0], **st}


# ---- multi-dimensional stress escalation -----------------------------------
# The Global Stress Index is a weighted blend, so a single hot component can
# be diluted inside the headline number. A real stress desk does not wait for
# the blend -- it escalates per dimension. This layer classifies every scored
# stress dimension into GREEN / AMBER / RED and edge-detects the moment any one
# of them crosses into RED on its own (credit, rate-vol, sovereign and funding
# routinely lead the equity matrix into a crisis).
ESC_RED = 75      # ACUTE
ESC_AMBER = 55    # STRESSED


def esc_band(score):
    if not isinstance(score, (int, float)):
        return None
    if score >= ESC_RED:
        return "RED"
    if score >= ESC_AMBER:
        return "AMBER"
    return "GREEN"


def build_stress_escalation(global_stress, market_stress, worst, iv,
                            credit, rates, sovereign, funding, contagion,
                            prev):
    """Assemble every scored stress dimension into a GREEN/AMBER/RED
    escalation matrix, edge-detect the dimensions that have NEWLY gone RED
    since the last run, and derive a single firm-wide stress posture."""
    dims = []

    def add(key, label, score, detail):
        b = esc_band(score)
        if b is None:
            return
        dims.append({"key": key, "label": label,
                     "score": int(round(score)), "band": b,
                     "detail": detail})

    if market_stress is not None:
        add("market_matrix", "Market Matrix", market_stress,
            ("worst market: %s -- %s at %d/100"
             % (worst["market"], worst["tracks"], worst["stress"]))
            if worst else "cross-market price action")
    if iv:
        add("equity_vol", "Equity Vol (VIX)", iv.get("stress_score"),
            "VIX %.1f, %s pct 1y percentile"
            % (iv.get("vix") or 0.0, iv.get("percentile_1y")))
    if credit:
        wt = credit.get("worst_tier") or {}
        add("credit", "Credit Spreads", credit.get("composite_score"),
            "worst tier: %s OAS %.2f pct"
            % (wt.get("name") or "n/a", wt.get("oas_pct") or 0.0))
    if rates:
        add("rate_vol", "Rate Volatility", rates.get("stress_score"),
            "10y yield realised vol %d bp annualised"
            % (rates.get("rate_vol_bp") or 0))
    if sovereign:
        ep = (sovereign.get("euro_periphery") or {}).get("stress_score")
        em = (sovereign.get("em_sovereign") or {}).get("stress_score")
        add("sovereign", "Sovereign Stress", sovereign.get("stress_score"),
            "euro periphery %s, EM sovereign %s"
            % (ep if ep is not None else "n/a",
               em if em is not None else "n/a"))
    if funding:
        add("funding", "USD Funding Plumbing", funding.get("stress_score"),
            "regime: %s" % (funding.get("regime") or "n/a"))
    if contagion:
        add("contagion", "Cross-Market Contagion",
            contagion.get("stress_score"),
            "%d pct of markets stressed"
            % (contagion.get("breadth_score") or 0))
    if global_stress is not None:
        add("global_index", "Global Stress Index", global_stress,
            "the headline weighted blend")

    dims.sort(key=lambda d: -d["score"])

    # edge detection -- a dimension counts as NEWLY red only when the prior
    # run carried an escalation block and that dimension was not RED then,
    # so a dimension that stays red does not re-alert every run.
    prev_esc = (prev or {}).get("escalation") or {}
    had_prev = bool(prev_esc.get("dimensions"))
    prev_band = {d.get("key"): d.get("band")
                 for d in prev_esc.get("dimensions", [])
                 if isinstance(d, dict)}
    newly_red = []
    if had_prev:
        newly_red = [d for d in dims
                     if d["band"] == "RED"
                     and prev_band.get(d["key"]) != "RED"]

    n_red = sum(1 for d in dims if d["band"] == "RED")
    n_amber = sum(1 for d in dims if d["band"] == "AMBER")
    posture = "RED" if n_red else ("AMBER" if n_amber else "GREEN")
    block = {
        "posture": posture,
        "n_red": n_red,
        "n_amber": n_amber,
        "dimensions": dims,
        "red_dimensions": [d["label"] for d in dims if d["band"] == "RED"],
        "newly_red": [d["key"] for d in newly_red],
        "baseline_run": not had_prev,
        "as_of": datetime.now(timezone.utc).isoformat(),
        "bands": {"green": "<55", "amber": "55-74 STRESSED",
                  "red": ">=75 ACUTE"},
        "note": ("Per-dimension escalation. The blended Global Stress Index "
                 "can sit calm while one dimension -- credit, rate vol, "
                 "sovereign or funding -- goes ACUTE on its own; this matrix "
                 "catches that and the tripwire fires on it directly."),
    }
    return block, newly_red


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    if isinstance(event, dict) and event.get("test_telegram"):
        send_telegram("\u2705 <b>Global Stress Matrix</b> -- Telegram "
                      "tripwire armed and reachable. You will get a single "
                      "push the moment any market OR any stress dimension "
                      "(credit, rate vol, sovereign, funding, VIX, "
                      "contagion) crosses into ACUTE.")
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "test_telegram": "sent"})}
    if not FMP:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": "no FMP key"})}

    jobs = [(s, "equity") for s in EQUITY] + [(s, "bond") for s in BONDS]
    rows = []
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        for res in ex.map(lambda j: scan(j[0], j[1]), jobs):
            if res:
                rows.append(res)

    equities = [r for r in rows if r["asset_class"] == "equity"]
    bonds = [r for r in rows if r["asset_class"] == "bond"]
    equities.sort(key=lambda r: -r["stress"])
    bonds.sort(key=lambda r: -r["stress"])

    def avg(xs):
        return round(sum(xs) / len(xs)) if xs else None

    eq_stress = avg([r["stress"] for r in equities])
    bd_stress = avg([r["stress"] for r in bonds])
    alls = [r["stress"] for r in rows]
    # the market-matrix reading: the average pulled up by the worst market
    market_stress = None
    if alls:
        market_stress = round(0.6 * (sum(alls) / len(alls))
                              + 0.4 * max(alls))

    # ---- additional stress dimensions ----------------------------------
    iv = implied_vol_panel()         # forward-looking equity fear (VIX)
    credit = credit_spread_panel()   # ICE BofA OAS credit ladder
    rates = rates_panel()            # Treasury rate volatility + curve
    sovereign = sovereign_panel()    # euro-periphery + EM sovereign stress
    funding = funding_panel()        # USD funding plumbing (cross-ref)
    contagion = contagion_index(rows)   # cross-market correlation
    breadth = stress_breadth(rows)      # how widespread the stress is
    haven = safe_haven_panel()          # active flight-to-safety demand

    # ---- the Global Stress Index: a weighted blend of the market
    # matrix, credit spreads, equity-implied vol, rate volatility,
    # cross-market contagion and sovereign-default stress, renormalised
    # over whatever components are available this run --------------------
    blend = []
    if market_stress is not None:
        blend.append((0.32, market_stress))
    if credit:
        blend.append((0.18, credit["composite_score"]))
    if iv:
        blend.append((0.17, iv["stress_score"]))
    if rates:
        blend.append((0.13, rates["stress_score"]))
    if contagion:
        blend.append((0.10, contagion["stress_score"]))
    if sovereign:
        blend.append((0.10, sovereign["stress_score"]))
    global_stress = None
    if blend:
        wsum = sum(w for w, _ in blend)
        global_stress = round(sum(w * s for w, s in blend) / wsum)

    worst = max(rows, key=lambda r: r["stress"]) if rows else None
    flashing = [r["market"] + " " + r["asset_class"]
                for r in rows if r["stress"] >= 75]

    # momentum needs the final index; history records it
    snaps = update_history(global_stress, market_stress, eq_stress,
                           bd_stress)
    momentum = (stress_momentum(snaps, global_stress)
                if global_stress is not None else None)

    headline = "n/a"
    if global_stress is not None and worst:
        extra = []
        if iv:
            extra.append("VIX %.0f" % iv["vix"])
        if credit:
            extra.append("credit %d" % credit["composite_score"])
        if rates:
            extra.append("rate-vol %d" % rates["rate_vol_bp"])
        if sovereign:
            extra.append("sovereign %d" % sovereign["stress_score"])
        if funding:
            extra.append("funding %d" % funding["stress_score"])
        if contagion:
            extra.append("contagion %d" % contagion["stress_score"])
        headline = (
            "Global Stress Index %d/100 (%s)%s. Equity stress %s, bond "
            "stress %s. Most stressed market: %s -- %s at %d/100 (%s).%s"
            % (global_stress, stress_level(global_stress),
               (" -- " + ", ".join(extra)) if extra else "",
               eq_stress, bd_stress, worst["market"], worst["tracks"],
               worst["stress"], worst["level"],
               (" Stress %s." % momentum["direction"])
               if momentum and momentum["direction"] != "n/a" else ""))

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-global-stress",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 1),
        "global_stress_index": global_stress,
        "global_stress_level": (stress_level(global_stress)
                                if global_stress is not None else None),
        "market_stress_index": market_stress,
        "equity_stress": eq_stress,
        "bond_stress": bd_stress,
        "implied_vol": iv,
        "credit_spreads": credit,
        "rates": rates,
        "sovereign": sovereign,
        "funding": funding,
        "contagion": contagion,
        "breadth": breadth,
        "safe_haven": haven,
        "stress_momentum": momentum,
        "worst_market": ({"market": worst["market"],
                          "asset_class": worst["asset_class"],
                          "stress": worst["stress"],
                          "level": worst["level"]} if worst else None),
        "flashing_red": flashing,
        "headline": headline,
        "equities": equities,
        "bonds": bonds,
        "thresholds": {"calm": "<32", "elevated": "32-54",
                       "stressed": "55-74", "acute": ">=75 (flashes red)"},
        "how_to_read": (
            "The Global Stress Index is a weighted blend of five "
            "dimensions: the market matrix (drawdown, realised vol and "
            "trend across 13 world equity and bond markets), credit "
            "spreads (HY and IG option-adjusted spreads), equity-implied "
            "volatility (the VIX), rate volatility (realised volatility of "
            "the 10-year Treasury yield -- a MOVE-style bond fear gauge), "
            "and cross-market contagion (how correlated the markets have "
            "become), plus sovereign-default stress (euro-area periphery "
            "BTP/Bonos-Bund spreads and EM USD sovereign debt versus "
            "Treasuries). 75+ is ACUTE. The market matrix drives the per-"
            "market flashing-red flags; breadth, the 2s10s curve, safe-"
            "haven demand and stress momentum are read alongside as "
            "confirmation. Credit is read across the full ICE BofA OAS "
            "ladder -- US high-yield BB/B/CCC, investment grade and EM "
            "corporate -- with the CCC-vs-BB dispersion flagging distress "
            "concentrating in the weakest credits."),
        "disclaimer": (
            "A market-stress monitor built from price action, credit "
            "spreads and implied volatility. It measures stress that is "
            "already present; it is not a forecast or investment advice."),
    }

    # ---- multi-dimensional escalation matrix --------------------------------
    # Classify every stress dimension GREEN/AMBER/RED and edge-detect the ones
    # that have just crossed into RED. Read once here so the tripwire below can
    # reuse the same previous-output snapshot for per-market edge detection.
    prev = read_prev_output()
    escalation, newly_red_dims = build_stress_escalation(
        global_stress, market_stress, worst, iv, credit, rates,
        sovereign, funding, contagion, prev)
    out["escalation"] = escalation

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out, default=str).encode("utf-8"),
                      ContentType="application/json")
    except Exception as e:
        print("S3 write fail: %s" % e)
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": str(e)})}

    # --- stress tripwire -- ONE consolidated push the moment any market OR
    # any stress dimension NEWLY crosses into ACUTE/RED. A dimension or market
    # that stays red does not re-alert; the previous output is the no-spam
    # state, and a broad risk-off that trips several dimensions at once still
    # sends a single message rather than one per dimension.
    alerted = False
    try:
        prev_flash = set(prev.get("flashing_red") or [])
        new_flash = [m for m in flashing if m not in prev_flash]
        if new_flash or newly_red_dims:
            lines = ["\U0001F6A8 <b>GLOBAL STRESS -- escalation</b>", ""]
            for d in newly_red_dims:
                lines.append("Just went ACUTE: <b>%s</b> %d/100 -- %s."
                             % (d["label"], d["score"], d["detail"]))
            if new_flash:
                lines.append("Markets just flashing red: <b>%s</b>."
                             % ", ".join(new_flash))
            lines.append("")
            lines.append("Stress posture: <b>%s</b> -- %d red, %d amber "
                         "across %d dimensions."
                         % (escalation["posture"], escalation["n_red"],
                            escalation["n_amber"],
                            len(escalation["dimensions"])))
            red_now = escalation.get("red_dimensions") or []
            if red_now:
                lines.append("All dimensions red now: %s."
                             % ", ".join(red_now))
            lines.append("Global Stress Index %s (%s) -- equity %s, bond %s."
                         % (global_stress,
                            stress_level(global_stress)
                            if global_stress is not None else "n/a",
                            eq_stress, bd_stress))
            lines.append("")
            lines.append("justhodl.ai/global-stress.html")
            send_telegram("\n".join(lines))
            alerted = True
    except Exception as e:
        print("[tg] stress alert err: %s" % e)

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "global_stress_index": global_stress,
        "global_stress_level": (stress_level(global_stress)
                                if global_stress is not None else None),
        "market_stress_index": market_stress,
        "equity_stress": eq_stress, "bond_stress": bd_stress,
        "vix": (iv or {}).get("vix"),
        "credit_composite": (credit or {}).get("composite_score"),
        "rate_vol_bp": (rates or {}).get("rate_vol_bp"),
        "sovereign_score": (sovereign or {}).get("stress_score"),
        "funding_score": (funding or {}).get("stress_score"),
        "contagion_score": (contagion or {}).get("stress_score"),
        "stress_direction": (momentum or {}).get("direction"),
        "markets_scored": len(rows), "flashing_red": len(flashing),
        "escalation_posture": escalation["posture"],
        "escalation_red": escalation["n_red"],
        "escalation_amber": escalation["n_amber"],
        "escalation_newly_red": escalation["newly_red"],
        "worst": worst["market"] if worst else None,
        "telegram_alert": alerted,
        "build_seconds": out["build_seconds"]})}
