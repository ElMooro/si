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
    ("em", "EEM", "Emerging Markets", "MSCI Emerging Markets"),
]
BONDS = [
    ("ust", "IEF", "US Treasuries", "7-10y US Treasuries"),
    ("uscredit", "HYG", "US Credit", "US high-yield corporates"),
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
    """The real bond-market stress signal -- option-adjusted spreads.
    HY and IG OAS by level and 1y percentile."""
    specs = [("hy", "BAMLH0A0HYM2", "US High-Yield OAS", 3.0, 9.0),
             ("ig", "BAMLC0A0CM", "US Investment-Grade OAS", 0.8, 2.5)]
    rows, scores = [], []
    for key, sid, name, calm, acute in specs:
        obs = fred_series(sid, days=460)
        vals = [v for _, v in obs]
        if len(vals) < 60:
            continue
        last = vals[-1]
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
    return {"composite_score": composite, "level": stress_level(composite),
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
    """Direction and rate of change of the Global Stress Index."""
    prior = [s.get("gsi") for s in snaps[:-1]
             if isinstance(s.get("gsi"), (int, float))]
    if not prior:
        return {"direction": "n/a", "change_5_runs": None,
                "change_20_runs": None, "runs_tracked": len(snaps)}
    ref5 = prior[-5] if len(prior) >= 5 else prior[0]
    ref20 = prior[-20] if len(prior) >= 20 else prior[0]
    d5 = gsi - ref5
    direction = ("RISING" if d5 >= 4 else
                 "FALLING" if d5 <= -4 else "STABLE")
    return {"direction": direction,
            "change_5_runs": round(d5),
            "change_20_runs": round(gsi - ref20),
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


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    if isinstance(event, dict) and event.get("test_telegram"):
        send_telegram("\u2705 <b>Global Stress Matrix</b> -- Telegram "
                      "tripwire armed and reachable. You will get a push the "
                      "moment any market goes ACUTE and flashes red.")
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
    credit = credit_spread_panel()   # HY + IG option-adjusted spreads
    contagion = contagion_index(rows)   # cross-market correlation
    breadth = stress_breadth(rows)      # how widespread the stress is
    haven = safe_haven_panel()          # active flight-to-safety demand

    # ---- the Global Stress Index: a weighted blend of the market
    # matrix, credit spreads, implied vol and contagion, renormalised
    # over whatever components are available this run --------------------
    blend = []
    if market_stress is not None:
        blend.append((0.40, market_stress))
    if credit:
        blend.append((0.22, credit["composite_score"]))
    if iv:
        blend.append((0.22, iv["stress_score"]))
    if contagion:
        blend.append((0.16, contagion["stress_score"]))
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
            "The Global Stress Index is a weighted blend of four "
            "dimensions: the market matrix (drawdown, realised vol and "
            "trend across 10 world equity and bond markets), credit "
            "spreads (HY and IG option-adjusted spreads), implied "
            "volatility (the VIX), and cross-market contagion (how "
            "correlated the markets have become). 75+ is ACUTE. The "
            "market matrix drives the per-market flashing-red flags; "
            "breadth, safe-haven demand and stress momentum are read "
            "alongside as confirmation."),
        "disclaimer": (
            "A market-stress monitor built from price action, credit "
            "spreads and implied volatility. It measures stress that is "
            "already present; it is not a forecast or investment advice."),
    }
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out, default=str).encode("utf-8"),
                      ContentType="application/json")
    except Exception as e:
        print("S3 write fail: %s" % e)
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": str(e)})}

    # --- ACUTE / flashing-red tripwire -- fires only for a market that has
    # NEWLY gone ACUTE since the last run, so a market that stays red does
    # not re-alert. The previous output is the no-spam state.
    alerted = False
    try:
        prev = read_prev_output()
        prev_flash = set(prev.get("flashing_red") or [])
        new_flash = [m for m in flashing if m not in prev_flash]
        prev_gsi = prev.get("global_stress_index")
        gsi_into_acute = (global_stress is not None and global_stress >= 75
                          and not (isinstance(prev_gsi, (int, float))
                                   and prev_gsi >= 75))
        if new_flash or gsi_into_acute:
            lines = ["\U0001F6A8 <b>GLOBAL STRESS -- flashing red</b>", ""]
            if new_flash:
                lines.append("Just went ACUTE: <b>%s</b>."
                             % ", ".join(new_flash))
            if gsi_into_acute:
                lines.append("The Global Stress Index itself has crossed "
                             "into ACUTE -- broad, cross-market stress.")
            lines.append("Now flashing red: %s."
                         % (", ".join(flashing) if flashing else "none"))
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
        "contagion_score": (contagion or {}).get("stress_score"),
        "stress_direction": (momentum or {}).get("direction"),
        "markets_scored": len(rows), "flashing_red": len(flashing),
        "worst": worst["market"] if worst else None,
        "telegram_alert": alerted,
        "build_seconds": out["build_seconds"]})}
