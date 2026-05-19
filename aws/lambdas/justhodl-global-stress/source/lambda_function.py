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
from datetime import datetime, timezone

import boto3

SCHEMA = "1.0"
BASE = "https://financialmodelingprep.com/stable"
FMP = os.environ.get("FMP_KEY", "")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/global-stress.json"

HIST_BARS = 300
SERIES_BARS = 130

s3 = boto3.client("s3")

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
    # global index leans on the average but is pulled up by the worst
    global_stress = None
    if alls:
        global_stress = round(0.6 * (sum(alls) / len(alls))
                              + 0.4 * max(alls))
    worst = max(rows, key=lambda r: r["stress"]) if rows else None
    flashing = [r["market"] + " " + r["asset_class"]
                for r in rows if r["stress"] >= 75]

    headline = "n/a"
    if global_stress is not None and worst:
        headline = (
            "Global Stress Index %d/100 (%s). Equity stress %s, bond "
            "stress %s. Most stressed: %s -- %s at %d/100 (%s)."
            % (global_stress, stress_level(global_stress),
               eq_stress, bd_stress, worst["market"],
               worst["tracks"], worst["stress"], worst["level"]))

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-global-stress",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 1),
        "global_stress_index": global_stress,
        "global_stress_level": (stress_level(global_stress)
                                if global_stress is not None else None),
        "equity_stress": eq_stress,
        "bond_stress": bd_stress,
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
            "Each market scores 0-100 on stress already visible in the "
            "tape: how far below its 52-week high it trades, where its "
            "20-day realised volatility sits in its own one-year range, "
            "and how far it sits below its 200-day average. 75+ is ACUTE "
            "and flashes red. The Global Stress Index blends the average "
            "with the single worst market, so one market blowing out "
            "still lifts the headline."),
        "disclaimer": (
            "A market-stress thermometer built from price action across "
            "global equity and bond ETFs. It measures stress that is "
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

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "global_stress_index": global_stress,
        "equity_stress": eq_stress, "bond_stress": bd_stress,
        "markets_scored": len(rows), "flashing_red": len(flashing),
        "worst": worst["market"] if worst else None,
        "build_seconds": out["build_seconds"]})}
