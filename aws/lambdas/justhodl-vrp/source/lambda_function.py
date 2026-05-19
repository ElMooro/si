"""justhodl-vrp -- the Volatility Risk Premium engine.

The volatility risk premium is the gap between the volatility the
options market PRICES (implied) and the volatility that actually
OCCURS (realized). It is, on average, positive: sellers of options
are paid a premium for underwriting volatility insurance. That
premium is one of the most-watched gauges on a volatility desk -- it
says whether selling vol is well paid right now, and, just as
importantly, when it inverts (realized running hotter than implied)
it is a clean, early stress signal.

The platform priced implied vol (vix-curve) and realized vol in
passing inside other engines, but never put the two together. This
engine does, the way a vol desk would:

  * REALIZED vol of the equity index over 10/21/63-day windows --
    close-to-close, plus a Garman-Klass range estimate that cuts the
    close-to-close noise.
  * CONTEMPORANEOUS VRP at each tenor -- implied minus realized, 9d /
    30d / 3m -- the live, tradeable read, and its percentile and
    z-score versus its own trailing year.
  * EX-POST VRP -- the honest scorecard: the vol VIX priced N days
    ago minus the vol that subsequently happened. This is whether the
    premium actually PAID, not just what it looks like today.
  * A regime read -- RICH / NORMAL / THIN / INVERTED -- with what it
    means for vol-sellers and, since the platform runs a tail hedge,
    for the cost of carrying long-vol protection.

Stylised research read on a hypothetical book; not investment advice.
"""
import json
import math
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/vrp.json"
HIST_KEY = "data/vrp-history.json"
SCHEMA = "1.0"
FMP_KEY = os.environ.get("FMP_KEY", "")

TRADING_DAYS = 252
RV_FETCH_DAYS = 470          # calendar days of SPY history to pull
PCTL_WINDOW = 252            # trailing window for the VRP percentile
EXPOST_HORIZON = 21          # trading days for the ex-post realized leg

s3 = boto3.client("s3", region_name=REGION)


# --------------------------------------------------------------------------
def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def num(v):
    try:
        return None if v is None else float(v)
    except Exception:
        return None


def stdev(xs):
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def _fmp_eod(endpoint, d_from):
    url = ("https://financialmodelingprep.com/stable/%s"
           "?symbol=SPY&from=%s&apikey=%s"
           % (endpoint, d_from, urllib.parse.quote(FMP_KEY)))
    req = urllib.request.Request(
        url, headers={"User-Agent": "justhodl-vrp/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        data = json.loads(r.read())
    rows = data if isinstance(data, list) else (data or {}).get(
        "historical", [])
    out = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        d = str(row.get("date") or "")[:10]
        c = num(row.get("close"))
        if c is None:
            c = num(row.get("price"))
        if d and c is not None:
            out.append({"date": d, "o": num(row.get("open")),
                        "h": num(row.get("high")),
                        "l": num(row.get("low")), "c": c})
    out.sort(key=lambda x: x["date"])
    return out


def fmp_spy_ohlc():
    """SPY daily bars via FMP /stable. Tries OHLC (full); falls back to
    close-only (light) so close-to-close RV never depends on OHLC."""
    if not FMP_KEY:
        return [], "missing_fmp_key"
    d_from = (datetime.now(timezone.utc).date()
              - timedelta(days=RV_FETCH_DAYS)).isoformat()
    try:
        out = _fmp_eod("historical-price-eod/full", d_from)
        if out:
            return out, "ok"
    except Exception as e:
        print("fmp full fail: %s" % e)
    try:
        out = _fmp_eod("historical-price-eod/light", d_from)
        if out:
            return out, "ok_light"
    except Exception as e:
        return [], "%s: %s" % (type(e).__name__, e)
    return [], "empty"


def rv_close_to_close(closes):
    """Annualised close-to-close realized vol (%) over the given closes."""
    if len(closes) < 3:
        return None
    rets = [math.log(closes[i] / closes[i - 1])
            for i in range(1, len(closes)) if closes[i - 1] > 0]
    sd = stdev(rets)
    return None if sd is None else sd * math.sqrt(TRADING_DAYS) * 100.0


def rv_garman_klass(bars):
    """Annualised Garman-Klass realized vol (%) over OHLC bars."""
    k = 2.0 * math.log(2.0) - 1.0
    daily = []
    for b in bars:
        o, h, lo, c = b.get("o"), b.get("h"), b.get("l"), b.get("c")
        if not all(isinstance(x, (int, float)) and x > 0
                   for x in (o, h, lo, c)):
            return None
        daily.append(0.5 * (math.log(h / lo)) ** 2
                     - k * (math.log(c / o)) ** 2)
    if not daily:
        return None
    var = sum(daily) / len(daily)
    if var < 0:
        var = 0.0
    return math.sqrt(var * TRADING_DAYS) * 100.0


def percentile_rank(series, value):
    if not series or value is None:
        return None
    below = sum(1 for x in series if x <= value)
    return round(100.0 * below / len(series), 1)


# --------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    # ---- implied side: vix-curve --------------------------------------
    vc = read_json("data/vix-curve.json") or {}
    cur = vc.get("current") or {}
    vix9d = num(cur.get("vix9d"))
    vix = num(cur.get("vix"))
    vix3m = num(cur.get("vix3m"))
    vix6m = num(cur.get("vix6m"))
    iv_date = vc.get("data_date")

    vch = read_json("data/vix-curve-history.json") or {}
    vix_dates = vch.get("dates") or []
    vix_series = (vch.get("series") or {}).get("vix") or []
    vix_by_date = {}
    for i, d in enumerate(vix_dates):
        if i < len(vix_series) and vix_series[i] is not None:
            vix_by_date[str(d)[:10]] = float(vix_series[i])

    # ---- realized side: SPY -------------------------------------------
    bars, spy_status = fmp_spy_ohlc()
    closes_all = [b["c"] for b in bars]

    def rv_window(n):
        return (rv_close_to_close(closes_all[-(n + 1):])
                if len(closes_all) > n else None)

    rv_10 = rv_window(10)
    rv_21 = rv_window(21)
    rv_63 = rv_window(63)
    rv_gk_21 = (rv_garman_klass(bars[-21:]) if len(bars) >= 21 else None)

    # ---- contemporaneous VRP by tenor ---------------------------------
    def vrp(iv, rv):
        return (round(iv - rv, 2) if (iv is not None and rv is not None)
                else None)

    vrp_9d = vrp(vix9d, rv_10)
    vrp_30d = vrp(vix, rv_21)
    vrp_3m = vrp(vix3m, rv_63)

    # ---- VRP time series + percentile/z-score -------------------------
    # rolling 21d close-to-close RV by date, paired with VIX same date
    vrp_series = []
    series_dates = []
    rv_by_date = {}
    for i in range(21, len(bars)):
        d = bars[i]["date"]
        rv = rv_close_to_close([b["c"] for b in bars[i - 21:i + 1]])
        if rv is None:
            continue
        rv_by_date[d] = rv
        iv = vix_by_date.get(d)
        if iv is not None:
            vrp_series.append(round(iv - rv, 2))
            series_dates.append(d)

    pctl = zscore = vrp_mean = vrp_min = vrp_max = None
    if vrp_series:
        win = vrp_series[-PCTL_WINDOW:]
        latest_vrp = vrp_30d if vrp_30d is not None else win[-1]
        pctl = percentile_rank(win, latest_vrp)
        m = sum(win) / len(win)
        sd = stdev(win)
        vrp_mean = round(m, 2)
        vrp_min = round(min(win), 2)
        vrp_max = round(max(win), 2)
        if sd and sd > 1e-9:
            zscore = round((latest_vrp - m) / sd, 2)

    # ---- ex-post VRP: vol VIX priced N days ago vs what happened ------
    expost_vals = []
    sorted_dates = [b["date"] for b in bars]
    for i in range(len(bars) - EXPOST_HORIZON):
        d = bars[i]["date"]
        iv = vix_by_date.get(d)
        if iv is None:
            continue
        fwd = [b["c"] for b in bars[i:i + EXPOST_HORIZON + 1]]
        fwd_rv = rv_close_to_close(fwd)
        if fwd_rv is not None:
            expost_vals.append(iv - fwd_rv)
    expost_mean = (round(sum(expost_vals) / len(expost_vals), 2)
                   if expost_vals else None)
    expost_hit = (round(100.0 * sum(1 for x in expost_vals if x > 0)
                        / len(expost_vals), 1) if expost_vals else None)

    # ---- VRP term structure -------------------------------------------
    term = None
    if vrp_9d is not None and vrp_3m is not None:
        if vrp_3m > vrp_9d + 0.5:
            term = "UPWARD"
        elif vrp_3m < vrp_9d - 0.5:
            term = "INVERTED"
        else:
            term = "FLAT"

    # ---- regime --------------------------------------------------------
    feed_ok = spy_status in ("ok", "ok_light") and vix is not None
    if not feed_ok:
        regime, rcolor = "UNAVAILABLE", "dim"
        headline = ("VRP unavailable -- %s."
                    % ("SPY feed " + spy_status if spy_status != "ok"
                       else "no implied-vol feed"))
        vsell = hedger = None
    elif vrp_30d is not None and vrp_30d < 0:
        regime, rcolor = "INVERTED", "red"
        headline = ("Volatility risk premium INVERTED -- 30-day realized "
                    "(%.1f) is running above implied (%.1f), VRP %.1f. "
                    "Realized is outrunning what the market prices; a "
                    "classic fast-tape stress signature."
                    % (rv_21 or 0, vix or 0, vrp_30d))
        vsell = ("Vol-selling is underwater here -- realized is beating "
                 "the premium collected. Stand down or hedge the short.")
        hedger = ("Long-vol protection is cheap relative to what is "
                  "actually moving -- carry is light, the regime is hot.")
    elif pctl is not None and pctl < 25:
        regime, rcolor = "THIN", "orange"
        headline = ("Volatility risk premium THIN -- VRP %.1f sits in the "
                    "%sth percentile of the past year. The premium for "
                    "underwriting vol is compressed."
                    % (vrp_30d if vrp_30d is not None else 0, int(pctl)))
        vsell = ("Vol-selling is poorly compensated -- a thin cushion "
                 "against a realized spike. Size down.")
        hedger = ("Protection is relatively cheap -- a reasonable window "
                  "to add or roll hedges before the premium re-rates.")
    elif pctl is not None and pctl >= 75:
        regime, rcolor = "RICH", "green"
        headline = ("Volatility risk premium RICH -- VRP %.1f in the %sth "
                    "percentile of the past year. Implied sits well above "
                    "realized; underwriting vol is well paid."
                    % (vrp_30d if vrp_30d is not None else 0, int(pctl)))
        vsell = ("Vol-selling is well compensated -- but a rich premium "
                 "is also a complacency reading; respect tail risk.")
        hedger = ("Long-vol protection is expensive -- hedge carry is "
                  "heavy here; favour spreads over outright premium.")
    else:
        regime, rcolor = "NORMAL", "cyan"
        headline = ("Volatility risk premium NORMAL -- VRP %.1f, around "
                    "the middle of its trailing-year range. Implied vol "
                    "carries its usual cushion over realized."
                    % (vrp_30d if vrp_30d is not None else 0))
        vsell = ("Vol-selling is fairly paid -- standard premium, "
                 "standard sizing.")
        hedger = ("Hedge carry is around normal -- no urgency either way "
                  "on protection.")

    desk_bits = [headline]
    if expost_mean is not None:
        desk_bits.append(
            "Ex-post, over the past year the vol priced 21 days ahead beat "
            "what realized by an average of %+.1f point(s) (%s%% of "
            "windows positive) -- the premium %s."
            % (expost_mean, int(expost_hit or 0),
               "has been earning" if expost_mean > 0
               else "has not been earning"))
    if term:
        desk_bits.append("VRP term structure is %s (9d %.1f vs 3m %.1f)."
                         % (term, vrp_9d or 0, vrp_3m or 0))
    desk_note = " ".join(desk_bits)

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-vrp",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),
        "feed_status": {"spy": spy_status,
                        "implied": "ok" if vix is not None else "missing"},

        "regime": regime,
        "regime_color": rcolor,
        "headline": headline,
        "desk_note": desk_note,

        "implied": {
            "vix9d": vix9d, "vix": vix, "vix3m": vix3m, "vix6m": vix6m,
            "source": "vix-curve (CBOE)", "source_date": iv_date,
        },
        "realized": {
            "rv_10d": round(rv_10, 2) if rv_10 is not None else None,
            "rv_21d": round(rv_21, 2) if rv_21 is not None else None,
            "rv_63d": round(rv_63, 2) if rv_63 is not None else None,
            "rv_garman_klass_21d": round(rv_gk_21, 2)
            if rv_gk_21 is not None else None,
            "underlying": "SPY", "n_bars": len(bars),
        },
        "vrp": {
            "vrp_9d": vrp_9d,
            "vrp_30d": vrp_30d,
            "vrp_3m": vrp_3m,
            "vrp_30d_percentile_1y": pctl,
            "vrp_30d_zscore_1y": zscore,
            "vrp_30d_mean_1y": vrp_mean,
            "vrp_30d_min_1y": vrp_min,
            "vrp_30d_max_1y": vrp_max,
            "term_structure": term,
            "expost_vrp_mean": expost_mean,
            "expost_positive_pct": expost_hit,
        },
        "interpretation": {
            "for_vol_sellers": vsell,
            "for_hedgers": hedger,
        },
        "series": {
            "dates": series_dates[-PCTL_WINDOW:],
            "vrp_30d": vrp_series[-PCTL_WINDOW:],
        },
        "parameters": {
            "trading_days": TRADING_DAYS,
            "percentile_window": PCTL_WINDOW,
            "expost_horizon_days": EXPOST_HORIZON,
            "rv_windows": [10, 21, 63],
        },
        "how_to_read": (
            "The volatility risk premium is implied vol minus realized "
            "vol -- what the options market charges for vol insurance "
            "over and above the vol that actually occurs. It is normally "
            "positive; RICH means underwriting vol is well paid (and a "
            "complacency tell), THIN means the premium is compressed, "
            "and INVERTED -- realized above implied -- is an early stress "
            "signal. The ex-post VRP is the honest scorecard: whether "
            "the premium actually paid over the past year, not just how "
            "it looks today."),
        "disclaimer": (
            "Realized vol from SPY as a proxy for the S&P 500; implied "
            "vol from the CBOE VIX complex. Stylised research read on a "
            "hypothetical book -- not investment advice."),
    }

    try:
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out, default=str).encode("utf-8"),
                      ContentType="application/json")
    except Exception as e:
        print("output write fail: %s" % e)

    try:
        hist = read_json(HIST_KEY)
        hsnaps = hist.get("snapshots") if isinstance(hist, dict) else []
        today = now.date().isoformat()
        hsnaps = [x for x in (hsnaps or []) if x.get("date") != today]
        hsnaps.append({
            "date": today, "generated_at": now.isoformat(),
            "regime": regime, "vrp_30d": vrp_30d,
            "vrp_30d_percentile_1y": pctl, "vix": vix, "rv_21d":
            round(rv_21, 2) if rv_21 is not None else None,
            "expost_vrp_mean": expost_mean,
        })
        hsnaps = hsnaps[-365:]
        s3.put_object(
            Bucket=BUCKET, Key=HIST_KEY,
            Body=json.dumps({"schema_version": SCHEMA, "engine":
                             "justhodl-vrp", "updated_at": now.isoformat(),
                             "snapshots": hsnaps},
                            default=str).encode("utf-8"),
            ContentType="application/json")
    except Exception as e:
        print("history write fail: %s" % e)

    return {"statusCode": 200, "body": json.dumps({
        "ok": feed_ok, "regime": regime, "vrp_30d": vrp_30d,
        "vrp_30d_percentile_1y": pctl, "vix": vix, "rv_21d":
        round(rv_21, 2) if rv_21 is not None else None,
        "expost_vrp_mean": expost_mean})}
