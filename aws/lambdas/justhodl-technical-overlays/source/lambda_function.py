"""
justhodl-technical-overlays  —  MA + Bollinger Band overlays and signals.

Produces:
  1. CHART SERIES for the Research Desk chart (MA20/50/200, BB upper/mid/lower).
  2. DETERMINISTIC SIGNALS (golden/death cross, BB %b, squeeze, MA stack) with an
     effective_trust flag set to PROBATIONARY on creation.

IMPORTANT — confluence wiring policy:
  This engine is registered in the scorecard and flows into best-setups, but it
  enters at effective_trust = 0 (PROBATIONARY) until it matures enough scored
  observations to earn weight through the SAME BH-FDR / deflated-Sharpe gate every
  other engine clears. Price-derived TA signals are highly correlated with each
  other and with existing engines; entering ungated would inflate effective_bets()
  and dilute the 3 proven engines. The gate decides if TA earns in. If it comes up
  alpha-negative like 28 other engines did, effective_trust keeps it out
  automatically. This is the Edge Accuracy Program working, not bypassing it.

Data: real Polygon aggregates (daily). Keys via SSM-first resolver.
Schedule suggestion: cron(10 21 * * ? *)  (after US close, before overnight rollup)
Payload: {"ticker":"AAPL"} or {"tickers":[...]}
"""
import os
import json
import math
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = os.environ.get("JH_BUCKET", "justhodl-dashboard-live")
S3_PREFIX = "data/technical-overlays"
POLY_BASE = "https://api.polygon.io"

_s3 = boto3.client("s3")


def _get_json(url, tries=3, backoff=0.6):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            last = e
            time.sleep(backoff * (2 ** i))
    raise RuntimeError(f"fetch failed: {url.split('?')[0]} :: {last}")


def fetch_daily_closes(ticker, days=420):
    """Real daily aggregates from Polygon. 420 calendar days -> ~290 trading days,
    enough for a 200-day MA with runway."""
    key = os.environ.get("POLYGON_KEY", "") or os.environ.get("POLYGON_API_KEY", "")
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    url = (f"{POLY_BASE}/v2/aggs/ticker/{ticker.upper()}/range/1/day/"
           f"{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={key}")
    j = _get_json(url)
    results = j.get("results", [])
    if not results:
        raise RuntimeError(f"no Polygon bars for {ticker}")
    bars = [{"t": datetime.fromtimestamp(r["t"] / 1000, timezone.utc).date().isoformat(),
             "c": r["c"], "h": r["h"], "l": r["l"], "v": r["v"]} for r in results]
    return bars


# ----------------------------------------------------------------------------
# indicators (pure functions, no lookahead)
# ----------------------------------------------------------------------------
def sma(vals, window):
    out = [None] * len(vals)
    if len(vals) < window:
        return out
    s = sum(vals[:window])
    out[window - 1] = s / window
    for i in range(window, len(vals)):
        s += vals[i] - vals[i - window]
        out[i] = s / window
    return out


def rolling_std(vals, window):
    out = [None] * len(vals)
    for i in range(window - 1, len(vals)):
        seg = vals[i - window + 1:i + 1]
        m = sum(seg) / window
        var = sum((x - m) ** 2 for x in seg) / window
        out[i] = math.sqrt(var)
    return out


def bollinger(closes, window=20, k=2.0):
    mid = sma(closes, window)
    sd = rolling_std(closes, window)
    upper = [(mid[i] + k * sd[i]) if mid[i] is not None and sd[i] is not None else None
             for i in range(len(closes))]
    lower = [(mid[i] - k * sd[i]) if mid[i] is not None and sd[i] is not None else None
             for i in range(len(closes))]
    return upper, mid, lower, sd


# ----------------------------------------------------------------------------
# signals — each returns direction + strength, all PROBATIONARY on emit
# ----------------------------------------------------------------------------
def compute_signals(bars):
    closes = [b["c"] for b in bars]
    if len(closes) < 205:
        return {"ok": False, "reason": f"need >=205 bars, have {len(closes)}"}

    ma20 = sma(closes, 20)
    ma50 = sma(closes, 50)
    ma200 = sma(closes, 200)
    bb_u, bb_m, bb_l, bb_sd = bollinger(closes, 20, 2.0)

    price = closes[-1]
    signals = []

    # 1) Golden / Death cross (50 vs 200)
    if ma50[-1] and ma200[-1] and ma50[-2] and ma200[-2]:
        prev = ma50[-2] - ma200[-2]
        now = ma50[-1] - ma200[-1]
        if prev <= 0 < now:
            signals.append({"name": "golden_cross", "dir": "bull", "strength": 1.0,
                            "detail": "50DMA crossed above 200DMA"})
        elif prev >= 0 > now:
            signals.append({"name": "death_cross", "dir": "bear", "strength": 1.0,
                            "detail": "50DMA crossed below 200DMA"})
        else:
            stack = "bull" if now > 0 else "bear"
            signals.append({"name": "ma_regime", "dir": stack,
                            "strength": min(abs(now) / price / 0.05, 1.0),
                            "detail": f"50DMA {'above' if now>0 else 'below'} 200DMA"})

    # 2) MA stack alignment (20>50>200 = strong uptrend)
    if ma20[-1] and ma50[-1] and ma200[-1]:
        if ma20[-1] > ma50[-1] > ma200[-1]:
            signals.append({"name": "ma_stack", "dir": "bull", "strength": 0.8,
                            "detail": "20>50>200 aligned bull stack"})
        elif ma20[-1] < ma50[-1] < ma200[-1]:
            signals.append({"name": "ma_stack", "dir": "bear", "strength": 0.8,
                            "detail": "20<50<200 aligned bear stack"})

    # 3) Bollinger %b (position within bands)
    pct_b = None
    if bb_u[-1] and bb_l[-1] and (bb_u[-1] - bb_l[-1]) != 0:
        pct_b = (price - bb_l[-1]) / (bb_u[-1] - bb_l[-1])
        if pct_b > 1.0:
            signals.append({"name": "bb_breakout_up", "dir": "bull",
                            "strength": min((pct_b - 1) * 5, 1.0),
                            "detail": f"close above upper band (%b={pct_b:.2f})"})
        elif pct_b < 0.0:
            signals.append({"name": "bb_breakout_down", "dir": "bear",
                            "strength": min(abs(pct_b) * 5, 1.0),
                            "detail": f"close below lower band (%b={pct_b:.2f})"})

    # 4) Bollinger squeeze (bandwidth at multi-month low = volatility compression)
    bandwidth = None
    if bb_u[-1] and bb_l[-1] and bb_m[-1]:
        bandwidth = (bb_u[-1] - bb_l[-1]) / bb_m[-1]
        bw_series = [((bb_u[i] - bb_l[i]) / bb_m[i])
                     for i in range(len(closes) - 120, len(closes))
                     if bb_u[i] and bb_l[i] and bb_m[i]]
        if bw_series and bandwidth <= min(bw_series) * 1.02:
            signals.append({"name": "bb_squeeze", "dir": "neutral", "strength": 0.7,
                            "detail": "bandwidth at ~120d low — vol compression, "
                                      "breakout pending (direction unknown)"})

    return {
        "ok": True,
        "price": round(price, 2),
        "ma20": round(ma20[-1], 2) if ma20[-1] else None,
        "ma50": round(ma50[-1], 2) if ma50[-1] else None,
        "ma200": round(ma200[-1], 2) if ma200[-1] else None,
        "bb_upper": round(bb_u[-1], 2) if bb_u[-1] else None,
        "bb_mid": round(bb_m[-1], 2) if bb_m[-1] else None,
        "bb_lower": round(bb_l[-1], 2) if bb_l[-1] else None,
        "pct_b": round(pct_b, 3) if pct_b is not None else None,
        "bandwidth": round(bandwidth, 4) if bandwidth is not None else None,
        "signals": signals,
    }


def build_chart_series(bars, tail=260):
    """Overlay series for the chart, trimmed to the last ~year of trading days."""
    closes = [b["c"] for b in bars]
    ma20 = sma(closes, 20)
    ma50 = sma(closes, 50)
    ma200 = sma(closes, 200)
    bb_u, bb_m, bb_l, _ = bollinger(closes, 20, 2.0)
    n = len(bars)
    s = max(0, n - tail)
    return [{
        "t": bars[i]["t"], "c": round(bars[i]["c"], 2),
        "ma20": round(ma20[i], 2) if ma20[i] else None,
        "ma50": round(ma50[i], 2) if ma50[i] else None,
        "ma200": round(ma200[i], 2) if ma200[i] else None,
        "bbU": round(bb_u[i], 2) if bb_u[i] else None,
        "bbM": round(bb_m[i], 2) if bb_m[i] else None,
        "bbL": round(bb_l[i], 2) if bb_l[i] else None,
    } for i in range(s, n)]


def analyze(ticker):
    bars = fetch_daily_closes(ticker)
    sig = compute_signals(bars)
    chart = build_chart_series(bars)

    # net directional lean from signals (for scorecard; strength-weighted)
    lean = 0.0
    if sig.get("ok"):
        for s in sig["signals"]:
            if s["dir"] == "bull":
                lean += s["strength"]
            elif s["dir"] == "bear":
                lean -= s["strength"]

    return {
        "ticker": ticker.upper(),
        "generated": datetime.now(timezone.utc).isoformat(),
        "indicators": sig,
        "chart_series": chart,
        "confluence": {
            "engine": "technical_overlays",
            "lean": round(lean, 3),
            "dir": "bull" if lean > 0.3 else ("bear" if lean < -0.3 else "neutral"),
            # THE GATE: probationary until scorecard matures it through BH-FDR
            "effective_trust": 0.0,
            "trust_status": "PROBATIONARY",
            "note": "Not weighted in confluence until matured via Edge Accuracy "
                    "Program (BH-FDR). Correlated price signal — gate decides "
                    "if it earns in.",
        },
        "_data_quality": "real:Polygon",
    }


def write_s3(result):
    key = f"{S3_PREFIX}/{result['ticker']}.json"
    _s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(result).encode("utf-8"),
                   ContentType="application/json", CacheControl="max-age=300")
    return key


def lambda_handler(event, context):
    tickers = event.get("tickers") or ([event["ticker"]] if event.get("ticker") else [])
    if not tickers:
        return {"statusCode": 400, "body": "provide ticker or tickers[]"}
    out = {}
    for t in tickers:
        try:
            res = analyze(t)
            key = write_s3(res)
            out[t.upper()] = {"ok": True, "s3": key,
                              "lean": res["confluence"]["lean"],
                              "trust": res["confluence"]["trust_status"]}
        except Exception as e:
            out[t.upper()] = {"ok": False, "error": str(e)}
    return {"statusCode": 200, "body": json.dumps(out)}


if __name__ == "__main__":
    import sys
    tk = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    print(json.dumps(analyze(tk), indent=2))
