"""justhodl-trend-reversal v1.0 — early trend-reversal detection. ops 4302.

Pure price-structure math on real daily closes (FMP historical, the
house rail). Universe = the desk's 13 ladder ETFs + best-setups top
names. Per asset, an ensemble of classical early-reversal tells, each
recorded with its value and trigger recency — no single indicator is
trusted alone, and no signal is invented when history is short:

  ma_break        close crosses the 50dma against the prevailing trend
  slope_flip      50dma slope sign change (10d)
  golden_death    50/200 cross within last 15 bars
  rsi_diverge     price extreme (20d) NOT confirmed by RSI14 extreme
  macd_flip       MACD histogram sign flip within 5 bars
  donchian_break  20d channel break against trend
  structure       HH/HL chain broken (lower-high after uptrend, or
                  higher-low after downtrend)
  atr_release     ATR14 compression (<70% of 60d median) releasing

reversal_score 0-100 = weighted fired-signals with recency decay;
direction TOP_FORMING (bearish tells inside an uptrend) or
BOTTOM_FORMING. Artifact: data/trend-reversal.json.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/trend-reversal.json"
FMP_KEY = os.environ.get("FMP_KEY") or os.environ.get("FMP_API_KEY", "")
LADDER = ["SPY", "IWM", "EFA", "EEM", "HYG", "IEF", "GLD", "SLV",
          "DBC", "VNQ"]
CRYPTO = {"BTC": "BTCUSD", "ETH": "ETHUSD"}
MAX_NAMES = int(os.environ.get("MAX_NAMES", "14"))
s3 = boto3.client("s3", region_name="us-east-1")
VERSION = "1.0"


def closes(sym, days=280):
    url = ("https://financialmodelingprep.com/stable/"
           f"historical-price-eod/full?symbol={sym}&apikey={FMP_KEY}")
    d = json.loads(urllib.request.urlopen(url, timeout=25).read())
    rows = d.get("historical", []) if isinstance(d, dict) else d
    out = []
    for r in rows:
        try:
            out.append((r["date"], float(r.get("close")
                                         or r.get("adjClose"))))
        except Exception:
            continue
    out.sort()
    return out[-days:]


def sma(xs, n):
    return [sum(xs[i - n + 1:i + 1]) / n if i >= n - 1 else None
            for i in range(len(xs))]


def rsi(xs, n=14):
    out = [None] * len(xs)
    g = l = 0.0
    for i in range(1, len(xs)):
        ch = xs[i] - xs[i - 1]
        up, dn = max(ch, 0), max(-ch, 0)
        if i <= n:
            g += up
            l += dn
            if i == n:
                ag, al = g / n, l / n
                out[i] = 100 - 100 / (1 + (ag / al if al else 99))
        else:
            ag = (ag * (n - 1) + up) / n
            al = (al * (n - 1) + dn) / n
            out[i] = 100 - 100 / (1 + (ag / al if al else 99))
    return out


def ema(xs, n):
    k = 2 / (n + 1)
    out, e = [], None
    for x in xs:
        e = x if e is None else x * k + e * (1 - k)
        out.append(e)
    return out


def atr_proxy(cl, n=14):  # close-to-close TR proxy (no OHLC needed)
    tr = [abs(cl[i] - cl[i - 1]) for i in range(1, len(cl))]
    return sma([0] + tr, n)


def analyze(sym, series):
    if len(series) < 210:
        return {"ticker": sym, "status": "insufficient_history",
                "n": len(series)}
    dates = [d for d, _ in series]
    c = [v for _, v in series]
    s20, s50, s200 = sma(c, 20), sma(c, 50), sma(c, 200)
    r14 = rsi(c)
    macd = [a - b for a, b in zip(ema(c, 12), ema(c, 26))]
    hist = [m - s for m, s in zip(macd, ema(macd, 9))]
    at = atr_proxy(c)
    i = len(c) - 1
    trend_up = s50[i] is not None and s200[i] is not None and \
        s50[i] > s200[i] and c[i] > s200[i]
    trend_dn = s50[i] is not None and s200[i] is not None and \
        s50[i] < s200[i] and c[i] < s200[i]
    prevail = "UP" if trend_up else "DOWN" if trend_dn else "FLAT"

    fired = []

    def fire(name, w, detail, ago=0):
        fired.append({"signal": name, "w": w, "detail": detail,
                      "bars_ago": ago})

    # ma_break against trend
    if prevail == "UP" and c[i] < s50[i] and c[i - 1] >= s50[i - 1]:
        fire("ma_break", 15, f"close {c[i]:.2f} < 50dma "
                             f"{s50[i]:.2f}")
    if prevail == "DOWN" and c[i] > s50[i] and c[i - 1] <= s50[i - 1]:
        fire("ma_break", 15, f"close {c[i]:.2f} > 50dma "
                             f"{s50[i]:.2f}")
    # slope flip
    if s50[i] and s50[i - 10]:
        sl_now = s50[i] - s50[i - 10]
        sl_prev = s50[i - 10] - s50[i - 20] if s50[i - 20] else None
        if sl_prev is not None and sl_now * sl_prev < 0:
            fire("slope_flip", 12,
                 f"50dma slope {sl_prev:+.2f}->{sl_now:+.2f}")
    # golden/death within 15 bars
    for k in range(max(1, i - 15), i + 1):
        if s50[k] and s200[k] and s50[k - 1] and s200[k - 1]:
            if (s50[k] - s200[k]) * (s50[k - 1] - s200[k - 1]) < 0:
                fire("golden_death", 18,
                     "50/200 cross", ago=i - k)
                break
    # rsi divergence at 20d price extreme
    win = c[i - 19:i + 1]
    if prevail == "UP" and c[i] >= max(win) - 1e-9:
        rwin = [x for x in r14[i - 19:i + 1] if x is not None]
        if rwin and r14[i] is not None and r14[i] < max(rwin) - 3:
            fire("rsi_diverge", 16,
                 f"new 20d high, RSI {r14[i]:.0f} < peak "
                 f"{max(rwin):.0f}")
    if prevail == "DOWN" and c[i] <= min(win) + 1e-9:
        rwin = [x for x in r14[i - 19:i + 1] if x is not None]
        if rwin and r14[i] is not None and r14[i] > min(rwin) + 3:
            fire("rsi_diverge", 16,
                 f"new 20d low, RSI {r14[i]:.0f} > trough "
                 f"{min(rwin):.0f}")
    # macd hist flip vs trend, within 5 bars
    for k in range(max(1, i - 5), i + 1):
        if hist[k] * hist[k - 1] < 0:
            against = (prevail == "UP" and hist[k] < 0) or \
                      (prevail == "DOWN" and hist[k] > 0)
            if against:
                fire("macd_flip", 12,
                     f"hist {hist[k-1]:+.3f}->{hist[k]:+.3f}",
                     ago=i - k)
            break
    # donchian 20 break against trend
    hi20, lo20 = max(c[i - 20:i]), min(c[i - 20:i])
    if prevail == "UP" and c[i] < lo20:
        fire("donchian_break", 14, f"below 20d low {lo20:.2f}")
    if prevail == "DOWN" and c[i] > hi20:
        fire("donchian_break", 14, f"above 20d high {hi20:.2f}")
    # structure break: lower high after uptrend / higher low after dn
    piv_hi = max(c[i - 40:i - 20]) if i >= 40 else None
    piv_lo = min(c[i - 40:i - 20]) if i >= 40 else None
    if prevail == "UP" and piv_hi and max(c[i - 19:i + 1]) < piv_hi:
        fire("structure", 13,
             f"lower high {max(c[i-19:i+1]):.2f} < {piv_hi:.2f}")
    if prevail == "DOWN" and piv_lo and min(c[i - 19:i + 1]) > piv_lo:
        fire("structure", 13,
             f"higher low {min(c[i-19:i+1]):.2f} > {piv_lo:.2f}")
    # atr compression release
    a_now = at[i]
    a_med = sorted(x for x in at[i - 60:i] if x)[
        len([x for x in at[i - 60:i] if x]) // 2] if i >= 60 else None
    if a_now and a_med and any(
            x and x < 0.7 * a_med for x in at[i - 10:i]) \
            and a_now > a_med:
        fire("atr_release", 10,
             f"ATRp {a_now:.2f} releasing from compression "
             f"(med {a_med:.2f})")

    score = 0.0
    for f in fired:
        score += f["w"] * (0.85 ** f.get("bars_ago", 0))
    score = round(min(100, score), 1)
    direction = (None if not fired or prevail == "FLAT" else
                 "TOP_FORMING" if prevail == "UP" else
                 "BOTTOM_FORMING")
    return {"ticker": sym, "as_of": dates[-1], "close": round(c[i], 2),
            "prevailing_trend": prevail,
            "reversal_score": score, "direction": direction,
            "n_signals": len(fired), "signals": fired,
            "px_vs_200dma_pct": round(100 * (c[i] / s200[i] - 1), 1)
            if s200[i] else None}


def lambda_handler(event=None, context=None):
    t0 = time.time()
    universe = list(LADDER)
    try:
        bs = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/best-setups.json")["Body"].read())
        for s0 in (bs.get("top_setups") or [])[:MAX_NAMES]:
            t = str(s0.get("ticker") or "").upper()
            if t and t not in universe:
                universe.append(t)
    except Exception as e:
        print(f"[reversal] best-setups skip: {str(e)[:70]}")
    rows, errs = [], []
    for sym in universe[:26]:
        fs = CRYPTO.get(sym, sym)
        try:
            rows.append(analyze(sym, closes(fs)))
        except Exception as e:
            errs.append(f"{sym}:{str(e)[:60]}")
    rows.sort(key=lambda r: -(r.get("reversal_score") or 0))
    hot = [r for r in rows if (r.get("reversal_score") or 0) >= 30]
    out = {"engine": "justhodl-trend-reversal", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(
               timespec="seconds"),
           "universe_n": len(rows), "hot_n": len(hot),
           "rows": rows, "errors": errs or None,
           "methodology": ("Ensemble of classical early-reversal "
                           "tells on real closes; score = recency-"
                           "decayed weighted signals; direction only "
                           "against a defined prevailing trend."),
           "elapsed_s": round(time.time() - t0, 1)}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    print(f"[reversal] {len(rows)} names, hot={len(hot)} "
          f"top={[(r['ticker'], r['reversal_score']) for r in rows[:3]]}"
          f" errs={len(errs)} {out['elapsed_s']}s")
    return {"ok": True, "n": len(rows), "hot": len(hot),
            "top": [(r["ticker"], r["reversal_score"])
                    for r in rows[:3]]}
