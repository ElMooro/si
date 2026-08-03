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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/trend-reversal.json"
FMP_KEY = os.environ.get("FMP_KEY") or os.environ.get("FMP_API_KEY", "")
LADDER = ["SPY", "IWM", "EFA", "EEM", "HYG", "IEF", "GLD", "SLV",
          "DBC", "VNQ"]
SECTOR_ETFS = ["XLK", "XLE", "XLF", "XLV", "XLI", "XLU", "XLB",
               "XLY", "XLP", "XLRE", "XLC", "SMH", "XBI", "KRE",
               "ITB", "XHB", "IYT", "XME", "XOP", "OIH", "TAN",
               "JETS", "IGV", "SOXX", "KWEB", "EWJ", "EWZ", "FXI"]
CRYPTO = {"BTC": "BTCUSD", "ETH": "ETHUSD"}
MAX_NAMES = int(os.environ.get("MAX_NAMES", "20"))
BATCH = int(os.environ.get("BATCH", "110"))
PART_KEY = "data/_tmp/trend-reversal-partial.json"
HIST_KEY = "data/trend-reversal-history.json"
SELF_FN = os.environ.get("AWS_LAMBDA_FUNCTION_NAME",
                         "justhodl-trend-reversal")
lam = __import__("boto3").client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
VERSION = "2.2.1"


def closes(sym, days=280):
    url = ("https://financialmodelingprep.com/stable/"
           f"historical-price-eod/full?symbol={sym}&apikey={FMP_KEY}")
    d = json.loads(urllib.request.urlopen(url, timeout=25).read())
    rows = d.get("historical", []) if isinstance(d, dict) else d
    out = []
    for r in rows:
        try:
            out.append((r["date"],
                        float(r.get("close") or r.get("adjClose")),
                        float(r.get("volume") or 0)))
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


def weekly_close(series):
    acc = {}
    for d, v, *_ in series:
        y, w, _2 = datetime.strptime(d, "%Y-%m-%d").isocalendar()
        acc[(y, w)] = v
    return [acc[k] for k in sorted(acc)]


def analyze(sym, series, sector=None):
    if len(series) < 210:
        return {"ticker": sym, "status": "insufficient_history",
                "n": len(series)}
    dates = [x[0] for x in series]
    c = [x[1] for x in series]
    vol = [x[2] for x in series]
    s20, s50, s200 = sma(c, 20), sma(c, 50), sma(c, 200)
    s100 = sma(c, 100)
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

    # ── v2 families: volume, band-walk, exhaustion, gaps, RSI regime,
    #    stretch context ──
    if any(vol[-20:]):
        obv = [0.0]
        for j in range(1, len(c)):
            obv.append(obv[-1] + (vol[j] if c[j] > c[j - 1]
                                  else -vol[j] if c[j] < c[j - 1]
                                  else 0))
        if prevail == "UP" and c[i] >= max(c[i - 19:i + 1]) - 1e-9 \
                and obv[i] < max(obv[i - 19:i + 1]) * 0.999:
            fire("obv_diverge", 14, "price 20d high, OBV below its "
                                    "own 20d peak")
        if prevail == "DOWN" and c[i] <= min(c[i - 19:i + 1]) + 1e-9 \
                and obv[i] > min(obv[i - 19:i + 1]) * 1.001:
            fire("obv_diverge", 14, "price 20d low, OBV above its "
                                    "own 20d trough")
    m20 = s20[i]
    if m20:
        sdv = (sum((x - m20) ** 2 for x in c[i - 19:i + 1])
               / 20) ** .5
        up_b, lo_b = m20 + 2 * sdv, m20 - 2 * sdv
        walked_up = sum(1 for k in range(i - 6, i)
                        if s20[k] and c[k] > s20[k] + 2 * sdv * .9)
        if prevail == "UP" and walked_up >= 3 and c[i] < up_b:
            fire("bb_walk_fail", 10,
                 f"upper-band walk broke, close {c[i]:.2f} < "
                 f"{up_b:.2f}")
        walked_dn = sum(1 for k in range(i - 6, i)
                        if s20[k] and c[k] < s20[k] - 2 * sdv * .9)
        if prevail == "DOWN" and walked_dn >= 3 and c[i] > lo_b:
            fire("bb_walk_fail", 10,
                 f"lower-band walk broke, close {c[i]:.2f} > "
                 f"{lo_b:.2f}")
    b1, b2, b3 = abs(c[i] - c[i - 1]), abs(c[i - 1] - c[i - 2]), \
        abs(c[i - 2] - c[i - 3])
    if prevail == "UP" and c[i] > c[i - 3] and b1 < b2 < b3:
        fire("momo_exhaust", 8, "three shrinking up-thrusts")
    if prevail == "DOWN" and c[i] < c[i - 3] and b1 < b2 < b3:
        fire("momo_exhaust", 8, "three shrinking down-thrusts")
    gap = (c[i - 1] - c[i - 2]) / c[i - 2] if c[i - 2] else 0
    if prevail == "UP" and gap > 0.02 and c[i] < c[i - 2]:
        fire("gap_fail", 9, f"+{gap*100:.1f}% thrust given back "
                            "next bar")
    if prevail == "DOWN" and gap < -0.02 and c[i] > c[i - 2]:
        fire("gap_fail", 9, f"{gap*100:.1f}% flush reclaimed "
                            "next bar")
    if r14[i] is not None and r14[i - 15] is not None:
        held_hi = all((x or 50) > 55 for x in r14[i - 40:i - 10]
                      if x is not None)
        held_lo = all((x or 50) < 45 for x in r14[i - 40:i - 10]
                      if x is not None)
        if prevail == "UP" and held_hi and r14[i] < 42:
            fire("rsi_regime", 9,
                 f"RSI regime shift: held >55, now {r14[i]:.0f}")
        if prevail == "DOWN" and held_lo and r14[i] > 58:
            fire("rsi_regime", 9,
                 f"RSI regime shift: held <45, now {r14[i]:.0f}")
    if s200[i]:
        dist = [cc / ss - 1 for cc, ss in zip(c[-160:], s200[-160:])
                if ss]
        mu_d = sum(dist) / len(dist)
        sd_d = (sum((x - mu_d) ** 2 for x in dist)
                / len(dist)) ** .5 or 1e-9
        zst = (dist[-1] - mu_d) / sd_d
        if prevail == "UP" and zst > 2 and fired:
            fire("stretch", 6, f"px vs 200dma z {zst:+.1f} "
                               "(exhaustion context)")
        if prevail == "DOWN" and zst < -2 and fired:
            fire("stretch", 6, f"px vs 200dma z {zst:+.1f} "
                               "(capitulation context)")
    score = 0.0
    for f in fired:
        score += f["w"] * (0.85 ** f.get("bars_ago", 0))
    # weekly higher-timeframe confirmation multiplier
    wk = weekly_close(series)
    weekly_conf = False
    if len(wk) > 30:
        wm = [a - b for a, b in zip(ema(wk, 6), ema(wk, 13))]
        wh = [m_ - s_ for m_, s_ in zip(wm, ema(wm, 5))]
        w_lohi = (max(wk[-4:]) < max(wk[-12:-4])
                  if prevail == "UP"
                  else min(wk[-4:]) > min(wk[-12:-4]))
        w_macd = (wh[-1] < 0 if prevail == "UP" else wh[-1] > 0)
        if fired and prevail != "FLAT" and (w_lohi or w_macd):
            weekly_conf = True
            score *= 1.25
    score = round(min(100, score), 1)
    direction = (None if not fired or prevail == "FLAT" else
                 "TOP_FORMING" if prevail == "UP" else
                 "BOTTOM_FORMING")
    FAM = {"ma_break": "trend", "slope_flip": "trend",
           "golden_death": "trend", "donchian_break": "structure",
           "structure": "structure", "rsi_diverge": "momentum",
           "macd_flip": "momentum", "rsi_regime": "momentum",
           "atr_release": "volatility", "bb_walk_fail": "volatility",
           "obv_diverge": "volume", "momo_exhaust": "momentum",
           "gap_fail": "structure", "stretch": "context"}
    fams = sorted({FAM.get(f["signal"], "other") for f in fired}
                  - {"context"})
    stage = (None if not direction else
             "CONFIRMED" if {"trend", "structure"} <= set(fams)
             else "DEVELOPING" if len(fams) >= 2 else "EARLY")
    def _ds(xs, k=24):
        xs = [x for x in xs if x is not None][-46:]
        if len(xs) < 4:
            return None
        step = max(1, len(xs) // k)
        pts = xs[::step][-k:]
        return [float(f"{x:.5g}") for x in pts]
    return {"ticker": sym, "as_of": dates[-1],
            "spk": _ds(c), "spk50": _ds(s50),
            "ma": {"d20": round(100*(c[i]/s20[i]-1),1) if s20[i] else None,
                   "d50": round(100*(c[i]/s50[i]-1),1) if s50[i] else None,
                   "d100": round(100*(c[i]/s100[i]-1),1) if s100[i] else None,
                   "d200": round(100*(c[i]/s200[i]-1),1) if s200[i] else None},
            "close": round(c[i], 2), "sector": sector,
            "prevailing_trend": prevail,
            "reversal_score": score, "direction": direction,
            "stage": stage, "families": fams,
            "weekly_confirm": weekly_conf,
            "n_signals": len(fired), "signals": fired,
            "closes30": [round(x, 2) for x in c[-30:]]
            if score >= 25 else None,
            "px_vs_200dma_pct": round(100 * (c[i] / s200[i] - 1), 1)
            if s200[i] else None}


ETF_GRID = ["QQQ", "DIA", "RSP", "MDY", "IVV", "VTI", "VT", "ACWI",
            "VEA", "VWO", "SMH", "SOXX", "XBI", "ITB", "XHB", "KRE",
            "XME", "GDX", "GDXJ", "SLX", "OIH", "IWD", "IWF", "MTUM",
            "QUAL", "USMV", "SPLV", "TLT", "IEI", "SHY", "LQD",
            "EMB", "TIP", "BND", "AGG", "USO", "UNG", "DBA", "DBB",
            "CPER", "URA", "WEAT", "CORN", "PALL", "UVXY", "EWJ",
            "FXI", "KWEB", "EWZ", "INDA", "EWG", "EWU", "EWA", "EWC",
            "EWY", "EWT", "EWH", "EWQ", "EWI", "EWP", "EWL", "EWW",
            "ARKK", "BITO", "IBIT"]
FX = {"EURUSD": "EURUSD", "USDJPY": "USDJPY", "GBPUSD": "GBPUSD",
      "AUDUSD": "AUDUSD", "USDCAD": "USDCAD", "USDCHF": "USDCHF",
      "USDMXN": "USDMXN", "NZDUSD": "NZDUSD", "EURJPY": "EURJPY",
      "EURGBP": "EURGBP", "USDBRL": "USDBRL", "USDINR": "USDINR"}
FUT = {"GOLD_FUT": "GCUSD", "SILVER_FUT": "SIUSD",
       "COPPER_FUT": "HGUSD", "WTI_FUT": "CLUSD",
       "NATGAS_FUT": "NGUSD"}
CRYPTO.update({"SOL": "SOLUSD", "XRP": "XRPUSD", "BNB": "BNBUSD",
               "ADA": "ADAUSD", "DOGE": "DOGEUSD", "AVAX": "AVAXUSD",
               "LINK": "LINKUSD", "DOT": "DOTUSD"})
FETCH_MAP = {**FX, **FUT, **CRYPTO}


def build_universe():
    uni, sectors, cls = [], {}, {}

    def tag(t, c):
        if t not in cls:
            cls[t] = c
    for t in LADDER + SECTOR_ETFS + ETF_GRID:
        uni.append(t)
        tag(t, "ETF")
    for t in list(FX) + list(FUT) + list(CRYPTO):
        uni.append(t)
        tag(t, "FX" if t in FX else "FUTURES" if t in FUT
            else "CRYPTO")
    try:
        bs = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/best-setups.json")["Body"].read())
        for s0 in (bs.get("top_setups") or [])[:MAX_NAMES]:
            t = str(s0.get("ticker") or "").upper()
            if t and t not in uni:
                uni.append(t)
                if s0.get("sector"):
                    sectors[t] = s0["sector"]
    except Exception as e:
        print(f"[reversal] setups skip: {str(e)[:60]}")
    try:
        cm = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/fundamental-census-matrix.json")["Body"].read())
        tick_col = cm.get("tickers")
        if isinstance(tick_col, list) and tick_col and \
                isinstance(tick_col[0], str):
            # 4311 truth: the matrix is COLUMNAR -- parallel lists
            sec_col = cm.get("sectors") or []
            n0 = len(uni)
            for i2, t in enumerate(tick_col):
                t = str(t).upper()
                if t and t not in uni:
                    uni.append(t)
                    tag(t, "SP500")
                if t and i2 < len(sec_col) and sec_col[i2]:
                    sectors[t] = sec_col[i2]
            print(f"[universe] census(columnar) +{len(uni)-n0}")
            rows = []
        else:
            rows = (cm.get("rows") or cm.get("companies")
                    or cm.get("matrix") or cm.get("data") or [])
        if isinstance(rows, dict):  # {ticker: {cols...}} shape
            rows = [dict(v, ticker=k) if isinstance(v, dict)
                    else {"ticker": k} for k, v in rows.items()]
        n0 = len(uni)
        for r0 in rows:
            t = str(r0.get("ticker") or r0.get("symbol")
                    or "").upper()
            if t and t not in uni:
                uni.append(t)
                tag(t, "SP500")
            sec = r0.get("sector") or r0.get("gics_sector") \
                or r0.get("Sector")
            if t and sec:
                sectors[t] = sec
        print(f"[universe] census +{len(uni)-n0} "
              f"(keys tried: rows/companies/matrix/data; "
              f"top={list(cm)[:6]})")
    except Exception as e:
        print(f"[reversal] census skip: {str(e)[:60]}")
    try:  # Nasdaq-100 constituents
        d = json.loads(urllib.request.urlopen(
            "https://financialmodelingprep.com/stable/"
            f"nasdaq-constituent?apikey={FMP_KEY}",
            timeout=25).read())
        n0 = len(uni)
        for r0 in d if isinstance(d, list) else []:
            t = str(r0.get("symbol") or "").upper()
            if t and t not in uni:
                uni.append(t)
                tag(t, "NDX")
            if t and r0.get("sector") and t not in sectors:
                sectors[t] = r0["sector"]
        print(f"[universe] ndx +{len(uni)-n0}")
    except Exception as e:
        print(f"[reversal] ndx skip: {str(e)[:60]}")
    return uni[:820], sectors, cls


def lambda_handler(event=None, context=None):
    t0 = time.time()
    event = event or {}
    cursor = int(event.get("cursor", 0))
    universe = event.get("universe")
    sectors = event.get("sectors") or {}
    cls_map = event.get("cls") or {}
    if universe is None:
        universe, sectors, cls_map = build_universe()
        try:  # fresh chain: clear partial
            s3.delete_object(Bucket=BUCKET, Key=PART_KEY)
        except Exception:
            pass
    batch = universe[cursor:cursor + BATCH]
    rows, errs = [], []
    for sym in batch:
        fs = FETCH_MAP.get(sym, sym)
        try:
            r_ = analyze(sym, closes(fs), sectors.get(sym))
            r_["asset_class"] = cls_map.get(sym, "SP500")
            rows.append(r_)
        except Exception as e:
            errs.append(f"{sym}:{str(e)[:46]}")
    try:
        part = json.loads(s3.get_object(
            Bucket=BUCKET, Key=PART_KEY)["Body"].read())
    except Exception:
        part = {"rows": [], "errors": []}
    part["rows"] += rows
    part["errors"] += errs
    s3.put_object(Bucket=BUCKET, Key=PART_KEY,
                  Body=json.dumps(part).encode(),
                  ContentType="application/json")
    nxt = cursor + BATCH
    if nxt < len(universe):
        lam.invoke(FunctionName=SELF_FN, InvocationType="Event",
                   Payload=json.dumps({
                       "cursor": nxt, "universe": universe,
                       "sectors": sectors,
                       "cls": cls_map}).encode())
        print(f"[reversal] chain {cursor}->{nxt}/{len(universe)} "
              f"(+{len(rows)} rows, {len(errs)} errs) "
              f"{time.time()-t0:.0f}s")
        return {"ok": True, "chained": nxt, "of": len(universe)}
    # ── finalize ──
    rows = [r for r in part["rows"]
            if r.get("status") != "insufficient_history"
            or True]
    best = {}
    for r in rows:  # chain-overlap dedupe: keep max score per ticker
        t = r.get("ticker")
        if t not in best or (r.get("reversal_score") or 0) > \
                (best[t].get("reversal_score") or 0):
            best[t] = r
    rows = list(best.values())
    good = [r for r in rows if "reversal_score" in r]
    good.sort(key=lambda r: -(r.get("reversal_score") or 0))
    hot = [r for r in good if (r.get("reversal_score") or 0) >= 30]
    n_top = sum(1 for r in good
                if r.get("direction") == "TOP_FORMING"
                and (r.get("reversal_score") or 0) >= 20)
    n_bot = sum(1 for r in good
                if r.get("direction") == "BOTTOM_FORMING"
                and (r.get("reversal_score") or 0) >= 20)
    breadth = {"n": len(good),
               "top_pct": round(100 * n_top / len(good), 1)
               if good else None,
               "bottom_pct": round(100 * n_bot / len(good), 1)
               if good else None,
               "note": "share of universe with score>=20 by "
                       "direction -- the market-turn gauge"}
    for r in good:  # cross-asset rows tile under their class
        if not r.get("sector"):
            r["sector"] = r.get("asset_class")
    sec_map = {}
    for r in good:
        se = r.get("sector") or "—"
        d0 = sec_map.setdefault(se, {"sector": se, "n": 0,
                                     "top": 0, "bot": 0,
                                     "sum": 0.0, "leaders": []})
        d0["n"] += 1
        d0["sum"] += r.get("reversal_score") or 0
        if (r.get("reversal_score") or 0) >= 20:
            if r.get("direction") == "TOP_FORMING":
                d0["top"] += 1
            elif r.get("direction") == "BOTTOM_FORMING":
                d0["bot"] += 1
        if len(d0["leaders"]) < 3 and \
                (r.get("reversal_score") or 0) >= 20:
            d0["leaders"].append(
                {"t": r["ticker"],
                 "s": r["reversal_score"],
                 "d": r["direction"]})
    sectors_out = sorted(
        ({"sector": v["sector"], "n": v["n"],
          "top_pct": round(100 * v["top"] / v["n"], 1),
          "bottom_pct": round(100 * v["bot"] / v["n"], 1),
          "avg_score": round(v["sum"] / v["n"], 1),
          "leaders": v["leaders"]}
         for v in sec_map.values() if v["n"] >= 3),
        key=lambda x: -(x["top_pct"] + x["bottom_pct"]))
    try:
        hist = json.loads(s3.get_object(
            Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
    except Exception:
        hist = {"days": [], "last_scores": {}}
    prev = hist.get("last_scores") or {}
    for r in good:
        pv = prev.get(r["ticker"])
        if pv is not None:
            r["score_delta"] = round(
                (r.get("reversal_score") or 0) - pv, 1)
    movers = sorted((r for r in good if r.get("score_delta")),
                    key=lambda r: -abs(r["score_delta"]))[:10]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hist["days"] = ([d for d in (hist.get("days") or [])
                     if d.get("d") != today][-179:]
                    + [{"d": today, "breadth": breadth,
                        "hot": [{"t": r["ticker"],
                                 "s": r["reversal_score"],
                                 "dir": r["direction"]}
                                for r in hot[:40]]}])
    hist["last_scores"] = {r["ticker"]:
                           r.get("reversal_score") or 0
                           for r in good}
    s3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                  Body=json.dumps(hist).encode(),
                  ContentType="application/json")
    built_bc = {}
    for t_ in (universe or []):
        c_ = cls_map.get(t_, "SP500")
        built_bc[c_] = built_bc.get(c_, 0) + 1
    dropped = ([{"t": x.get("ticker"),
                 "cls": x.get("asset_class"),
                 "why": "insufficient_history(%s)" % x.get("n")}
                for x in part["rows"]
                if x.get("status") == "insufficient_history"]
               + [{"t": e.split(":", 1)[0],
                   "cls": cls_map.get(e.split(":", 1)[0], "?"),
                   "why": e.split(":", 1)[-1][:60]}
                  for e in part.get("errors") or []])
    out = {"engine": "justhodl-trend-reversal", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(
               timespec="seconds"),
           "universe_n": len(good), "hot_n": len(hot),
           "built": {"n": len(universe or []),
                     "by_class": built_bc},
           "dropped": dropped[:60] or None,
           "n_dropped": len(dropped),
           "breadth": breadth, "sectors": sectors_out,
           "movers": [{"t": r["ticker"],
                       "delta": r["score_delta"],
                       "score": r["reversal_score"],
                       "dir": r.get("direction")}
                      for r in movers],
           "stages": {st: sum(1 for r in good
                              if r.get("stage") == st)
                      for st in ("EARLY", "DEVELOPING",
                                 "CONFIRMED")},
           "rows": good[:400],
           "errors": part["errors"][:25] or None,
           "methodology": ("v2: 14-signal ensemble across "
                           "trend/structure/momentum/volatility/"
                           "volume families on real closes+volume; "
                           "weekly higher-timeframe confirmation "
                           "x1.25; stage = family confluence; "
                           "sector breadth and market-turn gauge "
                           "from the full census universe; "
                           "self-chained batches."),
           "elapsed_s": round(time.time() - t0, 1)}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    try:
        s3.delete_object(Bucket=BUCKET, Key=PART_KEY)
    except Exception:
        pass
    print(f"[reversal] FINAL {len(good)} names hot={len(hot)} "
          f"breadth top {breadth['top_pct']}% / bot "
          f"{breadth['bottom_pct']}% "
          f"top={[(r['ticker'], r['reversal_score']) for r in good[:3]]}")
    return {"ok": True, "final": True, "n": len(good),
            "hot": len(hot), "breadth": breadth}
