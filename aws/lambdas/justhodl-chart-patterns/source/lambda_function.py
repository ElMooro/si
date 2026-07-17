"""
justhodl-chart-patterns -- the Chart Pattern Scanner.

A companion to the protected Stock Screener. It does not touch the
screener; it runs its own daily scan of the S&P 500 and surfaces four
classic technical setups, each with the price series needed to draw a
chart:

  200-DMA CROSS UP    -- price has just crossed above its 200-day
                         moving average (a long-term trend flip up).
  200-DMA CROSS DOWN  -- price has just crossed below its 200-day MA.
  DOUBLE TOP          -- two peaks at a similar level with a real
                         valley between them; a bearish reversal that
                         is CONFIRMED once price breaks the neckline.
  DOUBLE BOTTOM       -- the mirror image; a bullish reversal.

Honest framing: chart patterns are probabilistic, not deterministic.
The detector is deliberately strict -- swing-point prominence, a price
tolerance band, a minimum valley depth, a sensible time separation and
a recency window -- so it surfaces genuine setups rather than noise.
Each pattern is tagged CONFIRMED (neckline broken) or FORMING.

Data: S&P 500 from FMP /stable/sp500-constituent; daily closes from
FMP /stable/historical-price-eod/light. Output: data/chart-patterns.json.
"""
import concurrent.futures as cf
import json
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
import os

import boto3

SCHEMA = "1.0"
BASE = "https://financialmodelingprep.com/stable"
FMP = os.environ.get("FMP_KEY", "")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/chart-patterns.json"

# ---- scan parameters -------------------------------------------------------
HIST_BARS = 320          # daily closes pulled per symbol
SERIES_BARS = 200        # bars carried in the output for charting --
#                          must exceed the max pattern span so every
#                          peak/trough lands inside the chart window:
#                          PATTERN_RECENT (60) + SEP_MAX (120) = 180.
MIN_BARS_CROSS = 210     # need 200-DMA + an 8-day cross window
MIN_BARS_PATTERN = 90    # enough tape to host a double top/bottom
CROSS_RECENT = 5         # "just crossed" = within this many sessions
CROSS_CLEAN_PCT = 0.3    # today must sit at least this far off the MA

SWING_W = 5              # a swing high/low is the extreme of +/- this
PEAK_TOL = 0.04          # two peaks/troughs within 4% = "similar level"
VALLEY_MIN = 0.06        # >= 6% retrace between the two peaks/troughs
SEP_MIN = 10             # peaks at least this many sessions apart
SEP_MAX = 120            # ... and at most this many
PATTERN_RECENT = 60      # the 2nd peak/trough within this many sessions
LIST_CAP = 30            # max names surfaced per category

s3 = boto3.client("s3")


# ---- data ------------------------------------------------------------------
def fmp(path, params="", max_retries=3):
    url = "%s/%s?apikey=%s%s" % (BASE, path, FMP, params)
    last = None
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "JustHodl-ChartPatterns/1.0"})
            r = urllib.request.urlopen(req, timeout=25)
            return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429:
                time.sleep(1 + attempt * 2 + attempt ** 2)
                continue
            return None
        except Exception as e:
            last = e
            time.sleep(0.5 + attempt)
    return None


def get_sp500():
    data = fmp("sp500-constituent")
    out = []
    if isinstance(data, list):
        for row in data:
            sym = (row or {}).get("symbol")
            if sym:
                out.append(str(sym).strip())
    return sorted(set(out))


def get_history(symbol):
    """Returns (dates, closes, volumes) oldest-first, or None."""
    data = fmp("historical-price-eod/light", "&symbol=%s" % symbol)
    if not isinstance(data, list) or len(data) < MIN_BARS_PATTERN:
        return None
    rows = []
    for r in data:
        d = (r or {}).get("date")
        p = (r or {}).get("price")
        v = (r or {}).get("volume")
        if d is None or p is None:
            continue
        try:
            rows.append((str(d)[:10], float(p), float(v or 0)))
        except (TypeError, ValueError):
            continue
    rows.sort(key=lambda x: x[0])          # oldest first
    rows = rows[-HIST_BARS:]
    if len(rows) < MIN_BARS_PATTERN:
        return None
    return ([r[0] for r in rows], [r[1] for r in rows],
            [r[2] for r in rows])


# ---- maths -----------------------------------------------------------------
def sma_at(closes, t, n):
    """Simple MA of n closes ending at index t (inclusive)."""
    if t - n + 1 < 0:
        return None
    return sum(closes[t - n + 1:t + 1]) / float(n)


def sign(x):
    return 1 if x > 0 else (-1 if x < 0 else 0)


def detect_cross_200dma(closes):
    """Fresh cross of the 200-day MA within the last CROSS_RECENT days."""
    n = len(closes)
    if n < MIN_BARS_CROSS:
        return None
    spreads = []
    for t in range(n - 8, n):
        ma = sma_at(closes, t, 200)
        if ma is None:
            return None
        spreads.append(closes[t] - ma)
    today = spreads[-1]
    if today == 0:
        return None
    today_sign = sign(today)
    run = 0
    for sp in reversed(spreads):
        if sign(sp) == today_sign:
            run += 1
        else:
            break
    if run >= len(spreads):          # no opposite sign in the window
        return None
    days_since = run - 1             # run==1 -> crossed today
    if days_since > CROSS_RECENT:
        return None
    ma_today = sma_at(closes, n - 1, 200)
    pct_from_ma = (closes[-1] - ma_today) / ma_today * 100.0
    if abs(pct_from_ma) < CROSS_CLEAN_PCT:
        return None                  # still hugging the line -- not clean
    ma_20ago = sma_at(closes, n - 21, 200)
    ma_slope = None
    if ma_20ago:
        ma_slope = "rising" if ma_today > ma_20ago else "falling"
    return {
        "direction": "up" if today_sign > 0 else "down",
        "days_since_cross": days_since,
        "close": round(closes[-1], 2),
        "sma200": round(ma_today, 2),
        "pct_from_ma": round(pct_from_ma, 2),
        "ma_slope_20d": ma_slope,
    }


def swing_points(closes):
    """Swing highs and lows: an extreme over +/- SWING_W bars. On a
    tied extreme (a flat top/bottom) the first bar of the tie is taken,
    so each swing resolves to exactly one index."""
    highs, lows = [], []
    n = len(closes)
    w = SWING_W
    for i in range(w, n - w):
        window = closes[i - w:i + w + 1]
        c = closes[i]
        if c == max(window) and (i - w) + window.index(c) == i:
            highs.append(i)
        elif c == min(window) and (i - w) + window.index(c) == i:
            lows.append(i)
    return highs, lows


def _quality(p1, p2, depth, vol1, vol2):
    """0-100 pattern quality: level symmetry + valley depth + volume."""
    sym = max(0.0, 1.0 - abs(p2 - p1) / p1 / PEAK_TOL)        # 0..1
    dep = min(1.0, depth / 0.18)                               # 0..1
    vol = 1.0 if (vol1 and vol2 and vol2 < vol1) else 0.4
    return round(100.0 * (0.45 * sym + 0.40 * dep + 0.15 * vol))


def detect_double_top(dates, closes, vols):
    highs, lows = swing_points(closes)
    n = len(closes)
    best = None
    for a in range(len(highs)):
        for b in range(a + 1, len(highs)):
            i1, i2 = highs[a], highs[b]
            sep = i2 - i1
            if sep < SEP_MIN or sep > SEP_MAX:
                continue
            if i2 < n - PATTERN_RECENT:
                continue
            p1, p2 = closes[i1], closes[i2]
            if abs(p2 - p1) / p1 > PEAK_TOL:
                continue
            mids = [j for j in lows if i1 < j < i2]
            if not mids:
                continue
            it = min(mids, key=lambda j: closes[j])
            trough = closes[it]
            depth = (min(p1, p2) - trough) / min(p1, p2)
            if depth < VALLEY_MIN:
                continue
            status = "CONFIRMED" if closes[-1] < trough else "FORMING"
            q = _quality(p1, p2, depth, vols[i1], vols[i2])
            if best is None or q > best["quality"]:
                best = {
                    "pattern": "double_top",
                    "peak1": {"date": dates[i1], "price": round(p1, 2),
                              "idx": i1},
                    "peak2": {"date": dates[i2], "price": round(p2, 2),
                              "idx": i2},
                    "trough": {"date": dates[it], "price": round(trough, 2),
                               "idx": it},
                    "neckline": round(trough, 2),
                    "status": status,
                    "valley_depth_pct": round(depth * 100, 1),
                    "quality": q,
                    "last_close": round(closes[-1], 2),
                }
    return best


def detect_double_bottom(dates, closes, vols):
    highs, lows = swing_points(closes)
    n = len(closes)
    best = None
    for a in range(len(lows)):
        for b in range(a + 1, len(lows)):
            i1, i2 = lows[a], lows[b]
            sep = i2 - i1
            if sep < SEP_MIN or sep > SEP_MAX:
                continue
            if i2 < n - PATTERN_RECENT:
                continue
            t1, t2 = closes[i1], closes[i2]
            if abs(t2 - t1) / t1 > PEAK_TOL:
                continue
            mids = [j for j in highs if i1 < j < i2]
            if not mids:
                continue
            ip = max(mids, key=lambda j: closes[j])
            peak = closes[ip]
            depth = (peak - max(t1, t2)) / peak
            if depth < VALLEY_MIN:
                continue
            status = "CONFIRMED" if closes[-1] > peak else "FORMING"
            q = _quality(t1, t2, depth, vols[i1], vols[i2])
            if best is None or q > best["quality"]:
                best = {
                    "pattern": "double_bottom",
                    "trough1": {"date": dates[i1], "price": round(t1, 2),
                                "idx": i1},
                    "trough2": {"date": dates[i2], "price": round(t2, 2),
                                "idx": i2},
                    "peak": {"date": dates[ip], "price": round(peak, 2),
                             "idx": ip},
                    "neckline": round(peak, 2),
                    "status": status,
                    "rally_height_pct": round(depth * 100, 1),
                    "quality": q,
                    "last_close": round(closes[-1], 2),
                }
    return best


def tail_series(dates, closes, with_ma=False):
    """Last SERIES_BARS [date, close] (+ sma200) for charting."""
    n = len(closes)
    start = max(0, n - SERIES_BARS)
    out = []
    for t in range(start, n):
        if with_ma:
            ma = sma_at(closes, t, 200)
            out.append([dates[t], round(closes[t], 2),
                        round(ma, 2) if ma is not None else None])
        else:
            out.append([dates[t], round(closes[t], 2)])
    return out, start


def reindex(mark, start):
    """Shift an absolute bar index into the trimmed output series."""
    return {**mark, "idx": mark["idx"] - start}


# ---- handler ---------------------------------------------------------------
# ---- NEW: robust low-false-positive patterns + forward-expectancy ----------
def detect_volume_breakout(dates, closes, vols):
    """Price breaks its 60-day high on >2.5x average volume (new-high confirm)."""
    n = len(closes)
    if n < 70:
        return None
    i = n - 1
    win_hi = max(closes[i - 60:i])
    avg_vol = sum(vols[i - 20:i]) / 20 if vols[i - 20:i] else 0
    if avg_vol <= 0 or not vols[i]:
        return None
    vmult = vols[i] / avg_vol
    if closes[i] > win_hi and vmult >= 2.5:
        q = round(min(100, 50 + (vmult - 2.5) * 20 + (closes[i] / win_hi - 1) * 200))
        return {"pattern": "volume_breakout", "status": "CONFIRMED",
                "vol_multiple": round(vmult, 1), "breakout_level": round(win_hi, 2),
                "quality": q, "last_close": round(closes[i], 2)}
    return None


def detect_gap_and_go(dates, closes, vols):
    """Gap >3% on >2x avg volume that holds above the gap level."""
    n = len(closes)
    if n < 25:
        return None
    i = n - 1
    prev = closes[i - 1]
    if prev <= 0:
        return None
    gap = (closes[i] - prev) / prev
    avg_vol = sum(vols[i - 20:i]) / 20 if vols[i - 20:i] else 0
    vmult = (vols[i] / avg_vol) if avg_vol else 0
    if gap >= 0.03 and vmult >= 2.0 and closes[i] >= prev * 1.02:
        q = round(min(100, 45 + gap * 400 + (vmult - 2) * 12))
        return {"pattern": "gap_and_go", "status": "CONFIRMED",
                "gap_pct": round(gap * 100, 1), "vol_multiple": round(vmult, 1),
                "quality": q, "last_close": round(closes[i], 2)}
    return None


def _fwd_return(closes, idx, horizon):
    """Forward % return from idx to idx+horizon (None if not enough bars)."""
    if idx + horizon >= len(closes) or closes[idx] <= 0:
        return None
    return (closes[idx + horizon] / closes[idx] - 1) * 100


def backtest_expectancy(dates, closes, vols):
    """Walk the symbol's full history, find every PAST occurrence of each
    pattern, and record forward 20d/60d returns + quality. Returns a list of
    {pattern, quality, ret20, ret60} observations the handler aggregates across
    the universe into honest hit-rate / avg-return stats per pattern+bucket."""
    obs = []
    n = len(closes)
    # Volume-breakout & gap-and-go: cheap per-bar checks across history
    for i in range(70, n - 60):
        win_hi = max(closes[i - 60:i])
        avg_vol = sum(vols[i - 20:i]) / 20 if vols[i - 20:i] else 0
        if avg_vol > 0 and vols[i] and closes[i] > win_hi and vols[i] / avg_vol >= 2.5:
            q = round(min(100, 50 + (vols[i] / avg_vol - 2.5) * 20))
            obs.append({"pattern": "volume_breakout", "quality": q,
                        "ret20": _fwd_return(closes, i, 20), "ret60": _fwd_return(closes, i, 60)})
        prev = closes[i - 1]
        if prev > 0:
            gap = (closes[i] - prev) / prev
            if gap >= 0.03 and avg_vol and vols[i] / avg_vol >= 2.0:
                q = round(min(100, 45 + gap * 400))
                obs.append({"pattern": "gap_and_go", "quality": q,
                            "ret20": _fwd_return(closes, i, 20), "ret60": _fwd_return(closes, i, 60)})
    return obs


def scan_symbol(symbol):
    hist = get_history(symbol)
    if not hist:
        return None
    dates, closes, vols = hist
    res = {"symbol": symbol}
    cross = detect_cross_200dma(closes)
    dt = detect_double_top(dates, closes, vols)
    db = detect_double_bottom(dates, closes, vols)
    vb = detect_volume_breakout(dates, closes, vols)
    gg = detect_gap_and_go(dates, closes, vols)
    expectancy = backtest_expectancy(dates, closes, vols)
    if not (cross or dt or db or vb or gg):
        if expectancy:
            return {"symbol": symbol, "_expectancy": expectancy}
        return None
    if cross:
        ser, st = tail_series(dates, closes, with_ma=True)
        res["cross"] = {**cross, "series": ser}
    if dt:
        ser, st = tail_series(dates, closes, with_ma=False)
        res["double_top"] = {
            **{k: v for k, v in dt.items()
               if k not in ("peak1", "peak2", "trough")},
            "peak1": reindex(dt["peak1"], st),
            "peak2": reindex(dt["peak2"], st),
            "trough": reindex(dt["trough"], st),
            "series": ser}
    if db:
        ser, st = tail_series(dates, closes, with_ma=False)
        res["double_bottom"] = {
            **{k: v for k, v in db.items()
               if k not in ("trough1", "trough2", "peak")},
            "trough1": reindex(db["trough1"], st),
            "trough2": reindex(db["trough2"], st),
            "peak": reindex(db["peak"], st),
            "series": ser}
    if vb:
        ser, st = tail_series(dates, closes, with_ma=False)
        res["volume_breakout"] = {**vb, "series": ser}
    if gg:
        ser, st = tail_series(dates, closes, with_ma=False)
        res["gap_and_go"] = {**gg, "series": ser}
    if expectancy:
        res["_expectancy"] = expectancy
    return res


# ── ops 3396: SECTOR-ETF technical layer (fleet-audit gap fill) ──
# The audit found double-top/bottom + MA machinery existed only for S&P 500
# stocks; sector ETFs had no own-price ladder anywhere (and 100-DMA existed
# nowhere in the fleet). Same detectors, pointed at the 11 SPDRs + SPY.
SECTOR_ETFS = ["XLK", "XLF", "XLV", "XLY", "XLP", "XLE", "XLI", "XLB",
               "XLU", "XLRE", "XLC", "SPY"]


def sector_scan(sym):
    h = get_history(sym)
    if not h:
        return None
    dates, closes, vols = h
    out = {"symbol": sym, "close": round(closes[-1], 2), "date": dates[-1],
           "ma": {}}
    for w in (20, 50, 100, 200):
        if len(closes) < w + 1:
            out["ma"][f"d{w}"] = None
            continue
        m = sum(closes[-w:]) / w
        above = closes[-1] >= m
        cross = None
        if len(closes) > w + 6:
            for k in range(1, 6):
                mk = sum(closes[-w - k:-k]) / w
                if (closes[-1 - k] >= mk) != above:
                    cross = "up" if above else "down"
                    break
        out["ma"][f"d{w}"] = {"sma": round(m, 2), "above": above,
                              "dist_pct": round((closes[-1] / m - 1) * 100, 2),
                              "fresh_cross": cross}
    dt = detect_double_top(dates, closes, vols)
    db = detect_double_bottom(dates, closes, vols)
    if dt:
        out["double_top"] = dt
    if db:
        out["double_bottom"] = db
    ladder = [out["ma"][f"d{w}"]["above"] for w in (20, 50, 100, 200)
              if out["ma"].get(f"d{w}")]
    out["ladder_score"] = sum(ladder)
    out["posture"] = ("FULL UPTREND" if len(ladder) == 4 and all(ladder)
                      else "FULL DOWNTREND" if len(ladder) == 4 and not any(ladder)
                      else "MIXED")
    return out


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    if not FMP:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "error": "no FMP key"})}

    universe = get_sp500()
    if not universe:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False,
                                    "error": "empty universe"})}

    cross_up, cross_down, double_tops, double_bottoms = [], [], [], []
    volume_breakouts, gap_and_gos = [], []
    expectancy_obs = []
    n_scanned = 0
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for res in ex.map(scan_symbol, universe):
            if res is None:
                continue
            n_scanned += 1
            sym = res["symbol"]
            if res.get("_expectancy"):
                expectancy_obs.extend(res["_expectancy"])
            if "cross" in res:
                row = {"symbol": sym, **res["cross"]}
                (cross_up if row["direction"] == "up"
                 else cross_down).append(row)
            if "double_top" in res:
                double_tops.append({"symbol": sym, **res["double_top"]})
            if "double_bottom" in res:
                double_bottoms.append(
                    {"symbol": sym, **res["double_bottom"]})
            if "volume_breakout" in res:
                volume_breakouts.append({"symbol": sym, **res["volume_breakout"]})
            if "gap_and_go" in res:
                gap_and_gos.append({"symbol": sym, **res["gap_and_go"]})

    # ops 3396: sector-ETF layer
    sector_etfs = {}
    for _sym in SECTOR_ETFS:
        try:
            _r = sector_scan(_sym)
            if _r:
                sector_etfs[_r["symbol"]] = _r
        except Exception as _e:
            print(f"[sector] {_sym}: {str(_e)[:60]}")

    # ── Forward-expectancy: aggregate every historical occurrence into honest
    # hit-rate / avg-return stats per pattern + quality bucket. This is what
    # makes the scores credible (the audit's core ask). ──
    def _agg(obs, horizon_key):
        rets = [o[horizon_key] for o in obs if o.get(horizon_key) is not None]
        if len(rets) < 20:
            return None
        rets.sort()
        n = len(rets)
        return {
            "n": n,
            "avg_pct": round(sum(rets) / n, 2),
            "median_pct": round(rets[n // 2], 2),
            "hit_rate_pct": round(100 * sum(1 for r in rets if r > 0) / n, 1),
            "hit_5pct": round(100 * sum(1 for r in rets if r >= 5) / n, 1),
        }
    expectancy = {}
    for patt in ("volume_breakout", "gap_and_go"):
        po = [o for o in expectancy_obs if o["pattern"] == patt]
        for bucket, lo in (("all", 0), ("q60plus", 60), ("q80plus", 80)):
            bo = [o for o in po if o["quality"] >= lo]
            e20, e60 = _agg(bo, "ret20"), _agg(bo, "ret60")
            if e20 or e60:
                expectancy.setdefault(patt, {})[bucket] = {"fwd_20d": e20, "fwd_60d": e60}


    # crossovers: freshest first, then the cleanest break
    cross_up.sort(key=lambda r: (r["days_since_cross"],
                                 -abs(r["pct_from_ma"])))
    cross_down.sort(key=lambda r: (r["days_since_cross"],
                                   -abs(r["pct_from_ma"])))
    # patterns: confirmed first, then highest quality
    pk = lambda r: (0 if r["status"] == "CONFIRMED" else 1, -r["quality"])
    double_tops.sort(key=pk)
    double_bottoms.sort(key=pk)
    volume_breakouts.sort(key=lambda r: -r["quality"])
    gap_and_gos.sort(key=lambda r: -r["quality"])

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-chart-patterns",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 1),
        "universe_size": len(universe),
        "n_with_signal": n_scanned,
        "sector_etfs": sector_etfs,
        "counts": {
            "cross_up_200dma": len(cross_up),
            "cross_down_200dma": len(cross_down),
            "double_tops": len(double_tops),
            "double_bottoms": len(double_bottoms),
            "volume_breakouts": len(volume_breakouts),
            "gap_and_gos": len(gap_and_gos),
        },
        "cross_up_200dma": cross_up[:LIST_CAP],
        "cross_down_200dma": cross_down[:LIST_CAP],
        "double_tops": double_tops[:LIST_CAP],
        "double_bottoms": double_bottoms[:LIST_CAP],
        "volume_breakouts": volume_breakouts[:LIST_CAP],
        "gap_and_gos": gap_and_gos[:LIST_CAP],
        "expectancy": expectancy,
        "parameters": {
            "swing_window": SWING_W, "peak_tolerance_pct": PEAK_TOL * 100,
            "min_valley_depth_pct": VALLEY_MIN * 100,
            "cross_recent_sessions": CROSS_RECENT,
            "pattern_recency_sessions": PATTERN_RECENT,
        },
        "how_to_read": (
            "A daily S&P 500 scan for four classic technical setups. A "
            "200-DMA cross flags a long-term trend flip -- up is "
            "constructive, down is cautionary. A double top is a bearish "
            "reversal (two similar peaks, a real valley between); a double "
            "bottom is the bullish mirror. CONFIRMED means price has "
            "broken the neckline; FORMING means the shape is in place but "
            "unconfirmed. Patterns are probabilistic, not deterministic."),
        "disclaimer": (
            "Technical pattern detection on historical prices. It "
            "describes chart structure, it does not predict, and it is "
            "not investment advice."),
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

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "universe": len(universe), "n_with_signal": n_scanned,
        "counts": out["counts"],
        "build_seconds": out["build_seconds"]})}
