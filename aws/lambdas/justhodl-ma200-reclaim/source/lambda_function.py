"""
justhodl-ma200-reclaim  ·  v1.0  —  200-DMA RECLAIM & RETEST RADAR
================================================================================
Detects, across the full liquid US equity+ETF universe:

  1. FRESH CROSSES — every asset that just broke ABOVE or BELOW its 200-day SMA
     within the last N sessions ("just broke").
  2. SUCCESSFUL RETESTS — of the names that broke above, the ones that pulled
     back to the 200-DMA and HELD (the polarity flip: resistance → support),
     vs the ones that FAILED (closed back below = bull trap). The retest-held
     book is the high-probability continuation setup; the raw cross is noise.

Pressure-tested (not folklore):
  • A cross while the 200-DMA is still FALLING is weak; while it's turning UP is
    strong. We carry the 21-bar MA slope and 50>200 (golden-cross) alignment.
  • The edge is the RETEST, not the cross. State machine per name:
      BROKE_ABOVE  → reclaimed, not yet pulled back
      RETESTING    → currently back at the line (within band)
      RETEST_HELD  → touched the line / dipped, then closed back above (CONFIRMED)
      RETEST_FAILED→ closed back below after the breakout (failed reclaim)
  • Scalable: maintains a rolling per-ticker close matrix from the grouped-daily
    feed (one API call per session), liquidity-bounded — no per-ticker API storms.

Retest-held longs + fresh breakdowns log to the closed loop so the scorecard
grades forward excess-vs-SPY. We do NOT claim the retest beats the raw cross
until the ledger proves it.
"""
import os, json, time, urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import boto3

S3 = boto3.client("s3", "us-east-1")
DDB = boto3.resource("dynamodb", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ma200-reclaim.json"
BUF_KEY = "data/_ma200/closes.json"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
VERSION = "1.0.0"

KEEP = 235            # closes retained per ticker (enough for 200-dma + retest path)
MAX_NAMES = 1200      # liquidity-bounded universe cap (by dollar volume)
MIN_PX = 5.0
MIN_DVOL = 2.0e7      # $20M+ daily dollar volume = liquid
FRESH = 5             # cross within last N sessions = "just broke"
RETEST_BAND = 0.015   # within ±1.5% of the 200-DMA = at the line
RETEST_LOOKBACK = 45  # bars after a breakout to watch for a retest


def grouped(date):
    u = (f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"
         f"?adjusted=true&apiKey={POLY}")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=50).read())
        return {r["T"]: (float(r["c"]), float(r.get("v") or 0))
                for r in (j.get("results") or []) if r.get("c")}
    except Exception as e:
        print(f"[grouped] {date}: {str(e)[:50]}")
        return {}


def _sma(series, n):
    xs = [x for x in series[-n:] if x is not None]
    return sum(xs) / len(xs) if len(xs) >= n * 0.95 else None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    buf = {"dates": [], "series": {}}
    try:
        buf = json.loads(S3.get_object(Bucket=BUCKET, Key=BUF_KEY)["Body"].read())
    except Exception:
        pass
    dates = buf.get("dates", [])
    series = buf.get("series", {})
    have = set(dates)

    # sessions we want (newest KEEP+5), ascending
    today = datetime.now(timezone.utc).date()
    want, d = [], today
    while len(want) < KEEP + 5:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            want.append(d.isoformat())
    want = sorted(want)
    todo = [s for s in want if s not in have]
    for s in want[-2:]:                       # always refresh last 2
        if s not in todo:
            todo.append(s)
    todo = sorted(set(todo))

    fetched, new = 0, {}
    for ds in todo:
        if time.time() - t0 > 700:
            print("[budget] resuming buffer fill next run")
            break
        g = grouped(ds)
        fetched += 1
        if g:
            new[ds] = g

    # merge fetched sessions into the aligned matrix
    for ds in sorted(new.keys()):
        g = new[ds]
        if ds in dates:
            idx = dates.index(ds)
        else:
            dates.append(ds)
            idx = len(dates) - 1
            for T in series:
                series[T].append(None)
        for T, (c, _v) in g.items():
            if T not in series:
                series[T] = [None] * len(dates)
            series[T][idx] = round(c, 3)

    # liquidity prune (bound size): keep top MAX_NAMES by latest dollar volume
    newest = sorted(new.keys())[-1] if new else (dates[-1] if dates else None)
    # guarantee chronological order — later backfills can append dates out of order.
    if dates != sorted(dates):
        order = sorted(range(len(dates)), key=lambda i: dates[i])
        dates = [dates[i] for i in order]
        for T in series:
            series[T] = [series[T][i] for i in order]
    if newest and newest in new:
        liq = sorted(((c * v, T) for T, (c, v) in new[newest].items()
                      if c >= MIN_PX and c * v >= MIN_DVOL), reverse=True)[:MAX_NAMES]
        keep = {T for _, T in liq}
        if keep:
            series = {T: s for T, s in series.items() if T in keep}
    # cap history depth
    if len(dates) > KEEP:
        cut = len(dates) - KEEP
        dates = dates[cut:]
        for T in series:
            series[T] = series[T][cut:]

    S3.put_object(Bucket=BUCKET, Key=BUF_KEY,
                  Body=json.dumps({"dates": dates, "series": series}).encode(),
                  ContentType="application/json")

    n = len(dates)
    out = {"engine": "ma200-reclaim", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "buffer_sessions": n, "universe": len(series), "fetched_this_run": fetched,
           "duration_s": round(time.time() - t0, 1),
           "params": {"fresh_bars": FRESH, "retest_band_pct": RETEST_BAND * 100,
                      "retest_lookback": RETEST_LOOKBACK, "min_px": MIN_PX, "min_dollar_vol": MIN_DVOL}}

    if n < 205:
        out["status"] = f"WARMING — buffer at {n}/205 sessions, backfilling"
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                      ContentType="application/json", CacheControl="public, max-age=1800")
        print(f"[ma200] warming {n}/205 fetched={fetched}")
        return {"statusCode": 200, "body": json.dumps({"buffer_sessions": n})}

    above, below, held, failed, retesting = [], [], [], [], []
    L = min(RETEST_LOOKBACK + 8, n - 200)     # how many recent points get an ma200

    for T, ser in series.items():
        if len([x for x in ser if x is not None]) < 205:
            continue
        px = ser
        # ma200 over the last L indices (rolling exact mean)
        ma = [None] * n
        for i in range(n - L, n):
            win = [x for x in px[max(0, i - 199):i + 1] if x is not None]
            if len(win) >= 190:
                ma[i] = sum(win) / len(win)
        c0, m0 = px[-1], ma[-1]
        if m0 is None or c0 is None:
            continue
        lo = n - L                             # earliest index with an ma200

        def _cross(want_above):
            for i in range(n - 1, lo, -1):
                a, b, p, q = ma[i], ma[i - 1], px[i], px[i - 1]
                if None in (a, b, p, q):
                    continue
                if want_above and q <= b and p > a:
                    return i
                if (not want_above) and q >= b and p < a:
                    return i
            return None

        cab, cbl = _cross(True), _cross(False)
        ba = (n - 1 - cab) if cab is not None else None     # bars since break above
        bb = (n - 1 - cbl) if cbl is not None else None
        dist = (c0 - m0) / m0 * 100
        slope = ((ma[-1] - ma[-22]) / ma[-22] * 100) if ma[-22] else None
        ma50 = _sma([x for x in px if x is not None], 50)
        gc = (ma50 is not None and ma50 > m0)
        base = {"ticker": T, "price": round(c0, 2), "ma200": round(m0, 2),
                "dist_pct": round(dist, 2),
                "ma200_slope_pct": round(slope, 2) if slope is not None else None,
                "ma50_above_ma200": gc}

        # 1) fresh crosses — intact only (price still on the breakout side, not a same-week reversal)
        if ba is not None and ba <= FRESH and c0 > m0:
            r = dict(base); r["bars_since_cross"] = ba; above.append(r)
        if bb is not None and bb <= FRESH and c0 < m0:
            r = dict(base); r["bars_since_cross"] = bb; below.append(r)

        # 2) retest of a prior break ABOVE (within lookback)
        if cab is not None and ba >= 1:
            touch_i = None
            for j in range(cab + 1, n):
                if ma[j] is None or px[j] is None:
                    continue
                if abs(px[j] - ma[j]) / ma[j] <= RETEST_BAND or px[j] < ma[j]:
                    touch_i = j
                    break
            if touch_i is not None:
                r = dict(base); r["bars_since_cross"] = ba
                r["retest_age"] = n - 1 - touch_i
                r["retest_low_pct"] = round((px[touch_i] - ma[touch_i]) / ma[touch_i] * 100, 2)
                if c0 > m0 and abs(dist) <= RETEST_BAND * 100:
                    r["state"] = "RETESTING"; retesting.append(r)
                elif c0 > m0:
                    r["state"] = "RETEST_HELD_DIP" if px[touch_i] < ma[touch_i] else "RETEST_HELD"
                    held.append(r)
                else:
                    r["state"] = "RETEST_FAILED"; failed.append(r)

    # quality ranks: prefer rising MA + golden-cross alignment
    def q(x):
        return ((x.get("ma200_slope_pct") or -99) + (8 if x.get("ma50_above_ma200") else 0))

    def hq(x):   # held: rising MA + golden cross, fresher retest, not over-extended
        return (q(x) - 0.25 * (x.get("retest_age") or 0)
                - 0.10 * max(0.0, abs(x.get("dist_pct") or 0) - 8))
    above.sort(key=q, reverse=True)
    below.sort(key=lambda x: (x.get("ma200_slope_pct") or 99))
    held.sort(key=hq, reverse=True)
    retesting.sort(key=q, reverse=True)

    out["counts"] = {"fresh_above": len(above), "fresh_below": len(below),
                     "retest_held": len(held), "retest_failed": len(failed),
                     "retesting_now": len(retesting)}
    out["fresh_breakouts_above"] = above[:70]
    out["fresh_breakdowns_below"] = below[:70]
    out["retest_held"] = held[:70]
    out["retest_failed"] = failed[:50]
    out["retesting_now"] = retesting[:50]
    out["thesis"] = ("The retest is the edge, not the cross. A reclaim that pulls back to the 200-DMA and "
                     "holds confirms the polarity flip (resistance→support); a close back below is a bull trap. "
                     "Quality rises with a turning-up MA slope and 50>200 alignment.")

    # closed loop — grade retest-held longs + fresh breakdowns vs SPY
    try:
        nowt = datetime.now(timezone.utc)
        spy = (new.get(newest, {}) or {}).get("SPY")
        tbl = DDB.Table("justhodl-signals")
        logged = 0
        for grp, dr, cf in ((held[:15], "UP", 0.58), (below[:10], "DOWN", 0.55)):
            for r in grp:
                px0 = r["price"]
                tbl.put_item(Item={
                    "signal_id": f"ma200-{dr}#{r['ticker']}#{dates[-1]}",
                    "signal_type": "ma200_retest" if dr == "UP" else "ma200_breakdown",
                    "signal_value": str(r.get("dist_pct")), "predicted_direction": dr,
                    "confidence": Decimal(str(cf)), "measure_against": "ticker",
                    "baseline_price": str(px0), "benchmark": "SPY",
                    "check_windows": ["day_5", "day_21", "day_63"],
                    "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat()
                                         for w in (5, 21, 63)},
                    "outcomes": {}, "accuracy_scores": {}, "logged_at": nowt.isoformat(),
                    "logged_epoch": int(nowt.timestamp()), "status": "pending", "schema_version": "2",
                    "horizon_days_primary": 21,
                    "ttl": int(nowt.timestamp()) + 120 * 86400,
                    "metadata": {"engine": "ma200-reclaim", "v": VERSION, "state": r.get("state")},
                    "rationale": f"{r['ticker']} {r.get('state', dr)} 200-DMA (dist {r.get('dist_pct')}%, slope {r.get('ma200_slope_pct')}%)"})
                logged += 1
        out["signals_logged"] = logged
    except Exception as e:
        print(f"[loop] {str(e)[:70]}")

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[ma200] sess={n} uni={len(series)} above={len(above)} below={len(below)} "
          f"held={len(held)} failed={len(failed)} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps(out["counts"])}
