"""
justhodl-crypto-ma200  ·  v1.0  —  CRYPTO 200-DMA RECLAIM & RETEST RADAR
================================================================================
The crypto sibling of justhodl-ma200-reclaim. Same institutional logic — fresh
price-vs-200dma crosses (above/below, intact only) + the breakout→retest state
machine (HELD / FAILED / RETESTING) — applied to the liquid USD-quoted crypto
universe via Polygon's grouped crypto feed (one call per day, 24/7 calendar
sessions). Stablecoins are filtered out (a 200-DMA on a $1 peg is meaningless).

NOTE: crypto setups are surfaced but NOT logged to the equity scorecard, which
grades excess-vs-SPY. Crypto grading needs an excess-vs-BTC benchmark path — a
separate build. Until then this is a clean detector, measure-before-trust still
applies before treating the retest as a proven edge.
"""
import os, json, time, urllib.request
from datetime import datetime, timezone, timedelta
import boto3

S3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-ma200.json"
BUF_KEY = "data/_ma200/crypto-closes.json"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
VERSION = "1.0.0"

KEEP = 235
MAX_NAMES = 150
MIN_DVOL = 3.0e6          # $3M+ daily dollar volume (crypto liquidity floor)
FRESH = 5
RETEST_BAND = 0.02       # crypto is noisier — ±2% band
RETEST_LOOKBACK = 45
STABLES = {"USDT", "USDC", "DAI", "BUSD", "TUSD", "USDP", "FDUSD", "PYUSD", "GUSD",
           "USDD", "FRAX", "LUSD", "EURT", "EURS", "USTC", "USDE", "CRVUSD", "USDL"}


def grouped(date):
    u = (f"https://api.polygon.io/v2/aggs/grouped/locale/global/market/crypto/{date}"
         f"?adjusted=true&apiKey={POLY}")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=50).read())
        out = {}
        for r in (j.get("results") or []):
            t = str(r.get("T", ""))
            sym = t[2:] if t.startswith("X:") else t
            if not sym.endswith("USD") or r.get("c") in (None, 0):
                continue
            base = sym[:-3]
            if base in STABLES or not base:
                continue
            out[base] = (float(r["c"]), float(r.get("v") or 0))
        return out
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

    today = datetime.now(timezone.utc).date()
    want, d = [], today
    while len(want) < KEEP + 3:
        d -= timedelta(days=1)
        want.append(d.isoformat())        # crypto = every calendar day
    want = sorted(want)
    todo = [s for s in want if s not in have]
    for s in want[-2:]:
        if s not in todo:
            todo.append(s)
    todo = sorted(set(todo))

    fetched, new = 0, {}
    for ds in todo:
        if time.time() - t0 > 700:
            print("[budget] resuming next run")
            break
        g = grouped(ds)
        fetched += 1
        if g:
            new[ds] = g

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
            series[T][idx] = round(c, 6)

    newest = sorted(new.keys())[-1] if new else (dates[-1] if dates else None)
    if newest and newest in new:
        liq = sorted(((c * v, T) for T, (c, v) in new[newest].items() if c * v >= MIN_DVOL),
                     reverse=True)[:MAX_NAMES]
        keep = {T for _, T in liq}
        if keep:
            series = {T: s for T, s in series.items() if T in keep}
    if len(dates) > KEEP:
        cut = len(dates) - KEEP
        dates = dates[cut:]
        for T in series:
            series[T] = series[T][cut:]

    S3.put_object(Bucket=BUCKET, Key=BUF_KEY,
                  Body=json.dumps({"dates": dates, "series": series}).encode(),
                  ContentType="application/json")

    n = len(dates)
    out = {"engine": "crypto-ma200", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "buffer_sessions": n, "universe": len(series), "fetched_this_run": fetched,
           "duration_s": round(time.time() - t0, 1),
           "params": {"fresh_bars": FRESH, "retest_band_pct": RETEST_BAND * 100,
                      "retest_lookback": RETEST_LOOKBACK, "min_dollar_vol": MIN_DVOL,
                      "quote": "USD", "stablecoins_excluded": True}}

    if n < 205:
        out["status"] = f"WARMING — buffer at {n}/205 days, backfilling"
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                      ContentType="application/json", CacheControl="public, max-age=1800")
        print(f"[crypto-ma200] warming {n}/205 fetched={fetched}")
        return {"statusCode": 200, "body": json.dumps({"buffer_sessions": n})}

    above, below, held, failed, retesting = [], [], [], [], []
    L = min(RETEST_LOOKBACK + 8, n - 200)

    for T, ser in series.items():
        if len([x for x in ser if x is not None]) < 205:
            continue
        px = ser
        ma = [None] * n
        for i in range(n - L, n):
            win = [x for x in px[max(0, i - 199):i + 1] if x is not None]
            if len(win) >= 190:
                ma[i] = sum(win) / len(win)
        c0, m0 = px[-1], ma[-1]
        if m0 is None or c0 is None:
            continue
        lo = n - L

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
        ba = (n - 1 - cab) if cab is not None else None
        bb = (n - 1 - cbl) if cbl is not None else None
        dist = (c0 - m0) / m0 * 100
        slope = ((ma[-1] - ma[-22]) / ma[-22] * 100) if ma[-22] else None
        ma50 = _sma([x for x in px if x is not None], 50)
        gc = (ma50 is not None and ma50 > m0)
        base = {"ticker": T, "price": round(c0, 4 if c0 < 1 else 2), "ma200": round(m0, 4 if m0 < 1 else 2),
                "dist_pct": round(dist, 2),
                "ma200_slope_pct": round(slope, 2) if slope is not None else None,
                "ma50_above_ma200": gc}

        if ba is not None and ba <= FRESH and c0 > m0:
            r = dict(base); r["bars_since_cross"] = ba; above.append(r)
        if bb is not None and bb <= FRESH and c0 < m0:
            r = dict(base); r["bars_since_cross"] = bb; below.append(r)

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

    def q(x):
        return ((x.get("ma200_slope_pct") or -99) + (8 if x.get("ma50_above_ma200") else 0))

    def hq(x):
        return (q(x) - 0.25 * (x.get("retest_age") or 0)
                - 0.10 * max(0.0, abs(x.get("dist_pct") or 0) - 12))
    above.sort(key=q, reverse=True)
    below.sort(key=lambda x: (x.get("ma200_slope_pct") or 99))
    held.sort(key=hq, reverse=True)
    retesting.sort(key=q, reverse=True)

    out["counts"] = {"fresh_above": len(above), "fresh_below": len(below),
                     "retest_held": len(held), "retest_failed": len(failed),
                     "retesting_now": len(retesting)}
    out["fresh_breakouts_above"] = above[:60]
    out["fresh_breakdowns_below"] = below[:60]
    out["retest_held"] = held[:60]
    out["retest_failed"] = failed[:40]
    out["retesting_now"] = retesting[:40]
    out["thesis"] = ("Crypto 200-DMA reclaim & retest. The retest-held book is the polarity flip "
                     "(resistance→support) confirmed; raw crosses are noise, doubly so in crypto. "
                     "Quality rises with a turning-up MA slope and 50>200 alignment.")
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[crypto-ma200] sess={n} uni={len(series)} above={len(above)} below={len(below)} "
          f"held={len(held)} failed={len(failed)} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps(out["counts"])}
