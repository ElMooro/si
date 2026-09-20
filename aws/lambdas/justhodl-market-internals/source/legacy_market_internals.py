"""justhodl-market-internals v1.0 — ops 3185.

Khalid's watchlists reference 108 USI:* internals (advance/decline, TRIN,
new highs/lows, % above MA). Vendors charge for these. We COMPUTE them
from the Polygon grouped-daily feed he already pays for — one API call
returns every US ticker for a day, so the entire breadth complex costs
zero incremental dollars.

Metrics (daily, backfilled ~4y then incremental):
  ADVANCERS / DECLINERS / UNCHANGED
  ADVDEC_LINE        cumulative advance-decline line
  UP_VOLUME / DOWN_VOLUME
  TRIN               (adv/dec) / (upvol/downvol) — Arms index
  NEW_HIGHS / NEW_LOWS      252-day highs and lows
  PCT_ABOVE_50DMA / PCT_ABOVE_200DMA

State: data/market-internals-state.json.gz (per-ticker rolling closes)
Output: data/market-internals.json
"""

import gzip
import io
import json
import os
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import boto3
import urllib.request

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
POLY = os.environ.get("POLYGON_KEY", "")
OUT = "data/market-internals.json"
STATE = "data/market-internals-state.json.gz"
BACKFILL_DAYS = 1500          # ~4 trading years
BUDGET_S = 780
MIN_PRICE = 1.0               # ignore sub-$1 noise
MIN_VOL = 50_000

S3 = boto3.client("s3", region_name="us-east-1")


def s3_get(key, default=None, gz=False):
    try:
        b = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


def s3_put(key, doc, gz=False):
    b = json.dumps(doc).encode()
    if gz:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as f:
            f.write(b)
        b = buf.getvalue()
    S3.put_object(Bucket=BUCKET, Key=key, Body=b,
                  ContentType="application/json")


def grouped(day):
    try:
        u = ("https://api.polygon.io/v2/aggs/grouped/locale/us/market/"
             f"stocks/{day}?adjusted=true&apiKey={POLY}")
        r = urllib.request.urlopen(urllib.request.Request(
            u, headers={"User-Agent": "jh-internals/1.0"}), timeout=30)
        d = json.loads(r.read().decode())
        return day, (d.get("results") or [])
    except Exception:
        return day, []


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    st = s3_get(STATE, {}, gz=True) or {}
    have = set(st.get("days") or [])
    series = {k: dict(v) for k, v in (s3_get(OUT, {}) or {})
              .get("series", {}).items()}

    # the trading days we still need
    want = []
    d = now.date() - timedelta(days=1)
    while len(want) < BACKFILL_DAYS and d > now.date() - timedelta(days=2200):
        if d.weekday() < 5 and d.isoformat() not in have:
            want.append(d.isoformat())
        d -= timedelta(days=1)
    want.sort()
    print(f"[internals] {len(have)} days cached · {len(want)} to fetch")

    # per-ticker rolling closes (for MAs and 52w extremes)
    closes = defaultdict(lambda: deque(maxlen=252))
    for tk, arr in (st.get("closes") or {}).items():
        closes[tk].extend(arr)
    prev_close = dict(st.get("prev_close") or {})
    ad_line = float(st.get("ad_line") or 0.0)

    fetched = 0
    with ThreadPoolExecutor(max_workers=8) as ex:
        for day, rows in ex.map(grouped, want):
            if not rows:
                have.add(day)                # holiday / no data
                continue
            adv = dec = unch = 0
            upv = dnv = 0.0
            nh = nl = 0
            above50 = above200 = counted = 0
            for r in rows:
                tk, c, v = r.get("T"), r.get("c"), r.get("v") or 0
                if not tk or not c or c < MIN_PRICE or v < MIN_VOL:
                    continue
                pc = prev_close.get(tk)
                if pc:
                    if c > pc:
                        adv += 1
                        upv += v
                    elif c < pc:
                        dec += 1
                        dnv += v
                    else:
                        unch += 1
                dq = closes[tk]
                if len(dq) >= 60:
                    counted += 1
                    ma50 = sum(list(dq)[-50:]) / 50.0
                    if c > ma50:
                        above50 += 1
                    if len(dq) >= 200:
                        ma200 = sum(list(dq)[-200:]) / 200.0
                        if c > ma200:
                            above200 += 1
                    if c >= max(dq):
                        nh += 1
                    if c <= min(dq):
                        nl += 1
                dq.append(c)
                prev_close[tk] = c
            if adv + dec == 0:
                have.add(day)
                continue
            ad_line += (adv - dec)
            trin = None
            if dec and dnv and adv and upv:
                trin = round((adv / dec) / (upv / dnv), 3)
            put = lambda k, v: series.setdefault(k, {}).__setitem__(day, v)
            put("ADVANCERS", adv)
            put("DECLINERS", dec)
            put("UNCHANGED", unch)
            put("ADVDEC_LINE", round(ad_line, 1))
            put("UP_VOLUME", round(upv / 1e6, 2))
            put("DOWN_VOLUME", round(dnv / 1e6, 2))
            if trin:
                put("TRIN", trin)
            put("NEW_HIGHS", nh)
            put("NEW_LOWS", nl)
            if counted:
                put("PCT_ABOVE_50DMA", round(100 * above50 / counted, 2))
                put("PCT_ABOVE_200DMA", round(100 * above200 / counted, 2))
            have.add(day)
            fetched += 1
            if time.time() - t0 > BUDGET_S:
                print("[internals] budget reached — resuming next run")
                break

    # persist (cap the per-ticker tail to keep the state small)
    s3_put(STATE, {"days": sorted(have)[-BACKFILL_DAYS:],
                   "closes": {k: list(v)[-200:] for k, v in closes.items()
                              if len(v) >= 20},
                   "prev_close": prev_close,
                   "ad_line": ad_line}, gz=True)
    doc = {"generated_at": now.isoformat(), "version": "1.0",
           "source": "computed from Polygon grouped-daily (zero extra cost)",
           "days_covered": len(have), "days_added": fetched,
           "metrics": sorted(series.keys()),
           "latest": {k: (sorted(v.items())[-1] if v else None)
                      for k, v in series.items()},
           "series": series,
           "elapsed_s": round(time.time() - t0, 1)}
    s3_put(OUT, doc)
    print(json.dumps({"ok": True, "days": len(have), "added": fetched,
                      "metrics": len(series),
                      "elapsed": doc["elapsed_s"]}))
    return {"ok": True, "days_covered": len(have), "days_added": fetched,
            "n_metrics": len(series)}
