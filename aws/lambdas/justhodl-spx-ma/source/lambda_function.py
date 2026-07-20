"""justhodl-spx-ma v1.0 — THE dedicated S&P 500 moving-average command engine.

Khalid (2026-07-20): "Just as we monitor all the ETFs for 200/100/50/20 MAs we
should monitor the SP500 itself — an engine dedicated to the S&P 500 as a
whole, not single ETFs." Audit: index MA checks lived only as a SPY-vs-SMA20/50
sub-block inside industry-rotation; nothing owned the index ladder, and daily
membership breadth existed nowhere.

TWO LAYERS (extend-don't-duplicate — wires three existing systems):
  INDEX   ^GSPC vs SMA 20/50/100/200 — closes from data/spx-history-deep.json
          (the existing spx-history refresher) + today's live print from
          data/market-tape.json. Ladder stack state, distance %, 20d MA slopes,
          50x200 golden/death cross + days-since, MA compression (coil), regime.
  BREADTH the S&P 500 *as a whole*: % of the 503 members above each MA.
          50d/200d = instant + daily via FMP batch quotes (priceAvg50/200).
          20d/100d = self-building ledger: one Polygon grouped-daily call per
          day (all tickers, one request), bootstrapping backwards until 110
          trading days accrue — 20d breadth activates in-session, 100d labeled
          WARMING until filled. Membership = the census matrix ticker set.
Divergence flag: index above 200d while <50% of members are = NARROW MARKET.
Real data only; every layer degrades independently and reports coverage.
Out: data/spx-ma.json · ledger spx-ma/member-closes.json · daily 21:15 UTC.
"""
import json, os, time, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone
import boto3

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/spx-ma.json"
LEDGER_KEY = "spx-ma/member-closes.json"
s3 = boto3.client("s3", region_name="us-east-1")
FMP = os.environ.get("FMP_API_KEY") or ""
POLY = os.environ.get("POLYGON_API_KEY") or ""
UA = {"User-Agent": "JustHodl research contact@justhodl.ai"}
TARGET_DAYS = 110


def _g(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print("[spx-ma] load fail", key, str(e)[:60])
        return None


def _j(url, timeout=20):
    try:
        return json.loads(urllib.request.urlopen(
            urllib.request.Request(url, headers=UA), timeout=timeout).read())
    except Exception as e:
        print("[spx-ma] http fail", url[:70], str(e)[:70])
        return None


def sma(xs, n):
    return round(sum(xs[-n:]) / n, 2) if len(xs) >= n else None


def index_layer():
    doc = _g("data/spx-history-deep.json") or {}
    pts = doc.get("points") or []
    closes = [p[1] for p in pts if isinstance(p, (list, tuple)) and len(p) == 2
              and isinstance(p[1], (int, float))]
    last_date = pts[-1][0] if pts else None
    # bolt on the live tape print if fresher than history
    tape = _g("data/market-tape.json") or {}
    live = next((it.get("value") for it in tape.get("items") or []
                 if it.get("label") == "SPX" and isinstance(it.get("value"), (int, float))), None)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if live and last_date and last_date < today:
        closes = closes + [live]
    if len(closes) < 210:
        return {"error": f"history too short ({len(closes)})"}
    px = closes[-1]
    mas = {n: sma(closes, n) for n in (20, 50, 100, 200)}
    above = {n: bool(mas[n] and px > mas[n]) for n in mas}
    n_above = sum(above.values())
    stack = f"{n_above}/4 " + ("BULL STACK" if n_above == 4 else
                               "BEAR STACK" if n_above == 0 else "MIXED")
    dist = {n: round((px / mas[n] - 1) * 100, 2) for n in mas if mas[n]}
    slope = {}
    for n in mas:
        if len(closes) >= n + 20 and mas[n]:
            prev = sma(closes[:-20], n)
            if prev:
                slope[n] = round((mas[n] / prev - 1) * 100, 2)
    # 50x200 cross state + days since flip
    cross, days_since = None, None
    if len(closes) >= 260:
        rel = []
        for i in range(220, 0, -1):
            w = closes[:len(closes) - i + 1] if i > 1 else closes
            a, b = sma(w, 50), sma(w, 200)
            if a and b:
                rel.append(a - b)
        if rel:
            cross = "GOLDEN" if rel[-1] > 0 else "DEATH"
            days_since = 0
            for i in range(len(rel) - 1, 0, -1):
                if (rel[i] > 0) != (rel[i - 1] > 0):
                    break
                days_since += 1
            if days_since >= len(rel) - 1:
                days_since = f">{len(rel)}"
    comp = (round((max(mas.values()) / min(mas.values()) - 1) * 100, 2)
            if all(mas.values()) else None)
    regime = ("BULL" if above.get(200) and (slope.get(200, 0) or 0) >= 0 else
              "BEAR" if not above.get(200) and (slope.get(200, 0) or 0) < 0 else "TRANSITION")
    return {"price": round(px, 2), "as_of": (today if live and last_date < today else last_date),
            "sma": {str(k): v for k, v in mas.items()},
            "above": {str(k): v for k, v in above.items()},
            "stack": stack, "distance_pct": {str(k): v for k, v in dist.items()},
            "slope_20d_pct": {str(k): v for k, v in slope.items()},
            "cross_50x200": {"state": cross, "days_since_flip": days_since},
            "ma_compression_pct": comp, "regime": regime,
            "source": "spx-history-deep (existing engine) + market-tape live print"}


def members():
    mx = _g("data/fundamental-census-matrix.json") or {}
    return [t for t in (mx.get("tickers") or []) if isinstance(t, str) and t.isalnum()]


def breadth_quotes(tks):
    """50d/200d breadth via FMP batch quotes (priceAvg50/priceAvg200)."""
    a50 = a200 = n = 0
    for i in range(0, len(tks), 80):
        chunk = ",".join(tks[i:i + 80])
        rows = _j(f"https://financialmodelingprep.com/stable/quote?symbol={chunk}&apikey={FMP}")
        for r in rows or []:
            p, m5, m2 = r.get("price"), r.get("priceAvg50"), r.get("priceAvg200")
            if all(isinstance(x, (int, float)) and x > 0 for x in (p, m5, m2)):
                n += 1
                a50 += p > m5
                a200 += p > m2
        time.sleep(0.12)
    if not n:
        return {"error": "no quotes"}
    return {"n_priced": n, "above50_pct": round(100.0 * a50 / n, 1),
            "above200_pct": round(100.0 * a200 / n, 1),
            "spread_50_200": round(100.0 * (a50 - a200) / n, 1)}


def ledger_update(tks, budget_deadline):
    """Self-building member-close ledger from Polygon grouped-daily (one call
    per trading day; bootstraps backwards until TARGET_DAYS accrue)."""
    led = _g(LEDGER_KEY) or {"dates": [], "closes": {}}
    dates = led["dates"]
    tset = set(tks)

    def fetch_day(ds):
        j = _j("https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/"
               f"{ds}?adjusted=true&apiKey={POLY}", timeout=25)
        res = (j or {}).get("results") or []
        return {r["T"]: round(r["c"], 2) for r in res
                if r.get("T") in tset and isinstance(r.get("c"), (int, float))}

    def insert(ds, mp, front):
        if not mp or ds in dates:
            return False
        if front:
            dates.insert(0, ds)
        else:
            dates.append(ds)
        for t in tset:
            arr = led["closes"].setdefault(t, [None] * (len(dates) - 1))
            while len(arr) < len(dates) - 1:
                arr.append(None)
            if front:
                arr.insert(0, mp.get(t))
            else:
                arr.append(mp.get(t))
        return True

    added = 0
    # forward: latest completed sessions not yet in ledger
    d = datetime.now(timezone.utc).date()
    for back in range(1, 6):
        ds = (d - timedelta(days=back)).isoformat()
        if ds not in dates and datetime.fromisoformat(ds).weekday() < 5:
            if insert(ds, fetch_day(ds), front=False):
                added += 1
            time.sleep(0.15)
    dates_sorted = sorted(dates)
    if dates_sorted != dates:   # keep chronological
        order = {ds: i for i, ds in enumerate(dates)}
        idx = [order[ds] for ds in dates_sorted]
        for t, arr in led["closes"].items():
            led["closes"][t] = [arr[i] if i < len(arr) else None for i in idx]
        led["dates"] = dates = dates_sorted
    # backward bootstrap until target or budget
    probe = datetime.fromisoformat(dates[0]).date() if dates else d
    while len(dates) < TARGET_DAYS and time.time() < budget_deadline:
        probe = probe - timedelta(days=1)
        if probe.weekday() >= 5:
            continue
        if insert(probe.isoformat(), fetch_day(probe.isoformat()), front=True):
            added += 1
        time.sleep(0.15)
    # trim
    if len(dates) > TARGET_DAYS + 15:
        cut = len(dates) - TARGET_DAYS
        led["dates"] = dates = dates[cut:]
        for t in led["closes"]:
            led["closes"][t] = led["closes"][t][cut:]
    s3.put_object(Bucket=BUCKET, Key=LEDGER_KEY, Body=json.dumps(led).encode(),
                  ContentType="application/json")
    return led, added


def breadth_ledger(led, n_ma):
    ab = tot = 0
    for t, arr in (led.get("closes") or {}).items():
        xs = [x for x in arr if isinstance(x, (int, float))]
        if len(xs) >= n_ma:
            tot += 1
            if xs[-1] > sum(xs[-n_ma:]) / n_ma:
                ab += 1
    return (round(100.0 * ab / tot, 1), tot) if tot >= 300 else (None, tot)


def lambda_handler(event=None, context=None):
    t0 = time.time()
    deadline = t0 + min(720, (context.get_remaining_time_in_millis() / 1000 - 90)
                        if context else 720)
    idx = index_layer()
    tks = members()
    bq = breadth_quotes(tks) if tks else {"error": "no members"}
    led, added = ledger_update(tks, deadline) if tks else ({"dates": []}, 0)
    b20, c20 = breadth_ledger(led, 20)
    b100, c100 = breadth_ledger(led, 100)
    n_days = len(led.get("dates") or [])
    warming = {"ledger_days": n_days, "target_days": TARGET_DAYS, "added_this_run": added,
               "b20_ready": b20 is not None, "b100_ready": b100 is not None,
               "note": ("self-building: one grouped-daily call per session; 100d breadth "
                        "activates automatically as the ledger fills" if n_days < TARGET_DAYS
                        else "ledger full")}
    narrow = bool((idx.get("above") or {}).get("200") and
                  isinstance(bq.get("above200_pct"), (int, float)) and bq["above200_pct"] < 50)
    out = {"engine": "spx-ma", "version": "1.0.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "index": idx,
           "breadth": {"n_members": len(tks), **bq,
                       "above20_pct": b20, "above20_covered": c20,
                       "above100_pct": b100, "above100_covered": c100,
                       "divergence_narrow_market": narrow, "warming": warming},
           "siblings": {"per_etf_ladder": "data/industry-rotation.json (33 ETFs SMA50/100/200)",
                        "index_price_source": "data/spx-history-deep.json",
                        "weekly_member_cross_check": "census matrix above_ma40w"},
           "methodology": {"index": "^GSPC closes + live tape print; SMA20/50/100/200 ladder, stack, distances, 20d slopes, 50x200 cross + days-since, compression, regime",
                           "breadth": "S&P 500 membership (census set): 50/200 via FMP batch priceAvg fields daily; 20/100 from the self-building Polygon grouped-daily ledger",
                           "divergence": "index above 200d while <50% of members are = NARROW MARKET"},
           "disclaimer": "Real data only. Research, not advice.",
           "elapsed_s": round(time.time() - t0, 2)}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[spx-ma] px={idx.get('price')} stack={idx.get('stack')} "
          f"b50={bq.get('above50_pct')}% b200={bq.get('above200_pct')}% "
          f"b20={b20}% ledger={n_days}d(+{added}) {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "stack": idx.get("stack"), "b50": bq.get("above50_pct"),
        "b200": bq.get("above200_pct"), "b20": b20, "ledger_days": n_days})}
