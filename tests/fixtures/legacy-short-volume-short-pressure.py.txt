"""
justhodl-short-pressure — Daily Short-Volume Pressure

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
Polygon's daily short-volume endpoint (confirmed unlocked, ops/735) was
unused. Daily short volume is noisy in absolute terms — much of it is
market-maker hedging — so the signal is the RATIO measured against each
name's OWN recent baseline: a short-volume ratio pushing above a
ticker's 20-day norm is genuine building short pressure; a ratio
collapsing is short covering.

Runs over the master-ranker universe → data/short-pressure.json.
OUTPUT: data/short-pressure.json   SCHEDULE: daily 12:30 UTC
═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import statistics
import urllib.request
import urllib.error
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/short-pressure.json"
RANKER_KEY = "data/master-ranker.json"
POLY_KEY = os.environ.get("POLY_KEY", "")
MAX_UNIVERSE = 30
FALLBACK = ["AAPL", "MSFT", "NVDA", "TSLA", "AMD", "META", "AMZN", "GOOGL",
            "COIN", "PLTR", "MSTR", "SMCI", "GME", "AMC", "CVNA"]

s3 = boto3.client("s3", region_name="us-east-1")


def poly(path):
    url = f"https://api.polygon.io{path}{'&' if '?' in path else '?'}apiKey={POLY_KEY}"
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < 2:
                time.sleep(2 ** attempt)
                continue
            return None
        except Exception:
            if attempt < 2:
                time.sleep(1)
                continue
            return None
    return None


def ratio_of(rec):
    r = rec.get("short_volume_ratio")
    if r is not None:
        try:
            r = float(r)
            return r * 100 if r <= 1 else r          # normalise to percent
        except (TypeError, ValueError):
            pass
    sv, tv = rec.get("short_volume"), rec.get("total_volume")
    try:
        if sv is not None and tv:
            return float(sv) / float(tv) * 100
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return None


def analyse(ticker):
    try:
        data = poly(f"/stocks/v1/short-volume?ticker={ticker}&limit=24&order=desc")
        rows = (data or {}).get("results") or []
        ratios = [r for r in (ratio_of(x) for x in rows) if r is not None]
        if len(ratios) < 6:
            return {"ticker": ticker, "ok": False}
        latest = ratios[0]
        avg5 = round(statistics.mean(ratios[:5]), 1)
        base = ratios[1:21] if len(ratios) > 6 else ratios[1:]
        base_avg = statistics.mean(base)
        base_sd = statistics.pstdev(base) if len(base) > 2 else 0
        delta = round(latest - base_avg, 1)
        z = round((latest - base_avg) / base_sd, 2) if base_sd else None
        if z is not None and z >= 1.0:
            state = "PRESSURE BUILDING"
        elif z is not None and z <= -1.0:
            state = "SHORTS COVERING"
        else:
            state = "NORMAL"
        return {
            "ticker": ticker, "ok": True,
            "short_ratio_latest": round(latest, 1),
            "short_ratio_5d": avg5,
            "baseline_20d": round(base_avg, 1),
            "delta_vs_baseline": delta,
            "z_score": z,
            "state": state,
            "as_of": rows[0].get("date"),
        }
    except Exception as e:
        return {"ticker": ticker, "ok": False, "error": str(e)[:140]}


def universe():
    try:
        ranker = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=RANKER_KEY)["Body"].read())
        tk = [t.get("ticker") for t in (ranker.get("top_tickers") or []) if t.get("ticker")]
        tk = list(dict.fromkeys(tk))
        if tk:
            return tk[:MAX_UNIVERSE], "master-ranker top tickers"
    except Exception:
        pass
    return FALLBACK, "high-short-interest fallback list"


def lambda_handler(event, context):
    t0 = time.time()
    if not POLY_KEY:
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "err": "POLY_KEY missing"})}
    uni, src = universe()
    with ThreadPoolExecutor(max_workers=6) as ex:
        rows = list(ex.map(analyse, uni))
    names = [r for r in rows if r.get("ok")]
    failed = [r["ticker"] for r in rows if not r.get("ok")]
    names.sort(key=lambda r: (r.get("z_score") if r.get("z_score") is not None
                              else -99), reverse=True)
    building = [r for r in names if r["state"] == "PRESSURE BUILDING"]
    covering = [r for r in names if r["state"] == "SHORTS COVERING"]

    out = {
        "schema_version": "1.0",
        "method": "polygon_short_volume_vs_baseline",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "universe_source": src,
        "n_covered": len(names),
        "n_failed": len(failed),
        "n_pressure_building": len(building),
        "n_shorts_covering": len(covering),
        "names": names,
        "note": ("Daily short-volume ratio vs each name's own 20-day baseline "
                 "(z-score). Absolute short volume is noisy — much is "
                 "market-maker hedging — so only the deviation from a "
                 "ticker's norm is treated as signal. Not advice."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[short-pressure] {len(names)} covered, {len(building)} building, "
          f"{len(covering)} covering, {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_covered": len(names),
        "n_pressure_building": len(building),
        "n_shorts_covering": len(covering)})}
