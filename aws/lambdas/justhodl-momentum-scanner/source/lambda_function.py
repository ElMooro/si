"""
justhodl-momentum-scanner — Universe-wide multi-momentum signal across S&P 500.

For each S&P 500 ticker:
  - 1m/3m/6m/12m total returns
  - Composite momentum score = avg of percentile ranks across windows
  - 12-1 momentum (12m return EXCLUDING last month) — the academic factor
  - Volatility-adjusted momentum (return / 60d vol)
  - 52-week high distance (close to ATH = trending strongly)
  - Acceleration: (1m return - 6m return)/6m → is momentum accelerating or fading

Output rankings:
  - Top 50 momentum (composite)
  - Bottom 50 (potential mean-reversion candidates)
  - Top 20 by 12-1 (best academic factor)
  - Top 20 by accelerating momentum
  - Top 20 closest to 52-week high

Output: data/momentum-scanner.json
Schedule: daily at 12:30 UTC weekdays (market open + 1hr buffer)
"""
import json
import math
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/momentum-scanner.json"

POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")

WINDOWS = [21, 63, 126, 252]  # 1m, 3m, 6m, 12m
NAMES = {21: "1m", 63: "3m", 126: "6m", 252: "12m"}


def get_universe():
    """Pull S&P 500 list from screener/data.json."""
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="screener/data.json")
        d = json.loads(obj["Body"].read())
        stocks = d.get("stocks", [])
        return [(s["symbol"], s.get("name", ""), s.get("sector", ""), s.get("marketCap")) for s in stocks if s.get("symbol")]
    except Exception as e:
        print(f"[universe] {e}")
        return []


def polygon_bars(ticker, days=300):
    end = datetime.now(timezone.utc).date().isoformat()
    start = (datetime.now(timezone.utc).date() - timedelta(days=days + 100)).isoformat()
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}"
        f"?adjusted=true&sort=asc&limit=5000&apiKey={POLYGON_KEY}"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-momentum/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode())
        if data.get("results"):
            return [(b["t"], b["c"], b["h"]) for b in data["results"]]
    except Exception as e:
        if "429" in str(e):
            time.sleep(2)
            return polygon_bars(ticker, days)
        return []
    return []


def compute_momentum(bars):
    """Multi-window momentum + composite score."""
    if len(bars) < 252:
        return None

    last = bars[-1][1]
    out = {"last_close": last}

    # Returns
    for w in WINDOWS:
        if len(bars) > w:
            prev = bars[-1 - w][1]
            if prev > 0:
                out[f"ret_{NAMES[w]}"] = round((last / prev - 1) * 100, 3)

    # 12-1 momentum (academic): 252-day return excluding the last 21 days
    if len(bars) > 252 and len(bars) > 21:
        prev_252 = bars[-252][1]
        prev_21 = bars[-21][1]
        if prev_252 > 0 and prev_21 > 0:
            out["mom_12_1"] = round((prev_21 / prev_252 - 1) * 100, 3)

    # 60d realized vol (ann)
    closes = [b[1] for b in bars[-61:]]
    if len(closes) >= 21:
        rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0]
        if rets:
            mean = sum(rets) / len(rets)
            var = sum((r - mean) ** 2 for r in rets) / len(rets)
            vol_d = math.sqrt(var)
            vol_a = vol_d * math.sqrt(252)
            out["vol_60d_annualized"] = round(vol_a * 100, 2)
            # Vol-adjusted 3m momentum
            if "ret_3m" in out and vol_a > 0:
                out["vol_adj_mom_3m"] = round(out["ret_3m"] / (vol_a * 100), 3)

    # 52-week high distance
    if len(bars) >= 252:
        recent = bars[-252:]
        high_252 = max(b[2] for b in recent)
        if high_252 > 0:
            out["pct_from_52w_high"] = round((last / high_252 - 1) * 100, 3)
            out["high_252w"] = high_252

    # Acceleration: 1m vs 6m
    r1m = out.get("ret_1m")
    r6m = out.get("ret_6m")
    if r1m is not None and r6m is not None and r6m != 0:
        # Annualize 1m and compare to 6m annualized
        r1m_ann = (1 + r1m / 100) ** 12 - 1
        r6m_ann = (1 + r6m / 100) ** 2 - 1
        out["acceleration"] = round((r1m_ann - r6m_ann) * 100, 3)

    return out


def percentile_rank(value, sorted_vals):
    """0-100 percentile rank of value within sorted_vals (asc)."""
    if not sorted_vals or value is None:
        return None
    n = len(sorted_vals)
    # linear search for position
    lo, hi = 0, n
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_vals[mid] < value:
            lo = mid + 1
        else:
            hi = mid
    return round(lo / n * 100, 1)


def lambda_handler(event=None, context=None):
    started = time.time()
    universe = get_universe()
    print(f"[mom] universe size: {len(universe)}")
    if not universe:
        return {"statusCode": 500, "body": "no universe"}

    # Fetch all bars in parallel
    results = {}
    fetched = 0
    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = {ex.submit(polygon_bars, t[0]): t for t in universe}
        for f in as_completed(futs):
            sym, name, sector, mcap = futs[f]
            bars = f.result()
            fetched += 1
            if bars and len(bars) >= 252:
                m = compute_momentum(bars)
                if m:
                    m["symbol"] = sym
                    m["name"] = name
                    m["sector"] = sector
                    m["market_cap"] = mcap
                    results[sym] = m

    print(f"[mom] fetched={fetched}  with_data={len(results)}  duration_so_far={time.time()-started:.1f}s")

    rows = list(results.values())

    # Compute percentile ranks for each window
    for w in ["1m", "3m", "6m", "12m"]:
        key = f"ret_{w}"
        sorted_vals = sorted([r[key] for r in rows if r.get(key) is not None])
        for r in rows:
            if r.get(key) is not None:
                r[f"rank_{w}"] = percentile_rank(r[key], sorted_vals)

    # Composite momentum score (avg of 4 ranks)
    for r in rows:
        ranks = [r.get(f"rank_{w}") for w in ["1m", "3m", "6m", "12m"]]
        ranks = [x for x in ranks if x is not None]
        if len(ranks) == 4:
            r["composite_score"] = round(sum(ranks) / 4, 1)
        else:
            r["composite_score"] = None

    # Sort
    rows_with_composite = [r for r in rows if r.get("composite_score") is not None]
    by_composite = sorted(rows_with_composite, key=lambda x: -x["composite_score"])

    # 12-1
    rows_12_1 = [r for r in rows if r.get("mom_12_1") is not None]
    by_12_1 = sorted(rows_12_1, key=lambda x: -x["mom_12_1"])

    # Acceleration
    rows_accel = [r for r in rows if r.get("acceleration") is not None]
    by_accel = sorted(rows_accel, key=lambda x: -x["acceleration"])

    # 52w high distance
    rows_52w = [r for r in rows if r.get("pct_from_52w_high") is not None]
    by_52w = sorted(rows_52w, key=lambda x: -x["pct_from_52w_high"])

    # Vol-adjusted
    rows_voladj = [r for r in rows if r.get("vol_adj_mom_3m") is not None]
    by_voladj = sorted(rows_voladj, key=lambda x: -x["vol_adj_mom_3m"])

    # Sector breakdown
    by_sector = {}
    for r in rows_with_composite:
        sec = r.get("sector") or "Unknown"
        by_sector.setdefault(sec, []).append(r)
    sector_avg = []
    for sec, items in by_sector.items():
        avg = sum(it["composite_score"] for it in items) / len(items)
        sector_avg.append({
            "sector": sec,
            "avg_composite": round(avg, 1),
            "n_stocks": len(items),
            "top_stock": max(items, key=lambda x: x["composite_score"])["symbol"],
        })
    sector_avg.sort(key=lambda x: -x["avg_composite"])

    out = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 2),
        "universe_size": len(universe),
        "n_with_data": len(rows_with_composite),
        "fields": {
            "ret_1m": "1-month total return %",
            "ret_3m": "3-month total return %",
            "ret_6m": "6-month total return %",
            "ret_12m": "12-month total return %",
            "mom_12_1": "12-month return excluding last month (academic momentum factor)",
            "vol_60d_annualized": "60-day realized vol annualized %",
            "vol_adj_mom_3m": "3m return / annualized vol",
            "pct_from_52w_high": "% distance from 52w high (negative = below)",
            "acceleration": "Annualized 1m return - annualized 6m return",
            "composite_score": "Average percentile rank across 1m/3m/6m/12m (0-100)",
        },
        "top_50_composite": by_composite[:50],
        "bottom_50_composite": by_composite[-50:],
        "top_20_mom_12_1": by_12_1[:20],
        "top_20_acceleration": by_accel[:20],
        "top_20_at_52w_high": by_52w[:20],
        "top_20_vol_adj": by_voladj[:20],
        "sector_breakdown": sector_avg,
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=body, ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[mom] wrote {len(body):,}b in {out['duration_s']}s — top: {by_composite[0]['symbol']} score={by_composite[0]['composite_score']}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "duration_s": out["duration_s"],
            "universe_size": len(universe),
            "n_with_data": len(rows_with_composite),
            "top_5": [r["symbol"] for r in by_composite[:5]],
            "bottom_5": [r["symbol"] for r in by_composite[-5:]],
            "top_sector": sector_avg[0]["sector"] if sector_avg else None,
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2))
